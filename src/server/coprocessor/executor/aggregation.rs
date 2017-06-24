// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// FIXME: remove later
#![allow(dead_code)]

use util::collections::{HashMap, HashMapEntry as Entry};

use super::{Executor, Row};
use super::topn::ExprColumnRefVisitor;
use super::super::Result;
use super::super::endpoint::{inflate_with_col, SINGLE_GROUP};
use super::super::aggregate::{self, AggrFunc};

use tipb::schema::ColumnInfo;
use tipb::executor::Aggregation;
use tipb::expression::Expr;
use util::codec::datum::{self, DatumEncoder, approximate_size};
use util::codec::table::{RowColMeta, RowColsDict};
use util::xeval::{Evaluator, EvalContext};


struct AggregationExecutor<'a> {
    group_by: Vec<Expr>,
    aggr_func: Vec<Expr>,
    gks: Vec<Vec<u8>>,
    gk_aggrs: HashMap<Vec<u8>, Vec<Box<AggrFunc>>>,
    cursor: usize,
    executed: bool,
    ctx: EvalContext,
    cols: Vec<ColumnInfo>,
    src: &'a mut Executor,
}

impl<'a> AggregationExecutor<'a> {
    fn new(mut meta: Aggregation,
           ctx: EvalContext,
           columns: &[ColumnInfo],
           src: &'a mut Executor)
           -> Result<AggregationExecutor<'a>> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new();
        let group_by = meta.take_group_by().into_vec();
        try!(visitor.batch_visit(&group_by));
        let aggr_func = meta.take_agg_func().into_vec();
        try!(visitor.batch_visit(&aggr_func));
        // filter from all cols
        let cols = columns.iter()
            .filter(|col| visitor.col_ids.contains(&col.get_column_id()))
            .cloned()
            .collect();

        Ok(AggregationExecutor {
            group_by: group_by,
            aggr_func: aggr_func,
            gks: vec![],
            gk_aggrs: map![],
            cursor: 0,
            executed: false,
            ctx: ctx,
            cols: cols,
            src: src,
        })
    }

    fn get_group_key(&mut self, eval: &mut Evaluator) -> Result<Vec<u8>> {
        if self.group_by.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = box_try!(eval.eval(&self.ctx, expr));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(row) = try!(self.src.next()) {
            let mut eval = Evaluator::default();
            try!(inflate_with_col(&mut eval, &self.ctx, &row.data, &self.cols, row.handle));
            let gk = try!(self.get_group_key(&mut eval));
            match self.gk_aggrs.entry(gk.clone()) {
                Entry::Vacant(e) => {
                    let mut aggrs = Vec::with_capacity(self.aggr_func.len());
                    for expr in &self.aggr_func {
                        let mut aggr = try!(aggregate::build_aggr_func(expr));
                        let vals = box_try!(eval.batch_eval(&self.ctx, expr.get_children()));
                        try!(aggr.update(&self.ctx, vals));
                        aggrs.push(aggr);
                    }
                    self.gks.push(gk);
                    e.insert(aggrs);
                }
                Entry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.aggr_func.iter().zip(aggrs) {
                        let vals = box_try!(eval.batch_eval(&self.ctx, expr.get_children()));
                        box_try!(aggr.update(&self.ctx, vals));
                    }
                }
            }
        }
        Ok(())
    }
}


impl<'a> Executor for AggregationExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        if !self.executed {
            try!(self.aggregate());
            self.executed = true;
            assert_eq!(self.gks.len(), self.gk_aggrs.len());
        }

        if self.cursor >= self.gks.len() {
            return Ok(None);
        }
        // calc all aggr func
        let mut aggr_cols = Vec::with_capacity(2 * self.aggr_func.len());
        let gk = &self.gks[self.cursor];
        let mut aggrs = self.gk_aggrs.remove(gk).unwrap();
        for aggr in &mut aggrs {
            try!(aggr.calc(&mut aggr_cols));
        }
        // construct row data
        let value_size = gk.len() + approximate_size(&aggr_cols, false);
        let mut value = Vec::with_capacity(value_size);
        let mut meta = HashMap::with_capacity(1 + 2 * aggr_cols.len());
        let (mut id, mut offset) = (0, 0);
        /// push gk col
        value.extend_from_slice(gk);
        meta.insert(id, RowColMeta::new(offset, (value.len() - offset)));
        id = id + 1;
        offset = value.len();
        /// push aggr col
        for i in 0..aggr_cols.len() {
            box_try!(value.encode(&aggr_cols[i..i + 1], false));
            meta.insert(id, RowColMeta::new(offset, (value.len() - offset)));
            id = id + 1;
            offset = value.len();
        }
        self.cursor += 1;
        Ok(Some(Row {
            handle: 0,
            data: RowColsDict::new(meta, value),
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::i64;
    use super::super::table_scan::TableScanExecutor;
    use super::super::scanner::test::{TestStore, get_range, new_col_info};
    use super::super::topn::test::gen_table_data;
    use util::codec::datum::{Datum, DatumDecoder};
    use util::codec::number::NumberEncoder;
    use util::codec::mysql::decimal::Decimal;
    use util::codec::mysql::types;
    use storage::Statistics;

    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use protobuf::RepeatedField;

    #[inline]
    fn build_expr(tp: ExprType, id: Option<i64>, child: Option<Expr>) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        if tp == ExprType::ColumnRef {
            expr.mut_val().encode_i64(id.unwrap()).unwrap();
        } else {
            expr.mut_children().push(child.unwrap());
        }
        expr
    }

    fn build_group_by(col_ids: &[i64]) -> Vec<Expr> {
        let mut group_by = Vec::with_capacity(col_ids.len());
        for id in col_ids {
            group_by.push(build_expr(ExprType::ColumnRef, Some(*id), None));
        }
        group_by
    }

    fn build_aggr_func(aggrs: &[(ExprType, i64)]) -> Vec<Expr> {
        let mut aggr_func = Vec::with_capacity(aggrs.len());
        for aggr in aggrs {
            let &(tp, id) = aggr;
            let col_ref = build_expr(ExprType::ColumnRef, Some(id), None);
            aggr_func.push(build_expr(tp, None, Some(col_ref)));
        }
        aggr_func
    }

    #[test]
    fn test_aggregation() {
        // prepare data and store
        let tid = 1;
        let cis = vec![new_col_info(1, types::LONG_LONG),
                       new_col_info(2, types::VARCHAR),
                       new_col_info(3, types::NEW_DECIMAL)];
        let raw_data = vec![vec![Datum::I64(1), Datum::Bytes(b"a".to_vec()), Datum::Dec(7.into())],
                            vec![Datum::I64(2), Datum::Bytes(b"a".to_vec()), Datum::Dec(7.into())],
                            vec![Datum::I64(3), Datum::Bytes(b"b".to_vec()), Datum::Dec(8.into())],
                            vec![Datum::I64(4), Datum::Bytes(b"a".to_vec()), Datum::Dec(7.into())],
                            vec![Datum::I64(5), Datum::Bytes(b"f".to_vec()), Datum::Dec(5.into())],
                            vec![Datum::I64(6), Datum::Bytes(b"b".to_vec()), Datum::Dec(8.into())],
                            vec![Datum::I64(7), Datum::Bytes(b"f".to_vec()), Datum::Dec(6.into())]];
        let table_data = gen_table_data(tid, &cis, &raw_data);
        let mut test_store = TestStore::new(&table_data);
        // init table scan meta
        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // init TableScan Exectutor
        let key_ranges = vec![get_range(tid, i64::MIN, i64::MAX)];
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut statistics = Statistics::default();
        let mut ts_ect =
            TableScanExecutor::new(table_scan, key_ranges, snapshot, &mut statistics, start_ts);

        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![2, 3];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(RepeatedField::from_vec(group_by));
        let aggr_funcs = vec![(ExprType::Avg, 1), (ExprType::Count, 3)];
        let aggr_funcs = build_aggr_func(&aggr_funcs);
        aggregation.set_agg_func(RepeatedField::from_vec(aggr_funcs));
        // init Aggregation Executor
        let mut aggr_ect =
            AggregationExecutor::new(aggregation, EvalContext::default(), &cis, &mut ts_ect)
                .unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(row) = aggr_ect.next().unwrap() {
            row_data.push(row.data);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![(b"a", Decimal::from(7), 3 as u64, Decimal::from(7), 3 as u64),
                                   (b"b", Decimal::from(8), 2 as u64, Decimal::from(9), 2 as u64),
                                   (b"f", Decimal::from(5), 1 as u64, Decimal::from(5), 1 as u64),
                                   (b"f", Decimal::from(6), 1 as u64, Decimal::from(7), 1 as u64)];
        let expect_col_cnt = 4;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            assert_eq!(row.len(), expect_col_cnt);
            let gk = row.get(0).unwrap().decode().unwrap();
            assert_eq!(gk[0], Datum::from(expect_cols.0.as_ref()));
            assert_eq!(gk[1], Datum::from(expect_cols.1));
            assert_eq!(row.get(1).unwrap().decode_datum().unwrap(),
                       Datum::from(expect_cols.2));
            assert_eq!(row.get(2).unwrap().decode_datum().unwrap(),
                       Datum::from(expect_cols.3));
            assert_eq!(row.get(3).unwrap().decode_datum().unwrap(),
                       Datum::from(expect_cols.4));
        }
    }
}
