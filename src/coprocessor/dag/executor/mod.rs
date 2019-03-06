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

use std::sync::Arc;

use cop_datatype::prelude::*;
use cop_datatype::FieldTypeFlag;
use kvproto::coprocessor::KeyRange;
use tipb::expression::{Expr, ExprType};
use tipb::schema::ColumnInfo;

use crate::util::codec::number;
use crate::util::collections::HashSet;

use crate::coprocessor::codec::datum::{self, Datum, DatumEncoder};
use crate::coprocessor::codec::table::{self, RowColsDict};
use crate::coprocessor::dag::expr::{EvalContext, EvalWarnings};
use crate::coprocessor::util;
use crate::coprocessor::*;

mod aggregate;
mod aggregation;
mod index_scan;
mod limit;
mod scan;
mod scanner;
mod selection;
mod table_scan;
mod topn;
mod topn_heap;

mod metrics;

pub use self::aggregation::{HashAggExecutor, StreamAggExecutor};
pub use self::index_scan::IndexScanExecutor;
pub use self::limit::LimitExecutor;
pub use self::metrics::*;
pub use self::scan::ScanExecutor;
pub use self::scanner::{ScanOn, Scanner};
pub use self::selection::SelectionExecutor;
pub use self::table_scan::TableScanExecutor;
pub use self::topn::TopNExecutor;

/// An expression tree visitor that extracts all column offsets in the tree.
pub struct ExprColumnRefVisitor {
    cols_offset: HashSet<usize>,
    cols_len: usize,
}

impl ExprColumnRefVisitor {
    pub fn new(cols_len: usize) -> ExprColumnRefVisitor {
        ExprColumnRefVisitor {
            cols_offset: HashSet::default(),
            cols_len,
        }
    }

    pub fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            let offset = box_try!(number::decode_i64(&mut expr.get_val())) as usize;
            if offset >= self.cols_len {
                return Err(Error::Other(box_err!(
                    "offset {} overflow, should be less than {}",
                    offset,
                    self.cols_len
                )));
            }
            self.cols_offset.insert(offset);
        } else {
            for sub_expr in expr.get_children() {
                self.visit(sub_expr)?;
            }
        }
        Ok(())
    }

    pub fn batch_visit(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    pub fn column_offsets(self) -> Vec<usize> {
        self.cols_offset.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct OriginCols {
    pub handle: i64,
    pub data: RowColsDict,
    cols: Arc<Vec<ColumnInfo>>,
}

/// Row generated by aggregation.
#[derive(Debug)]
pub struct AggCols {
    // row's suffix, may be the binary of the group by key.
    suffix: Vec<u8>,
    value: Vec<Datum>,
}

impl AggCols {
    pub fn get_binary(&self) -> Result<Vec<u8>> {
        let mut value =
            Vec::with_capacity(self.suffix.len() + datum::approximate_size(&self.value, false));
        box_try!(value.encode(&self.value, false));
        if !self.suffix.is_empty() {
            value.extend_from_slice(&self.suffix);
        }
        Ok(value)
    }
}

#[derive(Debug)]
pub enum Row {
    Origin(OriginCols),
    Agg(AggCols),
}

impl Row {
    pub fn origin(handle: i64, data: RowColsDict, cols: Arc<Vec<ColumnInfo>>) -> Row {
        Row::Origin(OriginCols::new(handle, data, cols))
    }

    pub fn agg(value: Vec<Datum>, suffix: Vec<u8>) -> Row {
        Row::Agg(AggCols { suffix, value })
    }

    pub fn take_origin(self) -> OriginCols {
        match self {
            Row::Origin(row) => row,
            _ => unreachable!(),
        }
    }

    pub fn get_binary(&self, output_offsets: &[u32]) -> Result<Vec<u8>> {
        match self {
            Row::Origin(row) => row.get_binary(output_offsets),
            Row::Agg(row) => row.get_binary(), // ignore output offsets for aggregation.
        }
    }
}

impl OriginCols {
    pub fn new(handle: i64, data: RowColsDict, cols: Arc<Vec<ColumnInfo>>) -> OriginCols {
        OriginCols { handle, data, cols }
    }

    // get binary of each column in order of columns
    pub fn get_binary_cols(&self) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(self.cols.len());
        for col in self.cols.iter() {
            if col.get_pk_handle() {
                let v = util::get_pk(col, self.handle);
                let bt = box_try!(datum::encode_value(&[v]));
                res.push(bt);
                continue;
            }
            let col_id = col.get_column_id();
            let value = match self.data.get(col_id) {
                None if col.has_default_val() => col.get_default_val().to_vec(),
                None if col.flag().contains(FieldTypeFlag::NOT_NULL) => {
                    return Err(box_err!("column {} of {} is missing", col_id, self.handle));
                }
                None => box_try!(datum::encode_value(&[Datum::Null])),
                Some(bs) => bs.to_vec(),
            };
            res.push(value);
        }
        Ok(res)
    }

    pub fn get_binary(&self, output_offsets: &[u32]) -> Result<Vec<u8>> {
        // TODO capacity is not enough
        let mut values = Vec::with_capacity(self.data.value.len());
        for offset in output_offsets {
            let col = &self.cols[*offset as usize];
            let col_id = col.get_column_id();
            match self.data.get(col_id) {
                Some(value) => values.extend_from_slice(value),
                None if col.get_pk_handle() => {
                    let pk = util::get_pk(col, self.handle);
                    box_try!(values.encode(&[pk], false));
                }
                None if col.has_default_val() => {
                    values.extend_from_slice(col.get_default_val());
                }
                None if col.flag().contains(FieldTypeFlag::NOT_NULL) => {
                    return Err(box_err!("column {} of {} is missing", col_id, self.handle));
                }
                None => {
                    box_try!(values.encode(&[Datum::Null], false));
                }
            }
        }
        Ok(values)
    }

    // inflate with the real value(Datum) for each columns in offsets
    // inflate with Datum::Null for those cols not in offsets.
    // It's used in expression since column is marked with offset
    // in expression.
    pub fn inflate_cols_with_offsets(
        &self,
        ctx: &EvalContext,
        offsets: &[usize],
    ) -> Result<Vec<Datum>> {
        let mut res = vec![Datum::Null; self.cols.len()];
        for offset in offsets {
            let col = &self.cols[*offset];
            if col.get_pk_handle() {
                let v = util::get_pk(col, self.handle);
                res[*offset] = v;
            } else {
                let col_id = col.get_column_id();
                let value = match self.data.get(col_id) {
                    None if col.has_default_val() => {
                        // TODO: optimize it to decode default value only once.
                        box_try!(table::decode_col_value(
                            &mut col.get_default_val(),
                            ctx,
                            col
                        ))
                    }
                    None if col.flag().contains(FieldTypeFlag::NOT_NULL) => {
                        return Err(box_err!("column {} of {} is missing", col_id, self.handle));
                    }
                    None => Datum::Null,
                    Some(mut bs) => box_try!(table::decode_col_value(&mut bs, ctx, col)),
                };
                res[*offset] = value;
            }
        }
        Ok(res)
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
    fn collect_output_counts(&mut self, counts: &mut Vec<i64>);
    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics);
    fn get_len_of_columns(&self) -> usize;

    /// Only executors with eval computation need to implement `take_eval_warnings`
    /// It returns warnings happened during eval computation.
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        None
    }

    /// Only `TableScan` and `IndexScan` need to implement `start_scan`.
    fn start_scan(&mut self) {}

    /// Only `TableScan` and `IndexScan` need to implement `stop_scan`.
    ///
    /// It returns a `KeyRange` the executor has scaned.
    fn stop_scan(&mut self) -> Option<KeyRange> {
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::{Executor, TableScanExecutor};
    use crate::coprocessor::codec::{table, Datum};
    use crate::storage::engine::{Engine, Modify, RocksEngine, RocksSnapshot, TestEngineBuilder};
    use crate::storage::mvcc::MvccTxn;
    use crate::storage::SnapshotStore;
    use crate::storage::{Key, Mutation, Options};
    use crate::util::codec::number::NumberEncoder;
    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use kvproto::{
        coprocessor::KeyRange,
        kvrpcpb::{Context, IsolationLevel},
    };
    use protobuf::RepeatedField;
    use tipb::{
        executor::TableScan,
        expression::{Expr, ExprType},
        schema::ColumnInfo,
    };

    pub fn build_expr(tp: ExprType, id: Option<i64>, child: Option<Expr>) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        if tp == ExprType::ColumnRef {
            expr.mut_val().encode_i64(id.unwrap()).unwrap();
        } else {
            expr.mut_children().push(child.unwrap());
        }
        expr
    }

    pub fn new_col_info(cid: i64, tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.as_mut_accessor().set_tp(tp);
        col_info.set_column_id(cid);
        col_info
    }

    // the first column should be i64 since it will be used as row handle
    pub fn gen_table_data(
        tid: i64,
        cis: &[ColumnInfo],
        rows: &[Vec<Datum>],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut kv_data = Vec::new();
        let col_ids: Vec<i64> = cis.iter().map(|c| c.get_column_id()).collect();
        for cols in rows.iter() {
            let col_values: Vec<_> = cols.to_vec();
            let value = table::encode_row(col_values, &col_ids).unwrap();
            let key = table::encode_row_key(tid, cols[0].i64());
            kv_data.push((key, value));
        }
        kv_data
    }

    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;

    pub struct TestStore {
        snapshot: RocksSnapshot,
        ctx: Context,
        engine: RocksEngine,
    }

    impl TestStore {
        pub fn new(kv_data: &[(Vec<u8>, Vec<u8>)]) -> TestStore {
            let engine = TestEngineBuilder::new().build().unwrap();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                snapshot,
                ctx,
                engine,
            };
            store.init_data(kv_data);
            store
        }

        fn init_data(&mut self, kv_data: &[(Vec<u8>, Vec<u8>)]) {
            if kv_data.is_empty() {
                return;
            }

            // do prewrite.
            let txn_motifies = {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
                let mut pk = vec![];
                for &(ref key, ref value) in kv_data {
                    if pk.is_empty() {
                        pk = key.clone();
                    }
                    txn.prewrite(
                        Mutation::Put((Key::from_raw(key), value.to_vec())),
                        &pk,
                        &Options::default(),
                    )
                    .unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_motifies);

            // do commit
            let txn_modifies = {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
                for &(ref key, _) in kv_data {
                    txn.commit(Key::from_raw(key), COMMIT_TS).unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_modifies);
        }

        #[inline]
        fn write_modifies(&mut self, txn: Vec<Modify>) {
            self.engine.write(&self.ctx, txn).unwrap();
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        pub fn get_snapshot(&mut self) -> (RocksSnapshot, u64) {
            (self.snapshot.clone(), COMMIT_TS + 1)
        }
    }

    #[inline]
    pub fn get_range(table_id: i64, start: i64, end: i64) -> KeyRange {
        let mut key_range = KeyRange::new();
        key_range.set_start(table::encode_row_key(table_id, start));
        key_range.set_end(table::encode_row_key(table_id, end));
        key_range
    }

    pub fn gen_table_scan_executor(
        tid: i64,
        cis: Vec<ColumnInfo>,
        raw_data: &[Vec<Datum>],
        key_ranges: Option<Vec<KeyRange>>,
    ) -> Box<dyn Executor + Send> {
        let table_data = gen_table_data(tid, &cis, raw_data);
        let mut test_store = TestStore::new(&table_data);

        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));

        let key_ranges = key_ranges.unwrap_or_else(|| vec![get_range(tid, 0, i64::max_value())]);

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        Box::new(TableScanExecutor::table_scan(table_scan, key_ranges, store, true).unwrap())
    }
}
