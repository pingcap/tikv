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

use util::codec::number;
use util::collections::HashSet;

use coprocessor::codec::datum::{self, Datum, DatumEncoder};
use coprocessor::codec::table::{self, RowColsDict};
use coprocessor::dag::expr::{EvalContext, EvalWarnings};
use coprocessor::util;
use coprocessor::*;

mod aggregate;
mod aggregation;
mod index_scan;
mod limit;
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
    fn collect_execution_summary(&mut self, target: &mut [ExecutionSummary]);
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
