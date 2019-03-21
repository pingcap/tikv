// Copyright 2019 PingCAP, Inc.
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

use cop_datatype::EvalType;
use kvproto::coprocessor::KeyRange;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use crate::storage::Store;

use super::interface::*;
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::{Error, Result};

pub struct BatchIndexScanExecutor<S: Store>(
    super::scan_executor::ScanExecutor<
        S,
        IndexScanExecutorImpl,
        super::ranges_iter::PointRangeConditional,
    >,
);

impl<S: Store> BatchIndexScanExecutor<S> {
    pub fn new(
        store: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        desc: bool,
        unique: bool,
        // TODO: this does not mean that it is a unique index scan. What does it mean?
    ) -> Result<Self> {
        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_len_without_handle = 0;
        let mut decode_handle = false;
        for ci in &columns_info {
            schema.push(super::scan_executor::field_type_from_column_info(&ci));
            if ci.get_pk_handle() {
                decode_handle = true;
            } else {
                columns_len_without_handle += 1;
            }
        }

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_len_without_handle,
            decode_handle,
        };
        let wrapper = super::scan_executor::ScanExecutor::new(
            imp,
            store,
            desc,
            key_ranges,
            super::ranges_iter::PointRangeConditional::new(unique),
        )?;
        Ok(Self(wrapper))
    }
}

impl<S: Store> BatchExecutor for BatchIndexScanExecutor<S> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(expect_rows)
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.0.collect_statistics(destination);
    }
}

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// Number of interested columns (exclude PK handle column).
    columns_len_without_handle: usize,

    /// Whether PK handle column is interested. Handle will be always placed in the last column.
    decode_handle: bool,
}

impl super::scan_executor::ScanExecutorImpl for IndexScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    #[inline]
    fn build_scanner<S: Store>(
        &self,
        store: &S,
        desc: bool,
        range: KeyRange,
    ) -> Result<Scanner<S>> {
        Ok(Scanner::new(
            store,
            crate::coprocessor::dag::ScanOn::Index,
            desc,
            false,
            range,
        )?)
    }

    fn build_column_vec(&self, expect_rows: usize) -> LazyBatchColumnVec {
        // Construct empty columns, with PK in decoded format and the rest in raw format.

        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..self.columns_len_without_handle {
            columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
        }
        if self.decode_handle {
            // For primary key, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, in the value (for unique index).
            // Note that for normal index, primary key is appended at the end of key with a
            // datum flag.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                expect_rows,
                EvalType::Int,
            ));
        }

        LazyBatchColumnVec::from(columns)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        mut value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::coprocessor::codec::{datum, table};
        use crate::util::codec::number;
        use byteorder::{BigEndian, ReadBytesExt};

        // The payload part of the key
        let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];

        for i in 0..self.columns_len_without_handle {
            let (val, remaining) = datum::split_datum(key_payload, false)?;
            columns[i].push_raw(val);
            key_payload = remaining;
        }

        if self.decode_handle {
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            let handle_val = if key_payload.is_empty() {
                // This is a unique index, and we should look up PK handle in value.

                // NOTE: it is not `number::decode_i64`.
                value.read_i64::<BigEndian>().map_err(|_| {
                    Error::Other(box_err!("Failed to decode handle in value as i64"))
                })?
            } else {
                // This is a normal index. The remaining payload part is the PK handle.
                // Let's decode it and put in the column.

                let flag = key_payload[0];
                let mut val = &key_payload[1..];

                match flag {
                    datum::INT_FLAG => number::decode_i64(&mut val).map_err(|_| {
                        Error::Other(box_err!("Failed to decode handle in key as i64"))
                    })?,
                    datum::UINT_FLAG => {
                        (number::decode_u64(&mut val).map_err(|_| {
                            Error::Other(box_err!("Failed to decode handle in key as u64"))
                        })?) as i64
                    }
                    _ => {
                        return Err(Error::Other(box_err!("Unexpected handle flag {}", flag)));
                    }
                }
            };

            columns[self.columns_len_without_handle]
                .mut_decoded()
                .push_int(Some(handle_val));
        }

        Ok(())
    }
}
