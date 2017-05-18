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

use server::coprocessor::endpoint::{prefix_next, is_point};
use server::coprocessor::Result;
use tipb::executor::TableScan;
use kvproto::coprocessor::KeyRange;
use util::codec::table;
use util::collections::HashSet;
use storage::{Snapshot, Statistics};
use super::{Executor, Row};
use super::scanner::Scanner;

struct TableScanExecutor<'a> {
    meta: TableScan,
    col_ids: HashSet<i64>,
    cursor: usize,
    key_ranges: Vec<KeyRange>,
    scanner: Scanner<'a>,
}

impl<'a> TableScanExecutor<'a> {
    #[allow(dead_code)] //TODO:remove it
    pub fn new(meta: TableScan,
               key_ranges: Vec<KeyRange>,
               snapshot: &'a Snapshot,
               statistics: &'a mut Statistics,
               start_ts: u64)
               -> TableScanExecutor<'a> {
        let col_ids = meta.get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(|c| c.get_column_id())
            .collect();
        let scanner = Scanner::new(meta.get_desc(), false, snapshot, statistics, start_ts);
        TableScanExecutor {
            meta: meta,
            col_ids: col_ids,
            scanner: scanner,
            key_ranges: key_ranges,
            cursor: Default::default(),
        }
    }

    fn get_row_from_range(&mut self) -> Result<Option<Row>> {
        let range = &self.key_ranges[self.cursor];
        let kv = try!(self.scanner.next_row(range));
        let (key, value) = match kv {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };
        let h = box_try!(table::decode_handle(&key));
        let row_data = box_try!(table::cut_row(value, &self.col_ids));
        let seek_key = if self.meta.get_desc() {
            box_try!(table::truncate_as_row_key(&key)).to_vec()
        } else {
            prefix_next(&key)
        };
        self.scanner.set_seek_key(Some(seek_key));
        Ok(Some(Row::new(h, row_data)))
    }

    fn get_row_from_point(&mut self) -> Result<Option<Row>> {
        let key = self.key_ranges[self.cursor].get_start();
        let value = try!(self.scanner.get_row(key));
        if let Some(value) = value {
            let values = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(key));
            return Ok(Some(Row::new(h, values)));
        }
        Ok(None)
    }
}

impl<'a> Executor for TableScanExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        while self.cursor < self.key_ranges.len() {
            if is_point(&self.key_ranges[self.cursor]) {
                let data = try!(self.get_row_from_point());
                self.scanner.set_seek_key(None);
                self.cursor += 1;
                return Ok(data);
            }

            let data = try!(self.get_row_from_range());
            if data.is_none() {
                self.scanner.set_seek_key(None);
                self.cursor += 1;
                continue;
            }
            return Ok(data);
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::scanner::test::{Data, TestStore, prepare_table_data, get_range};
    use std::i64;
    use tipb::schema::ColumnInfo;
    use storage::Statistics;
    use protobuf::RepeatedField;
    use server::coprocessor::endpoint::{is_point, prefix_next};

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanExecutorMeta {
        data: Data,
        store: TestStore,
        table_scan: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl Default for TableScanExecutorMeta {
        fn default() -> TableScanExecutorMeta {
            let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
            let test_store = TestStore::new(&test_data.kv_data, test_data.pk.clone());
            let mut table_scan = TableScan::new();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanExecutorMeta {
                data: test_data,
                store: test_store,
                table_scan: table_scan,
                ranges: key_ranges,
                cols: cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut statistics = Statistics::default();
        let mut meta = TableScanExecutorMeta::default();
        // prepare range
        let mut range = KeyRange::new();
        range.set_start(meta.data.pk.clone());
        let end = prefix_next(&meta.data.pk.clone());
        range.set_end(end);
        assert!(is_point(&range));
        meta.ranges = vec![range];

        let (snapshot, start_ts) = meta.store.get_snapshot();

        let mut table_scanner = TableScanExecutor::new(meta.table_scan,
                                                       meta.ranges,
                                                       snapshot,
                                                       &mut statistics,
                                                       start_ts);

        let row = table_scanner.next().unwrap().unwrap();
        assert_eq!(row.handle, meta.data.pk_handle);
        assert_eq!(row.data.len(), meta.cols.len());

        let encode_data = &meta.data.encode_data[0];
        for col in &meta.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(encode_data[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_multiple_ranges() {
        let mut statistics = Statistics::default();
        let mut meta = TableScanExecutorMeta::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);
        let r3 = get_range(TABLE_ID, (KEY_NUMBER / 2) as i64, i64::MAX);
        meta.ranges = vec![r1, r2, r3];

        let (snapshot, start_ts) = meta.store.get_snapshot();
        let mut table_scanner = TableScanExecutor::new(meta.table_scan,
                                                       meta.ranges,
                                                       snapshot,
                                                       &mut statistics,
                                                       start_ts);

        for handle in 0..KEY_NUMBER {
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), meta.cols.len());
            let encode_data = &meta.data.encode_data[handle];
            for col in &meta.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut statistics = Statistics::default();
        let mut meta = TableScanExecutorMeta::default();
        meta.table_scan.set_desc(true);
        let (snapshot, start_ts) = meta.store.get_snapshot();
        let mut table_scanner = TableScanExecutor::new(meta.table_scan,
                                                       meta.ranges,
                                                       snapshot,
                                                       &mut statistics,
                                                       start_ts);

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), meta.cols.len());
            let encode_data = &meta.data.encode_data[handle];
            for col in &meta.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
