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

use std::iter::Peekable;
use std::mem;
use std::sync::Arc;
use std::vec::IntoIter;

use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;
use tipb::schema::ColumnInfo;

use storage::{Key, Store};
use util::collections::HashSet;

use coprocessor::codec::table;
use coprocessor::util;
use coprocessor::*;

use super::{CountCollector, ExecutionSummary, ExecutionSummaryCollector, ExecutorMetrics};
use super::{Executor, Row};
use super::{ScanOn, Scanner};

/// Scans rows from table records.
///
/// `row_id` in the key and datums in the value are processed.
pub struct TableScanExecutor<S: Store, C1: CountCollector, C2: ExecutionSummaryCollector> {
    store: S,
    desc: bool,
    col_ids: HashSet<i64>,
    columns: Arc<Vec<ColumnInfo>>,
    key_ranges: Peekable<IntoIter<KeyRange>>,
    // The current `KeyRange` scanning on, used to build `scan_range`.
    current_range: Option<KeyRange>,
    // The `KeyRange` scaned between `start_scan` and `stop_scan`.
    scan_range: KeyRange,
    scanner: Option<Scanner<S>>,
    metrics: ExecutorMetrics,

    // The number of scan keys for each range.
    count_collector: C1,

    // The execution detail of this executor
    exec_detail_collector: C2,

    first_collect: bool,
}

impl<S: Store, C1: CountCollector, C2: ExecutionSummaryCollector> TableScanExecutor<S, C1, C2> {
    pub fn new(
        mut meta: TableScan,
        mut key_ranges: Vec<KeyRange>,
        store: S,
        count_collector: C1,
        exec_detail_collector: C2,
    ) -> Result<TableScanExecutor<S, C1, C2>> {
        box_try!(table::check_table_ranges(&key_ranges));
        let col_ids = meta
            .get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(|c| c.get_column_id())
            .collect();

        let desc = meta.get_desc();
        if desc {
            key_ranges.reverse();
        }

        Ok(Self {
            store,
            desc,
            col_ids,
            columns: Arc::new(meta.take_columns().to_vec()),
            key_ranges: key_ranges.into_iter().peekable(),
            current_range: None,
            scan_range: KeyRange::default(),
            scanner: None,
            metrics: Default::default(),
            count_collector,
            exec_detail_collector,
            first_collect: true,
        })
    }

    fn get_row_from_range_scanner(&mut self) -> Result<Option<Row>> {
        if let Some(scanner) = self.scanner.as_mut() {
            self.metrics.scan_counter.inc_range();
            let (key, value) = match scanner.next_row()? {
                Some((key, value)) => (key, value),
                None => return Ok(None),
            };
            let row_data = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(&key));
            return Ok(Some(Row::origin(h, row_data, self.columns.clone())));
        }
        Ok(None)
    }

    fn get_row_from_point(&mut self, mut range: KeyRange) -> Result<Option<Row>> {
        let key = range.take_start();
        let value = self
            .store
            .get(&Key::from_raw(&key), &mut self.metrics.cf_stats)?;
        if let Some(value) = value {
            let values = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(&key));
            return Ok(Some(Row::origin(h, values, self.columns.clone())));
        }
        Ok(None)
    }

    fn new_scanner(&self, range: KeyRange) -> Result<Scanner<S>> {
        Scanner::new(
            &self.store,
            ScanOn::Table,
            self.desc,
            self.col_ids.is_empty(),
            range,
        ).map_err(Error::from)
    }
}

impl<S: Store, C1: CountCollector, C2: ExecutionSummaryCollector> Executor
    for TableScanExecutor<S, C1, C2>
{
    fn next(&mut self) -> Result<Option<Row>> {
        self.exec_detail_collector.inc_iterations();
        let _time = self.exec_detail_collector.accumulate_time();
        loop {
            if let Some(row) = self.get_row_from_range_scanner()? {
                self.count_collector.inc_counter();
                self.exec_detail_collector.inc_produced_rows();
                return Ok(Some(row));
            }

            if let Some(range) = self.key_ranges.next() {
                self.count_collector.handle_new_range();
                self.current_range = Some(range.clone());
                if util::is_point(&range) {
                    self.metrics.scan_counter.inc_point();
                    if let Some(row) = self.get_row_from_point(range)? {
                        self.count_collector.inc_counter();
                        self.exec_detail_collector.inc_produced_rows();
                        return Ok(Some(row));
                    }
                    continue;
                }
                self.scanner = match self.scanner.take() {
                    Some(mut scanner) => {
                        box_try!(scanner.reset_range(range, &self.store));
                        Some(scanner)
                    }
                    None => Some(self.new_scanner(range)?),
                };
                continue;
            }
            return Ok(None);
        }
    }

    fn start_scan(&mut self) {
        if let Some(range) = self.current_range.as_ref() {
            if !util::is_point(range) {
                let scanner = self.scanner.as_ref().unwrap();
                return scanner.start_scan(&mut self.scan_range);
            }
        }

        if let Some(range) = self.key_ranges.peek() {
            if !self.desc {
                self.scan_range.set_start(range.get_start().to_owned());
            } else {
                self.scan_range.set_end(range.get_end().to_owned());
            }
        }
    }

    fn stop_scan(&mut self) -> Option<KeyRange> {
        let mut ret_range = mem::replace(&mut self.scan_range, KeyRange::default());
        match self.current_range.as_ref() {
            Some(range) => {
                if !util::is_point(range) {
                    let scanner = self.scanner.as_mut().unwrap();
                    if scanner.stop_scan(&mut ret_range) {
                        return Some(ret_range);
                    }
                }
                if !self.desc {
                    ret_range.set_end(range.get_end().to_owned());
                } else {
                    ret_range.set_start(range.get_start().to_owned());
                }
            }
            // `stop_scan` will be called only if we get some data from
            // `current_range` so that it's unreachable.
            None => unreachable!(),
        }
        Some(ret_range)
    }

    #[inline]
    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.count_collector.collect(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        metrics.merge(&mut self.metrics);
        if let Some(scanner) = self.scanner.as_mut() {
            scanner.collect_statistics_into(&mut metrics.cf_stats);
        }
        if self.first_collect {
            metrics.executor_count.table_scan += 1;
            self.first_collect = false;
        }
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    fn collect_execution_summary(&mut self, target: &mut [ExecutionSummary]) {
        self.exec_detail_collector.collect(target);
    }
}

#[cfg(test)]
mod tests {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::schema::ColumnInfo;

    use storage::SnapshotStore;

    use super::super::scanner::tests::{
        get_point_range, get_range, prepare_table_data, Data, TestStore,
    };
    use super::super::{
        CountCollectorDisabled, CountCollectorNormal, ExecutionSummaryCollectorDisabled,
    };
    use super::*;

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: Data,
        store: TestStore,
        table_scan: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl TableScanTestWrapper {
        fn get_point_range(&self, handle: i64) -> KeyRange {
            get_point_range(TABLE_ID, handle)
        }
    }

    impl Default for TableScanTestWrapper {
        fn default() -> TableScanTestWrapper {
            let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
            let test_store = TestStore::new(&test_data.kv_data);
            let mut table_scan = TableScan::new();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store: test_store,
                table_scan,
                ranges: key_ranges,
                cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut wrapper = TableScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_range(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_range(handle);
        wrapper.ranges = vec![r1, r2];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner = TableScanExecutor::new(
            wrapper.table_scan,
            wrapper.ranges,
            store,
            CountCollectorNormal::default(),
            ExecutionSummaryCollectorDisabled,
        ).unwrap();

        let row = table_scanner.next().unwrap().unwrap().take_origin();
        assert_eq!(row.handle, handle as i64);
        assert_eq!(row.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
        let expected_counts = vec![0, 1];
        let mut counts = Vec::with_capacity(2);
        table_scanner.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_multiple_ranges() {
        let mut wrapper = TableScanTestWrapper::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner = TableScanExecutor::new(
            wrapper.table_scan,
            wrapper.ranges,
            store,
            CountCollectorDisabled,
            ExecutionSummaryCollectorDisabled,
        ).unwrap();

        for handle in 0..KEY_NUMBER {
            let row = table_scanner.next().unwrap().unwrap().take_origin();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut wrapper = TableScanTestWrapper::default();
        wrapper.table_scan.set_desc(true);

        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner = TableScanExecutor::new(
            wrapper.table_scan,
            wrapper.ranges,
            store,
            CountCollectorDisabled,
            ExecutionSummaryCollectorDisabled,
        ).unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner.next().unwrap().unwrap().take_origin();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
