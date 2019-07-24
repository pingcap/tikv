// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::Peekable, mem, sync::Arc, vec::IntoIter};

use kvproto::coprocessor::KeyRange;
use tipb::schema::ColumnInfo;

use super::{Executor, Row};
use crate::coprocessor::codec::table;
use crate::coprocessor::dag::execute_stats::ExecuteStats;
use crate::coprocessor::dag::expr::EvalWarnings;
use crate::coprocessor::{Error, Result};
use crate::storage::{Key, Statistics, Store};

// an InnerExecutor is used in ScanExecutor,
// hold the different logics between table scan and index scan
pub trait InnerExecutor: Send {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>>;
    // checks if the key range represents a point.
    fn is_point(&self, range: &KeyRange) -> bool;

    // indicate whether the scan is a key only scan.
    fn key_only(&self) -> bool;
}

// Executor for table scan and index scan
pub struct ScanExecutor<S: Store, T: InnerExecutor> {
    store: S,
    desc: bool,
    key_ranges: Peekable<IntoIter<KeyRange>>,
    current_range: Option<KeyRange>,
    scan_range: KeyRange,
    scanner: Option<super::super::scanner::Scanner<S>>,
    columns: Arc<Vec<ColumnInfo>>,
    inner: T,
    counts: Vec<usize>,
    storage_stats: Statistics,
}

impl<S: Store, T: InnerExecutor> ScanExecutor<S, T> {
    pub fn new(
        inner: T,
        desc: bool,
        columns: Vec<ColumnInfo>,
        mut key_ranges: Vec<KeyRange>,
        store: S,
    ) -> Result<Self> {
        box_try!(table::check_table_ranges(&key_ranges));
        if desc {
            key_ranges.reverse();
        }
        let counts = Vec::default();

        Ok(Self {
            inner,
            store,
            desc,
            columns: Arc::new(columns),
            key_ranges: key_ranges.into_iter().peekable(),
            current_range: None,
            scan_range: KeyRange::default(),
            scanner: None,
            counts,
            storage_stats: Default::default(),
        })
    }

    fn get_row_from_range_scanner(&mut self) -> Result<Option<Row>> {
        if let Some(scanner) = self.scanner.as_mut() {
            let (key, value) = match scanner.next_row()? {
                Some((key, value)) => (key, value),
                None => return Ok(None),
            };
            return self.inner.decode_row(key, value, self.columns.clone());
        }
        Ok(None)
    }

    fn get_row_from_point(&mut self, mut range: KeyRange) -> Result<Option<Row>> {
        let key = range.take_start();
        let value = self
            .store
            .get(&Key::from_raw(&key), &mut self.storage_stats)?;
        if let Some(value) = value {
            return self.inner.decode_row(key, value, self.columns.clone());
        }
        Ok(None)
    }

    #[inline]
    fn inc_last_count(&mut self) {
        self.counts.last_mut().map_or((), |val| *val += 1);
    }

    fn new_scanner(&self, range: KeyRange) -> Result<super::super::scanner::Scanner<S>> {
        super::super::Scanner::new(&self.store, self.desc, self.inner.key_only(), range)
            .map_err(Error::from)
    }
}

impl<S: Store, T: InnerExecutor> Executor for ScanExecutor<S, T> {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.get_row_from_range_scanner()? {
                self.inc_last_count();
                return Ok(Some(row));
            }
            if let Some(range) = self.key_ranges.next() {
                self.counts.push(0);
                self.current_range = Some(range.clone());
                if self.inner.is_point(&range) {
                    if let Some(row) = self.get_row_from_point(range)? {
                        self.inc_last_count();
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

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        dest.scanned_rows_per_range.append(&mut self.counts);
        self.counts.push(0);
    }

    fn collect_storage_stats(&mut self, dest: &mut Statistics) {
        dest.add(&self.storage_stats);
        self.storage_stats = Default::default();
        if let Some(scanner) = self.scanner.as_mut() {
            scanner.collect_statistics_into(dest);
        }
    }

    fn get_len_of_columns(&self) -> usize {
        self.columns.len()
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        None
    }

    fn start_scan(&mut self) {
        if let Some(range) = self.current_range.as_ref() {
            if !self.inner.is_point(range) {
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
                if !self.inner.is_point(range) {
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
}
