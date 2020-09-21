// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::cf_names::CFNamesExt;
use crate::errors::Result;
use crate::range::Range;

// FIXME: Find somewhere else to put this?
pub const MAX_DELETE_BATCH_COUNT: usize = 512;

pub trait MiscExt: CFNamesExt {
    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()>;

    fn delete_all_files_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_files_in_range_cf(cf, start_key, end_key, false)?;
        }

        Ok(())
    }

    fn delete_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()>;

    fn delete_blob_files_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_blob_files_in_range_cf(cf, start_key, end_key, false)?;
        }

        Ok(())
    }

    fn delete_blob_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()>;

    fn delete_all_in_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        use_delete_range: bool,
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_all_in_range_cf(cf, start_key, end_key, use_delete_range)?;
        }

        Ok(())
    }

    fn delete_all_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        use_delete_range: bool,
    ) -> Result<()>;

    /// Return the approximate number of records and size in the range of memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool>;

    /// Gets total used size of rocksdb engine, including:
    /// *  total size (bytes) of all SST files.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob files.
    ///
    fn get_engine_used_size(&self) -> Result<u64>;

    /// Roughly deletes files in multiple ranges.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in the database.
    ///    - After this operation, some keys in the range might be removed from existing snapshot,
    ///      so you shouldn't expect to be able to read data from the range using existing snapshots
    ///      any more.
    ///
    /// Ref: https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;

    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Check whether a database exists at a given path
    fn exists(path: &str) -> bool;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64>;

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>>;

    fn get_range_entries_and_versions(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<(u64, u64)>>;

    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>>;
}
