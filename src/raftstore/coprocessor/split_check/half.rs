// Copyright 2018 PingCAP, Inc.
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

use rocksdb::DB;

use super::super::{Coprocessor, ObserverContext, SplitCheckObserver};
use super::Status;

const BUCKET_NUMBER_LIMIT: usize = 1024;
#[derive(Default)]
pub struct HalfStatus {
    buckets: Vec<Vec<u8>>,
    cur_bucket_size: u64,
    each_bucket_size: u64,
}

impl HalfStatus {
    fn new(each_bucket_size: u64) -> HalfStatus {
        HalfStatus {
            each_bucket_size: each_bucket_size,
            ..Default::default()
        }
    }

    pub fn push_data(&mut self, key: &[u8], value_size: u64) {
        let current_len = key.len() as u64 + value_size;
        if self.buckets.is_empty() || self.cur_bucket_size >= self.each_bucket_size {
            self.check_and_adjust_buckets_num();
            self.buckets.push(key.to_vec());
            self.cur_bucket_size = 0;
        }
        self.cur_bucket_size += current_len;
    }

    fn check_and_adjust_buckets_num(&mut self) {
        if self.buckets.len() < BUCKET_NUMBER_LIMIT {
            return;
        }
        let buckets_num = self.buckets.len() / 2;
        for id in { 0..buckets_num } {
            self.buckets.swap(id, id * 2 + 1);
        }
        self.buckets.truncate(buckets_num);
        self.each_bucket_size *= 2;
    }

    pub fn split_key(self) -> Option<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            None
        } else {
            self.buckets.get(mid).cloned()
        }
    }
}

pub struct HalfCheckObserver {
    half_split_bucket_size: u64,
}

impl HalfCheckObserver {
    pub fn new(half_split_bucket_size: u64) -> HalfCheckObserver {
        HalfCheckObserver {
            half_split_bucket_size: half_split_bucket_size,
        }
    }
}

impl Coprocessor for HalfCheckObserver {}

impl SplitCheckObserver for HalfCheckObserver {
    fn new_split_check_status(
        &self,
        _ctx: &mut ObserverContext,
        status: &mut Status,
        _engine: &DB,
    ) {
        if status.auto_split {
            return;
        }
        status.half = Some(HalfStatus::new(self.half_split_bucket_size));
    }

    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        status: &mut Status,
        key: &[u8],
        value_size: u64,
    ) -> Option<Vec<u8>> {
        if let Some(status) = status.half.as_mut() {
            status.push_data(key, value_size);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use kvproto::metapb::Region;

    use storage::ALL_CFS;
    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask, SplitType};
    use util::rocksdb::{new_engine_opt, CFOptions};
    use util::worker::Runnable;
    use util::transport::RetryableSendCh;
    use util::config::ReadableSize;

    use raftstore::coprocessor::{Config, CoprocessorHost};
    use super::*;

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-split");
        let mut cfg = Config::default();
        cfg.half_split_bucket_size = ReadableSize(1);
        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be z0006
        for i in 0..12 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush(true).unwrap();

        runnable.run(SplitCheckTask::new(&region, SplitType::HalfSplit));
        match rx.try_recv() {
            Ok(Msg::SplitRegion {
                region_id,
                region_epoch,
                split_key,
                ..
            }) => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(&region_epoch, region.get_region_epoch());
                assert_eq!(split_key, b"0006");
            }
            others => panic!("expect split check result, but got {:?}", others),
        }
    }

    #[test]
    fn test_too_many_buckets() {
        let item_len = 100;
        let mut status = HalfStatus::new(item_len);
        for id in { 0..BUCKET_NUMBER_LIMIT } {
            let key_str = format!("{:09}", id);
            let key = key_str.into_bytes();
            let value_size = item_len - key.len() as u64;
            status.push_data(&key, value_size);
        }
        assert_eq!(status.buckets.len(), BUCKET_NUMBER_LIMIT);
        assert_eq!(status.each_bucket_size, item_len);
        // buckets number reach the upper limit
        status.push_data(b"xxx", item_len);
        // buckets merged
        assert_eq!(status.buckets.len(), BUCKET_NUMBER_LIMIT / 2 + 1);
        assert_eq!(status.each_bucket_size, item_len * 2);
        // check split_key
        let expect_key_id = BUCKET_NUMBER_LIMIT / 2 + 1;
        let expect_key = format!("{:09}", expect_key_id);
        assert_eq!(status.split_key(), Some(expect_key.into_bytes()));
    }

}
