// Copyright 2016 PingCAP, Inc.
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
use test::BenchSamples;
use tempdir::TempDir;

use test_util::*;
use tikv::storage::{self, Dsn, Mutation, Key};
use tikv::storage::txn::TxnStore;
use kvproto::kvrpcpb::Context;

use super::print_result;

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
fn bench_tombstone_scan(dsn: Dsn) -> BenchSamples {
    let engine = storage::new_engine(dsn).unwrap();

    let store = TxnStore::new(Arc::new(engine));
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Put((Key::from_raw(&k), v))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(&k)],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");

        ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Delete(Key::from_raw(&k))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(&k)],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    bench!{
        let (k, _) = kvs.next().unwrap();
        assert!(store.scan(Context::new(),
                           Key::from_raw(&k),
                           1,
                           ts_generator.next().unwrap())
                     .unwrap()
                     .is_empty())
    }
}

pub fn bench_engine() {
    let path = TempDir::new("bench-mvcc").unwrap();
    let dsn = Dsn::RocksDBPath(path.path().to_str().unwrap());
    printf!("benching tombstone scan with rocksdb\t...\t");
    print_result(bench_tombstone_scan(dsn));
}
