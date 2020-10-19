// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::{Context, IsolationLevel};
use test_storage::{SyncTestStorage, SyncTestStorageBuilder};
use tidb_query_datatype::codec::table;
use tikv::storage::{kv::RocksEngine, mvcc::MvccReader, Engine};
use txn_types::{Key, Mutation};

fn prepare_mvcc_data(key: &Key, n: u64) -> SyncTestStorage<RocksEngine> {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    for ts in 1..=n {
        let mutation = Mutation::Put((key.clone(), b"value".to_vec()));
        store
            .prewrite(
                Context::default(),
                vec![mutation],
                key.clone().into_encoded(),
                ts,
            )
            .unwrap();
        store
            .commit(Context::default(), vec![key.clone()], ts, ts + 1)
            .unwrap();
    }
    let engine = store.get_engine();
    let db = engine.get_rocksdb().get_sync_db();
    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);
    store
}

fn bench_get_txn_commit_record(b: &mut test::Bencher, single_key: bool, n: u64) {
    let key = Key::from_raw(&table::encode_row_key(1, 0));
    let store = prepare_mvcc_data(&key, n);
    b.iter(|| {
        let mut mvcc_reader = MvccReader::new(
            store.get_engine().snapshot(&Context::default()).unwrap(),
            None,
            true,
            IsolationLevel::Si,
        );
        mvcc_reader.set_single_key(single_key);
        mvcc_reader
            .get_txn_commit_record(&key, 1.into())
            .unwrap()
            .unwrap_single_record();
    });
}

#[bench]
fn bench_get_txn_commit_record_single_key_5(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, true, 5);
}

#[bench]
fn bench_get_txn_commit_record_non_single_key_5(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, false, 5);
}

#[bench]
fn bench_get_txn_commit_record_single_key_100(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, true, 100);
}

#[bench]
fn bench_get_txn_commit_record_non_single_key_100(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, false, 100);
}
