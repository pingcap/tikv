// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc::channel, thread, time::Duration};

use futures::executor::block_on;
use kvproto::kvrpcpb::Context;
use storage::mvcc;
use tikv::storage::txn::commands;
use tikv::storage::txn::tests::{must_prewrite_put, must_prewrite_put_err};
use tikv::storage::TestEngineBuilder;
use tikv::storage::{self, txn::tests::must_commit};
use tikv::storage::{lock_manager::DummyLockManager, TestStorageBuilder};
use txn_types::{Key, Mutation, TimeStamp};

#[test]
fn test_txn_failpoints() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let (k, v) = (b"k", b"v");
    fail::cfg("prewrite", "return(WriteConflict)").unwrap();
    must_prewrite_put_err(&engine, k, v, k, 10);
    fail::remove("prewrite");
    must_prewrite_put(&engine, k, v, k, 10);
    fail::cfg("commit", "delay(100)").unwrap();
    must_commit(&engine, k, 10, 20);
    fail::remove("commit");
}

#[test]
fn test_atomic_getting_max_ts_and_storing_memory_lock() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    let (prewrite_tx, prewrite_rx) = channel();
    // sleep a while between getting max ts and store the lock in memory
    fail::cfg("before-set-lock-in-memory", "sleep(500)").unwrap();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                40.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                Some(vec![]),
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    // sleep a while so prewrite gets max ts before get is triggered
    thread::sleep(Duration::from_millis(200));
    match block_on(storage.get(Context::default(), Key::from_raw(b"k"), 100.into())) {
        // In this case, min_commit_ts is smaller than the start ts, but the lock is visible
        // to the get.
        Err(storage::Error(box storage::ErrorInner::Mvcc(mvcc::Error(
            box mvcc::ErrorInner::KeyIsLocked(lock),
        )))) => {
            assert_eq!(lock.get_min_commit_ts(), 41);
        }
        res => panic!("unexpected result: {:?}", res),
    }
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert_eq!(res.min_commit_ts, 41.into());
}

#[test]
fn test_snapshot_must_be_later_than_updating_max_ts() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    // Suppose snapshot was before updating max_ts, after sleeping for 500ms the following prewrite should complete.
    fail::cfg("after-snapshot", "sleep(500)").unwrap();
    let read_ts = 20.into();
    let get_fut = storage.get(Context::default(), Key::from_raw(b"j"), read_ts);
    thread::sleep(Duration::from_millis(100));
    fail::remove("after-snapshot");
    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"j"), b"v".to_vec()))],
                b"j".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                Some(vec![]),
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    let has_lock = block_on(get_fut).is_err();
    let res = prewrite_rx.recv().unwrap().unwrap();
    // We must make sure either the lock is visible to the reader or min_commit_ts > read_ts.
    assert!(res.min_commit_ts > read_ts || has_lock);
}

#[test]
fn test_update_max_ts_before_scan_memory_locks() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    fail::cfg("before-storage-check-memory-locks", "sleep(500)").unwrap();
    let get_fut = storage.get(Context::default(), Key::from_raw(b"k"), 100.into());

    thread::sleep(Duration::from_millis(200));

    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                Some(vec![]),
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();

    // The prewritten lock is not seen by the reader
    assert_eq!(block_on(get_fut).unwrap(), None);
    // But we make sure in this case min_commit_ts is greater than start_ts.
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert_eq!(res.min_commit_ts, 101.into());
}
