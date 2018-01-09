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

extern crate rand;

use std::time::Instant;
use std::sync::mpsc::channel;
use std::sync::Arc;

use tikv::storage::{Key, Modify, Mutation, Options, Snapshot, SnapshotStore, Statistics};
use tikv::storage::mvcc::MvccTxn;
use tikv::raftstore::store::engine::SyncSnapshot;
use tikv::config::DbConfig;
use tikv::util::threadpool::{DefaultContext, ThreadPoolBuilder};
use tikv::util::config::ReadableSize;
use tikv::util::rocksdb::{get_cf_handle, new_engine_opt};
use kvproto::kvrpcpb::IsolationLevel;
use rocksdb::{Writable, WriteBatch, DB};
use rocksdb::rocksdb_options::DBOptions;

//use super::print_result;
//use test::BenchSamples;

use rand::Rng;
use tempdir::TempDir;

use utils::*;



#[inline]
fn do_write(db: &DB, modifies: Vec<Modify>) {
    let wb = WriteBatch::new();
    for rev in modifies {
        match rev {
            Modify::Delete(cf, k) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.delete_cf(handle, k.encoded()).unwrap();
            }
            Modify::Put(cf, k, v) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.put_cf(handle, k.encoded(), &v).unwrap();
            }
            Modify::DeleteRange(cf, start_key, end_key) => {
                let handle = get_cf_handle(db, cf).unwrap();
                wb.delete_range_cf(handle, start_key.encoded(), end_key.encoded())
                    .unwrap();
            }
        }
    }
    db.write_without_wal(wb).unwrap();
}

#[inline]
fn get_snapshot(db: Arc<DB>) -> Box<Snapshot> {
    box SyncSnapshot::new(db) as Box<Snapshot>
}

#[inline]
fn prewrite(db: Arc<DB>, mutations: &[Mutation], primary: &[u8], start_ts: u64) {
    let snapshot = get_snapshot(db.clone());
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for m in mutations {
        txn.prewrite(m.clone(), primary, &Options::default())
            .unwrap();
    }
    do_write(&*db, txn.into_modifies());
}

#[inline]
fn commit(db: Arc<DB>, keys: &[Key], start_ts: u64, commit_ts: u64) {
    let snapshot = get_snapshot(db.clone());
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for key in keys {
        txn.commit(key, commit_ts).unwrap();
    }
    do_write(&*db, txn.into_modifies());
}

fn prepare_test_db(versions: usize, value_len: usize, keys: &[Vec<u8>], path: &str) -> Arc<DB> {
    let mut config = DbConfig::default();
    // Use a huge write_buffer_size to avoid flushing data to disk.
    config.defaultcf.write_buffer_size = ReadableSize::gb(1);
    config.writecf.write_buffer_size = ReadableSize::gb(1);
    config.lockcf.write_buffer_size = ReadableSize::gb(1);

    let cf_ops = config.build_cf_opts();

    let db = Arc::new(new_engine_opt(path, DBOptions::new(), cf_ops).unwrap());

    for _ in 0..versions {
        for key in keys {
            let value = vec![0u8; value_len];
            let start_ts = next_ts();
            let commit_ts = next_ts();

            prewrite(
                db.clone(),
                &[Mutation::Put((Key::from_raw(key), value))],
                key,
                start_ts,
            );
            commit(db.clone(), &[Key::from_raw(key)], start_ts, commit_ts);
        }
    }
    db
}

#[inline]
fn get(db: Arc<DB>, key: &Key, statistics: &mut Statistics) -> Option<Vec<u8>> {
    let snapshot = get_snapshot(db.clone());
    let start_ts = next_ts();
    let snapstore = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
    snapstore.get(key, statistics).unwrap()
}

fn bench_get(db: Arc<DB>, keys: &[Vec<u8>]) -> f64 {
    let mut fake_statistics = Statistics::default();
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let key = rng.choose(keys).unwrap();
            let key = Key::from_raw(key);

            get(db.clone(), &key, &mut fake_statistics).unwrap()
        },
        500000,
    )
}

fn bench_set(db: Arc<DB>, keys: &[Vec<u8>], value_len: usize) -> f64 {
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();
            let value = vec![0u8; value_len];

            let key = rng.choose(keys).unwrap();

            prewrite(
                db.clone(),
                &[Mutation::Put((Key::from_raw(key), value))],
                key,
                start_ts,
            );
            commit(db.clone(), &[Key::from_raw(key)], start_ts, commit_ts)
        },
        500000,
    )
}

fn bench_delete(db: Arc<DB>, keys: &[Vec<u8>]) -> f64 {
    let mut rng = rand::thread_rng();
    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();

            let key = rng.choose(keys).unwrap();
            prewrite(
                db.clone(),
                &[Mutation::Delete(Key::from_raw(key))],
                key,
                start_ts,
            );
            commit(db.clone(), &[Key::from_raw(key)], start_ts, commit_ts)
        },
        500000,
    )
}

fn bench_batch_set_impl(db: Arc<DB>, keys: &[Vec<u8>], value_len: usize, batch_size: usize) -> f64 {
    // Avoid writing duplicated keys in a single transaction
    let mut indices: Vec<_> = (0..keys.len()).collect();
    let mut rng = rand::thread_rng();

    let mut keys_to_write: Vec<Key> = Vec::with_capacity(batch_size);
    let mut mutations: Vec<Mutation> = Vec::with_capacity(batch_size);

    do_bench(
        || {
            let start_ts = next_ts();
            let commit_ts = next_ts();

            keys_to_write.clear();
            mutations.clear();

            // Randomly select non-duplicated keys
            for i in 0..batch_size {
                let selected = rng.gen_range(i, keys.len());
                indices.swap(selected, i);

                let key = Key::from_raw(&keys[indices[i]]);
                let value = vec![0u8; value_len];

                mutations.push(Mutation::Put((key.clone(), value)));
                keys_to_write.push(key);
            }

            let primary = &keys[indices[0]];
            prewrite(db.clone(), &mutations, primary, start_ts);
            commit(db.clone(), &keys_to_write, start_ts, commit_ts)
        },
        640000 / (batch_size as u32),
    )
}

enum BenchType {
    Row,
    UniqueIndex,
}

// Run all bench with specified parameters
fn bench_single_row(
    table_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {

    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut keys);


    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} get\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    let ns = bench_get(db.clone(), &keys) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);

    println!(
        "benching mvcctxn {} set\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    let ns = bench_set(db.clone(), &keys, value_len) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);

    // Generate new db to bench delete, for the size of content was increased when benching set
    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} delete\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    let ns = bench_delete(db.clone(), &keys) as u64;
    println!("\t{:>11} ns per op  {:>11} ops", ns, 1_000_000_000 / ns);

}

fn bench_batch_set(
    table_size: usize,
    batch_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {
    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut keys);

    let dir = TempDir::new("bench-mvcctxn").unwrap();
    let db = prepare_test_db(
        version_count,
        value_len,
        &keys,
        dir.path().to_str().unwrap(),
    );

    println!(
        "benching mvcctxn {} batch write\trows:{} versions:{} data len:{} batch:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len,
        batch_size,
    );
    let ns = bench_batch_set_impl(db.clone(), &keys, value_len, batch_size);
    println!(
        "\t{:>11} ns per op  {:>11} ops  {:>11} ns per key  {:>11} key per sec",
        ns as u64,
        (1_000_000_000_f64 / ns) as u64,
        (ns / (batch_size as f64)) as u64,
        (1_000_000_000_f64 * (batch_size as f64) / ns) as u64
    );
}


fn bench_concurrent_batch_impl(
    txn_count: usize,
    data_len: usize,
    batch_size: usize,
    threads: usize,
    bench_type: &BenchType,
) {
    let (value_len, log_name) = match *bench_type {
        BenchType::Row => (data_len, "row"),
        BenchType::UniqueIndex => (8, "unique index"),
    };

    println!(
        "benching mvcctxn {} concurrent write\tbatch size:{} batch count:{} threads:{}\t...",
        log_name,
        batch_size,
        txn_count,
        threads
    );
    let time_record = record_time(
        || {
            let mut keys = match *bench_type {
                BenchType::Row => generate_row_keys(1, 0, txn_count * batch_size),
                BenchType::UniqueIndex => {
                    generate_unique_index_keys(1, 1, data_len, txn_count * batch_size)
                }
            };

            let mut rng = rand::thread_rng();
            rng.shuffle(&mut keys);

            let mut keys = keys.drain(..);

            // Divide keys into many tasks.
            let mut txns: Vec<Vec<_>> = (0..txn_count)
                .map(|_| (&mut keys).take(batch_size).collect())
                .collect();

            let dir = TempDir::new("bench-mvcctxn").unwrap();
            let db = prepare_test_db(0, 0, &[], dir.path().to_str().unwrap());

            let pool = ThreadPoolBuilder::<DefaultContext, _>::with_default_factory(
                String::from("bench-concurrent-mvcctxn"),
            ).thread_count(threads)
                .build();

            let (tx, rx) = channel::<()>();

            let start_time = Instant::now();

            let actual_count = txns.len();

            for mut txn in txns.drain(..) {
                let db = db.clone();
                let tx = tx.clone();
                pool.execute(move |_| {
                    let mutations: Vec<_> = txn.iter()
                        .map(|item| {
                            Mutation::Put((Key::from_raw(item), vec![0u8; value_len]))
                        })
                        .collect();
                    let primary = txn[0].clone();
                    let keys: Vec<_> = txn.drain(..).map(|item| Key::from_raw(&item)).collect();
                    let start_ts = next_ts();
                    prewrite(db.clone(), &mutations, &primary, start_ts);
                    commit(db.clone(), &keys, start_ts, next_ts());
                    // Signal that this task has been finished
                    tx.send(()).unwrap();
                })
            }

            // Wait until all tasks are finished
            for _ in 0..actual_count {
                rx.recv().unwrap();
            }

            start_time.elapsed()
        },
        5,
    );
    let ns = average(&time_record) as f64 / (txn_count as f64);
    println!(
        "\t{:>11} ns per op  {:>11} ops  {:>11} ns per key  {:>11} key per sec",
        ns as u64,
        (1_000_000_000_f64 / ns) as u64,
        (ns / (batch_size as f64)) as u64,
        (1_000_000_000_f64 * (batch_size as f64) / ns) as u64
    );
}



pub fn bench_mvcctxn() {
    for bench_type in &[BenchType::Row, BenchType::UniqueIndex] {
        for version_count in &[1, 16, 64] {
            bench_single_row(10_000, *version_count, 128, bench_type);
        }

        for value_len in &[32, 128, 1024] {
            bench_single_row(10_000, 5, *value_len, bench_type);
        }

        for batch_size in &[8, 64, 128, 256] {
            bench_batch_set(10_000, *batch_size, 5, 128, bench_type);
        }
    }
}


pub fn bench_concurrent_batch() {
    let table_size = 100_000;
    for batch_size in &[1, 8, 64, 128] {
        for threads in &[1, 2, 4, 8] {
            bench_concurrent_batch_impl(
                table_size / *batch_size,
                128,
                *batch_size,
                *threads,
                &BenchType::Row,
            );
        }
    }
}
