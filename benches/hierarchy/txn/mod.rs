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

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::engine::{Engine, TestEngineBuilder};
use tikv::storage::mvcc::MvccTxn;
use tikv::storage::{Key, Mutation, Options};

use super::*;

fn txn_prewrite(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::new();
    let option = Options::default();
    b.iter_with_setup(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> =
                KvGenerator::new(config.key_length, config.value_length)
                    .generate(DEFAULT_ITERATIONS)
                    .iter()
                    .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                    .collect();
            (mutations, &option)
        },
        |(mutations, option)| {
            for (mutation, primary) in mutations {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = MvccTxn::new(snapshot, 1, false).unwrap();
                txn.prewrite(mutation, &primary, option).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies).unwrap());
            }
        },
    )
}

fn txn_commit(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::new();
    let option = Options::default();
    b.iter_with_setup(
        || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut txn = MvccTxn::new(snapshot, 1, false).unwrap();
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            for (k, v) in &kvs {
                txn.prewrite(
                    Mutation::Put((Key::from_raw(&k), v.clone())),
                    &k.clone(),
                    &option,
                ).unwrap();
            }
            let modifies = txn.into_modifies();
            let _ = engine.write(&ctx, modifies);
            let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            keys
        },
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = MvccTxn::new(snapshot, 1, false).unwrap();
                txn.commit(key, 2).unwrap();
                let modifies = txn.into_modifies();
                black_box(engine.write(&ctx, modifies).unwrap())
            }
        },
    );
}

pub fn bench_txn(c: &mut Criterion) {
    c.bench_function_over_inputs("txn_prewrite", txn_prewrite, generate_kv_configs());
    c.bench_function_over_inputs("txn_commit", txn_commit, generate_kv_configs());
}
