// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine::rocks::util::get_cf_handle;
use engine::*;
use kvproto::kvrpcpb::Context;
use test_raftstore::*;
use tikv::storage::kv::*;
use tikv::storage::Key;
use tikv_util::config::ReadableSize;
use tikv_util::HandyRwLock;

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    let titan_paths: Vec<String> = cluster
        .get_kv_paths()
        .iter()
        .map(|kv| Path::new(kv).join("titandb").to_str().unwrap().to_owned())
        .collect();
    cluster.run();

    let region = cluster.get_region(b"");
    let leader_id = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader_id.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());

    bulk_insert(&ctx, &storage, 100);

    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    for path in &titan_paths {
        assert!(tikv_util::config::check_data_dir_non_empty(path.as_str(), "blob").is_ok());
    }
    for i in 0..3 {
        let db = cluster.get_engine(i);
        let handle = get_cf_handle(&db, CF_DEFAULT).unwrap();
        let mut opt = Vec::new();
        opt.push(("blob_run_mode", "kFallback"));
        assert!(db.set_options_cf(handle, &opt).is_ok());
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    sleep_ms(500);
    cluster.shutdown();
    configure_for_disable_titan(&mut cluster);
    cluster.start_new_engines().unwrap();
    for path in &titan_paths {
        assert!(tikv_util::config::check_data_dir_empty(path.as_str(), "blob").is_ok());
    }
}

fn bulk_insert<E: Engine>(ctx: &Context, engine: &E, size: usize) {
    for i in 0..size {
        must_put_cf(
            ctx,
            engine,
            CF_DEFAULT,
            format!("key-{}", i).as_bytes(),
            format!("value-{}", i).as_bytes(),
        );
    }
}

fn must_put_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
    engine
        .put_cf(ctx, cf, Key::from_raw(key), value.to_vec())
        .unwrap();
}
