// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_rocks::Compat;
use engine_traits::Peekable;
use kvproto::{metapb, raft_serverpb};
use test_raftstore::*;

fn test_bootstrap_half_way_failure(fp: &str) {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(0, 5, sim, pd_client);

    // Try to start this node, return after persisted some keys.
    fail::cfg(fp, "return").unwrap();
    cluster.start().unwrap_err();

    let engines = cluster.engines[&1].clone();
    let ident = engines
        .kv
        .c()
        .get_msg::<raft_serverpb::StoreIdent>(keys::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    let store_id = ident.get_store_id();
    debug!("store id {:?}", store_id);
    assert!(cluster.engines.insert(store_id, engines.clone()).is_none());

    // Check whether it can bootstrap cluster successfully.
    fail::remove(fp);
    cluster.start().unwrap();

    assert!(engines
        .kv
        .c()
        .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(store_id), k, v);
    for id in cluster.engines.keys() {
        must_get_equal(&cluster.get_engine(*id), k, v);
    }
}

#[test]
fn test_bootstrap_half_way_failure_after_bootstrap_store() {
    let fp = "node_after_bootstrap_store";
    test_bootstrap_half_way_failure(fp);
}

#[test]
fn test_bootstrap_half_way_failure_after_prepare_bootstrap_cluster() {
    let fp = "node_after_prepare_bootstrap_cluster";
    test_bootstrap_half_way_failure(fp);
}

#[test]
fn test_bootstrap_half_way_failure_after_bootstrap_cluster() {
    let fp = "node_after_bootstrap_cluster";
    test_bootstrap_half_way_failure(fp);
}
