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

use std::time::Duration;
use std::sync::Arc;
use std::thread;

use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use tikv::pd::PdClient;
use tikv::raftstore::store::keys;
use tikv::util::config::*;
use tikv::storage::CF_RAFT;
use tikv::raftstore::store::Peekable;

use super::node::new_node_cluster;
use super::util;
use raftstore::util::*;
use raftstore::transport_simulate::*;

#[test]
fn test_node_base_merge() {
    let mut cluster = new_node_cluster(0, 3);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(right.get_start_key(), b"k2");
    let get = util::new_request(
        right.get_id(),
        right.get_region_epoch().clone(),
        vec![util::new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_region(),
        "{:?}",
        resp
    );

    pd_client.must_merge(left.get_id(), right.get_id());

    let region = pd_client.get_region(b"k1").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    let orgin_epoch = left.get_region_epoch();
    let new_epoch = region.get_region_epoch();
    // PrepareMerge + CommitMerge, so it should be 2.
    assert_eq!(new_epoch.get_version(), orgin_epoch.get_version() + 2);
    assert_eq!(new_epoch.get_conf_ver(), orgin_epoch.get_conf_ver());
    let get = util::new_request(
        region.get_id(),
        new_epoch.to_owned(),
        vec![util::new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");

    let version = left.get_region_epoch().get_version();
    let conf_ver = left.get_region_epoch().get_conf_ver();
    'outer: for i in 1..4 {
        let state_key = keys::region_state_key(left.get_id());
        let mut state = RegionLocalState::default();
        for _ in 0..3 {
            state = cluster
                .get_engine(i)
                .get_msg_cf(CF_RAFT, &state_key)
                .unwrap()
                .unwrap();
            if state.get_state() == PeerState::Tombstone {
                let region = state.get_region();
                if region.get_region_epoch().get_version() == version + 1 {
                    assert_eq!(region.get_region_epoch().get_conf_ver(), conf_ver + 1);
                }
                continue 'outer;
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("store {} is still not merged: {:?}", i, state);
    }

    cluster.must_put(b"k4", b"v4");
}

#[test]
fn test_node_merge_with_admin_entries() {
    // ::util::init_log();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 100;
    cluster.cfg.raft_store.raft_log_gc_threshold = 500;
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize::mb(20);
    let pd_client = Arc::clone(&cluster.pd_client);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        left.get_id(),
        3,
    )));
    cluster.must_split(&left, b"k11");
    let res = cluster.try_merge(left.get_id(), right.get_id());
    // log gap contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    cluster.must_put(b"k22", b"v22");
    util::must_get_equal(&cluster.get_engine(3), b"k22", b"v22");

    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        right.get_id(),
        3,
    )));
    // It doesn't matter if the index and term is correct.
    let compact_log = util::new_compact_log_request(100, 10);
    let req = util::new_admin_request(right.get_id(), right.get_region_epoch(), compact_log);
    debug!("requesting {:?}", req);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);
    let res = cluster.try_merge(right.get_id(), left.get_id());
    // log gap contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    cluster.must_put(b"k23", b"v23");
    util::must_get_equal(&cluster.get_engine(3), b"k23", b"v23");
}

#[test]
fn test_node_check_merged_message() {
    // ::util::init_log();
    let mut cluster = new_node_cluster(0, 4);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 100;
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // test if orphan merging peer will be gc
    let mut region = pd_client.get_region(b"k2").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 2));
    cluster.must_split(&region, b"k2");
    let mut left = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(left.get_id(), new_peer(3, 3));
    let mut right = pd_client.get_region(b"k2").unwrap();
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        right.get_id(),
        3,
    )));
    pd_client.must_add_peer(right.get_id(), new_peer(3, 10));
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    pd_client.must_merge(left.get_id(), right.get_id());
    region = pd_client.get_region(b"k2").unwrap();
    util::must_get_none(&cluster.get_engine(3), b"k3");
    util::must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    let region_on_store3 = find_peer(&region, 3).unwrap().to_owned();
    pd_client.must_remove_peer(region.get_id(), region_on_store3);
    util::must_get_none(&cluster.get_engine(3), b"k1");
    cluster.clear_send_filters();
    pd_client.must_add_peer(region.get_id(), new_peer(3, 11));

    // test if stale peer before conf removal is destroyed automatically
    region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    left = pd_client.get_region(b"k1").unwrap();
    right = pd_client.get_region(b"k2").unwrap();
    pd_client.must_add_peer(left.get_id(), new_peer(4, 4));
    util::must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    cluster.add_send_filter(IsolationFilterFactory::new(4));
    pd_client.must_remove_peer(left.get_id(), new_peer(4, 4));
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.clear_send_filters();
    util::must_get_none(&cluster.get_engine(4), b"k1");

    // test gc work under complicated situation.
    cluster.must_put(b"k5", b"v5");
    region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k2");
    region = pd_client.get_region(b"k4").unwrap();
    cluster.must_split(&region, b"k4");
    left = pd_client.get_region(b"k1").unwrap();
    let middle = pd_client.get_region(b"k3").unwrap();
    right = pd_client.get_region(b"k5").unwrap();
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    pd_client.must_remove_peer(left.get_id(), left_on_store3);
    util::must_get_none(&cluster.get_engine(3), b"k1");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    left = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(left.get_id(), new_peer(3, 5));
    left = pd_client.get_region(b"k1").unwrap();
    pd_client.must_merge(middle.get_id(), left.get_id());
    pd_client.must_merge(right.get_id(), left.get_id());
    cluster.must_delete(b"k3");
    cluster.must_delete(b"k5");
    cluster.must_put(b"k4", b"v4");
    cluster.clear_send_filters();
    let engine3 = cluster.get_engine(3);
    util::must_get_equal(&engine3, b"k1", b"v1");
    util::must_get_equal(&engine3, b"k4", b"v4");
    util::must_get_none(&engine3, b"k3");
    util::must_get_none(&engine3, b"v5");
}

#[test]
fn test_node_merge_dist_isolation() {
    // ::util::init_log();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    cluster.must_transfer_leader(right.get_id(), new_peer(1, 1));
    let target_leader = left.get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    util::must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left region: 1         I 2 3(leader)
    // right region: 1(leader) I 2 3
    // I means isolation.
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");
    cluster.clear_send_filters();
    util::must_get_equal(&cluster.get_engine(1), b"k4", b"v4");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    cluster.must_put(b"k11", b"v11");
    pd_client.must_remove_peer(right.get_id(), new_peer(3, 3));
    cluster.must_put(b"k33", b"v33");

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3).direction(Direction::Recv),
    ));
    pd_client.must_add_peer(right.get_id(), new_peer(3, 4));
    let right = pd_client.get_region(b"k3").unwrap();
    // So cluster becomes:
    //  left region: 1         2   3(leader)
    // right region: 1(leader) 2  [3]
    // [x] means a replica exists logically but is not created on the store x yet.
    let res = cluster.try_merge(region.get_id(), right.get_id());
    // Leader can't find replica 3 of right region, so it fails.
    assert!(res.get_header().has_error(), "{:?}", res);

    let target_leader = left.get_peers()
        .iter()
        .find(|p| p.get_store_id() == 2)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");

    cluster.clear_send_filters();
    util::must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

#[test]
fn test_node_merge_brain_split() {
    // ::util::init_log();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();

    cluster.must_put(b"k11", b"v11");
    util::must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    let right = pd_client.get_region(b"k3").unwrap();
    pd_client.must_merge(left.get_id(), right.get_id());

    for i in 0..100 {
        cluster.must_put(format!("k4{}", i).as_bytes(), b"v4");
    }
    util::must_get_equal(&cluster.get_engine(2), b"k40", b"v4");
    util::must_get_equal(&cluster.get_engine(1), b"k40", b"v4");

    cluster.clear_send_filters();

    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k40", b"v5");

    let state_key = keys::region_state_key(left.get_id());
    let state: RegionLocalState = cluster
        .get_engine(3)
        .get_msg_cf(CF_RAFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
    util::must_get_equal(&cluster.get_engine(3), b"k40", b"v5");
    for i in 1..100 {
        util::must_get_equal(&cluster.get_engine(3), format!("k4{}", i).as_bytes(), b"v4");
    }
}
