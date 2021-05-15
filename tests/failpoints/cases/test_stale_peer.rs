// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::*;

use engine_rocks::Compat;
use engine_test::kv::{new_engine, KvTestEngine, KvTestSnapshot};
use engine_traits::Peekable;
use engine_traits::ALL_CFS;
use kvproto::raft_serverpb::RaftLocalState;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::RegionRunner;
use raftstore::store::RegionTask;
use raftstore::store::SnapManager;
use tempfile::Builder;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;
use tikv_util::worker::{LazyWorker, Worker};

#[test]
fn test_one_node_leader_missing() {
    let mut cluster = new_server_cluster(0, 1);

    // 50ms election timeout.
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let election_timeout = base_tick_interval * 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to make a valid config,
    // that is election timeout x 2 < peer stale state check < abnormal < max leader missing duration.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.cfg.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 7);

    // Panic if the cluster does not has a valid stale state.
    let check_stale_state = "peer_check_stale_state";
    fail::cfg(check_stale_state, "panic").unwrap();

    cluster.start().unwrap();

    // Check stale state 3 times,
    thread::sleep(cluster.cfg.raft_store.peer_stale_state_check_interval.0 * 3);
    fail::remove(check_stale_state);
}

#[test]
fn test_node_update_localreader_after_removed() {
    let mut cluster = new_node_cluster(0, 6);
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Add 4 peers.
    for i in 2..6 {
        pd_client.must_add_peer(r1, new_peer(i, i));
    }

    // Make sure peer 1 leads the region.
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // Make sure peer 2 is initialized.
    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, key, value);

    // Pause peer 2 apply worker if it executes AddNode.
    let add_node_fp = "apply_on_add_node_1_2";
    fail::cfg(add_node_fp, "pause").unwrap();

    // Add peer 6.
    pd_client.must_add_peer(r1, new_peer(6, 6));

    // Isolate peer 2 from rest of the cluster.
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Remove peer 2, so it will receive a gc msssage
    // after max_leader_missing_duration timeout.
    pd_client.must_remove_peer(r1, new_peer(2, 2));
    thread::sleep(cluster.cfg.raft_store.max_leader_missing_duration.0 * 2);

    // Continue peer 2 apply worker, so that peer 2 tries to
    // update region to its read delegate.
    fail::remove(add_node_fp);

    // Make sure peer 2 is removed in node 2.
    cluster.must_region_not_exist(r1, 2);
}

#[test]
fn test_stale_learner_restart() {
    let mut cluster = new_node_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.raft_log_gc_threshold = 10;
    let r = cluster.run_conf_change();
    cluster
        .pd_client
        .must_add_peer(r, new_learner_peer(2, 1003));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    // Simulates slow apply.
    fail::cfg("on_handle_apply_1003", "return").unwrap();
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    let state_key = keys::raft_state_key(r);
    let mut state: RaftLocalState = cluster
        .get_raft_engine(1)
        .c()
        .get_msg(&state_key)
        .unwrap()
        .unwrap();
    let last_index = state.get_last_index();
    let timer = Instant::now();
    while timer.elapsed() < Duration::from_secs(5) {
        state = cluster
            .get_raft_engine(2)
            .c()
            .get_msg(&state_key)
            .unwrap()
            .unwrap();
        if last_index <= state.get_hard_state().get_commit() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if state.last_index != last_index {
        panic!("store 2 has not catched up logs after 5 secs.");
    }
    cluster.shutdown();
    must_get_none(&cluster.get_engine(2), b"k2");
    fail::remove("on_handle_apply_1003");
    cluster.run_node(2).unwrap();
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
}

#[test]
fn test_not_clean_stale_peer_when_writestall() {
    let path = Builder::new()
        .prefix("test_not_clean_stale_peer_when_writestall")
        .tempdir()
        .unwrap();
    let engine = new_engine(
        path.path().join("db").to_str().unwrap(),
        None,
        ALL_CFS,
        None,
    )
    .unwrap();

    let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
    let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
    let bg_worker = Worker::new("region-worker");
    let mut worker: LazyWorker<RegionTask<KvTestSnapshot>> = bg_worker.lazy_build("region-worker");
    let sched = worker.scheduler();
    let (router, _) = std::sync::mpsc::sync_channel(11);
    let runner = RegionRunner::new(
        engine,
        mgr,
        0,
        false,
        CoprocessorHost::<KvTestEngine>::default(),
        router,
        Duration::from_millis(100),
    );
    worker.start_with_timer(runner);
    let mut ranges = vec![];
    for i in 0..10 {
        let mut key = b"k0".to_vec();
        key.extend_from_slice(i.to_string().as_bytes());
        ranges.push(key);
    }
    fail::cfg("region::ingest_maybe_stall", "return()").unwrap();
    let destroy_count = Arc::new(AtomicUsize::new(0));
    let apply_count = Arc::new(AtomicUsize::new(0));
    let d = destroy_count.clone();
    fail::cfg_callback("region::clean_stale_ranges", move || {
        d.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let a = apply_count.clone();
    fail::cfg_callback("region::handle_apply", move || {
        a.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    for i in 0..8 {
        sched
            .schedule(RegionTask::Destroy {
                region_id: i as u64 + 2,
                start_key: ranges[i].clone(),
                end_key: ranges[i + 1].clone(),
            })
            .unwrap();
    }
    sched
        .schedule(RegionTask::Apply {
            region_id: 8,
            status: Arc::new(AtomicUsize::new(0)),
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(2000));
    assert_eq!(0, destroy_count.load(Ordering::Acquire));
    assert_eq!(0, apply_count.load(Ordering::Acquire));

    fail::remove("region::ingest_maybe_stall");
    fail::cfg("region::handle_apply_return", "return()").unwrap();
    std::thread::sleep(Duration::from_millis(1600));
    assert_eq!(1, destroy_count.load(Ordering::Acquire));
    assert_eq!(1, apply_count.load(Ordering::Acquire));
    worker.stop_worker();
}
