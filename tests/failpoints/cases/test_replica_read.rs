// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::{Peekable, CF_RAFT};
use fail;
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use raft::eraftpb::MessageType;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use test_raftstore::*;
use tikv::raftstore::store::keys;
use tikv_util::HandyRwLock;

#[test]
fn test_wait_for_apply_index() {
    let _guard = crate::setup();
    let mut cluster = new_server_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_none_pending_peer(p2.clone());
    cluster.pd_client.must_none_pending_peer(p3.clone());

    let region = cluster.get_region(b"k0");
    cluster.must_transfer_leader(region.get_id(), p2.clone());

    // Block all write cmd applying of Peer 3.
    fail::cfg("on_apply_write_cmd", "sleep(2000)").unwrap();
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Peer 3 does not apply the cmd of putting 'k1' right now, then the follower read must
    // be blocked.
    must_get_none(&cluster.get_engine(3), b"k1");
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cf_cmd("default", b"k1")],
        false,
    );
    request.mut_header().set_peer(p3.clone());
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    // Must timeout here
    assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    fail::cfg("on_apply_write_cmd", "off").unwrap();

    // After write cmd applied, the follower read will be executed.
    match rx.recv_timeout(Duration::from_secs(3)) {
        Ok(resp) => {
            assert_eq!(resp.get_responses().len(), 1);
            assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");
        }
        Err(_) => panic!("follower read failed"),
    }
}

#[test]
fn test_duplicate_read_index_ctx() {
    let _guard = crate::setup();
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    cluster.cfg.raft_store.raft_heartbeat_ticks = 1;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_none_pending_peer(p2.clone());
    cluster.pd_client.must_none_pending_peer(p3.clone());
    let region = cluster.get_region(b"k0");
    assert_eq!(cluster.leader_of_region(region.get_id()).unwrap(), p1);

    // Delay all raft messages to peer 1.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let (sx, rx) = mpsc::sync_channel::<()>(2);
    let recv_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 1)
            .direction(Direction::Recv)
            .when(Arc::new(AtomicBool::new(true)))
            .reserve_dropped(Arc::clone(&dropped_msgs))
            .set_msg_callback(Arc::new(move |msg: &RaftMessage| {
                if msg.get_message().get_msg_type() == MessageType::MsgReadIndex {
                    sx.send(()).unwrap();
                }
            })),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    // send two read index requests to leader
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    request.mut_header().set_peer(p2.clone());
    let (cb2, rx2) = make_cb(&request);
    // send to peer 2
    cluster
        .sim
        .rl()
        .async_command_on_node(2, request.clone(), cb2)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
    request.mut_header().set_peer(p3.clone());
    let (cb3, rx3) = make_cb(&request);
    // send to peer 3
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request.clone(), cb3)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let router = cluster.sim.wl().get_router(1).unwrap();
    fail::cfg("pause_on_peer_collect_message", "pause").unwrap();
    cluster.sim.wl().clear_recv_filters(1);
    for raft_msg in mem::replace(dropped_msgs.lock().unwrap().as_mut(), vec![]) {
        router.send_raft_message(raft_msg).unwrap();
    }
    fail::cfg("pause_on_peer_collect_message", "off").unwrap();

    // read index response must not be dropped
    rx2.recv_timeout(Duration::from_secs(5)).unwrap();
    rx3.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn test_read_before_init() {
    let _guard = crate::setup();
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_none_pending_peer(p2.clone());
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");

    fail::cfg("before_apply_snap_update_region", "return").unwrap();
    // Add peer 3
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    thread::sleep(Duration::from_millis(500));
    let region = cluster.get_region(b"k0");
    assert_eq!(cluster.leader_of_region(r1).unwrap(), p1);

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cf_cmd("default", b"k0")],
        false,
    );
    request.mut_header().set_peer(p3.clone());
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("not initialized yet"),
        "{:?}",
        resp.get_header().get_error()
    );
}

#[test]
fn test_read_applying_snapshot() {
    let _guard = crate::setup();
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_none_pending_peer(p2.clone());

    // Don't apply snapshot to init peer 3
    fail::cfg("region_apply_snap", "pause").unwrap();
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    thread::sleep(Duration::from_millis(500));

    // Check if peer 3 is applying snapshot
    let region_key = keys::region_state_key(r1);
    let region_state: RegionLocalState = cluster
        .get_engine(3)
        .get_msg_cf(CF_RAFT, &region_key)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_state(), PeerState::Applying);
    let region = cluster.get_region(b"k0");
    assert_eq!(cluster.leader_of_region(r1).unwrap(), p1);

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cf_cmd("default", b"k0")],
        false,
    );
    request.mut_header().set_peer(p3.clone());
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    let resp = match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(r) => r,
        Err(_) => {
            fail::cfg("region_apply_snap", "off").unwrap();
            panic!("cannot receive response");
        }
    };
    fail::cfg("region_apply_snap", "off").unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("applying snapshot"),
        "{:?}",
        resp.get_header().get_error()
    );
}
