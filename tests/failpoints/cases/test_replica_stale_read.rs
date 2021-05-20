// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::{Context, GetResponse, Mutation, Op};
use kvproto::metapb::Peer;
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use std::sync::Arc;
use test_raftstore::*;
use tikv_util::HandyRwLock;

// A helpful wrapper to make the test logic clear
struct PeerClient {
    cli: TikvClient,
    ctx: Context,
}

impl PeerClient {
    fn new(cluster: &Cluster<ServerCluster>, region_id: u64, peer: Peer) -> PeerClient {
        let cli = {
            let env = Arc::new(Environment::new(1));
            let channel =
                ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(peer.get_store_id()));
            TikvClient::new(channel)
        };
        let ctx = {
            let epoch = cluster.get_region_epoch(region_id);
            let mut ctx = Context::default();
            ctx.set_region_id(region_id);
            ctx.set_peer(peer);
            ctx.set_region_epoch(epoch);
            ctx
        };
        PeerClient { cli, ctx }
    }

    fn kv_read(&self, key: Vec<u8>, ts: u64) -> GetResponse {
        kv_read(&self.cli, self.ctx.clone(), key, ts)
    }

    fn must_kv_read_equal(&self, key: Vec<u8>, val: Vec<u8>, ts: u64) {
        must_kv_read_equal(&self.cli, self.ctx.clone(), key, val, ts)
    }

    fn must_kv_write(&self, pd_client: &TestPdClient, kvs: Vec<Mutation>, pk: Vec<u8>) -> u64 {
        must_kv_write(pd_client, &self.cli, self.ctx.clone(), kvs, pk)
    }

    fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        must_kv_prewrite(&self.cli, self.ctx.clone(), muts, pk, ts)
    }

    fn must_kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64) {
        must_kv_commit(
            &self.cli,
            self.ctx.clone(),
            keys,
            start_ts,
            commit_ts,
            commit_ts,
        )
    }
}

// Testing how data replication could effect stale read service
#[test]
fn test_stale_read_basic_flow_replicate() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.sim.wl().start_resolved_ts_worker();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let mut leader_client = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    // Set the `stale_read` flag
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    // There should be no read index message while handling stale read request
    let on_step_read_index_msg = "on_step_read_index_msg";
    fail::cfg(on_step_read_index_msg, "panic").unwrap();

    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Can read `value1` with the newest ts
    follower_client2.must_kv_read_equal(
        b"key1".to_vec(),
        b"value1".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );

    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Follower 2 can still read `value1`, but can not read `value2` due
    // to it don't have enough data
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
    let resp1 = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp1.get_region_error().has_data_is_not_ready());

    // Leader have up to date data so it can read `value2`
    leader_client.must_kv_read_equal(
        b"key1".to_vec(),
        b"value2".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );

    // clear the `MsgAppend` filter
    cluster.clear_send_filters();

    // Now we can read `value2` with the newest ts
    follower_client2.must_kv_read_equal(
        b"key1".to_vec(),
        b"value2".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );
    fail::remove(on_step_read_index_msg);
}

// Testing how mvcc locks could effect stale read service
#[test]
fn test_stale_read_basic_flow_lock() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.sim.wl().start_resolved_ts_worker();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let leader_client = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);

    // There should be no read index message while handling stale read request
    let on_step_read_index_msg = "on_step_read_index_msg";
    fail::cfg(on_step_read_index_msg, "panic").unwrap();

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Prewrite on `key2` but not commit yet
    let k2_prewrite_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        k2_prewrite_ts,
    );
    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Assert `(key1, value2)` can't be readed with `commit_ts2` due to it's larger than the `start_ts` of `key2`.
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());
    // Still can read `(key1, value1)` since `commit_ts1` is less than the `key2` lock's `start_ts`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Prewrite on `key3` but not commit yet
    let k3_prewrite_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value1"[..])],
        b"key3".to_vec(),
        k3_prewrite_ts,
    );
    // Commit on `key2`
    let k2_commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    leader_client.must_kv_commit(vec![b"key2".to_vec()], k2_prewrite_ts, k2_commit_ts);

    // Although there is still lock on the region, but the min lock is refreshed
    // to the `key3`'s lock, now we can read `(key1, value2)` but not `(key2, value1)`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    let resp = follower_client2.kv_read(b"key2".to_vec(), k2_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    // Commit on `key3`
    let k3_commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    leader_client.must_kv_commit(vec![b"key3".to_vec()], k3_prewrite_ts, k3_commit_ts);

    // Now there is not lock on the region, we can read any
    // up to date data
    follower_client2.must_kv_read_equal(
        b"key2".to_vec(),
        b"value1".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );
    follower_client2.must_kv_read_equal(
        b"key3".to_vec(),
        b"value1".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );
    fail::remove(on_step_read_index_msg);
}

// Testing that if `resolved_ts` updated before `apply_index` update, the `safe_ts`
// won't be updated, hence the leader won't broadcast a wrong `(apply_index, safe_ts)`
// item to other replicas
#[test]
fn test_update_resoved_ts_before_apply_index() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.sim.wl().start_resolved_ts_worker();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let mut leader_client = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    // There should be no read index message while handling stale read request
    let on_step_read_index_msg = "on_step_read_index_msg";
    fail::cfg(on_step_read_index_msg, "panic").unwrap();

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Return before handling `apply_res`, to stop the leader updating the apply index
    let on_apply_res_fp = "on_apply_res";
    fail::cfg(on_apply_res_fp, "return()").unwrap();
    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Write `(key1, value2)`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Wait `resolved_ts` be updated
    sleep_ms(100);

    // The leader can't handle stale read with `commit_ts2` because its `safe_ts`
    // can't update due to its `apply_index` not update
    let resp = leader_client.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready(),);
    // The follower can't handle stale read with `commit_ts2` because it don't
    // have enough data
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());

    fail::remove(on_apply_res_fp);
    cluster.clear_send_filters();

    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);

    fail::remove(on_step_read_index_msg);
}

// Testing that the new elected leader should initialize the `resolver` correctly
#[test]
fn test_new_leader_init_resolver() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.sim.wl().start_resolved_ts_worker();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let mut peer_client1 = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let mut peer_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    peer_client1.ctx.set_stale_read(true);
    peer_client2.ctx.set_stale_read(true);

    // There should be no read index message while handling stale read request
    let on_step_read_index_msg = "on_step_read_index_msg";
    fail::cfg(on_step_read_index_msg, "panic").unwrap();

    // Write `(key1, value1)`
    let commit_ts1 = peer_client1.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // There are no lock in the region, the `safe_ts` should keep updating by the new leader,
    // so we can read `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(2, 2));
    peer_client1.must_kv_read_equal(
        b"key1".to_vec(),
        b"value1".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );

    // Prewrite on `key2` but not commit yet
    peer_client2.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );

    // There are locks in the region, the `safe_ts` can't be updated, so we can't read
    // `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = peer_client2.kv_read(
        b"key1".to_vec(),
        block_on(pd_client.get_tso()).unwrap().into_inner(),
    );
    assert!(resp.get_region_error().has_data_is_not_ready());
    // But we can read `key1` with `commit_ts1`
    peer_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
}
