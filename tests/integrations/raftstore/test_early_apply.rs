// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksSnapshot;
use raft::eraftpb::MessageType;
use raftstore::store::*;
use std::time::*;
use test_raftstore::*;

/// Allow lost situation.
#[derive(PartialEq, Eq, Clone, Copy)]
enum DataLost {
    /// The leader loses commit index.
    ///
    /// A leader can't lost both the committed entries and commit index
    /// at the same time.
    LeaderCommit,
    /// A follower loses committed entries and commit index.
    ///
    /// This can happen when the follower receives committed entries and
    /// commit index at the same batch.
    FollowerAppend,
    /// A follower loses commit index.
    FollowerCommit,
    /// All the nodes loses data.
    ///
    /// Typically, both leader and followers lose commit index.
    AllLost,
}

fn test<A, C>(cluster: &mut Cluster<NodeCluster>, action: A, check: C, mode: DataLost)
where
    A: FnOnce(&mut Cluster<NodeCluster>),
    C: FnOnce(&mut Cluster<NodeCluster>),
{
    let filter = match mode {
        DataLost::AllLost | DataLost::LeaderCommit => RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
        DataLost::FollowerCommit => RegionPacketFilter::new(1, 3)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
        DataLost::FollowerAppend => RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    };
    cluster.add_send_filter(CloneFilterFactory(filter));
    let last_index = cluster.raft_local_state(1, 1).get_last_index();
    action(cluster);
    if mode != DataLost::FollowerAppend {
        cluster.wait_last_index(1, 1, last_index + 1, Duration::from_secs(3));
    } else {
        cluster.wait_last_index(1, 2, last_index + 1, Duration::from_secs(3));
    }
    let mut snaps = vec![];
    snaps.push((1, RocksSnapshot::new(cluster.get_raft_engine(1))));
    if mode == DataLost::AllLost {
        snaps.push((2, RocksSnapshot::new(cluster.get_raft_engine(2))));
        snaps.push((3, RocksSnapshot::new(cluster.get_raft_engine(3))));
    }
    cluster.clear_send_filters();
    if mode == DataLost::FollowerAppend {
        let filter = RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgAppendResponse);
        cluster.add_send_filter(CloneFilterFactory(filter));
    }
    check(cluster);
    for (id, _) in &snaps {
        cluster.stop_node(*id);
    }
    // Simulate data lost in raft cf.
    for (id, snap) in &snaps {
        cluster.restore_raft(1, *id, snap);
    }
    cluster.clear_send_filters();
    for (id, _) in &snaps {
        cluster.run_node(*id).unwrap();
    }

    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    }
}

/// Test whether system can recover from mismatched raft state and apply state.
///
/// If TiKV is not shutdown gracefully, apply state may go ahead of raft
/// state. TiKV should be able to recognize the situation and start normally.
fn test_early_apply(mode: DataLost) {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    } else {
        cluster.must_transfer_leader(1, new_peer(3, 3));
    }
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    test(
        &mut cluster,
        |c| {
            c.async_put(b"k2", b"v2").unwrap();
        },
        |c| must_get_equal(&c.get_engine(1), b"k2", b"v2"),
        mode,
    );
    let region = cluster.get_region(b"");
    test(
        &mut cluster,
        |c| {
            c.split_region(&region, b"k2", Callback::None);
        },
        |c| c.wait_region_split(&region),
        mode,
    );
    if mode != DataLost::LeaderCommit && mode != DataLost::AllLost {
        test(
            &mut cluster,
            |c| {
                c.async_remove_peer(1, new_peer(1, 1)).unwrap();
            },
            |c| must_get_none(&c.get_engine(1), b"k2"),
            mode,
        );
    }
}

#[test]
fn test_leader_early_apply() {
    test_early_apply(DataLost::LeaderCommit)
}

#[test]
fn test_follower_commit_early_apply() {
    test_early_apply(DataLost::FollowerCommit)
}

#[test]
fn test_follower_append_early_apply() {
    test_early_apply(DataLost::FollowerAppend)
}

#[test]
fn test_all_node_crash() {
    test_early_apply(DataLost::AllLost)
}
