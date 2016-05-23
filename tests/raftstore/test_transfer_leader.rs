// Copyright 2016 PingCAP, Inc.
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

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use std::time::Duration;

fn test_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    cluster.bootstrap_region().expect("");
    cluster.start();

    // transfer leader to (2, 2)
    cluster.transfer_leader(1, new_peer(2, 2));
    // wait for leader transfer finish
    let (k1, v1) = (b"k1", b"v1");
    cluster.must_put(k1, v1);
    must_get_equal(&cluster.engines[&2], k1, v1);
    // check it
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(2, 2)));

    // transfer leader to (3, 3)
    cluster.transfer_leader(1, new_peer(3, 3));
    let (k2, v2) = (b"k2", b"v2");
    cluster.must_put(k2, v2);
    must_get_equal(&cluster.engines[&3], k2, v2);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(3, 3)));

    let mut region = cluster.get_region(b"k3");
    let mut req = new_request(region.get_id(),
                              region.take_region_epoch(),
                              vec![new_put_cmd(b"k3", b"v3")]);
    req.mut_header().set_peer(new_peer(3, 3));
    // transfer leader to (4, 4)
    cluster.transfer_leader(1, new_peer(4, 4));
    // send request to old leader (3, 3) directly and verify it fails
    let resp = cluster.call_command(req, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().get_error().has_not_leader());
}

#[test]
fn test_server_transfer_leader() {
    let mut cluster = new_node_cluster(0, 5);
    test_transfer_leader(&mut cluster);
}

#[test]
fn test_node_transfer_leader() {
    let mut cluster = new_server_cluster(0, 5);
    test_transfer_leader(&mut cluster);
}
