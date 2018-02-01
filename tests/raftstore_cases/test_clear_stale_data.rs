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

use std::thread;
use std::time::Duration;

use rocksdb::{Writable, DB};

use tikv::storage::types::Key;
use tikv::raftstore::store::keys;

use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;
use super::util::*;

fn new_mvcc_key(i: u8) -> Vec<u8> {
    Key::from_encoded(vec![i]).append_ts(0).encoded().to_owned()
}

fn init_db_with_sst_files(db: &DB, n: u8) {
    for cf in db.cf_names() {
        let handle = db.cf_handle(cf).unwrap();
        // Each SST file has only one kv.
        for i in 0..n {
            let k = keys::data_key(&new_mvcc_key(i));
            db.put_cf(handle, &k, &k).unwrap();
            db.flush_cf(handle, true).unwrap();
        }
        db.compact_range_cf(handle, None, None);
    }
}

fn check_db_files_at_level(db: &DB, level: u64, num_files: u64) {
    for cf in db.cf_names() {
        let handle = db.cf_handle(cf).unwrap();
        let name = format!("rocksdb.num-files-at-level{}", level);
        assert_eq!(db.get_property_int_cf(handle, &name).unwrap(), num_files);
    }
}

fn check_kv_in_all_cfs(db: &DB, i: u8, found: bool) {
    for cf in db.cf_names() {
        let handle = db.cf_handle(cf).unwrap();
        let k = keys::data_key(&new_mvcc_key(i));
        let v = db.get_cf(handle, &k).unwrap();
        if found {
            assert_eq!(v.unwrap(), &k);
        } else {
            assert!(v.is_none());
        }
    }
}

fn test_clear_stale_data<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster
        .cfg
        .rocksdb
        .defaultcf
        .level0_file_num_compaction_trigger = 1;
    cluster
        .cfg
        .rocksdb
        .writecf
        .level0_file_num_compaction_trigger = 1;
    cluster
        .cfg
        .rocksdb
        .lockcf
        .level0_file_num_compaction_trigger = 1;
    cluster
        .cfg
        .rocksdb
        .raftcf
        .level0_file_num_compaction_trigger = 1;

    cluster.start();
    // Cluster needs time to add peers.
    thread::sleep(Duration::from_secs(1));

    let n = 10;
    // Choose one node.
    let node_id = *cluster.get_node_ids().iter().next().unwrap();
    let db = cluster.get_engine(node_id);

    // Split into `n` regions.
    for i in 0..n {
        let k = new_mvcc_key(i);
        let region = cluster.get_region(&k);
        cluster.must_split(&region, &new_mvcc_key(i + 1));
    }

    // Generate `n` files in db at level 1.
    init_db_with_sst_files(&db, n);
    check_db_files_at_level(&db, 1, u64::from(n));

    // Restart the node.
    cluster.stop_node(node_id);
    cluster.run_node(node_id);
    // All kvs should still exist.
    for i in 0..n {
        check_kv_in_all_cfs(&db, i, true);
    }

    // Remove some peers from the node.
    cluster.pd_client.disable_default_rule();
    for i in 0..n {
        if i % 2 == 0 {
            continue;
        }
        let k = new_mvcc_key(i);
        let region = cluster.get_region(&k);
        let peer = find_peer(&region, node_id).unwrap().clone();
        cluster.pd_client.must_remove_peer(region.get_id(), peer);
    }

    // Restart the node.
    cluster.stop_node(node_id);
    cluster.run_node(node_id);
    // Keys in removed peers should not exist.
    for i in 0..n {
        check_kv_in_all_cfs(&db, i, i % 2 == 0);
    }
    check_db_files_at_level(&db, 1, u64::from(n) / 2);
}

#[test]
fn test_server_clear_stale_data() {
    let mut cluster = new_server_cluster(0, 3);
    test_clear_stale_data(&mut cluster);
}
