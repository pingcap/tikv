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

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;
use std::thread;
use tikv::util::{self as tikv_util, logger};
use std::env;

use rocksdb::{DB, WriteBatch, Writable};
use tempdir::TempDir;
use uuid::Uuid;
use protobuf;
use super::cluster::{Cluster, Simulator};

use tikv::raftstore::store::*;
use tikv::server::Config as ServerConfig;
use kvproto::metapb::{self, RegionEpoch};
use kvproto::raft_cmdpb::{Request, StatusRequest, AdminRequest, RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_cmdpb::{CmdType, StatusCmdType, AdminCmdType};
use kvproto::raftpb::ConfChangeType;

pub fn must_get_equal(engine: &Arc<DB>, key: &[u8], value: &[u8]) {
    for _ in 1..200 {
        if let Ok(res) = engine.get_value(&keys::data_key(key)) {
            if let Some(val) = res {
                assert_eq!(&*val, value);
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(false);
}

pub fn must_get_none(engine: &Arc<DB>, key: &[u8]) {
    for _ in 1..200 {
        if let Ok(res) = engine.get_value(&keys::data_key(key)) {
            if res.is_none() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(false);
}

pub fn new_engine(path: &TempDir) -> Arc<DB> {
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    Arc::new(db)
}

pub fn new_store_cfg() -> Config {
    Config {
        raft_base_tick_interval: 10,
        raft_heartbeat_ticks: 2,
        raft_election_timeout_ticks: 20,
        raft_log_gc_tick_interval: 100,
        raft_log_gc_threshold: 1,
        // because our tests may quit very soon, so we take a large
        // value to avoid pd ask failure.
        // TODO: should we ignore all mock pd failure?
        replica_check_tick_interval: 60 * 1000,
        region_check_size_diff: 10000,
        ..Config::default()
    }
}

pub fn new_server_config(cluster_id: u64) -> ServerConfig {
    let store_cfg = new_store_cfg();

    ServerConfig {
        cluster_id: cluster_id,
        addr: "127.0.0.1:0".to_owned(),
        store_cfg: store_cfg,
        ..ServerConfig::default()
    }
}

// Create a base request.
pub fn new_base_request(region_id: u64, epoch: RegionEpoch) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(epoch);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    req
}

pub fn new_request(region_id: u64, epoch: RegionEpoch, requests: Vec<Request>) -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch);
    req.set_requests(protobuf::RepeatedField::from_vec(requests));
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_delete_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd
}

pub fn new_seek_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Seek);
    cmd.mut_seek().set_key(key.to_vec());
    cmd
}

pub fn new_status_request(region_id: u64, request: StatusRequest) -> RaftCmdRequest {
    let mut req = new_base_request(region_id, RegionEpoch::new());
    req.set_status_request(request);
    req
}

pub fn new_region_detail_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCmdType::RegionDetail);
    cmd
}

pub fn new_region_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCmdType::RegionLeader);
    cmd
}

pub fn new_admin_request(region_id: u64,
                         epoch: &RegionEpoch,
                         request: AdminRequest)
                         -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch.clone());
    req.set_admin_request(request);
    req
}

pub fn new_change_peer_cmd(change_type: ConfChangeType, store_id: u64) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::ChangePeer);
    cmd.mut_change_peer().set_change_type(change_type);
    cmd.mut_change_peer().set_store_id(store_id);
    cmd
}

pub fn new_split_region_cmd(split_key: Option<Vec<u8>>, new_region_id: u64) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::Split);
    if let Some(key) = split_key {
        cmd.mut_split().set_split_key(key);
    }
    cmd.mut_split().set_new_region_id(new_region_id);
    cmd
}

pub fn new_compact_log_cmd(index: u64) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::CompactLog);
    cmd.mut_compact_log().set_compact_index(index);
    cmd
}

pub fn new_store(store_id: u64, addr: String) -> metapb::Store {
    let mut store = metapb::Store::new();
    store.set_id(store_id);
    store.set_address(addr);

    store
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

// A help function to initial logger.
pub fn init_log() {
    let level = logger::get_level_by_string(&env::var("LOG_LEVEL").unwrap_or("debug".to_owned()));
    tikv_util::init_log(level).unwrap();
}

pub fn is_error_response(resp: &RaftCmdResponse) -> bool {
    resp.get_header().has_error()
}


pub fn write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(k, &v).expect("");
    }
    db.write(wb).unwrap();
}

pub fn enc_write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), &v).expect("");
    }
    db.write(wb).expect("");
}

pub fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>,
                                     initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.bootstrap_region().expect("");
    cluster.start();
    for engine in cluster.engines.values() {
        enc_write_kvs(engine, initial_kvs);
    }
    cluster.leader_of_region(1).unwrap();
}
