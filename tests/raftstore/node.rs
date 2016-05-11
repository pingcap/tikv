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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::server::Node;
use tikv::raftstore::store::{self, Transport, msg};
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use tikv::raftstore::Result;
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use tikv::server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;
use super::transport_simulate::{Strategy, SimulateTransport, Filter};

pub struct ChannelTransport {
    pub routers: HashMap<u64, Arc<RwLock<ServerRaftStoreRouter>>>,
}

impl ChannelTransport {
    pub fn new() -> Arc<RwLock<ChannelTransport>> {
        Arc::new(RwLock::new(ChannelTransport { routers: HashMap::new() }))
    }
}

impl Transport for ChannelTransport {
    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        let to_store = msg.get_message().get_to();

        match self.routers.get(&to_store) {
            Some(h) => h.rl().send_raft_msg(msg),
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    cluster_id: u64,
    trans: Arc<RwLock<ChannelTransport>>,
    pd_client: Arc<RwLock<TestPdClient>>,
    nodes: HashMap<u64, Node<TestPdClient>>,
    simulate_trans: HashMap<u64, Arc<RwLock<SimulateChannelTransport>>>,
}

impl NodeCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> NodeCluster {
        NodeCluster {
            cluster_id: cluster_id,
            trans: ChannelTransport::new(),
            pd_client: pd_client,
            nodes: HashMap::new(),
            simulate_trans: HashMap::new(),
        }
    }
}

impl Simulator for NodeCluster {
    fn run_node(&mut self,
                node_id: u64,
                cfg: ServerConfig,
                engine: Arc<DB>,
                strategy: Vec<Strategy>)
                -> u64 {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));

        let mut event_loop = store::create_event_loop(&cfg.store_cfg).unwrap();
        let simulate_trans = SimulateTransport::new(strategy, self.trans.clone());
        let trans = Arc::new(RwLock::new(simulate_trans));
        let mut node = Node::new(&mut event_loop, &cfg, self.pd_client.clone());

        node.start(event_loop, engine, trans.clone()).unwrap();
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        self.trans.wl().routers.insert(node_id, node.raft_store_router());
        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, trans);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let node = self.nodes.remove(&node_id).unwrap();
        self.trans.wl().routers.remove(&node_id).unwrap();

        drop(node);
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn call_command(&self,
                    store_id: u64,
                    request: RaftCmdRequest,
                    timeout: Duration)
                    -> Result<RaftCmdResponse> {
        if !self.trans.rl().routers.contains_key(&store_id) {
            return Err(box_err!("missing sender for store {}", store_id));
        }

        let router = self.trans.rl().routers.get(&store_id).cloned().unwrap();
        let ch = router.rl().ch.clone();
        msg::call_command(&ch, request, timeout)
    }

    fn send_raft_msg(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.trans.rl().send(msg)
    }

    fn hook_transport(&self, node_id: u64, filters: Vec<RwLock<Box<Filter>>>) {
        let trans = self.simulate_trans.get(&node_id).unwrap();
        trans.wl().set_filters(filters);
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(NodeCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}
