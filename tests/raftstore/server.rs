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

use std::collections::{HashMap, HashSet};
use std::thread::{self, Builder};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::Duration;
use std::boxed::FnBox;
use std::net::TcpListener;

use rocksdb::DB;
use tempdir::TempDir;

use super::cluster::{Simulator, Cluster};
use tikv::server::{ServerChannel, Server, ServerTransport, create_event_loop, Msg};
use tikv::server::{Node, Config, create_raft_storage, PdStoreAddrResolver};
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::transport::RaftStoreRouter;
use tikv::raftstore::{Error, Result, store};
use tikv::raftstore::store::{Msg as StoreMsg, SnapManager};
use tikv::util::codec::rpc;
use tikv::util::transport::SendCh;
use tikv::util::core_runner::CoreRunner;
use tikv::storage::{Engine, CfName, ALL_CFS};
use tikv::util::make_std_tcp_conn;
use kvproto::raft_serverpb::{self, RaftMessage};
use kvproto::raft_cmdpb::*;

use super::pd::TestPdClient;
use super::transport_simulate::*;

type SimulateServerTransport = SimulateTransport<RaftMessage, ServerTransport>;

pub struct ServerCluster {
    routers: HashMap<u64, SimulateTransport<StoreMsg, ServerRaftStoreRouter>>,
    senders: HashMap<u64, SendCh<Msg>>,
    cores: HashMap<u64, CoreRunner>,
    handles: HashMap<u64, (Node<TestPdClient>, thread::JoinHandle<()>)>,
    addrs: HashMap<u64, SocketAddr>,
    conns: Mutex<HashMap<SocketAddr, Vec<TcpStream>>>,
    sim_trans: HashMap<u64, SimulateServerTransport>,
    store_chs: HashMap<u64, SendCh<StoreMsg>>,
    pub storages: HashMap<u64, Box<Engine>>,
    snap_paths: HashMap<u64, TempDir>,

    msg_id: AtomicUsize,
    pd_client: Arc<TestPdClient>,
}

impl ServerCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> ServerCluster {
        ServerCluster {
            routers: HashMap::new(),
            senders: HashMap::new(),
            cores: HashMap::new(),
            handles: HashMap::new(),
            addrs: HashMap::new(),
            sim_trans: HashMap::new(),
            conns: Mutex::new(HashMap::new()),
            msg_id: AtomicUsize::new(1),
            pd_client: pd_client,
            store_chs: HashMap::new(),
            storages: HashMap::new(),
            snap_paths: HashMap::new(),
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }


    fn pool_get(&self, addr: &SocketAddr) -> Result<TcpStream> {
        {
            let mut conns = self.conns
                .lock()
                .unwrap();
            let conn = conns.get_mut(addr);
            if let Some(mut pool) = conn {
                if !pool.is_empty() {
                    return Ok(pool.pop().unwrap());
                }
            }
        }

        let conn = make_std_tcp_conn(addr).unwrap();
        Ok(conn)
    }

    fn pool_put(&self, addr: &SocketAddr, conn: TcpStream) {
        let mut conns = self.conns
            .lock()
            .unwrap();
        let p = conns.entry(*addr).or_insert_with(Vec::new);
        p.push(conn);
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self, node_id: u64, cfg: Config, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.handles.contains_key(&node_id));
        assert!(node_id == 0 || !self.senders.contains_key(&node_id));

        let mut cfg = cfg;
        cfg.addr = if let Some(addr) = self.addrs.get(&node_id) {
            format!("{}", addr)
        } else {
            format!("127.0.0.1:{}", 31060 + node_id)
        };

        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = TempDir::new("test_cluster").unwrap();
            (p.path().to_str().unwrap().to_owned(), Some(p))
        } else {
            let p = self.snap_paths[&node_id].path().to_str().unwrap();
            (p.to_owned(), None)
        };

        // TODO: simplify creating raft server later.
        let mut event_loop = create_event_loop(&cfg).unwrap();
        let sendch = SendCh::new(event_loop.channel(), "cluster-simulator");
        let resolver = PdStoreAddrResolver::new(self.pd_client.clone()).unwrap();
        let trans = ServerTransport::new(sendch.clone());

        let mut store_event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
        let simulate_trans = SimulateTransport::new(trans.clone());
        let mut node = Node::new(&mut store_event_loop, &cfg, self.pd_client.clone());
        let snap_mgr = SnapManager::new(tmp_str,
                                        Some(node.get_sendch()),
                                        cfg.raft_store.use_sst_file_snapshot);

        node.start(store_event_loop,
                   engine.clone(),
                   simulate_trans.clone(),
                   snap_mgr.clone())
            .unwrap();
        let router = ServerRaftStoreRouter::new(node.get_sendch(), node.id());
        let sim_router = SimulateTransport::new(router);

        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }

        self.store_chs.insert(node_id, node.get_sendch());
        self.sim_trans.insert(node_id, simulate_trans);

        let mut store = create_raft_storage(sim_router.clone(), engine, &cfg).unwrap();
        store.start(&cfg.storage).unwrap();
        self.storages.insert(node_id, store.get_engine());

        let core = CoreRunner::new(thd_name!(format!("server-core-{}", node_id)));

        let server_chan = ServerChannel {
            raft_router: sim_router.clone(),
            snapshot_status_sender: node.get_snapshot_status_sender(),
        };
        let mut server = Server::new(&mut event_loop,
                                     core.remote(),
                                     &cfg,
                                     store,
                                     server_chan,
                                     resolver,
                                     snap_mgr)
            .unwrap();

        let ch = server.get_sendch();
        let addr = server.listening_addr();

        let t = Builder::new()
            .name(thd_name!(format!("server-{}", node_id)))
            .spawn(move || {
                server.run(&mut event_loop).unwrap();
            })
            .unwrap();

        self.handles.insert(node_id, (node, t));
        self.senders.insert(node_id, ch);
        self.cores.insert(node_id, core);
        self.routers.insert(node_id, sim_router);
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_paths[&node_id].path().to_str().unwrap().to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        let (mut node, h) = self.handles.remove(&node_id).unwrap();
        let ch = self.senders.remove(&node_id).unwrap();
        let core = self.cores.remove(&node_id).unwrap();
        let addr = &self.addrs[&node_id];
        let _ = self.store_chs.remove(&node_id).unwrap();
        self.conns
            .lock()
            .unwrap()
            .remove(addr);

        ch.try_send(Msg::Quit).unwrap();
        node.stop().unwrap();
        h.join().unwrap();
        core.stop();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.senders.keys().cloned().collect()
    }

    fn call_command_on_node(&self,
                            node_id: u64,
                            request: RaftCmdRequest,
                            timeout: Duration)
                            -> Result<RaftCmdResponse> {
        if !self.routers.contains_key(&node_id) {
            return Err(box_err!("missing sender for store {}", node_id));
        }

        let router = self.routers.get(&node_id).cloned().unwrap();
        wait_op!(|cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
                     router.send_command(request, cb).unwrap()
                 },
                 timeout)
            .ok_or_else(|| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    fn send_raft_msg(&self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_to_peer().get_store_id();
        let addr = &self.addrs[&store_id];

        let mut conn = self.pool_get(addr).unwrap();
        conn.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        try!(rpc::encode_msg(&mut conn, self.alloc_msg_id(), &raft_msg));

        self.pool_put(addr, conn);

        Ok(())
    }

    fn add_send_filter(&mut self, node_id: u64, filter: SendFilter) {
        self.sim_trans.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_send_filters(&mut self, node_id: u64) {
        self.sim_trans.get_mut(&node_id).unwrap().clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: RecvFilter) {
        self.routers.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.routers.get_mut(&node_id).unwrap().clear_filters();
    }

    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh<StoreMsg>> {
        self.store_chs.get(&node_id).cloned()
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    new_server_cluster_with_cfs(id, count, ALL_CFS)
}

pub fn new_server_cluster_with_cfs(id: u64,
                                   count: usize,
                                   cfs: &[CfName])
                                   -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(ServerCluster::new(pd_client.clone())));
    Cluster::new(id, count, cfs, sim, pd_client)
}

fn alloc_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
