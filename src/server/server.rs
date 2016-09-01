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
use std::option::Option;
use std::sync::atomic::{AtomicBool, Ordering};
use std::boxed::Box;
use std::net::SocketAddr;

use mio::{Token, Handler, EventLoop, EventLoopBuilder, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{MessageType, Message};
use super::{Msg, ConnData};
use super::conn::Conn;
use super::{Result, OnResponse, Config};
use util::worker::{Stopped, Worker};
use util::transport::SendCh;
use storage::Storage;
use raftstore::store::SnapManager;
use super::kv::StoreHandler;
use super::coprocessor::{RequestTask, EndPointHost, EndPointTask};
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snap::{Task as SnapTask, Runner as SnapHandler};
use raft::SnapshotStatus;
use util::sockopt::SocketOpt;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const DEFAULT_COPROCESSOR_BATCH: usize = 50;

pub fn create_event_loop<T, S>(config: &Config) -> Result<EventLoop<Server<T, S>>>
    where T: RaftStoreRouter,
          S: StoreAddrResolver
{
    let mut builder = EventLoopBuilder::new();
    builder.notify_capacity(config.notify_capacity);
    builder.messages_per_tick(config.messages_per_tick);
    let el = try!(builder.build());
    Ok(el)
}

pub fn bind(addr: &str) -> Result<TcpListener> {
    let laddr = try!(addr.parse());
    let listener = try!(TcpListener::bind(&laddr));
    Ok(listener)
}

pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver> {
    listener: TcpListener,
    // We use HashMap instead of common use mio slab to avoid token reusing.
    // In our raft server, a client with token 1 sends a raft command, we will
    // propose this command, execute it then send the response to the client with
    // token 1. But before the response, the client connection is broken and another
    // new client connects, mio slab may reuse the token 1 for it. So the subsequent
    // response will be sent to the new client.
    // To avoid this, we use the HashMap instead and can guarantee the token id is
    // unique and can't be reused.
    conns: HashMap<Token, Conn>,
    conn_token_counter: usize,
    sendch: SendCh<Msg>,

    // the cache of all known store ids
    // It's used for validating incoming raft messages. Everytime a server receives
    // a raft message, it will check:
    // 1. whether it from known store, if not, go to 2
    // 2. validate whether PD has the info of this store. if yes,
    //    mark this store id as "known" by inserting this store id into this cache
    // Only a message from a known store will be routed to the local store,
    // otherwise it will be thrown away.
    known_store_ids: HashSet<u64>,

    // store id -> Token
    // This is for communicating with other raft stores.
    store_tokens: HashMap<u64, Token>,
    store_resolving: HashSet<u64>,

    raft_router: T,

    store: StoreHandler,
    end_point_worker: Worker<EndPointTask>,

    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    resolver: S,

    cfg: Config,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Server<T, S> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: &Config,
               listener: TcpListener,
               storage: Storage,
               raft_router: T,
               resolver: S,
               snap_mgr: SnapManager)
               -> Result<Server<T, S>> {
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel());
        let store_handler = StoreHandler::new(storage);
        let end_point_worker = Worker::new("end-point-worker");
        let snap_worker = Worker::new("snap-handler");

        let svr = Server {
            listener: listener,
            sendch: sendch,
            conns: HashMap::new(),
            conn_token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            known_store_ids: HashSet::new(),
            store_tokens: HashMap::new(),
            store_resolving: HashSet::new(),
            raft_router: raft_router,
            store: store_handler,
            end_point_worker: end_point_worker,
            snap_mgr: snap_mgr,
            snap_worker: snap_worker,
            resolver: resolver,
            cfg: cfg.clone(),
        };

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        let end_point = EndPointHost::new(self.store.engine(),
                                          self.end_point_worker.scheduler(),
                                          self.cfg.end_point_concurrency);
        box_try!(self.end_point_worker.start_batch(end_point, DEFAULT_COPROCESSOR_BATCH));

        let ch = self.get_sendch();
        let snap_runner = SnapHandler::new(self.snap_mgr.clone(), self.raft_router.clone(), ch);
        box_try!(self.snap_worker.start(snap_runner));

        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> Result<SocketAddr> {
        let addr = try!(self.listener.local_addr());
        Ok(addr)
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(mut conn) => {
                debug!("remove connection token {:?}", token);
                // if connected to remote store, remove this too.
                if let Some(store_id) = conn.store_id {
                    warn!("remove store connection for store {} with token {:?}",
                          store_id,
                          token);
                    self.store_tokens.remove(&store_id);
                }

                if let Err(e) = event_loop.deregister(&conn.sock) {
                    error!("deregister conn err {:?}", e);
                }

                conn.close();
            }
            None => {
                debug!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Self>,
                    sock: TcpStream,
                    store_id: Option<u64>)
                    -> Result<Token> {
        let new_token = Token(self.conn_token_counter);
        self.conn_token_counter += 1;

        // TODO: check conn max capacity.

        try!(sock.set_nodelay(true));
        try!(sock.set_send_buffer_size(self.cfg.send_buffer_size));
        try!(sock.set_recv_buffer_size(self.cfg.recv_buffer_size));

        try!(event_loop.register(&sock,
                                 new_token,
                                 EventSet::readable() | EventSet::hup(),
                                 PollOpt::edge()));

        let conn = Conn::new(sock, new_token, store_id, self.snap_worker.scheduler());
        self.conns.insert(new_token, conn);
        debug!("register conn {:?}", new_token);

        Ok(new_token)
    }

    fn on_conn_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) -> Result<()> {
        let msgs = try!(match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return Ok(());
            }
            Some(conn) => conn.on_readable(event_loop),
        });

        if msgs.is_empty() {
            // Read no message, no need to handle.
            return Ok(());
        }

        for msg in msgs {
            try!(self.on_conn_msg(token, msg))
        }

        Ok(())
    }

    fn on_conn_msg(&mut self, token: Token, data: ConnData) -> Result<()> {
        let msg_id = data.msg_id;
        let mut msg = data.msg;

        let msg_type = msg.get_msg_type();
        match msg_type {
            MessageType::Raft => self.on_raft_msg(msg.take_raft()),
            MessageType::Cmd => self.on_raft_command(msg.take_cmd_req(), token, msg_id),
            MessageType::KvReq => {
                let req = msg.take_kv_req();
                debug!("notify Request token[{:?}] msg_id[{}] type[{:?}]",
                       token,
                       msg_id,
                       req.get_field_type());
                let on_resp = self.make_response_cb(token, msg_id);
                self.store.on_request(req, on_resp)
            }
            MessageType::CopReq => {
                let on_resp = self.make_response_cb(token, msg_id);
                let req = RequestTask::new(msg.take_cop_req(), on_resp);
                box_try!(self.end_point_worker.schedule(EndPointTask::Request(req)));
                Ok(())
            }
            _ => {
                Err(box_err!("unsupported message {:?} for token {:?} with msg id {}",
                             msg_type,
                             token,
                             msg_id))
            }
        }
    }

    // validates the source store id of this raft message before deliver it to the underlying store
    fn on_raft_msg(&mut self, msg: RaftMessage) -> Result<()> {
        let store_id = msg.get_from_peer().get_store_id();
        // check whether this store id is known already
        if self.known_store_ids.contains(&store_id) {
            try!(self.raft_router.send_raft_msg(msg));
            return Ok(());
        }

        // if we are resolving the store id, simply drop the message
        if self.store_resolving.contains(&store_id) {
            debug!("store {} address is being resolved, drop msg {:?}",
                   store_id,
                   msg);
            return Ok(());
        }

        // try to validate whether PD has the store info of the specified store id
        info!("begin to validate store id {}", store_id);
        self.store_resolving.insert(store_id);
        self.validate_store_id(store_id, msg)
    }

    fn on_raft_command(&mut self, msg: RaftCmdRequest, token: Token, msg_id: u64) -> Result<()> {
        trace!("handle raft command {:?}", msg);
        let on_resp = self.make_response_cb(token, msg_id);
        let cb = box move |resp| {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CmdResp);
            resp_msg.set_cmd_resp(resp);

            on_resp.call_box((resp_msg,));
            Ok(())
        };

        try!(self.raft_router.send_command(msg, cb));

        Ok(())
    }

    fn on_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        match token {
            SERVER_TOKEN => {
                loop {
                    // For edge trigger, we must accept all connections until None.
                    let sock = match self.listener.accept() {
                        Err(e) => {
                            error!("accept error: {:?}", e);
                            return;
                        }
                        Ok(None) => {
                            debug!("no connection, accept later.");
                            return;
                        }
                        Ok(Some((sock, addr))) => {
                            debug!("accept conn {}", addr);
                            sock
                        }
                    };

                    if let Err(e) = self.add_new_conn(event_loop, sock, None) {
                        error!("register conn err {:?}", e);
                    }
                }
            }
            token => {
                if let Err(e) = self.on_conn_readable(event_loop, token) {
                    debug!("handle read conn for token {:?} err {:?}, remove", token, e);
                    self.remove_conn(event_loop, token);
                }
            }

        }
    }

    fn on_writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let res = match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.on_writable(event_loop),
        };

        if let Err(e) = res {
            debug!("handle write conn err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn write_data(&mut self, event_loop: &mut EventLoop<Self>, token: Token, data: ConnData) {
        let res = match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.append_write_buf(event_loop, data),
        };

        if let Err(e) = res {
            debug!("handle write data err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn try_connect(&mut self,
                   event_loop: &mut EventLoop<Self>,
                   sock_addr: SocketAddr,
                   store_id_opt: Option<u64>)
                   -> Result<Token> {
        let sock = try!(TcpStream::connect(&sock_addr));
        let token = try!(self.add_new_conn(event_loop, sock, store_id_opt));
        Ok(token)
    }

    fn connect_store(&mut self,
                     event_loop: &mut EventLoop<Self>,
                     store_id: u64,
                     sock_addr: SocketAddr)
                     -> Result<Token> {
        // We may already create the connection before.
        if let Some(token) = self.store_tokens.get(&store_id).cloned() {
            debug!("token already exists for store {}, reuse", store_id);
            return Ok(token);
        }

        let token = try!(self.try_connect(event_loop, sock_addr, Some(store_id)));
        self.store_tokens.insert(store_id, token);
        Ok(token)
    }

    fn resolve_store(&mut self, store_id: u64, data: ConnData) {
        let ch = self.sendch.clone();
        let cb = box move |r| {
            if let Err(e) = ch.send(Msg::ResolveResult {
                store_id: store_id,
                sock_addr: r,
                data: data,
            }) {
                error!("send store sock msg err {:?}", e);
            }
        };
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("try to resolve err {:?}", e);
        }
    }

    // asks resolver to resolve the address of the specified store id
    // We could tell whether that store is valid by the result of address resolving.
    fn validate_store_id(&mut self, store_id: u64, msg: RaftMessage) -> Result<()> {
        let ch = self.sendch.clone();
        let cb = box move |r| {
            let valid = match r {
                Ok(_) => true,
                Err(_) => false,
            };
            if let Err(e) = ch.send(Msg::ValidateResult {
                store_id: store_id,
                valid: valid,
                msg: msg,
            }) {
                error!("send store id msg err {:?}", e)
            }
        };
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("try to validate store {} err {:?}", store_id, e);
        }
        Ok(())
    }

    fn report_unreachable(&self, data: ConnData) {
        if data.msg.has_raft() {
            return;
        }

        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();

        if let Err(e) = self.raft_router.report_unreachable(region_id, to_peer_id) {
            error!("report peer {} unreachable for region {} failed {:?}",
                   to_peer_id,
                   region_id,
                   e);
        }
    }

    fn send_store(&mut self, event_loop: &mut EventLoop<Self>, store_id: u64, data: ConnData) {
        if data.is_snapshot() {
            return self.resolve_store(store_id, data);
        }

        // check the corresponding token for store.
        if let Some(token) = self.store_tokens.get(&store_id).cloned() {
            return self.write_data(event_loop, token, data);
        }

        // No connection, try to resolve it.
        if self.store_resolving.contains(&store_id) {
            // If we are resolving the address, drop the message here.
            debug!("store {} address is being resolved, drop msg {}",
                   store_id,
                   data);
            self.report_unreachable(data);
            return;
        }

        info!("begin to resolve store {} address", store_id);
        self.store_resolving.insert(store_id);
        self.resolve_store(store_id, data);
    }

    fn on_resolve_failed(&mut self, store_id: u64, sock_addr: Result<SocketAddr>, data: ConnData) {
        let e = sock_addr.unwrap_err();
        warn!("resolve store {} address failed {:?}", store_id, e);

        self.report_unreachable(data)
    }

    fn on_resolve_result(&mut self,
                         event_loop: &mut EventLoop<Self>,
                         store_id: u64,
                         sock_addr: Result<SocketAddr>,
                         data: ConnData) {
        if !data.is_snapshot() {
            // clear resolving.
            self.store_resolving.remove(&store_id);
        }

        if sock_addr.is_err() {
            return self.on_resolve_failed(store_id, sock_addr, data);
        }

        let sock_addr = sock_addr.unwrap();
        info!("resolve store {} address ok, addr {}", store_id, sock_addr);

        if data.is_snapshot() {
            return self.send_snapshot_sock(sock_addr, data);
        }

        let token = match self.connect_store(event_loop, store_id, sock_addr) {
            Ok(token) => token,
            Err(e) => {
                self.report_unreachable(data);
                error!("connect store {} err {:?}", store_id, e);
                return;
            }
        };

        self.write_data(event_loop, token, data)
    }

    // Performs the actions basing on the store id validation result.
    // If the store id is valid, deliver the raft message to the underlying store.
    // Otherwise simply log a error and drop the message.
    fn on_validate_store_result(&mut self, store_id: u64, valid: bool, msg: RaftMessage) {
        // unmark this store id from resolving
        self.store_resolving.remove(&store_id);
        // if the store id is validated by PD to be invalid, drop the message
        if !valid {
            error!("drop msg {:?} from invalid store id {}", msg, store_id);
            return;
        }
        // send the message to the underlying store
        let from_peer = msg.get_from_peer().clone();
        let to_peer = msg.get_to_peer().clone();
        if let Err(e) = self.raft_router.send_raft_msg(msg) {
            error!("send msg from peer: {:?} to peer: {:?} err {:?}",
                   from_peer,
                   to_peer,
                   e);
        }
    }

    fn new_snapshot_reporter(&self, data: &ConnData) -> SnapshotReporter<T> {
        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();

        SnapshotReporter {
            router: self.raft_router.clone(),
            region_id: region_id,
            to_peer_id: to_peer_id,
            reported: AtomicBool::new(false),
        }
    }

    fn send_snapshot_sock(&mut self, sock_addr: SocketAddr, data: ConnData) {
        let rep = self.new_snapshot_reporter(&data);
        let cb = box move |res: Result<()>| {
            if let Err(_) = res {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        };
        if let Err(Stopped(SnapTask::SendTo { cb, .. })) = self.snap_worker
            .schedule(SnapTask::SendTo {
                addr: sock_addr,
                data: data,
                cb: cb,
            }) {
            error!("channel is closed, failed to schedule snapshot to {}",
                   sock_addr);
            cb(Err(box_err!("failed to schedule snapshot")));
        }
    }

    fn make_response_cb(&mut self, token: Token, msg_id: u64) -> OnResponse {
        let ch = self.sendch.clone();
        box move |res: Message| {
            let tp = res.get_msg_type();
            if let Err(e) = ch.send(Msg::WriteData {
                token: token,
                data: ConnData::new(msg_id, res),
            }) {
                error!("send {:?} resp failed with token {:?}, msg id {}, err {:?}",
                       tp,
                       token,
                       msg_id,
                       e);
            }
        }
    }
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Handler for Server<T, S> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.on_readable(event_loop, token);
        }

        if events.is_writable() {
            self.on_writable(event_loop, token);
        }

        if events.is_hup() {
            self.remove_conn(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData { token, data } => self.write_data(event_loop, token, data),
            Msg::SendStore { store_id, data } => self.send_store(event_loop, store_id, data),
            Msg::ResolveResult { store_id, sock_addr, data } => {
                self.on_resolve_result(event_loop, store_id, sock_addr, data)
            }
            Msg::ValidateResult { store_id, valid, msg } => {
                self.on_validate_store_result(store_id, valid, msg)
            }
            Msg::CloseConn { token } => self.remove_conn(event_loop, token),
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Self>, _: Msg) {
        // nothing to do now.
    }

    fn interrupted(&mut self, _: &mut EventLoop<Self>) {
        // To be able to be attached by gdb, we should not shutdown.
        // TODO: find a grace way to shutdown.
        // event_loop.shutdown();
    }

    fn tick(&mut self, el: &mut EventLoop<Self>) {
        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        // TODO: handle quit server if event_loop is_running() returns false.
        if !el.is_running() {
            let end_point_handle = self.end_point_worker.stop();
            if let Err(e) = self.store.stop() {
                error!("failed to stop store: {:?}", e);
            }
            if let Some(Err(e)) = end_point_handle.map(|h| h.join()) {
                error!("failed to stop end point: {:?}", e);
            }
        }
    }
}

struct SnapshotReporter<T: RaftStoreRouter + 'static> {
    router: T,
    region_id: u64,
    to_peer_id: u64,

    reported: AtomicBool,
}

impl<T: RaftStoreRouter + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        // return directly if already reported.
        if self.reported.compare_and_swap(false, true, Ordering::Relaxed) {
            return;
        }

        debug!("send snapshot to {} for {} {:?}",
               self.to_peer_id,
               self.region_id,
               status);


        if let Err(e) = self.router
            .report_snapshot(self.region_id, self.to_peer_id, status) {
            error!("report snapshot to peer {} with region {} err {:?}",
                   self.to_peer_id,
                   self.region_id,
                   e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use std::collections::HashSet;
    use std::sync::mpsc::{self, Sender};
    use std::net::{SocketAddr, TcpStream};

    use mio::tcp::TcpListener;

    use super::*;
    use super::super::{Msg, ConnData, Result, Config};
    use super::super::transport::RaftStoreRouter;
    use super::super::resolve::{StoreAddrResolver, Callback as ResolveCallback};
    use super::super::errors::Error;
    use pd::errors::Error as PdError;
    use storage::Storage;
    use kvproto::msgpb::{Message, MessageType};
    use kvproto::raft_serverpb::RaftMessage;
    use kvproto::metapb::Peer;
    use raftstore::Result as RaftStoreResult;
    use raftstore::store::{self, Msg as StoreMsg};
    use raft::SnapshotStatus;
    use util::codec::rpc;

    struct MockResolver {
        addr: SocketAddr,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            cb.call_box((Ok(self.addr),));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestRaftStoreRouter {
        tx: Sender<usize>,
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn report_snapshot(&self, _: u64, _: u64, _: SnapshotStatus) -> RaftStoreResult<()> {
            unimplemented!();
        }

        fn report_unreachable(&self, _: u64, _: u64) -> RaftStoreResult<()> {
            unimplemented!();
        }
    }

    #[test]
    fn test_peer_resolve() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        let resolver = MockResolver { addr: listener.local_addr().unwrap() };

        let cfg = Config::new();
        let mut event_loop = create_event_loop(&cfg).unwrap();
        let mut storage = Storage::new(&cfg.storage).unwrap();
        storage.start(&cfg.storage).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut server = Server::new(&mut event_loop,
                                     &cfg,
                                     listener,
                                     storage,
                                     TestRaftStoreRouter { tx: tx },
                                     resolver,
                                     store::new_snap_mgr("", None))
            .unwrap();

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            event_loop.run(&mut server).unwrap();
        });

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);

        ch.send(Msg::SendStore {
                store_id: 1,
                data: ConnData::new(0, msg),
            })
            .unwrap();

        rx.recv().unwrap();

        ch.send(Msg::Quit).unwrap();
        h.join().unwrap();
    }

    struct MockFailResolver {
        addr: SocketAddr,
        valid_store_ids: HashSet<u64>,
    }

    impl StoreAddrResolver for MockFailResolver {
        fn resolve(&self, id: u64, cb: ResolveCallback) -> Result<()> {
            match self.valid_store_ids.contains(&id) {
                true => cb.call_box((Ok(self.addr),)),
                false => {
                    cb.call_box((Err(Error::Pd(PdError::Other(box_err!("error from \
                                                                        MockFailResolver")))),))
                }
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestStoreIdRaftStoreRouter {
        tx: Sender<u64>,
    }

    impl RaftStoreRouter for TestStoreIdRaftStoreRouter {
        fn send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
            match msg {
                StoreMsg::RaftMessage(raft_msg) => {
                    let id = raft_msg.get_from_peer().get_store_id();
                    self.tx.send(id).unwrap();
                }
                _ => panic!("unexpected message"),
            }
            Ok(())
        }

        fn report_snapshot(&self, _: u64, _: u64, _: SnapshotStatus) -> RaftStoreResult<()> {
            unimplemented!();
        }

        fn report_unreachable(&self, _: u64, _: u64) -> RaftStoreResult<()> {
            unimplemented!();
        }
    }

    /// A helper function to construct a raft message with specified store id.
    fn new_raft_message(store_id: u64) -> Message {
        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);
        let mut from_peer = Peer::new();
        from_peer.set_store_id(store_id);
        let mut raft_msg = RaftMessage::new();
        raft_msg.set_from_peer(from_peer);
        msg.set_raft(raft_msg);
        msg
    }

    /// This test case tests whether store id validation is working.
    /// After the setup of server, it send two raft message with
    /// 1. invalid store id
    /// 2. valid store id
    /// and verify that only message 2 is delivered to the underlying store of that server.
    #[test]
    fn test_store_id_validation() {
        // setup the testing server
        let addr = "127.0.0.1:60001".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        let valid_store_id = 2u64;
        let mut valid_store_ids = HashSet::new();
        valid_store_ids.insert(valid_store_id);
        let resolver = MockFailResolver {
            addr: listener.local_addr().unwrap(),
            valid_store_ids: valid_store_ids,
        };
        let (tx, rx) = mpsc::channel();
        let cfg = Config::new();
        let mut event_loop = create_event_loop(&cfg).unwrap();
        let mut storage = Storage::new(&cfg.storage).unwrap();
        storage.start(&cfg.storage).unwrap();

        let mut server = Server::new(&mut event_loop,
                                     &cfg,
                                     listener,
                                     storage,
                                     TestStoreIdRaftStoreRouter { tx: tx },
                                     resolver,
                                     store::new_snap_mgr("", None))
            .unwrap();

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            event_loop.run(&mut server).unwrap();
        });

        // construct a raft message with invalid store id and send it to server
        let msg_1 = new_raft_message(1u64);
        let mut sock = TcpStream::connect(&addr).unwrap();
        sock.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
        rpc::encode_msg(&mut sock, 1u64, &msg_1).unwrap();

        // check the raft message is not delivered to the underlying store
        if let Ok(_) = rx.try_recv() {
            panic!("should not receive value");
        }

        // construct a raft message with valid store id and send it to server
        let msg_2 = new_raft_message(valid_store_id);
        rpc::encode_msg(&mut sock, 1u64, &msg_2).unwrap();

        // check the raft message is delivered to the underlying store
        let rid = rx.recv().unwrap();
        assert_eq!(rid, valid_store_id);

        // tear down the test case
        ch.send(Msg::Quit).unwrap();
        h.join().unwrap();
    }
}
