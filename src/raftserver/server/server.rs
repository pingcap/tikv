use std::collections::HashMap;
use std::option::Option;
use std::sync::{Arc, RwLock};
use std::vec::Vec;
use std::thread;
use std::boxed::Box;
use std::net::SocketAddr;

use rocksdb::DB;
use mio::{Token, Handler, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;

use raftserver::{Result, other};
use raftserver::store::cmd_resp;
use kvproto::raft_serverpb::{Message, MessageType};
use kvproto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};
use super::{Msg, SendCh, ConnData};
use super::conn::Conn;
use super::config::Config;
use super::transport::ServerTransport;
use super::node::Node;
use pd::Client as PdClient;
use util::HandyRwLock;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const INVALID_TOKEN: Token = Token(0);

pub struct Server<T: PdClient + 'static> {
    cfg: Config,

    listener: TcpListener,
    conns: Slab<Conn>,
    sendch: SendCh,

    peers: HashMap<String, Token>,

    store_handles: HashMap<u64, thread::JoinHandle<()>>,

    trans: Arc<RwLock<ServerTransport<T>>>,

    node: Node<T, ServerTransport<T>>,
}

pub fn create_event_loop<T: PdClient + 'static>() -> Result<EventLoop<Server<T>>> {
    let event_loop = try!(EventLoop::new());
    Ok(event_loop)
}

impl<T: PdClient + 'static> Server<T> {
    // Create a server with already initialized engines.
    // We must bootstrap all stores before running the server.
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: Config,
               engines: Vec<Arc<DB>>,
               pd_client: Arc<RwLock<T>>)
               -> Result<Server<T>> {
        try!(cfg.validate());


        let addr = try!((&cfg.addr).parse());
        let listener = try!(TcpListener::bind(&addr));

        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel());

        let max_conn_capacity = cfg.max_conn_capacity;
        let trans = Arc::new(RwLock::new(ServerTransport::new(cfg.cluster_id,
                                                              sendch.clone(),
                                                              pd_client.clone())));

        let node = Node::new(&cfg, pd_client, trans.clone());

        let mut svr = Server {
            cfg: cfg,
            listener: listener,
            sendch: sendch,
            conns: Slab::new_starting_at(FIRST_CUSTOM_TOKEN, max_conn_capacity),
            peers: HashMap::new(),
            store_handles: HashMap::new(),
            trans: trans,
            node: node,
        };

        try!(svr.node.start(engines));

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_node_id(&self) -> u64 {
        self.node.get_node_id()
    }

    pub fn get_sendch(&self) -> SendCh {
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
        let conn = self.conns.remove(token);
        match conn {
            Some(conn) => {
                // if connected to remote peer, remove this too.
                if let Some(addr) = conn.peer_addr {
                    self.peers.remove(&addr);
                }

                if let Err(e) = event_loop.deregister(&conn.sock) {
                    error!("deregister conn err {:?}", e);
                }
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Self>,
                    sock: TcpStream,
                    peer_addr: Option<String>)
                    -> Result<Token> {
        try!(sock.set_nodelay(true));

        // TODO: now slab crate doesn't support insert_with_opt, we should use it instead later.
        self.conns
            .insert_with(|new_token: Token| -> Conn {
                // TODO: if insert_with_opt used, we will return None for register error.
                // Now, just panic for this.
                event_loop.register(&sock,
                                    new_token,
                                    EventSet::readable() | EventSet::hup(),
                                    PollOpt::edge())
                          .unwrap();

                Conn::new(sock, new_token, peer_addr)
            })
            .ok_or_else(|| other("add new connection failed"))
    }

    fn handle_conn_readable(&mut self,
                            event_loop: &mut EventLoop<Self>,
                            token: Token)
                            -> Result<()> {
        let msgs = try!(match self.conns.get_mut(token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return Ok(());
            }
            Some(conn) => conn.read(event_loop),
        });

        if msgs.is_empty() {
            // Read no message, no need to handle.
            return Ok(());
        }

        let mut res = vec![];
        for msg in msgs {
            let resp = try!(self.handle_conn_msg(token, msg));
            if let Some(resp) = resp {
                res.push(resp);
            }

        }

        if res.is_empty() {
            return Ok(());
        }

        // append to write buffer here, no need using sender to notify.
        if let Some(conn) = self.conns.get_mut(token) {
            for data in res {
                try!(conn.append_write_buf(event_loop, data));
            }
        }

        Ok(())
    }

    fn handle_conn_msg(&mut self, token: Token, data: ConnData) -> Result<Option<ConnData>> {
        let msg_id = data.msg_id;
        let mut msg = data.msg;

        let msg_type = msg.get_msg_type();
        let res = try!(match msg_type {
            MessageType::Raft => {
                if let Err(e) = self.trans.rl().send_raft_msg(msg.take_raft()) {
                    // Should we return error to let outer close this connection later?
                    error!("send raft message for token {:?} with msg id {} err {:?}",
                           token,
                           msg_id,
                           e);
                }
                Ok(None)
            }
            MessageType::Command => self.handle_conn_command(token, msg_id, msg.take_cmd_req()),
            MessageType::CommandResp => {
                // Now we have no way to handle CommandResp type,
                // so log an error here and do nothing.
                error!("unsupported command response msg {:?} for token {:?} with msg id {}",
                       msg,
                       token,
                       msg_id);
                Ok(None)
            }
            _ => {
                Err(other(format!("unsupported message {:?} for token {:?} with msg id {}",
                                  msg_type,
                                  token,
                                  msg_id)))
            }

        });

        Ok(res)
    }

    fn handle_conn_command(&mut self,
                           token: Token,
                           msg_id: u64,
                           msg: RaftCommandRequest)
                           -> Result<Option<ConnData>> {
        let ch = self.sendch.clone();
        let cb = Box::new(move |resp: RaftCommandResponse| -> Result<()> {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CommandResp);
            resp_msg.set_cmd_resp(resp);
            // Use send channel to let server return the
            // response to the specified connection with token.
            ch.write_data(token, ConnData::new(msg_id, resp_msg))
        });

        let uuid = msg.get_header().get_uuid().to_vec();
        if let Err(e) = self.trans.rl().send_command(msg, cb) {
            // send error, reply an error response.
            warn!("send command for token {:?} with msg id {} err {:?}",
                  token,
                  msg_id,
                  e);
            let mut resp = cmd_resp::new_error(e);
            resp.mut_header().set_uuid(uuid);
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CommandResp);
            resp_msg.set_cmd_resp(resp);
            return Ok(Some(ConnData::new(msg_id, resp_msg)));
        }

        Ok(None)
    }


    fn handle_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
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
                if let Err(e) = self.handle_conn_readable(event_loop, token) {
                    warn!("handle read conn for token {:?} err {:?}, remove", token, e);
                    self.remove_conn(event_loop, token);
                }
            }

        }
    }

    fn handle_writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let res = match self.conns.get_mut(token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.write(event_loop),
        };

        if let Err(e) = res {
            warn!("handle write conn err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn handle_writedata(&mut self,
                        event_loop: &mut EventLoop<Self>,
                        token: Token,
                        data: ConnData) {
        let res = match self.conns.get_mut(token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.append_write_buf(event_loop, data),
        };

        if let Err(e) = res {
            warn!("handle write data err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn connect_peer(&mut self, event_loop: &mut EventLoop<Self>, addr: &str) -> Result<Token> {
        let peer_addr = try!(addr.parse());
        let sock = try!(TcpStream::connect(&peer_addr));
        let token = try!(self.add_new_conn(event_loop, sock, Some(addr.to_string())));
        self.peers.insert(addr.to_owned(), token);
        Ok(token)
    }

    fn handle_sendpeer(&mut self, event_loop: &mut EventLoop<Self>, addr: String, data: ConnData) {
        // check the corresponding token for peer address.
        let mut token = self.peers.get(&addr).map_or(INVALID_TOKEN, |t| *t);

        if token == INVALID_TOKEN {
            match self.connect_peer(event_loop, &addr) {
                Err(e) => {
                    error!("connect {:?} err {:?}", addr, e);
                    return;
                }
                Ok(new_token) => token = new_token,
            }
        }

        self.handle_writedata(event_loop, token, data);
    }
}

impl<T: PdClient + 'static> Handler for Server<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.handle_readable(event_loop, token);
        }

        if events.is_writable() {
            self.handle_writable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData{token, data} => self.handle_writedata(event_loop, token, data),
            Msg::SendPeer{addr, data} => self.handle_sendpeer(event_loop, addr, data),
            _ => panic!("unexpected msg"),
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Self>, _: Msg) {
        // nothing to do now.
    }

    fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
        event_loop.shutdown();
    }

    fn tick(&mut self, _: &mut EventLoop<Self>) {
        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        // TODO: handle quit server if event_loop is_running() returns false.
    }
}
