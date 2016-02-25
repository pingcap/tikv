use std::collections::HashMap;
use std::boxed::Box;
use std::io::{Read, Write};

use mio::{self, Token, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};
use protobuf::RepeatedField;

use proto::kvrpcpb;
use proto::kvrpcpb::{CmdGetRequest, CmdGetResponse, CmdScanRequest, CmdScanResponse,
                     CmdPrewriteRequest, CmdPrewriteResponse, CmdCommitRequest, CmdCommitResponse,
                     Request, Response, MessageType};
use storage;
use storage::{Key, Storage, Value, KvPair};

use super::conn::Conn;

pub type Result<T> = ::std::result::Result<T, ServerError>;
pub type ResultStorage<T> = ::std::result::Result<T, storage::Error>;

// Token(1) instead of Token(0)
// See here: https://github.com/hjr3/mob/blob/multi-echo-blog-post/src/main.rs#L115
pub const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);

quick_error! {
    #[derive(Debug)]
    pub enum ServerError {
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
        FormatError(desc: String) {
            description(desc)
        }
    }
}

pub struct Server {
    pub listener: TcpListener,
    pub conns: HashMap<Token, Conn>,
    pub token_counter: usize,
    store: Storage,
}

impl Server {
    pub fn new(listener: TcpListener, conns: HashMap<Token, Conn>, store: Storage) -> Server {
        Server {
            listener: listener,
            conns: conns,
            token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            store: store,
        }
    }

    pub fn handle_get(&mut self,
                      msg: &Request,
                      msg_id: u64,
                      token: Token,
                      event_loop: &mut EventLoop<Server>)
                      -> Result<()> {
        if !msg.has_cmd_get_req() {
            error!("Msg doesn't contain a CmdGetRequest");
            return Err(ServerError::FormatError("Msg doesn't contain a CmdGetRequest".to_owned()));
        }
        let cmd_get_req: &CmdGetRequest = msg.get_cmd_get_req();
        let key = cmd_get_req.get_key().to_vec();
        let sender = event_loop.channel();
        let received_cb = Box::new(move |v: ResultStorage<Option<Value>>| {
            let resp: Response = Server::cmd_get_done(v);
            let queue_msg: QueueMessage = QueueMessage::Response(token, msg_id, resp);
            if let Err(e) = sender.send(queue_msg) {
                error!("{:?}", e);
            }
        });
        self.store
            .async_get(key, cmd_get_req.get_version(), received_cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_scan(&mut self,
                       msg: &Request,
                       msg_id: u64,
                       token: Token,
                       event_loop: &mut EventLoop<Server>)
                       -> Result<()> {
        if !msg.has_cmd_scan_req() {
            error!("Msg doesn't containe a CmdScanRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdScanRequest"
                                                    .to_owned()));
        }
        let cmd_scan_req: &CmdScanRequest = msg.get_cmd_scan_req();
        let sender = event_loop.channel();
        // convert [u8] to Vec[u8]
        let start_key: Key = cmd_scan_req.get_key().to_vec();
        debug!("start_key [{:?}]", start_key);
        let received_cb = Box::new(move |kvs: ResultStorage<Vec<ResultStorage<KvPair>>>| {
            let resp: Response = Server::cmd_scan_done(kvs);
            let queue_msg: QueueMessage = QueueMessage::Response(token, msg_id, resp);
            if let Err(e) = sender.send(queue_msg) {
                error!("{:?}", e);
            }
        });
        self.store
            .async_scan(start_key,
                        cmd_scan_req.get_limit() as usize,
                        cmd_scan_req.get_version(),
                        received_cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_prewrite(&mut self,
                           msg: &Request,
                           msg_id: u64,
                           token: Token,
                           event_loop: &mut EventLoop<Server>)
                           -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            error!("Msg doesn't containe a CmdPrewriteRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdPrewriteRequest"
                                                    .to_owned()));
        }
        let cmd_prewrite_req: &CmdPrewriteRequest = msg.get_cmd_prewrite_req();
        let sender = event_loop.channel();
        let puts: Vec<_> = cmd_prewrite_req.get_puts()
                                           .iter()
                                           .map(|kv| {
                                               (kv.get_key().to_vec(), kv.get_value().to_vec())
                                           })
                                           .collect();
        let deletes: Vec<_> = cmd_prewrite_req.get_dels().to_vec();
        let locks: Vec<_> = cmd_prewrite_req.get_locks().to_vec();
        let received_cb = Box::new(move |r: ResultStorage<()>| {
            let resp: Response = Server::cmd_prewrite_done(r);
            let queue_msg: QueueMessage = QueueMessage::Response(token, msg_id, resp);
            let _ = sender.send(queue_msg).map_err(|e| error!("{:?}", e));
        });
        self.store
            .async_prewrite(puts,
                            deletes,
                            locks,
                            cmd_prewrite_req.get_start_version(),
                            received_cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_commit(&mut self,
                         msg: &Request,
                         msg_id: u64,
                         token: Token,
                         event_loop: &mut EventLoop<Server>)
                         -> Result<()> {
        if !msg.has_cmd_commit_req() {
            error!("Msg doesn't contain a CmdCommitRequest");
            return Err(ServerError::FormatError("Msg doesn't contain a CmdCommitRequest"
                                                    .to_owned()));
        }
        let cmd_commit_req: &CmdCommitRequest = msg.get_cmd_commit_req();
        let sender = event_loop.channel();
        let received_cb = Box::new(move |r: ResultStorage<()>| {
            let resp: Response = Server::cmd_commit_done(r);
            let queue_msg: QueueMessage = QueueMessage::Response(token, msg_id, resp);
            // [TODO]: retry
            let _ = sender.send(queue_msg).map_err(|e| error!("{:?}", e));
        });
        self.store
            .async_commit(cmd_commit_req.get_start_version(),
                          cmd_commit_req.get_commit_version(),
                          received_cb)
            .map_err(ServerError::Storage)
    }

    fn cmd_get_done(r: ResultStorage<Option<Value>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_get_resp: CmdGetResponse = CmdGetResponse::new();
        cmd_get_resp.set_ok(r.is_ok());
        match r {
            Ok(Some(val)) => cmd_get_resp.set_value(val),
            Ok(None) => cmd_get_resp.set_value(Vec::new()),
            Err(e) => error!("storage error: {:?}", e),
        }
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }

    #[allow(single_match)]
    fn cmd_scan_done(kvs: ResultStorage<Vec<ResultStorage<KvPair>>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_scan_resp: CmdScanResponse = CmdScanResponse::new();
        cmd_scan_resp.set_ok(kvs.is_ok());
        match kvs {
            Ok(v) => {
                // convert storage::KvPair to kvrpcpb::KvPair
                let mut new_kvs: Vec<kvrpcpb::KvPair> = Vec::new();
                for result in v {
                    match result {
                        Ok((ref key, ref value)) => {
                            let mut new_kv: kvrpcpb::KvPair = kvrpcpb::KvPair::new();
                            new_kv.set_key(key.clone());
                            new_kv.set_value(value.clone());
                            new_kvs.push(new_kv);
                        }
                        Err(_) => {} // TODO(disksing): should send lock to client
                    }

                }
                cmd_scan_resp.set_results(RepeatedField::from_vec(new_kvs));
            }
            Err(e) => {
                error!("storage error: {:?}", e);
            }
        }
        resp.set_field_type(MessageType::CmdScan);
        resp.set_cmd_scan_resp(cmd_scan_resp);
        resp
    }

    fn cmd_prewrite_done(r: ResultStorage<()>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_prewrite_resp: CmdPrewriteResponse = CmdPrewriteResponse::new();
        cmd_prewrite_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdPrewrite);
        resp.set_cmd_prewrite_resp(cmd_prewrite_resp);
        resp
    }

    fn cmd_commit_done(r: ResultStorage<()>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_commit_resp: CmdCommitResponse = CmdCommitResponse::new();
        cmd_commit_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdCommit);
        resp.set_cmd_commit_resp(cmd_commit_resp);
        resp
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Server>,
                    sock: TcpStream)
                    -> Result<(Token)> {
        let new_token = Token(self.token_counter);
        let _ = sock.set_nodelay(true).map_err(|e| error!("set nodelay failed {:?}", e));
        self.conns.insert(new_token, Conn::new(sock, new_token));
        self.token_counter += 1;

        event_loop.register(&self.conns[&new_token].sock,
                            new_token,
                            EventSet::readable() | EventSet::hup(),
                            PollOpt::edge())
                  .unwrap();
        Ok(new_token)
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(conn) => {
                let _ = event_loop.deregister(&conn.sock);
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn handle_server_readable(&mut self, event_loop: &mut EventLoop<Server>) {
        loop {
            let sock = match self.listener.accept() {
                // Some(sock, addr)
                Ok(Some((sock, _))) => sock,
                Ok(None) => {
                    debug!("no connection, accept later.");
                    return;
                }
                Err(e) => {
                    error!("accept error: {}", e);
                    return;
                }
            };
            let _ = self.add_new_conn(event_loop, sock);
        }
    }

    fn handle_conn_readable(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        if let Err(e) = conn.read(event_loop) {
            error!("read failed with {:?}", e);
        }
    }

    fn handle_writable(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        if let Err(e) = conn.write(event_loop) {
            error!("write failed with {:?}", e);
        }
    }

    fn handle_request(&mut self,
                      event_loop: &mut EventLoop<Server>,
                      token: Token,
                      msg_id: u64,
                      req: Request) {
        debug!("notify Request token[{}] msg_id[{}] type[{:?}]",
               token.0,
               msg_id,
               req.get_field_type());
        if let Err(e) = match req.get_field_type() {
            MessageType::CmdGet => self.handle_get(&req, msg_id, token, event_loop),
            MessageType::CmdScan => self.handle_scan(&req, msg_id, token, event_loop),
            MessageType::CmdPrewrite => self.handle_prewrite(&req, msg_id, token, event_loop),
            MessageType::CmdCommit => self.handle_commit(&req, msg_id, token, event_loop),
        } {
            error!("Some error occur err[{:?}]", e);
        }
    }

    fn handle_response(&mut self,
                       event_loop: &mut EventLoop<Server>,
                       token: Token,
                       msg_id: u64,
                       resp: Response) {

        debug!("notify Response token[{}] msg_id[{}] type[{:?}]",
               token.0,
               msg_id,
               resp.get_field_type());
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        let _ = conn.append_write_buf(event_loop, msg_id, resp);
    }
}

#[allow(dead_code)]
pub enum QueueMessage {
    // Request(token, msg_id, kvrpc_request)
    Request(Token, u64, Request),
    // Request(token, msg_id, kvrpc_response)
    Response(Token, u64, Response),
    Quit,
}

impl mio::Handler for Server {
    type Timeout = ();
    type Message = QueueMessage;

    fn ready(&mut self, event_loop: &mut EventLoop<Server>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            match token {
                SERVER_TOKEN => {
                    self.handle_server_readable(event_loop);
                }
                token => {
                    self.handle_conn_readable(event_loop, token);
                }
            }
        }

        if events.is_writable() {
            self.handle_writable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server>, msg: QueueMessage) {
        match msg {
            QueueMessage::Request(token, msg_id, req) => {
                self.handle_request(event_loop, token, msg_id, req);
            }
            QueueMessage::Response(token, msg_id, resp) => {
                self.handle_response(event_loop, token, msg_id, resp);
            }
            QueueMessage::Quit => event_loop.shutdown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use mio::EventLoop;

    use proto::kvrpcpb::*;
    use storage::{Key, Value, Storage, Dsn};
    use storage::Error::Other;
    use storage::KvPair as StorageKV;
    use super::*;

    #[test]
    fn test_quit() {
        use std::collections::HashMap;
        use mio::tcp::TcpListener;
        let mut event_loop = EventLoop::new().unwrap();
        let sender = event_loop.channel();
        let h = thread::spawn(move || {
            let l: TcpListener = TcpListener::bind(&"127.0.0.1:64321".parse().unwrap()).unwrap();
            let store: Storage = Storage::new(Dsn::Memory).unwrap();
            let mut srv: Server = Server::new(l, HashMap::new(), store);
            event_loop.run(&mut srv).unwrap();
        });
        // Without this thread will be hang.
        let _ = sender.send(QueueMessage::Quit);
        h.join().unwrap();
    }

    #[test]
    fn test_get_done_none() {
        let actual_resp: Response = Server::cmd_get_done(Ok(None));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_cmd_resp.set_value(Vec::new());
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_get_done_some() {
        let storage_val: Vec<_> = vec![0u8; 8];
        let actual_resp: Response = Server::cmd_get_done(Ok(Some(storage_val)));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_cmd_resp.set_value(vec![0u8; 8]);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    // #[should_panic]
    fn test_get_done_error() {
        let actual_resp: Response = Server::cmd_get_done(Err(Other(Box::new("error"))));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_ok(false);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_empty() {
        let actual_resp: Response = Server::cmd_scan_done(Ok(Vec::new()));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdScanResponse = CmdScanResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_resp.set_field_type(MessageType::CmdScan);
        exp_resp.set_cmd_scan_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_some() {
        let k0: Key = vec![0u8, 0u8];
        let v0: Value = vec![255u8, 255u8];
        let k1: Key = vec![0u8, 1u8];
        let v1: Value = vec![255u8, 254u8];
        let kvs: Vec<StorageKV> = vec![(k0.clone(), v0.clone()), (k1.clone(), v1.clone())];
        let actual_resp: Response = Server::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp: &CmdScanResponse = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(v1, actual_kvs[1].get_value());
    }

    #[test]
    fn test_prewrite_done_ok() {
        let actual_resp: Response = Server::cmd_prewrite_done(Ok(()));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_prewrite_done_err() {
        let err = Other(Box::new("prewrite error"));
        let actual_resp: Response = Server::cmd_prewrite_done(Err(err));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_commit_done_ok() {
        let actual_resp: Response = Server::cmd_commit_done(Ok(()));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_commit_done_err() {
        let err = Other(Box::new("commit error"));
        let actual_resp: Response = Server::cmd_commit_done(Err(err));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_commit_resp().get_ok());
    }
}
