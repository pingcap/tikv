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

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::collections::HashSet;
use util::codec::rpc;
use util::make_std_tcp_conn;

use rand::{self, Rng};

use kvproto::pdpb::{self, Request, Response};
use kvproto::msgpb::{Message, MessageType};

use super::{Result, protocol};
use super::metrics::*;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

const PD_RPC_PREFIX: &'static str = "/pd/rpc";

// Only for `validate_endpoints`.
const VALIDATE_MSG_ID: u64 = 0;
const VALIDATE_CLUSTER_ID: u64 = 0;

#[derive(Debug)]
struct RpcClientCore {
    endpoints: Vec<String>,
    stream: Option<TcpStream>,
}

fn send_msg(stream: &mut TcpStream, msg_id: u64, message: &Request) -> Result<(u64, Response)> {
    let timer = PD_SEND_MSG_HISTOGRAM.start_timer();

    let mut req = Message::new();

    req.set_msg_type(MessageType::PdReq);
    // TODO: optimize clone later in HTTP refactor.
    req.set_pd_req(message.clone());

    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, &req));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let mut resp = Message::new();
    let id = try!(rpc::decode_msg(stream, &mut resp));
    if resp.get_msg_type() != MessageType::PdResp {
        return Err(box_err!("invalid pd response type {:?}", resp.get_msg_type()));
    }
    timer.observe_duration();

    Ok((id, resp.take_pd_resp()))
}

fn rpc_connect(endpoint: &str) -> Result<TcpStream> {
    let mut stream = try!(make_std_tcp_conn(endpoint));
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));

    // Send a HTTP header to tell PD to hijack this connection for RPC.
    let header_str = format!("GET {} HTTP/1.0\r\n\r\n", PD_RPC_PREFIX);
    let header = header_str.as_bytes();
    match stream.write_all(header) {
        Ok(_) => Ok(stream),
        Err(err) => Err(box_err!("failed to connect to {} error: {:?}", endpoint, err)),
    }
}

impl RpcClientCore {
    fn new(endpoints: Vec<String>) -> RpcClientCore {
        RpcClientCore {
            endpoints: endpoints,
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        // Randomize endpoints.
        let len = self.endpoints.len();
        let mut indexes: Vec<usize> = (0..len).collect();
        rand::thread_rng().shuffle(&mut indexes);

        for i in indexes {
            let ep = &self.endpoints[i];
            match rpc_connect(ep.as_str()) {
                Ok(stream) => {
                    info!("PD client connects to {}", ep);
                    self.stream = Some(stream);
                    return Ok(());
                }

                Err(_) => {
                    error!("failed to connect to {}, try next", ep);
                    continue;
                }
            }
        }

        Err(box_err!("failed to connect to {:?}", self.endpoints))
    }

    fn send(&mut self, msg_id: u64, req: &Request) -> Result<Response> {
        // If we post failed, we should retry.
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            // If no stream, try connect first.
            if self.stream.is_none() && self.try_connect().is_err() {
                // TODO: figure out a better way to do backoff
                thread::sleep(Duration::from_millis(50));
                continue;
            }

            let mut stream = self.stream.take().unwrap();
            // We may send message to a not leader pd, retry.

            let (id, resp) = match send_msg(&mut stream, msg_id, req) {
                Err(e) => {
                    warn!("send message to pd failed {:?}", e);
                    // TODO: figure out a better way to do backoff
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Ok((id, resp)) => (id, resp),
            };

            if id != msg_id {
                return Err(box_err!("pd response msg_id not match, want {}, got {}", msg_id, id));
            }

            self.stream = Some(stream);

            return Ok(resp);
        }

        Err(box_err!("send message to pd failed"))
    }
}

#[derive(Debug)]
pub struct RpcClient {
    msg_id: AtomicUsize,
    core: Mutex<RpcClientCore>,
    pub cluster_id: u64,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let endpoints: Vec<String> = endpoints.split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();

        let mut cluster_id = VALIDATE_CLUSTER_ID;
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            match Self::validate_endpoints(&endpoints) {
                Ok(id) => {
                    cluster_id = id;
                    break;
                }
                Err(e) => {
                    warn!("failed to get cluster id from pd: {:?}", e);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }

        if cluster_id == VALIDATE_CLUSTER_ID {
            return Err(box_err!("failed to get cluster id from pd"));
        }

        Ok(RpcClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(RpcClientCore::new(endpoints)),
            cluster_id: cluster_id,
        })
    }

    pub fn send(&self, req: &Request) -> Result<Response> {
        let msg_id = self.alloc_msg_id();
        let resp = try!(self.core.lock().unwrap().send(msg_id, req));
        Ok(resp)
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }

    /// `validate_endpoints` validates pd members, make sure they are in the same cluster.
    /// It returns a cluster ID.
    /// Notice that it ignores failed pd nodes.
    /// Export for tests.
    pub fn validate_endpoints(endpoints: &[String]) -> Result<u64> {
        if endpoints.is_empty() {
            return Err(box_err!("empty PD endpoints"));
        }

        let len = endpoints.len();
        let mut endpoints_set = HashSet::with_capacity(len);

        let mut cluster_id = None;
        for ep in endpoints {
            if !endpoints_set.insert(ep) {
                return Err(box_err!("a duplicate PD url {}", ep));
            }

            let mut stream = match rpc_connect(ep.as_str()) {
                Ok(stream) => stream,
                // Ignore failed pd node.
                Err(_) => continue,
            };

            let mut req = protocol::new_request(VALIDATE_CLUSTER_ID,
                                                pdpb::CommandType::GetPDMembers);
            req.set_get_pd_members(pdpb::GetPDMembersRequest::new());
            let (mid, resp) = match send_msg(&mut stream, VALIDATE_MSG_ID, &req) {
                Ok((mid, resp)) => (mid, resp),
                // Ignore failed pd node.
                Err(_) => continue,
            };

            if mid != VALIDATE_MSG_ID {
                return Err(box_err!("PD response msg_id mismatch, want {}, got {}",
                                    VALIDATE_MSG_ID,
                                    mid));
            }

            // Check cluster ID.
            let cid = resp.get_header().get_cluster_id();
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(box_err!("PD response cluster_id mismatch, want {}, got {}",
                                        sample,
                                        cid));
                }
            } else {
                cluster_id = Some(cid);
            }
            // TODO: check all fields later?
        }

        cluster_id.ok_or(box_err!("PD cluster stop responding"))
    }
}
