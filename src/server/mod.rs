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

use std::thread;
use std::time::Duration;
use std::net::SocketAddr;

use bytes::ByteBuf;
use mio::{self, Token, NotifyError};
use protobuf::Message;

use kvproto::msgpb;
use util::codec::rpc;

pub mod config;
pub mod errors;
pub mod server;
mod conn;
mod kv;
pub mod coprocessor;
pub mod transport;
pub mod node;
pub mod resolve;

pub use self::config::{Config, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Result, Error};
pub use self::server::{Server, create_event_loop, bind};
pub use self::transport::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
pub use self::node::{Node, create_raft_storage};
pub use self::resolve::{StoreAddrResolver, PdStoreAddrResolver, MockStoreAddrResolver};

const MAX_SEND_RETRY_CNT: i32 = 20;

// send_msg wraps Sender and retries some times if queue is full.
pub fn send_msg<M: Send>(ch: &mio::Sender<M>, mut msg: M) -> Result<()> {
    for _ in 0..MAX_SEND_RETRY_CNT {
        let r = ch.send(msg);
        if r.is_ok() {
            return Ok(());
        }

        match r.unwrap_err() {
            NotifyError::Full(m) => {
                warn!("notify queue is full, sleep and retry");
                thread::sleep(Duration::from_millis(100));
                msg = m;
                continue;
            }
            e => {
                return Err(box_err!("{:?}", e));
            }
        }
    }

    // TODO: if we refactor with quick_error, we can use NotifyError instead later.
    Err(box_err!("notify channel is full"))
}


pub struct ConnData {
    msg_id: u64,
    msg: msgpb::Message,
}

impl ConnData {
    pub fn new(msg_id: u64, msg: msgpb::Message) -> ConnData {
        ConnData {
            msg_id: msg_id,
            msg: msg,
        }
    }

    pub fn encode_to_buf(&self) -> ByteBuf {
        let mut buf = ByteBuf::mut_with_capacity(rpc::MSG_HEADER_LEN +
                                                 self.msg.compute_size() as usize);

        // Must ok here
        rpc::encode_msg(&mut buf, self.msg_id, &self.msg).unwrap();

        buf.flip()
    }
}

pub enum Msg {
    // Quit event loop.
    Quit,
    // Write data to connection.
    WriteData {
        token: Token,
        data: ConnData,
    },
    // Send data to remote store.
    SendStore {
        store_id: u64,
        data: ConnData,
    },
    // Send data to remote peer with parsed socket address.
    SendStoreSock {
        store_id: u64,
        sock_addr: SocketAddr,
        data: ConnData,
    },
}

#[derive(Debug)]
pub struct SendCh {
    ch: mio::Sender<Msg>,
}

impl Clone for SendCh {
    fn clone(&self) -> SendCh {
        SendCh { ch: self.ch.clone() }
    }
}

impl SendCh {
    pub fn new(ch: mio::Sender<Msg>) -> SendCh {
        SendCh { ch: ch }
    }

    pub fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.ch, msg));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use mio::{EventLoop, Handler};

    use super::*;

    struct SenderHandler;

    impl Handler for SenderHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<SenderHandler>, msg: Msg) {
            if let Msg::Quit = msg {
                event_loop.shutdown()
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let ch = SendCh::new(event_loop.channel());
        let h = thread::spawn(move || {
            event_loop.run(&mut SenderHandler).unwrap();
        });

        ch.send(Msg::Quit).unwrap();

        h.join().unwrap();
    }
}
