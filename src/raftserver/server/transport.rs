use std::collections::HashMap;
use std::option::Option;
use std::sync::{Arc, RwLock, Mutex};

use raftserver::store::{Transport, SendCh as StoreSendCh, Callback};
use raftserver::{Result, other};
use kvproto::raft_serverpb::{Message, MessageType, RaftMessage};
use kvproto::raft_cmdpb::RaftCommandRequest;
use pd::PdClient;
use util::HandyRwLock;
use super::{SendCh, ConnData};

pub struct ServerTransport<T: PdClient> {
    cluster_id: u64,
    stores: HashMap<u64, StoreSendCh>,

    pd_client: Arc<RwLock<T>>,
    ch: SendCh,
    msg_id: Mutex<u64>,
}

impl<T: PdClient> ServerTransport<T> {
    pub fn new(cluster_id: u64, ch: SendCh, pd_client: Arc<RwLock<T>>) -> ServerTransport<T> {
        ServerTransport {
            cluster_id: cluster_id,
            stores: HashMap::new(),
            pd_client: pd_client.clone(),
            ch: ch,
            msg_id: Mutex::new(0),
        }
    }

    // Send RaftMessage to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    pub fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        let ch = try!(self.get_sendch(to_store_id));

        ch.send_raft_msg(msg)
    }

    // Send RaftCommandRequest to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    pub fn send_command(&self, msg: RaftCommandRequest, cb: Callback) -> Result<()> {
        let to_store_id = msg.get_header().get_peer().get_store_id();
        let ch = try!(self.get_sendch(to_store_id));

        ch.send_command(msg, cb)
    }

    fn get_sendch(&self, store_id: u64) -> Result<&StoreSendCh> {
        match self.stores.get(&store_id) {
            None => {
                Err(other(format!("send message to invalid store {}, missing send \
                                   channel",
                                  store_id)))
            }

            Some(ch) => Ok(ch),
        }
    }
}

impl<T: PdClient> Transport for ServerTransport<T> {
    fn add_sendch(&mut self, store_id: u64, ch: StoreSendCh) {
        self.stores.insert(store_id, ch);
    }

    fn remove_sendch(&mut self, store_id: u64) -> Option<StoreSendCh> {
        self.stores.remove(&store_id)
    }

    fn send(&self, msg: RaftMessage) -> Result<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if let Some(ch) = self.stores.get(&to_store_id) {
            // use store send channel directly.
            return ch.send_raft_msg(msg);
        }

        let to_node_id = msg.get_to_peer().get_node_id();
        let node_meta = try!(self.pd_client.rl().get_node(self.cluster_id, to_node_id));

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        let mut id = self.msg_id.lock().unwrap();
        *id += 1;
        try!(self.ch.send_peer(node_meta.get_address().to_owned(), ConnData::new(*id, req)));

        Ok(())
    }
}
