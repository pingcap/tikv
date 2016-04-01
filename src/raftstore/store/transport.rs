use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::RaftCmdRequest;

use raftstore::Result;
use super::Callback;
use super::msg::SendCh;


#[derive(Clone)]
pub struct StoreSendCh {
    pub store_id: u64,
    pub ch: SendCh,
}

impl StoreSendCh {
    pub fn new(store_id: u64, ch: SendCh) -> StoreSendCh {
        StoreSendCh {
            store_id: store_id,
            ch: ch,
        }
    }
}

// Transports message between different raft peers.
pub trait Transport : Send + Sync {
    // For transporting message with store send channel.
    // TODO: we may remove these to another trait or structure later.
    fn set_sendch(&mut self, StoreSendCh);
    fn remove_sendch(&mut self) -> Option<StoreSendCh>;

    // Send RaftCmdRequest to specified store, the store must exist in current node.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> Result<()>;

    fn send(&self, msg: RaftMessage) -> Result<()>;
}
