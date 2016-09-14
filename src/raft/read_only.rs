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

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::eraftpb::Message;
use std::collections::{HashSet, HashMap};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

impl Default for ReadOnlyOption {
    fn default() -> ReadOnlyOption {
        ReadOnlyOption::Safe
    }
}

// ReadState provides state for read only query.
// It's caller's responsibility to send MsgReadIndex first before getting
// this state from ready. It's also caller's duty to differentiate if this
// state is what it requests through request_ctx, e.g. given a unique id as
// request_ctx.
#[derive(Default, Debug, PartialEq, Clone)]
pub struct ReadState {
    pub index: u64,
    pub request_ctx: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    pub acks: HashSet<u64>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: Vec<Vec<u8>>,
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly {
            option: option,
            pending_read_index: HashMap::new(),
            read_index_queue: vec![],
        }
    }

    /// add_request adds a read only request into readonly struct.
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    /// `m` is the original read only request message from the local or remote node.
    pub fn add_request(&mut self, index: u64, m: Message) {
        let ctx = m.get_entries()[0].get_data().to_vec();
        if self.pending_read_index.contains_key(&ctx) {
            return;
        }
        let status = ReadIndexStatus {
            req: m,
            index: index,
            acks: HashSet::new(),
        };
        self.pending_read_index.insert(ctx.clone(), status);
        self.read_index_queue.push(ctx);
    }

    /// rev_ack notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    pub fn recv_ack(&mut self, m: &Message) -> usize {
        match self.pending_read_index.get_mut(&m.get_context().to_vec()) {
            None => 0,
            Some(rs) => {
                rs.acks.insert(m.get_from());
                // add one to include an ack from local node
                rs.acks.len() + 1
            }
        }
    }

    /// advance advances the read only request queue kept by the ReadOnly struct.
    /// It dequeues the requests until it finds the read only request that has
    /// the same context as the given `m`.
    pub fn advance(&mut self, m: &Message) -> Vec<ReadIndexStatus> {
        let mut i = 0;
        let mut found = false;
        let ctx = m.get_context().to_vec();

        for c in &self.read_index_queue {
            i += 1;
            if !self.pending_read_index.contains_key(c) {
                panic!("cannot find correspond read state from pending map");
            }
            if *c == ctx {
                found = true;
                break;
            }
        }

        let mut rss = vec![];
        if found {
            let to_keep = self.read_index_queue.split_off(i);
            for rs in &self.read_index_queue {
                match self.pending_read_index.remove(rs) {
                    Some(status) => rss.push(status),
                    None => panic!("cannot find correspond read state from pending map"),
                }
            }
            self.read_index_queue = to_keep;
        }
        rss
    }

    /// last_pending_request_ctx returns the context of the last pending read only
    /// request in ReadOnly struct.
    pub fn last_pending_request_ctx(&self) -> Option<Vec<u8>> {
        match self.read_index_queue.last() {
            None => None,
            Some(ctx) => Some(ctx.clone()),
        }
    }
}
