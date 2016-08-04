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

use kvproto::raft_serverpb::RaftMessage;
use kvproto::eraftpb::MessageType;
use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::{Msg as StoreMsg, Transport};
use tikv::server::transport::*;
use tikv::raft::SnapshotStatus;
use tikv::util::HandyRwLock;

use rand;
use std::sync::{Arc, RwLock, Mutex};
use std::time;
use std::usize;
use std::thread;
use std::vec::Vec;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub trait Channel<M>: Send + Clone {
    fn send(&self, m: M) -> Result<()>;
}

impl Channel<RaftMessage> for ServerTransport {
    fn send(&self, m: RaftMessage) -> Result<()> {
        Transport::send(self, m)
    }
}

impl Channel<StoreMsg> for ServerRaftStoreRouter {
    fn send(&self, m: StoreMsg) -> Result<()> {
        RaftStoreRouter::send(self, m)
    }
}

pub trait Filter<M>: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, _: &mut Vec<M>);
    // with after provided, one can change the return value arbitrarily
    fn after(&self, res: Result<()>) -> Result<()> {
        res
    }
}

pub type SendFilter = Box<Filter<RaftMessage>>;
pub type RecvFilter = Box<Filter<StoreMsg>>;

struct FilterDropPacket {
    rate: u32,
}

struct FilterDelay {
    duration: time::Duration,
}

impl<M> Filter<M> for FilterDropPacket {
    fn before(&self, msgs: &mut Vec<M>) {
        msgs.retain(|_| rand::random::<u32>() % 100u32 >= self.rate)
    }
}

impl<M> Filter<M> for FilterDelay {
    fn before(&self, _: &mut Vec<M>) {
        thread::sleep(self.duration);
    }
}

pub struct SimulateTransport<M, C: Channel<M>> {
    filters: Arc<RwLock<Vec<Box<Filter<M>>>>>,
    ch: C,
}

impl<M, C: Channel<M>> SimulateTransport<M, C> {
    pub fn new(ch: C) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: Arc::new(RwLock::new(vec![])),
            ch: ch,
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<Filter<M>>) {
        self.filters.wl().push(filter);
    }
}

impl<M, C: Channel<M>> Channel<M> for SimulateTransport<M, C> {
    fn send(&self, msg: M) -> Result<()> {
        let mut taken = 0;
        let mut msgs = vec![msg];
        let filters = self.filters.rl();
        for filter in filters.iter() {
            filter.before(&mut msgs);
            taken += 1;
            if msgs.is_empty() {
                break;
            }
        }
        let mut res = Ok(());
        if msgs.is_empty() {
            res = Err(Error::Timeout("drop by in SimulateTransport".to_owned()))
        } else {
            for msg in msgs {
                res = self.ch.send(msg);
                if res.is_err() {
                    break;
                }
            }
        }
        for filter in filters[..taken].iter().rev() {
            res = filter.after(res);
        }
        res
    }
}

impl<M, C: Channel<M>> Clone for SimulateTransport<M, C> {
    fn clone(&self) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: self.filters.clone(),
            ch: self.ch.clone(),
        }
    }
}

impl<C: Channel<RaftMessage>> Transport for SimulateTransport<RaftMessage, C> {
    fn send(&self, m: RaftMessage) -> Result<()> {
        Channel::send(self, m)
    }
}

impl<C: Channel<StoreMsg>> RaftStoreRouter for SimulateTransport<StoreMsg, C> {
    fn send(&self, m: StoreMsg) -> Result<()> {
        Channel::send(self, m)
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter>;
}

pub struct DropPacket {
    rate: u32,
}

impl DropPacket {
    pub fn new(rate: u32) -> DropPacket {
        DropPacket { rate: rate }
    }
}

impl FilterFactory for DropPacket {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![box FilterDropPacket { rate: self.rate }]
    }
}

pub struct Delay {
    duration: time::Duration,
}

impl Delay {
    pub fn new(duration: time::Duration) -> Delay {
        Delay { duration: duration }
    }
}

impl FilterFactory for Delay {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![box FilterDelay { duration: self.duration }]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
}

impl Filter<RaftMessage> for PartitionFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| !self.node_ids.contains(&m.get_to_peer().get_store_id()));
    }
}

pub struct Partition {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl Partition {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> Partition {
        Partition { s1: s1, s2: s2 }
    }
}

impl FilterFactory for Partition {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if self.s1.contains(&node_id) {
            return vec![box PartitionFilter { node_ids: self.s2.clone() }];
        }
        return vec![box PartitionFilter { node_ids: self.s1.clone() }];
    }
}

pub struct Isolate {
    node_id: u64,
}

impl Isolate {
    pub fn new(node_id: u64) -> Isolate {
        Isolate { node_id: node_id }
    }
}

impl FilterFactory for Isolate {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if node_id == self.node_id {
            return vec![box FilterDropPacket { rate: 100 }];
        }
        vec![box PartitionFilter { node_ids: vec![self.node_id] }]
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    Recv,
    Send,
    Both,
}

impl Direction {
    pub fn is_recv(&self) -> bool {
        match *self {
            Direction::Recv | Direction::Both => true,
            Direction::Send => false,
        }
    }

    pub fn is_send(&self) -> bool {
        match *self {
            Direction::Send | Direction::Both => true,
            Direction::Recv => false,
        }
    }
}

/// Drop specified messages for the store with special region.
///
/// If `msg_type` is None, all message will be filtered.
pub struct FilterRegionPacket {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    allow: AtomicUsize,
    msg_type: Option<MessageType>,
}

impl Filter<RaftMessage> for FilterRegionPacket {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| {
            let region_id = m.get_region_id();
            let from_store_id = m.get_from_peer().get_store_id();
            let to_store_id = m.get_to_peer().get_store_id();

            if self.region_id == region_id &&
               (self.direction.is_send() && self.store_id == from_store_id ||
                self.direction.is_recv() && self.store_id == to_store_id) &&
               self.msg_type.as_ref().map_or(true, |t| t == &m.get_message().get_msg_type()) {
                if self.allow.load(Ordering::Relaxed) > 0 {
                    self.allow.fetch_sub(1, Ordering::Relaxed);
                    return true;
                }
                return false;
            }
            true
        });
    }
}

pub struct IsolateRegionStore {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    allow: usize,
    msg_type: Option<MessageType>,
}

impl IsolateRegionStore {
    pub fn new(region_id: u64, store_id: u64) -> IsolateRegionStore {
        IsolateRegionStore {
            region_id: region_id,
            store_id: store_id,
            direction: Direction::Both,
            msg_type: None,
            allow: 0,
        }
    }

    pub fn direction(mut self, direction: Direction) -> IsolateRegionStore {
        self.direction = direction;
        self
    }

    pub fn msg_type(mut self, m_type: MessageType) -> IsolateRegionStore {
        self.msg_type = Some(m_type);
        self
    }

    pub fn allow(mut self, number: usize) -> IsolateRegionStore {
        self.allow = number;
        self
    }
}

impl FilterFactory for IsolateRegionStore {
    fn generate(&self, _: u64) -> Vec<Box<Filter<RaftMessage>>> {
        vec![box FilterRegionPacket {
                 region_id: self.region_id,
                 store_id: self.store_id,
                 direction: self.direction,
                 msg_type: self.msg_type.clone(),
                 allow: AtomicUsize::new(self.allow),
             }]
    }
}

struct SnapshotFilter {
    drop: AtomicBool,
}

impl Filter<RaftMessage> for SnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) {
        msgs.retain(|m| m.get_message().get_msg_type() != MessageType::MsgSnapshot);
        self.drop.store(msgs.is_empty(), Ordering::Relaxed);
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            Ok(())
        } else {
            x
        }
    }
}

pub struct DropSnapshot;

impl FilterFactory for DropSnapshot {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![box SnapshotFilter { drop: AtomicBool::new(false) }]
    }
}

/// Pause Snap
pub struct PauseFirstSnap {
    dropped: AtomicBool,
    stale: AtomicBool,
    pending_msg: Mutex<Vec<StoreMsg>>,
}

impl PauseFirstSnap {
    pub fn new() -> PauseFirstSnap {
        PauseFirstSnap {
            dropped: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            pending_msg: Mutex::new(vec![]),
        }
    }
}

impl Filter<StoreMsg> for PauseFirstSnap {
    fn before(&self, msgs: &mut Vec<StoreMsg>) {
        if self.stale.load(Ordering::Relaxed) {
            return;
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        for m in msgs.drain(..) {
            let paused = match m {
                StoreMsg::ReportSnapshot { ref status, .. } => *status == SnapshotStatus::Finish,
                StoreMsg::RaftMessage(ref msg) => {
                    msg.get_message().get_msg_type() == MessageType::MsgSnapshot
                }
                _ => false,
            };
            if paused {
                self.dropped.compare_and_swap(false, true, Ordering::Relaxed);
                pending_msg.push(m);
            } else {
                to_send.push(m);
            }
        }
        if pending_msg.len() > 1 {
            self.dropped.compare_and_swap(true, false, Ordering::Relaxed);
            msgs.extend(pending_msg.drain(..));
            self.stale.compare_and_swap(false, true, Ordering::Relaxed);
        }
        msgs.extend(to_send);
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        if res.is_err() && self.dropped.load(Ordering::Relaxed) {
            self.dropped.compare_and_swap(true, false, Ordering::Relaxed);
            Ok(())
        } else {
            res
        }
    }
}
