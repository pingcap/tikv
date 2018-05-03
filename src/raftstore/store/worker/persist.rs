// Copyright 2018 PingCAP, Inc.
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

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::Instant;

use kvproto::metapb;
use prometheus::local::LocalHistogram;
use raft::Ready;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{WriteBatch, DB};

use raftstore::store::msg::Msg;
use raftstore::store::peer_storage::InvokeContext;
use util::time::duration_to_sec;
use util::transport::SendCh;
use util::worker::Runnable;

use super::metrics::ASYNC_PERSIST;

pub enum Task {
    Persist {
        kv_wb: WriteBatch,
        raft_wb: WriteBatch,
        persist: Vec<(Ready, InvokeContext)>,
        sync_log: bool,

        timer: Instant,
    },
    Destory {
        region_id: u64,
        peer: metapb::Peer,
        keep_data: bool,
    },
}

impl Task {
    pub fn persist(
        kv_wb: WriteBatch,
        raft_wb: WriteBatch,
        persist: Vec<(Ready, InvokeContext)>,
        sync_log: bool,
    ) -> Task {
        let timer = Instant::now();
        Task::Persist {
            kv_wb,
            raft_wb,
            persist,
            sync_log,
            timer,
        }
    }

    pub fn destory(region_id: u64, peer: metapb::Peer, keep_data: bool) -> Task {
        Task::Destory {
            region_id,
            peer,
            keep_data,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Persist { .. } => write!(f, "Persist"),
            Task::Destory {
                region_id,
                ref peer,
                ..
            } => f.debug_struct("Destory")
                .field("region_id", &region_id)
                .field("peer", &peer)
                .finish(),
        }
    }
}

pub enum TaskRes {
    Persist {
        append_res: Vec<(Ready, InvokeContext)>,
        timer: Instant,
    },
    Destory {
        region_id: u64,
        peer: metapb::Peer,
        keep_data: bool,
    },
}

impl Display for TaskRes {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            TaskRes::Persist { .. } => write!(f, "Persist"),
            TaskRes::Destory {
                region_id,
                ref peer,
                ..
            } => f.debug_struct("Destory")
                .field("region_id", &region_id)
                .field("peer", &peer)
                .finish(),
        }
    }
}

pub struct Runner {
    tag: String,
    kv_engine: Arc<DB>,
    raft_engine: Arc<DB>,
    ch: Sender<TaskRes>,
    notifier: SendCh<Msg>,
    sync_log: bool,

    wait_duration: LocalHistogram,
    write_duration: LocalHistogram,
}

impl Runner {
    pub fn new(
        store_id: u64,
        kv_engine: Arc<DB>,
        raft_engine: Arc<DB>,
        ch: Sender<TaskRes>,
        notifier: SendCh<Msg>,
        sync_log: bool,
    ) -> Runner {
        let tag = format!("store {}", store_id);
        Runner {
            tag,
            kv_engine,
            raft_engine,
            ch,
            notifier,
            sync_log,
            wait_duration: ASYNC_PERSIST.with_label_values(&["wait"]).local(),
            write_duration: ASYNC_PERSIST.with_label_values(&["write"]).local(),
        }
    }

    fn handle_persist(
        &mut self,
        kv_wb: WriteBatch,
        raft_wb: WriteBatch,
        persist: Vec<(Ready, InvokeContext)>,
        sync_log: bool,
        timer: Instant,
    ) {
        self.wait_duration.observe(duration_to_sec(timer.elapsed()));
        let start = Instant::now();
        // apply_snapshot, peer_destroy will clear_meta, so we need write region state first.
        // otherwise, if program restart between two write, raft log will be removed,
        // but region state may not changed in disk.
        fail_point!("raft_before_save");
        if !kv_wb.is_empty() {
            // RegionLocalState, ApplyState
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.kv_engine
                .write_opt(kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_between_save");
        if !raft_wb.is_empty() {
            // RaftLocalState, Raft Log Entry
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.sync_log || sync_log);
            self.raft_engine
                .write_opt(raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_after_save");
        self.write_duration
            .observe(duration_to_sec(start.elapsed()));

        self.ch
            .send(TaskRes::Persist {
                append_res: persist,
                timer,
            })
            .unwrap();
    }

    fn kick(&self) {
        if let Err(e) = self.notifier.send(Msg::Kick) {
            error!("fail to kick raftstore: {:?}", e);
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Persist {
                kv_wb,
                raft_wb,
                persist,
                sync_log,
                timer,
            } => self.handle_persist(kv_wb, raft_wb, persist, sync_log, timer),
            Task::Destory {
                region_id,
                peer,
                keep_data,
            } => self.ch
                .send(TaskRes::Destory {
                    region_id,
                    peer,
                    keep_data,
                })
                .unwrap(),
        }
        self.kick();
    }

    fn on_tick(&mut self) {
        self.wait_duration.flush();
        self.write_duration.flush();
    }
}
