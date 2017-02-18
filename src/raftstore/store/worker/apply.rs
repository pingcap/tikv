// Copyright 2017 PingCAP, Inc.
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


use std::sync::Arc;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::fmt::{self, Display, Formatter};
use std::collections::hash_map::Entry as MapEntry;

use rocksdb::DB;
use uuid::Uuid;

use kvproto::metapb::Region;
use kvproto::eraftpb::Entry;
use kvproto::raft_serverpb::RaftApplyState;

use util::worker::Runnable;
use raftstore::store::Store;
use raftstore::store::msg::Callback;
use raftstore::store::peer::{self, Peer, ExecResult, ApplyDelegate, PendingCmd,
                             notify_stale_command};

pub struct Apply {
    region_id: u64,
    term: u64,
    entries: Vec<Entry>,
}

pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub region: Region,
}

impl Registration {
    pub fn new(peer: &Peer) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state.clone(),
            applied_index_term: peer.get_store().applied_index_term,
            region: peer.region().clone(),
        }
    }
}

pub struct Propose {
    id: u64,
    region_id: u64,
    is_conf_change: bool,
    uuid: Uuid,
    term: u64,
    cb: Callback,
}

pub struct Destroy {
    region_id: u64,
}

/// region related task.
pub enum Task {
    Apply(Apply),
    Registration(Registration),
    Propose(Propose),
    Destroy(Destroy),
}

impl Task {
    pub fn apply(region_id: u64, term: u64, entries: Vec<Entry>) -> Task {
        Task::Apply(Apply {
            region_id: region_id,
            term: term,
            entries: entries,
        })
    }

    pub fn register(peer: &Peer) -> Task {
        Task::Registration(Registration::new(peer))
    }

    pub fn propose(id: u64,
                   region_id: u64,
                   uuid: Uuid,
                   is_conf_change: bool,
                   term: u64,
                   cb: Callback)
                   -> Task {
        Task::Propose(Propose {
            id: id,
            region_id: region_id,
            uuid: uuid,
            term: term,
            is_conf_change: is_conf_change,
            cb: cb,
        })
    }

    pub fn destroy(region_id: u64) -> Task {
        Task::Destroy(Destroy { region_id: region_id })
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Apply(ref a) => write!(f, "[region {}] async apply", a.region_id),
            Task::Propose(ref p) => write!(f, "[region {}] propose", p.region_id),
            Task::Registration(ref r) => {
                write!(f, "[region {}] Reg {:?}", r.region.get_id(), r.apply_state)
            }
            Task::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct ApplyMetrics {
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: i64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub written_bytes: u64,
    pub written_keys: u64,
}

#[derive(Debug)]
pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: Vec<ExecResult>,
    pub metrics: ApplyMetrics,
}

pub enum TaskRes {
    Apply(ApplyRes),
    Destroy(ApplyDelegate),
}

// TODO: use threadpool to do task concurrently
pub struct Runner {
    db: Arc<DB>,
    delegates: HashMap<u64, ApplyDelegate>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new<T, C>(store: &Store<T, C>, notifier: Sender<TaskRes>) -> Runner {
        let mut delegates = HashMap::with_capacity(store.get_peers().len());
        for (&region_id, p) in store.get_peers() {
            delegates.insert(region_id, ApplyDelegate::from_peer(p));
        }
        Runner {
            db: store.engine(),
            delegates: delegates,
            notifier: notifier,
        }
    }

    fn handle_apply(&mut self, apply: Apply) {
        if apply.entries.is_empty() {
            return;
        }
        let mut e = match self.delegates.entry(apply.region_id) {
            MapEntry::Vacant(_) => {
                error!("[region {}] is missing", apply.region_id);
                return;
            }
            MapEntry::Occupied(e) => e,
        };
        {
            let delegate = e.get_mut();
            delegate.metrics = ApplyMetrics::default();
            delegate.term = apply.term;
            let results = delegate.handle_raft_committed_entries(apply.entries);

            if delegate.pending_remove {
                delegate.destroy();
            }

            self.notifier
                .send(TaskRes::Apply(ApplyRes {
                    region_id: apply.region_id,
                    apply_state: delegate.apply_state.clone(),
                    exec_res: results,
                    metrics: delegate.metrics.clone(),
                    applied_index_term: delegate.applied_index_term,
                }))
                .unwrap();
        }
        if e.get().pending_remove {
            e.remove();
        }
    }

    fn handle_propose(&mut self, p: Propose) {
        let cmd = PendingCmd {
            uuid: p.uuid,
            term: p.term,
            cb: Some(p.cb),
        };
        let delegate = match self.delegates.get_mut(&p.region_id) {
            Some(d) => d,
            None => {
                peer::notify_region_removed(p.region_id, p.id, cmd);
                return;
            }
        };
        assert_eq!(delegate.id, p.id);
        if p.is_conf_change {
            if let Some(cmd) = delegate.pending_cmds.take_conf_change() {
                // if it loses leadership before conf change is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have been changed.
                notify_stale_command(&delegate.tag, delegate.term, cmd);
            }
            delegate.pending_cmds.set_conf_change(cmd);
        } else {
            delegate.pending_cmds.append_normal(cmd);
        }
    }

    fn handle_registration(&mut self, s: Registration) {
        let peer_id = s.id;
        let region_id = s.region.get_id();
        let term = s.term;
        let delegate = ApplyDelegate::from_registration(self.db.clone(), s);
        if let Some(mut old_delegate) = self.delegates.insert(region_id, delegate) {
            assert_eq!(old_delegate.id, peer_id);
            old_delegate.term = term;
            old_delegate.clear_all_commands_as_stale();
        }
    }

    fn handle_destroy(&mut self, d: Destroy) {
        // Only respond when the meta exists. Otherwise if destroy is triggered
        // multiple times, the store may destroy wrong target peer.
        if let Some(mut meta) = self.delegates.remove(&d.region_id) {
            meta.destroy();
            self.notifier.send(TaskRes::Destroy(meta)).unwrap();
        }
    }

    fn handle_shutdown(&mut self) {
        for p in self.delegates.values_mut() {
            p.clear_pending_commands();
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Apply(a) => self.handle_apply(a),
            Task::Propose(p) => self.handle_propose(p),
            Task::Registration(s) => self.handle_registration(s),
            Task::Destroy(d) => self.handle_destroy(d),
        }
    }

    fn shutdown(&mut self) {
        self.handle_shutdown();
    }
}
