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

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

use rocksdb::DB;

use kvproto::metapb::RegionEpoch;
use raftstore::store::{PeerStorage, keys, Msg};
use raftstore::store::engine::{Iterable, IterOption, SeekMode};
use raftstore::Result;
use rocksdb::DBIterator;
use util::escape;
use util::transport::SendCh;
use util::worker::Runnable;
use storage::{CfName, LARGE_CFS};

use super::metrics::*;

#[derive(PartialEq, Eq)]
struct KeyEntry {
    key: Option<Vec<u8>>,
    pos: usize,
    value_size: usize,
}

impl KeyEntry {
    fn new(key: Vec<u8>, pos: usize, value_size: usize) -> KeyEntry {
        KeyEntry {
            key: Some(key),
            pos: pos,
            value_size: value_size,
        }
    }

    fn take(&mut self) -> KeyEntry {
        KeyEntry::new(self.key.take().unwrap(), self.pos, self.value_size)
    }

    fn len(&self) -> usize {
        self.key.as_ref().unwrap().len() + self.value_size
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.as_ref().unwrap().cmp(rhs.key.as_ref().unwrap()).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<'a> {
    iters: Vec<DBIterator<'a>>,
    heap: BinaryHeap<KeyEntry>,
}

impl<'a> MergedIterator<'a> {
    fn new(db: &'a DB,
           cfs: &[CfName],
           start_key: &[u8],
           end_key: &[u8],
           fill_cache: bool)
           -> Result<MergedIterator<'a>> {
        let mut iters = Vec::with_capacity(cfs.len());
        let mut heap = BinaryHeap::with_capacity(cfs.len());
        for (pos, cf) in cfs.into_iter().enumerate() {
            let mut iter = try!(db.new_iterator_cf(cf,
                                                   IterOption::new(Some(end_key.to_vec()),
                                                                   fill_cache,
                                                                   SeekMode::TotalOrderSeek)));
            if iter.seek(start_key.into()) {
                heap.push(KeyEntry::new(iter.key().to_vec(), pos, iter.value().len()));
            }
            iters.push(iter);
        }
        Ok(MergedIterator {
            iters: iters,
            heap: heap,
        })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let iter = &mut self.iters[pos];
        if iter.next() {
            // TODO: avoid copy key.
            let e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len());
            let mut front = self.heap.peek_mut().unwrap();
            let res = front.take();
            *front = e;
            Some(res)
        } else {
            self.heap.pop()
        }
    }
}

/// Split checking task.
pub struct Task {
    region_id: u64,
    epoch: RegionEpoch,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    engine: Arc<DB>,
}

impl Task {
    pub fn new(ps: &PeerStorage) -> Task {
        Task {
            region_id: ps.get_region_id(),
            epoch: ps.get_region().get_region_epoch().clone(),
            start_key: keys::enc_start_key(&ps.region),
            end_key: keys::enc_end_key(&ps.region),
            engine: ps.get_engine().clone(),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Split Check Task for {}", self.region_id)
    }
}

pub struct Runner {
    ch: SendCh<Msg>,
    region_max_size: u64,
    split_size: u64,
}

impl Runner {
    pub fn new(ch: SendCh<Msg>, region_max_size: u64, split_size: u64) -> Runner {
        Runner {
            ch: ch,
            region_max_size: region_max_size,
            split_size: split_size,
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("[region {}] executing task {} {}",
               task.region_id,
               escape(&task.start_key),
               escape(&task.end_key));
        CHECK_SPILT_COUNTER_VEC.with_label_values(&["all"]).inc();

        let mut size = 0;
        let mut split_key = vec![];
        let timer = CHECK_SPILT_HISTOGRAM.start_timer();

        let res = MergedIterator::new(task.engine.as_ref(),
                                      LARGE_CFS,
                                      &task.start_key,
                                      &task.end_key,
                                      false)
            .map(|mut iter| {
                while let Some(e) = iter.next() {
                    size += e.len() as u64;
                    if split_key.is_empty() && size > self.split_size {
                        split_key = e.key.unwrap();
                    }
                    if size >= self.region_max_size {
                        break;
                    }
                }
            });

        if let Err(e) = res {
            error!("failed to scan split key of region {}: {:?}",
                   task.region_id,
                   e);
            return;
        }

        timer.observe_duration();

        if size < self.region_max_size {
            debug!("[region {}] no need to send for {} < {}",
                   task.region_id,
                   size,
                   self.region_max_size);

            CHECK_SPILT_COUNTER_VEC.with_label_values(&["ignore"]).inc();
            return;
        }
        let res = self.ch.try_send(new_split_check_result(task.region_id, task.epoch, split_key));
        if let Err(e) = res {
            warn!("[region {}] failed to send check result, err {:?}",
                  task.region_id,
                  e);
        }

        CHECK_SPILT_COUNTER_VEC.with_label_values(&["success"]).inc();
    }
}

fn new_split_check_result(region_id: u64, epoch: RegionEpoch, split_key: Vec<u8>) -> Msg {
    Msg::SplitCheckResult {
        region_id: region_id,
        epoch: epoch,
        split_key: split_key,
    }
}
