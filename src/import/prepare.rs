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

use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::thread::{self, JoinHandle};

use rocksdb::SeekKey;
use kvproto::kvrpcpb::*;

use pd::RegionInfo;
use storage::types::Key;

use super::{Client, Config, Engine, Error, Result};
use super::region::*;

const MAX_RETRY_TIMES: u64 = 3;

pub struct PrepareJob {
    tag: String,
    cfg: Config,
    client: Arc<Client>,
    engine: Arc<Engine>,
    cf_name: String,
    job_counter: Arc<AtomicUsize>,
}

impl PrepareJob {
    pub fn new(
        cfg: Config,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: String,
    ) -> PrepareJob {
        PrepareJob {
            tag: format!("[PrepareJob {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            client: client,
            engine: engine,
            cf_name: cf_name,
            job_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn run(&self) -> Vec<RangeInfo> {
        let start = Instant::now();
        info!("{} start", self.tag);

        let (tx, rx) = mpsc::channel();
        let handles = self.run_prepare_threads(rx);
        let ranges = self.run_prepare_stream(tx);
        for h in handles {
            h.join().unwrap();
        }

        info!(
            "{} {} ranges takes {:?}",
            self.tag,
            ranges.len(),
            start.elapsed(),
        );
        ranges
    }

    fn run_prepare_stream(&self, tx: mpsc::Sender<RangeInfo>) -> Vec<RangeInfo> {
        let mut ranges = Vec::new();

        let mut ctx = RegionContext::new(self.client.clone(), self.cfg.region_split_size);
        let mut iter = self.engine.new_iter(&self.cf_name, false);
        iter.seek(SeekKey::Start);

        while iter.valid() {
            let start = iter.key().to_owned();

            ctx.reset(iter.key());
            loop {
                ctx.add(iter.key(), iter.value());
                if !iter.next() || ctx.should_stop_before(iter.key()) {
                    break;
                }
            }

            let end = if iter.valid() { iter.key() } else { RANGE_MAX };
            let range = RangeInfo::new(&start, end, ctx.raw_size());
            ranges.push(range.clone());

            tx.send(range).unwrap();
        }

        ranges
    }

    fn new_prepare_thread(&self, rx: Arc<Mutex<mpsc::Receiver<RangeInfo>>>) -> JoinHandle<()> {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();
        let cf_name = self.cf_name.clone();
        let job_counter = self.job_counter.clone();

        thread::Builder::new()
            .name("prepare-job".to_owned())
            .spawn(move || while let Ok(range) = rx.lock().unwrap().recv() {
                let id = job_counter.fetch_add(1, Ordering::SeqCst);
                let tag = format!("[PrepareJob {}:{}:{}]", engine.uuid(), cf_name, id);
                let job = PrepareRangeJob::new(tag, cfg.clone(), range, client.clone());
                let _ = job.run(); // Don't care about error here.
            })
            .unwrap()
    }

    fn run_prepare_threads(&self, rx: mpsc::Receiver<RangeInfo>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..self.cfg.max_import_jobs {
            handles.push(self.new_prepare_thread(rx.clone()));
        }
        handles
    }
}

struct PrepareRangeJob {
    tag: String,
    cfg: Config,
    range: RangeInfo,
    client: Arc<Client>,
}

impl PrepareRangeJob {
    fn new(tag: String, cfg: Config, range: RangeInfo, client: Arc<Client>) -> PrepareRangeJob {
        PrepareRangeJob {
            tag: tag,
            cfg: cfg,
            range: range,
            client: client,
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.range);

        let mut region = self.client.get_region(&self.range.start)?;

        // No need to split if the file is not large enough.
        if self.range.size > self.cfg.region_split_size / 2 &&
            RangeEnd(self.range.get_end()) < RangeEnd(region.get_end_key())
        {
            region = self.split_region(region, self.range.get_end().to_owned())?;
        }

        self.relocate_region(region)?;

        info!("{} takes {:?}", self.tag, start.elapsed());
        Ok(())
    }

    fn split_region(&self, mut region: RegionInfo, split_key: Vec<u8>) -> Result<RegionInfo> {
        for _ in 0..MAX_RETRY_TIMES {
            let ctx = new_context(&region);
            let store_id = ctx.get_peer().get_store_id();
            // The SplitRegion API accepts a raw key.
            let raw_key = Key::from_encoded(split_key.clone()).raw()?;

            let mut split = SplitRegionRequest::new();
            split.set_context(ctx);
            split.set_split_key(raw_key);

            let res = match self.client.split_region(store_id, split) {
                Ok(ref mut resp) if resp.has_region_error() => {
                    let mut error = resp.take_region_error();
                    if error.get_not_leader().has_leader() {
                        region.leader = Some(error.take_not_leader().take_leader());
                        continue;
                    }
                    Err(Error::SplitRegion(error))
                }
                res => res,
            };

            match res {
                Ok(mut resp) => {
                    info!(
                        "{} split {:?} to left {{{:?}}} and right {{{:?}}}",
                        self.tag,
                        region,
                        resp.get_left(),
                        resp.get_right(),
                    );
                    let region = resp.take_left();
                    // Just assume that the leader will be at the same store.
                    let leader = region
                        .get_peers()
                        .iter()
                        .find(|p| p.get_store_id() == store_id)
                        .cloned();
                    return Ok(RegionInfo::new(region, leader));
                }
                Err(e) => {
                    error!("{} split {:?}: {:?}", self.tag, region, e);
                    return Err(e);
                }
            }
        }

        unreachable!();
    }

    fn relocate_region(&self, _: RegionInfo) -> Result<()> {
        // TODO
        Ok(())
    }
}
