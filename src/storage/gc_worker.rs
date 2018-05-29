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

use super::engine::{Engine, ScanMode};
use super::mvcc::{MvccReader, MvccTxn, MAX_TXN_WRITE_SIZE};
use super::txn::GC_BATCH_SIZE;
use super::{Callback, Error, Key, Result};
use kvproto::kvrpcpb::Context;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use util::worker::{self, Builder, Runnable, Worker};

struct GCTask {
    pub ctx: Context,
    pub safe_point: u64,
    pub callback: Callback<()>,
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let epoch = match self.ctx.region_epoch.as_ref() {
            None => "None".to_string(),
            Some(e) => format!("{{{}, {}}}", e.conf_ver, e.version),
        };
        write!(
            f,
            "GC task at region {}, epoch {}, safe_point {}",
            self.ctx.region_id, epoch, self.safe_point
        )
    }
}

#[derive(Clone)]
pub struct GCWorker {
    engine: Box<Engine>,
    gc_ratio_threshold: f64,
    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,
}

impl GCWorker {
    pub fn new(engine: Box<Engine>, gc_ratio_threshold: f64) -> GCWorker {
        let worker = Arc::new(Mutex::new(Builder::new("gc-scheduler").create()));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GCWorker {
            engine,
            gc_ratio_threshold,
            worker,
            worker_scheduler,
        }
    }

    pub fn start(&self) -> Result<()> {
        self.worker
            .lock()
            .unwrap()
            .start(self.clone())
            .map_err(Error::from)
    }

    pub fn stop(&self) -> Result<()> {
        let h = self.worker.lock().unwrap().stop().unwrap();
        if let Err(e) = h.join() {
            Err(box_err!("failed to join gc_worker handle, err: {:?}", e))
        } else {
            Ok(())
        }
    }

    fn gc_scan_keys(
        &self,
        ctx: &Context,
        safe_point: u64,
        next_scan_key: Option<Key>,
    ) -> Result<(Option<Vec<Key>>, Option<Key>)> {
        let snapshot = self.engine.snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            ctx.get_isolation_level(),
        );
        if !reader.need_gc(safe_point, self.gc_ratio_threshold) {
            Ok((None, None))
        } else {
            reader
                .scan_keys(next_scan_key, GC_BATCH_SIZE)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        Ok((None, None))
                    } else {
                        Ok((Some(keys), next))
                    }
                })
        }
    }

    fn do_gc(
        &self,
        ctx: &Context,
        safe_point: u64,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.engine.snapshot(ctx)?;
        let mut txn = MvccTxn::new(
            snapshot,
            0,
            Some(ScanMode::Forward),
            ctx.get_isolation_level(),
            !ctx.get_not_fill_cache(),
        );
        for k in keys {
            txn.gc(&k, safe_point)?;
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_scan_key = Some(k);
                break;
            }
        }

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    pub fn gc(&self, ctx: Context, safe_point: u64) -> Result<()> {
        let mut next_key = None;
        loop {
            let (keys, next) = self.gc_scan_keys(&ctx, safe_point, next_key)?;
            if keys.is_none() {
                break;
            }

            next_key = self.do_gc(&ctx, safe_point, keys.unwrap(), next)?;
            if next_key.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask {
                ctx,
                safe_point,
                callback,
            })
            .map_err(|e| box_err!("failed to schedule gc task: {:?}", e))
    }
}

impl Runnable<GCTask> for GCWorker {
    fn run(&mut self, task: GCTask) {
        (task.callback)(self.gc(task.ctx, task.safe_point));
    }
}
