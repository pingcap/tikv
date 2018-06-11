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

use super::engine::{Engine, Error as EngineError, ScanMode, Snapshot, StatisticsSummary};
use super::metrics::*;
use super::mvcc::{MvccReader, MvccTxn, MAX_TXN_WRITE_SIZE};
use super::txn::GC_BATCH_SIZE;
use super::{Callback, Error, Key, Result};
use kvproto::kvrpcpb::Context;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use util::worker::{self, Builder, Runnable, ScheduleError, Worker};

pub const GC_MAX_PENDING_TASKS: usize = 2;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;

struct GCTask {
    pub ctx: Context,
    pub safe_point: u64,
    pub callback: Callback<()>,
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let epoch = format!("{:?}", self.ctx.region_epoch.as_ref());
        f.debug_struct("GCTask")
            .field("region", &self.ctx.get_region_id())
            .field("epoch", &epoch)
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

/// `GCRunner` is used to perform GC on the engine
struct GCRunner {
    engine: Box<Engine>,
    ratio_threshold: f64,

    stats: StatisticsSummary,
}

impl GCRunner {
    pub fn new(engine: Box<Engine>, ratio_threshold: f64) -> GCRunner {
        GCRunner {
            engine,
            ratio_threshold,

            stats: StatisticsSummary::default(),
        }
    }

    fn get_snapshot(&self, ctx: &mut Context) -> Result<Box<Snapshot>> {
        let timeout = Duration::from_secs(GC_SNAPSHOT_TIMEOUT_SECS);
        match wait_op!(|cb| self.engine.async_snapshot(ctx, cb), timeout) {
            Some((cb_ctx, Ok(snapshot))) => {
                if let Some(term) = cb_ctx.term {
                    ctx.set_term(term);
                }
                Ok(snapshot)
            }
            Some((_, Err(e))) => Err(e),
            None => Err(EngineError::Timeout(timeout)),
        }.map_err(Error::from)
    }

    /// Scan keys in the region. Returns scanned keys if any, and a key indicating scan progress
    fn scan_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        from: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            ctx.get_isolation_level(),
        );

        // If from.is_some(), it must not be the first scan of the region.
        // So we must continue doing GC.
        let skip_gc = from.is_none() && !reader.need_gc(safe_point, self.ratio_threshold);
        let res = if skip_gc {
            KV_GC_SKIPPED_COUNTER.inc();
            Ok((vec![], None))
        } else {
            reader
                .scan_keys(from.clone(), limit)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        assert!(next.is_none());
                        if from.is_none() {
                            KV_GC_EMPTY_RANGE_COUNTER.inc();
                        }
                    }
                    Ok((keys, next))
                })
        };
        self.stats.add_statistics(reader.get_statistics());
        res
    }

    /// Clean up outdated data.
    fn gc_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.get_snapshot(ctx)?;
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
        self.stats.add_statistics(txn.get_statistics());

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    pub fn gc(&mut self, mut ctx: Context, safe_point: u64) -> Result<()> {
        let _gc_timer = GC_DURATION_HISTOGRAM.start_coarse_timer();

        debug!(
            "doing gc on region {}, safe_point {}",
            ctx.get_region_id(),
            safe_point
        );

        let mut next_key = None;
        loop {
            let (keys, next) = self.scan_keys(&mut ctx, safe_point, next_key, GC_BATCH_SIZE)
                .map_err(|e| {
                    warn!("scan_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if keys.is_empty() {
                break;
            }

            next_key = self.gc_keys(&mut ctx, safe_point, keys, next)
                .map_err(|e| {
                    warn!("gc_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if next_key.is_none() {
                break;
            }
        }

        debug!(
            "gc on region {}, safe_point {} has finished",
            ctx.get_region_id(),
            safe_point
        );
        Ok(())
    }
}

impl Runnable<GCTask> for GCRunner {
    fn run(&mut self, task: GCTask) {
        GC_GCTASK_COUNTER.inc();
        let result = self.gc(task.ctx, task.safe_point);
        if result.is_err() {
            GC_GCTASK_FAIL_COUNTER.inc();
        }
        (task.callback)(result);
    }

    fn on_tick(&mut self) {
        let stats = mem::replace(&mut self.stats, StatisticsSummary::default());
        for (cf, details) in stats.stat.details() {
            for (tag, count) in details {
                GC_KEYS_COUNTER_VEC
                    .with_label_values(&[cf, tag])
                    .inc_by(count as i64);
            }
        }
    }
}

/// `GCWorker` is used to schedule GC operations
#[derive(Clone)]
pub struct GCWorker {
    engine: Box<Engine>,
    ratio_threshold: f64,
    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,
}

impl GCWorker {
    pub fn new(engine: Box<Engine>, ratio_threshold: f64) -> GCWorker {
        let worker = Arc::new(Mutex::new(
            Builder::new("gc-worker")
                .pending_capacity(GC_MAX_PENDING_TASKS)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GCWorker {
            engine,
            ratio_threshold,
            worker,
            worker_scheduler,
        }
    }

    pub fn start(&self) -> Result<()> {
        let runner = GCRunner::new(self.engine.clone(), self.ratio_threshold);
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn stop(&self) -> Result<()> {
        let h = self.worker.lock().unwrap().stop().unwrap();
        if let Err(e) = h.join() {
            Err(box_err!("failed to join gc_worker handle, err: {:?}", e))
        } else {
            Ok(())
        }
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask {
                ctx,
                safe_point,
                callback,
            })
            .or_else(|e| match e {
                ScheduleError::Full(task) => {
                    GC_TOO_BUSY_COUNTER.inc();
                    (task.callback)(Err(Error::GCWorkerTooBusy));
                    Ok(())
                }
                _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
            })
    }
}
