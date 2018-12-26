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

use super::engine::{
    Engine, Error as EngineError, RegionInfoProvider, ScanMode, StatisticsSummary,
};
use super::metrics::*;
use super::mvcc::{MvccReader, MvccTxn};
use super::{Callback, Error, Key, Result, CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::Future;
use kvproto::kvrpcpb::Context;
use kvproto::metapb;
use pd::PdClient;
use raft::StateRole;
use raftstore::store::keys;
use raftstore::store::msg::Msg as RaftStoreMsg;
use raftstore::store::util::{delete_all_in_range_cf, find_peer};
use raftstore::store::SeekRegionResult;
use rocksdb::rocksdb::DB;
use server::transport::{RaftStoreRouter, ServerRaftStoreRouter};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};
use util::rocksdb::get_cf_handle;
use util::time::{duration_to_sec, SlowTimer};
use util::worker::{self, Builder as WorkerBuilder, Runnable, ScheduleError, Worker};

// TODO: make it configurable.
pub const GC_BATCH_SIZE: usize = 512;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_PENDING_TASKS: usize = 2;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

const POLL_SAFE_POINT_INTERVAL_SECS: u64 = 60;
const GC_SEEK_REGION_LIMIT: u32 = 32;

const BEGIN_KEY: &[u8] = b"";

const STATE_NONE: &str = "";
const STATE_INIT: &str = "initializing";
const STATE_IDLE: &str = "idle";
const STATE_WORKING: &str = "working";

/// GCWorker can get safe point from something that implements `GCSafePointSourse`
/// TODO: Give it a better name?
pub trait GCSafePointProvider: Send + 'static {
    fn get_safe_point(&self) -> Result<u64>;
}

impl<T: PdClient + 'static> GCSafePointProvider for Arc<T> {
    fn get_safe_point(&self) -> Result<u64> {
        let future = self.get_gc_safe_point();
        future
            .wait()
            .map_err(|e| box_err!("failed to get safe point from PD: {:?}", e))
    }
}

enum GCTask {
    GC {
        ctx: Context,
        safe_point: u64,
        callback: Callback<()>,
    },
    UnsafeDestroyRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    },
}

impl GCTask {
    pub fn take_callback(self) -> Callback<()> {
        match self {
            GCTask::GC { callback, .. } => callback,
            GCTask::UnsafeDestroyRange { callback, .. } => callback,
        }
    }

    #[allow(dead_code)]
    pub fn replace_callback(&mut self, new_callback: Callback<()>) -> Callback<()> {
        let callback = match self {
            GCTask::GC {
                ref mut callback, ..
            } => callback,
            GCTask::UnsafeDestroyRange {
                ref mut callback, ..
            } => callback,
        };
        mem::replace(callback, new_callback)
    }

    pub fn get_label(&self) -> &'static str {
        match self {
            GCTask::GC { .. } => "gc",
            GCTask::UnsafeDestroyRange { .. } => "unsafe_destroy_range",
        }
    }
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            GCTask::GC {
                ctx, safe_point, ..
            } => {
                let epoch = format!("{:?}", ctx.region_epoch.as_ref());
                f.debug_struct("GC")
                    .field("region", &ctx.get_region_id())
                    .field("epoch", &epoch)
                    .field("safe_point", safe_point)
                    .finish()
            }
            GCTask::UnsafeDestroyRange {
                start_key, end_key, ..
            } => f
                .debug_struct("UnsafeDestroyRange")
                .field("start_key", &format!("{}", start_key))
                .field("end_key", &format!("{}", end_key))
                .finish(),
        }
    }
}

/// `GCRunner` is used to perform GC on the engine
struct GCRunner<E: Engine> {
    engine: E,
    local_storage: Option<Arc<DB>>,
    raft_store_router: Option<ServerRaftStoreRouter>,

    ratio_threshold: f64,

    stats: StatisticsSummary,
}

impl<E: Engine> GCRunner<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        ratio_threshold: f64,
    ) -> Self {
        Self {
            engine,
            local_storage,
            raft_store_router,
            ratio_threshold,
            stats: StatisticsSummary::default(),
        }
    }

    fn get_snapshot(&self, ctx: &mut Context) -> Result<E::Snap> {
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

        let is_range_start = from.is_none();

        // range start gc with from == None, and this is an optimization to
        // skip gc before scanning all data.
        let skip_gc = is_range_start && !reader.need_gc(safe_point, self.ratio_threshold);
        let res = if skip_gc {
            KV_GC_SKIPPED_COUNTER.inc();
            Ok((vec![], None))
        } else {
            reader
                .scan_keys(from, limit)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        assert!(next.is_none());
                        if is_range_start {
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
        let mut txn = MvccTxn::new(snapshot, 0, !ctx.get_not_fill_cache()).unwrap();
        for k in keys {
            let gc_info = txn.gc(k.clone(), safe_point)?;

            if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC found at least {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.found_versions,
                    k
                );
            }
            // TODO: we may delete only part of the versions in a batch, which may not beyond
            // the logging threshold `GC_LOG_DELETED_VERSION_THRESHOLD`.
            if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC deleted {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.deleted_versions,
                    k
                );
            }

            if !gc_info.is_completed {
                next_scan_key = Some(k);
                break;
            }
        }
        self.stats.add_statistics(&txn.take_statistics());

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    fn gc(&mut self, ctx: &mut Context, safe_point: u64) -> Result<()> {
        debug!(
            "doing gc on region {}, safe_point {}",
            ctx.get_region_id(),
            safe_point
        );

        let mut next_key = None;
        loop {
            // Scans at most `GC_BATCH_SIZE` keys
            let (keys, next) = self
                .scan_keys(ctx, safe_point, next_key, GC_BATCH_SIZE)
                .map_err(|e| {
                    warn!("gc scan_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if keys.is_empty() {
                break;
            }

            // Does the GC operation on all scanned keys
            next_key = self.gc_keys(ctx, safe_point, keys, next).map_err(|e| {
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

    fn unsafe_destroy_range(&self, _: &Context, start_key: &Key, end_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy range start_key: {}, end_key: {} started.",
            start_key, end_key
        );

        // TODO: Refine usage of errors

        let local_storage = self.local_storage.as_ref().ok_or_else(|| {
            let e: Error = box_err!("unsafe destroy range not supported: local_storage not set");
            warn!("unsafe destroy range failed: {:?}", &e);
            e
        })?;

        // Convert keys to RocksDB layer form
        // TODO: Logic coupled with raftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let start_data_key = keys::data_key(start_key.as_encoded());
        let end_data_key = keys::data_end_key(end_key.as_encoded());

        let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];

        // First, call delete_files_in_range to free as much disk space as possible
        let delete_files_start_time = Instant::now();
        for cf in cfs {
            let cf_handle = get_cf_handle(local_storage, cf).unwrap();
            local_storage
                .delete_files_in_range_cf(cf_handle, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_files_in_range_cf: {:?}",
                        e
                    );
                    e
                })?;
        }

        info!(
            "unsafe destroy range start_key: {}, end_key: {} finished deleting files in range, cost time: {:?}",
            start_key, end_key, delete_files_start_time.elapsed(),
        );

        // Then, delete all remaining keys in the range.
        let cleanup_all_start_time = Instant::now();
        for cf in cfs {
            // TODO: set use_delete_range with config here.
            delete_all_in_range_cf(local_storage, cf, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_all_in_range_cf: {:?}",
                        e
                    );
                    e
                })?;
        }

        let cleanup_all_time_cost = cleanup_all_start_time.elapsed();

        if let Some(router) = self.raft_store_router.as_ref() {
            router
                .send(RaftStoreMsg::ClearRegionSizeInRange {
                    start_key: start_key.as_encoded().to_vec(),
                    end_key: end_key.as_encoded().to_vec(),
                })
                .unwrap_or_else(|e| {
                    // Warn and ignore it.
                    warn!(
                        "unsafe destroy range: failed sending ClearRegionSizeInRange: {:?}",
                        e
                    );
                });
        } else {
            warn!("unsafe destroy range: can't clear region size information: raft_store_router not set");
        }

        info!(
            "unsafe destroy range start_key: {}, end_key: {} finished cleaning up all, cost time {:?}",
            start_key, end_key, cleanup_all_time_cost,
        );
        Ok(())
    }

    fn handle_gc_worker_task(&mut self, mut task: GCTask) {
        let label = task.get_label();
        GC_GCTASK_COUNTER_VEC.with_label_values(&[label]).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);

        let result = match &mut task {
            GCTask::GC {
                ctx, safe_point, ..
            } => self.gc(ctx, *safe_point),
            GCTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                ..
            } => self.unsafe_destroy_range(ctx, start_key, end_key),
        };

        GC_TASK_DURATION_HISTOGRAM_VEC
            .with_label_values(&[label])
            .observe(duration_to_sec(timer.elapsed()));
        slow_log!(timer, "{}", task);

        if result.is_err() {
            GC_GCTASK_FAIL_COUNTER_VEC.with_label_values(&[label]).inc();
        }
        (task.take_callback())(result);
    }
}

impl<E: Engine> Runnable<GCTask> for GCRunner<E> {
    #[inline]
    fn run(&mut self, task: GCTask) {
        self.handle_gc_worker_task(task);
    }

    // The default implementation of `run_batch` prints a warning to log when it takes over 1 second
    // to handle a task. It's not proper here, so override it to remove the log.
    #[inline]
    fn run_batch(&mut self, tasks: &mut Vec<GCTask>) {
        for task in tasks.drain(..) {
            self.run(task);
        }
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

/// Schedule a `GCTask` in a worker
fn schedule_gc(
    scheduler: &worker::Scheduler<GCTask>,
    ctx: Context,
    safe_point: u64,
    callback: Callback<()>,
) -> Result<()> {
    scheduler
        .schedule(GCTask::GC {
            ctx,
            safe_point,
            callback,
        })
        .or_else(|e| match e {
            ScheduleError::Full(task) => {
                GC_TOO_BUSY_COUNTER.inc();
                (task.take_callback())(Err(Error::GCWorkerTooBusy));
                Ok(())
            }
            _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
        })
}

/// Do GC synchronously
fn gc(scheduler: &worker::Scheduler<GCTask>, ctx: Context, safe_point: u64) -> Result<()> {
    wait_op!(|callback| schedule_gc(scheduler, ctx, safe_point, callback)).unwrap()
}

/// The configurations of the Automatic GC
pub struct AutoGCConfig<S: GCSafePointProvider, R: RegionInfoProvider> {
    pub safe_point_provider: S,
    pub region_info_provider: R,

    /// Used to find which peer of a region is on this TiKV, so that we can compose a `Context`.
    pub self_store_id: u64,

    pub poll_safe_point_interval: Duration,

    /// If this is set, safe_point will be checked before doing GC on every region while working.
    /// Otherwise safe_point will be only checked when `poll_safe_point_interval` has past since
    /// last checking.
    pub always_check_safe_point: bool,

    /// This will be called when a round GC has finished and goes back to idle state.
    pub on_gc_finished: Option<Box<Fn() + Send>>,
}

impl<S: GCSafePointProvider, R: RegionInfoProvider> AutoGCConfig<S, R> {
    /// Create a new config. Fields that are not mentioned in these params will be set to default.
    pub fn new(safe_point_provider: S, region_info_provider: R, self_store_id: u64) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_secs(POLL_SAFE_POINT_INTERVAL_SECS),
            always_check_safe_point: false,
            on_gc_finished: None,
        }
    }

    /// Create a config that is better for tests. The poll interval is as short as 0.1s and during
    /// GC it will never skip checking safe point.
    pub fn new_test_cfg(
        safe_point_provider: S,
        region_info_provider: R,
        self_store_id: u64,
    ) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_millis(100),
            always_check_safe_point: true,
            on_gc_finished: None,
        }
    }
}

#[derive(PartialEq, Debug)]
enum GCManagerError {
    Stopped,
}

type GCManagerResult<T> = ::std::result::Result<T, GCManagerError>;

/// GCManagerContext is used to check if GCManager should be stopped.
///
/// When GCManager is running, it might take very long time to GC a round. In a situation that the
/// safe point updates too frequently, GCManager may even keeps rewinding and never stops. So there
/// should be a way to stop it when TiKV is shutdown.
struct GCManagerContext {
    /// Used to receive stop signal. The sender side is hold in `GCManagerHandler`.
    /// If this field is `None`, the `GCManagerContext` will never stop.
    pub stop_signal_receiver: Option<mpsc::Receiver<()>>,
    is_stopped: bool,
}

impl GCManagerContext {
    /// Create a new `GCManagerContext` with no
    pub fn new() -> Self {
        Self {
            stop_signal_receiver: None,
            is_stopped: false,
        }
    }

    /// Sleep for a while. if a stop signal is received, returns imediately with
    /// `GCManagerError::Stopped`
    fn sleep_or_stop(&mut self, timeout: Duration) -> GCManagerResult<()> {
        if self.is_stopped {
            return Err(GCManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.recv_timeout(timeout) {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GCManagerError::Stopped)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(()),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("stop_signal_receiver unexpectedly disconnected")
                }
            },
            None => {
                thread::sleep(timeout);
                Ok(())
            }
        }
    }

    /// Check if
    fn check_stopped(&mut self) -> GCManagerResult<()> {
        if self.is_stopped {
            return Err(GCManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.try_recv() {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GCManagerError::Stopped)
                }
                Err(mpsc::TryRecvError::Empty) => Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("stop_signal_receiver unexpectedly disconnected")
                }
            },
            None => Ok(()),
        }
    }
}

fn make_context(mut region: metapb::Region, peer: metapb::Peer) -> Context {
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.take_region_epoch());
    ctx.set_peer(peer);
    ctx
}

#[inline]
fn set_status_metrics(state: &str) {
    for s in &[STATE_INIT, STATE_IDLE, STATE_WORKING] {
        AUTO_GC_STATUS_GAUGE_VEC
            .with_label_values(&[*s])
            .set(if state == *s { 1 } else { 0 });
    }
}

/// `GCWorkerThreadHandle` wraps `JoinHandle` of `GCWorkerThread` and helps to stop the
/// `GCWorkerThread` synchronously
struct GCManagerHandle {
    join_handle: JoinHandle<()>,
    stop_signal_sender: mpsc::Sender<()>,
}

impl GCManagerHandle {
    pub fn stop(self) -> Result<()> {
        let res: Result<()> = self
            .stop_signal_sender
            .send(())
            .map_err(|e| box_err!("failed to send stop signal to gc worker thread: {:?}", e));
        if res.is_err() {
            return res;
        }
        self.join_handle
            .join()
            .map_err(|e| box_err!("failed to join gc worker thread: {:?}", e))
    }
}

/// `GCManager` scans regions and does gc automatically.
struct GCManager<S: GCSafePointProvider, R: RegionInfoProvider> {
    cfg: AutoGCConfig<S, R>,

    safe_point: u64,

    worker_scheduler: worker::Scheduler<GCTask>,

    gc_worker_context: GCManagerContext,
}

impl<S: GCSafePointProvider, R: RegionInfoProvider> GCManager<S, R> {
    pub fn new(
        cfg: AutoGCConfig<S, R>,
        worker_scheduler: worker::Scheduler<GCTask>,
    ) -> GCManager<S, R> {
        GCManager {
            cfg,
            safe_point: 0,
            worker_scheduler,
            gc_worker_context: GCManagerContext::new(),
        }
    }

    fn start(mut self) -> Result<GCManagerHandle> {
        let (tx, rx) = mpsc::channel();
        self.gc_worker_context.stop_signal_receiver = Some(rx);
        let res: Result<_> = ThreadBuilder::new()
            .name(thd_name!("gc-manager"))
            .spawn(move || {
                info!("gc-manager is started");
                self.run();
                info!("gc-manager is stopped");
            })
            .map_err(|e| box_err!("failed to start gc manager: {:?}", e));
        res.map(|join_handle| GCManagerHandle {
            join_handle,
            stop_signal_sender: tx,
        })
    }

    fn run(&mut self) {
        let err = self.run_impl().unwrap_err();
        set_status_metrics(STATE_NONE);
        AUTO_GC_PROGRESS.set(0);
        assert_eq!(err, GCManagerError::Stopped);
    }

    fn run_impl(&mut self) -> GCManagerResult<()> {
        set_status_metrics(STATE_INIT);
        self.initialize()?;

        loop {
            set_status_metrics(STATE_IDLE);
            self.poll_next_safe_point()?;

            set_status_metrics(STATE_WORKING);
            self.work()?;

            if let Some(on_finished) = self.cfg.on_gc_finished.as_ref() {
                on_finished();
            }
        }
    }

    // Get current safe point
    fn initialize(&mut self) -> GCManagerResult<()> {
        info!("gc-manager is initializing");
        self.safe_point = 0;
        self.poll_next_safe_point()?;
        info!("gc-manager started at safe point {}", self.safe_point);
        Ok(())
    }

    fn work(&mut self) -> GCManagerResult<()> {
        let mut need_rewind = false;
        let mut end = None;
        let mut progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));

        // Records last time checking safe point from PD.
        let mut timer = Instant::now();
        // Records how many region we checked and reset to 0 when rewinding. Put it to metrics so
        // we can see the progress clearly.
        let mut progress_counter = 0;

        info!(
            "gc_worker: started auto gc with safe point {}",
            self.safe_point
        );

        loop {
            self.gc_worker_context.check_stopped()?;

            if need_rewind {
                if progress.is_none() {
                    // Worked to the end. restart from beginning
                    progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));
                    need_rewind = false;
                    progress_counter = 0;
                    info!("gc_worker: auto gc has rewound");
                }
            } else if progress.is_none()
                || (end.is_some() && progress.as_ref().unwrap() >= end.as_ref().unwrap())
            {
                info!("gc_worker: finished auto gc");
                AUTO_GC_PROGRESS.set(0);
                return Ok(());
            }

            assert!(progress.is_some());
            progress_counter += 1;
            AUTO_GC_PROGRESS.set(progress_counter);

            // Update safe_point periodically
            if self.need_update_safe_point(&timer) {
                if self.try_update_safe_point() {
                    if progress.as_ref().unwrap().as_encoded().is_empty() {
                        // `progress` is empty means the starting. We don't need to rewind. We just
                        // Continue GC to the end.
                        need_rewind = false;
                        end = None;
                    } else {
                        need_rewind = true;
                        end = progress.clone();
                        info!(
                            "gc_worker: safe point updated to {}, auto gc will rewind to {}",
                            self.safe_point,
                            end.as_ref().unwrap()
                        );
                    }
                }
                timer = Instant::now();
            }

            progress = self.gc_once(progress.unwrap())?;
        }
    }

    #[inline]
    fn need_update_safe_point(&self, timer: &Instant) -> bool {
        timer.elapsed() >= self.cfg.poll_safe_point_interval || self.cfg.always_check_safe_point
    }

    fn gc_once(&mut self, from_key: Key) -> GCManagerResult<Option<Key>> {
        // Get the information of the next region to do GC.
        let (ctx, next_key) = self.get_next_gc_context(from_key)?;
        if ctx.is_none() {
            // No more regions.
            return Ok(None);
        }
        let ctx = ctx.unwrap();

        // Do GC.
        // Ignore the error and continue, since it's useless to retry this.
        // TODO: Find a better way to handle errors.
        debug!(
            "trying gc region {}, epoch {:?}, end_key {}",
            ctx.get_region_id(),
            ctx.region_epoch.as_ref(),
            match &next_key {
                Some(key) => format!("{}", key),
                None => "None".to_string(),
            }
        );
        if let Err(e) = gc(&self.worker_scheduler, ctx.clone(), self.safe_point) {
            error!(
                "failed gc region {}, epoch {:?}, end_key {}, err: {:?}",
                ctx.get_region_id(),
                ctx.region_epoch.as_ref(),
                match &next_key {
                    Some(key) => format!("{}", key),
                    None => "None".to_string(),
                },
                e
            );
        }

        Ok(next_key)
    }

    fn try_update_safe_point(&mut self) -> bool {
        let safe_point = match self.cfg.safe_point_provider.get_safe_point() {
            Ok(res) => res,
            // Return false directly so we will check it a while later
            Err(e) => {
                warn!("failed to get safe point from pd: {:?}", e);
                return false;
            }
        };

        match safe_point.cmp(&self.safe_point) {
            Ordering::Less => {
                error!(
                    "got new safe point {} which is less than current safe point {}. \
                     there must be something wrong.",
                    safe_point, self.safe_point
                );
                self.safe_point = safe_point;
                false
            }
            Ordering::Equal => false,
            Ordering::Greater => {
                info!("gc_worker: safe point updated");
                self.safe_point = safe_point;
                true
            }
        }
    }

    fn poll_next_safe_point(&mut self) -> GCManagerResult<u64> {
        loop {
            if self.try_update_safe_point() {
                return Ok(self.safe_point);
            }

            self.gc_worker_context
                .sleep_or_stop(self.cfg.poll_safe_point_interval)?;
        }
    }

    /// Get the next region with end_key greater than given key, and the current TiKV is its leader,
    /// so we can do GC on it next.
    /// Returns context to call GC and end_key of the region.
    fn get_next_gc_context(
        &mut self,
        mut key: Key,
    ) -> GCManagerResult<(Option<Context>, Option<Key>)> {
        loop {
            self.gc_worker_context.check_stopped()?;

            let result;
            // Loop until successfully invoking seek_region
            // TODO: Sould there be any better error handling?
            loop {
                let res = self.cfg.region_info_provider.seek_region(
                    key.as_encoded(),
                    box |_, role| role == StateRole::Leader,
                    GC_SEEK_REGION_LIMIT,
                );
                if let Ok(r) = res {
                    result = r;
                    break;
                }
            }

            match result {
                SeekRegionResult::Found(mut region) => {
                    // It might be ok to leave other fields default
                    let end_key = Key::from_encoded(region.take_end_key());
                    // Determine if it's the last region
                    let end_key = if end_key.as_encoded().is_empty() {
                        None
                    } else {
                        Some(end_key)
                    };

                    if let Some(peer) = find_peer(&region, self.cfg.self_store_id).cloned() {
                        let ctx = make_context(region, peer);
                        return Ok((Some(ctx), end_key));
                    }

                    // `region` doesn't contains such a peer. Ignore it and continue.
                    match end_key {
                        Some(k) => key = k,
                        None => {
                            // Ended
                            return Ok((None, None));
                        }
                    }
                }
                SeekRegionResult::Ended => return Ok((None, None)),
                // Seek again from `next_key`
                SeekRegionResult::LimitExceeded { next_key } => key = Key::from_encoded(next_key),
            }
        }
    }
}

/// `GCWorker` is used to schedule GC operations
#[derive(Clone)]
pub struct GCWorker<E: Engine> {
    engine: E,
    /// `local_storage` represent the underlying RocksDB of the `engine`.
    local_storage: Option<Arc<DB>>,
    /// `raft_store_router` is useful to signal raftstore clean region size informations.
    raft_store_router: Option<ServerRaftStoreRouter>,

    ratio_threshold: f64,

    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,

    gc_manager_handle: Arc<Mutex<Option<GCManagerHandle>>>,
}

impl<E: Engine> GCWorker<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        ratio_threshold: f64,
    ) -> GCWorker<E> {
        let worker = Arc::new(Mutex::new(
            WorkerBuilder::new("gc-worker")
                .pending_capacity(GC_MAX_PENDING_TASKS)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GCWorker {
            engine,
            local_storage,
            raft_store_router,
            ratio_threshold,
            worker,
            worker_scheduler,
            gc_manager_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start_auto_gc<S: GCSafePointProvider, R: RegionInfoProvider>(
        &self,
        cfg: AutoGCConfig<S, R>,
    ) -> Result<()> {
        let mut handle = self.gc_manager_handle.lock().unwrap();
        assert!(handle.is_none());
        let new_handle = GCManager::new(cfg, self.worker_scheduler.clone()).start()?;
        *handle = Some(new_handle);
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let runner = GCRunner::new(
            self.engine.clone(),
            self.local_storage.take(),
            self.raft_store_router.take(),
            self.ratio_threshold,
        );
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn stop(&self) -> Result<()> {
        // Stop GCWorkerThread
        if let Some(h) = self.gc_manager_handle.lock().unwrap().take() {
            h.stop()?;
        }
        // Stop self
        let h = self.worker.lock().unwrap().stop().unwrap();
        if let Err(e) = h.join() {
            Err(box_err!("failed to join gc_worker handle, err: {:?}", e))
        } else {
            Ok(())
        }
    }

    fn handle_schedule_error(e: ScheduleError<GCTask>) -> Result<()> {
        match e {
            ScheduleError::Full(task) => {
                GC_TOO_BUSY_COUNTER.inc();
                (task.take_callback())(Err(Error::GCWorkerTooBusy));
                Ok(())
            }
            _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
        }
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask::GC {
                ctx,
                safe_point,
                callback,
            })
            .or_else(Self::handle_schedule_error)
    }

    /// Clean up all keys in a range and quickly free the disk space. The range might span over
    /// multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
    /// on RocksDB, bypassing the Raft layer. User must promise that, after calling `destroy_range`,
    /// the range will never be accessed any more. However, `destroy_range` is allowed to be called
    /// multiple times on an single range.
    pub fn async_unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
            })
            .or_else(Self::handle_schedule_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use kvproto::metapb;
    use raftstore::store::SeekRegionFilter;
    use std::collections::BTreeMap;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use storage::engine::Result as EngineResult;
    use storage::{Mutation, Options, Storage, TestEngineBuilder, TestStorageBuilder};

    struct MockSafePointProvider {
        rx: Receiver<u64>,
    }

    impl GCSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> Result<u64> {
            // Error will be ignored by GCManager, which is equivalent to that the safe_point
            // is not updated.
            self.rx.try_recv().map_err(|e| box_err!(e))
        }
    }

    #[derive(Clone)]
    struct MockRegionInfoProvider {
        // start_key -> (region_id, end_key)
        regions: BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
    }

    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(
            &self,
            from: &[u8],
            _filter: SeekRegionFilter,
            _limit: u32,
        ) -> EngineResult<SeekRegionResult> {
            let from = from.to_vec();
            match self.regions.range(from..).next() {
                Some((start_key, (region_id, end_key))) => {
                    let mut region = metapb::Region::default();
                    region.set_id(*region_id);
                    region.set_start_key(start_key.clone());
                    region.set_end_key(end_key.clone());
                    let mut p = metapb::Peer::default();
                    p.set_store_id(1);
                    region.mut_peers().push(p);
                    Ok(SeekRegionResult::Found(region))
                }
                None => Ok(SeekRegionResult::Ended),
            }
        }
    }

    struct MockGCRunner {
        tx: Sender<GCTask>,
    }

    impl Runnable<GCTask> for MockGCRunner {
        fn run(&mut self, mut t: GCTask) {
            let cb = t.replace_callback(box |_| {});
            self.tx.send(t).unwrap();
            cb(Ok(()));
        }
    }

    /// A set of utilities that helps testing `GCManager`.
    /// The safe_point polling interval is set to 100 ms.
    struct GCManagerTestUtil {
        gc_manager: Option<GCManager<MockSafePointProvider, MockRegionInfoProvider>>,
        worker: Worker<GCTask>,
        safe_point_sender: Sender<u64>,
        gc_task_receiver: Receiver<GCTask>,
    }

    impl GCManagerTestUtil {
        pub fn new(regions: BTreeMap<Vec<u8>, (u64, Vec<u8>)>) -> Self {
            let mut worker = WorkerBuilder::new("test-gc-worker").create();
            let (gc_task_sender, gc_task_receiver) = channel();
            worker.start(MockGCRunner { tx: gc_task_sender }).unwrap();

            let (safe_point_sender, safe_point_receiver) = channel();

            let mut cfg = AutoGCConfig::new(
                MockSafePointProvider {
                    rx: safe_point_receiver,
                },
                MockRegionInfoProvider { regions },
                1,
            );
            cfg.poll_safe_point_interval = Duration::from_millis(100);
            cfg.always_check_safe_point = true;

            let gc_manager = GCManager::new(cfg, worker.scheduler());
            Self {
                gc_manager: Some(gc_manager),
                worker,
                safe_point_sender,
                gc_task_receiver,
            }
        }

        /// Collect `GCTask`s that GCManager tried to execute
        pub fn collect_scheduled_tasks(&self) -> Vec<GCTask> {
            self.gc_task_receiver.try_iter().collect()
        }

        pub fn add_next_safe_point(&self, safe_point: u64) {
            self.safe_point_sender.send(safe_point).unwrap();
        }

        pub fn stop(&mut self) {
            self.worker.stop().unwrap().join().unwrap();
        }
    }

    /// Run a round of auto GC and check if it correctly GC regions as expected.
    ///
    /// Param `regions` is a `Vec` of tuples which is `(start_key, end_key, region_id)`
    ///
    /// The first value in param `safe_points` will be used to initialize the GCManager, and the remaining
    /// values will be checked before every time GC-ing a region. If the length of `safe_points` is
    /// less than executed GC tasks, the last value will be used for extra GC tasks.
    ///
    /// Param `expected_gc_tasks` is a `Vec` of tuples which is `(region_id, safe_point)`.
    fn test_auto_gc(
        regions: Vec<(Vec<u8>, Vec<u8>, u64)>,
        safe_points: Vec<u64>,
        expected_gc_tasks: Vec<(u64, u64)>,
    ) {
        let regions: BTreeMap<_, _> = regions
            .into_iter()
            .map(|(start_key, end_key, id)| (start_key, (id, end_key)))
            .into_iter()
            .collect();

        let mut test_util = GCManagerTestUtil::new(regions);

        for safe_point in &safe_points {
            test_util.add_next_safe_point(*safe_point);
        }
        test_util.gc_manager.as_mut().unwrap().initialize().unwrap();

        test_util.gc_manager.as_mut().unwrap().work().unwrap();
        test_util.stop();

        let gc_tasks: Vec<_> = test_util
            .collect_scheduled_tasks()
            .iter()
            .map(|task| match task {
                GCTask::GC {
                    ctx, safe_point, ..
                } => (ctx.get_region_id(), *safe_point),
                _ => unreachable!(),
            })
            .collect();

        // Following code asserts gc_tasks == expected_gc_tasks
        assert_eq!(gc_tasks.len(), expected_gc_tasks.len());
        let all_passed = gc_tasks.into_iter().zip(expected_gc_tasks.into_iter()).all(
            |((region, safe_point), (expect_region, expect_safe_point))| {
                region == expect_region && safe_point == expect_safe_point
            },
        );
        assert!(all_passed);
    }

    #[test]
    fn test_make_context() {
        let mut peer = metapb::Peer::default();
        peer.set_id(233);
        peer.set_store_id(2333);

        let mut epoch = metapb::RegionEpoch::default();
        epoch.set_conf_ver(123);
        epoch.set_version(456);
        let mut region = metapb::Region::default();
        region.set_region_epoch(epoch.clone());
        region.set_id(789);

        let ctx = make_context(region.clone(), peer.clone());
        assert_eq!(ctx.get_region_id(), region.get_id());
        assert_eq!(ctx.get_peer(), &peer);
        assert_eq!(ctx.get_region_epoch(), &epoch);
    }

    #[test]
    fn test_update_safe_point() {
        let mut test_util = GCManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.safe_point, 0);
        test_util.add_next_safe_point(233);
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.safe_point, 233);

        let (tx, rx) = channel();
        ThreadBuilder::new()
            .spawn(move || {
                let safe_point = gc_manager.poll_next_safe_point().unwrap();
                tx.send(safe_point).unwrap();
            })
            .unwrap();
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(234);
        assert_eq!(rx.recv().unwrap(), 234);

        test_util.stop();
    }

    #[test]
    fn test_auto_gc_once() {
        // First region starts with empty and last region ends with empty.
        let regions = vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );

        // First region doesn't starts with empty and last region doesn't ends with empty.
        let regions = vec![
            (b"0".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"8".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );
    }

    #[test]
    fn test_auto_gc_rewinding() {
        for regions in vec![
            // First region starts with empty and last region ends with empty.
            vec![
                (b"".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"".to_vec(), 4),
            ],
            // First region doesn't starts with empty and last region doesn't ends with empty.
            vec![
                (b"0".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"8".to_vec(), 4),
            ],
        ] {
            test_auto_gc(
                regions.clone(),
                vec![233, 234],
                vec![(1, 234), (2, 234), (3, 234), (4, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 234],
                vec![(1, 233), (2, 234), (3, 234), (4, 234), (1, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 233, 234],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 233),
                    (4, 234),
                    (1, 234),
                    (2, 234),
                    (3, 234),
                ],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 234, 235],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 235),
                    (1, 235),
                    (2, 235),
                    (3, 235),
                ],
            );

            let mut safe_points = vec![233, 233, 233, 234, 234, 234, 235];
            // The logic of `work` wastes a loop when the last region's end_key is not null, so it
            // will check safe point one more time before GC-ing the first region after rewinding.
            if !regions.last().unwrap().1.is_empty() {
                safe_points.insert(5, 234);
            }
            test_auto_gc(
                regions.clone(),
                safe_points,
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 234),
                    (1, 234),
                    (2, 235),
                    (3, 235),
                    (4, 235),
                    (1, 235),
                ],
            );
        }
    }

    /// Assert the data in `storage` is the same as `expected_data`. Keys in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(storage: &Storage<E>, expected_data: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let scan_res = storage
            .async_scan(
                Context::default(),
                Key::from_encoded_slice(b""),
                None,
                expected_data.len() + 1,
                1,
                Options::default(),
            )
            .wait()
            .unwrap();

        let all_equal = scan_res
            .into_iter()
            .map(|res| res.unwrap())
            .zip(expected_data.iter())
            .all(|((k1, v1), (k2, v2))| &k1 == k2 && &v1 == v2);
        assert!(all_equal);
    }

    fn test_destroy_range_impl(
        init_keys: &[Vec<u8>],
        start_ts: u64,
        commit_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let db = engine.get_rocksdb();
        let storage = TestStorageBuilder::from_engine(engine)
            .local_storage(db)
            .build()
            .unwrap();

        // Convert keys to key value pairs, where the value is "value-{key}".
        let data: BTreeMap<_, _> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                (Key::from_raw(key).into_encoded(), value)
            })
            .collect();

        // Generate `Mutation`s from these keys.
        let mutations: Vec<_> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                Mutation::Put((Key::from_raw(key), value))
            })
            .collect();
        let primary = init_keys[0].clone();

        // Write these data to the storage.
        wait_op!(|cb| storage.async_prewrite(
            Context::default(),
            mutations,
            primary,
            start_ts,
            Options::default(),
            cb
        )).unwrap()
            .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.async_commit(Context::default(), keys, start_ts, commit_ts, cb))
            .unwrap()
            .unwrap();

        // Assert these data is successfully written to the storage.
        check_data(&storage, &data);

        let start_key = Key::from_raw(start_key);
        let end_key = Key::from_raw(end_key);

        // Calculate expected data set after deleting the range.
        let data: BTreeMap<_, _> = data
            .into_iter()
            .filter(|(k, _)| k < start_key.as_encoded() || k >= end_key.as_encoded())
            .collect();

        // Invoke unsafe destroy range.
        wait_op!(|cb| storage.async_unsafe_destroy_range(
            Context::default(),
            start_key,
            end_key,
            cb
        )).unwrap()
            .unwrap();

        // Check remaining data is as expected.
        check_data(&storage, &data);

        Ok(())
    }

    #[test]
    fn test_destroy_range() {
        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2",
            b"key4",
        ).unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        ).unwrap();

        test_destroy_range_impl(
            &[
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
                b"key6".to_vec(),
                b"key7".to_vec(),
            ],
            5,
            10,
            b"key1",
            b"key9",
        ).unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2\x00",
            b"key4",
        ).unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00\x00",
        ).unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00",
        ).unwrap();
    }
}
