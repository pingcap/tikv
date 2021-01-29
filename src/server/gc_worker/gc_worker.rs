// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::executor::block_on;
use kvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};
use pd_client::{FeatureGate, PdClient};
use raftstore::coprocessor::{CoprocessorHost, RegionInfoProvider};
use raftstore::router::RaftStoreRouter;
use raftstore::store::msg::StoreMsg;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::time::{duration_to_sec, Limiter, SlowTimer};
use tikv_util::worker::{
    FutureRunnable, FutureScheduler, FutureWorker, Stopped as FutureWorkerStopped,
};
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use crate::storage::kv::{Engine, ScanMode, Statistics};
use crate::storage::mvcc::{check_need_gc, Error as MvccError, GcInfo, MvccReader, MvccTxn};

use super::applied_lock_collector::{AppliedLockCollector, Callback as LockCollectorCallback};
use super::config::{GcConfig, GcWorkerConfigManager};
use super::gc_manager::{AutoGcConfig, GcManager, GcManagerHandle};
use super::{Callback, CompactionFilterInitializer, Error, ErrorInner, Result};
use crate::storage::txn::gc;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_EXECUTING_TASKS: usize = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

/// Provides safe point.
pub trait GcSafePointProvider: Send + 'static {
    fn get_safe_point(&self) -> Result<TimeStamp>;
}

impl<T: PdClient + 'static> GcSafePointProvider for Arc<T> {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        block_on(self.get_gc_safe_point())
            .map(Into::into)
            .map_err(|e| box_err!("failed to get safe point from PD: {:?}", e))
    }
}

pub enum GcTask {
    Gc {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        safe_point: TimeStamp,
        callback: Callback<()>,
    },
    UnsafeDestroyRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    },
    PhysicalScanLock {
        ctx: Context,
        max_ts: TimeStamp,
        start_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    },
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&GcConfig, &Limiter) + Send>),
}

impl GcTask {
    pub fn get_enum_label(&self) -> GcCommandKind {
        match self {
            GcTask::Gc { .. } => GcCommandKind::gc,
            GcTask::UnsafeDestroyRange { .. } => GcCommandKind::unsafe_destroy_range,
            GcTask::PhysicalScanLock { .. } => GcCommandKind::physical_scan_lock,
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => GcCommandKind::validate_config,
        }
    }
}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GcTask::Gc {
                start_key,
                end_key,
                safe_point,
                ..
            } => f
                .debug_struct("GC")
                .field("start_key", &log_wrappers::Value::key(&start_key))
                .field("end_key", &log_wrappers::Value::key(&end_key))
                .field("safe_point", safe_point)
                .finish(),
            GcTask::UnsafeDestroyRange {
                start_key, end_key, ..
            } => f
                .debug_struct("UnsafeDestroyRange")
                .field("start_key", &format!("{}", start_key))
                .field("end_key", &format!("{}", end_key))
                .finish(),
            GcTask::PhysicalScanLock { max_ts, .. } => f
                .debug_struct("PhysicalScanLock")
                .field("max_ts", max_ts)
                .finish(),
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => write!(f, "Validate gc worker config"),
        }
    }
}

/// Used to perform GC operations on the engine.
struct GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    engine: E,

    raft_store_router: RR,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    cfg: GcConfig,
    cfg_tracker: Tracker<GcConfig>,

    stats: Statistics,
}

impl<E, RR> GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    pub fn new(
        engine: E,
        raft_store_router: RR,
        cfg_tracker: Tracker<GcConfig>,
        cfg: GcConfig,
    ) -> Self {
        let limiter = Limiter::new(if cfg.max_write_bytes_per_sec.0 > 0 {
            cfg.max_write_bytes_per_sec.0 as f64
        } else {
            INFINITY
        });
        Self {
            engine,
            raft_store_router,
            limiter,
            cfg,
            cfg_tracker,
            stats: Statistics::default(),
        }
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do further check after
    /// getting snapshot.
    fn need_gc(&self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> bool {
        let props = match self
            .engine
            .get_mvcc_properties_cf(CF_WRITE, safe_point, &start_key, &end_key)
        {
            Some(c) => c,
            None => return true,
        };
        if check_need_gc(safe_point, self.cfg.ratio_threshold, &props) {
            info!(
                "range needs GC";
                "start" => hex::encode_upper(start_key),
                "end" => hex::encode_upper(end_key),
                "props" => ?props,
            );
            return true;
        }
        false
    }

    /// Cleans up outdated data.
    fn gc_key(
        &mut self,
        safe_point: TimeStamp,
        key: &Key,
        gc_info: &mut GcInfo,
        txn: &mut MvccTxn<E::Snap>,
    ) -> Result<()> {
        let next_gc_info = gc(txn, key.clone(), safe_point)?;
        gc_info.found_versions += next_gc_info.found_versions;
        gc_info.deleted_versions += next_gc_info.deleted_versions;
        gc_info.is_completed = next_gc_info.is_completed;
        self.stats.add(&txn.take_statistics());
        Ok(())
    }

    fn new_txn(snap: E::Snap) -> MvccTxn<E::Snap> {
        // TODO txn only used for GC, but this is hacky, maybe need an Option?
        let concurrency_manager = ConcurrencyManager::new(1.into());
        MvccTxn::for_scan(
            snap,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            false,
            concurrency_manager,
        )
    }

    fn flush_txn(txn: MvccTxn<E::Snap>, limiter: &Limiter, engine: &E) -> Result<()> {
        let write_size = txn.write_size();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            limiter.blocking_consume(write_size);
            engine.modify_on_kv_engine(modifies)?;
        }
        Ok(())
    }

    fn gc(&mut self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> Result<()> {
        if !self.need_gc(start_key, end_key, safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return Ok(());
        }

        let mut reader = MvccReader::new(
            self.engine.snapshot_on_kv_engine(start_key, end_key)?,
            Some(ScanMode::Forward),
            false,
            IsolationLevel::Si,
        );

        let mut next_key = Some(Key::from_encoded_slice(start_key));
        while next_key.is_some() {
            // Scans at most `GcConfig.batch_keys` keys.
            let (keys, updated_next_key) = reader.scan_keys(next_key, self.cfg.batch_keys)?;
            next_key = updated_next_key;

            if keys.is_empty() {
                GC_EMPTY_RANGE_COUNTER.inc();
                break;
            }

            let mut keys = keys.into_iter();
            let mut txn = Self::new_txn(self.engine.snapshot_on_kv_engine(start_key, end_key)?);
            let (mut next_gc_key, mut gc_info) = (keys.next(), GcInfo::default());
            while let Some(ref key) = next_gc_key {
                if let Err(e) = self.gc_key(safe_point, key, &mut gc_info, &mut txn) {
                    error!(?e; "GC meets failure"; "key" => %key,);
                    // Switch to the next key if meets failure.
                    gc_info.is_completed = true;
                }
                if gc_info.is_completed {
                    if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                        debug!(
                            "GC found plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.found_versions,
                        );
                    }
                    if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                        debug!(
                            "GC deleted plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.deleted_versions,
                        );
                    }
                    next_gc_key = keys.next();
                    gc_info = GcInfo::default();
                } else {
                    Self::flush_txn(txn, &self.limiter, &self.engine)?;
                    txn = Self::new_txn(self.engine.snapshot_on_kv_engine(start_key, end_key)?);
                }
            }
            Self::flush_txn(txn, &self.limiter, &self.engine)?;
        }

        self.stats.add(reader.get_statistics());
        debug!(
            "gc has finished";
            "start_key" => log_wrappers::Value::key(start_key),
            "end_key" => log_wrappers::Value::key(end_key),
            "safe_point" => safe_point
        );
        Ok(())
    }

    fn unsafe_destroy_range(&self, _: &Context, start_key: &Key, end_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy range started";
            "start_key" => %start_key, "end_key" => %end_key
        );

        let local_storage = self.engine.kv_engine();

        // Convert keys to RocksDB layer form
        // TODO: Logic coupled with raftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let start_data_key = keys::data_key(start_key.as_encoded());
        let end_data_key = keys::data_end_key(end_key.as_encoded());

        let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];

        // First, use DeleteStrategy::DeleteFiles to free as much disk space as possible
        let delete_files_start_time = Instant::now();
        for cf in cfs {
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteFiles,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_files_in_range_cf"; "err" => ?e);
                    e
                })?;
        }

        info!(
            "unsafe destroy range finished deleting files in range";
            "start_key" => %start_key, "end_key" => %end_key,
            "cost_time" => ?delete_files_start_time.elapsed(),
        );

        // Then, delete all remaining keys in the range.
        let cleanup_all_start_time = Instant::now();
        for cf in cfs {
            // TODO: set use_delete_range with config here.
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteByKey,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_all_in_range_cf"; "err" => ?e);
                    e
                })?;
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteBlobs,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_blob_files_in_range"; "err" => ?e);
                    e
                })?;
        }

        info!(
            "unsafe destroy range finished cleaning up all";
            "start_key" => %start_key, "end_key" => %end_key, "cost_time" => ?cleanup_all_start_time.elapsed(),
        );

        self.raft_store_router
            .send_store_msg(StoreMsg::ClearRegionSizeInRange {
                start_key: start_key.as_encoded().to_vec(),
                end_key: end_key.as_encoded().to_vec(),
            })
            .unwrap_or_else(|e| {
                // Warn and ignore it.
                warn!("unsafe destroy range: failed sending ClearRegionSizeInRange"; "err" => ?e);
            });

        Ok(())
    }

    fn handle_physical_scan_lock(
        &self,
        _: &Context,
        max_ts: TimeStamp,
        start_key: &Key,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        let snap = self
            .engine
            .snapshot_on_kv_engine(start_key.as_encoded(), &[])
            .unwrap();
        let mut reader = MvccReader::new(snap, Some(ScanMode::Forward), false, IsolationLevel::Si);
        let (locks, _) = reader.scan_locks(Some(start_key), None, |l| l.ts <= max_ts, limit)?;

        let mut lock_infos = Vec::with_capacity(locks.len());
        for (key, lock) in locks {
            let raw_key = key.into_raw().map_err(MvccError::from)?;
            lock_infos.push(lock.into_lock_info(raw_key));
        }
        Ok(lock_infos)
    }

    fn update_statistics_metrics(&mut self) {
        let stats = mem::take(&mut self.stats);

        for (cf, details) in stats.details_enum().iter() {
            for (tag, count) in details.iter() {
                GC_KEYS_COUNTER_STATIC
                    .get(*cf)
                    .get(*tag)
                    .inc_by(*count as i64);
            }
        }
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            let limit = incoming.max_write_bytes_per_sec.0;
            self.limiter
                .set_speed_limit(if limit > 0 { limit as f64 } else { INFINITY });
            self.cfg = incoming.clone();
        }
    }
}

impl<E, RR> FutureRunnable<GcTask> for GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    #[inline]
    fn run(&mut self, task: GcTask) {
        let enum_label = task.get_enum_label();

        GC_GCTASK_COUNTER_STATIC.get(enum_label).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let update_metrics = |is_err| {
            GC_TASK_DURATION_HISTOGRAM_VEC
                .with_label_values(&[enum_label.get_str()])
                .observe(duration_to_sec(timer.elapsed()));

            if is_err {
                GC_GCTASK_FAIL_COUNTER_STATIC.get(enum_label).inc();
            }
        };

        // Refresh config before handle task
        self.refresh_cfg();

        match task {
            GcTask::Gc {
                start_key,
                end_key,
                safe_point,
                callback,
                ..
            } => {
                let res = self.gc(&start_key, &end_key, safe_point);
                update_metrics(res.is_err());
                callback(res);
                self.update_statistics_metrics();
                slow_log!(
                    T timer,
                    "GC on range [{}, {}), safe_point {}",
                    log_wrappers::Value::key(&start_key),
                    log_wrappers::Value::key(&end_key),
                    safe_point
                );
            }
            GcTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
            } => {
                let res = self.unsafe_destroy_range(&ctx, &start_key, &end_key);
                update_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "UnsafeDestroyRange start_key {:?}, end_key {:?}",
                    start_key,
                    end_key
                );
            }
            GcTask::PhysicalScanLock {
                ctx,
                max_ts,
                start_key,
                limit,
                callback,
            } => {
                let res = self.handle_physical_scan_lock(&ctx, max_ts, &start_key, limit);
                update_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "PhysicalScanLock start_key {:?}, max_ts {}, limit {}",
                    start_key,
                    max_ts,
                    limit,
                );
            }
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(f) => {
                f(&self.cfg, &self.limiter);
            }
        };
    }
}

/// When we failed to schedule a `GcTask` to `GcRunner`, use this to handle the `ScheduleError`.
fn handle_gc_task_schedule_error(e: FutureWorkerStopped<GcTask>) -> Result<()> {
    error!("failed to schedule gc task"; "err" => %e);
    Err(box_err!("failed to schedule gc task: {:?}", e))
}

/// Schedules a `GcTask` to the `GcRunner`.
fn schedule_gc(
    scheduler: &FutureScheduler<GcTask>,
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    safe_point: TimeStamp,
    callback: Callback<()>,
) -> Result<()> {
    scheduler
        .schedule(GcTask::Gc {
            region_id,
            start_key,
            end_key,
            safe_point,
            callback,
        })
        .or_else(handle_gc_task_schedule_error)
}

/// Does GC synchronously.
pub fn sync_gc(
    scheduler: &FutureScheduler<GcTask>,
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    safe_point: TimeStamp,
) -> Result<()> {
    wait_op!(|callback| schedule_gc(scheduler, region_id, start_key, end_key, safe_point, callback))
        .unwrap_or_else(|| {
            error!("failed to receive result of gc");
            Err(box_err!("gc_worker: failed to receive result of gc"))
        })
}

/// Used to schedule GC operations.
pub struct GcWorker<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine> + 'static,
{
    engine: E,

    /// `raft_store_router` is useful to signal raftstore clean region size informations.
    raft_store_router: RR,

    config_manager: GcWorkerConfigManager,

    /// How many requests are scheduled from outside and unfinished.
    scheduled_tasks: Arc<AtomicUsize>,

    /// How many strong references. The worker will be stopped
    /// once there are no more references.
    refs: Arc<AtomicUsize>,
    worker: Arc<Mutex<FutureWorker<GcTask>>>,
    worker_scheduler: FutureScheduler<GcTask>,

    applied_lock_collector: Option<Arc<AppliedLockCollector>>,

    gc_manager_handle: Arc<Mutex<Option<GcManagerHandle>>>,
    feature_gate: FeatureGate,
}

impl<E, RR> Clone for GcWorker<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    #[inline]
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);

        Self {
            engine: self.engine.clone(),
            raft_store_router: self.raft_store_router.clone(),
            config_manager: self.config_manager.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
            refs: self.refs.clone(),
            worker: self.worker.clone(),
            worker_scheduler: self.worker_scheduler.clone(),
            applied_lock_collector: self.applied_lock_collector.clone(),
            gc_manager_handle: self.gc_manager_handle.clone(),
            feature_gate: self.feature_gate.clone(),
        }
    }
}

impl<E, RR> Drop for GcWorker<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine> + 'static,
{
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        if refs != 1 {
            return;
        }

        let r = self.stop();
        if let Err(e) = r {
            error!(?e; "Failed to stop gc_worker");
        }
    }
}

impl<E, RR> GcWorker<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    pub fn new(
        engine: E,
        raft_store_router: RR,
        cfg: GcConfig,
        feature_gate: FeatureGate,
    ) -> GcWorker<E, RR> {
        let worker = Arc::new(Mutex::new(FutureWorker::new("gc-worker")));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GcWorker {
            engine,
            raft_store_router,
            config_manager: GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg))),
            scheduled_tasks: Arc::new(AtomicUsize::new(0)),
            refs: Arc::new(AtomicUsize::new(1)),
            worker,
            worker_scheduler,
            applied_lock_collector: None,
            gc_manager_handle: Arc::new(Mutex::new(None)),
            feature_gate,
        }
    }

    pub fn start_auto_gc<S: GcSafePointProvider, R: RegionInfoProvider>(
        &self,
        cfg: AutoGcConfig<S, R>,
        safe_point: Arc<AtomicU64>, // Store safe point here.
    ) -> Result<()> {
        assert!(
            cfg.self_store_id > 0,
            "AutoGcConfig::self_store_id shouldn't be 0"
        );

        let kvdb = self.engine.kv_engine();
        let cfg_mgr = self.config_manager.clone();
        let feature_gate = self.feature_gate.clone();
        kvdb.init_compaction_filter(safe_point.clone(), cfg_mgr, feature_gate);

        let mut handle = self.gc_manager_handle.lock().unwrap();
        assert!(handle.is_none());
        let new_handle = GcManager::new(
            cfg,
            safe_point,
            self.worker_scheduler.clone(),
            self.config_manager.clone(),
            self.feature_gate.clone(),
        )
        .start()?;
        *handle = Some(new_handle);
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let runner = GcRunner::new(
            self.engine.clone(),
            self.raft_store_router.clone(),
            self.config_manager.0.clone().tracker("gc-woker".to_owned()),
            self.config_manager.value().clone(),
        );
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn start_observe_lock_apply(
        &mut self,
        coprocessor_host: &mut CoprocessorHost<RocksEngine>,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()> {
        assert!(self.applied_lock_collector.is_none());
        let collector = Arc::new(AppliedLockCollector::new(
            coprocessor_host,
            concurrency_manager,
        )?);
        self.applied_lock_collector = Some(collector);
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        // Stop GcManager.
        if let Some(h) = self.gc_manager_handle.lock().unwrap().take() {
            h.stop()?;
        }
        // Stop self.
        if let Some(h) = self.worker.lock().unwrap().stop() {
            if let Err(e) = h.join() {
                return Err(box_err!("failed to join gc_worker handle, err: {:?}", e));
            }
        }
        Ok(())
    }

    pub fn scheduler(&self) -> FutureScheduler<GcTask> {
        self.worker_scheduler.clone()
    }

    /// Check whether GCWorker is busy. If busy, callback will be invoked with an error that
    /// indicates GCWorker is busy; otherwise, return a new callback that invokes the original
    /// callback as well as decrease the scheduled task counter.
    fn check_is_busy<T: 'static>(&self, callback: Callback<T>) -> Option<Callback<T>> {
        if self.scheduled_tasks.fetch_add(1, Ordering::SeqCst) >= GC_MAX_EXECUTING_TASKS {
            self.scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(Err(Error::from(ErrorInner::GcWorkerTooBusy)));
            return None;
        }
        let scheduled_tasks = Arc::clone(&self.scheduled_tasks);
        Some(Box::new(move |r| {
            scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(r);
        }))
    }

    /// Only for tests.
    pub fn gc(&self, safe_point: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            let start_key = vec![];
            let end_key = vec![];
            self.worker_scheduler
                .schedule(GcTask::Gc {
                    region_id: 0,
                    start_key,
                    end_key,
                    safe_point,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    /// Cleans up all keys in a range and quickly free the disk space. The range might span over
    /// multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
    /// on RocksDB, bypassing the Raft layer. User must promise that, after calling `destroy_range`,
    /// the range will never be accessed any more. However, `destroy_range` is allowed to be called
    /// multiple times on an single range.
    pub fn unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.unsafe_destroy_range.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::UnsafeDestroyRange {
                    ctx,
                    start_key,
                    end_key,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn get_config_manager(&self) -> GcWorkerConfigManager {
        self.config_manager.clone()
    }

    pub fn physical_scan_lock(
        &self,
        ctx: Context,
        max_ts: TimeStamp,
        start_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.physical_scan_lock.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::PhysicalScanLock {
                    ctx,
                    max_ts,
                    start_key,
                    limit,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn start_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.start_collecting(max_ts, callback))
    }

    pub fn get_collected_locks(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.get_collected_locks(max_ts, callback))
    }

    pub fn stop_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.stop_collecting(max_ts, callback))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;

    use engine_rocks::RocksSnapshot;
    use engine_traits::KvEngine;
    use futures::executor::block_on;
    use kvproto::{kvrpcpb::Op, metapb};
    use raftstore::router::RaftStoreBlackHole;
    use raftstore::store::RegionSnapshot;
    use tikv_util::codec::number::NumberEncoder;
    use tikv_util::future::paired_future_callback;
    use txn_types::Mutation;

    use crate::storage::kv::{
        self, write_modifies, Callback as EngineCallback, Modify, Result as EngineResult,
        SnapContext, TestEngineBuilder, WriteData,
    };
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::{txn::commands, Engine, Storage, TestStorageBuilder};

    use super::*;

    /// A wrapper of engine that adds the 'z' prefix to keys internally.
    /// For test engines, they writes keys into db directly, but in production a 'z' prefix will be
    /// added to keys by raftstore layer before writing to db. Some functionalities of `GCWorker`
    /// bypasses Raft layer, so they needs to know how data is actually represented in db. This
    /// wrapper allows test engines write 'z'-prefixed keys to db.
    #[derive(Clone)]
    struct PrefixedEngine(kv::RocksEngine);

    impl Engine for PrefixedEngine {
        // Use RegionSnapshot which can remove the z prefix internally.
        type Snap = RegionSnapshot<RocksSnapshot>;
        type Local = RocksEngine;

        fn kv_engine(&self) -> RocksEngine {
            self.0.kv_engine()
        }

        fn snapshot_on_kv_engine(
            &self,
            start_key: &[u8],
            end_key: &[u8],
        ) -> kv::Result<Self::Snap> {
            let mut region = metapb::Region::default();
            region.set_start_key(start_key.to_owned());
            region.set_end_key(end_key.to_owned());
            // Use a fake peer to avoid panic.
            region.mut_peers().push(Default::default());
            Ok(RegionSnapshot::from_snapshot(
                Arc::new(self.kv_engine().snapshot()),
                Arc::new(region),
            ))
        }

        fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
            for modify in &mut modifies {
                match modify {
                    Modify::Delete(_, ref mut key) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::Put(_, ref mut key, _) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::DeleteRange(_, ref mut key1, ref mut key2, _) => {
                        let bytes = keys::data_key(key1.as_encoded());
                        *key1 = Key::from_encoded(bytes);
                        let bytes = keys::data_end_key(key2.as_encoded());
                        *key2 = Key::from_encoded(bytes);
                    }
                }
            }
            write_modifies(&self.kv_engine(), modifies)
        }

        fn async_write(
            &self,
            ctx: &Context,
            mut batch: WriteData,
            callback: EngineCallback<()>,
        ) -> EngineResult<()> {
            batch.modifies.iter_mut().for_each(|modify| match modify {
                Modify::Delete(_, ref mut key) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::Put(_, ref mut key, _) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::DeleteRange(_, ref mut start_key, ref mut end_key, _) => {
                    *start_key = Key::from_encoded(keys::data_key(start_key.as_encoded()));
                    *end_key = Key::from_encoded(keys::data_end_key(end_key.as_encoded()));
                }
            });
            self.0.async_write(ctx, batch, callback)
        }

        fn async_snapshot(
            &self,
            ctx: SnapContext<'_>,
            callback: EngineCallback<Self::Snap>,
        ) -> EngineResult<()> {
            self.0.async_snapshot(
                ctx,
                Box::new(move |(cb_ctx, r)| {
                    callback((
                        cb_ctx,
                        r.map(|snap| {
                            let mut region = metapb::Region::default();
                            // Add a peer to pass initialized check.
                            region.mut_peers().push(metapb::Peer::default());
                            RegionSnapshot::from_snapshot(snap, Arc::new(region))
                        }),
                    ))
                }),
            )
        }
    }

    /// Assert the data in `storage` is the same as `expected_data`. Keys in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(
        storage: &Storage<E, DummyLockManager>,
        expected_data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let scan_res = block_on(storage.scan(
            Context::default(),
            Key::from_encoded_slice(b""),
            None,
            expected_data.len() + 1,
            0,
            1.into(),
            false,
            false,
        ))
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
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let storage =
            TestStorageBuilder::from_engine_and_lock_mgr(engine.clone(), DummyLockManager {})
                .build()
                .unwrap();
        let gate = FeatureGate::default();
        gate.set_version("5.0.0").unwrap();
        let mut gc_worker = GcWorker::new(engine, RaftStoreBlackHole, GcConfig::default(), gate);
        gc_worker.start().unwrap();
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

        let start_ts = start_ts.into();

        // Write these data to the storage.
        wait_op!(|cb| storage.sched_txn_command(
            commands::Prewrite::with_defaults(mutations, primary, start_ts),
            cb,
        ))
        .unwrap()
        .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.sched_txn_command(
            commands::Commit::new(keys, start_ts, commit_ts.into(), Context::default()),
            cb
        ))
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
        wait_op!(|cb| gc_worker.unsafe_destroy_range(Context::default(), start_key, end_key, cb))
            .unwrap()
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
        )
        .unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        )
        .unwrap();

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
        )
        .unwrap();

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
        )
        .unwrap();

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
        )
        .unwrap();

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
        )
        .unwrap();
    }

    #[test]
    fn test_physical_scan_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let prefixed_engine = PrefixedEngine(engine);
        let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            prefixed_engine.clone(),
            DummyLockManager {},
        )
        .build()
        .unwrap();
        let mut gc_worker = GcWorker::new(
            prefixed_engine,
            RaftStoreBlackHole,
            GcConfig::default(),
            FeatureGate::default(),
        );
        gc_worker.start().unwrap();

        let physical_scan_lock = |max_ts: u64, start_key, limit| {
            let (cb, f) = paired_future_callback();
            gc_worker
                .physical_scan_lock(Context::default(), max_ts.into(), start_key, limit, cb)
                .unwrap();
            block_on(f).unwrap()
        };

        let mut expected_lock_info = Vec::new();

        // Put locks into the storage.
        for i in 0..50 {
            let mut k = vec![];
            k.encode_u64(i).unwrap();
            let v = k.clone();

            let mutation = Mutation::Put((Key::from_raw(&k), v));

            let lock_ts = 10 + i % 3;

            // Collect all locks with ts <= 11 to check the result of physical_scan_lock.
            if lock_ts <= 11 {
                let mut info = LockInfo::default();
                info.set_primary_lock(k.clone());
                info.set_lock_version(lock_ts);
                info.set_key(k.clone());
                info.set_lock_type(Op::Put);
                expected_lock_info.push(info)
            }

            let (tx, rx) = channel();
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(vec![mutation], k, lock_ts.into()),
                    Box::new(move |res| tx.send(res).unwrap()),
                )
                .unwrap();
            rx.recv()
                .unwrap()
                .unwrap()
                .locks
                .into_iter()
                .for_each(|r| r.unwrap());
        }

        let res = physical_scan_lock(11, Key::from_raw(b""), 50).unwrap();
        assert_eq!(res, expected_lock_info);

        let res = physical_scan_lock(11, Key::from_raw(b""), 5).unwrap();
        assert_eq!(res[..], expected_lock_info[..5]);

        let mut start_key = vec![];
        start_key.encode_u64(4).unwrap();
        let res = physical_scan_lock(11, Key::from_raw(&start_key), 6).unwrap();
        // expected_locks[3] is the key 4.
        assert_eq!(res[..], expected_lock_info[3..9]);
    }
}
