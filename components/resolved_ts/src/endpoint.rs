// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, Snapshot};
use kvproto::errorpb;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ObserveChange, ObserveId, StoreMeta};
use raftstore::store::msg::{Callback, SignificantMsg};
use raftstore::store::util::RemoteLease;
use raftstore::store::RegionSnapshot;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};
use txn_types::{Key, TimeStamp};

use crate::advance::AdvanceTsWorker;
use crate::cmd::{ChangeLog, ChangeRow};
use crate::errors::Error;
use crate::observer::ChangeDataObserver;
use crate::resolver::Resolver;
use crate::scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool};
use crate::sinker::{CmdSinker, SinkCmd};

enum ResolverStatus {
    Pending {
        locks: Vec<PendingLock>,
        cancelled: Arc<AtomicBool>,
    },
    Ready,
}

enum PendingLock {
    Track {
        key: Key,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Key,
        start_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
    },
}

struct ObserveRegion {
    meta: Region,
    observe_id: ObserveId,
    // TODO: Get lease from raftstore.
    lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(meta: Region, resolved_ts: Arc<AtomicCell<TimeStamp>>) -> Self {
        ObserveRegion {
            resolver: Resolver::from_resolved_ts(meta.id, resolved_ts),
            meta,
            observe_id: ObserveId::new(),
            lease: None,
            resolver_status: ResolverStatus::Pending {
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        }
    }

    fn track_change_row(&mut self, rows: &[ChangeRow]) {
        match self.resolver_status {
            ResolverStatus::Pending { ref mut locks, .. } => {
                rows.iter().for_each(|row| match row {
                    ChangeRow::Prewrite { key, lock, .. } => locks.push(PendingLock::Track {
                        key: key.clone(),
                        start_ts: lock.ts,
                    }),
                    ChangeRow::Commit {
                        key,
                        commit_ts,
                        write,
                        ..
                    } => locks.push(PendingLock::Untrack {
                        key: key.clone(),
                        start_ts: write.start_ts,
                        commit_ts: *commit_ts,
                    }),
                })
            }
            ResolverStatus::Ready => rows.iter().for_each(|row| match row {
                ChangeRow::Prewrite { key, lock, .. } => self.resolver.track_lock(lock.ts, key),
                ChangeRow::Commit {
                    key,
                    commit_ts,
                    write,
                    ..
                } => self.resolver.untrack_lock(write.start_ts, *commit_ts, key),
            }),
        }
    }

    fn track_scan_locks(&mut self, entry: ScanEntry) {
        match entry {
            ScanEntry::Lock(locks) => {
                if let ResolverStatus::Ready = self.resolver_status {
                    panic!("region {:?} resolver has ready", self.meta.id)
                }
                for (key, lock) in locks {
                    self.resolver.track_lock(lock.ts, &key);
                }
            }
            ScanEntry::None => {
                let status = std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready);
                match status {
                    ResolverStatus::Pending { locks, .. } => {
                        locks.into_iter().for_each(|lock| match lock {
                            PendingLock::Track { key, start_ts } => {
                                self.resolver.track_lock(start_ts, &key)
                            }
                            PendingLock::Untrack {
                                key,
                                start_ts,
                                commit_ts,
                            } => self.resolver.untrack_lock(start_ts, commit_ts, &key),
                        })
                    }
                    ResolverStatus::Ready => panic!("region {:?} resolver has ready", self.meta.id),
                }
            }
            ScanEntry::TxnEntry(_) => panic!("unexpected entry type"),
        }
    }
}

pub struct Endpoint<T, E: KvEngine, C> {
    store_meta: Arc<Mutex<StoreMeta>>,
    regions: HashMap<u64, ObserveRegion>,
    raft_router: T,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    sinker: Option<C>,
    advance_worker: AdvanceTsWorker<T, E>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E, C> Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    pub fn new(
        scheduler: Scheduler<Task<E::Snapshot>>,
        raft_router: T,
        store_meta: Arc<Mutex<StoreMeta>>,
        advance_worker: AdvanceTsWorker<T, E>,
        scanner_pool: ScannerPool<T, E>,
        sinker: Option<C>,
    ) -> Self {
        let ep = Self {
            scheduler,
            raft_router,
            store_meta,
            advance_worker,
            scanner_pool,
            sinker,
            regions: HashMap::default(),
            _phantom: PhantomData::default(),
        };
        ep.register_advance_event();
        ep
    }

    fn register_region(&mut self, region: Region) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let observe_region = {
            let store_meta = self.store_meta.lock().unwrap();
            let reader = store_meta.readers.get(&region_id).expect("");
            ObserveRegion::new(region.clone(), reader.resolved_ts.clone())
        };
        let observe_id = observe_region.observe_id;
        let cancelled = match observe_region.resolver_status {
            ResolverStatus::Pending { ref cancelled, .. } => cancelled.clone(),
            ResolverStatus::Ready => panic!("illeagal created observe region"),
        };
        let scheduler = self.scheduler.clone();
        let scheduler1 = self.scheduler.clone();
        let scan_task = ScanTask {
            id: observe_region.observe_id.into_inner(),
            tag: String::new(),
            mode: ScanMode::LockOnly,
            region,
            checkpoint_ts: TimeStamp::zero(),
            cancelled: Box::new(move || cancelled.load(Ordering::Acquire)),
            send_entries: Box::new(move |entry| {
                scheduler
                    .schedule(Task::ScanLocks {
                        region_id,
                        observe_id,
                        entry,
                    })
                    .unwrap_or_else(|e| debug!("schedule resolved ts task failed"; "err" => ?e));
            }),
            before_start: None,
            on_error: Some(Box::new(move |region, error| {
                scheduler1
                    .schedule(Task::RegionError {
                        observe_id,
                        region,
                        error,
                    })
                    .unwrap();
            })),
        };
        self.scanner_pool.spawn_task(scan_task);
    }

    fn deregister_region(&mut self, observe_region: ObserveRegion) {
        let ObserveRegion {
            meta: region,
            observe_id,
            resolver_status,
            ..
        } = observe_region;
        let region_id = region.id;
        if let Err(e) = self.raft_router.significant_send(
            region_id,
            SignificantMsg::CaptureChange {
                cmd: ChangeCmd {
                    region_id,
                    observe_change: Some(ObserveChange {
                        id: observe_id,
                        range: ObserveRange::None,
                    }),
                },
                region_epoch: region.get_region_epoch().clone(),
                callback: Callback::None,
            },
        ) {
            info!("send msg to deregister region failed"; "region_id" => region_id, "err" => ?e);
        }
        if let ResolverStatus::Pending { cancelled, .. } = resolver_status {
            cancelled.store(true, Ordering::Release);
        }
    }

    fn region_destroyed(&mut self, region: &Region) {
        if let Some(observe_region) = self.regions.remove(&region.id) {
            debug!("region destroyed"; "region_id" => region.id);
            self.deregister_region(observe_region);
        }
    }

    fn region_updated(&mut self, region: Region) {
        if let Some(observe_region) = self.regions.remove(&region.id) {
            debug!("region updated"; "region_id" => region.id);
            self.deregister_region(observe_region);
            self.register_region(region);
        } else {
            debug!("update region failed, region has been deregisgered"; "region_id" => region.id);
        }
    }

    fn region_role_changed(&mut self, region: Region, role: StateRole) {
        match role {
            StateRole::Leader => self.register_region(region),
            _ => {
                if let Some(observe_region) = self.regions.remove(&region.id) {
                    self.deregister_region(observe_region)
                }
            }
        }
    }

    fn region_error(&mut self, region: Region, observe_id: ObserveId, error: Error) {
        if let Some(observe_region) = self.regions.get(&region.id) {
            if observe_region.observe_id != observe_id {
                return;
            }
            info!("region met error, recreate it"; "region_id" => region.id, "error" => ?error);
            return self.region_updated(region);
        }
    }

    fn advance_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp) {
        if regions.is_empty() {
            return;
        }
        for region_id in regions {
            if let Some(observe_region) = self.regions.get_mut(&region_id) {
                if let ResolverStatus::Ready = observe_region.resolver_status {
                    observe_region.resolver.resolve(ts);
                }
            }
        }
    }

    fn handle_change_log(
        &mut self,
        cmd_batch: Vec<CmdBatch>,
        snapshot: RegionSnapshot<E::Snapshot>,
    ) {
        let sink_cmds = cmd_batch
            .into_iter()
            .map(|batch| {
                if !batch.is_empty() {
                    let observe_region = self
                        .regions
                        .get_mut(&batch.region_id)
                        .expect("cannot find region to handle change log");
                    if observe_region.observe_id == batch.observe_id {
                        let region_id = observe_region.meta.id;
                        let change_logs: Vec<_> = ChangeLog::encode_change_log(region_id, batch);
                        for log in &change_logs {
                            match log {
                                ChangeLog::Rows { rows, .. } => {
                                    observe_region.track_change_row(rows)
                                }
                                _ => (),
                            }
                        }
                        return Some(SinkCmd {
                            logs: change_logs,
                            region_id,
                            observe_id: observe_region.observe_id,
                        });
                    } else {
                        debug!("resolved ts CmdBatch discarded";
                            "region_id" => batch.region_id,
                            "observe_id" => ?batch.observe_id,
                            "current" => ?observe_region.observe_id,
                        );
                    }
                }
                None
            })
            .filter_map(|v| v)
            .collect();
        if let Some(sinker) = self.sinker.as_mut() {
            sinker.sink_cmd(sink_cmds, snapshot);
        }
    }

    fn handle_scan_locks(&mut self, region_id: u64, observe_id: ObserveId, entry: ScanEntry) {
        match self.regions.get_mut(&region_id) {
            Some(observe_region) => {
                if observe_region.observe_id == observe_id {
                    observe_region.track_scan_locks(entry);
                }
            }
            None => {
                debug!("scan locks region not exist"; "region_id" => region_id, "observe_id" => ?observe_id);
            }
        }
    }

    fn register_advance_event(&self) {
        let regions = self.regions.keys().into_iter().copied().collect();
        self.advance_worker.register_advance_event(regions);
    }
}

pub enum Task<S: Snapshot> {
    RegionDestroyed(Region),
    RegionUpdated(Region),
    RegionRoleChanged {
        region: Region,
        role: StateRole,
    },
    RegionError {
        region: Region,
        observe_id: ObserveId,
        error: Error,
    },
    RegisterAdvanceEvent,
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
        snapshot: RegionSnapshot<S>,
    },
    ScanLocks {
        region_id: u64,
        observe_id: ObserveId,
        entry: ScanEntry,
    },
}

impl<S: Snapshot> fmt::Debug for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("ResolvedTsTask");
        match self {
            Task::RegionDestroyed(ref region) => de
                .field("name", &"region_destroyed")
                .field("region", &region)
                .finish(),
            Task::RegionUpdated(ref region) => de
                .field("name", &"region_updated")
                .field("region", &region)
                .finish(),
            Task::RegionRoleChanged {
                ref region,
                ref role,
            } => de
                .field("name", &"region_role_changed")
                .field("region", &region)
                .field("role", &role)
                .finish(),
            Task::RegionError {
                ref region,
                ref observe_id,
                ref error,
            } => de
                .field("name", &"region_error")
                .field("region", &region)
                .field("observe_id", &observe_id)
                .field("error", &error)
                .finish(),
            Task::AdvanceResolvedTs {
                ref regions,
                ref ts,
            } => de
                .field("name", &"advance_resolved_ts")
                .field("regions", &regions)
                .field("ts", &ts)
                .finish(),
            Task::ChangeLog { .. } => de.field("name", &"change_log").finish(),
            Task::ScanLocks {
                ref region_id,
                ref observe_id,
                ..
            } => de
                .field("name", &"scan_locks")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .finish(),
            Task::RegisterAdvanceEvent => de.field("name", &"register_advance_event").finish(),
        }
    }
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, E, C> Runnable for Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    type Task = Task<E::Snapshot>;

    fn run(&mut self, task: Task<E::Snapshot>) {
        debug!("run cdc task"; "task" => ?task);
        match task {
            Task::RegionDestroyed(ref region) => self.region_destroyed(region),
            Task::RegionUpdated(region) => self.region_updated(region),
            Task::RegionRoleChanged { region, role } => self.region_role_changed(region, role),
            Task::RegionError {
                region,
                observe_id,
                error,
            } => self.region_error(region, observe_id, error),
            Task::AdvanceResolvedTs { regions, ts } => self.advance_resolved_ts(regions, ts),
            Task::ChangeLog {
                cmd_batch,
                snapshot,
            } => self.handle_change_log(cmd_batch, snapshot),
            Task::ScanLocks {
                region_id,
                observe_id,
                entry,
            } => self.handle_scan_locks(region_id, observe_id, entry),
            Task::RegisterAdvanceEvent => self.register_advance_event(),
        }
    }
}
