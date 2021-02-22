// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

<<<<<<< HEAD
pub struct Observer<E: KvEngine> {
    cmd_batches: RefCell<Vec<CmdBatch>>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    need_old_value: bool,
}

impl<E: KvEngine> Observer<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        Observer {
            cmd_batches: RefCell::default(),
            scheduler,
            need_old_value: true,
        }
    }

    // Disable old value, currently only use in tests to avoid holding the snapshot
    // and cause data can not be deleted
    pub fn disable_old_value(&mut self) {
        self.need_old_value = false;
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        // 100 is the priority of the observer. CDC should have a high priority.
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }
}

impl<E: KvEngine> Clone for Observer<E> {
    fn clone(&self) -> Self {
        Self {
            cmd_batches: self.cmd_batches.clone(),
            scheduler: self.scheduler.clone(),
            need_old_value: self.need_old_value,
        }
    }
}

impl<E: KvEngine> Coprocessor for Observer<E> {}

impl<E: KvEngine> CmdObserver<E> for Observer<E> {
    fn on_prepare_for_apply(&self, cdc_id: ObserveID, rts_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(cdc_id, rts_id, region_id));
    }

    fn on_apply_cmd(&self, cdc_id: ObserveID, rts_id: ObserveID, region_id: u64, cmd: Cmd) {
=======
pub struct ChangeDataObserver<E: KvEngine> {
    cmd_batches: RefCell<Vec<CmdBatch>>,
    scheduler: Scheduler<Task<E::Snapshot>>,
}

impl<E: KvEngine> ChangeDataObserver<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        ChangeDataObserver {
            cmd_batches: RefCell::default(),
            scheduler,
        }
    }
}

impl<E: KvEngine> Coprocessor for ChangeDataObserver<E> {}

impl<E: KvEngine> CmdObserver<E> for ChangeDataObserver<E> {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, region_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
>>>>>>> copr: added sub duration and string
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .unwrap_or_else(|| panic!("region {} should exist some cmd batch", region_id))
<<<<<<< HEAD
            .push(cdc_id, rts_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        self.cmd_batches.borrow_mut().retain(|b| !b.is_empty());
=======
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
>>>>>>> copr: added sub duration and string
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            // TODO: only need it after enabling old value, may add a flag to indicate whether to get it.
<<<<<<< HEAD
            let snapshot = if self.need_old_value {
                Some(RegionSnapshot::from_snapshot(
                    Arc::new(engine.snapshot()),
                    Arc::new(region),
                ))
            } else {
                None
            };
=======
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
>>>>>>> copr: added sub duration and string
            if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
                cmd_batch: batches,
                snapshot,
            }) {
                info!("failed to schedule change log event"; "err" => ?e);
            }
        }
    }
}

<<<<<<< HEAD
impl<E: KvEngine> RoleObserver for Observer<E> {
=======
impl<E: KvEngine> RoleObserver for ChangeDataObserver<E> {
>>>>>>> copr: added sub duration and string
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if let Err(e) = self.scheduler.schedule(Task::RegionRoleChanged {
            role,
            region: ctx.region().clone(),
        }) {
            info!("failed to schedule region role changed event"; "err" => ?e);
        }
    }
}

<<<<<<< HEAD
impl<E: KvEngine> RegionChangeObserver for Observer<E> {
=======
impl<E: KvEngine> RegionChangeObserver for ChangeDataObserver<E> {
>>>>>>> copr: added sub duration and string
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
<<<<<<< HEAD
        // TODO: handle region update event
        if let RegionChangeEvent::Destroy = event {
            if let Err(e) = self
                .scheduler
                .schedule(Task::RegionDestroyed(ctx.region().clone()))
            {
                info!("failed to schedule region destroyed event"; "err" => ?e);
            }
=======
        match event {
            RegionChangeEvent::Destroy => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionDestroyed(ctx.region().clone()))
                {
                    info!("failed to schedule region destroyed event"; "err" => ?e);
                }
            }
            RegionChangeEvent::Update => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionUpdated(ctx.region().clone()))
                {
                    info!("failed to schedule region updated event"; "err" => ?e);
                }
            }
            RegionChangeEvent::Create => (),
>>>>>>> copr: added sub duration and string
        }
    }
}
