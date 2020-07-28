// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Commands used in the transaction system
#[macro_use]
mod macros;
mod acquire_pessimistic_lock;
mod check_txn_status;
mod cleanup;
mod commit;
mod mvcc_by_key;
mod mvcc_by_start_ts;
mod pause;
mod pessimistic_rollback;
mod prewrite;
mod prewrite_pessimistic;
mod resolve_lock;
mod resolve_lock_lite;
mod resolve_lock_readphase;
mod rollback;
mod scan_lock;
mod txn_heart_beat;

pub use acquire_pessimistic_lock::AcquirePessimisticLock;
pub use check_txn_status::CheckTxnStatus;
pub use cleanup::Cleanup;
pub use commit::Commit;
pub use mvcc_by_key::MvccByKey;
pub use mvcc_by_start_ts::MvccByStartTs;
pub use pause::Pause;
pub use pessimistic_rollback::PessimisticRollback;
pub use prewrite::Prewrite;
pub use prewrite_pessimistic::PrewritePessimistic;
pub use resolve_lock::ResolveLock;
pub use resolve_lock_lite::ResolveLockLite;
pub use resolve_lock_readphase::ResolveLockReadPhase;
pub use rollback::Rollback;
pub use scan_lock::ScanLock;
pub use txn_heart_beat::TxnHeartBeat;

use std::fmt::{self, Debug, Display, Formatter};
use std::iter::{self, FromIterator};
use std::marker::PhantomData;

use kvproto::kvrpcpb::*;
use txn_types::{Key, TimeStamp};

use crate::storage::lock_manager::WaitTimeout;
use crate::storage::metrics;
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::{
    MvccInfo, PessimisticLockRes, PrewriteResult, StorageCallbackType, TxnStatus,
};
use crate::storage::Result;
use tikv_util::collections::HashMap;

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](Storage) with functions like
/// [`Storage::prewrite`](Storage::prewrite) trait and are executed asynchronously.
// Logic related to these can be found in the `src/storage/txn/proccess.rs::process_write_impl` function.
pub enum Command {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticLock(AcquirePessimisticLock),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    ScanLock(ScanLock),
    ResolveLockReadPhase(ResolveLockReadPhase),
    ResolveLock(ResolveLock),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

pub struct TypedCommand<T> {
    pub cmd: Command,
    _pd: PhantomData<T>,
}

impl<T: StorageCallbackType> From<Command> for TypedCommand<T> {
    fn from(cmd: Command) -> TypedCommand<T> {
        TypedCommand {
            cmd,
            _pd: PhantomData,
        }
    }
}

impl<T> From<TypedCommand<T>> for Command {
    fn from(t: TypedCommand<T>) -> Command {
        t.cmd
    }
}

impl From<PrewriteRequest> for TypedCommand<PrewriteResult> {
    fn from(mut req: PrewriteRequest) -> Self {
        let for_update_ts = req.get_for_update_ts();
        if for_update_ts == 0 {
            Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                req.get_skip_constraint_check(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                if req.get_use_async_commit() {
                    Some(req.get_secondaries().into())
                } else {
                    None
                },
                req.take_context(),
            )
        } else {
            let is_pessimistic_lock = req.take_is_pessimistic_lock();
            let mutations = req
                .take_mutations()
                .into_iter()
                .map(Into::into)
                .zip(is_pessimistic_lock.into_iter())
                .collect();
            PrewritePessimistic::new(
                mutations,
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                for_update_ts.into(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                req.take_context(),
            )
        }
    }
}

impl From<PessimisticLockRequest> for TypedCommand<Result<PessimisticLockRes>> {
    fn from(mut req: PessimisticLockRequest) -> Self {
        let keys = req
            .take_mutations()
            .into_iter()
            .map(|x| match x.get_op() {
                Op::PessimisticLock => (
                    Key::from_raw(x.get_key()),
                    x.get_assertion() == Assertion::NotExist,
                ),
                _ => panic!("mismatch Op in pessimistic lock mutations"),
            })
            .collect();

        AcquirePessimisticLock::new(
            keys,
            req.take_primary_lock(),
            req.get_start_version().into(),
            req.get_lock_ttl(),
            req.get_is_first_lock(),
            req.get_for_update_ts().into(),
            WaitTimeout::from_encoded(req.get_wait_timeout()),
            req.get_return_values(),
            req.get_min_commit_ts().into(),
            req.take_context(),
        )
    }
}

impl From<CommitRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CommitRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        Commit::new(
            keys,
            req.get_start_version().into(),
            req.get_commit_version().into(),
            req.take_context(),
        )
    }
}

impl From<CleanupRequest> for TypedCommand<()> {
    fn from(mut req: CleanupRequest) -> Self {
        Cleanup::new(
            Key::from_raw(req.get_key()),
            req.get_start_version().into(),
            req.get_current_ts().into(),
            req.take_context(),
        )
    }
}

impl From<BatchRollbackRequest> for TypedCommand<()> {
    fn from(mut req: BatchRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
        Rollback::new(keys, req.get_start_version().into(), req.take_context())
    }
}

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<Result<()>>> {
    fn from(mut req: PessimisticRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        PessimisticRollback::new(
            keys,
            req.get_start_version().into(),
            req.get_for_update_ts().into(),
            req.take_context(),
        )
    }
}

impl From<TxnHeartBeatRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: TxnHeartBeatRequest) -> Self {
        TxnHeartBeat::new(
            Key::from_raw(req.get_primary_lock()),
            req.get_start_version().into(),
            req.get_advise_lock_ttl(),
            req.take_context(),
        )
    }
}

impl From<CheckTxnStatusRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CheckTxnStatusRequest) -> Self {
        CheckTxnStatus::new(
            Key::from_raw(req.get_primary_key()),
            req.get_lock_ts().into(),
            req.get_caller_start_ts().into(),
            req.get_current_ts().into(),
            req.get_rollback_if_not_exist(),
            req.take_context(),
        )
    }
}

impl From<ScanLockRequest> for TypedCommand<Vec<LockInfo>> {
    fn from(mut req: ScanLockRequest) -> Self {
        let start_key = if req.get_start_key().is_empty() {
            None
        } else {
            Some(Key::from_raw(req.get_start_key()))
        };

        ScanLock::new(
            req.get_max_version().into(),
            start_key,
            req.get_limit() as usize,
            req.take_context(),
        )
    }
}

impl From<ResolveLockRequest> for TypedCommand<()> {
    fn from(mut req: ResolveLockRequest) -> Self {
        let resolve_keys: Vec<Key> = req
            .get_keys()
            .iter()
            .map(|key| Key::from_raw(key))
            .collect();
        let txn_status = if req.get_start_version() > 0 {
            HashMap::from_iter(iter::once((
                req.get_start_version().into(),
                req.get_commit_version().into(),
            )))
        } else {
            HashMap::from_iter(
                req.take_txn_infos()
                    .into_iter()
                    .map(|info| (info.txn.into(), info.status.into())),
            )
        };

        if resolve_keys.is_empty() {
            ResolveLockReadPhase::new(txn_status, None, req.take_context())
        } else {
            let start_ts: TimeStamp = req.get_start_version().into();
            assert!(!start_ts.is_zero());
            let commit_ts = req.get_commit_version().into();
            ResolveLockLite::new(start_ts, commit_ts, resolve_keys, req.take_context())
        }
    }
}

impl From<MvccGetByKeyRequest> for TypedCommand<MvccInfo> {
    fn from(mut req: MvccGetByKeyRequest) -> Self {
        MvccByKey::new(Key::from_raw(req.get_key()), req.take_context())
    }
}

impl From<MvccGetByStartTsRequest> for TypedCommand<Option<(Key, MvccInfo)>> {
    fn from(mut req: MvccGetByStartTsRequest) -> Self {
        MvccByStartTs::new(req.get_start_ts().into(), req.take_context())
    }
}

pub trait CommandExt: Display {
    fn tag(&self) -> metrics::CommandKind;

    fn get_ctx(&self) -> &Context;

    fn get_ctx_mut(&mut self) -> &mut Context;

    fn incr_cmd_metric(&self);

    fn ts(&self) -> TimeStamp {
        TimeStamp::zero()
    }

    fn readonly(&self) -> bool {
        false
    }

    fn is_sys_cmd(&self) -> bool {
        false
    }

    fn can_be_pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize;

    fn gen_lock(&self, _latches: &Latches) -> latch::Lock;
}

impl Command {
    // These two are for backward compatibility, after some other refactors are done
    // we can remove Command totally and use `&dyn CommandExt` instead
    fn command_ext(&self) -> &dyn CommandExt {
        match &self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::ScanLock(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
        }
    }

    fn command_ext_mut(&mut self) -> &mut dyn CommandExt {
        match self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::ScanLock(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
        }
    }

    pub fn readonly(&self) -> bool {
        self.command_ext().readonly()
    }

    pub fn incr_cmd_metric(&self) {
        self.command_ext().incr_cmd_metric()
    }

    pub fn priority(&self) -> CommandPri {
        if self.command_ext().is_sys_cmd() {
            return CommandPri::High;
        }
        self.command_ext().get_ctx().get_priority()
    }

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        self.command_ext().tag()
    }

    pub fn ts(&self) -> TimeStamp {
        self.command_ext().ts()
    }

    pub fn write_bytes(&self) -> usize {
        self.command_ext().write_bytes()
    }

    pub fn gen_lock(&self, latches: &Latches) -> latch::Lock {
        self.command_ext().gen_lock(latches)
    }

    pub fn can_be_pipelined(&self) -> bool {
        self.command_ext().can_be_pipelined()
    }

    pub fn ctx(&self) -> &Context {
        self.command_ext().get_ctx()
    }

    pub fn ctx_mut(&mut self) -> &mut Context {
        self.command_ext_mut().get_ctx_mut()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}
