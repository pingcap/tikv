// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, Statistics};
use kvproto::kvrpcpb::ExtraOp;
use pd_client::PdClient;
use std::sync::Arc;
use txn_types::{Key, TimeStamp};

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        display => "kv::resolve_lock_lite", (),
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
        }
}

impl CommandExt for ResolveLockLite {
    ctx!();
    tag!(resolve_lock_lite);
    ts!(start_ts);
    command_method!(is_sys_cmd, bool, true);
    write_bytes!(resolve_keys: multiple);
    gen_lock!(resolve_keys: multiple);
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for ResolveLockLite {
    fn process_write(
        self,
        snapshot: S,
        lock_mgr: &L,
        pd_client: Arc<P>,
        _extra_op: ExtraOp,
        statistics: &mut Statistics,
        _pipelined_pessimistic_lock: bool,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            pd_client,
        );

        let rows = self.resolve_keys.len();
        // ti-client guarantees the size of resolve_keys will not too large, so no necessary
        // to control the write_size as ResolveLock.
        let mut released_locks = ReleasedLocks::new(self.start_ts, self.commit_ts);
        for key in self.resolve_keys {
            released_locks.push(if !self.commit_ts.is_zero() {
                txn.commit(key, self.commit_ts)?
            } else {
                txn.rollback(key)?
            });
        }
        released_locks.wake_up(lock_mgr);

        statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
        })
    }
}
