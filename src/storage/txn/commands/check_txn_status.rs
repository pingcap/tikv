// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, TxnStatus};

command! {
    /// Check the status of a transaction. This is usually invoked by a transaction that meets
    /// another transaction's lock. If the primary lock is expired, it will rollback the primary
    /// lock. If the primary lock exists but is not expired, it may update the transaction's
    /// `min_commit_ts`. Returns a [`TxnStatus`](TxnStatus) to represent the status.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) or
    /// [`Prewrite`](Command::Prewrite).
    CheckTxnStatus:
        cmd_ty => TxnStatus,
        display => "kv::command::check_txn_status {} @ {} curr({}, {}) | {:?}", (primary_key, lock_ts, caller_start_ts, current_ts, ctx),
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The lock's ts, namely the transaction's start_ts.
            lock_ts: TimeStamp,
            /// The start_ts of the transaction that invokes this command.
            caller_start_ts: TimeStamp,
            /// The approximate current_ts when the command is invoked.
            current_ts: TimeStamp,
            /// Specifies the behavior when neither commit/rollback record nor lock is found. If true,
            /// rollbacks that transaction; otherwise returns an error.
            rollback_if_not_exist: bool,
        }
}

impl CommandExt for CheckTxnStatus {
    ctx!();
    tag!(check_txn_status);
    ts!(lock_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckTxnStatus {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let mut released_locks = ReleasedLocks::new(self.lock_ts, TimeStamp::zero());
        let (txn_status, released) = txn.check_txn_status(
            self.primary_key,
            self.caller_start_ts,
            self.current_ts,
            self.rollback_if_not_exist,
        )?;
        released_locks.push(released);
        // The lock is released here only when the `check_txn_status` returns `TtlExpire`.
        if let TxnStatus::TtlExpire = txn_status {
            released_locks.wake_up(context.lock_mgr);
        }

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus { txn_status };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
