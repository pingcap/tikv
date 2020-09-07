// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::MVCC_CHECK_TXN_STATUS_COUNTER_VEC;
use crate::storage::mvcc::txn::MissingLockAction;
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, TxnStatus};
use std::mem;

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
    /// checks whether a transaction has expired its primary lock's TTL, rollback the
    /// transaction if expired, or update the transaction's min_commit_ts according to the metadata
    /// in the primary lock.
    /// When transaction T1 meets T2's lock, it may invoke this on T2's primary key. In this
    /// situation, `self.start_ts` is T2's `start_ts`, `caller_start_ts` is T1's `start_ts`, and
    /// the `current_ts` is literally the timestamp when this function is invoked. It may not be
    /// accurate.
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we update max_ts
        // to prevent this case from happening.
        context.concurrency_manager.update_max_ts(self.lock_ts);

        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let mut released_locks = ReleasedLocks::new(self.lock_ts, TimeStamp::zero());
        let ctx = mem::take(&mut self.ctx);
        fail_point!("check_txn_status", |err| Err(MvccError::from(
            crate::storage::mvcc::txn::make_txn_error(err, &self.primary_key, self.lock_ts)
        )
        .into()));

        let result = match txn.reader.load_lock(&self.primary_key)? {
            Some(mut lock) if lock.ts == self.lock_ts => {
                if lock.use_async_commit
                    && (!self.caller_start_ts.is_zero() || !self.current_ts.is_zero())
                {
                    return Err(MvccError::from(MvccErrorInner::Other(box_err!(
                        "cannot call check_txn_status with caller_start_ts or current_ts set on async commit transaction"
                    ))).into());
                }

                let is_pessimistic_txn = !lock.for_update_ts.is_zero();

                if lock.ts.physical() + lock.ttl < self.current_ts.physical() {
                    // If the lock is expired, clean it up.
                    let released = txn.check_write_and_rollback_lock(
                        self.primary_key,
                        &lock,
                        is_pessimistic_txn,
                    )?;
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
                    Ok((TxnStatus::TtlExpire, released))
                } else {
                    if !lock.min_commit_ts.is_zero()
                        // If lock.min_commit_ts is 0, it's not a large transaction and we can't push forward
                        // its min_commit_ts otherwise the transaction can't be committed by old version TiDB
                        // during rolling update.
                        // If the caller_start_ts is max, it's a point get in the autocommit transaction.
                        // We don't push forward lock's min_commit_ts and the point get can ingore the lock
                        // next time because it's not committed.
                        && !self.caller_start_ts.is_max()
                        // Push forward the min_commit_ts so that reading won't be blocked by locks.
                        && self.caller_start_ts >= lock.min_commit_ts
                    {
                        assert!(!lock.use_async_commit);
                        lock.min_commit_ts = self.caller_start_ts.next();

                        if lock.min_commit_ts < self.current_ts {
                            lock.min_commit_ts = self.current_ts;
                        }

                        txn.put_lock(self.primary_key, &lock);
                        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
                    }

                    Ok((TxnStatus::uncommitted(lock), None))
                }
            }
            // The rollback must be protected, see more on
            // [issue #7364](https://github.com/tikv/tikv/issues/7364)
            l => txn
                .check_txn_status_missing_lock(
                    self.primary_key,
                    l,
                    MissingLockAction::rollback(self.rollback_if_not_exist),
                )
                .map(|s| (s, None)),
        };
        let (txn_status, released) = result?;

        released_locks.push(released);
        // The lock is released here only when the `check_txn_status` returns `TtlExpire`.
        if let TxnStatus::TtlExpire = txn_status {
            released_locks.wake_up(context.lock_mgr);
        }

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus { txn_status };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::kv::{Engine, WriteData};
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::mvcc::tests::*;
    use crate::storage::txn::commands::{pessimistic_rollback, WriteCommand, WriteContext};
    use crate::storage::{types::TxnStatus, ProcessResult, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::WriteType;
    use txn_types::{Key, Lock, LockType, Mutation};

    pub fn must_success<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
        status_pred: impl FnOnce(TxnStatus) -> bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let lock_ts: TimeStamp = lock_ts.into();
        let command = crate::storage::txn::commands::CheckTxnStatus {
            ctx: Context::default(),
            primary_key: Key::from_raw(primary_key),
            lock_ts,
            caller_start_ts: caller_start_ts.into(),
            current_ts,
            rollback_if_not_exist,
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    pipelined_pessimistic_lock: false,
                    enable_async_commit: true,
                },
            )
            .unwrap();
        if let ProcessResult::TxnStatus { txn_status } = result.pr {
            assert!(status_pred(txn_status));
        } else {
            unreachable!();
        }
        write(engine, &ctx, result.to_be_write.modifies);
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let lock_ts: TimeStamp = lock_ts.into();
        let command = crate::storage::txn::commands::CheckTxnStatus {
            ctx,
            primary_key: Key::from_raw(primary_key),
            lock_ts,
            caller_start_ts: caller_start_ts.into(),
            current_ts,
            rollback_if_not_exist,
        };
        assert!(command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    pipelined_pessimistic_lock: false,
                    enable_async_commit: true,
                },
            )
            .is_err());
    }

    #[test]
    fn test_check_async_commit_txn_status() {
        // The preparation work is the same as test_async_prewrite_primary.
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        let snapshot = engine.snapshot(&ctx).unwrap();
        let cm = ConcurrencyManager::new(42.into());
        let mut txn = MvccTxn::new(snapshot, TimeStamp::new(2), true, cm.clone());

        let mutation = Mutation::Put((Key::from_raw(b"key"), b"value".to_vec()));
        let secondaries = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        txn.prewrite(
            mutation,
            b"key",
            &Some(secondaries.clone()),
            false,
            0,
            4,
            TimeStamp::zero(),
        )
        .unwrap();
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();

        let do_check_txn_status = |rollback_if_not_exist| {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let command = crate::storage::txn::commands::CheckTxnStatus {
                ctx: Default::default(),
                primary_key: Key::from_raw(b"key"),
                lock_ts: TimeStamp::new(2),
                caller_start_ts: 0.into(),
                current_ts: 0.into(),
                rollback_if_not_exist,
            };
            let result = command
                .process_write(
                    snapshot,
                    WriteContext {
                        lock_mgr: &DummyLockManager,
                        concurrency_manager: cm.clone(),
                        extra_op: Default::default(),
                        statistics: &mut Default::default(),
                        pipelined_pessimistic_lock: false,
                        enable_async_commit: true,
                    },
                )
                .unwrap();
            if let ProcessResult::TxnStatus { txn_status } = result.pr {
                assert_eq!(
                    txn_status,
                    TxnStatus::uncommitted(
                        Lock::new(
                            LockType::Put,
                            b"key".to_vec(),
                            2.into(),
                            0,
                            Some(b"value".to_vec()),
                            0.into(),
                            4,
                            43.into()
                        )
                        .use_async_commit(secondaries.clone())
                    )
                );
            } else {
                unreachable!();
            }
        };
        do_check_txn_status(true);
        do_check_txn_status(false);

        // Disallow calling check_txn_status on async commit transactions with caller_start_ts or
        // current_ts set.
        must_err(&engine, b"key", 2, 1, 0, true);
        must_err(&engine, b"key", 2, 0, 1, true);
    }

    fn test_check_txn_status_impl(rollback_if_not_exist: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let ts = TimeStamp::compose;

        // Shortcuts
        use super::TxnStatus::*;
        let committed = |commit_ts| move |s| s == TxnStatus::Committed { commit_ts };
        let uncommitted = |ttl, min_commit_ts| {
            move |s| {
                if let TxnStatus::Uncommitted { lock } = s {
                    lock.ttl == ttl && lock.min_commit_ts == min_commit_ts
                } else {
                    false
                }
            }
        };
        let r = rollback_if_not_exist;

        // Try to check a not exist thing.
        if r {
            must_success(&engine, k, ts(3, 0), ts(3, 1), ts(3, 2), r, |s| {
                s == LockNotExist
            });
            // A protected rollback record will be written.
            must_get_rollback_protected(&engine, k, ts(3, 0), true);
        } else {
            must_err(&engine, k, ts(3, 0), ts(3, 1), ts(3, 2), r);
        }

        // Lock the key with TTL=100.
        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(5, 0), 100, 0);
        // The initial min_commit_ts is start_ts + 1.
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(5, 1), false);

        // CheckTxnStatus with caller_start_ts = 0 and current_ts = 0 should just return the
        // information of the lock without changing it.
        must_success(&engine, k, ts(5, 0), 0, 0, r, uncommitted(100, ts(5, 1)));

        // Update min_commit_ts to current_ts.
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(6, 0),
            ts(7, 0),
            r,
            uncommitted(100, ts(7, 0)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(7, 0), false);

        // Update min_commit_ts to caller_start_ts + 1 if current_ts < caller_start_ts.
        // This case should be impossible. But if it happens, we prevents it.
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(9, 0),
            ts(8, 0),
            r,
            uncommitted(100, ts(9, 1)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(9, 1), false);

        // caller_start_ts < lock.min_commit_ts < current_ts
        // When caller_start_ts < lock.min_commit_ts, no need to update it.
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(8, 0),
            ts(10, 0),
            r,
            uncommitted(100, ts(9, 1)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(9, 1), false);

        // current_ts < lock.min_commit_ts < caller_start_ts
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(11, 0),
            ts(9, 0),
            r,
            uncommitted(100, ts(11, 1)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(11, 1), false);

        // For same caller_start_ts and current_ts, update min_commit_ts to caller_start_ts + 1
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            uncommitted(100, ts(12, 1)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(12, 1), false);

        // Logical time is also considered in the comparing
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(13, 1),
            ts(13, 3),
            r,
            uncommitted(100, ts(13, 3)),
        );
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(13, 3), false);

        must_commit(&engine, k, ts(5, 0), ts(15, 0));
        must_unlocked(&engine, k);

        // Check committed key will get the commit ts.
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            committed(ts(15, 0)),
        );
        must_unlocked(&engine, k);

        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(20, 0), 100, 0);

        // Check a committed transaction when there is another lock. Expect getting the commit ts.
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            committed(ts(15, 0)),
        );

        // Check a not existing transaction, the result depends on whether `rollback_if_not_exist`
        // is set.
        if r {
            must_success(&engine, k, ts(6, 0), ts(12, 0), ts(12, 0), r, |s| {
                s == LockNotExist
            });
            // And a rollback record will be written.
            must_seek_write(
                &engine,
                k,
                ts(6, 0),
                ts(6, 0),
                ts(6, 0),
                WriteType::Rollback,
            );
        } else {
            must_err(&engine, k, ts(6, 0), ts(12, 0), ts(12, 0), r);
        }

        // TTL check is based on physical time (in ms). When logical time's difference is larger
        // than TTL, the lock won't be resolved.
        must_success(
            &engine,
            k,
            ts(20, 0),
            ts(21, 105),
            ts(21, 105),
            r,
            uncommitted(100, ts(21, 106)),
        );
        must_large_txn_locked(&engine, k, ts(20, 0), 100, ts(21, 106), false);

        // If physical time's difference exceeds TTL, lock will be resolved.
        must_success(&engine, k, ts(20, 0), ts(121, 0), ts(121, 0), r, |s| {
            s == TtlExpire
        });
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            TimeStamp::max(),
            ts(20, 0),
            ts(20, 0),
            WriteType::Rollback,
        );

        // Push the min_commit_ts of pessimistic locks.
        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(4, 0), ts(130, 0), 200);
        must_large_txn_locked(&engine, k, ts(4, 0), 200, ts(130, 1), true);
        must_success(
            &engine,
            k,
            ts(4, 0),
            ts(135, 0),
            ts(135, 0),
            r,
            uncommitted(200, ts(135, 1)),
        );
        must_large_txn_locked(&engine, k, ts(4, 0), 200, ts(135, 1), true);

        // Commit the key.
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(4, 0), ts(130, 0), true);
        must_commit(&engine, k, ts(4, 0), ts(140, 0));
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, ts(4, 0), ts(140, 0));

        // Now the transactions are intersecting:
        // T1: start_ts = 5, commit_ts = 15
        // T2: start_ts = 20, rollback
        // T3: start_ts = 4, commit_ts = 140
        must_success(
            &engine,
            k,
            ts(4, 0),
            ts(10, 0),
            ts(10, 0),
            r,
            committed(ts(140, 0)),
        );
        must_success(
            &engine,
            k,
            ts(5, 0),
            ts(10, 0),
            ts(10, 0),
            r,
            committed(ts(15, 0)),
        );
        must_success(&engine, k, ts(20, 0), ts(10, 0), ts(10, 0), r, |s| {
            s == RolledBack
        });

        // Rollback expired pessimistic lock.
        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(150, 0), ts(150, 0), 100);
        must_success(
            &engine,
            k,
            ts(150, 0),
            ts(160, 0),
            ts(160, 0),
            r,
            uncommitted(100, ts(160, 1)),
        );
        must_large_txn_locked(&engine, k, ts(150, 0), 100, ts(160, 1), true);
        must_success(&engine, k, ts(150, 0), ts(160, 0), ts(260, 0), r, |s| {
            s == TtlExpire
        });
        must_unlocked(&engine, k);
        // Rolling back a pessimistic lock should leave Rollback mark.
        must_seek_write(
            &engine,
            k,
            TimeStamp::max(),
            ts(150, 0),
            ts(150, 0),
            WriteType::Rollback,
        );

        // Rollback when current_ts is u64::max_value()
        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(270, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(270, 0), 100, ts(270, 1), false);
        must_success(
            &engine,
            k,
            ts(270, 0),
            ts(271, 0),
            TimeStamp::max(),
            r,
            |s| s == TtlExpire,
        );
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            TimeStamp::max(),
            ts(270, 0),
            ts(270, 0),
            WriteType::Rollback,
        );

        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(280, 0), ts(280, 0), 100);
        must_large_txn_locked(&engine, k, ts(280, 0), 100, ts(280, 1), true);
        must_success(
            &engine,
            k,
            ts(280, 0),
            ts(281, 0),
            TimeStamp::max(),
            r,
            |s| s == TtlExpire,
        );
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            TimeStamp::max(),
            ts(280, 0),
            ts(280, 0),
            WriteType::Rollback,
        );

        // Don't push forward the min_commit_ts if the min_commit_ts of the lock is 0.
        must_acquire_pessimistic_lock_with_ttl(&engine, k, k, ts(290, 0), ts(290, 0), 100);
        must_success(
            &engine,
            k,
            ts(290, 0),
            ts(300, 0),
            ts(300, 0),
            r,
            uncommitted(100, TimeStamp::zero()),
        );
        must_large_txn_locked(&engine, k, ts(290, 0), 100, TimeStamp::zero(), true);
        pessimistic_rollback::tests::must_success(&engine, k, ts(290, 0), ts(290, 0));

        must_prewrite_put_impl(
            &engine,
            k,
            v,
            k,
            &None,
            ts(300, 0),
            false,
            100,
            TimeStamp::zero(),
            1,
            /* min_commit_ts */ TimeStamp::zero(),
            false,
        );
        must_success(
            &engine,
            k,
            ts(300, 0),
            ts(310, 0),
            ts(310, 0),
            r,
            uncommitted(100, TimeStamp::zero()),
        );
        must_large_txn_locked(&engine, k, ts(300, 0), 100, TimeStamp::zero(), false);
        must_rollback(&engine, k, ts(300, 0));

        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(310, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(310, 0), 100, ts(310, 1), false);
        // Don't push forward the min_commit_ts if caller_start_ts is max.
        must_success(
            &engine,
            k,
            ts(310, 0),
            TimeStamp::max(),
            ts(320, 0),
            r,
            uncommitted(100, ts(310, 1)),
        );
        must_commit(&engine, k, ts(310, 0), ts(315, 0));
        must_success(
            &engine,
            k,
            ts(310, 0),
            TimeStamp::max(),
            ts(320, 0),
            r,
            committed(ts(315, 0)),
        );
    }

    #[test]
    fn test_check_txn_status() {
        test_check_txn_status_impl(false);
        test_check_txn_status_impl(true);
    }
}
