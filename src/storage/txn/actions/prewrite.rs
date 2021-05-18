// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use crate::storage::{
    mvcc::{
        metrics::{
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM, MVCC_CONFLICT_COUNTER,
            MVCC_DUPLICATE_CMD_COUNTER_VEC,
        },
        Error, ErrorInner, Lock, LockType, MvccTxn, Result, SnapshotReader,
    },
    txn::actions::check_data_constraint::check_data_constraint,
    txn::LockInfo,
    Snapshot,
};
use fail::fail_point;
use std::cmp;
use txn_types::{
    is_short_value, Key, Mutation, MutationType, OldValue, TimeStamp, Value, Write, WriteType,
};

/// Prewrite a single mutation by creating and storing a lock and value.
pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    txn_props: &TransactionProperties,
    mutation: Mutation,
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_lock: bool,
) -> Result<(TimeStamp, OldValue)> {
    let mut mutation = PrewriteMutation::from_mutation(mutation, secondary_keys, txn_props)?;

    fail_point!(
        if txn_props.is_pessimistic() {
            "pessimistic_prewrite"
        } else {
            "prewrite"
        },
        |err| Err(crate::storage::mvcc::txn::make_txn_error(
            err,
            &mutation.key,
            mutation.txn_props.start_ts
        )
        .into())
    );

    let lock_status = match reader.load_lock(&mutation.key)? {
        Some(lock) => mutation.check_lock(lock, is_pessimistic_lock)?,
        None if is_pessimistic_lock => {
            amend_pessimistic_lock(&mutation.key, reader)?;
            LockStatus::None
        }
        None => LockStatus::None,
    };

    if let LockStatus::Locked(ts) = lock_status {
        return Ok((ts, OldValue::Unspecified));
    }

    // Note that the `prev_write` may have invalid GC fence.
    let prev_write = if !mutation.skip_constraint_check() {
        mutation.check_for_newer_version(reader)?
    } else {
        None
    };

    if mutation.should_not_write {
        // `checkNotExists` is equivalent to a get operation, so it should update the max_ts.
        txn.concurrency_manager.update_max_ts(txn_props.start_ts);
        let min_commit_ts = if mutation.need_min_commit_ts() {
            // Don't calculate the min_commit_ts according to the concurrency manager's max_ts
            // for a should_not_write mutation because it's not persisted and doesn't change data.
            cmp::max(txn_props.min_commit_ts, txn_props.start_ts.next())
        } else {
            TimeStamp::zero()
        };
        return Ok((min_commit_ts, OldValue::Unspecified));
    }

    let old_value = if txn_props.need_old_value
        && matches!(
            mutation.mutation_type,
            // Only Put, Delete and Insert may have old value.
            MutationType::Put | MutationType::Delete | MutationType::Insert
        ) {
        if mutation.mutation_type == MutationType::Insert {
            // The previous write of an Insert is guaranteed to be None.
            OldValue::None
        } else if mutation.skip_constraint_check() {
            if mutation.txn_props.is_pessimistic() {
                // Pessimistic transaction always skip constraint check in
                // "prewrite" stage, as it checks constraint in
                // "acquire pessimistic lock" stage.
                OldValue::Unspecified
            } else {
                // In optimistic transaction, caller ensures that there is no
                // previous write for the mutation, so there is no old value.
                OldValue::None
            }
        } else {
            // prev_write is loaded when skip_constraint_check is false.
            let prev_write_loaded = true;
            // The mutation reads and get a previous write.
            let ts = match txn_props.kind {
                TransactionKind::Optimistic(_) => txn_props.start_ts,
                TransactionKind::Pessimistic(for_update_ts) => for_update_ts,
            };
            reader.get_old_value(&mutation.key, ts, prev_write_loaded, prev_write)?
        }
    } else {
        OldValue::Unspecified
    };

    let final_min_commit_ts = mutation.write_lock(lock_status, txn)?;

    fail_point!("after_prewrite_one_key");

    Ok((final_min_commit_ts, old_value))
}

#[derive(Clone, Debug)]
pub struct TransactionProperties<'a> {
    pub start_ts: TimeStamp,
    pub kind: TransactionKind,
    pub commit_kind: CommitKind,
    pub primary: &'a [u8],
    pub txn_size: u64,
    pub lock_ttl: u64,
    pub min_commit_ts: TimeStamp,
    pub need_old_value: bool,
}

impl<'a> TransactionProperties<'a> {
    fn max_commit_ts(&self) -> TimeStamp {
        match &self.commit_kind {
            CommitKind::TwoPc => unreachable!(),
            CommitKind::OnePc(ts) => *ts,
            CommitKind::Async(ts) => *ts,
        }
    }

    fn is_pessimistic(&self) -> bool {
        match &self.kind {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    fn for_update_ts(&self) -> TimeStamp {
        match &self.kind {
            TransactionKind::Optimistic(_) => TimeStamp::zero(),
            TransactionKind::Pessimistic(ts) => *ts,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommitKind {
    TwoPc,
    /// max_commit_ts
    OnePc(TimeStamp),
    /// max_commit_ts
    Async(TimeStamp),
}

#[derive(Clone, Debug)]
pub enum TransactionKind {
    // bool is skip_constraint_check
    Optimistic(bool),
    // for_update_ts
    Pessimistic(TimeStamp),
}

enum LockStatus {
    // Lock has already been locked; min_commit_ts of lock.
    Locked(TimeStamp),
    Pessimistic,
    None,
}

impl LockStatus {
    fn has_pessimistic_lock(&self) -> bool {
        matches!(self, LockStatus::Pessimistic)
    }
}

/// A single mutation to be prewritten.
struct PrewriteMutation<'a> {
    key: Key,
    value: Option<Value>,
    mutation_type: MutationType,
    secondary_keys: &'a Option<Vec<Vec<u8>>>,
    min_commit_ts: TimeStamp,

    lock_type: Option<LockType>,
    lock_ttl: u64,

    should_not_exist: bool,
    should_not_write: bool,
    txn_props: &'a TransactionProperties<'a>,
}

impl<'a> PrewriteMutation<'a> {
    fn from_mutation(
        mutation: Mutation,
        secondary_keys: &'a Option<Vec<Vec<u8>>>,
        txn_props: &'a TransactionProperties<'a>,
    ) -> Result<PrewriteMutation<'a>> {
        let should_not_write = mutation.should_not_write();

        if txn_props.is_pessimistic() && should_not_write {
            return Err(box_err!(
                "cannot handle checkNotExists in pessimistic prewrite"
            ));
        }

        let should_not_exist = mutation.should_not_exists();
        let mutation_type = mutation.mutation_type();
        let lock_type = LockType::from_mutation(&mutation);
        let (key, value) = mutation.into_key_value();
        Ok(PrewriteMutation {
            key,
            value,
            mutation_type,
            secondary_keys,
            min_commit_ts: txn_props.min_commit_ts,

            lock_type,
            lock_ttl: txn_props.lock_ttl,

            should_not_exist,
            should_not_write,
            txn_props,
        })
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys and unique index keys.
    // The corresponding secondary index keys are not locked until pessimistic prewrite.
    // It's possible that lock conflict occurs on them, but the isolation is
    // guaranteed by pessimistic locks, so let TiDB resolves these locks immediately.
    fn lock_info(&self, lock: Lock) -> Result<LockInfo> {
        let mut info = lock.into_lock_info(self.key.to_raw()?);
        if self.txn_props.is_pessimistic() {
            info.set_lock_ttl(0);
        }
        Ok(info)
    }

    /// Check whether the current key is locked at any timestamp.
    fn check_lock(&mut self, lock: Lock, is_pessimistic_lock: bool) -> Result<LockStatus> {
        if lock.ts != self.txn_props.start_ts {
            // Abort on lock belonging to other transaction if
            // prewrites a pessimistic lock.
            if is_pessimistic_lock {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => self.txn_props.start_ts,
                    "key" => %self.key,
                    "lock_ts" => lock.ts
                );
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                }
                .into());
            }

            return Err(ErrorInner::KeyIsLocked(self.lock_info(lock)?).into());
        }

        if lock.lock_type == LockType::Pessimistic {
            // TODO: remove it in future
            if !self.txn_props.is_pessimistic() {
                return Err(ErrorInner::LockTypeNotMatch {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                    pessimistic: true,
                }
                .into());
            }

            // The lock is pessimistic and owned by this txn, go through to overwrite it.
            // The ttl and min_commit_ts of the lock may have been pushed forward.
            self.lock_ttl = std::cmp::max(self.lock_ttl, lock.ttl);
            self.min_commit_ts = std::cmp::max(self.min_commit_ts, lock.min_commit_ts);

            return Ok(LockStatus::Pessimistic);
        }

        // Duplicated command. No need to overwrite the lock and data.
        MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        let min_commit_ts = if lock.use_async_commit {
            lock.min_commit_ts
        } else {
            TimeStamp::zero()
        };
        Ok(LockStatus::Locked(min_commit_ts))
    }

    fn check_for_newer_version<S: Snapshot>(
        &self,
        reader: &mut SnapshotReader<S>,
    ) -> Result<Option<Write>> {
        match reader.seek_write(&self.key, TimeStamp::max())? {
            Some((commit_ts, write)) => {
                // Abort on writes after our start timestamp ...
                // If exists a commit version whose commit timestamp is larger than current start
                // timestamp, we should abort current prewrite.
                if commit_ts > self.txn_props.start_ts {
                    MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // If there's a write record whose commit_ts equals to our start ts, the current
                // transaction is ok to continue, unless the record means that the current
                // transaction has been rolled back.
                if commit_ts == self.txn_props.start_ts
                    && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
                {
                    MVCC_CONFLICT_COUNTER.rolled_back.inc();
                    // TODO: Maybe we need to add a new error for the rolled back case.
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // Should check it when no lock exists, otherwise it can report error when there is
                // a lock belonging to a committed transaction which deletes the key.
                check_data_constraint(reader, self.should_not_exist, &write, commit_ts, &self.key)?;

                Ok(Some(write))
            }
            None => Ok(None),
        }
    }

    fn write_lock(self, lock_status: LockStatus, txn: &mut MvccTxn) -> Result<TimeStamp> {
        let mut try_one_pc = self.try_one_pc();

        let mut lock = Lock::new(
            self.lock_type.unwrap(),
            self.txn_props.primary.to_vec(),
            self.txn_props.start_ts,
            self.lock_ttl,
            None,
            self.txn_props.for_update_ts(),
            self.txn_props.txn_size,
            self.min_commit_ts,
        );

        if let Some(value) = self.value {
            if is_short_value(&value) {
                // If the value is short, embed it in Lock.
                lock.short_value = Some(value);
            } else {
                // value is long
                txn.put_value(self.key.clone(), self.txn_props.start_ts, value);
            }
        }

        if let Some(secondary_keys) = self.secondary_keys {
            lock.use_async_commit = true;
            lock.secondaries = secondary_keys.to_owned();
        }

        let final_min_commit_ts = if lock.use_async_commit || try_one_pc {
            let res = async_commit_timestamps(
                &self.key,
                &mut lock,
                self.txn_props.start_ts,
                self.txn_props.for_update_ts(),
                self.txn_props.max_commit_ts(),
                txn,
            );
            fail_point!("after_calculate_min_commit_ts");
            if let Err(Error(box ErrorInner::CommitTsTooLarge { .. })) = &res {
                try_one_pc = false;
                lock.use_async_commit = false;
                lock.secondaries = Vec::new();
            }
            res
        } else {
            Ok(TimeStamp::zero())
        };

        if try_one_pc {
            txn.put_locks_for_1pc(self.key, lock, lock_status.has_pessimistic_lock());
        } else {
            txn.put_lock(self.key, &lock);
        }

        final_min_commit_ts
    }

    fn write_conflict_error(&self, write: &Write, commit_ts: TimeStamp) -> Result<()> {
        Err(ErrorInner::WriteConflict {
            start_ts: self.txn_props.start_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: commit_ts,
            key: self.key.to_raw()?,
            primary: self.txn_props.primary.to_vec(),
        }
        .into())
    }

    fn skip_constraint_check(&self) -> bool {
        match &self.txn_props.kind {
            TransactionKind::Optimistic(s) => *s,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    fn need_min_commit_ts(&self) -> bool {
        matches!(
            &self.txn_props.commit_kind,
            CommitKind::Async(_) | CommitKind::OnePc(_)
        )
    }

    fn try_one_pc(&self) -> bool {
        matches!(&self.txn_props.commit_kind, CommitKind::OnePc(_))
    }
}

// The final_min_commit_ts will be calculated if either async commit or 1PC is enabled.
// It's allowed to enable 1PC without enabling async commit.
fn async_commit_timestamps(
    key: &Key,
    lock: &mut Lock,
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    txn: &mut MvccTxn,
) -> Result<TimeStamp> {
    // This operation should not block because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
        ::futures_executor::block_on(txn.concurrency_manager.lock_key(key))
    });

    let final_min_commit_ts = key_guard.with_lock(|l| {
        let max_ts = txn.concurrency_manager.max_ts();
        fail_point!("before-set-lock-in-memory");
        let min_commit_ts = cmp::max(cmp::max(max_ts, start_ts), for_update_ts).next();
        let min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);

        #[cfg(feature = "failpoints")]
        let injected_fallback = (|| {
            fail_point!("async_commit_1pc_force_fallback", |_| {
                info!("[failpoint] injected fallback for async commit/1pc transaction"; "start_ts" => start_ts);
                true
            });
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let injected_fallback = false;

        let max_commit_ts = max_commit_ts;
        if (!max_commit_ts.is_zero() && min_commit_ts > max_commit_ts) || injected_fallback {
            warn!("commit_ts is too large, fallback to normal 2PC";
                "key" => log_wrappers::Value::key(key.as_encoded()),
                "start_ts" => start_ts,
                "min_commit_ts" => min_commit_ts,
                "max_commit_ts" => max_commit_ts,
                "lock" => ?lock);
            return Err(ErrorInner::CommitTsTooLarge {
                start_ts,
                min_commit_ts,
                max_commit_ts,
            });
        }

        lock.min_commit_ts = min_commit_ts;
        *l = Some(lock.clone());
        Ok(min_commit_ts)
    })?;

    txn.guards.push(key_guard);

    Ok(final_min_commit_ts)
}

// TiKV may fails to write pessimistic locks due to pipelined process.
// If the data is not changed after acquiring the lock, we can still prewrite the key.
fn amend_pessimistic_lock<S: Snapshot>(key: &Key, reader: &mut SnapshotReader<S>) -> Result<()> {
    if let Some((commit_ts, _)) = reader.seek_write(key, TimeStamp::max())? {
        // The invariants of pessimistic locks are:
        //   1. lock's for_update_ts >= key's latest commit_ts
        //   2. lock's for_update_ts >= txn's start_ts
        //   3. If the data is changed after acquiring the pessimistic lock, key's new commit_ts > lock's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to lock's for_update_ts, the data is not changed.
        // However, we can't get lock's for_update_ts in current implementation (txn's for_update_ts is updated for each DML),
        // we can only use txn's start_ts to check -- If the key's commit_ts is less than txn's start_ts, it's less than
        // lock's for_update_ts too.
        if commit_ts >= reader.start_ts {
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => reader.start_ts,
                "commit_ts" => commit_ts,
                "key" => %key
            );
            MVCC_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_lock_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: reader.start_ts,
                key: key.clone().into_raw()?,
            }
            .into());
        }
    }
    // Used pipelined pessimistic lock acquiring in this txn but failed
    // Luckily no other txn modified this lock, amend it by treat it as optimistic txn.
    MVCC_CONFLICT_COUNTER
        .pipelined_acquire_pessimistic_lock_amend_success
        .inc();
    Ok(())
}

pub mod tests {
    use super::*;
    #[cfg(test)]
    use crate::storage::{
        kv::RocksSnapshot,
        txn::{
            commands::prewrite::fallback_1pc_locks,
            tests::{
                must_acquire_pessimistic_lock, must_cleanup_with_gc_fence, must_commit,
                must_prewrite_delete, must_prewrite_lock, must_prewrite_put, must_rollback,
            },
        },
    };
    use crate::storage::{mvcc::tests::*, Engine};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use rand::{Rng, SeedableRng};
    #[cfg(test)]
    use std::sync::Arc;
    #[cfg(test)]
    use txn_types::OldValue;

    fn optimistic_txn_props(primary: &[u8], start_ts: TimeStamp) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
        }
    }

    #[cfg(test)]
    fn optimistic_async_props(
        primary: &[u8],
        start_ts: TimeStamp,
        max_commit_ts: TimeStamp,
        txn_size: u64,
        one_pc: bool,
    ) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: if one_pc {
                CommitKind::OnePc(max_commit_ts)
            } else {
                CommitKind::Async(max_commit_ts)
            },
            primary,
            txn_size,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
        }
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, true);

        let mut props = optimistic_txn_props(pk, ts);
        props.need_old_value = true;
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            &None,
            false,
        )?;
        // Insert must be None if the key is not lock, or be Unspecified if the
        // key is already locked.
        assert!(
            matches!(old_value, OldValue::None | OldValue::Unspecified),
            "{:?}",
            old_value
        );
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, true);

        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_txn_props(pk, ts),
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            true,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, true);

        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, false),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        assert!(must_locked(&engine, b"k1", 10).use_async_commit);
        // The written lock should not have use_async_commit flag.
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_async_commit_prewrite_min_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(41.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();

        // should_not_write mutations don't write locks or change data so that they needn't ask
        // the concurrency manager for max_ts. Its min_commit_ts may be less than or equal to max_ts.
        let mut props = optimistic_async_props(b"k0", 10.into(), 50.into(), 2, false);
        props.min_commit_ts = 11.into();
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::CheckNotExists(Key::from_raw(b"k0")),
            &Some(vec![]),
            false,
        )
        .unwrap();
        assert!(min_ts > props.start_ts);
        assert!(min_ts >= props.min_commit_ts);
        assert!(min_ts < 41.into());
        assert_eq!(old_value, OldValue::Unspecified);

        // `checkNotExists` is equivalent to a get operation, so it should update the max_ts.
        let mut props = optimistic_txn_props(b"k0", 42.into());
        props.min_commit_ts = 43.into();
        let mut txn = MvccTxn::new(42.into(), cm.clone());
        let mut reader = SnapshotReader::new(42.into(), snapshot.clone(), false);
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::CheckNotExists(Key::from_raw(b"k0")),
            &Some(vec![]),
            false,
        )
        .unwrap();
        assert_eq!(cm.max_ts(), props.start_ts);
        assert_eq!(old_value, OldValue::Unspecified);

        // should_write mutations' min_commit_ts must be > max_ts
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts > 42.into());
        assert!(min_ts < 50.into());
        assert_eq!(old_value, OldValue::None);

        for &should_not_write in &[false, true] {
            let mutation = if should_not_write {
                Mutation::CheckNotExists(Key::from_raw(b"k3"))
            } else {
                Mutation::Put((Key::from_raw(b"k3"), b"v1".to_vec()))
            };

            // min_commit_ts must be > start_ts
            let mut txn = MvccTxn::new(44.into(), cm.clone());
            let mut reader = SnapshotReader::new(44.into(), snapshot.clone(), false);
            let (min_ts, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &optimistic_async_props(b"k3", 44.into(), 50.into(), 2, false),
                mutation.clone(),
                &Some(vec![b"k4".to_vec()]),
                false,
            )
            .unwrap();
            assert!(min_ts > 44.into());
            assert!(min_ts < 50.into());
            txn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }

            // min_commit_ts must be > for_update_ts
            if !should_not_write {
                let mut props = optimistic_async_props(b"k5", 44.into(), 50.into(), 2, false);
                props.kind = TransactionKind::Pessimistic(45.into());
                let (min_ts, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &props,
                    mutation.clone(),
                    &Some(vec![b"k6".to_vec()]),
                    false,
                )
                .unwrap();
                assert!(min_ts > 45.into());
                assert!(min_ts < 50.into());
                txn.take_guards();
                // Pessimistic txn skips constraint check, does not read previous write.
                assert_eq!(old_value, OldValue::Unspecified);
            }

            // min_commit_ts must be >= txn min_commit_ts
            let mut props = optimistic_async_props(b"k7", 44.into(), 50.into(), 2, false);
            props.min_commit_ts = 46.into();
            let (min_ts, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &props,
                mutation.clone(),
                &Some(vec![b"k8".to_vec()]),
                false,
            )
            .unwrap();
            assert!(min_ts >= 46.into());
            assert!(min_ts < 50.into());
            txn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }
        }
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, true),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, true),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        fallback_1pc_locks(&mut txn);
        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        // success 1pc prewrite needs to be transformed to locks
        assert!(!must_locked(&engine, b"k1", 10).use_async_commit);
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, false);

        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &TransactionProperties {
                start_ts: ts,
                kind: TransactionKind::Pessimistic(TimeStamp::default()),
                commit_kind: CommitKind::TwoPc,
                primary: pk,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
            },
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            false,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::Async(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            true,
        )
        .unwrap();
        // Pessimistic txn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::OnePc(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            true,
        )
        .unwrap();
        // Pessimistic txn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_prewrite_check_gc_fence() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(1.into());

        // PUT,           Read
        //  `------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 30);
        must_cleanup_with_gc_fence(&engine, b"k1", 30, 0, 40, true);

        // PUT,           Read
        //  * (GC fence ts = 0)
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 30);
        must_cleanup_with_gc_fence(&engine, b"k2", 30, 0, 0, true);

        // PUT, LOCK,   LOCK, Read
        //  `---------^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 30);
        must_prewrite_lock(&engine, b"k3", b"k3", 37);
        must_commit(&engine, b"k3", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k3", 30, 0, 40, true);
        must_prewrite_lock(&engine, b"k3", b"k3", 42);
        must_commit(&engine, b"k3", 42, 43);

        // PUT, LOCK,   LOCK, Read
        //  *
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 30);
        must_prewrite_lock(&engine, b"k4", b"k4", 37);
        must_commit(&engine, b"k4", 37, 38);
        must_prewrite_lock(&engine, b"k4", b"k4", 42);
        must_commit(&engine, b"k4", 42, 43);
        must_cleanup_with_gc_fence(&engine, b"k4", 30, 0, 0, true);

        // PUT,   PUT,    READ
        //  `-----^ `------^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 14);
        must_commit(&engine, b"k5", 14, 20);
        must_prewrite_put(&engine, b"k5", b"v5x", b"k5", 21);
        must_commit(&engine, b"k5", 21, 30);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 30, 0, 40, true);

        // PUT,   PUT,    READ
        //  `-----^ *
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 15);
        must_commit(&engine, b"k6", 15, 20);
        must_prewrite_put(&engine, b"k6", b"v6x", b"k6", 22);
        must_commit(&engine, b"k6", 22, 30);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k6", 30, 0, 0, true);

        // PUT,  LOCK,    READ
        //  `----------^
        // Note that this case is special because usually the `LOCK` is the first write already got
        // during prewrite/acquire_pessimistic_lock and will continue searching an older version
        // from the `LOCK` record.
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 30);
        must_prewrite_lock(&engine, b"k7", b"k7", 37);
        must_commit(&engine, b"k7", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k7", 30, 0, 40, true);

        // 1. Check GC fence when doing constraint check with the older version.
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(50.into(), cm.clone());
        let mut reader = SnapshotReader::new(50.into(), snapshot.clone(), false);
        let txn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 6,
            lock_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
        };

        let cases = vec![
            (b"k1", true),
            (b"k2", false),
            (b"k3", true),
            (b"k4", false),
            (b"k5", true),
            (b"k6", false),
            (b"k7", true),
        ];

        for (key, success) in cases {
            let res = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::CheckNotExists(Key::from_raw(key)),
                &None,
                false,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::Unspecified);
            } else {
                res.unwrap_err();
            }

            let res = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::Insert((Key::from_raw(key), b"value".to_vec())),
                &None,
                false,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::None);
            } else {
                res.unwrap_err();
            }
        }
        // Don't actually write the txn so that the test data is not changed.
        drop(txn);

        // 2. Check GC fence when reading the old value.
        let mut txn = MvccTxn::new(50.into(), cm);
        let mut reader = SnapshotReader::new(50.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 6,
            lock_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
        };

        let cases: Vec<_> = vec![
            (b"k1" as &[u8], None),
            (b"k2", Some(b"v2" as &[u8])),
            (b"k3", None),
            (b"k4", Some(b"v4")),
            (b"k5", None),
            (b"k6", Some(b"v6x")),
            (b"k7", None),
        ]
        .into_iter()
        .map(|(k, v)| {
            let old_value = v
                .map(|value| OldValue::Value {
                    value: value.to_vec(),
                })
                .unwrap_or(OldValue::None);
            (Key::from_raw(k), old_value)
        })
        .collect();

        for (key, expected_value) in &cases {
            let (_, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::Put((key.clone(), b"value".to_vec())),
                &None,
                false,
            )
            .unwrap();
            assert_eq!(&old_value, expected_value, "key: {}", key);
        }
    }

    #[test]
    fn test_old_value_rollback_and_lock() {
        let engine_rollback = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine_rollback, b"k1", b"v1", b"k1", 10);
        must_commit(&engine_rollback, b"k1", 10, 30);

        must_prewrite_put(&engine_rollback, b"k1", b"v2", b"k1", 40);
        must_rollback(&engine_rollback, b"k1", 40, false);

        let engine_lock = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine_lock, b"k1", b"v1", b"k1", 10);
        must_commit(&engine_lock, b"k1", 10, 30);

        must_prewrite_lock(&engine_lock, b"k1", b"k1", 40);
        must_commit(&engine_lock, b"k1", 40, 45);

        for engine in &[engine_rollback, engine_lock] {
            let start_ts = TimeStamp::from(50);
            let txn_props = TransactionProperties {
                start_ts,
                kind: TransactionKind::Optimistic(false),
                commit_kind: CommitKind::TwoPc,
                primary: b"k1",
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
            };
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snapshot, true);
            let (_, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::Put((Key::from_raw(b"k1"), b"value".to_vec())),
                &None,
                false,
            )
            .unwrap();
            assert_eq!(
                old_value,
                OldValue::Value {
                    value: b"v1".to_vec(),
                }
            );
        }
    }

    // Prepares a test case that put, delete and lock a key and returns
    // a timestamp for testing the case.
    #[cfg(test)]
    pub fn old_value_put_delete_lock_insert<E: Engine>(engine: &E, key: &[u8]) -> TimeStamp {
        must_prewrite_put(engine, key, b"v1", key, 10);
        must_commit(engine, key, 10, 20);

        must_prewrite_delete(engine, key, key, 30);
        must_commit(engine, key, 30, 40);

        must_prewrite_lock(engine, key, key, 50);
        must_commit(engine, key, 50, 60);

        70.into()
    }

    #[test]
    fn test_old_value_put_delete_lock_insert() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let start_ts = old_value_put_delete_lock_insert(&engine, b"k1");
        let txn_props = TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: true,
        };
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Insert((Key::from_raw(b"k1"), b"v2".to_vec())),
            &None,
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);
    }

    #[cfg(test)]
    pub type OldValueRandomTest = Box<dyn Fn(Arc<RocksSnapshot>, TimeStamp) -> Result<OldValue>>;
    #[cfg(test)]
    pub fn old_value_random(
        key: &[u8],
        require_old_value_none: bool,
        tests: Vec<OldValueRandomTest>,
    ) {
        let mut ts = 1u64;
        let mut tso = || {
            ts += 1;
            ts
        };

        use std::time::SystemTime;
        // A simple valid operation sequence: p[prld]*
        // p: put, r: rollback, l: lock, d: delete
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rg = rand::rngs::StdRng::seed_from_u64(seed);

        // Generate 1000 random cases;
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cases = 1000;
        for _ in 0..cases {
            // At most 12 ops per-case.
            let ops_count = rg.gen::<u8>() % 12;
            let ops = (0..ops_count)
                .into_iter()
                .enumerate()
                .map(|(i, _)| {
                    if i == 0 {
                        // The first op must be put.
                        0
                    } else {
                        rg.gen::<u8>() % 4
                    }
                })
                .collect::<Vec<_>>();

            for (i, op) in ops.iter().enumerate() {
                let start_ts = tso();
                let commit_ts = tso();

                match op {
                    0 => {
                        must_prewrite_put(&engine, key, &[i as u8], key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    1 => {
                        must_prewrite_delete(&engine, key, key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    2 => {
                        must_prewrite_lock(&engine, key, key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    3 => {
                        must_prewrite_put(&engine, key, &[i as u8], key, start_ts);
                        must_rollback(&engine, key, start_ts, false);
                    }
                    _ => unreachable!(),
                }
            }
            let start_ts = TimeStamp::from(tso());
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let expect = {
                let mut reader = SnapshotReader::new(start_ts, snapshot.clone(), true);
                if let Some(write) = reader
                    .reader
                    .get_write(&Key::from_raw(key), start_ts, Some(start_ts))
                    .unwrap()
                {
                    assert_eq!(write.write_type, WriteType::Put);
                    match write.short_value {
                        Some(value) => OldValue::Value { value },
                        None => OldValue::ValueTimeStamp {
                            start_ts: write.start_ts,
                        },
                    }
                } else {
                    OldValue::None
                }
            };
            if require_old_value_none && expect != OldValue::None {
                continue;
            }
            for test in &tests {
                match test(snapshot.clone(), start_ts) {
                    Ok(old_value) => {
                        assert_eq!(old_value, expect, "seed: {} ops: {:?}", seed, ops);
                    }
                    Err(e) => {
                        panic!("error: {:?} seed: {} ops: {:?}", e, seed, ops);
                    }
                }
            }
        }
    }

    #[test]
    fn test_old_value_random() {
        let key = b"k1";
        let require_old_value_none = false;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |snapshot, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut txn = MvccTxn::new(start_ts, cm);
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                let txn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                };
                let (_, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &txn_props,
                    Mutation::Put((Key::from_raw(key), b"v2".to_vec())),
                    &None,
                    false,
                )?;
                Ok(old_value)
            })],
        )
    }

    #[test]
    fn test_old_value_random_none() {
        let key = b"k1";
        let require_old_value_none = true;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |snapshot, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut txn = MvccTxn::new(start_ts, cm);
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                let txn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                };
                let (_, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &txn_props,
                    Mutation::Insert((Key::from_raw(key), b"v2".to_vec())),
                    &None,
                    false,
                )?;
                Ok(old_value)
            })],
        )
    }
}
