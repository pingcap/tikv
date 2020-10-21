// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::shared::prewrite_key_value;
use crate::storage::mvcc::{
    metrics::MVCC_DUPLICATE_CMD_COUNTER_VEC, ErrorInner, LockType, Mutation, MvccTxn,
    Result as MvccResult, TimeStamp,
};
use crate::storage::Snapshot;

pub fn pessimistic_prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_lock: bool,
    mut lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    mut min_commit_ts: TimeStamp,
    pipelined_pessimistic_lock: bool,
) -> MvccResult<TimeStamp> {
    if mutation.should_not_write() {
        return Err(box_err!(
            "cannot handle checkNotExists in pessimistic prewrite"
        ));
    }
    let mutation_type = mutation.mutation_type();
    let lock_type = LockType::from_mutation(&mutation);
    let (key, value) = mutation.into_key_value();

    fail_point!("pessimistic_prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    if let Some(lock) = txn.reader.load_lock(&key)? {
        if lock.ts != txn.start_ts {
            // Abort on lock belonging to other transaction if
            // prewrites a pessimistic lock.
            if is_pessimistic_lock {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => txn.start_ts,
                    "key" => %key,
                    "lock_ts" => lock.ts
                );
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: txn.start_ts,
                    key: key.into_raw()?,
                }
                .into());
            }
            return Err(txn
                .handle_non_pessimistic_lock_conflict(key, lock)
                .unwrap_err());
        } else {
            if lock.lock_type != LockType::Pessimistic {
                // Duplicated command. No need to overwrite the lock and data.
                MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                return Ok(lock.min_commit_ts);
            }
            // The lock is pessimistic and owned by this txn, go through to overwrite it.
            // The ttl and min_commit_ts of the lock may have been pushed forward.
            lock_ttl = std::cmp::max(lock_ttl, lock.ttl);
            min_commit_ts = std::cmp::max(min_commit_ts, lock.min_commit_ts);
        }
    } else if is_pessimistic_lock {
        txn.amend_pessimistic_lock(pipelined_pessimistic_lock, &key)?;
    }

    txn.check_extra_op(&key, mutation_type, None)?;
    // No need to check data constraint, it's resolved by pessimistic locks.
    prewrite_key_value(
        txn,
        key,
        lock_type.unwrap(),
        primary,
        secondary_keys,
        value,
        lock_ttl,
        for_update_ts,
        txn_size,
        min_commit_ts,
    )
}

pub mod tests {
    use super::*;
    use crate::storage::mvcc::{Key, MvccTxn, Result as MvccResult, TimeStamp};
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        pessimistic_prewrite(
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            TimeStamp::default(),
            0,
            TimeStamp::default(),
            false,
        )?;
        Ok(())
    }
}
