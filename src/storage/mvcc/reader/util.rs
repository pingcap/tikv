// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::default_not_found_error;
use crate::storage::mvcc::{Error, Result};
use crate::storage::mvcc::{Lock, LockType, Write};
use crate::storage::{Cursor, Iterator, Key, Statistics, Value};

/// Representing check lock result.
#[derive(Debug)]
pub enum CheckLockResult {
    /// Key is locked. The key lock error is included.
    Locked(Error),

    /// Key is not locked or the lock is ignored.
    NotLocked,
}

/// Checks whether the lock conflicts with the given `ts`. If `ts == MaxU64`, the latest
/// committed version will be returned for primary key instead of leading to lock conflicts.
#[inline]
pub fn check_lock(key: &Key, ts: u64, lock: &Lock) -> Result<CheckLockResult> {
    if lock.ts > ts || lock.lock_type == LockType::Lock || lock.lock_type == LockType::Pessimistic {
        // Ignore lock when lock.ts > ts or lock's type is Lock or Pessimistic
        return Ok(CheckLockResult::NotLocked);
    }

    let raw_key = key.to_raw()?;

    if ts == std::u64::MAX && raw_key == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we ignore this lock.
        return Ok(CheckLockResult::NotLocked);
    }

    // There is a pending lock. Client should wait or clean it.
    let mut info = kvproto::kvrpcpb::LockInfo::default();
    info.set_primary_lock(lock.primary.clone());
    info.set_lock_version(lock.ts);
    info.set_key(raw_key);
    info.set_lock_ttl(lock.ttl);
    info.set_txn_size(lock.txn_size);
    Ok(CheckLockResult::Locked(Error::KeyIsLocked(info)))
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` operation.
///
/// Notice that the value may be already carried in the `write` (short value). In this
/// case, you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data corruption.
pub fn near_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    write: Write,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    assert!(write.short_value.is_none());
    let seek_key = user_key.clone().append_ts(write.start_ts);
    default_cursor.near_seek(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            write,
            "near_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

/// Similar to `near_load_data_by_write`, but accepts a `BackwardCursor` and use
/// `near_seek_for_prev` internally.
pub fn near_reverse_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `BackwardCursor`.
    user_key: &Key,
    write: Write,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    assert!(write.short_value.is_none());
    let seek_key = user_key.clone().append_ts(write.start_ts);
    default_cursor.near_seek_for_prev(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            write,
            "near_reverse_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_lock() {
        let expect_locked = |res| match res {
            CheckLockResult::Locked(_) => (),
            _ => panic!("unexpected CheckLockResult: {:?}", res),
        };

        let expect_not_locked = |res| match res {
            CheckLockResult::NotLocked => (),
            _ => panic!("unexpected CheckLockResult: {:?}", res),
        };

        let key = Key::from_raw(b"foo");
        let mut lock = Lock::new(LockType::Put, vec![], 100, 3, None, 0, 1);

        // Ignore the lock if read ts is less than the lock version
        expect_not_locked(check_lock(&key, 50, &lock).unwrap());

        // Returns the lock if read ts >= lock version
        expect_locked(check_lock(&key, 110, &lock).unwrap());

        // Ignore the lock if it is Lock or Pessimistic.
        lock.lock_type = LockType::Lock;
        expect_not_locked(check_lock(&key, 110, &lock).unwrap());
        lock.lock_type = LockType::Pessimistic;
        expect_not_locked(check_lock(&key, 110, &lock).unwrap());

        // Ignore the primary lock when reading the latest committed version by setting u64::MAX as ts
        lock.lock_type = LockType::Put;
        lock.primary = b"foo".to_vec();
        expect_not_locked(check_lock(&key, std::u64::MAX, &lock).unwrap());

        // Should not ignore the secondary lock even though reading the latest version
        lock.primary = b"bar".to_vec();
        expect_locked(check_lock(&key, std::u64::MAX, &lock).unwrap());
    }
}
