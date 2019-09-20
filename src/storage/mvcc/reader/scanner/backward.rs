// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use engine::CF_DEFAULT;
use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::write::{Write, WriteType};
use crate::storage::mvcc::Result;
use crate::storage::{Cursor, Key, Lock, Snapshot, Statistics, Value};

use super::super::util::CheckLockResult;
use super::ScannerConfig;

/// This struct can be used to scan keys starting from the given user key in the reverse order
/// (less than).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be tried. If the
/// isolation level is SI, locks will be checked first.
///
/// Use `ScannerBuilder` to build `BackwardScanner`.
pub struct BackwardScanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    lock_cursor: Cursor<S::Iter>,
    latest_cursor: Cursor<S::Iter>,
    /// `history cursor` is lazy created only when it's needed.
    history_cursor: Option<Cursor<S::Iter>>,
    history_valid: bool,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
}

impl<S: Snapshot> BackwardScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        latest_cursor: Cursor<S::Iter>,
    ) -> BackwardScanner<S> {
        BackwardScanner {
            cfg,
            lock_cursor,
            latest_cursor,
            statistics: Statistics::default(),
            history_cursor: None,
            history_valid: true,
            is_started: false,
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }

    pub fn check_locks(&mut self) -> Result<()> {
        if self.cfg.upper_bound.is_some() {
            self.lock_cursor.reverse_seek(
                self.cfg.upper_bound.as_ref().unwrap(),
                &mut self.statistics.lock,
            )?;
        } else {
            self.lock_cursor.seek_to_last(&mut self.statistics.lock);
        }

        loop {
            if self.lock_cursor.valid()? {
                let current_user_key = self.lock_cursor.key(&mut self.statistics.lock).to_vec();
                let lock = Lock::parse(self.lock_cursor.valud(&mut self.statistics.lock))?;
                if let CheckLockResult::Locked(e) =
                    super::super::util::check_lock(&current_user_key, self.cfg.ts, &lock)?
                {
                    return Err(e);
                }
            } else {
                return Ok(());
            }
            self.lock_cursor.prev(&mut self.statistics.lock);
        }
    }

    fn ensure_history_cursor(&mut self) -> Result<()> {
        if self.history_cursor.is_some() {
            Ok(())
        } else {
            self.history_cursor = Some(self.cfg.create_cf_cursor(CF_HISTORY)?);
            Ok(())
        }
    }

    /// Get the next key-value pair, in backward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.is_started {
            if self.cfg.upper_bound.is_some() {
                // TODO: `seek_to_last` is better, however it has performance issues currently.
                // TODO: We have no guarantee about whether or not the upper_bound has a
                // timestamp suffix, so currently it is not safe to change write_cursor's
                // reverse_seek to seek_for_prev. However in future, once we have different types
                // for them, this can be done safely.
                self.latest_cursor.reverse_seek(
                    self.cfg.upper_bound.as_ref().unwrap(),
                    &mut self.statistics.write,
                )?;
            } else {
                self.latest_cursor.seek_to_last(&mut self.statistics.write);
            }
            self.is_started = true;
        }

        let mut found = false;
        loop {
            if self.latest_cursor.valid()? {
                let key = self.latest_cursor.key(&mut self.statistics.latest).to_vec();
                let mut value = None;
                let mut latest =
                    Write::parse(self.latest_cursor.value(&mut self.statistics.latest))?;
                if self.cfg.ts >= latest.commit_ts {
                    found = true;
                    value = latest.take_value();
                } else if self.history_valid {
                    // seek from history
                    self.ensure_history_cursor();
                    let seek_key = key.clone().append_ts(self.cfg.ts);
                    self.history_cursor
                        .near_seek_for_prev(&seek_key, &mut statistics.history)?;
                    if self.history_cursor.vliad()? {
                        let history_key = self.history_cursor.key(&mut self.statistics.history);
                        if Key::is_user_key_eq(history_key, &key) {
                            found = true;
                            value = Some(self.history_cursor.value(&mut self.statistics.history));
                        }
                    } else {
                        self.history_valid = false;
                    }
                }
                // move to prev key
                self.latest_cursor.prev(&mut self.statistics.latest);
                if found {
                    return Ok(Some((key, value.unwrap())));
                }
            } else {
                return Ok(None);
            }
        }
    }
}
