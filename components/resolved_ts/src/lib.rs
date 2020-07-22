// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate slog_global;

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use tikv_util::collections::HashSet;
use txn_types::TimeStamp;

#[derive(PartialEq, Debug)]
pub struct TrackedTxn {
    pub min_commit_ts: TimeStamp,
    pub primary: Vec<u8>,
    pub locked_keys: HashSet<Vec<u8>>,
}

impl TrackedTxn {
    fn new(primary: Vec<u8>) -> Self {
        Self {
            min_commit_ts: TimeStamp::zero(),
            primary,
            locked_keys: Default::default(),
        }
    }
}

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // start_ts -> TrackedTxn.
    locks: HashMap<TimeStamp, TrackedTxn>,
    // min_commit_ts -> start_ts.
    txn_min_commit_ts: BTreeMap<TimeStamp, HashSet<TimeStamp>>,
    // The timestamps that guarantees no more commit will happen before.
    // None if the resolver is not initialized.
    resolved_ts: Option<TimeStamp>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
}

impl Resolver {
    pub fn new(region_id: u64) -> Resolver {
        Resolver {
            region_id,
            locks: HashMap::new(),
            txn_min_commit_ts: BTreeMap::new(),
            resolved_ts: None,
            min_ts: TimeStamp::zero(),
        }
    }

    pub fn init(&mut self) {
        self.resolved_ts = Some(TimeStamp::zero());
    }

    pub fn resolved_ts(&self) -> Option<TimeStamp> {
        self.resolved_ts
    }

    pub fn locks(&self) -> &HashMap<TimeStamp, TrackedTxn> {
        &self.locks
    }

    pub fn track_lock(
        &mut self,
        start_ts: TimeStamp,
        mut min_commit_ts: TimeStamp,
        key: Vec<u8>,
        primary: Vec<u8>,
    ) {
        if min_commit_ts < start_ts {
            min_commit_ts = start_ts.next();
        }
        debug!(
            "track lock {}@{}-{}, region {}",
            hex::encode_upper(key.clone()),
            start_ts,
            min_commit_ts,
            self.region_id
        );

        let mut locks_entry = self
            .locks
            .entry(start_ts)
            .or_insert_with(|| TrackedTxn::new(primary));
        if locks_entry.min_commit_ts < min_commit_ts {
            Self::update_min_commit_ts_map(
                &mut self.txn_min_commit_ts,
                start_ts,
                locks_entry.min_commit_ts,
                min_commit_ts,
            );
            locks_entry.min_commit_ts = min_commit_ts;
        }
        locks_entry.locked_keys.insert(key);
    }

    pub fn untrack_lock(
        &mut self,
        start_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
        key: Vec<u8>,
    ) {
        debug!(
            "untrack lock {}@{}, commit@{}, region {}",
            hex::encode_upper(key.clone()),
            start_ts,
            commit_ts.clone().unwrap_or_else(TimeStamp::zero),
            self.region_id,
        );
        if let Some(commit_ts) = commit_ts {
            assert!(
                self.resolved_ts.map_or(true, |rts| commit_ts > rts),
                "{}@{}, commit@{} < {:?}, region {}",
                hex::encode_upper(key),
                start_ts,
                commit_ts,
                self.resolved_ts,
                self.region_id
            );
            assert!(
                commit_ts > self.min_ts,
                "{}@{}, commit@{} < {:?}, region {}",
                hex::encode_upper(key),
                start_ts,
                commit_ts,
                self.min_ts,
                self.region_id
            );
        }

        let entry = self.locks.get_mut(&start_ts);
        // It's possible that rollback happens on a not existing transaction.
        assert!(
            entry.is_some() || commit_ts.is_none(),
            "{}@{}, commit@{} is not tracked, region {}",
            hex::encode_upper(key),
            start_ts,
            commit_ts.unwrap_or_else(TimeStamp::zero),
            self.region_id
        );
        if let Some(TrackedTxn {
            min_commit_ts,
            locked_keys,
            ..
        }) = entry
        {
            let min_commit_ts = *min_commit_ts;
            assert!(
                locked_keys.remove(&key) || commit_ts.is_none(),
                "{}@{}, commit@{} is not tracked, region {}, {:?}",
                hex::encode_upper(key),
                start_ts,
                commit_ts.unwrap_or_else(TimeStamp::zero),
                self.region_id,
                locked_keys
            );
            if locked_keys.is_empty() {
                self.locks.remove(&start_ts);
                Self::remove_min_commit_ts_entry(
                    &mut self.txn_min_commit_ts,
                    start_ts,
                    min_commit_ts,
                );
            }
        }
    }

    fn update_min_commit_ts_map(
        txn_min_commit_ts: &mut BTreeMap<TimeStamp, HashSet<TimeStamp>>,
        start_ts: TimeStamp,
        old_min_commit_ts: TimeStamp,
        new_min_commit_ts: TimeStamp,
    ) {
        if !old_min_commit_ts.is_zero() {
            Self::remove_min_commit_ts_entry(txn_min_commit_ts, start_ts, old_min_commit_ts);
        }

        txn_min_commit_ts
            .entry(new_min_commit_ts)
            .or_default()
            .insert(start_ts);
    }

    fn remove_min_commit_ts_entry(
        txn_min_commit_ts: &mut BTreeMap<TimeStamp, HashSet<TimeStamp>>,
        start_ts: TimeStamp,
        min_commit_ts: TimeStamp,
    ) {
        let entry = txn_min_commit_ts
            .get_mut(&min_commit_ts)
            .unwrap_or_else(|| {
                panic!(
                    "missing min_commit_ts record for txn {}, whose min_commit_ts is expected to be {}",
                    start_ts,
                    min_commit_ts
                );
            });
        assert!(
            entry.remove(&start_ts),
            "missing min_commit_ts record for txn {}, whose min_commit_ts is expected to be {}",
            start_ts,
            min_commit_ts
        );
        if entry.is_empty() {
            txn_min_commit_ts.remove(&min_commit_ts);
        }
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    /// Return None means the resolver is not initialized.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        let old_resolved_ts = self.resolved_ts?;

        // Find the min_commit_ts.
        let min_lock = self.txn_min_commit_ts.keys().next().cloned();
        let has_lock = min_lock.is_some();
        let min_lock_ts = min_lock.map(TimeStamp::prev).unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_lock_ts, min_ts);
        // Resolved ts never decrease.
        self.resolved_ts = Some(cmp::max(old_resolved_ts, new_resolved_ts));

        let new_min_ts = if has_lock {
            // If there are some lock, the min_ts must be smaller than
            // the min start ts, so it guarantees to be smaller than
            // any late arriving commit ts.
            new_resolved_ts // cmp::min(min_lock_ts - 1, min_ts)
        } else {
            min_ts
        };
        // Min ts never decrease.
        self.min_ts = cmp::max(self.min_ts, new_min_ts);

        self.resolved_ts
    }

    /// Gets information of unfinished transactions whose `min_commit_ts` is before the given ts.
    ///
    /// Returns a vector of (start_ts, primary key) pairs.
    pub fn txn_before_ts(&self, ts: TimeStamp) -> Vec<(TimeStamp, Vec<u8>)> {
        self.txn_min_commit_ts
            .range(..ts)
            .map(|(_, start_ts)| start_ts)
            .flatten()
            .map(|start_ts| {
                let txn = self.locks.get(start_ts).unwrap();
                (*start_ts, txn.primary.clone())
            })
            .collect()
    }

    pub fn update_txn_status(
        &mut self,
        txn_status: impl IntoIterator<Item = (TimeStamp, TimeStamp)>,
    ) {
        for (start_ts, min_commit_ts) in txn_status.into_iter() {
            if let Some(txn) = self.locks.get_mut(&start_ts) {
                if txn.min_commit_ts < min_commit_ts {
                    Self::update_min_commit_ts_map(
                        &mut self.txn_min_commit_ts,
                        start_ts,
                        txn.min_commit_ts,
                        min_commit_ts,
                    );
                    txn.min_commit_ts = min_commit_ts;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use txn_types::Key;

    #[derive(Clone)]
    enum Event {
        // start_ts, min_commit_ts, key
        Lock(u64, u64, Key),
        Unlock(u64, Option<u64>, Key),
        // min_ts, expect
        Resolve(u64, u64),
    }

    #[test]
    fn test_resolve() {
        let cases = vec![
            vec![Event::Lock(1, 0, Key::from_raw(b"a")), Event::Resolve(2, 1)],
            vec![
                Event::Lock(1, 0, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(3, 0, Key::from_raw(b"a")),
                Event::Unlock(3, Some(4), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(1, 0, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Lock(1, 0, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Lock(2, 0, Key::from_raw(b"a")),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller start_ts.
                Event::Lock(1, 0, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(1, Some(4), Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Unlock(1, None, Key::from_raw(b"a")),
                Event::Lock(2, 0, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Lock(2, 0, Key::from_raw(b"a")),
                Event::Resolve(4, 2),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(5, 5),
            ],
            // Rollback may contain a key that is not locked.
            vec![
                Event::Lock(1, 0, Key::from_raw(b"a")),
                Event::Unlock(1, None, Key::from_raw(b"b")),
                Event::Unlock(1, None, Key::from_raw(b"a")),
            ],
            // Lock may be updated.up
            vec![
                Event::Lock(1, 0, Key::from_raw(b"a")),
                Event::Lock(1, 3, Key::from_raw(b"a")),
                Event::Lock(1, 5, Key::from_raw(b"a")),
                Event::Lock(1, 7, Key::from_raw(b"a")),
                Event::Resolve(10, 6),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new(1);
            resolver.init();
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, min_commit_ts, key) => {
                        let key = key.into_raw().unwrap();
                        resolver.track_lock(start_ts.into(), min_commit_ts.into(), key.clone(), key)
                    }
                    Event::Unlock(start_ts, commit_ts, key) => resolver.untrack_lock(
                        start_ts.into(),
                        commit_ts.map(Into::into),
                        key.into_raw().unwrap(),
                    ),
                    Event::Resolve(min_ts, expect) => assert_eq!(
                        resolver.resolve(min_ts.into()).unwrap(),
                        expect.into(),
                        "case {}",
                        i
                    ),
                }
            }

            let mut resolver = Resolver::new(1);
            for e in case {
                match e {
                    Event::Lock(start_ts, min_commit_ts, key) => {
                        let key = key.into_raw().unwrap();
                        resolver.track_lock(start_ts.into(), min_commit_ts.into(), key.clone(), key)
                    }
                    Event::Unlock(start_ts, commit_ts, key) => resolver.untrack_lock(
                        start_ts.into(),
                        commit_ts.map(Into::into),
                        key.into_raw().unwrap(),
                    ),
                    Event::Resolve(min_ts, _) => {
                        assert_eq!(resolver.resolve(min_ts.into()), None, "case {}", i)
                    }
                }
            }
        }
    }

    #[test]
    fn test_get_txn_before_ts() {
        // Each case is represented with a vector of (start_ts, min_commit_ts, key, primary) tuple,
        // a ts to be used to invoke txn_before_ts, and the expected result.
        struct Case {
            locks: Vec<(u64, u64, &'static [u8], &'static [u8])>,
            ts: u64,
            expected_result: Vec<(u64, &'static [u8])>,
        }
        let cases = vec![
            Case {
                locks: vec![],
                ts: 10,
                expected_result: vec![],
            },
            Case {
                locks: vec![(10, 11, b"a", b"a"), (11, 12, b"c", b"d")],
                ts: 13,
                expected_result: vec![(10, b"a"), (11, b"d")],
            },
            Case {
                locks: vec![
                    (10, 16, b"a", b"a"),
                    (11, 12, b"c", b"d"),
                    (12, 14, b"e", b"f"),
                ],
                ts: 15,
                expected_result: vec![(11, b"d"), (12, b"f")],
            },
            Case {
                locks: vec![
                    (10, 0, b"a", b"b"),
                    (10, 20, b"b", b"b"),
                    (10, 0, b"c", b"b"),
                    (12, 18, b"e", b"e"),
                    (13, 18, b"f", b"f"),
                    (14, 18, b"g", b"g"),
                ],
                ts: 22,
                expected_result: vec![(10, b"b"), (12, b"e"), (13, b"f"), (14, b"g")],
            },
            Case {
                locks: vec![
                    (10, 0, b"a", b"b"),
                    (10, 20, b"b", b"b"),
                    (10, 0, b"c", b"b"),
                    (12, 18, b"e", b"e"),
                    (13, 18, b"f", b"f"),
                    (14, 18, b"g", b"g"),
                ],
                ts: 17,
                expected_result: vec![],
            },
            Case {
                locks: vec![
                    (10, 0, b"a", b"a"),
                    (11, 0, b"b", b"b"),
                    (12, 0, b"c", b"c"),
                ],
                ts: 12,
                expected_result: vec![(10, b"a")],
            },
        ];

        for (i, case) in cases.iter().enumerate() {
            let mut resolver = Resolver::new(1);
            resolver.init();
            for (start_ts, min_commit_ts, key, primary) in &case.locks {
                resolver.track_lock(
                    start_ts.into(),
                    min_commit_ts.into(),
                    key.to_vec(),
                    primary.to_vec(),
                );
            }
            let result = resolver.txn_before_ts(case.ts.into());
            let result_len = result.len();
            let result = result.into_iter().collect::<HashSet<_>>();
            // No duplicated items in the result.
            assert_eq!(result.len(), result_len);
            let expect = case
                .expected_result
                .iter()
                .map(|(start_ts, primary)| (TimeStamp::from(start_ts), primary.to_vec()))
                .collect::<HashSet<_>>();
            assert_eq!(result, expect, "case {}", i);
        }
    }
}
