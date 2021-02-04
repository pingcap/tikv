// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for the `engine_traits` crate
//!
//! These are basic tests that can be used to verify the conformance of
//! engines that implement the traits in the `engine_traits` crate.
//!
//! All engine instances are constructed through the `engine_test` crate,
//! so individual engines can be tested by setting that crate's feature flags.
//!
//! e.g. to test the `engine_sled` crate
//!
//! ```no_test
//! cargo test -p engine_traits_tests --no-default-features --features=protobuf-codec,test-engines-sled
//! ```

#![cfg(test)]


fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("tikv-engine-traits-tests")
        .tempdir()
        .unwrap()
}

struct TempDirEnginePair {
    // NB engine must drop before tempdir
    engine: engine_test::kv::KvTestEngine,
    tempdir: tempfile::TempDir,
}

fn default_engine() -> TempDirEnginePair {
    use engine_traits::CF_DEFAULT;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, &[CF_DEFAULT], None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}

fn engine_cfs(cfs: &[&str]) -> TempDirEnginePair {
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, cfs, None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}

fn assert_engine_error<T>(r: engine_traits::Result<T>) {
    match r {
        Err(engine_traits::Error::Engine(_)) => { },
        _ => panic!("expected Error::Engine"),
    }
}

mod ctor {
    //! Constructor tests

    use super::tempdir;

    use engine_traits::ALL_CFS;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::{EngineConstructorExt, DBOptions, CFOptions, ColumnFamilyOptions};

    #[test]
    fn new_engine_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let _db = KvTestEngine::new_engine(path, None, ALL_CFS, None).unwrap();
    }

    #[test]
    fn new_engine_opt_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cf_opts = ALL_CFS.iter().map(|cf| {
            CFOptions::new(cf, ColumnFamilyOptions::new())
        }).collect();
        let _db = KvTestEngine::new_engine_opt(path, db_opts, cf_opts).unwrap();
    }
}

mod basic_read_write {
    //! Reading and writing

    use super::{default_engine, engine_cfs};
    use engine_traits::{Peekable, SyncMutable};
    use engine_traits::{CF_WRITE, ALL_CFS, CF_DEFAULT};

    #[test]
    fn get_value_none() {
        let db = default_engine();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn put_get() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn get_value_cf_none() {
        let db = engine_cfs(&[CF_WRITE]);
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn put_get_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    // Store using put; load using get_cf(CF_DEFAULT)
    #[test]
    fn non_cf_methods_are_default_cf() {
        let db = engine_cfs(ALL_CFS);
        // Use the non-cf put function
        db.engine.put(b"foo", b"bar").unwrap();
        // Retreive with the cf get function
        let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn non_cf_methods_implicit_default_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
        // CF_DEFAULT always exists
        let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn delete_none() {
        let db = default_engine();
        let res = db.engine.delete(b"foo");
        assert!(res.is_ok());
    }

    #[test]
    fn delete_cf_none() {
        let db = engine_cfs(ALL_CFS);
        let res = db.engine.delete_cf(CF_WRITE, b"foo");
        assert!(res.is_ok());
    }

    #[test]
    fn delete() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_some());
        db.engine.delete(b"foo").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn delete_cf() {
        let db = engine_cfs(ALL_CFS);
        db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_some());
        db.engine.delete_cf(CF_WRITE, b"foo").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_none());
    }
}

mod scenario_writes {
    use super::engine_cfs;
    use engine_traits::{Peekable, SyncMutable, Result};
    use engine_traits::{CF_WRITE, ALL_CFS, CF_DEFAULT};
    use engine_traits::{WriteBatchExt, WriteBatch, Mutable};
    use engine_test::kv::KvTestEngine;

    #[derive(Eq, PartialEq)]
    enum WriteScenario {
        NoCf,
        DefaultCf,
        OtherCf,
        WriteBatchNoCf,
        WriteBatchDefaultCf,
        WriteBatchOtherCf,
    }

    struct WriteScenarioEngine {
        scenario: WriteScenario,
        db: crate::TempDirEnginePair,
    }

    impl WriteScenarioEngine {
        fn new(scenario: WriteScenario) -> WriteScenarioEngine {
            WriteScenarioEngine {
                scenario,
                db: engine_cfs(ALL_CFS),
            }
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
            use WriteScenario::*;
            match self.scenario {
                NoCf => self.db.engine.put(key, value),
                DefaultCf => self.db.engine.put_cf(CF_DEFAULT, key, value),
                OtherCf => self.db.engine.put_cf(CF_WRITE, key, value),
                WriteBatchNoCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.put(key, value)?;
                    wb.write()
                }
                WriteBatchDefaultCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.put_cf(CF_DEFAULT, key, value)?;
                    wb.write()
                }
                WriteBatchOtherCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.put_cf(CF_WRITE, key, value)?;
                    wb.write()
                }
            }
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            use WriteScenario::*;
            match self.scenario {
                NoCf => self.db.engine.delete(key),
                DefaultCf => self.db.engine.delete_cf(CF_DEFAULT, key),
                OtherCf => self.db.engine.delete_cf(CF_WRITE, key),
                WriteBatchNoCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete(key)?;
                    wb.write()
                }
                WriteBatchDefaultCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete_cf(CF_DEFAULT, key)?;
                    wb.write()
                }
                WriteBatchOtherCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete_cf(CF_WRITE, key)?;
                    wb.write()
                }
            }
        }

        fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
            use WriteScenario::*;
            match self.scenario {
                NoCf => self.db.engine.delete_range(start, end),
                DefaultCf => self.db.engine.delete_range_cf(CF_DEFAULT, start, end),
                OtherCf => self.db.engine.delete_range_cf(CF_WRITE, start, end),
                WriteBatchNoCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete_range(start, end)?;
                    wb.write()
                }
                WriteBatchDefaultCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete_range_cf(CF_DEFAULT, start, end)?;
                    wb.write()
                }
                WriteBatchOtherCf => {
                    let mut wb = self.db.engine.write_batch();
                    wb.delete_range_cf(CF_WRITE, start, end)?;
                    wb.write()
                }
            }
        }

        fn get_value(&self, key: &[u8]) -> Result<Option<<KvTestEngine as Peekable>::DBVector>> {
            use WriteScenario::*;
            match self.scenario {
                NoCf | DefaultCf | WriteBatchNoCf | WriteBatchDefaultCf => {
                    // Check that CF_DEFAULT is the default table
                    let r1 = self.db.engine.get_value(key);
                    let r2 = self.db.engine.get_value_cf(CF_DEFAULT, key);
                    match (&r1, &r2) {
                        (Ok(Some(ref r1)), Ok(Some(ref r2))) => assert_eq!(r1[..], r2[..]),
                        (Ok(None), Ok(None)) => { /* pass */ }
                        _ => { }
                    }
                    r1
                }
                OtherCf | WriteBatchOtherCf => self.db.engine.get_value_cf(CF_WRITE, key),
            }
        }
    }

    fn write_scenario_engine_no_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::NoCf)
    }

    fn write_scenario_engine_default_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::DefaultCf)
    }

    fn write_scenario_engine_other_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::OtherCf)
    }

    fn write_scenario_engine_write_batch_no_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::WriteBatchNoCf)
    }

    fn write_scenario_engine_write_batch_default_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::WriteBatchDefaultCf)
    }

    fn write_scenario_engine_write_batch_other_cf() -> WriteScenarioEngine {
        WriteScenarioEngine::new(WriteScenario::WriteBatchOtherCf)
    }

    macro_rules! scenario_test {
        ($name:ident $body:block) => {
            mod $name {
                mod no_cf {
                    use super::super::write_scenario_engine_no_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
                mod default_cf {
                    use super::super::write_scenario_engine_default_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
                mod other_cf {
                    use super::super::write_scenario_engine_other_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
                mod wb_no_cf {
                    use super::super::write_scenario_engine_write_batch_no_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
                mod wb_default_cf {
                    use super::super::write_scenario_engine_write_batch_default_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
                mod wb_other_cf {
                    use super::super::write_scenario_engine_write_batch_other_cf as write_scenario_engine;

                    #[test] fn $name() $body
                }
            }
        }
    }

    scenario_test! { delete {
        let db = write_scenario_engine();
        db.put(b"foo", b"bar").unwrap();
        let value = db.get_value(b"foo").unwrap();
        assert!(value.is_some());
        db.delete(b"foo").unwrap();
        let value = db.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }}


    scenario_test! { delete_range_inclusive_exclusive {
        let db = write_scenario_engine();

        db.put(b"a", b"").unwrap();
        db.put(b"b", b"").unwrap();
        db.put(b"c", b"").unwrap();
        db.put(b"d", b"").unwrap();
        db.put(b"e", b"").unwrap();

        db.delete_range(b"b", b"e").unwrap();

        assert!(db.get_value(b"a").unwrap().is_some());
        assert!(db.get_value(b"b").unwrap().is_none());
        assert!(db.get_value(b"c").unwrap().is_none());
        assert!(db.get_value(b"d").unwrap().is_none());
        assert!(db.get_value(b"e").unwrap().is_some());
    }}
}

mod delete_range {
    use super::{default_engine};
    use engine_traits::{SyncMutable, Peekable};
    use engine_traits::{CF_DEFAULT};
    use std::panic::{self, AssertUnwindSafe};

    #[test]
    fn delete_range_cf_inclusive_exclusive() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();

        db.engine.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn delete_range_cf_all_in_range() {
        let db = default_engine();

        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();

        db.engine.delete_range_cf(CF_DEFAULT, b"a", b"e").unwrap();

        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
    }

    #[test]
    fn delete_range_cf_equal_begin_and_end() {
        let db = default_engine();

        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();

        db.engine.delete_range_cf(CF_DEFAULT, b"c", b"c").unwrap();

        assert!(db.engine.get_value(b"b").unwrap().is_some());
        assert!(db.engine.get_value(b"c").unwrap().is_some());
        assert!(db.engine.get_value(b"d").unwrap().is_some());
    }

    #[test]
    fn delete_range_cf_reverse_range() {
        let db = default_engine();

        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            db.engine.delete_range_cf(CF_DEFAULT, b"d", b"b").unwrap();
        })).is_err());

        assert!(db.engine.get_value(b"b").unwrap().is_some());
        assert!(db.engine.get_value(b"c").unwrap().is_some());
        assert!(db.engine.get_value(b"d").unwrap().is_some());
    }

    #[test]
    fn delete_range_cf_bad_cf() {
        let db = default_engine();
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            db.engine.delete_range_cf("bogus", b"a", b"b").unwrap();
        })).is_err());
    }
}

mod cf_names {
    use super::{default_engine, engine_cfs};
    use engine_traits::{KvEngine, CFNamesExt, Snapshot};
    use engine_traits::{CF_DEFAULT, ALL_CFS, CF_WRITE};

    #[test]
    fn default_names() {
        let db = default_engine();
        let names = db.engine.cf_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], CF_DEFAULT);
    }

    #[test]
    fn cf_names() {
        let db = engine_cfs(ALL_CFS);
        let names = db.engine.cf_names();
        assert_eq!(names.len(), ALL_CFS.len());
        for cf in ALL_CFS {
            assert!(names.contains(cf));
        }
    }

    #[test]
    fn implicit_default_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        let names = db.engine.cf_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&CF_DEFAULT));
    }

    #[test]
    fn default_names_snapshot() {
        let db = default_engine();
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], CF_DEFAULT);
    }

    #[test]
    fn cf_names_snapshot() {
        let db = engine_cfs(ALL_CFS);
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), ALL_CFS.len());
        for cf in ALL_CFS {
            assert!(names.contains(cf));
        }
    }

    #[test]
    fn implicit_default_cf_snapshot() {
        let db = engine_cfs(&[CF_WRITE]);
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&CF_DEFAULT));
    }
}

mod iterator {
    use super::{default_engine};
    use engine_traits::{Iterable, Iterator, KvEngine};
    use engine_traits::SeekKey;
    use std::panic::{self, AssertUnwindSafe};

    fn iter_empty<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        let mut iter = i(e);

        assert_eq!(iter.valid().unwrap(), false);

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.prev();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.next();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());

        assert_eq!(iter.seek(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::Key(b"foo")).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Key(b"foo")).unwrap(), false);
    }

    #[test]
    fn iter_empty_engine() {
        let db = default_engine();
        iter_empty(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_empty_snapshot() {
        let db = default_engine();
        iter_empty(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

    #[test]
    fn iter_forward_engine() {
        let db = default_engine();
        iter_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_forward_snapshot() {
        let db = default_engine();
        iter_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

    #[test]
    fn iter_reverse_engine() {
        let db = default_engine();
        iter_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_reverse_snapshot() {
        let db = default_engine();
        iter_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_to_key_then_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_to_key_then_forward_engine() {
        let db = default_engine();
        seek_to_key_then_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_to_key_then_forward_snapshot() {
        let db = default_engine();
        seek_to_key_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_to_key_then_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_to_key_then_reverse_engine() {
        let db = default_engine();
        seek_to_key_then_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_to_key_then_reverse_snapshot() {
        let db = default_engine();
        seek_to_key_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_forward_then_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn iter_forward_then_reverse_engine() {
        let db = default_engine();
        iter_forward_then_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_forward_then_reverse_snapshot() {
        let db = default_engine();
        iter_forward_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_reverse_then_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn iter_reverse_then_forward_engine() {
        let db = default_engine();
        iter_reverse_then_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_reverse_then_forward_snapshot() {
        let db = default_engine();
        iter_reverse_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    // When seek finds an exact key then seek_for_prev behaves just like seek
    fn seek_for_prev<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek_for_prev(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert!(iter.seek_for_prev(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert!(iter.seek_for_prev(SeekKey::Key(b"c")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");
    }

    #[test]
    fn seek_for_prev_engine() {
        let db = default_engine();
        seek_for_prev(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_for_prev_snapshot() {
        let db = default_engine();
        seek_for_prev(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    // When Seek::Key doesn't find an exact match,
    // it still might succeed, but its behavior differs
    // based on whether `seek` or `seek_for_prev` is called.
    fn seek_key_miss<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek(SeekKey::Key(b"d")).unwrap());
        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_key_miss_engine() {
        let db = default_engine();
        seek_key_miss(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_key_miss_snapshot() {
        let db = default_engine();
        seek_key_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_key_prev_miss<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek_for_prev(SeekKey::Key(b"d")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek_for_prev(SeekKey::Key(b"b")).unwrap());
        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_key_prev_miss_engine() {
        let db = default_engine();
        seek_key_prev_miss(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_key_prev_miss_snapshot() {
        let db = default_engine();
        seek_key_prev_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
    }
}

mod snapshot_basic {
    use super::{default_engine, engine_cfs};
    use engine_traits::{KvEngine, SyncMutable, Peekable};
    use engine_traits::{ALL_CFS, CF_WRITE};

    #[test]
    fn snapshot_get_value() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        let value = snap.get_value(b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");

        let value = snap.get_value(b"b").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn snapshot_get_value_after_put() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        db.engine.put(b"a", b"aaa").unwrap();

        let value = snap.get_value(b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");
    }

    #[test]
    fn snapshot_get_value_cf() {
        let db = engine_cfs(ALL_CFS);

        db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");

        let value = snap.get_value_cf(CF_WRITE, b"b").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn snapshot_get_value_cf_after_put() {
        let db = engine_cfs(ALL_CFS);

        db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        db.engine.put_cf(CF_WRITE, b"a", b"aaa").unwrap();

        let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");
    }
}

mod read_consistency {
    //! Testing iterator and snapshot behavior in the presence of intermixed writes

    use super::{default_engine};
    use engine_traits::{KvEngine, SyncMutable, Peekable};
    use engine_traits::{Iterable, Iterator};

    #[test]
    fn snapshot_with_writes() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snapshot = db.engine.snapshot();

        assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");

        db.engine.put(b"b", b"bb").unwrap();

        assert!(snapshot.get_value(b"b").unwrap().is_none());
        assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");

        db.engine.delete(b"a").unwrap();

        assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");
        assert!(db.engine.get_value(b"a").unwrap().is_none());
    }

    // Both the snapshot and engine iterators maintain read consistency at a
    // single point in time. It seems the engine iterator is essentially just a
    // snapshot iterator.
    fn iterator_with_writes<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"").unwrap();
        e.put(b"c", b"").unwrap();

        let mut iter = i(e);

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), b"a");

        e.put(b"b", b"").unwrap();

        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), b"c");
        assert!(e.get_value(b"b").unwrap().is_some());

        e.put(b"d", b"").unwrap();

        assert!(!iter.next().unwrap());
        assert!(e.get_value(b"d").unwrap().is_some());

        e.delete(b"a").unwrap();
        e.delete(b"c").unwrap();

        iter.seek_to_first().unwrap();
        assert_eq!(iter.key(), b"a");
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), b"c");
        assert!(!iter.next().unwrap());

        assert!(e.get_value(b"a").unwrap().is_none());
        assert!(e.get_value(b"c").unwrap().is_none());
    }

    #[test]
    fn iterator_with_writes_engine() {
        let db = default_engine();
        iterator_with_writes(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iterator_with_writes_snapshot() {
        let db = default_engine();
        iterator_with_writes(&db.engine, |e| e.snapshot().iterator().unwrap());
    }
}

mod write_batch {
    use super::{default_engine, assert_engine_error};
    use engine_test::kv::KvTestEngine;
    use engine_traits::{Mutable, Peekable, WriteBatchExt, SyncMutable, WriteBatch};
    use engine_traits::CF_DEFAULT;
    use std::panic::{self, AssertUnwindSafe};

    #[test]
    fn write_batch_none_no_commit() {
        let db = default_engine();
        let wb = db.engine.write_batch();
        drop(wb);
    }

    #[test]
    fn write_batch_none() {
        let db = default_engine();
        let wb = db.engine.write_batch();
        wb.write().unwrap();
    }

    #[test]
    fn write_batch_put() {
        let db = default_engine();

        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"aa").unwrap();

        wb.write().unwrap();

        assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    }

    #[test]
    fn write_batch_delete() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let mut wb = db.engine.write_batch();

        wb.delete(b"a").unwrap();

        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_none());
    }

    #[test]
    fn write_batch_write_twice_1() {
        let db = default_engine();

        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"aa").unwrap();

        wb.write().unwrap();
        wb.write().unwrap();

        assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    }

    #[test]
    fn write_batch_write_twice_2() {
        let db = default_engine();

        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"aa").unwrap();

        wb.write().unwrap();

        db.engine.put(b"a", b"b").unwrap();
        assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"b");

        wb.write().unwrap();

        assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
    }

    #[test]
    fn write_batch_write_twice_3() {
        let db = default_engine();

        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"aa").unwrap();

        wb.write().unwrap();
        db.engine.put(b"a", b"b").unwrap();
        wb.put(b"b", b"bb").unwrap();
        wb.write().unwrap();

        assert_eq!(db.engine.get_value(b"a").unwrap().unwrap(), b"aa");
        assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");
    }

    #[test]
    fn write_batch_delete_range_cf_basic() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_inexact() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
        db.engine.put(b"g", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"f").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_none());
        assert!(db.engine.get_value(b"f").unwrap().is_none());
        assert!(db.engine.get_value(b"g").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_after_put() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"").unwrap();
        wb.put(b"b", b"").unwrap();
        wb.put(b"c", b"").unwrap();
        wb.put(b"d", b"").unwrap();
        wb.put(b"e", b"").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_none() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_twice() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_twice_1() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_twice_2() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
        db.engine.put(b"e", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();
        db.engine.put(b"c", b"").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"b", b"e").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_empty_range() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"b", b"b").unwrap();
        wb.write().unwrap();

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_some());
        assert!(db.engine.get_value(b"c").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_backward_range() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        wb.delete_range_cf(CF_DEFAULT, b"c", b"a").unwrap();
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            wb.write().unwrap();
        })).is_err());

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_some());
        assert!(db.engine.get_value(b"c").unwrap().is_some());
    }

    #[test]
    fn write_batch_delete_range_cf_backward_range_partial_commit() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"b", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();
           
        let mut wb = db.engine.write_batch();

        // Everything in the write batch before the panic
        // due to bad range is going to end up committed.
        wb.put(b"e", b"").unwrap();
        wb.delete(b"d").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"c", b"a").unwrap();
        wb.put(b"f", b"").unwrap();
        wb.delete(b"a").unwrap();

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            wb.write().unwrap();
        })).is_err());

        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"b").unwrap().is_some());
        assert!(db.engine.get_value(b"c").unwrap().is_some());
        assert!(db.engine.get_value(b"d").unwrap().is_none());
        assert!(db.engine.get_value(b"e").unwrap().is_some());
        assert!(db.engine.get_value(b"f").unwrap().is_none());
    }

    #[test]
    fn write_batch_is_empty() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        assert!(wb.is_empty());
        wb.put(b"a", b"").unwrap();
        assert!(!wb.is_empty());
        wb.write().unwrap();
        assert!(!wb.is_empty());
    }

    #[test]
    fn write_batch_count() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        assert_eq!(wb.count(), 0);
        wb.put(b"a", b"").unwrap();
        assert_eq!(wb.count(), 1);
        wb.write().unwrap();
        assert_eq!(wb.count(), 1);
    }

    #[test]
    fn write_batch_count_2() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        assert_eq!(wb.count(), 0);
        wb.put(b"a", b"").unwrap();
        assert_eq!(wb.count(), 1);
        wb.delete(b"a").unwrap();
        assert_eq!(wb.count(), 2);
        wb.delete_range_cf(CF_DEFAULT, b"a", b"b").unwrap();
        assert_eq!(wb.count(), 3);
        wb.write().unwrap();
        assert_eq!(wb.count(), 3);
    }

    #[test]
    fn write_batch_clear() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"").unwrap();
        wb.put(b"b", b"").unwrap();
        wb.clear();
        assert!(wb.is_empty());
        assert_eq!(wb.count(), 0);
        wb.write().unwrap();
        assert!(db.engine.get_value(b"a").unwrap().is_none());
    }

    #[test]
    fn cap_zero() {
        let db = default_engine();
        let mut wb = db.engine.write_batch_with_cap(0);
        wb.put(b"a", b"").unwrap();
        wb.put(b"b", b"").unwrap();
        wb.put(b"c", b"").unwrap();
        wb.put(b"d", b"").unwrap();
        wb.put(b"e", b"").unwrap();
        wb.put(b"f", b"").unwrap();
        wb.write().unwrap();
        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"f").unwrap().is_some());
    }

    /// Write batch capacity seems to just be a suggestions
    #[test]
    fn cap_two() {
        let db = default_engine();
        let mut wb = db.engine.write_batch_with_cap(2);
        wb.put(b"a", b"").unwrap();
        wb.put(b"b", b"").unwrap();
        wb.put(b"c", b"").unwrap();
        wb.put(b"d", b"").unwrap();
        wb.put(b"e", b"").unwrap();
        wb.put(b"f", b"").unwrap();
        wb.write().unwrap();
        assert!(db.engine.get_value(b"a").unwrap().is_some());
        assert!(db.engine.get_value(b"f").unwrap().is_some());
    }

    // We should write when count is greater than WRITE_BATCH_MAX_KEYS
    #[test]
    fn should_write_to_engine() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();
        let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

        let mut key = vec![];
        loop {
            key.push(b'a');
            wb.put(&key, b"").unwrap();
            if key.len() <= max_keys {
                assert!(!wb.should_write_to_engine());
            }
            if key.len() == max_keys + 1 {
                assert!(wb.should_write_to_engine());
                wb.write().unwrap();
                break;
            }
        }
    }

    // But there kind of aren't consequences for making huge write batches
    #[test]
    fn should_write_to_engine_but_whatever() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();
        let max_keys = KvTestEngine::WRITE_BATCH_MAX_KEYS;

        let mut key = vec![];
        loop {
            key.push(b'a');
            wb.put(&key, b"").unwrap();
            if key.len() <= max_keys {
                assert!(!wb.should_write_to_engine());
            }
            if key.len() > max_keys {
                assert!(wb.should_write_to_engine());
            }
            if key.len() == max_keys * 2 {
                assert!(wb.should_write_to_engine());
                wb.write().unwrap();
                break;
            }
        }

        let mut key = vec![];
        loop {
            key.push(b'a');
            assert!(db.engine.get_value(&key).unwrap().is_some());
            if key.len() == max_keys * 2 {
                break;
            }
        }
    }

    #[test]
    fn data_size() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        let size1 = wb.data_size();
        wb.put(b"a", b"").unwrap();
        let size2 = wb.data_size();
        assert!(size1 < size2);
        wb.write().unwrap();
        let size3 = wb.data_size();
        assert_eq!(size2, size3);
        wb.clear();
        let size4 = wb.data_size();
        assert_eq!(size4, size1);
        wb.put(b"a", b"").unwrap();
        let size5 = wb.data_size();
        assert!(size4 < size5);
        wb.delete(b"a").unwrap();
        let size6 = wb.data_size();
        assert!(size5 < size6);
        wb.delete_range_cf(CF_DEFAULT, b"a", b"b").unwrap();
        let size7 = wb.data_size();
        assert!(size6 < size7);
        wb.clear();
        let size8 = wb.data_size();
        assert_eq!(size8, size1);
    }

    #[test]
    fn save_point_rollback_none() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        let err = wb.rollback_to_save_point();
        assert_engine_error(err);
    }

    #[test]
    fn save_point_pop_none() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        let err = wb.rollback_to_save_point();
        assert_engine_error(err);
    }

    #[test]
    fn save_point_rollback_one() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.set_save_point();
        wb.put(b"a", b"").unwrap();

        wb.rollback_to_save_point().unwrap();

        let err = wb.rollback_to_save_point();
        assert_engine_error(err);
        let err = wb.pop_save_point();
        assert_engine_error(err);
        wb.write().unwrap();
        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn save_point_rollback_two() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.set_save_point();
        wb.put(b"a", b"").unwrap();
        wb.set_save_point();
        wb.put(b"b", b"").unwrap();

        wb.rollback_to_save_point().unwrap();
        wb.rollback_to_save_point().unwrap();

        let err = wb.rollback_to_save_point();
        assert_engine_error(err);
        let err = wb.pop_save_point();
        assert_engine_error(err);
        wb.write().unwrap();
        let a = db.engine.get_value(b"a").unwrap();
        assert!(a.is_none());
        let b = db.engine.get_value(b"b").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn save_point_rollback_partial() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"").unwrap();
        wb.set_save_point();
        wb.put(b"b", b"").unwrap();

        wb.rollback_to_save_point().unwrap();
        wb.write().unwrap();
        let a = db.engine.get_value(b"a").unwrap();
        assert!(a.is_some());
        let b = db.engine.get_value(b"b").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn save_point_pop_rollback() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.set_save_point();
        wb.put(b"a", b"").unwrap();
        wb.set_save_point();
        wb.put(b"a", b"").unwrap();

        wb.pop_save_point().unwrap();
        wb.rollback_to_save_point().unwrap();

        let err = wb.rollback_to_save_point();
        assert_engine_error(err);
        let err = wb.pop_save_point();
        assert_engine_error(err);
        wb.write().unwrap();
        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_none());
        let val = db.engine.get_value(b"b").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn save_point_rollback_after_write() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.set_save_point();
        wb.put(b"a", b"").unwrap();

        wb.write().unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_some());

        db.engine.delete(b"a").unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_none());

        wb.rollback_to_save_point().unwrap();
        wb.write().unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn save_point_same_rollback_one() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"").unwrap();

        wb.set_save_point();
        wb.set_save_point();
        wb.set_save_point();

        wb.put(b"b", b"").unwrap();

        wb.rollback_to_save_point().unwrap();

        wb.write().unwrap();

        let a = db.engine.get_value(b"a").unwrap();
        let b = db.engine.get_value(b"b").unwrap();

        assert!(a.is_some());
        assert!(b.is_none());
    }

    #[test]
    fn save_point_same_rollback_all() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.put(b"a", b"").unwrap();

        wb.set_save_point();
        wb.set_save_point();
        wb.set_save_point();

        wb.put(b"b", b"").unwrap();

        wb.rollback_to_save_point().unwrap();
        wb.rollback_to_save_point().unwrap();
        wb.rollback_to_save_point().unwrap();

        assert_engine_error(wb.pop_save_point());
        assert_engine_error(wb.rollback_to_save_point());

        wb.write().unwrap();

        let a = db.engine.get_value(b"a").unwrap();
        let b = db.engine.get_value(b"b").unwrap();

        assert!(a.is_some());
        assert!(b.is_none());
    }

    #[test]
    fn save_point_pop_after_write() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        wb.set_save_point();
        wb.put(b"a", b"").unwrap();

        wb.write().unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_some());

        db.engine.delete(b"a").unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_none());

        wb.pop_save_point().unwrap();
        wb.write().unwrap();

        let val = db.engine.get_value(b"a").unwrap();
        assert!(val.is_some());
    }

    #[test]
    fn save_point_all_commands() {
        let db = default_engine();
        let mut wb = db.engine.write_batch();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"d", b"").unwrap();

        wb.set_save_point();
        wb.delete(b"a").unwrap();
        wb.put(b"b", b"").unwrap();
        wb.delete_range_cf(CF_DEFAULT, b"c", b"e").unwrap();

        wb.rollback_to_save_point().unwrap();
        wb.write().unwrap();

        let a = db.engine.get_value(b"a").unwrap();
        let b = db.engine.get_value(b"b").unwrap();
        let d = db.engine.get_value(b"d").unwrap();
        assert!(a.is_some());
        assert!(b.is_none());
        assert!(d.is_some());
    }
}

mod misc {
    use super::{default_engine};
    use engine_traits::{KvEngine, SyncMutable, Peekable, MiscExt};

    #[test]
    fn sync_basic() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        db.engine.sync().unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn path() {
        let db = default_engine();
        let path = db.tempdir.path().to_str().unwrap();
        assert_eq!(db.engine.path(), path);
    }
}

