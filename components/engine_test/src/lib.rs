// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! An engine for use in the test suite
//!
//! This storage engine links to all other engines, providing a single storage
//! engine to run tests against.
//!
//! This provides a simple way to integrate non-RocksDB engines into the
//! existing test suite without too much disruption.
//!
//! The engine is chosen at compile time with feature flags (TODO document me).
//!
//! We'll probably revisit the engine-testing strategy in the future,
//! e.g. by using engine-parameterized tests instead.
//!
//! TiKV uses two different storage engine instances,
//! the "raft" engine, for storing consensus data,
//! and the "kv" engine, for storing user data.
//!
//! The types and constructors for these two engines are located in the `raft`
//! and `kv` modules respectively.
//!
//! This create also contains a `ctor` module that contains constructor methods
//! appropriate for constructing storage engines of any type. It is intended
//! that this module is _the only_ module within TiKV that knows about concrete
//! storage engines, and that it be extracted into its own crate for use in
//! TiKV.

/// Types and constructors for the "raft" engine
pub mod raft {
    use engine_traits::Result;
    use crate::ctor::{EngineConstructorExt, DBOptions, CFOptions};

    #[cfg(feature = "test-engine-raft-panic")]
    pub use engine_panic::{
        PanicEngine as RaftTestEngine,
        PanicSnapshot as RaftTestSnapshot,
        PanicWriteBatch as RaftTestWriteBatch,
    };

    #[cfg(feature = "test-engine-raft-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as RaftTestEngine,
        RocksSnapshot as RaftTestSnapshot,
        RocksWriteBatch as RaftTestWriteBatch,
    };

    pub fn new_engine(path: &str, db_opt: Option<DBOptions>, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<RaftTestEngine> {
        RaftTestEngine::new_engine(path, db_opt, cfs, opts)
    }

    pub fn new_engine_opt(path: &str, db_opt: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<RaftTestEngine> {
        RaftTestEngine::new_engine_opt(path, db_opt, cfs_opts)
    }
}

/// Types and constructors for the "kv" engine
pub mod kv {
    use engine_traits::Result;
    use crate::ctor::{EngineConstructorExt, DBOptions, CFOptions};

    #[cfg(feature = "test-engine-kv-panic")]
    pub use engine_panic::{
        PanicEngine as KvTestEngine,
        PanicSnapshot as KvTestSnapshot,
        PanicWriteBatch as KvTestWriteBatch,
    };

    #[cfg(feature = "test-engine-kv-rocksdb")]
    pub use engine_rocks::{
        RocksEngine as KvTestEngine,
        RocksSnapshot as KvTestSnapshot,
        RocksWriteBatch as KvTestWriteBatch,
    };

    pub fn new_engine(path: &str, db_opt: Option<DBOptions>, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<KvTestEngine> {
        KvTestEngine::new_engine(path, db_opt, cfs, opts)
    }

    pub fn new_engine_opt(path: &str, db_opt: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<KvTestEngine> {
        KvTestEngine::new_engine_opt(path, db_opt, cfs_opts)
    }
}

/// Create a storage engine with a concrete type. This should ultimately be the
/// only module within TiKv that needs to know about concrete engines. Other
/// code only uses the `engine_traits` abstractions.
///
/// At the moment this has a lot of open-coding of engine-specific
/// initialization, but in the future more constructor abstractions should be
/// pushed down into engine_traits.
///
/// This module itself is intended to be extracted from this crate into its own
/// crate, once the requirements for engine construction are better understood.
pub mod ctor {
    use engine_traits::Result;

    /// Engine construction
    ///
    /// For simplicity, all engine constructors are expected to configure every
    /// engine such that all of TiKV and its tests work correctly, for the
    /// constructed column families.
    ///
    /// Specifically, this means that RocksDB constructors should set up
    /// all properties collectors, always.
    pub trait EngineConstructorExt: Sized {
        /// Create a new engine with either:
        ///
        /// - The column families specified as `cfs`, with default options, or
        /// - The column families specified as `opts`, with options.
        ///
        /// Note that if `opts` is not `None` then the `cfs` argument is completely ignored.
        fn new_engine(path: &str, db_opt: Option<DBOptions>, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<Self>;

        /// Create a new engine with specified column families and options
        fn new_engine_opt(path: &str, db_opt: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<Self>;
    }

    pub struct DBOptions;

    impl DBOptions {
        pub fn new() -> DBOptions {
            DBOptions
        }
    }

    pub struct CFOptions<'a> {
        pub cf: &'a str,
        pub options: ColumnFamilyOptions,
    }

    impl<'a> CFOptions<'a> {
        pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
            CFOptions { cf, options }
        }
    }

    /// Properties for a single column family
    ///
    /// All engines must emulate column families, but at present it is not clear
    /// how non-RocksDB engines should deal with the wide variety of options for
    /// column families.
    ///
    /// At present this very closely mirrors the column family options
    /// for RocksDB, with the exception that it provides no capacity for
    /// installing table property collectors, which have little hope of being
    /// emulated on arbitrary engines.
    ///
    /// Instead, the RocksDB constructors need to always install the table
    /// property collectors that TiKV needs, and other engines need to
    /// accomplish the same high-level ends those table properties are used for
    /// by their own means.
    ///
    /// At present, they should probably emulate, reinterpret, or ignore them as
    /// suitable to get tikv functioning.
    ///
    /// In the future TiKV will probably have engine-specific configuration
    /// options.
    #[derive(Clone)]
    pub struct ColumnFamilyOptions {
        disable_auto_compactions: bool,
        level_zero_file_num_compaction_trigger: Option<i32>,
        /// On RocksDB, turns off the range properties collector. Only used in
        /// tests. Unclear how other engines should deal with this.
        no_range_properties: bool,
    }

    impl ColumnFamilyOptions {
        pub fn new() -> ColumnFamilyOptions {
            ColumnFamilyOptions {
                disable_auto_compactions: false,
                level_zero_file_num_compaction_trigger: None,
                no_range_properties: false,
            }
        }

        pub fn set_disable_auto_compactions(&mut self, v: bool) {
            self.disable_auto_compactions = v;
        }

        pub fn get_disable_auto_compactions(&self) -> bool {
            self.disable_auto_compactions
        }

        pub fn set_level_zero_file_num_compaction_trigger(&mut self, n: i32) {
            self.level_zero_file_num_compaction_trigger = Some(n);
        }

        pub fn get_level_zero_file_num_compaction_trigger(&self) -> Option<i32> {
            self.level_zero_file_num_compaction_trigger
        }

        pub fn set_no_range_properties(&mut self, v: bool) {
            self.no_range_properties = v;
        }

        pub fn get_no_range_properties(&self) -> bool {
            self.no_range_properties
        }
        
    }

    mod panic {
        use engine_traits::Result;
        use engine_panic::PanicEngine;
        use super::{EngineConstructorExt, DBOptions, CFOptions};

        impl EngineConstructorExt for engine_panic::PanicEngine {
            fn new_engine(_path: &str, _db_opt: Option<DBOptions>, _cfs: &[&str], _opts: Option<Vec<CFOptions>>) -> Result<Self> {
                Ok(PanicEngine)
            }

            fn new_engine_opt(_path: &str, _db_opt: DBOptions, _cfs_opts: Vec<CFOptions>) -> Result<Self> {
                Ok(PanicEngine)
            }
        }
    }

    mod rocks {
        use super::{EngineConstructorExt, ColumnFamilyOptions, DBOptions, CFOptions};

        use engine_traits::{Result, DBOptions as DBOptionsTrait, ColumnFamilyOptions as ColumnFamilyOptionsTrait};

        use engine_rocks::{RocksColumnFamilyOptions, RocksDBOptions};
        use engine_rocks::util::{new_engine_opt as rocks_new_engine_opt, new_engine as rocks_new_engine, RocksCFOptions};
        use engine_rocks::raw::{ColumnFamilyOptions as RawRocksColumnFamilyOptions};
        use engine_rocks::properties::{
            MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
        };

        impl EngineConstructorExt for engine_rocks::RocksEngine {
            // FIXME this is duplicating behavior from engine_rocks::raw_util in order to
            // call set_standard_cf_opts.
            fn new_engine(path: &str, db_opt: Option<DBOptions>, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<Self> {
                let rocks_db_opts = match db_opt {
                    Some(_opt) => panic!(), // todo
                    None => None,
                };
                let cfs_opts = match opts {
                    Some(opts) => opts,
                    None => {
                        let mut default_cfs_opts = Vec::with_capacity(cfs.len());
                        for cf in cfs {
                            default_cfs_opts.push(CFOptions::new(*cf, ColumnFamilyOptions::new()));
                        }
                        default_cfs_opts
                    }
                };
                let rocks_cfs_opts = cfs_opts
                    .iter()
                    .map(|cf_opts| {
                        let mut rocks_cf_opts = RocksColumnFamilyOptions::new();
                        set_standard_cf_opts(rocks_cf_opts.as_raw_mut(), &cf_opts.options);
                        set_cf_opts(&mut rocks_cf_opts, &cf_opts.options);
                        RocksCFOptions::new(cf_opts.cf, rocks_cf_opts)
                    })
                    .collect();
                rocks_new_engine(path, rocks_db_opts, &[], Some(rocks_cfs_opts))
            }

            fn new_engine_opt(path: &str, _db_opt: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<Self> {
                let rocks_db_opts = RocksDBOptions::new();
                let rocks_cfs_opts = cfs_opts
                    .iter()
                    .map(|cf_opts| {
                        let mut rocks_cf_opts = RocksColumnFamilyOptions::new();
                        set_standard_cf_opts(rocks_cf_opts.as_raw_mut(), &cf_opts.options);
                        set_cf_opts(&mut rocks_cf_opts, &cf_opts.options);
                        RocksCFOptions::new(cf_opts.cf, rocks_cf_opts)
                    })
                    .collect();
                rocks_new_engine_opt(path, rocks_db_opts, rocks_cfs_opts)
            }
        }

        fn set_standard_cf_opts(rocks_cf_opts: &mut RawRocksColumnFamilyOptions, cf_opts: &ColumnFamilyOptions) {
            if !cf_opts.get_no_range_properties() {
                let f = Box::new(RangePropertiesCollectorFactory::default());
                rocks_cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
            }
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            rocks_cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        }

        fn set_cf_opts(rocks_cf_opts: &mut RocksColumnFamilyOptions, cf_opts: &ColumnFamilyOptions) {
            if let Some(trigger) = cf_opts.get_level_zero_file_num_compaction_trigger() {
                rocks_cf_opts.set_level_zero_file_num_compaction_trigger(trigger);
            }
            if cf_opts.get_disable_auto_compactions() {
                rocks_cf_opts.set_disable_auto_compactions(true);
            }
        }
    }
}


/// Create a new set of engines in a temporary directory
///
/// This is little-used and probably shouldn't exist.
pub fn new_temp_engine(path: &tempfile::TempDir) -> engine_traits::Engines<crate::kv::KvTestEngine, crate::raft::RaftTestEngine> {
    let raft_path = path.path().join(std::path::Path::new("raft"));
    engine_traits::Engines::new(
        crate::kv::new_engine(
            path.path().to_str().unwrap(),
            None,
            engine_traits::ALL_CFS,
            None,
        )
        .unwrap(),
        crate::raft::new_engine(
            raft_path.to_str().unwrap(),
            None,
            &[engine_traits::CF_DEFAULT],
            None,
        )
        .unwrap(),
    )
}

