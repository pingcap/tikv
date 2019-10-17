// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::result::Result as StdResult;
use engine_traits::DBOptions;
use engine_traits::DBOptionsExt;
use crate::db::Rocks;
use rocksdb::DBOptions as RawDBOptions;

impl DBOptionsExt for Rocks {
    type DBOptions = RocksDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        RocksDBOptions::from_raw(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> StdResult<(), String> {
        self.as_inner().set_db_options(options)
    }
}

pub struct RocksDBOptions(RawDBOptions);

impl RocksDBOptions {
    pub fn from_raw(raw: RawDBOptions) -> RocksDBOptions {
        RocksDBOptions(raw)
    }
}

impl DBOptions for RocksDBOptions {
    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }
}
