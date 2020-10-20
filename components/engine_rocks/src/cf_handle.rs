// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksColumnFamilyOptions;
use crate::engine::RocksEngine;
use engine_traits::CFHandle;
use engine_traits::CFHandleExt;
use engine_traits::{Result};
use rocksdb::CFHandle as RawCFHandle;
use crate::util;

impl CFHandleExt for RocksEngine {
    type CFHandle = RocksCFHandle;
    type ColumnFamilyOptions = RocksColumnFamilyOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::ColumnFamilyOptions> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(RocksColumnFamilyOptions::from_raw(self.as_inner().get_options_cf(handle)))
    }

    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        self.as_inner()
            .set_options_cf(handle, options)
            .map_err(|e| box_err!(e))
    }
}

// FIXME: This nasty representation with pointer casting is due to the lack of
// generic associated types in Rust. See comment on the KvEngine::CFHandle
// associated type. This could also be fixed if the CFHandle impl was defined
// inside the rust-rocksdb crate where the RawCFHandles are managed, but that
// would be an ugly abstraction violation.
#[repr(transparent)]
pub struct RocksCFHandle(RawCFHandle);

impl RocksCFHandle {
    pub fn from_raw(raw: &RawCFHandle) -> &RocksCFHandle {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn as_inner(&self) -> &RawCFHandle {
        &self.0
    }
}

impl CFHandle for RocksCFHandle {}
