// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties,
    UserCollectedProperties,
};
use std::ops::Deref;

pub struct PanicTableProperties;

impl TableProperties<PanicUserCollectedProperties> for PanicTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> PanicUserCollectedProperties {
        panic!()
    }
}

pub struct PanicUserCollectedProperties;

impl UserCollectedProperties for PanicUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for PanicUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
