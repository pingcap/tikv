// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use crate::engine::PanicEngine;
use engine_traits::{
    Range, Result, TablePropertiesCollection, TablePropertiesExt,
    TablePropertiesCollectionIter,
    TablePropertiesKey,
    TableProperties,
    UserCollectedProperties,
};

impl TablePropertiesExt for PanicEngine {
    type TablePropertiesCollection = PanicTablePropertiesCollection;
    type TablePropertiesCollectionIter = PanicTablePropertiesCollectionIter;
    type TablePropertiesKey = PanicTablePropertiesKey;
    type TableProperties = PanicTableProperties;
    type UserCollectedProperties = PanicUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

type IA = PanicTablePropertiesCollectionIter;
type PKeyA = PanicTablePropertiesKey;
type PA = PanicTableProperties;
type UCPA = PanicUserCollectedProperties;

pub struct PanicTablePropertiesCollection;

impl TablePropertiesCollection<IA, PKeyA, PA, UCPA> for PanicTablePropertiesCollection {
    fn iter(&self) -> PanicTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct PanicTablePropertiesCollectionIter;

impl TablePropertiesCollectionIter<PKeyA, PA, UCPA> for PanicTablePropertiesCollectionIter { }

impl Iterator for PanicTablePropertiesCollectionIter {
    type Item = (PanicTablePropertiesKey, PanicTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct PanicTablePropertiesKey;

impl TablePropertiesKey for PanicTablePropertiesKey { }

impl Deref for PanicTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct PanicTableProperties;

impl TableProperties<UCPA> for PanicTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> PanicUserCollectedProperties {
        panic!()
    }
}

pub struct PanicUserCollectedProperties;

impl UserCollectedProperties for PanicUserCollectedProperties {
    fn get<Q: AsRef<[u8]>>(&self, index: Q) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}
