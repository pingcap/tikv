use engine_traits::KvEngine;
use std::sync::Arc;
use txn_types::{Key, Value};
use kvproto::metapb;

pub trait RegionCache: Send + Sync {
    fn get(&self, key: &Key) -> Option<Value>;
    fn valid(&self) -> bool;
    fn set_valid(&self, v: bool);
    fn region_id(&self) -> u64;
}

pub trait RegionCacheBuilder<E: KvEngine>: Send + Sync {
    fn build(&self, snap: E::Snapshot) -> Arc<dyn RegionCache>;
    fn region_id(&self) -> u64;
}

pub trait RegionCacheBuilderFactory<E: KvEngine>: Send + Sync {
    fn create_builder(&self, region: metapb::Region) -> Box<dyn RegionCacheBuilder<E>>;
}
