// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use ahash;
use std::cmp::Eq;
use std::hash::Hash;

pub type HashMap<K, V> =
    std::collections::HashMap<K, V, ahash::ABuildHasher>;
pub type HashSet<T> = std::collections::HashSet<T, ahash::ABuildHasher>;
pub use std::collections::hash_map::Entry as HashMapEntry;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, ahash::ABuildHasher)
}
