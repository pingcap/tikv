// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::{HashMap, HashSet};
use kvproto::kvrpcpb::{KeyRange, LockInfo};
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_cmdpb::ReadIndexRequest;
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::mem;

pub trait HeapSize {
    fn heap_size(&self) -> usize;
    fn total_size(&self) -> usize {
        mem::size_of_val(self) + self.heap_size()
    }
}

impl HeapSize for Region {
    #[inline]
    fn heap_size(&self) -> usize {
        self.start_key.len() + self.end_key.len() + mem::size_of::<RegionEpoch>()
    }
}

impl HeapSize for ReadIndexRequest {
    fn heap_size(&self) -> usize {
        let mut size = self.key_ranges.len() * mem::size_of::<KeyRange>();
        for range in &self.key_ranges {
            size += range.get_start_key().len() + range.get_end_key().len();
        }
        size
    }
}

impl HeapSize for LockInfo {
    fn heap_size(&self) -> usize {
        let mut size = self.secondaries.len()
            + mem::size_of::<Vec<u8>>()
            + self.primary_lock.len()
            + self.key.len();
        for key in &self.secondaries {
            size += key.len();
        }
        size
    }
}

impl<K: Eq + Hash, V> HeapSize for HashMap<K, V> {
    #[inline]
    fn heap_size(&self) -> usize {
        // std::Hashmap uses 7/8 of allocated memory.
        self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>()) * 8 / 7
    }
}

impl<K: Eq + Hash> HeapSize for HashSet<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>() * 8 / 7
    }
}

impl<K, V> HeapSize for BTreeMap<K, V> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.len() * (mem::size_of::<K>() + mem::size_of::<V>())
    }
}

impl<K> HeapSize for Vec<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}

impl<K> HeapSize for VecDeque<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}
