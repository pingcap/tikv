// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collections::{HashMap, HashMapEntry};
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

struct Record<K> {
    prev: NonNull<Record<K>>,
    next: NonNull<Record<K>>,
    key: MaybeUninit<K>,
}

struct ValueEntry<K, V> {
    value: V,
    record: NonNull<Record<K>>,
}

struct Trace<K> {
    head: Box<Record<K>>,
    tail: Box<Record<K>>,
}

impl<K> Trace<K> {
    fn new() -> Trace<K> {
        unsafe {
            let mut head = Box::new(Record {
                prev: NonNull::new_unchecked(1usize as _),
                next: NonNull::new_unchecked(1usize as _),
                key: MaybeUninit::uninit(),
            });
            let mut tail = Box::new(Record {
                prev: NonNull::new_unchecked(1usize as _),
                next: NonNull::new_unchecked(1usize as _),
                key: MaybeUninit::uninit(),
            });
            head.next = NonNull::new_unchecked(&mut *tail);
            tail.prev = NonNull::new_unchecked(&mut *head);

            Trace { head, tail }
        }
    }

    fn promote(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            record.as_mut().prev.as_mut().next = record.as_mut().next;
            record.as_mut().next.as_mut().prev = record.as_mut().prev;
            self.head.next.as_mut().prev = record;
            record.as_mut().next = self.head.next;
            record.as_mut().prev = NonNull::new_unchecked(&mut *self.head);
            self.head.next = record;
        }
    }

    fn delete(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            record.as_mut().prev.as_mut().next = record.as_mut().next;
            record.as_mut().next.as_mut().prev = record.as_mut().prev;

            ptr::drop_in_place(Box::from_raw(record.as_ptr()).key.as_mut_ptr());
        }
    }

    fn create(&mut self, key: K) -> NonNull<Record<K>> {
        let record = Box::leak(Box::new(Record {
            prev: unsafe { NonNull::new_unchecked(&mut *self.head) },
            next: self.head.next,
            key: MaybeUninit::new(key),
        }))
        .into();
        unsafe {
            self.head.next.as_mut().prev = record;
            self.head.next = record;
        }
        record
    }

    fn reuse_tail(&mut self, key: K) -> (K, NonNull<Record<K>>) {
        unsafe {
            let mut record = self.tail.prev;
            record.as_mut().prev.as_mut().next = NonNull::new_unchecked(&mut *self.tail);
            self.tail.prev = record.as_mut().prev;
            self.head.next.as_mut().prev = record;
            record.as_mut().prev = NonNull::new_unchecked(&mut *self.head);
            record.as_mut().next = self.head.next;
            self.head.next = record;
            let old_key = record.as_mut().key.as_ptr().read();
            record.as_mut().key = MaybeUninit::new(key);
            (old_key, record)
        }
    }

    fn clear(&mut self) {
        let mut cur = self.head.next;
        unsafe {
            while cur.as_ptr() != &mut *self.tail {
                let tmp = cur.as_mut().next;
                ptr::drop_in_place(Box::from_raw(cur.as_ptr()).key.as_mut_ptr());
                cur = tmp;
            }
        }
    }

    fn remove_tail(&mut self) -> K {
        unsafe {
            let mut record = self.tail.prev;
            record.as_mut().prev.as_mut().next = NonNull::new_unchecked(&mut *self.tail);
            self.tail.prev = record.as_mut().prev;
            let r = Box::from_raw(record.as_ptr());
            r.key.as_ptr().read()
        }
    }
}

pub struct LruCache<K, V> {
    map: HashMap<K, ValueEntry<K, V>>,
    trace: Trace<K>,
    capacity: usize,
}

impl<K, V> LruCache<K, V> {
    pub fn with_capacity(mut capacity: usize) -> LruCache<K, V> {
        if capacity == 0 {
            capacity = 1;
        }
        LruCache {
            map: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            trace: Trace::new(),
            capacity,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
        self.trace.clear();
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
{
    #[inline]
    pub fn resize(&mut self, mut new_cap: usize) {
        if new_cap == 0 {
            new_cap = 1;
        }
        if new_cap < self.capacity && self.map.len() > new_cap {
            for _ in new_cap..self.map.len() {
                let key = self.trace.remove_tail();
                self.map.remove(&key);
            }
            self.map.shrink_to_fit();
        }
        self.capacity = new_cap;
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) {
        let mut old_key = None;
        let map_len = self.map.len();
        match self.map.entry(key) {
            HashMapEntry::Occupied(mut e) => {
                let mut entry = e.get_mut();
                self.trace.promote(entry.record);
                entry.value = value;
            }
            HashMapEntry::Vacant(v) => {
                let record = if self.capacity == map_len {
                    let res = self.trace.reuse_tail(v.key().clone());
                    old_key = Some(res.0);
                    res.1
                } else {
                    self.trace.create(v.key().clone())
                };

                v.insert(ValueEntry { value, record });
            }
        }
        if let Some(o) = old_key {
            self.map.remove(&o);
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &K) {
        if let Some(v) = self.map.remove(key) {
            self.trace.delete(v.record);
        }
    }

    #[inline]
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            Some(v) => {
                self.trace.promote(v.record);
                Some(&v.value)
            }
            None => None,
        }
    }
}

unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..5 {
            map.insert(i, i);
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), None);
        }
    }

    #[test]
    fn test_query() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), None);
        }
    }

    #[test]
    fn test_empty() {
        let mut map = LruCache::with_capacity(0);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);

        map.resize(1);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));
        assert_eq!(map.get(&3), None);
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);

        map.remove(&3);
        assert_eq!(map.get(&3), None);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));

        map.resize(0);
        assert_eq!(map.get(&2), Some(&4));
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_remove() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in (0..3).chain(8..10) {
            map.remove(&i);
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in (0..3).chain(8..10) {
            assert_eq!(map.get(&i), None);
        }
        for i in (3..8).chain(10..15) {
            assert_eq!(map.get(&i), Some(&i));
        }
    }

    #[test]
    fn test_resize() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        map.resize(5);
        for i in 0..5 {
            assert_eq!(map.get(&i), None);
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 5);

        map.resize(10);
        assert_eq!(map.capacity(), 10);

        map.resize(5);
        for i in 5..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 5);

        map.resize(10);
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 5..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 10);
    }
}
