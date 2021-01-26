// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{ChunkRef, ChunkedVec, UnsafeRefInto};
use super::{Enum, EnumRef};
use crate::impl_chunked_vec_common;
use std::sync::Arc;
use tikv_util::buffer_vec::BufferVec;

/// `ChunkedVecEnum` stores enum in a compact way.
///
/// Inside `ChunkedVecEnum`:
/// - `data` stores the real enum data.
/// - `bitmap` indicates if an element at given index is null.
/// - `value` is an 1-based index enum data offset, 0 means this enum is ''
///
/// # Notes
///
/// Make sure operating `bitmap` and `value` together to prevent different
/// stored representation issue discussed at
/// https://github.com/tikv/tikv/pull/8948#discussion_r516463693
///
/// TODO: add way to set enum column data
#[derive(Debug, Clone)]
pub struct ChunkedVecEnum {
    data: Arc<BufferVec>,
    bitmap: BitVec,
    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    value: Vec<usize>,
}

impl ChunkedVecEnum {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<EnumRef> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            Some(EnumRef::new(&self.data, self.value[idx]))
        } else {
            None
        }
    }
}

impl ChunkedVec<Enum> for ChunkedVecEnum {
    impl_chunked_vec_common! { Enum }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Arc::new(BufferVec::new()),
            bitmap: BitVec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn push_data(&mut self, value: Enum) {
        self.bitmap.push(true);
        self.value.push(value.value());
    }

    #[inline]
    fn push_null(&mut self) {
        self.bitmap.push(false);
        self.value.push(0);
    }

    fn len(&self) -> usize {
        self.value.len()
    }

    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.bitmap.truncate(len);
            self.value.truncate(len);
        }
    }

    fn capacity(&self) -> usize {
        self.bitmap.capacity().max(self.value.capacity())
    }

    fn append(&mut self, other: &mut Self) {
        self.value.append(&mut other.value);
        self.bitmap.append(&mut other.bitmap);
    }

    fn to_vec(&self) -> Vec<Option<Enum>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(if self.bitmap.get(i) {
                Some(Enum::new(self.data.clone(), self.value[i]))
            } else {
                None
            });
        }
        x
    }
}

impl PartialEq for ChunkedVecEnum {
    fn eq(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        for idx in 0..self.data.len() {
            if self.data[idx] != other.data[idx] {
                return false;
            }
        }

        if !self.bitmap.eq(&other.bitmap) {
            return false;
        }

        if !self.value.eq(&other.value) {
            return false;
        }

        true
    }
}

impl<'a> ChunkRef<'a, EnumRef<'a>> for &'a ChunkedVecEnum {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<EnumRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<EnumRef<'a>> {
        None
    }
}

impl From<Vec<Option<Enum>>> for ChunkedVecEnum {
    fn from(v: Vec<Option<Enum>>) -> ChunkedVecEnum {
        ChunkedVecEnum::from_vec(v)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecEnum> for &'a ChunkedVecEnum {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecEnum {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> ChunkedVecEnum {
        let mut x: ChunkedVecEnum = ChunkedVecEnum::with_capacity(0);

        // FIXME: we need a set_data here, but for now, we set directly
        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我强爆啊");
        buf.push("我成功了");
        x.data = Arc::new(buf);

        x
    }

    #[test]
    fn test_basics() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new(x.data.clone(), 2)));
        x.push(None);
        x.push(Some(Enum::new(x.data.clone(), 1)));
        x.push(Some(Enum::new(x.data.clone(), 3)));

        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new(&x.data, 2)));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some(EnumRef::new(&x.data, 1)));
        assert_eq!(x.get(4), Some(EnumRef::new(&x.data, 3)));
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new(x.data.clone(), 2)));
        x.push(None);
        x.push(Some(Enum::new(x.data.clone(), 1)));
        x.push(Some(Enum::new(x.data.clone(), 3)));

        x.truncate(100);
        assert_eq!(x.len(), 5);

        x.truncate(3);
        assert_eq!(x.len(), 3);
        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new(&x.data, 2)));
        assert_eq!(x.get(2), None);

        x.truncate(1);
        assert_eq!(x.len(), 1);
        assert_eq!(x.get(0), None);

        x.truncate(0);
        assert_eq!(x.len(), 0);
    }

    #[test]
    fn test_append() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new(x.data.clone(), 2)));

        let mut y = setup();
        y.push(None);
        y.push(Some(Enum::new(x.data.clone(), 1)));
        y.push(Some(Enum::new(x.data.clone(), 3)));

        x.append(&mut y);
        assert_eq!(x.len(), 5);
        assert!(y.is_empty());

        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new(&x.data, 2)));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some(EnumRef::new(&x.data, 1)));
        assert_eq!(x.get(4), Some(EnumRef::new(&x.data, 3)));
    }
}
