// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core data types.

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::u64;

use util::codec::bytes;
use util::codec::number::{self, NumberEncoder};
use util::{codec, escape};

use storage::mvcc::{Lock, Write};

/// Value type which is essentially raw bytes.
pub type Value = Vec<u8>;

/// Key-value pair type.
///
/// The value is simply raw bytes; the key is a little bit tricky, which is
/// encoded bytes.
pub type KvPair = (Vec<u8>, Value);

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(u64, Write)>,
    /// start_ts and value
    pub values: Vec<(u64, Value)>,
}

/// Key type.
///
/// Keys have 2 types of binary representation - raw and encoded. The raw
/// representation is for public interface, the encoded representation is for
/// internal storage. We can get both representations from an instance of this
/// type.
///
/// Orthogonal to binary representation, keys may or may not embed a timestamp,
/// but this information is transparent to this type, the caller must use it
/// consistently.
#[derive(Debug)]
pub struct Key(Vec<u8>);

/// Core functions for `Key`.
impl Key {
    /// Creates a key from raw bytes.
    #[inline]
    pub fn from_raw(key: &[u8]) -> Key {
        Key(codec::bytes::encode_bytes(key))
    }

    /// Gets the raw representation of this key.
    #[inline]
    pub fn raw(&self) -> Result<Vec<u8>, codec::Error> {
        bytes::decode_bytes(&mut self.0.as_slice(), false)
    }

    /// Creates a key from encoded bytes.
    #[inline]
    pub fn from_encoded(encoded_key: Vec<u8>) -> Key {
        Key(encoded_key)
    }

    /// Gets the encoded representation of this key.
    #[inline]
    pub fn encoded(&self) -> &Vec<u8> {
        &self.0
    }

    /// Convert to encoded representation of this key.
    #[inline]
    pub fn take_encoded(self) -> Vec<u8> {
        self.0
    }

    /// Creates a new key by appending a `u64` timestamp to this key.
    #[inline]
    pub fn append_ts(self, ts: u64) -> Key {
        let mut encoded = self.0;
        encoded.encode_u64_desc(ts).unwrap();
        Key(encoded)
    }

    /// Gets the timestamp contained in this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped
    /// key.
    #[inline]
    pub fn decode_ts(&self) -> Result<u64, codec::Error> {
        Ok(Self::decode_ts_from(&self.0)?)
    }

    /// Creates a new key by truncating the timestamp from this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped key.
    #[inline]
    pub fn truncate_ts(mut self) -> Result<Key, codec::Error> {
        let len = self.0.len();
        if len < number::U64_SIZE {
            // TODO: IMHO, this should be an assertion failure instead of
            // returning an error. If this happens, it indicates a bug in
            // the caller module, have to make code change to fix it.
            //
            // Even if it passed the length check, it still could be buggy,
            // a better way is to introduce a type `TimestampedKey`, and
            // functions to convert between `TimestampedKey` and `Key`.
            // `TimestampedKey` is in a higher (MVCC) layer, while `Key` is
            // in the core storage engine layer.
            Err(codec::Error::KeyLength)
        } else {
            self.0.truncate(len - number::U64_SIZE);
            Ok(self)
        }
    }

    /// Split a ts encoded key, return the user key and timestamp.
    #[inline]
    pub fn split_on_ts_for(key: &[u8]) -> Result<(&[u8], u64), codec::Error> {
        if key.len() < number::U64_SIZE {
            Err(codec::Error::KeyLength)
        } else {
            let pos = key.len() - number::U64_SIZE;
            let k = &key[..pos];
            let mut ts = &key[pos..];
            Ok((k, number::decode_u64_desc(&mut ts)?))
        }
    }

    /// Extract the user key from a ts encoded key.
    #[inline]
    pub fn truncate_ts_for(key: &[u8]) -> Result<&[u8], codec::Error> {
        let len = key.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        Ok(&key[..key.len() - number::U64_SIZE])
    }

    /// Decode the timestamp from a ts encoded key.
    #[inline]
    pub fn decode_ts_from(key: &[u8]) -> Result<u64, codec::Error> {
        let len = key.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        let mut ts = &key[len - number::U64_SIZE..];
        number::decode_u64_desc(&mut ts)
    }

    /// Whether the user key part of a ts encoded key `ts_encoded_key` equals to the encoded
    /// user key `user_key`.
    #[inline]
    #[cfg_attr(feature = "cargo-clippy", allow(cast_ptr_alignment))]
    pub fn is_user_key_eq(ts_encoded_key: &[u8], user_key: &[u8]) -> bool {
        let user_key_len = user_key.len();
        if ts_encoded_key.len() != user_key_len + number::U64_SIZE {
            return false;
        }
        if user_key_len >= number::U64_SIZE {
            // We compare last 8 bytes as u64 first, then compare the rest.
            unsafe {
                let ks_ptr = ts_encoded_key[user_key_len - 8..].as_ptr() as *const u64;
                let rs_ptr = user_key[user_key_len - 8..].as_ptr() as *const u64;
                if *ks_ptr != *rs_ptr {
                    return false;
                }
            }
            ts_encoded_key[..user_key_len - 8] == user_key[..user_key_len - 8]
        } else {
            ts_encoded_key[..user_key_len] == user_key[..]
        }
    }
}

impl Clone for Key {
    fn clone(&self) -> Self {
        // default clone implemention use self.len() to reserve capacity
        // for the sake of appending ts, we need to reserve more
        let mut key = Vec::with_capacity(self.0.capacity());
        key.extend_from_slice(&self.0);
        Key(key)
    }
}

/// Hash for `Key`.
impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encoded().hash(state)
    }
}

/// Display for `Key`.
impl Display for Key {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", escape(&self.0))
    }
}

/// Partial equality for `Key`.
impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ts() {
        let k = b"k";
        let ts = 123;
        assert!(Key::split_on_ts_for(k).is_err());
        let enc = Key::from_encoded(k.to_vec()).append_ts(ts);
        let res = Key::split_on_ts_for(enc.encoded()).unwrap();
        assert_eq!(res, (k.as_ref(), ts));
    }

    #[test]
    fn test_is_user_key_eq() {
        // make a short name to keep format for the test.
        fn eq(a: &[u8], b: &[u8]) -> bool {
            Key::is_user_key_eq(a, b)
        }
        assert_eq!(false, eq(b"", b""));
        assert_eq!(false, eq(b"12345", b""));
        assert_eq!(true, eq(b"12345678", b""));
        assert_eq!(true, eq(b"x12345678", b"x"));
        assert_eq!(false, eq(b"x12345", b"x"));
        // user key len == 3
        assert_eq!(true, eq(b"xyz12345678", b"xyz"));
        assert_eq!(true, eq(b"xyz________", b"xyz"));
        assert_eq!(false, eq(b"xyy12345678", b"xyz"));
        assert_eq!(false, eq(b"yyz12345678", b"xyz"));
        assert_eq!(false, eq(b"xyz12345678", b"xy"));
        assert_eq!(false, eq(b"xyy12345678", b"xy"));
        assert_eq!(false, eq(b"yyz12345678", b"xy"));
        // user key len == 7
        assert_eq!(true, eq(b"abcdefg12345678", b"abcdefg"));
        assert_eq!(true, eq(b"abcdefgzzzzzzzz", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefg12345678", b"abcdef"));
        assert_eq!(false, eq(b"abcdefg12345678", b"bcdefg"));
        assert_eq!(false, eq(b"abcdefv12345678", b"abcdefg"));
        assert_eq!(false, eq(b"vbcdefg12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abccefg12345678", b"abcdefg"));
        // user key len == 8
        assert_eq!(true, eq(b"abcdefgh12345678", b"abcdefgh"));
        assert_eq!(true, eq(b"abcdefghyyyyyyyy", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"bcdefgh"));
        assert_eq!(false, eq(b"abcdefgz12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"zbcdefgh12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcddfgh12345678", b"abcdefgh"));
        // user key len == 9
        assert_eq!(true, eq(b"abcdefghi12345678", b"abcdefghi"));
        assert_eq!(true, eq(b"abcdefghixxxxxxxx", b"abcdefghi"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"bcdefghi"));
        assert_eq!(false, eq(b"abcdefghy12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"ybcdefghi12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"abcddfghi12345678", b"abcdefghi"));
        // user key len == 11
        assert_eq!(true, eq(b"abcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(true, eq(b"abcdefghijkabcdefgh", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"abcdefghij"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"bcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijx87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"xbcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abxdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"axcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdeffhijk87654321", b"abcdefghijk"));
    }
}
