// Copyright 2018 PingCAP, Inc.
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

use kvproto::coprocessor as coppb;
use tipb::schema::ColumnInfo;

use super::codec::datum::Datum;
use super::codec::mysql;

/// Get the smallest key which is larger than the key given.
pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &coppb::KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_prefix_next() {
        assert_eq!(prefix_next(&[]), vec![0]);
        assert_eq!(prefix_next(&[0]), vec![1]);
        assert_eq!(prefix_next(&[1]), vec![2]);
        assert_eq!(prefix_next(&[255]), vec![255, 0]);
        assert_eq!(prefix_next(&[255, 255, 255]), vec![255, 255, 255, 0]);
        assert_eq!(prefix_next(&[1, 255]), vec![2, 0]);
        assert_eq!(prefix_next(&[0, 1, 255]), vec![0, 2, 0]);
        assert_eq!(prefix_next(&[0, 1, 255, 5]), vec![0, 1, 255, 6]);
        assert_eq!(prefix_next(&[0, 1, 5, 255]), vec![0, 1, 6, 0]);
        assert_eq!(prefix_next(&[0, 1, 255, 255]), vec![0, 2, 0, 0]);
        assert_eq!(prefix_next(&[0, 255, 255, 255]), vec![1, 0, 0, 0]);
    }
}
