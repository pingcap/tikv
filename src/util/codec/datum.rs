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


use std::cmp::Ordering;
use std::i64;

use util::codec;
use super::{number, Result, Error, bytes, convert};

const NIL_FLAG: u8 = 0;
const BYTES_FLAG: u8 = 1;
const COMPACT_BYTES_FLAG: u8 = 2;
const INT_FLAG: u8 = 3;
const UINT_FLAG: u8 = 4;
// TODO: support following flag
// const FLOAT_FLAG: u8 = 5;
// const DECIMAL_FLAG: u8 = 6;
// const DURATION_FLAG: u8 = 7;
const MAX_FLAG: u8 = 250;

#[derive(PartialEq, Debug, Clone)]
pub enum Datum {
    Null,
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Bytes(Vec<u8>),
    Min,
    Max,
}

fn cmp_f64(l: f64, r: f64) -> Result<Ordering> {
    l.partial_cmp(&r)
     .ok_or_else(|| Error::InvalidDataType(format!("{} and {} can't be compared", l, r)))
}

#[allow(should_implement_trait)]
impl Datum {
    pub fn cmp(&self, datum: &Datum) -> Result<Ordering> {
        match *datum {
            Datum::Null => {
                match *self {
                    Datum::Null => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Greater),
                }
            }
            Datum::Min => {
                match *self {
                    Datum::Null => Ok(Ordering::Less),
                    Datum::Min => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Greater),
                }
            }
            Datum::Max => {
                match *self {
                    Datum::Max => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Less),
                }
            }
            Datum::I64(i) => self.cmp_i64(i),
            Datum::U64(u) => self.cmp_u64(u),
            Datum::F32(f) => self.cmp_f64(f as f64),
            Datum::F64(f) => self.cmp_f64(f),
            Datum::Bytes(ref bs) => self.cmp_bytes(bs),
        }
    }

    fn cmp_i64(&self, i: i64) -> Result<Ordering> {
        match *self {
            Datum::I64(ii) => Ok(ii.cmp(&i)),
            Datum::U64(u) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Ordering::Greater)
                } else {
                    Ok(u.cmp(&(i as u64)))
                }
            }
            _ => self.cmp_f64(i as f64),
        }
    }

    fn cmp_u64(&self, u: u64) -> Result<Ordering> {
        match *self {
            Datum::I64(i) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Ordering::Less)
                } else {
                    Ok(i.cmp(&(u as i64)))
                }
            }
            Datum::U64(uu) => Ok(uu.cmp(&u)),
            _ => self.cmp_f64(u as f64),
        }
    }

    fn cmp_f64(&self, f: f64) -> Result<Ordering> {
        match *self {
            Datum::Null | Datum::Min => Ok(Ordering::Less),
            Datum::Max => Ok(Ordering::Greater),
            Datum::I64(i) => cmp_f64(i as f64, f),
            Datum::U64(u) => cmp_f64(u as f64, f),
            Datum::F32(ff) => cmp_f64(ff as f64, f),
            Datum::F64(ff) => cmp_f64(ff, f),
            Datum::Bytes(ref bs) => {
                let ff = try!(convert::bytes_to_f64(bs));
                cmp_f64(ff, f)
            }
        }
    }

    fn cmp_bytes(&self, bs: &[u8]) -> Result<Ordering> {
        match *self {
            Datum::Null | Datum::Min => Ok(Ordering::Less),
            Datum::Max => Ok(Ordering::Greater),
            Datum::Bytes(ref bss) => Ok((bss as &[u8]).cmp(bs)),
            _ => {
                let f = try!(convert::bytes_to_f64(bs));
                self.cmp_f64(f)
            }
        }
    }

    // as_bool converts self to a bool.
    pub fn as_bool(&self) -> Result<bool> {
        let b = match *self {
            Datum::I64(i) => i != 0,
            Datum::U64(u) => u != 0,
            Datum::F32(f) => f.round() != 0f32,
            Datum::F64(f) => f.round() != 0f64,
            Datum::Bytes(ref bs) => !bs.is_empty() && try!(convert::bytes_to_int(bs)) != 0,
            _ => return Err(Error::InvalidDataType(format!("can't convert {:?} to bool", self))),
        };
        Ok(b)
    }

    /// into_string convert self into a string.
    pub fn into_string(self) -> Result<String> {
        let s = match self {
            Datum::I64(i) => format!("{}", i),
            Datum::U64(u) => format!("{}", u),
            Datum::F64(f) => format!("{}", f),
            Datum::F32(f) => format!("{}", f),
            Datum::Bytes(bs) => try!(String::from_utf8(bs)),
            d => return Err(Error::InvalidDataType(format!("can't convert {:?} to string", d))),
        };
        Ok(s)
    }
}

impl From<bool> for Datum {
    fn from(b: bool) -> Datum {
        if b {
            Datum::I64(1)
        } else {
            Datum::I64(0)
        }
    }
}

impl<T: Into<Datum>> From<Option<T>> for Datum {
    fn from(opt: Option<T>) -> Datum {
        match opt {
            None => Datum::Null,
            Some(t) => t.into(),
        }
    }
}

impl<'a> From<&'a [u8]> for Datum {
    fn from(data: &'a [u8]) -> Datum {
        Datum::Bytes(data.to_vec())
    }
}

/// `decode_datum` decodes on a datum from a byte slice generated by tidb.
pub fn decode_datum(buf: &[u8]) -> Result<(Datum, usize)> {
    // TODO use read instead.
    try!(codec::check_bound(buf, 1));

    let datum;
    let mut readed = 1;
    match buf[0] {
        INT_FLAG => {
            let v = try!(number::decode_i64(&buf[1..]));
            datum = Datum::I64(v);
            readed += 8;
        }
        UINT_FLAG => {
            let v = try!(number::decode_u64(&buf[1..]));
            datum = Datum::U64(v);
            readed += 8;
        }
        BYTES_FLAG => {
            let (v, l) = try!(bytes::decode_bytes(&buf[1..]));
            datum = Datum::Bytes(v);
            readed += l;
        }
        COMPACT_BYTES_FLAG => {
            let (v, l) = try!(bytes::decode_compact_bytes(&buf[1..]));
            datum = Datum::Bytes(v);
            readed += l;
        }
        NIL_FLAG => {
            datum = Datum::Null;
        }
        _ => unimplemented!(),
    }
    Ok((datum, readed))
}

/// `decode` decodes all datum from a byte slice generated by tidb.
pub fn decode(buf: &[u8]) -> Result<(Vec<Datum>, usize)> {
    let mut res = vec![];
    let mut readed = 0;
    while readed < buf.len() {
        let (v, vn) = try!(decode_datum(&buf[readed..]));
        res.push(v);
        readed += vn;
    }
    Ok((res, readed))
}

/// Get the approximate needed buffer size of values.
///
/// This function ensures that encoded values must fit in the given buffer size.
pub fn approximate_size(values: &[Datum], comparable: bool) -> usize {
    values.iter()
          .map(|v| {
              match *v {
                  Datum::I64(_) | Datum::U64(_) => 9,
                  Datum::Bytes(ref bs) => {
                      if comparable {
                          bytes::max_encoded_bytes_size(bs.len()) + 1
                      } else {
                          bs.len() + number::MAX_VAR_I64_LEN + 1
                      }
                  }
                  Datum::Null | Datum::Min | Datum::Max => 1,
                  _ => unimplemented!(),
              }
          })
          .sum()
}

fn encode_bytes(buf: &mut [u8], value: &[u8], comparable: bool) -> Result<usize> {
    try!(codec::check_bound(buf, 1 + value.len()));
    let len = if comparable {
        buf[0] = BYTES_FLAG;
        try!(bytes::encode_bytes_to_buf(&mut buf[1..], value))
    } else {
        buf[0] = COMPACT_BYTES_FLAG;
        try!(bytes::encode_compact_bytes(&mut buf[1..], value))
    };
    Ok(len + 1)
}

/// Encode values to buf slice.
pub fn encode(buf: &mut [u8], values: &[Datum], comparable: bool) -> Result<usize> {
    let mut idx = 0;
    let mut find_min = false;
    for v in values {
        if find_min {
            return Err(Error::InvalidDataType("MinValue should be the last datum.".to_owned()));
        }
        match *v {
            Datum::I64(i) => {
                buf[idx] = INT_FLAG;
                idx += 1;
                try!(number::encode_i64(&mut buf[idx..], i));
                idx += 8;
            }
            Datum::U64(u) => {
                buf[idx] = UINT_FLAG;
                idx += 1;
                try!(number::encode_u64(&mut buf[idx..], u));
                idx += 8;
            }
            Datum::Bytes(ref bs) => {
                idx += try!(encode_bytes(&mut buf[idx..], bs, comparable));
            }
            Datum::Null => {
                buf[idx] = NIL_FLAG;
                idx += 1;
            }
            Datum::Min => {
                buf[idx] = BYTES_FLAG; // for backward compatibility
                idx += 1;
                find_min = true;
            }
            Datum::Max => {
                buf[idx] = MAX_FLAG;
                idx += 1;
            }
            _ => unimplemented!(),
        }
    }
    Ok(idx)
}

pub fn encode_key(values: &[Datum]) -> Result<Vec<u8>> {
    let mut buf = vec![0; approximate_size(values, true)];
    let written = try!(encode(&mut buf, values, true));
    buf.truncate(written);
    Ok(buf)
}

pub fn encode_value(values: &[Datum]) -> Result<Vec<u8>> {
    let mut v = vec![0; approximate_size(values, false)];
    let written = try!(encode(&mut v, values, false));
    v.truncate(written);
    Ok(v)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::Ordering;
    use util::codec::Error;
    use std::f64;

    #[test]
    fn test_datum_codec() {
        let mut table = vec![
			vec![Datum::I64(1)],
			vec![Datum::U64(1), b"123".as_ref().into(), Datum::I64(-1)],
			vec![Datum::Null],
		];

        for vs in table.drain(..) {
            let mut buf = encode_key(&vs).unwrap();
            let (decoded, readed) = decode(&buf).unwrap();
            assert_eq!(buf.len(), readed);
            assert_eq!(vs, decoded);

            buf = encode_value(&vs).unwrap();
            let (decoded, readed) = decode(&buf).unwrap();
            assert_eq!(buf.len(), readed);
            assert_eq!(vs, decoded);
        }
    }

    #[test]
    fn test_datum_cmp() {
        let tests = vec![
            (Datum::F64(1.0), Datum::F64(1.0), Ordering::Equal),
            (Datum::F64(1.0), Datum::F64(f64::MAX), Ordering::Less),
            (Datum::F64(1.0), Datum::F64(f64::MIN), Ordering::Greater),
            (Datum::F64(1.0), Datum::F64(f64::INFINITY), Ordering::Less),
            (Datum::F64(1.0), Datum::F64(f64::NEG_INFINITY), Ordering::Greater),
            (Datum::F64(1.0), b"1".as_ref().into(), Ordering::Equal),
            (Datum::F64(f64::INFINITY), Datum::F64(f64::INFINITY), Ordering::Equal),
            (Datum::F64(f64::NEG_INFINITY), Datum::F64(f64::NEG_INFINITY), Ordering::Equal),
            (Datum::I64(1), Datum::I64(1), Ordering::Equal),
            (Datum::I64(-1), Datum::I64(1), Ordering::Less),
            (Datum::I64(-1), b"-1".as_ref().into(), Ordering::Equal),
            (Datum::U64(1), Datum::U64(1), Ordering::Equal),
            (Datum::U64(1), Datum::I64(-1), Ordering::Greater),
            (Datum::U64(1), b"1".as_ref().into(), Ordering::Equal),
            (b"1".as_ref().into(), b"1".as_ref().into(), Ordering::Equal),
            (b"1".as_ref().into(), Datum::I64(-1), Ordering::Greater),
            (b"1".as_ref().into(), Datum::U64(1), Ordering::Equal),
            (Datum::Null, Datum::I64(2), Ordering::Less),
            (Datum::Null, Datum::Null, Ordering::Equal),

            (false.into(), Datum::Null, Ordering::Greater),
            (false.into(), true.into(), Ordering::Less),
            (true.into(), true.into(), Ordering::Equal),
            (false.into(), false.into(), Ordering::Equal),
            (true.into(), Datum::I64(2), Ordering::Less),

            (Datum::F64(1.23), Datum::Null, Ordering::Greater),
            (Datum::F64(0.0), Datum::F64(3.45), Ordering::Less),
            (Datum::F64(354.23), Datum::F64(3.45), Ordering::Greater),
            (Datum::F64(3.452), Datum::F64(3.452), Ordering::Equal),

            (Datum::I64(432), Datum::Null, Ordering::Greater),
            (Datum::I64(-4), Datum::I64(32), Ordering::Less),
            (Datum::I64(4), Datum::I64(-32), Ordering::Greater),
            (Datum::I64(432), Datum::I64(12), Ordering::Greater),
            (Datum::I64(23), Datum::I64(128), Ordering::Less),
            (Datum::I64(123), Datum::I64(123), Ordering::Equal),
            (Datum::I64(23), Datum::I64(123), Ordering::Less),
            (Datum::I64(133), Datum::I64(183), Ordering::Less),

            (Datum::U64(123), Datum::U64(183), Ordering::Less),
            (Datum::U64(2), Datum::I64(-2), Ordering::Greater),
            (Datum::U64(2), Datum::I64(1), Ordering::Greater),

            (b"".as_ref().into(), Datum::Null, Ordering::Greater),
            (b"".as_ref().into(), b"24".as_ref().into(), Ordering::Less),
            (b"aasf".as_ref().into(), b"4".as_ref().into(), Ordering::Greater),
            (b"".as_ref().into(), b"".as_ref().into(), Ordering::Equal),
            (b"abc".as_ref().into(), b"ab".as_ref().into(), Ordering::Greater),
            (b"123".as_ref().into(), Datum::I64(1234), Ordering::Less),
            (b"".as_ref().into(), Datum::Null, Ordering::Greater),
        ];

        for (lhs, rhs, ret) in tests {
            if ret != lhs.cmp(&rhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", lhs, ret, rhs);
            }

            let rev_ret = match ret {
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            };

            if rev_ret != rhs.cmp(&lhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", rhs, rev_ret, lhs);
            }
        }
    }

    #[test]
    fn test_bad_datum_cmp() {
        let tests = vec![
            (Datum::F64(f64::NAN), Datum::F64(1.0)),
            (Datum::F64(f64::NAN), Datum::F64(f64::INFINITY)),
            (Datum::F64(f64::NAN), Datum::F64(f64::NEG_INFINITY)),
        ];

        for (lhs, rhs) in tests {
            if let Err(Error::InvalidDataType(_)) = lhs.cmp(&rhs) {
                continue;
            }
            panic!("invalid data type error should be thrown! compare {:?} with {:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_datum_to_bool() {
        let tests = vec![
            (Datum::I64(0), false),
            (Datum::I64(-1), true),
            (Datum::U64(0), false),
            (Datum::U64(1), true),
            (Datum::F32(0f32), false),
            (Datum::F32(0.4), false),
            (Datum::F32(0.5), true),
            (Datum::F32(-0.5), true),
            (Datum::F32(-0.4), false),
            (Datum::F64(0f64), false),
            (Datum::F64(0.4), false),
            (Datum::F64(0.5), true),
            (Datum::F64(-0.5), true),
            (Datum::F64(-0.4), false),
            (b"".as_ref().into(), false),
            (b"0.5".as_ref().into(), false),
            (b"0".as_ref().into(), false),
            (b"2".as_ref().into(), true),
            (b"abc".as_ref().into(), false),
        ];
        for (d, b) in tests {
            if d.as_bool().unwrap() ^ b {
                panic!("expect {:?} to be {}", d, b);
            }
        }
    }
}
