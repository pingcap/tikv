// Copyright 2017 PingCAP, Inc.
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

// FIXME(shirly): remove following later
#![allow(dead_code)]

use super::super::Result;
use std::cmp::Ordering;
use std::{self, str, f64};
use std::collections::BTreeMap;
use serde_json;
use std::io::{Read, Write};
use std::fmt;
use util::codec::Error;
use std::str::FromStr;
use util::codec::number::{NumberDecoder, NumberEncoder};
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::ser::{Serialize, Serializer, SerializeTuple, SerializeMap};
use serde::de::{self, Deserialize, Deserializer, Visitor, SeqAccess, MapAccess};

const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_I64: u8 = 0x09;
const TYPE_CODE_DOUBLE: u8 = 0x0b;
const TYPE_CODE_STRING: u8 = 0x0c;

const PRECEDENCE_BLOB: i32 = -1;
const PRECEDENCE_BIT: i32 = -2;
const PRECEDENCE_OPAQUE: i32 = -3;
const PRECEDENCE_DATETIME: i32 = -4;
const PRECEDENCE_TIME: i32 = -5;
const PRECEDENCE_DATE: i32 = -6;
const PRECEDENCE_BOOLEAN: i32 = -7;
const PRECEDENCE_ARRAY: i32 = -8;
const PRECEDENCE_OBJECT: i32 = -9;
const PRECEDENCE_STRING: i32 = -10;
const PRECEDENCE_NUMBER: i32 = -11;
const PRECEDENCE_NULL: i32 = -12;

const JSON_LITERAL_NIL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

const TYPE_LEN: usize = 1;
const LITERAL_LEN: usize = 1;
const U16_LEN: usize = 2;
const U32_LEN: usize = 4;
const NUMBER_LEN: usize = 8;
const KEY_ENTRY_LEN: usize = U32_LEN + U16_LEN;
const VALUE_ENTRY_LEN: usize = TYPE_LEN + U32_LEN;
const ELEMENT_COUNT_LEN: usize = U32_LEN;
const SIZE_LEN: usize = U32_LEN;

const ERR_CONVERT_FAILED: &str = "Can not covert from ";

/// Json implements type json used in tikv, it specifies the following
/// implementations:
/// 1. Serialize `json` values into binary representation, and reading values
///  back from the binary representation.
/// 2. Serialize `json` values into readable string representation, and reading
/// values back from string representation.
#[derive(Clone, Debug)]
pub enum Json {
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    I64(i64),
    Double(f64),
    String(String),
    Boolean(bool),
    None,
}

impl Json {
    fn get_type_code(&self) -> u8 {
        match *self {
            Json::Object(_) => TYPE_CODE_OBJECT,
            Json::Array(_) => TYPE_CODE_ARRAY,
            Json::Boolean(_) | Json::None => TYPE_CODE_LITERAL,
            Json::I64(_) => TYPE_CODE_I64,
            Json::Double(_) => TYPE_CODE_DOUBLE,
            Json::String(_) => TYPE_CODE_STRING,
        }
    }

    pub fn binary_len(&self) -> usize {
        TYPE_LEN + self.body_binary_len()
    }

    fn body_binary_len(&self) -> usize {
        match *self {
            Json::Object(ref d) => get_obj_binary_len(d),
            Json::Array(ref d) => get_array_binary_len(d),
            Json::Boolean(_) | Json::None => LITERAL_LEN,
            Json::I64(_) | Json::Double(_) => NUMBER_LEN,
            Json::String(ref d) => get_str_binary_len(d),
        }
    }

    fn get_precedence(&self) -> i32 {
        match *self {
            Json::Object(_) => PRECEDENCE_OBJECT,
            Json::Array(_) => PRECEDENCE_ARRAY,
            Json::Boolean(_) => PRECEDENCE_BOOLEAN,
            Json::None => PRECEDENCE_NULL,
            Json::I64(_) | Json::Double(_) => PRECEDENCE_NUMBER,
            Json::String(_) => PRECEDENCE_STRING,
        }
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn as_literal(&self) -> Result<u8> {
        match *self {
            Json::Boolean(d) => {
                if d {
                    Ok(JSON_LITERAL_TRUE)
                } else {
                    Ok(JSON_LITERAL_FALSE)
                }
            }
            Json::None => Ok(JSON_LITERAL_NIL),
            _ => {
                Err(invalid_type!("{} from {} to literal",
                                  ERR_CONVERT_FAILED,
                                  self.get_type_code()))
            }
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match *self {
            Json::Double(d) => Ok(d),
            Json::I64(d) => Ok(d as f64),
            Json::Boolean(_) => {
                let v = try!(self.as_literal());
                Ok(v as f64)
            }
            _ => {
                Err(invalid_type!("{} from {} to f64",
                                  ERR_CONVERT_FAILED,
                                  self.get_type_code()))
            }
        }
    }
}

impl FromStr for Json {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match serde_json::from_str(s) {
            Ok(value) => Ok(value),
            Err(e) => Err(invalid_type!("Illegal Json text:{:?}", e)),
        }
    }
}

fn get_obj_binary_len(data: &BTreeMap<String, Json>) -> usize {
    let element_count = data.len();
    let key_entries_len = element_count * KEY_ENTRY_LEN;
    let value_entries_len = element_count * VALUE_ENTRY_LEN;
    let mut keys_len = 0;
    let mut values_len = 0;
    for (k, v) in data {
        keys_len += k.len();
        if v.get_type_code() != TYPE_CODE_LITERAL {
            values_len += v.body_binary_len();
        }
    }
    // object: element-count size key-entry* value-entry* key* value*
    ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len + keys_len + values_len
}

fn get_array_binary_len(data: &[Json]) -> usize {
    let element_count = data.len();
    let value_entries_len = element_count * VALUE_ENTRY_LEN;
    let mut values_len = 0;
    for v in data {
        if v.get_type_code() != TYPE_CODE_LITERAL {
            values_len += v.body_binary_len();
        }
    }
    // array ::= element-count size value-entry* value*
    ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + values_len
}

fn get_str_binary_len(data: &str) -> usize {
    let len = data.as_bytes().len();
    get_var_u64_binary_len(len as u64) + len
}

fn get_var_u64_binary_len(mut v: u64) -> usize {
    let mut len = 1;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

impl Serialize for Json {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            Json::None => serializer.serialize_none(),
            Json::Boolean(d) => serializer.serialize_bool(d),
            Json::String(ref s) => serializer.serialize_str(s),
            Json::Object(ref obj) => {
                let mut map = try!(serializer.serialize_map(Some(obj.len())));
                for (k, v) in obj {
                    try!(map.serialize_entry(k, v));
                }
                map.end()
            }
            Json::Array(ref array) => {
                let mut tup = try!(serializer.serialize_tuple(array.len()));
                for item in array {
                    try!(tup.serialize_element(item));
                }
                tup.end()
            }
            Json::Double(d) => serializer.serialize_f64(d),
            Json::I64(d) => serializer.serialize_i64(d),
        }
    }
}

impl Eq for Json {}
impl Ord for Json {
    fn cmp(&self, right: &Json) -> Ordering {
        self.partial_cmp(right).unwrap()
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.partial_cmp(right).unwrap() == Ordering::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Ordering> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            if self.get_precedence() == PRECEDENCE_NUMBER {
                let left_data = self.as_f64().unwrap();
                let right_data = right.as_f64().unwrap();
                if (left_data - right_data).abs() < f64::EPSILON {
                    return Some(Ordering::Equal);
                } else {
                    return left_data.partial_cmp(&right_data);
                }
            }

            return match (self, right) {
                (&Json::Boolean(left_data), &Json::Boolean(right_data)) => {
                    left_data.partial_cmp(&right_data)
                }
                (&Json::String(ref left_data), &Json::String(ref right_data)) => {
                    left_data.partial_cmp(right_data)
                }
                (&Json::Array(ref left_data), &Json::Array(ref right_data)) => {
                    left_data.partial_cmp(right_data)
                }
                (&Json::Object(_), &Json::Object(_)) => {
                    let mut left_data = vec![];
                    let mut right_data = vec![];
                    left_data.encode_json(self).unwrap();
                    right_data.encode_json(right).unwrap();
                    left_data.partial_cmp(&right_data)
                }
                _ => Some(Ordering::Equal),
            };
        }

        let left_data = self.as_f64();
        let right_data = right.as_f64();
        // tidb treats boolean as integer, but boolean is different from integer in JSON.
        // so we need convert them to same type and then compare.
        if left_data.is_ok() && right_data.is_ok() {
            let left = left_data.unwrap();
            let right = right_data.unwrap();
            return left.partial_cmp(&right);
        }

        if precedence_diff > 0 {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}

struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a json value")
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::None)
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::Boolean(v))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::I64(v))
    }

    fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        self.visit_i64(v as i64)
    }

    fn visit_f64<E>(self, v: f64) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::Double(v))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::String(String::from(v)))
    }

    fn visit_seq<M>(self, mut seq: M) -> std::result::Result<Self::Value, M::Error>
        where M: SeqAccess<'de>
    {
        let mut seqs = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element()? {
            seqs.push(value);
        }
        Ok(Json::Array(seqs))
    }

    fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
        where M: MapAccess<'de>
    {
        let mut map = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }
        Ok(Json::Object(map))
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        deserializer.deserialize_any(JsonVisitor)
    }
}

// The binary Json format from `MySQL` 5.7 is in the following link:
// (https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h#L52)
// The only difference is that we use large `object` or large `array` for
// the small corresponding ones. That means in our implementation there
// is no difference between small `object` and big `object`, so does `array`.
pub trait JsonEncoder: NumberEncoder {
    fn encode_json(&mut self, data: &Json) -> Result<()> {
        try!(self.write_u8(data.get_type_code()));
        self.encode_json_body(data)
    }

    fn encode_json_body(&mut self, data: &Json) -> Result<()> {
        match *data {
            Json::Object(ref d) => self.encode_obj(d),
            Json::Array(ref d) => self.encode_array(d),
            Json::Boolean(_) | Json::None => {
                let v = try!(data.as_literal());
                self.encode_literal(v)
            }
            Json::I64(d) => self.encode_json_i64(d),
            Json::Double(d) => self.encode_json_f64(d),
            Json::String(ref d) => self.encode_str(d),
        }
    }

    fn encode_obj(&mut self, data: &BTreeMap<String, Json>) -> Result<()> {
        // object: element-count size key-entry* value-entry* key* value*
        let element_count = data.len();
        // key-entry ::= key-offset(uint32) key-length(uint16)
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut key_entries = Vec::with_capacity(key_entries_len);
        let mut encode_keys = Vec::new();
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;
        for key in data.keys() {
            let encode_key = key.as_bytes();
            let key_len = try!(encode_keys.write(encode_key));
            try!(key_entries.encode_u32_le(key_offset as u32));
            try!(key_entries.encode_u16_le(key_len as u16));
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data.values() {
            try!(value_entries.encode_json_item(value, &mut value_offset, &mut encode_values));
        }
        let size = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len +
                   encode_keys.len() + encode_values.len();
        try!(self.encode_u32_le(element_count as u32));
        try!(self.encode_u32_le(size as u32));
        try!(self.write_all(key_entries.as_mut()));
        try!(self.write_all(value_entries.as_mut()));
        try!(self.write_all(encode_keys.as_mut()));
        try!(self.write_all(encode_values.as_mut()));
        Ok(())
    }

    fn encode_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size value-entry* value*
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data {
            try!(value_entries.encode_json_item(value, &mut value_offset, &mut encode_values));
        }
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + encode_values.len();
        try!(self.encode_u32_le(element_count as u32));
        try!(self.encode_u32_le(total_size as u32));
        try!(self.write_all(value_entries.as_mut()));
        try!(self.write_all(encode_values.as_mut()));
        Ok(())
    }

    fn encode_literal(&mut self, data: u8) -> Result<()> {
        try!(self.write_u8(data));
        Ok(())
    }

    fn encode_json_i64(&mut self, data: i64) -> Result<()> {
        self.encode_i64_le(data)
    }

    fn encode_json_f64(&mut self, data: f64) -> Result<()> {
        self.encode_f64_le(data)
    }

    fn encode_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        try!(self.encode_var_u64(bytes_len));
        try!(self.write_all(bytes));
        Ok(())
    }

    fn encode_json_item(&mut self,
                        data: &Json,
                        offset: &mut u32,
                        data_buf: &mut Vec<u8>)
                        -> Result<()> {
        let code = data.get_type_code();
        try!(self.write_u8(code));
        match *data {
            // If the data has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            Json::Boolean(_) | Json::None => {
                let v = try!(data.as_literal());
                try!(self.write_u8(v));
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    try!(self.write_u8(JSON_LITERAL_NIL));
                }
            }
            _ => {
                try!(self.encode_u32_le(*offset));
                let start_len = data_buf.len();
                try!(data_buf.encode_json_body(data));
                *offset += (data_buf.len() - start_len) as u32;
            }
        };
        Ok(())
    }
}
impl<T: Write> JsonEncoder for T {}

pub trait JsonDecoder: NumberDecoder {
    fn decode_json(&mut self) -> Result<Json> {
        let code = try!(self.read_u8());
        self.decode_json_body(code)
    }

    fn decode_json_body(&mut self, code_type: u8) -> Result<Json> {
        match code_type {
            TYPE_CODE_OBJECT => self.decode_json_obj(),
            TYPE_CODE_ARRAY => self.decode_json_array(),
            TYPE_CODE_LITERAL => self.decode_json_literal(),
            TYPE_CODE_I64 => self.decode_json_i64(),
            TYPE_CODE_DOUBLE => self.decode_json_double(),
            TYPE_CODE_STRING => self.decode_json_str(),
            _ => Err(invalid_type!("unsupported type {:?}", code_type)),
        }
    }

    fn decode_json_obj(&mut self) -> Result<Json> {
        // count size key_entries value_entries keys values
        let element_count = try!(self.decode_u32_le()) as usize;
        let total_size = try!(self.decode_u32_le()) as usize;
        let left_size = total_size - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut data = vec![0; left_size];
        try!(self.read_exact(&mut data));
        let mut obj = BTreeMap::new();
        if element_count == 0 {
            return Ok(Json::Object(obj));
        }
        // key_entries
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        let mut key_entries_data = &data[0..key_entries_len];

        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = &data[key_entries_len..(key_entries_len + value_entries_len)];
        let mut key_offset = key_entries_len + value_entries_len;
        for _ in 0..element_count {
            let key_real_offset = try!(key_entries_data.decode_u32_le());
            let key_len = try!(key_entries_data.decode_u16_le());
            let key_data = &data[key_offset..(key_offset + key_len as usize)];
            let key = String::from(str::from_utf8(key_data).unwrap());
            let value =
                try!(value_entries_data.decode_json_item(&data[key_offset..], key_real_offset));
            obj.insert(key, value);
            key_offset += key_len as usize;
        }
        Ok(Json::Object(obj))
    }

    fn decode_json_array(&mut self) -> Result<Json> {
        // count size value_entries values
        let element_count = try!(self.decode_u32_le()) as usize;
        let total_size = try!(self.decode_u32_le());
        // already removed count and size
        let left_size = total_size as usize - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut data = vec![0; left_size];
        try!(self.read_exact(&mut data));
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = &data[0..value_entries_len];
        let values_data = &data[value_entries_len..];
        let mut array_data = Vec::with_capacity(element_count);
        let data_start_offset = (U32_LEN + U32_LEN + value_entries_len) as u32;
        for _ in 0..element_count {
            let value = try!(value_entries_data.decode_json_item(values_data, data_start_offset));
            array_data.push(value);
        }
        Ok(Json::Array(array_data))
    }

    fn decode_json_str(&mut self) -> Result<Json> {
        let length = try!(self.decode_var_u64());
        let mut encode_value = vec![0; length as usize];
        try!(self.read_exact(&mut encode_value));
        let value = try!(String::from_utf8(encode_value));
        Ok(Json::String(value))
    }

    fn decode_json_literal(&mut self) -> Result<Json> {
        match try!(self.read_u8()) {
            JSON_LITERAL_TRUE => Ok(Json::Boolean(true)),
            JSON_LITERAL_FALSE => Ok(Json::Boolean(false)),
            _ => Ok(Json::None),
        }
    }

    fn decode_json_double(&mut self) -> Result<Json> {
        let value = try!(self.decode_f64_le());
        Ok(Json::Double(value))
    }

    fn decode_json_i64(&mut self) -> Result<Json> {
        let value = try!(self.decode_i64_le());
        Ok(Json::I64(value))
    }

    fn decode_json_item(&mut self, values_data: &[u8], data_start_position: u32) -> Result<Json> {
        let mut entry = vec![0; VALUE_ENTRY_LEN];
        try!(self.read_exact(&mut entry));
        let mut entry = entry.as_slice();
        let code = try!(entry.read_u8());
        match code {
            TYPE_CODE_LITERAL => entry.decode_json_body(code),
            _ => {
                let real_offset = entry.decode_u32_le().unwrap();
                let offset_in_values = real_offset - data_start_position;
                let mut value = &values_data[offset_in_values as usize..];
                value.decode_json_body(code)
            }
        }
    }
}

impl<T: Read> JsonDecoder for T {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_serialize() {
        let jstr1 =
            r#"{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = r#"[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]"#;
        let j2: Json = jstr2.parse().unwrap();

        let json_nil = Json::None;
        let json_bool = Json::Boolean(true);
        let json_double = Json::Double(3.24);
        let json_str = Json::String(String::from("hello, 世界"));
        let test_cases = vec![json_nil, json_bool, json_double, json_str, j1, j2];
        for json in test_cases {
            let mut data = vec![];
            data.encode_json(&json).unwrap();
            let output = data.as_slice().decode_json().unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }


    #[test]
    fn test_from_str_for_object() {
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "c": null,"b": true}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = j1.to_string();
        let expect_str = r#"{"a":[1,"2",{"aa":"bb"},4.0,null],"b":true,"c":null}"#;
        assert_eq!(jstr2, expect_str);
    }

    #[test]
    fn test_from_str() {
        let legal_cases = vec!{
            (r#"{"key":"value"}"#, TYPE_CODE_OBJECT),
            (r#"["d1","d2"]"#, TYPE_CODE_ARRAY),
            (r#"3"#, TYPE_CODE_I64),
            (r#"3.0"#, TYPE_CODE_DOUBLE),
            (r#"null"#, TYPE_CODE_LITERAL),
            (r#"true"#, TYPE_CODE_LITERAL),
            (r#"false"#, TYPE_CODE_LITERAL),
        };

        for (json_str, code) in legal_cases {
            let json: Json = json_str.parse().unwrap();
            assert_eq!(json.get_type_code(), code);
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_err());
        }
    }

    #[test]
    fn test_cmp_json_between_same_type() {
        let test_cases = vec![
            ("false", "true"),
            ("3", "5"),
            ("3.0", "4.9"),
            (r#""hello""#, r#""hello, world""#),
            (r#"["a", "b"]"#, r#"["a", "c"]"#),
            (r#"{"a": "b"}"#, r#"{"a": "c"}"#),
        ];
        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
            assert_eq!(left, left);
        }
        assert_eq!(Json::None, Json::None);
    }

    #[test]
    fn test_cmp_json_between_diff_type() {
        let test_cases = vec![
            ("1.5", "2"),
            ("1.5", "false"),
            ("true", "1.5"),
            ("true", "2"),
            ("null", r#"{"a": "b"}"#),
            ("2", r#""hello, world""#),
            (r#""hello, world""#, r#"{"a": "b"}"#),
            (r#"{"a": "b"}"#, r#"["a", "b"]"#),
            (r#"["a", "b"]"#, "false"),
        ];

        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
        }

        assert_eq!(Json::I64(2), Json::Boolean(false));
    }
}
