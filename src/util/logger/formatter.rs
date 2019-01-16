// Copyright 2019 PingCAP, Inc.
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

use serde_json;
use std::io;

/// Writes file name into the writer, removes the character which not match `[a-zA-Z0-9\.-_]`
pub fn write_file_name<W>(writer: &mut W, file_name: &str) -> io::Result<()>
where
    W: io::Write + ?Sized,
{
    let mut start = 0;
    let bytes = file_name.as_bytes();
    for (index, &b) in bytes.iter().enumerate() {
        if (b >= b'A' && b <= b'Z')
            || (b >= b'a' && b <= b'z')
            || (b >= b'0' && b <= b'9')
            || b == b'.'
            || b == b'-'
            || b == b'_'
        {
            continue;
        }
        if start < index {
            writer.write_all((&file_name[start..index]).as_bytes())?;
        }
        start = index + 1;
    }
    if start < bytes.len() {
        writer.write_all((&file_name[start..]).as_bytes())?;
    }
    Ok(())
}

/// According to [RFC: Unified Log Format], the following characters need to be escaped:
///
/// - U+0000 (NULL) ~ U+0020 (SPACE)
/// - U+0022 (QUOTATION MARK)
/// - U+003D (EQUALS SIGN)
/// - U+005B (LEFT SQUARE BRACKET)
/// - U+005D (RIGHT SQUARE BRACKET)
///
/// [RFC: Unified Log Format]: (https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md)
///
#[inline]
fn need_escape(bytes: &[u8]) -> bool {
    for &byte in bytes {
        if byte <= 0x20 || byte == 0x22 || byte == 0x3D || byte == 0x5B || byte == 0x5D {
            return true;
        }
    }
    false
}

/// According to [RFC: Unified Log Format], escapes the given data and writes it into a writer.
/// If there is no character [`need escape`], write the data into the writer directly.
/// Else, call `serde_json::to_writer` which serializes the given data structure as JSON into a writer.
///
/// [RFC: Unified Log Format]: (https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md)
/// [`need escape`]: #method.need_escape
///
pub fn write_escaped_str<W>(writer: &mut W, value: &str) -> io::Result<()>
where
    W: io::Write + ?Sized,
{
    if !need_escape(value.as_bytes()) {
        writer.write_all(value.as_bytes())?;
    } else {
        serde_json::to_writer(writer, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_need_escape() {
        let cases = [
            ("abc", false),
            ("a b", true),
            ("a=b", true),
            ("a[b", true),
            ("a]b", true),
            ("\u{000f}", true),
            ("💖", false),
            ("�", false),
            ("\u{7fff}\u{ffff}", false),
            ("欢迎", false),
            ("欢迎 TiKV", true),
        ];
        for (input, expect) in &cases {
            assert_eq!(
                need_escape(input.as_bytes()),
                *expect,
                "{} | {}",
                input,
                expect
            );
        }
    }

    #[test]
    fn test_write_file_name() {
        let mut s = vec![];
        write_file_name(
            &mut s,
            "+=!@#$%^&*(){}|:\"<>/?\u{000f} 老虎 tiger 🐅\r\n\\-_1234567890.rs",
        ).unwrap();
        assert_eq!("tiger-_1234567890.rs", &String::from_utf8(s).unwrap())
    }
}
