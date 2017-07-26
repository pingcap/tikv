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

use std::fmt::{self, Write};
use std::str::{self, FromStr};
use std::ascii::AsciiExt;
use std::time::Duration;
use std::net::{SocketAddrV4, SocketAddrV6};

use url;
use regex::Regex;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Unexpected, Visitor};

use util::collections::HashMap;
use util;
use rocksdb::{DBCompressionType, DBRecoveryMode, CompactionPriority};

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        Limit(msg: String) {
            description(msg)
            display("{}", msg)
        }
        Address(msg: String) {
            description(msg)
            display("config address error: {}", msg)
        }
        StoreLabels(msg: String) {
            description(msg)
            display("store label error: {}", msg)
        }
        Value(msg: String) {
            description(msg)
            display("config value error: {}", msg)
        }
    }
}

pub fn parse_rocksdb_compression(tp: &str) -> Result<DBCompressionType, ConfigError> {
    match &*tp.trim().to_lowercase() {
        "no" => Ok(DBCompressionType::No),
        "snappy" => Ok(DBCompressionType::Snappy),
        "zlib" => Ok(DBCompressionType::Zlib),
        "bzip2" => Ok(DBCompressionType::Bz2),
        "lz4" => Ok(DBCompressionType::Lz4),
        "lz4hc" => Ok(DBCompressionType::Lz4hc),
        "zstd" => Ok(DBCompressionType::Zstd),
        _ => Err(ConfigError::Value(format!("invalid compression type {:?}", tp))),
    }
}

pub fn parse_rocksdb_per_level_compression(tp: &str)
                                           -> Result<Vec<DBCompressionType>, ConfigError> {
    tp.split(':')
        .map(parse_rocksdb_compression)
        .collect()
}

pub fn parse_rocksdb_wal_recovery_mode(mode: i64) -> Result<DBRecoveryMode, ConfigError> {
    match mode {
        0 => Ok(DBRecoveryMode::TolerateCorruptedTailRecords),
        1 => Ok(DBRecoveryMode::AbsoluteConsistency),
        2 => Ok(DBRecoveryMode::PointInTime),
        3 => Ok(DBRecoveryMode::SkipAnyCorruptedRecords),
        _ => Err(ConfigError::Value(format!("invalid recovery mode: {:?}", mode))),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "DBCompressionType")]
#[serde(rename_all = "kebab-case")]
pub enum CompressionType {
    No,
    Snappy,
    Zlib,
    #[serde(rename = "bzip2")]
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
    ZstdNotFinal,
    Disable,
}

macro_rules! numeric_enum_mod {
    ($name:ident $enum:ident { $($variant:ident = $value:expr, )* }) => {
        pub mod $name {
            use std::fmt;

            use serde::{Serializer, Deserializer};
            use serde::de::{self, Unexpected, Visitor};
            use rocksdb::$enum;

            pub fn serialize<S>(mode: &$enum, serializer: S) -> Result<S::Ok, S::Error>
                where S: Serializer
            {
                serializer.serialize_i64(*mode as i64)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<$enum, D::Error>
                where D: Deserializer<'de>
            {
                struct EnumVisitor;

                impl<'de> Visitor<'de> for EnumVisitor {
                    type Value = $enum;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        write!(formatter, concat!("valid ", stringify!($enum)))
                    }

                    fn visit_i64<E>(self, value: i64) -> Result<$enum, E>
                        where E: de::Error
                    {
                        match value {
                            $( $value => Ok($enum::$variant), )*
                            _ => Err(E::invalid_value(Unexpected::Signed(value), &self))
                        }
                    }
                }

                deserializer.deserialize_i64(EnumVisitor)
            }

            #[cfg(test)]
            mod tests {
                use toml;
                use rocksdb::$enum;

                #[test]
                fn test_serde() {
                    #[derive(Serialize, Deserialize, PartialEq)]
                    struct EnumHolder {
                        #[serde(with = "super")]
                        e: $enum,
                    }

                    let cases = vec![
                        $(($enum::$variant, $value), )*
                    ];
                    for (e, v) in cases {
                        let holder = EnumHolder { e };
                        let res = toml::to_string(&holder).unwrap();
                        let exp = format!("e = {}\n", v);
                        assert_eq!(res, exp);
                        let h: EnumHolder = toml::from_str(&exp).unwrap();
                        assert!(h == holder);
                    }
                }
            }
        }
    }
}

numeric_enum_mod!{compaction_pri_serde CompactionPriority {
    ByCompensatedSize = 0,
    OldestLargestSeqFirst = 1,
    OldestSmallestSeqFirst = 2,
    MinOverlappingRatio = 3,
}}

numeric_enum_mod!{recovery_mode_serde DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}}

pub fn parse_rocksdb_compaction_pri(priority: i64) -> Result<CompactionPriority, ConfigError> {
    match priority {
        0 => Ok(CompactionPriority::ByCompensatedSize),
        1 => Ok(CompactionPriority::OldestLargestSeqFirst),
        2 => Ok(CompactionPriority::OldestSmallestSeqFirst),
        3 => Ok(CompactionPriority::MinOverlappingRatio),
        _ => Err(ConfigError::Value(format!("invalid Compaction priority: {:?}", priority))),
    }
}

fn split_property(property: &str) -> Result<(f64, &str), ConfigError> {
    let mut indx = 0;
    for s in property.chars() {
        match s {
            '0'...'9' | '.' => {
                indx += 1;
            }
            _ => {
                break;
            }
        }
    }

    let (num, unit) = property.split_at(indx);
    num.parse::<f64>()
        .map(|f| (f, unit))
        .or_else(|_| Err(ConfigError::Value(format!("cannot parse {:?} as f64", num))))
}

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
const KB: u64 = UNIT * DATA_MAGNITUDE;
const MB: u64 = KB * DATA_MAGNITUDE;
const GB: u64 = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;

pub fn parse_readable_int(size: &str) -> Result<i64, ConfigError> {
    let (num, unit) = try!(split_property(size));

    match &*unit.to_lowercase() {
        // file size
        "kb" => Ok((num * (KB as f64)) as i64),
        "mb" => Ok((num * (MB as f64)) as i64),
        "gb" => Ok((num * (GB as f64)) as i64),
        "tb" => Ok((num * (TB as f64)) as i64),
        "pb" => Ok((num * (PB as f64)) as i64),

        // time
        "ms" => Ok((num * (MS as f64)) as i64),
        "s" => Ok((num * (SECOND as f64)) as i64),
        "m" => Ok((num * (MINUTE as f64)) as i64),
        "h" => Ok((num * (HOUR as f64)) as i64),

        _ => Err(ConfigError::Value(format!("invalid unit {:?}", unit))),
    }
}

pub fn parse_store_labels(labels: &str) -> Result<HashMap<String, String>, ConfigError> {
    let mut map = HashMap::default();

    let re = Regex::new(r"^[a-z0-9]([a-z0-9-._]*[a-z0-9])?$").unwrap();
    for label in labels.split(',') {
        if label.is_empty() {
            continue;
        }
        let label = label.to_lowercase();
        let kv: Vec<_> = label.split('=').collect();
        match kv[..] {
            [k, v] => {
                if !re.is_match(k) || !re.is_match(v) {
                    return Err(ConfigError::StoreLabels(format!("invalid label {:?}", label)));
                }
                if map.insert(k.to_owned(), v.to_owned()).is_some() {
                    return Err(ConfigError::StoreLabels(format!("duplicated label {:?}", label)));
                }
            }
            _ => {
                return Err(ConfigError::StoreLabels(format!("invalid label {:?}", label)));
            }
        }
    }

    Ok(map)
}

pub struct ReadableSize(pub u64);

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let size = self.0;
        let mut buffer = String::new();
        if size % PB == 0 {
            write!(buffer, "{}PB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
                where E: de::Error
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
                where E: de::Error
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
                where E: de::Error
            {
                let err_msg = "valid size, only KB, MB, GB, TB, PB are supported.";
                let size_str = size_str.trim();
                if size_str.len() < 2 {
                    // When it's a string, it should contains a unit and a number.
                    // So its length should be at least 2.
                    return Err(E::invalid_value(Unexpected::Str(size_str), &err_msg));
                }

                if !size_str.is_ascii() {
                    return Err(E::invalid_value(Unexpected::Str(size_str), &"ascii str"));
                }

                let mut chrs = size_str.chars();
                let mut unit_char = chrs.next_back().unwrap();
                let mut number_str = chrs.as_str();
                if unit_char == 'B' {
                    let b = chrs.next_back().unwrap();
                    if b < '0' || b > '9' {
                        number_str = chrs.as_str();
                        unit_char = b;
                    }
                }

                let unit = match unit_char {
                    'K' => KB,
                    'M' => MB,
                    'G' => GB,
                    'T' => TB,
                    'P' => PB,
                    _ => return Err(E::invalid_value(Unexpected::Str(size_str), &err_msg)),
                };
                match number_str.trim().parse::<f64>() {
                    Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
                    Err(_) => Err(E::invalid_value(Unexpected::Str(size_str), &err_msg)),
                }
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

pub struct ReadableDuration(pub Duration);

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut dur = util::duration_to_ms(self.0);
        let mut buffer = String::new();
        if dur >= HOUR {
            write!(buffer, "{}h", dur / HOUR).unwrap();
            dur %= HOUR;
        }
        if dur >= MINUTE {
            write!(buffer, "{}m", dur / MINUTE).unwrap();
            dur %= MINUTE;
        }
        if dur >= SECOND {
            write!(buffer, "{}s", dur / SECOND).unwrap();
            dur %= SECOND;
        }
        if dur > 0 {
            write!(buffer, "{}ms", dur).unwrap();
        }
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> Result<ReadableDuration, E>
                where E: de::Error
            {
                let dur_str = dur_str.trim();
                if !dur_str.is_ascii() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &"ascii string"));
                }
                let err_msg = "valid duration, only h, m, s, ms are supported.";
                let mut left = dur_str.as_bytes();
                let mut last_unit = HOUR + 1;
                let mut dur = 0f64;
                while let Some(idx) = left.iter().position(|c| b"hms".contains(c)) {
                    let (first, second) = left.split_at(idx);
                    let unit = if second.starts_with(b"ms") {
                        left = &left[idx + 2..];
                        MS
                    } else {
                        let u = match second[0] {
                            b'h' => HOUR,
                            b'm' => MINUTE,
                            b's' => SECOND,
                            _ => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                        };
                        left = &left[idx + 1..];
                        u
                    };
                    if unit >= last_unit {
                        return Err(E::invalid_value(Unexpected::Str(dur_str),
                                                    &"h, m, s, ms should occur in giving order."));
                    }
                    // do we need to check 12h360m?
                    let number_str = unsafe { str::from_utf8_unchecked(first) };
                    dur += match number_str.trim().parse::<f64>() {
                        Ok(n) => n * unit as f64,
                        Err(_) => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                    };
                    last_unit = unit;
                }
                if !left.is_empty() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg));
                }
                if dur.is_sign_negative() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str),
                                                &"duration should be positive."));
                }
                let secs = dur as u64 / SECOND as u64;
                let millis = (dur as u64 % SECOND as u64) as u32 * 1_000_000;
                Ok(ReadableDuration(Duration::new(secs, millis)))
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    use std::mem;
    use libc;

    unsafe {
        let mut fd_limit = mem::zeroed();
        let mut err = libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit);
        if err != 0 {
            return Err(ConfigError::Limit("check_max_open_fds failed".to_owned()));
        }
        if fd_limit.rlim_cur >= expect {
            return Ok(());
        }

        let prev_limit = fd_limit.rlim_cur;
        fd_limit.rlim_cur = expect;
        if fd_limit.rlim_max < expect {
            // If the process is not started by privileged user, this will fail.
            fd_limit.rlim_max = expect;
        }
        err = libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limit);
        if err == 0 {
            return Ok(());
        }
        Err(ConfigError::Limit(format!("the maximum number of open file descriptors is too \
                                        small, got {}, expect greater or equal to {}",
                                       prev_limit,
                                       expect)))
    }
}

#[cfg(not(unix))]
pub fn check_max_open_fds(_: u64) -> Result<(), ConfigError> {
    Ok(())
}

#[cfg(target_os = "linux")]
mod check_kernel {
    use std::fs;
    use std::io::Read;

    use super::ConfigError;

    // pub for tests.
    pub type Checker = Fn(i64, i64) -> bool;

    // pub for tests.
    pub fn check_kernel_params(param_path: &str,
                               expect: i64,
                               checker: Box<Checker>)
                               -> Result<(), ConfigError> {
        let mut buffer = String::new();
        try!(fs::File::open(param_path)
            .and_then(|mut f| f.read_to_string(&mut buffer))
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e))));

        let got = try!(buffer.trim_matches('\n')
            .parse::<i64>()
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e))));

        let mut param = String::new();
        // skip 3, ["", "proc", "sys", ...]
        for path in param_path.split('/').skip(3) {
            param.push_str(path);
            param.push('.');
        }
        param.pop();

        if !checker(got, expect) {
            return Err(ConfigError::Limit(format!("kernel parameters {} got {}, expect {}",
                                                  param,
                                                  got,
                                                  expect)));
        }

        info!("kernel parameters {}: {}", param, got);
        Ok(())
    }

    /// `check_kernel_params` checks kernel parameters, following are checked so far:
    ///   - `net.core.somaxconn` should be greater or equak to 32768.
    ///   - `net.ipv4.tcp_syncookies` should be 0
    ///   - `vm.swappiness` shoud be 0
    ///
    /// Note that: It works on **Linux** only.
    pub fn check_kernel() -> Vec<ConfigError> {
        let params: Vec<(&str, i64, Box<Checker>)> = vec![
            // Check net.core.somaxconn.
            ("/proc/sys/net/core/somaxconn", 32768, box |got, expect| got >= expect),
            // Check net.ipv4.tcp_syncookies.
            ("/proc/sys/net/ipv4/tcp_syncookies", 0, box |got, expect| got == expect),
            // Check vm.swappiness.
            ("/proc/sys/vm/swappiness", 0, box |got, expect| got == expect),
        ];

        let mut errors = Vec::with_capacity(params.len());
        for (param_path, expect, checker) in params {
            if let Err(e) = check_kernel_params(param_path, expect, checker) {
                errors.push(e);
            }
        }

        errors
    }
}

#[cfg(target_os = "linux")]
pub use self::check_kernel::check_kernel;

#[cfg(not(target_os = "linux"))]
pub fn check_kernel() -> Vec<ConfigError> {
    Vec::new()
}

/// `check_addr` validates an address. Addresses are formed like "Host:Port".
/// More details about **Host** and **Port** can be found in WHATWG URL Standard.
pub fn check_addr(addr: &str) -> Result<(), ConfigError> {
    // Try to validate "IPv4:Port" and "[IPv6]:Port".
    if SocketAddrV4::from_str(addr).is_ok() {
        return Ok(());
    }
    if SocketAddrV6::from_str(addr).is_ok() {
        return Ok(());
    }

    let parts: Vec<&str> = addr.split(':')
            .filter(|s| !s.is_empty()) // "Host:" or ":Port" are invalid.
            .collect();

    // ["Host", "Port"]
    if parts.len() != 2 {
        return Err(ConfigError::Address(format!("invalid addr: {:?}", addr)));
    }

    // Check Port.
    let port: u16 = try!(parts[1]
        .parse()
        .map_err(|_| ConfigError::Address(format!("invalid addr, parse port failed: {:?}", addr))));
    // Port = 0 is invalid.
    if port == 0 {
        return Err(ConfigError::Address(format!("invalid addr, port can not be 0: {:?}", addr)));
    }

    // Check Host.
    if let Err(e) = url::Host::parse(parts[0]) {
        return Err(ConfigError::Address(format!("invalid addr: {:?}", e)));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use toml;
    use rocksdb::DBRecoveryMode;

    #[test]
    fn test_parse_readable_size() {
        #[derive(Serialize, Deserialize)]
        struct SizeHolder {
            s: ReadableSize,
        }

        let legal_cases = vec![
            (2 * KB, "2KB"),
            (4 * MB, "4MB"),
            (5 * GB, "5GB"),
            (7 * TB, "7TB"),
            (11 * PB, "11PB"),
        ];
        for (size, exp) in legal_cases {
            let c = SizeHolder { s: ReadableSize(size) };
            let res_str = toml::to_string(&c).unwrap();
            let exp_str = format!("s = {:?}\n", exp);
            assert_eq!(res_str, exp_str);
            let res_size: SizeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_size.s.0, size);
        }

        let c = SizeHolder { s: ReadableSize(512) };
        let res_str = toml::to_string(&c).unwrap();
        assert_eq!(res_str, "s = 512\n");
        let res_size: SizeHolder = toml::from_str(&res_str).unwrap();
        assert_eq!(res_size.s.0, c.s.0);

        let decode_cases = vec![
            (" 0.5 PB", PB / 2),
            ("0.5 TB", TB / 2),
            ("0.5GB ", GB / 2),
            ("0.5MB", MB / 2),
            ("0.5KB", KB / 2),
            ("0.5P", PB / 2),
            ("0.5T", TB / 2),
            ("0.5G", GB / 2),
            ("0.5M", MB / 2),
            ("0.5K", KB / 2),
        ];
        for (src, exp) in decode_cases {
            let src = format!("s = {:?}", src);
            let res: SizeHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.s.0, exp);
        }

        let illegal_cases = vec![
            "0.5kb",
            "0.5kB",
            "0.5Kb",
            "0.5k",
            "0.5g",
            "gb",
            "1b",
        ];
        for src in illegal_cases {
            let src_str = format!("s = {:?}", src);
            assert!(toml::from_str::<SizeHolder>(&src_str).is_err(), "{}", src);
        }
    }

    #[test]
    fn test_parse_readable_duration() {
        #[derive(Serialize, Deserialize)]
        struct DurHolder {
            d: ReadableDuration,
        }

        let legal_cases = vec![
            (0, 1, "1ms"),
            (2, 0, "2s"),
            (4 * 60, 0, "4m"),
            (5 * 3600, 0, "5h"),
            (3600 + 2 * 60, 0, "1h2m"),
            (3600 + 2, 5, "1h2s5ms"),
        ];
        for (secs, ms, exp) in legal_cases {
            let d = DurHolder { d: ReadableDuration(Duration::new(secs, ms * 1_000_000)) };
            let res_str = toml::to_string(&d).unwrap();
            let exp_str = format!("d = {:?}\n", exp);
            assert_eq!(res_str, exp_str);
            let res_dur: DurHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_dur.d.0, d.d.0);
        }

        let decode_cases = vec![
            (" 0.5 h2m ", 3600 / 2 + 2 * 60, 0),
        ];
        for (src, secs, ms) in decode_cases {
            let src = format!("d = {:?}", src);
            let res: DurHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.d.0, Duration::new(secs, ms * 1_000_000));
        }

        let illegal_cases = vec![
            "1H",
            "1M",
            "1S",
            "1MS",
            "1h1h",
            "h",
        ];
        for src in illegal_cases {
            let src_str = format!("d = {:?}", src);
            assert!(toml::from_str::<DurHolder>(&src_str).is_err(), "{}", src);
        }
        assert!(toml::from_str::<DurHolder>("d = 23").is_err());
    }
    #[test]
    fn test_parse_readable_int() {
        // file size
        assert_eq!(1_024, parse_readable_int("1KB").unwrap());
        assert_eq!(1_048_576, parse_readable_int("1MB").unwrap());
        assert_eq!(1_073_741_824, parse_readable_int("1GB").unwrap());
        assert_eq!(1_099_511_627_776, parse_readable_int("1TB").unwrap());
        assert_eq!(1_125_899_906_842_624, parse_readable_int("1PB").unwrap());

        assert_eq!(1_024, parse_readable_int("1kb").unwrap());
        assert_eq!(1_048_576, parse_readable_int("1mb").unwrap());
        assert_eq!(1_073_741_824, parse_readable_int("1gb").unwrap());
        assert_eq!(1_099_511_627_776, parse_readable_int("1tb").unwrap());
        assert_eq!(1_125_899_906_842_624, parse_readable_int("1pb").unwrap());

        assert_eq!(1_536, parse_readable_int("1.5KB").unwrap());
        assert_eq!(1_572_864, parse_readable_int("1.5MB").unwrap());
        assert_eq!(1_610_612_736, parse_readable_int("1.5GB").unwrap());
        assert_eq!(1_649_267_441_664, parse_readable_int("1.5TB").unwrap());
        assert_eq!(1_688_849_860_263_936, parse_readable_int("1.5PB").unwrap());

        assert_eq!(100_663_296, parse_readable_int("96MB").unwrap());

        assert_eq!(1_429_365_116_108, parse_readable_int("1.3TB").unwrap());
        assert_eq!(1_463_669_878_895_411, parse_readable_int("1.3PB").unwrap());

        assert!(parse_readable_int("KB").is_err());
        assert!(parse_readable_int("MB").is_err());
        assert!(parse_readable_int("GB").is_err());
        assert!(parse_readable_int("TB").is_err());
        assert!(parse_readable_int("PB").is_err());

        // time
        assert_eq!(1, parse_readable_int("1ms").unwrap());
        assert_eq!(1_000, parse_readable_int("1s").unwrap());
        assert_eq!(60_000, parse_readable_int("1m").unwrap());
        assert_eq!(3_600_000, parse_readable_int("1h").unwrap());

        assert_eq!(1, parse_readable_int("1.3ms").unwrap());
        assert_eq!(1_000, parse_readable_int("1000ms").unwrap());
        assert_eq!(1_300, parse_readable_int("1.3s").unwrap());
        assert_eq!(1_500, parse_readable_int("1.5s").unwrap());
        assert_eq!(10_000, parse_readable_int("10s").unwrap());
        assert_eq!(78_000, parse_readable_int("1.3m").unwrap());
        assert_eq!(90_000, parse_readable_int("1.5m").unwrap());
        assert_eq!(4_680_000, parse_readable_int("1.3h").unwrap());
        assert_eq!(5_400_000, parse_readable_int("1.5h").unwrap());

        assert!(parse_readable_int("ms").is_err());
        assert!(parse_readable_int("s").is_err());
        assert!(parse_readable_int("m").is_err());
        assert!(parse_readable_int("h").is_err());

        assert!(parse_readable_int("1").is_err());
        assert!(parse_readable_int("foo").is_err());
    }

    #[test]
    fn test_parse_rocksdb_wal_recovery_mode() {
        assert!(DBRecoveryMode::TolerateCorruptedTailRecords ==
                parse_rocksdb_wal_recovery_mode(0).unwrap());
        assert!(DBRecoveryMode::AbsoluteConsistency == parse_rocksdb_wal_recovery_mode(1).unwrap());
        assert!(DBRecoveryMode::PointInTime == parse_rocksdb_wal_recovery_mode(2).unwrap());
        assert!(DBRecoveryMode::SkipAnyCorruptedRecords ==
                parse_rocksdb_wal_recovery_mode(3).unwrap());

        assert!(parse_rocksdb_wal_recovery_mode(4).is_err());
    }

    #[test]
    fn test_parse_compression_type() {
        #[derive(Serialize, Deserialize)]
        struct CompressionTypeHolder {
            #[serde(with = "CompressionType")]
            tp: DBCompressionType,
        }

        let case = vec![
            (DBCompressionType::No, "no"),
            (DBCompressionType::Snappy, "snappy"),
            (DBCompressionType::Zlib, "zlib"),
            (DBCompressionType::Bz2, "bzip2"),
            (DBCompressionType::Lz4, "lz4"),
            (DBCompressionType::Lz4hc, "lz4hc"),
            (DBCompressionType::Zstd, "zstd"),
            (DBCompressionType::ZstdNotFinal, "zstd-not-final"),
            (DBCompressionType::Disable, "disable"),
        ];
        for (tp, exp) in case {
            let holder = CompressionTypeHolder { tp: tp };
            let res = toml::to_string(&holder).unwrap();
            let exp_str = format!("tp = {:?}\n", exp);
            assert_eq!(res, exp_str);
            let h: CompressionTypeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(h.tp, holder.tp);
        }

        assert!(toml::from_str::<CompressionTypeHolder>("tp = \"tp\"").is_err());
    }

    #[test]
    fn test_parse_rocksdb_compaction_pri() {
        assert!(CompactionPriority::ByCompensatedSize == parse_rocksdb_compaction_pri(0).unwrap());
        assert!(CompactionPriority::OldestLargestSeqFirst ==
                parse_rocksdb_compaction_pri(1).unwrap());
        assert!(CompactionPriority::OldestSmallestSeqFirst ==
                parse_rocksdb_compaction_pri(2).unwrap());
        assert!(CompactionPriority::MinOverlappingRatio ==
                parse_rocksdb_compaction_pri(3).unwrap());

        assert!(parse_rocksdb_compaction_pri(4).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_check_kernel() {
        use std::i64;
        use super::check_kernel::{check_kernel_params, Checker};

        // The range of vm.swappiness is from 0 to 100.
        let table: Vec<(&str, i64, Box<Checker>, bool)> = vec![
            ("/proc/sys/vm/swappiness", i64::MAX, Box::new(|got, expect| got == expect), false),

            ("/proc/sys/vm/swappiness", i64::MAX, Box::new(|got, expect| got < expect), true),
        ];

        for (path, expect, checker, is_ok) in table {
            assert_eq!(check_kernel_params(path, expect, checker).is_ok(), is_ok);
        }
    }

    #[test]
    fn test_check_addrs() {
        let table = vec![
            ("127.0.0.1:8080",true),
            ("[::1]:8080",true),
            ("localhost:8080",true),
            ("pingcap.com:8080",true),
            ("funnydomain:8080",true),

            ("127.0.0.1",false),
            ("[::1]",false),
            ("localhost",false),
            ("pingcap.com",false),
            ("funnydomain",false),
            ("funnydomain:",false),

            ("root@google.com:8080",false),
            ("http://google.com:8080",false),
            ("google.com:8080/path",false),
            ("http://google.com:8080/path",false),
            ("http://google.com:8080/path?lang=en",false),
            ("http://google.com:8080/path?lang=en#top",false),

            ("ftp://ftp.is.co.za/rfc/rfc1808.txt",false),
            ("http://www.ietf.org/rfc/rfc2396.txt",false),
            ("ldap://[2001:db8::7]/c=GB?objectClass?one",false),
            ("mailto:John.Doe@example.com",false),
            ("news:comp.infosystems.www.servers.unix",false),
            ("tel:+1-816-555-1212",false),
            ("telnet://192.0.2.16:80/",false),
            ("urn:oasis:names:specification:docbook:dtd:xml:4.1.2",false),

            (":8080",false),
            ("8080",false),
            ("8080:",false),
        ];

        for (addr, is_ok) in table {
            assert_eq!(check_addr(addr).is_ok(), is_ok);
        }
    }

    #[test]
    fn test_store_labels() {
        let cases = vec![
            "abc",
            "abc=",
            "abc.012",
            "abc,012",
            "abc=123*",
            ".123=-abc",
            "abc,123=123.abc",
            "abc=123,abc=abc",
        ];

        for case in cases {
            assert!(parse_store_labels(case).is_err());
        }

        let map = parse_store_labels("").unwrap();
        assert!(map.is_empty());

        let map = parse_store_labels("a=0").unwrap();
        assert_eq!(map.get("a").unwrap(), "0");

        let map = parse_store_labels("a.1-2=b_1.2").unwrap();
        assert_eq!(map.get("a.1-2").unwrap(), "b_1.2");

        let map = parse_store_labels("a.1-2=b_1.2,cab-012=3ac.8b2").unwrap();
        assert_eq!(map.get("a.1-2").unwrap(), "b_1.2");
        assert_eq!(map.get("cab-012").unwrap(), "3ac.8b2");

        let map = parse_store_labels("zone=us-west-1,disk=ssd,Test=Test").unwrap();
        assert_eq!(map.get("zone").unwrap(), "us-west-1");
        assert_eq!(map.get("disk").unwrap(), "ssd");
        assert_eq!(map.get("test").unwrap(), "test");
    }
}
