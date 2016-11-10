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

use rocksdb::{DBCompressionType, DBRecoveryMode};

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        RocksDB
        ReadableNumber
        Limits(msg: String) {
            description(&msg)
            display("{}", msg)
        }
    }
}

pub fn parse_rocksdb_compression(tp: &str) -> Result<DBCompressionType, ConfigError> {
    match &*tp.to_lowercase() {
        "no" => Ok(DBCompressionType::DBNo),
        "snappy" => Ok(DBCompressionType::DBSnappy),
        "zlib" => Ok(DBCompressionType::DBZlib),
        "bzip2" => Ok(DBCompressionType::DBBz2),
        "lz4" => Ok(DBCompressionType::DBLz4),
        "lz4hc" => Ok(DBCompressionType::DBLz4hc),
        _ => Err(ConfigError::RocksDB),
    }
}

pub fn parse_rocksdb_per_level_compression(tp: &str)
                                           -> Result<Vec<DBCompressionType>, ConfigError> {
    let mut result: Vec<DBCompressionType> = vec![];
    let v: Vec<&str> = tp.split(':').collect();
    for i in &v {
        match &*i.to_lowercase() {
            "no" => result.push(DBCompressionType::DBNo),
            "snappy" => result.push(DBCompressionType::DBSnappy),
            "zlib" => result.push(DBCompressionType::DBZlib),
            "bzip2" => result.push(DBCompressionType::DBBz2),
            "lz4" => result.push(DBCompressionType::DBLz4),
            "lz4hc" => result.push(DBCompressionType::DBLz4hc),
            _ => return Err(ConfigError::RocksDB),
        }
    }

    Ok(result)
}

pub fn parse_rocksdb_wal_recovery_mode(mode: i64) -> Result<DBRecoveryMode, ConfigError> {
    match mode {
        0 => Ok(DBRecoveryMode::TolerateCorruptedTailRecords),
        1 => Ok(DBRecoveryMode::AbsoluteConsistency),
        2 => Ok(DBRecoveryMode::PointInTime),
        3 => Ok(DBRecoveryMode::SkipAnyCorruptedRecords),
        _ => Err(ConfigError::RocksDB),
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
    num.parse::<f64>().map(|f| (f, unit)).or(Err(ConfigError::ReadableNumber))
}

const UNIT: usize = 1;
const DATA_MAGNITUDE: usize = 1024;
const KB: usize = UNIT * DATA_MAGNITUDE;
const MB: usize = KB * DATA_MAGNITUDE;
const GB: usize = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: usize = 1000;
const TIME_MAGNITUDE_2: usize = 60;
const MS: usize = UNIT;
const SECOND: usize = MS * TIME_MAGNITUDE_1;
const MINTUE: usize = SECOND * TIME_MAGNITUDE_2;
const HOUR: usize = MINTUE * TIME_MAGNITUDE_2;

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
        "m" => Ok((num * (MINTUE as f64)) as i64),
        "h" => Ok((num * (HOUR as f64)) as i64),

        _ => Err(ConfigError::ReadableNumber),
    }
}

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    use libc;

    let fd_limit = unsafe {
        let mut fd_limit = ::std::mem::zeroed();
        if 0 != libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit) {
            return Err(ConfigError::Limits("check_max_open_fds failed".to_owned()));
        }

        fd_limit
    };

    if fd_limit.rlim_cur < expect {
        return Err(ConfigError::Limits(format!("the maximum number of open file descriptors is \
                                                too small, got {}, expect greater or equal to \
                                                {}",
                                               fd_limit.rlim_cur,
                                               expect)));
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    Ok(())
}

#[cfg(target_os = "linux")]
mod check_kernel {
    use std::fs;
    use std::io::Read;

    use super::ConfigError;

    type Checker = Fn(i64, i64) -> bool;

    // pub for tests.
    pub fn check_kernel_params(param_path: &str,
                               expect: i64,
                               checker: Box<Checker>)
                               -> Result<(), ConfigError> {
        let mut buffer = String::new();
        try!(fs::File::open(param_path)
            .and_then(|mut f| f.read_to_string(&mut buffer))
            .map_err(|e| ConfigError::Limits(format!("check_kernel_params failed {}", e))));

        let got = try!(buffer.trim_matches('\n')
            .parse::<i64>()
            .map_err(|e| ConfigError::Limits(format!("check_kernel_params failed {}", e))));

        if !checker(got, expect) {
            let mut param = String::new();

            // 3: ["", "proc", "sys", ...]
            for path in param_path.split('/').skip(3) {
                param.push_str(path);
                param.push('.');
            }
            param.pop();

            return Err(ConfigError::Limits(format!("{} got {}, expect {}", param, got, expect)));
        }

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

#[cfg(test)]
mod test {
    use super::*;
    use rocksdb::DBRecoveryMode;

    #[test]
    fn test_parse_readable_int() {
        // file size
        assert!(1_024 == parse_readable_int("1KB").unwrap());
        assert!(1_048_576 == parse_readable_int("1MB").unwrap());
        assert!(1_073_741_824 == parse_readable_int("1GB").unwrap());
        assert!(1_099_511_627_776 == parse_readable_int("1TB").unwrap());
        assert!(1_125_899_906_842_624 == parse_readable_int("1PB").unwrap());

        assert!(1_024 == parse_readable_int("1kb").unwrap());
        assert!(1_048_576 == parse_readable_int("1mb").unwrap());
        assert!(1_073_741_824 == parse_readable_int("1gb").unwrap());
        assert!(1_099_511_627_776 == parse_readable_int("1tb").unwrap());
        assert!(1_125_899_906_842_624 == parse_readable_int("1pb").unwrap());

        assert!(1_536 == parse_readable_int("1.5KB").unwrap());
        assert!(1_572_864 == parse_readable_int("1.5MB").unwrap());
        assert!(1_610_612_736 == parse_readable_int("1.5GB").unwrap());
        assert!(1_649_267_441_664 == parse_readable_int("1.5TB").unwrap());
        assert!(1_688_849_860_263_936 == parse_readable_int("1.5PB").unwrap());

        assert!(100_663_296 == parse_readable_int("96MB").unwrap());

        assert!(1_429_365_116_108 == parse_readable_int("1.3TB").unwrap());
        assert!(1_463_669_878_895_411 == parse_readable_int("1.3PB").unwrap());

        assert!(parse_readable_int("KB").is_err());
        assert!(parse_readable_int("MB").is_err());
        assert!(parse_readable_int("GB").is_err());
        assert!(parse_readable_int("TB").is_err());
        assert!(parse_readable_int("PB").is_err());

        // time
        assert!(1 == parse_readable_int("1ms").unwrap());
        assert!(1_000 == parse_readable_int("1s").unwrap());
        assert!(60_000 == parse_readable_int("1m").unwrap());
        assert!(3_600_000 == parse_readable_int("1h").unwrap());

        assert!(1 == parse_readable_int("1.3ms").unwrap());
        assert!(1_000 == parse_readable_int("1000ms").unwrap());
        assert!(1_300 == parse_readable_int("1.3s").unwrap());
        assert!(1_500 == parse_readable_int("1.5s").unwrap());
        assert!(10_000 == parse_readable_int("10s").unwrap());
        assert!(78_000 == parse_readable_int("1.3m").unwrap());
        assert!(90_000 == parse_readable_int("1.5m").unwrap());
        assert!(4_680_000 == parse_readable_int("1.3h").unwrap());
        assert!(5_400_000 == parse_readable_int("1.5h").unwrap());

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

    #[cfg(target_os = "linux")]
    #[test]
    fn test_check_kernel() {
        use std::i64;

        // The range of vm.swappiness is from 0 to 100.
        assert_eq!(super::check_kernel::check_kernel_params("/proc/sys/vm/swappiness",
                                                            i64::MAX,
                                                            box |got, expect| got == expect)
                       .is_ok(),
                   false);
        assert_eq!(super::check_kernel::check_kernel_params("/proc/sys/vm/swappiness",
                                                            i64::MAX,
                                                            box |got, expect| got < expect)
                       .is_ok(),
                   true);
    }
}
