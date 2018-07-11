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

use std::panic::{RefUnwindSafe, UnwindSafe};

use grpc;
use log::SetLoggerError;
use slog::{self, Drain};
use slog_scope;
use slog_stdlog;

pub use slog::Level;

const ENABLED_TARGETS: &[&str] = &[
    "tikv::",
    "tests::",
    "benches::",
    "integrations::",
    "failpoints::",
    "raft::",
];

pub fn init_log<D>(drain: D, level: Level) -> Result<(), SetLoggerError>
where
    D: Drain + Send + Sync + 'static + RefUnwindSafe + UnwindSafe,
    <D as slog::Drain>::Err: ::std::fmt::Debug,
{
    grpc::redirect_log();

    let drain = drain.filter_level(level).fuse();

    let logger = slog::Logger::root(drain, slog_o!());

    slog_scope::set_global_logger(logger).cancel_reset();
    slog_stdlog::init()
}

pub fn init_log_for_tikv_only<D>(drain: D, level: Level) -> Result<(), SetLoggerError>
where
    D: Drain + Send + Sync + 'static + RefUnwindSafe + UnwindSafe,
    <D as slog::Drain>::Err: ::std::fmt::Debug,
{
    let filtered = drain.filter(|record| {
        ENABLED_TARGETS
            .iter()
            .any(|target| record.module().starts_with(target))
    });
    init_log(filtered, level)
}

pub fn get_level_by_string(lv: &str) -> Option<Level> {
    match &*lv.to_owned().to_lowercase() {
        "critical" => Some(Level::Critical),
        "error" => Some(Level::Error),
        // We support `warn` due to legacy.
        "warning" | "warn" => Some(Level::Warning),
        "debug" => Some(Level::Debug),
        "trace" => Some(Level::Trace),
        "info" => Some(Level::Info),
        _ => None,
    }
}

// The `to_string()` function of `slog::Level` produces values like `erro` and `trce` instead of
// the full words. This produces the full word.
pub fn get_string_by_level(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warning",
        Level::Debug => "debug",
        Level::Trace => "trace",
        Level::Info => "info",
    }
}

#[test]
fn test_get_level_by_string() {
    // Ensure UPPER, Capitalized, and lower case all map over.
    assert_eq!(Some(Level::Trace), get_level_by_string("TRACE"));
    assert_eq!(Some(Level::Trace), get_level_by_string("Trace"));
    assert_eq!(Some(Level::Trace), get_level_by_string("trace"));
    // Due to legacy we need to ensure that `warn` maps to `Warning`.
    assert_eq!(Some(Level::Warning), get_level_by_string("warn"));
    assert_eq!(Some(Level::Warning), get_level_by_string("warning"));
    // Ensure that all non-defined values map to `Info`.
    assert_eq!(None, get_level_by_string("Off"));
    assert_eq!(None, get_level_by_string("definitely not an option"));
}
