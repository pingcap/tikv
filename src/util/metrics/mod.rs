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

use std::thread;
use std::time::Duration;

use prometheus::{self, *};

#[cfg(target_os = "linux")]
mod threads_linux;
#[cfg(target_os = "linux")]
pub use self::threads_linux::monitor_threads;

#[cfg(target_os = "linux")]
mod memory_linux;
#[cfg(target_os = "linux")]
pub use self::memory_linux::monitor_memory;

#[cfg(not(target_os = "linux"))]
mod threads_dummy;
#[cfg(not(target_os = "linux"))]
pub use self::threads_dummy::monitor_threads;

<<<<<<< HEAD:src/util/metrics/mod.rs
/// `run_prometheus` runs a background prometheus client.
=======
#[cfg(not(target_os = "linux"))]
mod memory_dummy;
#[cfg(not(target_os = "linux"))]
pub use self::memory_dummy::monitor_memory;

pub use self::allocator_metrics::monitor_allocator_stats;

pub mod allocator_metrics;

pub use self::metrics_reader::HistogramReader;

mod metrics_reader;

/// Runs a background Prometheus client.
>>>>>>> 7d13ca0... *: reduce sys_getdents syscall (#7306):components/tikv_util/src/metrics/mod.rs
pub fn run_prometheus(
    interval: Duration,
    address: &str,
    job: &str,
) -> Option<thread::JoinHandle<()>> {
    if interval == Duration::from_secs(0) {
        return None;
    }

    let job = job.to_owned();
    let address = address.to_owned();
    let handler = thread::Builder::new()
        .name("promepusher".to_owned())
        .spawn(move || loop {
            let metric_familys = prometheus::gather();

            let res = prometheus::push_metrics(
                &job,
                prometheus::hostname_grouping_key(),
                &address,
                metric_familys,
            );
            if let Err(e) = res {
                error!("fail to push metrics: {}", e);
            }

            thread::sleep(interval);
        })
        .unwrap();

    Some(handler)
}

pub fn dump() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_familys = prometheus::gather();
    for mf in metric_familys {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("prometheus encoding error: {:?}", e);
        }
    }
    String::from_utf8(buffer).unwrap()
}

lazy_static! {
    pub static ref CRITICAL_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_critical_error_total",
        "Counter of critical error.",
        &["type"]
    ).unwrap();
}
