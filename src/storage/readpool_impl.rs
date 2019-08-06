// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus::local::*;

use crate::config::StorageReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine};
use crate::storage::{FlowStatistics, FlowStatsReporter};
use tikv_util::collections::HashMap;
use tikv_util::future_pool::{Builder, Config, TaskLimitedFuturePool};

use super::metrics::*;
use super::Engine;

pub struct StorageLocalMetrics {
    local_sched_histogram_vec: LocalHistogramVec,
    local_sched_processing_read_histogram_vec: LocalHistogramVec,
    local_kv_command_keyread_histogram_vec: LocalHistogramVec,
    local_kv_command_counter_vec: LocalIntCounterVec,
    local_sched_commands_pri_counter_vec: LocalIntCounterVec,
    local_kv_command_scan_details: LocalIntCounterVec,
    local_read_flow_stats: HashMap<u64, FlowStatistics>,
}

thread_local! {
    static TLS_STORAGE_METRICS: RefCell<StorageLocalMetrics> = RefCell::new(
        StorageLocalMetrics {
            local_sched_histogram_vec: SCHED_HISTOGRAM_VEC.local(),
            local_sched_processing_read_histogram_vec: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            local_kv_command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            local_kv_command_counter_vec: KV_COMMAND_COUNTER_VEC.local(),
            local_sched_commands_pri_counter_vec: SCHED_COMMANDS_PRI_COUNTER_VEC.local(),
            local_kv_command_scan_details: KV_COMMAND_SCAN_DETAILS.local(),
            local_read_flow_stats: HashMap::default(),
        }
    );
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<TaskLimitedFuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let reporter2 = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .name_prefix(name)
                .on_tick(move || tls_flush(&reporter))
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(move || {
                    destroy_tls_engine::<E>();
                    tls_flush(&reporter2)
                })
                .build_with_task_limit()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &StorageReadPoolConfig,
    engine: E,
) -> Vec<TaskLimitedFuturePool> {
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .map(|config| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(|| destroy_tls_engine::<E>())
                .build_with_task_limit()
        })
        .collect()
}

#[inline]
fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut storage_metrics = m.borrow_mut();
        // Flush Prometheus metrics
        storage_metrics.local_sched_histogram_vec.flush();
        storage_metrics
            .local_sched_processing_read_histogram_vec
            .flush();
        storage_metrics
            .local_kv_command_keyread_histogram_vec
            .flush();
        storage_metrics.local_kv_command_counter_vec.flush();
        storage_metrics.local_sched_commands_pri_counter_vec.flush();
        storage_metrics.local_kv_command_scan_details.flush();

        // Report PD metrics
        if storage_metrics.local_read_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut storage_metrics.local_read_flow_stats);

        reporter.report_read_stats(read_stats);
    });
}

#[inline]
pub fn tls_collect_command_count(cmd: &str, priority: CommandPriority) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut storage_metrics = m.borrow_mut();
        storage_metrics
            .local_kv_command_counter_vec
            .with_label_values(&[cmd])
            .inc();
        storage_metrics
            .local_sched_commands_pri_counter_vec
            .with_label_values(&[priority.get_str()])
            .inc();
    });
}

#[inline]
pub fn tls_collect_command_duration(cmd: &str, duration: Duration) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_sched_histogram_vec
            .with_label_values(&[cmd])
            .observe(tikv_util::time::duration_to_sec(duration))
    });
}

#[inline]
pub fn tls_collect_key_reads(cmd: &str, count: usize) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_kv_command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count as f64)
    });
}

#[inline]
pub fn tls_processing_read_observe_duration<F, R>(cmd: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    TLS_STORAGE_METRICS.with(|m| {
        let now = tikv_util::time::Instant::now_coarse();
        let ret = f();
        m.borrow_mut()
            .local_sched_processing_read_histogram_vec
            .with_label_values(&[cmd])
            .observe(now.elapsed_secs());
        ret
    })
}

#[inline]
pub fn tls_collect_scan_count(cmd: &str, statistics: &crate::storage::Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        let histogram = &mut m.borrow_mut().local_kv_command_scan_details;
        for (cf, details) in statistics.details() {
            for (tag, count) in details {
                histogram
                    .with_label_values(&[cmd, cf, tag])
                    .inc_by(count as i64);
            }
        }
    });
}

#[inline]
pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_read_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}
