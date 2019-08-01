// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::sync::{Arc, Mutex};

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, Config, ReadPool};
use crate::storage::kv::{destroy_tls_engine, set_tls_engine};
use crate::storage::{Engine, FlowStatistics, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::worker::FutureScheduler;

use super::metrics::*;
use prometheus::local::*;

pub struct CopLocalMetrics {
    pub local_copr_req_histogram_vec: LocalHistogramVec,
    pub local_copr_req_handle_time: LocalHistogramVec,
    pub local_copr_req_wait_time: LocalHistogramVec,
    pub local_copr_req_error: LocalIntCounterVec,
    pub local_copr_scan_keys: LocalHistogramVec,
    pub local_copr_scan_details: LocalIntCounterVec,
    pub local_copr_rocksdb_perf_counter: LocalIntCounterVec,
    local_cop_flow_stats: HashMap<u64, FlowStatistics>,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_copr_req_histogram_vec:
                COPR_REQ_HISTOGRAM_VEC.local(),
            local_copr_req_handle_time:
                COPR_REQ_HANDLE_TIME.local(),
            local_copr_req_wait_time:
                COPR_REQ_WAIT_TIME.local(),
            local_copr_req_error:
                COPR_REQ_ERROR.local(),
            local_copr_scan_keys:
                COPR_SCAN_KEYS.local(),
            local_copr_scan_details:
                COPR_SCAN_DETAILS.local(),
            local_copr_rocksdb_perf_counter:
                COPR_ROCKSDB_PERF_COUNTER.local(),
            local_cop_flow_stats:
                HashMap::default(),
        }
    );
}

pub fn build_read_pool<E: Engine>(
    config: &readpool::Config,
    pd_sender: FutureScheduler<PdTask>,
    engine: E,
) -> ReadPool {
    let pd_sender2 = pd_sender.clone();
    let engine = Arc::new(Mutex::new(engine));

    Builder::from_config(config)
        .name_prefix("cop")
        .on_tick(move || tls_flush(&pd_sender))
        .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
        .before_stop(move || {
            destroy_tls_engine::<E>();
            tls_flush(&pd_sender2)
        })
        .build()
}

pub fn build_read_pool_for_test<E: Engine>(engine: E) -> ReadPool {
    let engine = Arc::new(Mutex::new(engine));

    Builder::from_config(&Config::default_for_test())
        .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
        .before_stop(|| destroy_tls_engine::<E>())
        .build()
}

#[inline]
fn tls_flush(pd_sender: &FutureScheduler<PdTask>) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut cop_metrics = m.borrow_mut();
        cop_metrics.local_copr_req_histogram_vec.flush();
        cop_metrics.local_copr_req_handle_time.flush();
        cop_metrics.local_copr_req_wait_time.flush();
        cop_metrics.local_copr_scan_keys.flush();
        cop_metrics.local_copr_rocksdb_perf_counter.flush();
        cop_metrics.local_copr_scan_details.flush();

        // Report PD metrics
        if cop_metrics.local_cop_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut cop_metrics.local_cop_flow_stats);

        let result = pd_sender.schedule(PdTask::ReadStats { read_stats });
        if let Err(e) = result {
            error!("Failed to send cop pool read flow statistics"; "err" => ?e);
        }
    });
}

pub fn tls_collect_cf_stats(region_id: u64, type_str: &str, stats: &Statistics) {
    // cf statistics group by type
    for (cf, details) in stats.details() {
        for (tag, count) in details {
            TLS_COP_METRICS.with(|m| {
                m.borrow_mut()
                    .local_copr_scan_details
                    .with_label_values(&[type_str, cf, tag])
                    .inc_by(count as i64);
            });
        }
    }
    // flow statistics group by region
    tls_collect_read_flow(region_id, stats);
}

#[inline]
pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
    TLS_COP_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_cop_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}
