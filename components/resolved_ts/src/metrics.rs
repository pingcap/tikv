// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref RESOLVED_TS_ADVANCE_METHOD: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_advance_method",
        "Resolved Ts advance method, 0 = advanced through raft command, 1 = advanced through store RPC"
    )
    .unwrap();
    pub static ref RTS_CHANNEL_PENDING_CMD_BYTES: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_channel_penging_cmd_bytes_total",
        "Total bytes of pending commands in the channel"
    )
    .unwrap();
    pub static ref CHECK_LEADER_REQ_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_check_leader_request_size_bytes",
        "Bucketed histogram of the check leader request size",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RESOLVED_TS_GAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_resolved_ts_gap_seconds",
        "Bucketed histogram of the gap between resolved tso and current time",
        exponential_buckets(0.001, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref RTS_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_scan_duration_seconds",
        "Bucketed histogram of resolved-ts async scan duration",
        exponential_buckets(0.005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RTS_SCAN_TASKS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resolved_ts_scan_tasks",
        "Total number of resolved-ts scan tasks",
        &["type"]
    )
    .unwrap();
    pub static ref RTS_MIN_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_resolved_ts_region",
        "The region which has minimal resolved ts"
    )
    .unwrap();
    pub static ref RTS_MIN_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_resolved_ts",
        "The minimal (non-zero) resolved ts for observe regions"
    )
    .unwrap();
    pub static ref RTS_ZERO_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_zero_resolved_ts",
        "The number of zero resolved ts for observe regions"
    )
    .unwrap();
    pub static ref RTS_LOCK_HEAP_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_lock_heap_bytes",
        "Total bytes in memory of resolved-ts observe regions's lock heap"
    )
    .unwrap();
    pub static ref RTS_REGION_RESOLVE_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resolved_ts_region_resolve_status",
        "The status of resolved-ts observe regions",
        &["type"]
    )
    .unwrap();
}
