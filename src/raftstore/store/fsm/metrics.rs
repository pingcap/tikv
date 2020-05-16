// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{exponential_buckets, Histogram, IntCounter, IntCounterVec};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref APPLY_PROPOSAL: Histogram = register_histogram!(
        "tikv_raftstore_apply_proposal",
        "The count of proposals sent by a region at once",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref APPLY_YIELD_COUNT: IntCounter = register_int_counter!(
        "tikv_raftstore_apply_yield_count",
        "The count of yield fsms"
    )
    .unwrap();
    pub static ref APPLY_RESCHEDULE_WAIT_DURATION: Histogram = register_histogram!(
        "tikv_raftstore_apply_reschedule_wait_duration_secs",
        "The interval of a fsm was rescheduled",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref APPLY_EXECUTE_DURATION: Histogram = register_histogram!(
        "tikv_raftstore_apply_execute_wait_duration_secs",
        "The interval of a fsm was handled",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RESCHEDULE_FSM_COUNT: IntCounterVec = register_int_counter_vec!(
        "batch_system_reschedule_count",
        "The count of rescheduled fsms",
        &["tag"]
    )
    .unwrap();
}

#[derive(Default)]
pub struct StoreStat {
    pub lock_cf_bytes_written: AtomicU64,
    pub engine_total_bytes_written: AtomicU64,
    pub engine_total_keys_written: AtomicU64,
    pub is_busy: AtomicBool,
}

#[derive(Clone, Default)]
pub struct GlobalStoreStat {
    pub stat: Arc<StoreStat>,
}

impl GlobalStoreStat {
    #[inline]
    pub fn local(&self) -> LocalStoreStat {
        LocalStoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,
            is_busy: false,

            global: self.clone(),
        }
    }
}

pub struct LocalStoreStat {
    pub lock_cf_bytes_written: u64,
    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,
    pub is_busy: bool,

    global: GlobalStoreStat,
}

impl Clone for LocalStoreStat {
    #[inline]
    fn clone(&self) -> LocalStoreStat {
        self.global.local()
    }
}

impl LocalStoreStat {
    pub fn flush(&mut self) {
        if self.lock_cf_bytes_written != 0 {
            self.global
                .stat
                .lock_cf_bytes_written
                .fetch_add(self.lock_cf_bytes_written, Ordering::Relaxed);
            self.lock_cf_bytes_written = 0;
        }
        if self.engine_total_bytes_written != 0 {
            self.global
                .stat
                .engine_total_bytes_written
                .fetch_add(self.engine_total_bytes_written, Ordering::Relaxed);
            self.engine_total_bytes_written = 0;
        }
        if self.engine_total_keys_written != 0 {
            self.global
                .stat
                .engine_total_keys_written
                .fetch_add(self.engine_total_keys_written, Ordering::Relaxed);
            self.engine_total_keys_written = 0;
        }
        if self.is_busy {
            self.global.stat.is_busy.store(true, Ordering::Relaxed);
            self.is_busy = false;
        }
    }
}
