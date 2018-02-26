// Copyright 2018 PingCAP, Inc.
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

use std::fmt;
use std::mem;
use prometheus::local::{LocalCounterVec, LocalHistogramTimer, LocalHistogramVec};

use server::readpool;
use util::futurepool;
use util::worker;
use util::collections::HashMap;
use pd;
use storage;

use super::metrics::*;

pub struct Context {
    pd_sender: Option<worker::FutureScheduler<pd::PdTask>>,

    command_duration: LocalHistogramVec,
    processing_read_duration: LocalHistogramVec,
    command_keyreads: LocalHistogramVec,
    // TODO: kv_command_counter, raw_command_counter, command_pri_counter can be merged together
    kv_command_counter: LocalCounterVec,
    raw_command_counter: LocalCounterVec,
    command_pri_counter: LocalCounterVec,
    scan_details: LocalCounterVec,

    read_flow_stats: HashMap<u64, storage::FlowStatistics>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context").finish()
    }
}

impl Context {
    pub fn new(pd_sender: Option<worker::FutureScheduler<pd::PdTask>>) -> Self {
        Context {
            pd_sender,
            command_duration: SCHED_HISTOGRAM_VEC.local(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            command_keyreads: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            kv_command_counter: KV_COMMAND_COUNTER_VEC.local(),
            raw_command_counter: RAWKV_COMMAND_COUNTER_VEC.local(),
            command_pri_counter: SCHED_COMMANDS_PRI_COUNTER_VEC.local(),
            scan_details: KV_COMMAND_SCAN_DETAILS.local(),
            read_flow_stats: HashMap::default(),
        }
    }

    #[inline]
    pub fn collect_command_duration(&mut self, cmd: &str) -> LocalHistogramTimer {
        self.command_duration
            .with_label_values(&[cmd])
            .start_coarse_timer()
    }

    #[inline]
    pub fn collect_processing_read_duration(&mut self, cmd: &str) -> LocalHistogramTimer {
        self.processing_read_duration
            .with_label_values(&[cmd])
            .start_coarse_timer()
    }

    #[inline]
    pub fn collect_command_count(&mut self, cmd: &str, priority: readpool::Priority, is_raw: bool) {
        if is_raw {
            self.raw_command_counter.with_label_values(&[cmd]).inc();
        } else {
            self.kv_command_counter.with_label_values(&[cmd]).inc();
        }
        self.command_pri_counter
            .with_label_values(&[&priority.to_string()])
            .inc();
    }

    #[inline]
    pub fn collect_key_reads(&mut self, cmd: &str, count: u64) {
        self.command_keyreads
            .with_label_values(&[cmd])
            .observe(count as f64);
    }

    #[inline]
    pub fn collect_scan_count(&mut self, cmd: &str, statistics: &storage::Statistics) {
        for (cf, details) in statistics.details() {
            for (tag, count) in details {
                self.scan_details
                    .with_label_values(&[cmd, cf, tag])
                    .inc_by(count as f64)
                    .unwrap();
            }
        }
    }

    #[inline]
    pub fn collect_read_flow(&mut self, region_id: u64, statistics: &storage::Statistics) {
        if self.pd_sender.is_none() {
            return;
        }
        let flow_stats = self.read_flow_stats
            .entry(region_id)
            .or_insert_with(storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    }
}

impl futurepool::Context for Context {
    fn on_tick(&mut self) {
        // Flush Prometheus metrics
        self.command_duration.flush();
        self.processing_read_duration.flush();
        self.command_keyreads.flush();
        self.kv_command_counter.flush();
        self.raw_command_counter.flush();
        self.command_pri_counter.flush();
        self.scan_details.flush();

        // Report PD metrics
        if !self.read_flow_stats.is_empty() {
            if let Some(ref sender) = self.pd_sender {
                let mut read_stats = HashMap::default();
                mem::swap(&mut read_stats, &mut self.read_flow_stats);
                let result = sender.schedule(pd::PdTask::ReadStats { read_stats });
                if let Err(e) = result {
                    error!("Failed to send readpool read flow statistics: {:?}", e);
                }
            }
        }
    }
}
