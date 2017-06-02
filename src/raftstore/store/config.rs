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

use std::u64;
use std::time::Duration;

use time::Duration as TimeDuration;

use raftstore::Result;

const RAFT_BASE_TICK_INTERVAL: u64 = 1000;
const RAFT_HEARTBEAT_TICKS: usize = 2;
const RAFT_ELECTION_TIMEOUT_TICKS: usize = 10;
const RAFT_MAX_SIZE_PER_MSG: u64 = 1024 * 1024;
const RAFT_MAX_INFLIGHT_MSGS: usize = 256;
const RAFT_ENTRY_MAX_SIZE: u64 = 8 * 1024 * 1024;
const RAFT_LOG_GC_INTERVAL: u64 = 10000;
const RAFT_LOG_GC_THRESHOLD: u64 = 50;
// Assume the average size of entries is 1k.
const RAFT_LOG_GC_COUNT_LIMIT: u64 = REGION_SPLIT_SIZE * 3 / 4 / 1024;
const RAFT_LOG_GC_SIZE_LIMIT: u64 = REGION_SPLIT_SIZE * 3 / 4;
const SPLIT_REGION_CHECK_TICK_INTERVAL: u64 = 10000;

pub const REGION_SPLIT_SIZE: u64 = 96 * 1024 * 1024;
pub const REGION_MAX_SIZE: u64 = REGION_SPLIT_SIZE / 2 * 3;
pub const REGION_CHECK_DIFF: u64 = REGION_SPLIT_SIZE / 8;

const REGION_COMPACT_CHECK_TICK_INTERVAL: u64 = 0; // disable manual compaction by default.
const REGION_COMPACT_DELETE_KEYS_COUNT: u64 = 1_000_000;
const PD_HEARTBEAT_TICK_INTERVAL: u64 = 60000;
const PD_STORE_HEARTBEAT_TICK_INTERVAL: u64 = 10000;
const STORE_CAPACITY: u64 = u64::MAX;
const DEFAULT_NOTIFY_CAPACITY: usize = 40960;
const DEFAULT_MGR_GC_TICK_INTERVAL: u64 = 60000;
const DEFAULT_SNAP_GC_TIMEOUT_SECS: u64 = 4 * 60 * 60; // 4 hours
const DEFAULT_MESSAGES_PER_TICK: usize = 4096;
const DEFAULT_MAX_PEER_DOWN_SECS: u64 = 300;
const DEFAULT_LOCK_CF_COMPACT_INTERVAL: u64 = 10 * 60 * 1000; // 10 min
const DEFAULT_LOCK_CF_COMPACT_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB
// If the leader missing for over 2 hours,
// a peer should consider itself as a stale peer that is out of region.
const DEFAULT_MAX_LEADER_MISSING_SECS: u64 = 2 * 60 * 60;
const DEFAULT_SNAPSHOT_APPLY_BATCH_SIZE: usize = 1024 * 1024 * 10; // 10m
// Disable consistency check by default as it will hurt performance.
// We should turn on this only in our tests.
const DEFAULT_CONSISTENCY_CHECK_INTERVAL: u64 = 0;

const DEFAULT_REPORT_REGION_FLOW_INTERVAL: u64 = 60000; // 60 seconds

const DEFAULT_RAFT_STORE_LEASE_SEC: i64 = 9; // 9 seconds

const DEFAULT_USE_SST_FILE_SNAPSHOT: bool = false;

#[derive(Debug, Clone)]
pub struct Config {
    // true for high reliability, prevent data loss when power failure.
    pub sync_log: bool,

    // store capacity.
    // TODO: if not set, we will use disk capacity instead.
    // Now we will use a default capacity if not set.
    pub capacity: u64,

    // raft_base_tick_interval is a base tick interval (ms).
    pub raft_base_tick_interval: u64,
    pub raft_heartbeat_ticks: usize,
    pub raft_election_timeout_ticks: usize,
    pub raft_max_size_per_msg: u64,
    pub raft_max_inflight_msgs: usize,
    // When the entry exceed the max size, reject to propose it.
    pub raft_entry_max_size: u64,

    // Interval to gc unnecessary raft log (ms).
    pub raft_log_gc_tick_interval: u64,
    // A threshold to gc stale raft log, must >= 1.
    pub raft_log_gc_threshold: u64,
    // When entry count exceed this value, gc will be forced trigger.
    pub raft_log_gc_count_limit: u64,
    // When the approximate size of raft log entries exceed this value,
    // gc will be forced trigger.
    pub raft_log_gc_size_limit: u64,

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: u64,
    /// When region [a, b) size meets region_max_size, it will be split
    /// into two region into [a, c), [c, b). And the size of [a, c) will
    /// be region_split_size (or a little bit smaller).
    pub region_max_size: u64,
    pub region_split_size: u64,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub region_check_size_diff: u64,
    /// Interval (ms) to check whether start compaction for a region.
    pub region_compact_check_interval: u64,
    /// When delete keys of a region exceeds the size, a compaction will
    /// be started.
    pub region_compact_delete_keys_count: u64,
    pub pd_heartbeat_tick_interval: u64,
    pub pd_store_heartbeat_tick_interval: u64,
    pub snap_mgr_gc_tick_interval: u64,
    pub snap_gc_timeout: u64,
    pub lock_cf_compact_interval: u64,
    pub lock_cf_compact_threshold: u64,

    pub notify_capacity: usize,
    pub messages_per_tick: usize,

    /// When a peer is not active for max_peer_down_duration,
    /// the peer is considered to be down and is reported to PD.
    pub max_peer_down_duration: Duration,

    /// If the leader of a peer is missing for longer than max_leader_missing_duration,
    /// the peer would ask pd to confirm whether it is valid in any region.
    /// If the peer is stale and is not valid in any region, it will destroy itself.
    pub max_leader_missing_duration: Duration,

    pub snap_apply_batch_size: usize,

    // Interval (ms) to check region whether the data is consistent.
    pub consistency_check_tick_interval: u64,

    pub report_region_flow_interval: u64,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: TimeDuration,

    pub use_sst_file_snapshot: bool,

    // Right region derive origin region id when split.
    pub right_derive_when_split: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            sync_log: true,
            capacity: STORE_CAPACITY,
            raft_base_tick_interval: RAFT_BASE_TICK_INTERVAL,
            raft_heartbeat_ticks: RAFT_HEARTBEAT_TICKS,
            raft_election_timeout_ticks: RAFT_ELECTION_TIMEOUT_TICKS,
            raft_max_size_per_msg: RAFT_MAX_SIZE_PER_MSG,
            raft_max_inflight_msgs: RAFT_MAX_INFLIGHT_MSGS,
            raft_entry_max_size: RAFT_ENTRY_MAX_SIZE,
            raft_log_gc_tick_interval: RAFT_LOG_GC_INTERVAL,
            raft_log_gc_threshold: RAFT_LOG_GC_THRESHOLD,
            raft_log_gc_count_limit: RAFT_LOG_GC_COUNT_LIMIT,
            raft_log_gc_size_limit: RAFT_LOG_GC_SIZE_LIMIT,
            split_region_check_tick_interval: SPLIT_REGION_CHECK_TICK_INTERVAL,
            region_max_size: REGION_MAX_SIZE,
            region_split_size: REGION_SPLIT_SIZE,
            region_check_size_diff: REGION_CHECK_DIFF,
            region_compact_check_interval: REGION_COMPACT_CHECK_TICK_INTERVAL,
            region_compact_delete_keys_count: REGION_COMPACT_DELETE_KEYS_COUNT,
            pd_heartbeat_tick_interval: PD_HEARTBEAT_TICK_INTERVAL,
            pd_store_heartbeat_tick_interval: PD_STORE_HEARTBEAT_TICK_INTERVAL,
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
            snap_mgr_gc_tick_interval: DEFAULT_MGR_GC_TICK_INTERVAL,
            snap_gc_timeout: DEFAULT_SNAP_GC_TIMEOUT_SECS,
            messages_per_tick: DEFAULT_MESSAGES_PER_TICK,
            max_peer_down_duration: Duration::from_secs(DEFAULT_MAX_PEER_DOWN_SECS),
            max_leader_missing_duration: Duration::from_secs(DEFAULT_MAX_LEADER_MISSING_SECS),
            snap_apply_batch_size: DEFAULT_SNAPSHOT_APPLY_BATCH_SIZE,
            lock_cf_compact_interval: DEFAULT_LOCK_CF_COMPACT_INTERVAL,
            lock_cf_compact_threshold: DEFAULT_LOCK_CF_COMPACT_THRESHOLD,
            consistency_check_tick_interval: DEFAULT_CONSISTENCY_CHECK_INTERVAL,
            report_region_flow_interval: DEFAULT_REPORT_REGION_FLOW_INTERVAL,
            raft_store_max_leader_lease: TimeDuration::seconds(DEFAULT_RAFT_STORE_LEASE_SEC),
            use_sst_file_snapshot: DEFAULT_USE_SST_FILE_SNAPSHOT,
            right_derive_when_split: true,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        if self.raft_heartbeat_ticks == 0 {
            return Err(box_err!("heartbeat tick must greater than 0"));
        }

        if self.raft_election_timeout_ticks <= self.raft_heartbeat_ticks {
            return Err(box_err!("election tick must be greater than heartbeat tick"));
        }

        if self.raft_log_gc_threshold < 1 {
            return Err(box_err!("raft log gc threshold must >= 1, not {}",
                                self.raft_log_gc_threshold));
        }

        if self.raft_log_gc_size_limit == 0 {
            return Err(box_err!("raft log gc size limit should large than 0."));
        }

        if self.region_max_size < self.region_split_size {
            return Err(box_err!("region max size {} must >= split size {}",
                                self.region_max_size,
                                self.region_split_size));
        }

        let election_timeout = self.raft_base_tick_interval *
                               self.raft_election_timeout_ticks as u64;
        let lease = self.raft_store_max_leader_lease.num_milliseconds() as u64;
        if election_timeout < lease {
            return Err(box_err!("election timeout {} ms is less than lease {} ms",
                                election_timeout,
                                lease));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration as TimeDuration;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());

        cfg.raft_heartbeat_ticks = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_heartbeat_ticks = 10;
        assert!(cfg.validate().is_err());

        cfg.raft_heartbeat_ticks = 11;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_threshold = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_size_limit = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.region_max_size = 10;
        cfg.region_split_size = 20;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_base_tick_interval = 1000;
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_store_max_leader_lease = TimeDuration::seconds(20);
        assert!(cfg.validate().is_err());
    }
}
