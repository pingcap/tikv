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

use util::config::ReadableSize;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
pub const DEFAULT_MAX_TASKS_PER_CORE: usize = 2 as usize * 1000;

pub const DEFAULT_STACK_SIZE_MB: u64 = 10;

#[derive(Debug)]
pub struct Config {
    pub high_concurrency: usize,
    pub normal_concurrency: usize,
    pub low_concurrency: usize,
    pub max_tasks_high: usize,
    pub max_tasks_normal: usize,
    pub max_tasks_low: usize,
    pub stack_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            high_concurrency: 4,
            normal_concurrency: 4,
            low_concurrency: 4,
            max_tasks_high: 4 * DEFAULT_MAX_TASKS_PER_CORE,
            max_tasks_normal: 4 * DEFAULT_MAX_TASKS_PER_CORE,
            max_tasks_low: 4 * DEFAULT_MAX_TASKS_PER_CORE,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }
}

impl Config {
    /// Only used in tests.
    pub fn with_concurrency(self, concurrency: usize) -> Self {
        Self {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_high: concurrency * DEFAULT_MAX_TASKS_PER_CORE,
            max_tasks_normal: concurrency * DEFAULT_MAX_TASKS_PER_CORE,
            max_tasks_low: concurrency * DEFAULT_MAX_TASKS_PER_CORE,
            ..self
        }
    }

    /// Only used in tests
    pub fn with_concurrency_for_test(self) -> Self {
        self.with_concurrency(2)
    }
}
