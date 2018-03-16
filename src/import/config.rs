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

use std::error::Error;
use std::result::Result;

use util::config::{ReadableDuration, ReadableSize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub import_dir: String,
    pub num_threads: usize,
    pub max_import_jobs: usize,
    pub max_import_sst_jobs: usize,
    pub max_prepare_duration: ReadableDuration,
    pub region_split_size: ReadableSize,
    pub stream_channel_window: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            import_dir: "/tmp/tikv/import".to_owned(),
            num_threads: 8,
            max_import_jobs: 8,
            max_import_sst_jobs: 2,
            max_prepare_duration: ReadableDuration::minutes(5),
            // Derive from coprocessor.region_split_size when it is 0.
            region_split_size: ReadableSize::mb(0),
            stream_channel_window: 128,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.max_import_jobs == 0 {
            return Err("import.max_import_jobs can not be 0".into());
        }
        if self.max_import_sst_jobs == 0 {
            return Err("import.max_import_sst_jobs can not be 0".into());
        }
        if self.region_split_size.0 == 0 {
            return Err("import.region_split_size can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        Ok(())
    }
}
