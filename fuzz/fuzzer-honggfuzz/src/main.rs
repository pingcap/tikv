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

#[macro_use]
extern crate honggfuzz;
extern crate fuzz_targets;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            fuzz_targets::run_fuzz_targets(option_env!("TIKV_FUZZ_TARGETS").unwrap_or_default(), data);
        });
    }
}
