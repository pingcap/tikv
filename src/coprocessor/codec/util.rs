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

use super::Result;

// `round_float` rounds float value to the nearest integer value with float64 format,
// like MySQL Round function.
// `round_float` uses default rounding mode, see
// https://dev.mysql.com/doc/refman/5.7/en/precision-math-rounding.html
// so rounding use "round half away from zero".
// e.g, 1.5 -> 2, -1.5 -> -2.
pub fn round_float(f: f64) -> f64 {
    f.round()
}

fn get_max_float(flen: i32, decimal: i32) -> f64 {
    let int_part_len = flen - decimal;
    let mut f = 10.pow(int_part_len);
    f -= 10.pow(-decimal);
    f
}

// Tries to truncate f.
// If the result exceeds the max/min float that flen/decimal allowed, returns the
// max/min float allowed.
pub fn truncate_float(f: f64, flen: i32, decimal: i32) -> Result<f64> {
    if f.is_nan() {
        // nan returns 0
        return Err(box_err!("{} value is out of range in FLOAT", f));
    }

    let max_float = get_max_float(flen, decimal);
    // TODO add impl

    if !f.is_infinite() {
        f =
    }
}
