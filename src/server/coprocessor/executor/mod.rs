// Copyright 2017 PingCAP, Inc.
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

use util::codec::table::RowColsDict;
use server::coprocessor::Result;

#[allow(dead_code)] //TODO:remove it
pub struct Row {
    handler: i64,
    data: RowColsDict,
}

#[allow(dead_code)] //TODO:remove it
impl Row {
    pub fn new(handler: i64, data: RowColsDict) -> Row {
        Row {
            handler: handler,
            data: data,
        }
    }

    pub fn get_mut_data(&mut self) -> (&mut RowColsDict) {
        &mut self.data
    }

    pub fn get_data(self) -> RowColsDict {
        self.data
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
}
