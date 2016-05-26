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

use std::fmt::{self, Formatter, Display};
use std::{io, fs};
use std::boxed::{Box, FnBox};

use super::{Result, Error};
use util::worker::{Runnable, Worker};
use bytes::{ByteBuf, MutByteBuf};

pub type Callback = Box<FnBox(Result<u64>) + Send>;

// TODO make it zero copy
pub struct Task {
    buf: ByteBuf,
    cb: Callback,
}

impl Task {
    pub fn new(buf: &[u8], cb: Callback) -> Task {
        Task {
            buf: ByteBuf::from_slice(buf),
            cb: cb,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot File Receiver Task")
    }
}

pub struct Runner {
    pub file: fs::File,
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let mut buf = task.buf;
        let resp = io::copy(&mut buf, &mut self.file);
        print!("should call call_box callback\n");
        task.cb.call_box((resp.map_err(Error::Io),))
    }
}

pub struct SnapshotReceiver {
    pub worker: Worker<Task>,
    pub buf: MutByteBuf,
    pub more: bool,
}
