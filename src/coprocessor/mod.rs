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

mod checksum;
pub mod codec;
mod dag;
mod endpoint;
pub mod local_metrics;
mod metrics;
mod readpool_context;
mod statistics;
mod util;

pub use self::endpoint::err_resp;
pub use self::readpool_context::Context as ReadPoolContext;

use std::boxed::FnBox;
use std::error;
use std::result;
use std::time::Duration;

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
use tipb;

use storage;
use util::time::Instant;

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;
pub const REQ_TYPE_CHECKSUM: i64 = 105;

const SINGLE_GROUP: &[u8] = b"SingleGroup";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: kvrpcpb::LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        Full(allow: usize) {
            description("running queue is full")
        }
        Eval(err: tipb::select::Error) {
            from()
            description("eval failed")
            display("eval error {:?}", err)
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<storage::engine::Error> for Error {
    fn from(e: storage::engine::Error) -> Error {
        match e {
            storage::engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<self::dag::expr::Error> for Error {
    fn from(e: self::dag::expr::Error) -> Error {
        Error::Eval(e.into())
    }
}

impl From<::util::codec::Error> for Error {
    fn from(e: ::util::codec::Error) -> Error {
        let mut err = tipb::select::Error::new();
        err.set_msg(format!("{}", e));
        Error::Eval(err)
    }
}

impl From<storage::txn::Error> for Error {
    fn from(e: storage::txn::Error) -> Error {
        match e {
            storage::txn::Error::Mvcc(storage::mvcc::Error::KeyIsLocked {
                primary,
                ts,
                key,
                ttl,
            }) => {
                let mut info = kvrpcpb::LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                info.set_lock_ttl(ttl);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

pub use self::endpoint::{Host as EndPointHost, RequestTask, Task as EndPointTask};

type HandlerStreamStepResult = Result<(Option<coppb::Response>, bool)>;

trait RequestHandler: Send {
    fn handle_request(&mut self) -> Result<coppb::Response> {
        panic!("unary request is not supported for this handler");
    }

    fn handle_streaming_request(&mut self) -> HandlerStreamStepResult {
        panic!("streaming request is not supported for this handler");
    }

    fn collect_metrics_into(&mut self, _metrics: &mut self::dag::executor::ExecutorMetrics) {
        // Do nothing by default
    }

    fn into_boxed(self) -> Box<RequestHandler>
    where
        Self: 'static + Sized,
    {
        box self
    }
}

/// `RequestHandlerBuilder` accepts a `Box<Snapshot>` and builds a `RequestHandler`.
type RequestHandlerBuilder =
    Box<FnBox(Box<storage::Snapshot + 'static>) -> Result<Box<RequestHandler>> + Send>;

#[derive(Debug)]
pub struct ReqContext {
    pub context: kvrpcpb::Context,
    pub table_scan: bool, // Whether is a table scan request.
    pub txn_start_ts: u64,
    pub start_time: Instant,
    pub deadline: Instant,
}

impl ReqContext {
    pub fn new(
        context: kvrpcpb::Context,
        txn_start_ts: u64,
        table_scan: bool,
        max_handle_duration: Duration,
    ) -> ReqContext {
        let start_time = Instant::now_coarse();
        let deadline = start_time + max_handle_duration;
        ReqContext {
            context,
            table_scan,
            txn_start_ts,
            start_time,
            deadline,
        }
    }

    #[inline]
    pub fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            "select"
        } else {
            "index"
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            let elapsed = now.duration_since(self.start_time);
            return Err(Error::Outdated(elapsed, self.get_scan_tag()));
        }
        Ok(())
    }
}
