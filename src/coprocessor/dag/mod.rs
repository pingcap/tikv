// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

pub use self::storage_impl::TiKVStorage;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query::storage::IntervalRange;
use tipb::{DagRequest, SelectResponse, StreamResponse};

use crate::coprocessor::metrics::*;
use crate::coprocessor::{Deadline, RequestHandler, Result};
use crate::storage::{Statistics, Store};

pub fn build_handler<S: Store + 'static>(
    req: DagRequest,
    ranges: Vec<KeyRange>,
    store: S,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    enable_batch_if_possible: bool,
) -> Result<Box<dyn RequestHandler>> {
    let mut is_batch = false;
    if enable_batch_if_possible {
        let is_supported =
            tidb_query::batch::runner::BatchExecutorsRunner::check_supported(req.get_executors());
        if let Err(e) = is_supported {
            // Not supported, will fallback to normal executor.
            // To avoid user worries, let's output success message.
            info!("Err(e) = is_supported, fuck.");
            debug!("Successfully use normal Coprocessor query engine"; "start_ts" => req.get_start_ts(), "reason" => %e);
        } else {
            if is_streaming {
                info!("Is batch in streaming mode");
            } else {
                info!("Is batch, and not in streaming mode");
            }

            is_batch = true;
        }
    }

    if is_batch {
        COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
        info!("Build batch executor with streaming {}", is_streaming);
        Ok(
            BatchDAGHandler::new(req, ranges, store, deadline, batch_row_limit, is_streaming)?
                .into_boxed(),
        )
    } else {
        info!("Build non-batch executor with streaming {}", is_streaming);
        COPR_DAG_REQ_COUNT.with_label_values(&["normal"]).inc();
        Ok(
            DAGHandler::new(req, ranges, store, deadline, batch_row_limit, is_streaming)?
                .into_boxed(),
        )
    }
}

pub struct DAGHandler(tidb_query::executor::ExecutorsRunner<Statistics>);

impl DAGHandler {
    pub fn new<S: Store + 'static>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        Ok(Self(tidb_query::executor::ExecutorsRunner::from_request(
            req,
            ranges,
            TiKVStorage::from(store),
            deadline,
            batch_row_limit,
            is_streaming,
        )?))
    }
}

impl RequestHandler for DAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        handle_qe_response(self.0.handle_request())
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        info!("None batch DAGHandler stream called.");
        handle_qe_stream_response(self.0.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.0.collect_storage_stats(dest);
    }
}

pub struct BatchDAGHandler(tidb_query::batch::runner::BatchExecutorsRunner<Statistics>);

impl BatchDAGHandler {
    pub fn new<S: Store + 'static>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        stream_batch_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        Ok(Self(
            tidb_query::batch::runner::BatchExecutorsRunner::from_request(
                req,
                ranges,
                TiKVStorage::from(store),
                deadline,
                stream_batch_row_limit,
                is_streaming,
            )?,
        ))
    }
}

impl RequestHandler for BatchDAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        handle_qe_response(self.0.handle_request())
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        info!("Batch DAGHandler stream called.");
        handle_qe_stream_response(self.0.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.0.collect_storage_stats(dest);
    }
}

fn handle_qe_response(result: tidb_query::Result<SelectResponse>) -> Result<Response> {
    use tidb_query::error::ErrorInner;

    match result {
        Ok(sel_resp) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            Ok(resp)
        }
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut sel_resp = SelectResponse::default();
                sel_resp.mut_error().set_code(err.code());
                sel_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: tidb_query::Result<(Option<(StreamResponse, IntervalRange)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use tidb_query::error::ErrorInner;

    match result {
        Ok((Some((s_resp, range)), finished)) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(s_resp.write_to_bytes()));
            resp.mut_range().set_start(range.lower_inclusive);
            resp.mut_range().set_end(range.upper_exclusive);
            Ok((Some(resp), finished))
        }
        Ok((None, finished)) => Ok((None, finished)),
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut s_resp = StreamResponse::default();
                s_resp.mut_error().set_code(err.code());
                s_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(s_resp.write_to_bytes()));
                Ok((Some(resp), true))
            }
        },
    }
}
