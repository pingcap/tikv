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

use std::usize;
use std::collections::BinaryHeap;
use std::time::{Instant, Duration};
use std::rc::Rc;
use std::fmt::{self, Display, Formatter, Debug};
use std::cmp::{self, Ordering as CmpOrdering};
use std::cell::RefCell;
use tipb::select::{self, SelectRequest, SelectResponse, DAGRequest, Chunk, RowMeta};
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType, ByItem};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use kvproto::coprocessor::{Request, Response, KeyRange};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::CommandPri;

use util::{escape, duration_to_ms, duration_to_sec, Either};
use util::worker::{BatchRunnable, Scheduler};
use util::collections::{HashMap, HashMapEntry as Entry, HashSet};
use util::threadpool::{ThreadPool, FifoQueue};
use util::codec::number::NumberDecoder;
use server::OnResponse;
use storage::{self, Engine, SnapshotStore, engine, Snapshot, Key, ScanMode, Statistics};

use super::codec::{table, datum, mysql};
use super::codec::table::{RowColsDict, TableDecoder};
use super::codec::datum::{DatumEncoder, Datum};
use super::xeval::{Evaluator, EvalContext};
use super::aggregate::{self, AggrFunc};
use super::dag::DAGContext;
use super::metrics::*;
use super::executor::Row;
use super::{Error, Result};

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;
pub const REQ_TYPE_DAG: i64 = 103;
pub const BATCH_ROW_COUNT: usize = 64;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
const REQUEST_CHECKPOINT: usize = 255;
// Assume a request can be finished in 0.1ms, a request at position x will wait about
// 0.0001 * x secs to be actual started. Hence the queue should have at most
// REQUEST_MAX_HANDLE_SECS / 0.0001 request.
const DEFAULT_MAX_RUNNING_TASK_COUNT: usize = REQUEST_MAX_HANDLE_SECS as usize * 10_000;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &'static str = "request outdated.";

const ENDPOINT_IS_BUSY: &'static str = "endpoint is busy";

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ThreadPool<FifoQueue<u64>, u64>,
    low_priority_pool: ThreadPool<FifoQueue<u64>, u64>,
    high_priority_pool: ThreadPool<FifoQueue<u64>, u64>,
    max_running_task_count: usize,
}

impl Host {
    pub fn new(engine: Box<Engine>, scheduler: Scheduler<Task>, concurrency: usize) -> Host {
        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            max_running_task_count: DEFAULT_MAX_RUNNING_TASK_COUNT,
            pool: ThreadPool::new(thd_name!("endpoint-normal-pool"),
                                  concurrency,
                                  FifoQueue::new()),
            low_priority_pool: ThreadPool::new(thd_name!("endpoint-low-pool"),
                                               concurrency,
                                               FifoQueue::new()),
            high_priority_pool: ThreadPool::new(thd_name!("endpoint-high-pool"),
                                                concurrency,
                                                FifoQueue::new()),
        }
    }

    fn running_task_count(&self) -> usize {
        self.pool.get_task_count() + self.low_priority_pool.get_task_count() +
        self.high_priority_pool.get_task_count()
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
        }
    }
}

enum CopRequest {
    Select(SelectRequest),
    DAG(DAGRequest),
}

pub struct RequestTask {
    req: Request,
    start_ts: Option<u64>,
    wait_time: Option<f64>,
    timer: Instant,
    // The deadline before which the task should be responded.
    deadline: Instant,
    statistics: Statistics,
    on_resp: OnResponse,
    cop_req: Option<Result<CopRequest>>,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse) -> RequestTask {
        let timer = Instant::now();
        let deadline = timer + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        let mut start_ts = None;
        let tp = req.get_tp();
        let cop_req = match tp {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                if let Err(e) = sel.merge_from_bytes(req.get_data()) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(sel.get_start_ts());
                    Ok(CopRequest::Select(sel))
                }
            }
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                if let Err(e) = dag.merge_from_bytes(req.get_data()) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(dag.get_start_ts());
                    Ok(CopRequest::DAG(dag))
                }
            }
            _ => Err(box_err!("unsupported tp {}", tp)),
        };
        RequestTask {
            req: req,
            start_ts: start_ts,
            wait_time: None,
            timer: timer,
            deadline: deadline,
            statistics: Default::default(),
            on_resp: on_resp,
            cop_req: Some(cop_req),
        }
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        check_if_outdated(self.deadline, self.req.get_tp())
    }

    fn stop_record_waiting(&mut self) {
        if self.wait_time.is_some() {
            return;
        }
        let wait_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_WAIT_TIME.with_label_values(&[get_req_type_str(self.req.get_tp())])
            .observe(wait_time);
        self.wait_time = Some(wait_time);
    }

    fn stop_record_handling(&mut self) {
        self.stop_record_waiting();

        let handle_time = duration_to_sec(self.timer.elapsed());
        let type_str = get_req_type_str(self.req.get_tp());
        COPR_REQ_HISTOGRAM_VEC.with_label_values(&[type_str]).observe(handle_time);
        let wait_time = self.wait_time.unwrap();
        COPR_REQ_HANDLE_TIME.with_label_values(&[type_str])
            .observe(handle_time - wait_time);


        COPR_SCAN_KEYS.with_label_values(&[type_str])
            .observe(self.statistics.total_op_count() as f64);

        for (cf, details) in self.statistics.details() {
            for (tag, count) in details {
                COPR_SCAN_DETAILS.with_label_values(&[type_str, cf, tag])
                    .observe(count as f64);
            }
        }

        if handle_time > SLOW_QUERY_LOWER_BOUND {
            info!("[region {}] handle {:?} [{}] takes {:?} [waiting: {:?}, keys: {}, hit: {}, \
                   ranges: {} ({:?})]",
                  self.req.get_context().get_region_id(),
                  self.start_ts,
                  type_str,
                  handle_time,
                  wait_time,
                  self.statistics.total_op_count(),
                  self.statistics.total_processed(),
                  self.req.get_ranges().len(),
                  self.req.get_ranges().get(0));
        }
    }

    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "request [context {:?}, tp: {}, ranges: {} ({:?})]",
               self.req.get_context(),
               self.req.get_tp(),
               self.req.get_ranges().len(),
               self.req.get_ranges().get(0))
    }
}

impl BatchRunnable<Task> for Host {
    // TODO: limit pending reqs
    #[allow(for_kv_map)]
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if let Err(e) = req.check_outdated() {
                        on_error(e, req);
                        continue;
                    }
                    let key = {
                        let ctx = req.req.get_context();
                        (ctx.get_region_id(),
                         ctx.get_region_epoch().get_version(),
                         ctx.get_peer().get_id())
                    };
                    let mut group = grouped_reqs.entry(key).or_insert_with(Vec::new);
                    group.push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    let reqs = self.reqs.remove(&q_id).unwrap();
                    let snap = match snap_res {
                        Ok(s) => s,
                        Err(e) => {
                            notify_batch_failed(e, reqs);
                            continue;
                        }
                    };

                    if self.running_task_count() >= self.max_running_task_count {
                        notify_batch_failed(Error::Full(self.max_running_task_count), reqs);
                        continue;
                    }

                    for req in reqs {
                        let pri = req.priority();
                        let pri_str = get_req_pri_str(pri);
                        let type_str = get_req_type_str(req.req.get_tp());
                        COPR_PENDING_REQS.with_label_values(&[type_str, pri_str]).add(1.0);
                        let end_point = TiDbEndPoint::new(snap.clone());
                        let txn_id = req.start_ts.unwrap_or_default();

                        if pri == CommandPri::Low {
                            self.low_priority_pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS.with_label_values(&[type_str, pri_str]).dec();
                            });
                        } else if pri == CommandPri::High {
                            self.high_priority_pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS.with_label_values(&[type_str, pri_str]).dec();
                            });
                        } else {
                            self.pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS.with_label_values(&[type_str, pri_str]).dec();
                            });
                        }
                    }
                }
            }
        }
        for (_, reqs) in grouped_reqs {
            self.last_req_id += 1;
            let id = self.last_req_id;
            let sched = self.sched.clone();
            if let Err(e) = self.engine.async_snapshot(reqs[0].req.get_context(),
                                                       box move |(_, res)| {
                                                           sched.schedule(Task::SnapRes(id, res))
                                                               .unwrap()
                                                       }) {
                notify_batch_failed(e, reqs);
                continue;
            }
            self.reqs.insert(id, reqs);
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
    }
}

fn err_resp(e: Error) -> Response {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            COPR_REQ_ERROR.with_label_values(&[tag]).inc();
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            COPR_REQ_ERROR.with_label_values(&["lock"]).inc();
        }
        Error::Outdated(deadline, now, tp) => {
            let t = get_req_type_str(tp);
            let elapsed = now.duration_since(deadline) +
                          Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
            OUTDATED_REQ_WAIT_TIME.with_label_values(&[t])
                .observe(elapsed.as_secs() as f64);

            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
        }
        Error::Full(allow) => {
            COPR_REQ_ERROR.with_label_values(&["full"]).inc();
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit {}", allow));
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            COPR_REQ_ERROR.with_label_values(&["other"]).inc();
        }
    }
    resp
}

fn on_error(e: Error, req: RequestTask) {
    let resp = err_resp(e);
    respond(resp, req)
}

fn notify_batch_failed<E: Into<Error> + Debug>(e: E, reqs: Vec<RequestTask>) {
    debug!("failed to handle batch request: {:?}", e);
    let resp = err_resp(e.into());
    for t in reqs {
        respond(resp.clone(), t)
    }
}

fn check_if_outdated(deadline: Instant, tp: i64) -> Result<()> {
    let now = Instant::now();
    if deadline <= now {
        return Err(Error::Outdated(deadline, now, tp));
    }
    Ok(())
}

fn respond(resp: Response, mut t: RequestTask) {
    t.stop_record_handling();
    (t.on_resp)(resp)
}

pub struct TiDbEndPoint {
    snap: Box<Snapshot>,
}

impl TiDbEndPoint {
    pub fn new(snap: Box<Snapshot>) -> TiDbEndPoint {
        TiDbEndPoint { snap: snap }
    }
}

impl TiDbEndPoint {
    fn handle_request(&self, mut t: RequestTask) {
        t.stop_record_waiting();
        if let Err(e) = t.check_outdated() {
            on_error(e, t);
            return;
        }
        let resp = match t.cop_req.take().unwrap() {
            Ok(CopRequest::Select(sel)) => self.handle_select(sel, &mut t),
            Ok(CopRequest::DAG(dag)) => self.handle_dag(dag, &mut t),
            Err(err) => Err(err),
        };
        match resp {
            Ok(r) => respond(r, t),
            Err(e) => on_error(e, t),
        }
    }

    pub fn handle_select(&self, sel: SelectRequest, t: &mut RequestTask) -> Result<Response> {
        let snap = SnapshotStore::new(self.snap.as_ref(),
                                      sel.get_start_ts(),
                                      t.req.get_context().get_isolation_level());
        let mut ctx = try!(SelectContext::new(sel, snap, t.deadline, &mut t.statistics));
        let mut range = t.req.get_ranges().to_vec();
        debug!("scanning range: {:?}", range);
        if ctx.core.desc_scan {
            range.reverse();
        }
        let res = match t.req.get_tp() {
            REQ_TYPE_SELECT => ctx.get_rows_from_sel(range),
            REQ_TYPE_INDEX => ctx.get_rows_from_idx(range),
            _ => unreachable!(),
        };
        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(()) => {
                sel_resp.set_chunks(RepeatedField::from_vec(ctx.core.chunks));
                let data = box_try!(sel_resp.write_to_bytes());
                resp.set_data(data);
            }
            Err(e) => {
                if let Error::Other(_) = e {
                    sel_resp.set_error(to_pb_error(&e));
                    resp.set_data(box_try!(sel_resp.write_to_bytes()));
                    resp.set_other_error(format!("{}", e));
                    COPR_REQ_ERROR.with_label_values(&["other"]).inc();
                } else {
                    return Err(e);
                }
            }
        }
        Ok(resp)
    }

    pub fn handle_dag(&self, dag: DAGRequest, t: &mut RequestTask) -> Result<Response> {
        let ranges = t.req.get_ranges().to_vec();
        let eval_ctx = Rc::new(box_try!(EvalContext::new(dag.get_time_zone_offset(),
                                                         dag.get_flags())));
        let mut ctx = DAGContext::new(dag,
                                      t.deadline,
                                      ranges,
                                      self.snap.as_ref(),
                                      eval_ctx.clone());
        try!(ctx.validate_dag());
        let mut exec = try!(ctx.build_dag(&mut t.statistics));
        let mut chunks = vec![];
        loop {
            match exec.next() {
                Ok(Some(row)) => {
                    try!(check_if_outdated(ctx.deadline, REQ_TYPE_DAG));
                    let mut chunk = get_chunk(&mut chunks);
                    let length = chunk.get_rows_data().len();
                    if ctx.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = try!(inflate_cols(&row, &ctx.columns));
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                    let mut meta = RowMeta::new();
                    meta.set_handle(row.handle);
                    meta.set_length((chunk.get_rows_data().len() - length) as i64);
                    chunk.mut_rows_meta().push(meta);
                }
                Ok(None) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_chunks(RepeatedField::from_vec(chunks));
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => {
                    if let Error::Other(_) = e {
                        let mut resp = Response::new();
                        let mut sel_resp = SelectResponse::new();
                        sel_resp.set_error(to_pb_error(&e));
                        resp.set_data(box_try!(sel_resp.write_to_bytes()));
                        resp.set_other_error(format!("{}", e));
                        return Ok(resp);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
}

fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(DEFAULT_ERROR_CODE);
    e.set_msg(format!("{}", err));
    e
}

pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[inline]
fn inflate_with_col<'a, T>(eval: &mut Evaluator,
                           ctx: &EvalContext,
                           values: &RowColsDict,
                           cols: T,
                           h: i64)
                           -> Result<()>
    where T: IntoIterator<Item = &'a ColumnInfo>
{
    for col in cols {
        let col_id = col.get_column_id();
        if let Entry::Vacant(e) = eval.row.entry(col_id) {
            if col.get_pk_handle() {
                let v = get_pk(col, h);
                e.insert(v);
            } else {
                let value = match values.get(col_id) {
                    None if col.has_default_val() => {
                        // TODO: optimize it to decode default value only once.
                        box_try!(col.get_default_val().decode_col_value(ctx, col))
                    }
                    None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                        return Err(box_err!("column {} of {} is missing", col_id, h));
                    }
                    None => Datum::Null,
                    Some(mut bs) => box_try!(bs.decode_col_value(ctx, col)),
                };
                e.insert(value);
            }
        }
    }
    Ok(())
}

// TODO(performance), there are too much decoding logic.
// we could do it only once when getting RowColsDict in cut_row.
#[inline]
fn inflate_cols(row: &Row, cols: &[ColumnInfo]) -> Result<Vec<u8>> {
    let data = &row.data;
    // TODO capacity is not enough
    let mut values = Vec::with_capacity(data.value.len());
    for col in cols {
        let col_id = col.get_column_id();
        match data.get(col_id) {
            Some(value) => values.extend_from_slice(value),
            None if col.get_pk_handle() => {
                let pk = get_pk(col, row.handle);
                box_try!(values.encode(&[pk], false));
            }
            None if col.has_default_val() => {
                values.extend_from_slice(col.get_default_val());
            }
            None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                return Err(box_err!("column {} of {} is missing", col_id, row.handle));
            }
            None => {
                box_try!(values.encode(&[Datum::Null], false));
            }
        }
    }
    Ok(values)
}

#[inline]
fn get_chunk(chunks: &mut Vec<Chunk>) -> &mut Chunk {
    if chunks.last().map_or(true, |chunk| chunk.get_rows_meta().len() >= BATCH_ROW_COUNT) {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}

pub struct SortRow {
    pub handle: i64,
    pub data: RowColsDict,
    pub key: Vec<Datum>,
    order_cols: Rc<Vec<ByItem>>,
    ctx: Rc<EvalContext>,
    err: Rc<RefCell<Option<String>>>,
}

impl SortRow {
    fn new(handle: i64,
           data: RowColsDict,
           key: Vec<Datum>,
           order_cols: Rc<Vec<ByItem>>,
           ctx: Rc<EvalContext>,
           err: Rc<RefCell<Option<String>>>)
           -> SortRow {
        SortRow {
            handle: handle,
            data: data,
            key: key,
            order_cols: order_cols,
            ctx: ctx,
            err: err,
        }
    }

    fn cmp_and_check(&self, right: &SortRow) -> Result<CmpOrdering> {
        // check err
        try!(self.check_err());
        let values = self.key.iter().zip(right.key.iter());
        for (col, (v1, v2)) in self.order_cols.as_ref().iter().zip(values) {
            match v1.cmp(self.ctx.as_ref(), v2) {
                Ok(CmpOrdering::Equal) => {
                    continue;
                }
                Ok(order) => {
                    if col.get_desc() {
                        return Ok(order.reverse());
                    }
                    return Ok(order);
                }
                Err(err) => {
                    self.set_err(format!("cmp failed with:{:?}", err));
                    try!(self.check_err());
                }
            }
        }
        Ok(CmpOrdering::Equal)
    }

    #[inline]
    fn check_err(&self) -> Result<()> {
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(())
    }

    fn set_err(&self, err_msg: String) {
        *self.err.borrow_mut() = Some(err_msg);
    }
}

pub struct TopNHeap {
    pub rows: BinaryHeap<SortRow>,
    limit: usize,
    err: Rc<RefCell<Option<String>>>,
}

const HEAP_MAX_CAPACITY: usize = 1024;

impl TopNHeap {
    pub fn new(limit: usize) -> Result<TopNHeap> {
        if limit == usize::MAX {
            return Err(box_err!("invalid limit"));
        }
        let cap = cmp::min(limit, HEAP_MAX_CAPACITY);
        Ok(TopNHeap {
            rows: BinaryHeap::with_capacity(cap),
            limit: limit,
            err: Rc::new(RefCell::new(None)),
        })
    }

    #[inline]
    pub fn check_err(&self) -> Result<()> {
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(())
    }

    pub fn try_add_row(&mut self,
                       handle: i64,
                       data: RowColsDict,
                       values: Vec<Datum>,
                       order_cols: Rc<Vec<ByItem>>,
                       ctx: Rc<EvalContext>)
                       -> Result<()> {
        let row = SortRow::new(handle, data, values, order_cols, ctx, self.err.clone());
        // push into heap when heap is not full
        if self.rows.len() < self.limit {
            self.rows.push(row);
        } else {
            // swap top value with row when heap is full and current row is less than top data
            let mut top_data = self.rows.peek_mut().unwrap();
            let order = try!(row.cmp_and_check(&top_data));
            if CmpOrdering::Less == order {
                *top_data = row;
            }
        }
        self.check_err()
    }

    pub fn into_sorted_vec(self) -> Result<Vec<SortRow>> {
        let sorted_data = self.rows.into_sorted_vec();
        // check is needed here since err may caused by any call of cmp
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(sorted_data)
    }
}

impl Ord for SortRow {
    fn cmp(&self, right: &SortRow) -> CmpOrdering {
        if let Ok(order) = self.cmp_and_check(right) {
            return order;
        }
        CmpOrdering::Equal
    }
}

impl PartialEq for SortRow {
    fn eq(&self, right: &SortRow) -> bool {
        self.cmp(right) == CmpOrdering::Equal
    }
}

impl Eq for SortRow {}

impl PartialOrd for SortRow {
    fn partial_cmp(&self, rhs: &SortRow) -> Option<CmpOrdering> {
        Some(self.cmp(rhs))
    }
}


pub struct SelectContextCore {
    ctx: Rc<EvalContext>,
    sel: SelectRequest,
    eval: Evaluator,
    cols: Either<HashSet<i64>, Vec<i64>>,
    pk_col: Option<ColumnInfo>,
    cond_cols: Vec<ColumnInfo>,
    topn_cols: Vec<ColumnInfo>,
    aggr: bool,
    aggr_cols: Vec<ColumnInfo>,
    topn: bool,
    topn_heap: Option<TopNHeap>,
    order_cols: Rc<Vec<ByItem>>,
    limit: usize,
    desc_scan: bool,
    gks: Vec<Rc<Vec<u8>>>,
    gk_aggrs: HashMap<Rc<Vec<u8>>, Vec<Box<AggrFunc>>>,
    chunks: Vec<Chunk>,
}

impl SelectContextCore {
    fn new(sel: SelectRequest) -> Result<SelectContextCore> {
        let cond_cols;
        let topn_cols;
        let mut order_by_cols: Vec<ByItem> = Vec::new();
        let mut aggr_cols = vec![];
        {
            let select_cols = if sel.has_table_info() {
                COPR_EXECUTOR_COUNT.with_label_values(&["tblscan"]).inc();
                sel.get_table_info().get_columns()
            } else {
                COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();
                sel.get_index_info().get_columns()
            };
            let mut cond_col_map = HashMap::default();
            if sel.has_field_where() {
                COPR_EXECUTOR_COUNT.with_label_values(&["selection"]).inc();
                try!(collect_col_in_expr(&mut cond_col_map, select_cols, sel.get_field_where()));
            }
            let mut aggr_cols_map = HashMap::default();
            for aggr in sel.get_aggregates() {
                try!(collect_col_in_expr(&mut aggr_cols_map, select_cols, aggr));
            }
            for item in sel.get_group_by() {
                try!(collect_col_in_expr(&mut aggr_cols_map, select_cols, item.get_expr()));
            }
            if !aggr_cols_map.is_empty() {
                for cond_col in cond_col_map.keys() {
                    aggr_cols_map.remove(cond_col);
                }
                aggr_cols = aggr_cols_map.into_iter().map(|(_, v)| v).collect();
            }
            cond_cols = cond_col_map.into_iter().map(|(_, v)| v).collect();

            // get topn cols
            let mut topn_col_map = HashMap::default();
            for item in sel.get_order_by() {
                try!(collect_col_in_expr(&mut topn_col_map, select_cols, item.get_expr()))
            }
            topn_cols = topn_col_map.into_iter().map(|(_, v)| v).collect();
            order_by_cols.extend_from_slice(sel.get_order_by())
        }

        let limit = if sel.has_limit() {
            COPR_EXECUTOR_COUNT.with_label_values(&["limit"]).inc();
            sel.get_limit() as usize
        } else {
            usize::MAX
        };

        // set topn
        let mut topn = false;
        let mut desc_can = false;
        if !sel.get_order_by().is_empty() {
            // order by pk,set desc_scan is enough
            if !sel.get_order_by()[0].has_expr() {
                desc_can = sel.get_order_by().first().map_or(false, |o| o.get_desc());
            } else {
                COPR_EXECUTOR_COUNT.with_label_values(&["topn"]).inc();
                topn = true;
            }
        }

        let mut pk_col = None;
        let cols = if sel.has_table_info() {
            Either::Left(sel.get_table_info()
                .get_columns()
                .iter()
                .filter(|c| !c.get_pk_handle())
                .map(|c| c.get_column_id())
                .collect())
        } else {
            let cols = sel.get_index_info().get_columns();
            if cols.last().map_or(false, |c| c.get_pk_handle()) {
                pk_col = Some(cols.last().unwrap().clone());
            }
            Either::Right(cols.iter().map(|c| c.get_column_id()).collect())
        };

        let aggr = if !sel.get_aggregates().is_empty() || !sel.get_group_by().is_empty() {
            COPR_EXECUTOR_COUNT.with_label_values(&["aggregation"]).inc();
            true
        } else {
            false
        };

        Ok(SelectContextCore {
            ctx: Rc::new(box_try!(EvalContext::new(sel.get_time_zone_offset(), sel.get_flags()))),
            aggr: aggr,
            aggr_cols: aggr_cols,
            topn_cols: topn_cols,
            sel: sel,
            eval: Default::default(),
            cols: cols,
            pk_col: pk_col,
            cond_cols: cond_cols,
            gks: vec![],
            gk_aggrs: map![],
            chunks: vec![],
            topn: topn,
            topn_heap: {
                if topn {
                    Some(try!(TopNHeap::new(limit)))
                } else {
                    None
                }
            },
            order_cols: Rc::new(order_by_cols),
            limit: limit,
            desc_scan: desc_can,
        })
    }

    fn handle_row(&mut self, h: i64, row_data: RowColsDict) -> Result<usize> {
        // clear all dirty values.
        self.eval.row.clear();
        if try!(self.should_skip(h, &row_data)) {
            return Ok(0);
        }

        // topn & aggr won't appear together
        if self.topn {
            try!(self.collect_topn_row(h, row_data));
            Ok(0)
        } else if self.aggr {
            try!(self.aggregate(h, &row_data));
            Ok(0)
        } else {
            try!(self.get_row(h, row_data));
            Ok(1)
        }
    }

    fn should_skip(&mut self, h: i64, values: &RowColsDict) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        try!(inflate_with_col(&mut self.eval, &self.ctx, values, &self.cond_cols, h));
        let res = box_try!(self.eval.eval(&self.ctx, self.sel.get_field_where()));
        let b = box_try!(res.into_bool(&self.ctx));
        Ok(b.map_or(true, |v| !v))
    }

    fn collect_topn_row(&mut self, h: i64, values: RowColsDict) -> Result<()> {
        try!(inflate_with_col(&mut self.eval, &self.ctx, &values, &self.topn_cols, h));
        let mut sort_keys = Vec::with_capacity(self.sel.get_order_by().len());
        // parse order by
        for col in self.sel.get_order_by() {
            let v = box_try!(self.eval.eval(&self.ctx, col.get_expr()));
            sort_keys.push(v);
        }

        self.topn_heap.as_mut().unwrap().try_add_row(h,
                                                     values,
                                                     sort_keys,
                                                     self.order_cols.clone(),
                                                     self.ctx.clone())
    }

    fn get_row(&mut self, h: i64, values: RowColsDict) -> Result<()> {
        let chunk = get_chunk(&mut self.chunks);
        let last_len = chunk.get_rows_data().len();
        let cols = if self.sel.has_table_info() {
            self.sel.get_table_info().get_columns()
        } else {
            self.sel.get_index_info().get_columns()
        };
        for col in cols {
            let col_id = col.get_column_id();
            if let Some(v) = values.get(col_id) {
                chunk.mut_rows_data().extend_from_slice(v);
                continue;
            }
            if col.get_pk_handle() {
                box_try!(datum::encode_to(chunk.mut_rows_data(), &[get_pk(col, h)], false));
            } else if col.has_default_val() {
                chunk.mut_rows_data().extend_from_slice(col.get_default_val());
            } else if mysql::has_not_null_flag(col.get_flag() as u64) {
                return Err(box_err!("column {} of {} is missing", col_id, h));
            } else {
                box_try!(datum::encode_to(chunk.mut_rows_data(), &[Datum::Null], false));
            }
        }
        let mut meta = RowMeta::new();
        meta.set_handle(h);
        meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
        chunk.mut_rows_meta().push(meta);
        Ok(())
    }

    fn get_group_key(&mut self) -> Result<Vec<u8>> {
        let items = self.sel.get_group_by();
        if items.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(items.len());
        for item in items {
            let v = box_try!(self.eval.eval(&self.ctx, item.get_expr()));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self, h: i64, values: &RowColsDict) -> Result<()> {
        try!(inflate_with_col(&mut self.eval, &self.ctx, values, &self.aggr_cols, h));
        let gk = Rc::new(try!(self.get_group_key()));
        let aggr_exprs = self.sel.get_aggregates();
        match self.gk_aggrs.entry(gk.clone()) {
            Entry::Occupied(e) => {
                let funcs = e.into_mut();
                for (expr, func) in aggr_exprs.iter().zip(funcs) {
                    // TODO: cache args
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    try!(func.update(&self.ctx, args));
                }
            }
            Entry::Vacant(e) => {
                let mut aggrs = Vec::with_capacity(aggr_exprs.len());
                for expr in aggr_exprs {
                    let mut aggr = try!(aggregate::build_aggr_func(expr));
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    try!(aggr.update(&self.ctx, args));
                    aggrs.push(aggr);
                }
                self.gks.push(gk);
                e.insert(aggrs);
            }
        }
        Ok(())
    }

    fn collect_topn_rows(&mut self) -> Result<()> {
        let sorted_data = try!(self.topn_heap.take().unwrap().into_sorted_vec());
        for row in sorted_data {
            try!(self.get_row(row.handle, row.data));
        }
        Ok(())
    }

    /// Convert aggregate partial result to rows.
    /// Data layout example:
    /// SQL: select count(c1), sum(c2), avg(c3) from t;
    /// Aggs: count(c1), sum(c2), avg(c3)
    /// Rows: groupKey1, count1, value2, count3, value3
    ///       groupKey2, count1, value2, count3, value3
    fn aggr_rows(&mut self) -> Result<()> {
        self.chunks = Vec::with_capacity((self.gk_aggrs.len() + BATCH_ROW_COUNT - 1) /
                                         BATCH_ROW_COUNT);
        // Each aggregate partial result will be converted to two datum.
        let mut row_data = Vec::with_capacity(1 + 2 * self.sel.get_aggregates().len());
        for gk in self.gks.drain(..) {
            let aggrs = self.gk_aggrs.remove(&gk).unwrap();

            let chunk = get_chunk(&mut self.chunks);
            // The first column is group key.
            row_data.push(Datum::Bytes(Rc::try_unwrap(gk).unwrap()));
            for mut aggr in aggrs {
                try!(aggr.calc(&mut row_data));
            }
            let last_len = chunk.get_rows_data().len();
            box_try!(datum::encode_to(chunk.mut_rows_data(), &row_data, false));
            let mut meta = RowMeta::new();
            meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
            chunk.mut_rows_meta().push(meta);
            row_data.clear();
        }
        Ok(())
    }
}

fn collect_col_in_expr(cols: &mut HashMap<i64, ColumnInfo>,
                       col_meta: &[ColumnInfo],
                       expr: &Expr)
                       -> Result<()> {
    if expr.get_tp() == ExprType::ColumnRef {
        let i = box_try!(expr.get_val().decode_i64());
        if let Entry::Vacant(e) = cols.entry(i) {
            for c in col_meta {
                if c.get_column_id() == i {
                    e.insert(c.clone());
                    return Ok(());
                }
            }
            return Err(box_err!("column meta of {} is missing", i));
        }
    }
    for c in expr.get_children() {
        try!(collect_col_in_expr(cols, col_meta, c));
    }
    Ok(())
}


pub struct SelectContext<'a> {
    snap: SnapshotStore<'a>,
    statistics: &'a mut Statistics,
    core: SelectContextCore,
    deadline: Instant,
}

impl<'a> SelectContext<'a> {
    fn new(sel: SelectRequest,
           snap: SnapshotStore<'a>,
           deadline: Instant,
           statistics: &'a mut Statistics)
           -> Result<SelectContext<'a>> {
        Ok(SelectContext {
            core: try!(SelectContextCore::new(sel)),
            snap: snap,
            deadline: deadline,
            statistics: statistics,
        })
    }

    fn get_rows_from_sel(&mut self, ranges: Vec<KeyRange>) -> Result<()> {
        let mut collected = 0;
        for ran in ranges {
            if collected >= self.core.limit {
                break;
            }
            let timer = Instant::now();
            let row_cnt = try!(self.get_rows_from_range(ran));
            debug!("fetch {} rows takes {} ms",
                   row_cnt,
                   duration_to_ms(timer.elapsed()));
            collected += row_cnt;
            try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
        }
        if self.core.topn {
            self.core.collect_topn_rows()
        } else if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(())
        }
    }

    fn key_only(&self) -> bool {
        match self.core.cols {
            Either::Left(ref cols) => cols.is_empty(),
            Either::Right(_) => false, // TODO: true when index is not uniq index.
        }
    }

    fn get_rows_from_range(&mut self, range: KeyRange) -> Result<usize> {
        let mut row_count = 0;
        if is_point(&range) {
            CORP_GET_OR_SCAN_COUNT.with_label_values(&["point"]).inc();
            let value = match try!(self.snap
                .get(&Key::from_raw(range.get_start()), &mut self.statistics)) {
                None => return Ok(0),
                Some(v) => v,
            };
            let values = {
                let ids = self.core.cols.as_ref().left().unwrap();
                box_try!(table::cut_row(value, ids))
            };
            let h = box_try!(table::decode_handle(range.get_start()));
            row_count += try!(self.core.handle_row(h, values));
        } else {
            CORP_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
            let mut seek_key = if self.core.desc_scan {
                range.get_end().to_vec()
            } else {
                range.get_start().to_vec()
            };
            let upper_bound = if !self.core.desc_scan && !range.get_end().is_empty() {
                Some(Key::from_raw(range.get_end()).encoded().clone())
            } else {
                None
            };
            let mut scanner = try!(self.snap.scanner(if self.core.desc_scan {
                                                         ScanMode::Backward
                                                     } else {
                                                         ScanMode::Forward
                                                     },
                                                     self.key_only(),
                                                     upper_bound,
                                                     self.statistics));
            while self.core.limit > row_count {
                if row_count & REQUEST_CHECKPOINT == 0 {
                    try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
                }
                let kv = if self.core.desc_scan {
                    try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
                } else {
                    try!(scanner.seek(Key::from_raw(&seek_key)))
                };
                let (key, value) = match kv {
                    Some((key, value)) => (box_try!(key.raw()), value),
                    None => break,
                };
                if range.get_start() > key.as_slice() || range.get_end() <= key.as_slice() {
                    debug!("key: {} out of range [{}, {})",
                           escape(&key),
                           escape(range.get_start()),
                           escape(range.get_end()));
                    break;
                }
                let h = box_try!(table::decode_handle(&key));
                let row_data = {
                    let ids = self.core.cols.as_ref().left().unwrap();
                    box_try!(table::cut_row(value, ids))
                };
                row_count += try!(self.core.handle_row(h, row_data));
                seek_key = if self.core.desc_scan {
                    box_try!(table::truncate_as_row_key(&key)).to_vec()
                } else {
                    prefix_next(&key)
                };
            }
        }
        Ok(row_count)
    }

    fn get_rows_from_idx(&mut self, ranges: Vec<KeyRange>) -> Result<()> {
        let mut collected = 0;
        for r in ranges {
            if collected >= self.core.limit {
                break;
            }
            collected += try!(self.get_idx_row_from_range(r));
            try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
        }
        if self.core.topn {
            self.core.collect_topn_rows()
        } else if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(())
        }
    }

    fn get_idx_row_from_range(&mut self, r: KeyRange) -> Result<usize> {
        let mut row_cnt = 0;
        let mut seek_key = if self.core.desc_scan {
            r.get_end().to_vec()
        } else {
            r.get_start().to_vec()
        };
        CORP_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
        let upper_bound = if !self.core.desc_scan && !r.get_end().is_empty() {
            Some(Key::from_raw(r.get_end()).encoded().clone())
        } else {
            None
        };
        let mut scanner = try!(self.snap.scanner(if self.core.desc_scan {
                                                     ScanMode::Backward
                                                 } else {
                                                     ScanMode::Forward
                                                 },
                                                 self.key_only(),
                                                 upper_bound,
                                                 self.statistics));
        while row_cnt < self.core.limit {
            if row_cnt & REQUEST_CHECKPOINT == 0 {
                try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
            }
            let nk = if self.core.desc_scan {
                try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
            } else {
                try!(scanner.seek(Key::from_raw(&seek_key)))
            };
            let (key, val) = match nk {
                Some((key, val)) => (box_try!(key.raw()), val),
                None => break,
            };
            if r.get_start() > key.as_slice() || r.get_end() <= key.as_slice() {
                debug!("key: {} out of range [{}, {})",
                       escape(&key),
                       escape(r.get_start()),
                       escape(r.get_end()));
                break;
            }
            seek_key = if self.core.desc_scan {
                key.clone()
            } else {
                prefix_next(&key)
            };
            {
                let (mut values, handle) = {
                    let mut ids = self.core.cols.as_ref().right().unwrap().clone();
                    if self.core.pk_col.is_some() {
                        ids.pop();
                    }
                    box_try!(table::cut_idx_key(key, &ids))
                };
                let handle = if handle.is_none() {
                    box_try!(val.as_slice().read_i64::<BigEndian>())
                } else {
                    handle.unwrap()
                };
                if let Some(ref pk_col) = self.core.pk_col {
                    let handle_datum = if mysql::has_unsigned_flag(pk_col.get_flag() as u64) {
                        // PK column is unsigned
                        datum::Datum::U64(handle as u64)
                    } else {
                        datum::Datum::I64(handle)
                    };
                    let mut bytes = box_try!(datum::encode_key(&[handle_datum]));
                    values.append(pk_col.get_column_id(), &mut bytes);
                }
                row_cnt += try!(self.core.handle_row(handle, values));
            }
        }
        Ok(row_cnt)
    }
}

pub const STR_REQ_TYPE_SELECT: &'static str = "select";
pub const STR_REQ_TYPE_INDEX: &'static str = "index";
pub const STR_REQ_TYPE_DAG: &'static str = "dag";
pub const STR_REQ_TYPE_UNKNOWN: &'static str = "unknown";

#[inline]
pub fn get_req_type_str(tp: i64) -> &'static str {
    match tp {
        REQ_TYPE_SELECT => STR_REQ_TYPE_SELECT,
        REQ_TYPE_INDEX => STR_REQ_TYPE_INDEX,
        REQ_TYPE_DAG => STR_REQ_TYPE_DAG,
        _ => STR_REQ_TYPE_UNKNOWN,
    }
}

pub const STR_REQ_PRI_LOW: &'static str = "low";
pub const STR_REQ_PRI_NORMAL: &'static str = "normal";
pub const STR_REQ_PRI_HIGH: &'static str = "high";

#[inline]
pub fn get_req_pri_str(pri: CommandPri) -> &'static str {
    match pri {
        CommandPri::Low => STR_REQ_PRI_LOW,
        CommandPri::Normal => STR_REQ_PRI_NORMAL,
        CommandPri::High => STR_REQ_PRI_HIGH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coprocessor::endpoint::TopNHeap;
    use util::worker::Worker;
    use storage::engine::{self, TEMP_DIR};

    use kvproto::coprocessor::Request;

    use tipb::expression::{Expr, ExprType, ByItem};

    use util::collections::HashMap;
    use util::codec::number::*;
    use coprocessor::codec::Datum;
    use coprocessor::codec::table::RowColsDict;
    use coprocessor::xeval::EvalContext;

    use std::rc::Rc;
    use std::sync::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_get_req_type_str() {
        assert_eq!(get_req_type_str(REQ_TYPE_SELECT), STR_REQ_TYPE_SELECT);
        assert_eq!(get_req_type_str(REQ_TYPE_INDEX), STR_REQ_TYPE_INDEX);
        assert_eq!(get_req_type_str(REQ_TYPE_DAG), STR_REQ_TYPE_DAG);
        assert_eq!(get_req_type_str(0), STR_REQ_TYPE_UNKNOWN);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let end_point = Host::new(engine, worker.scheduler(), 1);
        worker.start_batch(end_point, 30).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(Request::new(),
                                        box move |msg| {
                                            tx.send(msg).unwrap();
                                        });
        task.deadline -= Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS);
        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut end_point = Host::new(engine, worker.scheduler(), 1);
        end_point.max_running_task_count = 3;
        worker.start_batch(end_point, 30).unwrap();
        let (tx, rx) = mpsc::channel();
        for pos in 0..30 * 4 {
            let tx = tx.clone();
            let mut req = Request::new();
            if pos % 3 == 0 {
                req.mut_context().set_priority(CommandPri::Low);
            } else if pos % 3 == 1 {
                req.mut_context().set_priority(CommandPri::Normal);
            } else {
                req.mut_context().set_priority(CommandPri::High);
            }
            let task = RequestTask::new(req,
                                        box move |msg| {
                                            thread::sleep(Duration::from_millis(100));
                                            let _ = tx.send(msg);
                                        });
            worker.schedule(Task::Request(task)).unwrap();
        }
        for _ in 0..120 {
            let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if !resp.has_region_error() {
                continue;
            }
            assert!(resp.get_region_error().has_server_is_busy());
            return;
        }
        panic!("suppose to get ServerIsBusy error.");
    }

    fn new_order_by(col_id: i64, desc: bool) -> ByItem {
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col_id).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        item
    }

    #[test]
    fn test_topn_heap() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(5).unwrap();
        let test_data = vec![
            (1, String::from("data1"), Datum::Null, Datum::I64(1)),
            (2, String::from("data2"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
            (5, String::from("data5"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(6)),
            (6, String::from("data6"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(4)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
        ];

        let exp = vec![
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap.try_add_row(handle as i64,
                             row_data,
                             cur_key,
                             order_cols.clone(),
                             ctx.clone())
                .unwrap();
        }
        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_keys: Vec<Datum> = vec![name, count];
            assert_eq!(row.handle, handle);
            assert_eq!(row.key, exp_keys);
        }
    }

    #[test]
    fn test_topn_heap_with_cmp_error() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(5).unwrap();

        let std_key: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"name:1".to_vec());
        topn_heap.try_add_row(0 as i64, row_data, std_key, order_cols.clone(), ctx.clone())
            .unwrap();

        let std_key2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap.try_add_row(0 as i64,
                         row_data2,
                         std_key2,
                         order_cols.clone(),
                         ctx.clone())
            .unwrap();

        let bad_key1: Vec<Datum> = vec![Datum::I64(2), Datum::Bytes(b"aaa".to_vec())];
        let row_data3 = RowColsDict::new(HashMap::default(), b"name:3".to_vec());

        assert!(topn_heap.try_add_row(0 as i64,
                         row_data3,
                         bad_key1,
                         order_cols.clone(),
                         ctx.clone())
            .is_err());

        assert!(topn_heap.into_sorted_vec().is_err());
    }

    #[test]
    fn test_topn_heap_with_few_data() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(10).unwrap();
        let test_data = vec![
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
        ];

        let exp = vec![
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap.try_add_row(handle as i64,
                             row_data,
                             cur_key,
                             order_cols.clone(),
                             ctx.clone())
                .unwrap();
        }

        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_keys: Vec<Datum> = vec![name, count];
            assert_eq!(row.handle, handle);
            assert_eq!(row.key, exp_keys);
        }
    }

    #[test]
    fn test_topn_limit_oom() {
        let topn_heap = TopNHeap::new(usize::MAX - 1);
        assert!(topn_heap.is_ok());
        let topn_heap = TopNHeap::new(usize::MAX);
        assert!(topn_heap.is_err());
    }
}
