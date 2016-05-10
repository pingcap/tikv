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

use std::sync::Arc;
use std::collections::HashMap;
use std::{result, error};
use std::time::Instant;
use std::boxed::FnBox;

use tipb::select::{self, SelectRequest, SelectResponse, Row};
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use threadpool::ThreadPool;

use storage::{Engine, SnapshotStore, engine, txn, mvcc};
use kvproto::kvrpcpb::{Context, LockInfo};
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use kvproto::errorpb;
use storage::Key;
use util::codec::number::NumberDecoder;
use util::codec::datum::DatumDecoder;
use util::codec::{Datum, table, datum};
use util::xeval::Evaluator;
use util::{as_slice, escape};
use util::SlowTimer;
use super::OnResponse;

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;

const DEFAULT_ERROR_CODE: i32 = 1;

// TODO: make this number configurable.
const DEFAULT_POOL_SIZE: usize = 8;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
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

impl From<engine::Error> for Error {
    fn from(e: engine::Error) -> Error {
        match e {
            engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<txn::Error> for Error {
    fn from(e: txn::Error) -> Error {
        match e {
            txn::Error::Mvcc(mvcc::Error::KeyIsLocked { primary, ts, key }) => {
                let mut info = LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

pub struct EndPointHost {
    snap_endpoint: Arc<TiDbEndPoint>,
    pool: ThreadPool,
}

impl EndPointHost {
    pub fn new(engine: Arc<Box<Engine>>) -> EndPointHost {
        EndPointHost {
            snap_endpoint: Arc::new(TiDbEndPoint::new(engine)),
            pool: ThreadPool::new(DEFAULT_POOL_SIZE),
        }
    }

    pub fn on_request(&self, req: Request, on_resp: OnResponse) {
        let end_point = self.snap_endpoint.clone();
        self.pool.execute(move || {
            let timer = SlowTimer::new();
            let tp = req.get_tp();
            end_point.handle_request(req, on_resp);
            slow_log!(timer, "request type {} takes {:?}", tp, timer.elapsed());
        });
    }
}

type ResponseHandler = Box<FnBox(Response) -> ()>;

fn on_error(e: Error, cb: ResponseHandler) {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => resp.set_region_error(e),
        Error::Locked(info) => resp.set_locked(info),
        Error::Other(_) => resp.set_other_error(format!("{}", e)),
    }
    cb(resp)
}

pub struct TiDbEndPoint {
    engine: Arc<Box<Engine>>,
}

impl TiDbEndPoint {
    pub fn new(engine: Arc<Box<Engine>>) -> TiDbEndPoint {
        // TODO: Spawn a new thread for handling requests asynchronously.
        TiDbEndPoint { engine: engine }
    }

    fn new_snapshot<'a>(&'a self, ctx: &Context, start_ts: u64) -> Result<SnapshotStore<'a>> {
        let snapshot = try!(self.engine.snapshot(ctx));
        Ok(SnapshotStore::new(snapshot, start_ts))
    }
}

impl TiDbEndPoint {
    fn handle_request(&self, req: Request, on_resp: OnResponse) {
        let cb = box move |r| {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CopResp);
            resp_msg.set_cop_resp(r);
            on_resp.call_box((resp_msg,));
        };
        match req.get_tp() {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                if let Err(e) = sel.merge_from_bytes(req.get_data()) {
                    on_error(box_err!(e), cb);
                    return;
                }
                match self.handle_select(req, sel) {
                    Ok(r) => cb(r),
                    Err(e) => on_error(e, cb),
                }
            }
            t => on_error(box_err!("unsupported tp {}", t), cb),
        }
    }

    pub fn handle_select(&self, mut req: Request, sel: SelectRequest) -> Result<Response> {
        let ts = Instant::now();
        let snap = try!(self.new_snapshot(req.get_context(), sel.get_start_ts()));
        debug!("[TIME_SNAPSHOT] {:?}", ts.elapsed());
        let mut ctx = try!(SelectContext::new(sel, snap));
        let range = req.take_ranges().into_vec();
        debug!("scanning range: {:?}", range);
        let sel_ts = Instant::now();
        let res = if req.get_tp() == REQ_TYPE_SELECT {
            ctx.get_rows_from_sel(range)
        } else {
            ctx.get_rows_from_idx(range)
        };
        debug!("[TIME_SELECT] {:?} {:?}", req.get_tp(), sel_ts.elapsed());
        let resp_ts = Instant::now();
        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(rows) => sel_resp.set_rows(RepeatedField::from_vec(rows)),
            Err(e) => {
                if let Error::Other(_) = e {
                    // should we handle locked here too?
                    sel_resp.set_error(to_pb_error(&e));
                    // TODO add detail error
                    resp.set_other_error(format!("{}", e));
                } else {
                    // other error should be handle by ti client.
                    return Err(e);
                }
            }
        }
        let data = box_try!(sel_resp.write_to_bytes());
        resp.set_data(data);
        debug!("[TIME_COMPOSE_RESP] {:?}", resp_ts.elapsed());
        Ok(resp)
    }
}

fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(DEFAULT_ERROR_CODE);
    e.set_msg(format!("{}", err));
    e
}

fn prefix_next(key: &[u8]) -> Vec<u8> {
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
fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

struct SelectContext<'a> {
    sel: SelectRequest,
    snap: SnapshotStore<'a>,
    eval: Evaluator,
    cond_cols: HashMap<i64, ColumnInfo>,
}

fn collect_col_in_expr(cols: &mut HashMap<i64, ColumnInfo>,
                       col_meta: &[ColumnInfo],
                       expr: &Expr)
                       -> Result<()> {
    if expr.get_tp() == ExprType::ColumnRef {
        let i = box_try!(expr.get_val().decode_i64());
        for c in col_meta {
            if c.get_column_id() == i {
                cols.insert(i, c.clone());
                return Ok(());
            }
        }
        return Err(box_err!("column meta of {} is missing", i));
    }
    for c in expr.get_children() {
        try!(collect_col_in_expr(cols, col_meta, c));
    }
    Ok(())
}

impl<'a> SelectContext<'a> {
    fn new(sel: SelectRequest, snap: SnapshotStore<'a>) -> Result<SelectContext<'a>> {
        let mut ctx = SelectContext {
            sel: sel,
            snap: snap,
            eval: Default::default(),
            cond_cols: Default::default(),
        };
        if !ctx.sel.has_field_where() {
            return Ok(ctx);
        }
        {
            let cols = if ctx.sel.has_table_info() {
                ctx.sel.get_table_info().get_columns()
            } else {
                ctx.sel.get_index_info().get_columns()
            };
            try!(collect_col_in_expr(&mut ctx.cond_cols, cols, ctx.sel.get_field_where()));
        }
        Ok(ctx)
    }

    fn get_rows_from_sel(&mut self, ranges: Vec<KeyRange>) -> Result<Vec<Row>> {
        let mut rows = vec![];
        for ran in ranges {
            let ran_rows = try!(self.get_rows_from_range(ran));
            rows.extend(ran_rows);
        }
        Ok(rows)
    }

    fn get_rows_from_range(&mut self, mut range: KeyRange) -> Result<Vec<Row>> {
        let mut rows = vec![];
        if is_point(&range) {
            if let None = try!(self.snap.get(&Key::from_raw(range.get_start()))) {
                return Ok(rows);
            }
            let h = box_try!(table::decode_handle(range.get_start()));
            if try!(self.should_skip(h)) {
                return Ok(rows);
            }
            if let Some(row) = try!(self.get_row_by_handle(h)) {
                rows.push(row);
            }
        } else {
            let mut seek_key = range.take_start();
            loop {
                trace!("seek {}", escape(&seek_key));
                let timer = Instant::now();
                let mut res = try!(self.snap.scan(Key::from_raw(&seek_key), 1));
                trace!("scan takes {:?}", timer.elapsed());
                if res.is_empty() {
                    debug!("no more data to scan.");
                    break;
                }
                let (key, _) = try!(res.pop().unwrap());
                if range.get_end() <= &key {
                    debug!("reach end key: {} >= {}",
                           escape(&key),
                           escape(range.get_end()));
                    break;
                }
                let h = box_try!(table::decode_handle(&key));
                if try!(self.should_skip(h)) {
                    seek_key = prefix_next(&key);
                    continue;
                }
                if let Some(row) = try!(self.get_row_by_handle(h)) {
                    rows.push(row);
                }
                seek_key = prefix_next(&key);
            }
        }
        Ok(rows)
    }

    fn should_skip(&mut self, h: i64) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        let t_id = self.sel.get_table_info().get_table_id();
        for (&col_id, col) in &self.cond_cols {
            if col.get_pk_handle() {
                self.eval.row.insert(col_id, Datum::I64(h));
            } else {
                let key = table::encode_column_key(t_id, h, col_id);
                let data = try!(self.snap.get(&Key::from_raw(&key)));
                if data.is_none() {
                    return Err(box_err!("data is missing for [{}, {}, {}]", t_id, h, col_id));
                }
                let value = box_try!(data.unwrap().as_slice().decode_datum());
                self.eval.row.insert(col_id, value);
            }
        }
        let res = box_try!(self.eval.eval(self.sel.get_field_where()));
        let b = box_try!(res.into_bool());
        Ok(b.map_or(false, |v| !v))
    }

    fn get_row_by_handle(&self, h: i64) -> Result<Option<Row>> {
        let tid = self.sel.get_table_info().get_table_id();
        let columns = self.sel.get_table_info().get_columns();
        let mut row = Row::new();
        let handle = box_try!(datum::encode_value(&[Datum::I64(h)]));
        for col in columns {
            if col.get_pk_handle() {
                row.mut_data().extend(handle.clone());
            } else {
                let col_id = col.get_column_id();
                if self.cond_cols.contains_key(&col_id) {
                    let d = &self.eval.row[&col_id];
                    let bytes = box_try!(datum::encode_value(as_slice(d)));
                    row.mut_data().extend(bytes);
                } else {
                    let raw_key = table::encode_column_key(tid, h, col.get_column_id());
                    let key = Key::from_raw(&raw_key);
                    match try!(self.snap.get(&key)) {
                        None => return Err(box_err!("key {} not exists", key)),
                        Some(bs) => row.mut_data().extend(bs),
                    }
                }
            }
        }
        row.set_handle(handle);
        Ok(Some(row))
    }

    fn get_rows_from_idx(&self, ranges: Vec<KeyRange>) -> Result<Vec<Row>> {
        let mut rows = vec![];
        for r in ranges {
            let part = try!(self.get_idx_row_from_range(r));
            rows.extend(part);
        }
        Ok(rows)
    }

    fn get_idx_row_from_range(&self, mut r: KeyRange) -> Result<Vec<Row>> {
        let mut rows = vec![];
        let info = self.sel.get_index_info();
        let mut seek_key = r.take_start();
        loop {
            trace!("seek {}", escape(&seek_key));
            let timer = Instant::now();
            let mut nk = try!(self.snap.scan(Key::from_raw(&seek_key), 1));
            trace!("scan takes {:?}", timer.elapsed());
            if nk.is_empty() {
                debug!("no more data to scan");
                return Ok(rows);
            }
            let (key, value) = try!(nk.pop().unwrap());
            if r.get_end() <= &key {
                debug!("reach end key: {} >= {}", escape(&key), escape(r.get_end()));
                return Ok(rows);
            }
            let mut datums = box_try!(table::decode_index_key(&key));
            let handle = if datums.len() > info.get_columns().len() {
                datums.pop().unwrap()
            } else {
                let h = box_try!((&*value).read_i64::<BigEndian>());
                Datum::I64(h)
            };
            let data = box_try!(datum::encode_value(&datums));
            let handle_data = box_try!(datum::encode_value(&[handle]));
            let mut row = Row::new();
            row.set_handle(handle_data);
            row.set_data(data);
            rows.push(row);
            seek_key = prefix_next(&key);
        }
    }
}
