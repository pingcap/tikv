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

use futures::sync::{mpsc as futures_mpsc, oneshot};
use futures::{Future, Stream};
use std::collections::{BTreeMap, HashMap};
use std::i64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::{cmp, mem};

use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::kvrpcpb::Context;
use protobuf::{Message, RepeatedField};
use tikv::coprocessor::codec::chunk::Chunk as ArrowChunk;
use tikv::coprocessor::codec::mysql::types;
use tikv::coprocessor::codec::{datum, table, Datum};
use tikv::coprocessor::*;
use tikv::server::readpool::{self, ReadPool};
use tikv::server::{Config, OnResponse};
use tikv::storage::engine::{self, Engine, RocksEngine, TEMP_DIR};
use tikv::storage::{self, Key, Mutation, ALL_CFS};
use tikv::util::codec::number::*;
use tikv::util::worker::{Builder as WorkerBuilder, FutureWorker, Worker};
use tipb::executor::{
    Aggregation, ExecType, Executor, IndexScan, Limit, Selection, TableScan, TopN,
};
use tipb::expression::{ByItem, Expr, ExprType, FieldType, ScalarFuncSig};
use tipb::schema::{self, ColumnInfo};
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};

use storage::sync_storage::SyncStorage;
use storage::util::new_raft_engine;

const FLAG_IGNORE_TRUNCATE: u64 = 1;
const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);

const TYPE_VAR_CHAR: i32 = 15;
const TYPE_LONG: i32 = 8;

pub fn next_id() -> i64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed) as i64
}

fn field_type(tp: i32) -> FieldType {
    let mut ft = FieldType::new();
    ft.set_tp(tp);
    ft
}

fn unsigned_long() -> FieldType {
    let mut count_col = field_type(TYPE_LONG);
    count_col.set_flag(types::UNSIGNED_FLAG as u32);
    count_col
}

fn supported_encode_types() -> Vec<EncodeType> {
    vec![EncodeType::TypeDefault, EncodeType::TypeArrow]
}

fn field_types_from_expr(exprs: &[Expr]) -> Vec<FieldType> {
    let mut field_types = Vec::with_capacity(exprs.len());
    for expr in exprs {
        if expr.get_tp() == ExprType::Avg {
            field_types.push(unsigned_long());
        }
        field_types.push(expr.get_field_type().clone());
    }
    field_types
}

struct DefaultChunkSplitter {
    chunks: Vec<Chunk>,
    datums: Vec<Datum>,
    col_cnt: usize,
}

impl DefaultChunkSplitter {
    fn new(chunks: Vec<Chunk>, col_cnt: usize) -> DefaultChunkSplitter {
        DefaultChunkSplitter {
            chunks,
            col_cnt,
            datums: Vec::with_capacity(0),
        }
    }

    fn assert_chunk_datum_count(&self, datum_limit: usize) {
        let mut iter = self.chunks.iter();
        let res = iter.any(|x| datum::decode(&mut x.get_rows_data()).unwrap().len() != datum_limit);
        if res {
            assert!(iter.next().is_none());
        }
    }
}

impl Iterator for DefaultChunkSplitter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>> {
        loop {
            if self.chunks.is_empty() && self.datums.is_empty() {
                return None;
            } else if self.datums.is_empty() {
                let chunk = self.chunks.remove(0);
                let mut data = chunk.get_rows_data();
                self.datums = datum::decode(&mut data).unwrap();
                continue;
            }
            assert_eq!(self.datums.len() >= self.col_cnt, true);
            let mut cols = self.datums.split_off(self.col_cnt);
            mem::swap(&mut self.datums, &mut cols);
            return Some(cols);
        }
    }
}

struct ArrowChunkSplitter {
    row_id: usize,
    chunks: Vec<ArrowChunk>,
    tps: Vec<FieldType>,
}

impl ArrowChunkSplitter {
    fn new(mut buf: &[u8], tps: Vec<FieldType>) -> ArrowChunkSplitter {
        let mut chunks = Vec::new();
        while !buf.is_empty() {
            chunks.push(ArrowChunk::decode(&mut buf, &tps).unwrap());
        }
        ArrowChunkSplitter {
            chunks,
            tps,
            row_id: 0,
        }
    }

    fn assert_chunk_datum_count(&self, datum_limit: usize) {
        let mut iter = self.chunks.iter();
        let res = iter.any(|c| c.num_rows() * c.num_cols() != datum_limit);
        if res {
            assert!(iter.next().is_none());
        }
    }
}

impl Iterator for ArrowChunkSplitter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>> {
        while !self.chunks.is_empty() && self.chunks[0].get_row(self.row_id).is_none() {
            self.chunks.remove(0);
            self.row_id = 0;
        }
        if self.chunks.is_empty() {
            return None;
        }
        let row = self.chunks[0].get_row(self.row_id).unwrap();

        self.row_id += 1;
        Some(
            self.tps
                .iter()
                .enumerate()
                .map(|(col_id, tp)| row.get_datum(col_id, tp).unwrap())
                .collect(),
        )
    }
}

enum DAGChunkSplitter {
    DEFAULT(DefaultChunkSplitter),
    ARROW(ArrowChunkSplitter),
}

impl DAGChunkSplitter {
    fn new(
        encode_type: EncodeType,
        col_tps: &[FieldType],
        resp: &mut SelectResponse,
    ) -> DAGChunkSplitter {
        match encode_type {
            EncodeType::TypeDefault => {
                let splitter =
                    DefaultChunkSplitter::new(resp.take_chunks().into_vec(), col_tps.len());
                DAGChunkSplitter::DEFAULT(splitter)
            }
            EncodeType::TypeArrow => {
                let splitter = ArrowChunkSplitter::new(resp.get_row_batch_data(), col_tps.to_vec());
                DAGChunkSplitter::ARROW(splitter)
            }
        }
    }

    fn new_with_stream_response(
        encode_type: EncodeType,
        col_tps: &[FieldType],
        resp: &StreamResponse,
    ) -> DAGChunkSplitter {
        match encode_type {
            EncodeType::TypeDefault => {
                let mut chunk = Chunk::new();
                chunk.merge_from_bytes(resp.get_data()).unwrap();
                let splitter = DefaultChunkSplitter::new(vec![chunk], col_tps.len());
                DAGChunkSplitter::DEFAULT(splitter)
            }
            EncodeType::TypeArrow => {
                let splitter = ArrowChunkSplitter::new(resp.get_data(), col_tps.to_vec());
                DAGChunkSplitter::ARROW(splitter)
            }
        }
    }

    fn assert_chunk_datum_count(&self, datum_limit: usize) {
        match self {
            DAGChunkSplitter::DEFAULT(splitter) => splitter.assert_chunk_datum_count(datum_limit),
            DAGChunkSplitter::ARROW(splitter) => splitter.assert_chunk_datum_count(datum_limit),
        }
    }
}

impl Iterator for DAGChunkSplitter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>> {
        match self {
            DAGChunkSplitter::DEFAULT(splitter) => splitter.next(),
            DAGChunkSplitter::ARROW(splitter) => splitter.next(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Column {
    id: i64,
    col_type: i32,
    // negative means not a index key, 0 means primary key, positive means normal index key.
    pub index: i64,
    default_val: Option<i64>, // TODO: change it to Vec<u8> if other type value is needed for test.
}

struct ColumnBuilder {
    col_type: i32,
    index: i64,
    default_val: Option<i64>,
}

impl ColumnBuilder {
    fn new() -> ColumnBuilder {
        ColumnBuilder {
            col_type: TYPE_LONG,
            index: -1,
            default_val: None,
        }
    }

    fn col_type(mut self, t: i32) -> ColumnBuilder {
        self.col_type = t;
        self
    }

    fn primary_key(mut self, b: bool) -> ColumnBuilder {
        if b {
            self.index = 0;
        } else {
            self.index = -1;
        }
        self
    }

    fn index_key(mut self, idx_id: i64) -> ColumnBuilder {
        self.index = idx_id;
        self
    }

    fn default(mut self, val: i64) -> ColumnBuilder {
        self.default_val = Some(val);
        self
    }

    fn build(self) -> Column {
        Column {
            id: next_id(),
            col_type: self.col_type,
            index: self.index,
            default_val: self.default_val,
        }
    }
}

pub struct Table {
    id: i64,
    handle_id: i64,
    cols: BTreeMap<i64, Column>,
    idxs: BTreeMap<i64, Vec<i64>>,
}

impl Table {
    fn get_table_info(&self) -> schema::TableInfo {
        let mut tb_info = schema::TableInfo::new();
        tb_info.set_table_id(self.id);
        tb_info.set_columns(RepeatedField::from_vec(self.get_table_columns()));
        tb_info
    }

    pub fn get_table_columns(&self) -> Vec<ColumnInfo> {
        let mut tb_info = Vec::new();
        for col in self.cols.values() {
            let mut c_info = ColumnInfo::new();
            c_info.set_column_id(col.id);
            c_info.set_tp(col.col_type);
            c_info.set_pk_handle(col.index == 0);
            if let Some(dv) = col.default_val {
                c_info.set_default_val(datum::encode_value(&[Datum::I64(dv)]).unwrap())
            }
            tb_info.push(c_info);
        }
        tb_info
    }

    fn get_index_info(&self, index: i64, store_handle: bool) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.id);
        idx_info.set_index_id(index);
        let mut has_pk = false;
        for col_id in &self.idxs[&index] {
            let col = self.cols[col_id];
            let mut c_info = ColumnInfo::new();
            c_info.set_tp(col.col_type);
            c_info.set_column_id(col.id);
            if col.id == self.handle_id {
                c_info.set_pk_handle(true);
                has_pk = true
            }
            idx_info.mut_columns().push(c_info);
        }
        if !has_pk && store_handle {
            let mut handle_info = ColumnInfo::new();
            handle_info.set_tp(TYPE_LONG);
            handle_info.set_column_id(-1);
            handle_info.set_pk_handle(true);
            idx_info.mut_columns().push(handle_info);
        }
        idx_info
    }

    pub fn get_select_range(&self) -> KeyRange {
        let mut range = KeyRange::new();
        range.set_start(table::encode_row_key(self.id, i64::MIN));
        range.set_end(table::encode_row_key(self.id, i64::MAX));
        range
    }

    pub fn get_index_range(&self, idx: i64) -> KeyRange {
        let mut range = KeyRange::new();
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(i64::MIN).unwrap();
        range.set_start(table::encode_index_seek_key(self.id, idx, &buf));
        buf.clear();
        buf.encode_i64(i64::MAX).unwrap();
        range.set_end(table::encode_index_seek_key(self.id, idx, &buf));
        range
    }
}

struct TableBuilder {
    handle_id: i64,
    cols: BTreeMap<i64, Column>,
}

impl TableBuilder {
    fn new() -> TableBuilder {
        TableBuilder {
            handle_id: -1,
            cols: BTreeMap::new(),
        }
    }

    fn add_col(mut self, col: Column) -> TableBuilder {
        if col.index == 0 {
            if self.handle_id > 0 {
                self.handle_id = 0;
            } else if self.handle_id < 0 {
                // maybe need to check type.
                self.handle_id = col.id;
            }
        }
        self.cols.insert(col.id, col);
        self
    }

    fn build(mut self) -> Table {
        if self.handle_id <= 0 {
            self.handle_id = next_id();
        }
        let mut idx = BTreeMap::new();
        for (&id, col) in &self.cols {
            if col.index < 0 {
                continue;
            }
            let e = idx.entry(col.index).or_insert_with(Vec::new);
            e.push(id);
        }
        for (id, val) in &mut idx {
            if *id == 0 {
                continue;
            }
            // TODO: support uniq index.
            val.push(self.handle_id);
        }
        Table {
            id: next_id(),
            handle_id: self.handle_id,
            cols: self.cols,
            idxs: idx,
        }
    }
}

struct Insert<'a, E: Engine> {
    store: &'a mut Store<E>,
    table: &'a Table,
    values: BTreeMap<i64, Datum>,
}

impl<'a, E: Engine> Insert<'a, E> {
    fn new(store: &'a mut Store<E>, table: &'a Table) -> Self {
        Insert {
            store,
            table,
            values: BTreeMap::new(),
        }
    }

    fn set(mut self, col: Column, value: Datum) -> Self {
        assert!(self.table.cols.contains_key(&col.id));
        self.values.insert(col.id, value);
        self
    }

    fn execute(self) -> i64 {
        self.execute_with_ctx(Context::new())
    }

    fn execute_with_ctx(self, ctx: Context) -> i64 {
        let handle = self
            .values
            .get(&self.table.handle_id)
            .cloned()
            .unwrap_or_else(|| Datum::I64(next_id()));
        let key = table::encode_row_key(self.table.id, handle.i64());
        let ids: Vec<_> = self.values.keys().cloned().collect();
        let values: Vec<_> = self.values.values().cloned().collect();
        let value = table::encode_row(values, &ids).unwrap();
        let mut kvs = vec![];
        kvs.push((key, value));
        for (&id, idxs) in &self.table.idxs {
            let mut v: Vec<_> = idxs.iter().map(|id| self.values[id].clone()).collect();
            v.push(handle.clone());
            let encoded = datum::encode_key(&v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, id, &encoded);
            kvs.push((idx_key, vec![0]));
        }
        self.store.put(ctx, kvs);
        handle.i64()
    }
}

struct Delete<'a, E: Engine> {
    store: &'a mut Store<E>,
    table: &'a Table,
}

impl<'a, E: Engine> Delete<'a, E> {
    fn new(store: &'a mut Store<E>, table: &'a Table) -> Self {
        Delete { store, table }
    }

    fn execute(self, id: i64, row: Vec<Datum>) {
        let mut values = HashMap::new();
        for (&id, v) in self.table.cols.keys().zip(row) {
            values.insert(id, v);
        }
        let key = table::encode_row_key(self.table.id, id);
        let mut keys = vec![];
        keys.push(key);
        for (&idx_id, idx_cols) in &self.table.idxs {
            let mut v: Vec<_> = idx_cols.iter().map(|id| values[id].clone()).collect();
            v.push(Datum::I64(id));
            let encoded = datum::encode_key(&v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, idx_id, &encoded);
            keys.push(idx_key);
        }
        self.store.delete(keys);
    }
}

pub struct Store<E: Engine> {
    store: SyncStorage<E>,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl<E: Engine> Store<E> {
    fn new(engine: E) -> Self {
        let pd_worker = FutureWorker::new("test-future–worker");
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || storage::ReadPoolContext::new(pd_worker.scheduler())
        });
        Self {
            store: SyncStorage::from_engine(engine, &Default::default(), read_pool),
            current_ts: 1,
            handles: vec![],
        }
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    fn begin(&mut self) {
        self.current_ts = next_id() as u64;
        self.handles.clear();
    }

    fn insert_into<'a>(&'a mut self, table: &'a Table) -> Insert<'a, E> {
        Insert::new(self, table)
    }

    fn put(&mut self, ctx: Context, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv
            .drain(..)
            .map(|(k, v)| Mutation::Put((Key::from_raw(&k), v)))
            .collect();
        self.store.prewrite(ctx, kv, pk, self.current_ts).unwrap();
    }

    fn delete_from<'a>(&'a mut self, table: &'a Table) -> Delete<'a, E> {
        Delete::new(self, table)
    }

    fn delete(&mut self, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys
            .drain(..)
            .map(|k| Mutation::Delete(Key::from_raw(&k)))
            .collect();
        self.store
            .prewrite(Context::new(), mutations, pk, self.current_ts)
            .unwrap();
    }

    fn commit(&mut self) {
        self.commit_with_ctx(Context::new());
    }

    fn commit_with_ctx(&mut self, ctx: Context) {
        let handles = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        self.store
            .commit(ctx, handles, self.current_ts, next_id() as u64)
            .unwrap();
    }
}

/// An example table for test purpose.
pub struct ProductTable {
    pub id: Column,
    pub name: Column,
    pub count: Column,
    pub table: Table,
}

impl ProductTable {
    pub fn new() -> ProductTable {
        let id = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .primary_key(true)
            .build();
        let idx_id = next_id();
        let name = ColumnBuilder::new()
            .col_type(TYPE_VAR_CHAR)
            .index_key(idx_id)
            .build();
        let count = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .index_key(idx_id)
            .build();
        let table = TableBuilder::new()
            .add_col(id)
            .add_col(name)
            .add_col(count)
            .build();

        ProductTable {
            id,
            name,
            count,
            table,
        }
    }
}

fn init_data_with_engine_and_commit<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<E>, Worker<EndPointTask<E>>) {
    init_data_with_details(ctx, engine, tbl, vals, commit, Config::default())
}

fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: Config,
) -> (Store<E>, Worker<EndPointTask<E>>) {
    let mut store = Store::new(engine);

    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(&tbl.table)
            .set(tbl.id, Datum::I64(id))
            .set(tbl.name, name.map(|s| s.as_bytes()).into())
            .set(tbl.count, Datum::I64(count))
            .execute_with_ctx(ctx.clone());
    }
    if commit {
        store.commit_with_ctx(ctx);
    }
    let pd_worker = FutureWorker::new("test-pd-worker");
    let pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
        || ReadPoolContext::new(pd_worker.scheduler())
    });
    let mut end_point = WorkerBuilder::new("test select worker")
        .batch_size(5)
        .create();
    let runner = EndPointHost::new(store.get_engine(), end_point.scheduler(), &cfg, pool);
    end_point.start(runner).unwrap();

    (store, end_point)
}

pub fn init_data_with_commit(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<RocksEngine>, Worker<EndPointTask<RocksEngine>>) {
    let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
    init_data_with_engine_and_commit(Context::new(), engine, tbl, vals, commit)
}

// This function will create a Product table and initialize with the specified data.
fn init_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Worker<EndPointTask<RocksEngine>>) {
    init_data_with_commit(tbl, vals, true)
}

fn offset_for_column(cols: &[ColumnInfo], col_id: i64) -> i64 {
    for (offset, column) in cols.iter().enumerate() {
        if column.get_column_id() == col_id {
            return offset as i64;
        }
    }
    0 as i64
}

struct DAGSelect {
    execs: Vec<Executor>,
    cols: Vec<ColumnInfo>,
    order_by: Vec<ByItem>,
    limit: Option<u64>,
    aggregate: Vec<Expr>,
    group_by: Vec<Expr>,
    key_range: KeyRange,
    output_offsets: Option<Vec<u32>>,
    encode_type: EncodeType,
}

impl DAGSelect {
    fn from(table: &Table) -> DAGSelect {
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeTableScan);
        let mut tbl_scan = TableScan::new();
        let mut table_info = table.get_table_info();
        tbl_scan.set_table_id(table_info.get_table_id());
        let columns_info = table_info.take_columns();
        tbl_scan.set_columns(columns_info);
        exec.set_tbl_scan(tbl_scan);

        let mut range = KeyRange::new();
        range.set_start(table::encode_row_key(table.id, i64::MIN));
        range.set_end(table::encode_row_key(table.id, i64::MAX));

        DAGSelect {
            execs: vec![exec],
            cols: table.get_table_columns(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_range: range,
            output_offsets: None,
            encode_type: EncodeType::TypeDefault,
        }
    }

    fn from_index(table: &Table, index: Column) -> DAGSelect {
        let idx = index.index;
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeIndexScan);
        let mut scan = IndexScan::new();
        let mut index_info = table.get_index_info(idx, true);
        scan.set_table_id(index_info.get_table_id());
        scan.set_index_id(idx);

        let columns_info = index_info.take_columns();
        scan.set_columns(columns_info.clone());
        exec.set_idx_scan(scan);

        let range = table.get_index_range(idx);
        DAGSelect {
            execs: vec![exec],
            cols: columns_info.to_vec(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_range: range,
            output_offsets: None,
            encode_type: EncodeType::TypeDefault,
        }
    }

    fn limit(mut self, n: u64) -> DAGSelect {
        self.limit = Some(n);
        self
    }

    fn order_by(mut self, col: Column, desc: bool) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col_offset).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        self.order_by.push(item);
        self
    }

    fn count(mut self) -> DAGSelect {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::Count);
        expr.set_field_type(unsigned_long());
        self.aggregate.push(expr);
        self
    }

    fn aggr_col(mut self, col: Column, aggr_t: ExprType, field_type: FieldType) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut col_expr = Expr::new();
        col_expr.set_tp(ExprType::ColumnRef);
        col_expr.mut_val().encode_i64(col_offset).unwrap();
        let mut expr = Expr::new();
        expr.set_tp(aggr_t);
        expr.mut_children().push(col_expr);
        expr.set_field_type(field_type);
        self.aggregate.push(expr);
        self
    }

    fn first(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::First, field_type(col.col_type))
    }

    fn sum(self, col: Column) -> DAGSelect {
        self.aggr_col(
            col,
            ExprType::Sum,
            field_type(i32::from(types::NEW_DECIMAL)),
        )
    }

    fn avg(self, col: Column) -> DAGSelect {
        self.aggr_col(
            col,
            ExprType::Avg,
            field_type(i32::from(types::NEW_DECIMAL)),
        )
    }

    fn max(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Max, field_type(col.col_type))
    }

    fn min(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Min, field_type(col.col_type))
    }

    fn bit_and(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitAnd, unsigned_long())
    }

    fn bit_or(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitOr, unsigned_long())
    }

    fn bit_xor(self, col: Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitXor, unsigned_long())
    }

    fn group_by(mut self, cols: &[Column]) -> DAGSelect {
        for col in cols {
            let offset = offset_for_column(&self.cols, col.id);
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ColumnRef);
            expr.mut_val().encode_i64(offset).unwrap();
            let mut field_type = FieldType::new();
            field_type.set_tp(col.col_type);
            expr.set_field_type(field_type);
            self.group_by.push(expr);
        }
        self
    }

    fn output_offsets(mut self, output_offsets: Option<Vec<u32>>) -> DAGSelect {
        self.output_offsets = output_offsets;
        self
    }

    fn where_expr(mut self, expr: Expr) -> DAGSelect {
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeSelection);
        let mut selection = Selection::new();
        selection.mut_conditions().push(expr);
        exec.set_selection(selection);
        self.execs.push(exec);
        self
    }

    fn encode_type(mut self, encode_tp: EncodeType) -> DAGSelect {
        self.encode_type = encode_tp;
        self
    }

    fn build(self) -> (Request, Vec<FieldType>) {
        self.build_with(Context::new(), &[0])
    }

    fn build_with(mut self, ctx: Context, flags: &[u64]) -> (Request, Vec<FieldType>) {
        let mut field_types = vec![];
        if !self.aggregate.is_empty() || !self.group_by.is_empty() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeAggregation);
            let mut aggr = Aggregation::new();
            if !self.aggregate.is_empty() {
                field_types.extend(field_types_from_expr(&self.aggregate));
                aggr.set_agg_func(RepeatedField::from_vec(self.aggregate));
            }

            if !self.group_by.is_empty() {
                field_types.extend(self.group_by.iter().map(|e| e.get_field_type().clone()));
                aggr.set_group_by(RepeatedField::from_vec(self.group_by));
            }
            exec.set_aggregation(aggr);
            self.execs.push(exec);
        }

        if !self.order_by.is_empty() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeTopN);
            let mut topn = TopN::new();
            topn.set_order_by(RepeatedField::from_vec(self.order_by));
            if let Some(limit) = self.limit.take() {
                topn.set_limit(limit);
            }
            exec.set_topN(topn);
            self.execs.push(exec);
        }

        if let Some(l) = self.limit.take() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeLimit);
            let mut limit = Limit::new();
            limit.set_limit(l);
            exec.set_limit(limit);
            self.execs.push(exec);
        }

        let mut dag = DAGRequest::new();
        dag.set_executors(RepeatedField::from_vec(self.execs));
        dag.set_start_ts(next_id() as u64);
        dag.set_flags(flags.iter().fold(0, |acc, f| acc | *f));
        dag.set_collect_range_counts(true);

        let output_offsets = if self.output_offsets.is_some() {
            self.output_offsets.take().unwrap()
        } else {
            (0..self.cols.len() as u32).collect()
        };

        // it's not aggregation
        if field_types.is_empty() {
            for offset in &output_offsets {
                field_types.push(field_type(self.cols[*offset as usize].get_tp()));
            }
        }
        dag.set_output_offsets(output_offsets);
        dag.set_encode_type(self.encode_type);
        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());
        req.set_ranges(RepeatedField::from_vec(vec![self.key_range]));
        req.set_context(ctx);
        (req, field_types)
    }
}

#[test]
fn test_select() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in spliter.zip(data.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
        }
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_batch_row_limit() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];
    let batch_row_limit = 3;
    let chunk_datum_limit = batch_row_limit * 3; // we have 3 fields.
    let product = ProductTable::new();
    let (_, mut end_point) = {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_batch_row_limit = batch_row_limit;
        init_data_with_details(Context::new(), engine, &product, &data, true, cfg)
    };

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        splitter.assert_chunk_datum_count(chunk_datum_limit);
        for (row, (id, name, cnt)) in splitter.zip(data.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
        }
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_stream() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
        (8, Some("name:2"), 4),
    ];

    let product = ProductTable::new();
    let stream_row_limit = 2;
    let (_, mut end_point) = {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_stream_batch_row_limit = stream_row_limit;
        init_data_with_details(Context::new(), engine, &product, &data, true, cfg)
    };

    let expected_output_counts = vec![vec![2 as i64], vec![2 as i64], vec![1 as i64]];
    for encode_type in supported_encode_types() {
        let mut expected_ranges_last_byte = vec![(0, 3), (3, 6), (6, 255)];
        let check_range = move |resp: &Response| {
            let (start_last_byte, end_last_byte) = expected_ranges_last_byte.remove(0);
            let start = resp.get_range().get_start();
            let end = resp.get_range().get_end();
            assert_eq!(start[start.len() - 1], start_last_byte);
            assert_eq!(end[end.len() - 1], end_last_byte);
        };

        let (req, col_tps) = DAGSelect::from(&product.table)
            .encode_type(encode_type)
            .build();
        assert_eq!(req.get_ranges().len(), 1);
        let resps = handle_streaming_select(&end_point, req, check_range);
        assert_eq!(resps.len(), 3);
        for (i, resp) in resps.into_iter().enumerate() {
            assert_eq!(
                resp.get_output_counts(),
                expected_output_counts[i].as_slice()
            );

            let chunk_data_limit = stream_row_limit * 3; // we have 3 fields.
            let splitter = DAGChunkSplitter::new_with_stream_response(encode_type, &col_tps, &resp);
            splitter.assert_chunk_datum_count(chunk_data_limit);
            let j = cmp::min((i + 1) * stream_row_limit, data.len());
            let cur_data = &data[i * stream_row_limit..j];
            for (row, &(id, name, cnt)) in splitter.zip(cur_data) {
                let name_datum = name.map(|s| s.as_bytes()).into();
                let expected_encoded =
                    datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
                let result_encoded = datum::encode_value(&row).unwrap();
                assert_eq!(result_encoded, &*expected_encoded);
            }
        }
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_select_after_lease() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (cluster, raft_engine, ctx) = new_raft_engine(1, "");
    let (_, mut end_point) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    // Sleep until the leader lease is expired.
    thread::sleep(cluster.cfg.raft_store.raft_store_max_leader_lease.0);
    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .encode_type(encode_type)
            .build_with(ctx.clone(), &[0]);
        let mut resp = handle_select(&end_point, req);
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in splitter.zip(data.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
        }
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_scan_detail() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_batch_row_limit = 50;
        init_data_with_details(Context::new(), engine, &product, &data, true, cfg)
    };

    let reqs = vec![
        DAGSelect::from(&product.table).build(),
        DAGSelect::from_index(&product.table, product.name).build(),
        DAGSelect::from(&product.table)
            .encode_type(EncodeType::TypeArrow)
            .build(),
        DAGSelect::from_index(&product.table, product.name)
            .encode_type(EncodeType::TypeArrow)
            .build(),
    ];

    for (mut req, _) in reqs {
        req.mut_context().set_scan_detail(true);
        req.mut_context().set_handle_time(true);

        let resp = handle_request(&end_point, req);
        assert!(resp.get_exec_details().has_handle_time());

        let scan_detail = resp.get_exec_details().get_scan_detail();
        // Values would occur in data cf are inlined in write cf.
        assert_eq!(scan_detail.get_write().get_total(), 5);
        assert_eq!(scan_detail.get_write().get_processed(), 4);
        assert_eq!(scan_detail.get_lock().get_total(), 1);
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_group_by() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:2"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        // should only have name:0, name:2 and name:1
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, name) in splitter.zip(&[b"name:0", b"name:2", b"name:1"]) {
            let expected_encoded = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 3);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_aggr_count() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
        (Datum::Null, 1),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .count()
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, cnt)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::U64(cnt), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }

    let exp = vec![
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
        (vec![Datum::Null, Datum::I64(4)], 1),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .count()
            .group_by(&[product.name, product.count])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (gk_data, cnt)) in splitter.zip(exp.clone()) {
            let mut expected_datum = vec![Datum::U64(cnt)];
            expected_datum.extend_from_slice(gk_data.as_slice());
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_aggr_first() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (3, Some("name:5"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
        (8, None, 5),
        (9, Some("name:5"), 5),
        (10, None, 6),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), 1),
        (Datum::Bytes(b"name:3".to_vec()), 2),
        (Datum::Bytes(b"name:5".to_vec()), 3),
        (Datum::Null, 7),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .first(product.id)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, id)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::I64(id), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    let exp = vec![
        (2, Datum::Bytes(b"name:0".to_vec())),
        (3, Datum::Bytes(b"name:3".to_vec())),
        (1, Datum::Bytes(b"name:0".to_vec())),
        (4, Datum::Bytes(b"name:5".to_vec())),
        (5, Datum::Null),
        (6, Datum::Null),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .first(product.name)
            .encode_type(encode_type)
            .group_by(&[product.count])
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (count, name)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![name, Datum::I64(count)];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_aggr_avg() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    store
        .insert_into(&product.table)
        .set(product.id, Datum::I64(8))
        .set(product.name, Datum::Bytes(b"name:4".to_vec()))
        .set(product.count, Datum::Null)
        .execute();
    store.commit();

    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
        (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
        (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
        (Datum::Null, (Datum::Dec(4.into()), 1)),
        (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .avg(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, (sum, cnt))) in spliter.zip(exp.clone()) {
            let expected_datum = vec![Datum::U64(cnt), sum, name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_aggr_sum() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), 3),
        (Datum::Bytes(b"name:3".to_vec()), 3),
        (Datum::Bytes(b"name:5".to_vec()), 8),
        (Datum::Null, 4),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .sum(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, cnt)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::Dec(cnt.into()), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_aggr_extre() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product.table)
            .set(product.id, Datum::I64(id))
            .set(product.name, Datum::Bytes(name.to_vec()))
            .set(product.count, Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::I64(2),
            Datum::I64(1),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::I64(5),
            Datum::I64(4),
        ),
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .max(product.count)
            .min(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, max, min)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![max, min, name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_aggr_bit_ops() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product.table)
            .set(product.id, Datum::I64(id))
            .set(product.name, Datum::Bytes(name.to_vec()))
            .set(product.count, Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::U64(0),
            Datum::U64(3),
            Datum::U64(3),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::U64(3),
            Datum::U64(3),
            Datum::U64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::U64(4),
            Datum::U64(5),
            Datum::U64(1),
        ),
        (Datum::Null, Datum::U64(4), Datum::U64(4), Datum::U64(4)),
        (
            Datum::Bytes(b"name:6".to_vec()),
            Datum::U64(18446744073709551615),
            Datum::U64(0),
            Datum::U64(0),
        ),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .bit_and(product.count)
            .bit_or(product.count)
            .bit_xor(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, bitand, bitor, bitxor)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![bitand, bitor, bitxor, name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_order_by_column() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:6"), 4),
        (6, Some("name:5"), 4),
        (7, Some("name:4"), 4),
        (8, None, 4),
    ];

    let exp = vec![
        (8, None, 4),
        (7, Some("name:4"), 4),
        (6, Some("name:5"), 4),
        (5, Some("name:6"), 4),
        (2, Some("name:3"), 3),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .order_by(product.count, true)
            .order_by(product.name, false)
            .limit(5)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in splitter.zip(exp.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[i64::from(id).into(), name_datum, i64::from(cnt).into()])
                    .unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_order_by_pk_with_select_from_index() {
    let mut data = vec![
        (8, Some("name:0"), 2),
        (7, Some("name:3"), 3),
        (6, Some("name:0"), 1),
        (5, Some("name:6"), 4),
        (4, Some("name:5"), 4),
        (3, Some("name:4"), 4),
        (2, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .order_by(product.id, true)
            .limit(5)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in splitter.zip(expect.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[name_datum, (cnt as i64).into(), (id as i64).into()])
                    .unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_limit() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .limit(5)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in splitter.zip(expect.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_reverse() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    data.reverse();
    let expect: Vec<_> = data.drain(..5).collect();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .limit(5)
            .order_by(product.id, true)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in splitter.zip(expect.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }

    end_point.stop().unwrap().join().unwrap();
}

pub fn handle_request<E: Engine>(end_point: &Worker<EndPointTask<E>>, req: Request) -> Response {
    let (tx, rx) = oneshot::channel();
    let on_resp = OnResponse::Unary(tx);
    let req = RequestTask::new(String::from("127.0.0.1"), req, on_resp, 100).unwrap();
    end_point.schedule(EndPointTask::Request(req)).unwrap();
    rx.wait().unwrap()
}

fn handle_select<E: Engine>(end_point: &Worker<EndPointTask<E>>, req: Request) -> SelectResponse {
    let resp = handle_request(end_point, req);
    assert!(!resp.get_data().is_empty(), "{:?}", resp);
    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

fn handle_streaming_select<E: Engine, F>(
    end_point: &Worker<EndPointTask<E>>,
    req: Request,
    mut check_range: F,
) -> Vec<StreamResponse>
where
    F: FnMut(&Response) + Send + 'static,
{
    let (stream_tx, stream_rx) = futures_mpsc::channel(10);
    let req = RequestTask::new(
        String::from("127.0.0.1"),
        req,
        OnResponse::Streaming(stream_tx),
        100,
    ).unwrap();
    end_point.schedule(EndPointTask::Request(req)).unwrap();
    stream_rx
        .wait()
        .into_iter()
        .map(|resp| {
            let resp = resp.unwrap();
            check_range(&resp);
            assert!(!resp.get_data().is_empty());
            let mut stream_resp = StreamResponse::new();
            stream_resp.merge_from_bytes(resp.get_data()).unwrap();
            stream_resp
        })
        .collect()
}

#[test]
fn test_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.id)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, _, _)) in splitter.zip(data.clone()) {
            let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 6);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_index_reverse_limit() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    data.reverse();
    let expect: Vec<_> = data.drain(..5).collect();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.id)
            .limit(5)
            .order_by(product.id, true)
            .encode_type(encode_type)
            .build();

        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, _, _)) in splitter.zip(expect.clone()) {
            let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_limit_oom() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.id)
            .limit(100000000)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, _, _)) in splitter.zip(data.clone()) {
            let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 6);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_del_select() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    let (id, name, cnt) = data.remove(3);
    let name_datum = name.map(|s| s.as_bytes()).into();
    store
        .delete_from(&product.table)
        .execute(id, vec![id.into(), name_datum, cnt.into()]);
    store.commit();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.id)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        let mut row_count = 0;
        for _ in splitter {
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_index_group_by() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:2"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        // should only have name:0, name:2 and name:1
        let mut row_count = 0;
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, name) in splitter.zip(&[b"name:0", b"name:1", b"name:2"]) {
            let expected_encoded = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 3);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_index_aggr_count() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .count()
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        let expected_encoded = datum::encode_value(&[Datum::U64(data.len() as u64)]).unwrap();
        let ret_data = splitter.next();
        assert_eq!(ret_data.is_some(), true);
        let result_encoded = datum::encode_value(&ret_data.unwrap()).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        assert_eq!(splitter.next().is_none(), true);
    }
    let exp = vec![
        (Datum::Null, 1),
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .count()
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, cnt)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::U64(cnt), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }

    let exp = vec![
        (vec![Datum::Null, Datum::I64(4)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .count()
            .group_by(&[product.name, product.count])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (gk_data, cnt)) in splitter.zip(exp.clone()) {
            let mut expected_datum = vec![Datum::U64(cnt)];
            expected_datum.extend_from_slice(gk_data.as_slice());
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_index_aggr_first() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 7),
        (Datum::Bytes(b"name:0".to_vec()), 4),
        (Datum::Bytes(b"name:3".to_vec()), 2),
        (Datum::Bytes(b"name:5".to_vec()), 5),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .first(product.id)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, id)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::I64(id), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_index_aggr_avg() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    store
        .insert_into(&product.table)
        .set(product.id, Datum::I64(8))
        .set(product.name, Datum::Bytes(b"name:4".to_vec()))
        .set(product.count, Datum::Null)
        .execute();
    store.commit();

    let exp = vec![
        (Datum::Null, (Datum::Dec(4.into()), 1)),
        (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
        (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
        (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
        (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .avg(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);

        for (row, (name, (sum, cnt))) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::U64(cnt), sum, name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_index_aggr_sum() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 4),
        (Datum::Bytes(b"name:0".to_vec()), 3),
        (Datum::Bytes(b"name:3".to_vec()), 3),
        (Datum::Bytes(b"name:5".to_vec()), 8),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .sum(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let splitter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, cnt)) in splitter.zip(exp.clone()) {
            let expected_datum = vec![Datum::Dec(cnt.into()), name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_index_aggr_extre() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, mut end_point) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product.table)
            .set(product.id, Datum::I64(id))
            .set(product.name, Datum::Bytes(name.to_vec()))
            .set(product.count, Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::I64(2),
            Datum::I64(1),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::I64(5),
            Datum::I64(4),
        ),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from_index(&product.table, product.name)
            .max(product.count)
            .min(product.count)
            .group_by(&[product.name])
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let exp_len = exp.len();
        let spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (name, max, min)) in spliter.zip(exp.clone()) {
            let expected_datum = vec![max, min, name];
            let expected_encoded = datum::encode_value(&expected_datum).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp_len);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_where() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    let cols = product.table.get_table_columns();
    let cond = {
        let mut col = Expr::new();
        col.set_tp(ExprType::ColumnRef);
        let count_offset = offset_for_column(&cols, product.count.id);
        col.mut_val().encode_i64(count_offset).unwrap();

        let mut value = Expr::new();
        value.set_tp(ExprType::String);
        value.set_val(String::from("2").into_bytes());
        let mut right = Expr::new();
        right.set_tp(ExprType::ScalarFunc);
        right.set_sig(ScalarFuncSig::CastStringAsInt);
        right.mut_children().push(value);

        let mut cond = Expr::new();
        cond.set_tp(ExprType::ScalarFunc);
        cond.set_sig(ScalarFuncSig::LTInt);
        cond.mut_children().push(col);
        cond.mut_children().push(right);
        cond
    };

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .where_expr(cond.clone())
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        let row = spliter.next().unwrap();
        let (id, name, cnt) = data[2];
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded =
            datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        assert_eq!(spliter.next().is_none(), true);
    }
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_handle_truncate() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);
    let cols = product.table.get_table_columns();
    let cases = vec![
        {
            // count > "2x"
            let mut col = Expr::new();
            col.set_tp(ExprType::ColumnRef);
            let count_offset = offset_for_column(&cols, product.count.id);
            col.mut_val().encode_i64(count_offset).unwrap();

            // "2x" will be truncated.
            let mut value = Expr::new();
            value.set_tp(ExprType::String);
            value.set_val(String::from("2x").into_bytes());

            let mut right = Expr::new();
            right.set_tp(ExprType::ScalarFunc);
            right.set_sig(ScalarFuncSig::CastStringAsInt);
            right.mut_children().push(value);

            let mut cond = Expr::new();
            cond.set_tp(ExprType::ScalarFunc);
            cond.set_sig(ScalarFuncSig::LTInt);
            cond.mut_children().push(col);
            cond.mut_children().push(right);
            cond
        },
        {
            // id
            let mut col_id = Expr::new();
            col_id.set_tp(ExprType::ColumnRef);
            let id_offset = offset_for_column(&cols, product.id.id);
            col_id.mut_val().encode_i64(id_offset).unwrap();

            // "3x" will be truncated.
            let mut value = Expr::new();
            value.set_tp(ExprType::String);
            value.set_val(String::from("3x").into_bytes());

            let mut int_3 = Expr::new();
            int_3.set_tp(ExprType::ScalarFunc);
            int_3.set_sig(ScalarFuncSig::CastStringAsInt);
            int_3.mut_children().push(value);

            // count
            let mut col_count = Expr::new();
            col_count.set_tp(ExprType::ColumnRef);
            let count_offset = offset_for_column(&cols, product.count.id);
            col_count.mut_val().encode_i64(count_offset).unwrap();

            // "3x" + count
            let mut plus = Expr::new();
            plus.set_tp(ExprType::ScalarFunc);
            plus.set_sig(ScalarFuncSig::PlusInt);
            plus.mut_children().push(int_3);
            plus.mut_children().push(col_count);

            // id = "3x" + count
            let mut cond = Expr::new();
            cond.set_tp(ExprType::ScalarFunc);
            cond.set_sig(ScalarFuncSig::EQInt);
            cond.mut_children().push(col_id);
            cond.mut_children().push(plus);
            cond
        },
    ];

    for encode_type in supported_encode_types() {
        for cond in cases.clone() {
            // Ignore truncate error.
            let (req, _) = DAGSelect::from(&product.table)
                .where_expr(cond.clone())
                .encode_type(encode_type)
                .build_with(Context::new(), &[FLAG_IGNORE_TRUNCATE]);
            let resp = handle_select(&end_point, req);
            assert!(!resp.has_error());
            assert!(resp.get_warnings().is_empty());

            // truncate as warning
            let (req, col_tps) = DAGSelect::from(&product.table)
                .where_expr(cond.clone())
                .encode_type(encode_type)
                .build_with(Context::new(), &[FLAG_TRUNCATE_AS_WARNING]);
            let mut resp = handle_select(&end_point, req);
            assert!(!resp.has_error());
            assert!(!resp.get_warnings().is_empty());
            // check data
            let mut spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
            let row = spliter.next().unwrap();
            let (id, name, cnt) = data[2];
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            assert_eq!(spliter.next().is_none(), true);

            // Do NOT ignore truncate error.
            let (req, _) = DAGSelect::from(&product.table)
                .where_expr(cond.clone())
                .encode_type(encode_type)
                .build();
            let mut resp = handle_select(&end_point, req);
            assert!(resp.has_error());
            assert!(resp.get_warnings().is_empty());
        }
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_default_val() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let added = ColumnBuilder::new().col_type(TYPE_LONG).default(3).build();
    let mut tbl = TableBuilder::new()
        .add_col(product.id)
        .add_col(product.name)
        .add_col(product.count)
        .add_col(added)
        .build();
    tbl.id = product.table.id;

    let (_, mut end_point) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&tbl)
            .limit(5)
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let mut row_count = 0;
        let spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (id, name, cnt)) in spliter.zip(expect.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[id.into(), name_datum, cnt.into(), Datum::I64(3)]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, 5);
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_output_offsets() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    for encode_type in supported_encode_types() {
        let (req, col_tps) = DAGSelect::from(&product.table)
            .output_offsets(Some(vec![1]))
            .encode_type(encode_type)
            .build();
        let mut resp = handle_select(&end_point, req);
        let spliter = DAGChunkSplitter::new(encode_type, &col_tps, &mut resp);
        for (row, (_, name, _)) in spliter.zip(data.clone()) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded = datum::encode_value(&[name_datum]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
            assert_eq!(&*result_encoded, &*expected_encoded);
        }
    }

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_key_is_locked_for_primary() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_data_with_commit(&product, &data, false);

    let (req, _) = DAGSelect::from(&product.table).build();
    let resp = handle_request(&end_point, req);
    assert!(resp.get_data().is_empty(), "{:?}", resp);
    assert!(resp.has_locked(), "{:?}", resp);
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_key_is_locked_for_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_data_with_commit(&product, &data, false);

    let (req, _) = DAGSelect::from_index(&product.table, product.name).build();
    let resp = handle_request(&end_point, req);
    assert!(resp.get_data().is_empty(), "{:?}", resp);
    assert!(resp.has_locked(), "{:?}", resp);
    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_output_counts() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let (req, _) = DAGSelect::from(&product.table).build();
    let resp = handle_select(&end_point, req);
    assert_eq!(resp.get_output_counts(), [data.len() as i64]);

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_exec_details() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    // get none
    let (req, _) = DAGSelect::from(&product.table).build();
    let resp = handle_request(&end_point, req);
    assert!(!resp.has_exec_details());

    let flags = &[0];

    // get handle_time
    let mut ctx = Context::new();
    ctx.set_handle_time(true);
    let (req, _) = DAGSelect::from(&product.table).build_with(ctx, flags);
    let resp = handle_request(&end_point, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(exec_details.has_handle_time());
    assert!(!exec_details.has_scan_detail());

    // get scan detail
    let mut ctx = Context::new();
    ctx.set_scan_detail(true);
    let (req, _) = DAGSelect::from(&product.table).build_with(ctx, flags);
    let resp = handle_request(&end_point, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(!exec_details.has_handle_time());
    assert!(exec_details.has_scan_detail());

    // get both
    let mut ctx = Context::new();
    ctx.set_scan_detail(true);
    ctx.set_handle_time(true);
    let (req, _) = DAGSelect::from(&product.table).build_with(ctx, flags);
    let resp = handle_request(&end_point, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(exec_details.has_handle_time());
    assert!(exec_details.has_scan_detail());

    end_point.stop().unwrap().join().unwrap();
}

#[test]
fn test_invalid_range() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_with_data(&product, &data);

    let mut select = DAGSelect::from(&product.table);
    select.key_range.set_start(b"xxx".to_vec());
    select.key_range.set_end(b"zzz".to_vec());
    let (req, _) = select.build();
    let resp = handle_request(&end_point, req);
    assert!(!resp.get_other_error().is_empty());

    end_point.stop().unwrap().join().unwrap();
}
