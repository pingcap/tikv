use tikv::server::coprocessor::*;
use tikv::server::coprocessor;
use kvproto::kvrpcpb::Context;
use tikv::util::codec::{table, Datum, datum};
use tikv::util::codec::number::*;
use tikv::storage::{Mutation, Key, ALL_CFS};
use tikv::storage::engine::{self, Engine, TEMP_DIR};
use tikv::util::worker::Worker;
use kvproto::coprocessor::{Request, KeyRange};
use tipb::select::{SelectRequest, SelectResponse, Chunk};
use tipb::schema::{self, ColumnInfo};
use tipb::expression::{Expr, ExprType, ByItem};
use storage::sync_storage::SyncStorage;
use tikv::util::xeval::evaluator::FLAG_IGNORE_TRUNCATE;

use std::collections::{HashMap, BTreeMap};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::i64;
use protobuf::{RepeatedField, Message};

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);

const TYPE_VAR_CHAR: i32 = 1;
const TYPE_LONG: i32 = 2;

fn next_id() -> i64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed) as i64
}

fn row_cnt(chunks: &[Chunk]) -> usize {
    chunks.iter().fold(0, |l, r| l + r.get_rows_meta().len())
}

struct Row {
    handle: i64,
    data: Vec<u8>,
}

#[derive(Debug)]
struct ChunkSpliter {
    chunk: Vec<Chunk>,
    readed: usize,
    idx: usize,
}

impl ChunkSpliter {
    fn new(chunk: Vec<Chunk>) -> ChunkSpliter {
        ChunkSpliter {
            chunk: chunk,
            readed: 0,
            idx: 0,
        }
    }
}

impl Iterator for ChunkSpliter {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        loop {
            if self.chunk.is_empty() {
                return None;
            }
            if self.idx == self.chunk[0].get_rows_meta().len() {
                assert_eq!(self.readed, self.chunk[0].get_rows_data().len());
                self.idx = 0;
                self.readed = 0;
                self.chunk.swap_remove(0);
                continue;
            }
            let metas = self.chunk[0].get_rows_meta();
            let data = self.chunk[0].get_rows_data();
            let data_len = metas[self.idx].get_length();
            let row = Row {
                handle: metas[self.idx].get_handle(),
                data: data[self.readed..self.readed + data_len as usize].to_vec(),
            };
            self.readed += data_len as usize;
            self.idx += 1;
            return Some(row);
        }
    }
}

#[derive(Clone, Copy)]
struct Column {
    id: i64,
    col_type: i32,
    // negative means not a index key, 0 means primary key, positive means normal index key.
    index: i64,
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

struct Table {
    id: i64,
    handle_id: i64,
    cols: BTreeMap<i64, Column>,
    idxs: BTreeMap<i64, Vec<i64>>,
}

impl Table {
    fn get_table_info(&self) -> schema::TableInfo {
        let mut tb_info = schema::TableInfo::new();
        tb_info.set_table_id(self.id);
        for col in self.cols.values() {
            let mut c_info = ColumnInfo::new();
            c_info.set_column_id(col.id);
            c_info.set_tp(col.col_type);
            c_info.set_pk_handle(col.index == 0);
            if let Some(dv) = col.default_val {
                c_info.set_default_val(datum::encode_value(&[Datum::I64(dv)]).unwrap())
            }
            tb_info.mut_columns().push(c_info);
        }
        tb_info
    }

    fn get_index_info(&self, index: i64) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.id);
        idx_info.set_index_id(index);
        for col_id in &self.idxs[&index] {
            let col = self.cols[col_id];
            let mut c_info = ColumnInfo::new();
            c_info.set_tp(col.col_type);
            c_info.set_column_id(col.id);
            c_info.set_pk_handle(col.id == self.handle_id);
            idx_info.mut_columns().push(c_info);
        }
        if let Some(col) = self.cols.get(&self.handle_id) {
            let mut c_info = ColumnInfo::new();
            c_info.set_tp(col.col_type);
            c_info.set_column_id(col.id);
            c_info.set_pk_handle(true);
            idx_info.mut_columns().push(c_info);
        }
        idx_info
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

struct Insert<'a> {
    store: &'a mut Store,
    table: &'a Table,
    values: BTreeMap<i64, Datum>,
}

impl<'a> Insert<'a> {
    fn new(store: &'a mut Store, table: &'a Table) -> Insert<'a> {
        Insert {
            store: store,
            table: table,
            values: BTreeMap::new(),
        }
    }

    fn set(mut self, col: Column, value: Datum) -> Insert<'a> {
        assert!(self.table.cols.contains_key(&col.id));
        self.values.insert(col.id, value);
        self
    }

    fn execute(mut self) -> i64 {
        let handle = self.values
            .get(&self.table.handle_id)
            .cloned()
            .unwrap_or_else(|| Datum::I64(next_id()));
        let key = build_row_key(self.table.id, handle.i64());
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
        self.store.put(kvs);
        handle.i64()
    }
}

struct Select<'a> {
    table: &'a Table,
    sel: SelectRequest,
    idx: i64,
}

impl<'a> Select<'a> {
    fn from(table: &'a Table) -> Select<'a> {
        Select::new(table, None)
    }

    fn from_index(table: &'a Table, index: Column) -> Select<'a> {
        Select::new(table, Some(index))
    }

    fn new(table: &'a Table, idx: Option<Column>) -> Select<'a> {
        let mut sel = SelectRequest::new();
        sel.set_start_ts(next_id() as u64);

        Select {
            table: table,
            sel: sel,
            idx: idx.map_or(-1, |c| c.index),
        }
    }

    fn limit(mut self, n: i64) -> Select<'a> {
        self.sel.set_limit(n);
        self
    }

    fn order_by_pk(mut self, desc: bool) -> Select<'a> {
        let mut item = ByItem::new();
        item.set_desc(desc);
        self.sel.mut_order_by().push(item);
        self
    }

    fn order_by(mut self, col: Column, desc: bool) -> Select<'a> {
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col.id).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        self.sel.mut_order_by().push(item);
        self
    }


    fn count(mut self) -> Select<'a> {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::Count);
        self.sel.mut_aggregates().push(expr);
        self
    }

    fn aggr_col(mut self, col: Column, aggr_t: ExprType) -> Select<'a> {
        let mut col_expr = Expr::new();
        col_expr.set_tp(ExprType::ColumnRef);
        col_expr.mut_val().encode_i64(col.id).unwrap();
        let mut expr = Expr::new();
        expr.set_tp(aggr_t);
        expr.mut_children().push(col_expr);
        self.sel.mut_aggregates().push(expr);
        self
    }

    fn first(self, col: Column) -> Select<'a> {
        self.aggr_col(col, ExprType::First)
    }

    fn sum(self, col: Column) -> Select<'a> {
        self.aggr_col(col, ExprType::Sum)
    }

    fn avg(self, col: Column) -> Select<'a> {
        self.aggr_col(col, ExprType::Avg)
    }

    fn max(self, col: Column) -> Select<'a> {
        self.aggr_col(col, ExprType::Max)
    }

    fn min(self, col: Column) -> Select<'a> {
        self.aggr_col(col, ExprType::Min)
    }

    fn group_by(mut self, cols: &[Column]) -> Select<'a> {
        for col in cols {
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ColumnRef);
            expr.mut_val().encode_i64(col.id).unwrap();
            let mut item = ByItem::new();
            item.set_expr(expr);
            self.sel.mut_group_by().push(item);
        }
        self
    }

    fn where_expr(mut self, expr: Expr) -> Select<'a> {
        self.sel.set_field_where(expr);
        self
    }

    fn build(self) -> Request {
        self.build_with(&[0])
    }

    fn build_with(mut self, flags: &[u64]) -> Request {
        let mut req = Request::new();

        if self.idx < 0 {
            self.sel.set_table_info(self.table.get_table_info());
            req.set_tp(REQ_TYPE_SELECT);
        } else {
            self.sel.set_index_info(self.table.get_index_info(self.idx));
            req.set_tp(REQ_TYPE_INDEX);
        }
        self.sel.set_flags(flags.iter().fold(0, |acc, f| acc | *f));

        req.set_data(self.sel.write_to_bytes().unwrap());

        let mut range = KeyRange::new();

        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(i64::MIN).unwrap();
        if self.idx < 0 {
            range.set_start(table::encode_row_key(self.table.id, &buf));
        } else {
            range.set_start(table::encode_index_seek_key(self.table.id, self.idx, &buf));
        }

        buf.clear();
        buf.encode_i64(i64::MAX).unwrap();
        if self.idx < 0 {
            range.set_end(table::encode_row_key(self.table.id, &buf));
        } else {
            range.set_end(table::encode_index_seek_key(self.table.id, self.idx, &buf));
        }
        req.set_ranges(RepeatedField::from_vec(vec![range]));
        req
    }
}

struct Delete<'a> {
    store: &'a mut Store,
    table: &'a Table,
}

impl<'a> Delete<'a> {
    fn new(store: &'a mut Store, table: &'a Table) -> Delete<'a> {
        Delete {
            store: store,
            table: table,
        }
    }

    fn execute(mut self, id: i64, row: Vec<Datum>) {
        let mut values = HashMap::new();
        for (&id, v) in self.table.cols.keys().zip(row) {
            values.insert(id, v);
        }
        let key = build_row_key(self.table.id, id);
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

struct Store {
    store: SyncStorage,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl Store {
    fn new(engine: Box<Engine>) -> Store {
        Store {
            store: SyncStorage::from_engine(engine, &Default::default()),
            current_ts: 1,
            handles: vec![],
        }
    }

    fn get_engine(&self) -> Box<Engine> {
        self.store.get_engine()
    }

    fn begin(&mut self) {
        self.current_ts = next_id() as u64;
        self.handles.clear();
    }

    fn insert_into<'a>(&'a mut self, table: &'a Table) -> Insert<'a> {
        Insert::new(self, table)
    }

    fn put(&mut self, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv.drain(..).map(|(k, v)| Mutation::Put((Key::from_raw(&k), v))).collect();
        self.store.prewrite(Context::new(), kv, pk, self.current_ts).unwrap();
    }

    fn delete_from<'a>(&'a mut self, table: &'a Table) -> Delete<'a> {
        Delete::new(self, table)
    }

    fn delete(&mut self, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys.drain(..).map(|k| Mutation::Delete(Key::from_raw(&k))).collect();
        self.store.prewrite(Context::new(), mutations, pk, self.current_ts).unwrap();
    }

    fn commit(&mut self) {
        let handles = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        self.store
            .commit(Context::new(), handles, self.current_ts, next_id() as u64)
            .unwrap();
    }
}


fn build_row_key(table_id: i64, id: i64) -> Vec<u8> {
    let mut buf = [0; 8];
    (&mut buf as &mut [u8]).encode_i64(id).unwrap();
    table::encode_row_key(table_id, &buf)
}

/// An example table for test purpose.
struct ProductTable {
    id: Column,
    name: Column,
    count: Column,
    table: Table,
}

impl ProductTable {
    fn new() -> ProductTable {
        let id = ColumnBuilder::new().col_type(TYPE_LONG).primary_key(true).build();
        let idx_id = next_id();
        let name = ColumnBuilder::new().col_type(TYPE_VAR_CHAR).index_key(idx_id).build();
        let count = ColumnBuilder::new().col_type(TYPE_LONG).index_key(idx_id).build();
        let table = TableBuilder::new().add_col(id).add_col(name).add_col(count).build();

        ProductTable {
            id: id,
            name: name,
            count: count,
            table: table,
        }
    }
}

// This function will create a Product table and initialize with the specified data.
fn init_with_data(tbl: &ProductTable,
                  vals: &[(i64, Option<&str>, i64)])
                  -> (Store, Worker<EndPointTask>) {
    let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
    let mut store = Store::new(engine);

    store.begin();
    for &(id, name, count) in vals {
        store.insert_into(&tbl.table)
            .set(tbl.id, Datum::I64(id))
            .set(tbl.name, name.map(|s| s.as_bytes()).into())
            .set(tbl.count, Datum::I64(count))
            .execute();
    }
    store.commit();

    let mut end_point = Worker::new("test select worker");
    let runner = EndPointHost::new(store.get_engine(), end_point.scheduler(), 8, 2, 2);
    end_point.start_batch(runner, 5).unwrap();

    (store, end_point)
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

    let req = Select::from(&product.table).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), data.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (id, name, cnt)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()])
            .unwrap();
        assert_eq!(id, row.handle);
        assert_eq!(row.data, &*expected_encoded);
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

    let req = Select::from(&product.table).group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    // should only have name:0, name:2 and name:1
    assert_eq!(row_cnt(resp.get_chunks()), 3);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, name) in spliter.zip(&[b"name:0", b"name:2", b"name:1"]) {
        let gk = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
        let expected_encoded = datum::encode_value(&[Datum::Bytes(gk)]).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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

    let req = Select::from(&product.table).count().build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 1);
    let mut spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    let gk = Datum::Bytes(coprocessor::SINGLE_GROUP.to_vec());
    let mut expected_encoded = datum::encode_value(&[gk, Datum::U64(data.len() as u64)]).unwrap();
    assert_eq!(spliter.next().unwrap().data, &*expected_encoded);

    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
        (Datum::Null, 1),
    ];
    let req = Select::from(&product.table).count().group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::U64(cnt)];
        expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
    }

    let exp = vec![
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
        (vec![Datum::Null, Datum::I64(4)], 1),
    ];
    let req = Select::from(&product.table).count().group_by(&[product.name, product.count]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (gk_data, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&gk_data);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::U64(cnt)];
        expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from(&product.table).first(product.id).group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, id)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::I64(id)];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
    }

    let exp = vec![
        (2, Datum::Bytes(b"name:0".to_vec())),
        (3, Datum::Bytes(b"name:3".to_vec())),
        (1, Datum::Bytes(b"name:0".to_vec())),
        (4, Datum::Bytes(b"name:5".to_vec())),
        (5, Datum::Null),
        (6, Datum::Null),
    ];
    let req = Select::from(&product.table).first(product.name).group_by(&[product.count]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (count, name)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[Datum::I64(count)]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    store.insert_into(&product.table)
        .set(product.id, Datum::I64(8))
        .set(product.name, Datum::Bytes(b"name:4".to_vec()))
        .set(product.count, Datum::Null)
        .execute();
    store.commit();

    let exp = vec![(Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
                   (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
                   (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
                   (Datum::Null, (Datum::Dec(4.into()), 1)),
                   (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0))];
    let req = Select::from(&product.table).avg(product.count).group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, (sum, cnt))) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::U64(cnt), sum];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from(&product.table).sum(product.count).group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::Dec(cnt.into())];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
        store.insert_into(&product.table)
            .set(product.id, Datum::I64(id))
            .set(product.name, Datum::Bytes(name.to_vec()))
            .set(product.count, Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Bytes(b"name:0".to_vec()), Datum::I64(2), Datum::I64(1)),
        (Datum::Bytes(b"name:3".to_vec()), Datum::I64(3), Datum::I64(3)),
        (Datum::Bytes(b"name:5".to_vec()), Datum::I64(5), Datum::I64(4)),
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];
    let req = Select::from(&product.table)
        .max(product.count)
        .min(product.count)
        .group_by(&[product.name])
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, max, min)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), max, min];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from(&product.table)
        .order_by(product.count, true)
        .order_by(product.name, false)
        .limit(5)
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (id, name, cnt)) in spliter.zip(exp) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded =
            datum::encode_value(&[(id as i64).into(), name_datum, (cnt as i64).into()]).unwrap();
        assert_eq!(id as i64, row.handle);
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from_index(&product.table, product.name)
        .order_by(product.id, true)
        .limit(5)
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (id, _, _)) in spliter.zip(data.drain(..5)) {
        assert_eq!(id, row.handle);
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

    let req = Select::from(&product.table).limit(5).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (id, name, cnt)) in spliter.zip(data.drain(..5)) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
        assert_eq!(id, row.handle);
        assert_eq!(row.data, &*expected_encoded);
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

    let req = Select::from(&product.table).limit(5).order_by_pk(true).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    data.reverse();
    for (row, (id, name, cnt)) in spliter.zip(data.drain(..5)) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
        assert_eq!(id, row.handle);
        assert_eq!(row.data, &*expected_encoded);
    }

    end_point.stop().unwrap().join().unwrap();
}

fn handle_select(end_point: &Worker<EndPointTask>, req: Request) -> SelectResponse {
    let (tx, rx) = mpsc::channel();
    let req = RequestTask::new(req, box move |r| tx.send(r).unwrap());
    end_point.schedule(EndPointTask::Request(req)).unwrap();
    let resp = rx.recv().unwrap().take_cop_resp();
    assert!(resp.has_data(), "{:?}", resp);
    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
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

    let req = Select::from_index(&product.table, product.id).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), data.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    let mut handles: Vec<_> = spliter.map(|row| row.handle).collect();
    handles.sort();
    for (&h, (id, _, _)) in handles.iter().zip(data) {
        assert_eq!(id, h);
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

    let req = Select::from_index(&product.table, product.id).limit(5).order_by_pk(true).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    let handles = spliter.map(|row| row.handle);
    data.reverse();
    for (h, (id, _, _)) in handles.zip(data.drain(..5)) {
        assert_eq!(id, h);
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
    store.delete_from(&product.table).execute(id, vec![id.into(), name_datum, cnt.into()]);
    store.commit();

    let req = Select::from_index(&product.table, product.id).build();
    let resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), data.len());

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

    let req = Select::from_index(&product.table, product.name).group_by(&[product.name]).build();
    let mut resp = handle_select(&end_point, req);
    // should only have name:0, name:2 and name:1
    assert_eq!(row_cnt(resp.get_chunks()), 3);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, name) in spliter.zip(&[b"name:0", b"name:1", b"name:2"]) {
        let gk = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
        let expected_encoded = datum::encode_value(&[Datum::Bytes(gk)]).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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

    let req = Select::from_index(&product.table, product.name).count().build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 1);
    let mut spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    let gk = Datum::Bytes(coprocessor::SINGLE_GROUP.to_vec());
    let mut expected_encoded = datum::encode_value(&[gk, Datum::U64(data.len() as u64)]).unwrap();
    assert_eq!(spliter.next().unwrap().data, &*expected_encoded);

    let exp = vec![
        (Datum::Null, 1),
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
    ];
    let req = Select::from_index(&product.table, product.name)
        .count()
        .group_by(&[product.name])
        .build();
    resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::U64(cnt)];
        expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
    }

    let exp = vec![
        (vec![Datum::Null, Datum::I64(4)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
    ];
    let req = Select::from_index(&product.table, product.name)
        .count()
        .group_by(&[product.name, product.count])
        .build();
    resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (gk_data, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&gk_data);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::U64(cnt)];
        expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from_index(&product.table, product.name)
        .first(product.id)
        .group_by(&[product.name])
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, id)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::I64(id)];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    store.insert_into(&product.table)
        .set(product.id, Datum::I64(8))
        .set(product.name, Datum::Bytes(b"name:4".to_vec()))
        .set(product.count, Datum::Null)
        .execute();
    store.commit();

    let exp = vec![(Datum::Null, (Datum::Dec(4.into()), 1)),
                   (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
                   (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
                   (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
                   (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2))];
    let req = Select::from_index(&product.table, product.name)
        .avg(product.count)
        .group_by(&[product.name])
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, (sum, cnt))) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::U64(cnt), sum];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
    let req = Select::from_index(&product.table, product.name)
        .sum(product.count)
        .group_by(&[product.name])
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, cnt)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), Datum::Dec(cnt.into())];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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
        store.insert_into(&product.table)
            .set(product.id, Datum::I64(id))
            .set(product.name, Datum::Bytes(name.to_vec()))
            .set(product.count, Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (Datum::Bytes(b"name:0".to_vec()), Datum::I64(2), Datum::I64(1)),
        (Datum::Bytes(b"name:3".to_vec()), Datum::I64(3), Datum::I64(3)),
        (Datum::Bytes(b"name:5".to_vec()), Datum::I64(5), Datum::I64(4)),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];
    let req = Select::from_index(&product.table, product.name)
        .max(product.count)
        .min(product.count)
        .group_by(&[product.name])
        .build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), exp.len());
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (name, max, min)) in spliter.zip(exp) {
        let gk = datum::encode_value(&[name]).unwrap();
        let expected_datum = vec![Datum::Bytes(gk), max, min];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.data, &*expected_encoded);
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

    let cond = {
        let mut col = Expr::new();
        col.set_tp(ExprType::ColumnRef);
        col.mut_val().encode_i64(product.count.id).unwrap();

        let mut value = Expr::new();
        value.set_tp(ExprType::String);
        value.set_val(String::from("2").into_bytes());

        let mut cond = Expr::new();
        cond.set_tp(ExprType::LT);
        cond.mut_children().push(col);
        cond.mut_children().push(value);
        cond
    };

    let req = Select::from(&product.table).where_expr(cond).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 1);
    let mut spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    let row = spliter.next().unwrap();
    let (id, name, cnt) = data[2];
    let name_datum = name.map(|s| s.as_bytes()).into();
    let expected_encoded = datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
    assert_eq!(id, row.handle);
    assert_eq!(row.data, &*expected_encoded);

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

    let cases = vec![{
                         // count > "2x"
                         let mut col = Expr::new();
                         col.set_tp(ExprType::ColumnRef);
                         col.mut_val().encode_i64(product.count.id).unwrap();

                         // "2x" will be truncated.
                         let mut value = Expr::new();
                         value.set_tp(ExprType::String);
                         value.set_val(String::from("2x").into_bytes());

                         let mut cond = Expr::new();
                         cond.set_tp(ExprType::LT);
                         cond.mut_children().push(col);
                         cond.mut_children().push(value);
                         cond
                     },
                     {
                         // id
                         let mut col_id = Expr::new();
                         col_id.set_tp(ExprType::ColumnRef);
                         col_id.mut_val().encode_i64(product.id.id).unwrap();

                         // "3x" will be truncated.
                         let mut value = Expr::new();
                         value.set_tp(ExprType::String);
                         value.set_val(String::from("3x").into_bytes());

                         // count
                         let mut col_count = Expr::new();
                         col_count.set_tp(ExprType::ColumnRef);
                         col_count.mut_val().encode_i64(product.count.id).unwrap();

                         // "3x" + count
                         let mut plus = Expr::new();
                         plus.set_tp(ExprType::Plus);
                         plus.mut_children().push(value);
                         plus.mut_children().push(col_count);

                         // id = "3x" + count
                         let mut cond = Expr::new();
                         cond.set_tp(ExprType::EQ);
                         cond.mut_children().push(col_id);
                         cond.mut_children().push(plus);
                         cond
                     }];

    for cond in cases {
        // Ignore truncate error.
        let req = Select::from(&product.table)
            .where_expr(cond.clone())
            .build_with(&[FLAG_IGNORE_TRUNCATE]);
        let mut resp = handle_select(&end_point, req);
        assert_eq!(row_cnt(resp.get_chunks()), 1);
        let mut spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
        let row = spliter.next().unwrap();
        let (id, name, cnt) = data[2];
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()])
            .unwrap();
        assert_eq!(id, row.handle);
        assert_eq!(row.data, &*expected_encoded);

        // Do NOT ignore truncate error.
        let req = Select::from(&product.table).where_expr(cond.clone()).build();
        let (tx, rx) = mpsc::channel();
        let req = RequestTask::new(req, box move |r| tx.send(r).unwrap());
        end_point.schedule(EndPointTask::Request(req)).unwrap();
        let resp = rx.recv().unwrap().take_cop_resp();
        assert!(resp.has_other_error());
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

    let req = Select::from(&tbl).limit(5).build();
    let mut resp = handle_select(&end_point, req);
    assert_eq!(row_cnt(resp.get_chunks()), 5);
    let spliter = ChunkSpliter::new(resp.take_chunks().into_vec());
    for (row, (id, name, cnt)) in spliter.zip(data.drain(..5)) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded =
            datum::encode_value(&[id.into(), name_datum, cnt.into(), Datum::I64(3)]).unwrap();
        assert_eq!(id, row.handle);
        assert_eq!(row.data, &*expected_encoded);
    }

    end_point.stop().unwrap().join().unwrap();
}
