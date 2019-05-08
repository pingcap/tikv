// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::util::scan_bencher::ScanBencher;
use crate::util::store::*;

pub mod fixture;
pub mod util;

const ROWS: usize = 5000;

/// 1 interested column, which is PK (which is in the key)
///
/// This kind of scanner is used in SQLs like SELECT COUNT(*).
fn bench_table_scan_primary_key(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_two_columns(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, at the front of each row. Each row contains 100 columns.
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(column)`.
fn bench_table_scan_datum_front(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 2 interested columns, at the front of each row. Each row contains 100 columns.
fn bench_table_scan_datum_multi_front(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[
            table["col0"].as_column_info(),
            table["col1"].as_column_info(),
        ],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, at the end of each row. Each row contains 100 columns.
fn bench_table_scan_datum_end(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[table["col99"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 100 interested columns, all columns in the row are interested (i.e. there are totally 100
/// columns in the row).
fn bench_table_scan_datum_all(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &table.columns_info(),
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long but only PK is interested.
fn bench_table_scan_long_datum_primary_key(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long but a short column is interested.
fn bench_table_scan_long_datum_normal(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["foo"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long and the long column is interested.
fn bench_table_scan_long_datum_long(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["bar"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long and the all columns are interested.
fn bench_table_scan_long_datum_all(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[
            table["id"].as_column_info(),
            table["foo"].as_column_info(),
            table["bar"].as_column_info(),
        ],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, but the column is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 10 columns.
fn bench_table_scan_datum_absent(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_missing_column(ROWS, 10);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, but the column is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 100 columns.
fn bench_table_scan_datum_absent_large_row(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_missing_column(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, which is PK. However the range given are point ranges.
fn bench_table_scan_point_range(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = fixture::table_with_two_columns(ROWS);

    let mut ranges = vec![];
    for i in 0..=1024 {
        ranges.push(table.get_record_range_one(i));
    }

    input
        .0
        .bench(b, &[table["id"].as_column_info()], &ranges, &store, ());
}

#[derive(Clone)]
struct Input(Box<dyn ScanBencher<util::TableScanParam>>);

impl Input {
    pub fn new<T: ScanBencher<util::TableScanParam> + 'static>(b: T) -> Self {
        Self(Box::new(b))
    }
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name())
    }
}

pub fn bench(c: &mut criterion::Criterion) {
    let mut inputs = vec![
        Input::new(util::NormalTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::BatchTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::TableScanDAGBencher::<RocksStore>::new(false, ROWS)),
        Input::new(util::TableScanDAGBencher::<RocksStore>::new(true, ROWS)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs = vec![
            Input::new(util::NormalTableScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::BatchTableScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<MemStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<RocksStore>::new()),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(false, ROWS)),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(true, ROWS)),
        ];
        inputs.append(&mut additional_inputs);
    }

    c.bench_function_over_inputs(
        "table_scan_primary_key",
        bench_table_scan_primary_key,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_long_datum_all",
        bench_table_scan_long_datum_all,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_absent_large_row",
        bench_table_scan_datum_absent_large_row,
        inputs.clone(),
    );
    if crate::util::bench_level() >= 1 {
        c.bench_function_over_inputs(
            "table_scan_datum_front",
            bench_table_scan_datum_front,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_datum_all",
            bench_table_scan_datum_all,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_point_range",
            bench_table_scan_point_range,
            inputs.clone(),
        );
    }
    if crate::util::bench_level() >= 2 {
        c.bench_function_over_inputs(
            "table_scan_datum_multi_front",
            bench_table_scan_datum_multi_front,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_datum_end",
            bench_table_scan_datum_end,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_long_datum_primary_key",
            bench_table_scan_long_datum_primary_key,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_long_datum_normal",
            bench_table_scan_long_datum_normal,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_long_datum_long",
            bench_table_scan_long_datum_long,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "table_scan_datum_absent",
            bench_table_scan_datum_absent,
            inputs.clone(),
        );
    }
}
