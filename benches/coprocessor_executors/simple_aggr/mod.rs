// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use cop_datatype::FieldTypeTp;
use tipb::expression::ExprType;
use tipb_helper::ExprDefBuilder;

use crate::util::FixtureBuilder;

/// COUNT(1)
fn bench_simple_aggr_count_1(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &expr);
}

/// COUNT(COL) where COL is a int column
fn bench_simple_aggr_count_int_column(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
        .build();
    input.bencher.bench(b, &fb, &expr);
}

/// COUNT(COL) where COL is a real column
fn bench_simple_aggr_count_real_column(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_f64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
        .build();
    input.bencher.bench(b, &fb, &expr);
}

/// COUNT(COL) where COL is a bytes column (note: the column is very short)
fn bench_simple_aggr_count_bytes_column(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_bytes_random_fixed_len(10);
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarChar))
        .build();
    input.bencher.bench(b, &fb, &expr);
}

#[derive(Clone)]
struct Input {
    /// How many rows to aggregate
    src_rows: usize,

    /// The aggregate executor (batch / normal) to use
    bencher: Box<dyn util::SimpleAggrBencher>,
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/rows={}", self.bencher.name(), self.src_rows)
    }
}

pub fn bench(c: &mut criterion::Criterion) {
    let mut inputs = vec![];

    let mut rows_options = vec![5000];
    if crate::util::bench_level() >= 1 {
        rows_options.push(5);
    }
    if crate::util::bench_level() >= 2 {
        rows_options.push(1);
    }
    let bencher_options: Vec<Box<dyn util::SimpleAggrBencher>> =
        vec![Box::new(util::NormalBencher), Box::new(util::BatchBencher)];

    for bencher in &bencher_options {
        for rows in &rows_options {
            inputs.push(Input {
                src_rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    c.bench_function_over_inputs(
        "simple_aggr_count_1",
        bench_simple_aggr_count_1,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "simple_aggr_count_int_column",
        bench_simple_aggr_count_int_column,
        inputs.clone(),
    );
    if crate::util::bench_level() >= 2 {
        c.bench_function_over_inputs(
            "simple_aggr_count_real_column",
            bench_simple_aggr_count_real_column,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "simple_aggr_count_bytes_column",
            bench_simple_aggr_count_bytes_column,
            inputs.clone(),
        );
    }
}
