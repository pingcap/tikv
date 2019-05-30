// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::executor::Aggregation;
use tipb::expression::Expr;

use tikv::coprocessor::dag::batch::executors::BatchFastHashAggregationExecutor;
use tikv::coprocessor::dag::batch::executors::BatchSlowHashAggregationExecutor;
use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::executor::{Executor, HashAggExecutor};
use tikv::coprocessor::dag::expr::EvalConfig;

use crate::util::bencher::Bencher;
use crate::util::FixtureBuilder;

pub trait HashAggrBencher {
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    );

    fn box_clone(&self) -> Box<dyn HashAggrBencher>;
}

impl Clone for Box<dyn HashAggrBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal hash aggregation executor to bench the giving aggregate
/// expression.
pub struct NormalBencher;

impl HashAggrBencher for NormalBencher {
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let mut meta = Aggregation::new();
            meta.set_agg_func(aggr_expr.to_vec().into());
            meta.set_group_by(group_by_expr.to_vec().into());
            let src = fb.clone().build_normal_fixture_executor();
            let ex = HashAggExecutor::new(
                black_box(meta),
                black_box(Arc::new(EvalConfig::default())),
                black_box(Box::new(src)),
            )
            .unwrap();
            Box::new(ex) as Box<dyn Executor>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn HashAggrBencher> {
        Box::new(Self)
    }
}

/// A bencher that will use batch hash aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl HashAggrBencher for BatchBencher {
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            let mut meta = Aggregation::new();
            meta.set_agg_func(aggr_expr.to_vec().into());
            meta.set_group_by(group_by_expr.to_vec().into());
            if BatchFastHashAggregationExecutor::check_supported(&meta).is_ok() {
                let ex = BatchFastHashAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap();
                Box::new(ex) as Box<dyn BatchExecutor>
            } else {
                let ex = BatchSlowHashAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap();
                Box::new(ex) as Box<dyn BatchExecutor>
            }
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn HashAggrBencher> {
        Box::new(Self)
    }
}
