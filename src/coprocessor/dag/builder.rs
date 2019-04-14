// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};

use crate::storage::Store;

use super::batch::executors::*;
use super::batch::interface::*;
use super::executor::{
    Executor, HashAggExecutor, LimitExecutor, ScanExecutor, SelectionExecutor, StreamAggExecutor,
    TopNExecutor,
};
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::metrics::*;
use crate::coprocessor::*;

/// Utilities to build an executor DAG.
///
/// Currently all executors are executed in sequence and there is no task scheduler, so this
/// builder is in fact a pipeline builder. The builder will finally build a `Box<Executor>` which
/// may contain another executor as its source, embedded in the field, one after another. These
/// nested executors together form an executor pipeline that a single iteration at the out-most
/// executor (i.e. calling `next()`) will drive the whole pipeline.
pub struct DAGBuilder;

impl DAGBuilder {
    /// Given a list of executor descriptors and checks whether all executor descriptors can
    /// be used to build batch executors.
    pub fn check_build_batch(exec_descriptors: &[executor::Executor]) -> Result<()> {
        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    BatchTableScanExecutor::check_supported(&descriptor)?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanExecutor::check_supported(&descriptor)?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionExecutor::check_supported(&descriptor)?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationExecutor::check_supported(&descriptor)?;
                }
                ExecType::TypeLimit => {}
                _ => {
                    return Err(box_err!("Unsupported executor {:?}", ed.get_tp()));
                }
            }
        }

        Ok(())
    }

    // Note: `S` is `'static` because we have trait objects `Executor`.
    pub fn build_batch<S: Store + 'static, C: ExecSummaryCollector + 'static>(
        executor_descriptors: Vec<executor::Executor>,
        store: S,
        ranges: Vec<KeyRange>,
        config: Arc<EvalConfig>,
    ) -> Result<Box<dyn BatchExecutor>> {
        // Shared in multiple executors, so wrap with Rc.
        let mut executor_descriptors = executor_descriptors.into_iter();
        let mut first_ed = executor_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("No executors")))?;

        let mut executor: Box<dyn BatchExecutor>;
        let mut summary_slot_index = 0;

        match first_ed.get_tp() {
            ExecType::TypeTableScan => {
                // TODO: Use static metrics.
                COPR_EXECUTOR_COUNT.with_label_values(&["tblscan"]).inc();

                let mut descriptor = first_ed.take_tbl_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(BatchTableScanExecutor::new(
                    C::new(summary_slot_index),
                    store,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                )?);
            }
            ExecType::TypeIndexScan => {
                COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();

                let mut descriptor = first_ed.take_idx_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(BatchIndexScanExecutor::new(
                    C::new(summary_slot_index),
                    store,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                )?);
            }
            _ => {
                return Err(Error::Other(box_err!(
                    "Unexpected first executor {:?}",
                    first_ed.get_tp()
                )));
            }
        }

        for mut ed in executor_descriptors {
            summary_slot_index += 1;

            let new_executor: Box<dyn BatchExecutor> = match ed.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(Error::Other(box_err!(
                        "Unexpected non-first executor {:?}",
                        ed.get_tp()
                    )));
                }
                ExecType::TypeSelection => {
                    COPR_EXECUTOR_COUNT.with_label_values(&["selection"]).inc();

                    Box::new(BatchSelectionExecutor::new(
                        C::new(summary_slot_index),
                        config.clone(),
                        executor,
                        ed.take_selection().take_conditions().into_vec(),
                    )?)
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["simple_aggregation"])
                        .inc();

                    Box::new(BatchSimpleAggregationExecutor::new(
                        C::new(summary_slot_index),
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_agg_func().into_vec(),
                    )?)
                }
                _ => {
                    return Err(Error::Other(box_err!(
                        "Unexpected non-first executor {:?}",
                        first_ed.get_tp()
                    )));
                }
            };
            executor = new_executor;
        }

        Ok(executor)
    }

    /// Builds a normal executor pipeline.
    ///
    /// Normal executors iterate rows one by one.
    pub fn build_normal<S: Store + 'static>(
        exec_descriptors: Vec<executor::Executor>,
        store: S,
        ranges: Vec<KeyRange>,
        ctx: Arc<EvalConfig>,
        collect: bool,
    ) -> Result<Box<dyn Executor + Send>> {
        let mut exec_descriptors = exec_descriptors.into_iter();
        let first = exec_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("has no executor")))?;
        let mut src = Self::build_normal_first_executor(first, store, ranges, collect)?;
        for mut exec in exec_descriptors {
            let curr: Box<dyn Executor + Send> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!("got too much *scan exec, should be only one"));
                }
                ExecType::TypeSelection => Box::new(SelectionExecutor::new(
                    exec.take_selection(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeAggregation => Box::new(HashAggExecutor::new(
                    exec.take_aggregation(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeStreamAgg => Box::new(StreamAggExecutor::new(
                    Arc::clone(&ctx),
                    src,
                    exec.take_aggregation(),
                )?),
                ExecType::TypeTopN => {
                    Box::new(TopNExecutor::new(exec.take_topN(), Arc::clone(&ctx), src)?)
                }
                ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
            };
            src = curr;
        }
        Ok(src)
    }

    /// Builds the inner-most executor for the normal executor pipeline, which can produce rows to
    /// other executors and never receive rows from other executors.
    ///
    /// The inner-most executor must be a table scan executor or an index scan executor.
    fn build_normal_first_executor<S: Store + 'static>(
        mut first: executor::Executor,
        store: S,
        ranges: Vec<KeyRange>,
        collect: bool,
    ) -> Result<Box<dyn Executor + Send>> {
        match first.get_tp() {
            ExecType::TypeTableScan => {
                let ex = Box::new(ScanExecutor::table_scan(
                    first.take_tbl_scan(),
                    ranges,
                    store,
                    collect,
                )?);
                Ok(ex)
            }
            ExecType::TypeIndexScan => {
                let unique = first.get_idx_scan().get_unique();
                let ex = Box::new(ScanExecutor::index_scan(
                    first.take_idx_scan(),
                    ranges,
                    store,
                    unique,
                    collect,
                )?);
                Ok(ex)
            }
            _ => Err(box_err!(
                "first exec type should be *Scan, but get {:?}",
                first.get_tp()
            )),
        }
    }
}
