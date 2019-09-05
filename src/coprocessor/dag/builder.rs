// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};
use tipb::select::DAGRequest;

use crate::storage::Store;

use super::batch::executors::*;
use super::batch::interface::*;
use super::executor::{
    Executor, HashAggExecutor, LimitExecutor, ScanExecutor, SelectionExecutor, StreamAggExecutor,
    TopNExecutor,
};
use crate::coprocessor::dag::execute_stats::*;
use crate::coprocessor::dag::expr::{EvalConfig, Flag, SqlMode};
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
                    BatchTableScanExecutor::check_supported(&descriptor)
                        .map_err(|e| Error::Other(box_err!("BatchTableScanExecutor: {}", e)))?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanExecutor::check_supported(&descriptor)
                        .map_err(|e| Error::Other(box_err!("BatchIndexScanExecutor: {}", e)))?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionExecutor::check_supported(&descriptor)
                        .map_err(|e| Error::Other(box_err!("BatchSelectionExecutor: {}", e)))?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationExecutor::check_supported(&descriptor).map_err(|e| {
                        Error::Other(box_err!("BatchSimpleAggregationExecutor: {}", e))
                    })?;
                }
                ExecType::TypeAggregation => {
                    let descriptor = ed.get_aggregation();
                    if BatchFastHashAggregationExecutor::check_supported(&descriptor).is_err() {
                        BatchSlowHashAggregationExecutor::check_supported(&descriptor).map_err(
                            |e| Error::Other(box_err!("BatchSlowHashAggregationExecutor: {}", e)),
                        )?;
                    }
                }
                ExecType::TypeStreamAgg => {
                    // Note: We won't check whether the source of stream aggregation is in order.
                    //       It is undefined behavior if the source is unordered.
                    let descriptor = ed.get_aggregation();
                    BatchStreamAggregationExecutor::check_supported(&descriptor).map_err(|e| {
                        Error::Other(box_err!("BatchStreamAggregationExecutor: {}", e))
                    })?;
                }
                ExecType::TypeLimit => {}
                ExecType::TypeTopN => {
                    let descriptor = ed.get_topN();
                    BatchTopNExecutor::check_supported(&descriptor)
                        .map_err(|e| Error::Other(box_err!("BatchTopNExecutor: {}", e)))?;
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
        let mut executor_descriptors = executor_descriptors.into_iter();
        let mut first_ed = executor_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("No executors")))?;

        let mut executor: Box<dyn BatchExecutor>;
        let mut summary_slot_index = 0;

        match first_ed.get_tp() {
            ExecType::TypeTableScan => {
                // TODO: Use static metrics.
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_table_scan"])
                    .inc();

                let mut descriptor = first_ed.take_tbl_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(
                    BatchTableScanExecutor::new(
                        store,
                        config.clone(),
                        columns_info,
                        ranges,
                        descriptor.get_desc(),
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                );
            }
            ExecType::TypeIndexScan => {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_index_scan"])
                    .inc();

                let mut descriptor = first_ed.take_idx_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(
                    BatchIndexScanExecutor::new(
                        store,
                        config.clone(),
                        columns_info,
                        ranges,
                        descriptor.get_desc(),
                        descriptor.get_unique(),
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                );
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
                ExecType::TypeSelection => {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_selection"])
                        .inc();

                    Box::new(
                        BatchSelectionExecutor::new(
                            config.clone(),
                            executor,
                            ed.take_selection().take_conditions().into_vec(),
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_simple_aggr"])
                        .inc();

                    Box::new(
                        BatchSimpleAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_agg_func().into_vec(),
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeAggregation => {
                    if BatchFastHashAggregationExecutor::check_supported(&ed.get_aggregation())
                        .is_ok()
                    {
                        COPR_EXECUTOR_COUNT
                            .with_label_values(&["batch_fast_hash_aggr"])
                            .inc();

                        Box::new(
                            BatchFastHashAggregationExecutor::new(
                                config.clone(),
                                executor,
                                ed.mut_aggregation().take_group_by().into_vec(),
                                ed.mut_aggregation().take_agg_func().into_vec(),
                            )?
                            .with_summary_collector(C::new(summary_slot_index)),
                        )
                    } else {
                        COPR_EXECUTOR_COUNT
                            .with_label_values(&["batch_slow_hash_aggr"])
                            .inc();

                        Box::new(
                            BatchSlowHashAggregationExecutor::new(
                                config.clone(),
                                executor,
                                ed.mut_aggregation().take_group_by().into_vec(),
                                ed.mut_aggregation().take_agg_func().into_vec(),
                            )?
                            .with_summary_collector(C::new(summary_slot_index)),
                        )
                    }
                }
                ExecType::TypeStreamAgg => {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_stream_aggr"])
                        .inc();

                    Box::new(
                        BatchStreamAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into_vec(),
                            ed.mut_aggregation().take_agg_func().into_vec(),
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeLimit => {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_limit"])
                        .inc();

                    Box::new(
                        BatchLimitExecutor::new(executor, ed.get_limit().get_limit() as usize)?
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeTopN => {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_top_n"])
                        .inc();

                    let mut d = ed.take_topN();
                    let order_bys = d.get_order_by().len();
                    let mut order_exprs_def = Vec::with_capacity(order_bys);
                    let mut order_is_desc = Vec::with_capacity(order_bys);
                    for mut item in d.take_order_by().into_iter() {
                        order_exprs_def.push(item.take_expr());
                        order_is_desc.push(item.get_desc());
                    }

                    Box::new(
                        BatchTopNExecutor::new(
                            config.clone(),
                            executor,
                            order_exprs_def,
                            order_is_desc,
                            d.get_limit() as usize,
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
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
    pub fn build_normal<S: Store + 'static, C: ExecSummaryCollector + 'static>(
        exec_descriptors: Vec<executor::Executor>,
        store: S,
        ranges: Vec<KeyRange>,
        ctx: Arc<EvalConfig>,
    ) -> Result<Box<dyn Executor>> {
        let mut exec_descriptors = exec_descriptors.into_iter();
        let first = exec_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("has no executor")))?;

        let mut src = Self::build_normal_first_executor::<_, C>(first, store, ranges)?;
        let mut summary_slot_index = 0;

        for mut exec in exec_descriptors {
            summary_slot_index += 1;

            let curr: Box<dyn Executor> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!(
                        "Unexpected non-first executor: {:?}",
                        exec.get_tp()
                    ));
                }
                ExecType::TypeSelection => {
                    COPR_EXECUTOR_COUNT.with_label_values(&["selection"]).inc();

                    Box::new(
                        SelectionExecutor::new(exec.take_selection(), Arc::clone(&ctx), src)?
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeAggregation => {
                    COPR_EXECUTOR_COUNT.with_label_values(&["hash_aggr"]).inc();

                    Box::new(
                        HashAggExecutor::new(exec.take_aggregation(), Arc::clone(&ctx), src)?
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeStreamAgg => {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["stream_aggr"])
                        .inc();

                    Box::new(
                        StreamAggExecutor::new(Arc::clone(&ctx), src, exec.take_aggregation())?
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeTopN => {
                    COPR_EXECUTOR_COUNT.with_label_values(&["top_n"]).inc();

                    Box::new(
                        TopNExecutor::new(exec.take_topN(), Arc::clone(&ctx), src)?
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
                ExecType::TypeLimit => {
                    COPR_EXECUTOR_COUNT.with_label_values(&["limit"]).inc();

                    Box::new(
                        LimitExecutor::new(exec.take_limit(), src)
                            .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
            };
            src = curr;
        }
        Ok(src)
    }

    /// Builds the inner-most executor for the normal executor pipeline, which can produce rows to
    /// other executors and never receive rows from other executors.
    ///
    /// The inner-most executor must be a table scan executor or an index scan executor.
    fn build_normal_first_executor<S: Store + 'static, C: ExecSummaryCollector + 'static>(
        mut first: executor::Executor,
        store: S,
        ranges: Vec<KeyRange>,
    ) -> Result<Box<dyn Executor>> {
        match first.get_tp() {
            ExecType::TypeTableScan => {
                COPR_EXECUTOR_COUNT.with_label_values(&["table_scan"]).inc();

                let ex = Box::new(
                    ScanExecutor::table_scan(first.take_tbl_scan(), ranges, store)?
                        .with_summary_collector(C::new(0)),
                );
                Ok(ex)
            }
            ExecType::TypeIndexScan => {
                COPR_EXECUTOR_COUNT.with_label_values(&["index_scan"]).inc();

                let unique = first.get_idx_scan().get_unique();
                let ex = Box::new(
                    ScanExecutor::index_scan(first.take_idx_scan(), ranges, store, unique)?
                        .with_summary_collector(C::new(0)),
                );
                Ok(ex)
            }
            _ => Err(box_err!(
                "first exec type should be *Scan, but get {:?}",
                first.get_tp()
            )),
        }
    }

    fn build_dag<S: Store + 'static>(
        eval_cfg: EvalConfig,
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
    ) -> Result<super::DAGRequestHandler> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();

        let executor = if !(req.get_collect_execution_summaries()) {
            Self::build_normal::<_, ExecSummaryCollectorDisabled>(
                req.take_executors().into_vec(),
                store,
                ranges,
                Arc::new(eval_cfg),
            )?
        } else {
            Self::build_normal::<_, ExecSummaryCollectorEnabled>(
                req.take_executors().into_vec(),
                store,
                ranges,
                Arc::new(eval_cfg),
            )?
        };
        Ok(super::DAGRequestHandler::new(
            deadline,
            executor,
            req.take_output_offsets(),
            batch_row_limit,
            collect_exec_summary,
            ExecuteStats::new(if collect_exec_summary {
                executors_len
            } else {
                0 // Avoid allocation for executor summaries when it is not needed
            }),
        ))
    }

    fn build_batch_dag<S: Store + 'static>(
        deadline: Deadline,
        config: EvalConfig,
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
    ) -> Result<super::batch_handler::BatchDAGHandler> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();

        let config = Arc::new(config);
        let out_most_executor = if collect_exec_summary {
            super::builder::DAGBuilder::build_batch::<_, ExecSummaryCollectorEnabled>(
                req.take_executors().into_vec(),
                store,
                ranges,
                config.clone(),
            )?
        } else {
            super::builder::DAGBuilder::build_batch::<_, ExecSummaryCollectorDisabled>(
                req.take_executors().into_vec(),
                store,
                ranges,
                config.clone(),
            )?
        };

        // Check output offsets
        let output_offsets = req.take_output_offsets();
        let schema_len = out_most_executor.schema().len();
        for offset in &output_offsets {
            if (*offset as usize) >= schema_len {
                return Err(box_err!(
                    "Invalid output offset (schema has {} columns, access index {})",
                    schema_len,
                    offset
                ));
            }
        }

        Ok(super::batch_handler::BatchDAGHandler::new(
            deadline,
            out_most_executor,
            output_offsets,
            config,
            collect_exec_summary,
            ExecuteStats::new(if collect_exec_summary {
                executors_len
            } else {
                0 // Avoid allocation for executor summaries when it is not needed
            }),
        ))
    }

    pub fn build<S: Store + 'static>(
        req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        enable_batch_if_possible: bool,
    ) -> Result<Box<dyn RequestHandler>> {
        let mut eval_cfg = EvalConfig::from_flag(Flag::from_bits_truncate(req.get_flags()));
        // We respect time zone name first, then offset.
        if req.has_time_zone_name() && !req.get_time_zone_name().is_empty() {
            box_try!(eval_cfg.set_time_zone_by_name(req.get_time_zone_name()));
        } else if req.has_time_zone_offset() {
            box_try!(eval_cfg.set_time_zone_by_offset(req.get_time_zone_offset()));
        } else {
            // This should not be reachable. However we will not panic here in case
            // of compatibility issues.
        }
        if req.has_max_warning_count() {
            eval_cfg.set_max_warning_cnt(req.get_max_warning_count() as usize);
        }
        if req.has_sql_mode() {
            eval_cfg.set_sql_mode(SqlMode::from_bits_truncate(req.get_sql_mode()));
        }

        let mut is_batch = false;
        if enable_batch_if_possible && !is_streaming {
            let build_batch_result =
                super::builder::DAGBuilder::check_build_batch(req.get_executors());
            if let Err(e) = build_batch_result {
                debug!("Successfully use normal Coprocessor query engine"; "start_ts" => req.get_start_ts(), "reason" => %e);
            } else {
                is_batch = true;
            }
        }

        if is_batch {
            COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
            Ok(Self::build_batch_dag(deadline, eval_cfg, req, ranges, store)?.into_boxed())
        } else {
            COPR_DAG_REQ_COUNT.with_label_values(&["normal"]).inc();
            Ok(
                Self::build_dag(eval_cfg, req, ranges, store, deadline, batch_row_limit)?
                    .into_boxed(),
            )
        }
    }
}
