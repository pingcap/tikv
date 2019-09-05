// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod aggr_executor;
pub mod hash_aggr_helper;
#[cfg(test)]
pub mod mock_executor;
pub mod scan_executor;

use tikv_util::{erase_lifetime, erase_lifetime_mut};
use tipb::expression::FieldType;

use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::dag::rpn_expr::RpnStackNode;
use crate::coprocessor::Result;

/// Decodes all columns that are not decoded.
pub fn ensure_columns_decoded(
    tz: &Tz,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &mut LazyBatchColumnVec,
    input_logical_rows: &[usize],
) -> Result<()> {
    for expr in exprs {
        expr.ensure_columns_decoded(tz, schema, input_physical_columns, input_logical_rows)?;
    }
    Ok(())
}

/// Evaluates expressions and outputs the result into the given Vec. Lifetime of the expressions
/// are erased.
pub unsafe fn eval_exprs_decoded_no_lifetime<'a>(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    output: &mut Vec<RpnStackNode<'a>>,
) -> Result<()> {
    for expr in exprs {
        output.push(erase_lifetime(expr).eval_decoded(
            erase_lifetime_mut(ctx),
            erase_lifetime(schema),
            erase_lifetime(input_physical_columns),
            erase_lifetime(input_logical_rows),
            input_logical_rows.len(),
        )?)
    }
    Ok(())
}
