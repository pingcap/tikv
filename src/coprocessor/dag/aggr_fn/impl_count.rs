// Copyright 2019 PingCAP, Inc.
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

use cop_datatype::{FieldTypeFlag, FieldTypeTp};
use tipb::expression::{Expr, ExprType, FieldType};

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

pub struct AggrFnDefinitionParserCount;

impl super::parser::Parser for AggrFnDefinitionParserCount {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        if aggr_def.get_children().len() != 1 {
            return Err(box_err!(
                "Expect 1 parameter, but got {}",
                aggr_def.get_children().len()
            ));
        }

        // Check whether or not the children expr is supported
        let child = &aggr_def.get_children()[0];
        RpnExpressionBuilder::check_expr_tree_supported(child)?;

        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;

        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        let child = aggr_def.take_children().into_iter().next().unwrap();

        // COUNT outputs one column.
        out_schema.push({
            let mut ft = FieldType::new();
            ft.as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong)
                .set_flag(FieldTypeFlag::UNSIGNED);
            ft
        });

        // COUNT doesn't need to cast, directly use expression.
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            max_columns,
        )?);

        Ok(Box::new(AggrFnCount))
    }
}

#[derive(Debug)]
pub struct AggrFnCount;

impl super::AggrFunction for AggrFnCount {
    #[inline]
    fn name(&self) -> &'static str {
        "AggrFnCount"
    }

    #[inline]
    fn create_state(&self) -> Box<dyn super::AggrFunctionState> {
        Box::new(AggrFnStateCount::new())
    }
}

#[derive(Debug)]
pub struct AggrFnStateCount {
    count: usize,
}

impl AggrFnStateCount {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

// Manually implement `AggrFunctionStateUpdatePartial` to achieve best performance for
// `update_repeat` and `update_vector`. Also note that we support all
// `AggrFunctionStateUpdatePartial` in COUNT.

impl<T: Evaluable> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateCount {
    #[inline]
    fn update(&mut self, _ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn update_repeat(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    fn update_vector(&mut self, _ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()> {
        for value in values {
            if value.is_some() {
                self.count += 1;
            }
        }
        Ok(())
    }
}

impl<T> super::AggrFunctionStateResultPartial<T> for AggrFnStateCount
where
    T: super::AggrResultAppendable + ?Sized,
{
    #[inline]
    default fn push_result(&self, _ctx: &mut EvalContext, _target: &mut T) -> Result<()> {
        panic!("Unmatched result append target type")
    }
}

impl super::AggrFunctionStateResultPartial<Vec<Option<Int>>> for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut Vec<Option<Int>>) -> Result<()> {
        target.push(Some(self.count as Int));
        Ok(())
    }
}

impl super::AggrFunctionState for AggrFnStateCount {}
