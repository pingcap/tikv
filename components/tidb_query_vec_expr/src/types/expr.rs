// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;

use tipb::{ExprType, FieldType};

use super::super::function::RpnFnMeta;
use tidb_query_datatype::codec::data_type::ScalarValue;

#[derive(Debug, Clone, Copy)]
pub enum Operator {
    AggBitAnd,
    AggBitOr,
    AggBitXor,
}

impl From<ExprType> for Operator {
    fn from(expr: ExprType) -> Self {
        match expr {
            ExprType::AggBitAnd => Self::AggBitAnd,
            ExprType::AggBitOr => Self::AggBitOr,
            ExprType::AggBitXor => Self::AggBitXor,
            _ => panic!("not expected expr: {:?}", expr),
        }
    }
}

impl Into<ExprType> for Operator {
    fn into(self) -> ExprType {
        match self {
            Self::AggBitAnd => ExprType::AggBitAnd,
            Self::AggBitOr => ExprType::AggBitOr,
            Self::AggBitXor => ExprType::AggBitXor,
        }
    }
}

/// A type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function call.
    FnCall {
        func_meta: RpnFnMeta,
        args_len: usize,
        field_type: FieldType,
        metadata: Box<dyn Any + Send>,
    },

    /// Represents a scalar constant value.
    Constant {
        value: ScalarValue,
        field_type: FieldType,
    },

    /// Represents a reference to a column in the columns specified in evaluation.
    ColumnRef {
        offset: usize,
    },
    // Represents a operator call
    Operator {
        op: Operator,
        field_type: FieldType,
    },
}

impl RpnExpressionNode {
    /// Gets the field type.
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnExpressionNode::FnCall { field_type, .. } => field_type,
            RpnExpressionNode::Constant { field_type, .. } => field_type,
            RpnExpressionNode::Operator { field_type, .. } => field_type,
            RpnExpressionNode::ColumnRef { .. } => panic!(),
        }
    }

    pub fn expr_tp(&self) -> tipb::ExprType {
        use tidb_query_datatype::EvalType;

        match self {
            RpnExpressionNode::FnCall { .. } => ExprType::ScalarFunc,
            RpnExpressionNode::Constant { value, .. } => match value.eval_type() {
                EvalType::Bytes => ExprType::Bytes,
                EvalType::DateTime => ExprType::MysqlTime,
                EvalType::Decimal => ExprType::MysqlDecimal,
                EvalType::Duration => ExprType::MysqlDuration,
                EvalType::Int => ExprType::Int64,
                EvalType::Json => ExprType::MysqlJson,
                EvalType::Real => ExprType::Float64,
            },
            RpnExpressionNode::ColumnRef { .. } => ExprType::ColumnRef,
            RpnExpressionNode::Operator { op, .. } => (*op).into(),
        }
    }

    /// Borrows the function instance for `FnCall` variant.
    #[cfg(test)]
    pub fn fn_call_func(&self) -> RpnFnMeta {
        match self {
            RpnExpressionNode::FnCall { func_meta, .. } => *func_meta,
            _ => panic!(),
        }
    }

    /// Borrows the constant value for `Constant` variant.
    #[cfg(test)]
    pub fn constant_value(&self) -> &ScalarValue {
        match self {
            RpnExpressionNode::Constant { value, .. } => value,
            _ => panic!(),
        }
    }
}

/// An expression in Reverse Polish notation, which is simply a list of RPN expression nodes.
///
/// You may want to build it using `RpnExpressionBuilder`.
#[derive(Debug)]
pub struct RpnExpression(Vec<RpnExpressionNode>);

impl std::ops::Deref for RpnExpression {
    type Target = Vec<RpnExpressionNode>;

    fn deref(&self) -> &Vec<RpnExpressionNode> {
        &self.0
    }
}

impl std::ops::DerefMut for RpnExpression {
    fn deref_mut(&mut self) -> &mut Vec<RpnExpressionNode> {
        &mut self.0
    }
}

impl From<Vec<RpnExpressionNode>> for RpnExpression {
    fn from(v: Vec<RpnExpressionNode>) -> Self {
        Self(v)
    }
}

impl AsRef<[RpnExpressionNode]> for RpnExpression {
    fn as_ref(&self) -> &[RpnExpressionNode] {
        self.0.as_ref()
    }
}

impl AsMut<[RpnExpressionNode]> for RpnExpression {
    fn as_mut(&mut self) -> &mut [RpnExpressionNode] {
        self.0.as_mut()
    }
}

impl RpnExpression {
    /// Gets the field type of the return value.
    pub fn ret_field_type<'a>(&'a self, schema: &'a [FieldType]) -> &'a FieldType {
        assert!(!self.0.is_empty());
        let last_node = self.0.last().unwrap();
        match last_node {
            RpnExpressionNode::FnCall { field_type, .. } => field_type,
            RpnExpressionNode::Constant { field_type, .. } => field_type,
            RpnExpressionNode::Operator { field_type, .. } => field_type,
            RpnExpressionNode::ColumnRef { offset } => &schema[*offset],
        }
    }

    /// Unwraps into the underlying expression node vector.
    pub fn into_inner(self) -> Vec<RpnExpressionNode> {
        self.0
    }

    /// Returns true if the last element of expression is a `Constant` variant.
    pub fn is_last_constant(&self) -> bool {
        assert!(!self.0.is_empty());
        match self.0.last().unwrap() {
            RpnExpressionNode::Constant { .. } => true,
            _ => false,
        }
    }
}

// For `RpnExpression::eval`, see `expr_eval` file.
