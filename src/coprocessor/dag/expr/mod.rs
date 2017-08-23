// Copyright 2017 PingCAP, Inc.
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
// FIXME(shirly): remove following later
#![allow(dead_code, unused_variables)]

mod column;
mod constant;
mod fncall;
mod builtin_cast;
mod builtin_control;
mod builtin_op;
mod compare;
use self::compare::CmpOp;

use std::io;
use std::borrow::Cow;
use std::string::FromUtf8Error;

use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};

use coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::mysql::types;
use coprocessor::codec::Datum;
use util;
use util::codec::number::NumberDecoder;
use util::codec::Error as CError;

pub use coprocessor::select::xeval::EvalContext as StatementContext;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Type { has: &'static str, expected: &'static str } {
            description("type error")
            display("type error: cannot get {:?} result from {:?} expression", expected, has)
        }
        Codec(err: util::codec::Error) {
            from()
            description("codec error")
            display("codec error: {}", err)
            cause(err)
        }
        ColumnOffset(offset: usize) {
            description("column offset not found")
            display("illegal column offset: {}", offset)
        }
        Other(desc: &'static str) {
            description(desc)
            display("error {}", desc)
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Codec(CError::Encoding(err.utf8_error().into()))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Constant(Constant),
    ColumnRef(Column),
    ScalarFn(FnCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    offset: usize,
    tp: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Constant {
    val: Datum,
    tp: FieldType,
}

/// A single scalar function call
#[derive(Debug, Clone, PartialEq)]
pub struct FnCall {
    sig: ScalarFuncSig,
    children: Vec<Expression>,
    tp: FieldType,
}

impl Expression {
    fn new_const(v: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(Constant {
            val: v,
            tp: field_type,
        })
    }

    fn get_tp(&self) -> &FieldType {
        match *self {
            Expression::Constant(ref c) => &c.tp,
            Expression::ColumnRef(ref c) => &c.tp,
            Expression::ScalarFn(ref c) => &c.tp,
        }
    }

    fn eval_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_int(),
            Expression::ColumnRef(ref column) => column.eval_int(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::LTInt => f.compare_int(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEInt => f.compare_int(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTInt => f.compare_int(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEInt => f.compare_int(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQInt => f.compare_int(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEInt => f.compare_int(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQInt => f.compare_int(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTReal => f.compare_real(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEReal => f.compare_real(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTReal => f.compare_real(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEReal => f.compare_real(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQReal => f.compare_real(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEReal => f.compare_real(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQReal => f.compare_real(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTDecimal => f.compare_decimal(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEDecimal => f.compare_decimal(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTDecimal => f.compare_decimal(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEDecimal => f.compare_decimal(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQDecimal => f.compare_decimal(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEDecimal => f.compare_decimal(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQDecimal => f.compare_decimal(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTString => f.compare_string(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEString => f.compare_string(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTString => f.compare_string(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEString => f.compare_string(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQString => f.compare_string(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEString => f.compare_string(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQString => f.compare_string(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTTime => f.compare_time(ctx, row, CmpOp::LT),
                ScalarFuncSig::LETime => f.compare_time(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTTime => f.compare_time(ctx, row, CmpOp::GT),
                ScalarFuncSig::GETime => f.compare_time(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQTime => f.compare_time(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NETime => f.compare_time(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQTime => f.compare_time(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTDuration => f.compare_duration(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEDuration => f.compare_duration(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTDuration => f.compare_duration(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEDuration => f.compare_duration(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQDuration => f.compare_duration(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEDuration => f.compare_duration(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQDuration => f.compare_duration(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LTJson => f.compare_json(ctx, row, CmpOp::LT),
                ScalarFuncSig::LEJson => f.compare_json(ctx, row, CmpOp::LE),
                ScalarFuncSig::GTJson => f.compare_json(ctx, row, CmpOp::GT),
                ScalarFuncSig::GEJson => f.compare_json(ctx, row, CmpOp::GE),
                ScalarFuncSig::EQJson => f.compare_json(ctx, row, CmpOp::EQ),
                ScalarFuncSig::NEJson => f.compare_json(ctx, row, CmpOp::NE),
                ScalarFuncSig::NullEQJson => f.compare_json(ctx, row, CmpOp::NullEQ),

                ScalarFuncSig::LogicalAnd => f.logical_and(ctx, row),
                ScalarFuncSig::LogicalOr => f.logical_or(ctx, row),
                ScalarFuncSig::LogicalXor => f.logical_xor(ctx, row),

                ScalarFuncSig::UnaryNot => f.unary_not(ctx, row),
                ScalarFuncSig::IntIsNull => f.int_is_null(ctx, row),
                ScalarFuncSig::IntIsFalse => f.int_is_false(ctx, row),
                ScalarFuncSig::RealIsTrue => f.real_is_true(ctx, row),
                ScalarFuncSig::RealIsNull => f.real_is_null(ctx, row),
                ScalarFuncSig::DecimalIsNull => f.decimal_is_null(ctx, row),
                ScalarFuncSig::DecimalIsTrue => f.decimal_is_true(ctx, row),
                ScalarFuncSig::StringIsNull => f.string_is_null(ctx, row),
                ScalarFuncSig::TimeIsNull => f.time_is_null(ctx, row),
                ScalarFuncSig::DurationIsNull => f.duration_is_null(ctx, row),

                ScalarFuncSig::IfNullInt => f.if_null_int(ctx, row),
                ScalarFuncSig::IfInt => f.if_int(ctx, row),

                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_real(),
            Expression::ColumnRef(ref column) => column.eval_real(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::IfNullReal => f.if_null_real(ctx, row),
                ScalarFuncSig::IfReal => f.if_real(ctx, row),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_decimal(),
            Expression::ColumnRef(ref column) => column.eval_decimal(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::IfNullDecimal => f.if_null_decimal(ctx, row),
                ScalarFuncSig::IfDecimal => f.if_decimal(ctx, row),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_string<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_string(),
            Expression::ColumnRef(ref column) => column.eval_string(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::IfNullString => f.if_null_string(ctx, row),
                ScalarFuncSig::IfString => f.if_string(ctx, row),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_time(),
            Expression::ColumnRef(ref column) => column.eval_time(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::IfNullTime => f.if_null_time(ctx, row),
                ScalarFuncSig::IfTime => f.if_time(ctx, row),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_duration(),
            Expression::ColumnRef(ref column) => column.eval_duration(row),
            Expression::ScalarFn(ref f) => match f.sig {
                ScalarFuncSig::IfNullDuration => f.if_null_duration(ctx, row),
                ScalarFuncSig::IfDuration => f.if_duration(ctx, row),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_json(),
            Expression::ColumnRef(ref column) => column.eval_json(row),
            _ => unimplemented!(),
        }
    }

    /// IsHybridType checks whether a ClassString expression is a hybrid type value which will
    /// return different types of value in different context.
    /// For ENUM/SET which is consist of a string attribute `Name` and an int attribute `Value`,
    /// it will cause an error if we convert ENUM/SET to int as a string value.
    /// For Bit/Hex, we will get a wrong result if we convert it to int as a string value.
    /// For example, when convert `0b101` to int, the result should be 5, but we will get
    /// 101 if we regard it as a string.
    fn is_hybrid_type(&self) -> bool {
        match self.get_tp().get_tp() as u8 {
            types::ENUM | types::BIT | types::SET => {
                return true;
            }
            _ => {}
        }
        // TODO:For a constant, the field type will be inferred as `VARCHAR`
        // when the kind of it is `HEX` or `BIT`.
        false
    }
}

impl Expression {
    fn build(mut expr: Expr, row_len: usize) -> Result<Self> {
        let tp = expr.take_field_type();
        match expr.get_tp() {
            ExprType::Null => Ok(Expression::new_const(Datum::Null, tp)),
            ExprType::Int64 => expr.get_val()
                .decode_i64()
                .map(Datum::I64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::Uint64 => expr.get_val()
                .decode_u64()
                .map(Datum::U64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::String | ExprType::Bytes => {
                Ok(Expression::new_const(Datum::Bytes(expr.take_val()), tp))
            }
            ExprType::Float32 | ExprType::Float64 => expr.get_val()
                .decode_f64()
                .map(Datum::F64)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlDuration => expr.get_val()
                .decode_i64()
                .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                .map(Datum::Dur)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::MysqlDecimal => expr.get_val()
                .decode_decimal()
                .map(Datum::Dec)
                .map(|e| Expression::new_const(e, tp))
                .map_err(Error::from),
            ExprType::ScalarFunc => {
                try!(FnCall::check_args(
                    expr.get_sig(),
                    expr.get_children().len()
                ));
                expr.take_children()
                    .into_iter()
                    .map(|child| Expression::build(child, row_len))
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression::ScalarFn(FnCall {
                            sig: expr.get_sig(),
                            children: children,
                            tp: tp,
                        })
                    })
            }
            ExprType::ColumnRef => {
                let offset = try!(expr.get_val().decode_i64().map_err(Error::from)) as usize;
                try!(Column::check_offset(offset, row_len));
                let column = Column {
                    offset: offset,
                    tp: tp,
                };
                Ok(Expression::ColumnRef(column))
            }
            unhandled => unreachable!("can't handle {:?} expr in DAG mode", unhandled),
        }
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::select::xeval::evaluator::test::{col_expr, datum_expr};
    use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};
    use super::Expression;

    #[inline]
    pub fn str2dec(s: &str) -> Datum {
        Datum::Dec(s.parse().unwrap())
    }

    pub fn fncall_expr(sig: ScalarFuncSig, children: &[Expr]) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(sig);
        expr.set_field_type(FieldType::new());
        for child in children {
            expr.mut_children().push(child.clone());
        }
        expr
    }

    #[test]
    fn test_expression_build() {
        let colref = col_expr(1);
        let constant = datum_expr(Datum::Null);

        let tests = vec![
            (colref.clone(), 1, false),
            (colref.clone(), 2, true),
            (constant.clone(), 0, true),
            (
                fncall_expr(ScalarFuncSig::LTInt, &[colref.clone(), constant.clone()]),
                2,
                true,
            ),
            (
                fncall_expr(ScalarFuncSig::LTInt, &[colref.clone()]),
                0,
                false,
            ),
        ];

        for tt in tests {
            let expr = Expression::build(tt.0, tt.1);
            assert_eq!(expr.is_ok(), tt.2);
        }
    }
}
