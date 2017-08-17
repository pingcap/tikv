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
mod builtin_cast;
mod compare;
mod fncall;

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
                ScalarFuncSig::LTInt |
                ScalarFuncSig::LEInt |
                ScalarFuncSig::GTInt |
                ScalarFuncSig::GEInt |
                ScalarFuncSig::EQInt |
                ScalarFuncSig::NEInt |
                ScalarFuncSig::NullEQInt => f.compare_int(ctx, row, f.sig),

                ScalarFuncSig::LTReal |
                ScalarFuncSig::LEReal |
                ScalarFuncSig::GTReal |
                ScalarFuncSig::GEReal |
                ScalarFuncSig::EQReal |
                ScalarFuncSig::NEReal |
                ScalarFuncSig::NullEQReal => f.compare_real(ctx, row, f.sig),

                ScalarFuncSig::LTDecimal |
                ScalarFuncSig::LEDecimal |
                ScalarFuncSig::GTDecimal |
                ScalarFuncSig::GEDecimal |
                ScalarFuncSig::EQDecimal |
                ScalarFuncSig::NEDecimal |
                ScalarFuncSig::NullEQDecimal => f.compare_decimal(ctx, row, f.sig),

                ScalarFuncSig::LTString |
                ScalarFuncSig::LEString |
                ScalarFuncSig::GTString |
                ScalarFuncSig::GEString |
                ScalarFuncSig::EQString |
                ScalarFuncSig::NEString |
                ScalarFuncSig::NullEQString => f.compare_string(ctx, row, f.sig),

                ScalarFuncSig::LTTime |
                ScalarFuncSig::LETime |
                ScalarFuncSig::GTTime |
                ScalarFuncSig::GETime |
                ScalarFuncSig::EQTime |
                ScalarFuncSig::NETime |
                ScalarFuncSig::NullEQTime => f.compare_time(ctx, row, f.sig),

                ScalarFuncSig::LTDuration |
                ScalarFuncSig::LEDuration |
                ScalarFuncSig::GTDuration |
                ScalarFuncSig::GEDuration |
                ScalarFuncSig::EQDuration |
                ScalarFuncSig::NEDuration |
                ScalarFuncSig::NullEQDuration => f.compare_duration(ctx, row, f.sig),

                ScalarFuncSig::LTJson |
                ScalarFuncSig::LEJson |
                ScalarFuncSig::GTJson |
                ScalarFuncSig::GEJson |
                ScalarFuncSig::EQJson |
                ScalarFuncSig::NEJson |
                ScalarFuncSig::NullEQJson => f.compare_json(ctx, row, f.sig),
                _ => Err(Error::Other("Unknown signature")),
            },
        }
    }

    fn eval_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        match *self {
            Expression::Constant(ref constant) => constant.eval_real(),
            Expression::ColumnRef(ref column) => column.eval_real(row),
            _ => unimplemented!(),
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
            _ => unimplemented!(),
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
            _ => unimplemented!(),
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
            _ => unimplemented!(),
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
            _ => unimplemented!(),
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

    pub fn fncall_expr(sig: ScalarFuncSig, ft: FieldType, children: &[Expr]) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(sig);
        expr.set_field_type(ft);
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
                fncall_expr(
                    ScalarFuncSig::LTInt,
                    FieldType::new(),
                    &[colref.clone(), constant.clone()],
                ),
                2,
                true,
            ),
            (
                fncall_expr(ScalarFuncSig::LTInt, FieldType::new(), &[colref.clone()]),
                0,
                false,
            ),
        ];

        for tt in tests.into_iter() {
            let expr = Expression::build(tt.0, tt.1);
            assert_eq!(expr.is_ok(), tt.2);
        }
    }
}
