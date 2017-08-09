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

use std::io;
use std::convert::TryFrom;

use chrono::FixedOffset;

use tipb::expression::{Expr, ExprType, ScalarFuncSig, FieldType};

use coprocessor::codec::mysql::{Duration, Time, Decimal, MAX_FSP};
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::Datum;
use util;
use util::codec::number::NumberDecoder;

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

pub type Result<T> = ::std::result::Result<T, Error>;

pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

pub struct StatementContext {
    ignore_truncate: bool,
    truncate_as_warning: bool,
    timezone: FixedOffset,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    expr: ExprKind,
    ret_type: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Constant(Datum),
    ColumnRef(usize),
    ScalarFn(FnCall),
}

/// A single scalar function call
#[derive(Debug, Clone, PartialEq)]
pub struct FnCall {
    sig: ScalarFuncSig,
    children: Vec<Expression>,
}

impl Datum {
    fn to_i64(&self, field_type: &FieldType) -> Option<i64> {
        match *self {
            Datum::Null => None,
            _ => Some(self.i64()),
        }
    }

    fn to_f64(&self, field_type: &FieldType) -> Option<f64> {
        match *self {
            Datum::Null => None,
            _ => Some(self.f64()),
        }
    }
}

impl Expression {
    fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<i64>> {
        unimplemented!()
    }

    fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<f64>> {
        unimplemented!()
    }

    fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<String>> {
        unimplemented!()
    }

    fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Time>> {
        unimplemented!()
    }

    fn eval_duration(&self, row: &[Datum], ctx: &StatementContext) -> Result<Option<Duration>> {
        unimplemented!()
    }
}

impl TryFrom<Expr> for Expression {
    type Error = Error;

    fn try_from(expr: Expr) -> ::std::result::Result<Expression, Self::Error> {
        let mut expr = expr;
        let ret_type = expr.take_field_type();
        match expr.get_tp() {
            ExprType::Null => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Null),
                    ret_type: ret_type,
                })
            }
            ExprType::Int64 => {
                expr.get_val()
                    .decode_i64()
                    .map(Datum::I64)
                    .map(|e| {
                        Expression {
                            expr: ExprKind::Constant(e),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            ExprType::Uint64 => {
                expr.get_val()
                    .decode_u64()
                    .map(Datum::U64)
                    .map(|e| {
                        Expression {
                            expr: ExprKind::Constant(e),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            ExprType::String | ExprType::Bytes => {
                Ok(Expression {
                    expr: ExprKind::Constant(Datum::Bytes(expr.take_val())),
                    ret_type: ret_type,
                })
            }
            ExprType::Float32 |
            ExprType::Float64 => {
                expr.get_val()
                    .decode_f64()
                    .map(Datum::F64)
                    .map(|e| {
                        Expression {
                            expr: ExprKind::Constant(e),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            ExprType::MysqlDuration => {
                expr.get_val()
                    .decode_i64()
                    .and_then(|n| Duration::from_nanos(n, MAX_FSP))
                    .map(Datum::Dur)
                    .map(|e| {
                        Expression {
                            expr: ExprKind::Constant(e),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            ExprType::MysqlDecimal => {
                expr.get_val()
                    .decode_decimal()
                    .map(Datum::Dec)
                    .map(|e| {
                        Expression {
                            expr: ExprKind::Constant(e),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            // TODO(andelf): fn sig verification
            ExprType::ScalarFunc => {
                let sig = expr.get_sig();
                let mut expr = expr;
                expr.take_children()
                    .into_iter()
                    .map(Expression::try_from)
                    .collect::<Result<Vec<_>>>()
                    .map(|children| {
                        Expression {
                            expr: ExprKind::ScalarFn(FnCall {
                                sig: sig,
                                children: children,
                            }),
                            ret_type: ret_type,
                        }
                    })
            }
            ExprType::ColumnRef => {
                expr.get_val()
                    .decode_i64()
                    .map(|i| {
                        Expression {
                            expr: ExprKind::ColumnRef(i as usize),
                            ret_type: ret_type,
                        }
                    })
                    .map_err(Error::from)
            }
            unhandled => unreachable!("can't handle {:?} expr in DAG mode", unhandled),
        }
    }
}


#[test]
fn test_smoke() {
    use std::convert::TryInto;
    use util::codec::number::NumberEncoder;

    let mut pb = Expr::new();
    pb.set_tp(ExprType::ColumnRef);
    pb.mut_val().encode_i64(1).unwrap();

    let e: Result<Expression> = pb.try_into();
    let _ = e.unwrap();
}

