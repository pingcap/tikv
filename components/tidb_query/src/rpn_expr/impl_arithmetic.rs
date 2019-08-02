// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use super::super::expr::EvalContext;
use crate::codec::data_type::*;
use crate::codec::{self, div_i64, div_i64_with_u64, div_u64_with_i64, Error};
use crate::Result;

#[rpn_fn]
#[inline]
pub fn arithmetic<A: ArithmeticOp>(
    arg0: &Option<A::T>,
    arg1: &Option<A::T>,
) -> Result<Option<A::T>> {
    if let (Some(lhs), Some(rhs)) = (arg0, arg1) {
        A::calc(lhs, rhs)
    } else {
        // All arithmetical functions with a NULL argument return NULL
        Ok(None)
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn arithmetic_with_ctx<A: ArithmeticOpWithCtx>(
    ctx: &mut EvalContext,
    arg0: &Option<A::T>,
    arg1: &Option<A::T>,
) -> Result<Option<A::T>> {
    if let (Some(lhs), Some(rhs)) = (arg0, arg1) {
        A::calc(ctx, lhs, rhs)
    } else {
        Ok(None)
    }
}

pub trait ArithmeticOp {
    type T: Evaluable;

    fn calc(lhs: &Self::T, rhs: &Self::T) -> Result<Option<Self::T>>;
}

pub trait ArithmeticOpWithCtx {
    type T: Evaluable;

    fn calc(ctx: &mut EvalContext, lhs: &Self::T, rhs: &Self::T) -> Result<Option<Self::T>>;
}

#[derive(Debug)]
pub struct IntIntPlus;

impl ArithmeticOp for IntIntPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_add(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} + {})", lhs, rhs)).into())
            .map(Some)
    }
}

#[derive(Debug)]
pub struct IntUintPlus;

impl ArithmeticOp for IntUintPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        let res = if *lhs >= 0 {
            (*lhs as u64).checked_add(*rhs as u64)
        } else {
            (*rhs as u64).checked_sub(lhs.overflowing_neg().0 as u64)
        };
        res.ok_or_else(|| {
            Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", lhs, rhs)).into()
        })
        .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct UintIntPlus;

impl ArithmeticOp for UintIntPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        IntUintPlus::calc(rhs, lhs)
    }
}

#[derive(Debug)]
pub struct UintUintPlus;

impl ArithmeticOp for UintUintPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_add(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct RealPlus;

impl ArithmeticOp for RealPlus {
    type T = Real;

    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = *lhs + *rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} + {})", lhs, rhs)))?;
        }
        Ok(Some(res))
    }
}

#[derive(Debug)]
pub struct DecimalPlus;

impl ArithmeticOp for DecimalPlus {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs + rhs).into();
        Ok(Some(res?))
    }
}

#[derive(Debug)]
pub struct IntIntMinus;

impl ArithmeticOp for IntIntMinus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_sub(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
            .map(Some)
    }
}

#[derive(Debug)]
pub struct IntUintMinus;

impl ArithmeticOp for IntUintMinus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *lhs >= 0 {
            (*lhs as u64)
                .checked_sub(*rhs as u64)
                .ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
                .map(|v| Some(v as i64))
        } else {
            Err(Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)))?
        }
    }
}

#[derive(Debug)]
pub struct UintIntMinus;

impl ArithmeticOp for UintIntMinus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        let res = if *rhs >= 0 {
            (*lhs as u64).checked_sub(*rhs as u64)
        } else {
            (*lhs as u64).checked_add(rhs.overflowing_neg().0 as u64)
        };
        res.ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
            .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct UintUintMinus;

impl ArithmeticOp for UintUintMinus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_sub(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} - {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct RealMinus;

impl ArithmeticOp for RealMinus {
    type T = Real;

    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = *lhs - *rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} - {})", lhs, rhs)))?;
        }
        Ok(Some(res))
    }
}

#[derive(Debug)]
pub struct DecimalMinus;

impl ArithmeticOp for DecimalMinus {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs - rhs).into();
        Ok(Some(res?))
    }
}

#[derive(Debug)]
pub struct IntIntMod;

impl ArithmeticOp for IntIntMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(lhs % rhs))
    }
}

#[derive(Debug)]
pub struct IntUintMod;

impl ArithmeticOp for IntUintMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((lhs.overflowing_abs().0 as u64) % (*rhs as u64)) as i64,
        ))
    }
}

#[derive(Debug)]
pub struct UintIntMod;

impl ArithmeticOp for UintIntMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((*lhs as u64) % (rhs.overflowing_abs().0 as u64)) as i64,
        ))
    }
}

#[derive(Debug)]
pub struct UintUintMod;
impl ArithmeticOp for UintUintMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(((*lhs as u64) % (*rhs as u64)) as i64))
    }
}

#[derive(Debug)]
pub struct RealMod;

impl ArithmeticOp for RealMod {
    type T = Real;

    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        if (*rhs).into_inner() == 0f64 {
            return Ok(None);
        }
        Ok(Some(*lhs % *rhs))
    }
}

#[derive(Debug)]
pub struct DecimalMod;

impl ArithmeticOp for DecimalMod {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        use crate::codec::mysql::Res;

        if rhs.is_zero() {
            return Ok(None);
        }
        match lhs % rhs {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(v)),
                Res::Truncated(_) => Err(Error::truncated())?,
                Res::Overflow(_) => {
                    Err(Error::overflow("DECIMAL", &format!("({} % {})", lhs, rhs)))?
                }
            },
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct DecimalMultiply;

impl ArithmeticOp for DecimalMultiply {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs * rhs).into();
        Ok(Some(res?))
    }
}

#[derive(Debug)]
pub struct RealMultiply;

impl ArithmeticOp for RealMultiply {
    type T = Real;
    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = *lhs * *rhs;
        if res.is_infinite() {
            Err(Error::overflow("REAL", &format!("({} * {})", lhs, rhs)).into())
        } else {
            Ok(Some(res))
        }
    }
}

#[derive(Debug)]
pub struct IntIntMultiply;

impl ArithmeticOp for IntIntMultiply {
    type T = Int;
    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_mul(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} * {})", lhs, rhs)).into())
            .map(Some)
    }
}

#[derive(Debug)]
pub struct IntUintMultiply;

impl ArithmeticOp for IntUintMultiply {
    type T = Int;
    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *lhs >= 0 {
            (*lhs as u64).checked_mul(*rhs as u64).map(|x| x as i64)
        } else {
            None
        }
        .ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", lhs, rhs)).into())
        .map(Some)
    }
}

#[derive(Debug)]
pub struct UintIntMultiply;

impl ArithmeticOp for UintIntMultiply {
    type T = Int;
    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        IntUintMultiply::calc(rhs, lhs)
    }
}

#[derive(Debug)]
pub struct UintUintMultiply;

impl ArithmeticOp for UintUintMultiply {
    type T = Int;
    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_mul(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct IntDivideInt;

impl ArithmeticOp for IntDivideInt {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0 {
            return Ok(None);
        }
        Ok(Some(div_i64(*lhs, *rhs)?))
    }
}

#[derive(Debug)]
pub struct IntDivideUint;

impl ArithmeticOp for IntDivideUint {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0 {
            return Ok(None);
        }
        Ok(Some(div_i64_with_u64(*lhs, *rhs as u64).map(|r| r as i64)?))
    }
}

#[derive(Debug)]
pub struct UintDivideUint;

impl ArithmeticOp for UintDivideUint {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0 {
            return Ok(None);
        }
        Ok(Some(((*lhs as u64) / (*rhs as u64)) as i64))
    }
}

#[derive(Debug)]
pub struct UintDivideInt;

impl ArithmeticOp for UintDivideInt {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0 {
            return Ok(None);
        }
        Ok(Some(div_u64_with_i64(*lhs as u64, *rhs).map(|r| r as i64)?))
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
fn int_divide_decimal(
    ctx: &mut EvalContext,
    lhs: &Option<Decimal>,
    rhs: &Option<Decimal>,
) -> Result<Option<Int>> {
    use crate::codec::mysql::Res;

    if lhs.is_none() || rhs.is_none() {
        return Ok(None);
    }
    let lhs = lhs.as_ref().unwrap();
    let rhs = rhs.as_ref().unwrap();

    match lhs / rhs {
        Some(v) => match v {
            Res::Ok(v) => match v.as_i64() {
                Res::Ok(v_i64) => Ok(Some(v_i64)),
                Res::Truncated(v_i64) => Ok(Some(v_i64)),
                Res::Overflow(_) => {
                    Err(Error::overflow("BIGINT", &format!("({} / {})", lhs, rhs)))?
                }
            },
            Res::Truncated(_) => Err(Error::truncated())?,
            Res::Overflow(_) => Err(Error::overflow("DECIMAL", &format!("({} / {})", lhs, rhs)))?,
        },
        None => Ok(ctx.handle_division_by_zero().map(|()| None)?),
    }
}

pub struct DecimalDivide;

impl ArithmeticOpWithCtx for DecimalDivide {
    type T = Decimal;

    fn calc(ctx: &mut EvalContext, lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        use crate::codec::mysql::Res;

        Ok(match lhs / rhs {
            Some(value) => match value {
                Res::Ok(value) => Some(value),
                Res::Truncated(_) => ctx.handle_truncate(true).map(|_| None)?,
                Res::Overflow(_) => ctx
                    .handle_overflow(Error::overflow("DECIMAL", &format!("({} / {})", lhs, rhs)))
                    .map(|_| None)?,
            },
            None => ctx.handle_division_by_zero().map(|_| None)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::{FieldTypeFlag, FieldTypeTp};
    use tipb::expression::ScalarFuncSig;

    use crate::codec::error::ERR_DIVISION_BY_ZERO;
    use crate::expr::{EvalConfig, Flag, SqlMode};
    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_plus_int() {
        let test_cases = vec![
            (None, false, Some(1), false, None),
            (Some(1), false, None, false, None),
            (Some(17), false, Some(25), false, Some(42)),
            (
                Some(std::i64::MIN),
                false,
                Some((std::i64::MAX as u64 + 1) as i64),
                true,
                Some(0),
            ),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate(ScalarFuncSig::PlusInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_plus_real() {
        let test_cases = vec![
            (
                Real::new(1.01001).ok(),
                Real::new(-0.01).ok(),
                Real::new(1.00001).ok(),
                false,
            ),
            (Real::new(1e308).ok(), Real::new(1e308).ok(), None, true),
        ];
        for (lhs, rhs, expected, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::PlusReal);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_plus_decimal() {
        let test_cases = vec![("1.1", "2.2", "3.3")];
        for (lhs, rhs, expected) in test_cases {
            let expected: Option<Decimal> = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::PlusDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_minus_int() {
        let test_cases = vec![
            (None, false, Some(1), false, None, false),
            (Some(1), false, None, false, None, false),
            (Some(12), false, Some(1), false, Some(11), false),
            (
                Some(0),
                true,
                Some(std::i64::MIN),
                false,
                Some((std::i64::MAX as u64 + 1) as i64),
                false,
            ),
            (
                Some(std::i64::MIN),
                false,
                Some(std::i64::MAX),
                false,
                None,
                true,
            ),
            (
                Some(std::i64::MAX),
                false,
                Some(std::i64::MIN),
                false,
                None,
                true,
            ),
            (Some(-1), false, Some(2), true, None, true),
            (Some(1), true, Some(2), false, None, true),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected, is_err) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate(ScalarFuncSig::MinusInt);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_minus_real() {
        let test_cases = vec![
            (
                Real::new(1.01001).ok(),
                Real::new(-0.01).ok(),
                Real::new(1.02001).ok(),
                false,
            ),
            (
                Real::new(std::f64::MIN).ok(),
                Real::new(std::f64::MAX).ok(),
                None,
                true,
            ),
        ];
        for (lhs, rhs, expected, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::MinusReal);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_minus_decimal() {
        let test_cases = vec![("1.1", "2.2", "-1.1")];
        for (lhs, rhs, expected) in test_cases {
            let expected: Option<Decimal> = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::MinusDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_int() {
        let tests = vec![
            (Some(13), Some(11), Some(2)),
            (Some(-13), Some(11), Some(-2)),
            (Some(13), Some(-11), Some(2)),
            (Some(-13), Some(-11), Some(-2)),
            (Some(33), Some(11), Some(0)),
            (Some(33), Some(-11), Some(0)),
            (Some(-33), Some(-11), Some(0)),
            (Some(-11), None, None),
            (None, Some(-11), None),
            (Some(11), Some(0), None),
            (Some(-11), Some(0), None),
            (
                Some(std::i64::MAX),
                Some(std::i64::MIN),
                Some(std::i64::MAX),
            ),
            (Some(std::i64::MIN), Some(std::i64::MAX), Some(-1)),
        ];

        for (lhs, rhs, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::ModInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_int_unsigned() {
        let tests = vec![
            (
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
                false,
                Some(std::i64::MAX),
            ),
            (
                Some(std::i64::MIN),
                false,
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
            ),
        ];

        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in tests {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate(ScalarFuncSig::ModInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_real() {
        let tests = vec![
            (Real::new(1.0).ok(), None, None),
            (None, Real::new(1.0).ok(), None),
            (
                Real::new(1.0).ok(),
                Real::new(1.1).ok(),
                Real::new(1.0).ok(),
            ),
            (
                Real::new(-1.0).ok(),
                Real::new(1.1).ok(),
                Real::new(-1.0).ok(),
            ),
            (
                Real::new(1.0).ok(),
                Real::new(-1.1).ok(),
                Real::new(1.0).ok(),
            ),
            (
                Real::new(-1.0).ok(),
                Real::new(-1.1).ok(),
                Real::new(-1.0).ok(),
            ),
            (Real::new(1.0).ok(), Real::new(0.0).ok(), None),
        ];

        for (lhs, rhs, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::ModReal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_decimal() {
        let tests = vec![
            ("13", "11", "2"),
            ("-13", "11", "-2"),
            ("13", "-11", "2"),
            ("-13", "-11", "-2"),
            ("33", "11", "0"),
            ("-33", "11", "0"),
            ("33", "-11", "0"),
            ("-33", "-11", "0"),
            ("0.0000000001", "1.0", "0.0000000001"),
            ("1", "1.1", "1"),
            ("-1", "1.1", "-1"),
            ("1", "-1.1", "1"),
            ("-1", "-1.1", "-1"),
            ("3", "0", ""),
            ("-3", "0", ""),
            ("0", "0", ""),
            ("-3", "", ""),
            ("", ("-3"), ""),
            ("", "", ""),
        ];

        for (lhs, rhs, expected) in tests {
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::ModDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_multiply_decimal() {
        let test_cases = vec![("1.1", "2.2", "2.42")];
        for (lhs, rhs, expected) in test_cases {
            let expected: Option<Decimal> = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::MultiplyDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_int_divide_int() {
        let test_cases = vec![
            (13, false, 11, false, Some(1)),
            (13, false, -11, false, Some(-1)),
            (-13, false, 11, false, Some(-1)),
            (-13, false, -11, false, Some(1)),
            (33, false, 11, false, Some(3)),
            (33, false, -11, false, Some(-3)),
            (-33, false, 11, false, Some(-3)),
            (-33, false, -11, false, Some(3)),
            (11, false, 0, false, None),
            (-11, false, 0, false, None),
            (-3, false, 5, true, Some(0)),
            (3, false, -5, false, Some(0)),
            (std::i64::MIN + 1, false, -1, false, Some(std::i64::MAX)),
            (std::i64::MIN, false, 1, false, Some(std::i64::MIN)),
            (std::i64::MAX, false, 1, false, Some(std::i64::MAX)),
            (
                std::u64::MAX as i64,
                true,
                1,
                false,
                Some(std::u64::MAX as i64),
            ),
        ];

        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate(ScalarFuncSig::IntDivideInt)
                .unwrap();

            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_int_divide_int_overflow() {
        let test_cases = vec![
            (std::i64::MIN, false, -1, false),
            (-1, false, 1, true),
            (-2, false, 1, true),
            (1, true, -1, false),
            (2, true, -1, false),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            let output: Result<Option<Int>> = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate(ScalarFuncSig::IntDivideInt);
            assert!(output.is_err(), "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_int_divide_decimal() {
        let test_cases = vec![
            (Some(11.01), Some(1.1), Some(10)),
            (Some(-11.01), Some(1.1), Some(-10)),
            (Some(11.01), Some(-1.1), Some(-10)),
            (Some(-11.01), Some(-1.1), Some(10)),
            (Some(123.0), None, None),
            (None, Some(123.0), None),
            // divide by zero
            (Some(0.0), Some(0.0), None),
            (None, None, None),
        ];

        for (lhs, rhs, expected) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.map(|f| Decimal::from_f64(f).unwrap()))
                .push_param(rhs.map(|f| Decimal::from_f64(f).unwrap()))
                .evaluate(ScalarFuncSig::IntDivideDecimal)
                .unwrap();

            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_int_divide_decimal_overflow() {
        let test_cases = vec![
            (Decimal::from(std::i64::MIN), Decimal::from(-1)),
            (
                Decimal::from(std::i64::MAX),
                Decimal::from_f64(0.1).unwrap(),
            ),
        ];

        for (lhs, rhs) in test_cases {
            let output: Result<Option<Int>> = RpnFnScalarEvaluator::new()
                .push_param(lhs.clone())
                .push_param(rhs.clone())
                .evaluate(ScalarFuncSig::IntDivideDecimal);

            assert!(output.is_err(), "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_real_multiply() {
        let should_pass = vec![(1.01001, -0.01, Real::new(-0.0101001).ok())];

        for (lhs, rhs, expected) in should_pass {
            assert_eq!(
                expected,
                RpnFnScalarEvaluator::new()
                    .push_param(lhs)
                    .push_param(rhs)
                    .evaluate(ScalarFuncSig::MultiplyReal)
                    .unwrap()
            );
        }

        let should_fail = vec![
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MAX, std::f64::MIN),
        ];

        for (lhs, rhs) in should_fail {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param(lhs)
                    .push_param(rhs)
                    .evaluate::<Real>(ScalarFuncSig::MultiplyReal)
                    .is_err(),
                "{} * {} should fail",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_int_multiply() {
        let should_pass = vec![
            (11, 17, Some(187)),
            (-1, -3, Some(3)),
            (1, std::i64::MIN, Some(std::i64::MIN)),
        ];
        for (lhs, rhs, expected) in should_pass {
            assert_eq!(
                expected,
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, FieldTypeTp::LongLong)
                    .push_param_with_field_type(rhs, FieldTypeTp::LongLong)
                    .evaluate(ScalarFuncSig::MultiplyInt)
                    .unwrap()
            );
        }

        let should_fail = vec![(std::i64::MAX, 2), (std::i64::MIN, -1)];
        for (lhs, rhs) in should_fail {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, FieldTypeTp::LongLong)
                    .push_param_with_field_type(rhs, FieldTypeTp::LongLong)
                    .evaluate::<Int>(ScalarFuncSig::MultiplyInt)
                    .is_err(),
                "{} * {} should fail",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_int_uint_multiply() {
        let should_pass = vec![(std::i64::MAX, 1, Some(std::i64::MAX)), (3, 7, Some(21))];

        for (lhs, rhs, expected) in should_pass {
            assert_eq!(
                expected,
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, FieldTypeTp::LongLong)
                    .push_param_with_field_type(
                        rhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .evaluate(ScalarFuncSig::MultiplyInt)
                    .unwrap()
            );
        }

        let should_fail = vec![(-2, 1), (std::i64::MIN, 2)];
        for (lhs, rhs) in should_fail {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, FieldTypeTp::LongLong)
                    .push_param_with_field_type(
                        rhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .evaluate::<Int>(ScalarFuncSig::MultiplyInt)
                    .is_err(),
                "{} * {} should fail",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_uint_uint_multiply() {
        let should_pass = vec![
            (7, 11, Some(77)),
            (1, 2, Some(2)),
            (std::u64::MAX as i64, 1, Some(std::u64::MAX as i64)),
        ];

        for (lhs, rhs, expected) in should_pass {
            assert_eq!(
                expected,
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(
                        lhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .push_param_with_field_type(
                        rhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .evaluate(ScalarFuncSig::MultiplyIntUnsigned)
                    .unwrap()
            );
        }

        let should_fail = vec![(std::u64::MAX as i64, 2)];
        for (lhs, rhs) in should_fail {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(
                        lhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .push_param_with_field_type(
                        rhs,
                        FieldTypeBuilder::new()
                            .tp(FieldTypeTp::LongLong)
                            .flag(FieldTypeFlag::UNSIGNED)
                    )
                    .evaluate::<Int>(ScalarFuncSig::MultiplyIntUnsigned)
                    .is_err(),
                "{} * {} should fail",
                lhs,
                rhs
            );
        }
    }

    #[test]
    fn test_decimal_divide() {
        let normal = vec![
            (str2dec("2.2"), str2dec("1.1"), str2dec("2.0")),
            (str2dec("2.33"), str2dec("-0.01"), str2dec("-233")),
            (str2dec("2.33"), str2dec("0.01"), str2dec("233")),
            (None, str2dec("2"), None),
            (str2dec("123"), None, None),
        ];

        for (lhs, rhs, expected) in normal {
            let actual = RpnFnScalarEvaluator::new()
                .push_param(lhs.clone())
                .push_param(rhs.clone())
                .evaluate(ScalarFuncSig::DivideDecimal)
                .unwrap();

            assert_eq!(actual, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }

        let abnormal = vec![
            (str2dec("2.33"), str2dec("0.0")),
            (str2dec("2.33"), str2dec("-0.0")),
        ];

        // Vec<[(Flag, SqlMode, is_ok(bool), has_warning(bool))]>
        let modes = vec![
            // Warning
            (Flag::empty(), SqlMode::empty(), true, true),
            // Error
            (
                Flag::IN_UPDATE_OR_DELETE_STMT,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_TABLES,
                false,
                false,
            ),
            // Ok
            (
                Flag::IN_UPDATE_OR_DELETE_STMT,
                SqlMode::STRICT_ALL_TABLES,
                true,
                false,
            ),
            // Warning
            (
                Flag::IN_UPDATE_OR_DELETE_STMT | Flag::DIVIDED_BY_ZERO_AS_WARNING,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_TABLES,
                true,
                true,
            ),
        ];

        for (lhs, rhs) in abnormal {
            for &(flag, sql_mode, is_ok, has_warning) in &modes {
                // Construct an `EvalContext`
                let mut config = EvalConfig::new();
                config.set_flag(flag).set_sql_mode(sql_mode);

                let (result, mut ctx) = RpnFnScalarEvaluator::new()
                    .context(EvalContext::new(std::sync::Arc::new(config)))
                    .push_param(lhs.clone())
                    .push_param(rhs.clone())
                    .evaluate_ctx::<Decimal>(ScalarFuncSig::DivideDecimal);

                if is_ok {
                    assert_eq!(result.unwrap(), None);
                } else {
                    assert!(result.is_err());
                }

                if has_warning {
                    assert_eq!(
                        ctx.take_warnings().warnings[0].get_code(),
                        ERR_DIVISION_BY_ZERO
                    );
                } else {
                    assert!(ctx.take_warnings().warnings.is_empty());
                }
            }
        }
    }

    fn str2dec(s: &str) -> Option<Decimal> {
        s.parse().ok()
    }
}
