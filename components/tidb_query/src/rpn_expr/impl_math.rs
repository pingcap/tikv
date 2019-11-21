use tidb_query_codegen::rpn_fn;

use crc::{crc32, Hasher32};

use crate::codec::data_type::*;
use crate::codec::mysql::{self, RoundMode};
use crate::codec::Error;
use crate::expr::EvalContext;
use crate::Result;

#[rpn_fn]
#[inline]
pub fn pi() -> Result<Option<Real>> {
    Ok(Some(Real::from(std::f64::consts::PI)))
}

#[rpn_fn]
#[inline]
pub fn crc32(arg: &Option<Bytes>) -> Result<Option<Int>> {
    Ok(match arg {
        Some(arg) => {
            let mut digest = crc32::Digest::new(crc32::IEEE);
            digest.write(&arg);
            Some(i64::from(digest.sum32()))
        }
        _ => None,
    })
}

#[inline]
#[rpn_fn]
pub fn log_1_arg(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| f64_to_real(n.ln())))
}

#[inline]
#[rpn_fn]
pub fn log_2_arg(arg0: &Option<Real>, arg1: &Option<Real>) -> Result<Option<Real>> {
    Ok(match (arg0, arg1) {
        (Some(base), Some(n)) => f64_to_real(n.log(**base)),
        _ => None,
    })
}

#[inline]
#[rpn_fn]
pub fn log2(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| f64_to_real(n.log2())))
}

#[inline]
#[rpn_fn]
pub fn log10(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| f64_to_real(n.log10())))
}

// If the given f64 is finite, returns `Some(Real)`. Otherwise returns None.
fn f64_to_real(n: f64) -> Option<Real> {
    if n.is_finite() {
        Real::new(n).ok()
    } else {
        None
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn abs<A: Abs>(ctx: &mut EvalContext, arg: &Option<A::Type>) -> Result<Option<A::Type>> {
    if let Some(arg) = arg {
        A::abs(ctx, arg)
    } else {
        Ok(None)
    }
}

pub trait Abs {
    type Type: Evaluable;

    fn abs(_ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>>;
}

pub struct AbsInt;

impl Abs for AbsInt {
    type Type = Int;

    #[inline]
    fn abs(ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(match (*arg).checked_abs() {
            None => ctx
                .handle_overflow_err(Error::overflow("BIGINT", format!("abs({})", *arg)))
                .map(|_| None)?,
            Some(arg_abs) => Some(arg_abs),
        })
    }
}

pub struct AbsUInt;

impl Abs for AbsUInt {
    type Type = Int;

    #[inline]
    fn abs(_ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Some(*arg))
    }
}

pub struct AbsReal;

impl Abs for AbsReal {
    type Type = Real;

    #[inline]
    fn abs(_ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Some(num_traits::Signed::abs(arg)))
    }
}

pub struct AbsDecimal;

impl Abs for AbsDecimal {
    type Type = Decimal;

    #[inline]
    fn abs(ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(arg.to_owned().abs().into_result(ctx).map(Some)?)
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn ceil<C: Ceil>(ctx: &mut EvalContext, arg: &Option<C::Input>) -> Result<Option<C::Output>> {
    if let Some(arg) = arg {
        C::ceil(ctx, arg)
    } else {
        Ok(None)
    }
}

pub trait Ceil {
    type Input: Evaluable;
    type Output: Evaluable;

    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct CeilReal;

impl Ceil for CeilReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Real::new(arg.ceil()).ok())
    }
}

pub struct CeilDecToDec;

impl Ceil for CeilDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.ceil().into_result(ctx).map(Some)?)
    }
}

pub struct CeilDecToInt;

impl Ceil for CeilDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .ceil()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct CeilIntToInt;

impl Ceil for CeilIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn floor<T: Floor>(ctx: &mut EvalContext, arg: &Option<T::Input>) -> Result<Option<T::Output>> {
    if let Some(arg) = arg {
        T::floor(ctx, arg)
    } else {
        Ok(None)
    }
}

pub trait Floor {
    type Input: Evaluable;
    type Output: Evaluable;

    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct FloorReal;

impl Floor for FloorReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Real::new(arg.floor()).ok())
    }
}

pub struct FloorDecToInt;

impl Floor for FloorDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .floor()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct FloorDecToDec;

impl Floor for FloorDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.floor().into_result(ctx).map(Some)?)
    }
}

pub struct FloorIntToInt;

impl Floor for FloorIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn round<R: Round>(ctx: &mut EvalContext, arg: &Option<R::Type>) -> Result<Option<R::Type>> {
    if let Some(arg) = arg {
        R::round(ctx, arg)
    } else {
        Ok(None)
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn round_with_frac<R: Round>(
    ctx: &mut EvalContext,
    arg0: &Option<R::Type>,
    arg1: &Option<Int>,
) -> Result<Option<R::Type>> {
    match (arg0, arg1) {
        (Some(number), Some(digits)) => R::round_with_frac(ctx, number, *digits),
        _ => Ok(None),
    }
}

pub trait Round {
    type Type: Evaluable;

    fn round(_ctx: &mut EvalContext, _arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(None)
    }
    fn round_with_frac(
        _ctx: &mut EvalContext,
        _number: &Self::Type,
        _digits: Int,
    ) -> Result<Option<Self::Type>> {
        Ok(None)
    }
}

pub struct RoundInt;

impl Round for RoundInt {
    type Type = Int;

    #[inline]
    fn round(_ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Some(*arg))
    }
}

pub struct RoundReal;

impl Round for RoundReal {
    type Type = Real;

    #[inline]
    fn round(_ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Real::new(arg.round()).ok())
    }
}

pub struct RoundDec;

impl Round for RoundDec {
    type Type = Decimal;

    #[inline]
    fn round(ctx: &mut EvalContext, arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(arg
            .to_owned()
            .round(mysql::DEFAULT_FSP, RoundMode::HalfEven)
            .into_result(ctx)
            .map(Some)?)
    }
}

pub struct RoundWithFracInt;

impl Round for RoundWithFracInt {
    type Type = Int;

    #[inline]
    fn round_with_frac(
        _ctx: &mut EvalContext,
        number: &Self::Type,
        digits: Int,
    ) -> Result<Option<Self::Type>> {
        if digits >= 0 {
            Ok(Some(*number))
        } else {
            // The maximum number of digits for the i64 type is 19.
            if digits <= -19 {
                return Ok(Some(0));
            }
            let power = 10.0_f64.powi(-digits as i32);
            let frac = *number as f64 / power;
            Ok(Some((frac.round() * power) as i64))
        }
    }
}

pub struct RoundWithFracReal;

impl Round for RoundWithFracReal {
    type Type = Real;

    #[inline]
    fn round_with_frac(
        _ctx: &mut EvalContext,
        number: &Self::Type,
        digits: Int,
    ) -> Result<Option<Self::Type>> {
        let digits = if digits >= 0 {
            std::cmp::min(i64::from(std::f64::DIGITS), digits)
        } else {
            std::cmp::max(i64::from(std::f64::MIN_10_EXP) - 1, digits)
        };
        let power = 10.0_f64.powi(-digits as i32);
        let frac = number.into_inner() / power;
        Ok(Real::new(frac.round() * power).ok())
    }
}

pub struct RoundWithFracDec;

impl Round for RoundWithFracDec {
    type Type = Decimal;

    #[inline]
    fn round_with_frac(
        ctx: &mut EvalContext,
        number: &Self::Type,
        digits: Int,
    ) -> Result<Option<Self::Type>> {
        let digits = if digits >= 0 {
            std::cmp::min(i64::from(mysql::decimal::MAX_FRACTION), digits)
        } else {
            std::cmp::max(
                -i64::from(mysql::decimal::WORD_BUF_LEN * mysql::decimal::DIGITS_PER_WORD),
                digits,
            )
        };
        Ok(number
            .to_owned()
            .round(digits as i8, RoundMode::HalfEven)
            .into_result(ctx)
            .map(Some)?)
    }
}

#[inline]
#[rpn_fn]
fn sign(arg: &Option<Real>) -> Result<Option<Int>> {
    Ok(arg.and_then(|n| {
        if *n > 0f64 {
            Some(1)
        } else if *n == 0f64 {
            Some(0)
        } else {
            Some(-1)
        }
    }))
}

#[inline]
#[rpn_fn]
fn sqrt(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| {
        if *n < 0f64 {
            None
        } else {
            let res = n.sqrt();
            Real::new(res).ok()
        }
    }))
}

#[inline]
#[rpn_fn]
fn radians(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| Real::new(*n * std::f64::consts::PI / 180_f64).ok()))
}

#[inline]
#[rpn_fn]
pub fn exp(arg: &Option<Real>) -> Result<Option<Real>> {
    match arg {
        Some(x) => {
            let ret = x.exp();
            if ret.is_infinite() {
                Err(Error::overflow("DOUBLE", &format!("exp({})", x)).into())
            } else {
                Ok(Real::new(ret).ok())
            }
        }
        None => Ok(None),
    }
}

#[inline]
#[rpn_fn]
fn degrees(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| Real::new(n.to_degrees()).ok()))
}

#[inline]
#[rpn_fn]
fn sin(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.sin()).ok()))
}

#[inline]
#[rpn_fn]
fn cos(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.cos()).ok()))
}

#[inline]
#[rpn_fn]
fn tan(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.tan()).ok()))
}

#[inline]
#[rpn_fn]
fn cot(arg: &Option<Real>) -> Result<Option<Real>> {
    match arg {
        Some(arg) => {
            let tan = arg.tan();
            let cot = tan.recip();
            if cot.is_infinite() {
                Err(Error::overflow("DOUBLE", format!("cot({})", arg)).into())
            } else {
                Ok(Real::new(cot).ok())
            }
        }
        None => Ok(None),
    }
}

#[inline]
#[rpn_fn]
pub fn asin(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.asin()).ok()))
}

#[inline]
#[rpn_fn]
pub fn acos(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.acos()).ok()))
}

#[inline]
#[rpn_fn]
pub fn atan_1_arg(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map_or(None, |arg| Real::new(arg.atan()).ok()))
}

#[inline]
#[rpn_fn]
pub fn atan_2_args(arg0: &Option<Real>, arg1: &Option<Real>) -> Result<Option<Real>> {
    Ok(match (arg0, arg1) {
        (Some(arg0), Some(arg1)) => Real::new(arg0.atan2(arg1.into_inner())).ok(),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_pi() {
        let output = RpnFnScalarEvaluator::new()
            .evaluate::<Real>(ScalarFuncSig::Pi)
            .unwrap();
        assert_eq!(output, Some(Real::from(std::f64::consts::PI)));
    }

    #[test]
    fn test_crc32() {
        let cases = vec![
            (Some(""), Some(0)),
            (Some("-1"), Some(808273962)),
            (Some("mysql"), Some(2501908538)),
            (Some("MySQL"), Some(3259397556)),
            (Some("hello"), Some(907060870)),
            (Some("❤️"), Some(4067711813)),
            (None, None),
        ];

        for (input, expect) in cases {
            let input = input.map(|s| s.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Crc32)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_log_1_arg() {
        let test_cases = vec![
            (Some(std::f64::consts::E), Some(Real::from(1.0_f64))),
            (Some(100.0), Some(Real::from(4.605170185988092_f64))),
            (Some(-1.0), None),
            (Some(0.0), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Log1Arg)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_log_2_arg() {
        let test_cases = vec![
            (Some(10.0_f64), Some(100.0_f64), Some(Real::from(2.0_f64))),
            (Some(2.0_f64), Some(1.0_f64), Some(Real::from(0.0_f64))),
            (Some(0.5_f64), Some(0.25_f64), Some(Real::from(2.0_f64))),
            (Some(-0.23323_f64), Some(2.0_f64), None),
            (None, None, None),
            (Some(2.0_f64), None, None),
            (None, Some(2.0_f64), None),
        ];
        for (a1, a2, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(a1)
                .push_param(a2)
                .evaluate::<Real>(ScalarFuncSig::Log2Args)
                .unwrap();
            assert_eq!(output, expect, "arg1 {:?}, arg2 {:?}", a1, a2);
        }
    }

    #[test]
    fn test_log2() {
        let test_cases = vec![
            (Some(16_f64), Some(Real::from(4_f64))),
            (Some(5_f64), Some(Real::from(2.321928094887362_f64))),
            (Some(-1.234_f64), None),
            (Some(0_f64), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Log2)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_log10() {
        let test_cases = vec![
            (Some(100_f64), Some(Real::from(2_f64))),
            (Some(101_f64), Some(Real::from(2.0043213737826426_f64))),
            (Some(-1.234_f64), None),
            (Some(0_f64), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Log10)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_abs_int() {
        let test_cases = vec![
            (ScalarFuncSig::AbsInt, -3, Some(3), false),
            (
                ScalarFuncSig::AbsInt,
                std::i64::MAX,
                Some(std::i64::MAX),
                false,
            ),
            (
                ScalarFuncSig::AbsUInt,
                std::u64::MAX as i64,
                Some(std::u64::MAX as i64),
                false,
            ),
            (ScalarFuncSig::AbsInt, std::i64::MIN, Some(0), true),
        ];

        for (sig, arg, expect_output, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new().push_param(arg).evaluate(sig);

            if is_err {
                assert!(output.is_err());
            } else {
                let output = output.unwrap();
                assert_eq!(output, expect_output, "{:?}", arg);
            }
        }
    }

    #[test]
    fn test_abs_real() {
        let test_cases = vec![(3.5_f64, 3.5_f64), (-3.5_f64, 3.5_f64)];

        for (arg, expect_output) in test_cases {
            let expect_output = Real::new(expect_output).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::AbsReal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_abs_decimal() {
        let test_cases = vec![("1.1", "1.1"), ("-1.1", "1.1")];

        for (arg, expect_output) in test_cases {
            let arg = arg.parse::<Decimal>().ok();
            let expect_output = expect_output.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::AbsDecimal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_ceil_real() {
        let cases = vec![
            (4.0, 3.5),
            (4.0, 3.45),
            (4.0, 3.1),
            (-3.0, -3.45),
            (0.0, -0.1),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (expected, input) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::CeilReal)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("124", "123.456"),
            ("-123", "-123.456"),
        ];

        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::CeilDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_int() {
        let cases = vec![
            (124, "123.456"),
            (2, "1.23"),
            (-1, "-1.23"),
            (std::i64::MIN, "-9223372036854775808"),
        ];
        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::CeilDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (666, 666),
            (-3, -3),
            (-233, -233),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::CeilIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    fn test_unary_func_ok_none<I: Evaluable, O: Evaluable>(sig: ScalarFuncSig)
    where
        O: PartialEq,
        Option<I>: Into<ScalarValue>,
        Option<O>: From<ScalarValue>,
    {
        assert_eq!(
            None,
            RpnFnScalarEvaluator::new()
                .push_param(Option::<I>::None)
                .evaluate::<O>(sig)
                .unwrap()
        );
    }

    #[test]
    fn test_floor_real() {
        let cases = vec![
            (3.5, 3.0),
            (3.7, 3.0),
            (3.45, 3.0),
            (3.1, 3.0),
            (-3.45, -4.0),
            (-0.1, -1.0),
            (16140901064495871255.0, 16140901064495871255.0),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (input, expected) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::FloorReal)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::FloorReal);
    }

    #[test]
    fn test_floor_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("123.456", "123"),
            ("-123.456", "-124"),
        ];

        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::FloorDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Decimal>(ScalarFuncSig::FloorDecToDec);
    }

    #[test]
    fn test_floor_dec_to_int() {
        let cases = vec![
            ("123.456", 123),
            ("1.23", 1),
            ("-1.23", -2),
            ("-9223372036854775808", std::i64::MIN),
        ];
        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::FloorDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Int>(ScalarFuncSig::FloorDecToInt);
    }

    #[test]
    fn test_floor_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (-3, -3),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::FloorIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Int, Int>(ScalarFuncSig::FloorIntToInt);
    }

    #[test]
    fn test_round_int() {
        let test_cases = vec![
            (1, 1),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (arg, expect_output) in test_cases {
            let expect_output = Some(expect_output);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::RoundInt)
                .unwrap();
            assert_eq!(output, expect_output);
        }

        test_unary_func_ok_none::<Int, Int>(ScalarFuncSig::RoundInt);
    }

    #[test]
    fn test_round_real() {
        let test_cases = vec![
            (3.45_f64, 3_f64),
            (-3.45_f64, -3_f64),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];

        for (arg, expect_output) in test_cases {
            let arg = Real::from(arg);
            let expect_output = Real::new(expect_output).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::RoundReal)
                .unwrap();
            assert_eq!(output, expect_output);
        }

        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::RoundReal);
    }

    #[test]
    fn test_round_dec() {
        let test_cases = vec![
            ("123.456", "123.000"),
            ("123.656", "124.000"),
            ("-123.456", "-123.000"),
        ];

        for (arg, expect_output) in test_cases {
            let arg = arg.parse::<Decimal>().ok();
            let expect_output = expect_output.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::RoundDec)
                .unwrap();
            assert_eq!(output, expect_output);
        }

        test_unary_func_ok_none::<Decimal, Decimal>(ScalarFuncSig::RoundDec);
    }

    #[test]
    fn test_round_with_frac_int() {
        let test_cases = vec![
            (23, 2, 23),
            (23, -1, 20),
            (-27, -1, -30),
            (-27, -2, 0),
            (std::i64::MAX, std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MAX, std::i64::MIN),
            (std::i64::MAX, std::i64::MIN, 0),
            (std::i64::MIN, std::i64::MIN, 0),
            (std::i64::MAX, -19, 0),
            (std::i64::MAX, -18, 9000000000000000000),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let expect_output = Some(expect_output);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate::<Int>(ScalarFuncSig::RoundWithFracInt)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_with_frac_real() {
        let test_cases = vec![
            (-1.298_f64, 1, -1.3_f64),
            (-1.298_f64, 0, -1.0_f64),
            (23.298_f64, 2, 23.30_f64),
            (23.298_f64, -1, 20.0_f64),
            (23.298_f64, -2, 0.0_f64),
            (23.298_f64, -3, 0.0_f64),
            (
                1234567890.0123456789_f64,
                std::i64::MAX,
                1234567890.0123456789_f64,
            ),
            (
                -1234567890.0123456789_f64,
                std::i64::MAX,
                -1234567890.0123456789_f64,
            ),
            (1234567890.0123456789_f64, std::i64::MIN, 0.0_f64),
            (-1234567890.0123456789_f64, std::i64::MIN, 0.0_f64),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let arg0 = Real::from(arg0);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate::<Real>(ScalarFuncSig::RoundWithFracReal)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect_output).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_round_with_frac_dec() {
        let test_cases = vec![
            ("150.000", 2, "150.00"),
            ("150.257", 1, "150.3"),
            ("153.257", -1, "150"),
            ("153.257", -2, "200"),
            ("153.257", -3, "0"),
            (
                "1234567890.0123456",
                i64::from(mysql::decimal::MAX_FRACTION + 1),
                "1234567890.012345600000000000000000000000",
            ),
            (
                "-1234567890.0123456",
                i64::from(mysql::decimal::MAX_FRACTION + 1),
                "-1234567890.012345600000000000000000000000",
            ),
            (
                "123456789012345678901234567890123456789012345678901234567890123456789012345678901",
                -i64::from(mysql::decimal::WORD_BUF_LEN * mysql::decimal::DIGITS_PER_WORD - 1),
                "100000000000000000000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                "123456789012345678901234567890123456789012345678901234567890123456789012345678901",
                -i64::from(mysql::decimal::WORD_BUF_LEN * mysql::decimal::DIGITS_PER_WORD),
                "0",
            ),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let arg0 = arg0.parse::<Decimal>().ok();
            let expect_output = expect_output.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate::<Decimal>(ScalarFuncSig::RoundWithFracDec)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_sign() {
        let test_cases = vec![
            (None, None),
            (Some(42f64), Some(1)),
            (Some(0f64), Some(0)),
            (Some(-47f64), Some(-1)),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::Sign)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_sqrt() {
        let test_cases = vec![
            (None, None),
            (Some(64f64), Some(Real::from(8f64))),
            (Some(2f64), Some(Real::from(std::f64::consts::SQRT_2))),
            (Some(-16f64), None),
            (Some(std::f64::NAN), None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Sqrt)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_radians() {
        let test_cases = vec![
            (None, None),
            (Some(0_f64), Some(Real::from(0_f64))),
            (Some(180_f64), Some(Real::from(std::f64::consts::PI))),
            (
                Some(-360_f64),
                Some(Real::from(-2_f64 * std::f64::consts::PI)),
            ),
            (Some(std::f64::NAN), None),
            (
                Some(std::f64::INFINITY),
                Some(Real::from(std::f64::INFINITY)),
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Radians)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_exp() {
        let tests = vec![
            (1_f64, std::f64::consts::E),
            (1.23_f64, 3.4212295362896734),
            (-1.23_f64, 0.2922925776808594),
            (0_f64, 1_f64),
        ];
        for (x, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(x)))
                .evaluate(ScalarFuncSig::Exp)
                .unwrap();
            assert_eq!(output, Some(Real::from(expected)));
        }
        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::Exp);

        let overflow_tests = vec![100000_f64];
        for x in overflow_tests {
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(x)))
                .evaluate(ScalarFuncSig::Exp);
            assert!(output.is_err());
        }
    }

    #[test]
    fn test_degrees() {
        let tests_cases = vec![
            (None, None),
            (Some(std::f64::NAN), None),
            (Some(0f64), Some(Real::from(0f64))),
            (Some(1f64), Some(Real::from(57.29577951308232_f64))),
            (Some(std::f64::consts::PI), Some(Real::from(180.0_f64))),
            (
                Some(-std::f64::consts::PI / 2.0_f64),
                Some(Real::from(-90.0_f64)),
            ),
        ];
        for (input, expect) in tests_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Degrees)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_sin() {
        let test_cases = vec![
            (0.0_f64, 0.0_f64),
            (
                std::f64::consts::PI / 4.0_f64,
                std::f64::consts::FRAC_1_SQRT_2,
            ),
            (std::f64::consts::PI / 2.0_f64, 1.0_f64),
            (std::f64::consts::PI, 0.0_f64),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Sin)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_cos() {
        let test_cases = vec![
            (0f64, 1f64),
            (std::f64::consts::PI / 2f64, 0f64),
            (std::f64::consts::PI, -1f64),
            (-std::f64::consts::PI, -1f64),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Cos)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_tan() {
        let test_cases = vec![
            (0.0_f64, 0.0_f64),
            (std::f64::consts::PI / 4.0_f64, 1.0_f64),
            (-std::f64::consts::PI / 4.0_f64, -1.0_f64),
            (std::f64::consts::PI, 0.0_f64),
            (
                (std::f64::consts::PI * 3.0) / 4.0,
                f64::tan((std::f64::consts::PI * 3.0) / 4.0), //in mysql and rust, it equals -1.0000000000000002, not -1
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Tan)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_cot() {
        let test_cases = vec![
            (-1.0_f64, -0.6420926159343308_f64),
            (1.0_f64, 0.6420926159343308_f64),
            (
                std::f64::consts::PI / 4.0_f64,
                1.0_f64 / f64::tan(std::f64::consts::PI / 4.0_f64),
            ),
            (
                std::f64::consts::PI / 2.0_f64,
                1.0_f64 / f64::tan(std::f64::consts::PI / 2.0_f64),
            ),
            (
                std::f64::consts::PI,
                1.0_f64 / f64::tan(std::f64::consts::PI),
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Cot)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
        assert!(RpnFnScalarEvaluator::new()
            .push_param(Some(Real::from(0.0_f64)))
            .evaluate::<Real>(ScalarFuncSig::Cot)
            .is_err());
    }

    #[test]
    fn test_asin() {
        let test_cases = vec![
            (0.0_f64, 0.0_f64),
            (1.0_f64, std::f64::consts::PI / 2.0_f64),
            (-1.0_f64, -std::f64::consts::PI / 2.0_f64),
            (
                std::f64::consts::SQRT_2 / 2.0_f64,
                std::f64::consts::PI / 4.0_f64,
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Asin)
                .unwrap();
            assert!((output.unwrap() - expect).abs() < std::f64::EPSILON);
        }
        let invalid_test_cases = vec![
            (std::f64::INFINITY, None),
            (2.0_f64, None),
            (-2.0_f64, None),
        ];
        for (input, expect) in invalid_test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Asin)
                .unwrap();
            assert_eq!(expect, output);
        }
    }

    #[test]
    fn test_acos() {
        let test_cases = vec![
            (0.0_f64, std::f64::consts::PI / 2.0_f64),
            (1.0_f64, 0.0_f64),
            (-1.0_f64, std::f64::consts::PI),
            (
                std::f64::consts::SQRT_2 / 2.0_f64,
                std::f64::consts::PI / 4.0_f64,
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Acos)
                .unwrap();
            assert!((output.unwrap() - expect).abs() < std::f64::EPSILON);
        }
        let invalid_test_cases = vec![
            (std::f64::INFINITY, None),
            (2.0_f64, None),
            (-2.0_f64, None),
        ];
        for (input, expect) in invalid_test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Acos)
                .unwrap();
            assert_eq!(expect, output);
        }
    }

    #[test]
    fn test_atan_1_arg() {
        let test_cases = vec![
            (1.0_f64, std::f64::consts::PI / 4.0_f64),
            (-1.0_f64, -std::f64::consts::PI / 4.0_f64),
            (std::f64::MAX, std::f64::consts::PI / 2.0_f64),
            (std::f64::MIN, -std::f64::consts::PI / 2.0_f64),
            (0.0_f64, 0.0_f64),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Real>(ScalarFuncSig::Atan1Arg)
                .unwrap();
            assert!((output.unwrap() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_atan_2_args() {
        let test_cases = vec![
            (0.0_f64, 0.0_f64, 0.0_f64),
            (0.0_f64, -1.0_f64, std::f64::consts::PI),
            (1.0_f64, -1.0_f64, 3.0_f64 * std::f64::consts::PI / 4.0_f64),
            (-1.0_f64, 1.0_f64, -std::f64::consts::PI / 4.0_f64),
            (1.0_f64, 0.0_f64, std::f64::consts::PI / 2.0_f64),
        ];
        for (arg0, arg1, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate::<Real>(ScalarFuncSig::Atan2Args)
                .unwrap();
            assert!((output.unwrap() - expect).abs() < std::f64::EPSILON);
        }
    }
}
