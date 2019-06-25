// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::{self, i16, i32, i64, i8, str, u16, u32, u64, u8};

use cop_datatype::{self, FieldTypeTp};

use super::mysql::{Res, RoundMode, DEFAULT_FSP};
use super::{Error, Result};
use crate::coprocessor::codec::data_type::{DateTime, Decimal, Duration, Json};
use crate::coprocessor::codec::error::ERR_DATA_OUT_OF_RANGE;
use crate::coprocessor::dag::expr::EvalContext;

/// `integer_unsigned_upper_bound` returns the max u64 values of different mysql types
#[inline]
pub fn integer_unsigned_upper_bound(tp: FieldTypeTp) -> u64 {
    match tp {
        FieldTypeTp::Tiny => u64::from(u8::MAX),
        FieldTypeTp::Short => u64::from(u16::MAX),
        FieldTypeTp::Int24 => (1u64 << 24) - 1u64,
        FieldTypeTp::Long => u64::from(u32::MAX),
        FieldTypeTp::LongLong | FieldTypeTp::Bit | FieldTypeTp::Set | FieldTypeTp::Enum => u64::MAX,
        _ => panic!("input bytes is not a mysql type: {}", tp),
    }
}

/// `integer_signed_upper_bound` returns the max i64 values of different mysql types
#[inline]
pub fn integer_signed_upper_bound(tp: FieldTypeTp) -> i64 {
    match tp {
        FieldTypeTp::Tiny => i64::from(i8::MAX),
        FieldTypeTp::Short => i64::from(i16::MAX),
        FieldTypeTp::Int24 => (1i64 << 23) - 1i64,
        FieldTypeTp::Long => i64::from(i32::MAX),
        FieldTypeTp::LongLong => i64::MAX,
        _ => panic!("input bytes is not a mysql type: {}", tp),
    }
}

/// `integer_signed_lower_bound` returns the min i64 values of different mysql types
#[inline]
pub fn integer_signed_lower_bound(tp: FieldTypeTp) -> i64 {
    match tp {
        FieldTypeTp::Tiny => i64::from(i8::MIN),
        FieldTypeTp::Short => i64::from(i16::MIN),
        FieldTypeTp::Int24 => -1i64 << 23,
        FieldTypeTp::Long => i64::from(i32::MIN),
        FieldTypeTp::LongLong => i64::MIN,
        _ => panic!("input bytes is not a mysql type: {}", tp),
    }
}

/// `truncate_binary` truncates a buffer to the specified length.
#[inline]
pub fn truncate_binary(s: &mut Vec<u8>, flen: isize) {
    if flen != cop_datatype::UNSPECIFIED_LENGTH as isize && s.len() > flen as usize {
        s.truncate(flen as usize);
    }
}

/// `truncate_f64` (`TruncateFloat` in tidb) tries to truncate f.
/// If the result exceeds the max/min float that flen/decimal
/// allowed, returns the max/min float allowed.
pub fn truncate_f64(mut f: f64, flen: u8, decimal: u8) -> Res<f64> {
    if f.is_nan() {
        return Res::Overflow(0f64);
    }
    let shift = 10u64.pow(u32::from(decimal)) as f64;
    let maxf = 10u64.pow(u32::from(flen - decimal)) as f64 - 1.0 / shift;
    if f.is_finite() {
        let tmp = f * shift;
        if tmp.is_finite() {
            f = tmp.round() / shift
        }
    };

    if f > maxf {
        return Res::Overflow(maxf);
    }

    if f < -maxf {
        return Res::Overflow(-maxf);
    }
    Res::Ok(f)
}

/// `overflow` returns an overflowed error.
macro_rules! overflow {
    ($val:ident, $bound:tt) => {{
        Error::Eval(
            format!("constant {} overflows {}", $val, $bound),
            ERR_DATA_OUT_OF_RANGE,
        )
    }};
}

/// `convert_int_to_int` converts an int value to a diferent int value.
#[inline]
pub fn convert_int_to_int(ctx: &mut EvalContext, val: i64, tp: FieldTypeTp) -> Result<i64> {
    let lower_bound = integer_signed_lower_bound(tp);
    if val < lower_bound {
        ctx.handle_overflow(overflow!(val, lower_bound))?;
        return Ok(val);
    }
    let upper_bound = integer_signed_upper_bound(tp);
    if val > upper_bound {
        ctx.handle_overflow(overflow!(val, upper_bound))?;
        return Ok(val);
    }
    Ok(val)
}

/// `convert_uint_to_int` converts an uint value to an int value.
#[inline]
pub fn convert_uint_to_int(ctx: &mut EvalContext, val: u64, tp: FieldTypeTp) -> Result<i64> {
    let upper_bound = integer_signed_upper_bound(tp);
    if val > upper_bound as u64 {
        ctx.handle_overflow(overflow!(val, upper_bound))?;
        return Ok(val as i64);
    }
    Ok(val as i64)
}

/// `convert_float_to_int` converts an f64 value to an i64 value.
///  Returns the overflow error if the value exceeds the boundary.
#[inline]
pub fn convert_float_to_int(ctx: &mut EvalContext, fval: f64, tp: FieldTypeTp) -> Result<i64> {
    // TODO any performance problem to use round directly?
    let val = fval.round();
    let lower_bound = integer_signed_lower_bound(tp);
    if val < lower_bound as f64 {
        ctx.handle_overflow(overflow!(val, lower_bound))?;
        return Ok(val as i64);
    }

    let upper_bound = integer_signed_upper_bound(tp);
    if val > upper_bound as f64 {
        ctx.handle_overflow(overflow!(val, upper_bound))?;
        return Ok(val as i64);
    }
    Ok(val as i64)
}

/// `convert_bytes_to_int` converts a byte arrays to an i64 in best effort.
pub fn convert_bytes_to_int(ctx: &mut EvalContext, bytes: &[u8], tp: FieldTypeTp) -> Result<i64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_int_prefix(ctx, s)?;
    let val = bytes_to_int_without_context(vs.as_bytes());
    match val {
        Ok(val) => convert_int_to_int(ctx, val, tp),
        Err(_) => Err(Error::overflow("BIGINT", &vs)),
    }
}

/// `convert_datetime_to_int` converts a `DateTime` to an i64 value
#[inline]
pub fn convert_datetime_to_int(
    ctx: &mut EvalContext,
    dt: &DateTime,
    tp: FieldTypeTp,
) -> Result<i64> {
    // TODO: avoid this clone after refactor the `Time`
    let mut t = dt.clone();
    t.round_frac(DEFAULT_FSP)?;
    let val = t.to_decimal()?.as_i64_with_ctx(ctx)?;
    convert_int_to_int(ctx, val, tp)
}

/// `convert_duration_to_int` converts a `Duration` to an i64 value
#[inline]
pub fn convert_duration_to_int(
    ctx: &mut EvalContext,
    dur: &Duration,
    tp: FieldTypeTp,
) -> Result<i64> {
    // It's OK to clone because duration only occupies 8 bytes after #4858 merged
    let dur = dur.clone();
    let dur = dur.round_frac(DEFAULT_FSP)?;
    let val = dur.to_decimal()?.as_i64_with_ctx(ctx)?;
    convert_int_to_int(ctx, val, tp)
}

/// `convert_decimal_to_int` converts a `Decimal` to an i64 value
#[inline]
pub fn convert_decimal_to_int(
    ctx: &mut EvalContext,
    dec: &Decimal,
    tp: FieldTypeTp,
) -> Result<i64> {
    // TODO: avoid this clone
    let dec = match dec.clone().round(0, RoundMode::HalfEven) {
        Res::Ok(d) => d,
        Res::Overflow(d) => {
            ctx.handle_overflow(Error::overflow("DECIMAL", ""))?;
            d
        }
        Res::Truncated(d) => {
            ctx.handle_truncate(true)?;
            d
        }
    };
    let val = dec.as_i64_with_ctx(ctx)?;
    convert_int_to_int(ctx, val, tp)
}

/// `convert_json_to_int` converts a `Json` to an i64 value
#[inline]
pub fn convert_json_to_int(ctx: &mut EvalContext, json: &Json, tp: FieldTypeTp) -> Result<i64> {
    let val = json.cast_to_int();
    convert_int_to_int(ctx, val, tp)
}

/// `convert_float_to_uint` converts a f64 value to a u64 value.
/// Returns the overflow error if the value exceeds the boundary.
#[inline]
pub fn convert_float_to_uint(ctx: &mut EvalContext, fval: f64, tp: FieldTypeTp) -> Result<u64> {
    // TODO any performance problem to use round directly?
    let val = fval.round();
    if val < 0f64 {
        ctx.handle_overflow(overflow!(val, 0))?;
        return Ok(val as u64);
    }

    let upper_bound = integer_unsigned_upper_bound(tp);
    if val > upper_bound as f64 {
        ctx.handle_overflow(overflow!(val, upper_bound))?;
        return Ok(val as u64);
    }
    Ok(val as u64)
}

/// `bytes_to_int_without_context` converts a byte arrays to an i64
/// in best effort, but without context.
pub fn bytes_to_int_without_context(bytes: &[u8]) -> Result<i64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut negative = false;
    let mut r = Some(0i64);
    if let Some(&c) = trimed.next() {
        if c == b'-' {
            negative = true;
        } else if c >= b'0' && c <= b'9' {
            r = Some(i64::from(c) - i64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            let cur = i64::from(*c - b'0');
            r = r.and_then(|r| r.checked_mul(10)).and_then(|r| {
                if negative {
                    r.checked_sub(cur)
                } else {
                    r.checked_add(cur)
                }
            });

            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overflow("BIGINT", ""))
}

/// `bytes_to_uint_without_context` converts a byte arrays to an iu64
/// in best effort, but without context.
pub fn bytes_to_uint_without_context(bytes: &[u8]) -> Result<u64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut r = Some(0u64);
    if let Some(&c) = trimed.next() {
        if c >= b'0' && c <= b'9' {
            r = Some(u64::from(c) - u64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            r = r
                .and_then(|r| r.checked_mul(10))
                .and_then(|r| r.checked_add(u64::from(*c - b'0')));
            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overflow("BIGINT UNSIGNED", ""))
}

/// `bytes_to_uint` converts a byte arrays to an u64 in best effort.
pub fn bytes_to_uint(ctx: &mut EvalContext, bytes: &[u8]) -> Result<u64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_int_prefix(ctx, s)?;
    bytes_to_uint_without_context(vs.as_bytes())
        .map_err(|_| Error::overflow("BIGINT UNSIGNED", &vs))
}

fn bytes_to_f64_without_context(bytes: &[u8]) -> Result<f64> {
    let f = match std::str::from_utf8(bytes) {
        Ok(s) => match s.trim().parse::<f64>() {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "failed to parse float";
                    "from" => s,
                    "err" => %e,
                );
                0.0
            }
        },
        Err(e) => {
            error!(
                "failed to convert bytes to str";
                "err" => %e
            );
            0.0
        }
    };
    Ok(f)
}

/// `bytes_to_f64` converts a byte array to a float64 in best effort.
pub fn bytes_to_f64(ctx: &mut EvalContext, bytes: &[u8]) -> Result<f64> {
    let s = str::from_utf8(bytes)?.trim();
    let vs = get_valid_float_prefix(ctx, s)?;

    bytes_to_f64_without_context(vs.as_bytes())
}

fn get_valid_int_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<Cow<'a, str>> {
    let vs = get_valid_float_prefix(ctx, s)?;
    float_str_to_int_string(ctx, vs)
}

fn get_valid_float_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<&'a str> {
    let mut saw_dot = false;
    let mut saw_digit = false;
    let mut valid_len = 0;
    let mut e_idx = 0;
    for (i, c) in s.chars().enumerate() {
        if c == '+' || c == '-' {
            if i != 0 && i != e_idx + 1 {
                // "1e+1" is valid.
                break;
            }
        } else if c == '.' {
            if saw_dot || e_idx > 0 {
                // "1.1." or "1e1.1"
                break;
            }
            saw_dot = true;
            if saw_digit {
                // "123." is valid.
                valid_len = i + 1;
            }
        } else if c == 'e' || c == 'E' {
            if !saw_digit {
                // "+.e"
                break;
            }
            if e_idx != 0 {
                // "1e5e"
                break;
            }
            e_idx = i
        } else if c < '0' || c > '9' {
            break;
        } else {
            saw_digit = true;
            valid_len = i + 1;
        }
    }
    box_try!(ctx.handle_truncate(valid_len < s.len()));
    if valid_len == 0 {
        Ok("0")
    } else {
        Ok(&s[..valid_len])
    }
}

/// It converts a valid float string into valid integer string which can be
/// parsed by `i64::from_str`, we can't parse float first then convert it to string
/// because precision will be lost.
///
/// When the float string indicating a value that is overflowing the i64,
/// the original float string is returned and an overflow warning is attached
fn float_str_to_int_string<'a, 'b: 'a>(
    ctx: &mut EvalContext,
    valid_float: &'b str,
) -> Result<Cow<'a, str>> {
    let mut dot_idx = None;
    let mut e_idx = None;
    let mut int_cnt: i64 = 0;
    let mut digits_cnt: i64 = 0;

    for (i, c) in valid_float.chars().enumerate() {
        match c {
            '.' => dot_idx = Some(i),
            'e' | 'E' => e_idx = Some(i),
            '0'..='9' => {
                if e_idx.is_none() {
                    if dot_idx.is_none() {
                        int_cnt += 1;
                    }
                    digits_cnt += 1;
                }
            }
            _ => (),
        }
    }

    if dot_idx.is_none() && e_idx.is_none() {
        return Ok(Cow::Borrowed(valid_float));
    }

    if e_idx.is_none() {
        if int_cnt == 0 {
            return Ok(Cow::Borrowed("0"));
        }
        return Ok(Cow::Borrowed(&valid_float[..dot_idx.unwrap()]));
    }

    let exp = box_try!((&valid_float[e_idx.unwrap() + 1..]).parse::<i64>());
    if exp > 0 && int_cnt > (i64::MAX - exp) {
        // (exp + inc_cnt) overflows MaxInt64. Add warning and return original float string
        ctx.warnings
            .append_warning(Error::overflow("BIGINT", &valid_float));
        return Ok(Cow::Owned(valid_float.to_owned()));
    }
    if int_cnt + exp <= 0 {
        return Ok(Cow::Borrowed("0"));
    }

    let mut valid_int = String::from(&valid_float[..e_idx.unwrap()]);
    if dot_idx.is_some() {
        valid_int.remove(dot_idx.unwrap());
    }

    let extra_zero_count = exp + int_cnt - digits_cnt;
    if extra_zero_count > MAX_ZERO_COUNT {
        // Overflows MaxInt64. Add warning and return original float string
        ctx.warnings
            .append_warning(Error::overflow("BIGINT", &valid_float));
        return Ok(Cow::Owned(valid_float.to_owned()));
    }

    if extra_zero_count >= 0 {
        valid_int.extend((0..extra_zero_count).map(|_| '0'));
    } else {
        let len = valid_int.len();
        if extra_zero_count >= len as i64 {
            return Ok(Cow::Borrowed("0"));
        }
        valid_int.truncate((len as i64 + extra_zero_count) as usize);
    }

    Ok(Cow::Owned(valid_int))
}

const MAX_ZERO_COUNT: i64 = 20;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::f64::EPSILON;
    use std::sync::Arc;
    use std::{f64, i64, isize, u64};

    use crate::coprocessor::codec::error::ERR_DATA_OUT_OF_RANGE;
    use crate::coprocessor::dag::expr::Flag;
    use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};

    use super::*;

    #[test]
    fn test_convert_int_to_int() {
        let tests: Vec<(i64, FieldTypeTp, Option<i64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (-123, FieldTypeTp::Tiny, Some(-123)),
            (256, FieldTypeTp::Tiny, None),
            (-257, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (-123, FieldTypeTp::Short, Some(-123)),
            (65536, FieldTypeTp::Short, None),
            (-65537, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (-123, FieldTypeTp::Int24, Some(-123)),
            (8388610, FieldTypeTp::Int24, None),
            (-8388610, FieldTypeTp::Int24, None),
            (8388610, FieldTypeTp::Long, Some(8388610)),
            (-8388610, FieldTypeTp::Long, Some(-8388610)),
            (4294967297, FieldTypeTp::Long, None),
            (-4294967297, FieldTypeTp::Long, None),
            (8388610, FieldTypeTp::LongLong, Some(8388610)),
            (-8388610, FieldTypeTp::LongLong, Some(-8388610)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = convert_int_to_int(&mut ctx, from, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be overflow",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_convert_bytes_to_int_truncated() {
        let mut ctx = EvalContext::default();
        let bs = b"123bb";
        let val = convert_bytes_to_int(&mut ctx, bs, FieldTypeTp::LongLong);
        match val {
            Err(e) => assert!(
                e.description().contains("Data Truncated"),
                "expect data truncated, but got {:?}",
                e
            ),
            res => panic!("expect convert {:?} to truncated, but got {:?}", bs, res),
        };

        // IGNORE_TRUNCATE
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::IGNORE_TRUNCATE)));
        let val = convert_bytes_to_int(&mut ctx, bs, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);

        // TRUNCATE_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val = convert_bytes_to_int(&mut ctx, bs, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);
        assert_eq!(ctx.warnings.warnings.len(), 1);
    }

    #[test]
    fn test_convert_bytes_to_int_without_context() {
        let tests: Vec<(&'static [u8], i64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"-231", -231),
            (b"", 0),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_int_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> =
            vec![b"9223372036854775809", b"-9223372036854775810"];
        for bs in invalid_cases {
            match super::bytes_to_int_without_context(bs) {
                Err(e) => assert!(e.is_overflow()),
                res => panic!("expect convert {:?} to overflow, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_bytes_to_u64() {
        let tests: Vec<(&'static [u8], u64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"231", 231),
            (b"18446744073709551615", u64::MAX),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_uint_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> = vec![b"18446744073709551616"];
        for bs in invalid_cases {
            match super::bytes_to_uint_without_context(bs) {
                Err(e) => assert!(e.is_overflow()),
                res => panic!("expect convert {:?} to overflow, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_bytes_to_f64() {
        let tests: Vec<(&'static [u8], f64)> = vec![
            (b"", 0.0),
            (b" 23", 23.0),
            (b"-1", -1.0),
            (b"1.11", 1.11),
            (b"1.11.00", 0.0),
            (b"xx", 0.0),
            (b"0x00", 0.0),
            (b"11.xx", 0.0),
            (b"xx.11", 0.0),
        ];

        for (v, f) in tests {
            let ff = super::bytes_to_f64_without_context(v).unwrap();
            if (ff - f).abs() > EPSILON {
                panic!("{:?} should be decode to {}, but got {}", v, f, ff);
            }
        }
    }

    #[test]
    fn test_get_valid_float_prefix() {
        let cases = vec![
            ("-100", "-100"),
            ("1abc", "1"),
            ("-1-1", "-1"),
            ("+1+1", "+1"),
            ("123..34", "123."),
            ("123.23E-10", "123.23E-10"),
            ("1.1e1.3", "1.1e1"),
            ("11e1.3", "11e1"),
            ("1.1e-13a", "1.1e-13"),
            ("1.", "1."),
            (".1", ".1"),
            ("", "0"),
            ("123e+", "123"),
            ("123.e", "123."),
        ];

        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        for (i, o) in cases {
            assert_eq!(super::get_valid_float_prefix(&mut ctx, i).unwrap(), o);
        }
    }

    #[test]
    fn test_invalid_get_valid_int_prefix() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec!["1e21", "1e9223372036854775807"];

        // Firstly, make sure no error returns, instead a valid float string is returned
        for i in cases {
            let o = super::get_valid_int_prefix(&mut ctx, i);
            assert_eq!(o.unwrap(), i);
        }

        // Secondly, make sure warnings are attached when the float string cannot be casted to a valid int string
        let warnings = ctx.take_warnings().warnings;
        assert_eq!(warnings.len(), 2);
        for warning in warnings {
            assert_eq!(warning.get_code(), ERR_DATA_OUT_OF_RANGE);
        }
    }

    #[test]
    fn test_valid_get_valid_int_prefix() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec![
            (".1", "0"),
            (".0", "0"),
            ("123", "123"),
            ("123e1", "1230"),
            ("123.1e2", "12310"),
            ("123.45e5", "12345000"),
            ("123.45678e5", "12345678"),
            ("123.456789e5", "12345678"),
            ("-123.45678e5", "-12345678"),
            ("+123.45678e5", "+12345678"),
            ("9e20", "900000000000000000000"), // TODO: check code validity again on function float_str_to_int_string(),
                                               // as "900000000000000000000" is already larger than i64::MAX
        ];

        for (i, e) in cases {
            let o = super::get_valid_int_prefix(&mut ctx, i);
            assert_eq!(o.unwrap(), *e);
        }
        assert_eq!(ctx.take_warnings().warnings.len(), 0);
    }

    #[test]
    fn test_convert_uint_into_int() {
        let tests: Vec<(u64, FieldTypeTp, Option<i64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (256, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (65536, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (8388610, FieldTypeTp::Int24, None),
            (8388610, FieldTypeTp::Long, Some(8388610)),
            (4294967297, FieldTypeTp::Long, None),
            (4294967297, FieldTypeTp::LongLong, Some(4294967297)),
            (u64::MAX, FieldTypeTp::LongLong, None),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = convert_uint_to_int(&mut ctx, from, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be overflow",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_convert_float_to_int() {
        let tests: Vec<(f64, FieldTypeTp, Option<i64>)> = vec![
            (123.1, FieldTypeTp::Tiny, Some(123)),
            (123.6, FieldTypeTp::Tiny, Some(124)),
            (-123.1, FieldTypeTp::Tiny, Some(-123)),
            (-123.6, FieldTypeTp::Tiny, Some(-124)),
            (256.5, FieldTypeTp::Tiny, None),
            (256.1, FieldTypeTp::Short, Some(256)),
            (256.6, FieldTypeTp::Short, Some(257)),
            (-256.1, FieldTypeTp::Short, Some(-256)),
            (-256.6, FieldTypeTp::Short, Some(-257)),
            (65535.5, FieldTypeTp::Short, None),
            (65536.1, FieldTypeTp::Int24, Some(65536)),
            (65536.5, FieldTypeTp::Int24, Some(65537)),
            (-65536.1, FieldTypeTp::Int24, Some(-65536)),
            (-65536.5, FieldTypeTp::Int24, Some(-65537)),
            (8388610.2, FieldTypeTp::Int24, None),
            (8388610.4, FieldTypeTp::Long, Some(8388610)),
            (8388610.5, FieldTypeTp::Long, Some(8388611)),
            (-8388610.4, FieldTypeTp::Long, Some(-8388610)),
            (-8388610.5, FieldTypeTp::Long, Some(-8388611)),
            (4294967296.8, FieldTypeTp::Long, None),
            (4294967296.8, FieldTypeTp::LongLong, Some(4294967297)),
            (4294967297.1, FieldTypeTp::LongLong, Some(4294967297)),
            (-4294967296.8, FieldTypeTp::LongLong, Some(-4294967297)),
            (-4294967297.1, FieldTypeTp::LongLong, Some(-4294967297)),
            (f64::MAX, FieldTypeTp::LongLong, None),
            (f64::MIN, FieldTypeTp::LongLong, None),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = convert_float_to_int(&mut ctx, from, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be overflow",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_convert_float_to_uint() {
        let mut ctx = EvalContext::default();
        assert!(convert_float_to_uint(&mut ctx, f64::MIN, FieldTypeTp::LongLong).is_err());
        assert!(convert_float_to_uint(&mut ctx, f64::MAX, FieldTypeTp::LongLong).is_err());
        let v = convert_float_to_uint(&mut ctx, 0.1, FieldTypeTp::LongLong).unwrap();
        assert_eq!(v, 0);
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_truncate_binary() {
        let s = b"123456789".to_vec();
        let mut s1 = s.clone();
        truncate_binary(&mut s1, cop_datatype::UNSPECIFIED_LENGTH);
        assert_eq!(s1, s);
        let mut s2 = s.clone();
        truncate_binary(&mut s2, isize::MAX);
        assert_eq!(s2, s);
        let mut s3 = s;
        truncate_binary(&mut s3, 0);
        assert!(s3.is_empty());
        // TODO port tests from tidb(tidb haven't implemented now)
    }

    #[test]
    fn test_truncate_f64() {
        let cases = vec![
            (100.114, 10, 2, Res::Ok(100.11)),
            (100.115, 10, 2, Res::Ok(100.12)),
            (100.1156, 10, 3, Res::Ok(100.116)),
            (100.1156, 3, 1, Res::Overflow(99.9)),
            (1.36, 10, 2, Res::Ok(1.36)),
            (f64::NAN, 10, 1, Res::Overflow(0f64)),
        ];
        for (f, flen, decimal, exp) in cases {
            let res = truncate_f64(f, flen, decimal);
            assert_eq!(res, exp);
        }
    }
}
