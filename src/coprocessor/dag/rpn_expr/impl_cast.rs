// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use cop_codegen::rpn_fn;
use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
use tipb::expression::FieldType;

use crate::coprocessor::codec::convert::*;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpressionNode, RpnFnCallExtra};
use crate::coprocessor::Result;

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let from = box_try!(EvalType::try_from(from_field_type.tp()));
    let to = box_try!(EvalType::try_from(to_field_type.tp()));
    let func_meta = match (from, to) {
        (EvalType::Int, EvalType::Decimal) => {
            if !from_field_type.is_unsigned() && !to_field_type.is_unsigned() {
                cast_int_as_decimal_fn_meta()
            } else {
                cast_uint_as_decimal_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => cast_string_as_real_fn_meta(),
        (EvalType::DateTime, EvalType::Real) => cast_time_as_real_fn_meta(),
        (EvalType::Duration, EvalType::Real) => cast_duration_as_real_fn_meta(),
        (EvalType::Json, EvalType::Real) => cast_json_as_real_fn_meta(),
        (EvalType::Int, EvalType::Int) => {
            match (from_field_type.is_unsigned(), to_field_type.is_unsigned()) {
                (false, false) => cast_int_as_int_fn_meta(),
                (false, true) => cast_int_as_uint_fn_meta(),
                (true, false) => cast_uint_as_int_fn_meta(),
                (true, true) => cast_uint_as_uint_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_float_as_int_fn_meta()
            } else {
                cast_float_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_bytes_as_int_fn_meta()
            } else {
                cast_bytes_as_uint_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_decimal_as_int_fn_meta()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_datetime_as_int_fn_meta()
            } else {
                cast_datetime_as_uint_fn_meta()
            }
        }
        (EvalType::Duration, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_duration_as_int_fn_meta()
            } else {
                cast_duration_as_uint_fn_meta()
            }
        }
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_json_as_int_fn_meta()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }
        _ => return Err(box_err!("Unsupported cast from {} to {}", from, to)),
    };
    // This cast function is inserted by `Coprocessor` automatically,
    // the `inUnion` flag always false in this situation. Ideally,
    // the cast function should be inserted by TiDB and pushed down
    // with all implicit arguments.
    Ok(RpnExpressionNode::FnCall {
        func_meta,
        args_len: 1,
        field_type: to_field_type,
        implicit_args: Vec::new(),
    })
}

fn produce_dec_with_specified_tp(
    ctx: &mut EvalContext,
    dec: Decimal,
    ft: &FieldType,
) -> Result<Decimal> {
    // FIXME: The implementation is not exactly the same as TiDB's `ProduceDecWithSpecifiedTp`.
    let (flen, decimal) = (ft.flen(), ft.decimal());
    if flen == cop_datatype::UNSPECIFIED_LENGTH || decimal == cop_datatype::UNSPECIFIED_LENGTH {
        return Ok(dec);
    }
    Ok(dec.convert_to(ctx, flen as u8, decimal as u8)?)
}

/// Indicates whether the current expression is evaluated in union statement
///
/// Note: The TiDB will push down the `inUnion` flag by implicit constant arguments,
/// but some CAST expressions inserted by TiKV coprocessor use an empty vector to represent
/// the `inUnion` flag is false.
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[ScalarValue]) -> bool {
    implicit_args.get(0) == Some(&ScalarValue::Int(Some(1)))
}

/// The unsigned int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_uint_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = Decimal::from(*val as u64);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

/// The signed int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_int_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = Decimal::from(*val);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

macro_rules! cast_as_integer {
    ($ty:ty, $as_int_fn:ident, $as_int_conv:ident) => {
        cast_as_integer!($ty, $as_int_fn, $as_int_conv, val);
    };
    ($ty:ty, $as_int_fn:ident, $as_int_conv:ident, $expr:expr) => {
        #[rpn_fn(capture = [ctx])]
        #[inline]
        pub fn $as_int_fn(ctx: &mut EvalContext, val: &Option<$ty>) -> Result<Option<i64>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    let val = $as_int_conv(ctx, $expr, FieldTypeTp::LongLong)?;
                    Ok(Some(val))
                }
            }
        }
    };
}

cast_as_integer!(Int, cast_int_as_int, convert_int_to_int, *val);
cast_as_integer!(Int, cast_uint_as_int, convert_uint_to_int, *val as u64);
cast_as_integer!(
    Real,
    cast_float_as_int,
    convert_float_to_int,
    val.into_inner()
);
cast_as_integer!(Bytes, cast_bytes_as_int, convert_bytes_to_int);
cast_as_integer!(Decimal, cast_decimal_as_int, convert_decimal_to_int);
cast_as_integer!(DateTime, cast_datetime_as_int, convert_datetime_to_int);
cast_as_integer!(
    Duration,
    cast_duration_as_int,
    convert_duration_to_int,
    *val
);
cast_as_integer!(Json, cast_json_as_int, convert_json_to_int);

macro_rules! cast_as_unsigned_integer {
    ($ty:ty, $as_uint_fn:ident, $as_uint_conv:ident) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $as_uint_conv, val,);
    };
    ($ty:ty, $as_uint_fn:ident, $as_uint_conv:ident, $extra:expr) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $as_uint_conv, $extra,);
    };
    ($ty:ty, $as_uint_fn:ident, $as_uint_conv:ident, $extra:expr, $($hook:tt)*) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $as_uint_conv, $extra, $($hook)*);
    };
    (_inner, $ty:ty, $as_uint_fn:ident, $as_uint_conv:ident, $extra:expr, $($hook:tt)*) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        #[allow(unused)]
        pub fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<i64>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    $($hook)*;
                    let val = $as_uint_conv(ctx, $extra, FieldTypeTp::LongLong)?;
                    Ok(Some(val as i64))
                }
            }
        }
    };
}

cast_as_unsigned_integer!(
    Int,
    cast_int_as_uint,
    convert_int_to_uint,
    *val,
    if *val < 0 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Int, cast_uint_as_uint, convert_uint_to_uint, *val as u64);
cast_as_unsigned_integer!(
    Real,
    cast_float_as_uint,
    convert_float_to_uint,
    val.into_inner(),
    if val.into_inner() < 0f64 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Bytes, cast_bytes_as_uint, convert_bytes_to_uint);
cast_as_unsigned_integer!(
    Decimal,
    cast_decimal_as_uint,
    convert_decimal_to_uint,
    val,
    if val.is_negative() && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(DateTime, cast_datetime_as_uint, convert_datetime_to_uint);
cast_as_unsigned_integer!(
    Duration,
    cast_duration_as_uint,
    convert_duration_to_uint,
    *val
);
cast_as_unsigned_integer!(Json, cast_json_as_uint, convert_json_to_uint);

/// The implementation for push down signature `CastStringAsReal`.
#[rpn_fn(capture = [ctx])]
#[inline]
pub fn cast_string_as_real(ctx: &mut EvalContext, val: &Option<Bytes>) -> Result<Option<Real>> {
    use crate::coprocessor::codec::convert::bytes_to_f64;

    match val {
        None => Ok(None),
        Some(val) => {
            let val = bytes_to_f64(ctx, val.as_slice())?;
            // FIXME: There is an additional step `ProduceFloatWithSpecifiedTp` in TiDB.
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastTimeAsReal`.
#[rpn_fn]
#[inline]
pub fn cast_time_as_real(val: &Option<DateTime>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.to_decimal()?.as_f64()?;
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastDurationAsReal`.
#[rpn_fn]
#[inline]
fn cast_duration_as_real(val: &Option<Duration>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = Decimal::try_from(*val)?.as_f64()?;
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastJsonAsReal`.
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_real(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.cast_to_real(ctx)?;
            Ok(Real::new(val).ok())
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_in_union() {
        use super::*;

        assert_eq!(in_union(&[]), false);
        assert_eq!(in_union(&[ScalarValue::Int(None)]), false);
        assert_eq!(in_union(&[ScalarValue::Int(Some(0))]), false);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(0)), ScalarValue::Int(Some(1))]),
            false
        );
        assert_eq!(in_union(&[ScalarValue::Int(Some(1))]), true);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(1)), ScalarValue::Int(Some(0))]),
            true
        );
    }
}
