// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(test)]
#![feature(int_error_matching)]
#![feature(const_loop)]
#![feature(const_if_match)]
#![feature(ptr_offset_from)]

#[macro_use(box_err, box_try, try_opt)]
extern crate tikv_util;

#[macro_use(other_err)]
extern crate tidb_query_common;

#[cfg(test)]
extern crate test;

pub mod types;

pub mod impl_arithmetic;
pub mod impl_cast;
pub mod impl_compare;
pub mod impl_compare_in;
pub mod impl_control;
pub mod impl_encryption;
pub mod impl_json;
pub mod impl_like;
pub mod impl_math;
pub mod impl_miscellaneous;
pub mod impl_op;
pub mod impl_other;
pub mod impl_string;
pub mod impl_time;

pub use self::types::*;

use tidb_query_datatype::{Collation, FieldTypeAccessor, FieldTypeFlag};
use tipb::{Expr, FieldType, ScalarFuncSig};

use tidb_query_common::Result;
use tidb_query_datatype::codec::collation::*;
use tidb_query_datatype::codec::data_type::*;

use self::impl_arithmetic::*;
use self::impl_cast::*;
use self::impl_compare::*;
use self::impl_compare_in::*;
use self::impl_control::*;
use self::impl_encryption::*;
use self::impl_json::*;
use self::impl_like::*;
use self::impl_math::*;
use self::impl_miscellaneous::*;
use self::impl_op::*;
use self::impl_other::*;
use self::impl_string::*;
use self::impl_time::*;

fn map_string_compare_sig<Cmp: CmpOp>(ret_field_type: &FieldType) -> Result<RpnFnMeta> {
    Ok(match_template_collator! {
        TT, match ret_field_type.as_accessor().collation().map_err(tidb_query_datatype::codec::Error::from)? {
            Collation::TT => compare_fn_meta::<StringComparer<TT, Cmp>>()
        }
    })
}

fn map_compare_in_string_sig(ret_field_type: &FieldType) -> Result<RpnFnMeta> {
    Ok(match_template_collator! {
        TT, match ret_field_type.as_accessor().collation().map_err(tidb_query_datatype::codec::Error::from)? {
            Collation::TT => compare_in_by_hash_fn_meta::<CollationAwareBytesInByHash::<TT>>()
        }
    })
}

fn map_like_sig(ret_field_type: &FieldType) -> Result<RpnFnMeta> {
    Ok(match_template_collator! {
        TT, match ret_field_type.as_accessor().collation().map_err(tidb_query_datatype::codec::Error::from)? {
            Collation::TT => like_fn_meta::<TT>()
        }
    })
}

fn map_int_sig<F>(value: ScalarFuncSig, children: &[Expr], mapper: F) -> Result<RpnFnMeta>
where
    F: Fn(bool, bool) -> RpnFnMeta,
{
    // FIXME: The signature for different signed / unsigned int should be inferred at TiDB side.
    if children.len() != 2 {
        return Err(other_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    let lhs_is_unsigned = children[0]
        .get_field_type()
        .as_accessor()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    let rhs_is_unsigned = children[1]
        .get_field_type()
        .as_accessor()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    Ok(mapper(lhs_is_unsigned, rhs_is_unsigned))
}

fn compare_mapper<F: CmpOp>(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => compare_fn_meta::<BasicComparer<Int, F>>(),
        (false, true) => compare_fn_meta::<IntUintComparer<F>>(),
        (true, false) => compare_fn_meta::<UintIntComparer<F>>(),
        (true, true) => compare_fn_meta::<UintUintComparer<F>>(),
    }
}

fn plus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntPlus>(),
        (false, true) => arithmetic_fn_meta::<IntUintPlus>(),
        (true, false) => arithmetic_fn_meta::<UintIntPlus>(),
        (true, true) => arithmetic_fn_meta::<UintUintPlus>(),
    }
}

fn minus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntMinus>(),
        (false, true) => arithmetic_fn_meta::<IntUintMinus>(),
        (true, false) => arithmetic_fn_meta::<UintIntMinus>(),
        (true, true) => arithmetic_fn_meta::<UintUintMinus>(),
    }
}

fn multiply_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntMultiply>(),
        (false, true) => arithmetic_fn_meta::<IntUintMultiply>(),
        (true, false) => arithmetic_fn_meta::<UintIntMultiply>(),
        (true, true) => arithmetic_fn_meta::<UintUintMultiply>(),
    }
}

fn mod_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntMod>(),
        (false, true) => arithmetic_fn_meta::<IntUintMod>(),
        (true, false) => arithmetic_fn_meta::<UintIntMod>(),
        (true, true) => arithmetic_fn_meta::<UintUintMod>(),
    }
}

fn divide_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntDivideInt>(),
        (false, true) => arithmetic_fn_meta::<IntDivideUint>(),
        (true, false) => arithmetic_fn_meta::<UintDivideInt>(),
        (true, true) => arithmetic_fn_meta::<UintDivideUint>(),
    }
}

fn map_rhs_int_sig<F>(value: ScalarFuncSig, children: &[Expr], mapper: F) -> Result<RpnFnMeta>
where
    F: Fn(bool) -> RpnFnMeta,
{
    // FIXME: The signature for different signed / unsigned int should be inferred at TiDB side.
    if children.len() != 2 {
        return Err(other_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    let rhs_is_unsigned = children[1]
        .get_field_type()
        .as_accessor()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    Ok(mapper(rhs_is_unsigned))
}

fn truncate_int_mapper(rhs_is_unsigned: bool) -> RpnFnMeta {
    if rhs_is_unsigned {
        truncate_int_with_uint_fn_meta()
    } else {
        truncate_int_with_int_fn_meta()
    }
}

fn truncate_real_mapper(rhs_is_unsigned: bool) -> RpnFnMeta {
    if rhs_is_unsigned {
        truncate_real_with_uint_fn_meta()
    } else {
        truncate_real_with_int_fn_meta()
    }
}

pub fn map_unary_minus_int_func(value: ScalarFuncSig, children: &[Expr]) -> Result<RpnFnMeta> {
    if children.len() != 1 {
        return Err(other_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    if children[0]
        .get_field_type()
        .as_accessor()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED)
    {
        Ok(unary_minus_uint_fn_meta())
    } else {
        Ok(unary_minus_int_fn_meta())
    }
}

#[rustfmt::skip]
fn map_expr_node_to_rpn_func(expr: &Expr) -> Result<RpnFnMeta> {
    let value = expr.get_sig();
    let children = expr.get_children();
    let ft = expr.get_field_type();
    Ok(match value {
        // impl_arithmetic
        ScalarFuncSig::PlusInt => map_int_sig(value, children, plus_mapper)?,
        ScalarFuncSig::PlusIntUnsignedUnsigned => arithmetic_fn_meta::<UintUintPlus>(),
        ScalarFuncSig::PlusIntUnsignedSigned => arithmetic_fn_meta::<UintIntPlus>(),
        ScalarFuncSig::PlusIntSignedUnsigned => arithmetic_fn_meta::<IntUintPlus>(),
        ScalarFuncSig::PlusIntSignedSigned => arithmetic_fn_meta::<IntIntPlus>(),
        ScalarFuncSig::PlusReal => arithmetic_fn_meta::<RealPlus>(),
        ScalarFuncSig::PlusDecimal => arithmetic_fn_meta::<DecimalPlus>(),
        ScalarFuncSig::MinusInt => map_int_sig(value, children, minus_mapper)?,
        ScalarFuncSig::MinusReal => arithmetic_fn_meta::<RealMinus>(),
        ScalarFuncSig::MinusDecimal => arithmetic_fn_meta::<DecimalMinus>(),
        ScalarFuncSig::MultiplyDecimal => arithmetic_fn_meta::<DecimalMultiply>(),
        ScalarFuncSig::MultiplyInt => map_int_sig(value, children, multiply_mapper)?,
        ScalarFuncSig::MultiplyIntUnsigned => arithmetic_fn_meta::<UintUintMultiply>(),
        ScalarFuncSig::MultiplyReal => arithmetic_fn_meta::<RealMultiply>(),
        ScalarFuncSig::DivideDecimal => arithmetic_with_ctx_fn_meta::<DecimalDivide>(),
        ScalarFuncSig::DivideReal => arithmetic_with_ctx_fn_meta::<RealDivide>(),
        ScalarFuncSig::IntDivideInt => map_int_sig(value, children, divide_mapper)?,
        ScalarFuncSig::IntDivideDecimal => int_divide_decimal_fn_meta(),
        ScalarFuncSig::ModReal => arithmetic_fn_meta::<RealMod>(),
        ScalarFuncSig::ModDecimal => arithmetic_with_ctx_fn_meta::<DecimalMod>(),
        ScalarFuncSig::ModInt => map_int_sig(value, children, mod_mapper)?,
        // impl_cast
        ScalarFuncSig::CastIntAsInt |
        ScalarFuncSig::CastIntAsReal |
        ScalarFuncSig::CastIntAsString |
        ScalarFuncSig::CastIntAsDecimal |
        ScalarFuncSig::CastIntAsTime |
        ScalarFuncSig::CastIntAsDuration |
        ScalarFuncSig::CastIntAsJson |
        ScalarFuncSig::CastRealAsInt |
        ScalarFuncSig::CastRealAsReal |
        ScalarFuncSig::CastRealAsString |
        ScalarFuncSig::CastRealAsDecimal |
        ScalarFuncSig::CastRealAsTime |
        ScalarFuncSig::CastRealAsDuration |
        ScalarFuncSig::CastRealAsJson |
        ScalarFuncSig::CastDecimalAsInt |
        ScalarFuncSig::CastDecimalAsReal |
        ScalarFuncSig::CastDecimalAsString |
        ScalarFuncSig::CastDecimalAsDecimal |
        ScalarFuncSig::CastDecimalAsTime |
        ScalarFuncSig::CastDecimalAsDuration |
        ScalarFuncSig::CastDecimalAsJson |
        ScalarFuncSig::CastStringAsInt |
        ScalarFuncSig::CastStringAsReal |
        ScalarFuncSig::CastStringAsString |
        ScalarFuncSig::CastStringAsDecimal |
        ScalarFuncSig::CastStringAsTime |
        ScalarFuncSig::CastStringAsDuration |
        ScalarFuncSig::CastStringAsJson |
        ScalarFuncSig::CastTimeAsInt |
        ScalarFuncSig::CastTimeAsReal |
        ScalarFuncSig::CastTimeAsString |
        ScalarFuncSig::CastTimeAsDecimal |
        ScalarFuncSig::CastTimeAsTime |
        ScalarFuncSig::CastTimeAsDuration |
        ScalarFuncSig::CastTimeAsJson |
        ScalarFuncSig::CastDurationAsInt |
        ScalarFuncSig::CastDurationAsReal |
        ScalarFuncSig::CastDurationAsString |
        ScalarFuncSig::CastDurationAsDecimal |
        ScalarFuncSig::CastDurationAsTime |
        ScalarFuncSig::CastDurationAsDuration |
        ScalarFuncSig::CastDurationAsJson |
        ScalarFuncSig::CastJsonAsInt |
        ScalarFuncSig::CastJsonAsReal |
        ScalarFuncSig::CastJsonAsString |
        ScalarFuncSig::CastJsonAsDecimal |
        ScalarFuncSig::CastJsonAsTime |
        ScalarFuncSig::CastJsonAsDuration |
        ScalarFuncSig::CastJsonAsJson => map_cast_func(expr)?,
        // impl_compare
        ScalarFuncSig::LtInt => map_int_sig(value, children, compare_mapper::<CmpOpLT>)?,
        ScalarFuncSig::LtReal => compare_fn_meta::<BasicComparer<Real, CmpOpLT>>(),
        ScalarFuncSig::LtDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpLT>>(),
        ScalarFuncSig::LtString => map_string_compare_sig::<CmpOpLT>(ft)?,
        ScalarFuncSig::LtTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpLT>>(),
        ScalarFuncSig::LtDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpLT>>(),
        ScalarFuncSig::LtJson => compare_fn_meta::<BasicComparer<Json, CmpOpLT>>(),
        ScalarFuncSig::LeInt => map_int_sig(value, children, compare_mapper::<CmpOpLE>)?,
        ScalarFuncSig::LeReal => compare_fn_meta::<BasicComparer<Real, CmpOpLE>>(),
        ScalarFuncSig::LeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpLE>>(),
        ScalarFuncSig::LeString => map_string_compare_sig::<CmpOpLE>(ft)?,
        ScalarFuncSig::LeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpLE>>(),
        ScalarFuncSig::LeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpLE>>(),
        ScalarFuncSig::LeJson => compare_fn_meta::<BasicComparer<Json, CmpOpLE>>(),
        ScalarFuncSig::GtInt => map_int_sig(value, children, compare_mapper::<CmpOpGT>)?,
        ScalarFuncSig::GtReal => compare_fn_meta::<BasicComparer<Real, CmpOpGT>>(),
        ScalarFuncSig::GtDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpGT>>(),
        ScalarFuncSig::GtString => map_string_compare_sig::<CmpOpGT>(ft)?,
        ScalarFuncSig::GtTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpGT>>(),
        ScalarFuncSig::GtDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpGT>>(),
        ScalarFuncSig::GtJson => compare_fn_meta::<BasicComparer<Json, CmpOpGT>>(),
        ScalarFuncSig::GeInt => map_int_sig(value, children, compare_mapper::<CmpOpGE>)?,
        ScalarFuncSig::GeReal => compare_fn_meta::<BasicComparer<Real, CmpOpGE>>(),
        ScalarFuncSig::GeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpGE>>(),
        ScalarFuncSig::GeString => map_string_compare_sig::<CmpOpGE>(ft)?,
        ScalarFuncSig::GeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpGE>>(),
        ScalarFuncSig::GeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpGE>>(),
        ScalarFuncSig::GeJson => compare_fn_meta::<BasicComparer<Json, CmpOpGE>>(),
        ScalarFuncSig::NeInt => map_int_sig(value, children, compare_mapper::<CmpOpNE>)?,
        ScalarFuncSig::NeReal => compare_fn_meta::<BasicComparer<Real, CmpOpNE>>(),
        ScalarFuncSig::NeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpNE>>(),
        ScalarFuncSig::NeString => map_string_compare_sig::<CmpOpNE>(ft)?,
        ScalarFuncSig::NeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpNE>>(),
        ScalarFuncSig::NeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpNE>>(),
        ScalarFuncSig::NeJson => compare_fn_meta::<BasicComparer<Json, CmpOpNE>>(),
        ScalarFuncSig::EqInt => map_int_sig(value, children, compare_mapper::<CmpOpEQ>)?,
        ScalarFuncSig::EqReal => compare_fn_meta::<BasicComparer<Real, CmpOpEQ>>(),
        ScalarFuncSig::EqDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpEQ>>(),
        ScalarFuncSig::EqString => map_string_compare_sig::<CmpOpEQ>(ft)?,
        ScalarFuncSig::EqTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpEQ>>(),
        ScalarFuncSig::EqDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpEQ>>(),
        ScalarFuncSig::EqJson => compare_fn_meta::<BasicComparer<Json, CmpOpEQ>>(),
        ScalarFuncSig::NullEqInt => map_int_sig(value, children, compare_mapper::<CmpOpNullEQ>)?,
        ScalarFuncSig::NullEqReal => compare_fn_meta::<BasicComparer<Real, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqString => map_string_compare_sig::<CmpOpNullEQ>(ft)?,
        ScalarFuncSig::NullEqTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqJson => compare_fn_meta::<BasicComparer<Json, CmpOpNullEQ>>(),
        ScalarFuncSig::CoalesceInt => coalesce_fn_meta::<Int>(),
        ScalarFuncSig::CoalesceReal => coalesce_fn_meta::<Real>(),
        ScalarFuncSig::CoalesceString => coalesce_fn_meta::<Bytes>(),
        ScalarFuncSig::CoalesceDecimal => coalesce_fn_meta::<Decimal>(),
        ScalarFuncSig::CoalesceTime => coalesce_fn_meta::<DateTime>(),
        ScalarFuncSig::CoalesceDuration => coalesce_fn_meta::<Duration>(),
        ScalarFuncSig::CoalesceJson => coalesce_fn_meta::<Json>(),
        // impl_compare_in
        ScalarFuncSig::InInt => compare_in_by_hash_fn_meta::<NormalInByHash::<Int>>(),
        ScalarFuncSig::InReal => compare_in_by_hash_fn_meta::<NormalInByHash::<Real>>(),
        ScalarFuncSig::InString => map_compare_in_string_sig(ft)?,
        ScalarFuncSig::InDecimal => compare_in_by_hash_fn_meta::<NormalInByHash::<Decimal>>(),
        ScalarFuncSig::InTime => compare_in_by_compare_fn_meta::<DateTime>(),
        ScalarFuncSig::InDuration => compare_in_by_hash_fn_meta::<NormalInByHash::<Duration>>(),
        ScalarFuncSig::InJson => compare_in_by_compare_fn_meta::<Json>(),
        // impl_control
        ScalarFuncSig::IfNullInt => if_null_fn_meta::<Int>(),
        ScalarFuncSig::IfNullReal => if_null_fn_meta::<Real>(),
        ScalarFuncSig::IfNullString => if_null_fn_meta::<Bytes>(),
        ScalarFuncSig::IfNullDecimal => if_null_fn_meta::<Decimal>(),
        ScalarFuncSig::IfNullTime => if_null_fn_meta::<DateTime>(),
        ScalarFuncSig::IfNullDuration => if_null_fn_meta::<Duration>(),
        ScalarFuncSig::IfNullJson => if_null_fn_meta::<Json>(),
        ScalarFuncSig::IfInt => if_condition_fn_meta::<Int>(),
        ScalarFuncSig::IfReal => if_condition_fn_meta::<Real>(),
        ScalarFuncSig::IfDecimal => if_condition_fn_meta::<Decimal>(),
        ScalarFuncSig::IfTime => if_condition_fn_meta::<DateTime>(),
        ScalarFuncSig::IfString => if_condition_fn_meta::<Bytes>(),
        ScalarFuncSig::IfDuration => if_condition_fn_meta::<Duration>(),
        ScalarFuncSig::IfJson => if_condition_fn_meta::<Json>(),
        ScalarFuncSig::CaseWhenInt => case_when_fn_meta::<Int>(),
        ScalarFuncSig::CaseWhenReal => case_when_fn_meta::<Real>(),
        ScalarFuncSig::CaseWhenString => case_when_fn_meta::<Bytes>(),
        ScalarFuncSig::CaseWhenDecimal => case_when_fn_meta::<Decimal>(),
        ScalarFuncSig::CaseWhenTime => case_when_fn_meta::<DateTime>(),
        ScalarFuncSig::CaseWhenDuration => case_when_fn_meta::<Duration>(),
        ScalarFuncSig::CaseWhenJson => case_when_fn_meta::<Json>(),
        // impl_encryption
        ScalarFuncSig::UncompressedLength => uncompressed_length_fn_meta(),
        ScalarFuncSig::Md5 => md5_fn_meta(),
        ScalarFuncSig::Sha1 => sha1_fn_meta(),
        ScalarFuncSig::Sha2 => sha2_fn_meta(),
        ScalarFuncSig::RandomBytes => random_bytes_fn_meta(),
        // impl_json
        ScalarFuncSig::JsonDepthSig => json_depth_fn_meta(),
        ScalarFuncSig::JsonTypeSig => json_type_fn_meta(),
        ScalarFuncSig::JsonSetSig => json_set_fn_meta(),
        ScalarFuncSig::JsonReplaceSig => json_replace_fn_meta(),
        ScalarFuncSig::JsonInsertSig => json_insert_fn_meta(),
        ScalarFuncSig::JsonArraySig => json_array_fn_meta(),
        ScalarFuncSig::JsonObjectSig => json_object_fn_meta(),
        ScalarFuncSig::JsonMergeSig => json_merge_fn_meta(),
        ScalarFuncSig::JsonUnquoteSig => json_unquote_fn_meta(),
        ScalarFuncSig::JsonExtractSig => json_extract_fn_meta(),
        ScalarFuncSig::JsonLengthSig => json_length_fn_meta(),
        ScalarFuncSig::JsonRemoveSig => json_remove_fn_meta(),
        ScalarFuncSig::JsonKeysSig => json_keys_fn_meta(),
        ScalarFuncSig::JsonKeys2ArgsSig => json_keys_fn_meta(),
        // impl_like
        ScalarFuncSig::LikeSig => map_like_sig(ft)?,
        // impl_math
        ScalarFuncSig::AbsInt => abs_int_fn_meta(),
        ScalarFuncSig::AbsUInt => abs_uint_fn_meta(),
        ScalarFuncSig::AbsReal => abs_real_fn_meta(),
        ScalarFuncSig::AbsDecimal => abs_decimal_fn_meta(),
        ScalarFuncSig::CeilReal => ceil_fn_meta::<CeilReal>(),
        ScalarFuncSig::CeilDecToDec => ceil_fn_meta::<CeilDecToDec>(),
        ScalarFuncSig::CeilDecToInt => ceil_fn_meta::<CeilDecToInt>(),
        ScalarFuncSig::CeilIntToInt => ceil_fn_meta::<CeilIntToInt>(),
        ScalarFuncSig::CeilIntToDec => ceil_fn_meta::<CeilIntToDec>(),
        ScalarFuncSig::FloorReal => floor_fn_meta::<FloorReal>(),
        ScalarFuncSig::FloorDecToInt => floor_fn_meta::<FloorDecToInt>(),
        ScalarFuncSig::FloorDecToDec => floor_fn_meta::<FloorDecToDec>(),
        ScalarFuncSig::FloorIntToInt => floor_fn_meta::<FloorIntToInt>(),
        ScalarFuncSig::FloorIntToDec => floor_fn_meta::<FloorIntToDec>(),
        ScalarFuncSig::Pi => pi_fn_meta(),
        ScalarFuncSig::Crc32 => crc32_fn_meta(),
        ScalarFuncSig::Log1Arg => log_1_arg_fn_meta(),
        ScalarFuncSig::Log2Args => log_2_arg_fn_meta(),
        ScalarFuncSig::Log2 => log2_fn_meta(),
        ScalarFuncSig::Log10 => log10_fn_meta(),
        ScalarFuncSig::Sin => sin_fn_meta(),
        ScalarFuncSig::Cos => cos_fn_meta(),
        ScalarFuncSig::Tan => tan_fn_meta(),
        ScalarFuncSig::Cot => cot_fn_meta(),
        ScalarFuncSig::Pow => pow_fn_meta(),
        ScalarFuncSig::Asin => asin_fn_meta(),
        ScalarFuncSig::Acos => acos_fn_meta(),
        ScalarFuncSig::Atan1Arg => atan_1_arg_fn_meta(),
        ScalarFuncSig::Atan2Args => atan_2_args_fn_meta(),
        ScalarFuncSig::Sign => sign_fn_meta(),
        ScalarFuncSig::Sqrt => sqrt_fn_meta(),
        ScalarFuncSig::Exp => exp_fn_meta(),
        ScalarFuncSig::Degrees => degrees_fn_meta(),
        ScalarFuncSig::Radians => radians_fn_meta(),
        ScalarFuncSig::Conv => conv_fn_meta(),
        ScalarFuncSig::Rand => rand_fn_meta(),
        ScalarFuncSig::RandWithSeedFirstGen => rand_with_seed_first_gen_fn_meta(),
        ScalarFuncSig::RoundReal => round_real_fn_meta(),
        ScalarFuncSig::RoundInt => round_int_fn_meta(),
        ScalarFuncSig::RoundDec => round_dec_fn_meta(),
        ScalarFuncSig::TruncateInt => map_rhs_int_sig(value, children, truncate_int_mapper)?,
        ScalarFuncSig::TruncateReal => map_rhs_int_sig(value, children, truncate_real_mapper)?,
        // impl_miscellaneous
        ScalarFuncSig::DecimalAnyValue => any_value_fn_meta::<Decimal>(),
        ScalarFuncSig::DurationAnyValue => any_value_fn_meta::<Duration>(),
        ScalarFuncSig::IntAnyValue => any_value_fn_meta::<Int>(),
        ScalarFuncSig::JsonAnyValue => any_value_fn_meta::<Json>(),
        ScalarFuncSig::RealAnyValue => any_value_fn_meta::<Real>(),
        ScalarFuncSig::StringAnyValue => any_value_fn_meta::<Bytes>(),
        ScalarFuncSig::TimeAnyValue => any_value_fn_meta::<DateTime>(),
        ScalarFuncSig::InetAton => inet_aton_fn_meta(),
        ScalarFuncSig::InetNtoa => inet_ntoa_fn_meta(),
        ScalarFuncSig::Inet6Aton => inet6_aton_fn_meta(),
        ScalarFuncSig::Inet6Ntoa => inet6_ntoa_fn_meta(),
        ScalarFuncSig::IsIPv4 => is_ipv4_fn_meta(),
        ScalarFuncSig::IsIPv4Compat => is_ipv4_compat_fn_meta(),
        ScalarFuncSig::IsIPv4Mapped => is_ipv4_mapped_fn_meta(),
        ScalarFuncSig::IsIPv6 => is_ipv6_fn_meta(),
        ScalarFuncSig::Uuid => uuid_fn_meta(),
        // impl_op
        ScalarFuncSig::IntIsNull => is_null_fn_meta::<Int>(),
        ScalarFuncSig::RealIsNull => is_null_fn_meta::<Real>(),
        ScalarFuncSig::DecimalIsNull => is_null_fn_meta::<Decimal>(),
        ScalarFuncSig::StringIsNull => is_null_fn_meta::<Bytes>(),
        ScalarFuncSig::TimeIsNull => is_null_fn_meta::<DateTime>(),
        ScalarFuncSig::DurationIsNull => is_null_fn_meta::<Duration>(),
        ScalarFuncSig::JsonIsNull => is_null_fn_meta::<Json>(),
        ScalarFuncSig::IntIsTrue => int_is_true_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::IntIsTrueWithNull => int_is_true_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::RealIsTrue => real_is_true_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::RealIsTrueWithNull => real_is_true_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::DecimalIsTrue => decimal_is_true_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::DecimalIsTrueWithNull => decimal_is_true_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::IntIsFalse => int_is_false_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::IntIsFalseWithNull => int_is_false_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::RealIsFalse => real_is_false_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::RealIsFalseWithNull => real_is_false_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::DecimalIsFalse => decimal_is_false_fn_meta::<KeepNullOff>(),
        ScalarFuncSig::DecimalIsFalseWithNull => decimal_is_false_fn_meta::<KeepNullOn>(),
        ScalarFuncSig::LogicalAnd => logical_and_fn_meta(),
        ScalarFuncSig::LogicalOr => logical_or_fn_meta(),
        ScalarFuncSig::LogicalXor => logical_xor_fn_meta(),
        ScalarFuncSig::UnaryNotInt => unary_not_int_fn_meta(),
        ScalarFuncSig::UnaryNotReal => unary_not_real_fn_meta(),
        ScalarFuncSig::UnaryNotDecimal => unary_not_decimal_fn_meta(),
        ScalarFuncSig::UnaryMinusInt => map_unary_minus_int_func(value, children)?,
        ScalarFuncSig::UnaryMinusReal => unary_minus_real_fn_meta(),
        ScalarFuncSig::UnaryMinusDecimal => unary_minus_decimal_fn_meta(),
        ScalarFuncSig::BitAndSig => bit_and_fn_meta(),
        ScalarFuncSig::BitOrSig => bit_or_fn_meta(),
        ScalarFuncSig::BitXorSig => bit_xor_fn_meta(),
        ScalarFuncSig::BitNegSig => bit_neg_fn_meta(),
        ScalarFuncSig::LeftShift => left_shift_fn_meta(),
        ScalarFuncSig::RightShift => right_shift_fn_meta(),
        // impl_other
        ScalarFuncSig::BitCount => bit_count_fn_meta(),
        // impl_string
        ScalarFuncSig::Bin => bin_fn_meta(),
        ScalarFuncSig::Length => length_fn_meta(),
        ScalarFuncSig::BitLength => bit_length_fn_meta(),
        ScalarFuncSig::Concat => concat_fn_meta(),
        ScalarFuncSig::ConcatWs => concat_ws_fn_meta(),
        ScalarFuncSig::Ascii => ascii_fn_meta(),
        ScalarFuncSig::ReverseUtf8 => reverse_utf8_fn_meta(),
        ScalarFuncSig::Reverse => reverse_fn_meta(),
        ScalarFuncSig::HexIntArg => hex_int_arg_fn_meta(),
        ScalarFuncSig::HexStrArg => hex_str_arg_fn_meta(),
        ScalarFuncSig::LTrim => ltrim_fn_meta(),
        ScalarFuncSig::RTrim => rtrim_fn_meta(),
        ScalarFuncSig::Lpad => lpad_fn_meta(),
        ScalarFuncSig::Trim1Arg => trim_1_arg_fn_meta(),
        ScalarFuncSig::FromBase64 => from_base64_fn_meta(),
        ScalarFuncSig::Replace => replace_fn_meta(),
        ScalarFuncSig::Left => left_fn_meta(),
        ScalarFuncSig::LeftUtf8 => left_utf8_fn_meta(),
        ScalarFuncSig::Right => right_fn_meta(),
        ScalarFuncSig::RightUtf8 => right_utf8_fn_meta(),
        ScalarFuncSig::UpperUtf8 => upper_utf8_fn_meta(),
        ScalarFuncSig::Upper => upper_fn_meta(),
        ScalarFuncSig::Locate2Args => locate_2_args_fn_meta(),
        ScalarFuncSig::Locate3Args => locate_3_args_fn_meta(),
        ScalarFuncSig::FieldInt => field_fn_meta::<Int>(),
        ScalarFuncSig::FieldReal => field_fn_meta::<Real>(),
        ScalarFuncSig::FieldString => field_fn_meta::<Bytes>(),
        ScalarFuncSig::Elt => elt_fn_meta(),
        ScalarFuncSig::Space => space_fn_meta(),
        ScalarFuncSig::Strcmp => strcmp_fn_meta(),
        ScalarFuncSig::InstrUtf8 => instr_utf8_fn_meta(),
        ScalarFuncSig::OctInt => oct_int_fn_meta(),
        ScalarFuncSig::FindInSet => find_in_set_fn_meta(),
        ScalarFuncSig::CharLength => char_length_fn_meta(),
        ScalarFuncSig::CharLengthUtf8 => char_length_utf8_fn_meta(),
        ScalarFuncSig::ToBase64 => to_base64_fn_meta(),
        // impl_time
        ScalarFuncSig::DateFormatSig => date_format_fn_meta(),
        ScalarFuncSig::WeekOfYear => week_of_year_fn_meta(),
        ScalarFuncSig::DayOfYear => day_of_year_fn_meta(),
        ScalarFuncSig::DayOfWeek => day_of_week_fn_meta(),
        ScalarFuncSig::DayOfMonth => day_of_month_fn_meta(),
        ScalarFuncSig::WeekWithMode => week_with_mode_fn_meta(),
        ScalarFuncSig::WeekDay => week_day_fn_meta(),
        ScalarFuncSig::ToDays => to_days_fn_meta(),
        ScalarFuncSig::FromDays => from_days_fn_meta(),
        ScalarFuncSig::Year => year_fn_meta(),
        ScalarFuncSig::Month => month_fn_meta(),
        ScalarFuncSig::Hour => hour_fn_meta(),
        ScalarFuncSig::Minute => minute_fn_meta(),
        ScalarFuncSig::Second => second_fn_meta(),
        ScalarFuncSig::MicroSecond => micro_second_fn_meta(),
        ScalarFuncSig::DayName => day_name_fn_meta(),
        ScalarFuncSig::PeriodAdd => period_add_fn_meta(),
        ScalarFuncSig::PeriodDiff => period_diff_fn_meta(),
        ScalarFuncSig::LastDay => last_day_fn_meta(),
        _ => return Err(other_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
