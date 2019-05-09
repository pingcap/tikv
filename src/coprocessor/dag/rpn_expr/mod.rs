// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

mod impl_compare;
mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use cop_datatype::{FieldTypeAccessor, FieldTypeFlag};
use tipb::expression::{Expr, ScalarFuncSig};

use self::impl_compare::*;
use self::impl_op::*;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

fn map_compare_int_sig<F: CmpOp>(
    value: ScalarFuncSig,
    children: &[Expr],
) -> Result<Box<dyn RpnFunction>> {
    // FIXME: The signature for different signed / unsigned int should be inferred at TiDB side.
    if children.len() != 2 {
        return Err(box_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    let lhs_is_unsigned = children[0]
        .get_field_type()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    let rhs_is_unsigned = children[1]
        .get_field_type()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    Ok(match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => Box::new(RpnFnCompare::<BasicComparer<Int, F>>::new()),
        (false, true) => Box::new(RpnFnCompare::<IntUintComparer<F>>::new()),
        (true, false) => Box::new(RpnFnCompare::<UintIntComparer<F>>::new()),
        (true, true) => Box::new(RpnFnCompare::<UintUintComparer<F>>::new()),
    })
}

#[rustfmt::skip]
fn map_pb_sig_to_rpn_func(value: ScalarFuncSig, children: &[Expr]) -> Result<Box<dyn RpnFunction>> {
    Ok(match value {
        ScalarFuncSig::LTInt => map_compare_int_sig::<CmpOpLT>(value, children)?,
        ScalarFuncSig::LTReal => Box::new(RpnFnCompare::<RealComparer<CmpOpLT>>::new()),
        ScalarFuncSig::LTDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpLT>>::new()),
        ScalarFuncSig::LTString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpLT>>::new()),
        ScalarFuncSig::LTTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpLT>>::new()),
        ScalarFuncSig::LTDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpLT>>::new()),
        ScalarFuncSig::LTJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpLT>>::new()),
        ScalarFuncSig::LEInt => map_compare_int_sig::<CmpOpLE>(value, children)?,
        ScalarFuncSig::LEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpLE>>::new()),
        ScalarFuncSig::LEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpLE>>::new()),
        ScalarFuncSig::LEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpLE>>::new()),
        ScalarFuncSig::LETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpLE>>::new()),
        ScalarFuncSig::LEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpLE>>::new()),
        ScalarFuncSig::LEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpLE>>::new()),
        ScalarFuncSig::GTInt => map_compare_int_sig::<CmpOpGT>(value, children)?,
        ScalarFuncSig::GTReal => Box::new(RpnFnCompare::<RealComparer<CmpOpGT>>::new()),
        ScalarFuncSig::GTDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpGT>>::new()),
        ScalarFuncSig::GTString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpGT>>::new()),
        ScalarFuncSig::GTTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpGT>>::new()),
        ScalarFuncSig::GTDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpGT>>::new()),
        ScalarFuncSig::GTJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpGT>>::new()),
        ScalarFuncSig::GEInt => map_compare_int_sig::<CmpOpGE>(value, children)?,
        ScalarFuncSig::GEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpGE>>::new()),
        ScalarFuncSig::GEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpGE>>::new()),
        ScalarFuncSig::GEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpGE>>::new()),
        ScalarFuncSig::GETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpGE>>::new()),
        ScalarFuncSig::GEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpGE>>::new()),
        ScalarFuncSig::GEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpGE>>::new()),
        ScalarFuncSig::NEInt => map_compare_int_sig::<CmpOpNE>(value, children)?,
        ScalarFuncSig::NEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpNE>>::new()),
        ScalarFuncSig::NEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpNE>>::new()),
        ScalarFuncSig::NEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpNE>>::new()),
        ScalarFuncSig::NETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpNE>>::new()),
        ScalarFuncSig::NEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpNE>>::new()),
        ScalarFuncSig::NEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpNE>>::new()),
        ScalarFuncSig::EQInt => map_compare_int_sig::<CmpOpEQ>(value, children)?,
        ScalarFuncSig::EQReal => Box::new(RpnFnCompare::<RealComparer<CmpOpEQ>>::new()),
        ScalarFuncSig::EQDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpEQ>>::new()),
        ScalarFuncSig::EQString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpEQ>>::new()),
        ScalarFuncSig::EQTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpEQ>>::new()),
        ScalarFuncSig::EQDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpEQ>>::new()),
        ScalarFuncSig::EQJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpEQ>>::new()),
        ScalarFuncSig::NullEQInt => map_compare_int_sig::<CmpOpNullEQ>(value, children)?,
        ScalarFuncSig::NullEQReal => Box::new(RpnFnCompare::<RealComparer<CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpNullEQ>>::new()),
        ScalarFuncSig::LogicalAnd => Box::new(RpnFnLogicalAnd),
        ScalarFuncSig::LogicalOr => Box::new(RpnFnLogicalOr),
        _ => return Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
