// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod scalar;
mod vector;

// Concrete eval types without a nullable wrapper.
pub type Int = i64;
pub type Real = ordered_float::NotNan<f64>;
pub type Bytes = Vec<u8>;
pub use crate::codec::mysql::{Decimal, Duration, Json, JsonType, Time as DateTime};

// Dynamic eval types.
pub use self::scalar::{ScalarValue, ScalarValueRef};
pub use self::vector::{VectorValue, VectorValueExt};

use crate::EvalType;

use crate::codec::convert::ConvertTo;
use crate::expr::EvalContext;
use tidb_query_common::error::Result;

/// A trait of evaluating current concrete eval type into a MySQL logic value, represented by
/// Rust's `bool` type.
pub trait AsMySQLBool {
    /// Evaluates into a MySQL logic value.
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool>;
}

impl AsMySQLBool for Int {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(*self != 0)
    }
}

impl AsMySQLBool for Real {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(self.into_inner() != 0f64)
    }
}

impl AsMySQLBool for Bytes {
    #[inline]
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        Ok(!self.is_empty() && ConvertTo::<f64>::convert(self, context)? != 0f64)
    }
}

impl<T> AsMySQLBool for Option<T>
where
    T: AsMySQLBool,
{
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match self {
            None => Ok(false),
            Some(ref v) => v.as_mysql_bool(context),
        }
    }
}

pub macro match_template_evaluable($t:tt, $($tail:tt)*) {
    match_template::match_template! {
        $t = [Int, Real, Decimal, Bytes, DateTime, Duration, Json],
        $($tail)*
    }
}

/// A trait of all types that can be used during evaluation (eval type).
pub trait Evaluable: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EVAL_TYPE: EvalType;

    /// Converts a vector of this concrete type into a `VectorValue` in the same type;
    /// panics if the varient mismatches.
    fn into_vector_value(vec: Vec<Option<Self>>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for $ty {
            const EVAL_TYPE: EvalType = EvalType::$ty;

            #[inline]
            fn into_vector_value(vec: Vec<Option<Self>>) -> VectorValue {
                VectorValue::from(vec)
            }
        }
    };
}

impl_evaluable_type! { Int }
impl_evaluable_type! { Real }
impl_evaluable_type! { Decimal }
impl_evaluable_type! { Bytes }
impl_evaluable_type! { DateTime }
impl_evaluable_type! { Duration }
impl_evaluable_type! { Json }

pub trait IntoEvaluableRef<T>: Sized {
    /// Performs the conversion.
    fn into_evaluable_ref(self) -> T;
}

macro_rules! impl_into_evaluable_ref {
    ($ty:tt) => {
        impl<'a> IntoEvaluableRef<Option<&'a $ty>> for Option<&'a $ty> {
            fn into_evaluable_ref(self) -> Option<&'a $ty> {
                self
            }
        }
    };
}

impl_into_evaluable_ref! { Int }
impl_into_evaluable_ref! { Real }
impl_into_evaluable_ref! { Decimal }
impl_into_evaluable_ref! { DateTime }
impl_into_evaluable_ref! { Duration }

impl<'a> IntoEvaluableRef<Option<BytesRef<'a>>> for Option<&'a Bytes> {
    fn into_evaluable_ref(self) -> Option<BytesRef<'a>> {
        self.map(|x| x.as_slice())
    }
}

impl<'a> IntoEvaluableRef<Option<JsonRef<'a>>> for Option<&'a Json> {
    fn into_evaluable_ref(self) -> Option<JsonRef<'a>> {
        self.map(|x| x.as_ref())
    }
}

pub trait EvaluableRef<'a>: Clone + std::fmt::Debug + Send + Sync {
    const EVAL_TYPE: EvalType;

    /// Borrows this concrete type from a `ScalarValue` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self>;

    /// Borrows this concrete type from a `ScalarValueRef` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self>;
}

macro_rules! impl_evaluable_ref {
    ($ty:tt) => {
        impl<'a> EvaluableRef<'a> for &'a $ty {
            const EVAL_TYPE: EvalType = EvalType::$ty;
        
            fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
                match v {
                    ScalarValue::$ty(x) => x.as_ref(),
                    _ => unimplemented!(),
                }
            }
        
            fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
                match v {
                    ScalarValueRef::$ty(x) => x,
                    _ => unimplemented!(),
                }
            }
        }
    };
}


impl_evaluable_ref! { Int }
impl_evaluable_ref! { Real }
impl_evaluable_ref! { Decimal }
impl_evaluable_ref! { DateTime }
impl_evaluable_ref! { Duration }

impl<'a> EvaluableRef<'a> for BytesRef<'a> {
    const EVAL_TYPE: EvalType = EvalType::Bytes;

    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
        match v {
            ScalarValue::Bytes(x) => x.as_ref().map(|x| x.as_slice()),
            _ => unimplemented!(),
        }
    }

    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
        match v {
            ScalarValueRef::Bytes(x) => x,
            _ => unimplemented!(),
        }
    }
}

impl<'a> EvaluableRef<'a> for JsonRef<'a> {
    const EVAL_TYPE: EvalType = EvalType::Json;

    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
        match v {
            ScalarValue::Json(x) => x.as_ref().map(|x| x.as_ref()),
            _ => unimplemented!(),
        }
    }

    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
        match v {
            ScalarValueRef::Json(x) => x,
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64;

    #[test]
    fn test_bytes_as_bool() {
        let tests: Vec<(&'static [u8], Option<bool>)> = vec![
            (b"", Some(false)),
            (b" 23", Some(true)),
            (b"-1", Some(true)),
            (b"1.11", Some(true)),
            (b"1.11.00", None),
            (b"xx", None),
            (b"0x00", None),
            (b"11.xx", None),
            (b"xx.11", None),
            (
                b".0000000000000000000000000000000000000000000000000000001",
                Some(true),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (i, (v, expect)) in tests.into_iter().enumerate() {
            let rb: Result<bool> = v.to_vec().as_mysql_bool(&mut ctx);
            match expect {
                Some(val) => {
                    assert_eq!(rb.unwrap(), val);
                }
                None => {
                    assert!(
                        rb.is_err(),
                        "index: {}, {:?} should not be converted, but got: {:?}",
                        i,
                        v,
                        rb
                    );
                }
            }
        }

        // test overflow
        let mut ctx = EvalContext::default();
        let val: Result<bool> = f64::INFINITY
            .to_string()
            .as_bytes()
            .to_vec()
            .as_mysql_bool(&mut ctx);
        assert!(val.is_err());

        let mut ctx = EvalContext::default();
        let val: Result<bool> = f64::NEG_INFINITY
            .to_string()
            .as_bytes()
            .to_vec()
            .as_mysql_bool(&mut ctx);
        assert!(val.is_err());
    }

    #[test]
    fn test_real_as_bool() {
        let tests: Vec<(f64, Option<bool>)> = vec![
            (0.0, Some(false)),
            (1.3, Some(true)),
            (-1.234, Some(true)),
            (0.000000000000000000000000000000001, Some(true)),
            (-0.00000000000000000000000000000001, Some(true)),
            (f64::MAX, Some(true)),
            (f64::MIN, Some(true)),
            (f64::MIN_POSITIVE, Some(true)),
            (f64::INFINITY, Some(true)),
            (f64::NEG_INFINITY, Some(true)),
            (f64::NAN, None),
        ];

        let mut ctx = EvalContext::default();
        for (f, expected) in tests {
            match Real::new(f) {
                Ok(b) => {
                    let r = b.as_mysql_bool(&mut ctx).unwrap();
                    assert_eq!(r, expected.unwrap());
                }
                Err(_) => assert!(expected.is_none(), "{} to bool should fail", f,),
            }
        }
    }
}
