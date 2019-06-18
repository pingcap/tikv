// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::expr_util;
use crate::coprocessor::Result;

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 3)]
pub struct RpnFnLike;

impl RpnFnLike {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        target: &Option<Bytes>,
        pattern: &Option<Bytes>,
        escape: &Option<i64>,
    ) -> Result<Option<i64>> {
        match (target, pattern, escape) {
            (Some(target), Some(pattern), Some(escape)) => Ok(Some(expr_util::like::like(
                target.as_slice(),
                pattern.as_slice(),
                *escape as u32,
                0,
            )? as i64)),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"hello"#, r#"%HELLO%"#, '\\', Some(0)),
            (r#"Hello, World"#, r#"Hello, World"#, '\\', Some(1)),
            (r#"Hello, World"#, r#"Hello, %"#, '\\', Some(1)),
            (r#"Hello, World"#, r#"%, World"#, '\\', Some(1)),
            (r#"test"#, r#"te%st"#, '\\', Some(1)),
            (r#"test"#, r#"te%%st"#, '\\', Some(1)),
            (r#"test"#, r#"test%"#, '\\', Some(1)),
            (r#"test"#, r#"%test%"#, '\\', Some(1)),
            (r#"test"#, r#"t%e%s%t"#, '\\', Some(1)),
            (r#"test"#, r#"_%_%_%_"#, '\\', Some(1)),
            (r#"test"#, r#"_%_%st"#, '\\', Some(1)),
            (r#"C:"#, r#"%\"#, '\\', Some(0)),
            (r#"C:\"#, r#"%\"#, '\\', Some(1)),
            (r#"C:\Programs"#, r#"%\"#, '\\', Some(0)),
            (r#"C:\Programs\"#, r#"%\"#, '\\', Some(1)),
            (r#"C:"#, r#"%\\"#, '\\', Some(0)),
            (r#"C:\"#, r#"%\\"#, '\\', Some(1)),
            (r#"C:\Programs"#, r#"%\\"#, '\\', Some(0)),
            (r#"C:\Programs\"#, r#"%\\"#, '\\', Some(1)),
            (r#"C:\Programs\"#, r#"%Prog%"#, '\\', Some(1)),
            (r#"C:\Programs\"#, r#"%Pr_g%"#, '\\', Some(1)),
            (r#"C:\Programs\"#, r#"%%\"#, '%', Some(1)),
            (r#"C:\Programs%"#, r#"%%%"#, '%', Some(1)),
            (r#"C:\Programs%"#, r#"%%%%"#, '%', Some(1)),
            (r#"hello"#, r#"\%"#, '\\', Some(0)),
            (r#"%"#, r#"\%"#, '\\', Some(1)),
            (r#"3hello"#, r#"%%hello"#, '%', Some(1)),
            (r#"3hello"#, r#"3%hello"#, '3', Some(0)),
            (r#"3hello"#, r#"__hello"#, '_', Some(0)),
            (r#"3hello"#, r#"%_hello"#, '%', Some(1)),
        ];
        for (target, pattern, escape, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(target.to_owned().into_bytes())
                .push_param(pattern.to_owned().into_bytes())
                .push_param(escape as i64)
                .evaluate(ScalarFuncSig::LikeSig)
                .unwrap();
            assert_eq!(
                output, expected,
                "target={}, pattern={}, escape={}",
                target, pattern, escape
            );
        }
    }
}
