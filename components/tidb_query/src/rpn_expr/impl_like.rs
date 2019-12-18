// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::expr_util;
use crate::Result;

#[rpn_fn]
#[inline]
pub fn like(
    target: &Option<Bytes>,
    pattern: &Option<Bytes>,
    escape: &Option<i64>,
) -> Result<Option<i64>> {
    match (target, pattern, escape) {
        (Some(target), Some(pattern), Some(escape)) => Ok(Some(expr_util::like::like(
            target.as_slice(),
            pattern.as_slice(),
            *escape as u32,
        )? as i64)),
        _ => Ok(None),
    }
}

#[rpn_fn]
#[inline]
pub fn regexp(target: &Option<Bytes>, pattern: &Option<Bytes>) -> Result<Option<i64>> {
    match (target, pattern) {
        (Some(target), Some(pattern)) => {
            let target = String::from_utf8_lossy(target);
            let pattern = String::from_utf8_lossy(pattern);
            let pattern = format!("(?i){}", &pattern);

            // TODO: cache compiled result
            Ok(Some(match regex::Regex::new(&pattern) {
                Ok(s) => s.is_match(&target) as i64,
                Err(err) => return Err(box_err!("{:?}", err)),
            }))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;

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
            (
                r#"aaaaaaaaaaaaaaaaaaaaaaaaaaa"#,
                r#"a%a%a%a%a%a%a%a%b"#,
                '\\',
                Some(0),
            ),
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

    #[test]
    fn test_regexp() {
        let cases = vec![
            ("a", r"^$", Some(0)),
            ("a", r"a", Some(1)),
            ("b", r"a", Some(0)),
            ("aA", r"Aa", Some(1)),
            ("aaa", r".", Some(1)),
            ("ab", r"^.$", Some(0)),
            ("b", r"..", Some(0)),
            ("aab", r".ab", Some(1)),
            ("abcd", r".*", Some(1)),
            ("你", r"^.$", Some(1)),
            ("你好", r"你好", Some(1)),
            ("你好", r"^你好$", Some(1)),
            ("你好", r"^您好$", Some(0)),
        ];
        for (target, pattern, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(target.to_owned().into_bytes())
                .push_param(pattern.to_owned().into_bytes())
                .evaluate(ScalarFuncSig::RegexpSig)
                .unwrap();

            assert_eq!(output, expected, "target={}, pattern={}", target, pattern);
        }
    }
}
