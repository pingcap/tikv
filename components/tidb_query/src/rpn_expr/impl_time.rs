// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use super::super::expr::EvalContext;

use crate::codec::data_type::*;
use crate::codec::mysql::Time;
use crate::codec::Error;
use crate::Result;

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date_format(
    ctx: &mut EvalContext,
    t: &Option<DateTime>,
    layout: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    use std::str::from_utf8;

    if t.is_none() || layout.is_none() {
        return Ok(None);
    }
    let (t, layout) = (t.as_ref().unwrap(), layout.as_ref().unwrap());
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
            .map(|_| Ok(None))?;
    }

    let t = t.date_format(from_utf8(layout.as_slice()).map_err(Error::Encoding)?);
    if let Err(err) = t {
        return ctx.handle_invalid_time_error(err).map(|_| Ok(None))?;
    }

    Ok(Some(t.unwrap().into_bytes()))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn week_day(ctx: &mut EvalContext, t: &Option<DateTime>) -> Result<Option<Int>> {
    if t.is_none() {
        return Ok(None);
    }
    let t = t.as_ref().unwrap();
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
            .map(|_| Ok(None))?;
    }
    let day = t.weekday().num_days_from_monday();
    Ok(Some(i64::from(day)))
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn from_days(ctx: &mut EvalContext, arg: &Option<Int>) -> Result<Option<Time>> {
    arg.map_or(Ok(None), |daynr: Int| {
        let time = Time::from_days(ctx, daynr as u32)?;
        Ok(Some(time))
    })
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date_diff(
    ctx: &mut EvalContext,
    arg1: &Option<Time>,
    arg2: &Option<Time>,
) -> Result<Option<Int>> {
    if let (Some(lhs), Some(rhs)) = (arg1, arg2) {
        if lhs.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(lhs))
                .map(|_| Ok(None))?;
        }
        if rhs.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(rhs))
                .map(|_| Ok(None))?;
        }
        Ok(lhs.date_diff(*rhs))
    } else {
        Ok(None)
    }
}

#[rpn_fn]
#[inline]
pub fn month(t: &Option<DateTime>) -> Result<Option<Int>> {
    t.map_or(Ok(None), |time| Ok(Some(Int::from(time.month()))))
}

#[rpn_fn]
#[inline]
pub fn hour(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.hours())))
}

#[rpn_fn]
#[inline]
pub fn minute(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.minutes())))
}

#[rpn_fn]
#[inline]
pub fn second(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.secs())))
}

#[rpn_fn]
#[inline]
pub fn micro_second(t: &Option<Duration>) -> Result<Option<Int>> {
    Ok(t.as_ref().map(|t| i64::from(t.subsec_micros())))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_date_format() {
        use std::sync::Arc;

        use crate::expr::{EvalConfig, EvalContext, Flag, SqlMode};

        let cases = vec![
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u
                %V %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01
                01 01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                "2012-12-21 23:12:34.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y %%",
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51
                51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                "0000-01-01 00:00:00.123456",
                // Functions week() and yearweek() don't support multi mode,
                // so the result of "%U %u %V %Y" is different from MySQL.
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52
                4294967295 0000 00 %",
            ),
            (
                "2016-09-3 00:59:59.123456",
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35
                35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                "2012-10-01 00:00:00",
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40
                2012 2012 12 %",
            ),
        ];
        for (date, format, expect) in cases {
            let date =
                Some(DateTime::parse_datetime(&mut EvalContext::default(), date, 6, true).unwrap());
            let format = Some(format.as_bytes().to_vec());
            let expect = Some(expect.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(date.clone())
                .push_param(format.clone())
                .evaluate(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, expect, "{:?} {:?}", date, format);
        }

        //                // TODO: pass this test after refactoring the issue #3953 is fixed.
        //                {
        //                    let format: Option<Bytes> =  Some("abc%b %M %m %c %D %d %e %j".as_bytes().to_vec());
        //                    let time: Option<DateTime> = Some( DateTime::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap());
        //
        //                    let mut cfg = EvalConfig::new();
        //                    cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT)
        //                        .set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::STRICT_ALL_TABLES);
        //                    let ctx = EvalContext::new(Arc::new(cfg));
        //
        //                    let output = RpnFnScalarEvaluator::new()
        //                        .context(ctx)
        //                        .push_param(time.clone())
        //                        .push_param(format)
        //                        .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig);
        //                    assert!(output.is_err());
        //                }

        {
            let mut cfg = EvalConfig::new();
            cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT)
                .set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::STRICT_ALL_TABLES);
            let ctx = EvalContext::new(Arc::new(cfg));

            let output = RpnFnScalarEvaluator::new()
                .context(ctx)
                .push_param(None::<DateTime>)
                .push_param(None::<Bytes>)
                .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, None);
        }

        // test date format when format is None
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (Some("2010-01-07 23:12:34.12345"), None),
            (None, None),
            // TODO: pass this test after refactoring the issue #3953 is fixed.
            //            (
            //                "0000-00-00 00:00:00",
            //                Some(
            //                    "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
            //                            %x %Y %y %%",
            //                ),
            //            ),
        ];

        for (date, format) in cases {
            let date = date.map(|d| {
                DateTime::parse_datetime(&mut EvalContext::default(), d, 6, true).unwrap()
            });
            let format = format.map(|s| s.as_bytes().to_vec());

            let output = RpnFnScalarEvaluator::new()
                .push_param(date.clone())
                .push_param(format.clone())
                .evaluate::<Bytes>(ScalarFuncSig::DateFormatSig)
                .unwrap();
            assert_eq!(output, None, "{:?} {:?}", date, format);
        }
    }

    #[test]
    fn test_week_day() {
        let cases = vec![
            ("2018-12-03", Some(0i64)),
            ("2018-12-04", Some(1i64)),
            ("2018-12-05", Some(2i64)),
            ("2018-12-06", Some(3i64)),
            ("2018-12-07", Some(4i64)),
            ("2018-12-08", Some(5i64)),
            ("2018-12-09", Some(6i64)),
            ("0000-00-00", None),
            ("2018-12-00", None),
            ("2018-00-03", None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Some(DateTime::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(datetime.clone())
                .evaluate(ScalarFuncSig::WeekDay)
                .unwrap();
            assert_eq!(output, exp);
        }
        let output = RpnFnScalarEvaluator::new()
            .push_param(None::<DateTime>)
            .evaluate::<Int>(ScalarFuncSig::WeekDay)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_from_days() {
        let cases = vec![
            (ScalarValue::Int(Some(-140)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (ScalarValue::Int(Some(140)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (ScalarValue::Int(Some(735_000)), Some("2012-05-12")), // Leap year.
            (ScalarValue::Int(Some(735_030)), Some("2012-06-11")),
            (ScalarValue::Int(Some(735_130)), Some("2012-09-19")),
            (ScalarValue::Int(Some(734_909)), Some("2012-02-11")),
            (ScalarValue::Int(Some(734_878)), Some("2012-01-11")),
            (ScalarValue::Int(Some(734_927)), Some("2012-02-29")),
            (ScalarValue::Int(Some(734_634)), Some("2011-05-12")), // Non Leap year.
            (ScalarValue::Int(Some(734_664)), Some("2011-06-11")),
            (ScalarValue::Int(Some(734_764)), Some("2011-09-19")),
            (ScalarValue::Int(Some(734_544)), Some("2011-02-11")),
            (ScalarValue::Int(Some(734_513)), Some("2011-01-11")),
            (ScalarValue::Int(Some(3_652_424)), Some("9999-12-31")),
            (ScalarValue::Int(Some(3_652_425)), Some("0000-00-00")), // mysql FROM_DAYS returns 0000-00-00 for any day >= 3652425
            (ScalarValue::Int(None), None),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime: Option<Time> =
                exp.map(|exp: &str| Time::parse_date(&mut ctx, exp).unwrap());
            let output: Option<Time> = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::FromDays)
                .unwrap();
            assert_eq!(output, datetime);
        }
    }

    #[test]
    fn test_date_diff() {
        use crate::expr::{EvalConfig, EvalContext, Flag, SqlMode};
        use std::sync::Arc;

        let cases = vec![
            (
                "0000-01-01 00:00:00.000000",
                "0000-01-01 00:00:00.000000",
                0,
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                0,
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                1,
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-02 00:00:00.000000",
                -1,
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 23:59:59.999999",
                1,
            ),
            (
                "2018-02-01 23:59:59.999999",
                "2018-02-02 00:00:00.000000",
                -1,
            ),
        ];

        for (lhs, rhs, exp) in cases {
            let mut ctx = EvalContext::default();
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(Time::parse_datetime(&mut ctx, lhs, 6, true).unwrap()))
                .push_param(Some(Time::parse_datetime(&mut ctx, rhs, 6, true).unwrap()))
                .evaluate(ScalarFuncSig::DateDiff)
                .unwrap();
            assert_eq!(output, Some(exp));
        }

        let invalid_zero_cases = vec![
            (
                Some("0000-00-00 23:59:59.999999"),
                Some("2018-02-02 00:00:00.000000"),
            ),
            (
                Some("2018-02-01 23:59:59.999999"),
                Some("2018-00-00 23:59:59.999999"),
            ),
            (None, Some("2018-00-00 23:59:59.999999")),
            (Some("2018-00-00 23:59:59.999999"), None),
        ];

        for (lhs, rhs) in invalid_zero_cases {
            let mut ctx1 = EvalContext::default();
            let output1 = RpnFnScalarEvaluator::new()
                .push_param(
                    lhs.map(|inner| Time::parse_datetime(&mut ctx1, inner, 6, true).unwrap()),
                )
                .push_param(
                    rhs.map(|inner| Time::parse_datetime(&mut ctx1, inner, 6, true).unwrap()),
                )
                .evaluate::<Int>(ScalarFuncSig::DateDiff);
            assert!(output1.is_err());

            let mut cfg = EvalConfig::new();
            cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT)
                .set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::STRICT_ALL_TABLES);

            let mut ctx2 = EvalContext::new(Arc::new(cfg));
            let output2 = RpnFnScalarEvaluator::new()
                .push_param(
                    lhs.map(|inner| Time::parse_datetime(&mut ctx2, inner, 6, true).unwrap()),
                )
                .push_param(
                    rhs.map(|inner| Time::parse_datetime(&mut ctx2, inner, 6, true).unwrap()),
                )
                .evaluate::<Int>(ScalarFuncSig::DateDiff)
                .unwrap();
            assert_eq!(output2, None);
        }

        let l: Option<Time> = None;
        let r: Option<Time> = None;
        let output = RpnFnScalarEvaluator::new()
            .push_param(l)
            .push_param(r)
            .evaluate::<Int>(ScalarFuncSig::DateDiff)
            .unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn test_month() {
        let cases = vec![
            (Some("0000-00-00 00:00:00"), Some(0i64)),
            (Some("2018-01-01 01:01:01"), Some(1i64)),
            (Some("2018-02-01 01:01:01"), Some(2i64)),
            (Some("2018-03-01 01:01:01"), Some(3i64)),
            (Some("2018-04-01 01:01:01"), Some(4i64)),
            (Some("2018-05-01 01:01:01"), Some(5i64)),
            (Some("2018-06-01 01:01:01"), Some(6i64)),
            (Some("2018-07-01 01:01:01"), Some(7i64)),
            (Some("2018-08-01 01:01:01"), Some(8i64)),
            (Some("2018-09-01 01:01:01"), Some(9i64)),
            (Some("2018-10-01 01:01:01"), Some(10i64)),
            (Some("2018-11-01 01:01:01"), Some(11i64)),
            (Some("2018-12-01 01:01:01"), Some(12i64)),
            (None, None),
        ];
        let mut ctx = EvalContext::default();
        for (time, expect) in cases {
            let time = time.map(|t| DateTime::parse_datetime(&mut ctx, t, 6, true).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(time)
                .evaluate(ScalarFuncSig::Month)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_hour_min_sec_micro_sec() {
        // test hour, minute, second, micro_second
        let cases: Vec<(&str, i8, i64, i64, i64, i64)> = vec![
            ("0 00:00:00.0", 0, 0, 0, 0, 0),
            ("31 11:30:45", 0, 31 * 24 + 11, 30, 45, 0),
            ("11:30:45.123345", 3, 11, 30, 45, 123000),
            ("11:30:45.123345", 5, 11, 30, 45, 123350),
            ("11:30:45.123345", 6, 11, 30, 45, 123345),
            ("11:30:45.1233456", 6, 11, 30, 45, 123346),
            ("11:30:45.000010", 6, 11, 30, 45, 10),
            ("11:30:45.00010", 5, 11, 30, 45, 100),
            ("-11:30:45.9233456", 0, 11, 30, 46, 0),
            ("-11:30:45.9233456", 1, 11, 30, 45, 900000),
            ("272:59:59.94", 2, 272, 59, 59, 940000),
            ("272:59:59.99", 1, 273, 0, 0, 0),
            ("272:59:59.99", 0, 273, 0, 0, 0),
        ];

        for (arg, fsp, h, m, s, ms) in cases {
            let duration = Some(Duration::parse(arg.as_bytes(), fsp).unwrap());
            let test_case_func = |sig, res| {
                let output = RpnFnScalarEvaluator::new()
                    .push_param(duration.clone())
                    .evaluate::<Int>(sig)
                    .unwrap();
                assert_eq!(output, Some(res));
            };
            test_case_func(ScalarFuncSig::Hour, h);
            test_case_func(ScalarFuncSig::Minute, m);
            test_case_func(ScalarFuncSig::Second, s);
            test_case_func(ScalarFuncSig::MicroSecond, ms);
        }

        // test NULL case
        let test_null_case = |sig| {
            let output = RpnFnScalarEvaluator::new()
                .push_param(None::<Duration>)
                .evaluate::<Int>(sig)
                .unwrap();
            assert_eq!(output, None);
        };

        test_null_case(ScalarFuncSig::Hour);
        test_null_case(ScalarFuncSig::Minute);
        test_null_case(ScalarFuncSig::Second);
        test_null_case(ScalarFuncSig::MicroSecond);
    }
}
