// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::error::{self, Error};
use coprocessor::codec::mysql::{self, Time};
use coprocessor::codec::Datum;
use std::borrow::Cow;

fn handle_invalid_time_error(ctx: &mut EvalContext, err: Error) -> Option<Error> {
    if ctx.take_warnings().warnings[0].get_code() == error::ERR_TRUNCATE_WRONG_VALUE {
        return Some(err);
    }
    if ctx.cfg.strict_sql_mode && (ctx.cfg.in_insert_stmt || ctx.cfg.in_update_or_delete_stmt) {
        return Some(err);
    }
    ctx.warnings.append_warning(err);
    return None;
}

impl ScalarFunc {
    #[inline]
    pub fn date_format<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.invalid_zero() {
            return Err(box_err!("Incorrect datetime value: '{}'", t));
        }
        let format_mask = try_opt!(self.children[1].eval_string(ctx, row));
        let format_mask_str = String::from_utf8(format_mask.into_owned())?;
        let res = t.date_format(format_mask_str)?;
        Ok(Some(Cow::Owned(res.into_bytes())))
    }

    #[inline]
    pub fn date<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.is_zero() {
            match handle_invalid_time_error(ctx, Error::incorrect_datetime_value(&format!("{}", t)))
            {
                Some(err) => return Err(err),
                None => return Ok(None),
            }
        }
        let res = Time::new(
            t.get_time().date().and_hms(0, 0, 0),
            mysql::types::DATE,
            t.get_fsp() as i8,
        ).unwrap();
        Ok(Some(Cow::Owned(res)))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::error::ERR_TRUNCATE_WRONG_VALUE;
    use coprocessor::codec::mysql::Time;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;
    #[test]
    fn test_date_format() {
        let tests = vec![
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
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in tests {
            let arg1 = datum_expr(Datum::Time(Time::parse_utc_datetime(arg1, 6).unwrap()));
            let arg2 = datum_expr(Datum::Bytes(arg2.to_string().into_bytes()));
            let f = scalar_func_expr(ScalarFuncSig::DateFormatSig, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.to_string().into_bytes()));
        }
    }

    #[test]
    fn test_date() {
        let tests = vec![
            ("2011-11-11", "2011-11-11"),
            ("2011-11-11 10:10:10", "2011-11-11"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in tests {
            let arg = datum_expr(Datum::Time(Time::parse_utc_datetime(arg, 6).unwrap()));
            let exp = Datum::Time(Time::parse_utc_datetime(exp, 6).unwrap());
            let f = scalar_func_expr(ScalarFuncSig::Date, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let exp = Datum::Null;
        let f = scalar_func_expr(ScalarFuncSig::Date, &[input]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        assert_eq!(got, exp);

        // test zero case
        let arg = datum_expr(Datum::Time(
            Time::parse_utc_datetime("0000-00-00 00:00:00", 6).unwrap(),
        ));
        let f = scalar_func_expr(ScalarFuncSig::Date, &[arg]);
        let op = Expression::build(&mut ctx, f).unwrap();
        let got = op.eval(&mut ctx, &[]);
        match got {
            Ok(_) => assert!(false, "zero timestamp should be wrong"),
            Err(_) => assert_eq!(
                ctx.take_warnings().warnings[0].get_code(),
                ERR_TRUNCATE_WRONG_VALUE
            ),
        }
    }
}
