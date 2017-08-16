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

use std::borrow::Cow;

use coprocessor::codec::Datum;
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use super::{Constant, Error, Result};

#[inline]
pub fn datum_as_int(d: &Datum) -> Result<Option<i64>> {
    match *d {
        Datum::I64(i) => Ok(Some(i)),
        Datum::U64(u) => Ok(Some(u as i64)),
        _ => Err(Error::Other("Can't eval_int from Constant")),
    }
}

#[inline]
pub fn datum_as_real(d: &Datum) -> Result<Option<f64>> {
    match *d {
        Datum::F64(f) => Ok(Some(f)),
        _ => Err(Error::Other("Can't eval_real from Datum")),
    }
}

#[inline]
pub fn datum_as_decimal(d: &Datum) -> Result<Option<Cow<Decimal>>> {
    match *d {
        Datum::Dec(ref d) => Ok(Some(Cow::Borrowed(d))),
        _ => Err(Error::Other("Can't eval_decimal from Datum")),
    }
}

#[inline]
pub fn datum_as_string(d: &Datum) -> Result<Option<Cow<Vec<u8>>>> {
    match *d {
        Datum::Bytes(ref b) => Ok(Some(Cow::Borrowed(b))),
        _ => Err(Error::Other("Can't eval_string from Datum")),
    }
}

#[inline]
pub fn datum_as_time(d: &Datum) -> Result<Option<Cow<Time>>> {
    match *d {
        Datum::Time(ref t) => Ok(Some(Cow::Borrowed(t))),
        _ => Err(Error::Other("Can't eval_time from Datum")),
    }
}

#[inline]
pub fn datum_as_duration(d: &Datum) -> Result<Option<Cow<Duration>>> {
    match *d {
        Datum::Dur(ref d) => Ok(Some(Cow::Borrowed(d))),
        _ => Err(Error::Other("Can't eval_duration from Datum")),
    }
}

#[inline]
pub fn datum_as_json(d: &Datum) -> Result<Option<Cow<Json>>> {
    match *d {
        Datum::Json(ref j) => Ok(Some(Cow::Borrowed(j))),
        _ => Err(Error::Other("Can't eval_json from Datum")),
    }
}

impl Constant {
    #[inline]
    pub fn eval_int(&self) -> Result<Option<i64>> {
        datum_as_int(&self.val)
    }

    #[inline]
    pub fn eval_real(&self) -> Result<Option<f64>> {
        datum_as_real(&self.val)
    }

    #[inline]
    pub fn eval_decimal(&self) -> Result<Option<Cow<Decimal>>> {
        datum_as_decimal(&self.val)
    }

    #[inline]
    pub fn eval_string(&self) -> Result<Option<Cow<Vec<u8>>>> {
        datum_as_string(&self.val)
    }

    #[inline]
    pub fn eval_time(&self) -> Result<Option<Cow<Time>>> {
        datum_as_time(&self.val)
    }

    #[inline]
    pub fn eval_duration(&self) -> Result<Option<Cow<Duration>>> {
        datum_as_duration(&self.val)
    }

    #[inline]
    pub fn eval_json(&self) -> Result<Option<Cow<Json>>> {
        datum_as_json(&self.val)
    }
}

#[cfg(test)]
mod test {
    use std::u64;
    use std::convert::TryFrom;
    use super::super::{Expression, StatementContext};
    use super::super::test::datum_expr;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};

    #[derive(PartialEq, Debug)]
    struct EvalResults(
        Option<i64>,
        Option<f64>,
        Option<Decimal>,
        Option<Vec<u8>>,
        Option<Time>,
        Option<Duration>,
        Option<Json>,
    );

    #[test]
    fn test_constant_eval() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse("01:00:00".as_bytes(), 0).unwrap();

        let tests = vec![
            datum_expr(Datum::Null),
            datum_expr(Datum::I64(-30)),
            datum_expr(Datum::U64(u64::MAX)),
            datum_expr(Datum::F64(124.32)),
            datum_expr(Datum::Dec(dec.clone())),
            datum_expr(Datum::Bytes(s.clone())),
            datum_expr(Datum::Dur(dur.clone())),
        ];

        let expecteds = vec![
            EvalResults(None, None, None, None, None, None, None),
            EvalResults(Some(-30), None, None, None, None, None, None),
            EvalResults(Some(-1), None, None, None, None, None, None),
            EvalResults(None, Some(124.32), None, None, None, None, None),
            EvalResults(None, None, Some(dec.clone()), None, None, None, None),
            EvalResults(None, None, None, Some(s.clone()), None, None, None),
            EvalResults(None, None, None, None, None, Some(dur.clone()), None),
        ];

        let ctx = StatementContext::default();
        for (case, expected) in tests.into_iter().zip(expecteds.into_iter()) {
            let e = Expression::try_from(case).unwrap();

            let i = e.eval_int(&ctx, &[]).unwrap_or(None);
            let r = e.eval_real(&ctx, &[]).unwrap_or(None);
            let dec = e.eval_decimal(&ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let s = e.eval_string(&ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let t = e.eval_time(&ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = e.eval_duration(&ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let j = e.eval_json(&ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(i, r, dec, s, t, dur, j);
            assert_eq!(expected, result);
        }
    }
}
