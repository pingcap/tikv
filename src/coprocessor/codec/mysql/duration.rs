// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::time::Duration as StdDuration;
use std::{i64, str, u64};
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::BytesSlice;

use super::super::{Result, TEN_POW};
use super::{check_fsp, Decimal};

use bitfield::bitfield;

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const NANO_WIDTH: usize = 9;

const SECS_PER_HOUR: u32 = 3600;
const SECS_PER_MINUTE: u32 = 60;

const MAX_HOURS: u32 = 838;
const MAX_MINUTES: u32 = 59;
const MAX_SECONDS: u32 = 59;

#[inline]
fn check_hour(hour: u32) -> Result<u32> {
    if hour > MAX_HOURS {
        Err(invalid_type!(
            "invalid hour value: {} larger than {}",
            hour,
            MAX_HOURS
        ))
    } else {
        Ok(hour)
    }
}

#[inline]
fn check_minute(minute: u32) -> Result<u32> {
    if minute > MAX_MINUTES {
        Err(invalid_type!(
            "invalid minute value: {} larger than {}",
            minute,
            MAX_MINUTES
        ))
    } else {
        Ok(minute)
    }
}

#[inline]
fn check_second(second: u32) -> Result<u32> {
    if second > MAX_SECONDS {
        Err(invalid_type!(
            "invalid second value: {} larger than {}",
            second,
            MAX_SECONDS
        ))
    } else {
        Ok(second)
    }
}

mod parser {
    use super::{check_hour, check_minute, check_second, NANO_WIDTH, TEN_POW};
    use nom::character::complete::{digit1, multispace0, multispace1};
    use nom::{
        alt, call, char, complete, cond, do_parse, eof, map, map_res, opt, peek, preceded, tag,
        IResult,
    };

    #[inline]
    fn buf_to_int(buf: &[u8]) -> u32 {
        buf.iter().fold(0, |acc, c| acc * 10 + u32::from(c - b'0'))
    }

    /// Extracts a `u32` from a buffer which matches pattern: `\d+.*`
    ///
    /// ```compile_fail
    /// assert_eq!(read_int(b"123abc"), Ok((b"abc", 123)));
    /// assert_eq!(read_int(b"12:34"), Ok((b":34", 12)));
    /// assert!(read_int(b"12345678:1").is_err());
    /// ```
    ///
    /// NOTE:
    /// The range of MySQL's TIME is `-838:59:59 ~ 838:59:59`, so we can at most read 7 digits
    /// (pattern like `HHHMMSS`)
    fn read_int(input: &[u8]) -> IResult<&[u8], u32> {
        map_res!(input, digit1, |buf: &[u8]| {
            if buf.len() <= 7 {
                Ok(buf_to_int(buf))
            } else {
                Err(invalid_type!("invalid time value, more than {} digits", 7))
            }
        })
    }

    /// Extracts a `u32` with length `fsp` from a buffer which matches pattern: `\d+.*`
    /// This function assumes that `fsp` is valid
    ///
    /// ```compile_fail
    /// assert_eq!(read_int_with_fsp(b"1234567", 3), Ok(b"", 123400000));
    /// assert_eq!(read_int_with_fsp(b"1234", 6), Ok(b"", 123400000));
    /// ```
    ///
    /// NOTE:
    /// 1. The behavior of this function is similar to `read_int` except that it's designed to read the
    /// fractional part of a `TIME`
    /// 2. The fractional part will be align to a 9-digit number which it's easy to round with `fsp`
    ///
    /// FIXME: the fraction should not be round, it's incompatible with MySQL.
    fn read_int_with_fsp(input: &[u8], fsp: u8) -> IResult<&[u8], u32> {
        map!(input, digit1, |buf: &[u8]| -> u32 {
            let fsp = fsp as usize;
            let (fraction, len) = if fsp >= buf.len() {
                (buf_to_int(buf), buf.len())
            } else {
                (buf_to_int(&buf[..=fsp]), fsp + 1)
            };
            fraction * TEN_POW[NANO_WIDTH - len]
        })
    }

    /// Parse the sign of `Duration`, return true if it's negative otherwise false
    ///
    /// ```compile_fail
    /// assert_eq!(neg(b"- .123"),  Ok(b".123", true));
    /// assert_eq!(neg(b"-.123"),   Ok(b".123", true));
    /// assert_eq!(neg(b"- 11:21"), Ok(b"11:21", true));
    /// assert_eq!(neg(b"-11:21"),  Ok(b"11:21", true));
    /// assert_eq!(neg(b"11:21"),   Ok(b"11:21", false));
    /// ```
    fn neg(input: &[u8]) -> IResult<&[u8], bool> {
        do_parse!(
            input,
            neg: map!(opt!(complete!(char!('-'))), |flag| flag.is_some())
                >> preceded!(
                    multispace0,
                    alt!(complete!(peek!(call!(digit1))) | complete!(peek!(tag!("."))))
                )
                >> (neg)
        )
    }

    /// Parse the day/block(format like `HHMMSS`) value of the `Duration`,
    /// further paring will determine the value we got is a `day` value or `block` value.
    ///
    /// ```compile_fail
    /// assert_eq!(day(b"1 1:1"), Ok(b"1:1", Some(1)));
    /// assert_eq!(day(b"1234"), Ok(b"", Some(1234)));
    /// assert_eq!(day(b"1234.123"), Ok(b".123", Some(1234)));
    /// assert_eq!(day(b"1:2:3"), Ok(b"1:2:3", None));
    /// assert_eq!(day(b".123"), Ok(b".123", None));
    /// ```
    fn day(input: &[u8]) -> IResult<&[u8], Option<u32>> {
        opt!(
            input,
            do_parse!(
                day: read_int
                    >> alt!(
                        complete!(preceded!(multispace1, peek!(call!(digit1))))
                            | complete!(preceded!(
                                multispace0,
                                alt!(complete!(peek!(tag!("."))) | complete!(eof!()))
                            ))
                    )
                    >> (day)
            )
        )
    }

    /// Parse a separator ':'
    ///
    /// ```compile_fail
    /// assert_eq!(separator(b" : "), Ok(b"", true));
    /// assert_eq!(separator(b":"), Ok(b"", true));
    /// assert_eq!(separator(b";"), Ok(b";", false));
    /// ```
    fn separator(input: &[u8]) -> IResult<&[u8], bool> {
        do_parse!(
            input,
            multispace0
                >> has_separator: map!(opt!(complete!(char!(':'))), |flag| flag.is_some())
                >> multispace0
                >> (has_separator)
        )
    }

    /// Parse format like: `hh:mm` `hh:mm:ss`
    ///
    /// ```compile_fail
    /// assert_eq!(hhmmss(b"12:34:56"), Ok(b"", [12, 34, 56]));
    /// assert_eq!(hhmmss(b"12:34"), Ok(b"", [12, 34, None]));
    /// assert_eq!(hhmmss(b"1234"), Ok(b"", [None, None, None]));
    /// ```
    fn hhmmss(input: &[u8]) -> IResult<&[u8], [Option<u32>; 3]> {
        do_parse!(
            input,
            hour: opt!(map_res!(read_int, check_hour))
                >> has_mintue: separator
                >> minute: cond!(has_mintue, map_res!(read_int, check_minute))
                >> has_second: separator
                >> second: cond!(has_second, map_res!(read_int, check_second))
                >> ([hour, minute, second])
        )
    }

    /// Parse fractional part.
    ///
    /// ```compile_fail
    /// assert_eq!(fraction(" .123", 3), Ok(b"", Some(123)));
    /// assert_eq!(fraction("123", 3), Ok(b"", None));
    /// ```
    fn fraction(input: &[u8], fsp: u8) -> IResult<&[u8], Option<u32>> {
        do_parse!(
            input,
            multispace0
                >> opt!(complete!(char!('.')))
                >> fraction: opt!(call!(read_int_with_fsp, fsp))
                >> multispace0
                >> (fraction)
        )
    }

    /// Parse `Duration`
    pub fn parse(input: &[u8], fsp: u8) -> IResult<&[u8], (bool, [Option<u32>; 5])> {
        do_parse!(
            input,
            multispace0
                >> neg: neg
                >> day: day
                >> hhmmss: hhmmss
                >> fraction: call!(fraction, fsp)
                >> eof!()
                >> (neg, [day, hhmmss[0], hhmmss[1], hhmmss[2], fraction])
        )
    }

} /* parser */

bitfield! {
    #[derive(Clone, Copy)]
    pub struct Duration(u64);
    impl Debug;
    #[inline]
    pub bool, neg, set_neg: 55;
    #[inline]
    bool, unused, _: 54;
    #[inline]
    pub u32, hours, set_hours: 53, 44;
    #[inline]
    pub u32, minutes, set_minutes: 43, 38;
    #[inline]
    pub u32, secs, set_secs: 37, 32;
    #[inline]
    u32, micros, set_micros: 31, 8;
    #[inline]
    pub u8, fsp, set_fsp: 7, 0;
}

impl Duration {
    /// return the identity element of `Duration`
    pub fn zero() -> Duration {
        Duration(0)
    }

    pub fn is_zero(mut self) -> bool {
        self.set_neg(false);
        self.set_fsp(0);
        self.0 == 0
    }

    /// Return the absolute value of `Duration`
    pub fn abs(mut self) -> Self {
        self.set_neg(false);
        self
    }

    /// return the fractional part of `Duration`, in whole nanoseconds.
    pub fn subsec_nanos(self) -> u32 {
        self.micros() * 1000
    }

    /// return the fractional part of `Duration`, in whole microseconds.
    pub fn subsec_micros(self) -> u32 {
        self.micros()
    }

    /// Returns the number of whole seconds contained by this Duration.
    pub fn as_secs(self) -> u32 {
        self.hours() * SECS_PER_HOUR + self.minutes() * SECS_PER_MINUTE + self.secs()
    }

    /// Returns the number of seconds contained by this Duration as f64.
    /// The returned value does include the fractional (nanosecond) part of the duration.
    pub fn as_secs_f64(self) -> f64 {
        let res = f64::from(self.as_secs()) + f64::from(self.subsec_nanos()) * 10e-9;

        if self.neg() {
            -res
        } else {
            res
        }
    }

    pub fn as_nanos(self) -> i64 {
        let nanos = i64::from(self.as_secs()) * NANOS_PER_SEC + i64::from(self.subsec_nanos());

        if self.neg() {
            -nanos
        } else {
            nanos
        }
    }

    pub fn from_nanos(nanos: i64, fsp: i8) -> Result<Duration> {
        let neg = nanos < 0;

        let nanos = nanos.checked_abs().ok_or(invalid_type!("nanos overflow"))?;

        let dur = StdDuration::new(
            (nanos / NANOS_PER_SEC) as u64,
            (nanos % NANOS_PER_SEC) as u32,
        );
        Duration::new(dur, neg, fsp)
    }

    pub fn new(duration: StdDuration, neg: bool, fsp: i8) -> Result<Duration> {
        let fsp = check_fsp(fsp)?;

        let fraction = duration.subsec_nanos();
        let secs = duration.as_secs();

        let hour = secs / 3600;
        let minute = secs % 3600 / 60;
        let second = secs % 60;

        Duration::build(
            neg,
            hour as u32,
            minute as u32,
            second as u32,
            fraction,
            fsp,
        )
    }

    /// Build a `Duration` with details, truncate `fraction` with `fsp` and take the produced carry
    /// NOTE: the function assumes that the value of `hour/minute/second/fsp` is valid,
    /// so before you call function `build`, make sure you have checked their validity.
    fn build(
        neg: bool,
        mut hour: u32,
        mut minute: u32,
        mut second: u32,
        nanos: u32,
        fsp: u8,
    ) -> Result<Duration> {
        // Truncate `fraction` with `fsp`
        let mask = TEN_POW[NANO_WIDTH as usize - fsp as usize - 1];
        let mut micros = ((nanos / mask + 5) / 10 * 10 * mask) / 1_000;

        if micros >= 1_000_000 {
            micros %= 1_000_000;
            second += 1;
            minute += second / 60;
            hour += minute / 60;
            second %= 60;
            minute %= 60;
        }

        let hour = check_hour(hour)?;
        let mut duration = Duration(0);

        duration.set_neg(neg);
        duration.set_hours(hour);
        duration.set_minutes(minute);
        duration.set_secs(second);
        duration.set_micros(micros);
        duration.set_fsp(fsp);
        Ok(duration)
    }

    /// Parses the time form a formatted string with a fractional seconds part,
    /// returns the duration type `Time` value.
    /// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
    pub fn parse(input: &[u8], fsp: i8) -> Result<Duration> {
        if input.is_empty() {
            return Err(invalid_type!("invalid time format"));
        }

        let fsp = check_fsp(fsp)?;

        let (mut neg, [mut day, mut hour, mut minute, mut second, fraction]) =
            self::parser::parse(input, fsp)
                .map_err(|_| invalid_type!("invalid time format"))?
                .1;

        if day.is_some() && hour.is_none() {
            let block = day.take().unwrap();
            hour = Some(block / 10_000);
            minute = Some(block / 100 % 100);
            second = Some(block % 100);
        }

        let (hour, minute, second, fraction) = (
            hour.unwrap_or(0) + day.unwrap_or(0) * 24,
            minute.unwrap_or(0),
            second.unwrap_or(0),
            fraction.unwrap_or(0),
        );

        let secs = hour * SECS_PER_HOUR + minute * SECS_PER_MINUTE + second;

        if secs == 0 && fraction == 0 {
            neg = false;
        }

        Duration::build(neg, hour, minute, second, fraction, fsp as u8)
    }

    // TODO: impl TryFrom/TryInto instead
    pub fn to_decimal(self) -> Result<Decimal> {
        let mut buf = Vec::with_capacity(13);
        if self.neg() {
            write!(buf, "-")?;
        }

        write!(
            buf,
            "{:02}{:02}{:02}",
            self.hours(),
            self.minutes(),
            self.secs()
        )?;

        if self.fsp() > 0 {
            write!(buf, ".")?;
            write!(buf, "{:06}", self.micros())?;
        }

        let d = unsafe { str::from_utf8_unchecked(&buf).parse()? };
        Ok(d)
    }

    /// Rounds fractional seconds precision with new FSP and returns a new one.
    /// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
    /// so 10:10:10.999999 round 1 -> 10:10:11
    /// and 10:10:10.000000 round 0 -> 10:10:10
    pub fn round_frac(mut self, fsp: i8) -> Result<Self> {
        let fsp = check_fsp(fsp)?;

        if fsp >= self.fsp() {
            self.set_fsp(fsp);
            return Ok(self);
        }

        self.set_fsp(fsp);
        let neg = self.neg();
        let hour = self.hours();
        let minutes = self.minutes();
        let secs = self.secs();
        let nanos = self.micros() * 1000;

        Duration::build(neg, hour, minutes, secs, nanos, fsp)
    }

    /// Checked duration addition. Computes self + rhs, returning None if overflow occurred.
    pub fn checked_add(self, rhs: Duration) -> Option<Duration> {
        let add = self.as_nanos().checked_add(rhs.as_nanos())?;
        Duration::from_nanos(add, self.fsp().max(rhs.fsp()) as i8).ok()
    }

    /// Checked duration subtraction. Computes self - rhs, returning None if overflow occurred.
    pub fn checked_sub(self, rhs: Duration) -> Option<Duration> {
        let sub = self.as_nanos().checked_sub(rhs.as_nanos())?;
        Duration::from_nanos(sub, self.fsp().max(rhs.fsp()) as i8).ok()
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.neg() {
            write!(formatter, "-")?;
        }
        write!(
            formatter,
            "{:02}:{:02}:{:02}",
            self.hours(),
            self.minutes(),
            self.secs()
        )?;

        if self.fsp() > 0 {
            write!(formatter, ".{:06}", self.micros())?;
        }
        Ok(())
    }
}

impl PartialEq for Duration {
    fn eq(&self, dur: &Duration) -> bool {
        let (mut a, mut b) = (*self, *dur);
        a.set_fsp(0);
        b.set_fsp(0);
        a.0 == b.0
    }
}

impl std::hash::Hash for Duration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, dur: &Duration) -> Option<Ordering> {
        let mut a = *self;
        a.set_fsp(0);

        let mut b = *dur;
        b.set_fsp(0);

        Some(match (a.neg(), b.neg()) {
            (true, true) => b.abs().cmp(&a.abs()),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a.0.cmp(&b.0),
        })
    }
}

impl Eq for Duration {}

impl Ord for Duration {
    fn cmp(&self, dur: &Duration) -> Ordering {
        self.partial_cmp(dur).unwrap()
    }
}

impl<T: Write> DurationEncoder for T {}
pub trait DurationEncoder: NumberEncoder {
    fn encode_duration(&mut self, v: Duration) -> Result<()> {
        self.encode_u64(v.0).map_err(From::from)
    }
}

impl Duration {
    /// `decode` decodes duration encoded by `encode_duration`.
    pub fn decode(data: &mut BytesSlice<'_>) -> Result<Duration> {
        let inner = number::decode_u64(data)?;
        Ok(Duration(inner))
    }
}

impl crate::coprocessor::codec::data_type::AsMySQLBool for Duration {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::coprocessor::dag::expr::EvalContext,
    ) -> crate::coprocessor::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::codec::mysql::MAX_FSP;
    use tikv_util::escape;

    #[test]
    fn test_hours() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 31 * 24 + 11),
            ("11:30:45", 0, 11),
            ("-11:30:45.9233456", 0, 11),
            ("272:59:59", 0, 272),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.hours();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_minutes() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 30),
            ("11:30:45", 0, 30),
            ("-11:30:45.9233456", 0, 30),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.minutes();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_secs() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45", 0, 45),
            ("11:30:45", 0, 45),
            ("-11:30:45.9233456", 1, 45),
            ("-11:30:45.9233456", 0, 46),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.secs();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_micros() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45.123", 6, 123000),
            ("11:30:45.123345", 3, 123000),
            ("11:30:45.123345", 5, 123350),
            ("11:30:45.123345", 6, 123345),
            ("11:30:45.1233456", 6, 123346),
            ("11:30:45.9233456", 0, 0),
            ("11:30:45.000010", 6, 10),
            ("11:30:45.00010", 5, 100),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.micros();
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_nano_secs() {
        let cases: Vec<(&str, i8, u32)> = vec![
            ("31 11:30:45.123", 6, 123000),
            ("11:30:45.123345", 3, 123000),
            ("11:30:45.123345", 5, 123350),
            ("11:30:45.123345", 6, 123345),
            ("11:30:45.1233456", 6, 123346),
            ("11:30:45.9233456", 0, 0),
            ("11:30:45.000010", 6, 10),
            ("11:30:45.00010", 5, 100),
        ];

        for (input, fsp, exp) in cases {
            let dur = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = dur.subsec_nanos();
            assert_eq!(exp * 1000, res);
        }
    }

    #[test]
    fn test_parse() {
        let cases: Vec<(&'static [u8], i8, Option<&'static str>)> = vec![
            (b"10:11:12", 0, Some("10:11:12")),
            (b"101112", 0, Some("10:11:12")),
            (b"10:11", 0, Some("10:11:00")),
            (b"101112.123456", 0, Some("10:11:12")),
            (b"1112", 0, Some("00:11:12")),
            (b"12", 0, Some("00:00:12")),
            (b"1 12", 0, Some("36:00:00")),
            (b"1 10:11:12", 0, Some("34:11:12")),
            (b"1 10:11:12.123456", 0, Some("34:11:12")),
            (b"1 10:11:12.123456", 4, Some("34:11:12.123500")),
            (b"1 10:11:12.12", 4, Some("34:11:12.120000")),
            (b"1 10:11:12.1234565", 6, Some("34:11:12.123457")),
            (b"1 10:11:12.9999995", 6, Some("34:11:13.000000")),
            (b"1 10:11:12.123456", 7, None),
            (b"10:11:12.123456", 0, Some("10:11:12")),
            (b"1 10:11", 0, Some("34:11:00")),
            (b"1 10", 0, Some("34:00:00")),
            (b"24 10", 0, Some("586:00:00")),
            (b"-24 10", 0, Some("-586:00:00")),
            (b"0 10", 0, Some("10:00:00")),
            (b"-10:10:10", 0, Some("-10:10:10")),
            (b"-838:59:59", 0, Some("-838:59:59")),
            (b"838:59:59", 0, Some("838:59:59")),
            (b"23:60:59", 0, None),
            (b"54:59:59", 0, Some("54:59:59")),
            (b"2011-11-11 00:00:01", 0, None),
            (b"2011-11-11", 0, None),
            (b"--23", 0, None),
            (b"232 10", 0, None),
            (b"-232 10", 0, None),
            (b"00:00:00.1", 0, Some("00:00:00")),
            (b"00:00:00.1", 1, Some("00:00:00.100000")),
            (b"00:00:00.777777", 2, Some("00:00:00.780000")),
            (b"00:00:00.777777", 6, Some("00:00:00.777777")),
            (b"00:00:00.001", 3, Some("00:00:00.001000")),
            // NOTE: The following case is easy to fail.
            (b"- 1 ", 0, Some("-00:00:01")),
            (b"1:2:3", 0, Some("01:02:03")),
            (b"1 1:2:3", 0, Some("25:02:03")),
            (b"-1 1:2:3.123", 3, Some("-25:02:03.123000")),
            (b"-.123", 3, Some("-00:00:00.123000")),
            (b"12345", 0, Some("01:23:45")),
            (b"-123", 0, Some("-00:01:23")),
            (b"-23", 0, Some("-00:00:23")),
            (b"- 1 1", 0, Some("-25:00:00")),
            (b"-1 1", 0, Some("-25:00:00")),
            (b" - 1:2:3 .123 ", 3, Some("-01:02:03.123000")),
            (b" - 1 :2 :3 .123 ", 3, Some("-01:02:03.123000")),
            (b" - 1 : 2 :3 .123 ", 3, Some("-01:02:03.123000")),
            (b" - 1 : 2 :  3 .123 ", 3, Some("-01:02:03.123000")),
            (b" - 1 .123 ", 3, Some("-00:00:01.123000")),
            (b"-", 0, None),
            (b"", 0, None),
            (b"18446744073709551615:59:59", 0, None),
            (b"1::2:3", 0, None),
            (b"1.23 3", 0, None),
        ];

        for (input, fsp, expect) in cases {
            let d = Duration::parse(input, fsp);
            match expect {
                Some(exp) => {
                    let s = format!(
                        "{}",
                        d.unwrap_or_else(|e| panic!("{}: {:?}", escape(input), e))
                    );
                    if s != expect.unwrap() {
                        panic!("expect parse {} to {}, got {}", escape(input), exp, s);
                    }
                }
                None => {
                    if d.is_ok() {
                        panic!("{} should not be passed, got {:?}", escape(input), d);
                    }
                }
            }
        }
    }

    #[test]
    fn test_to_decimal() {
        let cases = vec![
            ("31 11:30:45", 0, "7553045"),
            ("31 11:30:45", 6, "7553045.000000"),
            ("31 11:30:45", 0, "7553045"),
            ("31 11:30:45.123", 6, "7553045.123000"),
            ("11:30:45", 0, "113045"),
            ("11:30:45", 6, "113045.000000"),
            ("11:30:45.123", 6, "113045.123000"),
            ("11:30:45.123345", 0, "113045"),
            ("11:30:45.123345", 3, "113045.123000"),
            ("11:30:45.123345", 5, "113045.123350"),
            ("11:30:45.123345", 6, "113045.123345"),
            ("11:30:45.1233456", 6, "113045.123346"),
            ("11:30:45.9233456", 0, "113046"),
            ("-11:30:45.9233456", 0, "-113046"),
        ];

        for (input, fsp, exp) in cases {
            let t = Duration::parse(input.as_bytes(), fsp).unwrap();
            let res = format!("{}", t.to_decimal().unwrap());
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_round_frac() {
        let cases = vec![
            ("11:30:45.123456", 4, "11:30:45.123500"),
            ("11:30:45.123456", 6, "11:30:45.123456"),
            ("11:30:45.123456", 0, "11:30:45"),
            ("11:59:59.999999", 3, "12:00:00.000000"),
            ("1 11:30:45.123456", 1, "35:30:45.100000"),
            ("1 11:30:45.999999", 4, "35:30:46.000000"),
            ("-1 11:30:45.999999", 0, "-35:30:46"),
            ("-1 11:59:59.9999", 2, "-36:00:00.000000"),
        ];
        for (input, fsp, exp) in cases {
            let t = Duration::parse(input.as_bytes(), MAX_FSP)
                .unwrap()
                .round_frac(fsp)
                .unwrap();
            let res = format!("{}", t);
            assert_eq!(exp, res);
        }
    }

    #[test]
    fn test_codec() {
        let cases = vec![
            ("11:30:45.123456", 4),
            ("11:30:45.123456", 6),
            ("11:30:45.123456", 0),
            ("11:59:59.999999", 3),
            ("1 11:30:45.123456", 1),
            ("1 11:30:45.999999", 4),
            ("-1 11:30:45.999999", 0),
            ("-1 11:59:59.9999", 2),
        ];
        for (input, fsp) in cases {
            let t = Duration::parse(input.as_bytes(), fsp).unwrap();
            let mut buf = vec![];
            buf.encode_duration(t).unwrap();
            let got = Duration::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(t, got);
        }
    }

    #[test]
    fn test_checked_add_and_sub_duration() {
        /// `MAX_TIME_IN_SECS` is the maximum for mysql time type.
        const MAX_TIME_IN_SECS: i64 =
            (MAX_HOURS * SECS_PER_HOUR + MAX_MINUTES * SECS_PER_MINUTE + MAX_SECONDS) as i64;

        let cases = vec![
            ("11:30:45.123456", "00:00:14.876545", "11:31:00.000001"),
            ("11:30:45.123456", "00:30:00", "12:00:45.123456"),
            ("11:30:45.123456", "12:30:00", "1 00:00:45.123456"),
            ("11:30:45.123456", "1 12:30:00", "2 00:00:45.123456"),
        ];
        for (lhs, rhs, exp) in cases.clone() {
            let lhs = Duration::parse(lhs.as_bytes(), 6).unwrap();
            let rhs = Duration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_add(rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }
        for (exp, rhs, lhs) in cases {
            let lhs = Duration::parse(lhs.as_bytes(), 6).unwrap();
            let rhs = Duration::parse(rhs.as_bytes(), 6).unwrap();
            let res = lhs.checked_sub(rhs).unwrap();
            let exp = Duration::parse(exp.as_bytes(), 6).unwrap();
            assert_eq!(res, exp);
        }

        let lhs = Duration::parse(b"00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_add(rhs), None);
        let lhs = Duration::parse(b"-00:00:01", 6).unwrap();
        let rhs = Duration::from_nanos(MAX_TIME_IN_SECS * NANOS_PER_SEC, 6).unwrap();
        assert_eq!(lhs.checked_sub(rhs), None);
    }
}
