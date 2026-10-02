//! Dates and date math in queries (plan M1.5 Task 7 item 7, Ruling 17):
//! `strict_date_optional_time` / `date_optional_time` (and the `_nanos`
//! variant, kept at µs), `epoch_millis`, and `now` / `<date>||` followed by
//! `+N<unit>`, `-N<unit>` and `/<unit>`.

use time::{Date, Duration, Month, PrimitiveDateTime, Time, UtcOffset};

use crate::error::EsError;

/// Which end of an imprecise date a bound takes (ES's `roundUp`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rounding {
    /// The first instant: `2026-09-24` is its midnight.
    Down,
    /// The last millisecond: `2026-09-24` is `23:59:59.999`.
    Up,
}

/// The format ES names in a date parse error.
const FORMAT: &str = "strict_date_optional_time||epoch_millis";

/// µs in one millisecond.
const MS: i64 = 1_000;

fn parse_error(expr: &str) -> EsError {
    EsError::new(
        400,
        "parse_exception",
        format!("failed to parse date field [{expr}] with format [{FORMAT}]"),
    )
}

fn math_error(reason: String) -> EsError {
    EsError::new(400, "parse_exception", reason)
}

/// `expr` as µs since the epoch, UTC (item 7): `now` or `<date>||` followed
/// by operations, or a bare date whose missing parts `round` fills.
pub fn parse_date_math(expr: &str, now_ms: i64, round: Rounding) -> Result<i64, EsError> {
    let (anchor, math) = if let Some(rest) = expr.strip_prefix("now") {
        (now_ms.saturating_mul(MS), rest)
    } else if let Some((date, rest)) = expr.split_once("||") {
        (parse_date(date, Rounding::Down)?, rest)
    } else {
        return parse_date(expr, round);
    };
    apply_math(expr, anchor, math, round)
}

/// A date without math: an ISO date-time (4-digit year; the missing parts
/// filled by `round`) or epoch milliseconds.
pub fn parse_date(text: &str, round: Rounding) -> Result<i64, EsError> {
    if let Some(us) = parse_iso(text, round) {
        return Ok(us);
    }
    epoch_millis(text).ok_or_else(|| parse_error(text))
}

/// Epoch milliseconds, optionally signed and with a fraction (to µs).
pub fn epoch_millis(text: &str) -> Option<i64> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let ms: i64 = whole.parse().ok()?;
    let mut sub = 0i64;
    for (i, b) in fraction.bytes().take(3).enumerate() {
        sub += i64::from(b - b'0') * 10i64.pow(2 - i as u32);
    }
    let us = ms.checked_mul(MS)?.checked_add(sub)?;
    Some(if negative { -us } else { us })
}

/// A cursor over the bytes of a date.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn digits(&mut self, n: usize) -> Option<u32> {
        let end = self.at.checked_add(n)?;
        let chunk = self.bytes.get(self.at..end)?;
        if !chunk.iter().all(u8::is_ascii_digit) {
            return None;
        }
        self.at = end;
        Some(
            chunk
                .iter()
                .fold(0u32, |acc, b| acc * 10 + u32::from(b - b'0')),
        )
    }

    fn eat(&mut self, b: u8) -> bool {
        if self.bytes.get(self.at) == Some(&b) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }
}

/// How precise a parsed date is: the unit a rounded-up date fills.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Precision {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Fraction,
}

/// `yyyy[-MM[-dd[Thh[:mm[:ss[.f]]]]]][Z|±hh[:mm]]` (a zone needs a time).
fn parse_iso(text: &str, round: Rounding) -> Option<i64> {
    let mut c = Cursor {
        bytes: text.as_bytes(),
        at: 0,
    };
    let year = c.digits(4)? as i32;
    let (mut month, mut day, mut hour, mut minute, mut second, mut micros) = (1, 1, 0, 0, 0, 0);
    let mut precision = Precision::Year;
    let mut offset = UtcOffset::UTC;
    if c.eat(b'-') {
        month = c.digits(2)?;
        precision = Precision::Month;
        if c.eat(b'-') {
            day = c.digits(2)?;
            precision = Precision::Day;
            if c.eat(b'T') {
                hour = c.digits(2)?;
                precision = Precision::Hour;
                if c.eat(b':') {
                    minute = c.digits(2)?;
                    precision = Precision::Minute;
                    if c.eat(b':') {
                        second = c.digits(2)?;
                        precision = Precision::Second;
                        if c.eat(b'.') || c.eat(b',') {
                            let start = c.at;
                            while c.bytes.get(c.at).is_some_and(u8::is_ascii_digit) {
                                c.at += 1;
                            }
                            let fraction = &text[start..c.at];
                            if fraction.is_empty() || fraction.len() > 9 {
                                return None;
                            }
                            micros = fraction
                                .bytes()
                                .chain(std::iter::repeat(b'0'))
                                .take(6)
                                .fold(0u32, |acc, b| acc * 10 + u32::from(b - b'0'));
                            precision = Precision::Fraction;
                        }
                    }
                }
                offset = parse_zone(&mut c)?;
            }
        }
    }
    if !c.done() {
        return None;
    }
    let date =
        Date::from_calendar_date(year, Month::try_from(month as u8).ok()?, day as u8).ok()?;
    let time = Time::from_hms_micro(hour as u8, minute as u8, second as u8, micros).ok()?;
    let start = PrimitiveDateTime::new(date, time).assume_offset(offset);
    let start_us = i64::try_from(start.unix_timestamp_nanos() / 1_000).ok()?;
    if round == Rounding::Down || precision == Precision::Fraction {
        return Some(start_us);
    }
    let local = PrimitiveDateTime::new(date, time);
    // ES's round-up parser fills only the time of day: a missing month or
    // day stays 1, so `2026-10` rounds up to `2026-10-01T23:59:59.999`
    // (checked against the 8.19 oracle, row T11-3).
    let next = match precision {
        Precision::Year | Precision::Month | Precision::Day => local.checked_add(Duration::DAY)?,
        Precision::Hour => local.checked_add(Duration::HOUR)?,
        Precision::Minute => local.checked_add(Duration::MINUTE)?,
        Precision::Second | Precision::Fraction => local.checked_add(Duration::SECOND)?,
    };
    let next_us = i64::try_from(next.assume_offset(offset).unix_timestamp_nanos() / 1_000).ok()?;
    Some(next_us - MS)
}

/// `Z`, `±hh`, `±hhmm` or `±hh:mm`; nothing is UTC.
fn parse_zone(c: &mut Cursor<'_>) -> Option<UtcOffset> {
    if c.eat(b'Z') {
        return Some(UtcOffset::UTC);
    }
    let sign = if c.eat(b'+') {
        1
    } else if c.eat(b'-') {
        -1
    } else {
        return Some(UtcOffset::UTC);
    };
    let hours = c.digits(2)? as i8;
    let minutes = if c.eat(b':') {
        c.digits(2)?
    } else {
        c.digits(2).unwrap_or(0)
    } as i8;
    UtcOffset::from_hms(sign * hours, sign * minutes, 0).ok()
}

/// `at` plus `months` calendar months, the day clamped to the month's end
/// (`2026-01-31 + 1M` is `2026-02-28`).
fn add_months(at: PrimitiveDateTime, months: i64) -> Option<PrimitiveDateTime> {
    let index =
        (i64::from(at.year()) * 12 + i64::from(u8::from(at.month())) - 1).checked_add(months)?;
    let year = i32::try_from(index.div_euclid(12)).ok()?;
    let month = Month::try_from(u8::try_from(index.rem_euclid(12) + 1).ok()?).ok()?;
    let last = time::util::days_in_month(month, year);
    let date = Date::from_calendar_date(year, month, at.day().min(last)).ok()?;
    Some(PrimitiveDateTime::new(date, at.time()))
}

fn to_datetime(us: i64) -> Option<PrimitiveDateTime> {
    let at = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(us) * 1_000).ok()?;
    Some(PrimitiveDateTime::new(at.date(), at.time()))
}

fn to_us(at: PrimitiveDateTime) -> Option<i64> {
    i64::try_from(at.assume_utc().unix_timestamp_nanos() / 1_000).ok()
}

/// The start of the `unit` holding `at` (weeks start on Monday).
fn floor(at: PrimitiveDateTime, unit: u8) -> Option<PrimitiveDateTime> {
    let date = at.date();
    Some(match unit {
        b'y' => PrimitiveDateTime::new(
            Date::from_calendar_date(date.year(), Month::January, 1).ok()?,
            Time::MIDNIGHT,
        ),
        b'M' => PrimitiveDateTime::new(
            Date::from_calendar_date(date.year(), date.month(), 1).ok()?,
            Time::MIDNIGHT,
        ),
        b'w' => {
            let back = i64::from(date.weekday().number_days_from_monday());
            PrimitiveDateTime::new(date.checked_sub(Duration::days(back))?, Time::MIDNIGHT)
        }
        b'd' => PrimitiveDateTime::new(date, Time::MIDNIGHT),
        b'h' | b'H' => PrimitiveDateTime::new(date, Time::from_hms(at.hour(), 0, 0).ok()?),
        b'm' => PrimitiveDateTime::new(date, Time::from_hms(at.hour(), at.minute(), 0).ok()?),
        b's' => PrimitiveDateTime::new(
            date,
            Time::from_hms(at.hour(), at.minute(), at.second()).ok()?,
        ),
        _ => return None,
    })
}

/// `at` plus `n` of `unit`.
fn add(at: PrimitiveDateTime, n: i64, unit: u8) -> Option<PrimitiveDateTime> {
    // `Duration::{weeks, days, hours, minutes}` panic on overflow.
    let secs = |per: i64| n.checked_mul(per).map(Duration::seconds);
    match unit {
        b'y' => add_months(at, n.checked_mul(12)?),
        b'M' => add_months(at, n),
        b'w' => at.checked_add(secs(604_800)?),
        b'd' => at.checked_add(secs(86_400)?),
        b'h' | b'H' => at.checked_add(secs(3_600)?),
        b'm' => at.checked_add(secs(60)?),
        b's' => at.checked_add(Duration::seconds(n)),
        _ => None,
    }
}

const UNITS: &[u8] = b"yMwdhHms";

/// The operations after the anchor, applied in order.
fn apply_math(expr: &str, anchor_us: i64, math: &str, round: Rounding) -> Result<i64, EsError> {
    let overflow = || parse_error(expr);
    let mut at = to_datetime(anchor_us).ok_or_else(overflow)?;
    let bytes = math.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let op = bytes[i];
        i += 1;
        match op {
            b'/' => {
                let Some(&unit) = bytes.get(i) else {
                    return Err(math_error(format!("truncated date math [{math}]")));
                };
                i += 1;
                if !UNITS.contains(&unit) {
                    return Err(math_error(format!(
                        "unit [{}] not supported for date math [{math}]",
                        unit as char
                    )));
                }
                at = floor(at, unit).ok_or_else(overflow)?;
                if round == Rounding::Up {
                    at = add(at, 1, unit).ok_or_else(overflow)? - Duration::milliseconds(1);
                }
            }
            b'+' | b'-' => {
                let start = i;
                while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                }
                let n: i64 = if start == i {
                    1
                } else {
                    math[start..i].parse().map_err(|_| overflow())?
                };
                let Some(&unit) = bytes.get(i) else {
                    return Err(math_error(format!("truncated date math [{math}]")));
                };
                i += 1;
                if !UNITS.contains(&unit) {
                    return Err(math_error(format!(
                        "unit [{}] not supported for date math [{math}]",
                        unit as char
                    )));
                }
                let n = if op == b'-' { -n } else { n };
                at = add(at, n, unit).ok_or_else(overflow)?;
            }
            _ => {
                return Err(math_error(format!(
                    "operator not supported for date math [{math}]"
                )));
            }
        }
    }
    to_us(at).ok_or_else(overflow)
}
