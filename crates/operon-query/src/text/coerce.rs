//! Query values coerced to a field's kind (plan M1.2 Task 2 rules 4–5, M1.5
//! C15: values follow the field kind).

use std::ops::Bound;

use operon_collection::{FieldKind, parse_date};
use serde_json::Value;

use crate::error::ServiceError;
use crate::ir::FieldValue;
use crate::json::pk::{format_uuid, parse_uuid};
use crate::json::values::format_date;

/// A value in a field's own type.
#[derive(Clone, Debug, PartialEq)]
pub enum Coerced {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    /// Milliseconds since the epoch.
    DateMs(i64),
    /// The value can never match this field.
    Never,
}

/// The comparison a range bound makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl RangeOp {
    fn is_lower(self) -> bool {
        matches!(self, RangeOp::Gt | RangeOp::Gte)
    }

    fn bound<T>(self, value: T) -> Bound<T> {
        match self {
            RangeOp::Gte | RangeOp::Lte => Bound::Included(value),
            RangeOp::Gt | RangeOp::Lt => Bound::Excluded(value),
        }
    }

    /// The bound of a value beyond the field's type: "match none" beyond the
    /// side the bound limits, "no bound" beyond the other.
    fn saturated(self, above: bool) -> Bound<Coerced> {
        if self.is_lower() == above {
            Bound::Included(Coerced::Never)
        } else {
            Bound::Unbounded
        }
    }
}

/// The dates Tantivy can hold, in epoch milliseconds (its i64 nanoseconds).
const DATE_MS: std::ops::RangeInclusive<i64> = i64::MIN / 1_000_000..=i64::MAX / 1_000_000;

/// 2^63 as an f64: the first f64 above `i64::MAX`.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// ES's name of a field kind.
fn kind_name(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text { .. } => "text",
        FieldKind::Keyword => "keyword",
        FieldKind::I64 => "long",
        FieldKind::F64 => "double",
        FieldKind::Bool => "boolean",
        FieldKind::Date => "date",
        FieldKind::Uuid => "uuid",
        FieldKind::Json => "json",
    }
}

fn type_name(value: &FieldValue) -> &'static str {
    match value {
        FieldValue::Str(_) => "string",
        FieldValue::I64(_) | FieldValue::U64(_) => "integer",
        FieldValue::F64(_) => "float",
        FieldValue::Bool(_) => "boolean",
        FieldValue::Date(_) => "date",
    }
}

fn wrong_type(kind: &FieldKind, field: &str, value: &FieldValue) -> ServiceError {
    ServiceError::InvalidArgument(format!(
        "cannot use a {} value on {} field {field}",
        type_name(value),
        kind_name(kind)
    ))
}

fn unparsable(kind: &FieldKind, field: &str, text: &str) -> ServiceError {
    ServiceError::InvalidArgument(format!(
        "cannot use {text:?} on {} field {field}",
        kind_name(kind)
    ))
}

/// RFC 3339 with 6 fraction digits and `Z`; the decimal µs outside the
/// years 0000–9999.
pub fn format_date_us(value_us: i64) -> String {
    format_date(value_us).unwrap_or_else(|| value_us.to_string())
}

/// `x` as an i64 when it is integral and in range.
fn integral_i64(x: f64) -> Option<i64> {
    (x.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&x)).then_some(x as i64)
}

fn date_ms(ms: i64) -> Coerced {
    if DATE_MS.contains(&ms) {
        Coerced::DateMs(ms)
    } else {
        Coerced::Never
    }
}

/// A term value for `kind`, per rule 4; Err = InvalidArgument with the
/// rule's message.
pub fn coerce_term(
    kind: &FieldKind,
    field: &str,
    value: &FieldValue,
) -> Result<Coerced, ServiceError> {
    let error = || Err(wrong_type(kind, field, value));
    Ok(match kind {
        FieldKind::Json => {
            return Err(ServiceError::InvalidArgument(format!(
                "use a path inside JSON field {field}"
            )));
        }
        FieldKind::Text { .. } | FieldKind::Keyword => Coerced::Str(match value {
            FieldValue::Str(s) => s.clone(),
            FieldValue::I64(n) => n.to_string(),
            FieldValue::U64(n) => n.to_string(),
            FieldValue::F64(x) => x.to_string(),
            FieldValue::Bool(b) => b.to_string(),
            FieldValue::Date(us) => format_date_us(*us),
        }),
        FieldKind::Uuid => match value {
            FieldValue::Str(s) => {
                parse_uuid(s).map_or(Coerced::Never, |bytes| Coerced::Str(format_uuid(&bytes)))
            }
            _ => Coerced::Never,
        },
        FieldKind::I64 => match value {
            FieldValue::Str(s) => match (s.parse::<i64>(), s.parse::<f64>()) {
                (Ok(n), _) => Coerced::I64(n),
                (_, Ok(x)) => integral_i64(x).map_or(Coerced::Never, Coerced::I64),
                _ => return Err(unparsable(kind, field, s)),
            },
            FieldValue::I64(n) => Coerced::I64(*n),
            FieldValue::U64(n) => i64::try_from(*n).map_or(Coerced::Never, Coerced::I64),
            FieldValue::F64(x) => integral_i64(*x).map_or(Coerced::Never, Coerced::I64),
            FieldValue::Bool(_) | FieldValue::Date(_) => return error(),
        },
        FieldKind::F64 => match value {
            FieldValue::Str(s) => match s.parse::<f64>() {
                Ok(x) if x.is_finite() => Coerced::F64(x),
                _ => return Err(unparsable(kind, field, s)),
            },
            FieldValue::I64(n) => Coerced::F64(*n as f64),
            FieldValue::U64(n) => Coerced::F64(*n as f64),
            FieldValue::F64(x) => Coerced::F64(*x),
            FieldValue::Bool(_) | FieldValue::Date(_) => return error(),
        },
        FieldKind::Bool => match value {
            FieldValue::Str(s) if s == "true" => Coerced::Bool(true),
            FieldValue::Str(s) if s == "false" => Coerced::Bool(false),
            FieldValue::Str(s) => return Err(unparsable(kind, field, s)),
            FieldValue::Bool(b) => Coerced::Bool(*b),
            _ => return error(),
        },
        FieldKind::Date => match value {
            FieldValue::Str(s) => match parse_date(&Value::String(s.clone())) {
                Ok(ms) => Coerced::DateMs(ms),
                Err(_) => return Err(unparsable(kind, field, s)),
            },
            FieldValue::I64(n) => date_ms(*n),
            FieldValue::U64(n) => i64::try_from(*n).map_or(Coerced::Never, date_ms),
            FieldValue::Date(us) if us.rem_euclid(1000) == 0 => date_ms(us.div_euclid(1000)),
            FieldValue::Date(_) => Coerced::Never,
            FieldValue::F64(_) | FieldValue::Bool(_) => return error(),
        },
    })
}

/// `ceil_div(a, b) = -((-a).div_euclid(b))`, without overflow for `i64::MIN`.
fn ceil_div(a: i64, b: i64) -> i64 {
    let floor = a.div_euclid(b);
    if a.rem_euclid(b) == 0 {
        floor
    } else {
        floor + 1
    }
}

/// The ms bound that compares exactly as the µs bound `value_us` does
/// against ms-precision values (rule 5).
pub fn date_bound_ms(value_us: i64, op: RangeOp) -> Bound<i64> {
    match op {
        RangeOp::Gte => Bound::Included(ceil_div(value_us, 1000)),
        RangeOp::Gt => Bound::Excluded(value_us.div_euclid(1000)),
        RangeOp::Lte => Bound::Included(value_us.div_euclid(1000)),
        RangeOp::Lt => Bound::Excluded(ceil_div(value_us, 1000)),
    }
}

/// An i64 bound of `op` from an integral f64, saturated beyond i64.
fn i64_bound(op: RangeOp, bound: Bound<f64>) -> Bound<Coerced> {
    let x = match bound {
        Bound::Included(x) | Bound::Excluded(x) => x,
        Bound::Unbounded => return Bound::Unbounded,
    };
    if x >= TWO_POW_63 {
        return op.saturated(true);
    }
    if x < -TWO_POW_63 {
        return op.saturated(false);
    }
    bound.map(|x| Coerced::I64(x as i64))
}

/// The i64 bound of an f64 value `x`, rounded as ES rounds a decimal bound
/// on a long field.
fn rounded_i64_bound(op: RangeOp, x: f64) -> Bound<Coerced> {
    let bound = match op {
        RangeOp::Gte => Bound::Included(x.ceil()),
        RangeOp::Gt => Bound::Excluded(x.floor()),
        RangeOp::Lte => Bound::Included(x.floor()),
        RangeOp::Lt => Bound::Excluded(x.ceil()),
    };
    i64_bound(op, bound)
}

/// A date bound in ms, saturated beyond Tantivy's dates.
fn date_bound(op: RangeOp, bound: Bound<i64>) -> Bound<Coerced> {
    match bound {
        Bound::Included(ms) | Bound::Excluded(ms) if ms > *DATE_MS.end() => op.saturated(true),
        Bound::Included(ms) | Bound::Excluded(ms) if ms < *DATE_MS.start() => op.saturated(false),
        bound => bound.map(Coerced::DateMs),
    }
}

/// A range bound for `kind`, per rule 5 (numbers and dates round to the
/// field's precision). `Unbounded` is no bound; a bound holding
/// [`Coerced::Never`] matches nothing.
pub fn coerce_bound(
    kind: &FieldKind,
    field: &str,
    op: RangeOp,
    value: &FieldValue,
) -> Result<Bound<Coerced>, ServiceError> {
    Ok(match (kind, value) {
        (FieldKind::I64, FieldValue::F64(x)) => rounded_i64_bound(op, *x),
        (FieldKind::I64, FieldValue::U64(n)) if i64::try_from(*n).is_err() => op.saturated(true),
        (FieldKind::I64, FieldValue::Str(s)) if s.parse::<i64>().is_err() => {
            match s.parse::<f64>() {
                Ok(x) if x.is_finite() => rounded_i64_bound(op, x),
                _ => return Err(unparsable(kind, field, s)),
            }
        }
        (FieldKind::Date, FieldValue::Date(us)) => date_bound(op, date_bound_ms(*us, op)),
        (FieldKind::Date, FieldValue::I64(ms)) => date_bound(op, op.bound(*ms)),
        (FieldKind::Date, FieldValue::U64(n)) => match i64::try_from(*n) {
            Ok(ms) => date_bound(op, op.bound(ms)),
            Err(_) => op.saturated(true),
        },
        _ => op.bound(coerce_term(kind, field, value)?),
    })
}
