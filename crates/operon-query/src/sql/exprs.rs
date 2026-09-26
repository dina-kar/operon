//! [`expr_to_query`] (plan M1.2 Task 10 rule 4): a DataFusion filter over a
//! collection's columns as an IR [`Query`], for `FilterBitmapExec`.
//!
//! The provider answers `Inexact` for every filter that converts, so
//! DataFusion re-applies it to the first-value columns: a converted query
//! may match more rows than the SQL predicate (a multi-valued field matches
//! on any value), never fewer. Two consequences:
//! - `NOT` converts only when its operand matches exactly the rows its
//!   predicate is true for and is never NULL (`IS [NOT] NULL` and `AND`,
//!   `OR`, `NOT` of those), since the complement of a superset is a subset;
//! - a comparison converts only on a field whose index answers it the way
//!   SQL compares the column: Keyword, I64, F64, Bool, Date and Uuid fields
//!   (Text fields are analyzed and Json columns are JSON text), with a
//!   literal of the column's type.

use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator, expr::InList};
use datafusion::scalar::ScalarValue;
use operon_collection::{CollectionSchema, FieldKind, FieldSpec, PrimaryKey};

use crate::ir::{FieldValue, Query};
use crate::json::pk::parse_uuid;

/// The `_id` column.
const ID: &str = "_id";

/// Rule 4: `expr` as a query over `schema`'s fields, or `None` when it does
/// not convert.
pub fn expr_to_query(expr: &Expr, schema: &CollectionSchema) -> Option<Query> {
    convert(expr, schema).map(|(query, _)| query)
}

fn bool_query(must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
    Query::Bool {
        must,
        should,
        must_not,
        filter: Vec::new(),
        minimum_should_match: None,
    }
}

/// The query and whether it is exact: it matches exactly the rows the
/// predicate is true for, and the predicate is never NULL.
fn convert(expr: &Expr, schema: &CollectionSchema) -> Option<(Query, bool)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let (a, exact_a) = convert(left, schema)?;
                let (b, exact_b) = convert(right, schema)?;
                Some((bool_query(vec![a, b], vec![], vec![]), exact_a && exact_b))
            }
            Operator::Or => {
                let (a, exact_a) = convert(left, schema)?;
                let (b, exact_b) = convert(right, schema)?;
                Some((bool_query(vec![], vec![a, b], vec![]), exact_a && exact_b))
            }
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                comparison(left, *op, right, schema).map(|query| (query, false))
            }
            _ => None,
        },
        Expr::Not(inner) => {
            let (query, exact) = convert(inner, schema)?;
            exact.then(|| (bool_query(vec![Query::MatchAll], vec![], vec![query]), true))
        }
        Expr::IsNotNull(inner) => {
            let spec = field(inner, schema)?;
            Some((
                Query::Exists {
                    field: spec.name.clone(),
                },
                true,
            ))
        }
        Expr::IsNull(inner) => {
            let spec = field(inner, schema)?;
            Some((
                Query::IsEmpty {
                    field: spec.name.clone(),
                },
                true,
            ))
        }
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => {
            let spec = field(expr, schema)?;
            let values = list
                .iter()
                .map(|item| literal(item).and_then(|value| typed(spec, value)))
                .collect::<Option<Vec<_>>>()?;
            Some((
                Query::Terms {
                    field: spec.name.clone(),
                    values,
                },
                false,
            ))
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            let spec = ranged(field(expr, schema)?)?;
            let low = typed(spec, literal(low)?)?;
            let high = typed(spec, literal(high)?)?;
            Some((
                Query::Range {
                    field: spec.name.clone(),
                    gt: None,
                    gte: Some(low),
                    lt: None,
                    lte: Some(high),
                },
                false,
            ))
        }
        _ => None,
    }
}

/// `col op lit` or `lit op col`.
fn comparison(left: &Expr, op: Operator, right: &Expr, schema: &CollectionSchema) -> Option<Query> {
    let (column, value, op) = match (left, right) {
        (Expr::Column(column), other) => (column, literal(other)?, op),
        (other, Expr::Column(column)) => (column, literal(other)?, op.swap()?),
        _ => return None,
    };
    if column.name == ID {
        return (op == Operator::Eq).then(|| ids(&value)).flatten();
    }
    let spec = schema
        .fields
        .iter()
        .find(|spec| spec.name == column.name)
        .filter(|spec| comparable(&spec.kind))?;
    let value = typed(spec, value)?;
    let field = spec.name.clone();
    if op == Operator::Eq {
        return Some(Query::Term { field, value });
    }
    ranged(spec)?;
    let (mut gt, mut gte, mut lt, mut lte) = (None, None, None, None);
    match op {
        Operator::Gt => gt = Some(value),
        Operator::GtEq => gte = Some(value),
        Operator::Lt => lt = Some(value),
        _ => lte = Some(value),
    }
    Some(Query::Range {
        field,
        gt,
        gte,
        lt,
        lte,
    })
}

/// `_id = 'x'`: every key whose display form is `x`.
fn ids(value: &FieldValue) -> Option<Query> {
    let FieldValue::Str(text) = value else {
        return None;
    };
    let mut keys = vec![PrimaryKey::Str(text.clone())];
    if let Ok(n) = text.parse::<u64>()
        && n.to_string() == *text
    {
        keys.push(PrimaryKey::U64(n));
    }
    if let Some(uuid) = parse_uuid(text) {
        keys.push(PrimaryKey::Uuid(uuid));
    }
    Some(Query::Ids(keys))
}

/// A collection field whose index answers comparisons and presence the way
/// SQL reads its first-value column.
fn comparable(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Keyword
            | FieldKind::I64
            | FieldKind::F64
            | FieldKind::Bool
            | FieldKind::Date
            | FieldKind::Uuid
    )
}

/// The field of a bare column reference.
fn field<'a>(expr: &Expr, schema: &'a CollectionSchema) -> Option<&'a FieldSpec> {
    let Expr::Column(column) = expr else {
        return None;
    };
    schema
        .fields
        .iter()
        .find(|spec| spec.name == column.name)
        .filter(|spec| comparable(&spec.kind))
}

/// A field ranges convert on.
fn ranged(spec: &FieldSpec) -> Option<&FieldSpec> {
    matches!(
        spec.kind,
        FieldKind::Keyword | FieldKind::I64 | FieldKind::F64 | FieldKind::Date
    )
    .then_some(spec)
}

/// A literal of the column's type for `spec`.
fn typed(spec: &FieldSpec, value: FieldValue) -> Option<FieldValue> {
    match (&spec.kind, value) {
        (FieldKind::Keyword, value @ FieldValue::Str(_)) => Some(value),
        (FieldKind::Uuid, FieldValue::Str(text)) => {
            parse_uuid(&text).map(|_| FieldValue::Str(text))
        }
        (FieldKind::I64, value @ (FieldValue::I64(_) | FieldValue::U64(_))) => Some(value),
        (
            FieldKind::F64,
            value @ (FieldValue::F64(_) | FieldValue::I64(_) | FieldValue::U64(_)),
        ) => Some(value),
        (FieldKind::Bool, value @ FieldValue::Bool(_)) => Some(value),
        (FieldKind::Date, value @ FieldValue::Date(_)) => Some(value),
        _ => None,
    }
}

/// A non-null literal as a field value: Utf8 → `Str`, integers → `I64`
/// (`U64` above `i64::MAX`), floats → `F64`, Boolean → `Bool`, timestamps →
/// `Date` (µs).
fn literal(expr: &Expr) -> Option<FieldValue> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    Some(match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => FieldValue::Str(s.clone()),
        ScalarValue::Int8(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::Int16(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => FieldValue::I64(*n),
        ScalarValue::UInt8(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::UInt16(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::UInt32(Some(n)) => FieldValue::I64(i64::from(*n)),
        ScalarValue::UInt64(Some(n)) => match i64::try_from(*n) {
            Ok(n) => FieldValue::I64(n),
            Err(_) => FieldValue::U64(*n),
        },
        ScalarValue::Float32(Some(x)) if x.is_finite() => FieldValue::F64(f64::from(*x)),
        ScalarValue::Float64(Some(x)) if x.is_finite() => FieldValue::F64(*x),
        ScalarValue::Boolean(Some(b)) => FieldValue::Bool(*b),
        ScalarValue::TimestampSecond(Some(s), _) => FieldValue::Date(s.checked_mul(1_000_000)?),
        ScalarValue::TimestampMillisecond(Some(ms), _) => FieldValue::Date(ms.checked_mul(1_000)?),
        ScalarValue::TimestampMicrosecond(Some(us), _) => FieldValue::Date(*us),
        // Truncated toward negative infinity would lose the ordering of a
        // bound; a nanosecond literal converts only when it is whole µs.
        ScalarValue::TimestampNanosecond(Some(ns), _) if ns % 1_000 == 0 => {
            FieldValue::Date(ns / 1_000)
        }
        _ => return None,
    })
}
