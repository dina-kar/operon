//! Field names and query values (plan M1.5 Task 7 items 2 and 4): what a
//! query's field names, and a JSON value coerced to that field's type.

use loams_collection::{FieldKind, FieldSpec};
use loams_query::FieldValue;
use loams_query::text::{ResolvedField, resolve_field};
use serde_json::Value;

use super::datemath::{Rounding, parse_date_math};
use crate::error::EsError;
use crate::mapping::{IndexView, VectorView};

/// ES's metadata field of document ids.
pub const ID_FIELD: &str = "_id";

/// What a field name in a query names.
#[derive(Clone, Debug)]
pub enum FieldRef<'a> {
    /// `_id`.
    Id,
    /// A mapped field.
    Field(&'a FieldSpec),
    /// A path inside a `flattened` (Json) field.
    Json { spec: &'a FieldSpec, path: String },
    /// A `dense_vector`.
    Vector(&'a VectorView),
    /// Nothing the mapping knows.
    Unmapped,
}

/// Resolves `name` against the index's mapping.
pub fn resolve<'a>(view: &'a IndexView, name: &str) -> FieldRef<'a> {
    if name == ID_FIELD {
        return FieldRef::Id;
    }
    if let Some(vector) = view.es.vectors.get(name) {
        return FieldRef::Vector(vector);
    }
    match resolve_field(&view.info.schema, name) {
        ResolvedField::Plain { spec } => FieldRef::Field(spec),
        ResolvedField::JsonPath { spec, path } => FieldRef::Json { spec, path },
        ResolvedField::Unknown => FieldRef::Unmapped,
    }
}

/// The ES type of the field at `name` (`text`, `keyword`, `long`, …).
pub fn es_type<'a>(view: &'a IndexView, name: &str) -> &'a str {
    view.es.es_types.get(name).map_or("", String::as_str)
}

/// 400 `query_shard_exception` `"failed to create query: <why>"`, as ES wraps
/// an error of query construction.
pub fn shard_error(why: impl Into<String>) -> EsError {
    EsError::new(
        400,
        "query_shard_exception",
        format!("failed to create query: {}", why.into()),
    )
}

/// Refuses a search on a field ES does not index (row T2-2): a `text` with
/// `index: false` and a `binary` are unindexed keywords in the schema, and
/// ES refuses queries on them.
pub fn check_searchable(view: &IndexView, name: &str, spec: &FieldSpec) -> Result<(), EsError> {
    match es_type(view, name) {
        // ES's `BinaryFieldMapper` text (row T11-3).
        "binary" => Err(shard_error("Binary fields do not support searching")),
        "text" if !spec.indexed => Err(shard_error(format!(
            "Cannot search on field [{name}] since it is not indexed."
        ))),
        _ => Ok(()),
    }
}

/// Whether [`check_searchable`] accepts the field.
pub fn is_searchable(view: &IndexView, name: &str, spec: &FieldSpec) -> bool {
    check_searchable(view, name, spec).is_ok()
}

/// Whether a field can be searched as text: a `Text` or `Keyword` field
/// that [`check_searchable`] accepts (wildcard expansion skips the others).
pub fn is_text_like(view: &IndexView, spec: &FieldSpec) -> bool {
    matches!(spec.kind, FieldKind::Text { .. } | FieldKind::Keyword)
        && check_searchable(view, &spec.name, spec).is_ok()
}

/// The canonical text of a scalar: a string as it is, a number as its JSON
/// text, a bool as `true`/`false`. `None` for null, arrays and objects.
pub fn scalar_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A JSON number as the IR value of its own class.
fn number_value(n: &serde_json::Number) -> FieldValue {
    if let Some(i) = n.as_i64() {
        FieldValue::I64(i)
    } else if let Some(u) = n.as_u64() {
        FieldValue::U64(u)
    } else {
        FieldValue::F64(n.as_f64().unwrap_or(f64::NAN))
    }
}

fn input_error(v: &Value) -> EsError {
    let text = scalar_text(v).unwrap_or_else(|| v.to_string());
    shard_error(format!("For input string: \"{text}\""))
}

/// `v` coerced to the type of `field` (item 4); `None` when the field is
/// unmapped. A date string goes through [`parse_date_math`] rounded down.
pub fn coerce(
    view: &IndexView,
    field: &str,
    v: &Value,
    now_ms: i64,
) -> Result<Option<FieldValue>, EsError> {
    coerce_rounded(view, field, v, now_ms, Rounding::Down)
}

/// [`coerce`] with the rounding of a date bound.
pub fn coerce_rounded(
    view: &IndexView,
    field: &str,
    v: &Value,
    now_ms: i64,
    round: Rounding,
) -> Result<Option<FieldValue>, EsError> {
    if matches!(v, Value::Null | Value::Array(_) | Value::Object(_)) {
        return Err(EsError::parsing(format!(
            "[{field}] expects a scalar value, found [{v}]"
        )));
    }
    match resolve(view, field) {
        FieldRef::Unmapped => Ok(None),
        FieldRef::Id => Ok(scalar_text(v).map(FieldValue::Str)),
        FieldRef::Vector(_) => Err(shard_error(format!(
            "Field [{field}] of type [dense_vector] does not support term or range queries"
        ))),
        // A flattened path keeps the JSON value's own type (row E11).
        FieldRef::Json { .. } => Ok(Some(json_class(v))),
        FieldRef::Field(spec) => kind_value(&spec.kind, v, now_ms, round).map(Some),
    }
}

/// A scalar in its own class: a number as I64/U64/F64, a string as `Str`,
/// a bool as `Bool`.
pub fn json_class(v: &Value) -> FieldValue {
    match v {
        Value::Number(n) => number_value(n),
        Value::Bool(b) => FieldValue::Bool(*b),
        other => FieldValue::Str(scalar_text(other).unwrap_or_default()),
    }
}

/// `v` as a value of `kind`.
fn kind_value(
    kind: &FieldKind,
    v: &Value,
    now_ms: i64,
    round: Rounding,
) -> Result<FieldValue, EsError> {
    Ok(match kind {
        FieldKind::Text { .. } | FieldKind::Keyword | FieldKind::Uuid => {
            FieldValue::Str(scalar_text(v).unwrap_or_default())
        }
        FieldKind::Json => json_class(v),
        FieldKind::I64 => match v {
            Value::Number(n) => match number_value(n) {
                // A fractional value stays a float: a term matches nothing
                // and a range bound rounds by its operator (M1.2 coerce).
                FieldValue::F64(x) if x.fract() == 0.0 && x.abs() < 9.2e18 => {
                    FieldValue::I64(x as i64)
                }
                other => other,
            },
            Value::String(s) => match (s.trim().parse::<i64>(), s.trim().parse::<f64>()) {
                (Ok(n), _) => FieldValue::I64(n),
                (_, Ok(x)) if x.is_finite() => {
                    if x.fract() == 0.0 && x.abs() < 9.2e18 {
                        FieldValue::I64(x as i64)
                    } else {
                        FieldValue::F64(x)
                    }
                }
                _ => return Err(input_error(v)),
            },
            _ => return Err(input_error(v)),
        },
        FieldKind::F64 => match v {
            Value::Number(n) => FieldValue::F64(n.as_f64().unwrap_or(f64::NAN)),
            Value::String(s) => match s.trim().parse::<f64>() {
                Ok(x) if x.is_finite() => FieldValue::F64(x),
                _ => return Err(input_error(v)),
            },
            _ => return Err(input_error(v)),
        },
        FieldKind::Bool => match v {
            Value::Bool(b) => FieldValue::Bool(*b),
            Value::String(s) if s == "true" => FieldValue::Bool(true),
            Value::String(s) if s == "false" || s.is_empty() => FieldValue::Bool(false),
            other => {
                return Err(shard_error(format!(
                    "Can't parse boolean value [{}], expected [true] or [false]",
                    scalar_text(other).unwrap_or_default()
                )));
            }
        },
        FieldKind::Date => match v {
            Value::Number(n) => match n.as_i64() {
                Some(ms) => FieldValue::Date(ms.saturating_mul(1_000)),
                None => {
                    let ms = n.as_f64().unwrap_or(f64::NAN);
                    if !ms.is_finite() {
                        return Err(input_error(v));
                    }
                    FieldValue::Date((ms * 1_000.0) as i64)
                }
            },
            Value::String(s) => FieldValue::Date(parse_date_math(s, now_ms, round)?),
            _ => return Err(input_error(v)),
        },
    })
}
