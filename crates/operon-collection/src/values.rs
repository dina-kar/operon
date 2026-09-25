//! Value extraction, coercion and document validation (overview §6.3, plan
//! M1.1 Task 6).
//!
//! A field's values are extracted from `_source` by its `source_path`
//! ([`extract`]) and coerced to its kind with Elasticsearch's rules
//! ([`coerce`]). [`check_document`] and [`check_patch`] validate a whole op
//! against a schema: its field values, its vectors, and the `_source` paths
//! no field maps (per [`DynamicMapping`]).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use operon_common::schema::{CollectionSchema, DynamicMapping, FieldKind};
use operon_quickwit::datetime::StrptimeParser;
use serde_json::{Map, Value};

use crate::doc::{DocOp, Document, SparseVector};
use crate::dynamic;

/// One typed value of a field, as it is indexed.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexValue {
    Text(String),
    Keyword(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    /// Milliseconds since the epoch.
    Date(i64),
    Uuid([u8; 16]),
}

/// One reason an op does not fit its schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// The field, `vectors.<name>`, `sparse_vectors.<name>`, the unmapped
    /// path (strict mapping) or `delete_keys`.
    pub field: String,
    pub message: String,
}

/// The typed values of a valid document: per non-Json field that has at
/// least one value, its index in `schema.fields` and its values, in field
/// order. (Json fields are indexed whole, plan M1.1 Task 8.)
#[derive(Clone, Debug, PartialEq)]
pub struct ExtractedDoc {
    pub values: Vec<(usize, Vec<IndexValue>)>,
}

/// Why an op was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocRejection {
    /// Every violation found, in check order: fields, then unmapped paths,
    /// then vectors, then sparse vectors (then, for a patch, `delete_keys`
    /// and its upsert document).
    Violations(Vec<Violation>),
    /// With [`DynamicMapping::Map`]: the unmapped paths that dynamic mapping
    /// would map (sorted); the gateway maps them first (R17).
    DynamicMappingRequired { paths: Vec<String> },
}

/// Every JSON value at `path` in `source`, in document order.
///
/// `path` is dot-separated. A key may itself contain dots: every way of
/// grouping the segments into keys is followed, so `{"a.b": 1}` and
/// `{"a": {"b": 1}}` both give `a.b`. Arrays are transparent at every step
/// and flattened, including at the end of the path. `null` values are
/// returned (they coerce to nothing).
///
/// `path == ""` gives the whole source as one object (Json fields only); it
/// is the only value not borrowed from `source`.
pub fn extract<'a>(source: &'a Map<String, Value>, path: &str) -> Vec<Cow<'a, Value>> {
    if path.is_empty() {
        return vec![Cow::Owned(Value::Object(source.clone()))];
    }
    let segments: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    extract_in(source, &segments, &mut out);
    out.into_iter().map(Cow::Borrowed).collect()
}

/// Resolves the non-empty `segments` against `object`.
fn extract_in<'a>(object: &'a Map<String, Value>, segments: &[&str], out: &mut Vec<&'a Value>) {
    for k in 1..=segments.len() {
        if let Some(value) = object.get(&segments[..k].join(".")) {
            extract_at(value, &segments[k..], out);
        }
    }
}

fn extract_at<'a>(value: &'a Value, segments: &[&str], out: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                extract_at(item, segments, out);
            }
        }
        _ if segments.is_empty() => out.push(value),
        Value::Object(object) => extract_in(object, segments, out),
        // A scalar with segments left: the path does not exist here.
        _ => {}
    }
}

fn kind_name(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text { .. } => "text",
        FieldKind::Keyword => "keyword",
        FieldKind::I64 => "i64",
        FieldKind::F64 => "f64",
        FieldKind::Bool => "bool",
        FieldKind::Date => "date",
        FieldKind::Uuid => "uuid",
        FieldKind::Json => "json",
    }
}

/// The longest value shown in a message, in bytes of its JSON text.
const MAX_SHOWN_VALUE: usize = 64;

/// `value` as JSON text, cut at [`MAX_SHOWN_VALUE`] bytes.
fn show(value: &Value) -> String {
    let mut text = value.to_string();
    if text.len() > MAX_SHOWN_VALUE {
        let mut end = MAX_SHOWN_VALUE;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    text
}

fn malformed(kind: &FieldKind, value: &Value) -> String {
    format!("cannot index {} as {}", show(value), kind_name(kind))
}

/// One JSON value as `kind`, with Elasticsearch's coercion: `Ok(None)` for
/// `null`, `Err(message)` when the value is malformed for the kind.
///
/// - `Text`, `Keyword`: strings; numbers and bools become their JSON text.
/// - `I64`: integers in range; other numbers, and strings that parse as a
///   number, truncated toward zero if that is in range (`3.5` and `"3.5"`
///   give 3).
/// - `F64`: numbers; strings that parse as a finite `f64`.
/// - `Bool`: `true`, `false`, `"true"`, `"false"`, and `""` (false).
/// - `Date`: [`parse_date`].
/// - `Uuid`: strings `uuid::Uuid::parse_str` accepts.
/// - `Json` is never coerced (a Json field is indexed whole, Task 8): every
///   value is an error.
///
/// Objects and arrays are malformed for every kind.
pub fn coerce(kind: &FieldKind, value: &Value) -> Result<Option<IndexValue>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let bad = || malformed(kind, value);
    let coerced = match (kind, value) {
        (FieldKind::Json, _) => {
            return Err("a json field is indexed whole, not coerced".to_string());
        }
        (_, Value::Object(_) | Value::Array(_)) => return Err(bad()),
        (FieldKind::Text { .. }, _) => IndexValue::Text(scalar_text(value)),
        (FieldKind::Keyword, _) => IndexValue::Keyword(scalar_text(value)),
        (FieldKind::I64, Value::Number(n)) => IndexValue::I64(
            n.as_i64()
                .or_else(|| float_to_i64(n.as_f64()?))
                .ok_or_else(bad)?,
        ),
        (FieldKind::I64, Value::String(s)) => IndexValue::I64(
            s.parse()
                .ok()
                .or_else(|| float_to_i64(s.parse().ok()?))
                .ok_or_else(bad)?,
        ),
        (FieldKind::F64, Value::Number(n)) => {
            IndexValue::F64(n.as_f64().filter(|f| f.is_finite()).ok_or_else(bad)?)
        }
        (FieldKind::F64, Value::String(s)) => IndexValue::F64(
            s.parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .ok_or_else(bad)?,
        ),
        (FieldKind::Bool, Value::Bool(b)) => IndexValue::Bool(*b),
        (FieldKind::Bool, Value::String(s)) => match s.as_str() {
            "true" => IndexValue::Bool(true),
            "false" | "" => IndexValue::Bool(false),
            _ => return Err(bad()),
        },
        (FieldKind::Date, _) => IndexValue::Date(parse_date(value).map_err(|_| bad())?),
        (FieldKind::Uuid, Value::String(s)) => {
            IndexValue::Uuid(*uuid::Uuid::parse_str(s).map_err(|_| bad())?.as_bytes())
        }
        _ => return Err(bad()),
    };
    Ok(Some(coerced))
}

/// A string as it is; a number or bool as its JSON text.
fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `f` truncated toward zero, if that is in the `i64` range (ES coerces
/// `3.5` to `3`). Not-a-number and infinities are out of range.
fn float_to_i64(f: f64) -> Option<i64> {
    // -2^63 is exact as an f64; 2^63 is the first value out of range.
    const BOUND: f64 = 9_223_372_036_854_775_808.0;
    let truncated = f.trunc();
    (-BOUND..BOUND)
        .contains(&truncated)
        .then_some(truncated as i64)
}

/// The Java-style date formats a string may use after the digit and RFC 3339
/// forms, in order: RFC 3339 (`yyyy-MM-dd'T'HH:mm:ss[.S…]` then `Z`, `±HH:MM`
/// or `±HHMM`), the same without an offset (UTC), `yyyy-MM-dd`,
/// `yyyy/MM/dd HH:mm:ss` and `yyyy/MM/dd`.
const DATE_FORMATS: [&str; 5] = [
    "yyyy-MM-dd'T'HH:mm:ss[.SSS]Z",
    "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
    "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd",
];

static DATE_PARSERS: LazyLock<Vec<StrptimeParser>> = LazyLock::new(|| {
    DATE_FORMATS
        .iter()
        .map(|format| {
            StrptimeParser::from_java_datetime_format(format).expect("a valid built-in format")
        })
        .collect()
});

/// Whether `s` is a non-empty run of ASCII digits: an epoch-millis string.
pub(crate) fn is_epoch_millis(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// A date string in one of the accepted formats, as epoch milliseconds.
pub(crate) fn parse_date_str(s: &str) -> Option<i64> {
    if is_epoch_millis(s) {
        return s.parse().ok();
    }
    DATE_PARSERS.iter().find_map(|parser| {
        let date = parser.parse_date_time(s).ok()?;
        i64::try_from(date.unix_timestamp_nanos().div_euclid(1_000_000)).ok()
    })
}

/// A date as milliseconds since the epoch (Elasticsearch
/// `strict_date_optional_time||epoch_millis` plus the dynamic-detection
/// formats).
///
/// A JSON integer is epoch milliseconds. A string is, in order: all digits
/// (epoch milliseconds); RFC 3339 (`2024-01-02T03:04:05Z`,
/// `2024-01-02T03:04:05.123+02:00`); `yyyy-MM-dd'T'HH:mm:ss[.SSS]` without
/// an offset (UTC); `yyyy-MM-dd`; `yyyy/MM/dd HH:mm:ss`; `yyyy/MM/dd`.
/// Anything else is malformed.
pub fn parse_date(value: &Value) -> Result<i64, String> {
    let millis = match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => parse_date_str(s),
        _ => None,
    };
    millis.ok_or_else(|| format!("cannot parse {} as a date", show(value)))
}

/// Calls `leaf(path, value)` for every leaf of `source` in document order
/// (keys sorted, then array order): every non-object, non-array, non-null
/// value, with its key path joined by `.`. Arrays are transparent.
pub(crate) fn for_each_leaf<'a>(
    source: &'a Map<String, Value>,
    leaf: &mut impl FnMut(&str, &'a Value),
) {
    let mut path = String::new();
    for (key, value) in source {
        path.clear();
        path.push_str(key);
        walk_leaves(value, &mut path, leaf);
    }
}

fn walk_leaves<'a>(value: &'a Value, path: &mut String, leaf: &mut impl FnMut(&str, &'a Value)) {
    match value {
        Value::Null => {}
        Value::Array(items) => {
            for item in items {
                walk_leaves(item, path, leaf);
            }
        }
        Value::Object(object) => {
            let len = path.len();
            for (key, nested) in object {
                path.truncate(len);
                path.push('.');
                path.push_str(key);
                walk_leaves(nested, path, leaf);
            }
            path.truncate(len);
        }
        _ => leaf(path, value),
    }
}

/// Whether some field covers `path`: a field reads it, or a Json field reads
/// the whole source or a prefix of it.
pub(crate) fn is_covered(schema: &CollectionSchema, path: &str) -> bool {
    schema.fields.iter().any(|field| {
        field.source_path == path
            || (field.kind == FieldKind::Json
                && (field.source_path.is_empty()
                    || path
                        .strip_prefix(field.source_path.as_str())
                        .is_some_and(|rest| rest.starts_with('.'))))
    })
}

/// The leaf paths of `source` (the key paths, joined by `.` with arrays
/// transparent, of every value that is not an object, `null` or an empty
/// array) that no field covers, sorted and without duplicates.
pub fn unmapped_paths(schema: &CollectionSchema, source: &Map<String, Value>) -> Vec<String> {
    let mut paths = BTreeSet::new();
    for_each_leaf(source, &mut |path, _| {
        if !paths.contains(path) && !is_covered(schema, path) {
            paths.insert(path.to_string());
        }
    });
    paths.into_iter().collect()
}

/// What checking one part of an op found.
#[derive(Default)]
struct Findings {
    violations: Vec<Violation>,
    /// Unmapped paths dynamic mapping would map (`DynamicMapping::Map`).
    required: BTreeSet<String>,
}

impl Findings {
    fn violation(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.violations.push(Violation {
            field: field.into(),
            message: message.into(),
        });
    }

    fn into_result<T>(self, ok: T) -> Result<T, DocRejection> {
        if !self.violations.is_empty() {
            // A document that breaks the schema is refused before any
            // mapping is proposed for it.
            Err(DocRejection::Violations(self.violations))
        } else if !self.required.is_empty() {
            Err(DocRejection::DynamicMappingRequired {
                paths: self.required.into_iter().collect(),
            })
        } else {
            Ok(ok)
        }
    }
}

/// Checks a source (a whole document's or a patch's): coerces every non-Json
/// field's values, and handles unmapped paths per `schema.dynamic`.
fn check_source(
    schema: &CollectionSchema,
    source: &Map<String, Value>,
    findings: &mut Findings,
) -> ExtractedDoc {
    let mut values = Vec::new();
    for (index, field) in schema.fields.iter().enumerate() {
        if field.kind == FieldKind::Json {
            continue;
        }
        let mut coerced = Vec::new();
        for value in extract(source, &field.source_path) {
            match coerce(&field.kind, &value) {
                Ok(Some(v)) => coerced.push(v),
                Ok(None) => {}
                Err(_) if field.ignore_malformed => {}
                Err(message) => findings.violation(&field.name, message),
            }
        }
        if !coerced.is_empty() {
            values.push((index, coerced));
        }
    }
    let unmapped = unmapped_paths(schema, source);
    match schema.dynamic {
        DynamicMapping::Strict => {
            for path in unmapped {
                let message = format!("strict dynamic mapping: {path} is not mapped");
                findings.violation(path, message);
            }
        }
        DynamicMapping::Ignore => {}
        DynamicMapping::Map => findings.required.extend(
            unmapped
                .into_iter()
                .filter(|path| dynamic::is_mappable(schema, path)),
        ),
    }
    ExtractedDoc { values }
}

/// Checks dense vectors: known names, the right dimension, finite values.
fn check_vectors<'v>(
    schema: &CollectionSchema,
    vectors: impl IntoIterator<Item = (&'v String, &'v Vec<f32>)>,
    findings: &mut Findings,
) {
    for (name, vector) in vectors {
        let field = format!("vectors.{name}");
        let Some((_, spec)) = schema.vector(name) else {
            findings.violation(field, "unknown vector");
            continue;
        };
        if vector.len() != spec.dim as usize {
            let message = format!("expected {} dimensions, got {}", spec.dim, vector.len());
            findings.violation(field, message);
        } else if vector.iter().any(|x| !x.is_finite()) {
            findings.violation(field, "every value must be finite");
        }
    }
}

/// Checks sparse vector names (the vectors themselves are canonical by
/// construction, and an empty one is valid).
fn check_sparse_vectors<'v>(
    schema: &CollectionSchema,
    names: impl IntoIterator<Item = &'v String>,
    findings: &mut Findings,
) {
    for name in names {
        if schema.sparse_vector(name).is_none() {
            findings.violation(format!("sparse_vectors.{name}"), "unknown sparse vector");
        }
    }
}

fn check_document_into(
    schema: &CollectionSchema,
    doc: &Document,
    findings: &mut Findings,
) -> ExtractedDoc {
    let extracted = check_source(schema, &doc.source, findings);
    check_vectors(schema, &doc.vectors, findings);
    check_sparse_vectors(schema, doc.sparse_vectors.keys(), findings);
    extracted
}

/// Validates a whole document against `schema`: every non-Json field's
/// values coerce (or are skipped with `ignore_malformed`), unmapped paths
/// follow `schema.dynamic`, and every dense and sparse vector is declared,
/// with a dense vector's dimension and finite values. A missing field or
/// vector is fine. Returns the typed values.
pub fn check_document(
    schema: &CollectionSchema,
    doc: &Document,
) -> Result<ExtractedDoc, DocRejection> {
    let mut findings = Findings::default();
    let extracted = check_document_into(schema, doc, &mut findings);
    findings.into_result(extracted)
}

/// The first `delete_keys` entry that is not a path (empty, or with an
/// empty segment), as a violation of `delete_keys`.
pub(crate) fn invalid_delete_key(delete_keys: &[String]) -> Option<Violation> {
    delete_keys
        .iter()
        .find(|key| key.split('.').any(str::is_empty))
        .map(|key| Violation {
            field: "delete_keys".to_string(),
            message: format!("{key:?} is not a dot-separated path"),
        })
}

/// Validates what a patch brings: its source as a partial document (its own
/// paths only), its vectors and sparse vectors to set (deletions need no
/// check), its `delete_keys` (each a path), and its upsert document in full.
/// An upsert is checked like [`check_document`]; a delete needs nothing.
pub fn check_patch(schema: &CollectionSchema, patch: &DocOp) -> Result<(), DocRejection> {
    let mut findings = Findings::default();
    match patch {
        DocOp::Upsert(doc) => {
            check_document_into(schema, doc, &mut findings);
        }
        DocOp::Delete(_) => {}
        DocOp::Patch {
            source,
            delete_keys,
            vectors,
            sparse_vectors,
            upsert,
            ..
        } => {
            check_source(schema, source, &mut findings);
            check_vectors(schema, set_entries(vectors), &mut findings);
            check_sparse_vectors(
                schema,
                set_entries::<SparseVector>(sparse_vectors).map(|(name, _)| name),
                &mut findings,
            );
            findings.violations.extend(invalid_delete_key(delete_keys));
            if let Some(doc) = upsert {
                check_document_into(schema, doc, &mut findings);
            }
        }
    }
    findings.into_result(())
}

/// The entries a patch sets (`Some`), skipping the ones it deletes.
fn set_entries<T>(changes: &BTreeMap<String, Option<T>>) -> impl Iterator<Item = (&String, &T)> {
    changes
        .iter()
        .filter_map(|(name, change)| Some((name, change.as_ref()?)))
}
