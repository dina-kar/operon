//! Projection of fetched documents (plan M1.2 Task 7 rules 1.8 and 10):
//! ES `_source` filtering, typed field values, and the projected
//! [`StoredDoc`] of get and scroll.

use std::collections::BTreeMap;

use operon_collection::{CollectionSchema, FieldKind, IndexValue, coerce, extract};
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow};
use crate::ir::FieldValue;
use crate::json::pk::format_uuid;
use crate::text::fields::{ResolvedField, resolve_field};
use crate::types::{Projection, SourceFilter, StoredDoc};

/// Whether glob `pattern` (`*` matches any run of characters, dots
/// included) matches all of `text`.
fn glob(pattern: &[u8], text: &[u8]) -> bool {
    // Greedy matching with backtracking to the last `*`.
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && pattern[p] == b'*' {
            star = Some((p, t));
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if let Some((sp, st)) = star {
            p = sp + 1;
            t = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|c| *c == b'*')
}

/// Whether some pattern matches `path` or one of its dotted prefixes.
fn matches_path(patterns: &[String], path: &str) -> bool {
    let prefixes = path
        .match_indices('.')
        .map(|(i, _)| &path[..i])
        .chain(std::iter::once(path));
    let prefixes: Vec<&str> = prefixes.collect();
    patterns.iter().any(|pattern| {
        prefixes
            .iter()
            .any(|prefix| glob(pattern.as_bytes(), prefix.as_bytes()))
    })
}

struct PathFilter<'a> {
    include: &'a [String],
    exclude: &'a [String],
}

impl PathFilter<'_> {
    /// A leaf at `path` is kept.
    fn keeps(&self, path: &str) -> bool {
        (self.include.is_empty() || matches_path(self.include, path))
            && !matches_path(self.exclude, path)
    }

    fn object(&self, object: &Map<String, Value>, prefix: &str) -> Map<String, Value> {
        let mut out = Map::new();
        for (key, value) in object {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            if let Some(value) = self.value(value, &path) {
                out.insert(key.clone(), value);
            }
        }
        out
    }

    /// `value` at `path` filtered; `None` when nothing of it is kept.
    fn value(&self, value: &Value, path: &str) -> Option<Value> {
        match value {
            Value::Object(object) if !object.is_empty() => {
                let kept = self.object(object, path);
                (!kept.is_empty()).then_some(Value::Object(kept))
            }
            // Arrays are transparent: each element at the same path.
            Value::Array(items) if !items.is_empty() => {
                let kept: Vec<Value> = items
                    .iter()
                    .filter_map(|item| self.value(item, path))
                    .collect();
                (!kept.is_empty()).then_some(Value::Array(kept))
            }
            _ => self.keeps(path).then(|| value.clone()),
        }
    }
}

/// `source` through `filter` (rule 10): `All` → the source; `None` →
/// `None`; `Paths` → ES `_source` include/exclude semantics, with objects
/// and arrays that the filter leaves empty removed.
pub fn filter_source(
    source: &Map<String, Value>,
    filter: &SourceFilter,
) -> Option<Map<String, Value>> {
    match filter {
        SourceFilter::All => Some(source.clone()),
        SourceFilter::None => None,
        SourceFilter::Paths { include, exclude } => {
            Some(PathFilter { include, exclude }.object(source, ""))
        }
    }
}

/// A typed value as a field value: dates in µs, UUIDs as their text.
pub(crate) fn index_value(value: IndexValue) -> FieldValue {
    match value {
        IndexValue::Text(s) | IndexValue::Keyword(s) => FieldValue::Str(s),
        IndexValue::I64(n) => FieldValue::I64(n),
        IndexValue::F64(x) => FieldValue::F64(x),
        IndexValue::Bool(b) => FieldValue::Bool(b),
        IndexValue::Date(ms) => FieldValue::Date(ms.saturating_mul(1_000)),
        IndexValue::Uuid(bytes) => FieldValue::Str(format_uuid(&bytes)),
    }
}

/// A JSON scalar as a field value (objects, arrays and nulls give none).
fn json_scalar(value: &Value) -> Option<FieldValue> {
    match value {
        Value::String(s) => Some(FieldValue::Str(s.clone())),
        Value::Bool(b) => Some(FieldValue::Bool(*b)),
        Value::Number(n) => n
            .as_i64()
            .map(FieldValue::I64)
            .or_else(|| n.as_u64().map(FieldValue::U64))
            .or_else(|| n.as_f64().map(FieldValue::F64)),
        _ => None,
    }
}

/// The source path of a JSON path under Json field `spec`.
pub(crate) fn json_source_path(source_path: &str, path: &str) -> String {
    if source_path.is_empty() {
        path.to_string()
    } else {
        format!("{source_path}.{path}")
    }
}

/// The values of each field in `fields` (rule 1.8): a typed field's values
/// at its `source_path` through `extract` and `coerce` (malformed values
/// skipped), a JSON path's scalars. Unknown fields and fields without a
/// value are left out.
pub fn field_values(
    schema: &CollectionSchema,
    source: &Map<String, Value>,
    fields: &[String],
) -> BTreeMap<String, Vec<FieldValue>> {
    let mut out = BTreeMap::new();
    for name in fields {
        let values: Vec<FieldValue> = match resolve_field(schema, name) {
            ResolvedField::Plain { spec } if spec.kind == FieldKind::Json => {
                extract(source, &spec.source_path)
                    .iter()
                    .filter_map(|value| json_scalar(value.as_ref()))
                    .collect()
            }
            ResolvedField::Plain { spec } => extract(source, &spec.source_path)
                .iter()
                .filter_map(|value| coerce(&spec.kind, value.as_ref()).ok().flatten())
                .map(index_value)
                .collect(),
            ResolvedField::JsonPath { spec, path } => {
                extract(source, &json_source_path(&spec.source_path, &path))
                    .iter()
                    .filter_map(|value| json_scalar(value.as_ref()))
                    .collect()
            }
            ResolvedField::Unknown => Vec::new(),
        };
        if !values.is_empty() {
            out.insert(name.clone(), values);
        }
    }
    out
}

/// The fetch columns of a projection (overview A30): the source when it,
/// its fields or `highlight` need it; each name of `select.vectors`
/// resolved against the dense, then the sparse vectors. A name that is
/// neither is `InvalidArgument`.
pub(crate) fn fetch_columns(
    schema: &CollectionSchema,
    select: &Projection,
    highlight: bool,
) -> Result<FetchColumns, ServiceError> {
    let mut columns = FetchColumns {
        source: select.source != SourceFilter::None || !select.fields.is_empty() || highlight,
        vectors: Vec::new(),
        sparse: Vec::new(),
    };
    for name in &select.vectors {
        if let Some(i) = schema.vectors.iter().position(|v| &v.name == name) {
            if !columns.vectors.contains(&i) {
                columns.vectors.push(i);
            }
        } else if let Some(i) = schema.sparse_vectors.iter().position(|v| &v.name == name) {
            if !columns.sparse.contains(&i) {
                columns.sparse.push(i);
            }
        } else {
            return Err(ServiceError::InvalidArgument(format!(
                "unknown vector {name}"
            )));
        }
    }
    Ok(columns)
}

/// A fetched row as the projected [`StoredDoc`] of get and scroll.
pub(crate) fn stored_doc(
    schema: &CollectionSchema,
    row: FetchedRow,
    select: &Projection,
) -> StoredDoc {
    let fields = match &row.source {
        Some(source) => field_values(schema, source, &select.fields),
        None => BTreeMap::new(),
    };
    StoredDoc {
        pk: row.pk,
        source: row
            .source
            .as_ref()
            .and_then(|source| filter_source(source, &select.source)),
        vectors: row.vectors,
        sparse_vectors: row.sparse_vectors,
        fields,
        seq_no: row.seq_no,
        partition: row.partition,
    }
}
