//! `group_by` (plan M1.2 Task 7 rule 5): the ranked candidates grouped by
//! the values of one field, groups ordered by their first hit.

use operon_collection::{CollectionSchema, FieldKind, coerce, extract};
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow, fetch_rows};
use crate::exec::project::{index_value, json_source_path};
use crate::exec::schema::Ranked;
use crate::ir::{FieldValue, GroupBy};
use crate::read::ReadView;
use crate::text::fields::{ResolvedField, resolve_field};

/// Candidates whose sources are fetched at a time.
const PAGE: usize = 256;

/// A JSON scalar group key: strings, integers and bools only.
fn json_key(value: &Value) -> Option<FieldValue> {
    match value {
        Value::String(s) => Some(FieldValue::Str(s.clone())),
        Value::Bool(b) => Some(FieldValue::Bool(*b)),
        Value::Number(n) => n
            .as_i64()
            .map(FieldValue::I64)
            .or_else(|| n.as_u64().map(FieldValue::U64)),
        _ => None,
    }
}

/// How the group keys of a source are read.
enum KeyPath<'a> {
    Typed { path: &'a str, kind: &'a FieldKind },
    Json(String),
}

impl KeyPath<'_> {
    /// The distinct keys of `source`, first occurrence first.
    fn keys(&self, source: &Map<String, Value>) -> Vec<FieldValue> {
        let values: Vec<FieldValue> = match self {
            KeyPath::Typed { path, kind } => extract(source, path)
                .iter()
                .filter_map(|value| coerce(kind, value.as_ref()).ok().flatten())
                .map(index_value)
                .collect(),
            KeyPath::Json(path) => extract(source, path)
                .iter()
                .filter_map(|value| json_key(value.as_ref()))
                .collect(),
        };
        let mut out: Vec<FieldValue> = Vec::with_capacity(values.len());
        for value in values {
            if !out.contains(&value) {
                out.push(value);
            }
        }
        out
    }
}

fn key_path<'a>(schema: &'a CollectionSchema, field: &str) -> Result<KeyPath<'a>, ServiceError> {
    match resolve_field(schema, field) {
        ResolvedField::Plain { spec } if spec.kind == FieldKind::Json => {
            Ok(KeyPath::Json(spec.source_path.clone()))
        }
        ResolvedField::Plain { spec } => Ok(KeyPath::Typed {
            path: &spec.source_path,
            kind: &spec.kind,
        }),
        ResolvedField::JsonPath { spec, path } => {
            Ok(KeyPath::Json(json_source_path(&spec.source_path, &path)))
        }
        ResolvedField::Unknown => Err(ServiceError::InvalidArgument(format!(
            "unknown group_by field {field}"
        ))),
    }
}

/// A group: its key and its hits with their fetched rows.
pub(crate) type Group = (FieldValue, Vec<(Ranked, FetchedRow)>);

/// Rule 5: walks `candidates` in order, fetching their rows (`columns`
/// plus the source) in pages of 256. A hit joins the group of every key it
/// has; a group keeps at most `group_size` hits; at most `limit` groups are
/// made, and the walk stops once all of them are full.
pub(crate) async fn group(
    view: &ReadView,
    candidates: &[Ranked],
    group_by: &GroupBy,
    columns: &FetchColumns,
) -> Result<Vec<Group>, ServiceError> {
    let path = key_path(&view.collection.schema, &group_by.field)?;
    let columns = FetchColumns {
        source: true,
        ..columns.clone()
    };
    let mut groups: Vec<Group> = Vec::new();
    let full = |groups: &[Group]| {
        groups.len() >= group_by.limit
            && groups
                .iter()
                .all(|(_, hits)| hits.len() >= group_by.group_size)
    };
    for page in candidates.chunks(PAGE) {
        let ids: Vec<u64> = page.iter().map(|hit| hit.row_id).collect();
        let rows = fetch_rows(view, &ids, &columns).await?;
        for (hit, row) in page.iter().zip(rows) {
            let keys = row
                .source
                .as_ref()
                .map(|s| path.keys(s))
                .unwrap_or_default();
            for key in keys {
                let at = match groups.iter().position(|(k, _)| *k == key) {
                    Some(at) => at,
                    None if groups.len() < group_by.limit => {
                        groups.push((key, Vec::new()));
                        groups.len() - 1
                    }
                    None => continue,
                };
                let hits = &mut groups[at].1;
                if hits.len() < group_by.group_size {
                    hits.push((hit.clone(), row.clone()));
                }
            }
            if full(&groups) {
                return Ok(groups);
            }
        }
    }
    Ok(groups)
}
