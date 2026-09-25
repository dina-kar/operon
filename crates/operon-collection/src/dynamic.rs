//! Elasticsearch dynamic mapping as a pure function (overview R17, plan
//! M1.1 Task 6): the fields to add so that every unmapped path is mapped.

use std::collections::{BTreeMap, BTreeSet};

use operon_common::schema::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec};
use serde_json::{Map, Value};

use crate::values::{for_each_leaf, is_covered, is_epoch_millis, parse_date_str};

/// Why no mapping could be proposed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DynamicMappingError {
    #[error("mapping would exceed the field limit of {limit}")]
    TooManyFields { limit: u32 },
}

/// The field ES 8 would map for `path`, as `kind`: indexed, not
/// `ignore_malformed`, and fast unless it is text.
fn proposed_field(path: &str, name: String, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name,
        source_path: path.to_string(),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    }
}

/// Whether `spec` is valid on its own under [`CollectionSchema::validate`]
/// (the name and source path rules).
fn valid_alone(spec: &FieldSpec) -> bool {
    CollectionSchema::new(vec![spec.clone()], vec![], DynamicMapping::Strict)
        .validate()
        .is_ok()
}

/// Whether dynamic mapping would map the unmapped `path`: a field named
/// `path` could be added to `schema`. Other paths stay in `_source` only.
pub(crate) fn is_mappable(schema: &CollectionSchema, path: &str) -> bool {
    schema.field(path).is_none()
        && valid_alone(&proposed_field(path, path.to_string(), FieldKind::Keyword))
}

/// The kind ES 8 maps a first value to: a date string (not digits only) is a
/// date, another string text; an integer i64; another number f64; a bool
/// bool.
fn dynamic_kind(value: &Value) -> Option<FieldKind> {
    Some(match value {
        Value::String(s) if !is_epoch_millis(s) && parse_date_str(s).is_some() => FieldKind::Date,
        Value::String(_) => FieldKind::Text {
            analyzer: "standard".to_string(),
            positions: true,
        },
        Value::Number(n) if n.is_i64() || n.is_u64() => FieldKind::I64,
        Value::Number(_) => FieldKind::F64,
        Value::Bool(_) => FieldKind::Bool,
        Value::Null | Value::Array(_) | Value::Object(_) => return None,
    })
}

/// ES dynamic mapping: the fields to add so that every unmapped path of
/// `sources` is mapped. Pure; the gateway proposes `UpdateCollectionSchema`
/// with them (R17).
///
/// The sources are walked in order, and each one's unmapped leaf paths in
/// sorted order; the first non-null value seen for a path decides its kind,
/// with ES 8's defaults: a string that parses as a date in a non-digit
/// format is a date, an integer i64, another number f64, a bool bool. Any
/// other string gives a `standard` text field named `<path>` plus a keyword
/// field `<path>.keyword` with the same source path, unless that name is
/// taken. Every field is indexed, and fast unless it is text. A
/// path whose field would be invalid (a name starting with `_`, containing
/// `..`, and so on) is not mapped and stays in `_source`. The output is
/// deterministic.
///
/// Fails with [`DynamicMappingError::TooManyFields`] if the schema's fields,
/// vectors and sparse vectors plus the proposal exceed `max_fields` (the
/// count [`CollectionSchema::validate`] checks).
pub fn propose_dynamic_fields(
    schema: &CollectionSchema,
    sources: &[&Map<String, Value>],
) -> Result<Vec<FieldSpec>, DynamicMappingError> {
    let mut taken: BTreeSet<String> = schema.fields.iter().map(|f| f.name.clone()).collect();
    let mut decided: BTreeSet<String> = BTreeSet::new();
    let mut proposed: Vec<FieldSpec> = Vec::new();
    for source in sources {
        // Each unmapped path of this source with its first non-null value.
        let mut firsts: BTreeMap<String, &Value> = BTreeMap::new();
        for_each_leaf(source, &mut |path, value| {
            if !firsts.contains_key(path) && !decided.contains(path) && !is_covered(schema, path) {
                firsts.insert(path.to_string(), value);
            }
        });
        for (path, value) in firsts {
            let Some(kind) = dynamic_kind(value) else {
                continue;
            };
            let is_text = matches!(kind, FieldKind::Text { .. });
            let spec = proposed_field(&path, path.clone(), kind);
            if !taken.contains(&spec.name) && valid_alone(&spec) {
                taken.insert(spec.name.clone());
                proposed.push(spec);
                let keyword = format!("{path}.keyword");
                if is_text && !taken.contains(&keyword) {
                    let keyword = proposed_field(&path, keyword, FieldKind::Keyword);
                    if valid_alone(&keyword) {
                        taken.insert(keyword.name.clone());
                        proposed.push(keyword);
                    }
                }
            }
            decided.insert(path);
        }
    }
    let total =
        schema.fields.len() + schema.vectors.len() + schema.sparse_vectors.len() + proposed.len();
    if total > schema.max_fields as usize {
        return Err(DynamicMappingError::TooManyFields {
            limit: schema.max_fields,
        });
    }
    Ok(proposed)
}
