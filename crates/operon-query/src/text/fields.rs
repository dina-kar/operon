//! Field names of a query, resolved against the collection schema (plan
//! M1.2 Task 2 rule 1).

use operon_collection::{CollectionSchema, FieldKind, FieldSpec};

/// What a field name of a query names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedField<'a> {
    /// An exact `FieldSpec` name.
    Plain {
        spec: &'a FieldSpec,
    },
    /// `"<json field name>.<path>"`: the longest Json field name that is a
    /// dot-prefix of the name.
    JsonPath {
        spec: &'a FieldSpec,
        path: String,
    },
    Unknown,
}

/// Resolves `name`: an exact field name is `Plain`; otherwise the longest
/// Json field name `n` with `name` starting `n + "."` gives `JsonPath`;
/// otherwise `Unknown`.
pub fn resolve_field<'a>(schema: &'a CollectionSchema, name: &str) -> ResolvedField<'a> {
    if let Some(spec) = schema.field(name) {
        return ResolvedField::Plain { spec };
    }
    schema
        .fields
        .iter()
        .filter(|spec| matches!(spec.kind, FieldKind::Json))
        .filter_map(|spec| {
            let path = name.strip_prefix(spec.name.as_str())?.strip_prefix('.')?;
            Some((spec, path))
        })
        .max_by_key(|(spec, _)| spec.name.len())
        .map_or(ResolvedField::Unknown, |(spec, path)| {
            ResolvedField::JsonPath {
                spec,
                path: path.to_string(),
            }
        })
}
