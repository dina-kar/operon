//! ES documents ⇄ collection documents (plan M1.5 Task 4 steps 2, 4 and 9,
//! Task 6): id rules, `dense_vector` values moved out of `_source` into
//! `Document.vectors` and put back on read (Ruling 2, overview A9), ES's
//! partial-document merge, and `_source` filtering.

use std::collections::BTreeMap;

use loams_collection::{Document, PrimaryKey};
use serde_json::{Map, Number, Value, json};

use crate::dsl::java_float;
use crate::error::EsError;
use crate::http::Params;
use crate::mapping::{EsSimilarity, IndexView};

/// The longest `_id`, in UTF-8 bytes.
pub const MAX_ID_BYTES: usize = 512;

/// The metadata fields a document's top level must not hold.
pub const METADATA_FIELDS: &[&str] = &[
    "_id",
    "_index",
    "_source",
    "_routing",
    "_ignored",
    "_seq_no",
    "_version",
    "_primary_term",
    "_field_names",
    "_doc_count",
    "_tier",
    "_data_stream_timestamp",
    "_nested_path",
    "_feature",
];

/// The longest value preview in a parse error.
const PREVIEW_CHARS: usize = 64;

/// ES's id rules (step 2).
pub fn validate_id(id: &str) -> Result<(), EsError> {
    if id.is_empty() {
        return Err(EsError::illegal_argument(
            "if _id is specified it must not be empty",
        ));
    }
    if id.len() > MAX_ID_BYTES {
        // ES's `IndexRequest.validate` names the whole id (row T11-3).
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            format!(
                "Validation Failed: 1: id [{id}] is too long, must be no longer than \
                 {MAX_ID_BYTES} bytes but was: {};",
                id.len()
            ),
        ));
    }
    Ok(())
}

/// The `_id` of a primary key: a string as it is, a u64 in decimal, a uuid
/// hyphenated in lowercase.
pub fn id_of(pk: &PrimaryKey) -> String {
    match pk {
        PrimaryKey::Str(s) => s.clone(),
        PrimaryKey::U64(n) => n.to_string(),
        PrimaryKey::Uuid(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!(
                "{}-{}-{}-{}-{}",
                &hex[..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..]
            )
        }
    }
}

/// `v` printed with the shortest digits that give back the same `f32`
/// (Ruling 2): `0.1`, not `0.10000000149011612`. A non-finite value is
/// `null`.
pub fn f32_json(v: f32) -> Value {
    format!("{v}")
        .parse::<f64>()
        .ok()
        .and_then(Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

/// ES's partial-document merge (step 9): where both values are objects they
/// merge recursively; anything else replaces, `null` and arrays included.
/// The same as `PatchMode::MergeDeep` (row E8).
pub fn merge_deep(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, value) in patch {
        match (target.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(nested)) => merge_deep(existing, nested),
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Puts each vector back at its path in `source` (Ruling 2): a path whose
/// key exists keeps its place, a new key is appended last in its parent.
pub fn restore_vectors(source: &mut Map<String, Value>, vectors: &BTreeMap<String, Vec<f32>>) {
    for (path, vector) in vectors {
        let value = Value::Array(vector.iter().map(|&x| f32_json(x)).collect());
        insert_path(source, path, value);
    }
}

/// Inserts `value` at the dot path `path`: a dotted key that exists is
/// replaced, else objects are followed (or made) segment by segment.
fn insert_path(map: &mut Map<String, Value>, path: &str, value: Value) {
    if map.contains_key(path) {
        map.insert(path.to_string(), value);
        return;
    }
    match path.split_once('.') {
        Some((head, rest)) => {
            let entry = map
                .entry(head.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if !entry.is_object() {
                *entry = Value::Object(Map::new());
            }
            if let Value::Object(inner) = entry {
                insert_path(inner, rest, value);
            }
        }
        None => {
            map.insert(path.to_string(), value);
        }
    }
}

/// Takes the value at the dot path `path` out of `map`: a dotted key, or
/// nested objects.
fn take_path(map: &mut Map<String, Value>, path: &str) -> Option<Value> {
    if let Some(value) = map.shift_remove(path) {
        return Some(value);
    }
    let mut split = path.len();
    while let Some(at) = path[..split].rfind('.') {
        let (head, rest) = (&path[..at], &path[at + 1..]);
        if let Some(Value::Object(inner)) = map.get_mut(head)
            && let Some(value) = take_path(inner, rest)
        {
            return Some(value);
        }
        split = at;
    }
    None
}

/// Whether the dot path `path` holds `null` in `map`.
fn is_null_at(map: &Map<String, Value>, path: &str) -> bool {
    if let Some(value) = map.get(path) {
        return value.is_null();
    }
    let mut split = path.len();
    while let Some(at) = path[..split].rfind('.') {
        if let Some(Value::Object(inner)) = map.get(&path[..at])
            && is_null_at(inner, &path[at + 1..])
        {
            return true;
        }
        split = at;
    }
    false
}

/// A value as a parse error previews it: strings bare, the rest as JSON, at
/// most 64 characters.
pub(crate) fn preview(value: &Value) -> String {
    let text = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    text.chars().take(PREVIEW_CHARS).collect()
}

/// 400 `document_parsing_exception`.
pub(crate) fn parsing_error(reason: impl Into<String>) -> EsError {
    EsError::new(400, "document_parsing_exception", reason)
}

/// "[1:1] failed to parse field [f] of type [t] in document with id 'id'",
/// with the value's preview when there is one.
pub(crate) fn field_error(field: &str, es_type: &str, id: &str, value: Option<&Value>) -> EsError {
    let mut reason = format!(
        "[1:1] failed to parse field [{field}] of type [{es_type}] in document with id '{id}'"
    );
    if let Some(value) = value {
        reason.push_str(&format!(". Preview of field's value: '{}'", preview(value)));
    }
    parsing_error(reason)
}

/// The vector paths of `view`: its named vectors with their facts, and its
/// pending ones (no dims yet) with their declared similarity.
fn vector_paths(view: &IndexView) -> Vec<(String, Option<u32>, Option<EsSimilarity>)> {
    let mut paths: Vec<_> = view
        .es
        .vectors
        .iter()
        .map(|(path, v)| (path.clone(), Some(v.dim), Some(v.similarity)))
        .collect();
    for (path, declared) in &view.es.pending_vectors {
        let similarity = match declared.get("similarity").and_then(Value::as_str) {
            Some(name) => EsSimilarity::parse(name),
            // Walk's default: cosine when indexed.
            None => {
                let indexed = declared
                    .get("index")
                    .is_none_or(|v| v != &Value::Bool(false) && v != &json!("false"));
                indexed.then_some(EsSimilarity::Cosine)
            }
        };
        paths.push((path.clone(), None, similarity));
    }
    paths
}

/// ES's error for a vector of the wrong length (row T11-3).
pub(crate) fn dims_error(path: &str, id: &str, found: usize, dims: usize) -> EsError {
    let why = format!(
        "The [dense_vector] field [{path}] in doc [document with id '{id}'] has a different \
         number of dimensions [{found}] than defined in the mapping [{dims}]"
    );
    parsing_error(format!("[1:1] failed to parse: {why}")).with(
        "caused_by",
        json!({"type": "illegal_argument_exception", "reason": why}),
    )
}

/// Checks one `dense_vector` value (step 4) and returns it as `f32`s.
fn parse_vector(
    path: &str,
    id: &str,
    value: &Value,
    dim: Option<u32>,
    similarity: Option<EsSimilarity>,
) -> Result<Vec<f32>, EsError> {
    let malformed = || field_error(path, "dense_vector", id, Some(value));
    let Value::Array(items) = value else {
        return Err(malformed());
    };
    let mut vector = Vec::with_capacity(items.len());
    for item in items {
        let x = item.as_f64().ok_or_else(malformed)? as f32;
        if !x.is_finite() {
            return Err(malformed());
        }
        vector.push(x);
    }
    if let Some(dim) = dim
        && vector.len() != dim as usize
    {
        return Err(dims_error(path, id, vector.len(), dim as usize));
    }
    let norm2: f64 = vector.iter().map(|&x| f64::from(x) * f64::from(x)).sum();
    // ES's texts (row T11-3): the reason with a preview of the vector, as
    // the `caused_by` of a "failed to parse".
    let refused = |why: &str| {
        let preview: Vec<String> = vector.iter().map(|x| java_float(*x)).collect();
        let why = format!("{why} Preview of invalid vector: [{}]", preview.join(", "));
        parsing_error(format!("[1:1] failed to parse: {why}")).with(
            "caused_by",
            json!({"type": "illegal_argument_exception", "reason": why}),
        )
    };
    match similarity {
        Some(EsSimilarity::DotProduct) if (norm2 - 1.0).abs() > 1e-4 => Err(refused(
            "The [dot_product] similarity can only be used with unit-length vectors.",
        )),
        Some(EsSimilarity::Cosine) if norm2 == 0.0 => Err(refused(
            "The [cosine] similarity does not support vectors with zero magnitude.",
        )),
        _ => Ok(vector),
    }
}

/// Moves every vector value of `source` into a map (step 4): an array of
/// finite numbers is taken out of the source; `null` stays in the source
/// as `null` and gives `None` (a partial update removes the vector with
/// it, row T4-3).
pub(crate) fn split_vectors(
    view: &IndexView,
    id: &str,
    source: &mut Map<String, Value>,
) -> Result<BTreeMap<String, Option<Vec<f32>>>, EsError> {
    let mut vectors = BTreeMap::new();
    for (path, dim, similarity) in vector_paths(view) {
        if is_null_at(source, &path) {
            vectors.insert(path, None);
            continue;
        }
        let Some(value) = take_path(source, &path) else {
            continue;
        };
        let vector = parse_vector(&path, id, &value, dim, similarity)?;
        vectors.insert(path, Some(vector));
    }
    Ok(vectors)
}

/// Checks a source's top level for metadata fields (step 4).
pub(crate) fn check_source(id: &str, source: &Map<String, Value>) -> Result<(), EsError> {
    if let Some((key, value)) = source
        .iter()
        .find(|(key, _)| METADATA_FIELDS.contains(&key.as_str()))
    {
        let inner = parsing_error(format!(
            "[1:1] Field [{key}] is a metadata field and cannot be added inside a document. Use \
             the index API request parameters."
        ));
        // The inner error is ES's root cause (row T11-3).
        let mut error =
            field_error(key, key, id, Some(value)).with("caused_by", inner.cause_value());
        error.root_cause = Some(Box::new(inner));
        return Err(error);
    }
    // A `binary` value is not checked: ES 8.19 indexes any value into a
    // `binary` field without doc values (row T11-3).
    Ok(())
}

/// The document of an ES `_source` (step 4): the source must be an object
/// without metadata fields, its vector values are moved into
/// `Document.vectors`.
pub fn to_document(
    view: &IndexView,
    id: &str,
    source: Map<String, Value>,
) -> Result<Document, EsError> {
    let mut source = source;
    check_source(id, &source)?;
    let vectors = split_vectors(view, id, &mut source)?
        .into_iter()
        .filter_map(|(path, vector)| vector.map(|v| (path, v)))
        .collect();
    Ok(Document {
        pk: PrimaryKey::Str(id.to_string()),
        source,
        vectors,
        sparse_vectors: BTreeMap::new(),
    })
}

/// A source that is not a JSON object (step 4).
pub(crate) fn not_an_object() -> EsError {
    parsing_error("[1:1] failed to parse: source is not an object")
}

// ----- `_source` filtering (Task 6) -----

/// A request's `_source` filter (Task 6 rule 1, ES `FetchSourceContext`):
/// `enabled == false` returns no `_source`; otherwise `includes` (empty:
/// everything) and `excludes` are glob patterns over dot paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFilter {
    pub enabled: bool,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
}

impl Default for SourceFilter {
    /// The whole source.
    fn default() -> Self {
        Self {
            enabled: true,
            includes: Vec::new(),
            excludes: Vec::new(),
        }
    }
}

impl SourceFilter {
    /// `_source`, `_source_includes` and `_source_excludes` (rule 2):
    /// `_source` is `true`, `false` or a comma list of includes, which
    /// `_source_includes` replaces; `_source=false` wins. `None` when none
    /// is given.
    pub fn from_params(p: &Params) -> Result<Option<SourceFilter>, EsError> {
        let source = p.str("_source");
        let mut includes = None;
        let enabled = match source {
            None => None,
            Some("true") => Some(true),
            Some("false") => Some(false),
            Some(_) => {
                includes = p.list("_source");
                None
            }
        };
        if let Some(list) = p.list("_source_includes") {
            includes = Some(list);
        }
        let excludes = p.list("_source_excludes");
        if source.is_none() && includes.is_none() && excludes.is_none() {
            return Ok(None);
        }
        Ok(Some(SourceFilter {
            enabled: enabled.unwrap_or(true),
            includes: includes.unwrap_or_default(),
            excludes: excludes.unwrap_or_default(),
        }))
    }

    /// A `_source` in a body: `true`, `false`, a pattern, a list of
    /// patterns, or `{"includes"|"include", "excludes"|"exclude"}` with a
    /// pattern or a list each.
    pub fn from_body(v: &Value) -> Result<SourceFilter, EsError> {
        let unknown = |key: &str, value: &Value| {
            EsError::parsing(format!(
                "Unknown key for a {} in [{key}].",
                token_name(value)
            ))
        };
        let patterns = |key: &str, value: &Value| -> Result<Vec<String>, EsError> {
            match value {
                Value::String(s) => Ok(vec![s.clone()]),
                Value::Array(items) => items
                    .iter()
                    .map(|item| match item {
                        Value::String(s) => Ok(s.clone()),
                        other => Err(unknown(key, other)),
                    })
                    .collect(),
                other => Err(unknown(key, other)),
            }
        };
        match v {
            Value::Bool(enabled) => Ok(SourceFilter {
                enabled: *enabled,
                ..SourceFilter::default()
            }),
            Value::String(_) | Value::Array(_) => Ok(SourceFilter {
                includes: patterns("_source", v)?,
                ..SourceFilter::default()
            }),
            Value::Object(map) => {
                let mut filter = SourceFilter::default();
                for (key, value) in map {
                    match key.as_str() {
                        "includes" | "include" => filter.includes = patterns(key, value)?,
                        "excludes" | "exclude" => filter.excludes = patterns(key, value)?,
                        _ => return Err(unknown(key, value)),
                    }
                }
                Ok(filter)
            }
            other => Err(EsError::parsing(format!(
                "Expected one of [VALUE_BOOLEAN, VALUE_STRING, START_ARRAY, START_OBJECT] but \
                 found [{}]",
                token_name(other)
            ))),
        }
    }

    /// Whether a leaf value at `path` survives [`SourceFilter::apply`]:
    /// what decides which vectors a read fetches (rule 3).
    pub fn keeps_path(&self, path: &str) -> bool {
        self.enabled
            && (self.includes.is_empty() || accepts(&self.includes, path))
            && !accepts(&self.excludes, path)
    }

    /// `source` filtered as ES 8.19 filters it (rule 1): an object or array
    /// the filter empties is dropped, one that was empty already is kept
    /// when its path is included (checked against the oracle, row T11-3).
    /// The caller leaves `_source` out when `enabled` is false.
    pub fn apply(&self, source: Map<String, Value>) -> Map<String, Value> {
        if self.includes.is_empty() && self.excludes.is_empty() {
            return source;
        }
        let include = (!self.includes.is_empty()).then_some(self.includes.as_slice());
        filter_map(&source, "", include, &self.excludes)
    }
}

/// ES's name of a JSON value's first token.
fn token_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "VALUE_NULL",
        Value::Bool(_) => "VALUE_BOOLEAN",
        Value::Number(_) => "VALUE_NUMBER",
        Value::String(_) => "VALUE_STRING",
        Value::Array(_) => "START_ARRAY",
        Value::Object(_) => "START_OBJECT",
    }
}

/// The positions an NFA for `pattern` can be at after reading `text` (`*`
/// matches any run of bytes, dots included); all false once it is dead.
fn glob_states(pattern: &[u8], text: &[u8]) -> Vec<bool> {
    let n = pattern.len();
    let close = |states: &mut [bool]| {
        for p in 0..n {
            if states[p] && pattern[p] == b'*' {
                states[p + 1] = true;
            }
        }
    };
    let mut states = vec![false; n + 1];
    states[0] = true;
    close(&mut states);
    for &c in text {
        let mut next = vec![false; n + 1];
        for p in (0..n).filter(|&p| states[p]) {
            if pattern[p] == b'*' {
                next[p] = true;
            } else if pattern[p] == c {
                next[p + 1] = true;
            }
        }
        close(&mut next);
        states = next;
    }
    states
}

/// Whether `pattern` matches all of `path`; `*` matches any sequence,
/// dots included.
pub fn glob_match(pattern: &str, path: &str) -> bool {
    glob_states(pattern.as_bytes(), path.as_bytes())[pattern.len()]
}

/// Whether some suffix `t` gives `glob_match(pattern, prefix + t)`.
pub fn glob_prefix_alive(pattern: &str, prefix: &str) -> bool {
    glob_states(pattern.as_bytes(), prefix.as_bytes()).contains(&true)
}

/// Whether some pattern matches `path` or one of its ancestors (ES's
/// automaton is built from `P` and `P.*`).
fn accepts(patterns: &[String], path: &str) -> bool {
    patterns.iter().any(|pattern| {
        glob_match(pattern, path)
            || path
                .match_indices('.')
                .any(|(at, _)| glob_match(pattern, &path[..at]))
    })
}

/// Whether some pattern can still match `path` or a path below it.
fn alive(patterns: &[String], path: &str) -> bool {
    accepts(patterns, path)
        || patterns
            .iter()
            .any(|pattern| glob_prefix_alive(pattern, path))
}

/// ES's map filter at `prefix` (empty, or a path ending in `.`). `include`
/// is `None` for "everything": no includes, or an ancestor matched one.
fn filter_map(
    map: &Map<String, Value>,
    prefix: &str,
    include: Option<&[String]>,
    excludes: &[String],
) -> Map<String, Value> {
    let mut out = Map::new();
    for (key, value) in map {
        let path = format!("{prefix}{key}");
        if include.is_some_and(|include| !alive(include, &path)) || accepts(excludes, &path) {
            continue;
        }
        let included = include.is_none_or(|include| accepts(include, &path));
        let inner = format!("{path}.");
        let sub_include = if included {
            // No exclude can match below: the value is kept whole.
            if !excludes.iter().any(|e| glob_prefix_alive(e, &inner)) {
                out.insert(key.clone(), value.clone());
                continue;
            }
            None
        } else {
            include
        };
        match value {
            Value::Object(object) => {
                if sub_include.is_some_and(|sub| !alive(sub, &inner)) {
                    continue;
                }
                let kept = filter_map(object, &inner, sub_include, excludes);
                if !kept.is_empty() || (included && object.is_empty()) {
                    out.insert(key.clone(), Value::Object(kept));
                }
            }
            Value::Array(items) => {
                let kept = filter_array(items, &path, sub_include, excludes, included);
                if !kept.is_empty() || (included && items.is_empty()) {
                    out.insert(key.clone(), Value::Array(kept));
                }
            }
            _ if included => {
                out.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    out
}

/// ES's list filter: objects are filtered at `path.`, nested arrays alike,
/// and scalars are kept iff the array's key is included.
fn filter_array(
    items: &[Value],
    path: &str,
    include: Option<&[String]>,
    excludes: &[String],
    included: bool,
) -> Vec<Value> {
    let inner = format!("{path}.");
    let mut out = Vec::new();
    for item in items {
        match item {
            Value::Object(object) => {
                let kept = filter_map(object, &inner, include, excludes);
                if !kept.is_empty() {
                    out.push(Value::Object(kept));
                }
            }
            Value::Array(nested) => {
                let kept = filter_array(nested, path, include, excludes, included);
                if !kept.is_empty() {
                    out.push(Value::Array(kept));
                }
            }
            scalar if included => out.push(scalar.clone()),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_follow_es_rules() {
        assert_eq!(
            validate_id("").expect_err("empty").reason,
            "if _id is specified it must not be empty"
        );
        assert!(validate_id(&"a".repeat(512)).is_ok());
        let e = validate_id(&"a".repeat(513)).expect_err("long");
        assert_eq!(e.kind, "action_request_validation_exception");
        assert_eq!(
            e.reason,
            format!(
                "Validation Failed: 1: id [{}] is too long, must be no longer than 512 bytes but \
                 was: 513;",
                "a".repeat(513)
            )
        );
    }

    #[test]
    fn primary_keys_print_as_ids() {
        assert_eq!(id_of(&PrimaryKey::Str("a/b".into())), "a/b");
        assert_eq!(id_of(&PrimaryKey::U64(42)), "42");
        let mut bytes = [0u8; 16];
        bytes[15] = 0xab;
        bytes[0] = 0x12;
        assert_eq!(
            id_of(&PrimaryKey::Uuid(bytes)),
            "12000000-0000-0000-0000-0000000000ab"
        );
    }

    #[test]
    fn floats_print_with_the_shortest_f32_digits() {
        assert_eq!(f32_json(0.1).to_string(), "0.1");
        assert_eq!(f32_json(0.3).to_string(), "0.3");
        assert_eq!(f32_json(-1.5).to_string(), "-1.5");
        assert_eq!(f32_json(f32::NAN), Value::Null);
    }

    #[test]
    fn merge_deep_merges_objects_and_replaces_the_rest() {
        let mut target = json!({"a": {"x": 1, "y": 2}, "t": [1, 2], "k": 1})
            .as_object()
            .cloned()
            .unwrap_or_default();
        let patch = json!({"a": {"y": 3}, "t": [9], "n": null, "k": {"z": 1}});
        merge_deep(&mut target, patch.as_object().expect("object"));
        assert_eq!(
            Value::Object(target),
            json!({"a": {"x": 1, "y": 3}, "t": [9], "k": {"z": 1}, "n": null})
        );
    }

    #[test]
    fn vectors_are_restored_at_their_paths() {
        let mut source = json!({"text": "x", "emb": null})
            .as_object()
            .cloned()
            .unwrap_or_default();
        let vectors = BTreeMap::from([
            ("emb".to_string(), vec![0.1, 0.2]),
            ("a.v".to_string(), vec![1.0]),
            ("vector".to_string(), vec![0.3]),
        ]);
        restore_vectors(&mut source, &vectors);
        assert_eq!(
            Value::Object(source).to_string(),
            r#"{"text":"x","emb":[0.1,0.2],"a":{"v":[1.0]},"vector":[0.3]}"#
        );
    }

    #[test]
    fn paths_are_taken_from_dotted_keys_and_objects() {
        let mut source = json!({"a": {"b": [1]}, "c.d": [2]})
            .as_object()
            .cloned()
            .unwrap_or_default();
        assert_eq!(take_path(&mut source, "a.b"), Some(json!([1])));
        assert_eq!(take_path(&mut source, "c.d"), Some(json!([2])));
        assert_eq!(take_path(&mut source, "x"), None);
        assert_eq!(Value::Object(source), json!({"a": {}}));
    }
}
