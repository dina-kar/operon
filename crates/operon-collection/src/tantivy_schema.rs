//! The Tantivy schema of a collection's splits and the Tantivy document of
//! each row (plan M1.1 Task 8 rules 3–4, Rulings 26–27).
//!
//! Field names equal `FieldSpec.name` (Tantivy's exact-name lookup wins over
//! JSON-path splitting), in `CollectionSchema.fields` order, after `_pk`,
//! `_rowid` and `_field_presence`. A Json field `n` is five Tantivy fields:
//! `n` itself, then `_text.n`, `_date.n`, `_null.n` and `_count.n`. The two
//! fields of each sparse vector come last.
//!
//! How M1.2 evaluates a filter on `"<n>.<path>"` of a Json field `n`:
//! - `Exists`: Tantivy's `ExistsQuery` on `n` with JSON subpaths;
//! - `IsNull`: the term `path` in `_null.n`;
//! - `IsEmpty`: NOT `Exists`;
//! - `ValuesCount`: a range on `_count.n.<path>`, OR-ed with `IsEmpty` when
//!   the range admits 0;
//! - `Match` / `MatchPhrase`: `_text.n`;
//! - a date `Range`: `_date.n`;
//! - `Term`, `Terms`, a numeric `Range`, `Prefix` and `Wildcard`: `n`
//!   (type-strict: a numeric range matches only numeric leaves).
//!
//! And a sparse vector `n` (M1.2's `SparseExec`): the candidates of a query
//! are the union of the postings of its indices in `_sparse.<n>`; `df(i)` is
//! the live postings count of term *i*; `N` is the live postings count of
//! [`SPARSE_PRESENT`]; a candidate's weights come from `_sparse_w.<n>`.

use std::collections::{BTreeMap, BTreeSet};

use operon_common::schema::{CollectionSchema, FieldKind};
use operon_quickwit::shim::PathHasher;
use operon_text::STANDARD;
use serde_json::{Map, Value};
use tantivy::schema::{
    BytesOptions, DateOptions, DateTimePrecision, Field, IndexRecordOption, JsonObjectOptions,
    NumericOptions, OwnedValue, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{DateTime, TantivyDocument};

use crate::doc::{Document, SparseVector};
use crate::error::CollectionError;
use crate::values::{ExtractedDoc, IndexValue, extract, is_epoch_millis, parse_date_str};

/// The primary key: canonical bytes, indexed and fast.
pub const PK_FIELD: &str = "_pk";
/// The Lance row id: u64, fast.
pub const ROWID_FIELD: &str = "_rowid";
/// Which fields a document has: u64 terms, indexed, in the format of the
/// vendored `query_ast/field_presence.rs`.
pub const FIELD_PRESENCE_FIELD: &str = "_field_presence";
/// The `_sparse.<name>` term every non-empty sparse vector has (Ruling 27).
pub const SPARSE_PRESENT: u64 = u64::MAX;

/// Tantivy's name for "do not tokenize".
const RAW: &str = "raw";

/// `"_sparse.<name>"`: one u64 term per index of the sparse vector.
pub fn sparse_postings_field(sparse: &str) -> String {
    format!("_sparse.{sparse}")
}

/// `"_sparse_w.<name>"`: the sparse vector, [`encode_sparse_weights`].
pub fn sparse_weights_field(sparse: &str) -> String {
    format!("_sparse_w.{sparse}")
}

/// A sparse vector's canonical encoding: `u32 LE nnz ‖ nnz × u32 LE indices
/// ‖ nnz × f32 LE values`.
pub fn encode_sparse_weights(v: &SparseVector) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 8 * v.len());
    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
    for index in v.indices() {
        out.extend_from_slice(&index.to_le_bytes());
    }
    for value in v.values() {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

/// Decodes [`encode_sparse_weights`]: a length that does not match `nnz`,
/// indices that are not strictly ascending or a non-finite value is
/// [`CollectionError::Corrupt`].
pub fn decode_sparse_weights(bytes: &[u8]) -> Result<SparseVector, CollectionError> {
    let corrupt = |message: String| CollectionError::Corrupt(format!("sparse weights: {message}"));
    let words: Vec<[u8; 4]> = bytes
        .chunks(4)
        .map(|chunk| chunk.try_into())
        .collect::<Result<_, _>>()
        .map_err(|_| {
            corrupt(format!(
                "{} bytes is not a whole number of words",
                bytes.len()
            ))
        })?;
    let Some((nnz, rest)) = words.split_first() else {
        return Err(corrupt("no length".to_string()));
    };
    let nnz = u32::from_le_bytes(*nnz) as usize;
    if rest.len() != 2 * nnz {
        return Err(corrupt(format!("{} words for {nnz} entries", rest.len())));
    }
    let (indices, values) = rest.split_at(nnz);
    let indices: Vec<u32> = indices.iter().map(|w| u32::from_le_bytes(*w)).collect();
    if indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(corrupt("indices are not strictly ascending".to_string()));
    }
    let values = values.iter().map(|w| f32::from_le_bytes(*w)).collect();
    SparseVector::new(indices, values).map_err(|err| corrupt(err.to_string()))
}

/// `"_text.<name>"`: the string leaves of a Json field, `standard`-analyzed.
pub fn text_companion(json_field: &str) -> String {
    format!("_text.{json_field}")
}

/// `"_date.<name>"`: the string leaves of a Json field that are dates.
pub fn date_companion(json_field: &str) -> String {
    format!("_date.{json_field}")
}

/// `"_null.<name>"`: the paths of a Json field whose value is `null`.
pub fn null_companion(json_field: &str) -> String {
    format!("_null.{json_field}")
}

/// `"_count.<name>"`: per path of a Json field, its number of values.
pub fn count_companion(json_field: &str) -> String {
    format!("_count.{json_field}")
}

/// The Tantivy fields of one collection field.
#[derive(Clone, Debug)]
pub enum FieldMap {
    Plain(Field),
    Json {
        main: Field,
        text: Field,
        date: Field,
        nulls: Field,
        counts: Field,
    },
}

/// The two Tantivy fields of one sparse vector.
#[derive(Clone, Copy, Debug)]
pub struct SparseFieldMap {
    pub postings: Field,
    pub weights: Field,
}

/// A collection's Tantivy schema and where each of its fields went.
#[derive(Clone, Debug)]
pub struct TantivyLayout {
    pub schema: Schema,
    pub pk: Field,
    pub rowid: Field,
    pub presence: Field,
    /// Parallel to `CollectionSchema.fields`.
    pub fields: Vec<FieldMap>,
    /// Parallel to `CollectionSchema.sparse_vectors`.
    pub sparse: Vec<SparseFieldMap>,
}

fn raw_basic() -> TextFieldIndexing {
    TextFieldIndexing::default()
        .set_tokenizer(RAW)
        .set_index_option(IndexRecordOption::Basic)
}

fn numeric(indexed: bool, fast: bool) -> NumericOptions {
    let mut options = NumericOptions::default();
    if indexed {
        options = options.set_indexed();
    }
    if fast {
        options = options.set_fast();
    }
    options
}

/// The Tantivy schema of `schema`'s splits (rule 3).
pub fn tantivy_layout(schema: &CollectionSchema) -> TantivyLayout {
    let mut builder = Schema::builder();
    let pk = builder.add_bytes_field(PK_FIELD, BytesOptions::default().set_indexed().set_fast());
    let rowid = builder.add_u64_field(ROWID_FIELD, NumericOptions::default().set_fast());
    let presence = builder.add_u64_field(
        FIELD_PRESENCE_FIELD,
        NumericOptions::default().set_indexed(),
    );
    let mut fields = Vec::with_capacity(schema.fields.len());
    for spec in &schema.fields {
        let name = spec.name.as_str();
        let map = match &spec.kind {
            FieldKind::Text {
                analyzer,
                positions,
            } => {
                let record = if *positions {
                    IndexRecordOption::WithFreqsAndPositions
                } else {
                    IndexRecordOption::WithFreqs
                };
                let indexing = TextFieldIndexing::default()
                    .set_tokenizer(analyzer)
                    .set_index_option(record)
                    .set_fieldnorms(true);
                let options = TextOptions::default().set_indexing_options(indexing);
                FieldMap::Plain(builder.add_text_field(name, options))
            }
            FieldKind::Keyword | FieldKind::Uuid => {
                let mut options = TextOptions::default();
                if spec.indexed {
                    options = options.set_indexing_options(raw_basic().set_fieldnorms(false));
                }
                if spec.fast {
                    options = options.set_fast(Some(RAW));
                }
                FieldMap::Plain(builder.add_text_field(name, options))
            }
            FieldKind::I64 => {
                FieldMap::Plain(builder.add_i64_field(name, numeric(spec.indexed, spec.fast)))
            }
            FieldKind::F64 => {
                FieldMap::Plain(builder.add_f64_field(name, numeric(spec.indexed, spec.fast)))
            }
            FieldKind::Bool => {
                FieldMap::Plain(builder.add_bool_field(name, numeric(spec.indexed, spec.fast)))
            }
            FieldKind::Date => {
                let mut options =
                    DateOptions::default().set_precision(DateTimePrecision::Milliseconds);
                if spec.indexed {
                    options = options.set_indexed();
                }
                if spec.fast {
                    options = options.set_fast();
                }
                FieldMap::Plain(builder.add_date_field(name, options))
            }
            FieldKind::Json => {
                let main = JsonObjectOptions::default()
                    .set_indexing_options(raw_basic())
                    .set_expand_dots_enabled()
                    .set_fast(Some(RAW));
                let text = JsonObjectOptions::default()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(STANDARD)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                    )
                    .set_expand_dots_enabled();
                // Dates and counts only: their leaves are never tokenized.
                let typed = || {
                    JsonObjectOptions::default()
                        .set_indexing_options(raw_basic())
                        .set_expand_dots_enabled()
                        .set_fast(None)
                };
                let nulls = TextOptions::default().set_indexing_options(raw_basic());
                FieldMap::Json {
                    main: builder.add_json_field(name, main),
                    text: builder.add_json_field(&text_companion(name), text),
                    date: builder.add_json_field(&date_companion(name), typed()),
                    nulls: builder.add_text_field(&null_companion(name), nulls),
                    counts: builder.add_json_field(&count_companion(name), typed()),
                }
            }
        };
        fields.push(map);
    }
    let sparse = schema
        .sparse_vectors
        .iter()
        .map(|spec| SparseFieldMap {
            postings: builder.add_u64_field(
                &sparse_postings_field(&spec.name),
                NumericOptions::default().set_indexed(),
            ),
            weights: builder.add_bytes_field(
                &sparse_weights_field(&spec.name),
                BytesOptions::default().set_fast(),
            ),
        })
        .collect();
    TantivyLayout {
        schema: builder.build(),
        pk,
        rowid,
        presence,
        fields,
        sparse,
    }
}

/// The `_field_presence` term of a whole (non-JSON) field, as the vendored
/// `compute_field_presence_hash(field, "")` builds it.
fn presence_term(field: Field) -> u64 {
    let mut hasher = PathHasher::default();
    hasher.append(&field.field_id().to_le_bytes());
    hasher.finish_leaf()
}

/// The Tantivy document of row `row_id`, which holds `doc` whose typed
/// values are `extracted` (`check_document`'s result under `schema`) (rule
/// 4). `layout` is `tantivy_layout(schema)`.
pub fn to_tantivy_doc(
    layout: &TantivyLayout,
    schema: &CollectionSchema,
    doc: &Document,
    extracted: &ExtractedDoc,
    row_id: u64,
) -> TantivyDocument {
    let mut out = TantivyDocument::default();
    out.add_bytes(layout.pk, &doc.pk.canonical());
    out.add_u64(layout.rowid, row_id);
    for (index, values) in &extracted.values {
        let Some(FieldMap::Plain(field)) = layout.fields.get(*index) else {
            continue;
        };
        let mut present = false;
        for value in values {
            present |= add_value(&mut out, *field, value);
        }
        if present {
            out.add_u64(layout.presence, presence_term(*field));
        }
    }
    for (spec, map) in schema.fields.iter().zip(&layout.fields) {
        add_json(&mut out, map, &doc.source, &spec.source_path);
    }
    for (spec, map) in schema.sparse_vectors.iter().zip(&layout.sparse) {
        let Some(vector) = doc.sparse_vectors.get(&spec.name) else {
            continue;
        };
        if vector.is_empty() {
            continue;
        }
        for index in vector.indices() {
            out.add_u64(map.postings, u64::from(*index));
        }
        out.add_u64(map.postings, SPARSE_PRESENT);
        out.add_bytes(map.weights, &encode_sparse_weights(vector));
    }
    out
}

/// Adds one typed value; returns whether it was added (a date that Tantivy
/// cannot represent is not, although validation already refuses it).
fn add_value(out: &mut TantivyDocument, field: Field, value: &IndexValue) -> bool {
    match value {
        IndexValue::Text(s) | IndexValue::Keyword(s) => out.add_text(field, s),
        IndexValue::I64(v) => out.add_i64(field, *v),
        IndexValue::F64(v) => out.add_f64(field, *v),
        IndexValue::Bool(v) => out.add_bool(field, *v),
        IndexValue::Date(millis) => match date(*millis) {
            Some(date) => out.add_date(field, date),
            None => return false,
        },
        IndexValue::Uuid(bytes) => out.add_text(
            field,
            uuid::Uuid::from_bytes(*bytes).hyphenated().to_string(),
        ),
    }
    true
}

/// Epoch milliseconds as a Tantivy date, if its nanoseconds fit an i64.
fn date(millis: i64) -> Option<DateTime> {
    millis
        .checked_mul(1_000_000)
        .map(DateTime::from_timestamp_nanos)
}

/// Indexes the objects at `source_path` into the five fields of a Json
/// field (rule 4); a plain field is left alone. A value there that is not an
/// object (after array flattening) is not indexed: a Json field holds
/// objects.
fn add_json(out: &mut TantivyDocument, map: &FieldMap, source: &Map<String, Value>, path: &str) {
    let FieldMap::Json {
        main,
        text,
        date,
        nulls,
        counts: count_field,
    } = *map
    else {
        return;
    };
    let mut null_paths = BTreeSet::new();
    let mut counts = BTreeMap::new();
    for value in extract(source, path) {
        let Value::Object(object) = value.as_ref() else {
            continue;
        };
        for (leaves, field) in [
            (Leaves::All, main),
            (Leaves::Strings, text),
            (Leaves::Dates, date),
        ] {
            let converted = convert_object(object, leaves);
            if !converted.is_empty() {
                out.add_object(field, converted);
            }
        }
        for (key, nested) in object {
            null_paths_at(key, nested, &mut null_paths);
            count_at(key, nested, &mut counts);
        }
    }
    for path in null_paths {
        out.add_text(nulls, path);
    }
    if !counts.is_empty() {
        // Dotted keys: the field expands dots, so `{"o": 1, "o.k": 2}` puts
        // 1 at `o` and 2 at `o.k`.
        let counts = counts
            .into_iter()
            .map(|(path, count)| (path, OwnedValue::U64(count)))
            .collect();
        out.add_object(count_field, counts);
    }
}

/// Which leaves of a Json value a field indexes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Leaves {
    /// Every non-null leaf: strings as strings (never as dates), numbers and
    /// bools typed.
    All,
    /// String leaves only.
    Strings,
    /// String leaves that `parse_date` accepts in a non-digit format, as
    /// dates.
    Dates,
}

fn convert_object(object: &Map<String, Value>, leaves: Leaves) -> BTreeMap<String, OwnedValue> {
    object
        .iter()
        .filter_map(|(key, value)| Some((key.clone(), convert(value, leaves)?)))
        .collect()
}

/// `value` restricted to `leaves`; `None` when nothing is left.
fn convert(value: &Value, leaves: Leaves) -> Option<OwnedValue> {
    let converted = match value {
        Value::Null => return None,
        Value::Object(object) => {
            let object: Vec<(String, OwnedValue)> =
                convert_object(object, leaves).into_iter().collect();
            if object.is_empty() {
                return None;
            }
            OwnedValue::Object(object)
        }
        Value::Array(items) => {
            let items: Vec<OwnedValue> = items
                .iter()
                .filter_map(|item| convert(item, leaves))
                .collect();
            if items.is_empty() {
                return None;
            }
            OwnedValue::Array(items)
        }
        Value::String(s) => match leaves {
            Leaves::All | Leaves::Strings => OwnedValue::Str(s.clone()),
            Leaves::Dates if !is_epoch_millis(s) => OwnedValue::Date(date(parse_date_str(s)?)?),
            Leaves::Dates => return None,
        },
        _ if leaves != Leaves::All => return None,
        Value::Bool(b) => OwnedValue::Bool(*b),
        Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => OwnedValue::I64(i),
            (None, Some(u), _) => OwnedValue::U64(u),
            (None, None, Some(f)) => OwnedValue::F64(f),
            (None, None, None) => return None,
        },
    };
    Some(converted)
}

/// Adds `path` for every `null` at or below `path` (array elements share
/// their array's path).
fn null_paths_at(path: &str, value: &Value, out: &mut BTreeSet<String>) {
    match value {
        Value::Null => {
            out.insert(path.to_string());
        }
        Value::Array(items) => {
            for item in items {
                null_paths_at(path, item, out);
            }
        }
        Value::Object(object) => {
            for (key, nested) in object {
                null_paths_at(&format!("{path}.{key}"), nested, out);
            }
        }
        _ => {}
    }
}

/// Counts, per path at or below `path`, its non-null values after array
/// flattening (an object counts as one value of its path).
fn count_at(path: &str, value: &Value, out: &mut BTreeMap<String, u64>) {
    match value {
        Value::Null => {}
        Value::Array(items) => {
            for item in items {
                count_at(path, item, out);
            }
        }
        Value::Object(object) => {
            *out.entry(path.to_string()).or_default() += 1;
            for (key, nested) in object {
                count_at(&format!("{path}.{key}"), nested, out);
            }
        }
        _ => *out.entry(path.to_string()).or_default() += 1,
    }
}
