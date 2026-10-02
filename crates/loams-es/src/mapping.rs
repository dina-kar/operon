//! Elasticsearch mappings and settings ⇄ collection schemas (plan M1.5
//! Task 2).
//!
//! What the engine needs goes into `FieldSpec`s and `VectorSpec`s; what only
//! ES clients see (the declared JSON of each path, the vector similarity
//! names and the index settings) goes into the schema's `es.*` annotations
//! (overview A1), so `GET _mapping` returns what was declared.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use loams_collection::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, DynamicMappingError, FieldKind,
    FieldSpec, HnswParams, KNOWN_ANALYZERS, Quantization, VectorElement, VectorIndexSpec,
    VectorSpec, propose_dynamic_fields, unmapped_paths,
};
use loams_common::CollectionId;
use loams_query::CollectionInfo;
use serde_json::{Map, Value, json};

use crate::error::EsError;

/// Annotation key prefix: `es.field.<path>` = the declared mapping JSON of
/// that path (without `fields` and `properties`), keys sorted.
pub const ANN_FIELD: &str = "es.field.";
/// The normalised user settings: a JSON object of flat `index.*` keys to
/// string values (everything but `index.number_of_shards`).
pub const ANN_SETTINGS: &str = "es.settings";
/// `es.similarity.<vector>` = `cosine`, `dot_product`, `l2_norm` or
/// `max_inner_product`.
pub const ANN_SIMILARITY: &str = "es.similarity.";

/// `index.max_result_window` unless set.
pub const DEFAULT_MAX_RESULT_WINDOW: usize = 10_000;
/// `dense_vector` `dims` range.
const MAX_DIMS: u32 = 4096;
/// `index.number_of_shards` range.
const MAX_SHARDS: u32 = 1024;
/// The largest `max_fields` a schema allows (M1.1).
const MAX_FIELDS_LIMIT: u32 = 100_000;
/// `index.version.created` of ES 8.19.0.
const VERSION_CREATED: &str = "8190099";

/// The date formats of Ruling 17.
const DATE_FORMATS: [&str; 4] = [
    "strict_date_optional_time",
    "date_optional_time",
    "strict_date_optional_time_nanos",
    "epoch_millis",
];

/// Field types outside Phase A.
const PHASE_B_TYPES: [&str; 22] = [
    "nested",
    "unsigned_long",
    "date_nanos",
    "ip",
    "geo_point",
    "geo_shape",
    "completion",
    "search_as_you_type",
    "token_count",
    "rank_feature",
    "rank_features",
    "sparse_vector",
    "semantic_text",
    "alias",
    "join",
    "percolator",
    "wildcard",
    "constant_keyword",
    "match_only_text",
    "version",
    "histogram",
    "aggregate_metric_double",
];

/// The similarities of a `dense_vector`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EsSimilarity {
    Cosine,
    DotProduct,
    L2Norm,
    MaxInnerProduct,
    /// A Manhattan vector made through another API; ES has no name for it.
    L1Norm,
}

impl EsSimilarity {
    /// The ES name of a similarity a mapping may declare.
    pub fn parse(name: &str) -> Option<EsSimilarity> {
        Some(match name {
            "cosine" => EsSimilarity::Cosine,
            "dot_product" => EsSimilarity::DotProduct,
            "l2_norm" => EsSimilarity::L2Norm,
            "max_inner_product" => EsSimilarity::MaxInnerProduct,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EsSimilarity::Cosine => "cosine",
            EsSimilarity::DotProduct => "dot_product",
            EsSimilarity::L2Norm => "l2_norm",
            EsSimilarity::MaxInnerProduct => "max_inner_product",
            EsSimilarity::L1Norm => "l1_norm",
        }
    }

    /// The engine distance (item 2).
    pub fn distance(self) -> Distance {
        match self {
            EsSimilarity::Cosine => Distance::Cosine,
            EsSimilarity::DotProduct | EsSimilarity::MaxInnerProduct => Distance::Dot,
            EsSimilarity::L2Norm => Distance::Euclid,
            EsSimilarity::L1Norm => Distance::Manhattan,
        }
    }

    /// The similarity of a vector without an `es.similarity` annotation.
    pub fn of_distance(distance: Distance) -> EsSimilarity {
        match distance {
            Distance::Cosine => EsSimilarity::Cosine,
            Distance::Dot => EsSimilarity::DotProduct,
            Distance::Euclid => EsSimilarity::L2Norm,
            Distance::Manhattan => EsSimilarity::L1Norm,
        }
    }
}

/// What a create or a put-mapping asks of the schema.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaPlan {
    pub fields: Vec<FieldSpec>,
    pub vectors: Vec<VectorSpec>,
    pub dynamic: DynamicMapping,
    pub max_fields: u32,
    pub annotations: BTreeMap<String, String>,
    /// `index.number_of_shards`.
    pub partitions: Option<u32>,
}

impl SchemaPlan {
    /// The version 1 schema a create makes from this plan.
    pub fn schema(&self) -> CollectionSchema {
        let mut schema =
            CollectionSchema::new(self.fields.clone(), self.vectors.clone(), self.dynamic);
        schema.max_fields = self.max_fields;
        schema.annotations = self.annotations.clone();
        schema
    }
}

/// 400 `mapper_parsing_exception`, wrapped as ES wraps every mapping parse
/// error: the reason `Failed to parse mapping: <why>`, with `<why>` as its
/// root cause and `caused_by` (checked against the 8.19 oracle, row T11-3).
fn mapper(reason: impl Into<String>) -> EsError {
    let reason = reason.into();
    let why = reason
        .strip_prefix("Failed to parse mapping: ")
        .unwrap_or(&reason)
        .to_string();
    let mut error = EsError::new(
        400,
        "mapper_parsing_exception",
        format!("Failed to parse mapping: {why}"),
    )
    .with(
        "caused_by",
        json!({"type": "mapper_parsing_exception", "reason": why}),
    );
    error.root_cause = Some(Box::new(EsError::new(400, "mapper_parsing_exception", why)));
    error
}

/// A field type ES does not know.
fn no_handler(ty: &str, path: &str) -> EsError {
    mapper(format!(
        "The mapper type [{ty}] declared on field [{path}] does not exist. It might have been \
         created within a future version or requires a plugin to be installed. Check the \
         documentation."
    ))
}

fn phase_a_type(ty: &str, path: &str) -> EsError {
    EsError::new(
        400,
        "mapper_parsing_exception",
        format!(
            "Loams does not support field type [{ty}] (declared on field [{path}]) in \
         Elasticsearch API Phase A"
        ),
    )
}

fn unknown_parameter(param: &str, path: &str, ty: &str) -> EsError {
    mapper(format!(
        "unknown parameter [{param}] on mapper [{path}] of type [{ty}]"
    ))
}

fn unknown_setting(key: &str) -> EsError {
    EsError::illegal_argument(format!(
        "unknown setting [{key}] please check that any required plugins are installed, or \
         check the breaking changes documentation for removed settings"
    ))
}

fn fields_limit(limit: u32) -> EsError {
    EsError::illegal_argument(format!("Limit of total fields [{limit}] has been exceeded"))
}

/// A value as ES prints it in messages: strings bare, the rest as JSON.
fn bare(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `value` with every object's keys sorted, recursively.
fn sorted(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let ordered: BTreeMap<&String, Value> =
                map.iter().map(|(k, v)| (k, sorted(v))).collect();
            Value::Object(ordered.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

/// The annotation of a declaration: its JSON minus `fields` and
/// `properties`, keys sorted.
fn declared_annotation(def: &Map<String, Value>) -> String {
    let mut declared = def.clone();
    declared.shift_remove("fields");
    declared.shift_remove("properties");
    sorted(&Value::Object(declared)).to_string()
}

/// ES's boolean parsing: a JSON bool, or the strings `true` and `false`.
fn as_bool(value: &Value, param: &str, path: &str) -> Result<bool, EsError> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::String(s) if s == "true" => Ok(true),
        Value::String(s) if s == "false" => Ok(false),
        other => Err(mapper(format!(
            "Failed to parse mapping: failed to parse [{param}] of field [{path}]: Failed to \
             parse value [{}] as only [true] or [false] are allowed.",
            bare(other)
        ))),
    }
}

/// A non-negative integer given as a number or a string.
fn as_u64(value: &Value, param: &str, path: &str) -> Result<u64, EsError> {
    let parsed = match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    };
    parsed.ok_or_else(|| {
        mapper(format!(
            "Failed to parse mapping: failed to parse [{param}] of field [{path}]: [{}] is \
             not a non-negative integer",
            bare(value)
        ))
    })
}

/// A string parameter.
fn as_str<'a>(value: &'a Value, param: &str, path: &str) -> Result<&'a str, EsError> {
    value.as_str().ok_or_else(|| {
        mapper(format!(
            "Failed to parse mapping: [{param}] of field [{path}] must be a string, got [{}]",
            bare(value)
        ))
    })
}

/// A root or object `dynamic` value.
fn parse_dynamic(value: &Value) -> Result<DynamicMapping, EsError> {
    match value {
        Value::Bool(true) => Ok(DynamicMapping::Map),
        Value::Bool(false) => Ok(DynamicMapping::Ignore),
        Value::String(s) => match s.as_str() {
            "true" => Ok(DynamicMapping::Map),
            "false" => Ok(DynamicMapping::Ignore),
            "strict" => Ok(DynamicMapping::Strict),
            "runtime" => Err(EsError::unsupported("dynamic: runtime")),
            other => Err(mapper(format!(
                "Failed to parse mapping: Could not convert [dynamic] to boolean: [{other}]"
            ))),
        },
        other => Err(mapper(format!(
            "Failed to parse mapping: Could not convert [dynamic] to boolean: [{}]",
            bare(other)
        ))),
    }
}

fn dynamic_name(dynamic: DynamicMapping) -> &'static str {
    match dynamic {
        DynamicMapping::Map => "true",
        DynamicMapping::Ignore => "false",
        DynamicMapping::Strict => "strict",
    }
}

// ----- settings -----

/// Flattens `settings` into `index.*` keys with string values: nested
/// objects are joined with dots, keys get an `index.` prefix if absent,
/// numbers and booleans are written as JSON text; `null` values are
/// dropped.
pub fn normalize_settings(settings: &Value) -> Result<BTreeMap<String, String>, EsError> {
    let Value::Object(map) = settings else {
        return Err(EsError::illegal_argument(format!(
            "settings must be an object, got [{}]",
            bare(settings)
        )));
    };
    fn walk(prefix: &str, map: &Map<String, Value>, out: &mut BTreeMap<String, String>) {
        for (key, value) in map {
            let key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match value {
                Value::Object(inner) => walk(&key, inner, out),
                Value::Null => {}
                other => {
                    let key = if key == "index" || key.starts_with("index.") {
                        key
                    } else {
                        format!("index.{key}")
                    };
                    out.insert(key, bare(other));
                }
            }
        }
    }
    let mut out = BTreeMap::new();
    walk("", map, &mut out);
    Ok(out)
}

/// A numeric setting within `min..=max`, with ES's wording.
fn setting_u32(key: &str, value: &str, min: u32, max: u32) -> Result<u32, EsError> {
    let parsed: u32 = value.parse().map_err(|_| {
        EsError::illegal_argument(format!(
            "Failed to parse value [{value}] for setting [{key}]"
        ))
    })?;
    if parsed < min {
        return Err(EsError::illegal_argument(format!(
            "Failed to parse value [{value}] for setting [{key}] must be >= {min}"
        )));
    }
    if parsed > max {
        return Err(EsError::illegal_argument(format!(
            "Failed to parse value [{value}] for setting [{key}] must be <= {max}"
        )));
    }
    Ok(parsed)
}

/// The settings a create accepts (item 3).
struct Settings {
    partitions: Option<u32>,
    max_fields: u32,
    /// Everything but `index.number_of_shards`.
    stored: BTreeMap<String, String>,
}

fn bm25_only() -> EsError {
    EsError::illegal_argument("Loams supports BM25 with k1=1.2 and b=0.75 only (Phase A)")
}

fn check_settings(flat: BTreeMap<String, String>) -> Result<Settings, EsError> {
    let mut partitions = None;
    let mut max_fields = DEFAULT_MAX_FIELDS;
    let mut similarities: BTreeMap<String, bool> = BTreeMap::new();
    // ES reports a similarity without a type before its parameters.
    let mut bm25_refused = false;
    for (key, value) in &flat {
        match key.as_str() {
            "index.number_of_shards" => {
                partitions = Some(setting_u32(key, value, 1, MAX_SHARDS)?);
            }
            "index.number_of_replicas" => {
                setting_u32(key, value, 0, u32::MAX)?;
            }
            "index.max_result_window" => {
                setting_u32(key, value, 1, u32::MAX)?;
            }
            "index.mapping.total_fields.limit" => {
                max_fields = setting_u32(key, value, 1, MAX_FIELDS_LIMIT)?;
            }
            "index.refresh_interval" | "index.default_pipeline" => {}
            _ if key.starts_with("index.analysis.") || key == "index.analysis" => {
                return Err(EsError::unsupported("index.analysis"));
            }
            _ => {
                let Some(rest) = key.strip_prefix("index.similarity.") else {
                    return Err(unknown_setting(key));
                };
                let Some((name, param)) = rest.rsplit_once('.') else {
                    return Err(unknown_setting(key));
                };
                let typed = similarities.entry(name.to_string()).or_default();
                match param {
                    "type" if value == "BM25" => *typed = true,
                    "type" => return Err(bm25_only()),
                    "k1" if value.parse::<f64>().ok() == Some(1.2) => {}
                    "b" if value.parse::<f64>().ok() == Some(0.75) => {}
                    "discount_overlaps" if value == "true" => {}
                    "k1" | "b" | "discount_overlaps" => bm25_refused = true,
                    _ => return Err(unknown_setting(key)),
                }
            }
        }
    }
    if let Some((name, _)) = similarities.iter().find(|(_, typed)| !**typed) {
        return Err(EsError::illegal_argument(format!(
            "Similarity [{name}] must have an associated type"
        )));
    }
    if bm25_refused {
        return Err(bm25_only());
    }
    let mut stored = flat;
    stored.remove("index.number_of_shards");
    Ok(Settings {
        partitions,
        max_fields,
        stored,
    })
}

/// The stored settings of a schema (`ANN_SETTINGS`).
fn stored_settings(schema: &CollectionSchema) -> BTreeMap<String, String> {
    schema
        .annotations
        .get(ANN_SETTINGS)
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or_default()
}

// ----- mappings -----

/// One declared path.
#[derive(Clone, Debug)]
enum Decl {
    Field {
        spec: FieldSpec,
        es_type: String,
    },
    /// `None`: a `dense_vector` without `dims` (the first document sets it).
    Vector {
        spec: Option<VectorSpec>,
    },
    Object {
        enabled: bool,
    },
}

#[derive(Clone, Debug)]
struct Declared {
    path: String,
    decl: Decl,
    /// `es.field.<path>`.
    annotation: String,
    /// `es.similarity.<path>`, for vectors.
    similarity: Option<EsSimilarity>,
}

/// The walk over a mapping's `properties`.
struct Walk<'a> {
    settings: &'a BTreeMap<String, String>,
    root_dynamic: DynamicMapping,
    out: Vec<Declared>,
}

/// Parameters every leaf type accepts and ignores (item 1).
const IGNORED: [&str; 3] = ["store", "eager_global_ordinals", "boost"];

impl Walk<'_> {
    fn properties(&mut self, props: &Value, prefix: &str) -> Result<(), EsError> {
        let Value::Object(props) = props else {
            return Err(mapper(format!(
                "Failed to parse mapping: Expected map for property [properties] on field \
                 [{}] but got [{}]",
                if prefix.is_empty() { "_doc" } else { prefix },
                bare(props)
            )));
        };
        for (key, def) in props {
            if key.trim().is_empty() {
                return Err(mapper(
                    "Failed to parse mapping: field name cannot be an empty string",
                ));
            }
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            let Value::Object(def) = def else {
                return Err(mapper(format!(
                    "Failed to parse mapping: Expected map for property [{path}] but got [{}]",
                    bare(def)
                )));
            };
            let ty = match def.get("type") {
                None => "object",
                Some(Value::String(ty)) => ty.as_str(),
                Some(other) => {
                    return Err(no_handler(&bare(other), &path));
                }
            };
            match ty {
                "object" => self.object(&path, def)?,
                "dense_vector" => self.vector(&path, def)?,
                _ => self.leaf(&path, &path, ty, def, false)?,
            }
        }
        Ok(())
    }

    fn object(&mut self, path: &str, def: &Map<String, Value>) -> Result<(), EsError> {
        let mut enabled = true;
        for (param, value) in def {
            match param.as_str() {
                "type" | "properties" => {}
                "enabled" => enabled = as_bool(value, param, path)?,
                "dynamic" => {
                    let dynamic = parse_dynamic(value)?;
                    if dynamic != self.root_dynamic {
                        return Err(EsError::unsupported(&format!(
                            "dynamic: {} on object [{path}] under a root with dynamic: {}",
                            dynamic_name(dynamic),
                            dynamic_name(self.root_dynamic)
                        )));
                    }
                }
                _ => return Err(unknown_parameter(param, path, "object")),
            }
        }
        self.out.push(Declared {
            path: path.to_string(),
            decl: Decl::Object { enabled },
            annotation: declared_annotation(def),
            similarity: None,
        });
        if enabled && let Some(props) = def.get("properties") {
            self.properties(props, path)?;
        }
        Ok(())
    }

    /// A leaf field at `path` reading `source_path` (they differ for a
    /// multi-field).
    fn leaf(
        &mut self,
        path: &str,
        source_path: &str,
        ty: &str,
        def: &Map<String, Value>,
        is_sub: bool,
    ) -> Result<(), EsError> {
        if PHASE_B_TYPES.contains(&ty) || ty.ends_with("_range") {
            return Err(phase_a_type(ty, path));
        }
        let known = matches!(
            ty,
            "text"
                | "keyword"
                | "long"
                | "integer"
                | "short"
                | "byte"
                | "double"
                | "float"
                | "half_float"
                | "scaled_float"
                | "boolean"
                | "date"
                | "flattened"
                | "binary"
        );
        if !known {
            return Err(no_handler(ty, path));
        }
        let numeric = matches!(
            ty,
            "long" | "integer" | "short" | "byte" | "double" | "float" | "half_float"
        ) || ty == "scaled_float";
        let multi_fields =
            !is_sub && (numeric || matches!(ty, "text" | "keyword" | "boolean" | "date"));
        let mut index = true;
        let mut doc_values = true;
        let mut ignore_malformed = false;
        let mut analyzer = "standard".to_string();
        let mut search_analyzer: Option<String> = None;
        let mut subfields: Option<&Map<String, Value>> = None;
        for (param, value) in def {
            let p = param.as_str();
            match p {
                "type" => {}
                _ if IGNORED.contains(&p) => {}
                "copy_to" | "null_value" | "index_options" | "term_vector" => {
                    return Err(EsError::unsupported(p));
                }
                "fields" if multi_fields => match value {
                    Value::Object(map) => subfields = Some(map),
                    other => {
                        return Err(mapper(format!(
                            "Failed to parse mapping: Expected map for property [fields] on \
                             field [{path}] but got [{}]",
                            bare(other)
                        )));
                    }
                },
                "index" if ty != "binary" && ty != "flattened" => {
                    index = as_bool(value, p, path)?;
                }
                "doc_values" if ty != "text" && ty != "flattened" => {
                    doc_values = as_bool(value, p, path)?;
                }
                "norms" if ty == "text" || ty == "keyword" => {
                    if ty == "text" && !as_bool(value, p, path)? {
                        return Err(EsError::unsupported("norms: false"));
                    }
                }
                "analyzer" if ty == "text" => {
                    analyzer = as_str(value, p, path)?.to_string();
                }
                "search_analyzer" if ty == "text" => {
                    search_analyzer = Some(as_str(value, p, path)?.to_string());
                }
                "similarity" if ty == "text" => {
                    let name = as_str(value, p, path)?;
                    let custom = self
                        .settings
                        .get(&format!("index.similarity.{name}.type"))
                        .is_some_and(|t| t == "BM25");
                    if name != "BM25" && !custom {
                        if name == "boolean" {
                            return Err(EsError::unsupported("similarity: boolean"));
                        }
                        return Err(mapper(format!(
                            "Failed to parse mapping: Unknown Similarity type [{name}] for \
                             field [{path}]"
                        )));
                    }
                }
                "fielddata" if ty == "text" => {
                    if as_bool(value, p, path)? {
                        return Err(EsError::unsupported("fielddata: true"));
                    }
                }
                "ignore_above" if ty == "keyword" => {
                    as_u64(value, p, path)?;
                }
                "normalizer" if ty == "keyword" => {
                    return Err(EsError::unsupported("normalizer"));
                }
                "ignore_malformed" if numeric || ty == "date" => {
                    ignore_malformed = as_bool(value, p, path)?;
                }
                "scaling_factor" if ty == "scaled_float" => {
                    if !value.is_number() {
                        return Err(mapper(format!(
                            "Failed to parse mapping: [scaling_factor] of field [{path}] must \
                             be a number"
                        )));
                    }
                }
                "format" if ty == "date" => {
                    let format = as_str(value, p, path)?;
                    if !format.split("||").all(|f| DATE_FORMATS.contains(&f.trim())) {
                        return Err(mapper(format!(
                            "Loams does not support date format [{format}] (Elasticsearch \
                             API Phase A)"
                        )));
                    }
                }
                _ => return Err(unknown_parameter(p, path, ty)),
            }
        }
        if ty == "scaled_float" && !def.contains_key("scaling_factor") {
            return Err(mapper(format!(
                "Failed to parse mapping: Field [scaling_factor] is required for field [{path}]"
            )));
        }
        if analyzer == "default" {
            analyzer = "standard".to_string();
        }
        if ty == "text" && !KNOWN_ANALYZERS.contains(&analyzer.as_str()) {
            return Err(mapper(format!(
                "Failed to parse mapping: analyzer [{analyzer}] has not been configured in \
                 mappings"
            )));
        }
        if let Some(search) = search_analyzer {
            let search = if search == "default" {
                "standard".to_string()
            } else {
                search
            };
            if search != analyzer {
                return Err(EsError::unsupported(
                    "search_analyzer different from analyzer",
                ));
            }
        }
        // M1.1 keeps a field only if it is indexed or fast: a field ES
        // neither indexes nor keeps doc values for is fast (row T2-2), and
        // an unindexed `text` or a `binary` is an unindexed keyword that
        // lives in `_source` (queries on it are refused by the gateway).
        let (kind, indexed, fast) = match ty {
            "text" if index => (
                FieldKind::Text {
                    analyzer,
                    positions: true,
                },
                true,
                false,
            ),
            "text" | "binary" => (FieldKind::Keyword, false, true),
            "keyword" => (FieldKind::Keyword, index, doc_values || !index),
            "long" | "integer" | "short" | "byte" => (FieldKind::I64, index, doc_values || !index),
            "boolean" => (FieldKind::Bool, index, doc_values || !index),
            "date" => (FieldKind::Date, index, doc_values || !index),
            "flattened" => (FieldKind::Json, true, true),
            _ => (FieldKind::F64, index, doc_values || !index),
        };
        self.out.push(Declared {
            path: path.to_string(),
            decl: Decl::Field {
                spec: FieldSpec {
                    name: path.to_string(),
                    source_path: source_path.to_string(),
                    kind,
                    indexed,
                    fast,
                    ignore_malformed,
                },
                es_type: ty.to_string(),
            },
            annotation: declared_annotation(def),
            similarity: None,
        });
        for (sub, sdef) in subfields.into_iter().flatten() {
            let sub_path = format!("{path}.{sub}");
            let Value::Object(sdef) = sdef else {
                return Err(mapper(format!(
                    "Failed to parse mapping: Expected map for property [{sub_path}] but got [{}]",
                    bare(sdef)
                )));
            };
            let sty = match sdef.get("type") {
                Some(Value::String(t)) => t.as_str(),
                _ => {
                    return Err(mapper(format!(
                        "Failed to parse mapping: No type specified for field [{sub}]"
                    )));
                }
            };
            match sty {
                "object" | "dense_vector" => return Err(phase_a_type(sty, &sub_path)),
                _ => self.leaf(&sub_path, path, sty, sdef, true)?,
            }
        }
        Ok(())
    }

    fn vector(&mut self, path: &str, def: &Map<String, Value>) -> Result<(), EsError> {
        let mut dims = None;
        let mut index = true;
        let mut similarity = None;
        let mut options: Option<&Map<String, Value>> = None;
        for (param, value) in def {
            let p = param.as_str();
            match p {
                "type" => {}
                "dims" => {
                    let n = match value {
                        Value::Number(n) => n.as_i64(),
                        Value::String(s) => s.parse().ok(),
                        _ => None,
                    }
                    .ok_or_else(|| {
                        mapper(format!(
                            "Failed to parse mapping: [dims] of field [{path}] must be an integer"
                        ))
                    })?;
                    if !(1..=i64::from(MAX_DIMS)).contains(&n) {
                        return Err(mapper(format!(
                            "The number of dimensions should be in the range [1, {MAX_DIMS}] \
                             but was [{n}]"
                        )));
                    }
                    dims = u32::try_from(n).ok();
                }
                "element_type" => {
                    let element = as_str(value, p, path)?;
                    if element != "float" {
                        return Err(EsError::unsupported(&format!("element_type: {element}")));
                    }
                }
                "index" => index = as_bool(value, p, path)?,
                "similarity" => {
                    let name = as_str(value, p, path)?;
                    similarity = Some(EsSimilarity::parse(name).ok_or_else(|| {
                        mapper(format!(
                            "Unknown value [{name}] for field [similarity] - accepted values \
                                 are [l2_norm, cosine, dot_product, max_inner_product]"
                        ))
                    })?);
                }
                "index_options" => match value {
                    Value::Object(map) => options = Some(map),
                    other => {
                        return Err(mapper(format!(
                            "Failed to parse mapping: [index_options] of field [{path}] must \
                             be an object, got [{}]",
                            bare(other)
                        )));
                    }
                },
                _ => return Err(unknown_parameter(p, path, "dense_vector")),
            }
        }
        let mut spec_index = if index {
            VectorIndexSpec::Auto
        } else {
            VectorIndexSpec::None
        };
        let mut hnsw = HnswParams::default();
        let mut quantization = None;
        if let Some(options) = options {
            if !index {
                return Err(mapper(format!(
                    "Failed to parse mapping: [index_options] on field [{path}] requires \
                     [index] to be [true]"
                )));
            }
            let kind = match options.get("type") {
                Some(Value::String(t)) => t.as_str(),
                _ => {
                    return Err(mapper(format!(
                        "Failed to parse mapping: [index_options] field [type] should be \
                         defined on field [{path}]"
                    )));
                }
            };
            let scalar = Quantization::Scalar {
                quantile_ppm: None,
                always_ram: false,
            };
            let graph = match kind {
                "hnsw" => true,
                "int8_hnsw" => {
                    quantization = Some(scalar);
                    true
                }
                "flat" => false,
                "int8_flat" => {
                    quantization = Some(scalar);
                    false
                }
                "int4_hnsw" | "int4_flat" | "bbq_hnsw" | "bbq_flat" => {
                    return Err(EsError::unsupported(&format!("index_options.type: {kind}")));
                }
                other => {
                    return Err(mapper(format!(
                        "Failed to parse mapping: Unknown vector index options type [{other}] \
                         for field [{path}]"
                    )));
                }
            };
            for (param, value) in options {
                match param.as_str() {
                    "type" => {}
                    "m" if graph => {
                        hnsw.m = u32::try_from(as_u64(value, "m", path)?).unwrap_or(u32::MAX);
                    }
                    "ef_construction" if graph => {
                        hnsw.ef_construct = u32::try_from(as_u64(value, "ef_construction", path)?)
                            .unwrap_or(u32::MAX);
                    }
                    other => {
                        return Err(mapper(format!(
                            "Failed to parse mapping: unknown parameter [{other}] in \
                             [index_options] of field [{path}]"
                        )));
                    }
                }
            }
            if !graph {
                spec_index = VectorIndexSpec::None;
            }
        }
        let recorded = similarity.or(index.then_some(EsSimilarity::Cosine));
        let distance = similarity.map_or(Distance::Cosine, EsSimilarity::distance);
        let spec = dims.map(|dim| VectorSpec {
            name: path.to_string(),
            dim,
            distance,
            element: VectorElement::F32,
            index: spec_index,
            hnsw,
            quantization,
        });
        self.out.push(Declared {
            path: path.to_string(),
            decl: Decl::Vector { spec },
            annotation: declared_annotation(def),
            similarity: recorded,
        });
        Ok(())
    }
}

/// A mapping's root: its `dynamic` (if given) and `properties`.
fn parse_root(mappings: &Value) -> Result<(Option<DynamicMapping>, Option<&Value>), EsError> {
    let Value::Object(root) = mappings else {
        return Err(mapper(format!(
            "Failed to parse mapping: Expected map for [mappings] but got [{}]",
            bare(mappings)
        )));
    };
    let mut dynamic = None;
    let mut unsupported = Vec::new();
    for (key, value) in root {
        match key.as_str() {
            "properties" => {}
            "dynamic" => dynamic = Some(parse_dynamic(value)?),
            "_source" => {
                let enabled_only = match value {
                    Value::Object(map) => map.iter().all(|(k, v)| {
                        k == "enabled" && (v == &Value::Bool(true) || v == &json!("true"))
                    }),
                    _ => false,
                };
                if !enabled_only {
                    return Err(EsError::unsupported(
                        "_source other than {\"enabled\": true}",
                    ));
                }
            }
            _ => unsupported.push(format!("[{key} : {}]", bare(value))),
        }
    }
    if !unsupported.is_empty() {
        return Err(mapper(format!(
            "Root mapping definition has unsupported parameters:  {}",
            unsupported.join(" ")
        )));
    }
    Ok((dynamic, root.get("properties")))
}

/// Validates the schema a plan leads to, as a mapping error.
fn check_schema(schema: &CollectionSchema) -> Result<(), EsError> {
    schema
        .validate()
        .map_err(|err| mapper(format!("Failed to parse mapping: {err}")))
}

/// The annotations of one declaration.
fn annotations_of(declared: &Declared, out: &mut BTreeMap<String, String>) {
    out.insert(
        format!("{ANN_FIELD}{}", declared.path),
        declared.annotation.clone(),
    );
    if let Some(similarity) = declared.similarity {
        out.insert(
            format!("{ANN_SIMILARITY}{}", declared.path),
            similarity.as_str().to_string(),
        );
    }
}

/// The schema of `PUT /{index}` (item 1–3).
pub fn plan_create(
    mappings: Option<&Value>,
    settings: Option<&Value>,
) -> Result<SchemaPlan, EsError> {
    let flat = match settings {
        Some(settings) => normalize_settings(settings)?,
        None => BTreeMap::new(),
    };
    let settings = check_settings(flat.clone())?;
    let (dynamic, props) = match mappings {
        Some(mappings) => parse_root(mappings)?,
        None => (None, None),
    };
    let dynamic = dynamic.unwrap_or(DynamicMapping::Map);
    let mut walk = Walk {
        settings: &flat,
        root_dynamic: dynamic,
        out: Vec::new(),
    };
    if let Some(props) = props {
        walk.properties(props, "")?;
    }
    let mut plan = SchemaPlan {
        fields: Vec::new(),
        vectors: Vec::new(),
        dynamic,
        max_fields: settings.max_fields,
        annotations: BTreeMap::new(),
        partitions: settings.partitions,
    };
    for declared in &walk.out {
        annotations_of(declared, &mut plan.annotations);
        match &declared.decl {
            Decl::Field { spec, .. } => plan.fields.push(spec.clone()),
            Decl::Vector { spec: Some(spec) } => plan.vectors.push(spec.clone()),
            Decl::Vector { spec: None } | Decl::Object { .. } => {}
        }
    }
    if !settings.stored.is_empty() {
        plan.annotations.insert(
            ANN_SETTINGS.to_string(),
            serde_json::to_string(&settings.stored).unwrap_or_default(),
        );
    }
    if plan.fields.len() + plan.vectors.len() > plan.max_fields as usize {
        return Err(fields_limit(plan.max_fields));
    }
    check_schema(&plan.schema())?;
    Ok(plan)
}

/// The ES type a mapped field shows: its declared type, else its kind's.
fn es_type_of(schema: &CollectionSchema, spec: &FieldSpec) -> String {
    declared_type(schema, &spec.name).unwrap_or_else(|| kind_type(&spec.kind).to_string())
}

/// The `type` of `path`'s annotation, if any.
fn declared_type(schema: &CollectionSchema, path: &str) -> Option<String> {
    let text = schema.annotations.get(&format!("{ANN_FIELD}{path}"))?;
    let value: Value = serde_json::from_str(text).ok()?;
    Some(
        value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("object")
            .to_string(),
    )
}

fn kind_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text { .. } => "text",
        FieldKind::Keyword | FieldKind::Uuid => "keyword",
        FieldKind::I64 => "long",
        FieldKind::F64 => "float",
        FieldKind::Bool => "boolean",
        FieldKind::Date => "date",
        FieldKind::Json => "flattened",
    }
}

fn type_change(path: &str, old: &str, new: &str) -> EsError {
    EsError::illegal_argument(format!(
        "mapper [{path}] cannot be changed from type [{old}] to [{new}]"
    ))
}

/// ES's conflict, with a `Cannot update parameter` line for each parameter
/// both declarations set to different values (row T11-3).
fn conflict(path: &str, old: Option<&String>, new: &str) -> EsError {
    let parse = |text: &str| serde_json::from_str::<Map<String, Value>>(text).unwrap_or_default();
    let mut reason = format!("Mapper for [{path}] conflicts with existing mapper");
    if let Some(old) = old {
        let (old, new) = (parse(old), parse(new));
        let mut first = true;
        for (key, before) in &old {
            if key == "type" {
                continue;
            }
            if let Some(after) = new.get(key)
                && after != before
            {
                reason.push_str(if first { ":" } else { "" });
                first = false;
                reason.push_str(&format!(
                    "\n\tCannot update parameter [{key}] from [{}] to [{}]",
                    bare(before),
                    bare(after)
                ));
            }
        }
    }
    EsError::illegal_argument(reason)
}

/// The `VectorSpec` of the pending `dense_vector` at `path` (declared
/// without `dims`, `declared` its stored JSON) once its first document gives
/// it `dim` dimensions (Task 4 step 4), with the `es.field.<path>`
/// annotation that now records `dims`, as ES's mapping then shows it.
pub fn pending_vector_spec(
    path: &str,
    declared: &Value,
    dim: usize,
) -> Result<(VectorSpec, String), EsError> {
    let mut def = declared.as_object().cloned().unwrap_or_default();
    def.insert("dims".to_string(), json!(dim));
    let settings = BTreeMap::new();
    let mut walk = Walk {
        settings: &settings,
        root_dynamic: DynamicMapping::Map,
        out: Vec::new(),
    };
    walk.vector(path, &def)?;
    match walk.out.pop() {
        Some(Declared {
            decl: Decl::Vector { spec: Some(spec) },
            annotation,
            ..
        }) => Ok((spec, annotation)),
        _ => Err(mapper(format!(
            "Failed to parse mapping: [dims] of field [{path}] must be set"
        ))),
    }
}

/// The additions of `PUT /{index}/_mapping` to `current` (item 4).
pub fn plan_put_mapping(
    current: &CollectionSchema,
    mappings: &Value,
) -> Result<SchemaPlan, EsError> {
    let flat = stored_settings(current);
    let (dynamic, props) = parse_root(mappings)?;
    if let Some(dynamic) = dynamic
        && dynamic != current.dynamic
    {
        return Err(EsError::unsupported(
            "changing [dynamic] after index creation",
        ));
    }
    let mut walk = Walk {
        settings: &flat,
        root_dynamic: current.dynamic,
        out: Vec::new(),
    };
    if let Some(props) = props {
        walk.properties(props, "")?;
    }
    let mut plan = SchemaPlan {
        fields: Vec::new(),
        vectors: Vec::new(),
        dynamic: current.dynamic,
        max_fields: current.max_fields,
        annotations: BTreeMap::new(),
        partitions: None,
    };
    for declared in &walk.out {
        let path = declared.path.as_str();
        let stored = current.annotations.get(&format!("{ANN_FIELD}{path}"));
        if stored == Some(&declared.annotation) {
            continue;
        }
        let existing_field = current.field(path);
        let existing_vector = current.vector(path).map(|(_, v)| v);
        match &declared.decl {
            Decl::Field { spec, es_type } => {
                if existing_vector.is_some() {
                    return Err(type_change(path, "dense_vector", es_type));
                }
                if let Some(existing) = existing_field {
                    let old = es_type_of(current, existing);
                    if &old != es_type {
                        return Err(type_change(path, &old, es_type));
                    }
                    if existing != spec || stored.is_some() {
                        return Err(conflict(path, stored, &declared.annotation));
                    }
                } else {
                    if let Some(old) = declared_type(current, path) {
                        return Err(type_change(path, &old, es_type));
                    }
                    plan.fields.push(spec.clone());
                }
            }
            Decl::Vector { spec } => {
                if let Some(existing) = existing_field {
                    return Err(type_change(
                        path,
                        &es_type_of(current, existing),
                        "dense_vector",
                    ));
                }
                match (existing_vector, spec) {
                    (Some(existing), Some(spec)) if existing == spec && stored.is_none() => {}
                    (Some(_), _) => return Err(conflict(path, stored, &declared.annotation)),
                    (None, spec) => {
                        match declared_type(current, path).as_deref() {
                            None | Some("dense_vector") => {}
                            Some(old) => return Err(type_change(path, old, "dense_vector")),
                        }
                        if let Some(spec) = spec {
                            plan.vectors.push(spec.clone());
                        }
                    }
                }
            }
            Decl::Object { enabled } => {
                if let Some(existing) = existing_field {
                    return Err(type_change(path, &es_type_of(current, existing), "object"));
                }
                if existing_vector.is_some() {
                    return Err(type_change(path, "dense_vector", "object"));
                }
                if let Some(text) = stored {
                    let old: Value = serde_json::from_str(text).unwrap_or(Value::Null);
                    let was_enabled = old.get("enabled").is_none_or(|v| {
                        v != &Value::Bool(false) && v != &Value::String("false".to_string())
                    });
                    match old.get("type").and_then(Value::as_str) {
                        None | Some("object") => {}
                        Some(t) => return Err(type_change(path, t, "object")),
                    }
                    if was_enabled != *enabled {
                        return Err(conflict(path, stored, &declared.annotation));
                    }
                    // Only `type: object` differs: keep what is stored.
                    continue;
                }
            }
        }
        annotations_of(declared, &mut plan.annotations);
    }
    let members = current.fields.len()
        + current.vectors.len()
        + current.sparse_vectors.len()
        + plan.fields.len()
        + plan.vectors.len();
    if members > current.max_fields as usize {
        return Err(fields_limit(current.max_fields));
    }
    let mut next = current.clone();
    next.fields.extend(plan.fields.iter().cloned());
    next.vectors.extend(plan.vectors.iter().cloned());
    next.annotations.extend(plan.annotations.clone());
    check_schema(&next)?;
    Ok(plan)
}

// ----- rendering -----

/// A node of the rendered mapping tree.
#[derive(Default)]
struct Node {
    /// A field or vector's JSON.
    leaf: Option<Map<String, Value>>,
    /// Multi-fields of a leaf.
    fields: BTreeMap<String, Value>,
    /// An object's declared JSON.
    object: Option<Map<String, Value>>,
    children: BTreeMap<String, Node>,
}

impl Node {
    fn at(&mut self, path: &str) -> &mut Node {
        path.split('.').fold(self, |node, segment| {
            node.children.entry(segment.to_string()).or_default()
        })
    }

    fn render(&self) -> Value {
        if let Some(leaf) = &self.leaf {
            let mut out = leaf.clone();
            if !self.fields.is_empty() {
                out.insert(
                    "fields".to_string(),
                    Value::Object(self.fields.clone().into_iter().collect()),
                );
            }
            return Value::Object(out);
        }
        let mut out = self.object.clone().unwrap_or_default();
        if !self.children.is_empty() {
            out.insert(
                "properties".to_string(),
                Node::render_children(&self.children),
            );
        }
        Value::Object(out)
    }

    fn render_children(children: &BTreeMap<String, Node>) -> Value {
        Value::Object(
            children
                .iter()
                .map(|(key, node)| (key.clone(), node.render()))
                .collect(),
        )
    }
}

/// The parsed annotation of `path`.
fn annotation_json(schema: &CollectionSchema, path: &str) -> Option<Map<String, Value>> {
    let text = schema.annotations.get(&format!("{ANN_FIELD}{path}"))?;
    match serde_json::from_str(text) {
        Ok(Value::Object(map)) => Some(map),
        _ => None,
    }
}

/// The JSON of an unannotated field (item 5).
fn default_field_json(spec: &FieldSpec, under_dynamic_text: bool) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("type".to_string(), json!(kind_type(&spec.kind)));
    match &spec.kind {
        FieldKind::Text { analyzer, .. } if analyzer != "standard" => {
            out.insert("analyzer".to_string(), json!(analyzer));
        }
        FieldKind::Keyword if under_dynamic_text => {
            out.insert("ignore_above".to_string(), json!(256));
        }
        _ => {}
    }
    out
}

/// The similarity of `vector`: its annotation, else its distance's.
fn vector_similarity(schema: &CollectionSchema, vector: &VectorSpec) -> EsSimilarity {
    schema
        .annotations
        .get(&format!("{ANN_SIMILARITY}{}", vector.name))
        .and_then(|name| EsSimilarity::parse(name))
        .unwrap_or_else(|| EsSimilarity::of_distance(vector.distance))
}

/// The multi-field sub name of `spec`, when its name is `<source_path>.<sub>`.
fn multi_field_sub(spec: &FieldSpec) -> Option<&str> {
    if spec.source_path.is_empty() {
        return None;
    }
    spec.name
        .strip_prefix(spec.source_path.as_str())?
        .strip_prefix('.')
}

/// `{"properties": …}` of `schema`, plus `"dynamic"` unless it is `true`
/// (item 5).
pub fn render_mappings(schema: &CollectionSchema) -> Value {
    let mut root = Node::default();
    let mut subs = Vec::new();
    for spec in &schema.fields {
        if multi_field_sub(spec).is_some() {
            subs.push(spec);
            continue;
        }
        let json =
            annotation_json(schema, &spec.name).unwrap_or_else(|| default_field_json(spec, false));
        root.at(&spec.name).leaf = Some(json);
    }
    for spec in subs {
        let sub = multi_field_sub(spec).unwrap_or_default();
        let parent_is_dynamic_text = schema.field(&spec.source_path).is_some_and(|parent| {
            matches!(parent.kind, FieldKind::Text { .. })
                && annotation_json(schema, &parent.name).is_none()
        });
        let json = annotation_json(schema, &spec.name).unwrap_or_else(|| {
            default_field_json(spec, parent_is_dynamic_text && sub == "keyword")
        });
        let parent = root.at(&spec.source_path);
        if parent.leaf.is_some() {
            parent.fields.insert(sub.to_string(), Value::Object(json));
        } else {
            root.at(&spec.name).leaf = Some(json);
        }
    }
    for vector in schema.vectors.iter().filter(|v| !v.name.is_empty()) {
        let json = annotation_json(schema, &vector.name).unwrap_or_else(|| {
            let mut out = Map::new();
            out.insert("type".to_string(), json!("dense_vector"));
            out.insert("dims".to_string(), json!(vector.dim));
            out.insert(
                "index".to_string(),
                json!(vector.index != VectorIndexSpec::None),
            );
            out.insert(
                "similarity".to_string(),
                json!(vector_similarity(schema, vector).as_str()),
            );
            out
        });
        root.at(&vector.name).leaf = Some(json);
    }
    // Declared objects and vectors still waiting for their `dims`.
    for (key, text) in schema.annotations.range(ANN_FIELD.to_string()..) {
        let Some(path) = key.strip_prefix(ANN_FIELD) else {
            break;
        };
        if schema.field(path).is_some() || schema.vector(path).is_some() {
            continue;
        }
        let Ok(Value::Object(json)) = serde_json::from_str::<Value>(text) else {
            continue;
        };
        let node = root.at(path);
        match json.get("type").and_then(Value::as_str) {
            None | Some("object") => node.object = Some(json),
            Some(_) => node.leaf = Some(json),
        }
    }
    let mut out = Map::new();
    match schema.dynamic {
        DynamicMapping::Map => {}
        DynamicMapping::Strict => {
            out.insert("dynamic".to_string(), json!("strict"));
        }
        DynamicMapping::Ignore => {
            out.insert("dynamic".to_string(), json!("false"));
        }
    }
    out.insert(
        "properties".to_string(),
        Node::render_children(&root.children),
    );
    Value::Object(out)
}

/// Inserts `value` at the dot path `path` of `map`, making objects on the
/// way.
fn insert_dotted(map: &mut Map<String, Value>, path: &str, value: Value) {
    match path.split_once('.') {
        None => {
            map.insert(path.to_string(), value);
        }
        Some((head, rest)) => {
            let entry = map
                .entry(head.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if !entry.is_object() {
                *entry = Value::Object(Map::new());
            }
            if let Value::Object(inner) = entry {
                insert_dotted(inner, rest, value);
            }
        }
    }
}

/// `{"index": {…}}` of an index (item 6); every value is a string.
pub fn render_settings(info: &CollectionInfo) -> Value {
    let mut index = Map::new();
    index.insert(
        "number_of_shards".to_string(),
        json!(info.partitions.to_string()),
    );
    index.insert("number_of_replicas".to_string(), json!("1"));
    index.insert("uuid".to_string(), json!(index_uuid(info.id)));
    index.insert(
        "creation_date".to_string(),
        json!(info.created_at_ms.to_string()),
    );
    index.insert("provided_name".to_string(), json!(info.name));
    index.insert("version".to_string(), json!({"created": VERSION_CREATED}));
    for (key, value) in stored_settings(&info.schema) {
        let path = key.strip_prefix("index.").unwrap_or(&key);
        insert_dotted(&mut index, path, Value::String(value));
    }
    json!({ "index": index })
}

/// base64url without padding of 8 zero bytes and the id as a big-endian
/// u64: 22 characters, stable for the life of the index.
pub fn index_uuid(id: CollectionId) -> String {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&id.0.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

// ----- the view the other endpoints read -----

/// An index as the gateway sees it: the collection and its ES-only facts.
#[derive(Clone, Debug)]
pub struct IndexView {
    pub name: String,
    pub info: CollectionInfo,
    pub es: EsMappingView,
}

/// The ES facts of an index's mapping.
#[derive(Clone, Debug, PartialEq)]
pub struct EsMappingView {
    /// Field path → ES type (`text`, `keyword`, `long`, `float`, `date`,
    /// `boolean`, `dense_vector`, …).
    pub es_types: BTreeMap<String, String>,
    /// Named vectors only (`""` is not addressable through ES).
    pub vectors: BTreeMap<String, VectorView>,
    /// `index.max_result_window`, else 10 000.
    pub max_result_window: usize,
    /// `index.default_pipeline` (Ruling 18).
    pub default_pipeline: Option<String>,
    /// `dense_vector` paths declared without `dims` (no `VectorSpec` yet).
    pub pending_vectors: BTreeMap<String, Value>,
    /// Objects declared with `enabled: false`: their paths stay in
    /// `_source` only.
    pub disabled_objects: BTreeSet<String>,
}

/// A `dense_vector` as ES sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorView {
    pub dim: u32,
    pub similarity: EsSimilarity,
    pub indexed: bool,
}

impl IndexView {
    pub fn new(info: CollectionInfo) -> IndexView {
        let schema = &info.schema;
        let mut es_types = BTreeMap::new();
        for spec in &schema.fields {
            es_types.insert(spec.name.clone(), es_type_of(schema, spec));
        }
        let mut vectors = BTreeMap::new();
        for vector in schema.vectors.iter().filter(|v| !v.name.is_empty()) {
            es_types.insert(vector.name.clone(), "dense_vector".to_string());
            vectors.insert(
                vector.name.clone(),
                VectorView {
                    dim: vector.dim,
                    similarity: vector_similarity(schema, vector),
                    indexed: vector.index != VectorIndexSpec::None,
                },
            );
        }
        let mut pending_vectors = BTreeMap::new();
        let mut disabled_objects = BTreeSet::new();
        for (key, text) in schema.annotations.range(ANN_FIELD.to_string()..) {
            let Some(path) = key.strip_prefix(ANN_FIELD) else {
                break;
            };
            if schema.field(path).is_some() || schema.vector(path).is_some() {
                continue;
            }
            let Ok(json) = serde_json::from_str::<Value>(text) else {
                continue;
            };
            match json.get("type").and_then(Value::as_str) {
                Some("dense_vector") => {
                    pending_vectors.insert(path.to_string(), json);
                }
                None | Some("object") => {
                    let disabled = json
                        .get("enabled")
                        .is_some_and(|v| v == &Value::Bool(false) || v == &json!("false"));
                    if disabled {
                        disabled_objects.insert(path.to_string());
                    }
                }
                Some(_) => {}
            }
        }
        let settings = stored_settings(schema);
        let max_result_window = settings
            .get("index.max_result_window")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_RESULT_WINDOW);
        let default_pipeline = settings.get("index.default_pipeline").cloned();
        IndexView {
            name: info.name.clone(),
            es: EsMappingView {
                es_types,
                vectors,
                max_result_window,
                default_pipeline,
                pending_vectors,
                disabled_objects,
            },
            info,
        }
    }

    /// The engine kind of the field at `path`.
    pub fn field_kind(&self, path: &str) -> Option<&FieldKind> {
        self.info.schema.field(path).map(|spec| &spec.kind)
    }
}

/// Removes the dot path `path` from `map`, whether its keys are nested
/// objects, dotted keys or arrays of objects.
fn remove_path(map: &mut Map<String, Value>, path: &str) {
    map.shift_remove(path);
    let mut split = path.len();
    while let Some(at) = path[..split].rfind('.') {
        let (head, rest) = (&path[..at], &path[at + 1..]);
        match map.get_mut(head) {
            Some(Value::Object(inner)) => remove_path(inner, rest),
            Some(Value::Array(items)) => {
                for item in items {
                    if let Value::Object(inner) = item {
                        remove_path(inner, rest);
                    }
                }
            }
            _ => {}
        }
        split = at;
    }
}

/// The object paths `schema` knows: declared objects and every proper
/// prefix of a mapped path.
fn known_objects(schema: &CollectionSchema) -> BTreeSet<String> {
    fn prefixes(known: &mut BTreeSet<String>, path: &str) {
        let mut at = 0;
        while let Some(dot) = path[at..].find('.') {
            known.insert(path[..at + dot].to_string());
            at += dot + 1;
        }
    }
    let mut known = BTreeSet::new();
    for spec in &schema.fields {
        prefixes(&mut known, &spec.source_path);
    }
    for vector in &schema.vectors {
        prefixes(&mut known, &vector.name);
    }
    for key in schema.annotations.keys() {
        if let Some(path) = key.strip_prefix(ANN_FIELD) {
            prefixes(&mut known, path);
            if schema.field(path).is_none() && schema.vector(path).is_none() {
                known.insert(path.to_string());
            }
        }
    }
    known
}

/// 400 `strict_dynamic_mapping_exception` for the unmapped `path`: the
/// first segment below the deepest object the mapping knows.
fn strict_error(schema: &CollectionSchema, path: &str) -> EsError {
    let known = known_objects(schema);
    let segments: Vec<&str> = path.split('.').collect();
    let mut depth = 0;
    for i in (1..segments.len()).rev() {
        if known.contains(&segments[..i].join(".")) {
            depth = i;
            break;
        }
    }
    let within = if depth == 0 {
        "_doc".to_string()
    } else {
        segments[..depth].join(".")
    };
    EsError::new(
        400,
        "strict_dynamic_mapping_exception",
        format!(
            "[1:1] mapping set to strict, dynamic introduction of [{}] within [{within}] is not \
             allowed",
            segments[depth]
        ),
    )
}

/// The fields dynamic mapping adds for `source` (item 7): M1.1's
/// `unmapped_paths` and `propose_dynamic_fields`, after the paths of
/// vectors and disabled objects are taken out.
pub fn dynamic_plan(
    view: &IndexView,
    source: &Map<String, Value>,
) -> Result<Vec<FieldSpec>, EsError> {
    let schema = &view.info.schema;
    let mut source = source.clone();
    let skipped = schema
        .vectors
        .iter()
        .map(|v| &v.name)
        .chain(view.es.pending_vectors.keys())
        .chain(view.es.disabled_objects.iter());
    for path in skipped {
        remove_path(&mut source, path);
    }
    match schema.dynamic {
        DynamicMapping::Ignore => Ok(Vec::new()),
        DynamicMapping::Strict => match unmapped_paths(schema, &source).first() {
            Some(path) => Err(strict_error(schema, path)),
            None => Ok(Vec::new()),
        },
        DynamicMapping::Map => match propose_dynamic_fields(schema, &[&source]) {
            Ok(fields) => Ok(fields),
            Err(DynamicMappingError::TooManyFields { limit }) => {
                let mut unlimited = schema.clone();
                unlimited.max_fields = u32::MAX;
                let n = propose_dynamic_fields(&unlimited, &[&source])
                    .map(|fields| fields.len())
                    .unwrap_or_default();
                let why = format!(
                    "Limit of total fields [{limit}] has been exceeded while adding new fields \
                     [{n}]"
                );
                Err(EsError::new(
                    400,
                    "document_parsing_exception",
                    format!("[1:1] failed to parse: {why}"),
                )
                .with(
                    "caused_by",
                    json!({"type": "illegal_argument_exception", "reason": why}),
                ))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_uuids_are_22_url_safe_characters() {
        let a = index_uuid(CollectionId(1));
        assert_eq!(a.len(), 22);
        assert_eq!(a, "AAAAAAAAAAAAAAAAAAAAAQ");
        assert_ne!(index_uuid(CollectionId(2)), a);
        assert!(
            index_uuid(CollectionId(u64::MAX))
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }

    #[test]
    fn paths_are_removed_through_objects_arrays_and_dotted_keys() {
        let mut source = json!({
            "a": {"b": 1, "c": 2},
            "x.y": 3,
            "list": [{"v": 1, "w": 2}, {"v": 3}],
        })
        .as_object()
        .cloned()
        .unwrap_or_default();
        remove_path(&mut source, "a.b");
        remove_path(&mut source, "x.y");
        remove_path(&mut source, "list.v");
        assert_eq!(
            Value::Object(source),
            json!({"a": {"c": 2}, "list": [{"w": 2}, {}]})
        );
    }
}
