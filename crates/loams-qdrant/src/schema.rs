//! Collections (Task 3): `CreateCollection` to `CollectionSchema`, the
//! `CollectionInfo` JSON, the alias rules, and the executors REST and gRPC
//! share.

use std::collections::BTreeMap;

use loams_collection::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, MAX_VECTOR_DIM, Quantization, SparseModifier, SparseVectorSpec, VectorElement,
    VectorIndexSpec, VectorSpec,
};
use loams_query::{AliasAction, CollectionInfo, Query, ServiceError};
use serde_json::{Map, Value, json};

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::jsonpath::JsonPath;
use crate::model::collections::{
    AliasDescription, AliasOperation, AliasesResponse, CollectionDescription, CollectionExistence,
    CollectionsResponse, CreateCollection, CreateFieldIndex, DistanceName, HnswConfigDiff,
    ModifierName, PayloadFieldSchema, QuantizationConfig, SparseVectorParams, VectorParams,
    VectorsConfig,
};
use crate::model::common::{UpdateResult, UpdateStatus};
use crate::{EXT_CREATE, PAYLOAD_FIELD, PAYLOAD_INDEX_PREFIX, QdrantGateway};

/// The annotation prefix of a vector added after creation: its request
/// body, for echo in `GET /collections/{c}` (row T3-3).
pub const EXT_VECTOR_PREFIX: &str = "qdrant.vector.";
/// The longest annotation value (`CollectionSchema` rule).
const MAX_ANNOTATION_BYTES: usize = 65_536;
/// The analyzer of the `payload` field's text companion (Ruling 6).
const STANDARD_ANALYZER: &str = "standard";

// ----- create -----

/// The schema of a collection created through the gateway (Task 3 step
/// 1.3). `raw` is the request body; it is kept as `annotations[EXT_CREATE]`
/// for echo (Ruling 16).
pub fn schema_from_create(
    _name: &str,
    req: &CreateCollection,
    raw: &Value,
) -> Result<CollectionSchema, GatewayError> {
    let mut sparse = Vec::new();
    for (name, params) in req.sparse_vectors.iter().flatten() {
        sparse.push(sparse_vector_spec(name, params)?);
    }
    let dense: Vec<(String, &VectorParams)> = match &req.vectors {
        None => Vec::new(),
        Some(VectorsConfig::Single(params)) => vec![(String::new(), params)],
        Some(VectorsConfig::Map(map)) => map.iter().map(|(n, p)| (n.clone(), p)).collect(),
    };
    for (name, _) in &dense {
        if sparse.iter().any(|s| &s.name == name) {
            return Err(GatewayError::BadRequest(format!(
                "Vector name {name} is used by a dense and a sparse vector"
            )));
        }
    }
    let mut vectors = Vec::with_capacity(dense.len());
    for (name, params) in &dense {
        vectors.push(vector_spec(name, params, req)?);
    }
    if req.sharding_method.as_deref() == Some("custom") {
        return Err(GatewayError::Unsupported("custom sharding".to_string()));
    }
    if req.init_from.as_ref().is_some_and(|v| !v.is_null()) {
        return Err(GatewayError::Unsupported("init_from".to_string()));
    }
    if let Some(q) = &req.quantization_config {
        quantization(q)?;
    }
    vectors.sort_by(|a, b| a.name.cmp(&b.name));
    sparse.sort_by(|a, b| a.name.cmp(&b.name));
    let body = raw.to_string();
    if body.len() > MAX_ANNOTATION_BYTES {
        return Err(GatewayError::BadRequest(
            "collection config too large".to_string(),
        ));
    }
    let mut schema = CollectionSchema::new(vec![payload_field()], vectors, DynamicMapping::Ignore)
        .with_sparse_vectors(sparse);
    schema.max_fields = DEFAULT_MAX_FIELDS;
    schema.annotations.insert(EXT_CREATE.to_string(), body);
    Ok(schema)
}

/// The catch-all JSON field over the whole payload (Ruling 5).
fn payload_field() -> FieldSpec {
    FieldSpec {
        name: PAYLOAD_FIELD.to_string(),
        source_path: String::new(),
        kind: FieldKind::Json,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

/// One dense vector: HNSW is Qdrant's defaults overlaid with the
/// collection's and then the vector's `hnsw_config`; quantization is the
/// vector's, else the collection's.
pub fn vector_spec(
    name: &str,
    params: &VectorParams,
    collection: &CreateCollection,
) -> Result<VectorSpec, GatewayError> {
    if params
        .multivector_config
        .as_ref()
        .is_some_and(|v| !v.is_null())
    {
        return Err(GatewayError::Unsupported("multivectors".to_string()));
    }
    if let Some(datatype) = params.datatype.as_deref().filter(|d| *d != "float32") {
        return Err(GatewayError::Unsupported(format!("datatype {datatype}")));
    }
    let dim = u32::try_from(params.size)
        .ok()
        .filter(|d| (1..=MAX_VECTOR_DIM).contains(d))
        .ok_or_else(|| GatewayError::BadRequest("Vector size must be in 1..=65536".to_string()))?;
    let mut hnsw = HnswParams::default();
    for diff in [&collection.hnsw_config, &params.hnsw_config]
        .into_iter()
        .flatten()
    {
        overlay_hnsw(&mut hnsw, diff);
    }
    let quantization = params
        .quantization_config
        .as_ref()
        .or(collection.quantization_config.as_ref())
        .map(quantization)
        .transpose()?;
    Ok(VectorSpec {
        name: name.to_string(),
        dim,
        distance: distance(params.distance),
        element: VectorElement::F32,
        index: VectorIndexSpec::Auto,
        hnsw,
        quantization,
    })
}

fn overlay_hnsw(hnsw: &mut HnswParams, diff: &HnswConfigDiff) {
    if let Some(m) = diff.m {
        hnsw.m = m;
    }
    if let Some(ef) = diff.ef_construct {
        hnsw.ef_construct = ef;
    }
    // Qdrant's value is already in KB.
    if let Some(t) = diff.full_scan_threshold {
        hnsw.full_scan_threshold_kb = t;
    }
    if let Some(p) = diff.payload_m {
        hnsw.payload_m = Some(p);
    }
    if let Some(on_disk) = diff.on_disk {
        hnsw.on_disk = on_disk;
    }
}

fn distance(d: DistanceName) -> Distance {
    match d {
        DistanceName::Cosine => Distance::Cosine,
        DistanceName::Euclid => Distance::Euclid,
        DistanceName::Dot => Distance::Dot,
        DistanceName::Manhattan => Distance::Manhattan,
    }
}

fn distance_name(d: Distance) -> &'static str {
    match d {
        Distance::Cosine => "Cosine",
        Distance::Euclid => "Euclid",
        Distance::Dot => "Dot",
        Distance::Manhattan => "Manhattan",
    }
}

/// Scalar, product or binary quantization for the hot tier; any other kind
/// is unsupported.
fn quantization(q: &QuantizationConfig) -> Result<Quantization, GatewayError> {
    match q {
        QuantizationConfig::Scalar { scalar } => {
            if scalar.r#type != "int8" {
                return Err(GatewayError::json(format!(
                    "unknown scalar quantization type {}",
                    scalar.r#type
                )));
            }
            let quantile_ppm = match scalar.quantile {
                None => None,
                Some(q) if (0.5..=1.0).contains(&q) => Some((f64::from(q) * 1e6).round() as u32),
                Some(_) => {
                    return Err(GatewayError::BadRequest(
                        "quantile must be in 0.5..=1.0".to_string(),
                    ));
                }
            };
            Ok(Quantization::Scalar {
                quantile_ppm,
                always_ram: scalar.always_ram.unwrap_or(false),
            })
        }
        QuantizationConfig::Product { product } => {
            let ratio = product
                .compression
                .strip_prefix('x')
                .and_then(|n| n.parse::<u32>().ok())
                .filter(|n| [4, 8, 16, 32, 64].contains(n))
                .ok_or_else(|| {
                    GatewayError::json(format!("unknown compression ratio {}", product.compression))
                })?;
            Ok(Quantization::Product {
                compression_ratio: ratio,
                always_ram: product.always_ram.unwrap_or(false),
            })
        }
        QuantizationConfig::Binary { binary } => Ok(Quantization::Binary {
            always_ram: binary.always_ram.unwrap_or(false),
        }),
        QuantizationConfig::Other(value) => {
            let kind = value
                .as_object()
                .and_then(|o| o.keys().next().cloned())
                .unwrap_or_else(|| "config".to_string());
            if ["scalar", "product", "binary"].contains(&kind.as_str()) {
                // A known kind whose body did not parse.
                return Err(GatewayError::json(format!("invalid {kind} quantization")));
            }
            Err(GatewayError::Unsupported(format!("quantization {kind}")))
        }
    }
}

/// One sparse vector (Ruling 21): `modifier` is kept, the index options
/// change nothing; a weight `datatype` other than `float32` is unsupported.
pub fn sparse_vector_spec(
    name: &str,
    params: &SparseVectorParams,
) -> Result<SparseVectorSpec, GatewayError> {
    if let Some(datatype) = params
        .index
        .as_ref()
        .and_then(|i| i.datatype.as_deref())
        .filter(|d| *d != "float32")
    {
        return Err(GatewayError::Unsupported(format!(
            "sparse datatype {datatype}"
        )));
    }
    if name.is_empty() {
        return Err(GatewayError::BadRequest(
            "Sparse vector name must not be empty".to_string(),
        ));
    }
    Ok(SparseVectorSpec {
        name: name.to_string(),
        modifier: match params.modifier {
            Some(ModifierName::Idf) => SparseModifier::Idf,
            Some(ModifierName::None) | None => SparseModifier::None,
        },
    })
}

/// The payload-index fields (`payload_index.*`) with their Qdrant keys.
pub fn payload_index_fields(schema: &CollectionSchema) -> Vec<(String, &FieldSpec)> {
    schema
        .fields
        .iter()
        .filter_map(|f| {
            f.name
                .strip_prefix(PAYLOAD_INDEX_PREFIX)
                .map(|key| (key.to_string(), f))
        })
        .collect()
}

/// The typed field of a payload index on `key` (Task 4 step 3.2):
/// `payload_index.<k>` over source path `<k>`, where `<k>` is the key
/// normalized. Every kind is lenient (Ruling 5); a `text` index is
/// accepted only in the configuration whose matching the `payload`
/// field reproduces (Ruling 6).
pub fn payload_index_field(
    key: &str,
    schema: &PayloadFieldSchema,
) -> Result<FieldSpec, GatewayError> {
    let path: JsonPath = key.parse()?;
    let normalized = path.normalized()?;
    let (type_name, params) = match schema {
        PayloadFieldSchema::Name(name) => (name.as_str(), None),
        PayloadFieldSchema::Params(params) => match params.get("type").and_then(Value::as_str) {
            Some(name) => (name, Some(params)),
            None => return Err(GatewayError::json("field_schema.type is required")),
        },
    };
    let kind = match type_name {
        "keyword" => FieldKind::Keyword,
        "integer" => FieldKind::I64,
        "float" => FieldKind::F64,
        "bool" => FieldKind::Bool,
        "datetime" => FieldKind::Date,
        "uuid" => FieldKind::Uuid,
        "text" => text_index_kind(params)?,
        "geo" => return Err(GatewayError::Unsupported("geo index".to_string())),
        other => {
            return Err(GatewayError::json(format!(
                "unknown payload index type `{other}`"
            )));
        }
    };
    let fast = !matches!(kind, FieldKind::Text { .. });
    Ok(FieldSpec {
        name: format!("{PAYLOAD_INDEX_PREFIX}{normalized}"),
        source_path: normalized,
        kind,
        indexed: true,
        fast,
        ignore_malformed: true,
    })
}

/// A `text` index: tokenizer `word` with `lowercase` unset or true, and
/// none of the options that change matching (Task 4 step 3.2).
fn text_index_kind(params: Option<&Map<String, Value>>) -> Result<FieldKind, GatewayError> {
    let empty = Map::new();
    let params = params.unwrap_or(&empty);
    let given = |key: &str| params.get(key).filter(|v| !v.is_null());
    let refuse = |option: String| {
        Err(GatewayError::Unsupported(format!(
            "text index option {option}"
        )))
    };
    match given("tokenizer") {
        None => {}
        Some(Value::String(t)) if t == "word" => {}
        Some(other) => return refuse(format!("tokenizer {}", other.as_str().unwrap_or("?"))),
    }
    if given("lowercase").is_some_and(|v| v != &Value::Bool(true)) {
        return refuse("lowercase: false".to_string());
    }
    for option in ["min_token_len", "max_token_len", "stopwords", "stemmer"] {
        if given(option).is_some() {
            return refuse(option.to_string());
        }
    }
    if given("ascii_folding") == Some(&Value::Bool(true)) {
        return refuse("ascii_folding".to_string());
    }
    Ok(FieldKind::Text {
        analyzer: STANDARD_ANALYZER.to_string(),
        positions: given("phrase_matching") == Some(&Value::Bool(true)),
    })
}

/// The stored create request of a collection made through the gateway.
fn stored_create(schema: &CollectionSchema) -> Option<Value> {
    schema
        .annotations
        .get(EXT_CREATE)
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .filter(Value::is_object)
}

// ----- info -----

/// Copies `keys` of `v` that are present and not null, in `keys` order.
fn pick(v: &Value, keys: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    if let Some(obj) = v.as_object() {
        for key in keys {
            if let Some(value) = obj.get(*key).filter(|x| !x.is_null()) {
                out.insert((*key).to_string(), value.clone());
            }
        }
    }
    out
}

const HNSW_KEYS: [&str; 6] = [
    "m",
    "ef_construct",
    "full_scan_threshold",
    "max_indexing_threads",
    "on_disk",
    "payload_m",
];

/// A quantization config within the 1.15 forbid fields.
fn quantization_echo(v: &Value) -> Option<Value> {
    let obj = v.as_object()?;
    let (kind, keys): (&str, &[&str]) = if obj.contains_key("scalar") {
        ("scalar", &["type", "quantile", "always_ram"])
    } else if obj.contains_key("product") {
        ("product", &["compression", "always_ram"])
    } else if obj.contains_key("binary") {
        ("binary", &["always_ram", "encoding", "query_encoding"])
    } else {
        return None;
    };
    Some(json!({ kind: pick(&obj[kind], keys) }))
}

/// A vector's parameters within the 1.15 forbid fields.
fn vector_echo(v: &Value) -> Value {
    let mut out = pick(v, &["size", "distance"]);
    if let Some(hnsw) = v.get("hnsw_config").filter(|x| x.is_object()) {
        out.insert("hnsw_config".into(), Value::Object(pick(hnsw, &HNSW_KEYS)));
    }
    if let Some(q) = v.get("quantization_config").and_then(quantization_echo) {
        out.insert("quantization_config".into(), q);
    }
    out.extend(pick(v, &["on_disk", "datatype"]));
    Value::Object(out)
}

fn vector_from_spec(spec: &VectorSpec) -> Value {
    json!({"size": spec.dim, "distance": distance_name(spec.distance)})
}

/// `config.params.vectors`: the create's form (single or map), plus the
/// vectors added later; from the schema for a collection not created
/// through the gateway.
fn vectors_json(schema: &CollectionSchema, stored: Option<&Value>) -> Value {
    let Some(stored) = stored else {
        return match schema.vectors.as_slice() {
            [only] if only.name.is_empty() => vector_from_spec(only),
            all => Value::Object(
                all.iter()
                    .map(|s| (s.name.clone(), vector_from_spec(s)))
                    .collect(),
            ),
        };
    };
    let raw = stored.get("vectors").filter(|v| !v.is_null());
    let form = raw.and_then(|v| serde_json::from_value::<VectorsConfig>(v.clone()).ok());
    let mut single = None;
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    match (form, raw) {
        (Some(VectorsConfig::Single(_)), Some(raw)) => single = Some(vector_echo(raw)),
        (Some(VectorsConfig::Map(_)), Some(Value::Object(raw))) => {
            for (name, v) in raw {
                map.insert(name.clone(), vector_echo(v));
            }
        }
        _ => {}
    }
    let known = |name: &str| map.contains_key(name) || (single.is_some() && name.is_empty());
    let added: Vec<(String, Value)> = schema
        .vectors
        .iter()
        .filter(|s| !known(&s.name))
        .map(|s| {
            let body = schema
                .annotations
                .get(&format!("{EXT_VECTOR_PREFIX}{}", s.name))
                .and_then(|b| serde_json::from_str::<Value>(b).ok());
            let echo = body
                .as_ref()
                .map_or_else(|| vector_from_spec(s), vector_echo);
            (s.name.clone(), echo)
        })
        .collect();
    match single {
        Some(single) if added.is_empty() => single,
        single => {
            if let Some(single) = single {
                map.insert(String::new(), single);
            }
            map.extend(added);
            Value::Object(map.into_iter().collect())
        }
    }
}

/// `config.params.sparse_vectors`: present iff the create carried a
/// non-null `sparse_vectors`, each entry as sent within the 1.15 forbid
/// fields; from the schema for a collection not created through the
/// gateway.
fn sparse_json(schema: &CollectionSchema, stored: Option<&Value>) -> Option<Value> {
    let Some(stored) = stored else {
        if schema.sparse_vectors.is_empty() {
            return None;
        }
        return Some(Value::Object(
            schema
                .sparse_vectors
                .iter()
                .map(|s| {
                    let v = match s.modifier {
                        SparseModifier::Idf => json!({"modifier": "idf"}),
                        SparseModifier::None => json!({}),
                    };
                    (s.name.clone(), v)
                })
                .collect(),
        ));
    };
    let raw = stored.get("sparse_vectors")?.as_object()?;
    let mut sorted: Vec<(&String, &Value)> = raw.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    Some(Value::Object(
        sorted
            .into_iter()
            .map(|(name, v)| {
                let mut out = Map::new();
                if let Some(index) = v.get("index").filter(|x| x.is_object()) {
                    out.insert(
                        "index".into(),
                        Value::Object(pick(index, &["full_scan_threshold", "on_disk", "datatype"])),
                    );
                }
                out.extend(pick(v, &["modifier"]));
                (name.clone(), Value::Object(out))
            })
            .collect(),
    ))
}

/// `defaults` overlaid with the object `given` (every key it has).
fn overlay(defaults: Value, given: Option<&Value>) -> Value {
    let mut out = defaults;
    if let (Some(out), Some(Value::Object(given))) = (out.as_object_mut(), given) {
        for (k, v) in given {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

fn data_type(kind: &FieldKind) -> Option<&'static str> {
    Some(match kind {
        FieldKind::Keyword => "keyword",
        FieldKind::I64 => "integer",
        FieldKind::F64 => "float",
        FieldKind::Bool => "bool",
        FieldKind::Date => "datetime",
        FieldKind::Uuid => "uuid",
        FieldKind::Text { .. } => "text",
        FieldKind::Json => return None,
    })
}

/// `GET /collections/{c}`'s result (Task 3 step 2). `payload_points` holds
/// the number of points with each payload-index key.
pub fn collection_info_json(
    info: &CollectionInfo,
    points_count: u64,
    payload_points: &BTreeMap<String, u64>,
) -> Value {
    let schema = &info.schema;
    let stored = stored_create(schema);
    let req = stored.clone().unwrap_or_else(|| json!({}));
    let given = |key: &str| req.get(key).filter(|v| !v.is_null());

    let mut params = Map::new();
    params.insert("vectors".into(), vectors_json(schema, stored.as_ref()));
    params.insert(
        "shard_number".into(),
        given("shard_number").cloned().unwrap_or(json!(1)),
    );
    if let Some(method) = given("sharding_method") {
        params.insert("sharding_method".into(), method.clone());
    }
    for key in ["replication_factor", "write_consistency_factor"] {
        params.insert(key.into(), given(key).cloned().unwrap_or(json!(1)));
    }
    params.insert(
        "on_disk_payload".into(),
        given("on_disk_payload").cloned().unwrap_or(json!(true)),
    );
    if let Some(sparse) = sparse_json(schema, stored.as_ref()) {
        params.insert("sparse_vectors".into(), sparse);
    }

    let mut hnsw = json!({
        "m": 16, "ef_construct": 100, "full_scan_threshold": 10000,
        "max_indexing_threads": 0, "on_disk": false,
    });
    if let (Some(out), Some(h)) = (hnsw.as_object_mut(), given("hnsw_config")) {
        out.extend(pick(h, &HNSW_KEYS));
    }
    let optimizer = overlay(
        json!({
            "deleted_threshold": 0.2, "vacuum_min_vector_number": 1000,
            "default_segment_number": 0, "max_segment_size": null, "memmap_threshold": null,
            "indexing_threshold": 10000, "flush_interval_sec": 5, "max_optimization_threads": null,
        }),
        given("optimizers_config"),
    );
    let wal = overlay(
        json!({"wal_capacity_mb": 32, "wal_segments_ahead": 0}),
        given("wal_config"),
    );
    let mut config = Map::new();
    config.insert("params".into(), Value::Object(params));
    config.insert("hnsw_config".into(), hnsw);
    config.insert("optimizer_config".into(), optimizer);
    config.insert("wal_config".into(), wal);
    if let Some(q) = given("quantization_config").and_then(quantization_echo) {
        config.insert("quantization_config".into(), q);
    }
    for key in ["strict_mode_config", "metadata"] {
        if let Some(v) = given(key) {
            config.insert(key.into(), v.clone());
        }
    }

    let mut payload_schema = Map::new();
    for (key, field) in payload_index_fields(schema) {
        let Some(data_type) = data_type(&field.kind) else {
            continue;
        };
        let mut entry = Map::new();
        entry.insert("data_type".into(), json!(data_type));
        if let FieldKind::Text { positions, .. } = &field.kind {
            entry.insert(
                "params".into(),
                json!({"type": "text", "tokenizer": "word", "lowercase": true,
                       "phrase_matching": positions}),
            );
        }
        entry.insert(
            "points".into(),
            json!(payload_points.get(&key).copied().unwrap_or(0)),
        );
        payload_schema.insert(key, Value::Object(entry));
    }

    json!({
        "status": "green",
        "optimizer_status": "ok",
        "segments_count": 1,
        "points_count": points_count,
        "indexed_vectors_count": points_count,
        "config": config,
        "payload_schema": payload_schema,
    })
}

// ----- aliases -----

/// One `(alias, collection)` pair per alias member, sorted by alias and
/// then collection name, from `CollectionInfo.aliases` (Task 3 step 4).
pub fn alias_pairs(infos: &[CollectionInfo]) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = infos
        .iter()
        .flat_map(|info| {
            info.aliases
                .iter()
                .map(|alias| (alias.clone(), info.name.clone()))
        })
        .collect();
    pairs.sort();
    pairs
}

/// The `update_aliases` actions of a request, in order, over the alias
/// state `pairs` (updated as each operation is compiled, so a later
/// operation sees an earlier one). A delete or rename of a missing alias is
/// `NotFound` (Qdrant's `Alias <name> does not exists!`, row T3-2), a
/// rename of an alias with several members `InvalidArgument`; either fails
/// the whole request.
pub fn alias_actions(
    ops: &[AliasOperation],
    pairs: &[(String, String)],
) -> Result<Vec<AliasAction>, GatewayError> {
    let mut state = AliasState {
        pairs: pairs.to_vec(),
        actions: Vec::new(),
    };
    for op in ops {
        match op {
            AliasOperation::Create { create_alias } => {
                state.create(&create_alias.alias_name, &create_alias.collection_name);
            }
            AliasOperation::Delete { delete_alias } => {
                let alias = &delete_alias.alias_name;
                if !state.pairs.iter().any(|(a, _)| a == alias) {
                    return Err(alias_not_found(alias));
                }
                state.delete(alias);
            }
            AliasOperation::Rename { rename_alias } => {
                let old = &rename_alias.old_alias_name;
                let members: Vec<&str> = state
                    .pairs
                    .iter()
                    .filter(|(a, _)| a == old)
                    .map(|(_, c)| c.as_str())
                    .collect();
                let collection = match members.as_slice() {
                    [] => return Err(alias_not_found(old)),
                    [one] => (*one).to_string(),
                    many => {
                        return Err(GatewayError::BadRequest(format!(
                            "alias [{old}] names {} collections [{}]; this operation needs one collection",
                            many.len(),
                            many.join(", ")
                        )));
                    }
                };
                state.delete(old);
                state.create(&rename_alias.new_alias_name, &collection);
            }
        }
    }
    Ok(state.actions)
}

fn alias_not_found(alias: &str) -> GatewayError {
    GatewayError::Service(ServiceError::NotFound {
        kind: "alias",
        name: alias.to_string(),
    })
}

/// The alias pairs as the actions compiled so far leave them.
struct AliasState {
    pairs: Vec<(String, String)>,
    actions: Vec<AliasAction>,
}

impl AliasState {
    /// `Delete` first when any pair names the alias, so a multi-target
    /// alias becomes a single-target one.
    fn create(&mut self, alias: &str, collection: &str) {
        if self.pairs.iter().any(|(a, _)| a == alias) {
            self.pairs.retain(|(a, _)| a != alias);
            self.actions.push(AliasAction::Delete {
                alias: alias.to_string(),
            });
        }
        self.pairs.push((alias.to_string(), collection.to_string()));
        self.actions.push(AliasAction::Create {
            alias: alias.to_string(),
            collection: collection.to_string(),
        });
    }

    fn delete(&mut self, alias: &str) {
        self.pairs.retain(|(a, _)| a != alias);
        self.actions.push(AliasAction::Delete {
            alias: alias.to_string(),
        });
    }
}

/// `{aliases: [{alias_name, collection_name}]}` over `pairs` (a
/// multi-target alias once per member), those of `collection` only when
/// given.
pub fn aliases_response(pairs: &[(String, String)], collection: Option<&str>) -> AliasesResponse {
    AliasesResponse {
        aliases: pairs
            .iter()
            .filter(|(_, c)| collection.is_none_or(|want| want == c))
            .map(|(a, c)| AliasDescription {
                alias_name: a.clone(),
                collection_name: c.clone(),
            })
            .collect(),
    }
}

// ----- executors -----

/// The collections of the request's namespace, by name (aliases are not
/// listed).
pub(crate) async fn list_collections(
    gw: QdrantGateway,
    ctx: RequestCtx,
) -> Result<CollectionsResponse, GatewayError> {
    let infos = gw.service().list_collections(&ctx.ns).await?;
    Ok(CollectionsResponse {
        collections: infos
            .into_iter()
            .map(|info| CollectionDescription { name: info.name })
            .collect(),
    })
}

/// `Ok(true)` when `name` names a collection or an alias.
async fn exists(gw: &QdrantGateway, ctx: &RequestCtx, name: &str) -> Result<bool, GatewayError> {
    match gw.service().get_collection(&ctx.ns, name).await {
        Ok(_) => Ok(true),
        Err(ServiceError::NotFound { .. }) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Create (step 1): an existing name is 409 (Ruling 17); `raw` is the
/// request body.
pub(crate) async fn create_collection(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
    raw: Value,
) -> Result<bool, GatewayError> {
    let req: CreateCollection =
        serde_json::from_value(raw.clone()).map_err(|e| GatewayError::json(e.to_string()))?;
    if exists(&gw, &ctx, &name).await? {
        return Err(GatewayError::CollectionExists(name));
    }
    let schema = schema_from_create(&name, &req, &raw)?;
    gw.service()
        .create_collection(&ctx.ns, &name, schema, None)
        .await?;
    Ok(true)
}

/// Info (step 2).
pub(crate) async fn collection_info(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
) -> Result<Value, GatewayError> {
    let service = gw.service();
    let info = service.get_collection(&ctx.ns, &name).await?;
    let points = service
        .count(&ctx.ns, &name, None, ctx.consistency.clone())
        .await?;
    let mut payload_points = BTreeMap::new();
    for (key, _) in payload_index_fields(&info.schema) {
        let filter = Query::Exists {
            field: format!("{PAYLOAD_FIELD}.{key}"),
        };
        let n = service
            .count(&ctx.ns, &name, Some(filter), ctx.consistency.clone())
            .await?;
        payload_points.insert(key, n);
    }
    Ok(collection_info_json(&info, points, &payload_points))
}

/// Delete (step 3): `false` when no collection has the name; an alias
/// name deletes nothing, as in Qdrant.
pub(crate) async fn delete_collection(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
) -> Result<bool, GatewayError> {
    Ok(gw.service().drop_collection(&ctx.ns, &name).await?)
}

/// Exists: a collection or an alias of that name.
pub(crate) async fn collection_exists(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
) -> Result<CollectionExistence, GatewayError> {
    Ok(CollectionExistence {
        exists: exists(&gw, &ctx, &name).await?,
    })
}

/// Every alias pair of the namespace.
async fn namespace_pairs(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
) -> Result<Vec<(String, String)>, GatewayError> {
    Ok(alias_pairs(&gw.service().list_collections(&ctx.ns).await?))
}

/// Aliases (step 4): every operation of the request in one atomic
/// `update_aliases` call.
pub(crate) async fn update_aliases(
    gw: QdrantGateway,
    ctx: RequestCtx,
    ops: Vec<AliasOperation>,
) -> Result<bool, GatewayError> {
    let pairs = namespace_pairs(&gw, &ctx).await?;
    let actions = alias_actions(&ops, &pairs)?;
    gw.service().update_aliases(&ctx.ns, actions).await?;
    Ok(true)
}

/// Every alias pair of the namespace, sorted.
pub(crate) async fn list_aliases(
    gw: QdrantGateway,
    ctx: RequestCtx,
) -> Result<AliasesResponse, GatewayError> {
    Ok(aliases_response(&namespace_pairs(&gw, &ctx).await?, None))
}

/// The aliases that have the collection (named, or through an alias) as a
/// member.
pub(crate) async fn collection_aliases(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
) -> Result<AliasesResponse, GatewayError> {
    let info = gw.service().get_collection(&ctx.ns, &name).await?;
    let pairs = alias_pairs(std::slice::from_ref(&info));
    Ok(aliases_response(&pairs, Some(&info.name)))
}

/// `PATCH /collections/{c}` (Ruling 16): the allowed no-op settings answer
/// `true`, any other key is unsupported.
pub(crate) async fn update_collection(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
    body: Value,
) -> Result<bool, GatewayError> {
    gw.service().get_collection(&ctx.ns, &name).await?;
    check_update(&body)?;
    Ok(true)
}

/// Ruling 16's no-op subset of an update.
pub fn check_update(body: &Value) -> Result<(), GatewayError> {
    let Some(obj) = body.as_object() else {
        return Err(GatewayError::json("expected an object"));
    };
    let refuse = |key: &str| {
        Err(GatewayError::Unsupported(format!(
            "collection update {key}"
        )))
    };
    for (key, value) in obj.iter().filter(|(_, v)| !v.is_null()) {
        match key.as_str() {
            "optimizers_config" | "strict_mode_config" => {}
            "params" => {
                for (sub, v) in value.as_object().into_iter().flatten() {
                    let allowed = [
                        "replication_factor",
                        "write_consistency_factor",
                        "read_fan_out_factor",
                        "on_disk_payload",
                    ];
                    if !v.is_null() && !allowed.contains(&sub.as_str()) {
                        return refuse(&format!("params.{sub}"));
                    }
                }
            }
            other => return refuse(other),
        }
    }
    Ok(())
}

/// A vector to add after creation: dense (its raw `VectorParams`), or
/// sparse.
#[derive(Clone, Debug, PartialEq)]
pub enum NewVector {
    Dense(Value),
    Sparse,
}

impl NewVector {
    /// `{"dense": params}`, `{"sparse": params}`, or plain `VectorParams`
    /// (a body with `size`); any other body is a sparse one (row T3-4).
    pub fn from_json(body: Value) -> Self {
        match body {
            Value::Object(mut obj) => {
                if let Some(dense) = obj.remove("dense").filter(|v| !v.is_null()) {
                    NewVector::Dense(dense)
                } else if obj.contains_key("size") {
                    NewVector::Dense(Value::Object(obj))
                } else {
                    NewVector::Sparse
                }
            }
            other => NewVector::Dense(other),
        }
    }
}

/// `PUT /collections/{c}/vectors/{v}` (step 6).
pub(crate) async fn create_vector_name(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    vector: String,
    body: NewVector,
) -> Result<UpdateResult, GatewayError> {
    let NewVector::Dense(raw) = body else {
        return Err(GatewayError::Unsupported(
            "adding a sparse vector after creation".to_string(),
        ));
    };
    let params: VectorParams =
        serde_json::from_value(raw.clone()).map_err(|e| GatewayError::json(e.to_string()))?;
    let service = gw.service();
    let info = service.get_collection(&ctx.ns, &collection).await?;
    let schema = &info.schema;
    if schema.vectors.iter().any(|v| v.name == vector)
        || schema.sparse_vectors.iter().any(|v| v.name == vector)
    {
        return Err(GatewayError::BadRequest(format!(
            "Vector {vector} already exists"
        )));
    }
    let stored: CreateCollection = stored_create(schema)
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let spec = vector_spec(&vector, &params, &stored)?;
    let annotations = BTreeMap::from([(format!("{EXT_VECTOR_PREFIX}{vector}"), raw.to_string())]);
    service
        .add_fields(&ctx.ns, &collection, Vec::new(), vec![spec], annotations)
        .await?;
    Ok(UpdateResult {
        operation_id: None,
        status: UpdateStatus::Completed,
    })
}

/// `PUT /collections/{c}/index` (Task 4 step 3): an equal existing field
/// answers `completed` at once; another kind of field is unsupported.
pub(crate) async fn create_field_index(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: CreateFieldIndex,
    wait: bool,
) -> Result<UpdateResult, GatewayError> {
    let Some(field_schema) = &request.field_schema else {
        return Err(GatewayError::BadRequest(
            "field_schema is required".to_string(),
        ));
    };
    let field = payload_index_field(&request.field_name, field_schema)?;
    let service = gw.service();
    let info = service.get_collection(&ctx.ns, &collection).await?;
    let completed = UpdateResult {
        operation_id: None,
        status: UpdateStatus::Completed,
    };
    if let Some(existing) = info.schema.fields.iter().find(|f| f.name == field.name) {
        return if existing.kind == field.kind {
            Ok(completed)
        } else {
            Err(GatewayError::Unsupported(
                "changing payload index type".to_string(),
            ))
        };
    }
    service
        .add_fields(
            &ctx.ns,
            &collection,
            vec![field],
            Vec::new(),
            BTreeMap::new(),
        )
        .await?;
    Ok(UpdateResult {
        operation_id: None,
        status: if wait {
            UpdateStatus::Completed
        } else {
            UpdateStatus::Acknowledged
        },
    })
}

/// `GET /cluster`: clustering is Loams's own (step 7).
pub(crate) fn cluster_status() -> Value {
    json!({"status": "disabled"})
}

/// The points of the one synthetic shard of `GET /collections/{c}/cluster`.
pub(crate) async fn cluster_points(
    gw: QdrantGateway,
    ctx: RequestCtx,
    name: String,
) -> Result<u64, GatewayError> {
    Ok(gw
        .service()
        .count(&ctx.ns, &name, None, ctx.consistency.clone())
        .await?)
}

/// `GET /collections/{c}/cluster`'s result.
pub(crate) fn cluster_info_json(points_count: u64) -> Value {
    json!({
        "peer_id": 0,
        "shard_count": 1,
        "local_shards": [{"shard_id": 0, "points_count": points_count, "state": "Active"}],
        "remote_shards": [],
        "shard_transfers": [],
    })
}
