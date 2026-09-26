//! `CollectionSchema` as JSON (plan M1.2 Task 1 rule 5): the SDK contract,
//! not the derived Raft wire form (row 0.2).
//!
//! Output always carries every key. Input fills the documented defaults,
//! ignores `version` (the service sets it; [`from_json`] returns version 1),
//! and refuses unknown keys with `InvalidArgument("unknown key {k} in {what}")`.
//! The serde adapter ([`serialize`], [`deserialize`]) keeps `version`, so a
//! `CollectionInfo` round-trips.

use std::collections::BTreeMap;

use operon_collection::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, Quantization, SparseModifier, SparseVectorSpec, VectorElement, VectorIndexSpec,
    VectorSpec,
};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};

use crate::error::ServiceError;

fn invalid(message: String) -> ServiceError {
    ServiceError::InvalidArgument(message)
}

/// An object being read: refuses unknown keys, reads typed values.
struct Obj<'a> {
    map: &'a Map<String, Value>,
    what: &'static str,
}

impl<'a> Obj<'a> {
    fn new(v: &'a Value, what: &'static str, allowed: &[&str]) -> Result<Self, ServiceError> {
        let map = v
            .as_object()
            .ok_or_else(|| invalid(format!("{what} must be an object, got {v}")))?;
        if let Some(key) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(invalid(format!("unknown key {key} in {what}")));
        }
        Ok(Self { map, what })
    }

    /// The value of `key`, `None` when missing or `null`.
    fn get(&self, key: &str) -> Option<&'a Value> {
        self.map.get(key).filter(|v| !v.is_null())
    }

    fn required(&self, key: &str) -> Result<&'a Value, ServiceError> {
        self.get(key)
            .ok_or_else(|| invalid(format!("{} needs {key}", self.what)))
    }

    fn wrong(&self, key: &str, expected: &str) -> ServiceError {
        invalid(format!("{}.{key} must be {expected}", self.what))
    }

    fn string(&self, key: &str) -> Result<Option<String>, ServiceError> {
        self.get(key)
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| self.wrong(key, "a string"))
            })
            .transpose()
    }

    fn required_string(&self, key: &str) -> Result<String, ServiceError> {
        self.required(key)?;
        Ok(self.string(key)?.unwrap_or_default())
    }

    fn bool(&self, key: &str, default: bool) -> Result<bool, ServiceError> {
        self.get(key)
            .map(|v| v.as_bool().ok_or_else(|| self.wrong(key, "a bool")))
            .transpose()
            .map(|v| v.unwrap_or(default))
    }

    fn u32(&self, key: &str) -> Result<Option<u32>, ServiceError> {
        self.get(key)
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| self.wrong(key, "an unsigned 32-bit integer"))
            })
            .transpose()
    }

    fn u8(&self, key: &str, default: u8) -> Result<u8, ServiceError> {
        self.get(key)
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| self.wrong(key, "an unsigned 8-bit integer"))
            })
            .transpose()
            .map(|v| v.unwrap_or(default))
    }

    fn list(&self, key: &str) -> Result<&'a [Value], ServiceError> {
        match self.get(key) {
            None => Ok(&[]),
            Some(v) => v
                .as_array()
                .map(Vec::as_slice)
                .ok_or_else(|| self.wrong(key, "a list")),
        }
    }
}

/// A one-key object `{name: body}` or a bare string `name`.
fn tagged<'a>(v: &'a Value, what: &str) -> Result<(&'a str, Option<&'a Value>), ServiceError> {
    match v {
        Value::String(name) => Ok((name.as_str(), None)),
        Value::Object(map) if map.len() == 1 => {
            let (name, body) = map.iter().next().expect("one key");
            Ok((name.as_str(), Some(body)))
        }
        other => Err(invalid(format!("invalid {what}: {other}"))),
    }
}

fn distance_name(distance: Distance) -> &'static str {
    match distance {
        Distance::Cosine => "cosine",
        Distance::Dot => "dot",
        Distance::Euclid => "euclid",
        Distance::Manhattan => "manhattan",
    }
}

fn parse_distance(name: &str) -> Option<Distance> {
    Some(match name {
        "cosine" => Distance::Cosine,
        "dot" => Distance::Dot,
        "euclid" => Distance::Euclid,
        "manhattan" => Distance::Manhattan,
        _ => return None,
    })
}

fn dynamic_name(dynamic: DynamicMapping) -> &'static str {
    match dynamic {
        DynamicMapping::Strict => "strict",
        DynamicMapping::Ignore => "ignore",
        DynamicMapping::Map => "map",
    }
}

fn kind_to_json(kind: &FieldKind) -> Value {
    match kind {
        FieldKind::Text {
            analyzer,
            positions,
        } => json!({"text": {"analyzer": analyzer, "positions": positions}}),
        FieldKind::Keyword => json!("keyword"),
        FieldKind::I64 => json!("i64"),
        FieldKind::F64 => json!("f64"),
        FieldKind::Bool => json!("bool"),
        FieldKind::Date => json!("date"),
        FieldKind::Uuid => json!("uuid"),
        FieldKind::Json => json!("json"),
    }
}

fn kind_from_json(v: &Value) -> Result<FieldKind, ServiceError> {
    let (name, body) = tagged(v, "field kind")?;
    let plain = |kind: FieldKind| match body {
        None => Ok(kind),
        Some(_) => Err(invalid(format!("field kind {name} takes no settings"))),
    };
    match name {
        "text" => {
            let body = body.cloned().unwrap_or_else(|| json!({}));
            let text = Obj::new(&body, "text", &["analyzer", "positions"])?;
            Ok(FieldKind::Text {
                analyzer: text
                    .string("analyzer")?
                    .unwrap_or_else(|| "standard".to_string()),
                positions: text.bool("positions", true)?,
            })
        }
        "keyword" => plain(FieldKind::Keyword),
        "i64" => plain(FieldKind::I64),
        "f64" => plain(FieldKind::F64),
        "bool" => plain(FieldKind::Bool),
        "date" => plain(FieldKind::Date),
        "uuid" => plain(FieldKind::Uuid),
        "json" => plain(FieldKind::Json),
        other => Err(invalid(format!("unknown field kind {other}"))),
    }
}

fn field_to_json(field: &FieldSpec) -> Value {
    json!({
        "name": field.name,
        "source_path": field.source_path,
        "kind": kind_to_json(&field.kind),
        "indexed": field.indexed,
        "fast": field.fast,
        "ignore_malformed": field.ignore_malformed,
    })
}

/// A field; `source_path` defaults to the name, `indexed` to true, `fast`
/// and `ignore_malformed` to false.
pub fn field_from_json(v: &Value) -> Result<FieldSpec, ServiceError> {
    let field = Obj::new(
        v,
        "field",
        &[
            "name",
            "source_path",
            "kind",
            "indexed",
            "fast",
            "ignore_malformed",
        ],
    )?;
    let name = field.required_string("name")?;
    Ok(FieldSpec {
        source_path: field.string("source_path")?.unwrap_or_else(|| name.clone()),
        kind: kind_from_json(field.required("kind")?)?,
        indexed: field.bool("indexed", true)?,
        fast: field.bool("fast", false)?,
        ignore_malformed: field.bool("ignore_malformed", false)?,
        name,
    })
}

fn index_to_json(index: &VectorIndexSpec) -> Value {
    match *index {
        VectorIndexSpec::Auto => json!("auto"),
        VectorIndexSpec::None => json!("none"),
        VectorIndexSpec::IvfPq {
            num_partitions,
            num_sub_vectors,
            num_bits,
        } => {
            json!({"ivf_pq": {"num_partitions": num_partitions, "num_sub_vectors": num_sub_vectors, "num_bits": num_bits}})
        }
        VectorIndexSpec::IvfRq {
            num_partitions,
            num_bits,
        } => json!({"ivf_rq": {"num_partitions": num_partitions, "num_bits": num_bits}}),
        VectorIndexSpec::IvfHnswSq { num_partitions } => {
            json!({"ivf_hnsw_sq": {"num_partitions": num_partitions}})
        }
    }
}

fn index_from_json(v: &Value) -> Result<VectorIndexSpec, ServiceError> {
    let (name, body) = tagged(v, "vector index")?;
    let empty = json!({});
    let body = body.unwrap_or(&empty);
    match name {
        "auto" | "none" if body.as_object().is_some_and(Map::is_empty) => Ok(if name == "auto" {
            VectorIndexSpec::Auto
        } else {
            VectorIndexSpec::None
        }),
        "ivf_pq" => {
            let o = Obj::new(
                body,
                "ivf_pq",
                &["num_partitions", "num_sub_vectors", "num_bits"],
            )?;
            Ok(VectorIndexSpec::IvfPq {
                num_partitions: o.u32("num_partitions")?,
                num_sub_vectors: o.u32("num_sub_vectors")?,
                num_bits: o.u8("num_bits", 8)?,
            })
        }
        "ivf_rq" => {
            let o = Obj::new(body, "ivf_rq", &["num_partitions", "num_bits"])?;
            Ok(VectorIndexSpec::IvfRq {
                num_partitions: o.u32("num_partitions")?,
                num_bits: o.u8("num_bits", 1)?,
            })
        }
        "ivf_hnsw_sq" => {
            let o = Obj::new(body, "ivf_hnsw_sq", &["num_partitions"])?;
            Ok(VectorIndexSpec::IvfHnswSq {
                num_partitions: o.u32("num_partitions")?,
            })
        }
        other => Err(invalid(format!("unknown vector index {other}"))),
    }
}

fn hnsw_to_json(hnsw: &HnswParams) -> Value {
    json!({
        "m": hnsw.m,
        "ef_construct": hnsw.ef_construct,
        "full_scan_threshold_kb": hnsw.full_scan_threshold_kb,
        "payload_m": hnsw.payload_m,
        "on_disk": hnsw.on_disk,
    })
}

fn hnsw_from_json(v: &Value) -> Result<HnswParams, ServiceError> {
    let o = Obj::new(
        v,
        "hnsw",
        &[
            "m",
            "ef_construct",
            "full_scan_threshold_kb",
            "payload_m",
            "on_disk",
        ],
    )?;
    let default = HnswParams::default();
    Ok(HnswParams {
        m: o.u32("m")?.unwrap_or(default.m),
        ef_construct: o.u32("ef_construct")?.unwrap_or(default.ef_construct),
        full_scan_threshold_kb: o
            .u32("full_scan_threshold_kb")?
            .unwrap_or(default.full_scan_threshold_kb),
        payload_m: o.u32("payload_m")?.or(default.payload_m),
        on_disk: o.bool("on_disk", default.on_disk)?,
    })
}

fn quantization_to_json(quantization: &Option<Quantization>) -> Value {
    match *quantization {
        None => Value::Null,
        Some(Quantization::Scalar {
            quantile_ppm,
            always_ram,
        }) => json!({"scalar": {"quantile_ppm": quantile_ppm, "always_ram": always_ram}}),
        Some(Quantization::Product {
            compression_ratio,
            always_ram,
        }) => {
            json!({"product": {"compression_ratio": compression_ratio, "always_ram": always_ram}})
        }
        Some(Quantization::Binary { always_ram }) => json!({"binary": {"always_ram": always_ram}}),
    }
}

fn quantization_from_json(v: &Value) -> Result<Quantization, ServiceError> {
    let (name, body) = tagged(v, "quantization")?;
    let empty = json!({});
    let body = body.unwrap_or(&empty);
    match name {
        "scalar" => {
            let o = Obj::new(body, "scalar", &["quantile_ppm", "always_ram"])?;
            Ok(Quantization::Scalar {
                quantile_ppm: o.u32("quantile_ppm")?,
                always_ram: o.bool("always_ram", false)?,
            })
        }
        "product" => {
            let o = Obj::new(body, "product", &["compression_ratio", "always_ram"])?;
            o.required("compression_ratio")?;
            Ok(Quantization::Product {
                compression_ratio: o.u32("compression_ratio")?.unwrap_or_default(),
                always_ram: o.bool("always_ram", false)?,
            })
        }
        "binary" => {
            let o = Obj::new(body, "binary", &["always_ram"])?;
            Ok(Quantization::Binary {
                always_ram: o.bool("always_ram", false)?,
            })
        }
        other => Err(invalid(format!("unknown quantization {other}"))),
    }
}

fn vector_to_json(vector: &VectorSpec) -> Value {
    json!({
        "name": vector.name,
        "dim": vector.dim,
        "distance": distance_name(vector.distance),
        "element": match vector.element { VectorElement::F32 => "f32" },
        "index": index_to_json(&vector.index),
        "hnsw": hnsw_to_json(&vector.hnsw),
        "quantization": quantization_to_json(&vector.quantization),
    })
}

/// A dense vector; `element` defaults to `f32`, `index` to `auto`, `hnsw` to
/// Qdrant's defaults and `quantization` to none.
pub fn vector_from_json(v: &Value) -> Result<VectorSpec, ServiceError> {
    let o = Obj::new(
        v,
        "vector",
        &[
            "name",
            "dim",
            "distance",
            "element",
            "index",
            "hnsw",
            "quantization",
        ],
    )?;
    o.required("dim")?;
    let distance = o.required_string("distance")?;
    let element = match o.string("element")?.as_deref() {
        None | Some("f32") => VectorElement::F32,
        Some(other) => return Err(invalid(format!("unknown vector element {other}"))),
    };
    Ok(VectorSpec {
        name: o.required_string("name")?,
        dim: o.u32("dim")?.unwrap_or_default(),
        distance: parse_distance(&distance)
            .ok_or_else(|| invalid(format!("unknown distance {distance}")))?,
        element,
        index: o
            .get("index")
            .map(index_from_json)
            .transpose()?
            .unwrap_or(VectorIndexSpec::Auto),
        hnsw: o
            .get("hnsw")
            .map(hnsw_from_json)
            .transpose()?
            .unwrap_or_default(),
        quantization: o
            .get("quantization")
            .map(quantization_from_json)
            .transpose()?,
    })
}

fn sparse_to_json(sparse: &SparseVectorSpec) -> Value {
    json!({
        "name": sparse.name,
        "modifier": match sparse.modifier { SparseModifier::None => "none", SparseModifier::Idf => "idf" },
    })
}

fn sparse_from_json(v: &Value) -> Result<SparseVectorSpec, ServiceError> {
    let o = Obj::new(v, "sparse vector", &["name", "modifier"])?;
    let modifier = match o.string("modifier")?.as_deref() {
        None | Some("none") => SparseModifier::None,
        Some("idf") => SparseModifier::Idf,
        Some(other) => return Err(invalid(format!("unknown sparse modifier {other}"))),
    };
    Ok(SparseVectorSpec {
        name: o.required_string("name")?,
        modifier,
    })
}

/// The schema's JSON, every key present.
pub fn to_json(s: &CollectionSchema) -> Value {
    json!({
        "version": s.version,
        "fields": s.fields.iter().map(field_to_json).collect::<Vec<_>>(),
        "vectors": s.vectors.iter().map(vector_to_json).collect::<Vec<_>>(),
        "sparse_vectors": s.sparse_vectors.iter().map(sparse_to_json).collect::<Vec<_>>(),
        "dynamic": dynamic_name(s.dynamic),
        "max_fields": s.max_fields,
        "annotations": s.annotations,
    })
}

/// A schema from its JSON, at version 1 whatever `version` says.
pub fn from_json(v: &Value) -> Result<CollectionSchema, ServiceError> {
    parse(v).map(|(_, schema)| schema)
}

/// The schema and the `version` key, if present.
fn parse(v: &Value) -> Result<(Option<u64>, CollectionSchema), ServiceError> {
    let o = Obj::new(
        v,
        "schema",
        &[
            "version",
            "fields",
            "vectors",
            "sparse_vectors",
            "dynamic",
            "max_fields",
            "annotations",
        ],
    )?;
    let version = o
        .get("version")
        .map(|v| {
            v.as_u64()
                .ok_or_else(|| o.wrong("version", "an unsigned integer"))
        })
        .transpose()?;
    let dynamic = match o.string("dynamic")?.as_deref() {
        None | Some("strict") => DynamicMapping::Strict,
        Some("ignore") => DynamicMapping::Ignore,
        Some("map") => DynamicMapping::Map,
        Some(other) => return Err(invalid(format!("unknown dynamic mapping {other}"))),
    };
    let annotations = match o.get("annotations") {
        None => BTreeMap::new(),
        Some(v) => v
            .as_object()
            .ok_or_else(|| o.wrong("annotations", "an object of strings"))?
            .iter()
            .map(|(key, value)| {
                value
                    .as_str()
                    .map(|value| (key.clone(), value.to_string()))
                    .ok_or_else(|| o.wrong("annotations", "an object of strings"))
            })
            .collect::<Result<_, _>>()?,
    };
    let schema = CollectionSchema {
        version: 1,
        fields: o
            .list("fields")?
            .iter()
            .map(field_from_json)
            .collect::<Result<_, _>>()?,
        vectors: o
            .list("vectors")?
            .iter()
            .map(vector_from_json)
            .collect::<Result<_, _>>()?,
        sparse_vectors: o
            .list("sparse_vectors")?
            .iter()
            .map(sparse_from_json)
            .collect::<Result<_, _>>()?,
        dynamic,
        max_fields: o.u32("max_fields")?.unwrap_or(DEFAULT_MAX_FIELDS),
        annotations,
    };
    Ok((version, schema))
}

pub fn serialize<S: Serializer>(schema: &CollectionSchema, s: S) -> Result<S::Ok, S::Error> {
    to_json(schema).serialize(s)
}

/// Like [`from_json`], but keeps `version` when present.
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<CollectionSchema, D::Error> {
    let value = Value::deserialize(d)?;
    let (version, mut schema) = parse(&value).map_err(D::Error::custom)?;
    if let Some(version) = version {
        schema.version = version;
    }
    Ok(schema)
}

/// `Option<Distance>` as `"cosine" | "dot" | "euclid" | "manhattan" | null`.
pub mod distance {
    use super::*;

    pub fn serialize<S: Serializer>(distance: &Option<Distance>, s: S) -> Result<S::Ok, S::Error> {
        distance.map(distance_name).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Distance>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|name| {
                parse_distance(&name)
                    .ok_or_else(|| D::Error::custom(format!("unknown distance {name}")))
            })
            .transpose()
    }
}
