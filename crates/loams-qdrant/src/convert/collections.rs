//! Collection messages: gRPC requests become the REST JSON bodies the
//! shared executors take, and the REST JSON results become gRPC replies,
//! so the two surfaces cannot drift apart (Task 3).
//!
//! The deprecated fields (`on_disk`, `always_ram`, `on_disk_payload`) are
//! the ones 1.15 clients send and read, so they are converted on purpose.
#![allow(deprecated)]

use loams_query::ManifestInfo;
use serde_json::{Map, Value, json};

use crate::convert::value::{map_to_payload, payload_to_map};
use crate::error::GatewayError;
use crate::model::collections::{
    AliasOperation, AliasesResponse, CreateAlias, DeleteAlias, RenameAlias,
};
use crate::proto::qdrant as pb;
use crate::snapshots;

/// Inserts `key: v` when `v` is `Some`.
fn put<T: Into<Value>>(out: &mut Map<String, Value>, key: &str, v: Option<T>) {
    if let Some(v) = v {
        out.insert(key.to_string(), v.into());
    }
}

// ----- enums -----

fn distance_to_name(d: i32) -> &'static str {
    match pb::Distance::try_from(d) {
        Ok(pb::Distance::Cosine) => "Cosine",
        Ok(pb::Distance::Euclid) => "Euclid",
        Ok(pb::Distance::Dot) => "Dot",
        Ok(pb::Distance::Manhattan) => "Manhattan",
        // Not a Qdrant distance name, so the body is a format error.
        _ => "UnknownDistance",
    }
}

fn distance_from_name(v: Option<&Value>) -> i32 {
    let d = match v.and_then(Value::as_str) {
        Some("Cosine") => pb::Distance::Cosine,
        Some("Euclid") => pb::Distance::Euclid,
        Some("Dot") => pb::Distance::Dot,
        Some("Manhattan") => pb::Distance::Manhattan,
        _ => pb::Distance::UnknownDistance,
    };
    d as i32
}

/// `None` for `Default` (no datatype given).
fn datatype_to_name(d: i32) -> Option<&'static str> {
    match pb::Datatype::try_from(d) {
        Ok(pb::Datatype::Float32) => Some("float32"),
        Ok(pb::Datatype::Uint8) => Some("uint8"),
        Ok(pb::Datatype::Float16) => Some("float16"),
        Ok(pb::Datatype::Turbo4) => Some("turbo4"),
        Ok(pb::Datatype::Default) => None,
        Err(_) => Some("unknown"),
    }
}

fn datatype_from_name(v: Option<&Value>) -> Option<i32> {
    let d = match v?.as_str()? {
        "float32" => pb::Datatype::Float32,
        "uint8" => pb::Datatype::Uint8,
        "float16" => pb::Datatype::Float16,
        _ => return None,
    };
    Some(d as i32)
}

fn modifier_to_name(m: i32) -> &'static str {
    match pb::Modifier::try_from(m) {
        Ok(pb::Modifier::Idf) => "idf",
        _ => "none",
    }
}

fn u64_of(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(Value::as_u64)
}

fn u32_of(v: &Value, key: &str) -> Option<u32> {
    u64_of(v, key).and_then(|n| u32::try_from(n).ok())
}

fn bool_of(v: &Value, key: &str) -> Option<bool> {
    v.get(key).and_then(Value::as_bool)
}

// ----- gRPC to JSON -----

/// gRPC `HnswConfigDiff` as REST JSON, set fields only.
fn hnsw_to_json(h: &pb::HnswConfigDiff) -> Value {
    let mut out = Map::new();
    put(&mut out, "m", h.m);
    put(&mut out, "ef_construct", h.ef_construct);
    put(&mut out, "full_scan_threshold", h.full_scan_threshold);
    put(&mut out, "max_indexing_threads", h.max_indexing_threads);
    put(&mut out, "on_disk", h.on_disk);
    put(&mut out, "payload_m", h.payload_m);
    Value::Object(out)
}

/// gRPC quantization as the REST `scalar`/`product`/`binary` object.
fn quantization_to_json(q: &pb::QuantizationConfig) -> Value {
    use pb::quantization_config::Quantization;
    match &q.quantization {
        Some(Quantization::Scalar(s)) => {
            let kind = match pb::QuantizationType::try_from(s.r#type) {
                Ok(pb::QuantizationType::Int8) => "int8",
                _ => "unknown",
            };
            let mut out = Map::new();
            out.insert("type".into(), json!(kind));
            put(&mut out, "quantile", s.quantile);
            put(&mut out, "always_ram", s.always_ram);
            json!({"scalar": out})
        }
        Some(Quantization::Product(p)) => {
            let ratio = match pb::CompressionRatio::try_from(p.compression) {
                Ok(pb::CompressionRatio::X4) => "x4",
                Ok(pb::CompressionRatio::X8) => "x8",
                Ok(pb::CompressionRatio::X16) => "x16",
                Ok(pb::CompressionRatio::X32) => "x32",
                Ok(pb::CompressionRatio::X64) => "x64",
                Err(_) => "unknown",
            };
            let mut out = Map::new();
            out.insert("compression".into(), json!(ratio));
            put(&mut out, "always_ram", p.always_ram);
            json!({"product": out})
        }
        Some(Quantization::Binary(b)) => {
            let mut out = Map::new();
            put(&mut out, "always_ram", b.always_ram);
            let encoding = b
                .encoding
                .map(|e| match pb::BinaryQuantizationEncoding::try_from(e) {
                    Ok(pb::BinaryQuantizationEncoding::TwoBits) => "two_bits",
                    Ok(pb::BinaryQuantizationEncoding::OneAndHalfBits) => "one_and_half_bits",
                    _ => "one_bit",
                });
            put(&mut out, "encoding", encoding);
            let query = b.query_encoding.as_ref().and_then(|q| q.variant).map(|v| {
                use pb::binary_quantization_query_encoding::{Setting, Variant};
                let Variant::Setting(s) = v;
                match Setting::try_from(s) {
                    Ok(Setting::Binary) => "binary",
                    Ok(Setting::Scalar4Bits) => "scalar4bits",
                    Ok(Setting::Scalar8Bits) => "scalar8bits",
                    _ => "default",
                }
            });
            put(&mut out, "query_encoding", query);
            json!({"binary": out})
        }
        Some(Quantization::Turboquant(_)) => json!({"turboquant": {}}),
        None => json!({"none": {}}),
    }
}

/// gRPC `VectorParams` as REST JSON.
fn vector_params_to_json(p: &pb::VectorParams) -> Value {
    let mut out = Map::new();
    out.insert("size".into(), json!(p.size));
    out.insert("distance".into(), json!(distance_to_name(p.distance)));
    put(
        &mut out,
        "hnsw_config",
        p.hnsw_config.as_ref().map(hnsw_to_json),
    );
    put(
        &mut out,
        "quantization_config",
        p.quantization_config.as_ref().map(quantization_to_json),
    );
    put(&mut out, "on_disk", p.on_disk);
    put(&mut out, "datatype", p.datatype.and_then(datatype_to_name));
    if p.multivector_config.is_some() {
        out.insert(
            "multivector_config".into(),
            json!({"comparator": "max_sim"}),
        );
    }
    Value::Object(out)
}

/// The single or named vectors config as REST JSON.
fn vectors_config_to_json(v: &pb::VectorsConfig) -> Value {
    use pb::vectors_config::Config;
    match &v.config {
        Some(Config::Params(p)) => vector_params_to_json(p),
        Some(Config::ParamsMap(m)) => Value::Object(
            m.map
                .iter()
                .map(|(k, p)| (k.clone(), vector_params_to_json(p)))
                .collect(),
        ),
        None => json!({}),
    }
}

/// The sparse vectors config as REST JSON.
fn sparse_config_to_json(s: &pb::SparseVectorConfig) -> Value {
    Value::Object(
        s.map
            .iter()
            .map(|(name, p)| {
                let mut out = Map::new();
                if let Some(index) = &p.index {
                    let mut i = Map::new();
                    put(&mut i, "full_scan_threshold", index.full_scan_threshold);
                    put(&mut i, "on_disk", index.on_disk);
                    put(
                        &mut i,
                        "datatype",
                        index.datatype.and_then(datatype_to_name),
                    );
                    out.insert("index".into(), Value::Object(i));
                }
                put(&mut out, "modifier", p.modifier.map(modifier_to_name));
                (name.clone(), Value::Object(out))
            })
            .collect(),
    )
}

/// The optimizer config diff as REST JSON, set fields only.
fn optimizers_to_json(o: &pb::OptimizersConfigDiff) -> Value {
    let mut out = Map::new();
    put(&mut out, "deleted_threshold", o.deleted_threshold);
    put(
        &mut out,
        "vacuum_min_vector_number",
        o.vacuum_min_vector_number,
    );
    put(&mut out, "default_segment_number", o.default_segment_number);
    put(&mut out, "max_segment_size", o.max_segment_size);
    put(&mut out, "memmap_threshold", o.memmap_threshold);
    put(&mut out, "indexing_threshold", o.indexing_threshold);
    put(&mut out, "flush_interval_sec", o.flush_interval_sec);
    use pb::max_optimization_threads::Variant;
    let threads = o
        .max_optimization_threads
        .as_ref()
        .and_then(|t| t.variant)
        .map(|v| match v {
            Variant::Value(n) => json!(n),
            Variant::Setting(_) => json!("auto"),
        })
        .or(o.deprecated_max_optimization_threads.map(|n| json!(n)));
    put(&mut out, "max_optimization_threads", threads);
    Value::Object(out)
}

/// The WAL config diff as REST JSON, set fields only.
fn wal_to_json(w: &pb::WalConfigDiff) -> Value {
    let mut out = Map::new();
    put(&mut out, "wal_capacity_mb", w.wal_capacity_mb);
    put(&mut out, "wal_segments_ahead", w.wal_segments_ahead);
    put(&mut out, "wal_retain_closed", w.wal_retain_closed);
    Value::Object(out)
}

/// The scalar settings of a strict-mode config (a no-op; the nested
/// multivector and sparse limits are dropped).
fn strict_to_json(s: &pb::StrictModeConfig) -> Value {
    let mut out = Map::new();
    put(&mut out, "enabled", s.enabled);
    put(&mut out, "max_query_limit", s.max_query_limit);
    put(&mut out, "max_timeout", s.max_timeout);
    put(
        &mut out,
        "unindexed_filtering_retrieve",
        s.unindexed_filtering_retrieve,
    );
    put(
        &mut out,
        "unindexed_filtering_update",
        s.unindexed_filtering_update,
    );
    put(&mut out, "search_max_hnsw_ef", s.search_max_hnsw_ef);
    put(&mut out, "search_allow_exact", s.search_allow_exact);
    put(
        &mut out,
        "search_max_oversampling",
        s.search_max_oversampling,
    );
    put(&mut out, "upsert_max_batchsize", s.upsert_max_batchsize);
    put(&mut out, "search_max_batchsize", s.search_max_batchsize);
    put(
        &mut out,
        "max_collection_vector_size_bytes",
        s.max_collection_vector_size_bytes,
    );
    put(&mut out, "read_rate_limit", s.read_rate_limit);
    put(&mut out, "write_rate_limit", s.write_rate_limit);
    put(
        &mut out,
        "max_collection_payload_size_bytes",
        s.max_collection_payload_size_bytes,
    );
    put(&mut out, "filter_max_conditions", s.filter_max_conditions);
    put(&mut out, "condition_max_size", s.condition_max_size);
    put(&mut out, "max_points_count", s.max_points_count);
    put(
        &mut out,
        "max_payload_index_count",
        s.max_payload_index_count,
    );
    Value::Object(out)
}

/// A gRPC `CreateCollection` as the REST body.
pub fn create_to_json(c: &pb::CreateCollection) -> Value {
    let mut out = Map::new();
    put(
        &mut out,
        "vectors",
        c.vectors_config.as_ref().map(vectors_config_to_json),
    );
    put(&mut out, "shard_number", c.shard_number);
    let method = c
        .sharding_method
        .map(|m| match pb::ShardingMethod::try_from(m) {
            Ok(pb::ShardingMethod::Custom) => "custom",
            _ => "auto",
        });
    put(&mut out, "sharding_method", method);
    put(&mut out, "replication_factor", c.replication_factor);
    put(
        &mut out,
        "write_consistency_factor",
        c.write_consistency_factor,
    );
    put(&mut out, "on_disk_payload", c.on_disk_payload);
    put(
        &mut out,
        "hnsw_config",
        c.hnsw_config.as_ref().map(hnsw_to_json),
    );
    put(
        &mut out,
        "wal_config",
        c.wal_config.as_ref().map(wal_to_json),
    );
    put(
        &mut out,
        "optimizers_config",
        c.optimizers_config.as_ref().map(optimizers_to_json),
    );
    put(
        &mut out,
        "quantization_config",
        c.quantization_config.as_ref().map(quantization_to_json),
    );
    put(
        &mut out,
        "sparse_vectors",
        c.sparse_vectors_config.as_ref().map(sparse_config_to_json),
    );
    put(
        &mut out,
        "strict_mode_config",
        c.strict_mode_config.as_ref().map(strict_to_json),
    );
    if !c.metadata.is_empty() {
        out.insert(
            "metadata".into(),
            Value::Object(payload_to_map(&c.metadata)),
        );
    }
    Value::Object(out)
}

/// A gRPC `UpdateCollection` as the REST PATCH body; each part present is a
/// key (with a placeholder value where only its presence matters).
pub fn update_to_json(u: &pb::UpdateCollection) -> Value {
    let mut out = Map::new();
    put(
        &mut out,
        "optimizers_config",
        u.optimizers_config.as_ref().map(optimizers_to_json),
    );
    if let Some(p) = &u.params {
        let mut params = Map::new();
        put(&mut params, "replication_factor", p.replication_factor);
        put(
            &mut params,
            "write_consistency_factor",
            p.write_consistency_factor,
        );
        put(&mut params, "read_fan_out_factor", p.read_fan_out_factor);
        put(&mut params, "on_disk_payload", p.on_disk_payload);
        put(
            &mut params,
            "read_fan_out_delay_ms",
            p.read_fan_out_delay_ms,
        );
        put(&mut params, "payload", p.payload.map(|_| json!({})));
        out.insert("params".into(), Value::Object(params));
    }
    let present = [
        ("hnsw_config", u.hnsw_config.is_some()),
        ("vectors", u.vectors_config.is_some()),
        ("quantization_config", u.quantization_config.is_some()),
        ("sparse_vectors", u.sparse_vectors_config.is_some()),
        ("metadata", !u.metadata.is_empty()),
    ];
    for (key, is) in present {
        if is {
            out.insert(key.into(), json!({}));
        }
    }
    put(
        &mut out,
        "strict_mode_config",
        u.strict_mode_config.as_ref().map(strict_to_json),
    );
    Value::Object(out)
}

/// `CreateVectorName`'s dense config as `VectorParams` JSON, or `None` for a
/// sparse (or absent) one.
pub fn dense_creation_to_json(r: &pb::CreateVectorNameRequest) -> Option<Value> {
    use pb::create_vector_name_request::VectorConfig;
    let Some(VectorConfig::DenseConfig(d)) = &r.vector_config else {
        return None;
    };
    let mut out = Map::new();
    out.insert("size".into(), json!(d.size));
    out.insert("distance".into(), json!(distance_to_name(d.distance)));
    put(&mut out, "datatype", d.datatype.and_then(datatype_to_name));
    if d.multivector_config.is_some() {
        out.insert(
            "multivector_config".into(),
            json!({"comparator": "max_sim"}),
        );
    }
    Some(Value::Object(out))
}

/// gRPC alias operations; one without an action is a format error.
pub fn alias_ops_from_grpc(
    ops: &[pb::AliasOperations],
) -> Result<Vec<AliasOperation>, GatewayError> {
    use pb::alias_operations::Action;
    ops.iter()
        .map(|op| match &op.action {
            Some(Action::CreateAlias(c)) => Ok(AliasOperation::Create {
                create_alias: CreateAlias {
                    collection_name: c.collection_name.clone(),
                    alias_name: c.alias_name.clone(),
                },
            }),
            Some(Action::DeleteAlias(d)) => Ok(AliasOperation::Delete {
                delete_alias: DeleteAlias {
                    alias_name: d.alias_name.clone(),
                },
            }),
            Some(Action::RenameAlias(r)) => Ok(AliasOperation::Rename {
                rename_alias: RenameAlias {
                    old_alias_name: r.old_alias_name.clone(),
                    new_alias_name: r.new_alias_name.clone(),
                },
            }),
            None => Err(GatewayError::json("alias operation without an action")),
        })
        .collect()
}

// ----- JSON to gRPC -----

/// The echoed HNSW JSON as gRPC.
fn hnsw_to_grpc(v: &Value) -> pb::HnswConfigDiff {
    pb::HnswConfigDiff {
        m: u64_of(v, "m"),
        ef_construct: u64_of(v, "ef_construct"),
        full_scan_threshold: u64_of(v, "full_scan_threshold"),
        max_indexing_threads: u64_of(v, "max_indexing_threads"),
        on_disk: bool_of(v, "on_disk"),
        payload_m: u64_of(v, "payload_m"),
        ..Default::default()
    }
}

/// The echoed quantization JSON as gRPC; `None` when unrecognized.
fn quantization_to_grpc(v: &Value) -> Option<pb::QuantizationConfig> {
    use pb::quantization_config::Quantization;
    let q = if let Some(s) = v.get("scalar") {
        Quantization::Scalar(pb::ScalarQuantization {
            r#type: pb::QuantizationType::Int8 as i32,
            quantile: s.get("quantile").and_then(Value::as_f64).map(|q| q as f32),
            always_ram: bool_of(s, "always_ram"),
            ..Default::default()
        })
    } else if let Some(p) = v.get("product") {
        let ratio = match p.get("compression").and_then(Value::as_str) {
            Some("x8") => pb::CompressionRatio::X8,
            Some("x16") => pb::CompressionRatio::X16,
            Some("x32") => pb::CompressionRatio::X32,
            Some("x64") => pb::CompressionRatio::X64,
            _ => pb::CompressionRatio::X4,
        };
        Quantization::Product(pb::ProductQuantization {
            compression: ratio as i32,
            always_ram: bool_of(p, "always_ram"),
            ..Default::default()
        })
    } else {
        let b = v.get("binary")?;
        use pb::binary_quantization_query_encoding::{Setting, Variant};
        let encoding = b.get("encoding").and_then(Value::as_str).map(|e| {
            let e = match e {
                "two_bits" => pb::BinaryQuantizationEncoding::TwoBits,
                "one_and_half_bits" => pb::BinaryQuantizationEncoding::OneAndHalfBits,
                _ => pb::BinaryQuantizationEncoding::OneBit,
            };
            e as i32
        });
        let query_encoding = b.get("query_encoding").and_then(Value::as_str).map(|q| {
            let s = match q {
                "binary" => Setting::Binary,
                "scalar4bits" => Setting::Scalar4Bits,
                "scalar8bits" => Setting::Scalar8Bits,
                _ => Setting::Default,
            };
            pb::BinaryQuantizationQueryEncoding {
                variant: Some(Variant::Setting(s as i32)),
            }
        });
        Quantization::Binary(pb::BinaryQuantization {
            always_ram: bool_of(b, "always_ram"),
            encoding,
            query_encoding,
            ..Default::default()
        })
    };
    Some(pb::QuantizationConfig {
        quantization: Some(q),
    })
}

/// One echoed vector's JSON as gRPC `VectorParams`.
fn vector_params_to_grpc(v: &Value) -> pb::VectorParams {
    pb::VectorParams {
        size: u64_of(v, "size").unwrap_or(0),
        distance: distance_from_name(v.get("distance")),
        hnsw_config: v.get("hnsw_config").map(hnsw_to_grpc),
        quantization_config: v.get("quantization_config").and_then(quantization_to_grpc),
        on_disk: bool_of(v, "on_disk"),
        datatype: datatype_from_name(v.get("datatype")),
        ..Default::default()
    }
}

/// The echoed vectors (single or map) as gRPC.
fn vectors_to_grpc(v: &Value) -> pb::VectorsConfig {
    use pb::vectors_config::Config;
    let config = if v.get("size").is_some() {
        Config::Params(vector_params_to_grpc(v))
    } else {
        Config::ParamsMap(pb::VectorParamsMap {
            map: v
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, p)| (k.clone(), vector_params_to_grpc(p)))
                .collect(),
        })
    };
    pb::VectorsConfig {
        config: Some(config),
    }
}

/// The echoed sparse vectors as gRPC.
fn sparse_to_grpc(v: &Value) -> pb::SparseVectorConfig {
    pb::SparseVectorConfig {
        map: v
            .as_object()
            .into_iter()
            .flatten()
            .map(|(name, p)| {
                let index = p.get("index").map(|i| pb::SparseIndexConfig {
                    full_scan_threshold: u64_of(i, "full_scan_threshold"),
                    on_disk: bool_of(i, "on_disk"),
                    datatype: datatype_from_name(i.get("datatype")),
                    ..Default::default()
                });
                let modifier = p.get("modifier").and_then(Value::as_str).map(|m| {
                    let m = if m == "idf" {
                        pb::Modifier::Idf
                    } else {
                        pb::Modifier::None
                    };
                    m as i32
                });
                (name.clone(), pb::SparseVectorParams { index, modifier })
            })
            .collect(),
    }
}

/// The echoed optimizer config as gRPC.
fn optimizers_to_grpc(v: &Value) -> pb::OptimizersConfigDiff {
    use pb::max_optimization_threads::{Setting, Variant};
    let threads = match v.get("max_optimization_threads") {
        Some(Value::Number(n)) => n.as_u64().map(Variant::Value),
        Some(Value::String(_)) => Some(Variant::Setting(Setting::Auto as i32)),
        _ => None,
    };
    pb::OptimizersConfigDiff {
        deleted_threshold: v.get("deleted_threshold").and_then(Value::as_f64),
        vacuum_min_vector_number: u64_of(v, "vacuum_min_vector_number"),
        default_segment_number: u64_of(v, "default_segment_number"),
        max_segment_size: u64_of(v, "max_segment_size"),
        memmap_threshold: u64_of(v, "memmap_threshold"),
        indexing_threshold: u64_of(v, "indexing_threshold"),
        flush_interval_sec: u64_of(v, "flush_interval_sec"),
        max_optimization_threads: threads.map(|t| pb::MaxOptimizationThreads { variant: Some(t) }),
        ..Default::default()
    }
}

/// The echoed WAL config as gRPC.
fn wal_to_grpc(v: &Value) -> pb::WalConfigDiff {
    pb::WalConfigDiff {
        wal_capacity_mb: u64_of(v, "wal_capacity_mb"),
        wal_segments_ahead: u64_of(v, "wal_segments_ahead"),
        wal_retain_closed: u64_of(v, "wal_retain_closed"),
    }
}

/// The echoed strict-mode config as gRPC (scalar settings only, T3-5).
fn strict_to_grpc(v: &Value) -> pb::StrictModeConfig {
    pb::StrictModeConfig {
        enabled: bool_of(v, "enabled"),
        max_query_limit: u32_of(v, "max_query_limit"),
        max_timeout: u32_of(v, "max_timeout"),
        unindexed_filtering_retrieve: bool_of(v, "unindexed_filtering_retrieve"),
        unindexed_filtering_update: bool_of(v, "unindexed_filtering_update"),
        search_max_hnsw_ef: u32_of(v, "search_max_hnsw_ef"),
        search_allow_exact: bool_of(v, "search_allow_exact"),
        search_max_oversampling: v
            .get("search_max_oversampling")
            .and_then(Value::as_f64)
            .map(|f| f as f32),
        upsert_max_batchsize: u64_of(v, "upsert_max_batchsize"),
        search_max_batchsize: u64_of(v, "search_max_batchsize"),
        max_collection_vector_size_bytes: u64_of(v, "max_collection_vector_size_bytes"),
        read_rate_limit: u32_of(v, "read_rate_limit"),
        write_rate_limit: u32_of(v, "write_rate_limit"),
        max_collection_payload_size_bytes: u64_of(v, "max_collection_payload_size_bytes"),
        filter_max_conditions: u64_of(v, "filter_max_conditions"),
        condition_max_size: u64_of(v, "condition_max_size"),
        max_points_count: u64_of(v, "max_points_count"),
        max_payload_index_count: u64_of(v, "max_payload_index_count"),
        ..Default::default()
    }
}

/// One `payload_schema` entry as gRPC.
fn payload_schema_to_grpc(v: &Value) -> pb::PayloadSchemaInfo {
    let data_type = match v.get("data_type").and_then(Value::as_str) {
        Some("keyword") => pb::PayloadSchemaType::Keyword,
        Some("integer") => pb::PayloadSchemaType::Integer,
        Some("float") => pb::PayloadSchemaType::Float,
        Some("bool") => pb::PayloadSchemaType::Bool,
        Some("datetime") => pb::PayloadSchemaType::Datetime,
        Some("uuid") => pb::PayloadSchemaType::Uuid,
        Some("text") => pb::PayloadSchemaType::Text,
        _ => pb::PayloadSchemaType::UnknownType,
    };
    let params = v.get("params").map(|p| pb::PayloadIndexParams {
        index_params: Some(pb::payload_index_params::IndexParams::TextIndexParams(
            pb::TextIndexParams {
                tokenizer: pb::TokenizerType::Word as i32,
                lowercase: bool_of(p, "lowercase"),
                phrase_matching: bool_of(p, "phrase_matching"),
                ..Default::default()
            },
        )),
    });
    pb::PayloadSchemaInfo {
        data_type: data_type as i32,
        params,
        points: u64_of(v, "points"),
    }
}

/// The REST `CollectionInfo` JSON as gRPC's `CollectionInfo`.
pub fn info_to_grpc(v: &Value) -> pb::CollectionInfo {
    let config = &v["config"];
    let params = &config["params"];
    let sharding_method = params
        .get("sharding_method")
        .and_then(Value::as_str)
        .map(|m| {
            let m = if m == "custom" {
                pb::ShardingMethod::Custom
            } else {
                pb::ShardingMethod::Auto
            };
            m as i32
        });
    let collection_params = pb::CollectionParams {
        shard_number: u32_of(params, "shard_number").unwrap_or(1),
        on_disk_payload: bool_of(params, "on_disk_payload").unwrap_or(true),
        vectors_config: params.get("vectors").map(vectors_to_grpc),
        replication_factor: u32_of(params, "replication_factor"),
        write_consistency_factor: u32_of(params, "write_consistency_factor"),
        sharding_method,
        sparse_vectors_config: params.get("sparse_vectors").map(sparse_to_grpc),
        ..Default::default()
    };
    let metadata = config
        .get("metadata")
        .and_then(Value::as_object)
        .map(map_to_payload)
        .unwrap_or_default();
    pb::CollectionInfo {
        status: pb::CollectionStatus::Green as i32,
        optimizer_status: Some(pb::OptimizerStatus {
            ok: true,
            error: String::new(),
        }),
        segments_count: 1,
        config: Some(pb::CollectionConfig {
            params: Some(collection_params),
            hnsw_config: config.get("hnsw_config").map(hnsw_to_grpc),
            optimizer_config: config.get("optimizer_config").map(optimizers_to_grpc),
            wal_config: config.get("wal_config").map(wal_to_grpc),
            quantization_config: config
                .get("quantization_config")
                .and_then(quantization_to_grpc),
            strict_mode_config: config.get("strict_mode_config").map(strict_to_grpc),
            metadata,
        }),
        payload_schema: v["payload_schema"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, s)| (k.clone(), payload_schema_to_grpc(s)))
            .collect(),
        points_count: u64_of(v, "points_count"),
        indexed_vectors_count: u64_of(v, "indexed_vectors_count"),
        ..Default::default()
    }
}

/// Alias pairs as gRPC `AliasDescription`s.
pub fn aliases_to_grpc(a: AliasesResponse) -> Vec<pb::AliasDescription> {
    a.aliases
        .into_iter()
        .map(|a| pb::AliasDescription {
            alias_name: a.alias_name,
            collection_name: a.collection_name,
        })
        .collect()
}

/// A manifest version as gRPC's `SnapshotDescription` (Ruling 19).
pub fn snapshot_to_grpc(collection: &str, m: &ManifestInfo) -> pb::SnapshotDescription {
    let ms = m.created_at_ms;
    pb::SnapshotDescription {
        name: snapshots::snapshot_name(collection, m),
        creation_time: Some(prost_types::Timestamp {
            seconds: i64::try_from(ms / 1000).unwrap_or(i64::MAX),
            nanos: i32::try_from((ms % 1000) * 1_000_000).unwrap_or(0),
        }),
        size: i64::try_from(m.size_bytes).unwrap_or(i64::MAX),
        checksum: None,
    }
}

/// The synthetic cluster info with `points_count` points on shard 0.
pub fn cluster_to_grpc(points_count: u64, time: f64) -> pb::CollectionClusterInfoResponse {
    pb::CollectionClusterInfoResponse {
        peer_id: 0,
        shard_count: 1,
        local_shards: vec![pb::LocalShardInfo {
            shard_id: 0,
            points_count,
            state: pb::ReplicaState::Active as i32,
            shard_key: None,
        }],
        time,
        ..Default::default()
    }
}
