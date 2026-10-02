//! `CreateCollection` to `CollectionSchema` and the `CollectionInfo` JSON
//! (plan M1.4 Task 3).

use std::collections::BTreeMap;

use loams_collection::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, HnswParams, Quantization,
    SparseModifier, SparseVectorSpec,
};
use loams_common::{CollectionId, StreamId};
use loams_qdrant::model::collections::CreateCollection;
use loams_qdrant::schema::{collection_info_json, schema_from_create};
use loams_qdrant::{EXT_CREATE, GatewayError};
use loams_query::CollectionInfo;
use serde_json::{Value, json};

fn create(body: Value) -> Result<CollectionSchema, GatewayError> {
    let req: CreateCollection = serde_json::from_value(body.clone()).expect("parses");
    schema_from_create("c", &req, &body)
}

fn info_of(schema: CollectionSchema) -> CollectionInfo {
    CollectionInfo {
        id: CollectionId(1),
        name: "c".into(),
        namespace: "default".into(),
        schema,
        partitions: 1,
        aliases: Vec::new(),
        stream: StreamId(1),
        manifest_version: 0,
        live_doc_count: 0,
        size_bytes: 0,
        created_at_ms: 0,
        link_lag_records: 0,
        hot: Default::default(),
        unapplied_bytes: 0,
        backpressure: Default::default(),
    }
}

fn info_json(body: Value) -> Value {
    let schema = create(body).expect("create");
    collection_info_json(&info_of(schema), 0, &BTreeMap::new())
}

fn assert_unsupported(result: Result<CollectionSchema, GatewayError>, what: &str) {
    match result {
        Err(GatewayError::Unsupported(f)) => assert!(f.contains(what), "{f}"),
        other => panic!("{what}: {other:?}"),
    }
}

fn assert_bad_request(result: Result<CollectionSchema, GatewayError>, what: &str) {
    match result {
        Err(GatewayError::BadRequest(m)) => assert!(m.contains(what), "{m}"),
        other => panic!("{what}: {other:?}"),
    }
}

#[test]
fn single_vector_maps_to_the_default_name() {
    let body = json!({"vectors": {"size": 4, "distance": "Cosine"}});
    let schema = create(body.clone()).expect("create");
    assert_eq!(schema.vectors.len(), 1);
    let v = &schema.vectors[0];
    assert_eq!(v.name, "");
    assert_eq!(v.dim, 4);
    assert_eq!(v.distance, Distance::Cosine);
    assert_eq!(v.hnsw, HnswParams::default());
    assert_eq!((v.hnsw.m, v.hnsw.ef_construct), (16, 100));
    assert_eq!(v.hnsw.full_scan_threshold_kb, 10_000);
    assert_eq!(schema.fields.len(), 1);
    let payload = &schema.fields[0];
    assert_eq!(
        (payload.name.as_str(), payload.source_path.as_str()),
        ("payload", "")
    );
    assert_eq!(payload.kind, FieldKind::Json);
    assert!(payload.indexed && payload.fast && !payload.ignore_malformed);
    assert_eq!(schema.dynamic, DynamicMapping::Ignore);
    assert_eq!(schema.max_fields, 1000);
    let stored: Value = serde_json::from_str(&schema.annotations[EXT_CREATE]).expect("json");
    assert_eq!(stored, body);
    schema.validate().expect("valid schema");
}

#[test]
fn named_vectors_are_sorted() {
    let schema = create(json!({"vectors": {
        "b": {"size": 2, "distance": "Dot"},
        "a": {"size": 3, "distance": "Euclid"},
    }}))
    .expect("create");
    let names: Vec<_> = schema.vectors.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(names, ["a", "b"]);
    assert_eq!(schema.vectors[0].distance, Distance::Euclid);
    assert_eq!(schema.vectors[1].dim, 2);
}

#[test]
fn hnsw_and_quantization_merge_in_qdrant_order() {
    let schema = create(json!({
        "vectors": {"size": 4, "distance": "Cosine",
                    "hnsw_config": {"ef_construct": 200},
                    "quantization_config": {"binary": {"always_ram": true}}},
        "hnsw_config": {"m": 32, "full_scan_threshold": 5000, "payload_m": 0},
        "quantization_config": {"scalar": {"type": "int8", "quantile": 0.99}},
    }))
    .expect("create");
    let v = &schema.vectors[0];
    assert_eq!((v.hnsw.m, v.hnsw.ef_construct), (32, 200));
    assert_eq!(v.hnsw.full_scan_threshold_kb, 5000);
    assert_eq!(v.hnsw.payload_m, Some(0));
    assert_eq!(
        v.quantization,
        Some(Quantization::Binary { always_ram: true })
    );

    let schema = create(json!({
        "vectors": {"size": 4, "distance": "Cosine"},
        "quantization_config": {"scalar": {"type": "int8", "quantile": 0.99}},
    }))
    .expect("create");
    assert_eq!(
        schema.vectors[0].quantization,
        Some(Quantization::Scalar {
            quantile_ppm: Some(990_000),
            always_ram: false
        })
    );
    let schema = create(json!({
        "vectors": {"size": 4, "distance": "Cosine",
                    "quantization_config": {"product": {"compression": "x16"}}},
    }))
    .expect("create");
    assert_eq!(
        schema.vectors[0].quantization,
        Some(Quantization::Product {
            compression_ratio: 16,
            always_ram: false
        })
    );
    assert_bad_request(
        create(json!({"vectors": {"size": 4, "distance": "Cosine"},
                      "quantization_config": {"scalar": {"type": "int8", "quantile": 0.2}}})),
        "quantile",
    );
}

#[test]
fn unsupported_create_settings_are_501() {
    let vector = |extra: Value| {
        let mut v = json!({"size": 4, "distance": "Cosine"});
        v.as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        v
    };
    assert_unsupported(
        create(
            json!({"vectors": vector(json!({"multivector_config": {"comparator": "max_sim"}}))}),
        ),
        "multivectors",
    );
    assert_unsupported(
        create(json!({"vectors": vector(json!({"datatype": "uint8"}))})),
        "datatype uint8",
    );
    assert_unsupported(
        create(json!({"sparse_vectors": {"s": {"index": {"datatype": "float16"}}}})),
        "sparse datatype float16",
    );
    assert_unsupported(
        create(json!({"vectors": vector(json!({})), "sharding_method": "custom"})),
        "custom sharding",
    );
    assert_unsupported(
        create(
            json!({"vectors": vector(json!({})), "quantization_config": {"turbo": {"bits": 4}}}),
        ),
        "quantization turbo",
    );
    assert_unsupported(
        create(json!({"vectors": vector(json!({})), "init_from": {"collection": "x"}})),
        "init_from",
    );
    // `init_from: null` (1.15 clients) is fine.
    create(json!({"vectors": vector(json!({})), "init_from": null})).expect("null init_from");
    assert_bad_request(
        create(json!({"vectors": {"size": 0, "distance": "Cosine"}})),
        "Vector size must be in 1..=65536",
    );
    assert_bad_request(
        create(json!({"vectors": {"size": 65_537, "distance": "Cosine"}})),
        "Vector size",
    );
    let bad: Result<CreateCollection, _> =
        serde_json::from_value(json!({"vectors": {"size": 4, "distance": "Hamming"}}));
    assert!(bad.is_err(), "an unknown distance is a format error");
}

#[test]
fn sparse_vectors_map_to_sparse_specs() {
    let schema = create(json!({"vectors": {}, "sparse_vectors": {
        "b": {},
        "a": {"index": {"on_disk": true}, "modifier": "idf"},
    }}))
    .expect("create");
    assert!(schema.vectors.is_empty());
    assert_eq!(
        schema.sparse_vectors,
        [
            SparseVectorSpec {
                name: "a".into(),
                modifier: SparseModifier::Idf
            },
            SparseVectorSpec {
                name: "b".into(),
                modifier: SparseModifier::None
            },
        ]
    );
    schema.validate().expect("valid schema");
    assert_bad_request(
        create(json!({"vectors": {"x": {"size": 2, "distance": "Dot"}},
                      "sparse_vectors": {"x": {}}})),
        "Vector name x is used by a dense and a sparse vector",
    );
    assert_bad_request(
        create(json!({"sparse_vectors": {"": {}}})),
        "Sparse vector name must not be empty",
    );
}

#[test]
fn info_json_echoes_sparse_vectors_within_the_1_15_forbid_fields() {
    let info = info_json(json!({"vectors": {}, "sparse_vectors": {"s": {
        "index": {"full_scan_threshold": 100, "on_disk": true, "memory": "cold", "datatype": "float32"},
        "modifier": "idf",
    }}}));
    assert_eq!(
        info["config"]["params"]["sparse_vectors"],
        json!({"s": {"index": {"full_scan_threshold": 100, "on_disk": true, "datatype": "float32"},
                     "modifier": "idf"}})
    );
    let info = info_json(json!({"vectors": {"size": 2, "distance": "Dot"}}));
    assert!(
        info["config"]["params"].get("sparse_vectors").is_none(),
        "{info}"
    );
    let info = info_json(json!({"vectors": {"size": 2, "distance": "Dot"}, "sparse_vectors": {}}));
    assert_eq!(info["config"]["params"]["sparse_vectors"], json!({}));
}

/// The 1.15.1 `extra="forbid"` models and the keys they allow.
const VECTOR_KEYS: &[&str] = &[
    "size",
    "distance",
    "hnsw_config",
    "quantization_config",
    "on_disk",
    "datatype",
];
const HNSW_KEYS: &[&str] = &[
    "m",
    "ef_construct",
    "full_scan_threshold",
    "max_indexing_threads",
    "on_disk",
    "payload_m",
];

fn only_keys(v: &Value, keys: &[&str], at: &str) {
    let obj = v
        .as_object()
        .unwrap_or_else(|| panic!("{at} is not an object: {v}"));
    for (k, x) in obj {
        assert!(keys.contains(&k.as_str()), "{at}.{k} is not allowed: {v}");
        assert!(!x.is_null(), "{at}.{k} is null");
    }
}

fn check_quantization(q: &Value, at: &str) {
    let obj = q.as_object().expect("quantization object");
    assert_eq!(obj.len(), 1, "{at}: {q}");
    let (kind, body) = obj.iter().next().expect("one kind");
    let keys: &[&str] = match kind.as_str() {
        "scalar" => &["type", "quantile", "always_ram"],
        "product" => &["compression", "always_ram"],
        "binary" => &["always_ram", "encoding", "query_encoding"],
        other => panic!("{at}: unexpected quantization {other}"),
    };
    only_keys(body, keys, &format!("{at}.{kind}"));
}

fn check_vector(v: &Value, at: &str) {
    only_keys(v, VECTOR_KEYS, at);
    if let Some(h) = v.get("hnsw_config") {
        only_keys(h, HNSW_KEYS, &format!("{at}.hnsw_config"));
    }
    if let Some(q) = v.get("quantization_config") {
        check_quantization(q, &format!("{at}.quantization_config"));
    }
}

/// Every forbid object of a `CollectionInfo` holds only its 1.15.1 keys.
fn check_forbid_models(info: &Value) {
    let params = &info["config"]["params"];
    let vectors = &params["vectors"];
    if vectors.get("size").is_some() {
        check_vector(vectors, "vectors");
    } else {
        for (name, v) in vectors.as_object().expect("vectors map") {
            check_vector(v, &format!("vectors.{name}"));
        }
    }
    for (name, s) in params["sparse_vectors"].as_object().into_iter().flatten() {
        only_keys(s, &["index", "modifier"], &format!("sparse_vectors.{name}"));
        if let Some(i) = s.get("index") {
            only_keys(
                i,
                &["full_scan_threshold", "on_disk", "datatype"],
                &format!("sparse_vectors.{name}.index"),
            );
        }
    }
    only_keys(&info["config"]["hnsw_config"], HNSW_KEYS, "hnsw_config");
    if let Some(q) = info["config"].get("quantization_config") {
        check_quantization(q, "quantization_config");
    }
    for (key, s) in info["payload_schema"].as_object().into_iter().flatten() {
        if let Some(p) = s.get("params") {
            only_keys(
                p,
                &["type", "tokenizer", "lowercase", "phrase_matching"],
                &format!("payload_schema.{key}.params"),
            );
        }
    }
}

#[test]
fn info_json_parses_under_the_1_15_forbid_fields() {
    let info = info_json(json!({
        "vectors": {"a": {"size": 4, "distance": "Cosine", "on_disk": true, "datatype": "float32",
                          "hnsw_config": {"m": 8, "inline_storage": true, "memory": "cold"},
                          "quantization_config": {"binary": {"always_ram": true, "encoding": "two_bits",
                                                             "query_encoding": "scalar8bits", "memory": "x"}},
                          "memory": "cold"},
                    "b": {"size": 2, "distance": "Manhattan",
                          "quantization_config": {"product": {"compression": "x8", "always_ram": false, "extra": 1}}}},
        "shard_number": 3, "sharding_method": "auto", "replication_factor": 2,
        "write_consistency_factor": 1, "on_disk_payload": false,
        "hnsw_config": {"m": 32, "ef_construct": 64, "full_scan_threshold": 1, "max_indexing_threads": 2,
                        "on_disk": true, "payload_m": 4, "inline_storage": false},
        "wal_config": {"wal_capacity_mb": 64},
        "optimizers_config": {"memmap_threshold": 1000},
        "quantization_config": {"scalar": {"type": "int8", "quantile": 0.95, "always_ram": true, "memory": "x"}},
        "sparse_vectors": {"s": {"index": {"on_disk": false, "memory": "pinned"}, "modifier": "none"}},
        "strict_mode_config": {"enabled": false},
        "metadata": {"owner": "me"},
    }));
    check_forbid_models(&info);
    let config = &info["config"];
    assert_eq!(config["hnsw_config"]["m"], 32);
    assert_eq!(config["hnsw_config"]["payload_m"], 4);
    assert_eq!(config["optimizer_config"]["memmap_threshold"], 1000);
    assert_eq!(config["wal_config"]["wal_capacity_mb"], 64);
    assert_eq!(config["params"]["shard_number"], 3);
    assert_eq!(config["params"]["on_disk_payload"], false);
    assert_eq!(config["metadata"], json!({"owner": "me"}));
    assert_eq!(info["status"], "green");
    assert_eq!(info["segments_count"], 1);

    // Defaults for a bare create.
    let info = info_json(json!({"vectors": {"size": 2, "distance": "Dot"}}));
    check_forbid_models(&info);
    let config = &info["config"];
    assert_eq!(
        config["hnsw_config"],
        json!({"m": 16, "ef_construct": 100, "full_scan_threshold": 10000,
               "max_indexing_threads": 0, "on_disk": false})
    );
    assert_eq!(config["params"]["shard_number"], 1);
    assert_eq!(config["params"]["replication_factor"], 1);
    assert_eq!(config["params"]["write_consistency_factor"], 1);
    assert_eq!(config["params"]["on_disk_payload"], true);
    assert!(config["params"].get("sharding_method").is_none());
    assert!(config.get("quantization_config").is_none());
    assert!(config.get("metadata").is_none());
    assert_eq!(
        config["wal_config"],
        json!({"wal_capacity_mb": 32, "wal_segments_ahead": 0})
    );
    assert_eq!(config["optimizer_config"]["indexing_threshold"], 10000);
}

#[test]
fn info_echoes_the_vectors_form() {
    let info =
        info_json(json!({"vectors": {"": {"size": 4, "distance": "Cosine", "on_disk": true}}}));
    assert_eq!(
        info["config"]["params"]["vectors"],
        json!({"": {"size": 4, "distance": "Cosine", "on_disk": true}})
    );
    let info = info_json(json!({"vectors": {"size": 4, "distance": "Euclid"}}));
    assert_eq!(
        info["config"]["params"]["vectors"],
        json!({"size": 4, "distance": "Euclid"})
    );
    let info = info_json(json!({"vectors": {}, "sparse_vectors": {"s": {}}}));
    assert_eq!(info["config"]["params"]["vectors"], json!({}));

    // A collection made elsewhere is rendered from its schema.
    let mut schema = create(json!({"vectors": {"size": 3, "distance": "Dot"},
                                   "sparse_vectors": {"s": {"modifier": "idf"}}}))
    .expect("create");
    schema.annotations.clear();
    let info = collection_info_json(&info_of(schema), 0, &BTreeMap::new());
    assert_eq!(
        info["config"]["params"]["vectors"],
        json!({"size": 3, "distance": "Dot"})
    );
    assert_eq!(
        info["config"]["params"]["sparse_vectors"],
        json!({"s": {"modifier": "idf"}})
    );
}
