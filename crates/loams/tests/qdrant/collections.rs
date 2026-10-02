//! Collections, aliases, snapshots and the no-op cluster endpoints (plan
//! M1.4 Task 3).

use std::collections::BTreeMap;

use loams_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, PrimaryKey, Quantization, SparseModifier,
    SparseVectorSpec,
};
use loams_common::{CollectionId, StreamId};
use loams_qdrant::GatewayError;
use loams_qdrant::model::collections::{AliasOperation, CreateAlias, RenameAlias};
use loams_qdrant::proto::qdrant as pb;
use loams_qdrant::schema::{alias_actions, alias_pairs, aliases_response};
use loams_query::{AliasAction, CollectionInfo, OpResult, ServiceError, WriteOptions};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;

const NS: &str = "default";

fn error(body: &Value) -> &str {
    body["status"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no status.error: {body}"))
}

impl Qd {
    /// `PUT /collections/{name}` with `body`, which must succeed.
    async fn create(&self, name: &str, body: Value) {
        let (status, reply) = self.put(&format!("/collections/{name}"), Some(body)).await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        assert_eq!(reply["result"], true, "{reply}");
    }

    /// `GET /collections/{name}`'s `result`.
    async fn info(&self, name: &str) -> Value {
        let (status, reply) = self.get(&format!("/collections/{name}"), None).await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        reply["result"].clone()
    }

    async fn aliases(&self, body: Value) -> (StatusCode, Value) {
        self.post("/collections/aliases", Some(body)).await
    }

    async fn schema(&self, name: &str) -> CollectionSchema {
        self.server
            .collections()
            .get_collection(NS, name)
            .await
            .expect("get_collection")
            .schema
    }

    async fn write_docs(&self, name: &str, n: u64) {
        let ops = (0..n)
            .map(|pk| {
                let source = json!({"n": pk});
                DocOp::Upsert(Document {
                    pk: PrimaryKey::U64(pk),
                    source: source.as_object().cloned().unwrap_or_default(),
                    vectors: BTreeMap::from([(String::new(), vec![1.0, pk as f32])]),
                    sparse_vectors: BTreeMap::new(),
                })
            })
            .collect();
        let result = self
            .server
            .collections()
            .write(NS, name, ops, WriteOptions::default())
            .await
            .expect("write");
        for op in &result.results {
            assert!(!matches!(op, OpResult::Rejected(_)), "{op:?}");
        }
    }
}

fn dense(size: u64) -> Value {
    json!({"size": size, "distance": "Cosine"})
}

// ----- create and info -----

#[tokio::test]
async fn create_accepts_both_vector_forms_and_echoes_them() {
    let qd = Qd::start().await;
    qd.create(
        "single",
        json!({"vectors": {"size": 4, "distance": "Cosine", "on_disk": true}}),
    )
    .await;
    qd.create(
        "named",
        json!({"vectors": {"": {"size": 4, "distance": "Euclid"}, "b": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    assert_eq!(
        qd.info("single").await["config"]["params"]["vectors"],
        json!({"size": 4, "distance": "Cosine", "on_disk": true})
    );
    assert_eq!(
        qd.info("named").await["config"]["params"]["vectors"],
        json!({"": {"size": 4, "distance": "Euclid"}, "b": {"size": 2, "distance": "Dot"}})
    );
    let names: Vec<String> = qd
        .schema("named")
        .await
        .vectors
        .into_iter()
        .map(|v| v.name)
        .collect();
    assert_eq!(names, ["", "b"]);
}

#[tokio::test]
async fn noop_settings_are_echoed() {
    let qd = Qd::start().await;
    qd.create(
        "c",
        json!({
            "vectors": {"size": 4, "distance": "Cosine", "on_disk": true},
            "optimizers_config": {"memmap_threshold": 1000},
            "on_disk_payload": true,
            "shard_number": 2,
            "replication_factor": 1,
            "write_consistency_factor": 1,
        }),
    )
    .await;
    let info = qd.info("c").await;
    let config = &info["config"];
    assert_eq!(config["params"]["vectors"]["on_disk"], true);
    assert_eq!(config["optimizer_config"]["memmap_threshold"], 1000);
    assert_eq!(config["params"]["on_disk_payload"], true);
    assert_eq!(config["params"]["shard_number"], 2);
    assert_eq!(info["status"], "green");
    assert_eq!(info["points_count"], 0);
}

#[tokio::test]
async fn an_empty_sparse_map_is_accepted() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4), "sparse_vectors": {}}))
        .await;
    assert_eq!(
        qd.info("c").await["config"]["params"]["sparse_vectors"],
        json!({})
    );
    let schema = qd.schema("c").await;
    assert_eq!(schema.vectors.len(), 1);
    assert!(schema.sparse_vectors.is_empty());
}

#[tokio::test]
async fn sparse_vectors_are_created_and_echoed() {
    let qd = Qd::start().await;
    // LangChain's SPARSE mode.
    qd.create(
        "lc",
        json!({"vectors": {}, "sparse_vectors": {"my-sparse-vector": {}}}),
    )
    .await;
    // LlamaIndex's hybrid collection.
    let li_sparse = json!({"text-sparse-new": {"index": {}, "modifier": "idf"}});
    qd.create(
        "li",
        json!({"vectors": {"text-dense": dense(4)}, "sparse_vectors": li_sparse}),
    )
    .await;
    assert_eq!(
        qd.info("lc").await["config"]["params"]["sparse_vectors"],
        json!({"my-sparse-vector": {}})
    );
    assert_eq!(
        qd.info("li").await["config"]["params"]["sparse_vectors"],
        li_sparse
    );
    assert_eq!(
        qd.schema("lc").await.sparse_vectors,
        [SparseVectorSpec {
            name: "my-sparse-vector".into(),
            modifier: SparseModifier::None
        }]
    );
    assert_eq!(
        qd.schema("li").await.sparse_vectors,
        [SparseVectorSpec {
            name: "text-sparse-new".into(),
            modifier: SparseModifier::Idf
        }]
    );

    // The same over gRPC.
    let mut collections = qd.collections().await;
    collections
        .create(pb::CreateCollection {
            collection_name: "grpc".into(),
            vectors_config: Some(pb::VectorsConfig {
                config: Some(pb::vectors_config::Config::ParamsMap(pb::VectorParamsMap {
                    map: Default::default(),
                })),
            }),
            sparse_vectors_config: Some(pb::SparseVectorConfig {
                map: [(
                    "text-sparse-new".to_string(),
                    pb::SparseVectorParams {
                        index: Some(pb::SparseIndexConfig::default()),
                        modifier: Some(pb::Modifier::Idf as i32),
                    },
                )]
                .into(),
            }),
            ..Default::default()
        })
        .await
        .expect("Create");
    let got = collections
        .get(pb::GetCollectionInfoRequest {
            collection_name: "grpc".into(),
        })
        .await
        .expect("Get")
        .into_inner();
    let params = got.result.unwrap().config.unwrap().params.unwrap();
    let sparse = params.sparse_vectors_config.expect("sparse config");
    let entry = &sparse.map["text-sparse-new"];
    assert_eq!(entry.modifier, Some(pb::Modifier::Idf as i32));
    assert_eq!(entry.index, Some(pb::SparseIndexConfig::default()));
    assert_eq!(
        qd.info("grpc").await["config"]["params"]["sparse_vectors"],
        li_sparse
    );
    assert_eq!(
        qd.schema("grpc").await.sparse_vectors[0].modifier,
        SparseModifier::Idf
    );
}

#[tokio::test]
async fn a_sparse_only_collection_is_allowed() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": {}, "sparse_vectors": {"s": {}}}))
        .await;
    assert_eq!(qd.info("c").await["config"]["params"]["vectors"], json!({}));
    assert!(qd.schema("c").await.vectors.is_empty());
}

#[tokio::test]
async fn creating_an_existing_collection_is_409_already_exists() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (status, body) = qd
        .put("/collections/c", Some(json!({"vectors": dense(4)})))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error(&body), "Wrong input: Collection `c` already exists!");
    assert!(error(&body).contains("already exists"));
}

#[tokio::test]
async fn invalid_create_bodies_are_400_or_501() {
    let qd = Qd::start().await;
    let (status, body) = qd
        .put(
            "/collections/c",
            Some(json!({"vectors": {"size": 4, "distance": "Hamming"}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        error(&body).starts_with("Format error in JSON body"),
        "{body}"
    );
    let (status, body) = qd
        .put(
            "/collections/c",
            Some(json!({"vectors": {"size": 4, "distance": "Cosine", "datatype": "uint8"}})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(error(&body), "Unsupported in Loams: datatype uint8");
    let (status, body) = qd
        .put(
            "/collections/c",
            Some(json!({"vectors": {"size": 0, "distance": "Cosine"}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        error(&body),
        "Wrong input: Vector size must be in 1..=65536"
    );
    let (status, _) = qd.get("/collections/c", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "nothing was created");
}

#[tokio::test]
async fn deleting_a_missing_collection_returns_false() {
    let qd = Qd::start().await;
    let (status, body) = qd.delete("/collections/nope", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], false);
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (_, body) = qd.delete("/collections/c", None).await;
    assert_eq!(body["result"], true);
    let (status, body) = qd.get("/collections/c", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error(&body), "Not found: Collection `c` doesn't exist!");
}

#[tokio::test]
async fn exists_reports_collections_and_aliases() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (status, _) = qd
        .aliases(
            json!({"actions": [{"create_alias": {"collection_name": "c", "alias_name": "a"}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    for (name, exists) in [("c", true), ("a", true), ("nope", false)] {
        let (status, body) = qd.get(&format!("/collections/{name}/exists"), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["result"], json!({"exists": exists}), "{name}");
    }
}

#[tokio::test]
async fn list_collections_returns_names_not_aliases() {
    let qd = Qd::start().await;
    qd.create("b", json!({"vectors": dense(4)})).await;
    qd.create("a", json!({"vectors": dense(4)})).await;
    qd.aliases(json!({"actions": [{"create_alias": {"collection_name": "a", "alias_name": "z"}}]}))
        .await;
    let (_, body) = qd.get("/collections", None).await;
    assert_eq!(
        body["result"],
        json!({"collections": [{"name": "a"}, {"name": "b"}]})
    );
}

/// The 1.15.1 `extra="forbid"` models and the keys they allow.
fn only_keys(v: &Value, keys: &[&str], at: &str) {
    let obj = v
        .as_object()
        .unwrap_or_else(|| panic!("{at} is not an object: {v}"));
    for (k, x) in obj {
        assert!(keys.contains(&k.as_str()), "{at}.{k} is not allowed");
        assert!(!x.is_null(), "{at}.{k} is null");
    }
}

const HNSW_KEYS: &[&str] = &[
    "m",
    "ef_construct",
    "full_scan_threshold",
    "max_indexing_threads",
    "on_disk",
    "payload_m",
];

fn only_quantization_keys(q: &Value, at: &str) {
    for (kind, body) in q.as_object().expect("object") {
        let keys: &[&str] = match kind.as_str() {
            "scalar" => &["type", "quantile", "always_ram"],
            "product" => &["compression", "always_ram"],
            "binary" => &["always_ram", "encoding", "query_encoding"],
            other => panic!("{at}: {other}"),
        };
        only_keys(body, keys, at);
    }
}

#[tokio::test]
async fn collection_info_keeps_to_the_1_15_field_subset() {
    let qd = Qd::start().await;
    qd.create(
        "c",
        json!({
            "vectors": {"v": {"size": 4, "distance": "Dot", "on_disk": false, "datatype": "float32",
                              "hnsw_config": {"m": 8, "inline_storage": true},
                              "quantization_config": {"scalar": {"type": "int8", "memory": "cold"}},
                              "memory": "cold"}},
            "shard_number": 1, "sharding_method": "auto", "replication_factor": 1,
            "write_consistency_factor": 1, "on_disk_payload": true,
            "hnsw_config": {"payload_m": 16, "inline_storage": false},
            "wal_config": {"wal_capacity_mb": 16},
            "optimizers_config": {"indexing_threshold": 0},
            "quantization_config": {"binary": {"always_ram": true, "memory": "x"}},
            "sparse_vectors": {"s": {"index": {"memory": "cold", "full_scan_threshold": 10}, "modifier": "idf"}},
            "strict_mode_config": {"enabled": false},
            "metadata": {"k": "v"},
        }),
    )
    .await;
    let info = qd.info("c").await;
    let v = &info["config"]["params"]["vectors"]["v"];
    only_keys(
        v,
        &[
            "size",
            "distance",
            "hnsw_config",
            "quantization_config",
            "on_disk",
            "datatype",
        ],
        "vectors.v",
    );
    only_keys(&v["hnsw_config"], HNSW_KEYS, "vectors.v.hnsw_config");
    only_quantization_keys(&v["quantization_config"], "vectors.v.quantization_config");
    only_keys(&info["config"]["hnsw_config"], HNSW_KEYS, "hnsw_config");
    only_quantization_keys(
        &info["config"]["quantization_config"],
        "quantization_config",
    );
    let s = &info["config"]["params"]["sparse_vectors"]["s"];
    only_keys(s, &["index", "modifier"], "sparse_vectors.s");
    only_keys(
        &s["index"],
        &["full_scan_threshold", "on_disk", "datatype"],
        "sparse_vectors.s.index",
    );
    assert_eq!(info["config"]["hnsw_config"]["payload_m"], 16);
    assert_eq!(info["config"]["optimizer_config"]["indexing_threshold"], 0);
    // The top level has exactly CollectionInfo's fields.
    let keys: Vec<&str> = info
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "status",
            "optimizer_status",
            "segments_count",
            "points_count",
            "indexed_vectors_count",
            "config",
            "payload_schema"
        ]
    );
}

#[tokio::test]
async fn hnsw_and_quantization_params_reach_the_vector_spec() {
    let qd = Qd::start().await;
    qd.create(
        "c",
        json!({
            "vectors": {"size": 4, "distance": "Cosine"},
            "hnsw_config": {"m": 32, "payload_m": 0},
            "quantization_config": {"scalar": {"type": "int8", "quantile": 0.99, "always_ram": true}},
        }),
    )
    .await;
    let schema = qd.schema("c").await;
    let v = &schema.vectors[0];
    assert_eq!(v.hnsw.m, 32);
    assert_eq!(v.hnsw.payload_m, Some(0));
    assert!(matches!(
        v.quantization,
        Some(Quantization::Scalar {
            quantile_ppm: Some(990_000),
            always_ram: true
        })
    ));
}

// ----- aliases -----

#[tokio::test]
async fn aliases_create_rename_delete_atomically() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (status, body) = qd
        .aliases(
            json!({"actions": [{"create_alias": {"collection_name": "c", "alias_name": "a1"}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], true);
    let (status, body) = qd
        .aliases(json!({"actions": [{"rename_alias": {"old_alias_name": "a1", "new_alias_name": "a2"}}]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = qd.get("/collections/a2", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = qd.get("/collections/a1", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A failing action leaves every alias unchanged.
    let (status, body) = qd
        .aliases(json!({"actions": [
            {"create_alias": {"collection_name": "c", "alias_name": "a3"}},
            {"delete_alias": {"alias_name": "a2"}},
            {"rename_alias": {"old_alias_name": "missing", "new_alias_name": "x"}},
        ]}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(error(&body), "Not found: Alias missing does not exists!");
    let (status, body) = qd
        .aliases(json!({"actions": [
            {"create_alias": {"collection_name": "c", "alias_name": "a3"}},
            {"create_alias": {"collection_name": "no_such", "alias_name": "a4"}},
        ]}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "a2", "collection_name": "c"}]})
    );

    let (status, body) = qd
        .aliases(json!({"actions": [{"delete_alias": {"alias_name": "a2"}}]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(body["result"], json!({"aliases": []}));
    let (status, body) = qd
        .aliases(json!({"actions": [{"delete_alias": {"alias_name": "a2"}}]}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn create_alias_repoints_an_existing_alias() {
    let qd = Qd::start().await;
    qd.create("c1", json!({"vectors": dense(4)})).await;
    qd.create("c2", json!({"vectors": dense(2)})).await;
    qd.aliases(
        json!({"actions": [{"create_alias": {"collection_name": "c1", "alias_name": "a"}}]}),
    )
    .await;
    let (status, body) = qd
        .aliases(
            json!({"actions": [{"create_alias": {"collection_name": "c2", "alias_name": "a"}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "a", "collection_name": "c2"}]})
    );
    let (_, body) = qd.get("/collections/c1/aliases", None).await;
    assert_eq!(body["result"], json!({"aliases": []}));
    let (_, body) = qd.get("/collections/c2/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "a", "collection_name": "c2"}]})
    );
    assert_eq!(qd.info("a").await["config"]["params"]["vectors"]["size"], 2);
}

fn info_with_aliases(name: &str, aliases: &[&str]) -> CollectionInfo {
    CollectionInfo {
        id: CollectionId(1),
        name: name.into(),
        namespace: NS.into(),
        schema: CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore),
        partitions: 1,
        aliases: aliases.iter().map(|a| (*a).to_string()).collect(),
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

/// The several-member rules on `CollectionInfo`s directly; the end-to-end
/// test is `a_multi_member_alias_is_listed_per_member_and_refused_where_one_is_needed`.
#[test]
fn alias_pairs_cover_every_member() {
    let infos = [
        info_with_aliases("c1", &["a", "m"]),
        info_with_aliases("c2", &["m"]),
    ];
    let pairs = alias_pairs(&infos);
    let owned = |a: &str, c: &str| (a.to_string(), c.to_string());
    assert_eq!(
        pairs,
        [owned("a", "c1"), owned("m", "c1"), owned("m", "c2")]
    );

    let rename = |old: &str| {
        [AliasOperation::Rename {
            rename_alias: RenameAlias {
                old_alias_name: old.into(),
                new_alias_name: "n".into(),
            },
        }]
    };
    match alias_actions(&rename("m"), &pairs) {
        Err(GatewayError::BadRequest(m)) => assert_eq!(
            m,
            "alias [m] names 2 collections [c1, c2]; this operation needs one collection"
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        alias_actions(&rename("a"), &pairs).expect("rename a"),
        [
            AliasAction::Delete { alias: "a".into() },
            AliasAction::Create {
                alias: "n".into(),
                collection: "c1".into()
            },
        ]
    );
    // create_alias over a multi-target alias makes it single-target.
    let create = [AliasOperation::Create {
        create_alias: CreateAlias {
            collection_name: "c2".into(),
            alias_name: "m".into(),
        },
    }];
    assert_eq!(
        alias_actions(&create, &pairs).expect("create m"),
        [
            AliasAction::Delete { alias: "m".into() },
            AliasAction::Create {
                alias: "m".into(),
                collection: "c2".into()
            },
        ]
    );
    let listed = serde_json::to_value(aliases_response(&pairs, None)).unwrap();
    assert_eq!(
        listed,
        json!({"aliases": [
            {"alias_name": "a", "collection_name": "c1"},
            {"alias_name": "m", "collection_name": "c1"},
            {"alias_name": "m", "collection_name": "c2"},
        ]})
    );
    let of_c2 = serde_json::to_value(aliases_response(&pairs, Some("c2"))).unwrap();
    assert_eq!(
        of_c2,
        json!({"aliases": [{"alias_name": "m", "collection_name": "c2"}]})
    );
    // A missing alias is NotFound.
    assert!(matches!(
        alias_actions(&rename("zz"), &pairs),
        Err(GatewayError::Service(ServiceError::NotFound {
            kind: "alias",
            ..
        }))
    ));
}

/// E14: now that M1.5 Task 0a makes aliases with several members, the
/// Task 3 paths run end to end: `GET /aliases` lists such an alias once per
/// member, `rename_alias` refuses it, a single-collection operation through
/// it is 400, and `create_alias` makes it single-target again.
#[tokio::test]
async fn a_multi_member_alias_is_listed_per_member_and_refused_where_one_is_needed() {
    let qd = Qd::start().await;
    qd.create("c1", json!({"vectors": dense(4)})).await;
    qd.create("c2", json!({"vectors": dense(4)})).await;
    let add = |collection: &str| loams_query::AliasTargetAction::Add {
        alias: "m".to_string(),
        collection: collection.to_string(),
        is_write_index: None,
    };
    qd.server
        .collections()
        .update_alias_targets(NS, vec![add("c1"), add("c2")])
        .await
        .expect("multi-member alias");
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [
            {"alias_name": "m", "collection_name": "c1"},
            {"alias_name": "m", "collection_name": "c2"},
        ]})
    );
    let (_, body) = qd.get("/collections/c2/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "m", "collection_name": "c2"}]})
    );
    let (status, body) = qd
        .aliases(
            json!({"actions": [{"rename_alias": {"old_alias_name": "m", "new_alias_name": "n"}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        error(&body).contains(
            "alias [m] names 2 collections [c1, c2]; this operation needs one collection"
        ),
        "{body}"
    );
    let (status, body) = qd
        .post("/collections/m/points/count", Some(json!({"exact": true})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(error(&body).contains("names 2 collections"), "{body}");
    let (status, body) = qd
        .aliases(
            json!({"actions": [{"create_alias": {"collection_name": "c2", "alias_name": "m"}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "m", "collection_name": "c2"}]})
    );
}

#[tokio::test]
async fn collection_ops_work_through_an_alias() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": {"size": 2, "distance": "Cosine"}}))
        .await;
    qd.aliases(json!({"actions": [{"create_alias": {"collection_name": "c", "alias_name": "a"}}]}))
        .await;
    qd.write_docs("c", 3).await;
    let (_, by_name) = qd
        .post("/collections/c/points/count", Some(json!({"exact": true})))
        .await;
    let (_, by_alias) = qd
        .post("/collections/a/points/count", Some(json!({"exact": true})))
        .await;
    assert_eq!(by_name["result"]["count"], 3, "{by_name}");
    assert_eq!(by_alias["result"], by_name["result"]);
    assert_eq!(qd.info("a").await["points_count"], 3);
    let (_, body) = qd.get("/collections/a/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "a", "collection_name": "c"}]})
    );
}

// ----- update, vectors, cluster, snapshots -----

#[tokio::test]
async fn patch_optimizer_config_is_a_noop() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let before = qd.info("c").await;
    let (status, body) = qd
        .patch(
            "/collections/c",
            Some(json!({"optimizers_config": {"indexing_threshold": 20000},
                        "params": {"replication_factor": 2, "on_disk_payload": false}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], true);
    assert_eq!(qd.info("c").await["config"], before["config"]);
    let (status, _) = qd
        .patch(
            "/collections/missing",
            Some(json!({"optimizers_config": {}})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_vectors_is_501() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (status, body) = qd
        .patch(
            "/collections/c",
            Some(json!({"vectors": {"": {"on_disk": true}}})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(
        error(&body),
        "Unsupported in Loams: collection update vectors"
    );
    let (status, body) = qd
        .patch(
            "/collections/c",
            Some(json!({"params": {"read_fan_out_delay_ms": 5}})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
}

#[tokio::test]
async fn create_vector_name_adds_a_vector() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": {"a": dense(4)}})).await;
    let (status, body) = qd
        .put(
            "/collections/c/vectors/b",
            Some(json!({"size": 3, "distance": "Euclid", "on_disk": true})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], json!({"status": "completed"}));
    let schema = qd.schema("c").await;
    let b = schema.vectors.iter().find(|v| v.name == "b").expect("b");
    assert_eq!(b.dim, 3);
    assert_eq!(
        qd.info("c").await["config"]["params"]["vectors"],
        json!({"a": {"size": 4, "distance": "Cosine"},
               "b": {"size": 3, "distance": "Euclid", "on_disk": true}})
    );
    let (status, body) = qd.put("/collections/c/vectors/a", Some(dense(4))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(error(&body), "Wrong input: Vector a already exists");
}

#[tokio::test]
async fn create_vector_name_refuses_a_sparse_vector() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": dense(4)})).await;
    let (status, body) = qd
        .put("/collections/c/vectors/s", Some(json!({"modifier": "idf"})))
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(
        error(&body),
        "Unsupported in Loams: adding a sparse vector after creation"
    );
    let status = qd
        .points()
        .await
        .create_vector_name(pb::CreateVectorNameRequest {
            collection_name: "c".into(),
            vector_name: "s".into(),
            vector_config: Some(pb::create_vector_name_request::VectorConfig::SparseConfig(
                pb::SparseVectorCreationConfig::default(),
            )),
            ..Default::default()
        })
        .await
        .expect_err("sparse");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn cluster_info_is_synthetic() {
    let qd = Qd::start().await;
    let (status, body) = qd.get("/cluster", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!({"status": "disabled"}));
    qd.create("c", json!({"vectors": {"size": 2, "distance": "Dot"}}))
        .await;
    qd.write_docs("c", 2).await;
    let (status, body) = qd.get("/collections/c/cluster", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"],
        json!({"peer_id": 0, "shard_count": 1,
               "local_shards": [{"shard_id": 0, "points_count": 2, "state": "Active"}],
               "remote_shards": [], "shard_transfers": []})
    );
    let reply = qd
        .collections()
        .await
        .collection_cluster_info(pb::CollectionClusterInfoRequest {
            collection_name: "c".into(),
        })
        .await
        .expect("CollectionClusterInfo")
        .into_inner();
    assert_eq!(reply.shard_count, 1);
    assert_eq!(reply.local_shards[0].points_count, 2);
    let (status, _) = qd.post("/collections/c/cluster", Some(json!({}))).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "mutations stay 501");
}

#[tokio::test]
async fn snapshots_list_manifest_versions() {
    let qd = Qd::start().await;
    qd.create("c", json!({"vectors": {"size": 2, "distance": "Cosine"}}))
        .await;
    // An empty collection has committed no manifest: its snapshot is
    // version 0, the empty collection, and the list shows it (the Python
    // client run of Task 10 found a 503 where Qdrant answers).
    let (status, body) = qd.post("/collections/c/snapshots", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let empty = body["result"].clone();
    assert_eq!(empty["name"], "c-00000000000000000000.snapshot", "{body}");
    assert_eq!(empty["size"], 0, "{body}");
    let (status, body) = qd.get("/collections/c/snapshots", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], json!([empty]), "{body}");
    // Right after the first write the snapshot waits for the first
    // manifest, which holds the write.
    qd.write_docs("c", 4).await;
    let (status, body) = qd.post("/collections/c/snapshots?wait=true", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let created = body["result"].clone();
    assert_ne!(created["name"], empty["name"], "{body}");
    let name = created["name"].as_str().expect("name");
    let digits = name
        .strip_prefix("c-")
        .and_then(|n| n.strip_suffix(".snapshot"))
        .expect("c-<version>.snapshot");
    assert_eq!(digits.len(), 20, "{name}");
    assert!(digits.bytes().all(|b| b.is_ascii_digit()), "{name}");
    let time = created["creation_time"].as_str().expect("creation_time");
    assert_eq!(time.len(), "2026-09-27T00:00:00.000000".len(), "{time}");
    assert!(created["size"].is_u64(), "{created}");
    assert!(created.get("checksum").is_none());
    let (status, body) = qd.get("/collections/c/snapshots", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed = body["result"].as_array().expect("list");
    assert!(listed.contains(&created), "{body}");

    let reply = qd
        .snapshots()
        .await
        .list(pb::ListSnapshotsRequest {
            collection_name: "c".into(),
        })
        .await
        .expect("List")
        .into_inner();
    assert!(reply.snapshot_descriptions.iter().any(|d| d.name == name));
    let reply = qd
        .snapshots()
        .await
        .create(pb::CreateSnapshotRequest {
            collection_name: "c".into(),
        })
        .await
        .expect("Create")
        .into_inner();
    let described = reply.snapshot_description.expect("description");
    assert!(described.creation_time.is_some());
}

#[tokio::test]
async fn grpc_collections_round_trip() {
    let qd = Qd::start().await;
    let mut collections = qd.collections().await;
    let reply = collections
        .create(pb::CreateCollection {
            collection_name: "g".into(),
            vectors_config: Some(pb::VectorsConfig {
                config: Some(pb::vectors_config::Config::Params(pb::VectorParams {
                    size: 4,
                    distance: pb::Distance::Euclid as i32,
                    ..Default::default()
                })),
            }),
            shard_number: Some(2),
            ..Default::default()
        })
        .await
        .expect("Create")
        .into_inner();
    assert!(reply.result);
    let status = collections
        .create(pb::CreateCollection {
            collection_name: "g".into(),
            ..Default::default()
        })
        .await
        .expect_err("exists");
    assert_eq!(status.code(), tonic::Code::AlreadyExists);

    let info = collections
        .get(pb::GetCollectionInfoRequest {
            collection_name: "g".into(),
        })
        .await
        .expect("Get")
        .into_inner()
        .result
        .expect("info");
    let params = info.config.unwrap().params.unwrap();
    assert_eq!(params.shard_number, 2);
    match params.vectors_config.and_then(|v| v.config) {
        Some(pb::vectors_config::Config::Params(p)) => {
            assert_eq!(p.size, 4);
            assert_eq!(p.distance, pb::Distance::Euclid as i32);
        }
        other => panic!("{other:?}"),
    }
    // REST sees the same collection.
    assert_eq!(
        qd.info("g").await["config"]["params"]["vectors"],
        json!({"size": 4, "distance": "Euclid"})
    );

    let listed = collections
        .list(pb::ListCollectionsRequest {})
        .await
        .expect("List")
        .into_inner();
    let names: Vec<_> = listed.collections.into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["g"]);

    let reply = collections
        .update_aliases(pb::ChangeAliases {
            actions: vec![pb::AliasOperations {
                action: Some(pb::alias_operations::Action::CreateAlias(pb::CreateAlias {
                    collection_name: "g".into(),
                    alias_name: "ga".into(),
                })),
            }],
            timeout: None,
        })
        .await
        .expect("UpdateAliases")
        .into_inner();
    assert!(reply.result);
    let exists = collections
        .collection_exists(pb::CollectionExistsRequest {
            collection_name: "ga".into(),
        })
        .await
        .expect("CollectionExists")
        .into_inner();
    assert!(exists.result.unwrap().exists);
    let all = collections
        .list_aliases(pb::ListAliasesRequest {})
        .await
        .expect("ListAliases")
        .into_inner();
    let pairs: Vec<_> = all
        .aliases
        .iter()
        .map(|a| (a.alias_name.as_str(), a.collection_name.as_str()))
        .collect();
    assert_eq!(pairs, [("ga", "g")]);
    let of_g = collections
        .list_collection_aliases(pb::ListCollectionAliasesRequest {
            collection_name: "g".into(),
        })
        .await
        .expect("ListCollectionAliases")
        .into_inner();
    assert_eq!(of_g.aliases, all.aliases);
    let (_, body) = qd.get("/aliases", None).await;
    assert_eq!(
        body["result"],
        json!({"aliases": [{"alias_name": "ga", "collection_name": "g"}]})
    );

    let reply = collections
        .update(pb::UpdateCollection {
            collection_name: "g".into(),
            optimizers_config: Some(pb::OptimizersConfigDiff::default()),
            ..Default::default()
        })
        .await
        .expect("Update")
        .into_inner();
    assert!(reply.result);

    let reply = collections
        .delete(pb::DeleteCollection {
            collection_name: "g".into(),
            timeout: None,
        })
        .await
        .expect("Delete")
        .into_inner();
    assert!(reply.result);
    let exists = collections
        .collection_exists(pb::CollectionExistsRequest {
            collection_name: "g".into(),
        })
        .await
        .expect("CollectionExists")
        .into_inner();
    assert!(!exists.result.unwrap().exists);
    let (status, _) = qd.get("/collections/g", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
