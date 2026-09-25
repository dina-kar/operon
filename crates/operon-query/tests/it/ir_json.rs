//! The search IR's JSON wire form, the service types, errors and validation
//! (plan M1.2 Task 1).

use std::collections::{BTreeMap, BTreeSet};

use operon_collection::{
    ConsistencyToken, Distance, DynamicMapping, FieldKind, HnswParams, PrimaryKey, SparseModifier,
    SparseVector, VectorElement, VectorIndexSpec,
};
use operon_common::{CollectionId, StreamId};
use operon_query::hot::{HotKind, HotState, HotStateKind, HotStatus};
use operon_query::json;
use operon_query::{
    AnnParams, BoolOperator, CollectionInfo, FieldValue, Fusion, Fuzziness, GroupBy, Highlight,
    HighlightField, Hit, HitGroup, MissingOrder, MultiMatchKind, OpPosition, OpResult, Projection,
    Query, ReadConsistency, Retriever, SearchLimits, SearchRequest, SearchResponse, ServiceError,
    SortKey, SortOrder, SortValue, SourceFilter, SparseParams, StoredDoc, TotalHits, TotalRelation,
    TrackTotalHits, WriteResult, validate_request,
};
use proptest::prelude::*;
use serde_json::{Map, Value, json};

const UUID_BYTES: [u8; 16] = [
    0x01, 0x90, 0xf5, 0xc4, 0x6c, 0x1e, 0x7b, 0x3a, 0x9d, 0x2e, 0x4f, 0x5a, 0x6b, 0x7c, 0x8d, 0x9e,
];

fn token(text: &str) -> ConsistencyToken {
    text.parse().expect("token")
}

fn from<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).expect("deserialize")
}

fn to(value: &impl serde::Serialize) -> Value {
    serde_json::to_value(value).expect("serialize")
}

fn fixture(name: &str) -> Value {
    let path = format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

fn obj(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

fn text_retriever(field: &str, text: &str, k: usize) -> Retriever {
    Retriever::Text {
        query: Query::Match {
            field: field.to_string(),
            text: text.to_string(),
            operator: BoolOperator::Or,
            minimum_should_match: None,
            fuzziness: None,
            analyzer: None,
        },
        k,
    }
}

fn vector_retriever(field: &str, query: Vec<f32>, k: usize) -> Retriever {
    Retriever::Vector {
        field: field.to_string(),
        query,
        k,
        params: AnnParams::default(),
        filter: None,
    }
}

#[test]
fn sparse_retrievers_use_the_json_form() {
    let retriever: Retriever = from(json!({"sparse": {
        "field": "s", "query": {"indices": [5, 1], "values": [0.5, 2.0]}, "k": 3
    }}));
    let Retriever::Sparse {
        field,
        query,
        k,
        filter,
        params,
    } = &retriever
    else {
        panic!("not sparse: {retriever:?}");
    };
    assert_eq!(field, "s");
    assert_eq!(query.indices(), [1, 5]);
    assert_eq!(query.values(), [2.0, 0.5]);
    assert_eq!(*k, 3);
    assert_eq!(*filter, None);
    assert_eq!(*params, SparseParams::default());
    assert_eq!(
        to(&retriever),
        json!({"sparse": {
            "field": "s", "query": {"indices": [1, 5], "values": [2.0, 0.5]}, "k": 3,
            "filter": null, "params": {"idf_corpus": null}
        }})
    );

    for bad in [
        json!({"indices": [1, 1], "values": [1, 2]}),
        json!({"indices": [1], "values": []}),
    ] {
        let body = json!({"sparse": {"field": "s", "query": bad, "k": 3}});
        assert!(
            serde_json::from_value::<Retriever>(body.clone()).is_err(),
            "{body}"
        );
    }

    let hit = Hit {
        pk: PrimaryKey::U64(1),
        score: 1.0,
        sort_values: vec![],
        source: None,
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        highlight: BTreeMap::new(),
        fields: BTreeMap::new(),
    };
    let value = obj(to(&hit));
    assert!(!value.contains_key("sparse_vectors"), "{value:?}");
    assert!(!value.contains_key("fields"), "{value:?}");
}

#[test]
fn the_wire_form_matches_the_fixture() {
    let file = fixture("hybrid_request.json");
    let request: SearchRequest = from(file.clone());
    let expected = SearchRequest {
        retrievers: vec![
            vector_retriever("embedding", vec![1.0, 0.0, 0.0], 10),
            text_retriever("body", "refund", 10),
        ],
        fusion: Some(Fusion::Rrf { k: 60 }),
        limit: 3,
        ..SearchRequest::new("kb")
    };
    assert_eq!(request, expected);
    assert_eq!(to(&request), file);
}

#[test]
fn primary_keys_use_the_a12_json_form() {
    let pk = |value: Value| json::pk::from_json(&value);
    assert_eq!(pk(json!(5)), Ok(PrimaryKey::U64(5)));
    assert_eq!(pk(json!("5")), Ok(PrimaryKey::Str("5".to_string())));
    let uuid = pk(json!({"uuid": "0190F5C4-6C1E-7B3A-9D2E-4F5A6B7C8D9E"})).expect("uuid");
    assert_eq!(uuid, PrimaryKey::Uuid(UUID_BYTES));
    assert_eq!(
        json::pk::to_json(&uuid),
        json!({"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"})
    );
    assert_eq!(
        pk(json!(18_446_744_073_709_551_615_u64)),
        Ok(PrimaryKey::U64(u64::MAX))
    );
    for bad in [
        json!(true),
        json!(1.5),
        json!(-1),
        json!(null),
        json!({"x": 1}),
    ] {
        assert!(
            matches!(pk(bad.clone()), Err(ServiceError::InvalidArgument(_))),
            "{bad}"
        );
    }
    // The serde adapter speaks the same form.
    let query: Query =
        from(json!({"ids": [5, "5", {"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"}]}));
    assert_eq!(
        query,
        Query::Ids(vec![
            PrimaryKey::U64(5),
            PrimaryKey::Str("5".to_string()),
            PrimaryKey::Uuid(UUID_BYTES)
        ])
    );
}

#[test]
fn schema_json_defaults_follow_the_sdk_contract() {
    let schema = json::schema::from_json(&json!({
        "fields": [{"name": "body", "kind": {"text": {"analyzer": "english"}}}],
        "vectors": [{"name": "e", "dim": 3, "distance": "cosine"}]
    }))
    .expect("schema");
    let field = &schema.fields[0];
    assert_eq!(field.source_path, "body");
    assert!(field.indexed);
    assert!(!field.fast);
    assert!(!field.ignore_malformed);
    assert_eq!(
        field.kind,
        FieldKind::Text {
            analyzer: "english".to_string(),
            positions: true
        }
    );
    assert_eq!(schema.dynamic, DynamicMapping::Strict);
    assert_eq!(schema.max_fields, 1000);
    assert!(schema.annotations.is_empty());
    let vector = &schema.vectors[0];
    assert_eq!(vector.dim, 3);
    assert_eq!(vector.distance, Distance::Cosine);
    assert_eq!(vector.element, VectorElement::F32);
    assert_eq!(vector.index, VectorIndexSpec::Auto);
    assert_eq!(vector.hnsw, HnswParams::default());
    assert_eq!(vector.quantization, None);
    assert!(schema.sparse_vectors.is_empty());

    let sparse = json::schema::from_json(&json!({
        "fields": [], "vectors": [], "sparse_vectors": [{"name": "s"}]
    }))
    .expect("sparse schema");
    assert_eq!(sparse.sparse_vectors[0].modifier, SparseModifier::None);

    let out = json::schema::to_json(&schema);
    assert_eq!(
        out,
        json!({
            "version": schema.version,
            "fields": [{"name": "body", "source_path": "body",
                        "kind": {"text": {"analyzer": "english", "positions": true}},
                        "indexed": true, "fast": false, "ignore_malformed": false}],
            "vectors": [{"name": "e", "dim": 3, "distance": "cosine", "element": "f32", "index": "auto",
                         "hnsw": {"m": 16, "ef_construct": 100, "full_scan_threshold_kb": 10000,
                                  "payload_m": null, "on_disk": false},
                         "quantization": null}],
            "sparse_vectors": [],
            "dynamic": "strict",
            "max_fields": 1000,
            "annotations": {}
        })
    );
    // The output form reads back as the same schema.
    assert_eq!(json::schema::from_json(&out), Ok(schema));

    assert_eq!(
        json::schema::from_json(
            &json!({"fields": [{"name": "x", "kind": "keyword", "fats": true}]})
        ),
        Err(ServiceError::InvalidArgument(
            "unknown key fats in field".to_string()
        ))
    );
}

#[test]
fn consistency_json_forms() {
    let forms = [
        (json!("strong"), ReadConsistency::Strong),
        (json!("eventual"), ReadConsistency::Eventual),
        (
            json!({"at_least": "v1:s7/p3@918274"}),
            ReadConsistency::AtLeast(token("v1:s7/p3@918274")),
        ),
        (
            json!({"pinned": {"manifest_version": 4, "token": "v1:s1/p0@9,s1/p1@3"}}),
            ReadConsistency::Pinned {
                manifest_version: 4,
                token: token("v1:s1/p0@9,s1/p1@3"),
            },
        ),
    ];
    for (value, consistency) in forms {
        assert_eq!(from::<ReadConsistency>(value.clone()), consistency);
        assert_eq!(to(&consistency), value);
    }
    assert!(serde_json::from_value::<ReadConsistency>(json!({"at_least": "v1:s01/p0@1"})).is_err());
}

#[test]
fn service_errors_round_trip_their_bodies() {
    let cases = [
        (
            ServiceError::NotFound {
                kind: "pin",
                name: "docs@3".to_string(),
            },
            "not_found",
            404,
        ),
        (
            ServiceError::AlreadyExists("docs".to_string()),
            "already_exists",
            409,
        ),
        (
            ServiceError::InvalidArgument("k must be between 1 and 100000".to_string()),
            "invalid_argument",
            400,
        ),
        (
            ServiceError::SchemaViolation {
                field: "n".to_string(),
                message: "not an integer: x".to_string(),
            },
            "schema_violation",
            400,
        ),
        (
            ServiceError::Unavailable("the tail is rebuilding".to_string()),
            "unavailable",
            503,
        ),
        (ServiceError::Timeout, "timeout", 504),
        (
            ServiceError::Internal("boom: with a colon".to_string()),
            "internal",
            500,
        ),
    ];
    for (err, code, status) in cases {
        assert_eq!(err.code(), code);
        assert_eq!(err.http_status(), status);
        assert_eq!(
            err.is_retryable(),
            matches!(err, ServiceError::Unavailable(_) | ServiceError::Timeout)
        );
        let body = err.to_body();
        assert_eq!(body["error"], json!(code));
        assert_eq!(body["message"], json!(err.to_string()));
        assert_eq!(ServiceError::from_body(&body), Some(err.clone()));
        assert_eq!(from::<ServiceError>(to(&err)), err);
    }
    let not_found = ServiceError::NotFound {
        kind: "pin",
        name: "c@1".to_string(),
    }
    .to_body();
    assert_eq!(not_found["kind"], json!("pin"));
    assert_eq!(not_found["name"], json!("c@1"));
    let violation = ServiceError::SchemaViolation {
        field: "n".to_string(),
        message: "m".to_string(),
    }
    .to_body();
    assert_eq!(violation["field"], json!("n"));
    assert_eq!(
        ServiceError::from_body(
            &json!({"error": "not_found", "message": "x", "kind": "planet", "name": "mars"})
        ),
        Some(ServiceError::NotFound {
            kind: "object",
            name: "mars".to_string()
        })
    );
    assert_eq!(
        ServiceError::from_body(&json!({"error": "teapot", "message": "x"})),
        None
    );
}

/// One invalid request per rule of Task 1 rule 4, in order.
#[test]
fn validation_messages_are_exact() {
    let limits = SearchLimits::default();
    assert_eq!(
        limits,
        SearchLimits {
            max_window: 100_000,
            max_retrievers: 16,
            max_fused_depth: 4,
            max_query_clauses: 1_024,
        }
    );
    let base = || SearchRequest::new("docs");
    let text = || text_retriever("body", "refund", 10);
    let vector = || vector_retriever("e", vec![1.0, 0.0], 10);
    let fused = |inputs: Vec<Retriever>| Retriever::Fused {
        inputs,
        fusion: Fusion::Rrf { k: 60 },
        k: 10,
    };
    let mut nested = text();
    for _ in 0..5 {
        nested = fused(vec![nested]);
    }
    let big_bool = Query::Bool {
        must: vec![Query::MatchAll; 1_024],
        should: vec![],
        must_not: vec![],
        filter: vec![],
        minimum_should_match: None,
    };

    let cases: Vec<(SearchRequest, &str)> = vec![
        (SearchRequest::new(""), "collection must not be empty"),
        (
            SearchRequest {
                retrievers: vec![text(); 17],
                fusion: Some(Fusion::Dbsf),
                ..base()
            },
            "at most 16 retrievers",
        ),
        (
            SearchRequest {
                retrievers: vec![nested],
                ..base()
            },
            "fused retrievers nest at most 4 deep",
        ),
        (
            SearchRequest {
                retrievers: vec![fused(vec![])],
                ..base()
            },
            "a fused retriever needs at least one input",
        ),
        (
            SearchRequest {
                retrievers: vec![text(), vector()],
                ..base()
            },
            "fusion is required with several retrievers",
        ),
        (
            SearchRequest {
                retrievers: vec![text(), vector()],
                fusion: Some(Fusion::WeightedSum { weights: vec![1.0] }),
                ..base()
            },
            "weighted_sum needs one weight per list, got 1 for 2",
        ),
        (
            SearchRequest {
                retrievers: vec![text_retriever("body", "x", 0)],
                ..base()
            },
            "k must be between 1 and 100000",
        ),
        (
            SearchRequest {
                offset: 99_995,
                limit: 10,
                ..base()
            },
            "offset + limit must be at most 100000",
        ),
        (
            SearchRequest {
                retrievers: vec![Retriever::Vector {
                    field: "e".to_string(),
                    query: vec![1.0],
                    k: 10,
                    params: AnnParams {
                        distance: Some(Distance::Dot),
                        ..AnnParams::default()
                    },
                    filter: None,
                }],
                ..base()
            },
            "a distance override needs exact search",
        ),
        (
            SearchRequest {
                sort: vec![SortKey::Score {
                    order: SortOrder::Desc,
                }],
                ..base()
            },
            "a score sort needs a retriever",
        ),
        (
            SearchRequest {
                retrievers: vec![vector()],
                sort: vec![SortKey::Field {
                    field: "n".to_string(),
                    order: SortOrder::Asc,
                    missing: MissingOrder::Last,
                }],
                ..base()
            },
            "a field sort allows at most one retriever, and it must be text",
        ),
        (
            SearchRequest {
                sort: vec![SortKey::Field {
                    field: "n".to_string(),
                    order: SortOrder::Asc,
                    missing: MissingOrder::Last,
                }],
                search_after: Some(vec![
                    SortValue::I64(1),
                    SortValue::I64(2),
                    SortValue::I64(3),
                ]),
                ..base()
            },
            "search_after has 3 values but the sort has 2 keys",
        ),
        (
            SearchRequest {
                group_by: Some(GroupBy {
                    field: "tag".to_string(),
                    group_size: 0,
                    limit: 10,
                }),
                ..base()
            },
            "group_size and limit of group_by must be at least 1",
        ),
        (
            SearchRequest {
                retrievers: vec![vector_retriever("e", vec![1.0, f32::NAN], 10)],
                ..base()
            },
            "non-finite number in retrievers[0].vector.query[1]",
        ),
        (
            SearchRequest {
                filter: Some(big_bool),
                ..base()
            },
            "a query has more than 1024 clauses",
        ),
    ];
    for (request, message) in cases {
        assert_eq!(
            validate_request(&request, &limits),
            Err(ServiceError::InvalidArgument(message.to_string())),
            "{request:?}"
        );
    }

    // Rule 14: the message carries serde's error after the prefix.
    let request = SearchRequest {
        aggregations: Some(json!({"a": {"nope": {}}})),
        ..base()
    };
    match validate_request(&request, &limits) {
        Err(ServiceError::InvalidArgument(message)) => assert!(
            message.starts_with("aggregations are not a valid aggregation request: "),
            "{message}"
        ),
        other => panic!("{other:?}"),
    }
    let valid_aggs = SearchRequest {
        aggregations: Some(json!({"tags": {"terms": {"field": "tag"}}})),
        ..base()
    };
    assert_eq!(validate_request(&valid_aggs, &limits), Ok(()));
    // Rule 15 names other paths too.
    let request = SearchRequest {
        score_threshold: Some(f32::INFINITY),
        ..base()
    };
    assert_eq!(
        validate_request(&request, &limits),
        Err(ServiceError::InvalidArgument(
            "non-finite number in score_threshold".to_string()
        ))
    );
    let request = SearchRequest {
        filter: Some(Query::Term {
            field: "x".to_string(),
            value: FieldValue::F64(f64::NAN),
        }),
        ..base()
    };
    assert_eq!(
        validate_request(&request, &limits),
        Err(ServiceError::InvalidArgument(
            "non-finite number in filter.term.value".to_string()
        ))
    );
    // A valid hybrid request passes.
    let ok = SearchRequest {
        retrievers: vec![text(), vector()],
        fusion: Some(Fusion::Rrf { k: 60 }),
        ..base()
    };
    assert_eq!(validate_request(&ok, &limits), Ok(()));
}

#[test]
fn op_result_existed_follows_the_table() {
    let table = [
        (OpResult::Created, Some(false)),
        (OpResult::NotFound, Some(false)),
        (OpResult::Updated, Some(true)),
        (OpResult::Deleted, Some(true)),
        (OpResult::Noop, Some(true)),
        (OpResult::Accepted, None),
        (OpResult::Rejected(ServiceError::Timeout), None),
    ];
    for (result, existed) in table {
        assert_eq!(result.existed(), existed, "{result:?}");
    }
}

#[test]
fn sort_and_field_values_serialize_normalized() {
    assert_eq!(to(&SortValue::U64(7)), json!(7));
    assert_eq!(from::<SortValue>(json!(7)), SortValue::I64(7));
    assert_eq!(SortValue::U64(7).normalized(), SortValue::I64(7));
    assert_eq!(
        from::<SortValue>(to(&SortValue::U64(u64::MAX))),
        SortValue::U64(u64::MAX)
    );
    assert_eq!(FieldValue::U64(7).normalized(), FieldValue::I64(7));
    assert_eq!(from::<FieldValue>(json!(7)), FieldValue::I64(7));
    assert_eq!(
        from::<FieldValue>(to(&FieldValue::U64(u64::MAX))),
        FieldValue::U64(u64::MAX)
    );
    let date = FieldValue::Date(1_704_153_600_000_000);
    assert_eq!(to(&date), json!({"date": "2024-01-02T00:00:00.000000Z"}));
    assert_eq!(
        from::<FieldValue>(json!({"date": "2024-01-02T00:00:00.000000Z"})),
        date
    );
    assert_eq!(
        from::<FieldValue>(json!({"date": "2024-01-02T01:00:00+01:00"})),
        date
    );
    assert_eq!(from::<FieldValue>(json!(1.5)), FieldValue::F64(1.5));
    assert_eq!(
        from::<FieldValue>(json!("2024-01-02")),
        FieldValue::Str("2024-01-02".to_string())
    );
    assert_eq!(
        to(&SortValue::Uuid(UUID_BYTES)),
        json!({"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"})
    );
    assert_eq!(from::<SortValue>(json!(null)), SortValue::Null);
    // The PK tie-break value is the key's own JSON (Ruling 9).
    for pk in [
        PrimaryKey::U64(3),
        PrimaryKey::U64(u64::MAX),
        PrimaryKey::Str("a".to_string()),
        PrimaryKey::Uuid(UUID_BYTES),
    ] {
        let value = SortValue::from_pk(&pk);
        assert_eq!(value.as_pk(), Some(pk.clone()));
        assert_eq!(to(&value), json::pk::to_json(&pk));
    }
    assert_eq!(SortValue::I64(-1).as_pk(), None);
    assert_eq!(SortValue::F64(1.0).as_pk(), None);
    assert_eq!(from::<Fuzziness>(json!("auto")), Fuzziness::Auto);
    assert_eq!(from::<Fuzziness>(json!(2)), Fuzziness::Edits(2));
    assert!(serde_json::from_value::<Fuzziness>(json!(3)).is_err());
    assert_eq!(to(&Fuzziness::Edits(1)), json!(1));
    assert_eq!(from::<SourceFilter>(json!("none")), SourceFilter::None);
    assert_eq!(
        from::<SourceFilter>(json!({"include": ["a"]})),
        SourceFilter::Paths {
            include: vec!["a".to_string()],
            exclude: vec![]
        }
    );
}

#[test]
fn hybrid_body_maps_to_the_ir() {
    const NOW_MS: u64 = 1_750_000_000_000;
    let body = fixture("hybrid_body.json");
    let request = json::hybrid::parse_query_body(body.clone(), NOW_MS).expect("hybrid body");
    let expected = SearchRequest {
        consistency: ReadConsistency::AtLeast(token("v1:s7/p3@918273")),
        retrievers: vec![
            vector_retriever("embedding", vec![0.12, 0.34, 0.56], 100),
            text_retriever("body", "refund policy for enterprise", 100),
        ],
        fusion: Some(Fusion::Rrf { k: 60 }),
        filter: Some(Query::Bool {
            must: vec![
                Query::Term {
                    field: "tenant".to_string(),
                    value: FieldValue::Str("t42".to_string()),
                },
                Query::Range {
                    field: "ts".to_string(),
                    gt: None,
                    gte: Some(FieldValue::Date(
                        (1_750_000_000_000 - 30 * 86_400_000) * 1000,
                    )),
                    lt: None,
                    lte: None,
                },
            ],
            should: vec![],
            must_not: vec![],
            filter: vec![],
            minimum_should_match: None,
        }),
        select: Projection {
            source: SourceFilter::Paths {
                include: vec![
                    "body".to_string(),
                    "entity_id".to_string(),
                    "_neighbors".to_string(),
                ],
                exclude: vec![],
            },
            vectors: vec![],
            fields: vec![],
        },
        limit: 10,
        ..SearchRequest::new("memories")
    };
    assert_eq!(request, expected);

    let mut with_expand = obj(body.clone());
    with_expand.insert("expand".to_string(), json!({"graph": "kg", "hops": 2}));
    assert_eq!(
        json::hybrid::parse_query_body(Value::Object(with_expand), NOW_MS),
        Err(ServiceError::InvalidArgument(
            "expand needs graphs, which arrive in M3".to_string()
        ))
    );
    let mut with_rerank = obj(body.clone());
    with_rerank.insert("rerank".to_string(), json!({"top_n": 20}));
    assert_eq!(
        json::hybrid::parse_query_body(Value::Object(with_rerank), NOW_MS),
        Err(ServiceError::InvalidArgument(
            "rerank arrives in M3".to_string()
        ))
    );
    let mut unknown = obj(body.clone());
    unknown.insert("limitt".to_string(), json!(3));
    assert_eq!(
        json::hybrid::parse_query_body(Value::Object(unknown), NOW_MS),
        Err(ServiceError::InvalidArgument(
            "unknown key limitt in the query body".to_string()
        ))
    );
    let mut c1 = obj(body);
    c1.insert(
        "consistency".to_string(),
        json!({"token": "c1:s7/p3@918273"}),
    );
    assert!(matches!(
        json::hybrid::parse_query_body(Value::Object(c1), NOW_MS),
        Err(ServiceError::InvalidArgument(_))
    ));
    // Date math the JSON date form cannot hold (years 0000–9999) is refused,
    // so every accepted request round-trips through JSON.
    for math in ["now-1000000d", "now+4000000d"] {
        let far = json!({"from": "collections.memories", "limit": 1,
                         "filter": {"range": {"ts": {"gte": math}}}});
        assert_eq!(
            json::hybrid::parse_query_body(far, NOW_MS),
            Err(ServiceError::InvalidArgument(format!(
                "date math {math:?} out of range"
            ))),
            "{math}"
        );
    }
    // Without `from` or `retrieve`, the body is a SearchRequest.
    let plain = json::hybrid::parse_query_body(json!({"collection": "kb", "limit": 2}), NOW_MS)
        .expect("plain request");
    assert_eq!(
        plain,
        SearchRequest {
            limit: 2,
            ..SearchRequest::new("kb")
        }
    );
}

// ----- ir_json_round_trips -----

fn name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,6}"
}

fn finite_f32() -> impl Strategy<Value = f32> {
    prop_oneof![Just(0.0f32), Just(-1.5f32), -1.0e6f32..1.0e6f32]
}

fn finite_f64() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.5f64), -1.0e12f64..1.0e12f64]
}

/// Years 0001..=9999, the RFC 3339 range.
fn date_micros() -> impl Strategy<Value = i64> {
    -62_135_596_800_000_000i64..=253_402_300_799_999_999i64
}

fn field_value() -> impl Strategy<Value = FieldValue> {
    prop_oneof![
        name().prop_map(FieldValue::Str),
        any::<i64>().prop_map(FieldValue::I64),
        any::<u64>().prop_map(FieldValue::U64),
        Just(FieldValue::U64(u64::MAX)),
        finite_f64().prop_map(FieldValue::F64),
        any::<bool>().prop_map(FieldValue::Bool),
        date_micros().prop_map(FieldValue::Date),
    ]
}

fn sort_value() -> impl Strategy<Value = SortValue> {
    prop_oneof![
        Just(SortValue::Null),
        any::<bool>().prop_map(SortValue::Bool),
        any::<i64>().prop_map(SortValue::I64),
        any::<u64>().prop_map(SortValue::U64),
        Just(SortValue::U64(u64::MAX)),
        finite_f64().prop_map(SortValue::F64),
        name().prop_map(SortValue::Str),
        any::<[u8; 16]>().prop_map(SortValue::Uuid),
    ]
}

fn primary_key() -> impl Strategy<Value = PrimaryKey> {
    prop_oneof![
        any::<u64>().prop_map(PrimaryKey::U64),
        Just(PrimaryKey::U64(u64::MAX)),
        any::<[u8; 16]>().prop_map(PrimaryKey::Uuid),
        "[a-zA-Z0-9 -]{1,12}".prop_map(PrimaryKey::Str),
    ]
}

fn fuzziness() -> impl Strategy<Value = Fuzziness> {
    prop_oneof![Just(Fuzziness::Auto), (0u8..=2).prop_map(Fuzziness::Edits)]
}

fn bool_operator() -> impl Strategy<Value = BoolOperator> {
    prop_oneof![Just(BoolOperator::Or), Just(BoolOperator::And)]
}

fn multi_match_kind() -> impl Strategy<Value = MultiMatchKind> {
    prop_oneof![
        Just(MultiMatchKind::BestFields),
        Just(MultiMatchKind::MostFields),
        Just(MultiMatchKind::CrossFields),
        Just(MultiMatchKind::Phrase),
        Just(MultiMatchKind::PhrasePrefix),
    ]
}

fn opt<T: std::fmt::Debug + Clone + 'static>(
    inner: impl Strategy<Value = T> + 'static,
) -> impl Strategy<Value = Option<T>> {
    prop::option::of(inner)
}

fn leaf_query() -> impl Strategy<Value = Query> {
    prop_oneof![
        Just(Query::MatchAll),
        Just(Query::MatchNone),
        (
            name(),
            name(),
            bool_operator(),
            opt(name()),
            opt(fuzziness()),
            opt(name())
        )
            .prop_map(|(field, text, operator, msm, fuzziness, analyzer)| {
                Query::Match {
                    field,
                    text,
                    operator,
                    minimum_should_match: msm,
                    fuzziness,
                    analyzer,
                }
            }),
        (name(), name(), 0u32..5).prop_map(|(field, text, slop)| Query::MatchPhrase {
            field,
            text,
            slop
        }),
        (
            prop::collection::vec((name(), finite_f32()), 1..3),
            name(),
            multi_match_kind(),
            bool_operator(),
            opt(finite_f32())
        )
            .prop_map(|(fields, text, kind, operator, tie_breaker)| {
                Query::MultiMatch {
                    fields,
                    text,
                    kind,
                    operator,
                    tie_breaker,
                }
            }),
        (name(), field_value()).prop_map(|(field, value)| Query::Term { field, value }),
        (name(), prop::collection::vec(field_value(), 0..3))
            .prop_map(|(field, values)| Query::Terms { field, values }),
        (
            name(),
            opt(field_value()),
            opt(field_value()),
            opt(field_value()),
            opt(field_value())
        )
            .prop_map(|(field, gt, gte, lt, lte)| Query::Range {
                field,
                gt,
                gte,
                lt,
                lte
            }),
        name().prop_map(|field| Query::Exists { field }),
        name().prop_map(|field| Query::IsNull { field }),
        name().prop_map(|field| Query::IsEmpty { field }),
        (
            name(),
            opt(any::<u64>()),
            opt(any::<u64>()),
            opt(any::<u64>()),
            opt(any::<u64>())
        )
            .prop_map(|(field, gt, gte, lt, lte)| Query::ValuesCount {
                field,
                gt,
                gte,
                lt,
                lte
            }),
        (name(), name()).prop_map(|(field, value)| Query::Prefix { field, value }),
        (name(), name()).prop_map(|(field, pattern)| Query::Wildcard { field, pattern }),
        (name(), name(), fuzziness()).prop_map(|(field, value, fuzziness)| Query::Fuzzy {
            field,
            value,
            fuzziness
        }),
        prop::collection::vec(primary_key(), 0..4).prop_map(Query::Ids),
        (name(), prop::collection::vec(name(), 0..3), bool_operator()).prop_map(
            |(query, default_fields, default_operator)| Query::QueryString {
                query,
                default_fields,
                default_operator,
            }
        ),
    ]
}

fn query() -> impl Strategy<Value = Query> {
    leaf_query().prop_recursive(3, 24, 4, |inner| {
        let list = || prop::collection::vec(inner.clone(), 0..3);
        prop_oneof![
            (list(), list(), list(), list(), opt(name())).prop_map(
                |(must, should, must_not, filter, minimum_should_match)| Query::Bool {
                    must,
                    should,
                    must_not,
                    filter,
                    minimum_should_match,
                }
            ),
            (inner.clone(), finite_f32()).prop_map(|(query, boost)| Query::Boost {
                query: Box::new(query),
                boost
            }),
            (inner.clone(), finite_f32()).prop_map(|(query, score)| Query::ConstantScore {
                query: Box::new(query),
                score
            }),
        ]
    })
}

fn distance() -> impl Strategy<Value = Distance> {
    prop_oneof![
        Just(Distance::Cosine),
        Just(Distance::Dot),
        Just(Distance::Euclid),
        Just(Distance::Manhattan),
    ]
}

fn ann_params() -> impl Strategy<Value = AnnParams> {
    (
        any::<bool>(),
        opt(any::<u32>()),
        opt(any::<u32>()),
        opt(any::<u32>()),
        opt(finite_f32()),
        opt(distance()),
    )
        .prop_map(
            |(exact, nprobes, refine_factor, ef, oversampling, distance)| AnnParams {
                exact,
                nprobes,
                refine_factor,
                ef,
                oversampling,
                distance,
            },
        )
}

fn fusion() -> impl Strategy<Value = Fusion> {
    prop_oneof![
        any::<u32>().prop_map(|k| Fusion::Rrf { k }),
        Just(Fusion::Dbsf),
        prop::collection::vec(finite_f32(), 0..3)
            .prop_map(|weights| Fusion::WeightedSum { weights }),
    ]
}

fn sparse_vector() -> impl Strategy<Value = SparseVector> {
    prop::collection::btree_map(any::<u32>(), finite_f32(), 0..4).prop_map(|entries| {
        let (indices, values) = entries.into_iter().unzip();
        SparseVector::new(indices, values).expect("sparse")
    })
}

fn leaf_retriever() -> impl Strategy<Value = Retriever> {
    prop_oneof![
        (
            name(),
            prop::collection::vec(finite_f32(), 0..4),
            1usize..100,
            ann_params(),
            opt(query())
        )
            .prop_map(|(field, query, k, params, filter)| Retriever::Vector {
                field,
                query,
                k,
                params,
                filter
            }),
        (query(), 1usize..100).prop_map(|(query, k)| Retriever::Text { query, k }),
        (
            name(),
            sparse_vector(),
            1usize..100,
            opt(query()),
            opt(query())
        )
            .prop_map(|(field, query, k, filter, idf_corpus)| Retriever::Sparse {
                field,
                query,
                k,
                filter,
                params: SparseParams { idf_corpus },
            }),
    ]
}

fn retriever() -> impl Strategy<Value = Retriever> {
    leaf_retriever().prop_recursive(3, 12, 3, |inner| {
        prop_oneof![
            (
                prop::collection::vec(inner.clone(), 0..3),
                fusion(),
                1usize..100
            )
                .prop_map(|(inputs, fusion, k)| Retriever::Fused { inputs, fusion, k }),
            (
                inner,
                name(),
                prop::collection::vec(finite_f32(), 0..3),
                1usize..100
            )
                .prop_map(|(input, field, query, k)| Retriever::Rescore {
                    input: Box::new(input),
                    field,
                    query,
                    k
                }),
        ]
    })
}

fn sort_order() -> impl Strategy<Value = SortOrder> {
    prop_oneof![Just(SortOrder::Asc), Just(SortOrder::Desc)]
}

fn sort_key() -> impl Strategy<Value = SortKey> {
    prop_oneof![
        sort_order().prop_map(|order| SortKey::Score { order }),
        sort_order().prop_map(|order| SortKey::Pk { order }),
        (
            name(),
            sort_order(),
            prop_oneof![Just(MissingOrder::First), Just(MissingOrder::Last)]
        )
            .prop_map(|(field, order, missing)| SortKey::Field {
                field,
                order,
                missing
            }),
    ]
}

fn source_filter() -> impl Strategy<Value = SourceFilter> {
    prop_oneof![
        Just(SourceFilter::All),
        Just(SourceFilter::None),
        (
            prop::collection::vec(name(), 0..3),
            prop::collection::vec(name(), 0..3)
        )
            .prop_map(|(include, exclude)| SourceFilter::Paths { include, exclude }),
    ]
}

fn projection() -> impl Strategy<Value = Projection> {
    (
        source_filter(),
        prop::collection::vec(name(), 0..3),
        prop::collection::vec(name(), 0..3),
    )
        .prop_map(|(source, vectors, fields)| Projection {
            source,
            vectors,
            fields,
        })
}

fn consistency_token() -> impl Strategy<Value = ConsistencyToken> {
    prop::collection::vec((any::<u64>(), any::<u32>(), any::<u64>()), 0..3).prop_map(|items| {
        ConsistencyToken(
            items
                .into_iter()
                .map(|(s, p, o)| (StreamId(s), p, o))
                .collect(),
        )
    })
}

fn read_consistency() -> impl Strategy<Value = ReadConsistency> {
    prop_oneof![
        Just(ReadConsistency::Strong),
        Just(ReadConsistency::Eventual),
        consistency_token().prop_map(ReadConsistency::AtLeast),
        (any::<u64>(), consistency_token()).prop_map(|(manifest_version, token)| {
            ReadConsistency::Pinned {
                manifest_version,
                token,
            }
        }),
    ]
}

fn highlight() -> impl Strategy<Value = Highlight> {
    prop::collection::vec((name(), name(), name(), 1usize..500, 0usize..10), 0..3).prop_map(
        |fields| Highlight {
            fields: fields
                .into_iter()
                .map(
                    |(field, pre_tag, post_tag, fragment_size, number_of_fragments)| {
                        HighlightField {
                            field,
                            pre_tag,
                            post_tag,
                            fragment_size,
                            number_of_fragments,
                        }
                    },
                )
                .collect(),
        },
    )
}

fn track_total_hits() -> impl Strategy<Value = TrackTotalHits> {
    prop_oneof![
        Just(TrackTotalHits::None),
        Just(TrackTotalHits::Exact),
        any::<u64>().prop_map(TrackTotalHits::UpTo),
    ]
}

prop_compose! {
    fn search_request()(
        collection in name(),
        consistency in read_consistency(),
        retrievers in prop::collection::vec(retriever(), 0..3),
        fusion in opt(fusion()),
        filter in opt(query()),
        sort in prop::collection::vec(sort_key(), 0..3),
        offset in 0usize..1000,
        limit in 0usize..1000,
        search_after in opt(prop::collection::vec(sort_value(), 0..3)),
        score_threshold in opt(finite_f32()),
        select in projection(),
        aggregations in opt(Just(json!({"tags": {"terms": {"field": "tag", "size": 3}}}))),
        highlight in opt(highlight()),
        group_by in opt((name(), 1usize..10, 1usize..10)),
        track_total_hits in track_total_hits(),
    ) -> SearchRequest {
        SearchRequest {
            collection,
            consistency,
            retrievers,
            fusion,
            filter,
            sort,
            offset,
            limit,
            search_after,
            score_threshold,
            select,
            aggregations,
            highlight,
            group_by: group_by.map(|(field, group_size, limit)| GroupBy { field, group_size, limit }),
            track_total_hits,
        }
    }
}

fn source() -> impl Strategy<Value = Option<Map<String, Value>>> {
    opt((name(), any::<i64>())
        .prop_map(|(key, n)| obj(json!({ key: n, "nested": {"x": [1, "a"]} }))))
}

fn vectors() -> impl Strategy<Value = BTreeMap<String, Vec<f32>>> {
    prop::collection::btree_map(name(), prop::collection::vec(finite_f32(), 0..3), 0..2)
}

fn sparse_vectors() -> impl Strategy<Value = BTreeMap<String, SparseVector>> {
    prop::collection::btree_map(name(), sparse_vector(), 0..2)
}

fn field_values() -> impl Strategy<Value = BTreeMap<String, Vec<FieldValue>>> {
    prop::collection::btree_map(
        name(),
        prop::collection::vec(field_value().prop_map(FieldValue::normalized), 0..3),
        0..2,
    )
}

fn hit() -> impl Strategy<Value = Hit> {
    (
        primary_key(),
        finite_f32(),
        prop::collection::vec(sort_value().prop_map(SortValue::normalized), 0..3),
        source(),
        vectors(),
        sparse_vectors(),
        prop::collection::btree_map(name(), prop::collection::vec(name(), 0..2), 0..2),
        field_values(),
    )
        .prop_map(
            |(pk, score, sort_values, source, vectors, sparse_vectors, highlight, fields)| Hit {
                pk,
                score,
                sort_values,
                source,
                vectors,
                sparse_vectors,
                highlight,
                fields,
            },
        )
}

fn service_error() -> impl Strategy<Value = ServiceError> {
    let kinds = operon_query::NOT_FOUND_KINDS;
    prop_oneof![
        (0..kinds.len(), ".{0,8}").prop_map(move |(i, name)| ServiceError::NotFound {
            kind: kinds[i],
            name
        }),
        ".{0,8}".prop_map(ServiceError::AlreadyExists),
        ".{0,8}".prop_map(ServiceError::InvalidArgument),
        (".{0,8}", ".{0,8}")
            .prop_map(|(field, message)| ServiceError::SchemaViolation { field, message }),
        ".{0,8}".prop_map(ServiceError::Unavailable),
        Just(ServiceError::Timeout),
        ".{0,8}".prop_map(ServiceError::Internal),
    ]
}

fn search_response() -> impl Strategy<Value = SearchResponse> {
    (
        prop::collection::vec(hit(), 0..3),
        opt((
            any::<u64>(),
            prop_oneof![Just(TotalRelation::Eq), Just(TotalRelation::Gte)],
        )),
        opt(Just(
            json!({"tags": {"buckets": [{"key": "a", "doc_count": 2}]}}),
        )),
        opt(prop::collection::vec(
            (
                field_value().prop_map(FieldValue::normalized),
                prop::collection::vec(hit(), 0..2),
            ),
            0..2,
        )),
        consistency_token(),
        prop::collection::btree_set(
            prop_oneof![Just(HotKind::Hnsw), Just(HotKind::Splits)],
            0..3,
        ),
    )
        .prop_map(
            |(hits, total, aggregations, groups, read_token, hot_used)| SearchResponse {
                hits,
                total: total.map(|(value, relation)| TotalHits { value, relation }),
                aggregations,
                groups: groups.map(|groups| {
                    groups
                        .into_iter()
                        .map(|(key, hits)| HitGroup { key, hits })
                        .collect()
                }),
                read_token,
                hot_used,
            },
        )
}

fn stored_doc() -> impl Strategy<Value = StoredDoc> {
    (
        primary_key(),
        source(),
        vectors(),
        sparse_vectors(),
        field_values(),
        any::<u64>(),
        any::<u32>(),
    )
        .prop_map(
            |(pk, source, vectors, sparse_vectors, fields, seq_no, partition)| StoredDoc {
                pk,
                source,
                vectors,
                sparse_vectors,
                fields,
                seq_no,
                partition,
            },
        )
}

fn hot_state() -> impl Strategy<Value = HotState> {
    (
        prop_oneof![
            Just(HotStateKind::Off),
            Just(HotStateKind::Building),
            Just(HotStateKind::Ready)
        ],
        opt(any::<u64>()),
    )
        .prop_map(|(state, source_version)| HotState {
            state,
            source_version,
        })
}

fn collection_info() -> impl Strategy<Value = CollectionInfo> {
    (
        (any::<u64>(), name(), name(), 1u32..16, prop::collection::vec(name(), 0..3)),
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()),
        (any::<u64>(), any::<u64>()),
        (hot_state(), hot_state(), hot_state()),
    )
        .prop_map(
            |(
                (id, name, namespace, partitions, mut aliases),
                (stream, manifest_version, live_doc_count, size_bytes),
                (created_at_ms, link_lag_records),
                (vectors, text, fragments),
            )| {
                aliases.sort();
                let schema = json::schema::from_json(&json!({
                    "fields": [{"name": "body", "kind": {"text": {}}}, {"name": "n", "kind": "i64", "fast": true}],
                    "vectors": [{"name": "e", "dim": 4, "distance": "dot",
                                 "index": {"ivf_pq": {"num_partitions": 8}},
                                 "quantization": {"scalar": {"quantile_ppm": 990000}}}],
                    "sparse_vectors": [{"name": "s", "modifier": "idf"}],
                    "dynamic": "map",
                    "annotations": {"operon.created_at_ms": "5"}
                }))
                .expect("schema");
                CollectionInfo {
                    id: CollectionId(id),
                    name,
                    namespace,
                    schema,
                    partitions,
                    aliases,
                    stream: StreamId(stream),
                    manifest_version,
                    live_doc_count,
                    size_bytes,
                    created_at_ms,
                    link_lag_records,
                    hot: HotStatus {
                        vectors,
                        text,
                        fragments,
                    },
                }
            },
        )
}

fn write_result() -> impl Strategy<Value = WriteResult> {
    let op_result = prop_oneof![
        Just(OpResult::Created),
        Just(OpResult::Updated),
        Just(OpResult::Deleted),
        Just(OpResult::NotFound),
        Just(OpResult::Noop),
        Just(OpResult::Accepted),
        service_error().prop_map(OpResult::Rejected),
    ];
    (
        consistency_token(),
        prop::collection::vec(op_result, 0..4),
        prop::collection::vec(
            opt((any::<u32>(), any::<u64>())
                .prop_map(|(partition, seq_no)| OpPosition { partition, seq_no })),
            0..4,
        ),
    )
        .prop_map(|(token, results, positions)| WriteResult {
            token,
            results,
            positions,
        })
}

fn round_trips<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let json = serde_json::to_value(value).expect("serialize");
    let text = serde_json::to_string(&json).expect("text");
    serde_json::from_str(&text).expect("deserialize")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn ir_json_round_trips(
        request in search_request(),
        response in search_response(),
        doc in stored_doc(),
        info in collection_info(),
        written in write_result(),
        err in service_error(),
    ) {
        prop_assert_eq!(round_trips(&request), request.clone().normalized());
        prop_assert_eq!(round_trips(&response), response);
        prop_assert_eq!(round_trips(&doc), doc);
        prop_assert_eq!(round_trips(&info), info);
        prop_assert_eq!(round_trips(&written), written);
        prop_assert_eq!(round_trips(&err), err);
    }
}

#[test]
fn search_request_defaults_fill_missing_keys() {
    let request: SearchRequest = from(json!({"collection": "kb"}));
    assert_eq!(request, SearchRequest::new("kb"));
    assert_eq!(request.limit, 10);
    assert_eq!(request.consistency, ReadConsistency::Strong);
    assert_eq!(request.track_total_hits, TrackTotalHits::None);
    let fusion: Fusion = from(json!({"rrf": {}}));
    assert_eq!(fusion, Fusion::Rrf { k: 60 });
    let field: HighlightField = from(json!({"field": "body"}));
    assert_eq!(
        (
            field.pre_tag.as_str(),
            field.post_tag.as_str(),
            field.fragment_size,
            field.number_of_fragments
        ),
        ("<em>", "</em>", 100, 5)
    );
    let group: GroupBy = from(json!({"field": "tag"}));
    assert_eq!((group.group_size, group.limit), (3, 10));
    let key: SortKey = from(json!({"field": {"field": "n"}}));
    assert_eq!(
        key,
        SortKey::Field {
            field: "n".to_string(),
            order: SortOrder::Asc,
            missing: MissingOrder::Last
        }
    );
    assert_eq!(
        from::<SortKey>(json!({"score": {}})),
        SortKey::Score {
            order: SortOrder::Desc
        }
    );
    assert_eq!(to(&TrackTotalHits::UpTo(5)), json!({"up_to": 5}));
    assert_eq!(
        to(&TotalHits {
            value: 3,
            relation: TotalRelation::Gte
        }),
        json!({"value": 3, "relation": "gte"})
    );
    let response = SearchResponse {
        hits: vec![],
        total: None,
        aggregations: None,
        groups: None,
        read_token: ConsistencyToken::default(),
        hot_used: BTreeSet::new(),
    };
    assert!(!obj(to(&response)).contains_key("hot_used"));
    assert_eq!(to(&response)["read_token"], json!("v1:"));
    let query: Query =
        from(json!({"multi_match": {"fields": [["title", 2.0], ["body", 1.0]], "text": "x"}}));
    assert_eq!(
        query,
        Query::MultiMatch {
            fields: vec![("title".to_string(), 2.0), ("body".to_string(), 1.0)],
            text: "x".to_string(),
            kind: MultiMatchKind::BestFields,
            operator: BoolOperator::Or,
            tie_breaker: None
        }
    );
}

// ----- The hot-tier and placement hooks -----

mod hooks {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use operon_collection::PrimaryKey;
    use operon_common::{CollectionId, NamespaceId};
    use operon_query::hot::{
        self, HOT_HEADER, HOT_USED_HEADER, HotKind, HotLayer, HotStatus, HotTier, HotUsed,
        NoHotTier, RequestHot,
    };
    use operon_query::placement::{
        FORWARDED_HEADER, LocalOnly, NoRemoteReads, Owner, Placement, RemoteReads,
    };
    use operon_query::{Projection, ReadConsistency, SearchRequest, ServiceError};
    use serde_json::{Value, json};
    use tower::{Layer, ServiceExt, service_fn};

    #[tokio::test]
    async fn the_hot_scope_is_task_local() {
        assert!(hot::current().is_none());
        let used = HotUsed::default();
        let request = RequestHot {
            enabled: true,
            used: used.clone(),
        };
        let seen = hot::scope(request, async {
            let current = hot::current().expect("inside the scope");
            current.used.record(HotKind::Splits);
            current.used.record(HotKind::Hnsw);
            current.enabled
        })
        .await;
        assert!(seen);
        assert!(hot::current().is_none());
        assert_eq!(
            used.kinds().into_iter().collect::<Vec<_>>(),
            vec![HotKind::Hnsw, HotKind::Splits]
        );
        assert_eq!(used.header_value(), "hnsw,splits");
        assert_eq!(HotUsed::default().header_value(), "none");
    }

    #[tokio::test]
    async fn the_hot_layer_parses_the_header_and_reports_used() {
        let calls = Arc::new(AtomicUsize::new(0));
        let saw = Arc::new(Mutex::new(None::<bool>));
        let inner = {
            let (calls, saw) = (calls.clone(), saw.clone());
            service_fn(move |request: http::Request<String>| {
                let (calls, saw) = (calls.clone(), saw.clone());
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let current = hot::current().expect("the layer sets the scope");
                    *saw.lock().expect("lock") = Some(current.enabled);
                    if current.enabled {
                        current.used.record(HotKind::Hnsw);
                    }
                    let mut response = http::Response::new(String::new());
                    if request.headers().contains_key("x-forwarded-used") {
                        response
                            .headers_mut()
                            .insert(HOT_USED_HEADER, http::HeaderValue::from_static("splits"));
                    }
                    Ok::<_, std::convert::Infallible>(response)
                }
            })
        };
        let service = HotLayer::new(true).layer(inner);
        let request = |header: Option<&str>| {
            let mut builder = http::Request::builder().uri("/");
            if let Some(value) = header {
                builder = builder.header("Operon-Hot", value);
            }
            builder.body(String::new()).expect("request")
        };

        let response = service.clone().oneshot(request(None)).await.expect("call");
        assert_eq!(response.headers()[HOT_USED_HEADER], "hnsw");
        assert_eq!(*saw.lock().expect("lock"), Some(true));

        let response = service
            .clone()
            .oneshot(request(Some("OFF")))
            .await
            .expect("call");
        assert_eq!(response.headers()[HOT_USED_HEADER], "none");
        assert_eq!(*saw.lock().expect("lock"), Some(false));

        let response = service
            .clone()
            .oneshot(request(Some("On")))
            .await
            .expect("call");
        assert_eq!(response.headers()[HOT_USED_HEADER], "hnsw");

        let before = calls.load(Ordering::SeqCst);
        let response = service
            .clone()
            .oneshot(request(Some("maybe")))
            .await
            .expect("call");
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            before,
            "the inner service ran"
        );
        let (data, _) = collect(response.into_body()).await;
        let body: Value = serde_json::from_slice(&data).expect("json body");
        assert_eq!(
            body,
            json!({"error": "invalid_argument",
                   "message": "invalid Operon-Hot header: maybe (expected on or off)"})
        );

        // A gRPC call gets a trailers-only INVALID_ARGUMENT.
        let mut grpc = request(Some("maybe"));
        grpc.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        let response = service.clone().oneshot(grpc).await.expect("call");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers()["grpc-status"], "3");
        assert_eq!(
            response.headers()["grpc-message"],
            "invalid Operon-Hot header: maybe (expected on or off)"
        );
        let (data, _) = collect(response.into_body()).await;
        assert!(data.is_empty());

        // A gateway that forwarded the request already set the header.
        let mut forwarded = request(None);
        forwarded
            .headers_mut()
            .insert("x-forwarded-used", http::HeaderValue::from_static("1"));
        let response = service.clone().oneshot(forwarded).await.expect("call");
        assert_eq!(response.headers()[HOT_USED_HEADER], "splits");

        assert_eq!(HOT_HEADER, "operon-hot");
        assert_eq!(hot::parse_hot_header("oN"), Ok(true));
        assert_eq!(hot::parse_hot_header("off"), Ok(false));
        assert!(matches!(
            hot::parse_hot_header("yes"),
            Err(ServiceError::InvalidArgument(_))
        ));
    }

    /// Every data frame of `body` and its trailers.
    async fn collect<B>(mut body: B) -> (Vec<u8>, Option<http::HeaderMap>)
    where
        B: http_body::Body<Data = bytes::Bytes> + Unpin,
        B::Error: std::fmt::Debug,
    {
        let mut data = Vec::new();
        let mut trailers = None;
        while let Some(frame) =
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
        {
            let frame = frame.expect("frame");
            match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => trailers = frame.into_trailers().ok(),
            }
        }
        (data, trailers)
    }

    /// A body that records a hot kind when it is polled, then ends with
    /// trailers, as a streaming gRPC response does.
    struct Streaming {
        step: u8,
    }

    impl http_body::Body for Streaming {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<bytes::Bytes>, Self::Error>>> {
            self.step += 1;
            std::task::Poll::Ready(match self.step {
                1 => {
                    let current = hot::current().expect("the body is polled in the scope");
                    current.used.record(HotKind::Splits);
                    Some(Ok(http_body::Frame::data(bytes::Bytes::from_static(b"x"))))
                }
                2 => Some(Ok(http_body::Frame::trailers(http::HeaderMap::new()))),
                _ => None,
            })
        }
    }

    #[tokio::test]
    async fn a_streaming_body_is_polled_in_the_hot_scope_and_reports_in_its_trailers() {
        let inner = service_fn(|_: http::Request<String>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(Streaming { step: 0 }))
        });
        let service = HotLayer::new(true).layer(inner);
        let request = http::Request::builder()
            .uri("/")
            .body(String::new())
            .expect("request");
        let response = service.oneshot(request).await.expect("call");
        // Nothing was used when the headers were written.
        assert_eq!(response.headers()[HOT_USED_HEADER], "none");
        let (data, trailers) = collect(response.into_body()).await;
        assert_eq!(data, b"x");
        assert_eq!(trailers.expect("trailers")[HOT_USED_HEADER], "splits");
    }

    #[tokio::test]
    async fn no_hot_tier_and_local_only_are_inert() {
        let (ns, cid) = (NamespaceId(1), CollectionId(2));
        let tier = NoHotTier;
        assert!(tier.ann(ns, cid, "_vector_0", 7).is_none());
        assert!(tier.split_file(ns, cid, ulid::Ulid::nil()).is_none());
        tier.record_access(ns, cid);
        assert_eq!(tier.status(ns, cid), HotStatus::default());
        assert_eq!(LocalOnly.owner(ns, cid), Owner::Local);
        assert_eq!(FORWARDED_HEADER, "operon-forwarded");

        let to = Owner::Remote {
            node_id: 2,
            addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        };
        let unavailable = |err: ServiceError| matches!(err, ServiceError::Unavailable(_));
        let reads = NoRemoteReads;
        assert!(unavailable(
            reads
                .search(&to, "ns", SearchRequest::new("c"))
                .await
                .expect_err("search")
        ));
        assert!(unavailable(
            reads
                .get(
                    &to,
                    "ns",
                    "c",
                    vec![PrimaryKey::U64(1)],
                    Projection::default(),
                    ReadConsistency::Strong
                )
                .await
                .expect_err("get")
        ));
        assert!(unavailable(
            reads
                .count(&to, "ns", "c", None, ReadConsistency::Strong)
                .await
                .expect_err("count")
        ));
        assert!(unavailable(
            reads
                .scroll(
                    &to,
                    "ns",
                    "c",
                    None,
                    None,
                    10,
                    Projection::default(),
                    ReadConsistency::Strong
                )
                .await
                .expect_err("scroll")
        ));
    }
}
