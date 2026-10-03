//! Point reads end to end (plan M1.4 Task 6): retrieve, `GET` of one
//! point, payload and vector selectors, scroll pages and offsets, count,
//! and the gRPC methods.

use loams_qdrant::convert::value::payload_to_map;
use loams_qdrant::proto::qdrant as pb;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;

fn error(body: &Value) -> &str {
    body["status"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no status.error: {body}"))
}

async fn create(qd: &Qd, name: &str, body: Value) {
    let (status, reply) = qd.put(&format!("/collections/{name}"), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
}

async fn create_single(qd: &Qd, name: &str) {
    create(qd, name, json!({"vectors": {"size": 2, "distance": "Dot"}})).await;
}

async fn upsert(qd: &Qd, name: &str, points: Value) {
    let (status, reply) = qd
        .put(
            &format!("/collections/{name}/points?wait=true"),
            Some(json!({ "points": points })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
}

/// `POST …/points` (retrieve) with `body`.
async fn retrieve(qd: &Qd, name: &str, body: Value) -> (StatusCode, Value) {
    qd.post(&format!("/collections/{name}/points"), Some(body))
        .await
}

async fn scroll(qd: &Qd, name: &str, body: Value) -> Value {
    let (status, reply) = qd
        .post(&format!("/collections/{name}/points/scroll"), Some(body))
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    reply["result"].clone()
}

fn ids(points: &Value) -> Vec<Value> {
    points
        .as_array()
        .expect("points")
        .iter()
        .map(|p| p["id"].clone())
        .collect()
}

/// Follows `next_page_offset` from the start with `body` (plus `limit`);
/// every id in order.
async fn scroll_all(qd: &Qd, name: &str, body: Value, limit: usize) -> Vec<Value> {
    let mut out = Vec::new();
    let mut offset = Value::Null;
    loop {
        let mut request = body.clone();
        request["limit"] = json!(limit);
        if !offset.is_null() {
            request["offset"] = offset.clone();
        }
        let page = scroll(qd, name, request).await;
        let page_ids = ids(&page["points"]);
        assert!(page_ids.len() <= limit);
        out.extend(page_ids);
        offset = page["next_page_offset"].clone();
        if offset.is_null() {
            assert!(
                page.as_object()
                    .expect("present")
                    .contains_key("next_page_offset"),
                "{page}"
            );
            return out;
        }
    }
}

// ----- retrieve -----

#[tokio::test]
async fn retrieve_returns_request_order_and_skips_missing() {
    let qd = Qd::start().await;
    create_single(&qd, "r").await;
    let uuid = "fa38d572-4f1e-4a76-9c47-7a3c7b5a1f00";
    upsert(
        &qd,
        "r",
        json!([
            {"id": 5, "vector": [1.0, 0.0], "payload": {"n": 5}},
            {"id": uuid, "vector": [0.0, 1.0], "payload": {"n": 6}},
        ]),
    )
    .await;
    let (status, reply) =
        retrieve(&qd, "r", json!({"ids": [5, uuid.to_uppercase(), 999, 5]})).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(
        reply["result"],
        json!([{"id": 5, "payload": {"n": 5}}, {"id": uuid, "payload": {"n": 6}}])
    );
}

#[tokio::test]
async fn get_point_404_uses_qdrant_text() {
    let qd = Qd::start().await;
    create_single(&qd, "g").await;
    upsert(
        &qd,
        "g",
        json!([{"id": 1, "vector": [1.0, 2.0], "payload": {"a": 1}}]),
    )
    .await;
    let (status, reply) = qd.get("/collections/g/points/1", None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(
        reply["result"],
        json!({"id": 1, "payload": {"a": 1}, "vector": [1.0, 2.0]})
    );
    let (status, reply) = qd.get("/collections/g/points/999", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        error(&reply),
        "Not found: Point with id 999 does not exists!"
    );
    let uuid = "fa38d572-4f1e-4a76-9c47-7a3c7b5a1f00";
    let (status, reply) = qd.get(&format!("/collections/g/points/{uuid}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        error(&reply),
        format!("Not found: Point with id {uuid} does not exists!")
    );
    let (status, reply) = qd.get("/collections/g/points/abc", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Can not recognize \"abc\" as point id"
    );
}

#[tokio::test]
async fn with_payload_selectors_match_qdrant() {
    let qd = Qd::start().await;
    create_single(&qd, "p").await;
    let payload = json!({"a": 1, "b": 2, "c": {"d": 3, "e": 4}});
    upsert(
        &qd,
        "p",
        json!([{"id": 1, "vector": [1.0, 0.0], "payload": payload}]),
    )
    .await;
    let cases = [
        (json!(true), Some(payload.clone())),
        (json!(false), None),
        (json!(["a"]), Some(json!({"a": 1}))),
        (json!({"include": ["c.d"]}), Some(json!({"c": {"d": 3}}))),
        (
            json!({"exclude": ["a"]}),
            Some(json!({"b": 2, "c": {"d": 3, "e": 4}})),
        ),
    ];
    for (selector, want) in cases {
        let (status, reply) =
            retrieve(&qd, "p", json!({"ids": [1], "with_payload": selector})).await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        let point = &reply["result"][0];
        match want {
            Some(want) => assert_eq!(point["payload"], want, "{selector}"),
            None => assert!(point.get("payload").is_none(), "{selector}: {point}"),
        }
        assert!(point.get("vector").is_none());
    }
    // The scroll default is `true` too.
    let page = scroll(&qd, "p", json!({})).await;
    assert_eq!(page["points"][0]["payload"], payload);
}

#[tokio::test]
async fn with_vector_names_select_vectors() {
    let qd = Qd::start().await;
    create_single(&qd, "one").await;
    create(
        &qd,
        "named",
        json!({"vectors": {"img": {"size": 2, "distance": "Dot"}, "txt": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    upsert(&qd, "one", json!([{"id": 1, "vector": [1.0, 2.0]}])).await;
    upsert(
        &qd,
        "named",
        json!([{"id": 1, "vector": {"img": [1.0, 2.0], "txt": [3.0, 4.0]}}]),
    )
    .await;
    let vector = |reply: &Value| reply["result"][0]["vector"].clone();
    let (_, reply) = retrieve(&qd, "named", json!({"ids": [1], "with_vector": ["img"]})).await;
    assert_eq!(vector(&reply), json!({"img": [1.0, 2.0]}));
    let (_, reply) = retrieve(&qd, "one", json!({"ids": [1], "with_vector": true})).await;
    assert_eq!(vector(&reply), json!([1.0, 2.0]));
    let (_, reply) = retrieve(&qd, "named", json!({"ids": [1], "with_vectors": true})).await;
    assert_eq!(
        vector(&reply),
        json!({"img": [1.0, 2.0], "txt": [3.0, 4.0]})
    );
    let (status, reply) =
        retrieve(&qd, "named", json!({"ids": [1], "with_vector": ["nope"]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Not existing vector name error: nope"
    );
}

#[tokio::test]
async fn scroll_vectors_use_qdrant_output_shape() {
    let qd = Qd::start().await;
    create(
        &qd,
        "map",
        json!({"vectors": {"": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    upsert(&qd, "map", json!([{"id": 1, "vector": {"": [1.0, 2.0]}}])).await;
    let page = scroll(&qd, "map", json!({"with_vector": true})).await;
    assert_eq!(page["points"][0]["vector"], json!([1.0, 2.0]));

    create(
        &qd,
        "h",
        json!({"vectors": {"": {"size": 2, "distance": "Dot"}}, "sparse_vectors": {"s": {}}}),
    )
    .await;
    upsert(
        &qd,
        "h",
        json!([{"id": 1, "vector": {"": [1.0, 2.0], "s": {"indices": [3], "values": [0.5]}}}]),
    )
    .await;
    let page = scroll(&qd, "h", json!({"with_vector": true})).await;
    assert_eq!(
        page["points"][0]["vector"],
        json!({"": [1.0, 2.0], "s": {"indices": [3], "values": [0.5]}})
    );
    let page = scroll(&qd, "h", json!({"with_vector": ["s"]})).await;
    assert_eq!(
        page["points"][0]["vector"],
        json!({"s": {"indices": [3], "values": [0.5]}})
    );
}

// ----- scroll -----

/// A deterministic lowercase hyphenated UUID from `seed`.
fn uuid_of(seed: u64) -> String {
    let mut x = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut bytes = [0_u8; 16];
    for b in &mut bytes {
        x ^= x >> 29;
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        *b = (x >> 32) as u8;
    }
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[tokio::test]
async fn scroll_pages_cover_mixed_ids_exactly_once() {
    let qd = Qd::start().await;
    create_single(&qd, "mix").await;
    let uuids: Vec<String> = (0..129).map(uuid_of).collect();
    let mut points: Vec<Value> = (0..=127_u64)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0]}))
        .collect();
    points.extend(uuids.iter().map(|u| json!({"id": u, "vector": [0.0, 1.0]})));
    upsert(&qd, "mix", Value::Array(points)).await;
    let got = scroll_all(&qd, "mix", json!({}), 10).await;
    let mut sorted_uuids = uuids.clone();
    sorted_uuids.sort();
    let want: Vec<Value> = (0..=127_u64)
        .map(Value::from)
        .chain(sorted_uuids.into_iter().map(Value::from))
        .collect();
    assert_eq!(got.len(), 257);
    assert_eq!(got, want);
}

#[tokio::test]
async fn scroll_with_offset_is_inclusive() {
    let qd = Qd::start().await;
    create_single(&qd, "off").await;
    let points: Vec<Value> = [1_u64, 2, 3, 4, 5, 6, 7, 9, 10]
        .iter()
        .map(|i| json!({"id": i, "vector": [1.0, 0.0]}))
        .collect();
    upsert(&qd, "off", Value::Array(points)).await;
    let page = scroll(&qd, "off", json!({"offset": 7, "limit": 2})).await;
    assert_eq!(ids(&page["points"]), vec![json!(7), json!(9)]);
    assert_eq!(page["next_page_offset"], json!(10));
    let page = scroll(&qd, "off", json!({"offset": 8, "limit": 5})).await;
    assert_eq!(ids(&page["points"]), vec![json!(9), json!(10)]);
    assert_eq!(page["next_page_offset"], Value::Null);
    let page = scroll(&qd, "off", json!({"offset": 0, "limit": 1})).await;
    assert_eq!(ids(&page["points"]), vec![json!(1)]);
    // limit 0 is refused.
    let (status, reply) = qd
        .post("/collections/off/points/scroll", Some(json!({"limit": 0})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error(&reply), "Wrong input: limit must be at least 1");
}

#[tokio::test]
async fn scroll_with_filter_pages_the_matches_only() {
    let qd = Qd::start().await;
    create_single(&qd, "f").await;
    let points: Vec<Value> = (0..30_u64)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0], "payload": {"even": i % 2 == 0}}))
        .collect();
    upsert(&qd, "f", Value::Array(points)).await;
    let got = scroll_all(
        &qd,
        "f",
        json!({"filter": {"must": [{"key": "even", "match": {"value": true}}]}}),
        4,
    )
    .await;
    let want: Vec<Value> = (0..30_u64).step_by(2).map(Value::from).collect();
    assert_eq!(got, want);
}

/// PR #50 review: a scroll's total `limit` is bounded by the search
/// window (`max_window`, 100,000), as a query's `offset + limit` is, so one
/// request cannot buffer the whole collection.
#[tokio::test]
async fn scroll_limit_is_bounded_by_the_search_window() {
    let qd = Qd::start_with(|config| {
        config.query.max_scroll_limit = 2;
        config.query.search.limits.max_window = 5;
    })
    .await;
    create_single(&qd, "cap").await;
    let points: Vec<Value> = (0..8_u64)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0]}))
        .collect();
    upsert(&qd, "cap", Value::Array(points)).await;
    let page = scroll(&qd, "cap", json!({"limit": 5})).await;
    assert_eq!(page["points"].as_array().expect("points").len(), 5);
    for limit in [json!(6), json!(18_446_744_073_709_551_615_u64)] {
        let (status, reply) = qd
            .post(
                "/collections/cap/points/scroll",
                Some(json!({ "limit": limit })),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
        assert_eq!(error(&reply), "Wrong input: limit must be at most 5");
    }
}

#[tokio::test]
async fn scroll_limits_over_the_service_page_are_read_in_pages() {
    let qd = Qd::start_with(|config| config.query.max_scroll_limit = 3).await;
    create_single(&qd, "big").await;
    let points: Vec<Value> = (0..12_u64)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0]}))
        .collect();
    upsert(&qd, "big", Value::Array(points)).await;
    let page = scroll(&qd, "big", json!({"limit": 10})).await;
    assert_eq!(
        ids(&page["points"]),
        (0..10_u64).map(Value::from).collect::<Vec<_>>()
    );
    assert_eq!(page["next_page_offset"], json!(10));
    // Writes by filter read their ids in pages of at most that size too.
    let (status, reply) = qd
        .post(
            "/collections/big/points/delete?wait=true",
            Some(json!({"filter": {"must": [{"has_id": [1, 5, 11]}]}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let page = scroll(&qd, "big", json!({"limit": 100})).await;
    assert_eq!(page["points"].as_array().expect("present").len(), 9);
}

#[tokio::test]
async fn scroll_order_by_is_501() {
    let qd = Qd::start().await;
    create_single(&qd, "o").await;
    let (status, reply) = qd
        .post(
            "/collections/o/points/scroll",
            Some(json!({"order_by": "n"})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(error(&reply), "Unsupported in Loams: order_by");
}

#[tokio::test]
async fn count_with_and_without_filter() {
    let qd = Qd::start().await;
    create_single(&qd, "c").await;
    let points: Vec<Value> = (0..10_u64)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0], "payload": {"n": i}}))
        .collect();
    upsert(&qd, "c", Value::Array(points)).await;
    for (body, want) in [
        (json!({}), 10),
        (json!({"exact": true}), 10),
        (json!({"exact": false}), 10),
        (
            json!({"filter": {"must": [{"key": "n", "range": {"lt": 4}}]}}),
            4,
        ),
    ] {
        let (status, reply) = qd
            .post("/collections/c/points/count", Some(body.clone()))
            .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        assert_eq!(reply["result"]["count"], want, "{body}");
    }
}

// ----- gRPC -----

/// A gRPC point as the REST JSON record.
fn record_json(p: &pb::RetrievedPoint) -> Value {
    use pb::point_id::PointIdOptions;
    use pb::vector_output::Vector;
    use pb::vectors_output::VectorsOptions;
    let id = match p.id.as_ref().and_then(|id| id.point_id_options.as_ref()) {
        Some(PointIdOptions::Num(n)) => json!(n),
        Some(PointIdOptions::Uuid(s)) => json!(s),
        None => Value::Null,
    };
    let one = |v: &pb::VectorOutput| match &v.vector {
        Some(Vector::Dense(d)) => json!(d.data),
        Some(Vector::Sparse(s)) => json!({"indices": s.indices, "values": s.values}),
        other => panic!("{other:?}"),
    };
    let mut out = json!({"id": id, "payload": Value::Object(payload_to_map(&p.payload))});
    match p.vectors.as_ref().and_then(|v| v.vectors_options.as_ref()) {
        Some(VectorsOptions::Vector(v)) => out["vector"] = one(v),
        Some(VectorsOptions::Vectors(named)) => {
            out["vector"] = Value::Object(
                named
                    .vectors
                    .iter()
                    .map(|(n, v)| (n.clone(), one(v)))
                    .collect(),
            );
        }
        None => {}
    }
    out
}

fn with_vectors(on: bool) -> Option<pb::WithVectorsSelector> {
    Some(pb::WithVectorsSelector {
        selector_options: Some(pb::with_vectors_selector::SelectorOptions::Enable(on)),
    })
}

#[tokio::test]
async fn grpc_reads_match_rest() {
    let qd = Qd::start().await;
    create(
        &qd,
        "h",
        json!({"vectors": {"": {"size": 2, "distance": "Dot"}}, "sparse_vectors": {"s": {}}}),
    )
    .await;
    let uuid = "fa38d572-4f1e-4a76-9c47-7a3c7b5a1f00";
    upsert(
        &qd,
        "h",
        json!([
            {"id": 1, "vector": {"": [1.0, 2.0], "s": {"indices": [4, 2], "values": [1.0, 0.5]}}, "payload": {"a": 1, "b": {"c": "x"}}},
            {"id": 2, "vector": {"": [3.0, 4.0]}, "payload": {"a": 2}},
            {"id": uuid, "vector": {"": [5.0, 6.0]}, "payload": {"a": 3}},
        ]),
    )
    .await;
    let rest = scroll(&qd, "h", json!({"with_vector": true, "limit": 2})).await;
    let mut points = qd.points().await;
    let page = points
        .scroll(pb::ScrollPoints {
            collection_name: "h".to_string(),
            limit: Some(2),
            with_vectors: with_vectors(true),
            ..Default::default()
        })
        .await
        .expect("scroll")
        .into_inner();
    let grpc: Vec<Value> = page.result.iter().map(record_json).collect();
    assert_eq!(Value::Array(grpc), rest["points"]);
    assert_eq!(
        page.next_page_offset
            .and_then(|id| id.point_id_options)
            .expect("next page"),
        pb::point_id::PointIdOptions::Uuid(uuid.to_string())
    );

    let (_, rest) = retrieve(&qd, "h", json!({"ids": [uuid, 1], "with_vector": true})).await;
    let got = points
        .get(pb::GetPoints {
            collection_name: "h".to_string(),
            ids: vec![
                pb::PointId {
                    point_id_options: Some(pb::point_id::PointIdOptions::Uuid(uuid.to_string())),
                },
                pb::PointId {
                    point_id_options: Some(pb::point_id::PointIdOptions::Num(1)),
                },
            ],
            with_vectors: with_vectors(true),
            ..Default::default()
        })
        .await
        .expect("get")
        .into_inner();
    let grpc: Vec<Value> = got.result.iter().map(record_json).collect();
    assert_eq!(Value::Array(grpc), rest["result"]);

    let counted = points
        .count(pb::CountPoints {
            collection_name: "h".to_string(),
            exact: Some(true),
            ..Default::default()
        })
        .await
        .expect("count")
        .into_inner();
    assert_eq!(counted.result.expect("result").count, 3);

    let err = points
        .scroll(pb::ScrollPoints {
            collection_name: "h".to_string(),
            order_by: Some(pb::OrderBy {
                key: "a".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect_err("order_by");
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn retrieve_refuses_more_ids_than_max_point_ids() {
    // Issue #298: a client-chosen id count must not size an allocation.
    let qd = Qd::start().await;
    create_single(&qd, "huge").await;
    let max = loams_qdrant::QdrantConfig::default().max_point_ids;
    let ids: Vec<u64> = (0..=max as u64).collect();
    let (status, reply) = retrieve(&qd, "huge", json!({ "ids": ids })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(
        error(&reply),
        format!(
            "Wrong input: The id list holds {} entries, more than the limit of {max}",
            max + 1
        )
    );
    // At the limit the request is served (no point exists, so none returns).
    let ids: Vec<u64> = (0..max as u64).collect();
    let (status, reply) = retrieve(&qd, "huge", json!({ "ids": ids })).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
}

#[tokio::test]
async fn grpc_retrieve_count_is_checked_before_conversion() {
    let qd = Qd::start_with(|config| {
        config.qdrant.as_mut().unwrap().max_point_ids = 2;
    })
    .await;
    create_single(&qd, "bounded").await;
    let mut client = qd.points().await;
    let err = client
        .get(pb::GetPoints {
            collection_name: "bounded".into(),
            ids: vec![pb::PointId::default(); 3],
            ..Default::default()
        })
        .await
        .expect_err("count precedes invalid ids");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "Wrong input: The id list holds 3 entries, more than the limit of 2"
    );
    for len in [0, 2] {
        let result = client
            .get(pb::GetPoints {
                collection_name: "bounded".into(),
                ids: vec![
                    pb::PointId {
                        point_id_options: Some(pb::point_id::PointIdOptions::Num(1))
                    };
                    len
                ],
                ..Default::default()
            })
            .await
            .expect("at or below count")
            .into_inner();
        assert!(result.result.is_empty());
    }
}

#[tokio::test]
async fn retrieve_limit_tracks_native_configuration() {
    let qd = Qd::start_with(|config| {
        config.query.max_get_keys = 2;
        config.qdrant.as_mut().unwrap().max_point_ids = 5;
    })
    .await;
    create_single(&qd, "native_limit").await;
    let (status, reply) = retrieve(&qd, "native_limit", json!({"ids": [0, 1, 2]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = "Wrong input: The id list holds 3 entries, more than the limit of 2";
    assert_eq!(error(&reply), message);
    let err = qd
        .points()
        .await
        .get(pb::GetPoints {
            collection_name: "native_limit".into(),
            ids: vec![pb::PointId::default(); 3],
            ..Default::default()
        })
        .await
        .expect_err("native ceiling before conversion");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), message);
    let (status, reply) = retrieve(&qd, "native_limit", json!({"ids": [0, 1]})).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"], json!([]));
}

#[tokio::test]
async fn retrieve_limit_does_not_block_single_point_get() {
    let qd = Qd::start_with(|config| {
        config.qdrant.as_mut().unwrap().max_point_ids = 0;
    })
    .await;
    create_single(&qd, "single").await;
    upsert(&qd, "single", json!([{"id": 1, "vector": [1.0, 2.0]}])).await;
    let (status, reply) = qd.get("/collections/single/points/1", None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"]["id"], 1);
    let (status, reply) = retrieve(&qd, "single", json!({"ids": [1]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: The id list holds 1 entries, more than the limit of 0"
    );
    let (status, reply) = retrieve(&qd, "single", json!({"ids": []})).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"], json!([]));
}
