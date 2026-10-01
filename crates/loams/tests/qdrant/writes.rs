//! Point writes end to end (plan M1.4 Task 5): upserts in every form,
//! deletes, payload and vector updates, batches, existence errors,
//! backpressure and the gRPC methods.

use std::collections::BTreeSet;
use std::time::Duration;

use loams_qdrant::convert::value::map_to_payload;
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

/// A collection with one unnamed 2-dimensional vector of `distance`.
async fn create_single(qd: &Qd, name: &str, distance: &str) {
    create(
        qd,
        name,
        json!({"vectors": {"size": 2, "distance": distance}}),
    )
    .await;
}

/// `path` under `/collections/{name}/points` with `?wait=true`.
async fn write(qd: &Qd, method: &str, name: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let path = format!("/collections/{name}/points{path}?wait=true");
    match method {
        "PUT" => qd.put(&path, Some(body)).await,
        _ => qd.post(&path, Some(body)).await,
    }
}

/// A write that must succeed.
async fn ok(qd: &Qd, method: &str, name: &str, path: &str, body: Value) -> Value {
    let (status, reply) = write(qd, method, name, path, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}: {reply}");
    let results = match &reply["result"] {
        Value::Array(results) => results.clone(),
        one => vec![one.clone()],
    };
    for result in results {
        assert_eq!(result["status"], "completed", "{reply}");
    }
    reply
}

async fn upsert(qd: &Qd, name: &str, points: Value) {
    ok(qd, "PUT", name, "", json!({ "points": points })).await;
}

/// Every point, with payload and vectors, through scroll.
async fn all(qd: &Qd, name: &str) -> Vec<Value> {
    let (status, reply) = qd
        .post(
            &format!("/collections/{name}/points/scroll"),
            Some(json!({"limit": 10_000, "with_payload": true, "with_vector": true})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    reply["result"]["points"]
        .as_array()
        .expect("points")
        .clone()
}

/// One point (payload and vectors), or `None`.
async fn one(qd: &Qd, name: &str, id: Value) -> Option<Value> {
    let (status, reply) = qd
        .post(
            &format!("/collections/{name}/points"),
            Some(json!({"ids": [id], "with_payload": true, "with_vector": true})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    reply["result"]
        .as_array()
        .expect("records")
        .first()
        .cloned()
}

fn ids(points: &[Value]) -> Vec<Value> {
    points.iter().map(|p| p["id"].clone()).collect()
}

fn keys(v: &Value) -> Vec<String> {
    v.as_object().expect("an object").keys().cloned().collect()
}

async fn count(qd: &Qd, name: &str) -> u64 {
    let (status, reply) = qd
        .post(
            &format!("/collections/{name}/points/count"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    reply["result"]["count"].as_u64().expect("count")
}

fn approx(got: &Value, want: &[f32]) {
    let got: Vec<f32> = serde_json::from_value(got.clone()).expect("a vector");
    assert_eq!(got.len(), want.len(), "{got:?} vs {want:?}");
    for (g, w) in got.iter().zip(want) {
        assert!((g - w).abs() < 1e-6, "{got:?} vs {want:?}");
    }
}

// ----- upserts -----

#[tokio::test]
async fn upsert_accepts_every_vector_and_id_form() {
    let qd = Qd::start().await;
    create_single(&qd, "one", "Dot").await;
    let uuid = "fa38d572-4f1e-4a76-9c47-7a3c7b5a1f00";
    let simple = "5C56C793691F4B3AB0A6A1B7E5A2E0C1";
    upsert(
        &qd,
        "one",
        json!([
            {"id": 432, "vector": [1.0, 2.0]},
            {"id": uuid, "vector": {"": [3.0, 4.0]}},
            {"id": simple, "vector": [5.0, 6.0], "payload": null},
        ]),
    )
    .await;
    ok(
        &qd,
        "PUT",
        "one",
        "",
        json!({"batch": {"ids": [7, 8], "vectors": [[1.0, 1.0], [2.0, 2.0]], "payloads": [{"a": 1}, null]}}),
    )
    .await;
    let got = all(&qd, "one").await;
    assert_eq!(
        ids(&got),
        vec![
            json!(7),
            json!(8),
            json!(432),
            json!("5c56c793-691f-4b3a-b0a6-a1b7e5a2e0c1"),
            json!(uuid)
        ]
    );
    assert_eq!(got[0]["payload"], json!({"a": 1}));
    assert_eq!(got[1]["payload"], json!({}));
    assert_eq!(got[4]["vector"], json!([3.0, 4.0]));

    create(
        &qd,
        "named",
        json!({"vectors": {"img": {"size": 2, "distance": "Dot"}, "txt": {"size": 3, "distance": "Dot"}}}),
    )
    .await;
    upsert(
        &qd,
        "named",
        json!([{"id": 1, "vector": {"img": [1.0, 2.0]}}]),
    )
    .await;
    ok(
        &qd,
        "PUT",
        "named",
        "",
        json!({"batch": {"ids": [2], "vectors": {"img": [[3.0, 4.0]], "txt": [[1.0, 2.0, 3.0]]}}}),
    )
    .await;
    let got = all(&qd, "named").await;
    assert_eq!(got[0]["vector"], json!({"img": [1.0, 2.0]}));
    assert_eq!(
        got[1]["vector"],
        json!({"img": [3.0, 4.0], "txt": [1.0, 2.0, 3.0]})
    );
    // An empty named map is a point without vectors.
    upsert(&qd, "named", json!([{"id": 3, "vector": {}}])).await;
    assert_eq!(
        one(&qd, "named", json!(3)).await.expect("3")["vector"],
        json!({})
    );
    // A batch whose lengths differ.
    let (status, reply) = write(
        &qd,
        "PUT",
        "one",
        "",
        json!({"batch": {"ids": [1, 2], "vectors": [[1.0, 1.0]]}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Number of ids, vectors and payloads must match"
    );
}

#[tokio::test]
async fn payload_round_trips_verbatim() {
    let qd = Qd::start().await;
    create_single(&qd, "p", "Dot").await;
    let payload = json!({
        "zeta": 1,
        "alpha": {"nested": {"deep": [1, 2, {"x": null}]}, "b": "two"},
        "metadata": null,
        "unicode": "héllo wörld ✓ 日本",
        "big": 9_007_199_254_740_993_u64,
        "neg": -12,
        "float": 1.5,
        "arr": ["a", 1, true, null],
        "mid": {}
    });
    upsert(
        &qd,
        "p",
        json!([{"id": 1, "vector": [1.0, 0.0], "payload": payload}]),
    )
    .await;
    let got = one(&qd, "p", json!(1)).await.expect("point");
    assert_eq!(got["payload"], payload);
    // `Map` equality ignores order under `preserve_order` (E5).
    assert_eq!(keys(&got["payload"]), keys(&payload));
    assert_eq!(keys(&got["payload"]["alpha"]), vec!["nested", "b"]);
    assert_eq!(got["payload"]["big"].as_u64(), Some(9_007_199_254_740_993));
}

#[tokio::test]
async fn cosine_vectors_are_stored_normalized() {
    let qd = Qd::start().await;
    create_single(&qd, "cos", "Cosine").await;
    create_single(&qd, "euc", "Euclid").await;
    upsert(
        &qd,
        "cos",
        json!([{"id": 1, "vector": [3.0, 4.0]}, {"id": 2, "vector": [0.0, 0.0]}]),
    )
    .await;
    upsert(&qd, "euc", json!([{"id": 1, "vector": [3.0, 4.0]}])).await;
    approx(
        &one(&qd, "cos", json!(1)).await.expect("present")["vector"],
        &[0.6, 0.8],
    );
    approx(
        &one(&qd, "cos", json!(2)).await.expect("present")["vector"],
        &[0.0, 0.0],
    );
    approx(
        &one(&qd, "euc", json!(1)).await.expect("present")["vector"],
        &[3.0, 4.0],
    );
}

#[tokio::test]
async fn wrong_dimension_and_unknown_vector_are_400_with_qdrant_text() {
    let qd = Qd::start().await;
    create_single(&qd, "d", "Dot").await;
    let cases = [
        (
            json!({"id": 1, "vector": [1.0, 2.0, 3.0]}),
            "Wrong input: Vector dimension error: expected dim: 2, got 3",
        ),
        (
            json!({"id": 1, "vector": {"nope": [1.0, 2.0]}}),
            "Wrong input: Not existing vector name error: nope",
        ),
        (
            json!({"id": "123", "vector": [1.0, 2.0]}),
            "Format error in JSON body: value 123 is not a valid point ID, valid values are either an unsigned integer or a UUID",
        ),
    ];
    for (point, text) in cases {
        let (status, reply) = write(&qd, "PUT", "d", "", json!({ "points": [point] })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
        assert_eq!(error(&reply), text);
    }
    assert_eq!(count(&qd, "d").await, 0);
}

#[tokio::test]
async fn multivector_values_are_501() {
    let qd = Qd::start().await;
    create_single(&qd, "m", "Dot").await;
    for vector in [
        json!([[1.0, 2.0], [3.0, 4.0]]),
        json!({"": [[1.0, 2.0]]}),
        json!({"": {"text": "hello", "model": "bm25"}}),
    ] {
        let (status, reply) = write(
            &qd,
            "PUT",
            "m",
            "",
            json!({"points": [{"id": 1, "vector": vector}]}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
        assert!(
            error(&reply).starts_with("Unsupported in Loams: "),
            "{reply}"
        );
    }
}

// ----- sparse vectors -----

async fn create_hybrid(qd: &Qd, name: &str) {
    create(
        qd,
        name,
        json!({"vectors": {"": {"size": 2, "distance": "Dot"}}, "sparse_vectors": {"s": {}}}),
    )
    .await;
}

#[tokio::test]
async fn sparse_values_are_stored_sorted_with_zero_weights() {
    let qd = Qd::start().await;
    create_hybrid(&qd, "h").await;
    upsert(
        &qd,
        "h",
        json!([{"id": 1, "vector": {"": [1.0, 2.0], "s": {"indices": [9, 2], "values": [0.0, 1.5]}}}]),
    )
    .await;
    assert_eq!(
        one(&qd, "h", json!(1)).await.expect("present")["vector"],
        json!({"": [1.0, 2.0], "s": {"indices": [2, 9], "values": [1.5, 0.0]}})
    );
    // LangChain's SPARSE mode: a sparse-only collection.
    create(
        &qd,
        "lc",
        json!({"vectors": {}, "sparse_vectors": {"my-sparse-vector": {}}}),
    )
    .await;
    upsert(
        &qd,
        "lc",
        json!([{"id": 1, "vector": {"my-sparse-vector": {"indices": [3, 1], "values": [1.0, 2.0]}}}]),
    )
    .await;
    assert_eq!(
        one(&qd, "lc", json!(1)).await.expect("present")["vector"],
        json!({"my-sparse-vector": {"indices": [1, 3], "values": [2.0, 1.0]}})
    );
}

#[tokio::test]
async fn invalid_sparse_values_are_400() {
    let qd = Qd::start().await;
    create_hybrid(&qd, "h").await;
    let cases = [
        (
            json!({"s": {"indices": [1, 1], "values": [1.0, 2.0]}}),
            "Wrong input: Sparse vector s: index 1 appears more than once",
        ),
        (
            json!({"s": {"indices": [1, 2], "values": [1.0]}}),
            "Wrong input: Sparse vector s: indices and values must have the same length (2 != 1)",
        ),
        (
            json!({"": {"indices": [1], "values": [1.0]}}),
            "Wrong input: Vector  is a dense vector",
        ),
        (
            json!({"t": {"indices": [1], "values": [1.0]}}),
            "Wrong input: Not existing vector name error: t",
        ),
        (
            json!({"s": [1.0, 2.0]}),
            "Wrong input: Vector s is a sparse vector",
        ),
    ];
    for (vector, text) in cases {
        let (status, reply) = write(
            &qd,
            "PUT",
            "h",
            "",
            json!({"points": [{"id": 2, "vector": {"": [0.0, 0.0]}}, {"id": 1, "vector": vector}]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
        assert_eq!(error(&reply), text);
    }
    // A NaN cannot travel in JSON; gRPC carries it.
    let mut points = qd.points().await;
    let err = points
        .upsert(pb::UpsertPoints {
            collection_name: "h".to_string(),
            wait: Some(true),
            points: vec![pb::PointStruct {
                id: Some(num(1)),
                payload: Default::default(),
                vectors: Some(named([("s", sparse(&[1], &[f32::NAN]))])),
            }],
            ..Default::default()
        })
        .await
        .expect_err("NaN");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "Wrong input: Sparse vector s: value for index 1 is not finite"
    );
    assert_eq!(count(&qd, "h").await, 0);
}

#[tokio::test]
async fn update_and_delete_vectors_handle_sparse_vectors() {
    let qd = Qd::start().await;
    create_hybrid(&qd, "h").await;
    upsert(
        &qd,
        "h",
        json!([{"id": 1, "vector": {"": [1.0, 2.0], "s": {"indices": [1], "values": [1.0]}}}]),
    )
    .await;
    ok(
        &qd,
        "PUT",
        "h",
        "/vectors",
        json!({"points": [{"id": 1, "vector": {"s": {"indices": [5, 4], "values": [2.0, 3.0]}}}]}),
    )
    .await;
    assert_eq!(
        one(&qd, "h", json!(1)).await.expect("present")["vector"],
        json!({"": [1.0, 2.0], "s": {"indices": [4, 5], "values": [3.0, 2.0]}})
    );
    ok(
        &qd,
        "POST",
        "h",
        "/vectors/delete",
        json!({"points": [1], "vector": ["s"]}),
    )
    .await;
    assert_eq!(
        one(&qd, "h", json!(1)).await.expect("present")["vector"],
        json!({"": [1.0, 2.0]})
    );
}

// ----- deletes -----

/// Points `0..n` with payload `{"n": i, "doc_id": "d<i % 3>"}`.
async fn seed(qd: &Qd, name: &str, n: u64) {
    for start in (0..n).step_by(1000) {
        let points: Vec<Value> = (start..n.min(start + 1000))
            .map(|i| json!({"id": i, "vector": [1.0, 0.0], "payload": {"n": i, "doc_id": format!("d{}", i % 3)}}))
            .collect();
        upsert(qd, name, Value::Array(points)).await;
    }
}

async fn id_set(qd: &Qd, name: &str) -> BTreeSet<u64> {
    all(qd, name)
        .await
        .iter()
        .map(|p| p["id"].as_u64().expect("u64 id"))
        .collect()
}

#[tokio::test]
async fn delete_by_ids_and_by_filter() {
    let qd = Qd::start().await;
    create_single(&qd, "del", "Dot").await;
    seed(&qd, "del", 10).await;
    ok(
        &qd,
        "POST",
        "del",
        "/delete",
        json!({"points": [1, 2, 999]}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "del",
        "/delete",
        json!({"filter": {"must": [{"key": "n", "range": {"gte": 7}}]}}),
    )
    .await;
    assert_eq!(id_set(&qd, "del").await, BTreeSet::from([0, 3, 4, 5, 6]));
    // Neither or both selectors.
    let (status, _) = write(&qd, "POST", "del", "/delete", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_by_filter_removes_exactly_the_matches() {
    let qd = Qd::start().await;
    create_single(&qd, "big", "Dot").await;
    seed(&qd, "big", 2_500).await;
    ok(
        &qd,
        "POST",
        "big",
        "/delete",
        json!({"filter": {"must": [{"key": "n", "range": {"gte": 100, "lt": 2_300}}]}}),
    )
    .await;
    let want: BTreeSet<u64> = (0..100).chain(2_300..2_500).collect();
    assert_eq!(id_set(&qd, "big").await, want);
}

#[tokio::test]
async fn llamaindex_delete_shapes_work() {
    let qd = Qd::start().await;
    create_single(&qd, "li", "Dot").await;
    seed(&qd, "li", 9).await;
    ok(
        &qd,
        "POST",
        "li",
        "/delete",
        json!({"filter": {"should": [{"has_id": [0, 4]}]}}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "li",
        "/delete",
        json!({"filter": {"must": [{"key": "doc_id", "match": {"value": "d1"}}]}}),
    )
    .await;
    assert_eq!(id_set(&qd, "li").await, BTreeSet::from([2, 3, 5, 6, 8]));
}

// ----- payload -----

async fn with_payload(qd: &Qd, name: &str, payload: Value) {
    create_single(qd, name, "Dot").await;
    upsert(
        qd,
        name,
        json!([
            {"id": 1, "vector": [1.0, 0.0], "payload": payload},
            {"id": 2, "vector": [1.0, 0.0], "payload": payload},
        ]),
    )
    .await;
}

async fn payload_of(qd: &Qd, name: &str, id: u64) -> Value {
    one(qd, name, json!(id)).await.expect("point")["payload"].clone()
}

#[tokio::test]
async fn set_payload_merges_top_level() {
    let qd = Qd::start().await;
    with_payload(&qd, "sp", json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4})).await;
    ok(
        &qd,
        "POST",
        "sp",
        "/payload",
        json!({"payload": {"b": {"c": 9}, "new": true, "e": null}, "points": [1]}),
    )
    .await;
    let got = payload_of(&qd, "sp", 1).await;
    assert_eq!(got, json!({"a": 1, "b": {"c": 9}, "new": true}));
    // An existing key keeps its place, a new one is appended (E5).
    assert_eq!(keys(&got), vec!["a", "b", "new"]);
    assert_eq!(
        payload_of(&qd, "sp", 2).await,
        json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4})
    );
}

#[tokio::test]
async fn set_payload_with_key_merges_at_the_path() {
    let qd = Qd::start().await;
    with_payload(&qd, "sk", json!({"a": 1, "b": {"c": 2, "d": 3}})).await;
    ok(
        &qd,
        "POST",
        "sk",
        "/payload",
        json!({"payload": {"c": 9, "x": [1]}, "points": [1], "key": "b"}),
    )
    .await;
    assert_eq!(
        payload_of(&qd, "sk", 1).await,
        json!({"a": 1, "b": {"c": 9, "d": 3, "x": [1]}})
    );
    // A missing path is created.
    ok(
        &qd,
        "POST",
        "sk",
        "/payload",
        json!({"payload": {"k": "v"}, "points": [2], "key": "new.deep"}),
    )
    .await;
    assert_eq!(
        payload_of(&qd, "sk", 2).await,
        json!({"a": 1, "b": {"c": 2, "d": 3}, "new": {"deep": {"k": "v"}}})
    );
}

#[tokio::test]
async fn overwrite_payload_replaces() {
    let qd = Qd::start().await;
    with_payload(&qd, "ow", json!({"a": 1, "b": {"c": 2, "d": 3}})).await;
    ok(
        &qd,
        "PUT",
        "ow",
        "/payload",
        json!({"payload": {"z": 0}, "points": [1]}),
    )
    .await;
    assert_eq!(payload_of(&qd, "ow", 1).await, json!({"z": 0}));
    ok(
        &qd,
        "PUT",
        "ow",
        "/payload",
        json!({"payload": {"c": 9}, "points": [2], "key": "b"}),
    )
    .await;
    assert_eq!(
        payload_of(&qd, "ow", 2).await,
        json!({"a": 1, "b": {"c": 9}})
    );
}

#[tokio::test]
async fn delete_payload_removes_keys_including_array_paths() {
    let qd = Qd::start().await;
    let payload =
        json!({"a": 1, "b": {"c": 2, "d": 3}, "arr": [{"x": 1, "y": 2}, {"x": 3}], "keep": true});
    with_payload(&qd, "dp", payload).await;
    ok(
        &qd,
        "POST",
        "dp",
        "/payload/delete",
        json!({"keys": ["a", "b.c"], "points": [1]}),
    )
    .await;
    assert_eq!(
        payload_of(&qd, "dp", 1).await,
        json!({"b": {"d": 3}, "arr": [{"x": 1, "y": 2}, {"x": 3}], "keep": true})
    );
    ok(
        &qd,
        "POST",
        "dp",
        "/payload/delete",
        json!({"keys": ["arr[].x", "b.d"], "points": [2]}),
    )
    .await;
    assert_eq!(
        payload_of(&qd, "dp", 2).await,
        json!({"a": 1, "b": {"c": 2}, "arr": [{"y": 2}, {}], "keep": true})
    );
}

#[tokio::test]
async fn clear_payload_empties() {
    let qd = Qd::start().await;
    with_payload(&qd, "cp", json!({"a": 1})).await;
    ok(&qd, "POST", "cp", "/payload/clear", json!({"points": [1]})).await;
    assert_eq!(payload_of(&qd, "cp", 1).await, json!({}));
    assert_eq!(payload_of(&qd, "cp", 2).await, json!({"a": 1}));
}

#[tokio::test]
async fn payload_ops_by_filter_touch_only_matches() {
    let qd = Qd::start().await;
    create_single(&qd, "pf", "Dot").await;
    seed(&qd, "pf", 9).await;
    let d0 = json!({"must": [{"key": "doc_id", "match": {"value": "d0"}}]});
    let d1 = json!({"must": [{"key": "doc_id", "match": {"value": "d1"}}]});
    ok(
        &qd,
        "POST",
        "pf",
        "/payload",
        json!({"payload": {"tag": "x"}, "filter": d0}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "pf",
        "/payload",
        json!({"payload": {"k": 1}, "filter": d1, "key": "sub"}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "pf",
        "/payload/delete",
        json!({"keys": ["n"], "filter": d1}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "pf",
        "/payload/clear",
        json!({"filter": {"must": [{"has_id": [8]}]}}),
    )
    .await;
    for point in all(&qd, "pf").await {
        let id = point["id"].as_u64().expect("present");
        let want = match id {
            8 => json!({}),
            _ if id % 3 == 0 => json!({"n": id, "doc_id": "d0", "tag": "x"}),
            _ if id % 3 == 1 => json!({"doc_id": "d1", "sub": {"k": 1}}),
            _ => json!({"n": id, "doc_id": "d2"}),
        };
        assert_eq!(point["payload"], want, "point {id}");
    }
}

#[tokio::test]
async fn set_payload_with_key_by_filter_keeps_qdrant_semantics() {
    // A plain key with scalar values runs as a native MergeDeep patch; an
    // object or a null value keeps the read-modify-write (M1.4 row E4 (a)),
    // so the object replaces and the null removes, as Qdrant's value_set.
    let qd = Qd::start().await;
    with_payload(&qd, "sk", json!({"sub": {"k": {"a": 1, "b": 2}, "x": 1}})).await;
    let both = json!({"must": [{"has_id": [1, 2]}]});
    ok(
        &qd,
        "POST",
        "sk",
        "/payload",
        json!({"payload": {"y": 2}, "filter": both, "key": "sub"}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "sk",
        "/payload",
        json!({"payload": {"k": {"a": 9}, "x": null}, "filter": both, "key": "sub"}),
    )
    .await;
    for id in [1, 2] {
        assert_eq!(
            payload_of(&qd, "sk", id).await,
            json!({"sub": {"k": {"a": 9}, "y": 2}}),
            "point {id}"
        );
    }
}

// ----- vectors -----

async fn create_named(qd: &Qd, name: &str) {
    create(
        qd,
        name,
        json!({"vectors": {"img": {"size": 2, "distance": "Dot"}, "txt": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    upsert(
        qd,
        name,
        json!([
            {"id": 1, "vector": {"img": [1.0, 1.0], "txt": [2.0, 2.0]}},
            {"id": 2, "vector": {"img": [3.0, 3.0], "txt": [4.0, 4.0]}},
        ]),
    )
    .await;
}

#[tokio::test]
async fn update_vectors_replaces_one_named_vector_only() {
    let qd = Qd::start().await;
    create_named(&qd, "uv").await;
    ok(
        &qd,
        "PUT",
        "uv",
        "/vectors",
        json!({"points": [{"id": 1, "vector": {"img": [9.0, 9.0]}}]}),
    )
    .await;
    assert_eq!(
        one(&qd, "uv", json!(1)).await.expect("present")["vector"],
        json!({"img": [9.0, 9.0], "txt": [2.0, 2.0]})
    );
    let (status, reply) = write(
        &qd,
        "PUT",
        "uv",
        "/vectors",
        json!({"points": [{"id": 1, "vector": {"img": [1.0]}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
}

#[tokio::test]
async fn delete_vectors_removes_a_named_vector() {
    let qd = Qd::start().await;
    create_named(&qd, "dv").await;
    ok(
        &qd,
        "POST",
        "dv",
        "/vectors/delete",
        json!({"points": [1], "vector": ["img"]}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "dv",
        "/vectors/delete",
        json!({"filter": {"must": [{"has_id": [2]}]}, "vector": ["txt"]}),
    )
    .await;
    assert_eq!(
        one(&qd, "dv", json!(1)).await.expect("present")["vector"],
        json!({"txt": [2.0, 2.0]})
    );
    assert_eq!(
        one(&qd, "dv", json!(2)).await.expect("present")["vector"],
        json!({"img": [3.0, 3.0]})
    );
    let (status, reply) = write(
        &qd,
        "POST",
        "dv",
        "/vectors/delete",
        json!({"points": [1], "vector": ["nope"]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Not existing vector name error: nope"
    );
}

// ----- existence -----

#[tokio::test]
async fn update_vectors_on_a_missing_point_is_404() {
    let qd = Qd::start().await;
    create_named(&qd, "mv").await;
    let (status, reply) = write(
        &qd,
        "PUT",
        "mv",
        "/vectors",
        json!({"points": [{"id": 999, "vector": {"img": [5.0, 5.0]}}, {"id": 2, "vector": {"img": [7.0, 7.0]}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{reply}");
    assert_eq!(error(&reply), "Not found: No point with id 999 found");
    // The whole batch was written: point 2 changed, 999 was not created.
    assert_eq!(
        one(&qd, "mv", json!(2)).await.expect("present")["vector"]["img"],
        json!([7.0, 7.0])
    );
    assert!(one(&qd, "mv", json!(999)).await.is_none());
}

#[tokio::test]
async fn set_payload_on_a_missing_id_is_404() {
    let qd = Qd::start().await;
    with_payload(&qd, "mp", json!({"a": 1})).await;
    let uuid = "0a38d572-4f1e-4a76-9c47-7a3c7b5a1f00";
    for key in [None, Some("sub")] {
        let mut body = json!({"payload": {"b": 2}, "points": [1, uuid]});
        if let Some(key) = key {
            body["key"] = json!(key);
        }
        let (status, reply) = write(&qd, "POST", "mp", "/payload", body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{reply}");
        assert_eq!(
            error(&reply),
            format!("Not found: No point with id {uuid} found")
        );
    }
    assert!(one(&qd, "mp", json!(uuid)).await.is_none());
}

// ----- batches, wait, tokens -----

/// Owner ruling O1 (row T5-12): a request is one atomic write, so over
/// 10,000 planned ops it is refused whole, asking the client to split it.
#[tokio::test]
async fn a_request_over_10000_ops_is_400_asking_to_split() {
    let qd = Qd::start().await;
    create_single(&qd, "big", "Dot").await;
    let points: Vec<Value> = (0..10_001u64)
        .map(|id| json!({"id": id, "vector": [1.0, 0.0]}))
        .collect();
    let (status, reply) = write(&qd, "PUT", "big", "", json!({ "points": points })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(
        error(&reply),
        "Wrong input: a write request holds at most 10000 operations, got 10001; split the batch into smaller requests"
    );
    assert_eq!(count(&qd, "big").await, 0, "nothing was written");
    // A batch counts its operations together.
    let half: Vec<Value> = (0..5_001u64)
        .map(|id| json!({"id": id, "vector": [1.0, 0.0]}))
        .collect();
    let (status, reply) = write(
        &qd,
        "POST",
        "big",
        "/batch",
        json!({"operations": [{"upsert": {"points": half}}, {"upsert": {"points": half}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert!(error(&reply).ends_with("got 10002; split the batch into smaller requests"));
    // A filter operation next to them does not lift the bound on the
    // operations the request lists (PR #51 review).
    let many: Vec<Value> = (0..10_001u64)
        .map(|id| json!({"id": id, "vector": [1.0, 0.0]}))
        .collect();
    let (status, reply) = write(
        &qd,
        "POST",
        "big",
        "/batch",
        json!({"operations": [
            {"upsert": {"points": many}},
            {"delete": {"filter": {"must": [{"key": "k", "match": {"value": 1}}]}}}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert!(error(&reply).ends_with("got 10001; split the batch into smaller requests"));
    assert_eq!(count(&qd, "big").await, 0, "nothing was written");
    // 10,000 is accepted.
    let points: Vec<Value> = (0..10_000u64)
        .map(|id| json!({"id": id, "vector": [1.0, 0.0]}))
        .collect();
    upsert(&qd, "big", json!(points)).await;
    assert_eq!(count(&qd, "big").await, 10_000);
}

#[tokio::test]
async fn batch_update_is_one_atomic_write() {
    let qd = Qd::start().await;
    create_named(&qd, "b").await;
    upsert(
        &qd,
        "b",
        json!([{"id": 3, "vector": {"img": [0.0, 1.0]}, "payload": {"x": 1, "y": 2}}]),
    )
    .await;
    let operations = json!([
        {"upsert": {"points": [{"id": 4, "vector": {"img": [1.0, 0.0]}, "payload": {"k": 4}}]}},
        {"delete": {"points": [2]}},
        {"set_payload": {"payload": {"s": 1}, "points": [1]}},
        {"overwrite_payload": {"payload": {"o": 1}, "points": [4]}},
        {"delete_payload": {"keys": ["x"], "points": [3]}},
        {"clear_payload": {"points": [1]}},
        {"update_vectors": {"points": [{"id": 3, "vector": {"txt": [5.0, 5.0]}}]}},
        {"delete_vectors": {"points": [1], "vector": ["txt"]}},
    ]);
    let reply = ok(
        &qd,
        "POST",
        "b",
        "/batch",
        json!({ "operations": operations }),
    )
    .await;
    let results = reply["result"].as_array().expect("results");
    assert_eq!(results.len(), 8);
    assert!(results.iter().all(|r| r == &results[0]), "{reply}");
    let got = all(&qd, "b").await;
    assert_eq!(ids(&got), vec![json!(1), json!(3), json!(4)]);
    assert_eq!(got[0]["payload"], json!({}));
    assert_eq!(got[0]["vector"], json!({"img": [1.0, 1.0]}));
    assert_eq!(got[1]["payload"], json!({"y": 2}));
    assert_eq!(
        got[1]["vector"],
        json!({"img": [0.0, 1.0], "txt": [5.0, 5.0]})
    );
    assert_eq!(got[2]["payload"], json!({"o": 1}));

    // Op 3 has a wrong dimension: nothing of the batch applies.
    let before = all(&qd, "b").await;
    let (status, reply) = write(
        &qd,
        "POST",
        "b",
        "/batch",
        json!({"operations": [
            {"delete": {"points": [1]}},
            {"set_payload": {"payload": {"never": true}, "points": [3]}},
            {"upsert": {"points": [{"id": 9, "vector": {"img": [1.0, 2.0, 3.0]}}]}},
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(all(&qd, "b").await, before);
}

#[tokio::test]
async fn a_write_is_visible_to_the_next_read_with_wait_false() {
    let qd = Qd::start().await;
    create_single(&qd, "w", "Dot").await;
    let (status, reply) = qd
        .put(
            "/collections/w/points",
            Some(json!({"points": [{"id": 1, "vector": [1.0, 0.0]}]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"]["status"], "acknowledged");
    assert!(reply["result"]["operation_id"].is_u64(), "{reply}");
    assert_eq!(count(&qd, "w").await, 1);
}

#[tokio::test]
async fn write_responses_carry_the_consistency_token() {
    let qd = Qd::start().await;
    create_single(&qd, "t", "Dot").await;
    let response = qd
        .http
        .put(format!("{}/collections/t/points", qd.rest))
        .json(&json!({"points": [{"id": 1, "vector": [1.0, 0.0]}]}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    let token = response
        .headers()
        .get("loams-consistency-token")
        .expect("token header")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(token.starts_with("v1:"), "{token}");
    // The token is accepted back as a read's lower bound.
    let (status, reply) = qd
        .send(
            reqwest::Method::POST,
            "/collections/t/points/count",
            Some(json!({})),
            &[("loams-consistency-token", &token)],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"]["count"], 1);
    let mut points = qd.points().await;
    let response = points
        .upsert(pb::UpsertPoints {
            collection_name: "t".to_string(),
            points: vec![dense_point(2, &[0.0, 1.0])],
            ..Default::default()
        })
        .await
        .expect("upsert");
    assert!(response.metadata().get("loams-consistency-token").is_some());
    assert_eq!(
        response.get_ref().result.as_ref().expect("present").status,
        pb::UpdateStatus::Acknowledged as i32
    );
}

#[tokio::test]
async fn update_mode_insert_only_is_501() {
    let qd = Qd::start().await;
    create_single(&qd, "um", "Dot").await;
    for extra in [
        json!({"update_mode": "insert_only"}),
        json!({"update_filter": {"must": []}}),
        json!({"shard_key": "a"}),
    ] {
        let mut body = json!({"points": [{"id": 1, "vector": [1.0, 0.0]}]});
        body.as_object_mut()
            .expect("present")
            .extend(extra.as_object().expect("present").clone());
        let (status, reply) = write(&qd, "PUT", "um", "", body).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
    }
    ok(
        &qd,
        "PUT",
        "um",
        "",
        json!({"points": [{"id": 1, "vector": [1.0, 0.0]}], "update_mode": "upsert"}),
    )
    .await;
}

// ----- backpressure (E2, E4) -----

/// A gateway whose link never commits within a test and whose budget is
/// `records` unapplied records.
async fn paused(records: u64, chunk: usize) -> Qd {
    Qd::start_with(|config| {
        config.link.batch_interval = Duration::from_secs(3600);
        config.link.batch_records = 1_000_000;
        config.query.backpressure.max_unapplied_records = records;
        config.query.backpressure.refresh_interval = Duration::ZERO;
        // The native writes by filter batch by the service's size (D87);
        // the read-modify-writes by filter by the gateway's chunk.
        config.query.filter_write_batch = chunk;
        if let Some(qdrant) = config.qdrant.as_mut() {
            qdrant.filter_write_chunk = chunk;
        }
    })
    .await
}

fn retry_after(response: &reqwest::Response) -> u64 {
    response
        .headers()
        .get("retry-after")
        .expect("Retry-After")
        .to_str()
        .expect("ascii")
        .parse()
        .expect("integer seconds")
}

#[tokio::test]
async fn a_write_over_the_budget_is_429_with_retry_after() {
    let qd = paused(3, 1000).await;
    create_single(&qd, "bp", "Dot").await;
    let points: Vec<Value> = (0..3)
        .map(|i| json!({"id": i, "vector": [1.0, 0.0]}))
        .collect();
    upsert(&qd, "bp", Value::Array(points)).await;
    let response = qd
        .http
        .put(format!("{}/collections/bp/points", qd.rest))
        .json(&json!({"points": [{"id": 9, "vector": [1.0, 0.0]}]}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(retry_after(&response) >= 1);
    let body: Value = response.json().await.expect("json");
    assert!(
        error(&body).starts_with("Rate limiting exceeded: "),
        "{body}"
    );
    let mut points = qd.points().await;
    let err = points
        .upsert(pb::UpsertPoints {
            collection_name: "bp".to_string(),
            points: vec![dense_point(10, &[1.0, 0.0])],
            ..Default::default()
        })
        .await
        .expect_err("throttled");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    let secs: u64 = err
        .metadata()
        .get("retry-after")
        .expect("retry-after")
        .to_str()
        .expect("ascii")
        .parse()
        .expect("integer");
    assert!(secs >= 1);
    assert_eq!(count(&qd, "bp").await, 3);
}

#[tokio::test]
async fn a_refused_filter_write_chunk_is_429_after_the_written_chunks() {
    let qd = paused(5, 2).await;
    create_single(&qd, "fc", "Dot").await;
    seed(&qd, "fc", 4).await;
    // The native delete by filter (D87): batch 1 (2 deletes) is admitted
    // at 4 unapplied records; batch 2 is refused at 6 and retried until the
    // 1 s timeout would pass, then the answer is 429.
    let all_points = json!({"filter": {"must": [{"key": "n", "range": {"gte": 0}}]}});
    let delete = || async {
        qd.http
            .post(format!(
                "{}/collections/fc/points/delete?wait=true&timeout=1",
                qd.rest
            ))
            .json(&all_points)
            .send()
            .await
            .expect("send")
    };
    let response = delete().await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(retry_after(&response) >= 1);
    assert_eq!(count(&qd, "fc").await, 2);
    // Now the first batch is refused too: it waits out the timeout (the
    // native write retries every batch, M1.5 row T9a-9), and nothing is
    // written.
    let response = delete().await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(retry_after(&response) >= 1);
    assert_eq!(count(&qd, "fc").await, 2);
}

// ----- gRPC -----

fn num(n: u64) -> pb::PointId {
    pb::PointId {
        point_id_options: Some(pb::point_id::PointIdOptions::Num(n)),
    }
}

fn dense(data: &[f32]) -> pb::Vector {
    pb::Vector {
        vector: Some(pb::vector::Vector::Dense(pb::DenseVector {
            data: data.to_vec(),
        })),
        ..Default::default()
    }
}

fn sparse(indices: &[u32], values: &[f32]) -> pb::Vector {
    pb::Vector {
        vector: Some(pb::vector::Vector::Sparse(pb::SparseVector {
            indices: indices.to_vec(),
            values: values.to_vec(),
        })),
        ..Default::default()
    }
}

fn named<const N: usize>(vectors: [(&str, pb::Vector); N]) -> pb::Vectors {
    pb::Vectors {
        vectors_options: Some(pb::vectors::VectorsOptions::Vectors(pb::NamedVectors {
            vectors: vectors
                .into_iter()
                .map(|(n, v)| (n.to_string(), v))
                .collect(),
        })),
    }
}

fn dense_point(id: u64, data: &[f32]) -> pb::PointStruct {
    pb::PointStruct {
        id: Some(num(id)),
        payload: Default::default(),
        vectors: Some(pb::Vectors {
            vectors_options: Some(pb::vectors::VectorsOptions::Vector(dense(data))),
        }),
    }
}

fn payload(v: Value) -> std::collections::HashMap<String, pb::Value> {
    map_to_payload(v.as_object().expect("an object"))
}

fn ids_selector(ids: &[u64]) -> Option<pb::PointsSelector> {
    Some(pb::PointsSelector {
        points_selector_one_of: Some(pb::points_selector::PointsSelectorOneOf::Points(
            pb::PointsIdsList {
                ids: ids.iter().map(|&i| num(i)).collect(),
            },
        )),
    })
}

#[tokio::test]
async fn grpc_writes_match_rest() {
    let qd = Qd::start().await;
    for name in ["r", "g"] {
        create_hybrid(&qd, name).await;
    }
    // REST.
    upsert(
        &qd,
        "r",
        json!([
            {"id": 1, "vector": {"": [1.0, 2.0]}, "payload": {"a": 1, "b": 2}},
            {"id": 2, "vector": {"": [3.0, 4.0], "s": {"indices": [2, 1], "values": [0.5, 0.25]}}, "payload": {"a": 2}},
            {"id": 3, "vector": {"": [5.0, 6.0]}, "payload": {"c": {"d": 1}}},
            {"id": 4, "vector": {"": [7.0, 8.0]}},
        ]),
    )
    .await;
    ok(
        &qd,
        "POST",
        "r",
        "/payload",
        json!({"payload": {"x": true}, "points": [1]}),
    )
    .await;
    ok(
        &qd,
        "PUT",
        "r",
        "/payload",
        json!({"payload": {"o": 1}, "points": [2]}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "r",
        "/payload/delete",
        json!({"keys": ["b"], "points": [1]}),
    )
    .await;
    ok(&qd, "POST", "r", "/payload/clear", json!({"points": [3]})).await;
    ok(
        &qd,
        "PUT",
        "r",
        "/vectors",
        json!({"points": [{"id": 3, "vector": {"s": {"indices": [7], "values": [1.0]}}}]}),
    )
    .await;
    ok(
        &qd,
        "POST",
        "r",
        "/vectors/delete",
        json!({"points": [2], "vector": ["s"]}),
    )
    .await;
    ok(&qd, "POST", "r", "/delete", json!({"points": [4]})).await;
    ok(
        &qd,
        "POST",
        "r",
        "/batch",
        json!({"operations": [{"set_payload": {"payload": {"k": "v"}, "points": [3], "key": "sub"}}]}),
    )
    .await;

    // gRPC.
    let mut points = qd.points().await;
    let g = || "g".to_string();
    let wait = Some(true);
    points
        .upsert(pb::UpsertPoints {
            collection_name: g(),
            wait,
            points: vec![
                pb::PointStruct {
                    id: Some(num(1)),
                    payload: payload(json!({"a": 1, "b": 2})),
                    vectors: Some(named([("", dense(&[1.0, 2.0]))])),
                },
                pb::PointStruct {
                    id: Some(num(2)),
                    payload: payload(json!({"a": 2})),
                    vectors: Some(named([
                        ("", dense(&[3.0, 4.0])),
                        ("s", sparse(&[2, 1], &[0.5, 0.25])),
                    ])),
                },
                pb::PointStruct {
                    id: Some(num(3)),
                    payload: payload(json!({"c": {"d": 1}})),
                    vectors: Some(named([("", dense(&[5.0, 6.0]))])),
                },
                dense_point(4, &[7.0, 8.0]),
            ],
            ..Default::default()
        })
        .await
        .expect("upsert");
    points
        .set_payload(pb::SetPayloadPoints {
            collection_name: g(),
            wait,
            payload: payload(json!({"x": true})),
            points_selector: ids_selector(&[1]),
            ..Default::default()
        })
        .await
        .expect("set");
    points
        .overwrite_payload(pb::SetPayloadPoints {
            collection_name: g(),
            wait,
            payload: payload(json!({"o": 1})),
            points_selector: ids_selector(&[2]),
            ..Default::default()
        })
        .await
        .expect("overwrite");
    points
        .delete_payload(pb::DeletePayloadPoints {
            collection_name: g(),
            wait,
            keys: vec!["b".to_string()],
            points_selector: ids_selector(&[1]),
            ..Default::default()
        })
        .await
        .expect("delete payload");
    points
        .clear_payload(pb::ClearPayloadPoints {
            collection_name: g(),
            wait,
            points: ids_selector(&[3]),
            ..Default::default()
        })
        .await
        .expect("clear");
    points
        .update_vectors(pb::UpdatePointVectors {
            collection_name: g(),
            wait,
            points: vec![pb::PointVectors {
                id: Some(num(3)),
                vectors: Some(named([("s", sparse(&[7], &[1.0]))])),
            }],
            ..Default::default()
        })
        .await
        .expect("update vectors");
    points
        .delete_vectors(pb::DeletePointVectors {
            collection_name: g(),
            wait,
            points_selector: ids_selector(&[2]),
            vectors: Some(pb::VectorsSelector {
                names: vec!["s".to_string()],
            }),
            ..Default::default()
        })
        .await
        .expect("delete vectors");
    let deleted = points
        .delete(pb::DeletePoints {
            collection_name: g(),
            wait,
            points: ids_selector(&[4]),
            ..Default::default()
        })
        .await
        .expect("delete");
    assert_eq!(
        deleted.get_ref().result.as_ref().expect("present").status,
        pb::UpdateStatus::Completed as i32
    );
    let batch = points
        .update_batch(pb::UpdateBatchPoints {
            collection_name: g(),
            wait,
            operations: vec![pb::PointsUpdateOperation {
                operation: Some(pb::points_update_operation::Operation::SetPayload(
                    pb::points_update_operation::SetPayload {
                        payload: payload(json!({"k": "v"})),
                        points_selector: ids_selector(&[3]),
                        key: Some("sub".to_string()),
                        ..Default::default()
                    },
                )),
            }],
            ..Default::default()
        })
        .await
        .expect("batch");
    assert_eq!(batch.get_ref().result.len(), 1);
    // A missing point over gRPC.
    let err = points
        .set_payload(pb::SetPayloadPoints {
            collection_name: g(),
            wait,
            payload: payload(json!({"x": 1})),
            points_selector: ids_selector(&[999]),
            ..Default::default()
        })
        .await
        .expect_err("missing");
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert_eq!(err.message(), "Not found: No point with id 999 found");

    let rest = all(&qd, "r").await;
    assert_eq!(rest.len(), 3);
    assert_eq!(rest, all(&qd, "g").await);
    assert_eq!(rest[2]["payload"], json!({"sub": {"k": "v"}}));
}
