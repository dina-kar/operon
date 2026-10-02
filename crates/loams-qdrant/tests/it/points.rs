//! The write-side vector checks (plan M1.4 Task 5, Rulings 8 and 21) and
//! the point request model (Tasks 5 and 6).

use loams_qdrant::GatewayError;
use loams_qdrant::model::collections::CreateCollection;
use loams_qdrant::model::points::{
    PointInsert, PointsSelector, ScrollRequest, UpdateOperation, UpdateOperations, VectorStruct,
};
use loams_qdrant::schema::schema_from_create;
use loams_qdrant::scoring::{check_sparse, check_vector, cosine_normalize};
use serde_json::{Value, json};

fn schema(body: Value) -> loams_collection::CollectionSchema {
    let req: CreateCollection = serde_json::from_value(body.clone()).expect("parses");
    schema_from_create("c", &req, &body).expect("schema")
}

fn bad(err: GatewayError) -> String {
    match err {
        GatewayError::BadRequest(m) => m,
        other => panic!("not a BadRequest: {other:?}"),
    }
}

#[test]
fn cosine_normalize_follows_qdrant_preprocess() {
    let mut v = [3.0_f32, 4.0];
    cosine_normalize(&mut v);
    assert!((v[0] - 0.6).abs() < 1e-6 && (v[1] - 0.8).abs() < 1e-6);
    // Zero and already-unit vectors are left as they are.
    let mut zero = [0.0_f32, 0.0];
    cosine_normalize(&mut zero);
    assert_eq!(zero, [0.0, 0.0]);
    let mut unit = [1.0_f32, 0.0];
    cosine_normalize(&mut unit);
    assert_eq!(unit, [1.0, 0.0]);
}

#[test]
fn check_vector_checks_name_dimension_and_finiteness() {
    let s = schema(json!({
        "vectors": {"c": {"size": 2, "distance": "Cosine"}, "e": {"size": 2, "distance": "Euclid"}},
        "sparse_vectors": {"s": {}}
    }));
    let mut v = vec![3.0, 4.0];
    check_vector(&s, "c", &mut v).expect("ok");
    assert!((v[0] - 0.6).abs() < 1e-6);
    let mut v = vec![3.0, 4.0];
    check_vector(&s, "e", &mut v).expect("ok");
    assert_eq!(v, [3.0, 4.0]);
    let cases = [
        (
            "c",
            vec![1.0],
            "Vector dimension error: expected dim: 2, got 1",
        ),
        (
            "c",
            vec![f32::NAN, 1.0],
            "Vector contains NaN or infinite values",
        ),
        ("x", vec![1.0, 1.0], "Not existing vector name error: x"),
        ("s", vec![1.0, 1.0], "Vector s is a sparse vector"),
    ];
    for (name, mut v, text) in cases {
        assert_eq!(bad(check_vector(&s, name, &mut v).unwrap_err()), text);
    }
}

#[test]
fn check_sparse_sorts_and_refuses_bad_values() {
    let s = schema(
        json!({"vectors": {"d": {"size": 2, "distance": "Dot"}}, "sparse_vectors": {"s": {}}}),
    );
    let v = check_sparse(&s, "s", vec![9, 2], vec![0.0, 1.5]).expect("ok");
    assert_eq!(v.indices(), [2, 9]);
    assert_eq!(v.values(), [1.5, 0.0]);
    let cases = [
        (
            "s",
            vec![1, 1],
            vec![1.0, 1.0],
            "Sparse vector s: index 1 appears more than once",
        ),
        ("d", vec![1], vec![1.0], "Vector d is a dense vector"),
        ("x", vec![1], vec![1.0], "Not existing vector name error: x"),
    ];
    for (name, i, v, text) in cases {
        assert_eq!(bad(check_sparse(&s, name, i, v).unwrap_err()), text);
    }
}

#[test]
fn point_insert_reads_both_forms() {
    let list: PointInsert =
        serde_json::from_value(json!({"points": [{"id": 1, "vector": [1.0]}]})).expect("list");
    assert!(matches!(list, PointInsert::List { ref points, .. } if points.len() == 1));
    let batch: PointInsert =
        serde_json::from_value(json!({"batch": {"ids": [1], "vectors": [[1.0]]}})).expect("batch");
    assert!(matches!(batch, PointInsert::Batch { .. }));
    assert!(serde_json::from_value::<PointInsert>(json!({})).is_err());
    let multi: VectorStruct = serde_json::from_value(json!([[1.0], [2.0]])).expect("multi");
    assert!(matches!(multi, VectorStruct::Multi(_)));
}

#[test]
fn selectors_keep_the_filter_error_text() {
    let ids: PointsSelector = serde_json::from_value(json!({"points": [1, 2]})).expect("ids");
    assert!(matches!(ids, PointsSelector::Ids { .. }));
    let err = serde_json::from_value::<PointsSelector>(json!({"filter": {"musst": []}}))
        .expect_err("typo");
    assert!(err.to_string().contains("unknown field `musst`"), "{err}");
    let err = serde_json::from_value::<UpdateOperations>(
        json!({"operations": [{"delete": {"filter": {"musst": []}}}]}),
    )
    .expect_err("typo");
    assert!(err.to_string().contains("unknown field `musst`"), "{err}");
}

#[test]
fn update_operations_read_by_key() {
    let ops: UpdateOperations = serde_json::from_value(json!({"operations": [
        {"upsert": {"points": []}},
        {"delete": {"points": [1]}},
        {"set_payload": {"payload": {}, "points": [1]}},
        {"overwrite_payload": {"payload": {}, "points": [1]}},
        {"delete_payload": {"keys": ["a"], "points": [1]}},
        {"clear_payload": {"points": [1]}},
        {"update_vectors": {"points": []}},
        {"delete_vectors": {"points": [1], "vector": [""]}},
    ]}))
    .expect("operations");
    assert!(matches!(
        ops.operations.as_slice(),
        [
            UpdateOperation::Upsert { .. },
            UpdateOperation::Delete { .. },
            UpdateOperation::SetPayload { .. },
            UpdateOperation::OverwritePayload { .. },
            UpdateOperation::DeletePayload { .. },
            UpdateOperation::ClearPayload { .. },
            UpdateOperation::UpdateVectors { .. },
            UpdateOperation::DeleteVectors { .. },
        ]
    ));
    assert!(serde_json::from_value::<UpdateOperation>(json!({"nope": {}})).is_err());
}

#[test]
fn scroll_request_accepts_with_vectors_alias() {
    let r: ScrollRequest =
        serde_json::from_value(json!({"with_vectors": true, "limit": 3})).expect("scroll");
    assert!(r.with_vector.is_some());
    assert_eq!(r.limit, Some(3));
}
