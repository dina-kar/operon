//! Grouped queries end to end (plan M1.4 Task 9, Ruling 11): Qdrant's
//! collect-then-fill driver, group keys, ordering, lookups, the legacy
//! group routes and gRPC.

// The legacy gRPC methods are deprecated in Qdrant's protos.
#![allow(deprecated)]

use loams_collection::{Distance, PrimaryKey, SparseVector};
use loams_qdrant::proto::qdrant as pb;
use loams_qdrant::scoring::sparse_reference;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;
use crate::query::{
    Rng, assert_hits, brute, create, dense_input, error, grpc_hits, hits, random_collection,
    sparse_collection, upsert,
};

// ----- helpers -----

/// A groups route's `groups`, which must succeed.
async fn groups(qd: &Qd, coll: &str, route: &str, body: Value) -> Vec<Value> {
    let (status, reply) = groups_raw(qd, coll, route, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{route} {body} → {reply}");
    reply["result"]["groups"]
        .as_array()
        .unwrap_or_else(|| panic!("no groups: {reply}"))
        .clone()
}

async fn groups_raw(qd: &Qd, coll: &str, route: &str, body: Value) -> (StatusCode, Value) {
    qd.post(&format!("/collections/{coll}/points/{route}"), Some(body))
        .await
}

/// `(id, [(point, score)])` of each group.
fn shape(groups: &[Value]) -> Vec<(Value, Vec<(u64, f32)>)> {
    groups
        .iter()
        .map(|g| (g["id"].clone(), hits(&g["hits"])))
        .collect()
}

/// Groups `ranked` (best first) by `keys` of each point, in order of each
/// group's best hit: the first `limit` groups, `size` hits each.
fn brute_groups(
    ranked: &[(u64, f32)],
    keys: impl Fn(u64) -> Vec<Value>,
    limit: usize,
    size: usize,
) -> Vec<(Value, Vec<(u64, f32)>)> {
    let mut out: Vec<(Value, Vec<(u64, f32)>)> = Vec::new();
    for &(id, score) in ranked {
        let mut seen = Vec::new();
        for key in keys(id) {
            if seen.contains(&key) {
                continue;
            }
            seen.push(key.clone());
            match out.iter_mut().find(|(k, _)| *k == key) {
                Some((_, hits)) => hits.push((id, score)),
                None => out.push((key, vec![(id, score)])),
            }
        }
    }
    out.truncate(limit);
    for (_, hits) in &mut out {
        hits.truncate(size);
    }
    out
}

fn assert_groups(got: &[(Value, Vec<(u64, f32)>)], want: &[(Value, Vec<(u64, f32)>)]) {
    let keys =
        |g: &[(Value, Vec<(u64, f32)>)]| g.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    assert_eq!(keys(got), keys(want), "got {got:?}\nwant {want:?}");
    for ((_, a), (_, b)) in got.iter().zip(want) {
        assert_hits(a, b, 1e-5);
    }
}

/// A point with vector `[s, 0]`, whose Dot score for `[1, 0]` is `s`.
fn scored_point(id: u64, s: f32, payload: Value) -> Value {
    json!({"id": id, "vector": [s, 0.0], "payload": payload})
}

// ----- the driver -----

/// Key of point `id`: 60 values, even ones integers, odd ones strings.
fn doc_key(id: u64) -> Value {
    let v = (id * 7) % 60;
    if v.is_multiple_of(2) {
        json!(v)
    } else {
        json!(format!("k{v}"))
    }
}

#[tokio::test]
async fn groups_match_a_brute_force_grouping() {
    let qd = Qd::start().await;
    create(
        &qd,
        "g",
        json!({"vectors": {"size": 4, "distance": "Cosine"}}),
    )
    .await;
    let mut rng = Rng(301);
    let points: Vec<(u64, Vec<f32>)> = (0..400).map(|id| (id, rng.vector(4))).collect();
    upsert(
        &qd,
        "g",
        points
            .iter()
            .map(
                |(id, v)| json!({"id": id, "vector": v, "payload": {"doc": doc_key(*id), "n": id}}),
            )
            .collect(),
    )
    .await;
    for q in [vec![0.3, -0.2, 0.8, 0.1], vec![-0.5, 0.5, 0.1, 0.9]] {
        let ranked = brute(Distance::Cosine, &q, &points, 400);
        let want = brute_groups(&ranked, |id| vec![doc_key(id)], 5, 3);
        let got = groups(
            &qd,
            "g",
            "query/groups",
            json!({"query": q, "group_by": "doc", "limit": 5, "group_size": 3}),
        )
        .await;
        assert_groups(&shape(&got), &want);
        // Hits carry no payload by default; with it, the request's selector.
        assert!(got[0]["hits"][0].get("payload").is_none());
        let got = groups(
            &qd,
            "g",
            "query/groups",
            json!({"query": q, "group_by": "doc", "limit": 5, "group_size": 3,
                   "with_payload": ["n"], "filter": {"must_not": [{"key": "n", "range": {"lt": 100}}]}}),
        )
        .await;
        let filtered: Vec<(u64, f32)> = ranked
            .iter()
            .copied()
            .filter(|(id, _)| *id >= 100)
            .collect();
        assert_groups(
            &shape(&got),
            &brute_groups(&filtered, |id| vec![doc_key(id)], 5, 3),
        );
        let first = &got[0]["hits"][0];
        assert_eq!(first["payload"], json!({"n": first["id"]}));
    }
    // Defaults: 10 groups of 3.
    let got = groups(
        &qd,
        "g",
        "query/groups",
        json!({"query": [1.0, 0.0, 0.0, 0.0], "group_by": "doc"}),
    )
    .await;
    assert_eq!(got.len(), 10);
    assert!(
        got.iter()
            .all(|g| g["hits"].as_array().expect("hits").len() == 3)
    );
}

#[tokio::test]
async fn array_values_join_several_groups() {
    let qd = Qd::start().await;
    create(&qd, "a", json!({"vectors": {"size": 2, "distance": "Dot"}})).await;
    upsert(
        &qd,
        "a",
        vec![
            scored_point(1, 1.0, json!({"doc": ["a", "b"]})),
            scored_point(2, 0.9, json!({"doc": "a"})),
            scored_point(3, 0.8, json!({"doc": "b"})),
            scored_point(4, 0.7, json!({"doc": ["b", "b"]})),
            scored_point(5, 0.6, json!({"doc": ["c", ["d"]]})),
        ],
    )
    .await;
    let got = groups(
        &qd,
        "a",
        "query/groups",
        json!({"query": [1.0, 0.0], "group_by": "doc", "limit": 5, "group_size": 3}),
    )
    .await;
    // Point 1 leads both `a` and `b`; equal best hits are ordered by key.
    // A nested array is no key, so point 5 joins no group.
    assert_groups(
        &shape(&got),
        &[
            (json!("a"), vec![(1, 1.0), (2, 0.9)]),
            (json!("b"), vec![(1, 1.0), (3, 0.8), (4, 0.7)]),
        ],
    );
    // `doc[]` reads array elements only (Qdrant's `value_get`): the plain
    // strings of points 2 and 3 are no key there, and point 5's nested
    // array gives `c` and `d`.
    let again = groups(
        &qd,
        "a",
        "query/groups",
        json!({"query": [1.0, 0.0], "group_by": "doc[]", "limit": 5, "group_size": 3}),
    )
    .await;
    assert_groups(
        &shape(&again),
        &[
            (json!("a"), vec![(1, 1.0)]),
            (json!("b"), vec![(1, 1.0), (4, 0.7)]),
            (json!("c"), vec![(5, 0.6)]),
            (json!("d"), vec![(5, 0.6)]),
        ],
    );
}

#[tokio::test]
async fn non_string_non_integer_keys_are_ignored() {
    let qd = Qd::start().await;
    create(&qd, "k", json!({"vectors": {"size": 2, "distance": "Dot"}})).await;
    upsert(
        &qd,
        "k",
        vec![
            scored_point(1, 10.0, json!({"doc": true})),
            scored_point(2, 9.0, json!({"doc": 1.5})),
            scored_point(3, 8.0, json!({"doc": {"x": 1}})),
            scored_point(4, 7.0, json!({"doc": null})),
            // One value that is no key voids the point (Qdrant's aggregator).
            scored_point(5, 6.0, json!({"doc": ["a", true]})),
            scored_point(6, 5.0, json!({"doc": "a"})),
            scored_point(7, 4.0, json!({"doc": 7})),
            scored_point(8, 3.0, json!({"other": "a"})),
            scored_point(9, 2.0, json!({"doc": []})),
            scored_point(10, 1.0, json!({"doc": 2.0})),
            scored_point(11, 0.5, json!({"doc": -3})),
        ],
    )
    .await;
    let got = groups(
        &qd,
        "k",
        "query/groups",
        json!({"query": [1.0, 0.0], "group_by": "doc", "limit": 10, "group_size": 3}),
    )
    .await;
    assert_groups(
        &shape(&got),
        &[
            (json!("a"), vec![(6, 5.0)]),
            (json!(7), vec![(7, 4.0)]),
            (json!(-3), vec![(11, 0.5)]),
        ],
    );
}

#[tokio::test]
async fn groups_are_ordered_by_best_hit_in_distance_order() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "e", Distance::Euclid, 3, 90, 302).await;
    let q = vec![0.2, 0.4, -0.1];
    let ranked = brute(Distance::Euclid, &q, &points, 90);
    let want = brute_groups(&ranked, |id| vec![json!(id % 3)], 3, 4);
    let got = shape(
        &groups(
            &qd,
            "e",
            "query/groups",
            json!({"query": q, "group_by": "g", "limit": 3, "group_size": 4}),
        )
        .await,
    );
    assert_groups(&got, &want);
    // Distances ascend within and across the groups' best hits.
    let best: Vec<f32> = got.iter().map(|(_, h)| h[0].1).collect();
    assert!(best.windows(2).all(|w| w[0] <= w[1]), "{best:?}");
    assert!(
        got.iter()
            .all(|(_, h)| h.windows(2).all(|w| w[0].1 <= w[1].1))
    );
}

#[tokio::test]
async fn sparse_groups_fill_in_the_fill_phase() {
    // Group `A` has one top hit and two members far down the ranking; the
    // five collect requests only find other groups, so `A` reaches its
    // size in the fill phase.
    let qd = Qd::start().await;
    create(&qd, "f", json!({"vectors": {"size": 2, "distance": "Dot"}})).await;
    let mut points = vec![
        scored_point(1, 100.0, json!({"doc": "A"})),
        scored_point(2, 1.0, json!({"doc": "A"})),
        scored_point(3, 2.0, json!({"doc": "A"})),
    ];
    for (g, name) in ["B", "C", "D", "E", "F", "G", "H"].into_iter().enumerate() {
        for i in 0..6u64 {
            let id = 10 * (g as u64 + 1) + i;
            let s = 50.0 - 6.0 * g as f32 - i as f32;
            points.push(scored_point(id, s, json!({"doc": name})));
        }
    }
    upsert(&qd, "f", points).await;
    let got = groups(
        &qd,
        "f",
        "query/groups",
        json!({"query": [1.0, 0.0], "group_by": "doc", "limit": 2, "group_size": 3}),
    )
    .await;
    assert_groups(
        &shape(&got),
        &[
            (json!("A"), vec![(1, 100.0), (3, 2.0), (2, 1.0)]),
            (json!("B"), vec![(10, 50.0), (11, 49.0), (12, 48.0)]),
        ],
    );
}

#[tokio::test]
async fn groups_over_a_sparse_query_match_a_brute_force_grouping() {
    let qd = Qd::start().await;
    let stored = sparse_collection(&qd, "sp").await;
    let docs: Vec<(PrimaryKey, SparseVector)> = stored["t"].clone();
    for (indices, values) in [
        (vec![3u32, 7, 11], vec![1.0f32, 0.5, 2.0]),
        (vec![1, 2, 4, 8, 16], vec![0.2, 0.4, 0.6, 0.8, 1.0]),
    ] {
        let query = SparseVector::new(indices.clone(), values.clone()).expect("sparse");
        let ranked: Vec<(u64, f32)> = sparse_reference(&docs, &query, false, 300)
            .into_iter()
            .map(|(pk, s)| match pk {
                PrimaryKey::U64(n) => (n, s),
                other => panic!("{other:?}"),
            })
            .collect();
        let want = brute_groups(&ranked, |id| vec![json!(id % 3)], 3, 4);
        let got = groups(
            &qd,
            "sp",
            "query/groups",
            json!({"query": {"indices": indices, "values": values}, "using": "t",
                   "group_by": "g", "limit": 3, "group_size": 4}),
        )
        .await;
        assert_groups(&shape(&got), &want);
    }
}

#[tokio::test]
async fn group_requests_are_validated() {
    let qd = Qd::start().await;
    random_collection(&qd, "v", Distance::Dot, 2, 10, 303).await;
    for (body, message) in [
        (
            json!({"query": [1.0, 0.0], "group_by": "g", "group_size": 0}),
            "Wrong input: group_size must be at least 1",
        ),
        (
            json!({"query": [1.0, 0.0], "group_by": "g", "limit": 0}),
            "Wrong input: limit must be at least 1",
        ),
        (
            json!({"query": [1.0, 0.0], "group_by": "a..b"}),
            "Format error in JSON body: Invalid json path: 'a..b'",
        ),
    ] {
        let (status, reply) = groups_raw(&qd, "v", "query/groups", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} → {reply}");
        assert_eq!(error(&reply), message, "{body}");
    }
    let (status, reply) = groups_raw(&qd, "v", "query/groups", json!({"query": [1.0, 0.0]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert!(
        error(&reply).contains("missing field `group_by`"),
        "{reply}"
    );
}

// ----- lookups -----

const UUID: &str = "0b9c6a1e-9d3c-4f2e-8a47-1c2d3e4f5a6b";

async fn books_and_authors(qd: &Qd) {
    create(
        qd,
        "books",
        json!({"vectors": {"size": 2, "distance": "Dot"}}),
    )
    .await;
    upsert(
        qd,
        "books",
        vec![
            scored_point(1, 5.0, json!({"author": 1})),
            scored_point(2, 4.0, json!({"author": UUID})),
            scored_point(3, 3.0, json!({"author": "nobody"})),
            scored_point(4, 2.0, json!({"author": 99})),
            scored_point(5, 1.0, json!({"author": 1})),
        ],
    )
    .await;
    create(
        qd,
        "authors",
        json!({"vectors": {"size": 2, "distance": "Dot"}}),
    )
    .await;
    upsert(
        qd,
        "authors",
        vec![
            json!({"id": 1, "vector": [0.5, 0.5], "payload": {"name": "Ann"}}),
            json!({"id": UUID, "vector": [1.0, 2.0], "payload": {"name": "Bo"}}),
        ],
    )
    .await;
}

#[tokio::test]
async fn with_lookup_attaches_points() {
    let qd = Qd::start().await;
    books_and_authors(&qd).await;
    let body = |lookup: Value| json!({"query": [1.0, 0.0], "group_by": "author", "limit": 10, "group_size": 2, "with_lookup": lookup});
    // The string form: payload, no vectors.
    let got = groups(&qd, "books", "query/groups", body(json!("authors"))).await;
    assert_eq!(
        got.iter().map(|g| g["id"].clone()).collect::<Vec<_>>(),
        [json!(1), json!(UUID), json!("nobody"), json!(99)]
    );
    assert_eq!(
        got[0]["lookup"],
        json!({"id": 1, "payload": {"name": "Ann"}})
    );
    assert_eq!(
        got[1]["lookup"],
        json!({"id": UUID, "payload": {"name": "Bo"}})
    );
    // A key that is no point id, or a point that is not there: no lookup.
    assert!(got[2].get("lookup").is_none() && got[3].get("lookup").is_none());
    assert_eq!(hits(&got[0]["hits"]), [(1, 5.0), (5, 1.0)]);
    // The object form with its own selectors.
    let got = groups(
        &qd,
        "books",
        "query/groups",
        body(json!({"collection": "authors", "with_payload": false, "with_vectors": true})),
    )
    .await;
    assert_eq!(got[0]["lookup"], json!({"id": 1, "vector": [0.5, 0.5]}));
    let got = groups(
        &qd,
        "books",
        "query/groups",
        body(json!({"collection": "authors"})),
    )
    .await;
    assert_eq!(
        got[1]["lookup"],
        json!({"id": UUID, "payload": {"name": "Bo"}})
    );
    // A missing lookup collection is 404.
    let (status, _) = groups_raw(&qd, "books", "query/groups", body(json!("nope"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ----- legacy routes and gRPC -----

#[tokio::test]
async fn legacy_search_groups_and_recommend_groups_work() {
    let qd = Qd::start().await;
    random_collection(&qd, "l", Distance::Cosine, 3, 90, 304).await;
    let q = vec![0.1, 0.7, -0.3];
    let want = groups(
        &qd,
        "l",
        "query/groups",
        json!({"query": q, "group_by": "g", "group_size": 2, "limit": 3, "with_payload": true}),
    )
    .await;
    let got = groups(
        &qd,
        "l",
        "search/groups",
        json!({"vector": q, "group_by": "g", "group_size": 2, "limit": 3, "with_payload": true}),
    )
    .await;
    assert_eq!(got, want);
    let want = groups(
        &qd,
        "l",
        "query/groups",
        json!({"query": {"recommend": {"positive": [1, 2], "negative": [3], "strategy": "best_score"}},
               "group_by": "g", "group_size": 3, "limit": 2}),
    )
    .await;
    let got = groups(
        &qd,
        "l",
        "recommend/groups",
        json!({"positive": [1, 2], "negative": [3], "strategy": "best_score", "group_by": "g", "group_size": 3, "limit": 2}),
    )
    .await;
    assert_eq!(got, want);
    assert!(got.iter().all(|g| {
        hits(&g["hits"])
            .iter()
            .all(|(id, _)| ![1, 2, 3].contains(id))
    }));
    // The legacy group routes need `group_size` and `limit`.
    let (status, reply) = groups_raw(
        &qd,
        "l",
        "search/groups",
        json!({"vector": q, "group_by": "g", "limit": 3}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
}

fn group_id(g: &pb::PointGroup) -> Value {
    match g.id.as_ref().and_then(|id| id.kind.as_ref()) {
        Some(pb::group_id::Kind::UnsignedValue(n)) => json!(n),
        Some(pb::group_id::Kind::IntegerValue(n)) => json!(n),
        Some(pb::group_id::Kind::StringValue(s)) => json!(s),
        None => panic!("no group id"),
    }
}

fn grpc_shape(result: Option<pb::GroupsResult>) -> Vec<(Value, Vec<(u64, f32)>)> {
    result
        .expect("groups")
        .groups
        .iter()
        .map(|g| (group_id(g), grpc_hits(&g.hits)))
        .collect()
}

#[tokio::test]
async fn grpc_groups_match_rest() {
    let qd = Qd::start().await;
    books_and_authors(&qd).await;
    let mut client = qd.points().await;
    let rest = groups(
        &qd,
        "books",
        "query/groups",
        json!({"query": [1.0, 0.0], "group_by": "author", "limit": 10, "group_size": 2, "with_lookup": "authors"}),
    )
    .await;
    let lookup = Some(pb::WithLookup {
        collection: "authors".into(),
        with_payload: None,
        with_vectors: None,
    });
    let reply = client
        .query_groups(pb::QueryPointGroups {
            collection_name: "books".into(),
            query: Some(pb::Query {
                variant: Some(pb::query::Variant::Nearest(dense_input(&[1.0, 0.0]))),
            }),
            group_by: "author".into(),
            limit: Some(10),
            group_size: Some(2),
            with_lookup: lookup.clone(),
            ..Default::default()
        })
        .await
        .expect("QueryGroups")
        .into_inner();
    let result = reply.result.expect("result");
    assert_eq!(grpc_shape(Some(result.clone())), shape(&rest));
    // The lookup carries the payload, as over REST.
    let first = result.groups[0].lookup.as_ref().expect("lookup");
    assert_eq!(
        first.payload["name"].kind,
        Some(pb::value::Kind::StringValue("Ann".into()))
    );
    assert!(result.groups[2].lookup.is_none());
    let reply = client
        .search_groups(pb::SearchPointGroups {
            collection_name: "books".into(),
            vector: vec![1.0, 0.0],
            group_by: "author".into(),
            limit: 10,
            group_size: 2,
            with_lookup: lookup,
            ..Default::default()
        })
        .await
        .expect("SearchGroups")
        .into_inner();
    assert_eq!(grpc_shape(reply.result), shape(&rest));
    let rest = groups(
        &qd,
        "books",
        "recommend/groups",
        json!({"positive": [1], "strategy": "sum_scores", "group_by": "author", "group_size": 1, "limit": 3}),
    )
    .await;
    let reply = client
        .recommend_groups(pb::RecommendPointGroups {
            collection_name: "books".into(),
            positive: vec![pb::PointId {
                point_id_options: Some(pb::point_id::PointIdOptions::Num(1)),
            }],
            strategy: Some(pb::RecommendStrategy::SumScores as i32),
            group_by: "author".into(),
            limit: 3,
            group_size: 1,
            ..Default::default()
        })
        .await
        .expect("RecommendGroups")
        .into_inner();
    assert_eq!(grpc_shape(reply.result), shape(&rest));
    let err = client
        .query_groups(pb::QueryPointGroups {
            collection_name: "books".into(),
            group_by: "author".into(),
            group_size: Some(0),
            ..Default::default()
        })
        .await
        .expect_err("group_size 0");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
