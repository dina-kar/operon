//! The universal query end to end (plan M1.4 Task 7): nearest per
//! distance, thresholds, offsets, ids and `lookup_from`, prefetch, fusion
//! (E3), rescore, sparse nearest and IDF, batches, and gRPC.

use std::collections::BTreeMap;

use loams_collection::{Distance, PrimaryKey, SparseVector};
use loams_qdrant::proto::qdrant as pb;
use loams_qdrant::scoring::{cosine_normalize, dbsf, rrf, sparse_reference};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;

// ----- helpers -----

/// A small deterministic generator (xorshift64*).
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in [-1, 1).
    pub(crate) fn f(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    pub(crate) fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.f()).collect()
    }
}

pub(crate) fn error(body: &Value) -> &str {
    body["status"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no status.error: {body}"))
}

pub(crate) async fn create(qd: &Qd, name: &str, body: Value) {
    let (status, reply) = qd.put(&format!("/collections/{name}"), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
}

pub(crate) async fn upsert(qd: &Qd, name: &str, points: Vec<Value>) {
    for chunk in points.chunks(100) {
        let (status, reply) = qd
            .put(
                &format!("/collections/{name}/points?wait=true"),
                Some(json!({ "points": chunk })),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
    }
}

pub(crate) async fn query_raw(qd: &Qd, name: &str, body: Value) -> (StatusCode, Value) {
    qd.post(&format!("/collections/{name}/points/query"), Some(body))
        .await
}

/// The `points` of a query that must succeed.
pub(crate) async fn query(qd: &Qd, name: &str, body: Value) -> Value {
    let (status, reply) = query_raw(qd, name, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body} → {reply}");
    reply["result"]["points"].clone()
}

/// `(id, score)` of each point.
pub(crate) fn hits(points: &Value) -> Vec<(u64, f32)> {
    points
        .as_array()
        .unwrap_or_else(|| panic!("not a list: {points}"))
        .iter()
        .map(|p| {
            (
                p["id"].as_u64().expect("numeric id"),
                p["score"].as_f64().expect("score") as f32,
            )
        })
        .collect()
}

pub(crate) fn ids(hits: &[(u64, f32)]) -> Vec<u64> {
    hits.iter().map(|(id, _)| *id).collect()
}

pub(crate) fn assert_hits(got: &[(u64, f32)], want: &[(u64, f32)], tolerance: f32) {
    assert_eq!(ids(got), ids(want), "got {got:?}\nwant {want:?}");
    for ((_, a), (_, b)) in got.iter().zip(want) {
        assert!(
            (a - b).abs() <= tolerance * b.abs().max(1.0),
            "got {got:?}\nwant {want:?}"
        );
    }
}

fn pk_hits(list: &[(PrimaryKey, f32)]) -> Vec<(u64, f32)> {
    list.iter()
        .map(|(pk, s)| match pk {
            PrimaryKey::U64(n) => (*n, *s),
            other => panic!("{other:?}"),
        })
        .collect()
}

fn pks(hits: &[(u64, f32)]) -> Vec<PrimaryKey> {
    hits.iter().map(|(id, _)| PrimaryKey::U64(*id)).collect()
}

/// Qdrant's score of `v` for `q` (vectors as stored: normalized for
/// Cosine).
pub(crate) fn qdrant_score(distance: Distance, q: &[f32], v: &[f32]) -> f32 {
    let pairs = q.iter().zip(v);
    match distance {
        Distance::Cosine | Distance::Dot => pairs.map(|(a, b)| a * b).sum(),
        Distance::Euclid => pairs.map(|(a, b)| (a - b) * (a - b)).sum::<f32>().sqrt(),
        Distance::Manhattan => pairs.map(|(a, b)| (a - b).abs()).sum(),
    }
}

pub(crate) fn larger_is_better(distance: Distance) -> bool {
    matches!(distance, Distance::Cosine | Distance::Dot)
}

/// The brute-force top `limit` over `points`, Qdrant-scored and ordered.
pub(crate) fn brute(
    distance: Distance,
    q: &[f32],
    points: &[(u64, Vec<f32>)],
    limit: usize,
) -> Vec<(u64, f32)> {
    let mut q = q.to_vec();
    if distance == Distance::Cosine {
        cosine_normalize(&mut q);
    }
    let mut scored: Vec<(u64, f32)> = points
        .iter()
        .map(|(id, v)| {
            let mut v = v.clone();
            if distance == Distance::Cosine {
                cosine_normalize(&mut v);
            }
            (*id, qdrant_score(distance, &q, &v))
        })
        .collect();
    scored.sort_by(|a, b| {
        let by = if larger_is_better(distance) {
            b.1.total_cmp(&a.1)
        } else {
            a.1.total_cmp(&b.1)
        };
        by.then(a.0.cmp(&b.0))
    });
    scored.truncate(limit);
    scored
}

pub(crate) fn name(distance: Distance) -> &'static str {
    match distance {
        Distance::Cosine => "Cosine",
        Distance::Dot => "Dot",
        Distance::Euclid => "Euclid",
        Distance::Manhattan => "Manhattan",
    }
}

/// A single-vector collection of `n` random points of `dim`; the points.
pub(crate) async fn random_collection(
    qd: &Qd,
    coll: &str,
    distance: Distance,
    dim: usize,
    n: u64,
    seed: u64,
) -> Vec<(u64, Vec<f32>)> {
    create(
        qd,
        coll,
        json!({"vectors": {"size": dim, "distance": name(distance)}}),
    )
    .await;
    let mut rng = Rng(seed);
    let points: Vec<(u64, Vec<f32>)> = (0..n).map(|id| (id, rng.vector(dim))).collect();
    upsert(
        qd,
        coll,
        points
            .iter()
            .map(|(id, v)| json!({"id": id, "vector": v, "payload": {"g": id % 3}}))
            .collect(),
    )
    .await;
    points
}

// ----- nearest per distance -----

#[tokio::test]
async fn nearest_query_matches_qdrant_semantics() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "c", Distance::Cosine, 8, 200, 7).await;
    let mut rng = Rng(99);
    for _ in 0..3 {
        let q = rng.vector(8);
        let got = hits(
            &query(
                &qd,
                "c",
                json!({"query": q, "limit": 10, "params": {"exact": true}}),
            )
            .await,
        );
        assert_hits(&got, &brute(Distance::Cosine, &q, &points, 10), 1e-5);
    }
}

#[tokio::test]
async fn euclid_query_returns_distances_ascending() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "e", Distance::Euclid, 4, 60, 11).await;
    let q = Rng(5).vector(4);
    let got = hits(&query(&qd, "e", json!({"query": q, "limit": 10})).await);
    assert_hits(&got, &brute(Distance::Euclid, &q, &points, 10), 1e-5);
    assert!(got.windows(2).all(|w| w[0].1 <= w[1].1), "{got:?}");
}

#[tokio::test]
async fn manhattan_query_returns_l1_distances() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "m", Distance::Manhattan, 4, 60, 12).await;
    let q = Rng(6).vector(4);
    let got = hits(&query(&qd, "m", json!({"query": {"nearest": q}, "limit": 7})).await);
    assert_hits(&got, &brute(Distance::Manhattan, &q, &points, 7), 1e-5);
}

#[tokio::test]
async fn dot_query_returns_dot_products() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "d", Distance::Dot, 4, 60, 13).await;
    let q = Rng(8).vector(4);
    let got = hits(&query(&qd, "d", json!({"query": q, "limit": 7})).await);
    assert_hits(&got, &brute(Distance::Dot, &q, &points, 7), 1e-5);
}

/// Two Dot vectors `a` and `b` over points 1–4: prefetch `a` of limit 2 is
/// `[1, 2]` and prefetch `b` is `[3, 2]`.
async fn two_vector_collection(qd: &Qd, coll: &str) {
    create(
        qd,
        coll,
        json!({"vectors": {"a": {"size": 2, "distance": "Dot"}, "b": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    let rows = [
        (1, [10.0, 0.0], [0.0, 0.0]),
        (2, [9.0, 0.0], [9.0, 0.0]),
        (3, [1.0, 0.0], [10.0, 0.0]),
        (4, [0.0, 0.0], [1.0, 0.0]),
    ];
    upsert(
        qd,
        coll,
        rows.iter()
            .map(|(id, a, b)| json!({"id": id, "vector": {"a": a, "b": b}, "payload": {"g": id % 2}}))
            .collect(),
    )
    .await;
}

fn ab_prefetch(limit: usize) -> Value {
    json!([
        {"query": [1.0, 0.0], "using": "a", "limit": limit},
        {"query": [1.0, 0.0], "using": "b", "limit": limit}
    ])
}

#[tokio::test]
async fn thresholds_follow_the_distance_order() {
    let qd = Qd::start().await;
    let cosine = random_collection(&qd, "c", Distance::Cosine, 3, 80, 21).await;
    let q = vec![1.0, 0.2, -0.1];
    let got = hits(
        &query(
            &qd,
            "c",
            json!({"query": q, "limit": 80, "score_threshold": 0.9}),
        )
        .await,
    );
    let want: Vec<(u64, f32)> = brute(Distance::Cosine, &q, &cosine, 80)
        .into_iter()
        .filter(|(_, s)| *s > 0.9)
        .collect();
    assert!(!want.is_empty() && want.len() < 80);
    assert_hits(&got, &want, 1e-5);
    let euclid = random_collection(&qd, "e", Distance::Euclid, 3, 80, 22).await;
    let got = hits(
        &query(
            &qd,
            "e",
            json!({"query": q, "limit": 80, "score_threshold": 1.0}),
        )
        .await,
    );
    let want: Vec<(u64, f32)> = brute(Distance::Euclid, &q, &euclid, 80)
        .into_iter()
        .filter(|(_, s)| *s < 1.0)
        .collect();
    assert!(!want.is_empty() && want.len() < 80);
    assert_hits(&got, &want, 1e-5);
    // RRF keeps `>=`: 1 and 3 score exactly 1/2, 2 scores 2/3.
    two_vector_collection(&qd, "ab").await;
    let fused = |t: f32| json!({"prefetch": ab_prefetch(2), "query": {"fusion": "rrf"}, "score_threshold": t});
    let got = hits(&query(&qd, "ab", fused(0.5)).await);
    assert_hits(&got, &[(2, 2.0 / 3.0), (1, 0.5), (3, 0.5)], 1e-6);
    let got = hits(&query(&qd, "ab", fused(0.6)).await);
    assert_eq!(ids(&got), [2]);
}

#[tokio::test]
async fn using_empty_string_is_the_default_vector() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "u", Distance::Dot, 3, 30, 31).await;
    let q = vec![0.5, -0.5, 1.0];
    let plain = hits(&query(&qd, "u", json!({"query": q, "limit": 5})).await);
    let with = hits(&query(&qd, "u", json!({"query": q, "using": "", "limit": 5})).await);
    assert_eq!(plain, with);
    assert_hits(&plain, &brute(Distance::Dot, &q, &points, 5), 1e-5);
    // A map that names `""` next to another vector.
    create(
        &qd,
        "m",
        json!({"vectors": {"": {"size": 2, "distance": "Dot"}, "x": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    upsert(
        &qd,
        "m",
        vec![
            json!({"id": 1, "vector": {"": [1.0, 0.0], "x": [0.0, 1.0]}}),
            json!({"id": 2, "vector": {"": [0.0, 1.0], "x": [1.0, 0.0]}}),
        ],
    )
    .await;
    let got = hits(&query(&qd, "m", json!({"query": [1.0, 0.0], "using": ""})).await);
    assert_eq!(ids(&got), [1, 2]);
    let got = hits(&query(&qd, "m", json!({"query": [1.0, 0.0], "using": "x"})).await);
    assert_eq!(ids(&got), [2, 1]);
    let (status, reply) = query_raw(&qd, "m", json!({"query": [1.0, 0.0], "using": "nope"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Not existing vector name error: nope"
    );
}

#[tokio::test]
async fn offset_skips_after_ordering() {
    let qd = Qd::start().await;
    random_collection(&qd, "o", Distance::Euclid, 3, 40, 41).await;
    let q = vec![0.1, 0.2, 0.3];
    let all = hits(&query(&qd, "o", json!({"query": q, "limit": 8})).await);
    let page = hits(&query(&qd, "o", json!({"query": q, "limit": 5, "offset": 3})).await);
    assert_eq!(page, all[3..8]);
    // Offset and a threshold: the threshold cuts first.
    let cut = hits(
        &query(
            &qd,
            "o",
            json!({"query": q, "limit": 8, "score_threshold": all[4].1}),
        )
        .await,
    );
    let page = hits(
        &query(
            &qd,
            "o",
            json!({"query": q, "limit": 8, "offset": 2, "score_threshold": all[4].1}),
        )
        .await,
    );
    assert_eq!(page, cut[2..]);
}

#[tokio::test]
async fn nearest_by_id_excludes_the_id() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "n", Distance::Cosine, 4, 50, 51).await;
    let v5 = points[5].1.clone();
    let by_vector = hits(&query(&qd, "n", json!({"query": v5, "limit": 6})).await);
    assert_eq!(by_vector[0].0, 5);
    let want: Vec<(u64, f32)> = by_vector.into_iter().filter(|(id, _)| *id != 5).collect();
    for body in [
        json!({"query": 5, "limit": 5}),
        json!({"query": {"nearest": 5}, "limit": 5}),
    ] {
        assert_hits(&hits(&query(&qd, "n", body).await), &want, 1e-5);
    }
    // A missing example id is 404; an invalid one a format error.
    let (status, reply) = query_raw(&qd, "n", json!({"query": 999})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{reply}");
    assert_eq!(error(&reply), "Not found: No point with id 999 found");
    let (status, _) = query_raw(&qd, "n", json!({"query": "not-an-id"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn lookup_from_takes_the_vector_from_another_collection() {
    let qd = Qd::start().await;
    let target = random_collection(&qd, "t", Distance::Dot, 3, 30, 61).await;
    create(
        &qd,
        "src",
        json!({"vectors": {"other": {"size": 3, "distance": "Dot"}}}),
    )
    .await;
    let v = vec![0.3, -0.9, 0.4];
    upsert(&qd, "src", vec![json!({"id": 5, "vector": {"other": v}})]).await;
    let got = hits(
        &query(
            &qd,
            "t",
            json!({"query": 5, "lookup_from": {"collection": "src", "vector": "other"}, "limit": 30}),
        )
        .await,
    );
    // Point 5 of `t` is not excluded: the example came from `src`.
    assert!(ids(&got).contains(&5));
    assert_hits(&got, &brute(Distance::Dot, &v, &target, 30), 1e-5);
    let (status, reply) = query_raw(
        &qd,
        "t",
        json!({"query": 5, "lookup_from": {"collection": "src"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(
        error(&reply),
        "Wrong input: Not existing vector name error: "
    );
}

#[tokio::test]
async fn lookup_from_the_queried_collection_excludes_the_id() {
    // Qdrant excludes an example id unless `lookup_from` names another
    // collection (`collection_query.rs` `get_referenced_point_ids_on_collection`,
    // row T8-4); an alias of the queried collection counts as another.
    let qd = Qd::start().await;
    create(
        &qd,
        "self",
        json!({"vectors": {"a": {"size": 2, "distance": "Dot"}, "b": {"size": 2, "distance": "Dot"}}}),
    )
    .await;
    upsert(
        &qd,
        "self",
        (0..10u64)
            .map(|id| json!({"id": id, "vector": {"a": [id as f32, 1.0], "b": [1.0, id as f32]}}))
            .collect(),
    )
    .await;
    let got = hits(
        &query(
            &qd,
            "self",
            json!({"query": 3, "using": "a", "lookup_from": {"collection": "self", "vector": "b"}, "limit": 10}),
        )
        .await,
    );
    // Point 3's `b` is [1, 3]; scored against `a`, point 3 would be there.
    assert!(!ids(&got).contains(&3), "{got:?}");
    assert_eq!(got.len(), 9);
    let (status, reply) = qd
        .post(
            "/collections/aliases",
            Some(json!({"actions": [{"create_alias": {"collection_name": "self", "alias_name": "me"}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let got = hits(
        &query(
            &qd,
            "self",
            json!({"query": 3, "using": "a", "lookup_from": {"collection": "me", "vector": "b"}, "limit": 10}),
        )
        .await,
    );
    assert!(ids(&got).contains(&3), "{got:?}");
}

#[tokio::test]
async fn query_validation_errors_are_qdrant_400s() {
    let qd = Qd::start().await;
    ab_random(&qd, "v").await;
    let two = json!([{"query": [1.0, 0.0, 0.0, 0.0], "using": "a"}, {"query": [0.0, 1.0, 0.0, 0.0], "using": "b"}]);
    for (body, message) in [
        (
            json!({"prefetch": two[0].clone()}),
            "A query is needed to merge the prefetches. Can't have prefetches without defining a query.",
        ),
        (
            json!({"score_threshold": 0.1}),
            "A query is needed to use the score_threshold. Can't have score_threshold without defining a query.",
        ),
        (
            json!({"prefetch": two.clone(), "query": {"fusion": "rrf"}, "using": "a"}),
            "Fusion queries cannot be combined with the 'using' field.",
        ),
    ] {
        let (status, reply) = query_raw(&qd, "v", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} → {reply}");
        assert_eq!(error(&reply), format!("Wrong input: {message}"), "{body}");
    }
}

// ----- prefetch, fusion, rescore -----

/// Vectors `a` (Cosine) and `b` (Dot), dim 4, 60 random points; the points
/// of each.
pub(crate) async fn ab_random(qd: &Qd, coll: &str) -> (Vec<(u64, Vec<f32>)>, Vec<(u64, Vec<f32>)>) {
    create(
        qd,
        coll,
        json!({"vectors": {"a": {"size": 4, "distance": "Cosine"}, "b": {"size": 4, "distance": "Dot"}}}),
    )
    .await;
    let mut rng = Rng(71);
    let rows: Vec<(u64, Vec<f32>, Vec<f32>)> = (0..60)
        .map(|id| (id, rng.vector(4), rng.vector(4)))
        .collect();
    upsert(
        qd,
        coll,
        rows.iter()
            .map(|(id, a, b)| json!({"id": id, "vector": {"a": a, "b": b}, "payload": {"g": id % 3}}))
            .collect(),
    )
    .await;
    (
        rows.iter().map(|(id, a, _)| (*id, a.clone())).collect(),
        rows.iter().map(|(id, _, b)| (*id, b.clone())).collect(),
    )
}

#[tokio::test]
async fn rrf_and_dbsf_fusion_match_the_reference() {
    let qd = Qd::start().await;
    ab_random(&qd, "f").await;
    let pa = json!({"query": [0.1, 0.9, -0.3, 0.2], "using": "a", "limit": 10, "params": {"exact": true}});
    let pb = json!({"query": [0.5, -0.2, 0.7, 0.1], "using": "b", "limit": 10, "params": {"exact": true}});
    let la = hits(&query(&qd, "f", pa.clone()).await);
    let lb = hits(&query(&qd, "f", pb.clone()).await);
    let want = pk_hits(&rrf(&[pks(&la), pks(&lb)], 2));
    let got = hits(
        &query(
            &qd,
            "f",
            json!({"prefetch": [pa, pb], "query": {"fusion": "rrf"}, "limit": 20}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-5);
    // `rrf.k` 5 is Qdrant's k.
    let want = pk_hits(&rrf(&[pks(&la), pks(&lb)], 5));
    let got = hits(
        &query(
            &qd,
            "f",
            json!({"prefetch": [pa, pb], "query": {"rrf": {"k": 5}}, "limit": 20}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-5);
    let scored = |l: &[(u64, f32)]| -> Vec<(PrimaryKey, f32)> {
        l.iter().map(|(id, s)| (PrimaryKey::U64(*id), *s)).collect()
    };
    let want = pk_hits(&dbsf(&[scored(&la), scored(&lb)]));
    let got = hits(
        &query(
            &qd,
            "f",
            json!({"prefetch": [pa, pb], "query": {"fusion": "dbsf"}, "limit": 20}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-5);
}

#[tokio::test]
async fn root_fusion_returns_the_union_up_to_limit() {
    // E3: disjoint prefetches of 3 under a root fusion of limit 10 give all
    // 6 points, not the largest prefetch's 3.
    let qd = Qd::start().await;
    ab_random(&qd, "u").await;
    let g = |v: u64| json!({"must": [{"key": "g", "match": {"value": v}}]});
    let body = json!({
        "prefetch": [
            {"query": [1.0, 0.0, 0.0, 0.0], "using": "a", "filter": g(0), "limit": 3},
            {"query": [0.0, 1.0, 0.0, 0.0], "using": "b", "filter": g(1), "limit": 3}
        ],
        "query": {"fusion": "rrf"},
        "limit": 10
    });
    let got = hits(&query(&qd, "u", body.clone()).await);
    assert_eq!(got.len(), 6, "{got:?}");
    let mut body = body;
    body["limit"] = json!(4);
    body["offset"] = json!(1);
    assert_eq!(hits(&query(&qd, "u", body).await), got[1..5]);
}

#[tokio::test]
async fn rescore_over_prefetch_scores_exactly() {
    let qd = Qd::start().await;
    let (_, b) = ab_random(&qd, "r").await;
    let prefetch = json!({"query": [0.3, 0.3, 0.3, -0.5], "using": "a", "limit": 20, "params": {"exact": true}});
    let candidates = ids(&hits(&query(&qd, "r", prefetch.clone()).await));
    let qb = vec![0.2, -0.4, 0.9, 0.0];
    let pool: Vec<(u64, Vec<f32>)> = b
        .into_iter()
        .filter(|(id, _)| candidates.contains(id))
        .collect();
    let got = hits(
        &query(
            &qd,
            "r",
            json!({"prefetch": prefetch, "query": qb, "using": "b", "limit": 5}),
        )
        .await,
    );
    assert_hits(&got, &brute(Distance::Dot, &qb, &pool, 5), 1e-5);
}

#[tokio::test]
async fn nested_prefetch_runs() {
    let qd = Qd::start().await;
    let (a, _) = ab_random(&qd, "n").await;
    let qa = vec![0.4, 0.4, -0.2, 0.1];
    let inner = json!({
        "prefetch": [
            {"query": qa, "using": "a", "limit": 5},
            {"query": [0.1, 0.2, 0.3, 0.4], "using": "b", "limit": 5}
        ],
        "query": {"fusion": "rrf"},
        "limit": 6
    });
    let union = ids(&hits(&query(&qd, "n", inner.clone()).await));
    assert_eq!(union.len(), 6);
    let got = hits(
        &query(
            &qd,
            "n",
            json!({"prefetch": inner, "query": qa, "using": "a", "limit": 3}),
        )
        .await,
    );
    let pool: Vec<(u64, Vec<f32>)> = a.into_iter().filter(|(id, _)| union.contains(id)).collect();
    assert_hits(&got, &brute(Distance::Cosine, &qa, &pool, 3), 1e-5);
}

#[tokio::test]
async fn query_without_query_is_filter_order() {
    let qd = Qd::start().await;
    random_collection(&qd, "w", Distance::Dot, 2, 30, 81).await;
    let got = hits(
        &query(
            &qd,
            "w",
            json!({"filter": {"must": [{"key": "g", "match": {"value": 1}}]}, "limit": 5, "offset": 1}),
        )
        .await,
    );
    assert_eq!(got, [(4, 0.0), (7, 0.0), (10, 0.0), (13, 0.0), (16, 0.0)]);
    // Errors keep Qdrant's status.
    let (status, reply) = query_raw(&qd, "w", json!({"query": {"fusion": "rrf"}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error(&reply), "Wrong input: Fusion query requires prefetch");
    let (status, reply) = query_raw(&qd, "w", json!({"query": {"order_by": "g"}})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(error(&reply), "Unsupported in Loams: order_by");
    let (status, _) = query_raw(&qd, "missing", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn query_batch_runs_each_request() {
    let qd = Qd::start().await;
    random_collection(&qd, "b", Distance::Cosine, 3, 40, 91).await;
    let r1 = json!({"query": [1.0, 0.0, 0.0], "limit": 3});
    let r2 = json!({"query": [0.0, 1.0, 0.0], "limit": 4, "filter": {"must": [{"key": "g", "match": {"value": 2}}]}});
    let (status, reply) = qd
        .post(
            "/collections/b/points/query/batch",
            Some(json!({"searches": [r1, r2]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let results = reply["result"].as_array().expect("list");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["points"], query(&qd, "b", r1).await);
    assert_eq!(results[1]["points"], query(&qd, "b", r2).await);
    // One failing request fails the batch.
    let (status, reply) = qd
        .post(
            "/collections/b/points/query/batch",
            Some(json!({"searches": [{"query": [1.0, 0.0, 0.0]}, {"query": {"fusion": "rrf"}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
}

// ----- sparse -----

fn random_sparse(rng: &mut Rng) -> (Vec<u32>, Vec<f32>) {
    let n = rng.below(8) as usize;
    let mut indices: Vec<u32> = Vec::new();
    while indices.len() < n {
        let i = rng.below(40) as u32;
        if !indices.contains(&i) {
            indices.push(i);
        }
    }
    let values = indices
        .iter()
        .map(|_| {
            if rng.below(6) == 0 {
                0.0
            } else {
                (rng.f() + 1.0) * 1.5
            }
        })
        .collect();
    (indices, values)
}

/// 300 points with sparse `s` (IDF) and `t` (none): some lack a vector,
/// some have an empty one, some carry zero weights; payload `g = id % 3`.
/// The stored vectors of each name.
pub(crate) async fn sparse_collection(
    qd: &Qd,
    coll: &str,
) -> BTreeMap<&'static str, Vec<(PrimaryKey, SparseVector)>> {
    create(
        qd,
        coll,
        json!({"vectors": {}, "sparse_vectors": {"s": {"modifier": "idf"}, "t": {}}}),
    )
    .await;
    let mut rng = Rng(101);
    let mut stored: BTreeMap<&'static str, Vec<(PrimaryKey, SparseVector)>> = BTreeMap::new();
    let mut points = Vec::new();
    for id in 0..300u64 {
        let mut vector = serde_json::Map::new();
        for name in ["s", "t"] {
            match rng.below(10) {
                0 => {}
                1 => {
                    vector.insert(name.into(), json!({"indices": [], "values": []}));
                    stored.entry(name).or_default().push((
                        PrimaryKey::U64(id),
                        SparseVector::new(vec![], vec![]).expect("empty"),
                    ));
                }
                _ => {
                    let (indices, values) = random_sparse(&mut rng);
                    vector.insert(name.into(), json!({"indices": indices, "values": values}));
                    stored.entry(name).or_default().push((
                        PrimaryKey::U64(id),
                        SparseVector::new(indices, values).expect("sparse"),
                    ));
                }
            }
        }
        points.push(json!({"id": id, "vector": vector, "payload": {"g": id % 3}}));
    }
    upsert(qd, coll, points).await;
    stored
}

#[tokio::test]
async fn sparse_query_matches_the_reference_scorer() {
    let qd = Qd::start().await;
    let stored = sparse_collection(&qd, "sp").await;
    let mut rng = Rng(202);
    for round in 0..10 {
        let name = if round % 2 == 0 { "s" } else { "t" };
        let (mut indices, mut values) = random_sparse(&mut rng);
        if indices.is_empty() {
            indices = vec![3];
            values = vec![1.0];
        }
        let q = SparseVector::new(indices.clone(), values.clone()).expect("query");
        let want = pk_hits(&sparse_reference(&stored[name], &q, name == "s", 10));
        let got = hits(
            &query(
                &qd,
                "sp",
                json!({"query": {"indices": indices, "values": values}, "using": name, "limit": 10}),
            )
            .await,
        );
        assert_hits(&got, &want, 1e-6);
        // Points sharing no index never appear.
        for id in ids(&got) {
            let (_, v) = stored[name]
                .iter()
                .find(|(pk, _)| *pk == PrimaryKey::U64(id))
                .expect("stored");
            assert!(v.indices().iter().any(|i| q.indices().contains(i)));
        }
    }
    let got = query(
        &qd,
        "sp",
        json!({"query": {"indices": [], "values": []}, "using": "s"}),
    )
    .await;
    assert_eq!(got, json!([]));
}

#[tokio::test]
async fn sparse_thresholds_keep_scores_above() {
    let qd = Qd::start().await;
    let stored = sparse_collection(&qd, "st").await;
    let q =
        SparseVector::new(vec![1, 2, 3, 4, 5, 6], vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]).expect("q");
    let want: Vec<(u64, f32)> = pk_hits(&sparse_reference(&stored["t"], &q, false, 300))
        .into_iter()
        .filter(|(_, s)| *s > 2.0)
        .collect();
    assert!(!want.is_empty());
    let got = hits(
        &query(
            &qd,
            "st",
            json!({"query": {"indices": [1, 2, 3, 4, 5, 6], "values": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]},
                   "using": "t", "limit": 300, "score_threshold": 2.0}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-6);
}

#[tokio::test]
async fn idf_corpus_changes_the_scores_like_the_reference() {
    let qd = Qd::start().await;
    let stored = sparse_collection(&qd, "idf").await;
    let g1 = json!({"must": [{"key": "g", "match": {"value": 1}}]});
    let corpus: Vec<(PrimaryKey, SparseVector)> = stored["s"]
        .iter()
        .filter(|(pk, _)| matches!(pk, PrimaryKey::U64(n) if n % 3 == 1))
        .cloned()
        .collect();
    let q = SparseVector::new(vec![2, 5, 9], vec![1.0, 0.5, 2.0]).expect("q");
    let want = pk_hits(&sparse_reference(&corpus, &q, true, 10));
    let body = |idf: Value| {
        json!({"query": {"indices": [2, 5, 9], "values": [1.0, 0.5, 2.0]}, "using": "s",
               "filter": g1, "limit": 10, "params": {"idf": idf}})
    };
    let got = hits(&query(&qd, "idf", body(json!({"corpus": g1}))).await);
    assert_hits(&got, &want, 1e-6);
    let global = hits(&query(&qd, "idf", body(json!("global"))).await);
    assert_ne!(global, got, "the corpus changes the IDF weights");
    let (status, reply) = query_raw(
        &qd,
        "idf",
        json!({"query": {"indices": [1], "values": [1.0]}, "using": "t", "params": {"idf": "global"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
}

/// Dense `""` (Cosine, 3) and sparse `langchain-sparse`, 60 points with
/// `metadata.page = id % 4`.
async fn hybrid_collection(qd: &Qd, coll: &str) {
    create(
        qd,
        coll,
        json!({"vectors": {"": {"size": 3, "distance": "Cosine"}}, "sparse_vectors": {"langchain-sparse": {}}}),
    )
    .await;
    let mut rng = Rng(303);
    let points = (0..60u64)
        .map(|id| {
            let (indices, values) = random_sparse(&mut rng);
            json!({"id": id,
                   "vector": {"": rng.vector(3), "langchain-sparse": {"indices": indices, "values": values}},
                   "payload": {"metadata": {"page": id % 4}}})
        })
        .collect();
    upsert(qd, coll, points).await;
}

fn page(n: u64) -> Value {
    json!({"must": [{"key": "metadata.page", "match": {"value": n}}]})
}

fn hybrid_prefetch() -> (Value, Value) {
    (
        json!({"using": "", "query": [0.3, -0.2, 0.8], "filter": page(1), "limit": 5}),
        json!({"using": "langchain-sparse", "query": {"indices": [1, 2, 3, 4, 5, 6, 7, 8], "values": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]},
               "filter": page(1), "limit": 5}),
    )
}

#[tokio::test]
async fn dense_and_sparse_prefetches_fuse_like_qdrant() {
    let qd = Qd::start().await;
    hybrid_collection(&qd, "h").await;
    let (dense, sparse) = hybrid_prefetch();
    let ld = hits(&query(&qd, "h", dense.clone()).await);
    let ls = hits(&query(&qd, "h", sparse.clone()).await);
    assert!(!ld.is_empty() && !ls.is_empty());
    let body = |fusion: &str| {
        json!({"prefetch": [dense, sparse], "query": {"fusion": fusion}, "limit": 5, "offset": 0,
               "with_payload": true, "with_vector": false})
    };
    let want = pk_hits(&rrf(&[pks(&ld), pks(&ls)], 2));
    let got = hits(&query(&qd, "h", body("rrf")).await);
    assert_hits(&got, &want[..want.len().min(5)], 1e-5);
    let scored = |l: &[(u64, f32)]| -> Vec<(PrimaryKey, f32)> {
        l.iter().map(|(id, s)| (PrimaryKey::U64(*id), *s)).collect()
    };
    let want = pk_hits(&dbsf(&[scored(&ld), scored(&ls)]));
    let got = query(&qd, "h", body("dbsf")).await;
    assert_hits(&hits(&got), &want[..want.len().min(5)], 1e-5);
    assert!(got[0]["payload"]["metadata"]["page"] == 1, "{got}");
}

#[tokio::test]
async fn query_batch_runs_dense_and_sparse_requests() {
    // LlamaIndex's hybrid batch (`li103:…/qdrant/base.py:1060-1147`).
    let qd = Qd::start().await;
    hybrid_collection(&qd, "lb").await;
    let (dense, sparse) = hybrid_prefetch();
    let (status, reply) = qd
        .post(
            "/collections/lb/points/query/batch",
            Some(json!({"searches": [dense, sparse]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"][0]["points"], query(&qd, "lb", dense).await);
    assert_eq!(reply["result"][1]["points"], query(&qd, "lb", sparse).await);
}

#[tokio::test]
async fn with_payload_default_is_false_for_query() {
    let qd = Qd::start().await;
    random_collection(&qd, "p", Distance::Dot, 2, 5, 111).await;
    let got = query(&qd, "p", json!({"query": [1.0, 0.0], "limit": 2})).await;
    for p in got.as_array().expect("list") {
        let keys: Vec<&String> = p.as_object().expect("object").keys().collect();
        assert_eq!(keys, ["id", "version", "score"], "{p}");
        assert_eq!(p["version"], 0);
    }
    let got = query(
        &qd,
        "p",
        json!({"query": [1.0, 0.0], "limit": 1, "with_payload": true, "with_vector": true}),
    )
    .await;
    assert!(got[0]["payload"]["g"].is_number(), "{got}");
    assert_eq!(got[0]["vector"].as_array().map(Vec::len), Some(2), "{got}");
    let got = query(
        &qd,
        "p",
        json!({"query": [1.0, 0.0], "limit": 1, "with_payload": ["g"], "with_vectors": [""]}),
    )
    .await;
    assert!(got[0]["payload"]["g"].is_number(), "{got}");
    assert!(got[0]["vector"].is_array(), "{got}");
}

// ----- gRPC -----

pub(crate) fn dense_input(v: &[f32]) -> pb::VectorInput {
    pb::VectorInput {
        variant: Some(pb::vector_input::Variant::Dense(pb::DenseVector {
            data: v.to_vec(),
        })),
    }
}

fn nearest(input: pb::VectorInput) -> pb::Query {
    pb::Query {
        variant: Some(pb::query::Variant::Nearest(input)),
    }
}

fn fusion(f: pb::Fusion) -> pb::Query {
    pb::Query {
        variant: Some(pb::query::Variant::Fusion(f as i32)),
    }
}

pub(crate) fn grpc_hits(points: &[pb::ScoredPoint]) -> Vec<(u64, f32)> {
    points
        .iter()
        .map(|p| {
            let id = match p.id.as_ref().and_then(|id| id.point_id_options.as_ref()) {
                Some(pb::point_id::PointIdOptions::Num(n)) => *n,
                other => panic!("{other:?}"),
            };
            (id, p.score)
        })
        .collect()
}

fn page_filter(n: i64) -> pb::Filter {
    pb::Filter {
        must: vec![pb::Condition {
            condition_one_of: Some(pb::condition::ConditionOneOf::Field(pb::FieldCondition {
                key: "metadata.page".into(),
                r#match: Some(pb::Match {
                    match_value: Some(pb::r#match::MatchValue::Integer(n)),
                }),
                ..Default::default()
            })),
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn grpc_query_matches_rest() {
    let qd = Qd::start().await;
    ab_random(&qd, "g").await;
    hybrid_collection(&qd, "gh").await;
    let mut client = qd.points().await;
    let qa = [0.1_f32, 0.9, -0.3, 0.2];
    let qb = [0.5_f32, -0.2, 0.7, 0.1];
    let pa = pb::PrefetchQuery {
        query: Some(nearest(dense_input(&qa))),
        using: Some("a".into()),
        limit: Some(10),
        ..Default::default()
    };
    let pbq = pb::PrefetchQuery {
        query: Some(nearest(dense_input(&qb))),
        using: Some("b".into()),
        limit: Some(10),
        ..Default::default()
    };
    let cases: Vec<(pb::QueryPoints, Value)> = vec![
        (
            pb::QueryPoints {
                collection_name: "g".into(),
                query: Some(nearest(dense_input(&qa))),
                using: Some("a".into()),
                limit: Some(5),
                offset: Some(1),
                ..Default::default()
            },
            json!({"query": qa, "using": "a", "limit": 5, "offset": 1}),
        ),
        (
            pb::QueryPoints {
                collection_name: "g".into(),
                prefetch: vec![pa.clone(), pbq.clone()],
                query: Some(fusion(pb::Fusion::Rrf)),
                limit: Some(8),
                ..Default::default()
            },
            json!({"prefetch": [{"query": qa, "using": "a", "limit": 10}, {"query": qb, "using": "b", "limit": 10}],
                   "query": {"fusion": "rrf"}, "limit": 8}),
        ),
        (
            pb::QueryPoints {
                collection_name: "g".into(),
                prefetch: vec![pa.clone()],
                query: Some(nearest(dense_input(&qb))),
                using: Some("b".into()),
                limit: Some(4),
                ..Default::default()
            },
            json!({"prefetch": {"query": qa, "using": "a", "limit": 10}, "query": qb, "using": "b", "limit": 4}),
        ),
        (
            pb::QueryPoints {
                collection_name: "g".into(),
                query: Some(nearest(pb::VectorInput {
                    variant: Some(pb::vector_input::Variant::Id(pb::PointId {
                        point_id_options: Some(pb::point_id::PointIdOptions::Num(3)),
                    })),
                })),
                using: Some("b".into()),
                limit: Some(4),
                score_threshold: Some(-10.0),
                ..Default::default()
            },
            json!({"query": 3, "using": "b", "limit": 4, "score_threshold": -10.0}),
        ),
        (
            pb::QueryPoints {
                collection_name: "gh".into(),
                query: Some(nearest(pb::VectorInput {
                    variant: Some(pb::vector_input::Variant::Sparse(pb::SparseVector {
                        values: vec![1.0, 2.0],
                        indices: vec![4, 1],
                    })),
                })),
                using: Some("langchain-sparse".into()),
                filter: Some(page_filter(2)),
                ..Default::default()
            },
            json!({"query": {"indices": [4, 1], "values": [1.0, 2.0]}, "using": "langchain-sparse", "filter": page(2)}),
        ),
        (
            pb::QueryPoints {
                collection_name: "gh".into(),
                prefetch: vec![
                    pb::PrefetchQuery {
                        query: Some(nearest(dense_input(&[0.3, -0.2, 0.8]))),
                        using: Some(String::new()),
                        filter: Some(page_filter(1)),
                        limit: Some(5),
                        ..Default::default()
                    },
                    pb::PrefetchQuery {
                        query: Some(nearest(pb::VectorInput {
                            variant: Some(pb::vector_input::Variant::Sparse(pb::SparseVector {
                                values: vec![1.0; 8],
                                indices: (1..=8).collect(),
                            })),
                        })),
                        using: Some("langchain-sparse".into()),
                        filter: Some(page_filter(1)),
                        limit: Some(5),
                        ..Default::default()
                    },
                ],
                query: Some(fusion(pb::Fusion::Dbsf)),
                limit: Some(5),
                ..Default::default()
            },
            {
                let (dense, sparse) = hybrid_prefetch();
                json!({"prefetch": [dense, sparse], "query": {"fusion": "dbsf"}, "limit": 5})
            },
        ),
    ];
    for (grpc, rest) in &cases {
        let reply = client
            .query(grpc.clone())
            .await
            .unwrap_or_else(|s| panic!("{rest}: {s:?}"))
            .into_inner();
        let coll = grpc.collection_name.clone();
        let want = hits(&query(&qd, &coll, rest.clone()).await);
        assert_eq!(grpc_hits(&reply.result), want, "{rest}");
        assert!(
            reply
                .result
                .iter()
                .all(|p| p.payload.is_empty() && p.vectors.is_none())
        );
    }
    // QueryBatch answers one result per request, in order.
    let batch = client
        .query_batch(pb::QueryBatchPoints {
            collection_name: "g".into(),
            query_points: vec![cases[0].0.clone(), cases[1].0.clone()],
            ..Default::default()
        })
        .await
        .expect("batch")
        .into_inner();
    assert_eq!(batch.result.len(), 2);
    for (i, result) in batch.result.iter().enumerate() {
        assert_eq!(
            grpc_hits(&result.result),
            hits(&query(&qd, "g", cases[i].1.clone()).await)
        );
    }
    // Errors keep Qdrant's codes.
    let status = client
        .query(pb::QueryPoints {
            collection_name: "g".into(),
            query: Some(fusion(pb::Fusion::Rrf)),
            ..Default::default()
        })
        .await
        .expect_err("no prefetch");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Wrong input: Fusion query requires prefetch"
    );
    let status = client
        .query(pb::QueryPoints {
            collection_name: "g".into(),
            query: Some(pb::Query {
                variant: Some(pb::query::Variant::Sample(pb::Sample::Random as i32)),
            }),
            ..Default::default()
        })
        .await
        .expect_err("sample");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    // Payload and vectors when asked for.
    let reply = client
        .query(pb::QueryPoints {
            collection_name: "gh".into(),
            query: Some(nearest(dense_input(&[1.0, 0.0, 0.0]))),
            limit: Some(1),
            with_payload: Some(pb::WithPayloadSelector {
                selector_options: Some(pb::with_payload_selector::SelectorOptions::Enable(true)),
            }),
            with_vectors: Some(pb::WithVectorsSelector {
                selector_options: Some(pb::with_vectors_selector::SelectorOptions::Enable(true)),
            }),
            ..Default::default()
        })
        .await
        .expect("query")
        .into_inner();
    assert!(reply.result[0].payload.contains_key("metadata"));
    assert!(reply.result[0].vectors.is_some());
    assert_eq!(reply.result[0].version, 0);
}

#[tokio::test]
async fn query_batch_refuses_more_than_max_batch_queries() {
    // Issue #298: a client-chosen batch length must not size an allocation.
    let qd = Qd::start().await;
    random_collection(&qd, "hb", Distance::Cosine, 3, 4, 7).await;
    let max = loams_qdrant::QdrantConfig::default().max_batch_queries;
    let searches = vec![json!({"query": [1.0, 0.0], "limit": 1}); max + 1];
    let (status, reply) = qd
        .post(
            "/collections/hb/points/query/batch",
            Some(json!({ "searches": searches })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(
        error(&reply),
        format!(
            "Wrong input: The query batch holds {} entries, more than the limit of {max}",
            max + 1
        )
    );
}

#[tokio::test]
async fn grpc_batch_counts_are_checked_before_conversion() {
    let qd = Qd::start_with(|config| {
        config.qdrant.as_mut().unwrap().max_batch_queries = 2;
    })
    .await;
    random_collection(&qd, "bounded", Distance::Cosine, 3, 4, 7).await;
    let mut client = qd.points().await;
    let message = "Wrong input: The query batch holds 3 entries, more than the limit of 2";
    // The invalid entries must not be converted before rejecting the count.
    let invalid = pb::QueryPoints {
        query: Some(pb::Query {
            variant: Some(pb::query::Variant::Nearest(pb::VectorInput::default())),
        }),
        ..Default::default()
    };
    assert!(loams_qdrant::convert::query::query_request_from_grpc(&invalid).is_err());
    let err = client
        .query_batch(pb::QueryBatchPoints {
            collection_name: "bounded".into(),
            query_points: vec![invalid; 3],
            ..Default::default()
        })
        .await
        .expect_err("query count");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), message);
    #[allow(deprecated)]
    let err = client
        .search_batch(pb::SearchBatchPoints {
            collection_name: "bounded".into(),
            search_points: vec![pb::SearchPoints::default(); 3],
            ..Default::default()
        })
        .await
        .expect_err("search count");
    assert_eq!(err.message(), message);
    #[allow(deprecated)]
    let err = client
        .recommend_batch(pb::RecommendBatchPoints {
            collection_name: "bounded".into(),
            recommend_points: vec![pb::RecommendPoints::default(); 3],
            ..Default::default()
        })
        .await
        .expect_err("recommend count");
    assert_eq!(err.message(), message);
    #[allow(deprecated)]
    let err = client
        .discover_batch(pb::DiscoverBatchPoints {
            collection_name: "bounded".into(),
            discover_points: vec![pb::DiscoverPoints::default(); 3],
            ..Default::default()
        })
        .await
        .expect_err("discover count");
    assert_eq!(err.message(), message);
    for len in [0, 2] {
        let result = client
            .query_batch(pb::QueryBatchPoints {
                collection_name: "bounded".into(),
                query_points: vec![
                    pb::QueryPoints {
                        limit: Some(1),
                        ..Default::default()
                    };
                    len
                ],
                ..Default::default()
            })
            .await
            .expect("at or below count")
            .into_inner();
        assert_eq!(result.result.len(), len);
        let (status, result) = qd
            .post(
                "/collections/bounded/points/query/batch",
                Some(json!({"searches": vec![json!({"limit": 1}); len]})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["result"].as_array().unwrap().len(), len);
    }
    for (route, entry) in [
        ("search", json!({"vector": [1.0, 0.0, 0.0], "limit": 1})),
        ("recommend", json!({"positive": [0], "limit": 1})),
        ("discover", json!({"context": [], "limit": 1})),
    ] {
        let (status, result) = qd
            .post(
                &format!("/collections/bounded/points/{route}/batch"),
                Some(json!({"searches": vec![entry; 3]})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");
        assert_eq!(error(&result), message);
    }
}
