//! Gateway-scored queries end to end (plan M1.4 Task 8, Ruling 10):
//! `recommend`'s strategies, `discover`, `context` and MMR, against
//! brute-force references over every point; then the legacy `search`,
//! `recommend` and `discover` routes, their batches and gRPC methods.

// The legacy gRPC methods are deprecated in Qdrant's protos.
#![allow(deprecated)]

use loams_collection::{Distance, PrimaryKey};
use loams_qdrant::proto::qdrant as pb;
use loams_qdrant::scoring::{
    best_score, context_score, cosine_normalize, discover_score, mmr_select, sum_scores,
};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;
use crate::query::{
    ab_random, assert_hits, brute, create, dense_input, error, grpc_hits, hits, ids, query,
    random_collection, sparse_collection, upsert,
};

// ----- helpers -----

/// `v` as stored: normalized for Cosine.
fn stored(distance: Distance, v: &[f32]) -> Vec<f32> {
    let mut v = v.to_vec();
    if distance == Distance::Cosine {
        cosine_normalize(&mut v);
    }
    v
}

/// The vector of point `id`, as stored.
fn vector_of(distance: Distance, points: &[(u64, Vec<f32>)], id: u64) -> Vec<f32> {
    let (_, v) = points.iter().find(|(p, _)| *p == id).expect("point");
    stored(distance, v)
}

/// Every point but `exclude`, scored by `score` (larger is better),
/// ordered by score then id, the first `limit`.
fn brute_scored(
    distance: Distance,
    points: &[(u64, Vec<f32>)],
    exclude: &[u64],
    limit: usize,
    score: impl Fn(&[f32]) -> f32,
) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = points
        .iter()
        .filter(|(id, _)| !exclude.contains(id))
        .map(|(id, v)| (*id, score(&stored(distance, v))))
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    scored.truncate(limit);
    scored
}

// ----- recommend -----

#[tokio::test]
async fn recommend_average_vector_equals_nearest_of_the_average() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "r", Distance::Cosine, 4, 100, 101).await;
    let (p1, p2, n1) = (
        vector_of(Distance::Cosine, &points, 1),
        vector_of(Distance::Cosine, &points, 2),
        vector_of(Distance::Cosine, &points, 3),
    );
    // avg(pos) + avg(pos) − avg(neg) over the stored (normalized) vectors.
    let avg: Vec<f32> = (0..4).map(|i| p1[i] + p2[i] - n1[i]).collect();
    let want: Vec<(u64, f32)> = brute(Distance::Cosine, &avg, &points, 13)
        .into_iter()
        .filter(|(id, _)| ![1, 2, 3].contains(id))
        .take(10)
        .collect();
    for body in [
        json!({"query": {"recommend": {"positive": [1, 2], "negative": [3]}}}),
        json!({"query": {"recommend": {"positive": [1, 2], "negative": [3], "strategy": "average_vector"}}}),
    ] {
        assert_hits(&hits(&query(&qd, "r", body).await), &want, 1e-5);
    }
    // Vectors as examples, not only ids (nothing is excluded then).
    let want = brute(Distance::Cosine, &avg, &points, 10);
    let got = hits(
        &query(
            &qd,
            "r",
            json!({"query": {"recommend": {"positive": [p1, p2], "negative": [n1]}}}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-5);
}

#[tokio::test]
async fn recommend_best_score_matches_brute_force() {
    let qd = Qd::start().await;
    let d = Distance::Cosine;
    let points = random_collection(&qd, "b", d, 4, 300, 102).await;
    let pos = vec![vector_of(d, &points, 1), vector_of(d, &points, 2)];
    let neg = vec![vector_of(d, &points, 3)];
    // Every point is a candidate (candidate_k ≥ 300), so the whole ranking
    // is exact.
    let want = brute_scored(d, &points, &[1, 2, 3], 297, |c| {
        best_score(d, c, &pos, &neg)
    });
    let body = json!({"query": {"recommend": {"positive": [1, 2], "negative": [3], "strategy": "best_score"}}, "limit": 297});
    let got = hits(&query(&qd, "b", body).await);
    assert_hits(&got, &want, 1e-5);
    assert!(
        got.iter().any(|(_, s)| *s < 0.0),
        "some are closer to the negative"
    );
    // Custom scores keep `score > t` on Cosine (Ruling 7), and `offset`.
    let body = json!({"query": {"recommend": {"positive": [1, 2], "negative": [3], "strategy": "best_score"}},
                      "limit": 70, "offset": 10, "score_threshold": 0.6});
    let want: Vec<(u64, f32)> = want
        .into_iter()
        .filter(|(_, s)| *s > 0.6)
        .skip(10)
        .take(70)
        .collect();
    assert_hits(&hits(&query(&qd, "b", body).await), &want, 1e-5);
    // Negatives only: every score is −sig(n).
    let got = hits(
        &query(
            &qd,
            "b",
            json!({"query": {"recommend": {"negative": [3], "strategy": "best_score"}}, "limit": 5}),
        )
        .await,
    );
    assert!(!got.is_empty() && got.iter().all(|(id, s)| *s < 0.0 && *id != 3));
}

#[tokio::test]
async fn recommend_sum_scores_matches_brute_force() {
    let qd = Qd::start().await;
    let d = Distance::Euclid;
    let points = random_collection(&qd, "s", d, 3, 300, 103).await;
    let pos = vec![vector_of(d, &points, 10), vector_of(d, &points, 20)];
    let neg = vec![vector_of(d, &points, 30), vector_of(d, &points, 40)];
    let want = brute_scored(d, &points, &[10, 20, 30, 40], 80, |c| {
        sum_scores(d, c, &pos, &neg)
    });
    let body = json!({"query": {"recommend": {"positive": [10, 20], "negative": [30, 40], "strategy": "sum_scores"}}, "limit": 80});
    assert_hits(&hits(&query(&qd, "s", body).await), &want, 1e-4);
    // On Euclid a custom-score threshold keeps `score < t`, as Qdrant's
    // distance order does (Ruling 7).
    let t = want[40].1;
    let body = json!({"query": {"recommend": {"positive": [10, 20], "negative": [30, 40], "strategy": "sum_scores"}},
                      "limit": 80, "score_threshold": t});
    let got = hits(&query(&qd, "s", body).await);
    let all = brute_scored(d, &points, &[10, 20, 30, 40], 300, |c| {
        sum_scores(d, c, &pos, &neg)
    });
    let kept: Vec<(u64, f32)> = all.into_iter().filter(|(_, s)| *s < t).take(80).collect();
    assert_hits(&got, &kept, 1e-4);
    assert_eq!(got.len(), 80);
    assert!(got.iter().all(|(_, s)| *s < t));
    // Negatives only, which Qdrant accepts (owner ruling on row T8-7): on
    // Euclid the legs are exact scans away from the negatives, here
    // covering the whole collection (candidate_k = 4 × 80 ≥ 300).
    let want = brute_scored(d, &points, &[30, 40], 80, |c| sum_scores(d, c, &[], &neg));
    let body = json!({"query": {"recommend": {"negative": [30, 40], "strategy": "sum_scores"}}, "limit": 80});
    assert_hits(&hits(&query(&qd, "s", body).await), &want, 1e-4);
}

/// Negatives only (review of #57): the best scores belong to the points
/// farthest from the negatives, so a collection larger than `candidate_k`
/// (100 here) must still return them: the legs search away from the
/// negatives. Exact on Cosine and Dot.
#[tokio::test]
async fn negatives_only_recommend_finds_the_farthest_points() {
    let qd = Qd::start().await;
    for (name, d) in [("nc", Distance::Cosine), ("nd", Distance::Dot)] {
        let points = random_collection(&qd, name, d, 4, 600, 106).await;
        let neg = vec![vector_of(d, &points, 30), vector_of(d, &points, 40)];
        let want = brute_scored(d, &points, &[30, 40], 5, |c| sum_scores(d, c, &[], &neg));
        let body = json!({"query": {"recommend": {"negative": [30, 40], "strategy": "sum_scores"}}, "limit": 5});
        assert_hits(&hits(&query(&qd, name, body).await), &want, 1e-4);
        let one = std::slice::from_ref(&neg[0]);
        let want = brute_scored(d, &points, &[30], 5, |c| best_score(d, c, &[], one));
        let body = json!({"query": {"recommend": {"negative": [30], "strategy": "best_score"}}, "limit": 5});
        assert_hits(&hits(&query(&qd, name, body).await), &want, 1e-4);
    }
}

// ----- discover, context -----

#[tokio::test]
async fn discover_matches_brute_force() {
    let qd = Qd::start().await;
    let d = Distance::Dot;
    let points = random_collection(&qd, "d", d, 4, 300, 104).await;
    let target = vector_of(d, &points, 5);
    let pairs = vec![
        (vector_of(d, &points, 6), vector_of(d, &points, 7)),
        (vector_of(d, &points, 8), vector_of(d, &points, 9)),
    ];
    let want = brute_scored(d, &points, &[5, 6, 7, 8, 9], 80, |c| {
        discover_score(d, c, &target, &pairs)
    });
    let body = json!({"query": {"discover": {"target": 5, "context": [
        {"positive": 6, "negative": 7}, {"positive": 8, "negative": 9}]}}, "limit": 80});
    let got = hits(&query(&qd, "d", body).await);
    assert_hits(&got, &want, 1e-5);
    // Ranks and sigmoids: every score lies in (rank, rank + 1).
    assert!(got.iter().all(|(_, s)| (-2.0..3.0).contains(s)));
    // Without pairs, discover scores sig(sim(target)) (row T8-6).
    let want = brute_scored(d, &points, &[5], 10, |c| discover_score(d, c, &target, &[]));
    let got = hits(&query(&qd, "d", json!({"query": {"discover": {"target": 5}}})).await);
    assert_hits(&got, &want, 1e-5);
    assert!(got.iter().all(|(_, s)| (0.0..1.0).contains(s)));
}

#[tokio::test]
async fn context_matches_brute_force() {
    let qd = Qd::start().await;
    let d = Distance::Manhattan;
    let points = random_collection(&qd, "c", d, 3, 300, 105).await;
    let pairs = vec![
        (vector_of(d, &points, 1), vector_of(d, &points, 2)),
        (vector_of(d, &points, 3), vector_of(d, &points, 4)),
    ];
    let want = brute_scored(d, &points, &[1, 2, 3, 4], 80, |c| {
        context_score(d, c, &pairs)
    });
    let body = json!({"query": {"context": [{"positive": 1, "negative": 2}, {"positive": 3, "negative": 4}]}, "limit": 80});
    let got = hits(&query(&qd, "c", body).await);
    assert_hits(&got, &want, 1e-5);
    assert!(got.iter().all(|(_, s)| *s <= 0.0));
    // One pair may be given without a list.
    let one = vec![pairs[0].clone()];
    let want = brute_scored(d, &points, &[1, 2], 80, |c| context_score(d, c, &one));
    let got = hits(
        &query(
            &qd,
            "c",
            json!({"query": {"context": {"positive": 1, "negative": 2}}, "limit": 80}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-5);
    // An empty context scores every point 0, as in Qdrant (owner ruling on
    // row T8-7).
    let got = hits(&query(&qd, "c", json!({"query": {"context": []}, "limit": 5})).await);
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|(_, s)| *s == 0.0));
}

#[tokio::test]
async fn example_ids_are_excluded_only_from_their_own_collection() {
    let qd = Qd::start().await;
    let d = Distance::Dot;
    let points = random_collection(&qd, "own", d, 3, 40, 106).await;
    create(
        &qd,
        "other",
        json!({"vectors": {"size": 3, "distance": "Dot"}}),
    )
    .await;
    let v = vec![0.2, -0.7, 0.5];
    upsert(&qd, "other", vec![json!({"id": 1, "vector": v})]).await;
    let reco = |lookup: Value| json!({"query": {"recommend": {"positive": [1], "strategy": "sum_scores"}}, "limit": 40, "lookup_from": lookup});
    // From the queried collection (no lookup, or one naming it): excluded.
    for lookup in [Value::Null, json!({"collection": "own"})] {
        let got = hits(&query(&qd, "own", reco(lookup)).await);
        assert_eq!(got.len(), 39);
        assert!(!ids(&got).contains(&1));
    }
    // From another collection: point 1 of `own` stays, scored against the
    // other collection's vector.
    let got = hits(&query(&qd, "own", reco(json!({"collection": "other"}))).await);
    assert!(ids(&got).contains(&1));
    let want = brute_scored(d, &points, &[], 40, |c| {
        sum_scores(d, c, std::slice::from_ref(&v), &[])
    });
    assert_hits(&got, &want, 1e-5);
}

// ----- MMR -----

#[tokio::test]
async fn mmr_with_zero_diversity_is_relevance_order() {
    // LangChain's check (`lc:tests/integration_tests/qdrant_vector_store/test_mmr.py:59-76`).
    let qd = Qd::start().await;
    let points = random_collection(&qd, "m", Distance::Euclid, 3, 60, 107).await;
    let q = vec![0.1, -0.4, 0.3];
    let body = json!({"query": {"nearest": q, "mmr": {"diversity": 0.0, "candidates_limit": 10}},
                      "limit": 10, "with_vector": true});
    let reply = query(&qd, "m", body).await;
    // Relevance order, with distances as scores.
    assert_hits(
        &hits(&reply),
        &brute(Distance::Euclid, &q, &points, 10),
        1e-5,
    );
    // Euclid vectors come back as stored.
    for p in reply.as_array().expect("points") {
        let id = p["id"].as_u64().expect("id");
        let v: Vec<f32> = serde_json::from_value(p["vector"].clone()).expect("vector");
        assert_eq!(v, points[id as usize].1);
    }
}

#[tokio::test]
async fn mmr_matches_the_reference() {
    let qd = Qd::start().await;
    let d = Distance::Cosine;
    let points = random_collection(&qd, "mr", d, 4, 50, 108).await;
    let q = vec![0.3, 0.1, -0.5, 0.2];
    let q_stored = stored(d, &q);
    for (limit, offset, candidates) in [(10, 0, 50), (5, 3, 20), (10, 0, 10)] {
        let pool = brute(d, &q, &points, candidates);
        let vectors: Vec<(PrimaryKey, Vec<f32>)> = pool
            .iter()
            .map(|(id, _)| (PrimaryKey::U64(*id), vector_of(d, &points, *id)))
            .collect();
        let picks = mmr_select(d, &q_stored, &vectors, 0.5, offset + limit);
        let want: Vec<(u64, f32)> = picks
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|i| pool[i])
            .collect();
        let body = json!({"query": {"nearest": q, "mmr": {"diversity": 0.5, "candidates_limit": candidates}},
                          "limit": limit, "offset": offset});
        let got = hits(&query(&qd, "mr", body).await);
        assert_hits(&got, &want, 1e-5);
        // Diversity changes the order of the plain nearest results.
        if candidates == 50 {
            assert_ne!(ids(&got), ids(&pool[..limit]));
        }
    }
    // `candidates_limit` defaults to `limit`; a threshold cuts the
    // candidates first.
    let pool = brute(d, &q, &points, 6);
    let t = pool[3].1;
    let body = json!({"query": {"nearest": q, "mmr": {}}, "limit": 6, "score_threshold": t});
    let got = hits(&query(&qd, "mr", body).await);
    let mut got_ids = ids(&got);
    got_ids.sort_unstable();
    let mut want_ids: Vec<u64> = pool[..3].iter().map(|(id, _)| *id).collect();
    want_ids.sort_unstable();
    assert_eq!(got_ids, want_ids);
}

// ----- legacy routes (semantics 5–6) -----

/// A legacy route's result, which must succeed: a bare list, or a list of
/// lists for a batch.
async fn legacy(qd: &Qd, coll: &str, route: &str, body: Value) -> Value {
    let (status, reply) = qd
        .post(
            &format!("/collections/{coll}/points/{route}"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{route} {body} → {reply}");
    assert_eq!(reply["status"], "ok");
    reply["result"].clone()
}

#[tokio::test]
async fn legacy_search_and_query_agree() {
    let qd = Qd::start().await;
    let mut client = qd.points().await;
    for (coll, distance, seed) in [
        ("lc", Distance::Cosine, 201),
        ("le", Distance::Euclid, 202),
        ("ld", Distance::Dot, 203),
    ] {
        let points = random_collection(&qd, coll, distance, 4, 80, seed).await;
        let q = vec![0.2, -0.1, 0.6, 0.3];
        let filter = json!({"must": [{"key": "g", "match": {"value": 1}}]});
        let want = hits(
            &query(
                &qd,
                coll,
                json!({"query": q, "using": "", "limit": 7, "offset": 2, "filter": filter}),
            )
            .await,
        );
        for vector in [json!(q), json!({"name": "", "vector": q})] {
            let body = json!({"vector": vector, "limit": 7, "offset": 2, "filter": filter});
            assert_hits(&hits(&legacy(&qd, coll, "search", body).await), &want, 1e-6);
        }
        // `top` is `limit`'s alias; the brute force agrees.
        let got = hits(&legacy(&qd, coll, "search", json!({"vector": q, "top": 5})).await);
        assert_hits(&got, &brute(distance, &q, &points, 5), 1e-5);
        let reply = client
            .search(pb::SearchPoints {
                collection_name: coll.into(),
                vector: q.clone(),
                limit: 5,
                ..Default::default()
            })
            .await
            .expect("Search")
            .into_inner();
        assert_hits(&grpc_hits(&reply.result), &got, 1e-6);
    }
    // A named vector: `{name, vector}` is `using`.
    ab_random(&qd, "ab").await;
    let q = [0.4_f32, 0.1, -0.2, 0.9];
    let want = hits(&query(&qd, "ab", json!({"query": q, "using": "b", "limit": 6})).await);
    let got = hits(
        &legacy(
            &qd,
            "ab",
            "search",
            json!({"vector": {"name": "b", "vector": q}, "limit": 6}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-6);
    // A sparse vector: `{name, vector: {indices, values}}`, and gRPC
    // `sparse_indices`.
    sparse_collection(&qd, "sp").await;
    let sparse = json!({"indices": [3, 7, 11], "values": [1.0, 0.5, 2.0]});
    let want = hits(
        &query(
            &qd,
            "sp",
            json!({"query": sparse, "using": "t", "limit": 10}),
        )
        .await,
    );
    assert!(!want.is_empty());
    let got = hits(
        &legacy(
            &qd,
            "sp",
            "search",
            json!({"vector": {"name": "t", "vector": sparse}, "limit": 10}),
        )
        .await,
    );
    assert_hits(&got, &want, 1e-6);
    let reply = client
        .search(pb::SearchPoints {
            collection_name: "sp".into(),
            vector: vec![1.0, 0.5, 2.0],
            sparse_indices: Some(pb::SparseIndices {
                data: vec![3, 7, 11],
            }),
            vector_name: Some("t".into()),
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("sparse Search")
        .into_inner();
    assert_hits(&grpc_hits(&reply.result), &want, 1e-6);
    // `limit` is required.
    let (status, reply) = qd
        .post(
            "/collections/ab/points/search",
            Some(json!({"vector": {"name": "b", "vector": q}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert!(
        error(&reply).starts_with("Format error in JSON body: missing field `limit`"),
        "{reply}"
    );
}

#[tokio::test]
async fn sparse_recommend_and_discover_are_501() {
    let qd = Qd::start().await;
    sparse_collection(&qd, "sp").await;
    let v = json!({"indices": [1, 4], "values": [0.5, 1.0]});
    let pair = json!({"positive": v, "negative": {"indices": [2], "values": [1.0]}});
    for (route, body, feature) in [
        (
            "recommend",
            json!({"positive": [v], "using": "s", "limit": 3}),
            "sparse recommend",
        ),
        (
            "recommend/batch",
            json!({"searches": [{"positive": [v], "using": "s", "limit": 3}]}),
            "sparse recommend",
        ),
        (
            "discover",
            json!({"target": v, "context": [pair], "using": "t", "limit": 3}),
            "sparse discover",
        ),
        (
            "discover",
            json!({"context": [pair], "using": "t", "limit": 3}),
            "sparse context",
        ),
    ] {
        let (status, reply) = qd
            .post(&format!("/collections/sp/points/{route}"), Some(body))
            .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{route} {reply}");
        assert_eq!(error(&reply), format!("Unsupported in Loams: {feature}"));
    }
}

#[tokio::test]
async fn euclid_vectors_are_returned_raw() {
    // LangChain 1.15.1's client-side MMR reads `search` with vectors.
    let qd = Qd::start().await;
    let points = random_collection(&qd, "raw", Distance::Euclid, 3, 30, 204).await;
    let result = legacy(
        &qd,
        "raw",
        "search",
        json!({"vector": [0.5, 0.5, 0.5], "limit": 30, "with_vector": true, "with_payload": true}),
    )
    .await;
    let list = result.as_array().expect("a bare list");
    assert_eq!(list.len(), 30);
    for p in list {
        let id = p["id"].as_u64().expect("id");
        let v: Vec<f32> = serde_json::from_value(p["vector"].clone()).expect("vector");
        assert_eq!(v, points[id as usize].1);
        assert_eq!(p["payload"], json!({"g": id % 3}));
        assert_eq!(p["version"], 0);
    }
    // `with_payload` and `with_vector` default to false.
    let result = legacy(
        &qd,
        "raw",
        "search",
        json!({"vector": [0.5, 0.5, 0.5], "limit": 1}),
    )
    .await;
    assert_eq!(
        result[0]
            .as_object()
            .expect("point")
            .keys()
            .collect::<Vec<_>>(),
        ["id", "version", "score"]
    );
}

#[tokio::test]
async fn legacy_batches_return_lists_of_lists() {
    let qd = Qd::start().await;
    let points = random_collection(&qd, "bt", Distance::Dot, 3, 50, 205).await;
    let (qa, qb) = (vec![1.0, 0.0, 0.0], vec![0.0, 0.3, -1.0]);
    let result = legacy(
        &qd,
        "bt",
        "search/batch",
        json!({"searches": [{"vector": qa, "limit": 3}, {"vector": qb, "limit": 4}]}),
    )
    .await;
    let lists = result.as_array().expect("lists");
    assert_eq!(lists.len(), 2);
    assert_hits(
        &hits(&lists[0]),
        &brute(Distance::Dot, &qa, &points, 3),
        1e-5,
    );
    assert_hits(
        &hits(&lists[1]),
        &brute(Distance::Dot, &qb, &points, 4),
        1e-5,
    );
    let reco = json!({"positive": [1], "negative": [2], "strategy": "best_score", "limit": 5});
    let single = legacy(&qd, "bt", "recommend", reco.clone()).await;
    let result = legacy(
        &qd,
        "bt",
        "recommend/batch",
        json!({"searches": [reco, {"positive": [3], "limit": 2}]}),
    )
    .await;
    assert_eq!(result[0], single);
    assert_eq!(result[1].as_array().expect("list").len(), 2);
    let disc = json!({"target": 4, "context": [{"positive": 5, "negative": 6}], "limit": 5});
    let single = legacy(&qd, "bt", "discover", disc.clone()).await;
    let result = legacy(
        &qd,
        "bt",
        "discover/batch",
        json!({"searches": [disc, {"context": [{"positive": 7, "negative": 8}], "limit": 3}]}),
    )
    .await;
    assert_eq!(result[0], single);
    assert_eq!(result[1].as_array().expect("list").len(), 3);
    // One failing request fails the batch.
    let (status, reply) = qd
        .post(
            "/collections/bt/points/search/batch",
            Some(json!({"searches": [{"vector": qa, "limit": 3}, {"vector": [1.0], "limit": 3}]})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
}

#[tokio::test]
async fn legacy_recommend_defaults_to_average_vector() {
    let qd = Qd::start().await;
    random_collection(&qd, "ld", Distance::Cosine, 4, 60, 206).await;
    let want = query(
        &qd,
        "ld",
        json!({"query": {"recommend": {"positive": [1, 2], "negative": [3], "strategy": "average_vector"}}, "limit": 6}),
    )
    .await;
    let got = legacy(
        &qd,
        "ld",
        "recommend",
        json!({"positive": [1, 2], "negative": [3], "limit": 6}),
    )
    .await;
    assert_eq!(got, want);
    // The other legacy forms equal their query forms too.
    for (route, legacy_body, query_body) in [
        (
            "recommend",
            json!({"positive": [1], "negative": [2, 3], "strategy": "sum_scores", "limit": 5, "offset": 1, "score_threshold": -1.0}),
            json!({"query": {"recommend": {"positive": [1], "negative": [2, 3], "strategy": "sum_scores"}}, "limit": 5, "offset": 1, "score_threshold": -1.0}),
        ),
        (
            "discover",
            json!({"target": 1, "context": [{"positive": 2, "negative": 3}], "limit": 5}),
            json!({"query": {"discover": {"target": 1, "context": [{"positive": 2, "negative": 3}]}}, "limit": 5}),
        ),
        (
            "discover",
            json!({"context": [{"positive": 2, "negative": 3}], "limit": 5, "with_payload": true}),
            json!({"query": {"context": [{"positive": 2, "negative": 3}]}, "limit": 5, "with_payload": true}),
        ),
    ] {
        let got = legacy(&qd, "ld", route, legacy_body).await;
        assert_eq!(got, query(&qd, "ld", query_body).await, "{route}");
    }
    let (status, reply) = qd
        .post(
            "/collections/ld/points/recommend",
            Some(json!({"negative": [3], "limit": 3})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error(&reply), "Wrong input: No positive examples given");
    // The legacy discover needs a target or a pair (Qdrant's `discovery.rs`),
    // though the universal query accepts an empty context.
    for body in [json!({"limit": 3}), json!({"context": [], "limit": 3})] {
        let (status, reply) = qd
            .post("/collections/ld/points/discover", Some(body.clone()))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            error(&reply),
            "Wrong input: target and/or context_pairs must be specified"
        );
    }
}

/// PR #53 review: the legacy REST bodies carry `shard_key` and refuse it
/// with 501, as the universal query and the gRPC methods do.
#[tokio::test]
async fn legacy_routes_refuse_shard_keys() {
    let qd = Qd::start().await;
    random_collection(&qd, "sk", Distance::Dot, 2, 10, 210).await;
    for (route, body) in [
        (
            "search",
            json!({"vector": [1.0, 0.0], "limit": 3, "shard_key": "a"}),
        ),
        (
            "search/batch",
            json!({"searches": [{"vector": [1.0, 0.0], "limit": 3, "shard_key": "a"}]}),
        ),
        (
            "recommend",
            json!({"positive": [1], "limit": 3, "shard_key": "a"}),
        ),
        (
            "discover",
            json!({"target": 1, "limit": 3, "shard_key": "a"}),
        ),
        (
            "search/groups",
            json!({"vector": [1.0, 0.0], "group_by": "g", "limit": 2, "group_size": 1, "shard_key": "a"}),
        ),
        (
            "recommend/groups",
            json!({"positive": [1], "group_by": "g", "limit": 2, "group_size": 1, "shard_key": "a"}),
        ),
    ] {
        let (status, reply) = qd
            .post(&format!("/collections/sk/points/{route}"), Some(body))
            .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{route}: {reply}");
        assert_eq!(error(&reply), "Unsupported in Loams: shard_key", "{route}");
    }
}

fn id(n: u64) -> pb::PointId {
    pb::PointId {
        point_id_options: Some(pb::point_id::PointIdOptions::Num(n)),
    }
}

fn example(n: u64) -> pb::VectorExample {
    pb::VectorExample {
        example: Some(pb::vector_example::Example::Id(id(n))),
    }
}

#[tokio::test]
async fn grpc_legacy_methods_match_rest() {
    let qd = Qd::start().await;
    ab_random(&qd, "g").await;
    let mut client = qd.points().await;
    let q = vec![0.3_f32, -0.6, 0.2, 0.5];
    let search = pb::SearchPoints {
        collection_name: "g".into(),
        vector: q.clone(),
        vector_name: Some("a".into()),
        limit: 6,
        offset: Some(1),
        score_threshold: Some(-0.5),
        ..Default::default()
    };
    let rest = hits(&legacy(&qd, "g", "search", json!({"vector": {"name": "a", "vector": q}, "limit": 6, "offset": 1, "score_threshold": -0.5})).await);
    let got = client
        .search(search.clone())
        .await
        .expect("Search")
        .into_inner();
    assert_hits(&grpc_hits(&got.result), &rest, 1e-6);
    let got = client
        .search_batch(pb::SearchBatchPoints {
            collection_name: "g".into(),
            search_points: vec![search.clone(), search],
            ..Default::default()
        })
        .await
        .expect("SearchBatch")
        .into_inner();
    assert_eq!(got.result.len(), 2);
    assert_hits(&grpc_hits(&got.result[1].result), &rest, 1e-6);
    // Recommend: ids and vectors, a strategy, `using`.
    let recommend = pb::RecommendPoints {
        collection_name: "g".into(),
        positive: vec![id(1), id(2)],
        negative_vectors: vec![pb::Vector {
            vector: Some(pb::vector::Vector::Dense(pb::DenseVector {
                data: q.clone(),
            })),
            ..Default::default()
        }],
        strategy: Some(pb::RecommendStrategy::BestScore as i32),
        using: Some("b".into()),
        limit: 5,
        ..Default::default()
    };
    let rest = hits(&legacy(&qd, "g", "recommend", json!({"positive": [1, 2], "negative": [q], "strategy": "best_score", "using": "b", "limit": 5})).await);
    let got = client
        .recommend(recommend.clone())
        .await
        .expect("Recommend")
        .into_inner();
    assert_hits(&grpc_hits(&got.result), &rest, 1e-6);
    let got = client
        .recommend_batch(pb::RecommendBatchPoints {
            collection_name: "g".into(),
            recommend_points: vec![recommend],
            ..Default::default()
        })
        .await
        .expect("RecommendBatch")
        .into_inner();
    assert_hits(&grpc_hits(&got.result[0].result), &rest, 1e-6);
    // Discover: a target and pairs, then pairs only (context).
    let discover = pb::DiscoverPoints {
        collection_name: "g".into(),
        target: Some(pb::TargetVector {
            target: Some(pb::target_vector::Target::Single(example(4))),
        }),
        context: vec![pb::ContextExamplePair {
            positive: Some(example(5)),
            negative: Some(example(6)),
        }],
        using: Some("a".into()),
        limit: 4,
        ..Default::default()
    };
    let rest = hits(&legacy(&qd, "g", "discover", json!({"target": 4, "context": [{"positive": 5, "negative": 6}], "using": "a", "limit": 4})).await);
    let got = client
        .discover(discover.clone())
        .await
        .expect("Discover")
        .into_inner();
    assert_hits(&grpc_hits(&got.result), &rest, 1e-6);
    let context = pb::DiscoverPoints {
        target: None,
        ..discover.clone()
    };
    let rest_context = hits(
        &legacy(
            &qd,
            "g",
            "discover",
            json!({"context": [{"positive": 5, "negative": 6}], "using": "a", "limit": 4}),
        )
        .await,
    );
    let got = client
        .discover_batch(pb::DiscoverBatchPoints {
            collection_name: "g".into(),
            discover_points: vec![discover, context],
            ..Default::default()
        })
        .await
        .expect("DiscoverBatch")
        .into_inner();
    assert_hits(&grpc_hits(&got.result[0].result), &rest, 1e-6);
    assert_hits(&grpc_hits(&got.result[1].result), &rest_context, 1e-6);
    // The universal query's gateway-scored kinds over gRPC.
    let got = client
        .query(pb::QueryPoints {
            collection_name: "g".into(),
            query: Some(pb::Query {
                variant: Some(pb::query::Variant::NearestWithMmr(
                    pb::NearestInputWithMmr {
                        nearest: Some(dense_input(&q)),
                        mmr: Some(pb::Mmr {
                            diversity: Some(0.3),
                            candidates_limit: Some(20),
                        }),
                    },
                )),
            }),
            using: Some("a".into()),
            limit: Some(5),
            ..Default::default()
        })
        .await
        .expect("Query mmr")
        .into_inner();
    let rest = hits(&query(&qd, "g", json!({"query": {"nearest": q, "mmr": {"diversity": 0.3, "candidates_limit": 20}}, "using": "a", "limit": 5})).await);
    assert_hits(&grpc_hits(&got.result), &rest, 1e-6);
    // Errors keep their codes.
    let err = client
        .recommend(pb::RecommendPoints {
            collection_name: "g".into(),
            negative: vec![id(1)],
            using: Some("a".into()),
            limit: 3,
            ..Default::default()
        })
        .await
        .expect_err("no positive");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = client
        .discover(pb::DiscoverPoints {
            collection_name: "g".into(),
            target: Some(pb::TargetVector { target: None }),
            limit: 3,
            ..Default::default()
        })
        .await
        .expect_err("empty target");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = client
        .discover(pb::DiscoverPoints {
            collection_name: "g".into(),
            limit: 3,
            ..Default::default()
        })
        .await
        .expect_err("neither target nor pairs");
    assert_eq!(
        (err.code(), err.message()),
        (
            tonic::Code::InvalidArgument,
            "Wrong input: target and/or context_pairs must be specified"
        )
    );
}
