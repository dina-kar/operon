//! Gateway-scored queries end to end (plan M1.4 Task 8, Ruling 10):
//! `recommend`'s strategies, `discover`, `context` and MMR, against
//! brute-force references over every point.

use operon_collection::{Distance, PrimaryKey};
use operon_qdrant::scoring::{
    best_score, context_score, cosine_normalize, discover_score, mmr_select, sum_scores,
};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Qd;
use crate::query::{
    assert_hits, brute, create, error, hits, ids, query, query_raw, random_collection, upsert,
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
    let (status, reply) = query_raw(&qd, "c", json!({"query": {"context": []}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error(&reply),
        "Wrong input: Context query requires at least one pair"
    );
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
    let want = brute_scored(d, &points, &[], 40, |c| sum_scores(d, c, &[v.clone()], &[]));
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
