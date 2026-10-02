//! The read-side scoring reference (plan M1.4 Task 7, Rulings 7–9 and
//! 21): Qdrant's RRF and DBSF, raw similarities, score conversion and
//! thresholds, the sparse reference scorer, and cosine normalization.
//! Task 8 (Ruling 10): the recommend, discover and context scores, MMR and
//! the candidate bound.

use loams_collection::{Distance, PrimaryKey, SparseVector};
use loams_qdrant::query::ScoreKind;
use loams_qdrant::scoring::{
    average_vector, best_score, candidate_k, context_score, cosine_normalize, dbsf, discover_score,
    mmr_select, passes_threshold, raw_similarity, rrf, sparse_reference, sum_scores,
    to_qdrant_score,
};

fn pk(n: u64) -> PrimaryKey {
    PrimaryKey::U64(n)
}

fn close(a: f32, b: f32) -> bool {
    (a - b).abs() < 1e-6
}

fn assert_list(got: &[(PrimaryKey, f32)], want: &[(u64, f32)]) {
    assert_eq!(got.len(), want.len(), "{got:?}");
    for ((pk_got, s_got), (id, s_want)) in got.iter().zip(want) {
        assert_eq!(pk_got, &pk(*id), "{got:?}");
        assert!(close(*s_got, *s_want), "{got:?} vs {want:?}");
    }
}

#[test]
fn rrf_reference_matches_qdrant_unit_values() {
    let (a, b, c) = (1, 2, 3);
    let got = rrf(&[vec![pk(a), pk(b)], vec![pk(b), pk(c)]], 2);
    assert_list(
        &got,
        &[(b, 1.0 / 3.0 + 1.0 / 2.0), (a, 1.0 / 2.0), (c, 1.0 / 3.0)],
    );
    // Equal scores are ordered by key.
    let got = rrf(&[vec![pk(9)], vec![pk(4)]], 2);
    assert_list(&got, &[(4, 0.5), (9, 0.5)]);
}

#[test]
fn dbsf_reference_matches_qdrant_unit_values() {
    // One point scores 0.5.
    assert_list(&dbsf(&[vec![(pk(1), 7.0)]]), &[(1, 0.5)]);
    // All-equal scores give 0.5 everywhere.
    assert_list(
        &dbsf(&[vec![(pk(1), 2.0), (pk(2), 2.0), (pk(3), 2.0)]]),
        &[(1, 0.5), (2, 0.5), (3, 0.5)],
    );
    // [1, 2, 3]: μ = 2, σ = 1 (n − 1), so (s − (μ − 3σ)) / 6σ = (s + 1) / 6.
    let one = vec![(pk(1), 1.0), (pk(2), 2.0), (pk(3), 3.0)];
    assert_list(
        &dbsf(std::slice::from_ref(&one)),
        &[(3, 4.0 / 6.0), (2, 3.0 / 6.0), (1, 2.0 / 6.0)],
    );
    // Two lists are summed per point.
    assert_list(
        &dbsf(&[one, vec![(pk(2), 5.0)]]),
        &[(2, 3.0 / 6.0 + 0.5), (3, 4.0 / 6.0), (1, 2.0 / 6.0)],
    );
}

#[test]
fn raw_similarity_per_distance() {
    assert_eq!(
        raw_similarity(Distance::Euclid, &[0.0, 0.0], &[3.0, 4.0]),
        -25.0
    );
    assert_eq!(
        raw_similarity(Distance::Manhattan, &[0.0, 0.0], &[3.0, 4.0]),
        -7.0
    );
    assert_eq!(
        raw_similarity(Distance::Dot, &[1.0, 2.0], &[3.0, 4.0]),
        11.0
    );
    // Cosine normalizes both first.
    assert!(close(
        raw_similarity(Distance::Cosine, &[3.0, 4.0], &[6.0, 8.0]),
        1.0
    ));
    assert!(close(
        raw_similarity(Distance::Cosine, &[1.0, 0.0], &[0.0, 5.0]),
        0.0
    ));
}

#[test]
fn score_conversion_and_thresholds() {
    use Distance::{Cosine, Dot, Euclid, Manhattan};
    let cases: &[(Distance, ScoreKind, f32, f32, f32, bool)] = &[
        (Cosine, ScoreKind::Distance, 0.95, 0.9, 0.95, true),
        (Cosine, ScoreKind::Distance, 0.9, 0.9, 0.9, false),
        (Dot, ScoreKind::Distance, 3.0, 2.0, 3.0, true),
        (Euclid, ScoreKind::Distance, -0.5, 1.0, 0.5, true),
        (Euclid, ScoreKind::Distance, -1.0, 1.0, 1.0, false),
        (Manhattan, ScoreKind::Distance, -2.0, 3.0, 2.0, true),
        (Manhattan, ScoreKind::Distance, -4.0, 3.0, 4.0, false),
        // Fusion keeps `>=`, whatever the vectors' distance.
        (Dot, ScoreKind::Fusion, 0.5, 0.5, 0.5, true),
        (Euclid, ScoreKind::Fusion, 0.4, 0.5, 0.4, false),
        // Custom scores are not converted, but follow the distance order.
        (Euclid, ScoreKind::Custom, 0.7, 0.8, 0.7, true),
        (Cosine, ScoreKind::Custom, 0.7, 0.8, 0.7, false),
        // No query: every score is 0.0 and no threshold applies.
        (Dot, ScoreKind::Filter, 3.0, 1.0, 0.0, true),
        // A NaN from the IR is 0.0.
        (Cosine, ScoreKind::Distance, f32::NAN, 0.5, 0.0, false),
    ];
    for (distance, kind, ir, t, score, passes) in cases {
        let got = to_qdrant_score(*distance, kind, *ir);
        assert_eq!(got, *score, "{distance:?} {kind:?} {ir}");
        assert_eq!(
            passes_threshold(*distance, kind, got, *t),
            *passes,
            "{distance:?} {kind:?} {ir} {t}"
        );
    }
}

#[test]
fn cosine_normalize_matches_qdrant_edge_cases() {
    let mut v = [3.0_f32, 4.0];
    cosine_normalize(&mut v);
    assert!(close(v[0], 0.6) && close(v[1], 0.8));
    // Below f32::EPSILON squared length: unchanged.
    let mut tiny = [1.0e-4_f32, 0.0];
    cosine_normalize(&mut tiny);
    assert_eq!(tiny, [1.0e-4, 0.0]);
    // Within 1e-6 of unit length: unchanged.
    let mut near = [1.0_f32 + 2.0e-7, 0.0];
    cosine_normalize(&mut near);
    assert_eq!(near, [1.0 + 2.0e-7, 0.0]);
    let mut zero = [0.0_f32; 3];
    cosine_normalize(&mut zero);
    assert_eq!(zero, [0.0; 3]);
}

fn sv(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("sparse")
}

#[test]
fn sparse_reference_scores_overlap_only_with_idf() {
    let docs = vec![
        (pk(1), sv(&[1, 2], &[1.0, 2.0])),
        (pk(2), sv(&[2], &[0.0])),
        (pk(3), sv(&[7], &[5.0])),
        (pk(4), sv(&[], &[])),
    ];
    let query = sv(&[2, 9], &[3.0, 1.0]);
    // Plain dot: 1 scores 6, 2 shares index 2 with a zero weight (0), 3 and
    // 4 share nothing.
    let got = sparse_reference(&docs, &query, false, 10);
    assert_list(&got, &[(1, 6.0), (2, 0.0)]);
    // IDF over the three non-empty vectors: index 2 has df 2, index 9 df 0.
    let idf2 = ((3.0_f32 - 2.0 + 0.5) / (2.0 + 0.5) + 1.0).ln();
    let got = sparse_reference(&docs, &query, true, 1);
    assert_list(&got, &[(1, 3.0 * idf2 * 2.0)]);
    assert!(sparse_reference(&docs, &sv(&[], &[]), false, 10).is_empty());
}

// ----- Task 8: gateway-scored queries -----

/// Qdrant's `scaled_fast_sigmoid`.
fn sig(x: f32) -> f32 {
    0.5 * (x / (1.0 + x.abs()) + 1.0)
}

#[test]
fn average_vector_matches_qdrant() {
    let pos = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
    // avg(pos) + avg(pos) − avg(neg) = [0.5, 0.5] × 2 − [1, 1].
    assert_eq!(
        average_vector(&pos, &[vec![1.0, 1.0]]).expect("average"),
        vec![0.0, 0.0]
    );
    // No negatives: the plain average.
    assert_eq!(average_vector(&pos, &[]).expect("average"), vec![0.5, 0.5]);
    assert_eq!(
        average_vector(&[vec![1.0, 3.0]], &[vec![3.0, 1.0], vec![1.0, 1.0]]).expect("average"),
        vec![0.0, 5.0]
    );
    assert!(average_vector(&[], &[vec![1.0, 0.0]]).is_err());
}

#[test]
fn best_score_signs() {
    let c = [1.0, 0.0];
    // Closer to the negative: −sig(max negative similarity).
    let s = best_score(Distance::Dot, &c, &[vec![0.0, 1.0]], &[vec![1.0, 0.0]]);
    assert!(close(s, -sig(1.0)) && s < 0.0, "{s}");
    // Closer to a positive: sig(max positive similarity).
    let s = best_score(
        Distance::Dot,
        &c,
        &[vec![2.0, 0.0], vec![0.5, 0.0]],
        &[vec![0.0, 1.0]],
    );
    assert!(close(s, sig(2.0)), "{s}");
    // An empty list counts as −∞.
    assert!(close(
        best_score(Distance::Dot, &c, &[vec![-3.0, 0.0]], &[]),
        sig(-3.0)
    ));
    assert!(close(
        best_score(Distance::Dot, &c, &[], &[vec![-3.0, 0.0]]),
        -sig(-3.0)
    ));
    // Equal similarities are not `p > n`: the negative wins.
    let s = best_score(Distance::Euclid, &c, &[vec![1.0, 1.0]], &[vec![1.0, -1.0]]);
    assert!(close(s, -sig(-1.0)), "{s}");
}

#[test]
fn sum_scores_matches_the_formula() {
    // Euclid similarities: −25 − 1 for the positives, −4 for the negative.
    let s = sum_scores(
        Distance::Euclid,
        &[0.0, 0.0],
        &[vec![3.0, 4.0], vec![1.0, 0.0]],
        &[vec![0.0, 2.0]],
    );
    assert_eq!(s, -26.0 - -4.0);
    assert_eq!(
        sum_scores(Distance::Dot, &[1.0, 2.0], &[vec![1.0, 1.0]], &[]),
        3.0
    );
}

#[test]
fn discover_rank_plus_sigmoid() {
    let c = [1.0, 0.0];
    let target = [2.0, 0.0];
    let closer_to_pos = (vec![1.0, 0.0], vec![0.0, 1.0]);
    let closer_to_neg = (vec![0.0, 1.0], vec![1.0, 0.0]);
    let tie = (vec![0.0, 5.0], vec![0.0, 5.0]);
    let s = discover_score(
        Distance::Dot,
        &c,
        &target,
        std::slice::from_ref(&closer_to_pos),
    );
    assert!(close(s, 1.0 + sig(2.0)), "{s}");
    let s = discover_score(
        Distance::Dot,
        &c,
        &target,
        &[closer_to_pos, closer_to_neg.clone(), tie],
    );
    assert!(close(s, sig(2.0)), "{s}");
    let s = discover_score(Distance::Dot, &c, &target, &[closer_to_neg]);
    assert!(close(s, -1.0 + sig(2.0)), "{s}");
    // No pairs: the sigmoid of the target similarity alone.
    assert!(close(
        discover_score(Distance::Dot, &c, &target, &[]),
        sig(2.0)
    ));
}

#[test]
fn context_loss_is_nonpositive() {
    let c = [1.0, 0.0];
    // Closer to the positive: min(1 − ε, 0) = 0.
    let happy = (vec![1.0, 0.0], vec![0.0, 1.0]);
    // Closer to the negative: x = 0 − 2 − ε, x / (1 + |x|).
    let sad = (vec![0.0, 1.0], vec![2.0, 0.0]);
    assert_eq!(
        context_score(Distance::Dot, &c, std::slice::from_ref(&happy)),
        0.0
    );
    let x = -2.0 - f32::EPSILON;
    let s = context_score(Distance::Dot, &c, &[happy, sad.clone()]);
    assert!(close(s, x / (1.0 + x.abs())), "{s}");
    // A tie still costs the margin.
    let tie = (vec![0.0, 1.0], vec![0.0, 1.0]);
    let s = context_score(Distance::Dot, &c, &[tie]);
    assert!(s < 0.0 && s > -1.0e-6, "{s}");
    let mut rng = 0x9E37_79B9_u32;
    for _ in 0..100 {
        let mut f = || {
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            (rng % 2000) as f32 / 1000.0 - 1.0
        };
        let c = [f(), f()];
        let pair = (vec![f(), f()], vec![f(), f()]);
        for d in [
            Distance::Cosine,
            Distance::Dot,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            assert!(context_score(d, &c, std::slice::from_ref(&pair)) <= 0.0);
        }
    }
}

fn unit(v: [f32; 2]) -> Vec<f32> {
    let mut v = v.to_vec();
    cosine_normalize(&mut v);
    v
}

#[test]
fn mmr_select_with_lambda_one_is_relevance_order() {
    let q = [1.0, 0.0];
    let candidates: Vec<(PrimaryKey, Vec<f32>)> = [[0.2, 1.0], [1.0, 0.1], [0.5, 0.5], [-1.0, 0.0]]
        .into_iter()
        .enumerate()
        .map(|(i, v)| (pk(i as u64), unit(v)))
        .collect();
    assert_eq!(
        mmr_select(Distance::Cosine, &q, &candidates, 1.0, 4),
        vec![1, 2, 0, 3]
    );
    assert_eq!(
        mmr_select(Distance::Cosine, &q, &candidates, 1.0, 2),
        vec![1, 2]
    );
    assert!(mmr_select(Distance::Cosine, &q, &candidates, 1.0, 0).is_empty());
    assert!(mmr_select(Distance::Cosine, &q, &[], 0.5, 3).is_empty());
}

#[test]
fn mmr_select_matches_a_hand_computed_example() {
    // q = [0.8, 0.6]; relevance: c0 [1, 0] 0.8, c1 [0.6, 0.8] 0.96,
    // c2 [0.96, 0.28] 0.936, c3 [0, 1] 0.6.
    let q = [0.8, 0.6];
    let candidates: Vec<(PrimaryKey, Vec<f32>)> =
        [[1.0, 0.0], [0.6, 0.8], [0.96, 0.28], [0.0, 1.0]]
            .into_iter()
            .enumerate()
            .map(|(i, v)| (pk(i as u64), v.to_vec()))
            .collect();
    // λ = 0.5. First the most relevant, c1. Then 0.5·rel − 0.5·max sim to
    // the picks: c0 0.4 − 0.3 = 0.1, c2 0.468 − 0.4 = 0.068, c3 0.3 − 0.4
    // = −0.1, so c0. Then c2 0.468 − 0.5·max(0.8, 0.96) = −0.012 beats c3
    // 0.3 − 0.5·max(0.8, 0) = −0.1; c3 last.
    assert_eq!(
        mmr_select(Distance::Cosine, &q, &candidates, 0.5, 4),
        vec![1, 0, 2, 3]
    );
    // Relevance alone would be c1, c2, c0, c3.
    assert_eq!(
        mmr_select(Distance::Cosine, &q, &candidates, 1.0, 4),
        vec![1, 2, 0, 3]
    );
}

#[test]
fn candidate_k_bounds() {
    assert_eq!(candidate_k(0, 10, 10_000), 100);
    assert_eq!(candidate_k(0, 50, 10_000), 200);
    assert_eq!(candidate_k(0, 5_000, 10_000), 10_000);
    assert_eq!(candidate_k(20, 30, 10_000), 200);
    assert_eq!(candidate_k(0, 10, 50), 50);
    assert_eq!(candidate_k(usize::MAX, 10, 10_000), 10_000);
}
