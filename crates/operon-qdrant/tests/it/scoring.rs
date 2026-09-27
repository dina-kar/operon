//! The read-side scoring reference (plan M1.4 Task 7, Rulings 7–9 and
//! 21): Qdrant's RRF and DBSF, raw similarities, score conversion and
//! thresholds, the sparse reference scorer, and cosine normalization.

use operon_collection::{Distance, PrimaryKey, SparseVector};
use operon_qdrant::query::ScoreKind;
use operon_qdrant::scoring::{
    cosine_normalize, dbsf, passes_threshold, raw_similarity, rrf, sparse_reference,
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
