//! Search assembly (plan M1.2 Task 7): fusion, document fetch, tail merge,
//! the planner, paging, groups, get, count and scroll.

use operon_collection::PrimaryKey;
use operon_query::Fusion;
use operon_query::exec::{Ranked, fuse};

// ----- fusion -----

fn ranked(pk: u64, score: f32) -> Ranked {
    Ranked {
        row_id: pk,
        pk: PrimaryKey::U64(pk),
        score,
        sort: Vec::new(),
    }
}

/// A = [d1 0.9, d2 0.8, d3 0.7], B = [d3 10, d1 5] (M1.4 S1, S2).
fn lists() -> Vec<Vec<Ranked>> {
    vec![
        vec![ranked(1, 0.9), ranked(2, 0.8), ranked(3, 0.7)],
        vec![ranked(3, 10.0), ranked(1, 5.0)],
    ]
}

fn scores(fused: &[Ranked]) -> Vec<(u64, f32)> {
    fused
        .iter()
        .map(|hit| match hit.pk {
            PrimaryKey::U64(pk) => (pk, hit.score),
            _ => panic!("u64 keys"),
        })
        .collect()
}

fn assert_close(actual: &[(u64, f32)], expected: &[(u64, f32)]) {
    assert_eq!(actual.len(), expected.len(), "{actual:?}");
    for ((pk, score), (want_pk, want)) in actual.iter().zip(expected) {
        assert_eq!(pk, want_pk, "{actual:?}");
        assert!((score - want).abs() <= 1e-7, "{pk}: {score} vs {want}");
    }
}

#[test]
fn rrf_golden_values() {
    let fused = fuse(&lists(), &Fusion::Rrf { k: 60 });
    assert_close(
        &scores(&fused),
        &[(1, 0.032522473), (3, 0.03226646), (2, 0.016129032)],
    );
    let fused = fuse(&lists(), &Fusion::Rrf { k: 1 });
    assert_close(
        &scores(&fused),
        &[(1, 0.8333334), (3, 0.75), (2, 0.33333334)],
    );
}

#[test]
fn dbsf_golden_values() {
    let fused = fuse(&lists(), &Fusion::Dbsf);
    assert_close(
        &scores(&fused),
        &[(1, 1.0488155), (3, 0.9511845), (2, 0.50000006)],
    );
    // A list of one hit normalizes to 0.5.
    let fused = fuse(&[lists()[0].clone(), vec![ranked(4, 3.0)]], &Fusion::Dbsf);
    let d4 = scores(&fused)
        .into_iter()
        .find(|(pk, _)| *pk == 4)
        .expect("d4");
    assert_eq!(d4.1, 0.5);
}

#[test]
fn dbsf_is_not_clamped() {
    let mut outlier: Vec<Ranked> = vec![ranked(1, 100.0)];
    outlier.extend((2..=20).map(|pk| ranked(pk, 1.0)));
    let fused = fuse(&[outlier], &Fusion::Dbsf);
    assert_eq!(scores(&fused)[0].0, 1);
    assert!(fused[0].score > 1.0, "{}", fused[0].score);
}

#[test]
fn weighted_sum_golden_values() {
    let fused = fuse(
        &lists(),
        &Fusion::WeightedSum {
            weights: vec![2.0, 0.5],
        },
    );
    assert_close(&scores(&fused), &[(3, 6.4), (1, 4.3), (2, 1.6)]);
}
