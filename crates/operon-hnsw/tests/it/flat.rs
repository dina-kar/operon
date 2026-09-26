//! The exact engine (plan M1.3 Task 3 rule 4).

use std::path::Path;
use std::sync::Arc;

use operon_hnsw::{
    BuildSpec, Distance, FLAT_ENGINE, FLAT_FILE, FlatEngine, HnswEngine, HnswError, HnswIndex,
    IdFilter, Point, SearchParams, default_engine, engine_by_name, exact_score,
};
use roaring::RoaringTreemap;
use tempfile::TempDir;

fn points(vectors: &[(u64, [f32; 2])]) -> Vec<Point> {
    vectors
        .iter()
        .map(|(id, v)| Point::new(*id, v.to_vec()))
        .collect()
}

/// Builds `points` with the flat engine into `dir` and opens the result.
fn build_in(spec: &BuildSpec, points: Vec<Point>, dir: &Path) -> Arc<dyn HnswIndex> {
    let mut builder = FlatEngine.builder(spec, dir).expect("builder");
    builder.add(points).expect("add");
    let built = builder.finish(dir).expect("finish");
    assert_eq!(built.engine, FLAT_ENGINE);
    FlatEngine.open(spec, dir).expect("open")
}

type Hits = Vec<(u64, f32)>;

fn search(index: &dyn HnswIndex, query: &[f32], k: usize) -> Hits {
    index
        .search(query, k, IdFilter::All, SearchParams::default())
        .expect("search")
}

const FOUR: [(u64, [f32; 2]); 4] = [
    (1, [1.0, 0.0]),
    (2, [0.0, 1.0]),
    (3, [3.0, 4.0]),
    (4, [-1.0, 0.0]),
];

#[test]
fn flat_search_is_exact_for_every_distance() {
    assert_eq!(
        exact_score(Distance::Euclid, &[0.0, 0.0], &[3.0, 4.0]),
        -5.0
    );
    assert_eq!(
        exact_score(Distance::Manhattan, &[0.0, 0.0], &[3.0, 4.0]),
        -7.0
    );
    assert_eq!(exact_score(Distance::Dot, &[1.0, 2.0], &[3.0, 4.0]), 11.0);
    assert_eq!(exact_score(Distance::Cosine, &[0.0, 0.0], &[3.0, 4.0]), 0.0);

    let cases: [(Distance, [f32; 2], Hits); 4] = [
        (
            Distance::Cosine,
            [1.0, 0.0],
            vec![(1, 1.0), (3, 0.6), (2, 0.0), (4, -1.0)],
        ),
        (
            Distance::Dot,
            [1.0, 0.0],
            vec![(3, 3.0), (1, 1.0), (2, 0.0), (4, -1.0)],
        ),
        (
            Distance::Euclid,
            [1.0, 1.0],
            vec![
                (1, -1.0),
                (2, -1.0),
                (4, -(5.0f32).sqrt()),
                (3, -(13.0f32).sqrt()),
            ],
        ),
        (
            Distance::Manhattan,
            [1.0, 1.0],
            vec![(1, -1.0), (2, -1.0), (4, -3.0), (3, -5.0)],
        ),
    ];
    for (distance, query, expected) in cases {
        let dir = TempDir::new().expect("temp dir");
        let spec = BuildSpec::new(2, distance);
        let index = build_in(&spec, points(&FOUR), dir.path());
        let hits = search(index.as_ref(), &query, 10);
        assert_eq!(hits.len(), expected.len(), "{distance:?}");
        for ((id, score), (want_id, want_score)) in hits.iter().zip(&expected) {
            assert_eq!(id, want_id, "{distance:?}: {hits:?}");
            assert!(
                (score - want_score).abs() < 1e-6,
                "{distance:?}: {hits:?} vs {expected:?}"
            );
        }
        assert_eq!(search(index.as_ref(), &query, 2).len(), 2);
    }
}

#[test]
fn flat_files_round_trip() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(2, Distance::Dot);
    let mut builder = FlatEngine.builder(&spec, dir.path()).expect("builder");
    builder.add(points(&FOUR[..2])).expect("add");
    // A later point with the same id replaces the earlier one.
    builder
        .add(points(&[
            (2, [0.0, 2.0]),
            (3, [3.0, 4.0]),
            (4, [-1.0, 0.0]),
        ]))
        .expect("add");
    let out = dir.path().join("out");
    std::fs::create_dir(&out).expect("mkdir");
    let built = builder.finish(&out).expect("finish");
    assert_eq!(built.engine, FLAT_ENGINE);
    assert_eq!(built.files, vec![FLAT_FILE.to_owned()]);
    assert_eq!(built.points, 4);

    // The files open anywhere they are copied to.
    let copy = TempDir::new().expect("temp dir");
    std::fs::copy(out.join(FLAT_FILE), copy.path().join(FLAT_FILE)).expect("copy");
    let index = FlatEngine.open(&spec, copy.path()).expect("open");
    assert_eq!(index.len(), 4);
    assert_eq!(
        search(index.as_ref(), &[0.0, 1.0], 10),
        vec![(3, 4.0), (2, 2.0), (1, 0.0), (4, 0.0)]
    );

    assert_eq!(
        engine_by_name(FLAT_ENGINE).expect("flat").name(),
        FLAT_ENGINE
    );
    assert!(engine_by_name("hnswlib").is_none());
    assert!(!default_engine().name().is_empty());
}

#[test]
fn a_flipped_byte_is_corrupt() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(2, Distance::Cosine);
    build_in(&spec, points(&FOUR), dir.path());
    let path = dir.path().join(FLAT_FILE);
    let good = std::fs::read(&path).expect("read");

    // Every byte: magic, version, dim, distance, count, ids, vectors, crc.
    for i in 0..good.len() {
        let mut bad = good.clone();
        bad[i] ^= 0x01;
        std::fs::write(&path, &bad).expect("write");
        let err = FlatEngine.open(&spec, dir.path()).expect_err("corrupt");
        assert!(matches!(err, HnswError::Corrupt(_)), "byte {i}: {err}");
    }
    std::fs::write(&path, &good[..good.len() - 1]).expect("write");
    let err = FlatEngine.open(&spec, dir.path()).expect_err("truncated");
    assert!(matches!(err, HnswError::Corrupt(_)), "{err}");

    // An intact file of another dim or distance is not this index.
    std::fs::write(&path, &good).expect("write");
    for other in [
        BuildSpec::new(3, Distance::Cosine),
        BuildSpec::new(2, Distance::Dot),
    ] {
        let err = FlatEngine.open(&other, dir.path()).expect_err("mismatch");
        assert!(matches!(err, HnswError::Corrupt(_)), "{other:?}: {err}");
    }
    FlatEngine.open(&spec, dir.path()).expect("intact");
}

#[test]
fn id_filters_restrict_results() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(2, Distance::Euclid);
    let all: Vec<(u64, [f32; 2])> = (0..20).map(|i| (i, [i as f32, 0.0])).collect();
    let index = build_in(&spec, points(&all), dir.path());

    let only: RoaringTreemap = [3u64, 7, 15].into_iter().collect();
    let hits = index
        .search(
            &[0.0, 0.0],
            10,
            IdFilter::Only(&only),
            SearchParams::default(),
        )
        .expect("search");
    assert_eq!(hits.iter().map(|h| h.0).collect::<Vec<_>>(), vec![3, 7, 15]);

    let except: RoaringTreemap = (0..5).collect();
    let hits = index
        .search(
            &[0.0, 0.0],
            3,
            IdFilter::Except(&except),
            SearchParams::default(),
        )
        .expect("search");
    assert_eq!(hits.iter().map(|h| h.0).collect::<Vec<_>>(), vec![5, 6, 7]);

    let empty = RoaringTreemap::new();
    let hits = index
        .search(
            &[0.0, 0.0],
            3,
            IdFilter::Only(&empty),
            SearchParams::default(),
        )
        .expect("search");
    assert!(hits.is_empty());
}

#[test]
fn ties_order_by_id() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(2, Distance::Dot);
    let tied: Vec<(u64, [f32; 2])> = [9u64, 2, 30, 5, 17]
        .into_iter()
        .map(|id| (id, [1.0, 1.0]))
        .chain([(1, [0.0, 0.5])])
        .collect();
    let index = build_in(&spec, points(&tied), dir.path());
    let ids = |k| {
        search(index.as_ref(), &[1.0, 1.0], k)
            .into_iter()
            .map(|h| h.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(10), vec![2, 5, 9, 17, 30, 1]);
    // A cut inside a tie keeps the smallest ids.
    assert_eq!(ids(3), vec![2, 5, 9]);
}

#[test]
fn appendable_flat_sees_appended_points() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(2, Distance::Dot);
    let index = FlatEngine
        .appendable(&spec, dir.path())
        .expect("appendable");
    assert_eq!(index.len(), 0);
    assert!(search(index.as_ref(), &[1.0, 0.0], 5).is_empty());

    index.append(points(&FOUR[..2])).expect("append");
    assert_eq!(search(index.as_ref(), &[1.0, 0.0], 1), vec![(1, 1.0)]);
    index.optimize().expect("optimize");
    index.append(points(&FOUR[2..])).expect("append");
    assert_eq!(index.len(), 4);
    assert_eq!(search(index.as_ref(), &[1.0, 0.0], 1), vec![(3, 3.0)]);
    // An append with a known id replaces that point.
    index.append(points(&[(3, [0.0, 0.0])])).expect("append");
    assert_eq!(index.len(), 4);
    assert_eq!(search(index.as_ref(), &[1.0, 0.0], 1), vec![(1, 1.0)]);
}

#[test]
fn invalid_vectors_are_rejected() {
    let dir = TempDir::new().expect("temp dir");
    for dim in [0, operon_common::schema::MAX_VECTOR_DIM + 1] {
        let err = FlatEngine
            .builder(&BuildSpec::new(dim, Distance::Dot), dir.path())
            .err()
            .expect("bad dim");
        assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    }

    let spec = BuildSpec::new(2, Distance::Dot);
    let bad_points = [
        Point::new(1, vec![1.0]),
        Point::new(1, vec![1.0, 2.0, 3.0]),
        Point::new(1, vec![1.0, f32::NAN]),
        Point::new(1, vec![f32::INFINITY, 0.0]),
    ];
    let mut builder = FlatEngine.builder(&spec, dir.path()).expect("builder");
    let appendable = FlatEngine
        .appendable(&spec, dir.path())
        .expect("appendable");
    for point in bad_points {
        let err = builder.add(vec![point.clone()]).expect_err("builder add");
        assert!(matches!(err, HnswError::Invalid(_)), "{err}");
        let err = appendable.append(vec![point]).expect_err("append");
        assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    }
    assert_eq!(appendable.len(), 0);

    appendable.append(points(&FOUR)).expect("append");
    for query in [&[1.0][..], &[1.0, 2.0, 3.0], &[f32::NAN, 0.0]] {
        let err = appendable
            .search(query, 3, IdFilter::All, SearchParams::default())
            .expect_err("bad query");
        assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    }
    assert!(search(appendable.as_ref(), &[1.0, 0.0], 0).is_empty());
}
