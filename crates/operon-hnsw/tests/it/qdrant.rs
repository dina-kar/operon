//! The qdrant-edge engine (plan M1.3 Task 3 rules 5–7).
#![cfg(feature = "qdrant")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use operon_hnsw::{
    BuildSpec, BuiltFiles, Distance, FlatEngine, HnswEngine, HnswError, HnswIndex, IdFilter,
    PayloadField, PayloadKind, PayloadValue, Point, QDRANT_ENGINE, QdrantEngine, Quantization,
    SearchParams, VECTOR_NAME, default_engine, edge_config, engine_by_name, exact_score,
};
use qdrant_edge::EdgeShardRead as _;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use roaring::RoaringTreemap;
use tempfile::TempDir;

fn vectors(seed: u64, n: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (0..n)
        .map(|_| (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect())
        .collect()
}

fn points(vectors: &[Vec<f32>]) -> Vec<Point> {
    vectors
        .iter()
        .enumerate()
        .map(|(id, v)| Point::new(id as u64, v.clone()))
        .collect()
}

/// A finished build: the temp dir, the output directory and what was built.
struct Built {
    _dir: TempDir,
    out: PathBuf,
    files: BuiltFiles,
}

fn build(spec: &BuildSpec, points: Vec<Point>) -> Built {
    let dir = TempDir::new().expect("temp dir");
    let work = dir.path().join("work");
    let out = dir.path().join("out");
    std::fs::create_dir(&out).expect("mkdir");
    let mut builder = QdrantEngine.builder(spec, &work).expect("builder");
    // Two batches, so the build sees more than one upsert.
    let mut points = points;
    let second = points.split_off(points.len() / 2);
    builder.add(points).expect("add");
    builder.add(second).expect("add");
    let files = builder.finish(&out).expect("finish");
    assert!(!work.join("shard").exists(), "the build shard is removed");
    Built {
        _dir: dir,
        out,
        files,
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy");
        }
    }
}

/// Every `segments/*/vector_index-v/<file>` of a built index.
fn index_files(out: &Path, file: &str) -> Vec<PathBuf> {
    std::fs::read_dir(out.join("segments"))
        .expect("segments")
        .map(|e| {
            e.expect("entry")
                .path()
                .join(format!("vector_index-{VECTOR_NAME}"))
                .join(file)
        })
        .filter(|p| p.exists())
        .collect()
}

fn top(index: &dyn HnswIndex, query: &[f32], k: usize, params: SearchParams) -> Vec<(u64, f32)> {
    index
        .search(query, k, IdFilter::All, params)
        .expect("search")
}

fn self_hits(index: &dyn HnswIndex, vectors: &[Vec<f32>], ids: impl Iterator<Item = u64>) -> usize {
    ids.filter(|&id| {
        top(index, &vectors[id as usize], 1, SearchParams::default())
            .first()
            .is_some_and(|hit| hit.0 == id)
    })
    .count()
}

#[test]
fn builds_publishes_and_opens_read_only() {
    let data = vectors(1, 3_000, 16);
    let spec = BuildSpec::new(16, Distance::Cosine);
    let built = build(&spec, points(&data));
    assert_eq!(built.files.engine, QDRANT_ENGINE);
    assert_eq!(built.files.points, 3_000);
    assert!(built.out.join("segments_manifest.json").is_file());
    assert!(!index_files(&built.out, "graph.bin").is_empty());
    let mut sorted = built.files.files.clone();
    sorted.sort();
    assert_eq!(built.files.files, sorted);
    assert!(
        built
            .files
            .files
            .contains(&"segments_manifest.json".to_owned())
    );
    assert!(
        built
            .files
            .files
            .iter()
            .all(|f| !f.starts_with('/') && !f.contains('\\'))
    );
    for file in &built.files.files {
        assert!(built.out.join(file).is_file(), "{file}");
    }

    let elsewhere = TempDir::new().expect("temp dir");
    copy_dir(&built.out, elsewhere.path());
    let index = QdrantEngine.open(&spec, elsewhere.path()).expect("open");
    assert_eq!(index.len(), 3_000);
    assert_eq!(self_hits(index.as_ref(), &data, (0..3_000).step_by(60)), 50);

    assert_eq!(
        engine_by_name(QDRANT_ENGINE).expect("qdrant").name(),
        QDRANT_ENGINE
    );
    assert_eq!(default_engine().name(), QDRANT_ENGINE);
}

#[test]
fn recall_at_10_against_flat_is_at_least_0_95() {
    let data = vectors(2, 5_000, 32);
    let mut spec = BuildSpec::new(32, Distance::Cosine);
    spec.hnsw.m = 16;
    spec.hnsw.ef_construct = 100;
    let built = build(&spec, points(&data));
    let index = QdrantEngine.open(&spec, &built.out).expect("open");
    let exact = FlatEngine
        .appendable(&spec, built.out.as_path())
        .expect("flat");
    exact.append(points(&data)).expect("append");

    let params = SearchParams {
        ef: Some(128),
        exact: false,
    };
    let mut found = 0;
    for query in vectors(3, 100, 32) {
        let want: BTreeSet<u64> = top(exact.as_ref(), &query, 10, params)
            .into_iter()
            .map(|h| h.0)
            .collect();
        found += top(index.as_ref(), &query, 10, params)
            .into_iter()
            .filter(|h| want.contains(&h.0))
            .count();
    }
    let recall = found as f64 / 1_000.0;
    assert!(recall >= 0.95, "recall@10 = {recall}");
}

#[test]
fn scores_follow_operon_conventions() {
    let data = vectors(4, 1_000, 8);
    let queries = vectors(5, 5, 8);
    for distance in [
        Distance::Cosine,
        Distance::Dot,
        Distance::Euclid,
        Distance::Manhattan,
    ] {
        let spec = BuildSpec::new(8, distance);
        let built = build(&spec, points(&data));
        let index = QdrantEngine.open(&spec, &built.out).expect("open");
        for query in &queries {
            for exact in [false, true] {
                let params = SearchParams { ef: None, exact };
                let hits = top(index.as_ref(), query, 10, params);
                assert_eq!(hits.len(), 10, "{distance:?}");
                for (id, score) in &hits {
                    let want = exact_score(distance, query, &data[*id as usize]);
                    let tolerance = 1e-4 * want.abs().max(1.0);
                    assert!(
                        (score - want).abs() <= tolerance,
                        "{distance:?} id {id}: {score} vs {want}"
                    );
                }
                assert!(
                    hits.windows(2)
                        .all(|w| w[0].1 > w[1].1 || (w[0].1 == w[1].1 && w[0].0 < w[1].0)),
                    "{distance:?}: {hits:?}"
                );
            }
        }
    }
}

#[test]
fn has_id_filters_restrict_results() {
    let data = vectors(6, 2_000, 16);
    let spec = BuildSpec::new(16, Distance::Euclid);
    let built = build(&spec, points(&data));
    let index = QdrantEngine.open(&spec, &built.out).expect("open");
    let query = &data[0];

    let only: RoaringTreemap = (0..10).map(|i| i * 150 + 7).collect();
    let hits = index
        .search(query, 20, IdFilter::Only(&only), SearchParams::default())
        .expect("search");
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|h| only.contains(h.0)), "{hits:?}");

    let empty = RoaringTreemap::new();
    let hits = index
        .search(query, 20, IdFilter::Only(&empty), SearchParams::default())
        .expect("search");
    assert!(hits.is_empty());

    let nearest: RoaringTreemap = top(index.as_ref(), query, 10, SearchParams::default())
        .into_iter()
        .map(|h| h.0)
        .collect();
    assert_eq!(nearest.len(), 10);
    let hits = index
        .search(
            query,
            10,
            IdFilter::Except(&nearest),
            SearchParams::default(),
        )
        .expect("search");
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|h| !nearest.contains(h.0)), "{hits:?}");
    let hits = index
        .search(query, 10, IdFilter::Except(&empty), SearchParams::default())
        .expect("search");
    assert_eq!(hits.len(), 10);
}

#[test]
fn each_quantization_builds_and_self_hits() {
    let data = vectors(7, 2_000, 32);
    for quantization in [
        Quantization::Scalar {
            quantile_ppm: Some(990_000),
            always_ram: true,
        },
        Quantization::Product {
            compression_ratio: 16,
            always_ram: false,
        },
        Quantization::Binary { always_ram: true },
    ] {
        let mut spec = BuildSpec::new(32, Distance::Cosine);
        spec.quantization = Some(quantization);
        let built = build(&spec, points(&data));
        let index = QdrantEngine.open(&spec, &built.out).expect("open");
        assert_eq!(index.len(), 2_000);
        let hits = self_hits(index.as_ref(), &data, (0..2_000).step_by(40));
        assert!(hits >= 48, "{quantization:?}: {hits} of 50 self-hits");
    }
}

#[test]
fn hnsw_params_reach_the_built_index() {
    let data = vectors(8, 2_000, 16);
    let mut spec = BuildSpec::new(16, Distance::Dot);
    spec.hnsw.m = 24;
    spec.hnsw.ef_construct = 150;
    let built = build(&spec, points(&data));
    let configs = index_files(&built.out, "hnsw_config.json");
    assert!(!configs.is_empty());
    for path in configs {
        let text: String = std::fs::read_to_string(&path)
            .expect("read")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(text.contains("\"m\":24"), "{}: {text}", path.display());
        assert!(
            text.contains("\"ef_construct\":150"),
            "{}: {text}",
            path.display()
        );
    }
}

#[test]
fn payload_fields_get_payload_indexes() {
    let data = vectors(9, 2_000, 16);
    let mut spec = BuildSpec::new(16, Distance::Cosine);
    spec.payload_fields = vec![
        PayloadField {
            key: "f0".into(),
            kind: PayloadKind::Keyword,
        },
        PayloadField {
            key: "f3".into(),
            kind: PayloadKind::Uuid,
        },
    ];
    let points: Vec<Point> = points(&data)
        .into_iter()
        .map(|mut p| {
            let tag = format!("tag{}", p.id % 7);
            p.payload = vec![
                ("f0".into(), vec![PayloadValue::Keyword(tag)]),
                (
                    "f3".into(),
                    vec![
                        PayloadValue::Uuid([p.id as u8; 16]),
                        PayloadValue::Uuid([1; 16]),
                    ],
                ),
            ];
            p
        })
        .collect();
    let built = build(&spec, points);
    let index = QdrantEngine.open(&spec, &built.out).expect("open");
    assert_eq!(index.len(), 2_000);

    let shard = qdrant_edge::ReadOnlyEdgeShard::open_mmap(&built.out).expect("open_mmap");
    let schema = shard.info().expect("info").payload_schema;
    let keys: BTreeSet<String> = schema.keys().map(|k| k.to_string()).collect();
    assert!(keys.contains("f0"), "{keys:?}");
    assert!(keys.contains("f3"), "{keys:?}");
}

#[test]
fn a_tiny_build_stays_searchable() {
    let data = vectors(10, 5, 16);
    let spec = BuildSpec::new(16, Distance::Cosine);
    let built = build(&spec, points(&data));
    assert_eq!(built.files.points, 5);
    assert!(index_files(&built.out, "graph.bin").is_empty());
    let index = QdrantEngine.open(&spec, &built.out).expect("open");
    assert_eq!(index.len(), 5);
    assert_eq!(self_hits(index.as_ref(), &data, 0..5), 5);
}

#[test]
fn the_appendable_index_is_correct_before_and_after_optimize() {
    let data = vectors(11, 2_000, 16);
    let spec = BuildSpec::new(16, Distance::Cosine);
    let dir = TempDir::new().expect("temp dir");
    let index = QdrantEngine
        .appendable(&spec, &dir.path().join("delta"))
        .expect("appendable");
    assert_eq!(index.len(), 0);
    let mut all = points(&data);
    let second = all.split_off(1_000);
    index.append(all).expect("append");
    index.append(second).expect("append");
    assert_eq!(index.len(), 2_000);
    assert_eq!(self_hits(index.as_ref(), &data, 0..2_000), 2_000);
    index.optimize().expect("optimize");
    assert_eq!(index.len(), 2_000);
    assert_eq!(self_hits(index.as_ref(), &data, 0..2_000), 2_000);
}

#[test]
fn a_second_builder_in_a_used_directory_fails_cleanly() {
    let dir = TempDir::new().expect("temp dir");
    let spec = BuildSpec::new(4, Distance::Dot);
    let mut first = QdrantEngine.builder(&spec, dir.path()).expect("builder");
    first.add(points(&vectors(12, 10, 4))).expect("add");
    let err = QdrantEngine
        .builder(&spec, dir.path())
        .err()
        .expect("second builder");
    assert!(matches!(err, HnswError::Engine(_)), "{err}");
    drop(first);
    let err = QdrantEngine
        .builder(&spec, dir.path())
        .err()
        .expect("third builder");
    assert!(matches!(err, HnswError::Engine(_)), "{err}");
}

#[test]
fn invalid_input_is_rejected() {
    let dir = TempDir::new().expect("temp dir");
    let err = QdrantEngine
        .builder(&BuildSpec::new(0, Distance::Dot), dir.path())
        .err()
        .expect("dim 0");
    assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    let mut spec = BuildSpec::new(4, Distance::Dot);
    spec.quantization = Some(Quantization::Product {
        compression_ratio: 3,
        always_ram: false,
    });
    assert!(matches!(edge_config(&spec), Err(HnswError::Invalid(_))));

    let spec = BuildSpec::new(4, Distance::Dot);
    let index = QdrantEngine
        .appendable(&spec, &dir.path().join("delta"))
        .expect("appendable");
    let err = index
        .append(vec![Point::new(1, vec![1.0, f32::NAN, 0.0, 0.0])])
        .expect_err("nan");
    assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    index.append(points(&vectors(13, 10, 4))).expect("append");
    let err = index
        .search(&[1.0, 2.0], 3, IdFilter::All, SearchParams::default())
        .expect_err("short query");
    assert!(matches!(err, HnswError::Invalid(_)), "{err}");
    assert!(
        top(
            index.as_ref(),
            &[1.0, 0.0, 0.0, 0.0],
            0,
            SearchParams::default()
        )
        .is_empty()
    );
}

#[test]
#[allow(deprecated)] // `always_ram` is deprecated in qdrant 1.19.
fn edge_config_follows_rule_5() {
    let mut spec = BuildSpec::new(24, Distance::Euclid);
    spec.hnsw.m = 20;
    spec.hnsw.ef_construct = 64;
    spec.hnsw.full_scan_threshold_kb = 500;
    spec.hnsw.payload_m = Some(0);
    spec.indexing_threads = 3;
    spec.quantization = Some(Quantization::Scalar {
        quantile_ppm: Some(990_000),
        always_ram: true,
    });
    let config = edge_config(&spec).expect("config");
    let vector = config.vectors.get(VECTOR_NAME).expect("vector v");
    assert_eq!(config.vectors.len(), 1);
    assert_eq!(vector.size, 24);
    assert_eq!(vector.distance, qdrant_edge::Distance::Euclid);
    let hnsw = vector.hnsw_config.expect("hnsw");
    assert_eq!(
        (
            hnsw.m,
            hnsw.ef_construct,
            hnsw.full_scan_threshold,
            hnsw.max_indexing_threads,
            hnsw.payload_m
        ),
        (20, 64, 500, 3, Some(0))
    );
    let want: qdrant_edge::QuantizationConfig = qdrant_edge::ScalarQuantizationConfig {
        r#type: qdrant_edge::ScalarType::Int8,
        quantile: Some(0.99),
        always_ram: Some(true),
        memory: None,
    }
    .into();
    assert_eq!(vector.quantization_config, Some(want));
    assert_eq!(
        config
            .optimizers
            .as_ref()
            .and_then(|o| o.indexing_threshold),
        Some(1)
    );
    let wal = config.wal_options.as_ref().expect("wal");
    assert_eq!(wal.segment_capacity, 1 << 20);
    assert_eq!(
        wal.segment_queue_len,
        qdrant_edge::WalOptions::default().segment_queue_len
    );

    for (ratio, want) in [
        (4, qdrant_edge::CompressionRatio::X4),
        (64, qdrant_edge::CompressionRatio::X64),
    ] {
        spec.quantization = Some(Quantization::Product {
            compression_ratio: ratio,
            always_ram: false,
        });
        let config = edge_config(&spec).expect("config");
        let want: qdrant_edge::QuantizationConfig = qdrant_edge::ProductQuantizationConfig {
            compression: want,
            always_ram: Some(false),
            memory: None,
        }
        .into();
        assert_eq!(config.vectors[VECTOR_NAME].quantization_config, Some(want));
    }
    spec.quantization = Some(Quantization::Binary { always_ram: false });
    let config = edge_config(&spec).expect("config");
    let want: qdrant_edge::QuantizationConfig = qdrant_edge::BinaryQuantizationConfig {
        always_ram: Some(false),
        memory: None,
        encoding: None,
        query_encoding: None,
    }
    .into();
    assert_eq!(config.vectors[VECTOR_NAME].quantization_config, Some(want));
}
