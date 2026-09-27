//! Vector checks and Qdrant's distance arithmetic. Task 5 adds the write
//! side: vector names, dimensions, finiteness and cosine normalization
//! (Ruling 8), and sparse values (Ruling 21). Task 7 adds the read side:
//! search parameters, score conversion and thresholds (Ruling 7), and the
//! RRF, DBSF and sparse references the tests compare the IR with (Rulings
//! 9, 21).

use std::collections::BTreeMap;

use operon_collection::{CollectionSchema, Distance, PrimaryKey, SparseVector};
use operon_query::AnnParams;
use serde_json::Value;

use crate::error::GatewayError;
use crate::model::common::VectorInput;
use crate::model::query::SearchParams;
use crate::query::ScoreKind;

/// Qdrant's `cosine_preprocess`: divides by the length, unless the squared
/// length is below `f32::EPSILON` or within `1e-6` of 1
/// (`qdrant:lib/segment/src/spaces/simple.rs:228-235`, `tools.rs:14-16`).
pub fn cosine_normalize(v: &mut [f32]) {
    let squared: f32 = v.iter().map(|x| x * x).sum();
    if squared < f32::EPSILON || (squared - 1.0).abs() <= 1.0e-6 {
        return;
    }
    let length = squared.sqrt();
    v.iter_mut().for_each(|x| *x /= length);
}

fn not_existing(name: &str) -> GatewayError {
    GatewayError::BadRequest(format!("Not existing vector name error: {name}"))
}

/// A dense value for `name`: the name must be a dense schema vector, the
/// length its dimension, every value finite; Cosine vectors are then
/// normalized.
pub fn check_vector(
    schema: &CollectionSchema,
    name: &str,
    v: &mut [f32],
) -> Result<(), GatewayError> {
    let Some(spec) = schema.vectors.iter().find(|s| s.name == name) else {
        if schema.sparse_vectors.iter().any(|s| s.name == name) {
            return Err(GatewayError::BadRequest(format!(
                "Vector {name} is a sparse vector"
            )));
        }
        return Err(not_existing(name));
    };
    if v.len() != spec.dim as usize {
        return Err(GatewayError::BadRequest(format!(
            "Vector dimension error: expected dim: {}, got {}",
            spec.dim,
            v.len()
        )));
    }
    if v.iter().any(|x| !x.is_finite()) {
        return Err(GatewayError::BadRequest(
            "Vector contains NaN or infinite values".to_string(),
        ));
    }
    if spec.distance == Distance::Cosine {
        cosine_normalize(v);
    }
    Ok(())
}

/// A sparse value for `name`: the name must be a sparse schema vector; the
/// value goes through `SparseVector::new` (sorted, zero weights kept).
pub fn check_sparse(
    schema: &CollectionSchema,
    name: &str,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> Result<SparseVector, GatewayError> {
    if !schema.sparse_vectors.iter().any(|s| s.name == name) {
        if schema.vectors.iter().any(|s| s.name == name) {
            return Err(GatewayError::BadRequest(format!(
                "Vector {name} is a dense vector"
            )));
        }
        return Err(not_existing(name));
    }
    SparseVector::new(indices, values)
        .map_err(|err| GatewayError::BadRequest(format!("Sparse vector {name}: {err}")))
}

/// Checked vectors, split by kind.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CheckedVectors {
    pub dense: BTreeMap<String, Vec<f32>>,
    pub sparse: BTreeMap<String, SparseVector>,
}

/// Qdrant's text for a named value that is no vector at all.
pub(crate) fn not_a_vector() -> GatewayError {
    GatewayError::json("data did not match any variant of untagged enum VectorStruct")
}

/// Named values checked one by one ([`check_vector`], [`check_sparse`]);
/// multivectors and inference objects are unsupported (Ruling 15).
pub(crate) fn check_named(
    schema: &CollectionSchema,
    named: BTreeMap<String, VectorInput>,
) -> Result<CheckedVectors, GatewayError> {
    let mut out = CheckedVectors::default();
    for (name, input) in named {
        match input {
            VectorInput::Dense(mut v) => {
                check_vector(schema, &name, &mut v)?;
                out.dense.insert(name, v);
            }
            VectorInput::Sparse { indices, values } => {
                let v = check_sparse(schema, &name, indices, values)?;
                out.sparse.insert(name, v);
            }
            VectorInput::Multi(_) => {
                return Err(GatewayError::Unsupported("multivectors".to_string()));
            }
            VectorInput::Id(Value::Object(_)) | VectorInput::Object(_) => {
                return Err(GatewayError::Unsupported("inference objects".to_string()));
            }
            VectorInput::Id(_) => return Err(not_a_vector()),
        }
    }
    Ok(out)
}

// ----- read side (Task 7) -----

/// Dense search parameters as the IR's: `exact`, `ef = hnsw_ef` and the
/// quantization `oversampling`; `nprobes` and `refine_factor` are left to
/// the IR.
pub fn ann_params(p: Option<&SearchParams>) -> AnnParams {
    let Some(p) = p else {
        return AnnParams::default();
    };
    AnnParams {
        exact: p.exact,
        ef: p.hnsw_ef,
        oversampling: p
            .quantization
            .as_ref()
            .and_then(|q| q.oversampling)
            .map(|x| x as f32),
        ..AnnParams::default()
    }
}

fn smaller_is_better(distance: Distance) -> bool {
    matches!(distance, Distance::Euclid | Distance::Manhattan)
}

/// Qdrant's score of an IR score (Rulings 7, 8): vector scores are the IR's
/// for Cosine and Dot and the distance (`-ir`) for Euclid and Manhattan;
/// fusion and custom scores are the IR's; a query without a query scores
/// `0.0`. A NaN is `0.0`.
pub fn to_qdrant_score(distance: Distance, kind: &ScoreKind, ir_score: f32) -> f32 {
    if ir_score.is_nan() {
        return 0.0;
    }
    match kind {
        ScoreKind::Distance if smaller_is_better(distance) => -ir_score,
        ScoreKind::Distance | ScoreKind::Fusion | ScoreKind::Custom => ir_score,
        ScoreKind::Filter => 0.0,
    }
}

/// Qdrant's `score_threshold` (Ruling 7): `score > t` for vector and custom
/// scores of Cosine and Dot, `score < t` for Euclid and Manhattan, and
/// `score >= t` for fusion. A query without a query keeps every point.
pub fn passes_threshold(distance: Distance, kind: &ScoreKind, score: f32, t: f32) -> bool {
    match kind {
        ScoreKind::Fusion => score >= t,
        ScoreKind::Filter => true,
        ScoreKind::Distance | ScoreKind::Custom if smaller_is_better(distance) => score < t,
        ScoreKind::Distance | ScoreKind::Custom => score > t,
    }
}

/// Fused scores by key, ordered by score descending, then key ascending.
fn ordered(scores: BTreeMap<PrimaryKey, f32>) -> Vec<(PrimaryKey, f32)> {
    let mut out: Vec<(PrimaryKey, f32)> = scores.into_iter().collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// Qdrant's RRF, the test oracle: `Σ 1/(pos + k)` over the lists, with
/// `pos` 0-based (`qdrant:lib/segment/src/common/reciprocal_rank_fusion.rs`).
pub fn rrf(lists: &[Vec<PrimaryKey>], qdrant_k: u32) -> Vec<(PrimaryKey, f32)> {
    let mut scores: BTreeMap<PrimaryKey, f32> = BTreeMap::new();
    for list in lists {
        for (pos, pk) in list.iter().enumerate() {
            *scores.entry(pk.clone()).or_default() += 1.0 / (pos as f32 + qdrant_k as f32);
        }
    }
    ordered(scores)
}

/// Qdrant's DBSF normalization of one list: `(s - (μ - 3σ)) / 6σ` with the
/// sample deviation (Welford); one point, or `μ - 3σ == μ + 3σ`, is 0.5.
fn distr_norm(scores: &[f32]) -> Vec<f32> {
    if scores.len() < 2 {
        return vec![0.5; scores.len()];
    }
    let (mut mean, mut aggregate) = (0.0_f32, 0.0_f32);
    for (i, s) in scores.iter().enumerate() {
        let delta = s - mean;
        mean += delta / (i + 1) as f32;
        aggregate += delta * (s - mean);
    }
    let sigma = (aggregate / (scores.len() - 1) as f32).sqrt();
    let (low, high) = (mean - 3.0 * sigma, mean + 3.0 * sigma);
    if low == high {
        return vec![0.5; scores.len()];
    }
    scores.iter().map(|s| (s - low) / (high - low)).collect()
}

/// Qdrant's DBSF, the test oracle: each list normalized, then summed per
/// point (`qdrant:lib/segment/src/common/score_fusion.rs`).
pub fn dbsf(lists: &[Vec<(PrimaryKey, f32)>]) -> Vec<(PrimaryKey, f32)> {
    let mut scores: BTreeMap<PrimaryKey, f32> = BTreeMap::new();
    for list in lists {
        let raw: Vec<f32> = list.iter().map(|(_, s)| *s).collect();
        for ((pk, _), s) in list.iter().zip(distr_norm(&raw)) {
            *scores.entry(pk.clone()).or_default() += s;
        }
    }
    ordered(scores)
}

/// Qdrant's `fancy_idf`: `ln((n - df + 0.5) / (df + 0.5) + 1)` in f32.
fn fancy_idf(n: usize, df: usize) -> f32 {
    let (n, df) = (n as f32, df as f32);
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// The sparse test oracle ("Qdrant protocol facts", sparse search): only
/// points sharing an index with the query are candidates; the score is
/// the f32 dot over shared indices in index order; with `idf` each query
/// weight is first multiplied by `fancy_idf` over the `docs` with a
/// non-empty vector. The top `k`, by score descending then key.
pub fn sparse_reference(
    docs: &[(PrimaryKey, SparseVector)],
    query: &SparseVector,
    idf: bool,
    k: usize,
) -> Vec<(PrimaryKey, f32)> {
    let weights: Vec<f32> = if idf {
        let n = docs.iter().filter(|(_, v)| !v.is_empty()).count();
        query
            .indices()
            .iter()
            .zip(query.values())
            .map(|(i, w)| {
                let df = docs
                    .iter()
                    .filter(|(_, v)| v.indices().binary_search(i).is_ok())
                    .count();
                w * fancy_idf(n, df)
            })
            .collect()
    } else {
        query.values().to_vec()
    };
    let mut scored: BTreeMap<PrimaryKey, f32> = BTreeMap::new();
    for (pk, v) in docs {
        let mut score = 0.0_f32;
        let mut shared = false;
        for (i, w) in query.indices().iter().zip(&weights) {
            if let Ok(at) = v.indices().binary_search(i) {
                shared = true;
                score += w * v.values()[at];
            }
        }
        if shared {
            scored.insert(pk.clone(), score);
        }
    }
    let mut out = ordered(scored);
    out.truncate(k);
    out
}

/// Qdrant's raw similarity (`qdrant:lib/segment/src/spaces/simple.rs`):
/// the dot product for Dot and for Cosine (both vectors normalized first),
/// `-Σ (aᵢ - bᵢ)²` for Euclid and `-Σ |aᵢ - bᵢ|` for Manhattan.
pub fn raw_similarity(distance: Distance, a: &[f32], b: &[f32]) -> f32 {
    match distance {
        Distance::Dot => a.iter().zip(b).map(|(x, y)| x * y).sum(),
        Distance::Cosine => {
            let (mut a, mut b) = (a.to_vec(), b.to_vec());
            cosine_normalize(&mut a);
            cosine_normalize(&mut b);
            a.iter().zip(&b).map(|(x, y)| x * y).sum()
        }
        Distance::Euclid => -a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f32>(),
        Distance::Manhattan => -a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum::<f32>(),
    }
}
