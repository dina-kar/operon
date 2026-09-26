//! The engine-neutral types and traits (plan M1.3 Task 3).

use std::cmp::Ordering;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use operon_common::schema::MAX_VECTOR_DIM;
pub use operon_common::schema::{Distance, HnswParams, Quantization};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::HnswError;

/// What an index is built for: one dense vector column.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuildSpec {
    pub dim: u32,
    pub distance: Distance,
    pub hnsw: HnswParams,
    pub quantization: Option<Quantization>,
    /// Payload fields that get payload indexes (Ruling 3).
    pub payload_fields: Vec<PayloadField>,
    /// Graph build threads; 0 = the engine's default.
    pub indexing_threads: usize,
}

impl BuildSpec {
    /// A spec with Qdrant's default HNSW parameters, no quantization and no
    /// payload fields.
    pub fn new(dim: u32, distance: Distance) -> Self {
        Self {
            dim,
            distance,
            hnsw: HnswParams::default(),
            quantization: None,
            payload_fields: Vec::new(),
            indexing_threads: 0,
        }
    }
}

/// A payload field copied into point payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PayloadField {
    /// `"f<field index>"`.
    pub key: String,
    pub kind: PayloadKind,
}

/// The payload index type of a [`PayloadField`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadKind {
    Keyword,
    Integer,
    Bool,
    Uuid,
}

/// One payload value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PayloadValue {
    Keyword(String),
    Integer(i64),
    Bool(bool),
    Uuid([u8; 16]),
}

/// A point: a row id, its vector and its payload values by key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub id: u64,
    pub vector: Vec<f32>,
    pub payload: Vec<(String, Vec<PayloadValue>)>,
}

impl Point {
    /// A point without payload.
    pub fn new(id: u64, vector: Vec<f32>) -> Self {
        Self {
            id,
            vector,
            payload: Vec::new(),
        }
    }
}

/// Per-search parameters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchParams {
    /// The HNSW beam width; `None` = the engine's default.
    pub ef: Option<u32>,
    /// Scan every point instead of walking the graph.
    pub exact: bool,
}

/// Which point ids a search may return.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IdFilter<'a> {
    All,
    Only(&'a RoaringTreemap),
    Except(&'a RoaringTreemap),
}

impl IdFilter<'_> {
    /// Whether the filter lets `id` through.
    pub fn allows(&self, id: u64) -> bool {
        match self {
            IdFilter::All => true,
            IdFilter::Only(ids) => ids.contains(id),
            IdFilter::Except(ids) => !ids.contains(id),
        }
    }
}

/// What [`HnswBuilder::finish`] wrote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltFiles {
    /// [`HnswEngine::name`].
    pub engine: String,
    /// Every file under the output directory: relative, '/'-separated, sorted.
    pub files: Vec<String>,
    /// Distinct point ids in the index.
    pub points: u64,
}

/// An HNSW implementation.
pub trait HnswEngine: Send + Sync + fmt::Debug {
    /// "qdrant-edge-0.8" or "flat": recorded in artifact descriptors.
    fn name(&self) -> &'static str;
    fn builder(&self, spec: &BuildSpec, work_dir: &Path)
    -> Result<Box<dyn HnswBuilder>, HnswError>;
    /// Opens files that `HnswBuilder::finish` wrote (possibly copied elsewhere) read-only.
    fn open(&self, spec: &BuildSpec, dir: &Path) -> Result<Arc<dyn HnswIndex>, HnswError>;
    /// A mutable index in `work_dir` (the delta index of a view, Task 6).
    fn appendable(
        &self,
        spec: &BuildSpec,
        work_dir: &Path,
    ) -> Result<Arc<dyn AppendableHnsw>, HnswError>;
}

/// Builds one index.
pub trait HnswBuilder: Send {
    fn add(&mut self, points: Vec<Point>) -> Result<(), HnswError>;
    /// Builds the index and moves the files `open` needs into `out_dir` (empty, created by the caller).
    fn finish(self: Box<Self>, out_dir: &Path) -> Result<BuiltFiles, HnswError>;
}

/// A searchable index.
pub trait HnswIndex: Send + Sync + fmt::Debug {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Up to `k` points, best first, scores larger-is-better (rule 3), ties by id ascending.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: IdFilter<'_>,
        params: SearchParams,
    ) -> Result<Vec<(u64, f32)>, HnswError>;
}

/// An index that accepts points after it was created.
pub trait AppendableHnsw: HnswIndex {
    fn append(&self, points: Vec<Point>) -> Result<(), HnswError>;
    /// Builds graph structure for appended points; results stay valid before, during and after.
    fn optimize(&self) -> Result<(), HnswError>;
}

/// Rule 2: `dim` in `1..=MAX_VECTOR_DIM`.
pub(crate) fn check_spec(spec: &BuildSpec) -> Result<(), HnswError> {
    if !(1..=MAX_VECTOR_DIM).contains(&spec.dim) {
        return Err(HnswError::Invalid(format!(
            "dim {} is outside 1..={MAX_VECTOR_DIM}",
            spec.dim
        )));
    }
    Ok(())
}

/// Rule 2: every vector has `dim` finite values.
pub(crate) fn check_points(dim: u32, points: &[Point]) -> Result<(), HnswError> {
    for point in points {
        check_vector(dim, &point.vector)
            .map_err(|e| HnswError::Invalid(format!("point {}: {e}", point.id)))?;
    }
    Ok(())
}

/// Rule 2: a query has `dim` finite values.
pub(crate) fn check_query(dim: u32, query: &[f32]) -> Result<(), HnswError> {
    check_vector(dim, query).map_err(|e| HnswError::Invalid(format!("query: {e}")))
}

fn check_vector(dim: u32, vector: &[f32]) -> Result<(), String> {
    if vector.len() != dim as usize {
        return Err(format!("{} values, expected {dim}", vector.len()));
    }
    if let Some(i) = vector.iter().position(|v| !v.is_finite()) {
        return Err(format!("value {i} is not finite"));
    }
    Ok(())
}

/// Rule 3's order: score descending, then id ascending.
pub(crate) fn sort_hits(hits: &mut [(u64, f32)]) {
    hits.sort_unstable_by(hit_order);
}

pub(crate) fn hit_order(a: &(u64, f32), b: &(u64, f32)) -> Ordering {
    b.1.total_cmp(&a.1).then(a.0.cmp(&b.0))
}
