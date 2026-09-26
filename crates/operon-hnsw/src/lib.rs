//! HNSW indexes for Operon's hot tier (plan M1.3 Task 3; design §04, §06 §5):
//!
//! - the engine-neutral traits: [`HnswEngine`] builds ([`HnswBuilder`]),
//!   opens ([`HnswIndex`]) and creates appendable ([`AppendableHnsw`])
//!   indexes of one dense vector column, keyed by row id;
//! - [`FlatEngine`]: an exact scan, for tests and builds without `qdrant`;
//! - `QdrantEngine` (feature `qdrant`): qdrant-edge's HNSW. It is the only
//!   code in the workspace that names `qdrant-edge`.
//!
//! Scores are larger-is-better for every distance (overview §6.6):
//! Euclid and Manhattan scores are negated distances. Every method blocks;
//! async callers run them on `spawn_blocking`.

mod error;
mod flat;
#[cfg(feature = "qdrant")]
mod qdrant;
mod types;

use std::sync::Arc;

pub use error::HnswError;
pub use flat::{FLAT_ENGINE, FLAT_FILE, FLAT_MAGIC, FlatEngine, exact_score};
#[cfg(feature = "qdrant")]
pub use qdrant::{QDRANT_ENGINE, QdrantEngine, VECTOR_NAME, edge_config};
pub use types::{
    AppendableHnsw, BuildSpec, BuiltFiles, Distance, HnswBuilder, HnswEngine, HnswIndex,
    HnswParams, IdFilter, PayloadField, PayloadKind, PayloadValue, Point, Quantization,
    SearchParams,
};

/// "flat" always; "qdrant-edge-0.8" with feature `qdrant`.
pub fn engine_by_name(name: &str) -> Option<Arc<dyn HnswEngine>> {
    match name {
        FLAT_ENGINE => Some(Arc::new(FlatEngine)),
        #[cfg(feature = "qdrant")]
        QDRANT_ENGINE => Some(Arc::new(QdrantEngine)),
        _ => None,
    }
}

/// The qdrant-edge engine with feature `qdrant`, else the flat engine.
#[cfg(feature = "qdrant")]
pub fn default_engine() -> Arc<dyn HnswEngine> {
    Arc::new(QdrantEngine)
}

/// The qdrant-edge engine with feature `qdrant`, else the flat engine.
#[cfg(not(feature = "qdrant"))]
pub fn default_engine() -> Arc<dyn HnswEngine> {
    Arc::new(FlatEngine)
}
