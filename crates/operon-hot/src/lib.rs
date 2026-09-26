//! Operon's hot tier (plan M1.3; design §04).
//!
//! - [`artifact`]: the HNSW artifact format (descriptor, covered set, zstd
//!   chunked files under `hot/hnsw/`), publishing and downloading it, and
//!   artifact currency (Ruling 1);
//! - [`build`]: [`HotBuildSource`], the worker task that builds artifacts
//!   from manifest versions and commits them under the collection manifest;
//! - [`tier`], [`live`], [`view`] and [`delta`]: [`HotTierImpl`], which
//!   loads the artifacts of the hot collections this node owns and serves
//!   per-manifest views of them, each with a delta index of the rows
//!   inserted since.
//!
//! This crate reaches the metastore only through
//! [`MetaStore`](operon_common::meta::MetaStore) (D47).

pub mod artifact;
pub mod build;
mod config;
pub mod delta;
mod error;
pub mod live;
pub mod tier;
pub mod view;

pub use artifact::{
    ARTIFACT_FORMAT_VERSION, ArtifactDescriptor, ArtifactFile, COVERED_FILE, COVERED_MAGIC,
    Currency, CurrencyCache, DESCRIPTOR_FILE, DESCRIPTOR_MAGIC, FILES_DIR, HNSW_KIND,
    artifact_prefix, chunk_path, currency, decode_covered, decode_descriptor, download,
    effective_source_version, encode_covered, encode_descriptor, publish,
};
#[cfg(feature = "test-util")]
pub use build::HotBuildHook;
pub use build::{
    BUILD_TASK_PREFIX, BuildDecision, HotBuildSource, HotBuildStep, PROMOTE_LEASE_PREFIX,
    build_spec, decide, effective_hot, payload_fields, promote_lease_key,
};
pub use config::{HotBuildConfig, HotTierConfig};
pub use delta::{DeltaIndex, Extension};
pub use error::TierError;
pub use live::{DeletedDocsCache, live_rows, live_rows_cached};
pub use tier::{HotTierImpl, ReconcileReport, TierCounters};
pub use view::{ColumnView, LoadedArtifact};

/// Evaluates a named failpoint. With the `failpoints` feature the `fail`
/// crate may act on it (the crash gate aborts the process there); without
/// it, this expands to nothing.
macro_rules! failpoint {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name);
    };
}
pub(crate) use failpoint;
