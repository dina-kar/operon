//! How hot artifacts are built (plan M1.3 Task 5; Ruling 16).

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Hot artifact builds: where, when, and how big.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotBuildConfig {
    /// Build directories: `<data_dir>/hot-build`. Cleared when a
    /// `HotBuildSource` starts.
    pub work_dir: PathBuf,
    /// A stale artifact is rebuilt once this many rows were inserted since
    /// it...
    pub rebuild_min_inserted: u64,
    /// ...or this share of its points (parts per million), whichever is
    /// larger (Ruling 16).
    pub rebuild_inserted_ppm: u32,
    /// A stale artifact is rebuilt once it has been stale this long.
    pub rebuild_max_staleness: Duration,
    /// The oldest an artifact commit's new objects may be at its CAS; must
    /// be below `gc.grace` (Task 13).
    pub artifact_commit_delay: Duration,
    /// The uncompressed size of an artifact chunk.
    pub chunk_bytes: u64,
    /// Graph build threads; 0 = the engine's default.
    pub indexing_threads: usize,
    /// Rows per Lance scan batch of a build.
    pub scan_batch_rows: usize,
    /// How often an unchanged hot column is checked again.
    pub poll_interval: Duration,
    /// The most payload fields a build copies (Ruling 3).
    pub max_payload_fields: usize,
    /// Rebases of one artifact commit before the run gives up.
    pub max_rebases: u32,
    /// Every collection is hot for vectors and text on this process
    /// (`--hot-pin-all`, Ruling 8).
    pub pin_all: bool,
}

impl HotBuildConfig {
    /// The defaults, building under `<data_dir>/hot-build`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            work_dir: data_dir.join("hot-build"),
            rebuild_min_inserted: 10_000,
            rebuild_inserted_ppm: 200_000,
            rebuild_max_staleness: Duration::from_secs(10 * 60),
            artifact_commit_delay: Duration::from_secs(30 * 60),
            chunk_bytes: 256 << 20,
            indexing_threads: 0,
            scan_batch_rows: 8_192,
            poll_interval: Duration::from_secs(10),
            max_payload_fields: 8,
            max_rebases: 5,
            pin_all: false,
        }
    }
}
