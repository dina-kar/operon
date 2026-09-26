//! Hot-tier configuration: artifact builds (plan M1.3 Task 5; Ruling 16)
//! and the tier of one node (Tasks 6 and 7).

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

/// The hot tier of one node: where it keeps local copies, its budgets, and
/// how often it reconciles (plan M1.3 Tasks 6 and 7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotTierConfig {
    /// `--hot=on|off`. Off: every hook answers `None` and nothing is loaded.
    pub enabled: bool,
    /// `--hot-pin-all` (Ruling 8).
    pub pin_all: bool,
    /// Local copies (NVMe): `<data_dir>/hot`. `hnsw/` and `delta/` under it
    /// are cleared at start.
    pub dir: PathBuf,
    pub nvme_bytes: u64,
    pub ram_bytes: u64,
    /// At most this many artifacts open at once (Ruling 18).
    pub max_loaded_artifacts: usize,
    /// The reconcile loop runs this often, and whenever the metastore
    /// changes (at most once per 100 ms).
    pub reconcile_interval: Duration,
    /// Chunk GETs in flight per artifact download.
    pub download_parallelism: usize,
    /// A delta index scans at most this many rows (rule 2).
    pub delta_max_rows: u64,
    /// A delta index is optimized after this many appended points.
    pub delta_optimize_rows: u64,
    /// A view that excludes more rows than this is not served (Ruling 2).
    pub max_view_exclusions: u64,
    /// The newest views kept per column.
    pub views_per_column: usize,
    /// Task 7.
    pub split_linger: Duration,
    /// Task 7: fragment bytes prefetched per reconcile pass.
    pub fragments_max_bytes: u64,
    /// Task 7 (Ruling 7): off by default.
    pub auto_promote: bool,
    /// Task 7: hits per heat window that promote a collection.
    pub promote_min_hits: u32,
    /// Task 7: hits per heat window under which a promotion lapses.
    pub demote_below_hits: u32,
    /// Task 7.
    pub heat_window: Duration,
    /// Task 7: the TTL of a promotion lease.
    pub promote_lease_ttl: Duration,
}

impl HotTierConfig {
    /// The defaults, with local copies under `<data_dir>/hot`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            enabled: true,
            pin_all: false,
            dir: data_dir.join("hot"),
            nvme_bytes: 100 << 30,
            ram_bytes: 8 << 30,
            max_loaded_artifacts: 32,
            reconcile_interval: Duration::from_secs(1),
            download_parallelism: 4,
            delta_max_rows: 200_000,
            delta_optimize_rows: 20_000,
            max_view_exclusions: 100_000,
            views_per_column: 4,
            split_linger: Duration::from_secs(60),
            fragments_max_bytes: 32 << 30,
            auto_promote: false,
            promote_min_hits: 64,
            demote_below_hits: 4,
            heat_window: Duration::from_secs(60),
            promote_lease_ttl: Duration::from_secs(15 * 60),
        }
    }
}
