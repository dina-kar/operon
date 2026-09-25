use std::time::Duration;

/// How collections use Lance (plan M1.1 Task 7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LanceConfig {
    /// The Lance session's index cache, in bytes.
    pub index_cache_bytes: usize,
    /// The Lance session's metadata cache (manifests, row-id indexes), in bytes.
    pub metadata_cache_bytes: usize,
    /// Concurrent object-store requests per Lance operation.
    pub io_parallelism: usize,
    /// Rows per Lance data file.
    pub max_rows_per_file: usize,
    /// Rows per row group within a data file.
    pub max_rows_per_group: usize,
}

impl Default for LanceConfig {
    fn default() -> Self {
        Self {
            index_cache_bytes: 256 * 1024 * 1024,
            metadata_cache_bytes: 64 * 1024 * 1024,
            io_parallelism: 32,
            max_rows_per_file: 1_048_576,
            max_rows_per_group: 1_024,
        }
    }
}

/// How collections commit, index, trim and keep their manifests (plan M1.1
/// Task 9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionConfig {
    /// The oldest a link-apply commit's new objects may be at its CAS; the
    /// server sets it to `link.max_commit_delay`.
    pub max_commit_delay: Duration,
    /// Whether the implicit stream is trimmed below the oldest retained
    /// manifest (Ruling 12).
    pub trim: bool,
    pub trim_interval: Duration,
    /// How long a superseded manifest stays readable (§03 §7; measured from
    /// its supersession, overview A21).
    pub time_travel_retention: Duration,
    /// Ancestors of the live manifest always kept; the one source of this
    /// setting (GC receives it from here). The server sets it to
    /// `gc.keep_manifests`.
    pub keep_manifests: usize,
    /// Rows before a first vector index is built.
    pub index_min_rows: u64,
    /// Unindexed rows before a delta index segment is built.
    pub index_delta_min_rows: u64,
    /// Segments before a full rebuild replaces them all.
    pub index_max_segments: usize,
    /// Unindexed rows before the `_pk` BTREE index is rebuilt.
    pub pk_index_min_unindexed_rows: u64,
    /// The oldest an index build's objects may be at its CAS; must be below
    /// `gc.grace`.
    pub index_commit_delay: Duration,
    pub index_poll_interval: Duration,
    /// Keys per PK-index or Lance lookup round.
    pub max_lookup_batch: usize,
    /// Manifests the [`ManifestCache`](crate::ManifestCache) holds.
    pub manifest_cache_entries: usize,
}

impl Default for CollectionConfig {
    fn default() -> Self {
        Self {
            max_commit_delay: Duration::from_secs(600),
            trim: true,
            trim_interval: Duration::from_secs(60),
            time_travel_retention: Duration::from_secs(24 * 60 * 60),
            keep_manifests: 10,
            index_min_rows: 5_000,
            index_delta_min_rows: 20_000,
            index_max_segments: 4,
            pk_index_min_unindexed_rows: 10_000,
            index_commit_delay: Duration::from_secs(30 * 60),
            index_poll_interval: Duration::from_secs(30),
            max_lookup_batch: 1_000,
            manifest_cache_entries: 100_000,
        }
    }
}
