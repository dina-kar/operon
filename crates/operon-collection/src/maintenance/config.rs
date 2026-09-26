//! How background maintenance runs (plan M1.3 Tasks 1 and 2).

use std::time::Duration;

use operon_quickwit::merge_policy::StableLogMergePolicyConfig;

/// Split merges and Lance compaction: what runs, when, and how big.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceConfig {
    /// Whether split merges run.
    pub merge: bool,
    /// Quickwit's stable log merge policy (Ruling 4).
    pub merge_policy: StableLogMergePolicyConfig,
    /// A split of this many live docs or more is mature: it never merges
    /// again (it can still be purged).
    pub split_num_docs_target: usize,
    /// The most live docs one merge re-indexes: the policy's operation is
    /// cut to its smallest inputs that fit (a merge holds its output split
    /// in memory while it builds it). A purge is never cut.
    pub max_merge_docs: u64,
    /// The most input bytes (`size_bytes`, deleted docs included) one merge
    /// reads, cut like `max_merge_docs`.
    pub max_merge_bytes: u64,
    /// A split outside every merge with at least this many deleted docs...
    pub purge_min_deleted: u64,
    /// ...and at least this share of deleted docs (parts per million) is
    /// rewritten alone.
    pub purge_deleted_ppm: u32,
    /// Whether Lance compaction runs.
    pub compaction: bool,
    /// The rows of a compacted fragment.
    pub compaction_target_rows: usize,
    /// Fragments under half of `compaction_target_rows` before a compaction
    /// runs.
    pub compaction_min_small_fragments: usize,
    /// The share of deleted rows (parts per million) at which a fragment is
    /// rewritten to drop them.
    pub compaction_deleted_ppm: u32,
    /// Lance compaction tasks one run executes.
    pub max_compaction_tasks_per_run: usize,
    /// How often an unchanged collection is checked again.
    pub poll_interval: Duration,
    /// The oldest a maintenance commit's new objects may be at its CAS; must
    /// be below `gc.grace`.
    pub commit_delay: Duration,
    /// Rebases of one maintenance commit before the run gives up.
    pub max_rebases: u32,
    /// Rows per Lance `take_rows` round of a merge.
    pub take_batch_rows: usize,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            merge: true,
            merge_policy: StableLogMergePolicyConfig {
                min_level_num_docs: 100_000,
                merge_factor: 10,
                max_merge_factor: 12,
                maturation_period: Duration::from_hours(48),
            },
            split_num_docs_target: 10_000_000,
            max_merge_docs: 10_000_000,
            max_merge_bytes: 1 << 30,
            purge_min_deleted: 1_000,
            purge_deleted_ppm: 300_000,
            compaction: true,
            compaction_target_rows: 1_048_576,
            compaction_min_small_fragments: 4,
            compaction_deleted_ppm: 100_000,
            max_compaction_tasks_per_run: 8,
            poll_interval: Duration::from_secs(30),
            commit_delay: Duration::from_secs(30 * 60),
            max_rebases: 5,
            take_batch_rows: 1_000,
        }
    }
}
