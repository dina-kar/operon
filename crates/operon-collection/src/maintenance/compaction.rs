//! Lance compaction (plan M1.3 Task 2, Ruling 5; overview R7): Lance's own
//! planner and rewriter (`plan_compaction`, `CompactionTask::execute`)
//! write the new data files: stable row ids are re-chunked, deleted rows
//! dropped, nothing committed. The task gives the new fragments real ids
//! from the parent's `max_fragment_id + 1` and commits
//! `Operation::Rewrite` *detached* through [`LanceCommitter::commit`]
//! (never `commit_compaction`, which reserves fragment ids with a mainline
//! commit). Lance's manifest build then moves every row-id-domain index's
//! fragment bitmap to the new fragments, so vector and `_pk` indexes stay
//! valid without a rebuild.
//!
//! The new Lance version is committed under a new collection manifest by the
//! fenced, freshness-checked pointer CAS. The detached commit resolves no
//! conflicts, so on a `Conflict` the task rebases itself: a group is
//! re-applied onto the new live Lance version only if every one of its old
//! fragments is unchanged there (a fragment that gained a deletion file
//! would otherwise resurrect the deleted rows), with fresh ids; the data
//! files are reused, never rewritten. Splits, the PK index and `applied`
//! are never touched: row ids do not change.
//!
//! Lance's compaction is async and does its CPU work on Lance's own CPU
//! pool, so it is awaited like every other Lance write.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, CompactionTask, plan_compaction};
use lance::dataset::transaction::{Operation, RewriteGroup};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_table::format::{Fragment, IndexMetadata};
use operon_common::meta::{Consistency, Fence, MetaStore};
use operon_common::{CollectionId, NamespaceId};
use operon_link::CommitError;
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};

use super::{CollectionPoller, MaintenanceConfig};
use crate::chain::live_manifest;
use crate::error::CollectionError;
use crate::lance::LanceCommitter;
use crate::manifest::{CollectionManifest, CommitKind};
use crate::snapshot::CollectionContext;
#[cfg(feature = "test-util")]
use crate::target::CollectionCommitHook;
use crate::target::{CollectionCommitStep, PointerCas, put_manifest};

/// The prefix of a compaction task's key: `TaskKey::new(ns, "collection-compact/<cid>")`.
pub const COMPACTION_TASK_PREFIX: &str = "collection-compact/";

/// Rows per row group of a compacted data file.
const COMPACTED_ROWS_PER_GROUP: usize = 1_024;

/// Lance's compaction options for `config` (Task 2 rule 2): fragments of
/// `compaction_target_rows`, deletions materialized from a share of
/// `compaction_deleted_ppm`, no deferred index remap (stable row ids need
/// none), file format 2.1 (R18).
pub fn compaction_options(config: &MaintenanceConfig) -> CompactionOptions {
    CompactionOptions {
        target_rows_per_fragment: config.compaction_target_rows,
        max_rows_per_group: COMPACTED_ROWS_PER_GROUP,
        materialize_deletions: true,
        materialize_deletions_threshold: config.compaction_deleted_ppm as f32 / 1e6,
        defer_index_remap: false,
        data_storage_version: Some(LanceFileVersion::V2_1),
        ..CompactionOptions::default()
    }
}

/// Pure: whether `dataset` has at least `compaction_min_small_fragments`
/// fragments under half of `compaction_target_rows` rows, or a fragment
/// whose deletion file holds at least `compaction_deleted_ppm` of its rows
/// (Task 2 rule 1).
pub fn needs_compaction(dataset: &Dataset, config: &MaintenanceConfig) -> bool {
    let fragments = dataset.manifest.fragments.iter();
    let small_below = config.compaction_target_rows / 2;
    let small = fragments
        .clone()
        .filter(|f| f.physical_rows.is_some_and(|rows| rows < small_below))
        .count();
    if small >= config.compaction_min_small_fragments.max(1) {
        return true;
    }
    fragments.into_iter().any(|fragment| {
        let (Some(rows), Some(deletions)) = (fragment.physical_rows, &fragment.deletion_file)
        else {
            return false;
        };
        deletions.num_deleted_rows.is_some_and(|deleted| {
            deleted as u128 * 1_000_000 >= u128::from(config.compaction_deleted_ppm) * rows as u128
        })
    })
}

/// Assigns ids `next..` to every new fragment of every group, in order;
/// returns the next free id. Lance's writer leaves them 0, and the Rewrite
/// arm of its manifest build reads the groups' ids to move the index
/// bitmaps, so this is required.
pub fn assign_fragment_ids(groups: &mut [RewriteGroup], next: u64) -> u64 {
    let mut next = next;
    for fragment in groups.iter_mut().flat_map(|g| g.new_fragments.iter_mut()) {
        fragment.id = next;
        next += 1;
    }
    next
}

/// The first id free in `dataset`: `max_fragment_id + 1`, and never 0
/// (which Lance reads as "unassigned").
fn next_fragment_id(dataset: &Dataset) -> u64 {
    dataset
        .manifest
        .max_fragment_id
        .map_or(1, |max| u64::from(max) + 1)
}

/// The groups of `groups` still valid on `parent` (Task 2 rule 4), with
/// fresh ids: a group is kept iff every old fragment equals the fragment of
/// `parent` with its id (one that gained a deletion file, was rewritten or
/// removed fails), and no index of `indices` covers some but not all of its
/// old fragments.
pub fn rebase_groups(
    groups: Vec<RewriteGroup>,
    parent: &Dataset,
    indices: &[IndexMetadata],
) -> Vec<RewriteGroup> {
    let current: HashMap<u64, &Fragment> = parent
        .manifest
        .fragments
        .iter()
        .map(|fragment| (fragment.id, fragment))
        .collect();
    let mut kept: Vec<RewriteGroup> = groups
        .into_iter()
        .filter(|group| {
            let unchanged = group
                .old_fragments
                .iter()
                .all(|old| current.get(&old.id).is_some_and(|now| *now == old));
            let ids: Vec<u32> = group
                .old_fragments
                .iter()
                .filter_map(|old| u32::try_from(old.id).ok())
                .collect();
            let split = indices.iter().any(|index| {
                index.fragment_bitmap.as_ref().is_some_and(|bitmap| {
                    let covered = ids.iter().filter(|id| bitmap.contains(**id)).count();
                    covered > 0 && covered < ids.len()
                })
            });
            unchanged && !split
        })
        .collect();
    assign_fragment_ids(&mut kept, next_fragment_id(parent));
    kept
}

/// The live manifest a commit builds on, and its Lance version.
#[derive(Clone)]
struct Base {
    path: String,
    manifest: Arc<CollectionManifest>,
    dataset: Arc<Dataset>,
}

/// State shared by a source and its tasks.
struct Shared {
    ctx: CollectionContext,
    config: MaintenanceConfig,
    poller: CollectionPoller,
}

/// Proposes one compaction task per due collection (keyed
/// `collection-compact/<cid>`, at [`Priority::Compaction`]); none while
/// `compaction` is off. A run executes at most
/// `max_compaction_tasks_per_run` of Lance's compaction tasks and commits
/// them as one Rewrite.
#[derive(Clone)]
pub struct LanceCompactionSource {
    shared: Arc<Shared>,
    #[cfg(feature = "test-util")]
    hook: Option<CollectionCommitHook>,
}

impl fmt::Debug for LanceCompactionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceCompactionSource")
            .field("ctx", &self.shared.ctx)
            .field("config", &self.shared.config)
            .field("poller", &self.shared.poller)
            .finish_non_exhaustive()
    }
}

impl LanceCompactionSource {
    pub fn new(ctx: CollectionContext, config: MaintenanceConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                ctx,
                config,
                poller: CollectionPoller::new(),
            }),
            #[cfg(feature = "test-util")]
            hook: None,
        }
    }

    /// Test hook: every compaction awaits `hook` at
    /// [`CollectionCommitStep::AfterLanceCommit`],
    /// [`CollectionCommitStep::AfterManifestPut`] and
    /// [`CollectionCommitStep::AfterCas`]. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: CollectionCommitHook) -> Self {
        self.hook = Some(hook);
        self
    }
}

#[async_trait]
impl TaskSource for LanceCompactionSource {
    fn priority(&self) -> Priority {
        Priority::Compaction
    }

    async fn candidates(&self, meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let config = &self.shared.config;
        if !config.compaction {
            return Ok(Vec::new());
        }
        let due = self.shared.poller.due(meta, config.poll_interval).await?;
        Ok(due
            .into_iter()
            .map(|c| {
                let task: Arc<dyn Task> = Arc::new(CompactionRun {
                    source: self.clone(),
                    ns: c.ns,
                    cid: c.cid,
                });
                (
                    TaskKey::new(c.ns, format!("{COMPACTION_TASK_PREFIX}{}", c.cid)),
                    task,
                )
            })
            .collect())
    }
}

/// Compacts the Lance dataset of one collection.
struct CompactionRun {
    source: LanceCompactionSource,
    ns: NamespaceId,
    cid: CollectionId,
}

fn failed(err: CollectionError) -> TaskError {
    TaskError::failed(err)
}

#[async_trait]
impl Task for CompactionRun {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let result = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return Ok(TaskOutcome::Done),
            result = self.run_inner(&ctx.fence) => result,
        };
        let poller = &self.source.shared.poller;
        match &result {
            Ok((TaskOutcome::Idle, version)) => poller.checked(self.cid, Some(*version)),
            _ => poller.checked(self.cid, None),
        }
        result.map(|(outcome, _)| outcome)
    }
}

impl CompactionRun {
    fn ctx(&self) -> &CollectionContext {
        &self.source.shared.ctx
    }

    fn config(&self) -> &MaintenanceConfig {
        &self.source.shared.config
    }

    async fn step(&self, step: CollectionCommitStep, fence: &Fence) {
        match step {
            CollectionCommitStep::AfterLanceCommit => {
                crate::failpoint!("compaction.after_lance_commit");
            }
            CollectionCommitStep::AfterManifestPut => {
                crate::failpoint!("compaction.after_manifest_put");
            }
            CollectionCommitStep::AfterCas => {
                crate::failpoint!("compaction.after_cas");
            }
            _ => {}
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.source.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    /// The live manifest's version (`Linearizable`; 0 before the first
    /// commit), and the manifest with its Lance version once it has data.
    async fn base(&self) -> Result<(u64, Option<Base>), CollectionError> {
        let ctx = self.ctx();
        let live = live_manifest(
            &*ctx.meta,
            &ctx.store,
            &ctx.manifests,
            self.ns,
            self.cid,
            Consistency::Linearizable,
        )
        .await?;
        let Some((path, manifest)) = live else {
            return Ok((0, None));
        };
        let version = manifest.version;
        if manifest.lance_version == 0 {
            return Ok((version, None));
        }
        let dataset = ctx
            .lance
            .open(self.ns, self.cid, manifest.lance_version)
            .await?;
        Ok((
            version,
            Some(Base {
                path,
                manifest,
                dataset,
            }),
        ))
    }

    /// Plans on the live Lance version and commits what the first tasks
    /// rewrote (Task 2 rule 3); returns the outcome and the pointer version
    /// it last saw.
    async fn run_inner(&self, fence: &Fence) -> Result<(TaskOutcome, u64), TaskError> {
        let (version, base) = match self.base().await {
            Ok(base) => base,
            // Dropped: the poller forgets it at the next poll.
            Err(CollectionError::NotFound(_)) => return Ok((TaskOutcome::Idle, 0)),
            Err(err) => return Err(failed(err)),
        };
        let Some(base) = base else {
            return Ok((TaskOutcome::Idle, version));
        };
        let config = self.config();
        if !needs_compaction(&base.dataset, config) {
            return Ok((TaskOutcome::Idle, version));
        }
        let plan = plan_compaction(&base.dataset, &compaction_options(config))
            .await
            .map_err(|e| failed(e.into()))?;
        let tasks: Vec<CompactionTask> = plan
            .compaction_tasks()
            .take(config.max_compaction_tasks_per_run.max(1))
            .collect();
        if tasks.is_empty() {
            return Ok((TaskOutcome::Idle, version));
        }
        let more = plan.num_tasks() > tasks.len();
        let started = self.ctx().meta.now_ms();
        let mut groups = Vec::with_capacity(tasks.len());
        for task in &tasks {
            let rewritten = task
                .execute(&base.dataset)
                .await
                .map_err(|e| failed(e.into()))?;
            groups.push(RewriteGroup {
                old_fragments: rewritten.original_fragments,
                new_fragments: rewritten.new_fragments,
            });
        }
        assign_fragment_ids(&mut groups, next_fragment_id(&base.dataset));
        tracing::info!(
            collection = %self.cid,
            groups = groups.len(),
            old = groups.iter().map(|g| g.old_fragments.len()).sum::<usize>(),
            new = groups.iter().map(|g| g.new_fragments.len()).sum::<usize>(),
            "rewrote lance fragments"
        );
        match self.commit(base, groups, started, fence).await? {
            Some(committed) => {
                let outcome = match more {
                    true => TaskOutcome::MoreWork,
                    false => TaskOutcome::Idle,
                };
                Ok((outcome, committed))
            }
            None => Ok((TaskOutcome::MoreWork, 0)),
        }
    }

    /// Commits `groups` on `base` (Task 2 rules 3.5–3.7), rebasing on a
    /// `Conflict` (rule 4); returns the new manifest version, or `None` when
    /// no group survived a rebase.
    async fn commit(
        &self,
        mut base: Base,
        mut groups: Vec<RewriteGroup>,
        started: u64,
        fence: &Fence,
    ) -> Result<Option<u64>, TaskError> {
        let ctx = self.ctx();
        let (ns, cid) = (self.ns, self.cid);
        let max_rebases = self.config().max_rebases;
        for rebase in 0..=max_rebases {
            let operation = Operation::Rewrite {
                groups: groups.clone(),
                rewritten_indices: vec![],
                frag_reuse_index: None,
            };
            let committed = LanceCommitter::commit(&ctx.lance, &base.dataset, operation)
                .await
                .map_err(failed)?;
            self.step(CollectionCommitStep::AfterLanceCommit, fence)
                .await;
            let (before, after) = (
                base.dataset
                    .count_rows(None)
                    .await
                    .map_err(|e| failed(e.into()))?,
                committed
                    .count_rows(None)
                    .await
                    .map_err(|e| failed(e.into()))?,
            );
            if before != after {
                return Err(failed(CollectionError::Internal(format!(
                    "compacting lance version {} of collection {cid} left {after} rows of {before}",
                    base.dataset.manifest.version
                ))));
            }
            let manifest = self
                .manifest(&base, &committed, started)
                .await
                .map_err(failed)?;
            let path = put_manifest(ctx, ns, &manifest).await.map_err(failed)?;
            self.step(CollectionCommitStep::AfterManifestPut, fence)
                .await;
            let cas = PointerCas {
                ns,
                cid,
                parent_version: base.manifest.version,
                path: &path,
                started,
                max_age: self.config().commit_delay,
            };
            match cas.run(ctx, fence).await {
                Ok(version) => {
                    self.step(CollectionCommitStep::AfterCas, fence).await;
                    tracing::info!(
                        collection = %cid,
                        version,
                        lance_version = committed.manifest.version,
                        rebases = rebase,
                        "committed a lance compaction"
                    );
                    return Ok(Some(version));
                }
                Err(CommitError::Conflict) => {
                    let (_, live) = self.base().await.map_err(failed)?;
                    let live = live.ok_or_else(|| {
                        failed(CollectionError::Corrupt(format!(
                            "collection {cid} lost its lance dataset"
                        )))
                    })?;
                    let indices = live
                        .dataset
                        .load_indices()
                        .await
                        .map_err(|e| failed(e.into()))?;
                    let offered = groups.len();
                    groups = rebase_groups(groups, &live.dataset, &indices);
                    tracing::info!(
                        collection = %cid,
                        parent = base.manifest.version,
                        live = live.manifest.version,
                        kept = groups.len(),
                        dropped = offered - groups.len(),
                        "a lance compaction conflicted; rebasing onto the live manifest"
                    );
                    if groups.is_empty() {
                        return Ok(None);
                    }
                    base = live;
                }
                Err(CommitError::Fenced) => return Err(TaskError::Fenced),
                Err(CommitError::Other(err)) => return Err(TaskError::failed(err)),
            }
        }
        Err(failed(CollectionError::Blocked(format!(
            "a lance compaction of collection {cid} conflicted {} times",
            max_rebases + 1
        ))))
    }

    /// Manifest v+1: `base`'s at the `committed` Lance version, with the
    /// index entries of the indexes that version has (their uuids and
    /// watermarks do not change) (Task 2 rule 3.7).
    async fn manifest(
        &self,
        base: &Base,
        committed: &Dataset,
        started: u64,
    ) -> Result<CollectionManifest, CollectionError> {
        let parent = &base.manifest;
        let uuids: BTreeSet<String> = committed
            .load_indices()
            .await?
            .iter()
            .map(|index| index.uuid.to_string())
            .collect();
        let mut vector_indexes = parent.vector_indexes.clone();
        vector_indexes.retain(|v| uuids.contains(&v.lance_index_uuid));
        let mut scalar_indexes = parent.scalar_indexes.clone();
        scalar_indexes.retain(|s| uuids.contains(&s.lance_index_uuid));
        Ok(CollectionManifest {
            version: parent.version + 1,
            parent_version: parent.version,
            parent_manifest: Some(base.path.clone()),
            created_at_ms: started,
            lance_version: committed.manifest.version,
            vector_indexes,
            scalar_indexes,
            kind: CommitKind::Maintenance,
            pk_delta: None,
            dead_letters: None,
            ..(**parent).clone()
        })
    }
}
