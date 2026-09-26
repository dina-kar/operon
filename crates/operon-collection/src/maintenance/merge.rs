//! Split merges (plan M1.3 Task 1, Ruling 4; design §03 §3.2): Quickwit's
//! stable log merge policy picks the splits, and a merge *re-indexes* the
//! live docs of its inputs from `_source` (Lance `take_rows`) with the
//! inputs' own schema, in ascending row-id order, into one new split.
//! Deleted docs are dropped; the merged split's `row_id_ranges` are the
//! maximal runs of the surviving row ids, so `RowLocator` and every later
//! merge keep working. Only splits of one `schema_version` merge. A split
//! with many deleted docs and no merge partner is rewritten alone (a purge).
//!
//! The merged split is committed under a new manifest by the fenced,
//! freshness-checked pointer CAS of link apply. On a `Conflict` the task
//! reloads the live manifest: if an input is gone it abandons the merge (the
//! merged split is GC's); otherwise it carries the deletes that landed on
//! the inputs meanwhile into a delete bitmap of the merged split and commits
//! again. The PK index, Lance and `applied` are never touched: row ids do not
//! change.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use operon_common::meta::{Consistency, Fence};
use operon_common::{CollectionId, NamespaceId};
use operon_link::CommitError;
use operon_quickwit::merge_policy::{MergePolicy, StableLogMergePolicy};
use operon_quickwit::shim::{SplitId, SplitMaturity, SplitMetadata};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use roaring::RoaringBitmap;
use tantivy::TantivyDocument;
use ulid::Ulid;

use super::{CollectionPoller, MaintenanceConfig};
use crate::chain::live_manifest;
use crate::doc::Document;
use crate::error::CollectionError;
use crate::manifest::{CollectionManifest, CommitKind, SplitRef};
use crate::paths::{delete_bitmap_path, split_path};
use crate::schema::{CollectionSchema, DynamicMapping, FieldKind};
use crate::snapshot::{CollectionContext, CollectionSnapshot, read_deleted_docs};
use crate::tantivy_schema::{
    TantivyLayout, count_companion, date_companion, null_companion, tantivy_layout, text_companion,
    to_tantivy_doc,
};
#[cfg(feature = "test-util")]
use crate::target::CollectionCommitHook;
use crate::target::{
    CollectionCommitStep, PointerCas, millis, object_ulid, put_manifest, put_unique,
};
use crate::values::check_document;

/// The prefix of a merge task's key: `TaskKey::new(ns, "collection-merge/<cid>")`.
pub const MERGE_TASK_PREFIX: &str = "collection-merge/";

/// `take_rows` batches of re-indexed documents queued for the split writer.
const STREAM_BATCHES: usize = 2;

/// One merge: its input splits, ascending by ULID; `purge` when a single
/// split is rewritten only to drop its deleted docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergePlan {
    pub inputs: Vec<Ulid>,
    pub purge: bool,
}

/// The vendored policy decides maturity with the wall clock, while
/// `created_at_ms` is on the metastore clock (Task 0 E20): a split that is
/// not mature at `now_ms` is handed over with this creation second and a
/// zero maturation period, which the wall clock never reaches.
const NEVER_MATURE_TIMESTAMP: i64 = i64::MAX;

/// Pure: the merges `manifest` needs at the metastore time `now_ms`, in
/// execution order (Task 1 rule 2; Task 0 E20).
///
/// Per `schema_version`, the stable log merge policy plans over the splits'
/// live doc counts; a split is mature (never merged) when it holds
/// `split_num_docs_target` live docs or was created `maturation_period`
/// before `now_ms`. Each operation is cut to its smallest inputs within
/// `max_merge_docs` and `max_merge_bytes`, and dropped if fewer than two
/// remain. Then every split outside those merges with at least
/// `purge_min_deleted` deleted docs, and a deleted share of at least
/// `purge_deleted_ppm`, is purged alone. Plans are ordered by their smallest
/// input.
pub fn plan_merges(
    manifest: &CollectionManifest,
    config: &MaintenanceConfig,
    now_ms: u64,
) -> Vec<MergePlan> {
    let policy =
        StableLogMergePolicy::new(config.merge_policy.clone(), config.split_num_docs_target);
    let maturation_ms = millis(config.merge_policy.maturation_period);
    let mut groups: BTreeMap<u64, Vec<&SplitRef>> = BTreeMap::new();
    for split in &manifest.splits {
        groups.entry(split.schema_version).or_default().push(split);
    }
    let mut plans = Vec::new();
    let mut merging: BTreeSet<Ulid> = BTreeSet::new();
    for splits in groups.values() {
        let mut metas: Vec<SplitMetadata> = splits
            .iter()
            .map(|split| {
                let live = split.doc_count.saturating_sub(split.deleted_count);
                let num_docs = usize::try_from(live).unwrap_or(usize::MAX);
                let merge_ops = split.merge_ops as usize;
                let mature = policy.split_maturity(num_docs, merge_ops) == SplitMaturity::Mature
                    || split.created_at_ms.saturating_add(maturation_ms) <= now_ms;
                let (maturity, create_timestamp) = match mature {
                    true => (
                        SplitMaturity::Mature,
                        i64::try_from(split.created_at_ms / 1000).unwrap_or(i64::MAX),
                    ),
                    false => (
                        SplitMaturity::Immature {
                            maturation_period: std::time::Duration::ZERO,
                        },
                        NEVER_MATURE_TIMESTAMP,
                    ),
                };
                SplitMetadata {
                    split_id: SplitId::from(split.ulid.to_string()),
                    num_docs,
                    time_range: None,
                    maturity,
                    create_timestamp,
                    num_merge_ops: merge_ops,
                    footer_offsets: split.footer_range.clone(),
                }
            })
            .collect();
        for operation in policy.operations(&mut metas) {
            let chosen: Vec<&SplitRef> = operation
                .splits
                .iter()
                .filter_map(|meta| Ulid::from_string(meta.split_id()).ok())
                .filter_map(|ulid| splits.iter().copied().find(|s| s.ulid == ulid))
                .collect();
            let mut inputs = bounded(chosen, config);
            if inputs.len() < 2 {
                continue;
            }
            inputs.sort_unstable();
            merging.extend(inputs.iter().copied());
            plans.push(MergePlan {
                inputs,
                purge: false,
            });
        }
    }
    for split in &manifest.splits {
        let heavy = split.deleted_count >= config.purge_min_deleted
            && u128::from(split.deleted_count) * 1_000_000
                >= u128::from(config.purge_deleted_ppm) * u128::from(split.doc_count);
        if heavy && !merging.contains(&split.ulid) {
            plans.push(MergePlan {
                inputs: vec![split.ulid],
                purge: true,
            });
        }
    }
    plans.sort_by_key(|plan| plan.inputs.first().copied());
    plans
}

/// The inputs of one policy operation that one merge takes: its smallest
/// splits (by live docs, then ULID) while their live docs stay within
/// `max_merge_docs` and their bytes within `max_merge_bytes`. A split whose
/// bytes do not fit is skipped and the scan goes on (the order is by docs,
/// not bytes, so a later split may still fit); the first split whose docs do
/// not fit ends it. A merge holds its output split in memory while it builds
/// it, so this bounds it; the splits left out are planned again once the
/// merged split has landed.
fn bounded(mut splits: Vec<&SplitRef>, config: &MaintenanceConfig) -> Vec<Ulid> {
    let live = |split: &SplitRef| split.doc_count.saturating_sub(split.deleted_count);
    splits.sort_by_key(|split| (live(split), split.ulid));
    let (mut docs, mut bytes) = (0u64, 0u64);
    let mut inputs = Vec::with_capacity(splits.len());
    for split in splits {
        let next_docs = docs.saturating_add(live(split));
        let next_bytes = bytes.saturating_add(split.size_bytes);
        if next_docs > config.max_merge_docs {
            break;
        }
        if next_bytes > config.max_merge_bytes {
            continue;
        }
        (docs, bytes) = (next_docs, next_bytes);
        inputs.push(split.ulid);
    }
    inputs
}

/// Whether every Tantivy field of collection field `kind` named `name`
/// exists in `schema`.
fn has_field(schema: &tantivy::schema::Schema, name: &str, kind: &FieldKind) -> bool {
    let has = |name: &str| schema.get_field(name).is_ok();
    match kind {
        FieldKind::Json => {
            has(name)
                && has(&text_companion(name))
                && has(&date_companion(name))
                && has(&null_companion(name))
                && has(&count_companion(name))
        }
        _ => has(name),
    }
}

/// The schema a split was written with: `current` with `fields` cut to the
/// longest prefix whose Tantivy fields all exist in `split_schema`;
/// [`CollectionError::Corrupt`] unless `tantivy_layout(prefix).schema ==
/// *split_schema` (fields are append-only, so the split's schema is such a
/// prefix, A4).
pub fn schema_for_split(
    current: &CollectionSchema,
    split_schema: &tantivy::schema::Schema,
) -> Result<CollectionSchema, CollectionError> {
    let prefix = current
        .fields
        .iter()
        .take_while(|spec| has_field(split_schema, &spec.name, &spec.kind))
        .count();
    let mut schema = current.clone();
    schema.fields.truncate(prefix);
    if tantivy_layout(&schema).schema != *split_schema {
        return Err(CollectionError::Corrupt(format!(
            "a split's tantivy schema is not the layout of the first {prefix} fields of schema version {}",
            current.version
        )));
    }
    Ok(schema)
}

/// Maximal runs of consecutive ids in an ascending slice (Ruling 4).
pub fn row_id_runs(sorted_row_ids: &[u64]) -> Vec<Range<u64>> {
    let mut out: Vec<Range<u64>> = Vec::new();
    for &id in sorted_row_ids {
        match out.last_mut() {
            Some(last) if last.end == id => last.end += 1,
            _ => out.push(id..id + 1),
        }
    }
    out
}

/// The row id of doc `doc` of `split`: its ranges in order, by running doc
/// base.
fn row_of(split: &SplitRef, doc: u32) -> Option<u64> {
    let mut base = 0u64;
    let doc = u64::from(doc);
    for range in &split.row_id_ranges {
        let len = range.end - range.start;
        if doc < base + len {
            return Some(range.start + (doc - base));
        }
        base += len;
    }
    None
}

/// The row ids of `split`'s docs outside `deleted`, in doc-id order.
fn live_rows(split: &SplitRef, deleted: &RoaringBitmap) -> Vec<u64> {
    split
        .row_id_ranges
        .iter()
        .flat_map(|range| range.clone())
        .enumerate()
        .filter(|(doc, _)| u32::try_from(*doc).is_ok_and(|doc| !deleted.contains(doc)))
        .map(|(_, row)| row)
        .collect()
}

/// Manifest v+1 on `parent` (at `parent_path`): its splits without
/// `inputs`, with `merged` at the index of the first input (Task 1 rule 4).
fn merged_manifest(
    parent_path: &str,
    parent: &CollectionManifest,
    inputs: &[Ulid],
    merged: Option<&SplitRef>,
    started: u64,
) -> CollectionManifest {
    let first = parent.splits.iter().position(|s| inputs.contains(&s.ulid));
    let mut splits = Vec::with_capacity(parent.splits.len());
    for (index, split) in parent.splits.iter().enumerate() {
        if Some(index) == first
            && let Some(merged) = merged
        {
            splits.push(merged.clone());
        }
        if !inputs.contains(&split.ulid) {
            splits.push(split.clone());
        }
    }
    CollectionManifest {
        version: parent.version + 1,
        parent_version: parent.version,
        parent_manifest: Some(parent_path.to_string()),
        created_at_ms: started,
        splits,
        kind: CommitKind::Maintenance,
        pk_delta: None,
        dead_letters: None,
        ..parent.clone()
    }
}

/// State shared by a source and its tasks.
struct Shared {
    ctx: CollectionContext,
    config: MaintenanceConfig,
    poller: CollectionPoller,
}

/// Proposes one merge task per due collection (keyed
/// `collection-merge/<cid>`, at [`Priority::Compaction`]); none while
/// `merge` is off. A run does at most one [`MergePlan`] and ends `MoreWork`
/// if the new manifest needs another, else `Idle`.
#[derive(Clone)]
pub struct SplitMergeSource {
    shared: Arc<Shared>,
    #[cfg(feature = "test-util")]
    hook: Option<CollectionCommitHook>,
}

impl fmt::Debug for SplitMergeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SplitMergeSource")
            .field("ctx", &self.shared.ctx)
            .field("config", &self.shared.config)
            .field("poller", &self.shared.poller)
            .finish_non_exhaustive()
    }
}

impl SplitMergeSource {
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

    /// Test hook: every merge awaits `hook` at
    /// [`CollectionCommitStep::AfterSplitPut`],
    /// [`CollectionCommitStep::AfterManifestPut`] and
    /// [`CollectionCommitStep::AfterCas`]. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: CollectionCommitHook) -> Self {
        self.hook = Some(hook);
        self
    }
}

#[async_trait]
impl TaskSource for SplitMergeSource {
    fn priority(&self) -> Priority {
        Priority::Compaction
    }

    async fn candidates(
        &self,
        meta: &dyn operon_common::meta::MetaStore,
    ) -> Result<Vec<Candidate>, TaskError> {
        let config = &self.shared.config;
        if !config.merge {
            return Ok(Vec::new());
        }
        let due = self.shared.poller.due(meta, config.poll_interval).await?;
        Ok(due
            .into_iter()
            .map(|c| {
                let task: Arc<dyn Task> = Arc::new(MergeTask {
                    source: self.clone(),
                    ns: c.ns,
                    cid: c.cid,
                });
                (
                    TaskKey::new(c.ns, format!("{MERGE_TASK_PREFIX}{}", c.cid)),
                    task,
                )
            })
            .collect())
    }
}

/// How a merge ended.
enum Merged {
    /// Committed as this manifest.
    Committed(Box<CollectionManifest>),
    /// An input left the live manifest; nothing was committed.
    Abandoned,
}

/// Merges the splits of one collection.
struct MergeTask {
    source: SplitMergeSource,
    ns: NamespaceId,
    cid: CollectionId,
}

fn failed(err: CollectionError) -> TaskError {
    TaskError::failed(err)
}

#[async_trait]
impl Task for MergeTask {
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

/// The inputs of a merge as its parent manifest has them, with their
/// deleted docs there.
struct Inputs {
    splits: Vec<SplitRef>,
    deleted: Vec<RoaringBitmap>,
}

impl MergeTask {
    fn ctx(&self) -> &CollectionContext {
        &self.source.shared.ctx
    }

    fn config(&self) -> &MaintenanceConfig {
        &self.source.shared.config
    }

    async fn step(&self, step: CollectionCommitStep, fence: &Fence) {
        match step {
            CollectionCommitStep::AfterSplitPut => {
                crate::failpoint!("merge.after_split_put");
            }
            CollectionCommitStep::AfterManifestPut => {
                crate::failpoint!("merge.after_manifest_put");
            }
            CollectionCommitStep::AfterCas => {
                crate::failpoint!("merge.after_cas");
            }
            _ => {}
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.source.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    /// Plans on the live manifest and runs the first plan; returns the
    /// outcome and the pointer version it last saw (0 when abandoned).
    async fn run_inner(&self, fence: &Fence) -> Result<(TaskOutcome, u64), TaskError> {
        let snapshot = match CollectionSnapshot::open(
            self.ctx(),
            self.ns,
            self.cid,
            Consistency::Linearizable,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            // Dropped: the poller forgets it at the next poll.
            Err(CollectionError::NotFound(_)) => return Ok((TaskOutcome::Idle, 0)),
            Err(err) => return Err(failed(err)),
        };
        let version = snapshot.manifest().version;
        let plans = plan_merges(snapshot.manifest(), self.config(), self.ctx().meta.now_ms());
        let Some(plan) = plans.first() else {
            return Ok((TaskOutcome::Idle, version));
        };
        match self.merge(&snapshot, plan, fence).await? {
            Merged::Committed(manifest) => {
                let more =
                    !plan_merges(&manifest, self.config(), self.ctx().meta.now_ms()).is_empty();
                let outcome = match more {
                    true => TaskOutcome::MoreWork,
                    false => TaskOutcome::Idle,
                };
                Ok((outcome, manifest.version))
            }
            Merged::Abandoned => Ok((TaskOutcome::MoreWork, 0)),
        }
    }

    /// The inputs of `plan` in `parent`, in plan order, with their deleted
    /// docs; `None` if one is missing.
    async fn inputs(
        &self,
        parent: &CollectionManifest,
        plan: &MergePlan,
    ) -> Result<Option<Inputs>, CollectionError> {
        let mut splits = Vec::with_capacity(plan.inputs.len());
        let mut deleted = Vec::with_capacity(plan.inputs.len());
        for ulid in &plan.inputs {
            let Some(split) = parent.splits.iter().find(|s| s.ulid == *ulid) else {
                return Ok(None);
            };
            deleted.push(read_deleted_docs(&self.ctx().store, split).await?);
            splits.push(split.clone());
        }
        Ok(Some(Inputs { splits, deleted }))
    }

    /// Builds the merged split of `plan` on `snapshot` (Task 1 rule 3) and
    /// commits it (rules 4 and 5).
    async fn merge(
        &self,
        snapshot: &CollectionSnapshot,
        plan: &MergePlan,
        fence: &Fence,
    ) -> Result<Merged, TaskError> {
        let cid = self.cid;
        let parent = snapshot.manifest();
        let parent_path = snapshot.manifest_path().ok_or_else(|| {
            failed(CollectionError::Internal(format!(
                "collection {cid} has splits but no manifest path"
            )))
        })?;
        let inputs = self
            .inputs(parent, plan)
            .await
            .map_err(failed)?
            .ok_or_else(|| {
                failed(CollectionError::Internal(format!(
                    "a merge plan of collection {cid} names a split its manifest lacks"
                )))
            })?;

        // 2. One Tantivy schema, and the collection schema it was written with.
        let mut split_schema: Option<tantivy::schema::Schema> = None;
        for split in &inputs.splits {
            let index = snapshot.open_split(split).await.map_err(failed)?;
            let schema = index.schema();
            match &split_schema {
                None => split_schema = Some(schema),
                Some(first) if *first != schema => {
                    return Err(failed(CollectionError::Corrupt(format!(
                        "split {} of collection {cid} has another tantivy schema than split {}",
                        split.ulid, inputs.splits[0].ulid
                    ))));
                }
                Some(_) => {}
            }
        }
        let split_schema = split_schema.ok_or_else(|| {
            failed(CollectionError::Internal(
                "a merge plan without inputs".into(),
            ))
        })?;
        let mut schema =
            schema_for_split(&snapshot.collection().schema, &split_schema).map_err(failed)?;
        schema.dynamic = DynamicMapping::Ignore;

        // 3. The live rows, ascending.
        let mut rows: Vec<u64> = inputs
            .splits
            .iter()
            .zip(&inputs.deleted)
            .flat_map(|(split, deleted)| live_rows(split, deleted))
            .collect();
        rows.sort_unstable();
        if let Some(pair) = rows.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(failed(CollectionError::Corrupt(format!(
                "row {} is in two splits of collection {cid}",
                pair[0]
            ))));
        }

        // 4.–5. The merged split. The commit's clock starts once it is
        // built, so a long build cannot outrun `commit_delay`.
        let (merged, started) = match rows.is_empty() {
            true => (None, self.ctx().meta.now_ms()),
            false => {
                let (split, started) = self
                    .build(snapshot, &inputs, &schema, &rows, fence)
                    .await
                    .map_err(failed)?;
                (Some(split), started)
            }
        };
        self.step(CollectionCommitStep::AfterSplitPut, fence).await;
        tracing::info!(
            collection = %cid,
            inputs = ?plan.inputs,
            purge = plan.purge,
            docs = rows.len(),
            "built a merged split"
        );
        self.commit(
            (parent_path.to_string(), Arc::new(parent.clone())),
            plan,
            &inputs,
            &rows,
            merged,
            started,
            fence,
        )
        .await
    }

    /// Takes `rows` from Lance, re-indexes them with `schema`, and PUTs the
    /// split (Task 1 rules 3.4 and 3.5). The documents stream into the split
    /// writer on a blocking thread, one `take_rows` batch at a time. Returns
    /// the split and the commit's start: `meta.now_ms()` read after the
    /// build, which names the split and its `created_at_ms`.
    async fn build(
        &self,
        snapshot: &CollectionSnapshot,
        inputs: &Inputs,
        schema: &CollectionSchema,
        rows: &[u64],
        fence: &Fence,
    ) -> Result<(SplitRef, u64), CollectionError> {
        let ctx = self.ctx();
        let (ns, cid) = (self.ns, self.cid);
        let layout = tantivy_layout(schema);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<TantivyDocument>>(STREAM_BATCHES);
        let tantivy_schema = layout.schema.clone();
        let writer = tokio::task::spawn_blocking(move || {
            let docs = std::iter::from_fn(move || rx.blocking_recv()).flatten();
            operon_text::build_split_from(tantivy_schema, docs)
        });
        let fed = self.feed(snapshot, inputs, schema, &layout, rows, tx).await;
        let built = writer
            .await
            .map_err(|err| CollectionError::Internal(format!("building a merged split: {err}")));
        // A feeding error explains a short build; report it first.
        fed?;
        let built = built??;
        if built.doc_count != rows.len() as u64 {
            return Err(CollectionError::Internal(format!(
                "a merged split has {} docs, not the {} rows",
                built.doc_count,
                rows.len()
            )));
        }
        self.step(CollectionCommitStep::AfterSplitBuild, fence)
            .await;
        let started = ctx.meta.now_ms();
        let ulid = object_ulid(started);
        let size_bytes = built.bytes.len() as u64;
        put_unique(&ctx.store, &split_path(ns, cid, ulid), built.bytes).await?;
        let schema_version = inputs.splits.first().map_or(0, |s| s.schema_version);
        let merge_ops = inputs.splits.iter().map(|s| s.merge_ops).max().unwrap_or(0);
        let split = SplitRef {
            ulid,
            doc_count: built.doc_count,
            deleted_count: 0,
            size_bytes,
            footer_range: built.footer_range,
            row_id_ranges: row_id_runs(rows),
            delete_bitmap: None,
            schema_version,
            created_at_ms: started,
            merge_ops: merge_ops + 1,
        };
        Ok((split, started))
    }

    /// Sends the re-indexed documents of `rows`, in order, one `take_rows`
    /// batch per message; stops early if the writer is gone (its error
    /// is the build's).
    async fn feed(
        &self,
        snapshot: &CollectionSnapshot,
        inputs: &Inputs,
        schema: &CollectionSchema,
        layout: &TantivyLayout,
        rows: &[u64],
        tx: tokio::sync::mpsc::Sender<Vec<TantivyDocument>>,
    ) -> Result<(), CollectionError> {
        let cid = self.cid;
        for chunk in rows.chunks(self.config().take_batch_rows.max(1)) {
            let found = snapshot.take_rows(chunk).await?;
            let mut docs = Vec::with_capacity(chunk.len());
            for (&row_id, stored) in chunk.iter().zip(found) {
                let Some(stored) = stored else {
                    let owner = inputs
                        .splits
                        .iter()
                        .find(|s| s.row_id_ranges.iter().any(|r| r.contains(&row_id)))
                        .map_or_else(|| "?".to_string(), |s| s.ulid.to_string());
                    return Err(CollectionError::Corrupt(format!(
                        "row {row_id} of split {owner} is not in lance version {}",
                        snapshot.manifest().lance_version
                    )));
                };
                let doc = Document {
                    pk: stored.pk,
                    source: stored.source,
                    vectors: stored.vectors,
                    sparse_vectors: stored.sparse_vectors,
                };
                let extracted = check_document(schema, &doc).map_err(|rejection| {
                    CollectionError::Corrupt(format!(
                        "row {row_id} of collection {cid} no longer fits its split's schema: {rejection:?}"
                    ))
                })?;
                docs.push(to_tantivy_doc(layout, schema, &doc, &extracted, row_id));
            }
            if tx.send(docs).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Commits `merged` (or only the removal of the inputs) on `base`,
    /// rebasing on a `Conflict` (Task 1 rules 4 and 5).
    #[allow(clippy::too_many_arguments)]
    async fn commit(
        &self,
        mut base: (String, Arc<CollectionManifest>),
        plan: &MergePlan,
        inputs: &Inputs,
        rows: &[u64],
        mut merged: Option<SplitRef>,
        started: u64,
        fence: &Fence,
    ) -> Result<Merged, TaskError> {
        let ctx = self.ctx();
        let (ns, cid) = (self.ns, self.cid);
        let max_rebases = self.config().max_rebases;
        for rebase in 0..=max_rebases {
            let (parent_path, parent) = &base;
            let manifest =
                merged_manifest(parent_path, parent, &plan.inputs, merged.as_ref(), started);
            let path = put_manifest(ctx, ns, &manifest).await.map_err(failed)?;
            self.step(CollectionCommitStep::AfterManifestPut, fence)
                .await;
            let cas = PointerCas {
                ns,
                cid,
                parent_version: parent.version,
                path: &path,
                started,
                max_age: self.config().commit_delay,
            };
            match cas.run(ctx, fence).await {
                Ok(version) => {
                    self.step(CollectionCommitStep::AfterCas, fence).await;
                    tracing::info!(
                        collection = %cid,
                        inputs = ?plan.inputs,
                        version,
                        rebases = rebase,
                        "committed a split merge"
                    );
                    return Ok(Merged::Committed(Box::new(manifest)));
                }
                Err(CommitError::Conflict) => {
                    let live = live_manifest(
                        &*ctx.meta,
                        &ctx.store,
                        &ctx.manifests,
                        ns,
                        cid,
                        Consistency::Linearizable,
                    )
                    .await
                    .map_err(failed)?
                    .ok_or_else(|| {
                        failed(CollectionError::Corrupt(format!(
                            "collection {cid} lost its manifest pointer"
                        )))
                    })?;
                    let Some(now) = self.inputs(&live.1, plan).await.map_err(failed)? else {
                        tracing::info!(
                            collection = %cid,
                            inputs = ?plan.inputs,
                            "an input of a split merge is gone; abandoning it"
                        );
                        return Ok(Merged::Abandoned);
                    };
                    if let Some(split) = merged.take() {
                        merged = self
                            .carry_deletes(split, inputs, &now, rows, started)
                            .await
                            .map_err(failed)?;
                    }
                    tracing::info!(
                        collection = %cid,
                        parent = parent.version,
                        live = live.1.version,
                        "a split merge conflicted; rebasing onto the live manifest"
                    );
                    base = live;
                }
                Err(CommitError::Fenced) => return Err(TaskError::Fenced),
                Err(CommitError::Other(err)) => return Err(TaskError::failed(err)),
            }
        }
        Err(failed(CollectionError::Blocked(format!(
            "a split merge of collection {cid} conflicted {} times",
            max_rebases + 1
        ))))
    }

    /// `merged` with the docs deleted from the inputs since the build
    /// (`before` → `now`) deleted too (Task 1 rule 5); `None` when every doc
    /// is.
    async fn carry_deletes(
        &self,
        mut merged: SplitRef,
        before: &Inputs,
        now: &Inputs,
        rows: &[u64],
        started: u64,
    ) -> Result<Option<SplitRef>, CollectionError> {
        let mut docs = RoaringBitmap::new();
        for ((split, old), new) in before.splits.iter().zip(&before.deleted).zip(&now.deleted) {
            for doc in new - old {
                let position = row_of(split, doc)
                    .and_then(|row| rows.binary_search(&row).ok())
                    .and_then(|position| u32::try_from(position).ok())
                    .ok_or_else(|| {
                        CollectionError::Corrupt(format!(
                            "doc {doc} of split {}, deleted during a merge, was not live when it started",
                            split.ulid
                        ))
                    })?;
                docs.insert(position);
            }
        }
        if docs.len() == merged.deleted_count {
            return Ok(Some(merged));
        }
        if docs.len() >= merged.doc_count {
            return Ok(None);
        }
        let doc_count = u32::try_from(merged.doc_count).map_err(|_| {
            CollectionError::Corrupt(format!("split {} has over 2^32 docs", merged.ulid))
        })?;
        let path = delete_bitmap_path(self.ns, self.cid, merged.ulid, object_ulid(started));
        let bytes = operon_text::encode_delete_bitmap(merged.ulid, doc_count, &docs)?;
        put_unique(&self.ctx().store, &path, bytes).await?;
        merged.deleted_count = docs.len();
        merged.delete_bitmap = Some(path);
        Ok(Some(merged))
    }
}
