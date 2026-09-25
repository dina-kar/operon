//! The collection link target (plan M1.1 Task 10; overview R7, R8, §6.4):
//! link apply folds the `DocOp` records of a collection's implicit stream
//! into a Lance version, a Tantivy split, delete bitmaps and the PK index,
//! under one collection manifest committed by a fenced, freshness-checked
//! CAS of the pointer `collection/<cid>`.
//!
//! Commit order (§03 §3.3): the Lance version (detached, R7), the split and
//! the changed delete bitmaps, the PK delta and the dead letters, the
//! manifest, the CAS, then the PK index. The PK index is derived state
//! (R8, Ruling 7): it is written only after the CAS, carries the `applied`
//! offsets it reflects under [`PK_WATERMARK_KEY`], and a new handle repairs
//! it from the manifest chain's PK deltas (or rebuilds it from Lance) before
//! it resolves any key.
//!
//! Per key, ops fold latest-wins in partition order (a key lives in one
//! partition; a record on another is dead-lettered). A *writing* op is an
//! upsert, or a patch whose result differs from the state before it; a key
//! whose final state is present gets a new row iff some op of the batch
//! wrote it, with the `(partition, offset)` of the last writing op. So the
//! result, `seq_no` included, does not depend on how records are batched.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lance::dataset::transaction::Operation;
use lance::index::DatasetIndexExt;
use operon_common::{CollectionId, NamespaceId};
use operon_link::{ApplyBatch, CommitError, LinkError, LinkTarget, LinkTargetFactory, TargetState};
use operon_meta::{
    ApplyError, COLLECTION_KIND, Collection, Consistency, Fence, Freshness, Link, LinkId,
    MetaClient, MetaError, collection_pk_prefix, collection_pointer_key, log_stale_object,
};
use operon_pk::{PkError, PkIndex, PkIndexConfig};
use operon_store::{Store, StoreError};
use roaring::RoaringBitmap;
use ulid::Ulid;

use crate::arrow_schema::{NewRow, to_record_batch};
use crate::chain::live_manifest;
use crate::codec::decode;
use crate::deadletter::{DeadLetter, encode_dead_letters};
use crate::doc::{DocOp, Document};
use crate::error::CollectionError;
use crate::lance::{LanceCommitter, pk_row_ids};
use crate::manifest::{CollectionManifest, CommitKind, SplitRef};
use crate::paths::{
    dead_letters_path, delete_bitmap_path, manifest_path, pk_delta_path, split_path,
};
use crate::pk::{PrimaryKey, partition_of};
use crate::pkindex::{PkWatermark, decode_pk_delta, encode_pk_delta, parse_pk_value, pk_value};
use crate::resolve::{fold, needs_current};
use crate::schema::{CollectionSchema, DynamicMapping};
use crate::snapshot::{CollectionContext, CollectionSnapshot};
use crate::tantivy_schema::{tantivy_layout, to_tantivy_doc};
use crate::values::{DocRejection, ExtractedDoc, check_document};

/// The PK index key of the watermark (Ruling 7). Primary keys start with
/// `0x01`, `0x02` or `0x03`, so it never collides with one.
pub const PK_WATERMARK_KEY: &[u8] = b"\x00watermark";

/// The first byte of every primary key's canonical bytes (`0x01` u64,
/// `0x02` uuid, `0x03` string).
const PK_TAGS: [u8; 3] = [0x01, 0x02, 0x03];

/// Entries per PK index write of a rebuild.
const REBUILD_CHUNK: usize = 10_000;

/// Concurrent PK index lookups within one round.
const LOOKUP_PARALLELISM: usize = 64;

/// The steps of a collection commit, where the crash gate's failpoints sit
/// and where a test hook can hold a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionCommitStep {
    /// After the Lance commit (`collection.after_lance_commit`).
    AfterLanceCommit,
    /// After the split and the delete bitmaps (`collection.after_split_put`).
    AfterSplitPut,
    /// After the manifest PUT (`collection.after_manifest_put`).
    AfterManifestPut,
    /// After the pointer CAS (`collection.after_cas`).
    AfterCas,
    /// After the PK index write (`collection.after_pk_write`).
    AfterPkWrite,
}

/// A test hook awaited at every [`CollectionCommitStep`], given the
/// committing task's fence. Only with the `test-util` feature.
#[cfg(feature = "test-util")]
pub type CollectionCommitHook = Arc<
    dyn Fn(CollectionCommitStep, Fence) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
>;

/// A test hook awaited after the `i`-th PK index write of a rebuild (0 is
/// the [`PkWatermark::rebuilding`] marker when the rebuild takes several
/// writes). Only with the `test-util` feature.
#[cfg(feature = "test-util")]
pub type RebuildHook = Arc<dyn Fn(usize) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

/// Serves [`COLLECTION_KIND`] links: one cached [`CollectionTarget`] per
/// link, so a PK index handle survives across task runs (Ruling 7).
pub struct CollectionTargetFactory {
    ctx: CollectionContext,
    targets: Mutex<BTreeMap<LinkId, Arc<CollectionTarget>>>,
    /// Entries per PK index write of a rebuild.
    rebuild_chunk: usize,
    #[cfg(feature = "test-util")]
    hook: Option<CollectionCommitHook>,
    #[cfg(feature = "test-util")]
    rebuild_hook: Option<RebuildHook>,
}

impl fmt::Debug for CollectionTargetFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("CollectionTargetFactory")
            .field("ctx", &self.ctx)
            .field("targets", &targets)
            .finish_non_exhaustive()
    }
}

impl CollectionTargetFactory {
    pub fn new(ctx: CollectionContext) -> Self {
        Self {
            ctx,
            targets: Mutex::default(),
            rebuild_chunk: REBUILD_CHUNK,
            #[cfg(feature = "test-util")]
            hook: None,
            #[cfg(feature = "test-util")]
            rebuild_hook: None,
        }
    }

    /// Test hook: every target of this factory awaits `hook` at each commit
    /// step. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: CollectionCommitHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Closes every cached PK index handle (server shutdown). A commit in
    /// flight finishes first.
    pub async fn close(&self) {
        let targets: Vec<Arc<CollectionTarget>> =
            std::mem::take(&mut *self.targets.lock().unwrap_or_else(PoisonError::into_inner))
                .into_values()
                .collect();
        close_all(targets).await;
    }

    /// Test hook: PK index rebuilds write `chunk` entries at a time and
    /// await `hook` after each write. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_rebuild_chunks(mut self, chunk: usize, hook: RebuildHook) -> Self {
        self.rebuild_chunk = chunk.max(1);
        self.rebuild_hook = Some(hook);
        self
    }

    /// The links whose targets are cached. Only with the `test-util`
    /// feature.
    #[cfg(feature = "test-util")]
    pub fn cached_links(&self) -> Vec<LinkId> {
        self.targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

/// Closes the PK index handles of `targets`, each after its commit in
/// flight, if any.
async fn close_all(targets: Vec<Arc<CollectionTarget>>) {
    for target in targets {
        let mut pk = target.pk.lock().await;
        discard(pk.take()).await;
    }
}

#[async_trait]
impl LinkTargetFactory for CollectionTargetFactory {
    fn kind(&self) -> &str {
        COLLECTION_KIND
    }

    /// The cached target of `link`, created on first use. It reads and
    /// commits through the context's metastore client; `meta` is unused.
    fn open(&self, _meta: &MetaClient, link: &Link) -> Result<Arc<dyn LinkTarget>, LinkError> {
        let mut targets = self.targets.lock().unwrap_or_else(PoisonError::into_inner);
        let target = targets.entry(link.id).or_insert_with(|| {
            Arc::new(CollectionTarget {
                link: link.id,
                namespace: link.namespace,
                ctx: self.ctx.clone(),
                parent: Mutex::default(),
                pk: tokio::sync::Mutex::default(),
                rebuild_chunk: self.rebuild_chunk,
                #[cfg(feature = "test-util")]
                hook: self.hook.clone(),
                #[cfg(feature = "test-util")]
                rebuild_hook: self.rebuild_hook.clone(),
            })
        });
        Ok(target.clone())
    }

    /// Evicts the targets of links that are gone (their collection was
    /// dropped) and closes their PK index handles.
    async fn retain(&self, links: &BTreeSet<LinkId>) {
        let gone: Vec<Arc<CollectionTarget>> = {
            let mut targets = self.targets.lock().unwrap_or_else(PoisonError::into_inner);
            let ids: Vec<LinkId> = targets
                .keys()
                .filter(|id| !links.contains(id))
                .copied()
                .collect();
            ids.iter().filter_map(|id| targets.remove(id)).collect()
        };
        close_all(gone).await;
    }
}

/// A manifest a commit builds on: the live one when it was read.
#[derive(Clone, Debug)]
struct Parent {
    /// `None` at version 0 (no commit yet).
    path: Option<String>,
    manifest: Arc<CollectionManifest>,
}

/// An open PK index writer and the watermark it holds.
struct PkState {
    index: PkIndex,
    watermark: PkWatermark,
}

/// What a fresh handle's repair did.
enum Repaired {
    /// The index now reflects the parent; the watermark it holds.
    To(PkWatermark),
    /// The index reflects a newer manifest than the parent (its watermark):
    /// nothing was written.
    StaleParent(PkWatermark),
}

/// What the manifest chain says about a PK index.
enum Replay {
    /// The keys changed since the manifest the index reflects.
    Deltas(BTreeMap<Vec<u8>, Option<u64>>),
    /// A manifest or PK delta of the chain is gone: rebuild from Lance.
    Gone,
    /// No manifest of the chain is the one the index reflects.
    Unmatched,
}

/// Closes a PK index handle that is no longer used.
async fn discard(state: Option<PkState>) {
    if let Some(state) = state
        && let Err(err) = state.index.close().await
    {
        tracing::debug!(%err, "closing a pk index handle");
    }
}

/// The collection target of one link.
pub struct CollectionTarget {
    link: LinkId,
    namespace: NamespaceId,
    ctx: CollectionContext,
    /// The manifest the last `load` or commit saw.
    parent: Mutex<Option<Parent>>,
    /// The PK index writer, kept across commits while its watermark matches
    /// the parent's `applied`. Held for a whole commit, so commits through
    /// one target never interleave.
    pk: tokio::sync::Mutex<Option<PkState>>,
    rebuild_chunk: usize,
    #[cfg(feature = "test-util")]
    hook: Option<CollectionCommitHook>,
    #[cfg(feature = "test-util")]
    rebuild_hook: Option<RebuildHook>,
}

impl fmt::Debug for CollectionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionTarget")
            .field("link", &self.link)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A new object's ULID, with the commit's start as its time: GC then sees
/// it as no younger than it is.
fn object_ulid(started_ms: u64) -> Ulid {
    Ulid::from_parts(started_ms, Ulid::generate().random())
}

/// PUTs `bytes` create-only at our own unique `path`. An `AlreadyExists` is
/// a retry of our own PUT (a lost acknowledgement) if the stored bytes are
/// identical; if nothing is there, the original (retryable) error stands.
async fn put_unique(store: &Store, path: &str, bytes: Bytes) -> Result<(), CollectionError> {
    match store.put_if_absent(path, bytes.clone()).await {
        Ok(_) => Ok(()),
        Err(err @ StoreError::AlreadyExists { .. }) => match store.get(path).await {
            Ok((stored, _)) if stored == bytes => Ok(()),
            Ok(_) => Err(CollectionError::Corrupt(format!(
                "{path} exists with other content"
            ))),
            Err(StoreError::NotFound { .. }) => Err(err.into()),
            Err(other) => Err(other.into()),
        },
        Err(err) => Err(err.into()),
    }
}

/// The first reason `rejection` gives, as a dead-letter reason.
fn schema_reason(rejection: DocRejection) -> String {
    match rejection {
        DocRejection::Violations(violations) => match violations.into_iter().next() {
            Some(v) => format!("schema: {}: {}", v.field, v.message),
            None => "schema: rejected".to_string(),
        },
        DocRejection::DynamicMappingRequired { paths } => {
            format!("schema: {}: dynamic mapping required", paths.join(", "))
        }
    }
}

/// Maximal runs of consecutive ids, in order (Ruling 8).
fn runs(ids: &[u64]) -> Vec<std::ops::Range<u64>> {
    let mut out: Vec<std::ops::Range<u64>> = Vec::new();
    for &id in ids {
        match out.last_mut() {
            Some(last) if last.end == id => last.end += 1,
            _ => out.push(id..id + 1),
        }
    }
    out
}

/// One op of a key, with the record that carried it.
struct KeyOp {
    partition: u32,
    offset: u64,
    op: DocOp,
    /// For a valid upsert, its document's typed values.
    extracted: Option<ExtractedDoc>,
}

/// A document to insert, with the record that last wrote it.
struct Insert {
    doc: Document,
    extracted: ExtractedDoc,
    partition: u32,
    offset: u64,
}

/// What a batch does: inserted documents (by key, in canonical order), old
/// rows to delete, keys deleted without replacement, and dead letters.
#[derive(Default)]
struct Resolution {
    inserts: BTreeMap<PrimaryKey, Insert>,
    deleted_rows: Vec<u64>,
    deleted_keys: Vec<PrimaryKey>,
    letters: Vec<DeadLetter>,
}

impl Resolution {
    fn letter(&mut self, partition: u32, record: &operon_log::OffsetRecord, reason: String) {
        tracing::warn!(
            partition,
            offset = record.offset,
            %reason,
            "a collection record was dead-lettered"
        );
        self.letters.push(DeadLetter {
            partition,
            offset: record.offset,
            key: record.record.key.as_ref().map(|k| k.to_vec()),
            value: record.record.value.as_ref().map(|v| v.to_vec()),
            reason,
        });
    }

    fn changes_rows(&self) -> bool {
        !self.inserts.is_empty() || !self.deleted_rows.is_empty()
    }
}

/// What one key's ops leave.
enum Outcome {
    Unchanged,
    Delete,
    Upsert(Insert),
}

/// Folds one key's ops over its committed document `current` (`None` if it
/// has no row, or if the first op overwrites it and it was not read). `ops`
/// hold no invalid upsert. A patch whose result breaks `lenient` is
/// dead-lettered into `resolution` and skipped.
///
/// The key gets a new row iff its final state is present and some op
/// *wrote* it: an upsert, or a patch whose result differs from the state
/// before it (controller ruling P32, refining rule 3.5's "differs from the
/// committed one", which cannot be told for keys whose committed document is
/// not read). The row carries the last writing op's record, so a key's
/// result and `seq_no` do not depend on how its records were batched, and a
/// patch that changes nothing keeps the committed row.
fn fold_key(
    lenient: &CollectionSchema,
    current: Option<Document>,
    ops: Vec<KeyOp>,
    records: &BTreeMap<(u32, u64), &operon_log::OffsetRecord>,
    resolution: &mut Resolution,
    has_row: bool,
) -> Result<Outcome, CollectionError> {
    let mut state = current;
    // The last writing op: the typed values of its result and its record.
    let mut written: Option<(ExtractedDoc, u32, u64)> = None;
    for key_op in ops {
        let KeyOp {
            partition,
            offset,
            op,
            extracted,
        } = key_op;
        match &op {
            DocOp::Upsert(_) => {
                state = fold(state, [&op]);
                written = extracted.map(|e| (e, partition, offset));
            }
            DocOp::Delete(_) => {
                state = None;
                written = None;
            }
            DocOp::Patch { .. } => match fold(state.clone(), [&op]) {
                // A patch of a missing key without `upsert`: nothing.
                None => {}
                // No change: not a write.
                Some(next) if state.as_ref() == Some(&next) => {}
                Some(next) => match check_document(lenient, &next) {
                    Ok(e) => {
                        state = Some(next);
                        written = Some((e, partition, offset));
                    }
                    Err(rejection) => {
                        let record = records.get(&(partition, offset)).ok_or_else(|| {
                            CollectionError::Internal(format!(
                                "record {partition}/{offset} of a key's ops is not in the batch"
                            ))
                        })?;
                        resolution.letter(partition, record, schema_reason(rejection));
                    }
                },
            },
        }
    }
    Ok(match (state, written) {
        (Some(doc), Some((extracted, partition, offset))) => Outcome::Upsert(Insert {
            doc,
            extracted,
            partition,
            offset,
        }),
        // Only no-op patches (and dead letters): the committed row stands.
        (Some(_), None) => Outcome::Unchanged,
        (None, _) if has_row => Outcome::Delete,
        (None, _) => Outcome::Unchanged,
    })
}

impl CollectionTarget {
    async fn step(&self, step: CollectionCommitStep, fence: &Fence) {
        match step {
            CollectionCommitStep::AfterLanceCommit => {
                crate::failpoint!("collection.after_lance_commit");
            }
            CollectionCommitStep::AfterSplitPut => {
                crate::failpoint!("collection.after_split_put");
            }
            CollectionCommitStep::AfterManifestPut => {
                crate::failpoint!("collection.after_manifest_put");
            }
            CollectionCommitStep::AfterCas => {
                crate::failpoint!("collection.after_cas");
            }
            CollectionCommitStep::AfterPkWrite => {
                crate::failpoint!("collection.after_pk_write");
            }
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    fn remember(&self, parent: Parent) {
        *self.parent.lock().unwrap_or_else(PoisonError::into_inner) = Some(parent);
    }

    fn remembered(&self, version: u64) -> Option<Parent> {
        self.parent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .filter(|p| p.manifest.version == version)
    }

    /// The collection of this link, else `NotFound` (it was dropped).
    async fn collection(&self, consistency: Consistency) -> Result<Collection, CollectionError> {
        let link = self.link;
        self.ctx
            .meta
            .read(consistency, |s| s.collection_for_link(link).cloned())
            .await?
            .ok_or_else(|| CollectionError::NotFound(format!("collection of link {link}")))
    }

    /// The live manifest of `cid` (the empty one before the first commit).
    async fn live(&self, cid: CollectionId) -> Result<Parent, CollectionError> {
        let ctx = &self.ctx;
        let live = live_manifest(
            &ctx.meta,
            &ctx.store,
            &ctx.manifests,
            self.namespace,
            cid,
            Consistency::Local,
        )
        .await?;
        Ok(match live {
            Some((path, manifest)) => Parent {
                path: Some(path),
                manifest,
            },
            None => Parent {
                path: None,
                manifest: Arc::new(CollectionManifest::empty(cid)),
            },
        })
    }

    async fn load_state(&self) -> Result<TargetState, CollectionError> {
        let collection = match self.collection(Consistency::Local).await {
            Ok(collection) => collection,
            Err(err) => {
                // Dropped: the handle is of no more use. Never waits for a
                // commit in flight (that commit closes it itself).
                if matches!(err, CollectionError::NotFound(_))
                    && let Ok(mut pk) = self.pk.try_lock()
                {
                    discard(pk.take()).await;
                }
                return Err(err);
            }
        };
        let parent = self.live(collection.id).await?;
        let state = TargetState {
            version: parent.manifest.version,
            applied: parent.manifest.applied.clone(),
        };
        self.remember(parent);
        Ok(state)
    }

    /// The PK index handle for a commit on `parent`: the cached one if its
    /// watermark matches `parent.applied`, else a new one (which fences
    /// every other writer), repaired to `parent`. If the index is ahead of
    /// `parent`, the parent is stale: the new handle is kept (with the
    /// index's watermark) and the commit is a `Conflict`, so the task
    /// reloads; the index is never rolled back.
    async fn pk_handle<'g>(
        &self,
        pk: &'g mut Option<PkState>,
        cid: CollectionId,
        parent: &Parent,
    ) -> Result<&'g mut PkState, CommitError> {
        let reusable = pk
            .as_ref()
            .is_some_and(|state| state.watermark.applied == parent.manifest.applied);
        if !reusable {
            discard(pk.take()).await;
            let index = PkIndex::open(
                &self.ctx.store,
                &collection_pk_prefix(self.namespace, cid),
                PkIndexConfig::default(),
            )
            .await
            .map_err(CollectionError::from)?;
            match self.repair(&index, cid, parent).await {
                Ok(Repaired::To(watermark)) => *pk = Some(PkState { index, watermark }),
                Ok(Repaired::StaleParent(watermark)) => {
                    tracing::info!(
                        link = %self.link,
                        parent = parent.manifest.version,
                        index = watermark.manifest_version,
                        "the pk index is ahead of the commit's parent; reloading"
                    );
                    *pk = Some(PkState { index, watermark });
                    return Err(CommitError::Conflict);
                }
                Err(err) => {
                    let state = PkState {
                        index,
                        watermark: PkWatermark::default(),
                    };
                    discard(Some(state)).await;
                    return Err(err.into());
                }
            }
        }
        Ok(pk
            .as_mut()
            .ok_or_else(|| CollectionError::Internal("no pk index handle".to_string()))?)
    }

    /// Brings a fresh handle's index to `parent` (rule 5), unless the index
    /// is ahead of `parent` (written by a commit `parent` does not know).
    async fn repair(
        &self,
        index: &PkIndex,
        cid: CollectionId,
        parent: &Parent,
    ) -> Result<Repaired, CollectionError> {
        let current = match index.get(PK_WATERMARK_KEY).await? {
            Some(bytes) => PkWatermark::decode(&bytes)?,
            None => PkWatermark::default(),
        };
        if current.applied == parent.manifest.applied {
            return Ok(Repaired::To(current));
        }
        let target = PkWatermark {
            manifest_version: parent.manifest.version,
            applied: parent.manifest.applied.clone(),
        };
        if current == PkWatermark::rebuilding() {
            self.rebuild(index, cid, parent, &target).await?;
            return Ok(Repaired::To(target));
        }
        if current.manifest_version > parent.manifest.version {
            return Ok(Repaired::StaleParent(current));
        }
        match self.replayed_deltas(&current, parent).await? {
            Replay::Deltas(entries) => {
                let mut batch: Vec<(Bytes, Option<Bytes>)> = entries
                    .into_iter()
                    .map(|(key, row)| (Bytes::from(key), row.map(pk_bytes)))
                    .collect();
                batch.push(watermark_entry(&target));
                index.write(batch).await?;
                tracing::info!(
                    link = %self.link,
                    from = current.manifest_version,
                    to = target.manifest_version,
                    "repaired the pk index from pk deltas"
                );
            }
            Replay::Gone => {
                self.rebuild(index, cid, parent, &target).await?;
            }
            // No manifest of the chain matches the index: it reflects a
            // commit `parent` does not know.
            Replay::Unmatched => return Ok(Repaired::StaleParent(current)),
        }
        Ok(Repaired::To(target))
    }

    /// The PK changes between the manifest the index reflects (`watermark`)
    /// and `parent`, merged oldest first.
    async fn replayed_deltas(
        &self,
        watermark: &PkWatermark,
        parent: &Parent,
    ) -> Result<Replay, CollectionError> {
        let ctx = &self.ctx;
        // Newest first: the manifests the index does not reflect yet.
        let mut newer: Vec<Arc<CollectionManifest>> = Vec::new();
        let mut manifest = parent.manifest.clone();
        loop {
            if manifest.applied == watermark.applied {
                break;
            }
            if manifest.version == 0 {
                // Nothing committed, yet the index holds something.
                return Ok(Replay::Unmatched);
            }
            newer.push(manifest.clone());
            let Some(path) = manifest.parent_manifest.clone() else {
                // Passed the root: fine only if the index is empty.
                if *watermark == PkWatermark::default() {
                    break;
                }
                return Ok(Replay::Unmatched);
            };
            let ancestor = match ctx.manifests.load(&ctx.store, &path).await {
                Ok(ancestor) => ancestor,
                Err(CollectionError::Store(StoreError::NotFound { .. })) => {
                    return Ok(Replay::Gone);
                }
                Err(err) => return Err(err),
            };
            if ancestor.version != manifest.parent_version
                || ancestor.collection_id != manifest.collection_id
            {
                return Err(CollectionError::Corrupt(format!(
                    "{path} is version {} of collection {}, not the parent version {} of collection {}",
                    ancestor.version,
                    ancestor.collection_id,
                    manifest.parent_version,
                    manifest.collection_id
                )));
            }
            manifest = ancestor;
        }
        let mut merged = BTreeMap::new();
        for manifest in newer.iter().rev() {
            if manifest.kind != CommitKind::LinkApply {
                continue;
            }
            let Some(path) = &manifest.pk_delta else {
                continue;
            };
            let bytes = match ctx.store.get(path).await {
                Ok((bytes, _)) => bytes,
                Err(StoreError::NotFound { .. }) => return Ok(Replay::Gone),
                Err(err) => return Err(err.into()),
            };
            merged.extend(decode_pk_delta(&bytes)?);
        }
        Ok(Replay::Deltas(merged))
    }

    /// Rebuilds the index from `parent`'s Lance version: puts for every row,
    /// deletes for every other key, then `target`. A rebuild that needs
    /// several writes first replaces the watermark with
    /// [`PkWatermark::rebuilding`] and writes `target` last, so an
    /// interrupted rebuild is never trusted and runs again.
    async fn rebuild(
        &self,
        index: &PkIndex,
        cid: CollectionId,
        parent: &Parent,
        target: &PkWatermark,
    ) -> Result<(), CollectionError> {
        let rows = match parent.manifest.lance_version {
            0 => Vec::new(),
            version => {
                let dataset = self.ctx.lance.open(self.namespace, cid, version).await?;
                pk_row_ids(&dataset, None).await?
            }
        };
        let live: BTreeMap<Vec<u8>, u64> = rows.into_iter().collect();
        let mut entries: Vec<(Bytes, Option<Bytes>)> = Vec::new();
        for tag in PK_TAGS {
            for (key, _) in index.scan_prefix(&[tag], usize::MAX).await? {
                if !live.contains_key(key.as_ref()) {
                    entries.push((key, None));
                }
            }
        }
        entries.extend(
            live.into_iter()
                .map(|(key, row)| (Bytes::from(key), Some(pk_bytes(row)))),
        );
        entries.push(watermark_entry(target));
        let mut writes = 0;
        if entries.len() > self.rebuild_chunk {
            index
                .write(vec![watermark_entry(&PkWatermark::rebuilding())])
                .await?;
            self.rebuild_step(writes).await;
            writes += 1;
        }
        let mut entries = entries.into_iter().peekable();
        while entries.peek().is_some() {
            index
                .write(entries.by_ref().take(self.rebuild_chunk).collect())
                .await?;
            self.rebuild_step(writes).await;
            writes += 1;
        }
        tracing::info!(
            link = %self.link,
            to = target.manifest_version,
            writes,
            "rebuilt the pk index from lance"
        );
        Ok(())
    }

    async fn rebuild_step(&self, write: usize) {
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.rebuild_hook {
            hook(write).await;
        }
        let _ = write;
    }

    /// Decodes the batch and folds every key (rules 3.4 and 3.5).
    async fn resolve(
        &self,
        index: &PkIndex,
        collection: &Collection,
        snapshot: &CollectionSnapshot,
        batch: &ApplyBatch,
    ) -> Result<Resolution, CollectionError> {
        let lenient = CollectionSchema {
            dynamic: DynamicMapping::Ignore,
            ..collection.schema.clone()
        };
        let mut resolution = Resolution::default();
        let mut records = BTreeMap::new();
        let mut keys: BTreeMap<PrimaryKey, Vec<KeyOp>> = BTreeMap::new();
        for (partition, record) in &batch.records {
            let partition = *partition;
            records.insert((partition, record.offset), record);
            let op = match decode(&record.record) {
                Ok(op) => op,
                Err(err) => {
                    resolution.letter(partition, record, format!("undecodable: {err}"));
                    continue;
                }
            };
            let home = partition_of(op.pk(), collection.partitions);
            if home != partition {
                let reason = format!("wrong_partition: the key belongs to partition {home}");
                resolution.letter(partition, record, reason);
                continue;
            }
            // An invalid upsert is skipped before anything else looks at the
            // key's ops, so it cannot decide whether the key is read.
            let extracted = match &op {
                DocOp::Upsert(doc) => match check_document(&lenient, doc) {
                    Ok(extracted) => Some(extracted),
                    Err(rejection) => {
                        resolution.letter(partition, record, schema_reason(rejection));
                        continue;
                    }
                },
                _ => None,
            };
            keys.entry(op.pk().clone()).or_default().push(KeyOp {
                partition,
                offset: record.offset,
                op,
                extracted,
            });
        }
        let keys: Vec<(PrimaryKey, Vec<KeyOp>)> = keys.into_iter().collect();
        let mut keys = keys.into_iter().peekable();
        let round = self.ctx.config.max_lookup_batch.max(1);
        while keys.peek().is_some() {
            let chunk: Vec<(PrimaryKey, Vec<KeyOp>)> = keys.by_ref().take(round).collect();
            let canonical: Vec<Vec<u8>> = chunk.iter().map(|(pk, _)| pk.canonical()).collect();
            let lookups: Vec<_> = canonical.iter().map(|key| index.get(key)).collect();
            let values: Vec<Option<Bytes>> = futures::stream::iter(lookups)
                .buffered(LOOKUP_PARALLELISM)
                .try_collect()
                .await?;
            let old = values
                .iter()
                .map(|value| value.as_deref().map(parse_pk_value).transpose())
                .collect::<Result<Vec<Option<u64>>, _>>()?;
            let fetch: Vec<u64> = chunk
                .iter()
                .zip(&old)
                .filter(|((_, ops), row)| row.is_some() && needs_current(ops.iter().map(|o| &o.op)))
                .filter_map(|(_, row)| *row)
                .collect();
            let fetched = snapshot.take_rows(&fetch).await?;
            let mut current: BTreeMap<u64, Document> = BTreeMap::new();
            for (row_id, doc) in fetch.iter().zip(fetched) {
                let doc = doc.ok_or_else(|| {
                    CollectionError::Corrupt(format!(
                        "the pk index names row {row_id}, which lance version {} does not have",
                        snapshot.manifest().lance_version
                    ))
                })?;
                current.insert(
                    *row_id,
                    Document {
                        pk: doc.pk,
                        source: doc.source,
                        vectors: doc.vectors,
                        sparse_vectors: doc.sparse_vectors,
                    },
                );
            }
            for ((pk, ops), old) in chunk.into_iter().zip(old) {
                let committed = old.and_then(|row| current.remove(&row));
                if let Some(doc) = &committed
                    && doc.pk != pk
                {
                    return Err(CollectionError::Corrupt(format!(
                        "the pk index maps {pk:?} to a row of {:?}",
                        doc.pk
                    )));
                }
                match fold_key(
                    &lenient,
                    committed,
                    ops,
                    &records,
                    &mut resolution,
                    old.is_some(),
                )? {
                    Outcome::Unchanged => {}
                    Outcome::Delete => {
                        resolution.deleted_rows.extend(old);
                        resolution.deleted_keys.push(pk);
                    }
                    Outcome::Upsert(insert) => {
                        resolution.deleted_rows.extend(old);
                        resolution.inserts.insert(pk, insert);
                    }
                }
            }
        }
        resolution.letters.sort_by_key(|l| (l.partition, l.offset));
        Ok(resolution)
    }

    /// Everything of a commit up to and including the PK write (rule 3).
    async fn commit_locked(
        &self,
        pk: &mut Option<PkState>,
        expected_version: u64,
        batch: ApplyBatch,
        fence: &Fence,
    ) -> Result<u64, CommitError> {
        let ctx = &self.ctx;
        let ns = self.namespace;
        // 1. The parent.
        let started = ctx.meta.now_ms();
        let parent = match self.remembered(expected_version) {
            Some(parent) => parent,
            None => {
                let collection = self.collection(Consistency::Local).await?;
                let parent = self.live(collection.id).await?;
                if parent.manifest.version != expected_version {
                    return Err(CommitError::Conflict);
                }
                parent
            }
        };
        let v = parent.manifest.version;
        // 2. The collection and its current schema. Linearizable: a lagging
        // local schema could dead-letter records validated against a newer
        // one.
        let collection = self.collection(Consistency::Linearizable).await?;
        let cid = collection.id;
        if cid != parent.manifest.collection_id {
            return Err(CollectionError::NotFound(format!(
                "collection {} of link {}",
                parent.manifest.collection_id, self.link
            ))
            .into());
        }
        let schema = &collection.schema;
        let skipped = skipped_offsets(&parent.manifest, &batch)?;
        let snapshot = CollectionSnapshot::at(
            ctx,
            ns,
            collection.clone(),
            parent.path.clone(),
            parent.manifest.clone(),
        )
        .await?;
        // 3.–5. The PK handle, then every key.
        let state = self.pk_handle(pk, cid, &parent).await?;
        let resolution = self
            .resolve(&state.index, &collection, &snapshot, &batch)
            .await?;

        // 6. Lance.
        let mut lance_version = parent.manifest.lance_version;
        let mut vector_indexes = parent.manifest.vector_indexes.clone();
        let mut scalar_indexes = parent.manifest.scalar_indexes.clone();
        let mut new_rows: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        if resolution.changes_rows() {
            let base = match snapshot.dataset() {
                Some(dataset) => dataset.clone(),
                None => ctx.lance.ensure_created(ns, cid).await?,
            };
            let base = LanceCommitter::ensure_vectors(&ctx.lance, &base, schema).await?;
            let mut new_fragments = Vec::new();
            if !resolution.inserts.is_empty() {
                let rows: Vec<NewRow<'_>> = resolution
                    .inserts
                    .values()
                    .map(|insert| NewRow {
                        doc: &insert.doc,
                        partition: insert.partition,
                        offset: insert.offset,
                    })
                    .collect();
                let batch = to_record_batch(schema, &rows)?;
                new_fragments = LanceCommitter::write_fragments(&ctx.lance, &base, batch).await?;
            }
            let operation = if resolution.deleted_rows.is_empty() {
                Operation::Append {
                    fragments: new_fragments,
                }
            } else {
                let (updated_fragments, removed_fragment_ids) =
                    LanceCommitter::delete_rows(&base, &resolution.deleted_rows).await?;
                Operation::Update {
                    removed_fragment_ids,
                    updated_fragments,
                    new_fragments,
                    fields_modified: vec![],
                    compacted_sstables: vec![],
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: None,
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                }
            };
            let committed = LanceCommitter::commit(&ctx.lance, &base, operation).await?;
            self.step(CollectionCommitStep::AfterLanceCommit, fence)
                .await;
            // 7. Row ids of the new rows.
            new_rows = LanceCommitter::new_row_ids(&committed, &base)
                .await?
                .into_iter()
                .collect();
            if new_rows.len() != resolution.inserts.len() {
                return Err(CollectionError::Internal(format!(
                    "lance version {} has {} new rows, not the {} written",
                    committed.manifest.version,
                    new_rows.len(),
                    resolution.inserts.len()
                ))
                .into());
            }
            lance_version = committed.manifest.version;
            let uuids: BTreeSet<String> = committed
                .load_indices()
                .await
                .map_err(CollectionError::from)?
                .iter()
                .map(|index| index.uuid.to_string())
                .collect();
            vector_indexes.retain(|v| uuids.contains(&v.lance_index_uuid));
            scalar_indexes.retain(|s| uuids.contains(&s.lance_index_uuid));
        } else {
            self.step(CollectionCommitStep::AfterLanceCommit, fence)
                .await;
        }

        // 8. The split of the new rows and the changed delete bitmaps.
        let mut splits = parent.manifest.splits.clone();
        let mut deleted_docs: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
        for &row_id in &resolution.deleted_rows {
            let (split, doc) = snapshot.locate_row(row_id).ok_or_else(|| {
                CollectionError::Corrupt(format!(
                    "row {row_id} is in no split of manifest version {v}"
                ))
            })?;
            deleted_docs.entry(split).or_default().insert(doc);
        }
        let mut emptied = BTreeSet::new();
        for (index, docs) in deleted_docs {
            let split = &mut splits[index];
            let mut deleted = snapshot
                .deleted_docs(&parent.manifest.splits[index])
                .await?;
            deleted |= docs;
            split.deleted_count = deleted.len();
            if split.deleted_count >= split.doc_count {
                emptied.insert(index);
                continue;
            }
            let doc_count = u32::try_from(split.doc_count).map_err(|_| {
                CollectionError::Corrupt(format!("split {} has over 2^32 docs", split.ulid))
            })?;
            let path = delete_bitmap_path(ns, cid, split.ulid, object_ulid(started));
            let bytes = operon_text::encode_delete_bitmap(split.ulid, doc_count, &deleted)
                .map_err(CollectionError::from)?;
            put_unique(&ctx.store, &path, bytes).await?;
            split.delete_bitmap = Some(path);
        }
        let mut splits: Vec<SplitRef> = splits
            .into_iter()
            .enumerate()
            .filter(|(index, _)| !emptied.contains(index))
            .map(|(_, split)| split)
            .collect();
        if !resolution.inserts.is_empty() {
            splits.push(
                self.write_split(ns, cid, schema, &resolution, &new_rows, started)
                    .await?,
            );
        }
        self.step(CollectionCommitStep::AfterSplitPut, fence).await;

        // 9. The PK delta and the dead letters.
        let mut delta: Vec<(Vec<u8>, Option<u64>)> = Vec::new();
        for pk in resolution.inserts.keys() {
            let key = pk.canonical();
            let row = new_rows.get(&key).copied().ok_or_else(|| {
                CollectionError::Internal(format!("lance has no new row for {pk:?}"))
            })?;
            delta.push((key, Some(row)));
        }
        delta.extend(
            resolution
                .deleted_keys
                .iter()
                .map(|pk| (pk.canonical(), None)),
        );
        delta.sort_by(|a, b| a.0.cmp(&b.0));
        let pk_delta = match delta.is_empty() {
            true => None,
            false => {
                let path = pk_delta_path(ns, cid, v + 1, object_ulid(started));
                put_unique(&ctx.store, &path, encode_pk_delta(&delta)).await?;
                Some(path)
            }
        };
        let dead_letters = match resolution.letters.is_empty() {
            true => None,
            false => {
                let path = dead_letters_path(ns, cid, v + 1, object_ulid(started));
                put_unique(&ctx.store, &path, encode_dead_letters(&resolution.letters)).await?;
                Some(path)
            }
        };

        // 10. The manifest.
        let mut applied = parent.manifest.applied.clone();
        applied.extend(batch.applied_after.iter().map(|(p, o)| (*p, *o)));
        let inserted = resolution.inserts.len() as u64;
        let deleted = resolution.deleted_rows.len() as u64;
        let live_doc_count = (parent.manifest.live_doc_count + inserted)
            .checked_sub(deleted)
            .ok_or_else(|| {
                CollectionError::Corrupt(format!(
                    "manifest version {v} counts {} live docs, fewer than the {deleted} deleted",
                    parent.manifest.live_doc_count
                ))
            })?;
        let manifest = CollectionManifest {
            version: v + 1,
            parent_version: v,
            parent_manifest: parent.path.clone(),
            collection_id: cid,
            schema_version: schema.version,
            created_at_ms: started,
            lance_version,
            splits,
            vector_indexes,
            scalar_indexes,
            hot_artifacts: parent.manifest.hot_artifacts.clone(),
            applied,
            live_doc_count,
            kind: CommitKind::LinkApply,
            pk_delta,
            dead_letters,
            dead_letters_total: parent.manifest.dead_letters_total
                + resolution.letters.len() as u64,
            skipped_offsets_total: parent.manifest.skipped_offsets_total + skipped,
        };
        let path = manifest_path(ns, cid, v + 1, object_ulid(started));
        put_unique(
            &ctx.store,
            &path,
            crate::manifest::encode_manifest(&manifest),
        )
        .await?;
        self.step(CollectionCommitStep::AfterManifestPut, fence)
            .await;

        // 11. Too slow: GC could delete the new objects around the time the
        // pointer starts referencing them. Leave them unreferenced.
        let max_commit_delay = ctx.config.max_commit_delay;
        let took = ctx.meta.now_ms().saturating_sub(started);
        if took > millis(max_commit_delay) {
            tracing::warn!(
                link = %self.link,
                manifest = %path,
                took_ms = took,
                max_commit_delay_ms = millis(max_commit_delay),
                "a collection commit took longer than max_commit_delay; it is not committed"
            );
            return Err(CollectionError::Blocked(format!(
                "the commit of {path} took {took} ms, longer than {max_commit_delay:?}"
            ))
            .into());
        }

        // 12. The CAS.
        let fresh = Freshness {
            created_at_ms: started,
            max_age_ms: millis(max_commit_delay),
        };
        let result = ctx
            .meta
            .cas_pointer_fresh(
                ns,
                &collection_pointer_key(cid),
                (v > 0).then_some(v),
                &path,
                Some(fence.clone()),
                Some(fresh),
            )
            .await;
        let version = match result {
            Ok(version) => version,
            // A lost acknowledgement: the pointer names our manifest, which
            // only we could have written (a unique path).
            Err(MetaError::Rejected(ApplyError::VersionMismatch {
                current: Some(current),
            })) if current.version == v + 1 && current.value == path => current.version,
            Err(err) => return Err(cas_error(err, ctx.meta.now_ms(), cid)),
        };
        if version != v + 1 {
            return Err(CollectionError::Corrupt(format!(
                "the pointer of collection {cid} moved from {v} to {version}"
            ))
            .into());
        }
        self.step(CollectionCommitStep::AfterCas, fence).await;
        let watermark = PkWatermark {
            manifest_version: version,
            applied: manifest.applied.clone(),
        };
        self.remember(Parent {
            path: Some(path),
            manifest: Arc::new(manifest),
        });

        // 13. The PK index, after the CAS.
        let mut write: Vec<(Bytes, Option<Bytes>)> = delta
            .into_iter()
            .map(|(key, row)| (Bytes::from(key), row.map(pk_bytes)))
            .collect();
        write.push(watermark_entry(&watermark));
        let written = match pk.as_mut() {
            Some(state) => state.index.write(write).await,
            None => Err(PkError::Closed),
        };
        match written {
            Ok(()) => {
                if let Some(state) = pk.as_mut() {
                    state.watermark = watermark;
                }
            }
            // The commit landed; the next task repairs the index.
            Err(PkError::Fenced) => return Err(CommitError::Fenced),
            Err(err) => {
                tracing::warn!(
                    link = %self.link,
                    version,
                    %err,
                    "writing the pk index after a commit failed; the next commit repairs it"
                );
                discard(pk.take()).await;
            }
        }
        self.step(CollectionCommitStep::AfterPkWrite, fence).await;
        Ok(version)
    }

    /// Builds and PUTs the split of the new rows, in ascending row-id order
    /// (Ruling 8).
    async fn write_split(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        schema: &CollectionSchema,
        resolution: &Resolution,
        new_rows: &BTreeMap<Vec<u8>, u64>,
        started: u64,
    ) -> Result<SplitRef, CollectionError> {
        let mut docs: Vec<(u64, &Insert)> = resolution
            .inserts
            .iter()
            .map(|(pk, insert)| {
                new_rows
                    .get(&pk.canonical())
                    .map(|row| (*row, insert))
                    .ok_or_else(|| {
                        CollectionError::Internal(format!("lance has no new row for {pk:?}"))
                    })
            })
            .collect::<Result<_, _>>()?;
        docs.sort_by_key(|(row, _)| *row);
        let layout = tantivy_layout(schema);
        let row_ids: Vec<u64> = docs.iter().map(|(row, _)| *row).collect();
        let tantivy_docs = docs
            .iter()
            .map(|(row, insert)| {
                to_tantivy_doc(&layout, schema, &insert.doc, &insert.extracted, *row)
            })
            .collect();
        let tantivy_schema = layout.schema.clone();
        let built = tokio::task::spawn_blocking(move || {
            operon_text::build_split(tantivy_schema, tantivy_docs)
        })
        .await
        .map_err(|err| CollectionError::Internal(format!("building a split: {err}")))??;
        let ulid = object_ulid(started);
        let size_bytes = built.bytes.len() as u64;
        put_unique(&self.ctx.store, &split_path(ns, cid, ulid), built.bytes).await?;
        Ok(SplitRef {
            ulid,
            doc_count: built.doc_count,
            deleted_count: 0,
            size_bytes,
            footer_range: built.footer_range,
            row_id_ranges: runs(&row_ids),
            delete_bitmap: None,
            schema_version: schema.version,
            created_at_ms: started,
            merge_ops: 0,
        })
    }
}

/// A PK index value for `row`.
fn pk_bytes(row: u64) -> Bytes {
    Bytes::copy_from_slice(&pk_value(row))
}

fn watermark_entry(watermark: &PkWatermark) -> (Bytes, Option<Bytes>) {
    (
        Bytes::from_static(PK_WATERMARK_KEY),
        Some(watermark.encode()),
    )
}

/// Offsets the batch covers without a record (trimmed before the link got
/// there). A record outside its partition's range, or a partition that goes
/// back, is `Corrupt`.
fn skipped_offsets(
    parent: &CollectionManifest,
    batch: &ApplyBatch,
) -> Result<u64, CollectionError> {
    let mut per_partition: BTreeMap<u32, u64> = BTreeMap::new();
    for (partition, record) in &batch.records {
        let from = parent.applied.get(partition).copied().unwrap_or(0);
        let to = batch.applied_after.get(partition).copied().unwrap_or(from);
        if !(from..to).contains(&record.offset) {
            return Err(CollectionError::Corrupt(format!(
                "record {partition}/{} is outside the batch's range {from}..{to}",
                record.offset
            )));
        }
        *per_partition.entry(*partition).or_default() += 1;
    }
    let mut skipped = 0u64;
    for (partition, to) in &batch.applied_after {
        let from = parent.applied.get(partition).copied().unwrap_or(0);
        let covered = to.checked_sub(from).ok_or_else(|| {
            CollectionError::Corrupt(format!(
                "partition {partition} would go back from {from} to {to}"
            ))
        })?;
        let records = per_partition.get(partition).copied().unwrap_or(0);
        skipped += covered.saturating_sub(records);
    }
    Ok(skipped)
}

/// Maps a refused pointer CAS; `proposer_now_ms` is this node's metastore
/// clock, logged with a stale-object refusal.
fn cas_error(err: MetaError, proposer_now_ms: u64, cid: CollectionId) -> CommitError {
    match err {
        MetaError::Rejected(ApplyError::VersionMismatch { .. }) => CommitError::Conflict,
        MetaError::Rejected(ApplyError::Fenced { .. }) => CommitError::Fenced,
        // Too late: the new objects are left to GC, and the next run commits
        // the batch again with new ones.
        MetaError::Rejected(err @ ApplyError::StaleObject { .. }) => {
            log_stale_object(&err, proposer_now_ms);
            CommitError::Other(LinkError::Blocked(err.to_string()))
        }
        // Dropped while the task ran (Ruling 14).
        MetaError::Rejected(ApplyError::CollectionNotFound(_)) => {
            CommitError::Other(LinkError::NotFound(format!("collection {cid}")))
        }
        other => CommitError::Other(LinkError::Meta(other)),
    }
}

impl From<CollectionError> for CommitError {
    /// A fenced PK index writer is a fenced commit; anything else is the
    /// target's error.
    fn from(err: CollectionError) -> Self {
        match err {
            CollectionError::Pk(PkError::Fenced) => CommitError::Fenced,
            other => CommitError::Other(other.into()),
        }
    }
}

#[async_trait]
impl LinkTarget for CollectionTarget {
    /// Reads the collection and its live manifest (one pointer read and at
    /// most one manifest GET); never opens a writer.
    async fn load(&self) -> Result<TargetState, LinkError> {
        Ok(self.load_state().await?)
    }

    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        fence: &Fence,
    ) -> Result<u64, CommitError> {
        let mut pk = self.pk.lock().await;
        let result = self
            .commit_locked(&mut pk, expected_version, batch, fence)
            .await;
        // Fenced (the lease or the PK index moved on) or dropped: the handle
        // is of no more use.
        if matches!(
            result,
            Err(CommitError::Fenced | CommitError::Other(LinkError::NotFound(_)))
        ) {
            discard(pk.take()).await;
        }
        result
    }
}
