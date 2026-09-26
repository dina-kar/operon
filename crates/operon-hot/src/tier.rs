//! [`HotTierImpl`]: the hot tier M1.2's query engine consults (plan M1.3
//! Task 6 rules 4–6). A reconcile loop loads the HNSW artifacts of the hot
//! collections this node owns, extends each artifact's delta index for every
//! new manifest version, and publishes a [`ColumnView`] per version; `ann`
//! answers with the view of exactly the version asked for.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use futures::StreamExt;
use operon_collection::{
    CollectionContext, CollectionManifest, CollectionSnapshot, HotArtifactRef, SplitRef,
    lance_prefix, split_path, vector_column,
};
use operon_common::meta::{ApplyError, Consistency, HotConfig, Lease, MetaError};
use operon_common::{CollectionId, NamespaceId};
use operon_hnsw::HnswEngine;
use operon_query::hot::{HotAnn, HotTier};
use operon_query::placement::{Owner, Placement};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::TierError;
use crate::artifact::{CurrencyCache, HNSW_KIND, effective_source_version};
use crate::build::{PROMOTE_LEASE_PREFIX, effective_hot, promote_lease_key};
use crate::config::HotTierConfig;
use crate::delta::DeltaIndex;
use crate::live::{DeletedDocsCache, live_rows_cached};
use crate::prefetch::{FragmentProgress, PrefetchPass, prefetch_fragments_resuming};
use crate::splits::{PinnedSplits, delete_files, download_split};
use crate::view::{ColumnView, LoadedArtifact};

/// The shortest time between two reconcile passes of the loop (rule 5).
const MIN_RECONCILE_GAP: Duration = Duration::from_millis(100);

/// The local subdirectories of loaded artifacts and delta indexes.
const HNSW_DIR: &str = "hnsw";
const DELTA_DIR: &str = "delta";
/// The local subdirectory of pinned splits (Task 7).
const SPLITS_DIR: &str = "splits";

/// What the tier has served since it started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TierCounters {
    pub ann_served: u64,
    pub ann_missed: u64,
    pub split_files_served: u64,
    /// Artifacts loaded.
    pub loads: u64,
    /// Artifact loads that failed.
    pub load_failures: u64,
}

/// What one reconcile pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Artifacts loaded.
    pub loaded: u32,
    /// Views published.
    pub views: u32,
    /// Columns (an artifact, its delta and its views) dropped because their
    /// collection is no longer owned, hot, or present.
    pub dropped: u32,
    /// Splits downloaded and pinned (Task 7).
    pub pinned_splits: u32,
    /// Lance bytes read through the range cache (Task 7).
    pub prefetched_bytes: u64,
    /// One line per failed collection, artifact load or split download.
    pub failures: Vec<String>,
}

#[derive(Default)]
struct Counters {
    ann_served: AtomicU64,
    ann_missed: AtomicU64,
    split_files_served: AtomicU64,
    loads: AtomicU64,
    load_failures: AtomicU64,
}

/// One column's loaded artifact, its delta index and its newest views
/// (oldest first).
#[derive(Clone, Debug)]
struct ColumnState {
    prefix: String,
    artifact: Arc<LoadedArtifact>,
    delta: Arc<DeltaIndex>,
    views: Vec<Arc<ColumnView>>,
}

/// The columns of one collection, by column name.
type Columns = BTreeMap<String, ColumnState>;

/// A collection with its namespace.
type Key = (NamespaceId, CollectionId);

/// The pinned splits of one collection's live manifest at the last pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TextSummary {
    pub(crate) manifest_version: u64,
    pub(crate) splits: u64,
    pub(crate) pinned: u64,
}

/// The collections a pass keeps each kind of structure for.
#[derive(Debug, Default)]
struct Wanted {
    vectors: HashSet<Key>,
    text: HashSet<Key>,
    fragments: HashSet<Key>,
}

impl Wanted {
    fn add(&mut self, key: Key, hot: HotConfig) {
        if hot.vectors {
            self.vectors.insert(key);
        }
        if hot.text {
            self.text.insert(key);
        }
        if hot.fragments {
            self.fragments.insert(key);
        }
    }
}

/// The fragment prefetch of one collection (rule 2).
#[derive(Clone, Debug, Default)]
pub(crate) struct FragmentState {
    pub(crate) lance_version: u64,
    pub(crate) progress: FragmentProgress,
    pub(crate) last: PrefetchPass,
}

struct TierInner {
    ctx: CollectionContext,
    config: HotTierConfig,
    node_id: u64,
    placement: Arc<dyn Placement>,
    engine: Arc<dyn HnswEngine>,
    /// Every loaded column of every owned, hot collection.
    state: RwLock<HashMap<Key, Columns>>,
    /// Pinned splits (Task 7 rule 1).
    splits: PinnedSplits,
    /// The last pass's pinned splits per collection with effective `text`.
    text: RwLock<HashMap<Key, TextSummary>>,
    /// Fragment prefetch per collection with effective `fragments`.
    fragments: Mutex<HashMap<Key, FragmentState>>,
    counters: Counters,
    /// Serializes reconcile passes.
    reconciling: tokio::sync::Mutex<()>,
    currency: CurrencyCache,
    deleted: DeletedDocsCache,
    cancel: CancellationToken,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for TierInner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// The hot tier of this node. Cheap to clone.
#[derive(Clone)]
pub struct HotTierImpl {
    inner: Arc<TierInner>,
}

impl std::fmt::Debug for HotTierImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns: usize = self.read_state().values().map(BTreeMap::len).sum();
        f.debug_struct("HotTierImpl")
            .field("node_id", &self.inner.node_id)
            .field("enabled", &self.inner.config.enabled)
            .field("dir", &self.inner.config.dir)
            .field("engine", &self.inner.engine.name())
            .field("columns", &columns)
            .finish_non_exhaustive()
    }
}

/// Removes `dir` and everything under it; a missing directory is fine.
async fn remove_all(dir: PathBuf) -> Result<(), TierError> {
    tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&dir) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    })
    .await
    .map_err(|err| TierError::Other(format!("clearing the hot directory: {err}")))??;
    Ok(())
}

impl HotTierImpl {
    /// Spawns the reconcile loop (rule 5) unless `config.enabled` is false (then every hook answers None).
    pub async fn start(
        ctx: CollectionContext,
        config: HotTierConfig,
        node_id: u64,
        placement: Arc<dyn Placement>,
        engine: Arc<dyn HnswEngine>,
    ) -> Result<Self, TierError> {
        let tier = Self::new(ctx, config, node_id, placement, engine).await?;
        if tier.inner.config.enabled {
            tier.spawn_loop();
        }
        Ok(tier)
    }

    /// [`start`](Self::start) without the loop: passes run only through
    /// [`reconcile_once`](Self::reconcile_once) (tests, and callers that
    /// drive the tier themselves; row 6.5). Removes `dir/hnsw/`,
    /// `dir/delta/` and `dir/splits/` first (rule 6): nothing local is
    /// trusted across restarts.
    pub async fn new(
        ctx: CollectionContext,
        config: HotTierConfig,
        node_id: u64,
        placement: Arc<dyn Placement>,
        engine: Arc<dyn HnswEngine>,
    ) -> Result<Self, TierError> {
        remove_all(config.dir.join(HNSW_DIR)).await?;
        remove_all(config.dir.join(DELTA_DIR)).await?;
        remove_all(config.dir.join(SPLITS_DIR)).await?;
        let splits = PinnedSplits::new(config.dir.join(SPLITS_DIR));
        Ok(Self {
            inner: Arc::new(TierInner {
                ctx,
                config,
                node_id,
                placement,
                engine,
                state: RwLock::new(HashMap::new()),
                splits,
                text: RwLock::new(HashMap::new()),
                fragments: Mutex::new(HashMap::new()),
                counters: Counters::default(),
                reconciling: tokio::sync::Mutex::new(()),
                currency: CurrencyCache::new(),
                deleted: DeletedDocsCache::new(),
                cancel: CancellationToken::new(),
                task: Mutex::new(None),
            }),
        })
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Key, Columns>> {
        self.inner
            .state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs [`reconcile_once`](Self::reconcile_once) every
    /// `reconcile_interval` and whenever the metastore changes, at most once
    /// per 100 ms, until [`shutdown`](Self::shutdown) or the last handle is
    /// dropped.
    fn spawn_loop(&self) {
        let weak: Weak<TierInner> = Arc::downgrade(&self.inner);
        let meta = self.inner.ctx.meta.clone();
        let cancel = self.inner.cancel.clone();
        let interval = self.inner.config.reconcile_interval;
        let handle = tokio::spawn(async move {
            let mut changes = Some(meta.watch_changes());
            loop {
                let started = Instant::now();
                {
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    let tier = HotTierImpl { inner };
                    if let Err(err) = tier.reconcile_once().await {
                        tracing::warn!(%err, "a hot tier reconcile pass failed");
                    }
                }
                let changed = async {
                    match &mut changes {
                        Some(watch) => {
                            if watch.changed().await.is_err() {
                                // The metastore stopped: poll on the interval only.
                                changes = None;
                            }
                        }
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(interval) => {}
                    () = changed => {}
                }
                let gap = MIN_RECONCILE_GAP.saturating_sub(started.elapsed());
                if !gap.is_zero() {
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(gap) => {}
                    }
                }
            }
        });
        *self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }

    /// Stops the loop and drops every view, artifact and delta the tier
    /// holds (a view a query still holds keeps its files until dropped).
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        let handle = self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(handle) = handle
            && let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::warn!(%err, "the hot tier reconcile loop panicked");
        }
        let _pass = self.inner.reconciling.lock().await;
        self.inner
            .state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.inner.splits.clear();
        self.inner
            .text
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.inner
            .fragments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// The pinned splits of this node.
    pub fn pinned_splits(&self) -> &PinnedSplits {
        &self.inner.splits
    }

    /// The published view of `column` of `(ns, cid)` at `manifest_version`,
    /// with its artifact and delta (for status and tests); unlike
    /// [`HotTier::ann`] it checks neither ownership nor counts.
    pub fn column_view(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<ColumnView>> {
        self.read_state()
            .get(&(ns, cid))
            .and_then(|columns| columns.get(column))
            .and_then(|state| {
                state
                    .views
                    .iter()
                    .find(|view| view.version() == manifest_version)
                    .cloned()
            })
    }

    pub fn counters(&self) -> TierCounters {
        let c = &self.inner.counters;
        TierCounters {
            ann_served: c.ann_served.load(Ordering::Relaxed),
            ann_missed: c.ann_missed.load(Ordering::Relaxed),
            split_files_served: c.split_files_served.load(Ordering::Relaxed),
            loads: c.loads.load(Ordering::Relaxed),
            load_failures: c.load_failures.load(Ordering::Relaxed),
        }
    }

    /// Rule 5 (and Task 7 rules 1–2): one pass over every collection. A
    /// failure of one collection, artifact or split is logged and reported,
    /// and retried on the next pass.
    pub async fn reconcile_once(&self) -> Result<ReconcileReport, TierError> {
        let inner = &*self.inner;
        let mut report = ReconcileReport::default();
        if !inner.config.enabled {
            return Ok(report);
        }
        let _pass = inner.reconciling.lock().await;
        let meta = &*inner.ctx.meta;
        let now = Instant::now();

        // 1. One `Local` read of every collection, its pointer and its
        // effective hot configuration.
        let heads = meta.collection_heads(Consistency::Local, None).await?;
        let leases: BTreeMap<String, Lease> = meta
            .leases_with_prefix(Consistency::Local, PROMOTE_LEASE_PREFIX)
            .await?
            .into_iter()
            .collect();
        let now_ms = meta.now_ms();
        let mut wanted = Wanted::default();
        let mut fragment_budget = inner.config.fragments_max_bytes;
        for head in heads {
            let collection = head.collection;
            let (ns, cid) = (collection.namespace, collection.id);
            if head.pointer.is_none() {
                continue;
            }
            if inner.placement.owner(ns, cid) != Owner::Local {
                continue;
            }
            let pinned = match meta.collection_hot(Consistency::Local, ns, cid).await {
                Ok(pinned) => pinned,
                // Dropped since the heads were read.
                Err(MetaError::Rejected(ApplyError::CollectionNotFound(_))) => continue,
                Err(err) => return Err(err.into()),
            };
            let lease = leases.get(&promote_lease_key(ns, cid));
            let mut hot = effective_hot(pinned, lease, inner.config.pin_all, now_ms);
            hot.vectors &= !collection.schema.vectors.is_empty();
            if !hot.any() {
                continue;
            }
            // Wanted before any work, so a failed pass keeps what it had.
            wanted.add((ns, cid), hot);

            // 2. Load, extend, pin and prefetch.
            let snapshot = match CollectionSnapshot::open(&inner.ctx, ns, cid, Consistency::Local)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    tracing::warn!(namespace = %ns, collection = %cid, %err, "opening a hot collection failed");
                    report.failures.push(format!("{ns}/{cid}: {err}"));
                    continue;
                }
            };
            if hot.vectors {
                match self
                    .reconcile_vectors(ns, cid, &snapshot, &mut report)
                    .await
                {
                    Ok(columns) => {
                        let mut state = inner.state.write().unwrap_or_else(PoisonError::into_inner);
                        let before = state.get(&(ns, cid)).map_or(0, BTreeMap::len);
                        report.dropped += before.saturating_sub(columns.len()) as u32;
                        state.insert((ns, cid), columns);
                    }
                    Err(err) => {
                        tracing::warn!(namespace = %ns, collection = %cid, %err, "reconciling hot vectors failed");
                        report.failures.push(format!("{ns}/{cid} vectors: {err}"));
                    }
                }
            }
            if hot.text {
                self.reconcile_text(ns, cid, snapshot.manifest(), now, &mut report)
                    .await;
            }
            if hot.fragments
                && let Err(err) = self
                    .reconcile_fragments(ns, cid, &snapshot, &mut fragment_budget, &mut report)
                    .await
            {
                tracing::warn!(namespace = %ns, collection = %cid, %err, "prefetching fragments failed");
                report.failures.push(format!("{ns}/{cid} fragments: {err}"));
            }
        }

        // 3. Structures of collections no longer owned, hot or present.
        {
            let mut state = inner.state.write().unwrap_or_else(PoisonError::into_inner);
            state.retain(|key, columns| {
                let keep = wanted.vectors.contains(key);
                if !keep {
                    report.dropped += columns.len() as u32;
                }
                keep
            });
        }
        inner.splits.retain_serving(&wanted.text);
        inner
            .text
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|key, _| wanted.text.contains(key));
        inner
            .fragments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|key, _| wanted.fragments.contains(key));
        delete_files(inner.splits.expire(inner.config.split_linger, now)).await;
        Ok(report)
    }

    /// Task 7 rule 1 for one collection with effective `text`: serve its
    /// splits, mark the live ones referenced, and download the missing ones.
    async fn reconcile_text(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        manifest: &CollectionManifest,
        now: Instant,
        report: &mut ReconcileReport,
    ) {
        let inner = &*self.inner;
        let splits = &inner.splits;
        splits.set_serving(ns, cid, true);
        splits.touch(ns, cid, manifest.splits.iter().map(|s| s.ulid), now);
        // Owned jobs: a stream over borrowing closures makes the pass's
        // future `Send` for one lifetime only (row 6.8).
        let missing: Vec<(Ulid, u64, u64, PathBuf)> = manifest
            .splits
            .iter()
            .filter(|split| !splits.contains(&(ns, cid, split.ulid)))
            .map(|split: &SplitRef| {
                (
                    split.ulid,
                    split.size_bytes,
                    split.footer_range.start,
                    splits.local_path(ns, cid, split.ulid),
                )
            })
            .collect();
        let store = inner.ctx.store.clone();
        let downloads: Vec<_> = futures::stream::iter(missing)
            .map(move |(ulid, size, footer_start, to)| {
                let store = store.clone();
                async move {
                    let path = split_path(ns, cid, ulid);
                    let result = download_split(&store, &path, size, footer_start, &to).await;
                    (ulid, size, to, result)
                }
            })
            .buffer_unordered(inner.config.download_parallelism.max(1))
            .collect()
            .await;
        for (ulid, size, to, result) in downloads {
            match result {
                Ok(()) => {
                    splits.insert((ns, cid, ulid), to, size, now);
                    report.pinned_splits += 1;
                }
                Err(err) => {
                    tracing::warn!(namespace = %ns, collection = %cid, split = %ulid, %err, "pinning a split failed");
                    report
                        .failures
                        .push(format!("{ns}/{cid} split {ulid}: {err}"));
                }
            }
        }
        let pinned = manifest
            .splits
            .iter()
            .filter(|split| splits.contains(&(ns, cid, split.ulid)))
            .count() as u64;
        inner
            .text
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                (ns, cid),
                TextSummary {
                    manifest_version: manifest.version,
                    splits: manifest.splits.len() as u64,
                    pinned,
                },
            );
    }

    /// Task 7 rule 2 for one collection with effective `fragments`: read
    /// its Lance version's files through the range cache, within what is
    /// left of the pass's `fragments_max_bytes`.
    async fn reconcile_fragments(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        snapshot: &CollectionSnapshot,
        budget: &mut u64,
        report: &mut ReconcileReport,
    ) -> Result<(), TierError> {
        let inner = &*self.inner;
        let lance_version = snapshot.manifest().lance_version;
        let mut state = inner
            .fragments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(ns, cid))
            .unwrap_or_default();
        let result = async {
            if state.lance_version == lance_version && state.last.complete() && lance_version > 0 {
                return Ok(());
            }
            let Some(dataset) = snapshot.dataset() else {
                // No Lance data yet: nothing to read.
                state.lance_version = lance_version;
                state.last = PrefetchPass::default();
                return Ok(());
            };
            let pass = prefetch_fragments_resuming(
                &inner.ctx.cache,
                &inner.ctx.store,
                &lance_prefix(ns, cid),
                dataset,
                &mut state.progress,
                *budget,
            )
            .await?;
            *budget = budget.saturating_sub(pass.read);
            report.prefetched_bytes += pass.read;
            state.lance_version = lance_version;
            state.last = pass;
            Ok(())
        }
        .await;
        inner
            .fragments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((ns, cid), state);
        result
    }

    /// Rule 5.2 for the vectors of one owned, hot collection: its new
    /// column states.
    async fn reconcile_vectors(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        snapshot: &CollectionSnapshot,
        report: &mut ReconcileReport,
    ) -> Result<Columns, TierError> {
        let inner = &*self.inner;
        let old = self
            .read_state()
            .get(&(ns, cid))
            .cloned()
            .unwrap_or_default();
        let Some(path) = snapshot.manifest_path() else {
            return Ok(Columns::new());
        };
        let manifest = snapshot.manifest();
        if !manifest.hot_artifacts.iter().any(|a| a.kind == HNSW_KIND) {
            return Ok(Columns::new());
        }
        let live = live_rows_cached(snapshot, &inner.deleted).await?;
        let mut columns = Columns::new();
        for index in 0..snapshot.collection().schema.vectors.len() {
            let column = vector_column(index);
            let Some(reference) = manifest
                .hot_artifacts
                .iter()
                .find(|a| a.kind == HNSW_KIND && a.column == column)
            else {
                continue;
            };
            let mut state = old.get(&column).cloned();
            if state.as_ref().is_none_or(|s| s.prefix != reference.prefix) {
                // Task 7's budget admits every artifact until it lands.
                match self.load_column(ns, cid, &column, reference).await {
                    Ok(loaded) => {
                        inner.counters.loads.fetch_add(1, Ordering::Relaxed);
                        report.loaded += 1;
                        state = Some(loaded);
                    }
                    Err(err) => {
                        inner.counters.load_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            namespace = %ns,
                            collection = %cid,
                            %column,
                            prefix = %reference.prefix,
                            %err,
                            "loading a hot artifact failed"
                        );
                        report
                            .failures
                            .push(format!("{ns}/{cid}/{column} {}: {err}", reference.prefix));
                    }
                }
            }
            // A failed replacement keeps serving the previous artifact:
            // an artifact is valid for every later manifest (Ruling 1).
            let Some(mut state) = state else {
                continue;
            };
            if !state.views.iter().any(|v| v.version() == manifest.version) {
                state
                    .delta
                    .extend(snapshot, &live, &state.artifact, &inner.config)
                    .await?;
                let artifact_source = state.artifact.descriptor.source_version;
                let source_version = self
                    .effective_source(path, manifest, artifact_source)
                    .await?;
                let view = ColumnView::new(
                    manifest.version,
                    source_version,
                    state.artifact.clone(),
                    state.delta.clone(),
                    &live,
                );
                let excluded = view.excluded().len();
                if excluded > inner.config.max_view_exclusions {
                    tracing::info!(
                        namespace = %ns,
                        collection = %cid,
                        %column,
                        version = manifest.version,
                        excluded,
                        "a hot view excludes too many rows; not served until the artifact is rebuilt"
                    );
                } else {
                    state.views.push(Arc::new(view));
                    report.views += 1;
                    let keep = inner.config.views_per_column.max(1);
                    if state.views.len() > keep {
                        let extra = state.views.len() - keep;
                        state.views.drain(..extra);
                    }
                }
            }
            columns.insert(column, state);
        }
        Ok(columns)
    }

    /// The effective source version (Ruling 1) of an artifact built from
    /// `artifact_source` at the manifest `manifest` (at `path`).
    async fn effective_source(
        &self,
        path: &str,
        manifest: &CollectionManifest,
        artifact_source: u64,
    ) -> Result<u64, TierError> {
        let ctx = &self.inner.ctx;
        let currency = self
            .inner
            .currency
            .currency(
                &ctx.store,
                &ctx.manifests,
                (path, manifest),
                artifact_source,
            )
            .await?;
        Ok(effective_source_version(
            currency,
            manifest.version,
            artifact_source,
        ))
    }

    /// `dir/<kind>/<ns>/<cid>/<column>`.
    fn local_dir(&self, kind: &str, ns: NamespaceId, cid: CollectionId, column: &str) -> PathBuf {
        self.inner
            .config
            .dir
            .join(kind)
            .join(ns.to_string())
            .join(cid.to_string())
            .join(column)
    }

    /// Downloads and opens `reference`, and creates its fresh delta index.
    async fn load_column(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        reference: &HotArtifactRef,
    ) -> Result<ColumnState, TierError> {
        let inner = &*self.inner;
        let ulid = Ulid::generate();
        let dir = self
            .local_dir(HNSW_DIR, ns, cid, column)
            .join(format!("{:020}-{ulid}", reference.source_version));
        let delta_dir = self
            .local_dir(DELTA_DIR, ns, cid, column)
            .join(ulid.to_string());
        let artifact = LoadedArtifact::load(
            &inner.ctx.store,
            &reference.prefix,
            dir,
            inner.config.download_parallelism,
            &inner.engine,
        )
        .await?;
        let descriptor = &artifact.descriptor;
        if descriptor.collection != cid.0
            || descriptor.column != column
            || descriptor.source_version != reference.source_version
        {
            return Err(TierError::Corrupt(format!(
                "{}: the descriptor is of collection {} column {} version {}",
                reference.prefix,
                descriptor.collection,
                descriptor.column,
                descriptor.source_version
            )));
        }
        let artifact = Arc::new(artifact);
        let delta = DeltaIndex::create(&artifact, delta_dir).await?;
        Ok(ColumnState {
            prefix: reference.prefix.clone(),
            artifact,
            delta,
            views: Vec::new(),
        })
    }
}

impl HotTier for HotTierImpl {
    fn ann(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<dyn HotAnn>> {
        let counters = &self.inner.counters;
        let found = match self.inner.config.enabled
            && self.inner.placement.owner(ns, cid) == Owner::Local
        {
            false => None,
            true => self.column_view(ns, cid, column, manifest_version),
        };
        match found {
            Some(view) => {
                counters.ann_served.fetch_add(1, Ordering::Relaxed);
                Some(view as Arc<dyn HotAnn>)
            }
            None => {
                counters.ann_missed.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Task 7 rule 1: the pinned file of `split` while the collection is
    /// owned here with effective `text` (a map lookup, no I/O).
    fn split_file(&self, ns: NamespaceId, cid: CollectionId, split: ulid::Ulid) -> Option<PathBuf> {
        if !self.inner.config.enabled || self.inner.placement.owner(ns, cid) != Owner::Local {
            return None;
        }
        let path = self.inner.splits.path(ns, cid, split)?;
        self.inner
            .counters
            .split_files_served
            .fetch_add(1, Ordering::Relaxed);
        Some(path)
    }

    /// Task 7.
    fn record_access(&self, _: NamespaceId, _: CollectionId) {}
}
