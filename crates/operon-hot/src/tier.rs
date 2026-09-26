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
use crate::artifact::{
    CurrencyCache, DESCRIPTOR_FILE, HNSW_KIND, decode_descriptor, effective_source_version,
};
use crate::budget::{Budget, HotClass, Resident, StructureKind, plan_admission, plan_shrink};
use crate::build::{PROMOTE_LEASE_PREFIX, effective_hot, promote_lease_key};
use crate::config::HotTierConfig;
use crate::delta::DeltaIndex;
use crate::heat::HeatSketch;
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
    /// Structures evicted to bring the node back under its budget (Task 7).
    pub evicted: u32,
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
    /// The delta directory's disk usage after its last extension.
    delta_nvme: u64,
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

/// Every structure: what a promotion or `warm` makes hot.
const ALL: HotConfig = HotConfig {
    vectors: true,
    text: true,
    fragments: true,
};

/// What the last pass decided for one owned, hot collection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HotInfo {
    /// The catalog configuration.
    pub(crate) catalog: HotConfig,
    /// The effective configuration (rule 4 included).
    pub(crate) effective: HotConfig,
    /// The pinned part: the catalog's, OR `pin_all`'s (class `Pinned`).
    pub(crate) pinned: HotConfig,
    /// A promotion lease is held (by this node or, as read, another).
    pub(crate) promoted: bool,
}

/// Which structures of a collection an admission refused in the last pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OverBudget {
    pub(crate) vectors: bool,
    pub(crate) text: bool,
}

/// A promotion lease this node holds.
#[derive(Clone, Copy, Debug)]
struct Promotion {
    epoch: u64,
    renewed: Instant,
}

/// The collections a pass keeps each kind of structure for.
#[derive(Debug, Default)]
struct Wanted {
    vectors: HashSet<Key>,
    text: HashSet<Key>,
    fragments: HashSet<Key>,
}

impl Wanted {
    fn of(&self, key: Key) -> HotConfig {
        HotConfig {
            vectors: self.vectors.contains(&key),
            text: self.text.contains(&key),
            fragments: self.fragments.contains(&key),
        }
    }

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
    /// Heat (rule 4) and when it was last halved.
    heat: HeatSketch,
    last_decay: Mutex<Instant>,
    /// The budget enforced (rule 3); starts from the configuration.
    budget: Mutex<Budget>,
    /// Collections warmed on this node (rule 5).
    warm: Mutex<HashSet<Key>>,
    /// Promotion leases this node holds (rule 4).
    promotions: Mutex<HashMap<Key, Promotion>>,
    /// The last pass's decisions per owned, hot collection.
    info: RwLock<HashMap<Key, HotInfo>>,
    /// The last pass's refused admissions.
    over_budget: RwLock<HashMap<Key, OverBudget>>,
    /// The last load error of each column, until it loads.
    load_errors: RwLock<HashMap<(Key, String), String>>,
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

/// The disk usage of the files under `dir` (allocated blocks on Unix, so
/// sparse files count what they use); 0 when it cannot be read.
async fn disk_usage(dir: PathBuf) -> u64 {
    fn walk(dir: &std::path::Path) -> std::io::Result<u64> {
        let mut total = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                total += walk(&entry.path())?;
            } else {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    total += meta.blocks() * 512;
                }
                #[cfg(not(unix))]
                {
                    total += meta.len();
                }
            }
        }
        Ok(total)
    }
    tokio::task::spawn_blocking(move || walk(&dir).unwrap_or(0))
        .await
        .unwrap_or(0)
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
        let budget = Budget {
            nvme_bytes: config.nvme_bytes,
            ram_bytes: config.ram_bytes,
            max_artifacts: config.max_loaded_artifacts,
        };
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
                heat: HeatSketch::new(),
                last_decay: Mutex::new(Instant::now()),
                budget: Mutex::new(budget),
                warm: Mutex::new(HashSet::new()),
                promotions: Mutex::new(HashMap::new()),
                info: RwLock::new(HashMap::new()),
                over_budget: RwLock::new(HashMap::new()),
                load_errors: RwLock::new(HashMap::new()),
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

    /// Rule 5 (and Task 7 rules 1–4): one pass over every collection. A
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
        self.decay_heat(now);

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
        let mut infos = HashMap::new();
        let mut fragment_budget = inner.config.fragments_max_bytes;
        let mut owned = HashSet::new();
        for head in heads {
            let collection = head.collection;
            let (ns, cid) = (collection.namespace, collection.id);
            if head.pointer.is_none() {
                continue;
            }
            if inner.placement.owner(ns, cid) != Owner::Local {
                continue;
            }
            let catalog = match meta.collection_hot(Consistency::Local, ns, cid).await {
                Ok(catalog) => catalog,
                // Dropped since the heads were read.
                Err(MetaError::Rejected(ApplyError::CollectionNotFound(_))) => continue,
                Err(err) => return Err(err.into()),
            };
            owned.insert((ns, cid));
            let pinned = effective_hot(catalog, None, inner.config.pin_all, now_ms);
            let heat = inner.heat.estimate(ns, cid);
            let warm = {
                let mut warm = inner.warm.lock().unwrap_or_else(PoisonError::into_inner);
                if warm.contains(&(ns, cid)) && heat < inner.config.demote_below_hits {
                    warm.remove(&(ns, cid));
                }
                warm.contains(&(ns, cid))
            };
            let ours = self.promote(ns, cid, heat, pinned.any()).await;
            let lease_key = promote_lease_key(ns, cid);
            // The lease as read at the start of the pass, unless it is ours:
            // then what `promote` just did decides.
            let lease = leases
                .get(&lease_key)
                .filter(|lease| lease.owner.as_deref() != Some(self.promote_owner().as_str()));
            let mut hot = effective_hot(catalog, lease, inner.config.pin_all, now_ms);
            let promoted = ours || lease.is_some_and(|l| l.is_held_at(now_ms));
            if ours || warm {
                hot = hot.or(ALL);
            }
            let mut serve = hot;
            serve.vectors &= !collection.schema.vectors.is_empty();
            if !hot.any() {
                continue;
            }
            infos.insert(
                (ns, cid),
                HotInfo {
                    catalog,
                    effective: hot,
                    pinned,
                    promoted,
                },
            );
            // Wanted before any work, so a failed pass keeps what it had.
            wanted.add((ns, cid), serve);
        }
        *inner.info.write().unwrap_or_else(PoisonError::into_inner) = infos.clone();

        // Collections no longer promoted by this node release their lease
        // once they are not owned here any more.
        self.release_unowned_promotions(&owned).await;

        // 2. Load, extend, pin and prefetch.
        let mut over = HashMap::new();
        let mut keys: Vec<Key> = infos.keys().copied().collect();
        keys.sort_unstable();
        for (ns, cid) in keys {
            let serve = wanted.of((ns, cid));
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
            let mut flags = OverBudget::default();
            if serve.vectors
                && let Err(err) = self
                    .reconcile_vectors(ns, cid, &snapshot, &mut flags, &mut report)
                    .await
            {
                tracing::warn!(namespace = %ns, collection = %cid, %err, "reconciling hot vectors failed");
                report.failures.push(format!("{ns}/{cid} vectors: {err}"));
            }
            if serve.text {
                self.reconcile_text(ns, cid, snapshot.manifest(), now, &mut flags, &mut report)
                    .await;
            }
            if serve.fragments
                && let Err(err) = self
                    .reconcile_fragments(ns, cid, &snapshot, &mut fragment_budget, &mut report)
                    .await
            {
                tracing::warn!(namespace = %ns, collection = %cid, %err, "prefetching fragments failed");
                report.failures.push(format!("{ns}/{cid} fragments: {err}"));
            }
            over.insert((ns, cid), flags);
        }
        *inner
            .over_budget
            .write()
            .unwrap_or_else(PoisonError::into_inner) = over;

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

        // 4. A budget that shrank, or deltas that grew (rule 3).
        let resident = self.resident();
        let evict = plan_shrink(&resident, &self.budget());
        report.evicted += evict.len() as u32;
        self.evict(evict.into_iter().map(|i| &resident[i]), now);
        delete_files(inner.splits.expire(inner.config.split_linger, now)).await;
        Ok(report)
    }

    /// Halves the heat sketch once per `heat_window` that has passed.
    fn decay_heat(&self, now: Instant) {
        let window = self.inner.config.heat_window;
        if window.is_zero() {
            return;
        }
        let mut last = self
            .inner
            .last_decay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let windows = now.saturating_duration_since(*last).as_nanos() / window.as_nanos();
        if windows == 0 {
            return;
        }
        // Eight halvings empty every u8 counter.
        for _ in 0..windows.min(8) {
            self.inner.heat.decay();
        }
        *last = match u32::try_from(windows) {
            Ok(n) if windows <= 8 => *last + window * n,
            _ => now,
        };
    }

    /// The owner string of this node's promotion leases (Ruling 7).
    fn promote_owner(&self) -> String {
        format!("{};vectors,text,fragments", self.inner.node_id)
    }

    /// Rule 4 for one owned collection with heat `heat`: with
    /// `auto_promote`, takes the promotion lease at `promote_min_hits`,
    /// renews it once half its TTL has passed, and releases it under
    /// `demote_below_hits`; a pinned collection, or a tier without
    /// `auto_promote`, releases any it holds. Returns whether this node
    /// holds it after the call.
    async fn promote(&self, ns: NamespaceId, cid: CollectionId, heat: u32, pinned: bool) -> bool {
        let inner = &*self.inner;
        let config = &inner.config;
        let meta = &*inner.ctx.meta;
        let key = promote_lease_key(ns, cid);
        let owner = self.promote_owner();
        let current = inner
            .promotions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(ns, cid))
            .copied();
        let ttl = config.promote_lease_ttl;
        let keep = config.auto_promote
            && !pinned
            && (heat >= config.promote_min_hits
                || (current.is_some() && heat >= config.demote_below_hits));
        let next = match (keep, current) {
            (false, None) => None,
            (false, Some(held)) => {
                if let Err(err) = meta.release_lease(&key, &owner, held.epoch).await {
                    tracing::warn!(%key, %err, "releasing a promotion lease");
                }
                None
            }
            (true, None) => match meta.acquire_lease(&key, &owner, ttl).await {
                Ok(grant) => Some(Promotion {
                    epoch: grant.epoch,
                    renewed: Instant::now(),
                }),
                Err(MetaError::Rejected(ApplyError::LeaseHeld { .. })) => None,
                Err(err) => {
                    tracing::warn!(%key, %err, "taking a promotion lease");
                    None
                }
            },
            (true, Some(held)) if held.renewed.elapsed() >= ttl / 2 => {
                match meta.renew_lease(&key, &owner, held.epoch, ttl).await {
                    Ok(grant) => Some(Promotion {
                        epoch: grant.epoch,
                        renewed: Instant::now(),
                    }),
                    Err(MetaError::Rejected(_)) => None,
                    Err(err) => {
                        // Retried on the next pass; the lease may still hold.
                        tracing::warn!(%key, %err, "renewing a promotion lease");
                        Some(held)
                    }
                }
            }
            (true, Some(held)) => Some(held),
        };
        let mut promotions = inner
            .promotions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match next {
            Some(held) => promotions.insert((ns, cid), held),
            None => promotions.remove(&(ns, cid)),
        };
        next.is_some()
    }

    /// Releases the promotion leases of collections this node no longer
    /// owns (or that were dropped).
    async fn release_unowned_promotions(&self, owned: &HashSet<Key>) {
        let gone: Vec<(Key, Promotion)> = {
            let mut promotions = self
                .inner
                .promotions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let gone = promotions
                .iter()
                .filter(|(key, _)| !owned.contains(key))
                .map(|(key, held)| (*key, *held))
                .collect();
            promotions.retain(|key, _| owned.contains(key));
            gone
        };
        let owner = self.promote_owner();
        for ((ns, cid), held) in gone {
            let key = promote_lease_key(ns, cid);
            if let Err(err) = self
                .inner
                .ctx
                .meta
                .release_lease(&key, &owner, held.epoch)
                .await
            {
                tracing::warn!(%key, %err, "releasing a promotion lease");
            }
        }
    }

    /// The budget the tier enforces now.
    pub fn budget(&self) -> Budget {
        *self
            .inner
            .budget
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Changes the budget; the next pass evicts what no longer fits
    /// ([`plan_shrink`]).
    pub fn set_budget(&self, budget: Budget) {
        *self
            .inner
            .budget
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = budget;
    }

    /// Temporarily promotes a collection on this node (the warm API, §04 §5): heat is set to `promote_min_hits`
    /// and one reconcile pass runs for it; it is demoted like any promoted collection when its heat decays.
    pub async fn warm(&self, ns: NamespaceId, cid: CollectionId) -> Result<(), TierError> {
        let inner = &*self.inner;
        if !inner.config.enabled {
            return Err(TierError::Other(format!(
                "the hot tier is off on node {}",
                inner.node_id
            )));
        }
        inner.heat.raise_to(ns, cid, inner.config.promote_min_hits);
        inner
            .warm
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((ns, cid));
        self.reconcile_once().await?;
        Ok(())
    }

    /// The heat of `(ns, cid)` (its sketch estimate).
    pub fn heat(&self, ns: NamespaceId, cid: CollectionId) -> u32 {
        self.inner.heat.estimate(ns, cid)
    }

    /// Every budgeted structure this node holds (rule 3): loaded artifacts,
    /// their delta indexes and pinned splits, with their class and heat.
    pub fn resident(&self) -> Vec<Resident> {
        let inner = &*self.inner;
        let info = inner.info.read().unwrap_or_else(PoisonError::into_inner);
        let class_of = |key: &Key, pinned: fn(&HotConfig) -> bool| -> (HotClass, u32) {
            let heat = inner.heat.estimate(key.0, key.1);
            match info.get(key) {
                Some(info) if pinned(&info.pinned) => {
                    (HotClass::Pinned, heat.max(inner.config.promote_min_hits))
                }
                Some(_) => (HotClass::Promoted, heat),
                // Lingering after its collection stopped being hot.
                None => (HotClass::Promoted, 0),
            }
        };
        let mut out = Vec::new();
        for (key, columns) in self.read_state().iter() {
            let (class, heat) = class_of(key, |p| p.vectors);
            for (column, state) in columns {
                // An artifact is sized as its descriptor sizes it, exactly as
                // its admission did, so a loaded artifact and a candidate of
                // the same size tie and never displace each other; what
                // grows (the delta, and the views' row sets) is the delta's.
                let descriptor = &state.artifact.descriptor;
                out.push(Resident {
                    namespace: key.0,
                    collection: key.1,
                    kind: StructureKind::Hnsw,
                    id: column.clone(),
                    nvme_bytes: state.artifact.bytes,
                    ram_bytes: descriptor.covered_len,
                    class,
                    heat,
                });
                let views: u64 = state.views.iter().map(|view| view.ram_bytes()).sum();
                out.push(Resident {
                    namespace: key.0,
                    collection: key.1,
                    kind: StructureKind::Delta,
                    id: column.clone(),
                    nvme_bytes: state.delta_nvme,
                    ram_bytes: state.delta.scanned().serialized_size() as u64 + views,
                    class,
                    heat,
                });
            }
        }
        for (key, splits) in inner.splits.by_collection() {
            let (class, heat) = class_of(&key, |p| p.text);
            for (ulid, size) in splits {
                out.push(Resident {
                    namespace: key.0,
                    collection: key.1,
                    kind: StructureKind::Split,
                    id: ulid.to_string(),
                    nvme_bytes: size,
                    ram_bytes: 0,
                    class,
                    heat,
                });
            }
        }
        out.sort_by(|a, b| {
            (a.namespace, a.collection, a.kind, &a.id).cmp(&(
                b.namespace,
                b.collection,
                b.kind,
                &b.id,
            ))
        });
        out
    }

    /// Drops the tier's reference to each of `victims` (rule 3): an artifact
    /// or delta takes its column (artifact, delta and views) with it; a
    /// split leaves the map now and its file goes after `split_linger`. A
    /// query that holds a view or a split path keeps using it.
    fn evict<'a>(&self, victims: impl IntoIterator<Item = &'a Resident>, now: Instant) {
        let inner = &*self.inner;
        for victim in victims {
            let key = (victim.namespace, victim.collection);
            tracing::info!(
                namespace = %victim.namespace,
                collection = %victim.collection,
                kind = ?victim.kind,
                id = %victim.id,
                "evicting a hot structure"
            );
            match victim.kind {
                StructureKind::Hnsw | StructureKind::Delta => {
                    let mut state = inner.state.write().unwrap_or_else(PoisonError::into_inner);
                    if let Some(columns) = state.get_mut(&key) {
                        columns.remove(&victim.id);
                    }
                }
                StructureKind::Split => {
                    if let Ok(ulid) = victim.id.parse::<Ulid>() {
                        inner.splits.evict(&(key.0, key.1, ulid), now);
                    }
                }
            }
        }
    }

    /// Admits `candidate` against everything resident except `exclude`
    /// (rule 3) and applies the evictions; false (and nothing evicted) when
    /// it cannot fit.
    fn admit(
        &self,
        candidate: &Resident,
        exclude: impl Fn(&Resident) -> bool,
        pending: &[Resident],
        now: Instant,
    ) -> Result<(), TierError> {
        let mut resident: Vec<Resident> = self
            .resident()
            .into_iter()
            .filter(|r| !exclude(r))
            .collect();
        let base = resident.len();
        resident.extend(pending.iter().cloned());
        let evict = plan_admission(&resident, candidate, &self.budget())?;
        // A structure admitted earlier in this pass (`pending`) is never
        // evicted by a later one; callers admit in an order that makes this
        // hold (splits by size ascending), and a plan that needs it anyway
        // is refused.
        if evict.iter().any(|&i| i >= base) {
            return Err(TierError::OverBudget(format!(
                "{} {}/{} {} would evict a structure admitted in the same pass",
                candidate.id, candidate.namespace, candidate.collection, candidate.id
            )));
        }
        self.evict(evict.iter().map(|&i| &resident[i]), now);
        Ok(())
    }

    /// The class and heat of a structure of `(ns, cid)` that `pinned`
    /// decides.
    fn class_and_heat(&self, ns: NamespaceId, cid: CollectionId, pinned: bool) -> (HotClass, u32) {
        let heat = self.inner.heat.estimate(ns, cid);
        match pinned {
            true => (
                HotClass::Pinned,
                heat.max(self.inner.config.promote_min_hits),
            ),
            false => (HotClass::Promoted, heat),
        }
    }

    fn info(&self, ns: NamespaceId, cid: CollectionId) -> Option<HotInfo> {
        self.inner
            .info
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(ns, cid))
            .copied()
    }

    /// Task 7 rule 1 for one collection with effective `text`: serve its
    /// splits, mark the live ones referenced, and download the missing ones
    /// the budget admits.
    async fn reconcile_text(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        manifest: &CollectionManifest,
        now: Instant,
        over: &mut OverBudget,
        report: &mut ReconcileReport,
    ) {
        let inner = &*self.inner;
        let splits = &inner.splits;
        splits.set_serving(ns, cid, true);
        splits.touch(ns, cid, manifest.splits.iter().map(|s| s.ulid), now);
        let pinned = self.info(ns, cid).is_some_and(|info| info.pinned.text);
        let (class, heat) = self.class_and_heat(ns, cid, pinned);
        // Admitted one by one, smallest first (so a later split never
        // outranks an earlier one of the pass by heat per byte); the
        // downloads then run in parallel.
        let mut wanted: Vec<&SplitRef> = manifest
            .splits
            .iter()
            .filter(|split| !splits.contains(&(ns, cid, split.ulid)))
            .collect();
        wanted.sort_by_key(|split| (split.size_bytes, split.ulid));
        let mut admitted: Vec<Resident> = Vec::new();
        let mut missing: Vec<(Ulid, u64, u64, PathBuf)> = Vec::new();
        for split in wanted {
            let candidate = Resident {
                namespace: ns,
                collection: cid,
                kind: StructureKind::Split,
                id: split.ulid.to_string(),
                nvme_bytes: split.size_bytes,
                ram_bytes: 0,
                class,
                heat,
            };
            match self.admit(&candidate, |_| false, &admitted, now) {
                Ok(()) => {
                    admitted.push(candidate);
                    missing.push((
                        split.ulid,
                        split.size_bytes,
                        split.footer_range.start,
                        splits.local_path(ns, cid, split.ulid),
                    ));
                }
                Err(err) => {
                    over.text = true;
                    tracing::debug!(namespace = %ns, collection = %cid, split = %split.ulid, %err, "a split is over budget");
                }
            }
        }
        // Owned jobs: a stream over borrowing closures makes the pass's
        // future `Send` for one lifetime only (row 6.8).
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

    /// Rule 5.2 for the vectors of one owned, hot collection: loads the
    /// artifacts the budget admits, extends their deltas and publishes their
    /// views. Each column's state is written back as it is done, so an
    /// eviction by a later column or collection of the pass sticks.
    async fn reconcile_vectors(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        snapshot: &CollectionSnapshot,
        over: &mut OverBudget,
        report: &mut ReconcileReport,
    ) -> Result<(), TierError> {
        let inner = &*self.inner;
        let key = (ns, cid);
        let manifest = snapshot.manifest();
        let columns: Vec<(String, HotArtifactRef)> =
            (0..snapshot.collection().schema.vectors.len())
                .filter_map(|index| {
                    let column = vector_column(index);
                    manifest
                        .hot_artifacts
                        .iter()
                        .find(|a| a.kind == HNSW_KIND && a.column == column)
                        .map(|reference| (column, reference.clone()))
                })
                .collect();
        // Columns whose artifact left the manifest (or the schema).
        {
            let mut state = inner.state.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(loaded) = state.get_mut(&key) {
                let before = loaded.len();
                loaded.retain(|column, _| columns.iter().any(|(c, _)| c == column));
                report.dropped += (before - loaded.len()) as u32;
            }
        }
        let Some(path) = snapshot.manifest_path() else {
            return Ok(());
        };
        if columns.is_empty() {
            return Ok(());
        }
        let live = live_rows_cached(snapshot, &inner.deleted).await?;
        let pinned = self.info(ns, cid).is_some_and(|info| info.pinned.vectors);
        let now = Instant::now();
        for (column, reference) in columns {
            let mut state = self
                .read_state()
                .get(&key)
                .and_then(|loaded| loaded.get(&column))
                .cloned();
            if state.as_ref().is_none_or(|s| s.prefix != reference.prefix) {
                match self
                    .admit_artifact(ns, cid, &column, &reference, pinned, now)
                    .await
                {
                    Ok(true) => match self.load_column(ns, cid, &column, &reference).await {
                        Ok(loaded) => {
                            inner.counters.loads.fetch_add(1, Ordering::Relaxed);
                            report.loaded += 1;
                            self.set_load_error(key, &column, None);
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
                            self.set_load_error(key, &column, Some(err.to_string()));
                        }
                    },
                    Ok(false) => {
                        over.vectors = true;
                    }
                    Err(err) => {
                        tracing::warn!(namespace = %ns, collection = %cid, %column, %err, "sizing a hot artifact failed");
                        report
                            .failures
                            .push(format!("{ns}/{cid}/{column} {}: {err}", reference.prefix));
                        self.set_load_error(key, &column, Some(err.to_string()));
                    }
                }
            }
            // A failed or refused replacement keeps serving the previous
            // artifact: an artifact is valid for every later manifest
            // (Ruling 1).
            let Some(mut state) = state else {
                continue;
            };
            if !state.views.iter().any(|v| v.version() == manifest.version) {
                state
                    .delta
                    .extend(snapshot, &live, &state.artifact, &inner.config)
                    .await?;
                state.delta_nvme = disk_usage(state.delta.dir().to_path_buf()).await;
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
            inner
                .state
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(key)
                .or_default()
                .insert(column, state);
        }
        Ok(())
    }

    /// Rule 3 for a new or replacing artifact of `column`: its descriptor
    /// sizes it (NVMe: its files; RAM: its covered set), and the column's
    /// current artifact and delta, which it replaces, do not count. Ok(false)
    /// when it does not fit.
    async fn admit_artifact(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        reference: &HotArtifactRef,
        pinned: bool,
        now: Instant,
    ) -> Result<bool, TierError> {
        let (bytes, _) = self
            .inner
            .ctx
            .store
            .get(&format!("{}{DESCRIPTOR_FILE}", reference.prefix))
            .await?;
        let descriptor = decode_descriptor(&bytes)?;
        let (class, heat) = self.class_and_heat(ns, cid, pinned);
        let candidate = Resident {
            namespace: ns,
            collection: cid,
            kind: StructureKind::Hnsw,
            id: column.to_string(),
            nvme_bytes: descriptor.files.iter().map(|f| f.size).sum(),
            ram_bytes: descriptor.covered_len,
            class,
            heat,
        };
        let replaced = |r: &Resident| {
            r.namespace == ns
                && r.collection == cid
                && r.id == column
                && r.kind != StructureKind::Split
        };
        match self.admit(&candidate, replaced, &[], now) {
            Ok(()) => Ok(true),
            Err(TierError::OverBudget(message)) => {
                tracing::debug!(namespace = %ns, collection = %cid, %column, %message, "a hot artifact is over budget");
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn set_load_error(&self, key: Key, column: &str, error: Option<String>) {
        let mut errors = self
            .inner
            .load_errors
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        match error {
            Some(error) => errors.insert((key, column.to_string()), error),
            None => errors.remove(&(key, column.to_string())),
        };
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
            delta_nvme: 0,
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

    /// Task 7 rule 4: one hit in the heat sketch.
    fn record_access(&self, ns: NamespaceId, cid: CollectionId) {
        if self.inner.config.enabled {
            self.inner.heat.record(ns, cid);
        }
    }
}
