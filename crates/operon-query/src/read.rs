//! Read views (plan M1.2 Task 4): one [`ReadView`] per request, a durable
//! snapshot at one manifest plus a tail snapshot relative to that manifest,
//! resolved per [`ReadConsistency`].
//!
//! - `Strong` reads the implicit stream's high watermarks with one
//!   linearizable metastore read, then waits until the tail covers them.
//! - `AtLeast` waits for the token's offsets.
//! - `Eventual` takes whatever the tail holds.
//! - `Pinned` opens a retained manifest and a range tail built over exactly
//!   `(applied, token]` (Ruling 1, Ruling 14).
//!
//! A tail over its memory bound answers with a range tail over the live
//! manifest instead (§09 §7).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use operon_collection::{
    CollectionContext, CollectionError, CollectionManifest, CollectionSnapshot, ConsistencyToken,
    SplitRef, live_manifest, retained_chain,
};
use operon_common::meta::{Collection, CollectionHead, Consistency, MetaError};
use operon_common::{CollectionId, NamespaceId};
use operon_log::{LogError, LogReader};
use roaring::RoaringBitmap;

use crate::error::ServiceError;
use crate::hot::{HotTier, HotUsed, RequestHot};
use crate::ir::ReadConsistency;
use crate::tail::{
    RangeTailCache, Tail, TailBudget, TailConfig, TailError, TailRegistry, TailSnapshot, TailState,
    build_range_tail,
};

/// How reads resolve their views.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadConfig {
    /// 10 s: how long Strong/AtLeast wait for the tail.
    pub consistency_wait: Duration,
    /// 256 (cid, manifest version) → CollectionSnapshot.
    pub snapshot_cache_entries: u64,
    /// 16 (cid, manifest version, token) → range tail.
    pub pinned_tail_entries: u64,
    /// 1 GiB per range tail.
    pub range_tail_max_bytes: usize,
    /// 128 MiB of decoded delete bitmaps (row 0.41).
    pub bitmap_cache_bytes: u64,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            consistency_wait: Duration::from_secs(10),
            snapshot_cache_entries: 256,
            pinned_tail_entries: 16,
            range_tail_max_bytes: 1 << 30,
            bitmap_cache_bytes: 128 << 20,
        }
    }
}

/// A snapshot stays cached this long after its last read.
const SNAPSHOT_IDLE: Duration = Duration::from_secs(300);

/// An error of collection storage: retryable ones are `Unavailable`, the
/// rest `Internal` (rule 5).
pub(crate) fn collection_error(err: CollectionError) -> ServiceError {
    if err.is_retryable() {
        ServiceError::Unavailable(err.to_string())
    } else {
        ServiceError::Internal(err.to_string())
    }
}

pub(crate) fn meta_error(err: MetaError) -> ServiceError {
    collection_error(CollectionError::Meta(err))
}

fn log_error(err: LogError) -> ServiceError {
    match err {
        LogError::Backpressure | LogError::Store(_) | LogError::Cache(_) => {
            ServiceError::Unavailable(err.to_string())
        }
        other => ServiceError::Internal(other.to_string()),
    }
}

fn collection_not_found(collection: &Collection) -> ServiceError {
    ServiceError::NotFound {
        kind: "collection",
        name: collection.name.clone(),
    }
}

/// A tail error as a service error (rule 5).
fn tail_error(err: TailError, collection: &Collection) -> ServiceError {
    match err {
        TailError::Timeout { .. } => ServiceError::Timeout,
        TailError::Stopped => collection_not_found(collection),
        TailError::Log(err) => log_error(err),
        TailError::Collection(err) => collection_error(err),
        err @ (TailError::Overflow { .. } | TailError::Trimmed { .. }) => {
            ServiceError::Unavailable(format!("collection {}: {err}", collection.name))
        }
    }
}

/// Decoded split delete bitmaps by bitmap path (immutable objects), weighted
/// by serialized size; one per [`Reads`] (row 0.41).
///
/// M1.1's `CollectionSnapshot::deleted_docs` reads the object on every call,
/// bypassing the range cache.
#[derive(Clone)]
pub struct DeleteBitmapCache {
    cache: moka::future::Cache<String, Arc<RoaringBitmap>>,
    empty: Arc<RoaringBitmap>,
}

impl fmt::Debug for DeleteBitmapCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteBitmapCache")
            .field("entries", &self.cache.entry_count())
            .field("bytes", &self.cache.weighted_size())
            .finish()
    }
}

impl DeleteBitmapCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|path: &String, bitmap: &Arc<RoaringBitmap>| {
                    u32::try_from(bitmap.serialized_size() + path.len()).unwrap_or(u32::MAX)
                })
                .build(),
            empty: Arc::new(RoaringBitmap::new()),
        }
    }

    /// `split.delete_bitmap == None` → an empty bitmap, no cache entry;
    /// otherwise `snapshot.deleted_docs(split)` once per path.
    pub async fn deleted_docs(
        &self,
        snapshot: &CollectionSnapshot,
        split: &SplitRef,
    ) -> Result<Arc<RoaringBitmap>, ServiceError> {
        let Some(path) = &split.delete_bitmap else {
            return Ok(self.empty.clone());
        };
        self.cache
            .try_get_with(path.clone(), async {
                snapshot
                    .deleted_docs(split)
                    .await
                    .map(Arc::new)
                    .map_err(collection_error)
            })
            .await
            .map_err(|err| (*err).clone())
    }
}

/// What one request reads: a durable snapshot, the tail relative to its
/// manifest, and the hot tier in effect.
#[derive(Clone, Debug)]
pub struct ReadView {
    pub ns: NamespaceId,
    pub collection: Collection,
    pub snapshot: CollectionSnapshot,
    pub tail: Arc<TailSnapshot>,
    pub read_token: ConsistencyToken,
    pub hot: Arc<dyn HotTier>,
    pub hot_used: HotUsed,
    /// Every operator takes a split's deleted docs from here, never from
    /// `deleted_docs` directly.
    pub bitmaps: DeleteBitmapCache,
}

impl ReadView {
    /// Rows of the view: live durable rows of the manifest minus the shadow,
    /// plus the tail's live docs.
    pub fn live_rows(&self) -> u64 {
        self.snapshot
            .manifest()
            .live_doc_count
            .saturating_sub(self.tail.shadow().len())
            + self.tail.live_count()
    }

    /// Whether the durable row `row_id` is superseded by a tail entry.
    pub fn is_shadowed(&self, row_id: u64) -> bool {
        self.tail.shadow().contains(row_id)
    }
}

/// Resolves read views: the tails of this node, the snapshot and range-tail
/// caches and the delete-bitmap cache.
pub struct Reads {
    ctx: CollectionContext,
    reader: LogReader,
    tails: TailRegistry,
    snapshots: moka::future::Cache<(CollectionId, u64), CollectionSnapshot>,
    ranges: RangeTailCache,
    bitmaps: DeleteBitmapCache,
    config: ReadConfig,
}

impl fmt::Debug for Reads {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reads")
            .field("tails", &self.tails)
            .field("snapshots", &self.snapshots.entry_count())
            .field("bitmaps", &self.bitmaps)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The partitions' offsets of `head`'s high watermarks.
fn high_watermarks(head: &CollectionHead) -> BTreeMap<u32, u64> {
    (0..head.collection.partitions)
        .map(|p| {
            let hwm = head.high_watermarks.get(p as usize).copied().unwrap_or(0);
            (p, hwm)
        })
        .collect()
}

/// The token of `offsets` on `collection`'s stream, one entry per partition.
fn token_of(collection: &Collection, offsets: impl Fn(u32) -> u64) -> ConsistencyToken {
    ConsistencyToken(
        (0..collection.partitions)
            .map(|p| (collection.stream, p, offsets(p)))
            .collect(),
    )
}

fn offset_in(map: &BTreeMap<u32, u64>, partition: u32) -> u64 {
    map.get(&partition).copied().unwrap_or(0)
}

impl Reads {
    pub fn new(
        ctx: CollectionContext,
        reader: LogReader,
        tail: TailConfig,
        read: ReadConfig,
    ) -> Self {
        let budget = Arc::new(TailBudget::new(tail.total_max_bytes));
        let tails = TailRegistry::new(ctx.clone(), reader.clone(), tail, budget);
        Self {
            tails,
            snapshots: moka::future::Cache::builder()
                .max_capacity(read.snapshot_cache_entries)
                .time_to_idle(SNAPSHOT_IDLE)
                .build(),
            ranges: RangeTailCache::new(read.pinned_tail_entries),
            bitmaps: DeleteBitmapCache::new(read.bitmap_cache_bytes),
            ctx,
            reader,
            config: read,
        }
    }

    pub fn config(&self) -> &ReadConfig {
        &self.config
    }

    pub fn context(&self) -> &CollectionContext {
        &self.ctx
    }

    pub fn bitmaps(&self) -> &DeleteBitmapCache {
        &self.bitmaps
    }

    /// The view of `collection` (of namespace `ns`) for `consistency`.
    pub async fn view(
        &self,
        ns: NamespaceId,
        collection: &Collection,
        consistency: &ReadConsistency,
        hot: &RequestHot,
        hot_tier: Arc<dyn HotTier>,
    ) -> Result<ReadView, ServiceError> {
        if collection.namespace != ns {
            return Err(collection_not_found(collection));
        }
        let (collection, snapshot, tail, read_token) = match consistency {
            ReadConsistency::Strong => {
                let head = self.head(collection).await?;
                let targets = high_watermarks(&head);
                self.synced(ns, head.collection, &targets).await?
            }
            ReadConsistency::AtLeast(token) => {
                let targets: BTreeMap<u32, u64> = (0..collection.partitions)
                    .map(|p| (p, token.offset(collection.stream, p).unwrap_or(0)))
                    .collect();
                self.synced(ns, collection.clone(), &targets).await?
            }
            ReadConsistency::Eventual => self.eventual(ns, collection.clone()).await?,
            ReadConsistency::Pinned {
                manifest_version,
                token,
            } => {
                self.pinned(ns, collection.clone(), *manifest_version, token)
                    .await?
            }
        };
        hot_tier.record_access(ns, collection.id);
        Ok(ReadView {
            ns,
            collection,
            snapshot,
            tail,
            read_token,
            hot: hot_tier,
            hot_used: hot.used.clone(),
            bitmaps: self.bitmaps.clone(),
        })
    }

    /// The manifest version and the token of the high watermarks, from one
    /// linearizable read; starts the tail, so a pinned read soon after finds
    /// the range in the log (rule 7).
    pub async fn pin(
        &self,
        collection: &Collection,
    ) -> Result<(u64, ConsistencyToken), ServiceError> {
        let head = self.head(collection).await?;
        let version = head.pointer.as_ref().map_or(0, |pointer| pointer.version);
        let hwm = high_watermarks(&head);
        self.tails.tail(collection.namespace, &head.collection);
        Ok((version, token_of(&head.collection, |p| offset_in(&hwm, p))))
    }

    /// The running tail of `cid`, if any.
    pub fn tail(&self, cid: CollectionId) -> Option<Arc<Tail>> {
        self.tails.get(cid)
    }

    /// Wakes the tail of `cid`.
    pub fn notify(&self, cid: CollectionId) {
        self.tails.notify(cid);
    }

    pub fn tail_state(&self, cid: CollectionId) -> Option<TailState> {
        self.tails.state(cid)
    }

    /// Stops and forgets the tail of `cid` (the collection was dropped).
    pub async fn stop_collection(&self, cid: CollectionId) {
        self.tails.stop(cid).await;
    }

    pub async fn shutdown(&self) {
        self.tails.shutdown().await;
    }

    /// The collection record, its pointer and its stream bounds, from one
    /// linearizable read.
    async fn head(&self, collection: &Collection) -> Result<CollectionHead, ServiceError> {
        self.ctx
            .meta
            .collection_head(Consistency::Linearizable, collection.id)
            .await
            .map_err(meta_error)?
            .filter(|head| head.collection.namespace == collection.namespace)
            .ok_or_else(|| collection_not_found(collection))
    }

    /// The snapshot of `manifest`, opened once per (collection, version)
    /// while cached (rule 1).
    async fn snapshot(
        &self,
        ns: NamespaceId,
        collection: &Collection,
        path: Option<String>,
        manifest: Arc<CollectionManifest>,
    ) -> Result<CollectionSnapshot, ServiceError> {
        let key = (collection.id, manifest.version);
        self.snapshots
            .try_get_with(key, async {
                CollectionSnapshot::at(&self.ctx, ns, collection.clone(), path, manifest)
                    .await
                    .map_err(collection_error)
            })
            .await
            .map_err(|err| (*err).clone())
    }

    /// Strong and AtLeast from step 2: the tail covering `targets`, or the
    /// overflow fallback.
    async fn synced(
        &self,
        ns: NamespaceId,
        collection: Collection,
        targets: &BTreeMap<u32, u64>,
    ) -> Result<Resolved, ServiceError> {
        let tail = self.tails.tail(ns, &collection);
        let deadline = tokio::time::Instant::now() + self.config.consistency_wait;
        match tail.sync(targets, deadline).await {
            Ok(tail) => {
                let snapshot = self
                    .snapshot(
                        ns,
                        &collection,
                        tail.manifest_path().map(str::to_string),
                        tail.manifest().clone(),
                    )
                    .await?;
                let token = token_of(&collection, |p| offset_in(tail.head(), p));
                Ok((collection, snapshot, tail, token))
            }
            Err(TailError::Overflow { .. }) => self.overflow(ns, collection, targets).await,
            Err(err) => Err(tail_error(err, &collection)),
        }
    }

    /// The live manifest (`Local`), or the empty one before the first commit.
    async fn live(
        &self,
        ns: NamespaceId,
        collection: &Collection,
    ) -> Result<(Option<String>, Arc<CollectionManifest>), ServiceError> {
        let live = live_manifest(
            &*self.ctx.meta,
            &self.ctx.store,
            &self.ctx.manifests,
            ns,
            collection.id,
            Consistency::Local,
        )
        .await
        .map_err(collection_error)?;
        Ok(match live {
            Some((path, manifest)) => (Some(path), manifest),
            None => (None, Arc::new(CollectionManifest::empty(collection.id))),
        })
    }

    /// Rule 4: the live manifest plus a range tail over `(applied, targets]`.
    async fn overflow(
        &self,
        ns: NamespaceId,
        collection: Collection,
        targets: &BTreeMap<u32, u64>,
    ) -> Result<Resolved, ServiceError> {
        let (path, manifest) = self.live(ns, &collection).await?;
        let snapshot = self
            .snapshot(ns, &collection, path, manifest.clone())
            .await?;
        let built = build_range_tail(
            ns,
            &collection,
            &self.ctx,
            &self.reader,
            &snapshot,
            targets,
            self.config.range_tail_max_bytes,
        )
        .await;
        match built {
            Ok(tail) => {
                let token = token_of(&collection, |p| offset_in(tail.head(), p));
                Ok((collection, snapshot, tail, token))
            }
            Err(TailError::Overflow { .. }) => {
                let lag: u64 = targets
                    .iter()
                    .map(|(p, target)| target.saturating_sub(offset_in(&manifest.applied, *p)))
                    .sum();
                Err(ServiceError::Unavailable(format!(
                    "collection {}: {lag} records are not yet applied; retry after the link catches up",
                    collection.name
                )))
            }
            Err(err) => Err(tail_error(err, &collection)),
        }
    }

    /// Eventual: what the running tail holds, or the live manifest with an
    /// empty tail (and the tail starts in the background).
    async fn eventual(
        &self,
        ns: NamespaceId,
        collection: Collection,
    ) -> Result<Resolved, ServiceError> {
        let current = self
            .tails
            .get(collection.id)
            .map(|tail| tail.current())
            .filter(|tail| tail.is_started());
        let tail = match current {
            Some(tail) => tail,
            None => {
                self.tails.tail(ns, &collection);
                let (path, manifest) = self.live(ns, &collection).await?;
                TailSnapshot::empty(path, manifest, collection.schema.version)
            }
        };
        let snapshot = self
            .snapshot(
                ns,
                &collection,
                tail.manifest_path().map(str::to_string),
                tail.manifest().clone(),
            )
            .await?;
        let applied = &tail.manifest().applied;
        let token = token_of(&collection, |p| {
            offset_in(applied, p).max(offset_in(tail.head(), p))
        });
        Ok((collection, snapshot, tail, token))
    }

    /// Pinned `{manifest_version: v, token}` (rule 3).
    async fn pinned(
        &self,
        ns: NamespaceId,
        collection: Collection,
        version: u64,
        token: &ConsistencyToken,
    ) -> Result<Resolved, ServiceError> {
        let gone = || ServiceError::NotFound {
            kind: "pin",
            name: format!("{}@{version}", collection.name),
        };
        // 1. The token names the collection's stream for every partition and
        //    no other stream.
        let mut upper: BTreeMap<u32, u64> = BTreeMap::new();
        for &(stream, partition, offset) in &token.0 {
            if stream != collection.stream || partition >= collection.partitions {
                return Err(gone());
            }
            let slot = upper.entry(partition).or_insert(0);
            *slot = (*slot).max(offset);
        }
        if upper.len() != collection.partitions as usize {
            return Err(gone());
        }
        let head = self.head(&collection).await?;
        if head.collection.stream != collection.stream {
            return Err(gone());
        }
        let (path, manifest) = if version == 0 {
            // 2. Before the first commit: valid while the log starts at 0.
            if head.log_start_offsets.iter().any(|start| *start != 0) {
                return Err(gone());
            }
            (None, Arc::new(CollectionManifest::empty(collection.id)))
        } else {
            // 3. A retained manifest.
            self.retained(&head, version).await?.ok_or_else(gone)?
        };
        if upper
            .iter()
            .any(|(p, offset)| *offset < offset_in(&manifest.applied, *p))
        {
            return Err(ServiceError::InvalidArgument(format!(
                "the pinned token is older than manifest {version}"
            )));
        }
        let snapshot = self.snapshot(ns, &collection, path, manifest).await?;
        // 4. The range tail over (applied, token], cached.
        let key = (
            collection.id,
            version,
            token.clone().normalized().to_string(),
        );
        let tail = self
            .ranges
            .get_or_build(key, async {
                build_range_tail(
                    ns,
                    &collection,
                    &self.ctx,
                    &self.reader,
                    &snapshot,
                    &upper,
                    self.config.range_tail_max_bytes,
                )
                .await
                .map_err(|err| match err {
                    TailError::Trimmed { .. } => gone(),
                    err => tail_error(err, &collection),
                })
            })
            .await?;
        Ok((collection, snapshot, tail, token.clone()))
    }

    /// Manifest `version` of the live chain if it is retained (the rule of
    /// `CollectionSnapshot::open_version`, at the clock of `head`), without
    /// opening its Lance version, so the snapshot cache serves it.
    async fn retained(
        &self,
        head: &CollectionHead,
        version: u64,
    ) -> Result<Option<(Option<String>, Arc<CollectionManifest>)>, ServiceError> {
        let Some(pointer) = &head.pointer else {
            return Ok(None);
        };
        let cid = head.collection.id;
        let live = self
            .ctx
            .manifests
            .load(&self.ctx.store, &pointer.value)
            .await
            .map_err(collection_error)?;
        if live.version != pointer.version || live.collection_id != cid {
            return Err(ServiceError::Internal(format!(
                "{} is version {} of collection {}, but the pointer of collection {cid} is at version {}",
                pointer.value, live.version, live.collection_id, pointer.version
            )));
        }
        if version > live.version {
            return Ok(None);
        }
        let chain = retained_chain(
            &self.ctx.store,
            &self.ctx.manifests,
            (pointer.value.clone(), live),
            self.ctx.config.keep_manifests,
            self.ctx.config.time_travel_retention,
            head.clock_ms,
        )
        .await
        .map_err(collection_error)?;
        Ok(chain
            .into_iter()
            .find(|(_, manifest)| manifest.version == version)
            .map(|(path, manifest)| (Some(path), manifest)))
    }
}

/// A resolved view: the collection record, the durable snapshot, the tail
/// relative to it and the read token.
type Resolved = (
    Collection,
    CollectionSnapshot,
    Arc<TailSnapshot>,
    ConsistencyToken,
);
