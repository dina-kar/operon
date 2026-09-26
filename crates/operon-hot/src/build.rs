//! The hot artifact build task (plan M1.3 Task 5; Rulings 1, 3, 7, 8, 16,
//! 19; design §04 §3, §09 §5).
//!
//! [`HotBuildSource`] proposes `hot-build/<cid>/<i>` for every dense vector
//! *i* of every collection that is effectively hot for vectors (a catalog
//! pin, `--hot-pin-all`, or a held promotion lease). A run builds an HNSW
//! artifact from one manifest version *s* when the column has none or its
//! artifact is stale and due (Ruling 16): it scans the Lance column (and
//! `_source` for payload fields) at *s*, feeds the engine's builder on a
//! blocking thread, publishes the artifact under `hot/hnsw/`, and commits a
//! `Maintenance` manifest that references it, through the same fenced,
//! freshness-checked pointer CAS as every collection commit, rebasing on a
//! `Conflict`. The artifact stays valid for every newer manifest (Ruling 1),
//! so a rebase never rebuilds it; a newer artifact already referenced wins.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use arrow_array::{Array, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use async_trait::async_trait;
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::ROW_ID;
use operon_collection::{
    CollectionContext, CollectionError, CollectionManifest, CollectionSnapshot, CommitKind,
    HotArtifactRef, IndexValue, PointerCas, SOURCE_COLUMN, coerce, extract, live_manifest,
    put_manifest, vector_column,
};
use operon_common::meta::{ApplyError, Consistency, Fence, HotConfig, Lease, MetaError, MetaStore};
use operon_common::schema::{CollectionSchema, FieldKind, VectorSpec};
use operon_common::{CollectionId, NamespaceId};
use operon_hnsw::{
    BuildSpec, BuiltFiles, HnswEngine, HnswError, PayloadField, PayloadKind, PayloadValue, Point,
};
use operon_link::{CommitError, LinkError};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use roaring::RoaringTreemap;
use serde_json::{Map, Value};
use ulid::Ulid;

use crate::artifact::{
    ArtifactDescriptor, CurrencyCache, DESCRIPTOR_FILE, HNSW_KIND, artifact_prefix,
    decode_descriptor, publish,
};
use crate::{HotBuildConfig, TierError};

/// The prefix of a build task's key: `TaskKey::new(ns, "hot-build/<cid>/<vector index>")`.
pub const BUILD_TASK_PREFIX: &str = "hot-build/";
/// The prefix of promotion leases: `hot-promote/<ns>/<cid>` (E64), owner
/// `"<node id>;<structures>"` (Ruling 7).
pub const PROMOTE_LEASE_PREFIX: &str = "hot-promote/";

/// Point batches queued between the scan and the builder.
const BUILD_QUEUE: usize = 4;

/// The promotion lease of collection `cid` of `ns`: `hot-promote/<ns>/<cid>`.
pub fn promote_lease_key(ns: NamespaceId, cid: CollectionId) -> String {
    format!("{PROMOTE_LEASE_PREFIX}{ns}/{cid}")
}

/// The steps of an artifact build where a test hook can hold it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotBuildStep {
    /// After the artifact's descriptor PUT (`hot.after_artifact_put`).
    AfterArtifactPut,
    /// After the manifest PUT (`hot.after_manifest_put`).
    AfterManifestPut,
    /// After the pointer CAS (`hot.after_cas`).
    AfterCas,
}

/// A test hook awaited at every [`HotBuildStep`], given the build's fence.
/// Only with the `test-util` feature.
#[cfg(feature = "test-util")]
pub type HotBuildHook =
    Arc<dyn Fn(HotBuildStep, Fence) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

/// The structures a promotion lease's owner string names after its `;`
/// (`vectors`, `text`, `fragments`, comma-separated; others are ignored).
fn promoted(owner: &str) -> HotConfig {
    let mut hot = HotConfig::default();
    let Some((_, structures)) = owner.split_once(';') else {
        return hot;
    };
    for name in structures.split(',').map(str::trim) {
        match name {
            "vectors" => hot.vectors = true,
            "text" => hot.text = true,
            "fragments" => hot.fragments = true,
            _ => {}
        }
    }
    hot
}

/// The effective hot configuration of a collection (rule 1): its catalog
/// configuration, OR `{vectors, text}` under `pin_all` (Ruling 8), OR the
/// structures a promotion lease held at `now_ms` names (Ruling 7), field by
/// field.
pub fn effective_hot(
    config: HotConfig,
    promote_lease: Option<&Lease>,
    pin_all: bool,
    now_ms: u64,
) -> HotConfig {
    let mut hot = config;
    if pin_all {
        hot = hot.or(HotConfig {
            vectors: true,
            text: true,
            fragments: false,
        });
    }
    if let Some(lease) = promote_lease
        && lease.is_held_at(now_ms)
        && let Some(owner) = &lease.owner
    {
        hot = hot.or(promoted(owner));
    }
    hot
}

/// Ruling 3: the payload fields a build copies. None when
/// `vector.hnsw.payload_m == Some(0)`; otherwise the first `max` indexed
/// `Keyword`, `I64`, `Bool` and `Uuid` fields in schema order, keyed
/// `f<field index>`.
pub fn payload_fields(
    schema: &CollectionSchema,
    vector: &VectorSpec,
    max: usize,
) -> Vec<(usize, PayloadField)> {
    if vector.hnsw.payload_m == Some(0) {
        return Vec::new();
    }
    schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.indexed)
        .filter_map(|(index, field)| {
            let kind = match field.kind {
                FieldKind::Keyword => PayloadKind::Keyword,
                FieldKind::I64 => PayloadKind::Integer,
                FieldKind::Bool => PayloadKind::Bool,
                FieldKind::Uuid => PayloadKind::Uuid,
                _ => return None,
            };
            Some((
                index,
                PayloadField {
                    key: format!("f{index}"),
                    kind,
                },
            ))
        })
        .take(max)
        .collect()
}

/// The build spec of dense vector `vector` (an index of `schema.vectors`,
/// which must exist): its dim, distance, HNSW parameters and quantization,
/// the payload fields, and `indexing_threads`.
pub fn build_spec(schema: &CollectionSchema, vector: usize, config: &HotBuildConfig) -> BuildSpec {
    let spec = &schema.vectors[vector];
    BuildSpec {
        dim: spec.dim,
        distance: spec.distance,
        hnsw: spec.hnsw,
        quantization: spec.quantization,
        payload_fields: payload_fields(schema, spec, config.max_payload_fields)
            .into_iter()
            .map(|(_, field)| field)
            .collect(),
        indexing_threads: config.indexing_threads,
    }
}

/// Whether a column needs a build (rule 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildDecision {
    /// No usable artifact: none referenced, or its descriptor is gone or
    /// corrupt.
    Missing,
    /// The referenced artifact is current (Ruling 1).
    Current,
    /// Stale: `inserted` rows since it and stale for `stale_for_ms`; `due`
    /// when either reaches its threshold (Ruling 16).
    Stale {
        inserted: u64,
        stale_for_ms: u64,
        due: bool,
    },
}

/// Rule 4: whether `column` of the manifest `live` needs a build, given the
/// dataset's `next_row_id` at `live` and the metastore time `now_ms`.
pub async fn decide(
    ctx: &CollectionContext,
    live: (&str, &CollectionManifest),
    column: &str,
    dataset_next_row_id: u64,
    config: &HotBuildConfig,
    now_ms: u64,
) -> Result<BuildDecision, TierError> {
    decide_with(ctx, None, live, column, dataset_next_row_id, config, now_ms).await
}

/// [`decide`], with currency results cached in `cache` when given.
async fn decide_with(
    ctx: &CollectionContext,
    cache: Option<&CurrencyCache>,
    live: (&str, &CollectionManifest),
    column: &str,
    dataset_next_row_id: u64,
    config: &HotBuildConfig,
    now_ms: u64,
) -> Result<BuildDecision, TierError> {
    let Some(artifact) = live
        .1
        .hot_artifacts
        .iter()
        .find(|a| a.kind == HNSW_KIND && a.column == column)
    else {
        return Ok(BuildDecision::Missing);
    };
    let path = format!("{}{DESCRIPTOR_FILE}", artifact.prefix);
    let descriptor = match ctx.store.get(&path).await {
        Ok((bytes, _)) => match decode_descriptor(&bytes) {
            Ok(descriptor) if descriptor.column == column => descriptor,
            Ok(_) | Err(_) => {
                tracing::warn!(%path, "a referenced hot artifact's descriptor is unusable; rebuilding");
                return Ok(BuildDecision::Missing);
            }
        },
        Err(operon_store::StoreError::NotFound { .. }) => {
            tracing::warn!(%path, "a referenced hot artifact has no descriptor; rebuilding");
            return Ok(BuildDecision::Missing);
        }
        Err(err) => return Err(err.into()),
    };
    let currency = match cache {
        Some(cache) => {
            cache
                .currency(&ctx.store, &ctx.manifests, live, artifact.source_version)
                .await?
        }
        None => {
            crate::artifact::currency(&ctx.store, &ctx.manifests, live, artifact.source_version)
                .await?
        }
    };
    if currency.current {
        return Ok(BuildDecision::Current);
    }
    let inserted = dataset_next_row_id.saturating_sub(descriptor.next_row_id);
    let stale_for_ms = now_ms.saturating_sub(currency.stale_since_ms.unwrap_or(0));
    let by_share =
        u128::from(descriptor.points) * u128::from(config.rebuild_inserted_ppm) / 1_000_000;
    let threshold = u128::from(config.rebuild_min_inserted).max(by_share);
    let due = u128::from(inserted) >= threshold
        || u128::from(stale_for_ms) >= config.rebuild_max_staleness.as_millis();
    Ok(BuildDecision::Stale {
        inserted,
        stale_for_ms,
        due,
    })
}

/// State shared by a source and its tasks.
struct Shared {
    ctx: CollectionContext,
    config: HotBuildConfig,
    engine: Arc<dyn HnswEngine>,
    /// Per (collection, vector index), the pointer version its last idle
    /// run saw and when.
    last_checked: Mutex<BTreeMap<(CollectionId, usize), (u64, Instant)>>,
    currency: CurrencyCache,
}

/// Proposes `hot-build/<cid>/<i>` (at [`Priority::HotBuild`]) for every
/// dense vector of every collection that is effectively hot for vectors,
/// when the collection's pointer moved since that key's last idle run or
/// that run is older than `poll_interval` (rule 2). Sparse vectors get no
/// artifact (Ruling 19).
#[derive(Clone)]
pub struct HotBuildSource {
    shared: Arc<Shared>,
    #[cfg(feature = "test-util")]
    hook: Option<HotBuildHook>,
}

impl fmt::Debug for HotBuildSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotBuildSource")
            .field("ctx", &self.shared.ctx)
            .field("config", &self.shared.config)
            .field("engine", &self.shared.engine.name())
            .finish_non_exhaustive()
    }
}

impl HotBuildSource {
    /// A source building with `engine`. Removes everything under
    /// `config.work_dir` first: a crashed build's leftovers (rule 6).
    pub fn new(
        ctx: CollectionContext,
        config: HotBuildConfig,
        engine: Arc<dyn HnswEngine>,
    ) -> Result<Self, TierError> {
        match std::fs::remove_dir_all(&config.work_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        std::fs::create_dir_all(&config.work_dir)?;
        Ok(Self {
            shared: Arc::new(Shared {
                ctx,
                config,
                engine,
                last_checked: Mutex::new(BTreeMap::new()),
                currency: CurrencyCache::new(),
            }),
            #[cfg(feature = "test-util")]
            hook: None,
        })
    }

    /// Test hook: every build awaits `hook` at each [`HotBuildStep`]. Only
    /// with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: HotBuildHook) -> Self {
        self.hook = Some(hook);
        self
    }

    fn checked(&self, key: (CollectionId, usize), idle_at: Option<u64>) {
        let mut checked = self
            .shared
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match idle_at {
            Some(version) => {
                checked.insert(key, (version, Instant::now()));
            }
            None => {
                checked.remove(&key);
            }
        }
    }
}

#[async_trait]
impl TaskSource for HotBuildSource {
    fn priority(&self) -> Priority {
        Priority::HotBuild
    }

    async fn candidates(&self, meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let config = &self.shared.config;
        let heads = meta.collection_heads(Consistency::Local, None).await?;
        let leases: BTreeMap<String, Lease> = meta
            .leases_with_prefix(Consistency::Local, PROMOTE_LEASE_PREFIX)
            .await?
            .into_iter()
            .collect();
        let now_ms = meta.now_ms();
        // (key, pointer version) of every hot dense vector.
        let mut hot = Vec::new();
        for head in heads {
            let Some(pointer) = head.pointer else {
                continue;
            };
            let collection = head.collection;
            let (ns, cid) = (collection.namespace, collection.id);
            let pinned = match meta.collection_hot(Consistency::Local, ns, cid).await {
                Ok(pinned) => pinned,
                // Dropped since the heads were read.
                Err(MetaError::Rejected(ApplyError::CollectionNotFound(_))) => continue,
                Err(err) => return Err(err.into()),
            };
            let lease = leases.get(&promote_lease_key(ns, cid));
            if !effective_hot(pinned, lease, config.pin_all, now_ms).vectors {
                continue;
            }
            for index in 0..collection.schema.vectors.len() {
                hot.push((ns, cid, index, pointer.version));
            }
        }
        let mut checked = self
            .shared
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let live: BTreeSet<(CollectionId, usize)> = hot
            .iter()
            .map(|(_, cid, index, _)| (*cid, *index))
            .collect();
        checked.retain(|key, _| live.contains(key));
        Ok(hot
            .into_iter()
            .filter(|(_, cid, index, version)| {
                checked.get(&(*cid, *index)).is_none_or(|(seen, at)| {
                    *seen != *version || at.elapsed() >= config.poll_interval
                })
            })
            .map(|(ns, cid, vector, _)| {
                let task: Arc<dyn Task> = Arc::new(BuildTask {
                    source: self.clone(),
                    ns,
                    cid,
                    vector,
                });
                (
                    TaskKey::new(ns, format!("{BUILD_TASK_PREFIX}{cid}/{vector}")),
                    task,
                )
            })
            .collect())
    }
}

/// Builds and commits the artifact of one dense vector of one collection.
struct BuildTask {
    source: HotBuildSource,
    ns: NamespaceId,
    cid: CollectionId,
    vector: usize,
}

fn failed(err: impl Into<TierError>) -> TaskError {
    TaskError::failed(err.into())
}

/// Removes a build directory when dropped: every exit path of a run (rule
/// 6), cancellation included.
struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        remove_dir(&self.0);
    }
}

/// Removes `dir` and everything under it; a missing directory is fine.
fn remove_dir(dir: &Path) {
    if let Err(err) = std::fs::remove_dir_all(dir)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(dir = %dir.display(), %err, "removing a hot directory");
    }
}

/// Removes a loaded artifact's or a delta index's local directory when its
/// last user drops it (Task 6): on a blocking thread when a Tokio runtime is
/// running, since that user may be a query on an async worker thread and the
/// directory may hold gigabytes (row 6.7); inline otherwise.
#[derive(Debug)]
pub(crate) struct LocalCopy(pub(crate) PathBuf);

impl Drop for LocalCopy {
    fn drop(&mut self) {
        let dir = std::mem::take(&mut self.0);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn_blocking(move || remove_dir(&dir));
            }
            Err(_) => remove_dir(&dir),
        }
    }
}

#[async_trait]
impl Task for BuildTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let result = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return Ok(TaskOutcome::Done),
            result = self.run_inner(&ctx.fence) => result,
        };
        let key = (self.cid, self.vector);
        match &result {
            Ok((TaskOutcome::Idle, version)) => self.source.checked(key, Some(*version)),
            _ => self.source.checked(key, None),
        }
        result.map(|(outcome, _)| outcome)
    }
}

impl BuildTask {
    fn ctx(&self) -> &CollectionContext {
        &self.source.shared.ctx
    }

    fn config(&self) -> &HotBuildConfig {
        &self.source.shared.config
    }

    async fn step(&self, step: HotBuildStep, fence: &Fence) {
        match step {
            HotBuildStep::AfterArtifactPut => {}
            HotBuildStep::AfterManifestPut => {
                crate::failpoint!("hot.after_manifest_put");
            }
            HotBuildStep::AfterCas => {
                crate::failpoint!("hot.after_cas");
            }
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.source.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    /// Whether the collection is still effectively hot for vectors
    /// (`Linearizable`); `None` when it was dropped.
    async fn still_hot(&self) -> Result<Option<bool>, TaskError> {
        let meta = &*self.ctx().meta;
        let pinned = match meta
            .collection_hot(Consistency::Linearizable, self.ns, self.cid)
            .await
        {
            Ok(pinned) => pinned,
            Err(MetaError::Rejected(ApplyError::CollectionNotFound(_))) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let lease = meta
            .lease(
                Consistency::Linearizable,
                &promote_lease_key(self.ns, self.cid),
            )
            .await?;
        let hot = effective_hot(pinned, lease.as_ref(), self.config().pin_all, meta.now_ms());
        Ok(Some(hot.vectors))
    }

    /// Rule 5; returns the outcome and the pointer version an idle run saw.
    async fn run_inner(&self, fence: &Fence) -> Result<(TaskOutcome, u64), TaskError> {
        let (ns, cid, index) = (self.ns, self.cid, self.vector);
        let ctx = self.ctx();
        let snapshot = match CollectionSnapshot::open(ctx, ns, cid, Consistency::Linearizable).await
        {
            Ok(snapshot) => snapshot,
            Err(CollectionError::NotFound(_)) => return Ok((TaskOutcome::Idle, 0)),
            Err(err) => return Err(failed(err)),
        };
        let manifest = snapshot.manifest();
        let idle = Ok((TaskOutcome::Idle, manifest.version));
        let schema = &snapshot.collection().schema;
        let column = vector_column(index);
        let (Some(dataset), Some(vector), Some(manifest_path)) = (
            snapshot.dataset(),
            schema.vectors.get(index),
            snapshot.manifest_path(),
        ) else {
            return idle;
        };
        // A vector added after the Lance version was written (E51).
        if dataset.schema().field(&column).is_none() {
            return idle;
        }
        if self.still_hot().await? != Some(true) {
            return idle;
        }
        let next_row_id = dataset.manifest.next_row_id;
        let decision = decide_with(
            ctx,
            Some(&self.source.shared.currency),
            (manifest_path, manifest),
            &column,
            next_row_id,
            self.config(),
            ctx.meta.now_ms(),
        )
        .await
        .map_err(failed)?;
        match decision {
            BuildDecision::Missing | BuildDecision::Stale { due: true, .. } => {}
            BuildDecision::Current | BuildDecision::Stale { due: false, .. } => return idle,
        }

        // 2.–3. Build.
        let source_version = manifest.version;
        let dir = self
            .config()
            .work_dir
            .join(cid.to_string())
            .join(index.to_string())
            .join(Ulid::generate().to_string());
        let guard = DirGuard(dir.clone());
        let spec = build_spec(schema, index, self.config());
        let fields = payload_fields(schema, vector, self.config().max_payload_fields);
        let (built, scanned) = self
            .build(
                dataset.clone(),
                schema,
                &column,
                spec.clone(),
                &fields,
                &dir,
            )
            .await
            .map_err(failed)?;

        // 4. Publish.
        let started = ctx.meta.now_ms();
        let ulid = Ulid::from_parts(started, Ulid::generate().random());
        let prefix = artifact_prefix(ns, cid, &column, source_version, ulid);
        let descriptor = ArtifactDescriptor {
            engine: self.source.shared.engine.name().to_string(),
            collection: cid.0,
            column: column.clone(),
            vector: vector.name.clone(),
            spec,
            source_version,
            lance_version: manifest.lance_version,
            next_row_id,
            points: built.points,
            scanned: scanned.len(),
            files: Vec::new(),
            covered_len: 0,
            covered_crc32c: 0,
            chunk_bytes: self.config().chunk_bytes,
            created_at_ms: started,
        };
        publish(
            &ctx.store,
            &prefix,
            &dir.join("out"),
            &built,
            &scanned,
            descriptor,
        )
        .await
        .map_err(failed)?;
        tracing::info!(
            collection = %cid,
            %column,
            source_version,
            points = built.points,
            scanned = scanned.len(),
            %prefix,
            "published a hot artifact"
        );
        self.step(HotBuildStep::AfterArtifactPut, fence).await;
        drop(guard);

        // 5. Commit.
        self.commit(&column, prefix, source_version, started, fence)
            .await
    }

    /// Scans `column` of `dataset` into the engine's builder, on a blocking
    /// thread fed through a bounded channel (rule 5.3); returns the built
    /// files (under `dir/out`) and every scanned row id.
    async fn build(
        &self,
        dataset: Arc<Dataset>,
        schema: &CollectionSchema,
        column: &str,
        spec: BuildSpec,
        fields: &[(usize, PayloadField)],
        dir: &Path,
    ) -> Result<(BuiltFiles, RoaringTreemap), TierError> {
        let (build_dir, out_dir) = (dir.join("build"), dir.join("out"));
        std::fs::create_dir_all(&build_dir)?;
        std::fs::create_dir_all(&out_dir)?;
        let engine = self.source.shared.engine.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Point>>(BUILD_QUEUE);
        let dim = spec.dim;
        let builder = tokio::task::spawn_blocking(move || -> Result<BuiltFiles, HnswError> {
            let mut builder = engine.builder(&spec, &build_dir)?;
            while let Some(points) = rx.blocking_recv() {
                builder.add(points)?;
            }
            builder.finish(&out_dir)
        });
        let scanned = self.scan(&dataset, schema, column, dim, fields, tx).await;
        let built = builder
            .await
            .map_err(|err| TierError::Other(format!("building a hot artifact: {err}")));
        // A scan error explains a short build; report it first.
        let scanned = scanned?;
        let built = built??;
        Ok((built, scanned))
    }

    /// Sends the points of every row of `column` (with the payload `fields`
    /// from `_source`), one batch per scan batch; returns every scanned row
    /// id. Stops early if the builder is gone (its error is the build's).
    async fn scan(
        &self,
        dataset: &Dataset,
        schema: &CollectionSchema,
        column: &str,
        dim: u32,
        fields: &[(usize, PayloadField)],
        tx: tokio::sync::mpsc::Sender<Vec<Point>>,
    ) -> Result<RoaringTreemap, TierError> {
        let mut columns = vec![column.to_string()];
        if !fields.is_empty() {
            columns.push(SOURCE_COLUMN.to_string());
        }
        let mut scanner = dataset.scan();
        scanner
            .project(&columns)
            .map_err(CollectionError::from)?
            .with_row_id()
            .batch_size(self.config().scan_batch_rows.max(1));
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(CollectionError::from)?;
        let mut scanned = RoaringTreemap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(CollectionError::from)?;
            let points = batch_points(&batch, schema, column, dim, fields, &mut scanned)?;
            if !points.is_empty() && tx.send(points).await.is_err() {
                break;
            }
        }
        Ok(scanned)
    }

    /// Commits `prefix` as `column`'s HNSW artifact on the live manifest,
    /// rebasing on a `Conflict` (rule 5.5).
    async fn commit(
        &self,
        column: &str,
        prefix: String,
        source_version: u64,
        started: u64,
        fence: &Fence,
    ) -> Result<(TaskOutcome, u64), TaskError> {
        let ctx = self.ctx();
        let (ns, cid) = (self.ns, self.cid);
        let max_rebases = self.config().max_rebases;
        for rebase in 0..=max_rebases {
            let Some((parent_path, parent)) = live_manifest(
                &*ctx.meta,
                &ctx.store,
                &ctx.manifests,
                ns,
                cid,
                Consistency::Linearizable,
            )
            .await
            .map_err(failed)?
            else {
                let exists = ctx
                    .meta
                    .collection(Consistency::Linearizable, cid)
                    .await
                    .map_err(failed)?
                    .is_some_and(|c| c.namespace == ns);
                if !exists {
                    // Dropped while the build ran (row 5.5).
                    return Ok((TaskOutcome::Idle, 0));
                }
                return Err(failed(CollectionError::Corrupt(format!(
                    "collection {cid} lost its manifest pointer"
                ))));
            };
            if parent.hot_artifacts.iter().any(|a| {
                a.kind == HNSW_KIND && a.column == column && a.source_version > source_version
            }) {
                tracing::info!(
                    collection = %cid,
                    %column,
                    source_version,
                    "a newer hot artifact is already committed; abandoning this one"
                );
                return Ok((TaskOutcome::Done, 0));
            }
            let manifest = artifact_manifest(
                &parent_path,
                &parent,
                HotArtifactRef {
                    kind: HNSW_KIND.to_string(),
                    column: column.to_string(),
                    prefix: prefix.clone(),
                    source_version,
                },
                started,
            );
            let path = put_manifest(ctx, ns, &manifest).await.map_err(failed)?;
            self.step(HotBuildStep::AfterManifestPut, fence).await;
            let cas = PointerCas {
                ns,
                cid,
                parent_version: parent.version,
                path: &path,
                started,
                max_age: self.config().artifact_commit_delay,
            };
            match cas.run(ctx, fence).await {
                Ok(version) => {
                    self.step(HotBuildStep::AfterCas, fence).await;
                    tracing::info!(
                        collection = %cid,
                        %column,
                        source_version,
                        version,
                        rebases = rebase,
                        "committed a hot artifact"
                    );
                    return Ok((TaskOutcome::Done, version));
                }
                Err(CommitError::Conflict) => {
                    tracing::info!(
                        collection = %cid,
                        parent = parent.version,
                        "a hot artifact commit conflicted; rebasing onto the live manifest"
                    );
                }
                Err(CommitError::Fenced) => return Err(TaskError::Fenced),
                // Dropped while the build ran.
                Err(CommitError::Other(LinkError::NotFound(_))) => {
                    return Ok((TaskOutcome::Idle, 0));
                }
                Err(CommitError::Other(err)) => return Err(TaskError::failed(err)),
            }
        }
        Err(failed(CollectionError::Blocked(format!(
            "a hot artifact commit of collection {cid} conflicted {} times",
            max_rebases + 1
        ))))
    }
}

/// Manifest v+1 on `parent` (at `parent_path`) with `artifact` replacing the
/// `hnsw` entry of its column (appended if there is none): a `Maintenance`
/// commit started at `started`, without PK delta or dead letters.
fn artifact_manifest(
    parent_path: &str,
    parent: &CollectionManifest,
    artifact: HotArtifactRef,
    started: u64,
) -> CollectionManifest {
    let mut hot_artifacts = parent.hot_artifacts.clone();
    match hot_artifacts
        .iter_mut()
        .find(|a| a.kind == artifact.kind && a.column == artifact.column)
    {
        Some(existing) => *existing = artifact,
        None => hot_artifacts.push(artifact),
    }
    CollectionManifest {
        version: parent.version + 1,
        parent_version: parent.version,
        parent_manifest: Some(parent_path.to_string()),
        created_at_ms: started,
        hot_artifacts,
        kind: CommitKind::Maintenance,
        pk_delta: None,
        dead_letters: None,
        ..parent.clone()
    }
}

fn corrupt_column(column: &str, message: impl fmt::Display) -> TierError {
    TierError::Collection(CollectionError::Corrupt(format!("{column}: {message}")))
}

/// The points of one scan batch; adds every row id to `scanned`. A row whose
/// vector is null is scanned but not a point.
fn batch_points(
    batch: &RecordBatch,
    schema: &CollectionSchema,
    column: &str,
    dim: u32,
    fields: &[(usize, PayloadField)],
    scanned: &mut RoaringTreemap,
) -> Result<Vec<Point>, TierError> {
    let row_ids = batch
        .column_by_name(ROW_ID)
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| corrupt_column(column, "the scan has no row ids"))?;
    let vectors = batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>())
        .ok_or_else(|| corrupt_column(column, "not a fixed-size list"))?;
    let sources = match fields.is_empty() {
        true => None,
        false => Some(
            batch
                .column_by_name(SOURCE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| corrupt_column(SOURCE_COLUMN, "not binary"))?,
        ),
    };
    let mut points = Vec::new();
    for row in 0..batch.num_rows() {
        let row_id = row_ids.value(row);
        scanned.insert(row_id);
        if vectors.is_null(row) {
            continue;
        }
        let values = vectors.value(row);
        let values = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| corrupt_column(column, "not f32 values"))?;
        if values.len() != dim as usize || values.null_count() > 0 {
            return Err(corrupt_column(
                column,
                format!("row {row_id} has a malformed vector"),
            ));
        }
        let payload = match sources {
            None => Vec::new(),
            Some(sources) => {
                let source: Map<String, Value> = serde_json::from_slice(sources.value(row))
                    .map_err(|err| corrupt_column(SOURCE_COLUMN, format!("row {row_id}: {err}")))?;
                payload(schema, fields, &source)
            }
        };
        points.push(Point {
            id: row_id,
            vector: values.values().to_vec(),
            payload,
        });
    }
    Ok(points)
}

/// The payload fields of an artifact's spec with their schema field
/// indexes, recovered from their `f<field index>` keys (Ruling 3), so a
/// delta index copies exactly the fields its artifact copied.
pub(crate) fn spec_payload_fields(spec: &BuildSpec) -> Vec<(usize, PayloadField)> {
    spec.payload_fields
        .iter()
        .filter_map(|field| {
            let index = field.key.strip_prefix('f')?.parse().ok()?;
            Some((index, field.clone()))
        })
        .collect()
}

/// The payload of one document: per field, its values from `_source`
/// coerced to the field's kind; malformed values and nulls are skipped
/// (rule 5.3), and a field without values is left out.
pub(crate) fn payload(
    schema: &CollectionSchema,
    fields: &[(usize, PayloadField)],
    source: &Map<String, Value>,
) -> Vec<(String, Vec<PayloadValue>)> {
    let mut out = Vec::new();
    for (index, field) in fields {
        let Some(spec) = schema.fields.get(*index) else {
            continue;
        };
        let values: Vec<PayloadValue> = extract(source, &spec.source_path)
            .iter()
            .filter_map(|value| coerce(&spec.kind, value.as_ref()).ok().flatten())
            .filter_map(|value| match (field.kind, value) {
                (PayloadKind::Keyword, IndexValue::Keyword(s)) => Some(PayloadValue::Keyword(s)),
                (PayloadKind::Integer, IndexValue::I64(n)) => Some(PayloadValue::Integer(n)),
                (PayloadKind::Bool, IndexValue::Bool(b)) => Some(PayloadValue::Bool(b)),
                (PayloadKind::Uuid, IndexValue::Uuid(u)) => Some(PayloadValue::Uuid(u)),
                _ => None,
            })
            .collect();
        if !values.is_empty() {
            out.push((field.key.clone(), values));
        }
    }
    out
}
