//! Index builds as worker tasks (plan M1.1 Task 11; overview R9, A18;
//! Rulings 3 and 27): Lance vector indexes (IVF_PQ by default, IVF_RQ,
//! IVF_HNSW_SQ) and the `_pk` BTREE, one task per collection keyed
//! `collection-index/<cid>`, at [`Priority::Compaction`].
//!
//! A vector index grows by *delta segments*, each built over the fragments
//! no segment covers yet (`CreateIndexBuilder::fragments`), and a full
//! rebuild replaces every segment once there are `index_max_segments` of
//! them; `optimize_indices` commits to the Lance mainline and is never used
//! (Ruling 3). Every build is staged (`execute_uncommitted`) and committed
//! as a detached `Operation::CreateIndex` ([`LanceCommitter::commit`]),
//! then under a new manifest by the same fenced, freshness-checked CAS as
//! link apply. On a `Conflict` the same index files are re-committed onto
//! the new live manifest (a rebase), never rebuilt. The PK index is never
//! touched.
//!
//! Lance's 8-bit PQ refuses fewer than 256 training rows, so no trained
//! index is built over fewer than `MIN_TRAINING_ROWS` rows that have the
//! vector (controller ruling P1): such work is deferred, not failed, until
//! enough rows exist. Unindexed rows cost only latency: search over them is
//! a brute-force scan, which is exact. Rows without the vector (a null
//! column) are never indexed and never found by vector search. Sparse
//! vectors get no Lance index: their index is the split (Ruling 27).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance::deps::datafusion::prelude::col;
use lance::index::vector::VectorIndexParams;
use lance::index::{CreateIndexBuilder, DatasetIndexExt};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::{IndexParams, IndexType};
use lance_linalg::distance::DistanceType;
use lance_table::format::IndexMetadata;
use operon_common::{CollectionId, NamespaceId};
use operon_link::CommitError;
use operon_meta::{Collection, Consistency, Fence, MetaClient, collection_pointer_key};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use roaring::RoaringBitmap;

use crate::arrow_schema::{PK_COLUMN, vector_column};
use crate::chain::live_manifest;
use crate::config::CollectionConfig;
use crate::error::CollectionError;
use crate::lance::LanceCommitter;
use crate::manifest::{
    CollectionManifest, CommitKind, ScalarIndexRef, VectorIndexKind, VectorIndexRef,
};
use crate::schema::{CollectionSchema, Distance, VectorIndexSpec, VectorSpec};
use crate::snapshot::CollectionContext;
#[cfg(feature = "test-util")]
use crate::target::CollectionCommitHook;
use crate::target::{CollectionCommitStep, PointerCas, put_manifest};

/// The prefix of an index task's key: `TaskKey::new(ns, "collection-index/<cid>")`.
pub const INDEX_TASK_PREFIX: &str = "collection-index/";

/// The Lance name of the `_pk` BTREE index.
pub const PK_INDEX_NAME: &str = "pk_btree";

/// The fewest rows with the vector a trained index (IVF centroids, PQ
/// codebooks) is built over: Lance's 8-bit PQ needs 256 (`lance-index`
/// `pq/builder.rs`), and every other trained index gets the same floor
/// (controller ruling P1).
const MIN_TRAINING_ROWS: u64 = 256;

/// Rebases of one index commit before the run gives up.
const MAX_REBASES: usize = 5;

/// k-means iterations of PQ training.
const PQ_MAX_ITERATIONS: usize = 50;

/// The most IVF partitions a default partition count gives.
const MAX_PARTITIONS: u64 = 4096;

/// The Lance name of vector `vector_index`'s index: `vec_<i>`.
pub fn vector_index_name(vector_index: usize) -> String {
    format!("vec_{vector_index}")
}

/// One index build a collection needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexWork {
    /// A full build of vector `vector`'s index, replacing every segment.
    VectorFull { vector: usize },
    /// A delta segment over `fragments`, the fragments no segment covers.
    VectorDelta { vector: usize, fragments: Vec<u32> },
    /// A full (re)build of the `_pk` BTREE.
    PkBtree,
}

/// The index kind a vector gets: `Auto` is IVF_PQ except for Manhattan,
/// which Lance cannot index; `None` is no index.
fn effective_kind(spec: &VectorSpec) -> Option<VectorIndexKind> {
    match spec.index {
        VectorIndexSpec::None => None,
        _ if spec.distance == Distance::Manhattan => None,
        VectorIndexSpec::Auto | VectorIndexSpec::IvfPq { .. } => Some(VectorIndexKind::IvfPq),
        VectorIndexSpec::IvfRq { .. } => Some(VectorIndexKind::IvfRq),
        VectorIndexSpec::IvfHnswSq { .. } => Some(VectorIndexKind::IvfHnswSq),
    }
}

/// The segments of index `name` and the fragments they cover.
fn coverage(indices: &[IndexMetadata], name: &str) -> (usize, RoaringBitmap) {
    let mut segments = 0;
    let mut covered = RoaringBitmap::new();
    for index in indices.iter().filter(|index| index.name == name) {
        segments += 1;
        if let Some(bitmap) = &index.fragment_bitmap {
            covered |= bitmap;
        }
    }
    (segments, covered)
}

/// The fragments of `dataset` outside `covered`, and their live rows.
fn unindexed(dataset: &Dataset, covered: &RoaringBitmap) -> (Vec<u32>, u64) {
    let mut fragments = Vec::new();
    let mut rows = 0u64;
    for fragment in dataset.manifest.fragments.iter() {
        let Ok(id) = u32::try_from(fragment.id) else {
            continue;
        };
        if covered.contains(id) {
            continue;
        }
        fragments.push(id);
        rows += fragment.num_rows().unwrap_or(0) as u64;
    }
    (fragments, rows)
}

/// What `manifest` and its Lance version `dataset` (whose indexes are
/// `indices`) need, in order: each indexed vector's work, then the `_pk`
/// BTREE. Pure.
///
/// - A vector gets a first, full build once `live_doc_count` reaches
///   `index_min_rows`; once it has segments, a delta segment over the
///   unindexed fragments when they hold `index_delta_min_rows` live rows, or
///   a full rebuild instead when it has `index_max_segments` segments. Both
///   thresholds are at least `MIN_TRAINING_ROWS`. A vector whose column
///   this Lance version lacks (added lazily, Ruling 5) gets nothing.
/// - The `_pk` BTREE is built once `live_doc_count` reaches
///   `pk_index_min_unindexed_rows`, and rebuilt when that many rows are
///   unindexed.
pub fn plan_index_work(
    schema: &CollectionSchema,
    manifest: &CollectionManifest,
    dataset: &Dataset,
    indices: &[IndexMetadata],
    config: &CollectionConfig,
) -> Vec<IndexWork> {
    let mut work = Vec::new();
    for (vector, spec) in schema.vectors.iter().enumerate() {
        if effective_kind(spec).is_none()
            || dataset.schema().field(&vector_column(vector)).is_none()
        {
            continue;
        }
        let (segments, covered) = coverage(indices, &vector_index_name(vector));
        let (fragments, rows) = unindexed(dataset, &covered);
        if segments == 0 {
            if manifest.live_doc_count >= config.index_min_rows.max(MIN_TRAINING_ROWS) {
                work.push(IndexWork::VectorFull { vector });
            }
        } else if rows >= config.index_delta_min_rows.max(MIN_TRAINING_ROWS) {
            work.push(if segments >= config.index_max_segments {
                IndexWork::VectorFull { vector }
            } else {
                IndexWork::VectorDelta { vector, fragments }
            });
        }
    }
    let min_pk_rows = config.pk_index_min_unindexed_rows.max(1);
    let (segments, covered) = coverage(indices, PK_INDEX_NAME);
    let (_, rows) = unindexed(dataset, &covered);
    let needed = match segments {
        0 => manifest.live_doc_count >= min_pk_rows,
        _ => rows >= min_pk_rows,
    };
    if needed {
        work.push(IndexWork::PkBtree);
    }
    work
}

/// The Lance distance of a dense vector's distance.
fn distance_type(distance: Distance) -> Option<DistanceType> {
    match distance {
        Distance::Cosine => Some(DistanceType::Cosine),
        Distance::Dot => Some(DistanceType::Dot),
        Distance::Euclid => Some(DistanceType::L2),
        Distance::Manhattan => None,
    }
}

/// `clamp(round(sqrt(rows)), 1, 4096)`.
fn default_partitions(rows: u64) -> usize {
    let partitions = (rows as f64).sqrt().round() as u64;
    partitions.clamp(1, MAX_PARTITIONS) as usize
}

/// `dim / k`, with `k` the first of 16, 8, 4, 2, 1 that divides `dim`.
fn default_sub_vectors(dim: u32) -> usize {
    let k = [16, 8, 4, 2, 1]
        .into_iter()
        .find(|k| dim.is_multiple_of(*k))
        .unwrap_or(1);
    (dim / k) as usize
}

/// The IVF partitions of vector `spec`'s index over `rows` rows: the spec's
/// `num_partitions`, else [`default_partitions`].
fn partitions_for(spec: &VectorSpec, rows: u64) -> usize {
    let given = match spec.index {
        VectorIndexSpec::Auto | VectorIndexSpec::None => None,
        VectorIndexSpec::IvfPq { num_partitions, .. }
        | VectorIndexSpec::IvfRq { num_partitions, .. }
        | VectorIndexSpec::IvfHnswSq { num_partitions } => num_partitions,
    };
    given.map_or_else(|| default_partitions(rows), |p| p as usize)
}

/// The fewest rows with the vector that vector `spec`'s index can be
/// trained on when built over `rows` rows: `MIN_TRAINING_ROWS`, and at
/// least one row per IVF partition (Lance's k-means refuses fewer rows than
/// centroids, `lance-index` `kmeans.rs`). PQ's own k-means needs 2^num_bits
/// ≤ 256 rows, which the floor covers.
fn training_minimum(spec: &VectorSpec, rows: u64) -> u64 {
    MIN_TRAINING_ROWS.max(partitions_for(spec, rows) as u64)
}

/// The Lance parameters of vector `spec`'s index over `rows` rows.
fn vector_params(spec: &VectorSpec, rows: u64) -> Option<VectorIndexParams> {
    let metric = distance_type(spec.distance)?;
    let partitions = partitions_for(spec, rows);
    Some(match spec.index {
        VectorIndexSpec::None => return None,
        VectorIndexSpec::Auto => VectorIndexParams::ivf_pq(
            partitions,
            8,
            default_sub_vectors(spec.dim),
            metric,
            PQ_MAX_ITERATIONS,
        ),
        VectorIndexSpec::IvfPq {
            num_sub_vectors,
            num_bits,
            ..
        } => VectorIndexParams::ivf_pq(
            partitions,
            num_bits,
            num_sub_vectors.map_or_else(|| default_sub_vectors(spec.dim), |n| n as usize),
            metric,
            PQ_MAX_ITERATIONS,
        ),
        VectorIndexSpec::IvfRq { num_bits, .. } => {
            VectorIndexParams::ivf_rq(partitions, num_bits, metric)
        }
        VectorIndexSpec::IvfHnswSq { .. } => VectorIndexParams::with_ivf_hnsw_sq_params(
            metric,
            IvfBuildParams::new(partitions),
            HnswBuildParams::default()
                .num_edges(spec.hnsw.m as usize)
                .ef_construction(spec.hnsw.ef_construct as usize),
            SQBuildParams::default(),
        ),
    })
}

/// The live rows of `dataset` (only of `fragments`, if given) whose
/// `column` is not null.
async fn rows_with(
    dataset: &Dataset,
    column: &str,
    fragments: Option<&[u32]>,
) -> Result<u64, CollectionError> {
    let mut scanner = dataset.scan();
    if let Some(ids) = fragments {
        let ids: BTreeSet<u64> = ids.iter().map(|id| u64::from(*id)).collect();
        scanner.with_fragments(
            dataset
                .manifest
                .fragments
                .iter()
                .filter(|fragment| ids.contains(&fragment.id))
                .cloned()
                .collect(),
        );
    }
    scanner.filter_expr(col(column).is_not_null());
    Ok(scanner.count_rows().await?)
}

/// What a build makes, for its manifest entry.
#[derive(Clone, Copy, Debug)]
enum Built {
    Vector {
        vector: usize,
        kind: VectorIndexKind,
    },
    Pk,
}

/// Lance parameters of either kind.
enum Params {
    Vector(VectorIndexParams),
    Scalar(ScalarIndexParams),
}

impl Params {
    fn as_dyn(&self) -> &dyn IndexParams {
        match self {
            Params::Vector(params) => params,
            Params::Scalar(params) => params,
        }
    }
}

/// One index build, ready to run.
struct Build {
    name: String,
    column: String,
    index_type: IndexType,
    params: Params,
    /// A delta segment's fragments; `None` for a full build.
    fragments: Option<Vec<u32>>,
    built: Built,
}

impl fmt::Debug for Build {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Build")
            .field("name", &self.name)
            .field("column", &self.column)
            .field("fragments", &self.fragments)
            .field("built", &self.built)
            .finish_non_exhaustive()
    }
}

/// A built index, staged in Lance but not committed.
struct Staged {
    index: IndexMetadata,
    /// The `next_row_id` of the Lance version it was built on.
    indexed_upto: u64,
    built: Built,
    /// The commit's start, no later than the index files.
    started: u64,
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
    /// Per collection, the pointer version its last idle run saw and when.
    last_checked: Mutex<BTreeMap<CollectionId, (u64, Instant)>>,
}

/// Proposes one index-build task per collection (keyed
/// `collection-index/<cid>`, at [`Priority::Compaction`]) whose pointer
/// version moved since its last idle run, or whose last check is older
/// than `index_poll_interval`. A run does one [`IndexWork`] and ends
/// `MoreWork` if more remains, else `Idle`. Registered at worker start; it
/// discovers collections from the metastore.
#[derive(Clone)]
pub struct IndexBuildSource {
    shared: Arc<Shared>,
    #[cfg(feature = "test-util")]
    hook: Option<CollectionCommitHook>,
}

impl fmt::Debug for IndexBuildSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let checked = self
            .shared
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("IndexBuildSource")
            .field("ctx", &self.shared.ctx)
            .field("checked", &checked)
            .finish_non_exhaustive()
    }
}

impl IndexBuildSource {
    pub fn new(ctx: CollectionContext) -> Self {
        Self {
            shared: Arc::new(Shared {
                ctx,
                last_checked: Mutex::default(),
            }),
            #[cfg(feature = "test-util")]
            hook: None,
        }
    }

    /// Test hook: every index commit awaits `hook` at
    /// [`CollectionCommitStep::AfterLanceCommit`] and
    /// [`CollectionCommitStep::AfterCas`]. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: CollectionCommitHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Records how a run of `cid` ended: an idle run at pointer `version`
    /// is not proposed again until the pointer moves or the poll interval
    /// passes; any other run is proposed at the next poll.
    fn checked(&self, cid: CollectionId, idle_at: Option<u64>) {
        let mut checked = self
            .shared
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match idle_at {
            Some(version) => {
                checked.insert(cid, (version, Instant::now()));
            }
            None => {
                checked.remove(&cid);
            }
        }
    }
}

#[async_trait]
impl TaskSource for IndexBuildSource {
    fn priority(&self) -> Priority {
        Priority::Compaction
    }

    async fn candidates(&self, meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        let collections: Vec<(NamespaceId, CollectionId, u64)> = meta
            .read(Consistency::Local, |s| {
                s.all_collections()
                    .filter_map(|c| {
                        let pointer = s.pointer(c.namespace, &collection_pointer_key(c.id))?;
                        Some((c.namespace, c.id, pointer.version))
                    })
                    .collect()
            })
            .await?;
        let interval = self.shared.ctx.config.index_poll_interval;
        let mut checked = self
            .shared
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let live: BTreeSet<CollectionId> = collections.iter().map(|(_, cid, _)| *cid).collect();
        checked.retain(|cid, _| live.contains(cid));
        let mut candidates = Vec::new();
        for (ns, cid, version) in collections {
            let due = checked
                .get(&cid)
                .is_none_or(|(seen, at)| *seen != version || at.elapsed() >= interval);
            if !due {
                continue;
            }
            let task: Arc<dyn Task> = Arc::new(IndexTask {
                source: self.clone(),
                ns,
                cid,
            });
            candidates.push((TaskKey::new(ns, format!("{INDEX_TASK_PREFIX}{cid}")), task));
        }
        Ok(candidates)
    }
}

/// How one run ended, and the pointer version it last saw.
type Ran = (TaskOutcome, u64);

/// Builds the indexes of one collection.
struct IndexTask {
    source: IndexBuildSource,
    ns: NamespaceId,
    cid: CollectionId,
}

fn failed(err: CollectionError) -> TaskError {
    TaskError::failed(err)
}

#[async_trait]
impl Task for IndexTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let result = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return Ok(TaskOutcome::Done),
            result = self.run_inner(&ctx.fence) => result,
        };
        match &result {
            Ok((TaskOutcome::Idle, version)) => self.source.checked(self.cid, Some(*version)),
            _ => self.source.checked(self.cid, None),
        }
        result.map(|(outcome, _)| outcome)
    }
}

impl IndexTask {
    fn ctx(&self) -> &CollectionContext {
        &self.source.shared.ctx
    }

    async fn step(&self, step: CollectionCommitStep, fence: &Fence) {
        match step {
            CollectionCommitStep::AfterLanceCommit => {
                crate::failpoint!("collection.index.after_lance_commit");
            }
            CollectionCommitStep::AfterCas => {
                crate::failpoint!("collection.index.after_cas");
            }
            _ => {}
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.source.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    /// The collection, if it still exists in its namespace.
    async fn collection(&self) -> Result<Option<Collection>, CollectionError> {
        let (ns, cid) = (self.ns, self.cid);
        Ok(self
            .ctx()
            .meta
            .read(Consistency::Local, |s| {
                s.collection(cid).filter(|c| c.namespace == ns).cloned()
            })
            .await?)
    }

    /// The live manifest's version (`Linearizable`; 0 before the first
    /// commit), and the manifest with its Lance version once it has data.
    async fn base(&self) -> Result<(u64, Option<Base>), CollectionError> {
        let ctx = self.ctx();
        let live = live_manifest(
            &ctx.meta,
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

    /// Plans the work and does the first item that is neither deferred nor
    /// failing: one item's deferral or failure never blocks the items after
    /// it. A run where every buildable item failed returns the first
    /// failure; `Fenced` ends the run at once.
    async fn run_inner(&self, fence: &Fence) -> Result<Ran, TaskError> {
        let Some(collection) = self.collection().await.map_err(failed)? else {
            return Ok((TaskOutcome::Idle, 0));
        };
        let (version, base) = self.base().await.map_err(failed)?;
        let Some(base) = base else {
            // No data yet: idle at the live version, so the collection is
            // proposed again only when it moves or the poll interval passes.
            return Ok((TaskOutcome::Idle, version));
        };
        let indices = base
            .dataset
            .load_indices()
            .await
            .map_err(|e| failed(e.into()))?;
        let work = plan_index_work(
            &collection.schema,
            &base.manifest,
            &base.dataset,
            &indices,
            &self.ctx().config,
        );
        let mut first_failure = None;
        for (done, item) in work.iter().enumerate() {
            let prepared = self.prepare(&collection.schema, &base.dataset, item).await;
            let result = match prepared {
                Ok(None) => continue,
                Ok(Some(build)) => {
                    self.build(&collection.schema, base.clone(), build, fence)
                        .await
                }
                Err(err) => Err(failed(err)),
            };
            match result {
                Ok(version) => {
                    let outcome = match done + 1 < work.len() {
                        true => TaskOutcome::MoreWork,
                        false => TaskOutcome::Idle,
                    };
                    return Ok((outcome, version));
                }
                Err(TaskError::Fenced) => return Err(TaskError::Fenced),
                Err(err) => {
                    tracing::warn!(
                        collection = %self.cid,
                        work = ?item,
                        %err,
                        "an index build failed; trying the next index"
                    );
                    first_failure.get_or_insert(err);
                }
            }
        }
        match first_failure {
            Some(err) => Err(err),
            None => Ok((TaskOutcome::Idle, base.manifest.version)),
        }
    }

    /// The build `item` needs, or `None` to defer it: a vector whose rows
    /// being indexed have fewer vectors than its index can be trained on
    /// ([`training_minimum`]).
    async fn prepare(
        &self,
        schema: &CollectionSchema,
        dataset: &Dataset,
        item: &IndexWork,
    ) -> Result<Option<Build>, CollectionError> {
        let (vector, fragments) = match item {
            IndexWork::PkBtree => {
                return Ok(Some(Build {
                    name: PK_INDEX_NAME.to_string(),
                    column: PK_COLUMN.to_string(),
                    index_type: IndexType::BTree,
                    params: Params::Scalar(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
                    fragments: None,
                    built: Built::Pk,
                }));
            }
            IndexWork::VectorFull { vector } => (*vector, None),
            IndexWork::VectorDelta { vector, fragments } => (*vector, Some(fragments.clone())),
        };
        let spec = schema.vectors.get(vector).ok_or_else(|| {
            CollectionError::Internal(format!("index work for missing vector {vector}"))
        })?;
        let column = vector_column(vector);
        let rows = rows_with(dataset, &column, fragments.as_deref()).await?;
        let needed = training_minimum(spec, rows);
        if rows < needed {
            tracing::debug!(
                collection = %self.cid,
                vector = %spec.name,
                rows,
                needed,
                "too few rows with the vector to train an index; deferred"
            );
            return Ok(None);
        }
        let (Some(kind), Some(params)) = (effective_kind(spec), vector_params(spec, rows)) else {
            return Ok(None);
        };
        Ok(Some(Build {
            name: vector_index_name(vector),
            column,
            index_type: IndexType::Vector,
            params: Params::Vector(params),
            fragments,
            built: Built::Vector { vector, kind },
        }))
    }

    /// Builds `build` on `base`'s Lance version and commits it (rule 4),
    /// rebasing onto the live manifest on a `Conflict`; returns the new
    /// manifest version.
    async fn build(
        &self,
        schema: &CollectionSchema,
        mut base: Base,
        build: Build,
        fence: &Fence,
    ) -> Result<u64, TaskError> {
        let ctx = self.ctx();
        let started = ctx.meta.now_ms();
        let mut dataset = (*base.dataset).clone();
        let mut builder = CreateIndexBuilder::new(
            &mut dataset,
            &[build.column.as_str()],
            build.index_type,
            build.params.as_dyn(),
        )
        .name(build.name.clone())
        // Lance refuses to build under a name in use without `replace`; it
        // removes nothing itself: `removed` below says what this build
        // replaces.
        .replace(true);
        if let Some(fragments) = &build.fragments {
            builder = builder.fragments(fragments.clone());
        }
        let index = builder
            .execute_uncommitted()
            .await
            .map_err(|e| failed(e.into()))?;
        tracing::info!(
            collection = %self.cid,
            index = %build.name,
            uuid = %index.uuid,
            full = build.fragments.is_none(),
            "built an index"
        );
        let staged = Staged {
            index,
            indexed_upto: base.dataset.manifest.next_row_id,
            built: build.built,
            started,
        };
        for rebase in 0..=MAX_REBASES {
            let removed = match build.fragments {
                Some(_) => Vec::new(),
                None => base
                    .dataset
                    .load_indices()
                    .await
                    .map_err(|e| failed(e.into()))?
                    .iter()
                    .filter(|existing| existing.name == build.name)
                    .cloned()
                    .collect(),
            };
            let operation = Operation::CreateIndex {
                new_indices: vec![staged.index.clone()],
                removed_indices: removed,
            };
            let committed = LanceCommitter::commit(&ctx.lance, &base.dataset, operation)
                .await
                .map_err(failed)?;
            self.step(CollectionCommitStep::AfterLanceCommit, fence)
                .await;
            let manifest = self
                .manifest(schema, &base, &committed, &staged)
                .await
                .map_err(failed)?;
            let path = put_manifest(ctx, self.ns, &manifest)
                .await
                .map_err(failed)?;
            let cas = PointerCas {
                ns: self.ns,
                cid: self.cid,
                parent_version: base.manifest.version,
                path: &path,
                started,
                max_age: ctx.config.index_commit_delay,
            };
            match cas.run(ctx, fence).await {
                Ok(version) => {
                    self.step(CollectionCommitStep::AfterCas, fence).await;
                    tracing::info!(
                        collection = %self.cid,
                        index = %build.name,
                        version,
                        rebases = rebase,
                        "committed an index"
                    );
                    return Ok(version);
                }
                Err(CommitError::Conflict) => {
                    tracing::info!(
                        collection = %self.cid,
                        index = %build.name,
                        parent = base.manifest.version,
                        "an index commit conflicted; rebasing onto the live manifest"
                    );
                    base = self.base().await.map_err(failed)?.1.ok_or_else(|| {
                        failed(CollectionError::Corrupt(format!(
                            "collection {} lost its lance dataset",
                            self.cid
                        )))
                    })?;
                }
                Err(CommitError::Fenced) => return Err(TaskError::Fenced),
                Err(CommitError::Other(err)) => return Err(TaskError::failed(err)),
            }
        }
        Err(failed(CollectionError::Blocked(format!(
            "the {} index commit of collection {} conflicted {} times",
            build.name,
            self.cid,
            MAX_REBASES + 1
        ))))
    }

    /// Manifest v+1: `base`'s, at the `committed` Lance version, with the
    /// index entries recomputed from its indexes (rule 4.5). This commit's
    /// PK delta and dead letters are none (they belong to the commit that
    /// wrote them, controller ruling P14).
    async fn manifest(
        &self,
        schema: &CollectionSchema,
        base: &Base,
        committed: &Dataset,
        staged: &Staged,
    ) -> Result<CollectionManifest, CollectionError> {
        let Staged {
            index,
            indexed_upto,
            built,
            started,
        } = staged;
        let (indexed_upto, built, started) = (*indexed_upto, *built, *started);
        let parent = &base.manifest;
        let known_vectors: BTreeMap<&str, &VectorIndexRef> = parent
            .vector_indexes
            .iter()
            .map(|v| (v.lance_index_uuid.as_str(), v))
            .collect();
        let known_scalars: BTreeMap<&str, &ScalarIndexRef> = parent
            .scalar_indexes
            .iter()
            .map(|s| (s.lance_index_uuid.as_str(), s))
            .collect();
        let unknown = |uuid: &str, name: &str| {
            CollectionError::Corrupt(format!(
                "lance version {} of collection {} has index {name} ({uuid}), which manifest version {} does not name",
                committed.manifest.version, self.cid, parent.version
            ))
        };
        let mut vector_indexes = Vec::new();
        let mut scalar_indexes = Vec::new();
        for existing in committed.load_indices().await?.iter() {
            let uuid = existing.uuid.to_string();
            let is_new = existing.uuid == index.uuid;
            if existing.name == PK_INDEX_NAME {
                let indexed_row_ids_upto = match (is_new, known_scalars.get(uuid.as_str())) {
                    (true, _) => indexed_upto,
                    (false, Some(known)) => known.indexed_row_ids_upto,
                    (false, None) => return Err(unknown(&uuid, &existing.name)),
                };
                scalar_indexes.push(ScalarIndexRef {
                    column: PK_COLUMN.to_string(),
                    lance_index_uuid: uuid,
                    indexed_row_ids_upto,
                    index_name: existing.name.clone(),
                });
                continue;
            }
            let Some(vector) =
                (0..schema.vectors.len()).find(|i| vector_index_name(*i) == existing.name)
            else {
                continue;
            };
            let (indexed_row_ids_upto, kind) =
                match (is_new, built, known_vectors.get(uuid.as_str())) {
                    (true, Built::Vector { kind, .. }, _) => (indexed_upto, kind),
                    (false, _, Some(known)) => (known.indexed_row_ids_upto, known.kind),
                    _ => return Err(unknown(&uuid, &existing.name)),
                };
            vector_indexes.push(VectorIndexRef {
                column: vector_column(vector),
                lance_index_uuid: uuid,
                indexed_row_ids_upto,
                index_name: existing.name.clone(),
                vector: schema.vectors[vector].name.clone(),
                kind,
            });
        }
        vector_indexes.sort_by(|a, b| {
            (&a.index_name, a.indexed_row_ids_upto, &a.lance_index_uuid).cmp(&(
                &b.index_name,
                b.indexed_row_ids_upto,
                &b.lance_index_uuid,
            ))
        });
        if let Built::Vector { vector, .. } = built
            && !vector_indexes
                .iter()
                .any(|v| v.lance_index_uuid == index.uuid.to_string())
        {
            return Err(CollectionError::Internal(format!(
                "the new index of vector {vector} is not in lance version {}",
                committed.manifest.version
            )));
        }
        Ok(CollectionManifest {
            version: parent.version + 1,
            parent_version: parent.version,
            parent_manifest: Some(base.path.clone()),
            created_at_ms: started,
            lance_version: committed.manifest.version,
            vector_indexes,
            scalar_indexes,
            kind: CommitKind::IndexBuild,
            pk_delta: None,
            dead_letters: None,
            ..(**parent).clone()
        })
    }
}
