//! `AnnExec` (plan M1.2 Task 6 rules 1–6, 8): dense vector search over the
//! manifest's Lance version, the tail and the hot tier.
//!
//! The strategy is chosen per request (rule 4): exact brute force, brute
//! force over a filter's few rows, the hot tier's artifact, or Lance's index
//! with the filter applied before (prefilter) or after (postfilter) it.
//! Whatever produced a candidate, its score is recomputed by
//! [`vector::score`] from the stored vector (Ruling 3, R12): Lance's
//! `_distance` and a `HotAnn`'s scores are never returned. The tail is
//! always searched by brute force, and shadowed rows never leave the
//! operator.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{Array, AsArray, Float32Array, RecordBatch};
use datafusion::arrow::datatypes::{Float32Type, SchemaRef, UInt64Type};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::scanner::{RowAddrMask, RowAddrTreeMap};
use lance::dataset::{ProjectionRequest, ROW_ID};
// Lance's internal extension (no API stability; `lance` is pinned at =12.0.0).
use lance::index::DatasetIndexInternalExt;
use lance_linalg::distance::DistanceType;
use operon_collection::{
    CollectionError, Distance, PK_COLUMN, PrimaryKey, VectorSpec, vector_column,
};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::error::ServiceError;
use crate::exec::filter_bitmap::FilterBitmapExec;
use crate::exec::mask::RowSet;
use crate::exec::schema::{Ranked, ranked_schema, ranked_to_batch};
use crate::exec::{df_error, plan_properties};
use crate::hot::{HotAnn, HotKind};
use crate::ir::AnnParams;
use crate::read::{ReadView, collection_error};
use crate::tail::TAIL_ROWID_BASE;
use crate::vector::{AnnConfig, AnnStrategy, score};

fn lance_error(err: lance::Error) -> ServiceError {
    collection_error(CollectionError::from(err))
}

/// A scored row of the view.
#[derive(Clone, Debug)]
pub(crate) struct Scored {
    pub row_id: u64,
    pub pk: PrimaryKey,
    pub score: f32,
}

/// The output order: score descending, then PK ascending (R10).
fn rank(a: &Scored, b: &Scored) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then_with(|| a.pk.cmp(&b.pk))
        .then_with(|| a.row_id.cmp(&b.row_id))
}

/// A heap entry whose greatest element is the worst hit.
struct Worst(Scored);

impl PartialEq for Worst {
    fn eq(&self, other: &Self) -> bool {
        rank(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for Worst {}

impl PartialOrd for Worst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Worst {
    fn cmp(&self, other: &Self) -> Ordering {
        rank(&self.0, &other.0)
    }
}

/// The best `k` hits seen, by (score desc, pk asc).
pub(crate) struct TopK {
    k: usize,
    heap: BinaryHeap<Worst>,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k.saturating_add(1).min(1 << 16)),
        }
    }

    /// Whether a hit scoring `score` may enter: `false` only when it is
    /// certainly worse than the current k-th (the key breaks ties).
    pub fn may_admit(&self, score: f32) -> bool {
        match self.heap.peek() {
            _ if self.k == 0 => false,
            Some(worst) if self.heap.len() == self.k => {
                score.total_cmp(&worst.0.score) != Ordering::Less
            }
            _ => true,
        }
    }

    pub fn push(&mut self, hit: Scored) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() == self.k {
            let worst = &self.heap.peek().expect("a full heap").0;
            if rank(&hit, worst) != Ordering::Less {
                return;
            }
        }
        self.heap.push(Worst(hit));
        if self.heap.len() > self.k {
            self.heap.pop();
        }
    }

    pub fn into_sorted(self) -> Vec<Scored> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|w| w.0)
            .collect()
    }
}

/// Merges hit lists into the best `k`.
pub(crate) fn best(k: usize, lists: impl IntoIterator<Item = Vec<Scored>>) -> Vec<Scored> {
    let mut top = TopK::new(k);
    for list in lists {
        for hit in list {
            top.push(hit);
        }
    }
    top.into_sorted()
}

/// Lance's distance of an indexed metric (Manhattan is never indexed).
fn distance_type(distance: Distance) -> Option<DistanceType> {
    match distance {
        Distance::Cosine => Some(DistanceType::Cosine),
        Distance::Dot => Some(DistanceType::Dot),
        Distance::Euclid => Some(DistanceType::L2),
        Distance::Manhattan => None,
    }
}

/// The rows of a Lance batch projected to `_pk`, the vector column and
/// `_rowid`: `(row id, key, vector or None)`.
fn batch_rows(
    batch: &RecordBatch,
    column: &str,
    dim: usize,
    mut each: impl FnMut(u64, PrimaryKey, Option<&[f32]>) -> Result<(), ServiceError>,
) -> Result<(), ServiceError> {
    let missing = |name: &str| ServiceError::Internal(format!("a Lance batch lacks {name}"));
    let row_ids = batch
        .column_by_name(ROW_ID)
        .ok_or_else(|| missing(ROW_ID))?
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| ServiceError::Internal(format!("{ROW_ID} is not UInt64")))?;
    let pks = batch
        .column_by_name(PK_COLUMN)
        .ok_or_else(|| missing(PK_COLUMN))?
        .as_binary_opt::<i32>()
        .ok_or_else(|| ServiceError::Internal(format!("{PK_COLUMN} is not Binary")))?;
    let vectors = batch
        .column_by_name(column)
        .ok_or_else(|| missing(column))?
        .as_fixed_size_list_opt()
        .ok_or_else(|| ServiceError::Internal(format!("{column} is not a FixedSizeList")))?;
    let values: &Float32Array = vectors
        .values()
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| ServiceError::Internal(format!("{column} is not a list of Float32")))?;
    if vectors.value_length() as usize != dim {
        return Err(ServiceError::Internal(format!(
            "{column} has {} dimensions, the schema {dim}",
            vectors.value_length()
        )));
    }
    let values = values.values();
    for row in 0..batch.num_rows() {
        let pk = PrimaryKey::from_canonical(pks.value(row))
            .map_err(|err| ServiceError::Internal(format!("{PK_COLUMN}: {err}")))?;
        let vector = if vectors.is_null(row) {
            None
        } else {
            let start = vectors.value_offset(row) as usize;
            values.get(start..start + dim)
        };
        each(row_ids.value(row), pk, vector)?;
    }
    Ok(())
}

/// What a Lance search is restricted to.
enum Mask<'a> {
    Allow(&'a RoaringTreemap),
    Block(&'a RoaringTreemap),
}

impl Mask<'_> {
    fn lance(&self) -> Option<RowAddrMask> {
        match self {
            Mask::Allow(rows) => Some(RowAddrMask::from_allowed(RowAddrTreeMap::from(
                (*rows).clone(),
            ))),
            Mask::Block(rows) if rows.is_empty() => None,
            Mask::Block(rows) => Some(RowAddrMask::from_block(RowAddrTreeMap::from(
                (*rows).clone(),
            ))),
        }
    }

    fn admits(&self, row: u64) -> bool {
        match self {
            Mask::Allow(rows) => rows.contains(row),
            Mask::Block(rows) => !rows.contains(row),
        }
    }
}

struct Inner {
    view: Arc<ReadView>,
    field: String,
    query: Vec<f32>,
    k: usize,
    params: AnnParams,
    allow: Option<Arc<FilterBitmapExec>>,
    config: AnnConfig,
    strategy: Mutex<Option<AnnStrategy>>,
}

/// Dense vector top-k of `field` over the view, restricted to `allow` (a
/// [`FilterBitmapExec`]) when given. Output: [`ranked_schema`], ordered by
/// (score desc, pk asc), at most `k` rows.
pub struct AnnExec {
    inner: Arc<Inner>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for AnnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnnExec")
            .field("collection", &self.inner.view.collection.name)
            .field("field", &self.inner.field)
            .field("k", &self.inner.k)
            .field("params", &self.inner.params)
            .field("filtered", &self.inner.allow.is_some())
            .finish_non_exhaustive()
    }
}

impl AnnExec {
    pub fn new(
        view: Arc<ReadView>,
        field: String,
        query: Vec<f32>,
        k: usize,
        params: AnnParams,
        allow: Option<Arc<FilterBitmapExec>>,
        config: AnnConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                view,
                field,
                query,
                k,
                params,
                allow,
                config,
                strategy: Mutex::new(None),
            }),
            properties: plan_properties(ranked_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The strategy of the last execution; `None` before one (tests and
    /// metrics).
    pub fn strategy(&self) -> Option<AnnStrategy> {
        *self
            .inner
            .strategy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The hits: at most `k`, by (score desc, pk asc).
    pub async fn search(&self) -> Result<Vec<Ranked>, ServiceError> {
        self.inner.search(&self.metrics).await
    }
}

/// The durable side of one search: the manifest's Lance version and what
/// every Lance search of the request shares.
struct Durable<'a> {
    view: &'a ReadView,
    dataset: Option<&'a Arc<Dataset>>,
    column: String,
    dim: usize,
    metric: Distance,
    query: &'a [f32],
    params: &'a AnnParams,
    config: &'a AnnConfig,
    spec: &'a VectorSpec,
    retries: Count,
}

impl Durable<'_> {
    /// The Lance version has the vector column (a vector added after the
    /// first commit may be absent, M1.1 Ruling 5).
    fn dataset(&self) -> Option<&Arc<Dataset>> {
        self.dataset
            .filter(|dataset| dataset.schema().field(&self.column).is_some())
    }

    fn shadow(&self) -> &RoaringTreemap {
        self.view.tail.shadow()
    }

    fn scored(&self, row_id: u64, pk: PrimaryKey, vector: &[f32]) -> Scored {
        Scored {
            row_id,
            pk,
            score: score(self.metric, self.query, vector),
        }
    }

    /// Brute force over every durable row with the vector (`allowed`: only
    /// those rows), the best `k` (rules 4.1–4.2).
    async fn scan(
        &self,
        allowed: Option<&RoaringTreemap>,
        k: usize,
    ) -> Result<Vec<Scored>, ServiceError> {
        let Some(dataset) = self.dataset() else {
            return Ok(Vec::new());
        };
        if allowed.is_some_and(RoaringTreemap::is_empty) || k == 0 {
            return Ok(Vec::new());
        }
        let mask = match allowed {
            Some(rows) => Mask::Allow(rows),
            None => Mask::Block(self.shadow()),
        };
        let mut scanner = dataset.scan();
        scanner
            .project(&[PK_COLUMN, self.column.as_str()])
            .map_err(lance_error)?
            .with_row_id()
            .use_index(false)
            .batch_size(self.config.scan_batch_rows.max(1));
        if let Some(mask) = mask.lance() {
            scanner.with_row_addr_prefilter(mask);
        }
        let mut stream = scanner.try_into_stream().await.map_err(lance_error)?;
        let mut top = TopK::new(k);
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(lance_error)?;
            batch_rows(&batch, &self.column, self.dim, |row_id, pk, vector| {
                if let Some(vector) = vector
                    && mask.admits(row_id)
                    && !self.shadow().contains(row_id)
                {
                    top.push(self.scored(row_id, pk, vector));
                }
                Ok(())
            })?;
        }
        Ok(top.into_sorted())
    }

    /// The default nprobes of the vector's index (rule 8): the largest
    /// segment's partition count, read from the Lance index.
    async fn nprobes(&self, dataset: &Dataset, index_name: &str) -> usize {
        if let Some(n) = self.params.nprobes {
            return n as usize;
        }
        let partitions = match dataset
            .open_logical_vector_index(&self.column, index_name)
            .await
        {
            Ok(index) => match index.as_ivf() {
                Ok(ivf) => ivf
                    .num_partitions_per_segment()
                    .into_iter()
                    .map(|(_, n)| n)
                    .max()
                    .unwrap_or(0),
                Err(err) => {
                    tracing::warn!(index = index_name, %err, "ann: the vector index is not IVF");
                    0
                }
            },
            Err(err) => {
                tracing::warn!(index = index_name, %err, "ann: opening the vector index");
                0
            }
        };
        self.config.default_nprobes(partitions)
    }

    /// One Lance `nearest` search of `k` restricted by `mask`; the rescored
    /// candidates the mask admits, and how many rows Lance returned.
    async fn nearest(
        &self,
        index_name: &str,
        k: usize,
        mask: &Mask<'_>,
        prefilter: bool,
    ) -> Result<(Vec<Scored>, usize), ServiceError> {
        let Some(dataset) = self.dataset() else {
            return Ok((Vec::new(), 0));
        };
        let Some(metric) = distance_type(self.metric) else {
            return Err(ServiceError::Internal(format!(
                "{:?} has no index search",
                self.metric
            )));
        };
        if k == 0 {
            return Ok((Vec::new(), 0));
        }
        let nprobes = self.nprobes(dataset, index_name).await;
        let query = Float32Array::from(self.query.to_vec());
        let mut scanner = dataset.scan();
        scanner
            .nearest(&self.column, &query, k)
            .map_err(lance_error)?
            .nprobes(nprobes)
            .refine(
                self.params
                    .refine_factor
                    .unwrap_or(self.config.default_refine_factor),
            )
            .distance_metric(metric);
        if let Some(ef) = self.params.ef {
            scanner.ef(ef as usize);
        }
        if let Some(lance_mask) = mask.lance() {
            scanner.with_row_addr_prefilter(lance_mask);
        }
        scanner
            .prefilter(prefilter)
            .project(&[PK_COLUMN, self.column.as_str()])
            .map_err(lance_error)?
            .with_row_id();
        let mut stream = scanner.try_into_stream().await.map_err(lance_error)?;
        let mut returned = 0;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(lance_error)?;
            returned += batch.num_rows();
            batch_rows(&batch, &self.column, self.dim, |row_id, pk, vector| {
                if let Some(vector) = vector
                    && mask.admits(row_id)
                    && !self.shadow().contains(row_id)
                {
                    out.push(self.scored(row_id, pk, vector));
                }
                Ok(())
            })?;
        }
        Ok((out, returned))
    }

    /// Rules 4.4–4.5 over the rows outside `covered`: prefilter or
    /// postfilter with a filter's durable rows `allow_d` (shadowed rows
    /// already removed), else one search blocking the shadow.
    async fn index_search(
        &self,
        index_name: &str,
        k: usize,
        allow_d: Option<&RoaringTreemap>,
        covered: Option<&RoaringTreemap>,
        n_d: u64,
    ) -> Result<(Vec<Scored>, AnnStrategy), ServiceError> {
        let block = match covered {
            Some(covered) => covered | self.shadow(),
            None => self.shadow().clone(),
        };
        let Some(allow_d) = allow_d else {
            let (hits, _) = self
                .nearest(index_name, k, &Mask::Block(&block), false)
                .await?;
            return Ok((hits, AnnStrategy::Postfilter));
        };
        let allowed = match covered {
            Some(covered) => allow_d - covered,
            None => allow_d.clone(),
        };
        let selectivity = if n_d == 0 {
            0.0
        } else {
            allow_d.len() as f64 / n_d as f64
        };
        if allowed.is_empty() {
            return Ok((Vec::new(), AnnStrategy::Prefilter));
        }
        if selectivity > self.config.prefilter_max_selectivity {
            // Postfilter: oversample, keep the allowed rows, retry larger.
            let wanted = (k as f64 * self.config.postfilter_oversample / selectivity).ceil();
            let mut k1 = (wanted as u64).clamp(1, n_d.max(1)) as usize;
            let mut retries = 0;
            loop {
                let (hits, returned) = self
                    .nearest(index_name, k1, &Mask::Block(&block), false)
                    .await?;
                let kept: Vec<Scored> = hits
                    .into_iter()
                    .filter(|hit| allowed.contains(hit.row_id))
                    .collect();
                if kept.len() >= k || returned < k1 {
                    return Ok((kept, AnnStrategy::Postfilter));
                }
                if retries >= self.config.postfilter_retries || k1 as u64 >= n_d {
                    break;
                }
                retries += 1;
                self.retries.add(1);
                k1 = k1.saturating_mul(4).min(n_d as usize);
            }
        }
        // Prefilter: the index searches only the allowed rows; Lance's flat
        // branch covers the fragments the index does not.
        let (hits, _) = self
            .nearest(index_name, k, &Mask::Allow(&allowed), true)
            .await?;
        Ok((hits, AnnStrategy::Prefilter))
    }

    /// The exact scores of durable rows `rows` (live, unshadowed), from one
    /// Lance take of `[_pk, vector]`.
    async fn take(&self, rows: &[u64]) -> Result<Vec<Scored>, ServiceError> {
        let Some(dataset) = self.dataset() else {
            return Ok(Vec::new());
        };
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let projection = dataset
            .schema()
            .project_preserve_system_columns(&[PK_COLUMN, self.column.as_str(), ROW_ID])
            .map_err(lance_error)?;
        let batch = dataset
            .take_rows(rows, ProjectionRequest::from_schema(projection))
            .await
            .map_err(lance_error)?;
        let mut out = Vec::with_capacity(rows.len());
        batch_rows(&batch, &self.column, self.dim, |row_id, pk, vector| {
            if let Some(vector) = vector
                && !self.shadow().contains(row_id)
            {
                out.push(self.scored(row_id, pk, vector));
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// Whether durable row `row_id` is live in the view: in a split of the
    /// manifest, not deleted there, and not shadowed (rule 6.3).
    async fn is_live(
        &self,
        row_id: u64,
        deleted: &mut BTreeMap<usize, Arc<RoaringBitmap>>,
    ) -> Result<bool, ServiceError> {
        if row_id >= TAIL_ROWID_BASE || self.shadow().contains(row_id) {
            return Ok(false);
        }
        let Some((split, doc)) = self.view.snapshot.locate_row(row_id) else {
            return Ok(false);
        };
        let bitmap = match deleted.get(&split) {
            Some(bitmap) => bitmap.clone(),
            None => {
                let Some(split_ref) = self.view.snapshot.splits().get(split) else {
                    return Ok(false);
                };
                let bitmap = self
                    .view
                    .bitmaps
                    .deleted_docs(&self.view.snapshot, split_ref)
                    .await?;
                deleted.insert(split, bitmap.clone());
                bitmap
            }
        };
        Ok(!bitmap.contains(doc))
    }

    /// Rule 6: the hot artifact `hot` over its covered rows, merged with an
    /// index search of the rest. `None` → the durable path serves the whole
    /// search.
    async fn hot(
        &self,
        hot: &dyn HotAnn,
        index_name: &str,
        k: usize,
        allow_d: Option<&RoaringTreemap>,
        n_d: u64,
    ) -> Result<Option<Vec<Scored>>, ServiceError> {
        let mut k2 = k
            .saturating_mul(self.config.hot_overfetch)
            .saturating_add(16);
        let mut retries = 0;
        let mut deleted = BTreeMap::new();
        let live = loop {
            let hits = match hot.search(self.query, k2, allow_d, self.params.ef).await {
                Ok(hits) => hits,
                Err(err) => {
                    tracing::warn!(collection = %self.view.collection.name, column = %self.column, %err, "ann: the hot artifact failed; using the durable path");
                    return Ok(None);
                }
            };
            let returned = hits.len();
            let mut rows: Vec<u64> = hits.into_iter().map(|(row, _)| row).collect();
            rows.sort_unstable();
            rows.dedup();
            let mut live = Vec::with_capacity(rows.len());
            for row in rows {
                if allow_d.is_none_or(|allow| allow.contains(row))
                    && self.is_live(row, &mut deleted).await?
                {
                    live.push(row);
                }
            }
            if live.len() >= k || returned < k2 {
                break live;
            }
            if retries >= self.config.hot_retries {
                return Ok(None);
            }
            retries += 1;
            self.retries.add(1);
            k2 = k2.saturating_mul(4);
        };
        let hot_hits = self.take(&live).await?;
        let (uncovered, _) = self
            .index_search(index_name, k, allow_d, Some(hot.covered()), n_d)
            .await?;
        Ok(Some(best(k, [hot_hits, uncovered])))
    }
}

impl Inner {
    fn record(&self, strategy: AnnStrategy) {
        *self.strategy.lock().unwrap_or_else(PoisonError::into_inner) = Some(strategy);
    }

    /// Rule 2: the vector's position and spec in the schema, and its
    /// metric.
    fn spec(&self) -> Result<(usize, &VectorSpec, Distance), ServiceError> {
        let schema = &self.view.collection.schema;
        let Some((index, spec)) = schema
            .vectors
            .iter()
            .enumerate()
            .find(|(_, spec)| spec.name == self.field)
        else {
            if schema.sparse_vectors.iter().any(|s| s.name == self.field) {
                return Err(ServiceError::InvalidArgument(format!(
                    "{} is a sparse vector",
                    self.field
                )));
            }
            return Err(ServiceError::InvalidArgument(format!(
                "unknown vector {}",
                self.field
            )));
        };
        if self.query.len() != spec.dim as usize {
            return Err(ServiceError::InvalidArgument(format!(
                "vector {} has {} dimensions, the query has {}",
                self.field,
                spec.dim,
                self.query.len()
            )));
        }
        Ok((index, spec, self.params.distance.unwrap_or(spec.distance)))
    }

    /// The tail's live docs with the vector (in `allow_t` when filtered),
    /// by brute force (rule 5).
    fn tail_hits(&self, metric: Distance, allow_t: Option<&RoaringTreemap>) -> Vec<Scored> {
        let mut top = TopK::new(self.k);
        for doc in self.view.tail.live_docs() {
            if allow_t.is_some_and(|allow| !allow.contains(doc.row_id)) {
                continue;
            }
            let Some(vector) = doc.doc.as_ref().and_then(|d| d.vectors.get(&self.field)) else {
                continue;
            };
            if vector.len() != self.query.len() {
                continue;
            }
            top.push(Scored {
                row_id: doc.row_id,
                pk: doc.pk.clone(),
                score: score(metric, &self.query, vector),
            });
        }
        top.into_sorted()
    }

    async fn search(&self, metrics: &ExecutionPlanMetricsSet) -> Result<Vec<Ranked>, ServiceError> {
        let timer = MetricBuilder::new(metrics).elapsed_compute(0);
        let started = std::time::Instant::now();
        let (index, spec, metric) = self.spec()?;
        let k = self.k;
        // 3. The candidate rows: the filter's durable and tail parts.
        let allow = match &self.allow {
            None => None,
            Some(exec) => match exec.rows().await? {
                RowSet::All => None,
                RowSet::Rows(rows) => Some(rows),
            },
        };
        let view = &*self.view;
        let (allow_d, allow_t) = match &allow {
            None => (None, None),
            Some(rows) => {
                let mut durable = rows.clone();
                durable.remove_range(TAIL_ROWID_BASE..);
                let tail = rows - &durable;
                (Some(&durable - view.tail.shadow()), Some(tail))
            }
        };
        let n_d = view.live_rows().saturating_sub(view.tail.live_count());
        let durable = Durable {
            view,
            dataset: view.snapshot.dataset(),
            column: vector_column(index),
            dim: spec.dim as usize,
            metric,
            query: &self.query,
            params: &self.params,
            config: &self.config,
            spec,
            retries: MetricBuilder::new(metrics).counter("retries", 0),
        };
        let manifest = view.snapshot.manifest();
        let index_name = manifest
            .vector_indexes
            .iter()
            .find(|entry| entry.vector == spec.name)
            .map(|entry| entry.index_name.clone());
        let tail_hits = self.tail_hits(metric, allow_t.as_ref());

        // 4. The strategy, first match wins.
        let threshold = self
            .config
            .brute_force_threshold(durable.spec.hnsw.full_scan_threshold_kb, spec.dim);
        let (strategy, durable_hits) = match index_name {
            _ if k == 0 => (AnnStrategy::Exact, Vec::new()),
            None => (AnnStrategy::Exact, durable.scan(allow_d.as_ref(), k).await?),
            Some(_)
                if self.params.exact
                    || self.params.distance.is_some()
                    || spec.distance == Distance::Manhattan =>
            {
                (AnnStrategy::Exact, durable.scan(allow_d.as_ref(), k).await?)
            }
            Some(_)
                if allow_d
                    .as_ref()
                    .is_some_and(|a| a.len() as usize <= threshold) =>
            {
                (
                    AnnStrategy::BruteForceAllowed,
                    durable.scan(allow_d.as_ref(), k).await?,
                )
            }
            Some(index_name) => {
                let hot = view.hot.ann(
                    view.ns,
                    view.collection.id,
                    &durable.column,
                    manifest.version,
                );
                let from_hot = match &hot {
                    Some(hot) => {
                        durable
                            .hot(hot.as_ref(), &index_name, k, allow_d.as_ref(), n_d)
                            .await?
                    }
                    None => None,
                };
                match from_hot {
                    Some(hits) => {
                        view.hot_used.record(HotKind::Hnsw);
                        (AnnStrategy::Hot, hits)
                    }
                    None => {
                        let (hits, strategy) = durable
                            .index_search(&index_name, k, allow_d.as_ref(), None, n_d)
                            .await?;
                        (strategy, hits)
                    }
                }
            }
        };
        // 5. Merge with the tail.
        let hits = best(k, [durable_hits, tail_hits]);
        self.record(strategy);
        timer.add_elapsed(started);
        Ok(hits
            .into_iter()
            .map(|hit| Ranked {
                row_id: hit.row_id,
                pk: hit.pk,
                score: hit.score,
                sort: Vec::new(),
            })
            .collect())
    }
}

impl DisplayAs for AnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AnnExec: collection={}, field={}, k={}, filtered={}",
            self.inner.view.collection.name,
            self.inner.field,
            self.inner.k,
            self.inner.allow.is_some()
        )
    }
}

impl ExecutionPlan for AnnExec {
    fn name(&self) -> &str {
        "AnnExec"
    }

    fn schema(&self) -> SchemaRef {
        ranked_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "AnnExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "AnnExec has one partition, not {partition}"
            )));
        }
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let stream = futures::stream::once(async move {
            let hits = inner.search(&metrics).await.map_err(df_error)?;
            output_rows.add(hits.len());
            Ok(ranked_to_batch(&hits))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ranked_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
