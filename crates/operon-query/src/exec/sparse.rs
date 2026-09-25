//! `SparseExec` (plan M1.2 Task 6 rule 7; Ruling 21, overview A29): exact
//! sparse-vector search over every split of the view and the tail.
//!
//! The candidates are the union of the query indices' postings in
//! `_sparse.<name>`, minus deleted, shadowed and filtered docs. Every
//! candidate is scored exactly from its `_sparse_w.<name>` weights with
//! Qdrant's dot product, the query weighted by live-only IDF for `Idf`
//! fields. Nothing is pruned, so results are equal whatever the split
//! layout and whether the splits come from the hot tier.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use operon_collection::{
    PK_FIELD, PrimaryKey, ROWID_FIELD, SPARSE_PRESENT, SparseModifier, SparseVector,
    SparseVectorSpec, decode_sparse_weights, sparse_postings_field, sparse_weights_field,
};
use operon_quickwit::doc_mapper::{FastFieldWarmupInfo, WarmupInfo};
use roaring::{RoaringBitmap, RoaringTreemap};
use tantivy::schema::{IndexRecordOption, Schema};
use tantivy::{DocSet, Searcher, TERMINATED, Term};

use crate::error::ServiceError;
use crate::exec::ann::{Scored, TopK, best};
use crate::exec::filter_bitmap::FilterBitmapExec;
use crate::exec::mask::RowSet;
use crate::exec::schema::{Ranked, ranked_schema, ranked_to_batch};
use crate::exec::{blocking, df_error, plan_properties, tantivy_error};
use crate::read::ReadView;
use crate::sparse::{SparseStats, sparse_score};
use crate::text::splits::{open_splits_with, tail_segment_masks};
use crate::text::stats::StatsCache;

struct Inner {
    view: Arc<ReadView>,
    field: String,
    query: SparseVector,
    k: usize,
    allow: Option<Arc<FilterBitmapExec>>,
    corpus: Option<Arc<FilterBitmapExec>>,
    stats: StatsCache,
    parallelism: usize,
}

/// Exact sparse-vector top-k of `field` over the view, restricted to
/// `allow` when given, with IDF statistics over `corpus` (default: every
/// live doc). Output: [`ranked_schema`], ordered by (score desc, pk asc),
/// at most `k` rows.
pub struct SparseExec {
    inner: Arc<Inner>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for SparseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SparseExec")
            .field("collection", &self.inner.view.collection.name)
            .field("field", &self.inner.field)
            .field("k", &self.inner.k)
            .field("entries", &self.inner.query.len())
            .field("filtered", &self.inner.allow.is_some())
            .field("corpus", &self.inner.corpus.is_some())
            .finish_non_exhaustive()
    }
}

impl SparseExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        view: Arc<ReadView>,
        field: String,
        query: SparseVector,
        k: usize,
        allow: Option<Arc<FilterBitmapExec>>,
        corpus: Option<Arc<FilterBitmapExec>>,
        stats: StatsCache,
        parallelism: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                view,
                field,
                query,
                k,
                allow,
                corpus,
                stats,
                parallelism: parallelism.max(1),
            }),
            properties: plan_properties(ranked_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The hits: at most `k`, by (score desc, pk asc).
    pub async fn search(&self) -> Result<Vec<Ranked>, ServiceError> {
        self.inner.search(&self.metrics).await
    }
}

/// One searchable index: a split or the tail's RAM index, with its masked
/// docs per segment.
struct SparseUnit {
    searcher: Searcher,
    masks: Vec<RoaringBitmap>,
}

/// What every unit of one search shares.
struct Search {
    postings: String,
    weights: String,
    query: SparseVector,
    allow: Option<RoaringTreemap>,
    k: usize,
}

/// The unmasked docs of one segment that have any of the query's indices,
/// ascending (rule 7.4).
fn candidates(
    reader: &tantivy::SegmentReader,
    field: tantivy::schema::Field,
    indices: &[u32],
    masked: &RoaringBitmap,
) -> Result<RoaringBitmap, ServiceError> {
    let inverted = reader.inverted_index(field).map_err(tantivy_error)?;
    let mut docs = RoaringBitmap::new();
    for index in indices {
        let term = Term::from_field_u64(field, u64::from(*index));
        let Some(mut postings) = inverted
            .read_postings(&term, IndexRecordOption::Basic)
            .map_err(tantivy_error)?
        else {
            continue;
        };
        let mut doc = postings.doc();
        while doc != TERMINATED {
            docs.insert(doc);
            doc = postings.advance();
        }
    }
    docs -= masked;
    Ok(docs)
}

/// The best `k` of one unit (rule 7.5).
fn unit_top(unit: SparseUnit, search: &Search) -> Result<Vec<Scored>, ServiceError> {
    let schema = unit.searcher.schema();
    let Ok(field) = schema.get_field(&search.postings) else {
        // A split written before the vector existed contributes nothing.
        return Ok(Vec::new());
    };
    let mut top = TopK::new(search.k);
    for (segment, reader) in unit.searcher.segment_readers().iter().enumerate() {
        let docs = candidates(reader, field, search.query.indices(), &unit.masks[segment])?;
        if docs.is_empty() {
            continue;
        }
        let fast = reader.fast_fields();
        let Some(weights) = fast.bytes(&search.weights).map_err(tantivy_error)? else {
            continue;
        };
        let rowids = fast.u64(ROWID_FIELD).map_err(tantivy_error)?;
        let pks = fast
            .bytes(PK_FIELD)
            .map_err(tantivy_error)?
            .ok_or_else(|| ServiceError::Internal("a split without _pk".to_string()))?;
        // Every candidate's weights ordinal first, then the ordinals in
        // ascending order in one pass over the dictionary (row 0.62).
        let mut admitted: Vec<(tantivy::DocId, u64, u64)> = Vec::with_capacity(docs.len() as usize);
        for doc in &docs {
            let Some(row_id) = rowids.first(doc) else {
                return Err(ServiceError::Internal(format!("doc {doc} has no _rowid")));
            };
            if search
                .allow
                .as_ref()
                .is_some_and(|allow| !allow.contains(row_id))
            {
                continue;
            }
            if let Some(ord) = weights.term_ords(doc).next() {
                admitted.push((doc, row_id, ord));
            }
        }
        let mut ords: Vec<u64> = admitted.iter().map(|(_, _, ord)| *ord).collect();
        ords.sort_unstable();
        ords.dedup();
        let mut vectors: Vec<SparseVector> = Vec::with_capacity(ords.len());
        let mut failed = None;
        let found = weights
            .dictionary()
            .sorted_ords_to_term_cb(ords.iter().copied(), |bytes| {
                match decode_sparse_weights(bytes) {
                    Ok(vector) => vectors.push(vector),
                    Err(err) => failed = Some(err),
                }
                Ok(())
            })
            .map_err(tantivy_error)?;
        if let Some(err) = failed {
            return Err(ServiceError::Internal(format!("{}: {err}", search.weights)));
        }
        if !found || vectors.len() != ords.len() {
            return Err(ServiceError::Internal(format!(
                "{}: a weights ordinal is missing from its dictionary",
                search.weights
            )));
        }
        for (doc, row_id, ord) in admitted {
            let at = ords
                .binary_search(&ord)
                .expect("every ordinal was resolved");
            let Some(score) = sparse_score(&search.query, &vectors[at]) else {
                continue;
            };
            if !top.may_admit(score) {
                continue;
            }
            let mut bytes = Vec::new();
            let pk_ord = pks
                .term_ords(doc)
                .next()
                .ok_or_else(|| ServiceError::Internal(format!("doc {doc} has no _pk")))?;
            pks.ord_to_bytes(pk_ord, &mut bytes)
                .map_err(tantivy_error)?;
            let pk = PrimaryKey::from_canonical(&bytes)
                .map_err(|err| ServiceError::Internal(format!("_pk of doc {doc}: {err}")))?;
            top.push(Scored { row_id, pk, score });
        }
    }
    Ok(top.into_sorted())
}

impl Inner {
    /// Rule 7.1: the sparse vector's spec.
    fn spec(&self) -> Result<&SparseVectorSpec, ServiceError> {
        let schema = &self.view.collection.schema;
        let Some(spec) = schema
            .sparse_vectors
            .iter()
            .find(|spec| spec.name == self.field)
        else {
            if schema.vectors.iter().any(|v| v.name == self.field) {
                return Err(ServiceError::InvalidArgument(format!(
                    "{} is a dense vector",
                    self.field
                )));
            }
            return Err(ServiceError::InvalidArgument(format!(
                "unknown sparse vector {}",
                self.field
            )));
        };
        if self.corpus.is_some() && spec.modifier != SparseModifier::Idf {
            return Err(ServiceError::InvalidArgument(
                "idf_corpus needs a sparse vector with the idf modifier".to_string(),
            ));
        }
        Ok(spec)
    }

    async fn search(&self, metrics: &ExecutionPlanMetricsSet) -> Result<Vec<Ranked>, ServiceError> {
        let spec = self.spec()?;
        if self.query.is_empty() || self.k == 0 {
            return Ok(Vec::new());
        }
        let timer = MetricBuilder::new(metrics).elapsed_compute(0);
        let started = std::time::Instant::now();
        let view = &*self.view;
        let postings = sparse_postings_field(&self.field);
        let weights = sparse_weights_field(&self.field);
        // 2. The splits, warmed for the query's postings, `SPARSE_PRESENT`
        //    and the columns read.
        let mut values: BTreeSet<u64> =
            self.query.indices().iter().map(|i| u64::from(*i)).collect();
        values.insert(SPARSE_PRESENT);
        let warm = |schema: &Schema| {
            let mut warmup = WarmupInfo::default();
            if let Ok(field) = schema.get_field(&postings) {
                let terms = warmup.terms_grouped_by_field.entry(field).or_default();
                for value in &values {
                    terms.insert(Term::from_field_u64(field, *value), false);
                }
            }
            for name in [ROWID_FIELD, PK_FIELD, weights.as_str()] {
                if schema.get_field(name).is_ok() {
                    warmup.fast_fields.insert(FastFieldWarmupInfo {
                        name: name.to_string(),
                        with_subfields: false,
                    });
                }
            }
            Ok(warmup)
        };
        let splits = open_splits_with(view, &warm, self.parallelism).await?;
        MetricBuilder::new(metrics)
            .counter("splits_searched", 0)
            .add(splits.len());
        let allow = match &self.allow {
            None => None,
            Some(exec) => match exec.rows().await? {
                RowSet::All => None,
                RowSet::Rows(rows) => Some(rows),
            },
        };
        // 3. The IDF-weighted query.
        let query = match spec.modifier {
            SparseModifier::None => self.query.clone(),
            SparseModifier::Idf => {
                let corpus = match &self.corpus {
                    Some(exec) => Some(exec.rows().await?),
                    None => None,
                };
                let stats = SparseStats::compute(
                    view,
                    &splits,
                    &self.field,
                    self.query.indices(),
                    corpus.as_ref(),
                    &self.stats,
                )
                .await?;
                stats.weigh(&self.query)
            }
        };
        // 4–5. Per split, then the tail, the best k; merged in that order.
        let mut units: Vec<SparseUnit> = splits
            .iter()
            .map(|split| SparseUnit {
                searcher: split.searcher.clone(),
                masks: split.segment_masks(),
            })
            .collect();
        drop(splits);
        if let Some(searcher) = view.tail.searcher() {
            units.push(SparseUnit {
                searcher: searcher.clone(),
                masks: tail_segment_masks(searcher, view.tail.live())?,
            });
        }
        let search = Arc::new(Search {
            postings,
            weights,
            query,
            allow,
            k: self.k,
        });
        let parts = futures::stream::iter(units)
            .map(|unit| {
                let search = search.clone();
                blocking(move || unit_top(unit, &search))
            })
            .buffered(self.parallelism)
            .collect::<Vec<_>>()
            .await;
        let parts = parts.into_iter().collect::<Result<Vec<_>, _>>()?;
        let hits = best(self.k, parts);
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

impl DisplayAs for SparseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SparseExec: collection={}, field={}, k={}, filtered={}",
            self.inner.view.collection.name,
            self.inner.field,
            self.inner.k,
            self.inner.allow.is_some()
        )
    }
}

impl ExecutionPlan for SparseExec {
    fn name(&self) -> &str {
        "SparseExec"
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
                "SparseExec has no children".to_string(),
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
                "SparseExec has one partition, not {partition}"
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
