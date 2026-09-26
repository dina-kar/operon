//! `TantivySearchExec` (plan M1.2 Task 5 rules 4–6): BM25 search over every
//! split of the view plus the tail, with global live-only statistics
//! (Ruling 2), masking deleted and shadowed docs before any top-k cut.
//!
//! Score mode prunes with block-max WAND per index. WAND sums a doc's term
//! scores in an order that depends on the postings around it, so every
//! candidate is rescored with the query's plain scorer (the clause order),
//! and hits are equal whatever the split layout. The pruning threshold sits
//! a relative `SLACK` below the k-th best score so that rescoring cannot
//! move a hit across it, and ties at the k-th score are kept.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
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
use operon_quickwit::query::tokenizers::TokenizerManager;
use roaring::RoaringBitmap;
use tantivy::query::{EnableScoring, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED};

use crate::error::ServiceError;
use crate::exec::order::{EffectiveSort, RankMode};
use crate::exec::schema::{Ranked, ranked_schema, ranked_to_batch};
use crate::exec::{
    Columns, SortField, Unit, blocking, df_error, open_units, plan_properties, sort_fields,
    tantivy_error,
};
use crate::ir::{Query, SortValue};
use crate::read::ReadView;
use crate::text::compile::QueryCompiler;
use crate::text::query_tokenizers;
use crate::text::stats::{GlobalStats, StatsCache, StatsRecorder};

/// The pruning threshold is this fraction of the k-th score below it.
const SLACK: f32 = 1e-4;

/// The pruning threshold of a k-th best score `s`: strictly below `s` by at
/// least the rescoring error of a sum of up to a few hundred terms.
fn floor_of(s: Score) -> Score {
    (s - s.abs() * SLACK).next_down()
}

/// An f32 with a total order.
#[derive(Clone, Copy, PartialEq)]
struct Total(f32);

impl Eq for Total {}

impl PartialOrd for Total {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Total {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

struct Inner {
    view: Arc<ReadView>,
    query: Query,
    filter: Option<Query>,
    k: usize,
    sort: EffectiveSort,
    search_after: Option<Vec<SortValue>>,
    stats: StatsCache,
    parallelism: usize,
}

/// BM25 top-k (score mode) or every match ordered by field values (field
/// mode) of `query ∧ filter` over the view. Output: [`ranked_schema`],
/// ordered by `sort`, at most `k` rows.
pub struct TantivySearchExec {
    inner: Arc<Inner>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for TantivySearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivySearchExec")
            .field("collection", &self.inner.view.collection.name)
            .field("query", &self.inner.query)
            .field("filter", &self.inner.filter)
            .field("k", &self.inner.k)
            .field("sort", &self.inner.sort)
            .finish_non_exhaustive()
    }
}

impl TantivySearchExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        view: Arc<ReadView>,
        query: Query,
        filter: Option<Query>,
        k: usize,
        sort: EffectiveSort,
        search_after: Option<Vec<SortValue>>,
        stats: StatsCache,
        parallelism: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                view,
                query,
                filter,
                k,
                sort,
                search_after,
                stats,
                parallelism: parallelism.max(1),
            }),
            properties: plan_properties(ranked_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Matches of `query ∧ filter` over the view (rule 6).
    pub async fn count(&self) -> Result<u64, ServiceError> {
        self.inner.count(&self.metrics).await
    }

    /// The hits: at most `k`, ordered by the effective sort.
    pub async fn search(&self) -> Result<Vec<Ranked>, ServiceError> {
        self.inner.search(&self.metrics).await
    }
}

impl Inner {
    fn compiler_for<'a>(
        &'a self,
        tokenizers: &'a TokenizerManager,
    ) -> impl Fn(&tantivy::schema::Schema) -> Result<crate::text::CompiledQuery, ServiceError>
    + Send
    + Sync
    + 'a {
        move |schema| {
            QueryCompiler::new(&self.view.collection.schema, schema, tokenizers)
                .compile_with_filter(&self.query, self.filter.as_ref())
        }
    }

    async fn count(&self, metrics: &ExecutionPlanMetricsSet) -> Result<u64, ServiceError> {
        if matches!(self.query, Query::MatchAll) && self.filter.is_none() {
            return Ok(self.view.live_rows());
        }
        let tokenizers = query_tokenizers();
        let compile = self.compiler_for(&tokenizers);
        let (splits, units) = open_units(&self.view, &compile, &[], self.parallelism).await?;
        drop(splits);
        MetricBuilder::new(metrics)
            .counter("splits_searched", 0)
            .add(units.iter().filter(|u| !u.is_tail).count());
        let counts = futures::stream::iter(units)
            .map(|unit| blocking(move || count_unit(unit)))
            .buffered(self.parallelism)
            .collect::<Vec<_>>()
            .await;
        counts.into_iter().sum()
    }

    async fn search(&self, metrics: &ExecutionPlanMetricsSet) -> Result<Vec<Ranked>, ServiceError> {
        let timer = MetricBuilder::new(metrics).elapsed_compute(0);
        let started = std::time::Instant::now();
        if let Some(after) = &self.search_after {
            self.sort.check_search_after(after)?;
        }
        let fields = sort_fields(&self.view.collection.schema, &self.sort)?;
        if self.k == 0 {
            return Ok(Vec::new());
        }
        let tokenizers = query_tokenizers();
        let compile = self.compiler_for(&tokenizers);
        let (splits, units) = open_units(&self.view, &compile, &fields, self.parallelism).await?;
        MetricBuilder::new(metrics)
            .counter("splits_searched", 0)
            .add(splits.len());
        let scored = self.sort.has_score();
        let stats = if scored {
            // The fields and terms the BM25 weights ask for.
            let mut wanted_fields = BTreeSet::new();
            let mut wanted_terms = BTreeSet::new();
            for unit in &units {
                let recorder = StatsRecorder::new(unit.searcher.schema());
                unit.query
                    .weight(EnableScoring::enabled_from_statistics_provider(
                        &recorder,
                        &unit.searcher,
                    ))
                    .map_err(tantivy_error)?;
                wanted_fields.extend(recorder.fields.into_inner());
                wanted_terms.extend(recorder.terms.into_inner());
            }
            let stats = GlobalStats::compute(
                &self.view,
                &splits,
                &wanted_terms,
                &wanted_fields,
                &self.stats,
                self.parallelism,
            )
            .await?;
            if stats.num_docs == 0 {
                return Ok(Vec::new());
            }
            Some(Arc::new(stats))
        } else {
            None
        };
        drop(splits);
        let job = Arc::new(Job {
            k: self.k,
            sort: self.sort.clone(),
            search_after: self.search_after.clone(),
            fields,
            stats,
        });
        let results = futures::stream::iter(units)
            .map(|unit| {
                let job = job.clone();
                blocking(move || job.search_unit(unit))
            })
            .buffered(self.parallelism)
            .collect::<Vec<_>>()
            .await;
        let mut hits = Vec::new();
        for result in results {
            hits.extend(result?);
        }
        hits.sort_by(|a, b| self.sort.compare(a, b));
        hits.truncate(self.k);
        timer.add_elapsed(started);
        Ok(hits)
    }
}

/// Matches of one index minus its masked docs.
fn count_unit(unit: Unit) -> Result<u64, ServiceError> {
    let weight = unit
        .query
        .weight(EnableScoring::disabled_from_searcher(&unit.searcher))
        .map_err(tantivy_error)?;
    let mut total = 0u64;
    for (segment, reader) in unit.searcher.segment_readers().iter().enumerate() {
        let masked = &unit.masks[segment];
        weight
            .for_each_no_score(reader, &mut |docs| {
                total += docs.iter().filter(|doc| !masked.contains(**doc)).count() as u64;
            })
            .map_err(tantivy_error)?;
    }
    Ok(total)
}

/// What every index of one search shares.
struct Job {
    k: usize,
    sort: EffectiveSort,
    search_after: Option<Vec<SortValue>>,
    fields: Vec<SortField>,
    stats: Option<Arc<GlobalStats>>,
}

impl Job {
    fn search_unit(&self, unit: Unit) -> Result<Vec<Ranked>, ServiceError> {
        let schema = unit.searcher.schema();
        let weight = match &self.stats {
            Some(stats) => {
                let provider = stats.provider(schema);
                unit.query
                    .weight(EnableScoring::enabled_from_statistics_provider(
                        &provider,
                        &unit.searcher,
                    ))
            }
            None => unit
                .query
                .weight(EnableScoring::disabled_from_searcher(&unit.searcher)),
        }
        .map_err(tantivy_error)?;
        let mut hits = Vec::new();
        for (segment, reader) in unit.searcher.segment_readers().iter().enumerate() {
            let masked = &unit.masks[segment];
            let columns = Columns::open(reader, &self.fields)?;
            let found = match self.sort.mode {
                RankMode::Score => self.score_segment(weight.as_ref(), reader, masked, &columns)?,
                RankMode::Field => self.field_segment(weight.as_ref(), reader, masked, &columns)?,
            };
            hits.extend(found);
        }
        hits.sort_by(|a, b| self.sort.compare(a, b));
        hits.truncate(self.k);
        Ok(hits)
    }

    /// Whether the doc may come after `search_after` whatever its exact
    /// score within the rescoring error.
    fn maybe_after(
        &self,
        columns: &Columns,
        doc: DocId,
        score: Score,
        after: &[SortValue],
    ) -> Result<bool, ServiceError> {
        let mut hit = columns.ranked(doc, score)?;
        let delta = score.abs() * SLACK * 2.0;
        hit.score = score - delta;
        if self.sort.is_after(&hit, after) {
            return Ok(true);
        }
        hit.score = score + delta;
        Ok(self.sort.is_after(&hit, after))
    }

    /// Rule 4: block-max WAND top-k of one segment, keeping every candidate
    /// at or above the threshold, then exact rescoring.
    fn score_segment(
        &self,
        weight: &dyn Weight,
        reader: &SegmentReader,
        masked: &RoaringBitmap,
        columns: &Columns,
    ) -> Result<Vec<Ranked>, ServiceError> {
        let k = self.k;
        let mut top: BinaryHeap<Reverse<Total>> = BinaryHeap::with_capacity(k + 1);
        let mut candidates: Vec<(DocId, Score)> = Vec::new();
        let mut floor = f32::MIN;
        let mut failed = None;
        weight
            .for_each_pruning(f32::MIN, reader, &mut |doc, score| {
                if masked.contains(doc) || score < floor {
                    return floor;
                }
                if let Some(after) = &self.search_after {
                    match self.maybe_after(columns, doc, score, after) {
                        Ok(true) => {}
                        Ok(false) => return floor,
                        Err(err) => {
                            failed = Some(err);
                            return f32::MAX;
                        }
                    }
                }
                candidates.push((doc, score));
                top.push(Reverse(Total(score)));
                if top.len() > k {
                    top.pop();
                }
                if top.len() == k
                    && let Some(Reverse(Total(kth))) = top.peek()
                {
                    floor = floor_of(*kth);
                }
                if candidates.len() > 4 * k + 64 {
                    candidates.retain(|(_, score)| *score >= floor);
                }
                floor
            })
            .map_err(tantivy_error)?;
        if let Some(err) = failed {
            return Err(err);
        }
        candidates.retain(|(_, score)| *score >= floor);
        candidates.sort_unstable_by_key(|(doc, _)| *doc);
        // Exact scores in the query's clause order.
        let mut scorer = weight.scorer(reader, 1.0).map_err(tantivy_error)?;
        let mut hits = Vec::with_capacity(candidates.len());
        for (doc, pruned) in candidates {
            let current = scorer.doc();
            let score = if current != TERMINATED && current <= doc && scorer.seek(doc) == doc {
                scorer.score()
            } else {
                pruned
            };
            let hit = columns.ranked(doc, score)?;
            if let Some(after) = &self.search_after
                && !self.sort.is_after(&hit, after)
            {
                continue;
            }
            hits.push(hit);
        }
        hits.sort_by(|a, b| self.sort.compare(a, b));
        hits.truncate(k);
        Ok(hits)
    }

    /// Rule 5: every match, ordered by the sort keys' fast-field values, in
    /// a bounded buffer of the best `k`.
    fn field_segment(
        &self,
        weight: &dyn Weight,
        reader: &SegmentReader,
        masked: &RoaringBitmap,
        columns: &Columns,
    ) -> Result<Vec<Ranked>, ServiceError> {
        let k = self.k;
        let mut best: Vec<Ranked> = Vec::new();
        let mut worst: Option<Ranked> = None;
        let mut failed: Option<ServiceError> = None;
        let mut visit = |doc: DocId, score: Score| -> Result<(), ServiceError> {
            if masked.contains(doc) {
                return Ok(());
            }
            let sort = columns.sort_values(doc);
            if let Some(worst) = &worst {
                let probe = Ranked {
                    row_id: 0,
                    pk: worst.pk.clone(),
                    score,
                    sort: sort.clone(),
                };
                // Worse than the k-th even before the PK tie-break.
                if self.sort.compare_ignoring_pk(&probe, worst) == Ordering::Greater {
                    return Ok(());
                }
            }
            let hit = Ranked {
                row_id: columns.row_id(doc)?,
                pk: columns.pk(doc)?,
                score,
                sort,
            };
            if let Some(after) = &self.search_after
                && !self.sort.is_after(&hit, after)
            {
                return Ok(());
            }
            best.push(hit);
            if best.len() >= 2 * k.max(1) {
                best.sort_by(|a, b| self.sort.compare(a, b));
                best.truncate(k);
                worst = best.last().cloned();
            }
            Ok(())
        };
        if self.sort.has_score() {
            weight
                .for_each(reader, &mut |doc, score| {
                    if failed.is_none()
                        && let Err(err) = visit(doc, score)
                    {
                        failed = Some(err);
                    }
                })
                .map_err(tantivy_error)?;
        } else {
            weight
                .for_each_no_score(reader, &mut |docs| {
                    for doc in docs {
                        if failed.is_none()
                            && let Err(err) = visit(*doc, 0.0)
                        {
                            failed = Some(err);
                        }
                    }
                })
                .map_err(tantivy_error)?;
        }
        if let Some(err) = failed {
            return Err(err);
        }
        best.sort_by(|a, b| self.sort.compare(a, b));
        best.truncate(k);
        Ok(best)
    }
}

impl DisplayAs for TantivySearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TantivySearchExec: collection={}, k={}, mode={:?}, query={:?}, filter={:?}",
            self.inner.view.collection.name,
            self.inner.k,
            self.inner.sort.mode,
            self.inner.query,
            self.inner.filter
        )
    }
}

impl ExecutionPlan for TantivySearchExec {
    fn name(&self) -> &str {
        "TantivySearchExec"
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
                "TantivySearchExec has no children".to_string(),
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
                "TantivySearchExec has one partition, not {partition}"
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
