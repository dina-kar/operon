//! The search planner (plan M1.2 Task 7 rules 1–8, 11): plans a
//! [`SearchRequest`] over one read view as a tree of the operators, runs
//! it, and assembles the response; and point reads, counts and scrolls.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use operon_collection::{CollectionSchema, FieldKind, PrimaryKey};

use crate::error::ServiceError;
use crate::exec::aggs::{AggDomain, aggregate};
use crate::exec::ann::AnnExec;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow, fetch_rows};
use crate::exec::filter_bitmap::FilterBitmapExec;
use crate::exec::fusion::{FusionExec, collect_ranked};
use crate::exec::groups::{Group, group, groups_full};
use crate::exec::mask::RowSet;
use crate::exec::order::{EffectiveSort, RankMode};
use crate::exec::project::{fetch_columns, field_values, filter_source, stored_doc};
use crate::exec::schema::{Ranked, ranked_schema, ranked_to_batch};
use crate::exec::scroll::KeyPages;
use crate::exec::sparse::SparseExec;
use crate::exec::tantivy_search::TantivySearchExec;
use crate::exec::{df_error, plan_properties};
use crate::ir::{
    Fusion, Hit, HitGroup, Query, Retriever, SearchRequest, SearchResponse, SortKey, TotalHits,
    TotalRelation, TrackTotalHits,
};
use crate::read::ReadView;
use crate::text::fields::{ResolvedField, resolve_field};
use crate::text::highlight::{check_highlight, highlight, highlight_stats};
use crate::text::pkdict::PkDictCache;
use crate::text::stats::StatsCache;
use crate::types::{Projection, StoredDoc};
use crate::validate::{SearchLimits, validate_request};
use crate::vector::{AnnConfig, score};

/// How searches are planned and bounded.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchConfig {
    pub limits: SearchLimits,
    /// 8: splits opened and searched at a time.
    pub parallelism: usize,
    /// 100 000: a scroll filter with at most this many durable matches
    /// sorts them instead of walking the PK dictionaries.
    pub scroll_sort_threshold: u64,
    /// 256 MiB of warm PK dictionaries.
    pub pk_dict_cache_bytes: u64,
    /// 1 000 000 cached per-split statistics.
    pub stats_cache_entries: u64,
    /// 500 000 000 bytes per aggregation collector (Tantivy's default).
    pub agg_memory_limit: u64,
    /// 65 000 buckets (Tantivy's default).
    pub agg_bucket_limit: u32,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            limits: SearchLimits::default(),
            parallelism: 8,
            scroll_sort_threshold: 100_000,
            pk_dict_cache_bytes: 256 << 20,
            stats_cache_entries: 1_000_000,
            agg_memory_limit: 500_000_000,
            agg_bucket_limit: 65_000,
        }
    }
}

/// A future computing a ranked list, given the task context.
type RankedFn =
    dyn Fn(Arc<TaskContext>) -> BoxFuture<'static, Result<Vec<Ranked>, ServiceError>> + Send + Sync;

/// A ranked list computed by a closure: `Rescore` and the constant-score
/// PK walk. Output: [`ranked_schema`].
struct RankedFnExec {
    name: &'static str,
    children: Vec<Arc<dyn ExecutionPlan>>,
    run: Arc<RankedFn>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for RankedFnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(self.name).finish_non_exhaustive()
    }
}

impl RankedFnExec {
    fn new(name: &'static str, children: Vec<Arc<dyn ExecutionPlan>>, run: Arc<RankedFn>) -> Self {
        Self {
            name,
            children,
            run,
            properties: plan_properties(ranked_schema()),
        }
    }
}

impl DisplayAs for RankedFnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl ExecutionPlan for RankedFnExec {
    fn name(&self) -> &str {
        self.name
    }

    fn schema(&self) -> SchemaRef {
        ranked_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() == self.children.len() {
            // The closure owns its inputs; the children are for display.
            Ok(self)
        } else {
            Err(DataFusionError::Internal(format!(
                "{} has {} children",
                self.name,
                self.children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "{} has one partition, not {partition}",
                self.name
            )));
        }
        let future = (self.run)(context);
        let stream = futures::stream::once(async move {
            let hits = future.await.map_err(df_error)?;
            Ok(ranked_to_batch(&hits))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ranked_schema(),
            stream,
        )))
    }
}

/// `a ∧ b`.
fn and(a: Option<&Query>, b: Option<&Query>) -> Option<Query> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Query::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: vec![a.clone(), b.clone()],
            minimum_should_match: None,
        }),
        (Some(q), None) | (None, Some(q)) => Some(q.clone()),
        (None, None) => None,
    }
}

/// `query ∧ filter` as one query whose matches are the scoring query's.
fn matching(query: &Query, filter: Option<&Query>) -> Query {
    match filter {
        None => query.clone(),
        Some(filter) => Query::Bool {
            must: vec![query.clone()],
            should: Vec::new(),
            must_not: Vec::new(),
            filter: vec![filter.clone()],
            minimum_should_match: None,
        },
    }
}

/// The score every match of `query` gets when it scores no BM25 leaf and
/// no match can score differently from another (rule 11); `None`
/// otherwise. Leaves the compiler always scores 1.0 (Task 2 rule 2) count,
/// as do `constant_score`, `boost` of such a query, and `bool` queries
/// without `should` clauses and with at most one `must` (filter clauses
/// score 0.0, `must_not` none).
fn uniform_score(schema: &CollectionSchema, query: &Query) -> Option<f32> {
    match query {
        Query::MatchAll
        | Query::Terms { .. }
        | Query::Range { .. }
        | Query::Exists { .. }
        | Query::IsNull { .. }
        | Query::IsEmpty { .. }
        | Query::ValuesCount { .. }
        | Query::Ids(_) => Some(1.0),
        Query::Term { field, value } => {
            let scored = match resolve_field(schema, field) {
                ResolvedField::Plain { spec } => {
                    matches!(spec.kind, FieldKind::Text { .. } | FieldKind::Keyword)
                }
                ResolvedField::JsonPath { .. } => matches!(value, crate::ir::FieldValue::Str(_)),
                ResolvedField::Unknown => false,
            };
            (!scored).then_some(1.0)
        }
        Query::ConstantScore { score, .. } => Some(*score),
        Query::Boost { query, boost } => uniform_score(schema, query).map(|s| s * boost),
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => {
            if !should.is_empty() || must.len() > 1 {
                return None;
            }
            match must.first() {
                Some(must) => {
                    let mut s = uniform_score(schema, must)?;
                    for _ in filter {
                        s += 0.0;
                    }
                    Some(s)
                }
                None if filter.is_empty() && must_not.is_empty() => Some(1.0),
                None => Some(0.0),
            }
        }
        _ => None,
    }
}

/// Whether every key of the sort reads the score or is the trailing PK.
fn score_only(sort: &EffectiveSort) -> bool {
    sort.mode == RankMode::Score
        && sort
            .keys
            .iter()
            .all(|key| matches!(key, SortKey::Score { .. } | SortKey::Pk { .. }))
}

/// Plans, runs and assembles searches; serves get, count and scroll.
pub struct SearchPlanner {
    config: SearchConfig,
    ann: AnnConfig,
    stats: StatsCache,
    keys: KeyPages,
}

impl fmt::Debug for SearchPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchPlanner")
            .field("config", &self.config)
            .field("ann", &self.ann)
            .finish_non_exhaustive()
    }
}

/// What planning one request shares.
struct Plan<'a> {
    planner: &'a SearchPlanner,
    view: &'a Arc<ReadView>,
    request: &'a SearchRequest,
    sort: &'a EffectiveSort,
}

impl Plan<'_> {
    fn filter_exec(&self, filter: Option<Query>) -> Option<Arc<FilterBitmapExec>> {
        filter.map(|filter| {
            Arc::new(FilterBitmapExec::new(
                self.view.clone(),
                filter,
                self.planner.config.parallelism,
            ))
        })
    }

    fn text_exec(&self, query: &Query, k: usize) -> TantivySearchExec {
        TantivySearchExec::new(
            self.view.clone(),
            query.clone(),
            self.request.filter.clone(),
            k,
            EffectiveSort::by_score(),
            None,
            self.planner.stats.clone(),
            self.planner.config.parallelism,
        )
    }

    /// `R(r)` (rule 1.2).
    fn retriever(&self, retriever: &Retriever) -> Result<Arc<dyn ExecutionPlan>, ServiceError> {
        let view = self.view;
        let config = &self.planner.config;
        Ok(match retriever {
            Retriever::Vector {
                field,
                query,
                k,
                params,
                filter,
            } => Arc::new(AnnExec::new(
                view.clone(),
                field.clone(),
                query.clone(),
                *k,
                params.clone(),
                self.filter_exec(and(self.request.filter.as_ref(), filter.as_ref())),
                self.planner.ann.clone(),
            )),
            Retriever::Text { query, k } => match uniform_score(&view.collection.schema, query) {
                Some(constant) if score_only(self.sort) => self.constant_text(query, *k, constant),
                _ => Arc::new(self.text_exec(query, *k)),
            },
            Retriever::Fused { inputs, fusion, k } => {
                let inputs = inputs
                    .iter()
                    .map(|input| self.retriever(input))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(FusionExec::new(inputs, fusion.clone(), *k))
            }
            Retriever::Rescore {
                input,
                field,
                query,
                k,
            } => self.rescore(self.retriever(input)?, field, query, *k)?,
            Retriever::Sparse {
                field,
                query,
                k,
                filter,
                params,
            } => Arc::new(SparseExec::new(
                view.clone(),
                field.clone(),
                query.clone(),
                *k,
                self.filter_exec(and(self.request.filter.as_ref(), filter.as_ref())),
                self.filter_exec(params.idf_corpus.clone()),
                self.planner.stats.clone(),
                config.parallelism,
            )),
        })
    }

    /// Rule 11: every match of a constant-score text query ties, so its top
    /// k are its k smallest keys, walked in PK order.
    fn constant_text(&self, query: &Query, k: usize, constant: f32) -> Arc<dyn ExecutionPlan> {
        let view = self.view.clone();
        let keys = self.planner.keys.clone();
        let parallelism = self.planner.config.parallelism;
        let filter = self.request.filter.clone();
        // Scoring adds the filter's 0.0 to the query's score.
        let score = if filter.is_some() {
            constant + 0.0
        } else {
            constant
        };
        let matches = matching(query, filter.as_ref());
        let run: Arc<RankedFn> = Arc::new(move |_context| {
            let (view, keys, matches) = (view.clone(), keys.clone(), matches.clone());
            Box::pin(async move {
                let rows = FilterBitmapExec::new(view.clone(), matches, parallelism)
                    .rows()
                    .await?;
                let page = keys.page(&view, rows, None, k).await?;
                Ok(page
                    .into_iter()
                    .map(|(row_id, pk)| Ranked {
                        row_id,
                        pk,
                        score,
                        sort: Vec::new(),
                    })
                    .collect())
            })
        });
        Arc::new(RankedFnExec::new("ConstantScoreTextExec", Vec::new(), run))
    }

    /// Rule 3: the input's candidates rescored exactly against `field`.
    fn rescore(
        &self,
        input: Arc<dyn ExecutionPlan>,
        field: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Arc<dyn ExecutionPlan>, ServiceError> {
        let schema = &self.view.collection.schema;
        let Some((index, spec)) = schema
            .vectors
            .iter()
            .enumerate()
            .find(|(_, spec)| spec.name == field)
        else {
            if schema.sparse_vectors.iter().any(|s| s.name == field) {
                return Err(ServiceError::InvalidArgument(format!(
                    "{field} is a sparse vector"
                )));
            }
            return Err(ServiceError::InvalidArgument(format!(
                "unknown vector {field}"
            )));
        };
        if query.len() != spec.dim as usize {
            return Err(ServiceError::InvalidArgument(format!(
                "vector {field} has {} dimensions, the query has {}",
                spec.dim,
                query.len()
            )));
        }
        let distance = spec.distance;
        let name = spec.name.clone();
        let query = query.to_vec();
        let view = self.view.clone();
        let child = input.clone();
        let run: Arc<RankedFn> = Arc::new(move |context| {
            let (view, input, query, name) =
                (view.clone(), input.clone(), query.clone(), name.clone());
            Box::pin(async move {
                let candidates = collect_ranked(input, context).await?;
                let ids: Vec<u64> = candidates.iter().map(|hit| hit.row_id).collect();
                let columns = FetchColumns {
                    source: false,
                    vectors: vec![index],
                    sparse: Vec::new(),
                };
                let rows = fetch_rows(&view, &ids, &columns).await?;
                let mut out: Vec<Ranked> = candidates
                    .into_iter()
                    .zip(rows)
                    .filter_map(|(hit, row)| {
                        let vector = row.vectors.get(&name)?;
                        Some(Ranked {
                            score: score(distance, &query, vector),
                            ..hit
                        })
                    })
                    .collect();
                out.sort_by(|a, b| {
                    b.score
                        .total_cmp(&a.score)
                        .then_with(|| a.pk.cmp(&b.pk))
                        .then_with(|| a.row_id.cmp(&b.row_id))
                });
                out.truncate(k);
                Ok(out)
            })
        });
        Ok(Arc::new(RankedFnExec::new("RescoreExec", vec![child], run)))
    }
}

/// What the candidates of a request are, for totals and aggregations.
pub(crate) enum Domain {
    /// Every match of `Text ∧ filter` (a text-only request, or field mode).
    Text(Query),
    /// Every match of the filter (no retrievers).
    Filter,
    /// The fused candidates after `score_threshold`.
    Candidates(Vec<Ranked>),
}

impl SearchPlanner {
    pub fn new(config: SearchConfig, ann: AnnConfig) -> Self {
        let stats = StatsCache::new(config.stats_cache_entries);
        let keys = KeyPages {
            pk_dict: PkDictCache::new(config.pk_dict_cache_bytes),
            sort_threshold: config.scroll_sort_threshold,
            parallelism: config.parallelism.max(1),
        };
        Self {
            config,
            ann,
            stats,
            keys,
        }
    }

    pub fn config(&self) -> &SearchConfig {
        &self.config
    }

    /// Plans and runs `request` over `view` (rule 1).
    pub async fn search(
        &self,
        view: Arc<ReadView>,
        request: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        // 1.
        validate_request(&request, &self.config.limits)?;
        let sort = EffectiveSort::of(&request)?;
        if let Some(after) = &request.search_after {
            sort.check_search_after(after)?;
        }
        let schema = &view.collection.schema;
        let columns = fetch_columns(schema, &request.select, request.highlight.is_some())?;
        if let Some(highlight) = &request.highlight {
            check_highlight(schema, highlight)?;
        }
        let context = datafusion::prelude::SessionContext::new().task_ctx();
        let plan = Plan {
            planner: self,
            view: &view,
            request: &request,
            sort: &sort,
        };
        let text_only = request.retrievers.len() == 1
            && matches!(request.retrievers[0], Retriever::Text { .. });
        // Groups computed while sizing a field-mode window, if any.
        let mut prepared: Option<Vec<Group>> = None;
        // 2–5. The ranked candidates, in the effective order.
        let (candidates, domain) = match sort.mode {
            RankMode::Field => {
                let query = match request.retrievers.first() {
                    Some(Retriever::Text { query, .. }) => query.clone(),
                    _ => Query::MatchAll,
                };
                // Grouping needs enough matches to fill its groups: start at
                // `limit × group_size` and double the window while the
                // groups are not full and more matches exist, up to
                // `max_window` (validation keeps `offset + limit` within it).
                let max_window = self.config.limits.max_window;
                let mut window = request.offset + request.limit;
                if let Some(group_by) = &request.group_by {
                    window = window.max(
                        group_by
                            .limit
                            .saturating_mul(group_by.group_size)
                            .min(max_window),
                    );
                }
                let hits = loop {
                    let exec = TantivySearchExec::new(
                        view.clone(),
                        query.clone(),
                        request.filter.clone(),
                        window,
                        sort.clone(),
                        request.search_after.clone(),
                        self.stats.clone(),
                        self.config.parallelism,
                    );
                    let hits = collect_ranked(Arc::new(exec), context.clone()).await?;
                    let Some(group_by) = &request.group_by else {
                        break hits;
                    };
                    // Every exit keeps the groups it computed, so they are
                    // not grouped again below.
                    let groups = group(&view, &hits, group_by, &columns).await?;
                    if hits.len() < window || window >= max_window || groups_full(&groups, group_by)
                    {
                        prepared = Some(groups);
                        break hits;
                    }
                    window = window.saturating_mul(2).min(max_window);
                };
                let domain = if request.retrievers.is_empty() {
                    Domain::Filter
                } else {
                    Domain::Text(query)
                };
                (hits, domain)
            }
            RankMode::Score => {
                let mut plans = request
                    .retrievers
                    .iter()
                    .map(|retriever| plan.retriever(retriever))
                    .collect::<Result<Vec<_>, _>>()?;
                let root: Arc<dyn ExecutionPlan> = match (&request.fusion, plans.len()) {
                    (None, 1) => plans.pop().expect("one plan"),
                    (fusion, _) => {
                        let window = request
                            .retrievers
                            .iter()
                            .map(retriever_k)
                            .max()
                            .unwrap_or(0);
                        let fusion = fusion.clone().unwrap_or(Fusion::Rrf { k: 60 });
                        Arc::new(FusionExec::new(plans, fusion, window))
                    }
                };
                let mut hits = collect_ranked(root, context.clone()).await?;
                if let Some(threshold) = request.score_threshold {
                    hits.retain(|hit| hit.score >= threshold);
                }
                let domain = match &request.retrievers[0] {
                    // A threshold cuts the matches, so the domain is the
                    // candidates that pass it.
                    Retriever::Text { query, .. }
                        if text_only && request.score_threshold.is_none() =>
                    {
                        Domain::Text(query.clone())
                    }
                    _ => Domain::Candidates(hits.clone()),
                };
                if let Some(after) = &request.search_after {
                    hits.retain(|hit| sort.is_after(hit, after));
                }
                hits.sort_by(|a, b| sort.compare(a, b));
                (hits, domain)
            }
        };
        // 6–8. Groups, or the page, with their highlights (Task 8).
        let stats = match &request.highlight {
            Some(_) => {
                Some(highlight_stats(&view, &request, &self.stats, self.config.parallelism).await?)
            }
            None => None,
        };
        let hits_of = |members: Vec<(Ranked, FetchedRow)>| -> Result<Vec<Hit>, ServiceError> {
            let (ranked, rows): (Vec<Ranked>, Vec<FetchedRow>) = members.into_iter().unzip();
            let highlights = match &stats {
                Some(stats) => highlight(schema, &view, &request, stats, &rows)?,
                None => vec![BTreeMap::new(); rows.len()],
            };
            Ok(ranked
                .iter()
                .zip(rows)
                .zip(highlights)
                .map(|((ranked, row), highlight)| Hit {
                    highlight,
                    ..self.hit(&view, &request, &sort, ranked, row)
                })
                .collect())
        };
        let (hits, groups) = match &request.group_by {
            Some(group_by) => {
                let groups = match prepared {
                    Some(groups) => groups,
                    None => group(&view, &candidates, group_by, &columns).await?,
                };
                let groups = groups
                    .into_iter()
                    .map(|(key, members)| {
                        Ok(HitGroup {
                            key,
                            hits: hits_of(members)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ServiceError>>()?;
                (Vec::new(), Some(groups))
            }
            None => {
                let page: Vec<Ranked> = candidates
                    .iter()
                    .skip(request.offset)
                    .take(request.limit)
                    .cloned()
                    .collect();
                let ids: Vec<u64> = page.iter().map(|hit| hit.row_id).collect();
                let rows = fetch_rows(&view, &ids, &columns).await?;
                (hits_of(page.into_iter().zip(rows).collect())?, None)
            }
        };
        // 9.
        let total = self.total(&view, &request, &domain).await?;
        let aggregations = match &request.aggregations {
            Some(aggregations) => {
                let domain = match domain {
                    Domain::Text(query) => AggDomain::Query {
                        query,
                        filter: request.filter.clone(),
                    },
                    Domain::Filter => AggDomain::Filter(request.filter.clone()),
                    Domain::Candidates(hits) => {
                        AggDomain::Rows(hits.iter().map(|hit| hit.row_id).collect())
                    }
                };
                Some(aggregate(&view, aggregations, domain, &self.config).await?)
            }
            None => None,
        };
        Ok(SearchResponse {
            hits,
            total,
            aggregations,
            groups,
            read_token: view.read_token.clone(),
            hot_used: view.hot_used.kinds(),
        })
    }

    /// One hit (rule 1.8).
    fn hit(
        &self,
        view: &ReadView,
        request: &SearchRequest,
        sort: &EffectiveSort,
        ranked: &Ranked,
        row: FetchedRow,
    ) -> Hit {
        let schema = &view.collection.schema;
        let fields = match &row.source {
            Some(source) => field_values(schema, source, &request.select.fields),
            None => Default::default(),
        };
        Hit {
            pk: row.pk,
            score: ranked.score,
            sort_values: sort.sort_values(ranked),
            source: row
                .source
                .as_ref()
                .and_then(|source| filter_source(source, &request.select.source)),
            vectors: row.vectors,
            sparse_vectors: row.sparse_vectors,
            highlight: Default::default(),
            fields,
        }
    }

    /// Rule 4.
    async fn total(
        &self,
        view: &Arc<ReadView>,
        request: &SearchRequest,
        domain: &Domain,
    ) -> Result<Option<TotalHits>, ServiceError> {
        let bound = match request.track_total_hits {
            TrackTotalHits::None => return Ok(None),
            TrackTotalHits::Exact => None,
            TrackTotalHits::UpTo(n) => Some(n),
        };
        let count = match domain {
            Domain::Text(query) => {
                TantivySearchExec::new(
                    view.clone(),
                    query.clone(),
                    request.filter.clone(),
                    0,
                    EffectiveSort::by_score(),
                    None,
                    self.stats.clone(),
                    self.config.parallelism,
                )
                .count()
                .await?
            }
            Domain::Filter => self.count(view.clone(), request.filter.clone()).await?,
            Domain::Candidates(hits) => hits.len() as u64,
        };
        Ok(Some(match bound {
            Some(n) if count > n => TotalHits {
                value: n,
                relation: TotalRelation::Gte,
            },
            _ => TotalHits {
                value: count,
                relation: TotalRelation::Eq,
            },
        }))
    }

    /// Rule 7: one entry per key, in request order, misses as `None`.
    pub async fn get(
        &self,
        view: &ReadView,
        pks: &[PrimaryKey],
        select: &Projection,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        crate::exec::get::get(view, pks, select).await
    }

    /// Rule 8: the filter's matches over durable rows and the tail; every
    /// live row without one.
    pub async fn count(
        &self,
        view: Arc<ReadView>,
        filter: Option<Query>,
    ) -> Result<u64, ServiceError> {
        let Some(filter) = filter else {
            return Ok(view.live_rows());
        };
        let live = view.live_rows();
        match FilterBitmapExec::new(view, filter, self.config.parallelism)
            .rows()
            .await?
        {
            RowSet::All => Ok(live),
            RowSet::Rows(rows) => Ok(rows.len()),
        }
    }

    /// Rule 6: the next `limit` documents after `after` (exclusive) in PK
    /// order that match `filter`, and the key to continue after when more
    /// remain.
    pub async fn scroll(
        &self,
        view: Arc<ReadView>,
        filter: Option<Query>,
        after: Option<PrimaryKey>,
        limit: usize,
        select: &Projection,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        let schema = &view.collection.schema;
        let columns = fetch_columns(schema, select, false)?;
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let rows = match filter {
            None => RowSet::All,
            Some(filter) => {
                FilterBitmapExec::new(view.clone(), filter, self.config.parallelism)
                    .rows()
                    .await?
            }
        };
        let mut page = self
            .keys
            .page(&view, rows, after.as_ref(), limit.saturating_add(1))
            .await?;
        let more = page.len() > limit;
        page.truncate(limit);
        let ids: Vec<u64> = page.iter().map(|(row, _)| *row).collect();
        let docs: Vec<StoredDoc> = fetch_rows(&view, &ids, &columns)
            .await?
            .into_iter()
            .map(|row| stored_doc(schema, row, select))
            .collect();
        let next = if more {
            docs.last().map(|doc| doc.pk.clone())
        } else {
            None
        };
        Ok((docs, next))
    }
}

fn retriever_k(retriever: &Retriever) -> usize {
    match retriever {
        Retriever::Vector { k, .. }
        | Retriever::Text { k, .. }
        | Retriever::Fused { k, .. }
        | Retriever::Rescore { k, .. }
        | Retriever::Sparse { k, .. } => *k,
    }
}
