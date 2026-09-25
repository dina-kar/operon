//! Aggregations (plan M1.2 Task 8 rules 1–5; Ruling 6): Tantivy's
//! aggregations over every split of the view plus the tail, masking
//! deleted, shadowed and out-of-domain docs, merged in memory in manifest
//! order, then the tail, into the ES-shaped JSON Quickwit serves.
//!
//! Size bound: each split's intermediate result is bounded by its
//! collector's `AggregationLimitsGuard`, but nothing prunes the merged
//! result to `segment_size` (Quickwit's `prune_intermediate_results` was
//! not vendored), so a merged `terms` result holds up to Σ over splits of
//! their buckets (at most splits × `agg_bucket_limit`) until
//! `into_final_result` applies `size` and checks the bucket limit again.
//!
//! `top_hits` sorts only by fast fields; hits that tie on every sort key
//! come back in split, then doc order (a documented limitation).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::StreamExt;
use operon_collection::{CollectionSchema, FieldKind, PrimaryKey, ROWID_FIELD};
use operon_quickwit::doc_mapper::FastFieldWarmupInfo;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde_json::{Map, Value};
use tantivy::aggregation::agg_req::{Aggregations, get_fast_field_names};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{
    AggContextParams, AggregationLimitsGuard, DistributedAggregationCollector,
};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

use crate::error::ServiceError;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow, fetch_rows};
use crate::exec::planner::SearchConfig;
use crate::exec::project::{field_values, filter_source};
use crate::exec::{Unit, blocking, open_units_with};
use crate::ir::Query;
use crate::read::ReadView;
use crate::text::compile::{CompileMode, QueryCompiler};
use crate::text::fields::{ResolvedField, resolve_field};
use crate::text::query_tokenizers;
use crate::types::SourceFilter;

/// The docs an aggregation runs over (Ruling 6).
#[derive(Clone, Debug)]
pub enum AggDomain {
    /// Every match of `query ∧ filter` (a text-only request, or field
    /// mode).
    Query { query: Query, filter: Option<Query> },
    /// Every match of the filter (no retrievers).
    Filter(Option<Query>),
    /// These rows (the fused candidates after `score_threshold`).
    Rows(RoaringTreemap),
}

/// A callback on one `top_hits` result and its agg-name path.
type TopHitsVisitor<'a> = dyn FnMut(&[String], &mut Value) -> Result<(), ServiceError> + 'a;

/// A collector wrapper that skips masked docs and, for
/// [`AggDomain::Rows`], docs whose `_rowid` is not allowed.
pub struct MaskedCollector<C> {
    inner: C,
    /// Per segment ordinal, the masked docs.
    masks: Vec<RoaringBitmap>,
    allow: Option<Arc<RoaringTreemap>>,
}

impl<C> std::fmt::Debug for MaskedCollector<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskedCollector")
            .field("segments", &self.masks.len())
            .field("rows", &self.allow.as_ref().map(|rows| rows.len()))
            .finish_non_exhaustive()
    }
}

impl<S> std::fmt::Debug for MaskedSegmentCollector<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskedSegmentCollector")
            .field("masked", &self.mask.len())
            .finish_non_exhaustive()
    }
}

impl<C> MaskedCollector<C> {
    pub fn new(inner: C, masks: Vec<RoaringBitmap>, allow: Option<Arc<RoaringTreemap>>) -> Self {
        Self {
            inner,
            masks,
            allow,
        }
    }
}

/// The segment side of a [`MaskedCollector`].
pub struct MaskedSegmentCollector<S> {
    inner: S,
    mask: RoaringBitmap,
    allow: Option<(Column<u64>, Arc<RoaringTreemap>)>,
    buffer: Vec<DocId>,
}

impl<S> MaskedSegmentCollector<S> {
    fn admits(&self, doc: DocId) -> bool {
        if self.mask.contains(doc) {
            return false;
        }
        match &self.allow {
            None => true,
            Some((rowid, allow)) => rowid.first(doc).is_some_and(|row| allow.contains(row)),
        }
    }
}

impl<S: SegmentCollector> SegmentCollector for MaskedSegmentCollector<S> {
    type Fruit = S::Fruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        if self.admits(doc) {
            self.inner.collect(doc, score);
        }
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.clear();
        buffer.extend(docs.iter().copied().filter(|doc| self.admits(*doc)));
        if !buffer.is_empty() {
            self.inner.collect_block(&buffer);
        }
        self.buffer = buffer;
    }

    fn harvest(self) -> Self::Fruit {
        self.inner.harvest()
    }
}

impl<C: Collector> Collector for MaskedCollector<C> {
    type Fruit = C::Fruit;
    type Child = MaskedSegmentCollector<C::Child>;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let inner = self.inner.for_segment(segment_local_id, reader)?;
        let allow = match &self.allow {
            None => None,
            Some(allow) => Some((reader.fast_fields().u64(ROWID_FIELD)?, allow.clone())),
        };
        Ok(MaskedSegmentCollector {
            inner,
            mask: self
                .masks
                .get(segment_local_id as usize)
                .cloned()
                .unwrap_or_default(),
            allow,
            buffer: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.inner.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        self.inner.merge_fruits(segment_fruits)
    }
}

fn invalid(message: impl std::fmt::Display) -> ServiceError {
    ServiceError::InvalidArgument(format!("aggregations: {message}"))
}

/// A Tantivy error of an aggregation: a request or schema error is the
/// caller's, the rest `Internal`.
fn agg_error(err: TantivyError) -> ServiceError {
    match err {
        TantivyError::AggregationError(_)
        | TantivyError::InvalidArgument(_)
        | TantivyError::SchemaError(_)
        | TantivyError::FieldNotFound(_) => invalid(err),
        other => ServiceError::Internal(format!("aggregations: {other}")),
    }
}

/// What a `top_hits` node asked for besides Tantivy's sort.
#[derive(Clone, Debug, PartialEq)]
struct TopHitsOptions {
    source: SourceFilter,
    docvalue_fields: Vec<String>,
}

fn strings(value: &Value, what: &str) -> Result<Vec<String>, ServiceError> {
    match value {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| invalid(format!("top_hits {what} must be strings")))
            })
            .collect(),
        _ => Err(invalid(format!("top_hits {what} must be strings"))),
    }
}

/// A `top_hits` `_source` option (rule 2.1).
fn source_option(value: Option<Value>) -> Result<SourceFilter, ServiceError> {
    Ok(match value {
        None | Some(Value::Bool(true)) => SourceFilter::All,
        Some(Value::Bool(false)) => SourceFilter::None,
        Some(value @ (Value::String(_) | Value::Array(_))) => SourceFilter::Paths {
            include: strings(&value, "_source")?,
            exclude: Vec::new(),
        },
        Some(Value::Object(object)) => {
            let list = |keys: [&str; 2]| -> Result<Vec<String>, ServiceError> {
                match keys.iter().find_map(|key| object.get(*key)) {
                    Some(value) => strings(value, "_source"),
                    None => Ok(Vec::new()),
                }
            };
            SourceFilter::Paths {
                include: list(["includes", "include"])?,
                exclude: list(["excludes", "exclude"])?,
            }
        }
        Some(_) => {
            return Err(invalid(
                "top_hits _source must be a bool, a list or an object",
            ));
        }
    })
}

/// The sub-aggregations of an aggregation node.
fn sub_aggs(node: &Map<String, Value>) -> Option<&Map<String, Value>> {
    node.get("aggs")
        .or_else(|| node.get("aggregations"))
        .and_then(Value::as_object)
}

fn sub_aggs_mut(node: &mut Map<String, Value>) -> Option<&mut Map<String, Value>> {
    let key = if node.contains_key("aggs") {
        "aggs"
    } else {
        "aggregations"
    };
    node.get_mut(key).and_then(Value::as_object_mut)
}

/// Rule 2: every `top_hits` node loses its `_source` and `docvalue_fields`
/// (remembered by agg-name path) and reads `_rowid` instead.
fn rewrite(
    aggs: &mut Map<String, Value>,
    path: &mut Vec<String>,
    out: &mut BTreeMap<Vec<String>, TopHitsOptions>,
) -> Result<(), ServiceError> {
    for (name, node) in aggs.iter_mut() {
        let Some(node) = node.as_object_mut() else {
            continue;
        };
        path.push(name.clone());
        if let Some(Value::Object(top)) = node.get_mut("top_hits") {
            let source = source_option(top.remove("_source"))?;
            let docvalue_fields = match top.remove("docvalue_fields") {
                Some(value) => strings(&value, "docvalue_fields")?,
                None => Vec::new(),
            };
            top.insert(
                "docvalue_fields".to_string(),
                Value::Array(vec![Value::String(ROWID_FIELD.to_string())]),
            );
            out.insert(
                path.clone(),
                TopHitsOptions {
                    source,
                    docvalue_fields,
                },
            );
        }
        if let Some(sub) = sub_aggs_mut(node) {
            rewrite(sub, path, out)?;
        }
        path.pop();
    }
    Ok(())
}

/// Calls `f` with every `top_hits` result of `result` and its agg-name
/// path, walking `request` (the rewritten request) alongside: a bucket
/// aggregation's sub-results sit in each of its `buckets` (a list, or an
/// object when keyed), a single-bucket one's in the result itself.
fn visit_top_hits(
    result: &mut Map<String, Value>,
    request: &Map<String, Value>,
    path: &mut Vec<String>,
    f: &mut TopHitsVisitor<'_>,
) -> Result<(), ServiceError> {
    for (name, node) in request {
        let (Some(node), Some(res)) = (node.as_object(), result.get_mut(name)) else {
            continue;
        };
        path.push(name.clone());
        if node.contains_key("top_hits") {
            f(path, res)?;
        } else if let Some(sub) = sub_aggs(node) {
            let res = res.as_object_mut();
            if let Some(res) = res {
                match res.get_mut("buckets") {
                    Some(Value::Array(buckets)) => {
                        for bucket in buckets {
                            if let Some(bucket) = bucket.as_object_mut() {
                                visit_top_hits(bucket, sub, path, f)?;
                            }
                        }
                    }
                    Some(Value::Object(buckets)) => {
                        for bucket in buckets.values_mut() {
                            if let Some(bucket) = bucket.as_object_mut() {
                                visit_top_hits(bucket, sub, path, f)?;
                            }
                        }
                    }
                    _ => visit_top_hits(res, sub, path, f)?,
                }
            }
        }
        path.pop();
    }
    Ok(())
}

/// The `_rowid` a `top_hits` hit carries in its `docvalue_fields`.
fn hit_rowid(hit: &Value) -> Result<u64, ServiceError> {
    let value = hit
        .get("docvalue_fields")
        .and_then(|fields| fields.get(ROWID_FIELD));
    let value = match value {
        Some(Value::Array(items)) => items.first(),
        other => other,
    };
    value
        .and_then(Value::as_u64)
        .ok_or_else(|| ServiceError::Internal(format!("a top_hits hit without {ROWID_FIELD}")))
}

/// Rule 2's last check: every field an aggregation reads is a fast field
/// of the current schema (a JSON path under a fast Json field counts).
fn check_fast_fields(
    schema: &CollectionSchema,
    aggs: &Aggregations,
) -> Result<BTreeSet<String>, ServiceError> {
    let names: BTreeSet<String> = get_fast_field_names(aggs).into_iter().collect();
    for name in &names {
        if name == ROWID_FIELD {
            continue;
        }
        let fast = match resolve_field(schema, name) {
            ResolvedField::Plain { spec } => spec.fast && spec.kind != FieldKind::Json,
            ResolvedField::JsonPath { spec, .. } => spec.fast,
            ResolvedField::Unknown => false,
        };
        if !fast {
            return Err(ServiceError::InvalidArgument(format!(
                "aggregation field {name} is not a fast field"
            )));
        }
    }
    Ok(names)
}

/// The fast columns to warm for `names`: a field's own column, a JSON
/// path's Json field with its subpaths.
fn fast_warmups(schema: &CollectionSchema, names: &BTreeSet<String>) -> Vec<FastFieldWarmupInfo> {
    let mut out: Vec<FastFieldWarmupInfo> = Vec::new();
    let mut push = |info: FastFieldWarmupInfo| {
        if !out.contains(&info) {
            out.push(info);
        }
    };
    push(FastFieldWarmupInfo {
        name: ROWID_FIELD.to_string(),
        with_subfields: false,
    });
    for name in names {
        match resolve_field(schema, name) {
            ResolvedField::JsonPath { spec, .. } => push(FastFieldWarmupInfo {
                name: spec.name.clone(),
                with_subfields: true,
            }),
            _ => push(FastFieldWarmupInfo {
                name: name.clone(),
                with_subfields: false,
            }),
        }
    }
    out
}

fn limits(config: &SearchConfig) -> AggregationLimitsGuard {
    AggregationLimitsGuard::new(Some(config.agg_memory_limit), Some(config.agg_bucket_limit))
}

/// Runs the aggregation request `request` (Tantivy's, ES-shaped JSON) over
/// `domain` of the view, and returns the ES-shaped result.
pub async fn aggregate(
    view: &ReadView,
    request: &Value,
    domain: AggDomain,
    config: &SearchConfig,
) -> Result<Value, ServiceError> {
    // 2. The rewrite, parse and checks.
    let mut rewritten = request.clone();
    let Some(top) = rewritten.as_object_mut() else {
        return Err(invalid("the request must be an object"));
    };
    let mut options = BTreeMap::new();
    rewrite(top, &mut Vec::new(), &mut options)?;
    let aggs: Aggregations = serde_json::from_value(rewritten.clone()).map_err(invalid)?;
    let schema = &view.collection.schema;
    let names = check_fast_fields(schema, &aggs)?;
    // 3. Every split in manifest order, then the tail.
    let tokenizers = query_tokenizers();
    let (query, filter, allow) = match domain {
        AggDomain::Query { query, filter } => (Some(query), filter, None),
        AggDomain::Filter(filter) => (None, filter, None),
        AggDomain::Rows(rows) => (None, None, Some(Arc::new(rows))),
    };
    let compile = |split: &tantivy::schema::Schema| {
        let compiler = QueryCompiler::new(schema, split, &tokenizers);
        match (&query, &filter) {
            (Some(query), filter) => compiler.compile_with_filter(query, filter.as_ref()),
            (None, Some(filter)) => compiler.compile(filter, CompileMode::Filter),
            (None, None) => compiler.compile(&Query::MatchAll, CompileMode::Filter),
        }
    };
    let fast = fast_warmups(schema, &names);
    let (splits, units) = open_units_with(view, &compile, &fast, config.parallelism).await?;
    drop(splits);
    let results = futures::stream::iter(units)
        .map(|unit: Unit| {
            let aggs = aggs.clone();
            let allow = allow.clone();
            let limits = limits(config);
            blocking(move || {
                let collector = MaskedCollector::new(
                    DistributedAggregationCollector::from_aggs(
                        aggs,
                        AggContextParams::new(limits, operon_text::tokenizer_manager()),
                    ),
                    unit.masks,
                    allow,
                );
                unit.searcher
                    .search(unit.query.as_ref(), &collector)
                    .map_err(agg_error)
            })
        })
        .buffered(config.parallelism.max(1))
        .collect::<Vec<_>>()
        .await;
    // 4. The merge, in that order.
    let mut merged: Option<IntermediateAggregationResults> = None;
    for result in results {
        let result = result?;
        match &mut merged {
            None => merged = Some(result),
            Some(merged) => merged.merge_fruits(result).map_err(agg_error)?,
        }
    }
    let merged = merged.unwrap_or_default();
    let final_result = merged
        .into_final_result(aggs, limits(config))
        .map_err(agg_error)?;
    let mut json = serde_json::to_value(final_result)
        .map_err(|err| ServiceError::Internal(format!("aggregation result: {err}")))?;
    // 5. `top_hits`.
    if !options.is_empty() {
        let request_aggs = rewritten.as_object().expect("an object");
        if let Some(result) = json.as_object_mut() {
            top_hits(view, result, request_aggs, &options).await?;
        }
    }
    Ok(json)
}

/// Rule 5: each `top_hits` hit's `_rowid` resolved with one fetch for the
/// whole response, and the hit rewritten to `{_id, _score: null, _source,
/// sort, fields}`.
async fn top_hits(
    view: &ReadView,
    result: &mut Map<String, Value>,
    request: &Map<String, Value>,
    options: &BTreeMap<Vec<String>, TopHitsOptions>,
) -> Result<(), ServiceError> {
    let mut ids: BTreeSet<u64> = BTreeSet::new();
    visit_top_hits(result, request, &mut Vec::new(), &mut |_, res| {
        if let Some(Value::Array(hits)) = res.get("hits") {
            for hit in hits {
                ids.insert(hit_rowid(hit)?);
            }
        }
        Ok(())
    })?;
    let ids: Vec<u64> = ids.into_iter().collect();
    let columns = FetchColumns {
        source: true,
        ..FetchColumns::default()
    };
    let rows: BTreeMap<u64, FetchedRow> = fetch_rows(view, &ids, &columns)
        .await?
        .into_iter()
        .map(|row| (row.row_id, row))
        .collect();
    let schema = &view.collection.schema;
    visit_top_hits(result, request, &mut Vec::new(), &mut |path, res| {
        let option = options
            .get(path)
            .ok_or_else(|| ServiceError::Internal(format!("no top_hits options at {path:?}")))?;
        let Some(Value::Array(hits)) = res.get_mut("hits") else {
            return Ok(());
        };
        for hit in hits.iter_mut() {
            let row = rows.get(&hit_rowid(hit)?).ok_or_else(|| {
                ServiceError::Internal("a top_hits row was not fetched".to_string())
            })?;
            *hit = top_hit(schema, hit, row, option)?;
        }
        Ok(())
    })
}

fn top_hit(
    schema: &CollectionSchema,
    hit: &Value,
    row: &FetchedRow,
    option: &TopHitsOptions,
) -> Result<Value, ServiceError> {
    let mut out = Map::new();
    out.insert("_id".to_string(), pk_json(&row.pk));
    out.insert("_score".to_string(), Value::Null);
    if let Some(source) = row
        .source
        .as_ref()
        .and_then(|source| filter_source(source, &option.source))
    {
        out.insert("_source".to_string(), Value::Object(source));
    }
    if let Some(sort) = hit.get("sort") {
        out.insert("sort".to_string(), sort.clone());
    }
    if !option.docvalue_fields.is_empty() {
        let values = match &row.source {
            Some(source) => field_values(schema, source, &option.docvalue_fields),
            None => BTreeMap::new(),
        };
        let fields = serde_json::to_value(values)
            .map_err(|err| ServiceError::Internal(format!("top_hits fields: {err}")))?;
        out.insert("fields".to_string(), fields);
    }
    Ok(Value::Object(out))
}

fn pk_json(pk: &PrimaryKey) -> Value {
    crate::json::pk::to_json(pk)
}
