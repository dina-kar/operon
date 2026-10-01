//! Search execution (plan M1.5 Task 9): `_search`, `_count` and
//! `_msearch` over the compiled [`SearchPlan`]s, with ES scores.
//!
//! - A plan is compiled per concrete index. One index runs its plan as it
//!   is; several (a comma list, a wildcard, an alias with several members)
//!   run one query-only, knn-only or field-sorted search each and merge by
//!   ES score (or sort values), then PK, then index name (Ruling 15).
//! - A hybrid sum runs its text part at the request's consistency and its
//!   vector parts at `AtLeast` the text part's token (Ruling 3).
//! - Every request reads at the request context's consistency (Task 8
//!   item 9).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, Uri};
use axum::response::Response;
use futures::StreamExt;
use loams_collection::{ConsistencyToken, PrimaryKey};
use loams_query::exec::order::compare_values;
use loams_query::{
    Hit, MissingOrder, Projection, Query, ReadConsistency, Retriever, SearchRequest, ServiceError,
    SortKey, SortOrder, SortValue, SourceFilter as IrSourceFilter, TotalHits, TotalRelation,
    TrackTotalHits,
};
use serde_json::{Map, Value, json};

use super::body::track_total_hits;
use super::compile::url_query;
use super::render::{cap_total, render_hit, render_response, shards, to_es_score};
use super::{EsScore, RenderSpec, SEARCH_PARAMS, SearchParams, SearchPlan, parse_params};
use crate::dsl::query::token_name;
use crate::dsl::{QueryContext, parse_query};
use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, json_body, respond};
use crate::mapping::{EsSimilarity, IndexView, index_uuid};
use crate::names::{IndexExpr, ResolveOptions, resolve};
use crate::{EsGateway, TOKEN_HEADER};

/// `_count`'s parameters (Task 9 routes).
const COUNT_PARAMS: &[&str] = &[
    "q",
    "df",
    "default_operator",
    "analyzer",
    "lenient",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "preference",
    "routing",
    "min_score",
    "terminate_after",
];

/// `_msearch`'s parameters.
const MSEARCH_PARAMS: &[&str] = &[
    "max_concurrent_searches",
    "rest_total_hits_as_int",
    "search_type",
    "ccs_minimize_roundtrips",
    "typed_keys",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
];

/// The keys of an `_msearch` header line.
const MSEARCH_HEADER_KEYS: &[&str] = &[
    "index",
    "preference",
    "routing",
    "search_type",
    "request_cache",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
];

/// A search answer and the read token of the view it came from (one
/// index only: tokens of different collections do not combine).
#[derive(Clone, Debug)]
pub struct SearchOutcome {
    pub body: Value,
    pub read_token: Option<ConsistencyToken>,
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// `err` with the configured node in a `search_phase_execution_exception`
/// (compile names [`SEARCH_NODE`]).
fn renode(mut err: EsError, node: &str) -> EsError {
    if err.kind == "search_phase_execution_exception"
        && let Some(Value::Array(shards)) = err.extra.get_mut("failed_shards")
    {
        for shard in shards {
            if let Some(obj) = shard.as_object_mut() {
                obj.insert("node".to_string(), json!(node));
            }
        }
    }
    err
}

/// The texts of every `query_string` in `query`.
fn query_strings<'a>(query: &'a Query, out: &mut Vec<&'a str>) {
    match query {
        Query::QueryString { query, .. } => out.push(query),
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => {
            for q in must.iter().chain(should).chain(must_not).chain(filter) {
                query_strings(q, out);
            }
        }
        Query::Boost { query, .. } | Query::ConstantScore { query, .. } => {
            query_strings(query, out)
        }
        _ => {}
    }
}

fn retriever_query_strings<'a>(retriever: &'a Retriever, out: &mut Vec<&'a str>) {
    match retriever {
        Retriever::Text { query, .. } => query_strings(query, out),
        Retriever::Vector {
            filter: Some(filter),
            ..
        }
        | Retriever::Sparse {
            filter: Some(filter),
            ..
        } => query_strings(filter, out),
        Retriever::Fused { inputs, .. } => {
            for input in inputs {
                retriever_query_strings(input, out);
            }
        }
        Retriever::Rescore { input, .. } => retriever_query_strings(input, out),
        _ => {}
    }
}

/// The `query_string` texts of `request` (for the parse error's reason).
fn request_query_strings(request: &SearchRequest) -> Vec<String> {
    let mut out = Vec::new();
    for retriever in &request.retrievers {
        retriever_query_strings(retriever, &mut out);
    }
    if let Some(filter) = &request.filter {
        query_strings(filter, &mut out);
    }
    out.into_iter().map(str::to_string).collect()
}

/// The ES error of a service error while searching `view` (item 8): a
/// `query_string` syntax error is `query_shard_exception` "Failed to parse
/// query [<q>]" (Task 7 item 3), and any other 400 is wrapped in
/// `search_phase_execution_exception` for the index; other statuses (a
/// missing index, backpressure, unavailability) pass through.
fn search_error(err: ServiceError, view: &IndexView, texts: &[String], node: &str) -> EsError {
    if let ServiceError::InvalidArgument(message) = &err
        && let Some(detail) = message.strip_prefix("query_string: ")
    {
        let text = texts.first().map_or("", String::as_str);
        let inner = EsError::new(
            400,
            "query_shard_exception",
            format!("Failed to parse query [{text}]"),
        )
        .with("index_uuid", index_uuid(view.info.id))
        .with("index", view.name.as_str())
        .with(
            "caused_by",
            json!({"type": "parse_exception", "reason": detail}),
        );
        return EsError::search_phase(inner, &view.name, node);
    }
    let error = EsError::from_service(err, ErrorContext::Read);
    if error.status == 400 {
        EsError::search_phase(error, &view.name, node)
    } else {
        error
    }
}

/// One index's hits, each with its ES score (boost applied).
struct Part {
    index: String,
    hits: Vec<(Hit, f32)>,
    total: Option<TotalHits>,
    token: ConsistencyToken,
}

/// Runs `request` on `view` at `consistency`.
async fn run(
    gw: &EsGateway,
    ctx: &RequestCtx,
    view: &IndexView,
    mut request: SearchRequest,
    consistency: ReadConsistency,
) -> Result<loams_query::SearchResponse, EsError> {
    request.consistency = consistency;
    let texts = request_query_strings(&request);
    gw.service()
        .search(&ctx.namespace, request)
        .await
        .map_err(|err| search_error(err, view, &texts, &gw.config().node_name))
}

/// Converts and boosts the scores of `hits`, and applies `min_score` when
/// the engine had no threshold for it.
fn scored(hits: Vec<Hit>, render: &RenderSpec, threshold_set: bool) -> Vec<(Hit, f32)> {
    let mut out: Vec<(Hit, f32)> = hits
        .into_iter()
        .map(|hit| {
            let es = to_es_score(render.score, hit.score) * render.boost;
            (hit, es)
        })
        .collect();
    if let (Some(min), false) = (render.min_score, threshold_set) {
        out.retain(|(_, es)| *es >= min);
    }
    out
}

/// A `Single` plan's request on one index.
async fn run_single(
    gw: &EsGateway,
    ctx: &RequestCtx,
    view: &IndexView,
    request: SearchRequest,
    render: &RenderSpec,
) -> Result<Part, EsError> {
    let threshold_set = request.score_threshold.is_some();
    let response = run(gw, ctx, view, request, ctx.consistency.clone()).await?;
    Ok(Part {
        index: view.name.clone(),
        hits: scored(response.hits, render, threshold_set),
        total: response.total,
        token: response.read_token,
    })
}

/// `count_filter` with its `Ids` placeholder holding the knn hits (Task 8
/// row T8-4).
fn fill_ids(filter: Query, pks: Vec<PrimaryKey>) -> Query {
    match filter {
        Query::Ids(ids) if ids.is_empty() => Query::Ids(pks),
        Query::Bool {
            must,
            mut should,
            must_not,
            filter,
            minimum_should_match,
        } => {
            if let Some(Query::Ids(ids)) = should.last_mut()
                && ids.is_empty()
            {
                *ids = pks;
            }
            Query::Bool {
                must,
                should,
                must_not,
                filter,
                minimum_should_match,
            }
        }
        other => other,
    }
}

/// Counts `filter`'s matches in `view` for a total tracked as `track`.
async fn count_total(
    gw: &EsGateway,
    ctx: &RequestCtx,
    view: &IndexView,
    filter: Query,
    consistency: ReadConsistency,
    track: TrackTotalHits,
) -> Result<Option<TotalHits>, EsError> {
    if track == TrackTotalHits::None {
        return Ok(None);
    }
    let filter = match filter {
        Query::MatchAll => None,
        other => Some(other),
    };
    let texts = filter
        .as_ref()
        .map(|f| {
            let mut out = Vec::new();
            query_strings(f, &mut out);
            out.into_iter().map(str::to_string).collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let value = gw
        .service()
        .count(&ctx.namespace, &view.name, filter, consistency)
        .await
        .map_err(|err| search_error(err, view, &texts, &gw.config().node_name))?;
    Ok(cap_total(
        TotalHits {
            value,
            relation: TotalRelation::Eq,
        },
        track,
    ))
}

/// The ES score sum of a query and knn searches (item 3).
async fn run_hybrid(
    gw: &EsGateway,
    ctx: &RequestCtx,
    view: &IndexView,
    text: Option<(SearchRequest, f32)>,
    vectors: Vec<(SearchRequest, EsSimilarity, f32)>,
    count_filter: Query,
    render: &RenderSpec,
) -> Result<Part, EsError> {
    let mut merged: HashMap<PrimaryKey, (Hit, f32)> = HashMap::new();
    let mut token: Option<ConsistencyToken> = None;
    let at = |token: &Option<ConsistencyToken>| match token {
        Some(token) => ReadConsistency::AtLeast(token.clone()),
        None => ctx.consistency.clone(),
    };
    if let Some((request, boost)) = text {
        let response = run(gw, ctx, view, request, at(&token)).await?;
        token = Some(response.read_token);
        for hit in response.hits {
            let es = boost * to_es_score(EsScore::Bm25, hit.score);
            merged.insert(hit.pk.clone(), (hit, es));
        }
    }
    let mut knn_pks = Vec::new();
    for (request, similarity, boost) in vectors {
        let response = run(gw, ctx, view, request, at(&token)).await?;
        if token.is_none() {
            token = Some(response.read_token);
        }
        for hit in response.hits {
            let es = boost * to_es_score(EsScore::Knn(similarity), hit.score);
            knn_pks.push(hit.pk.clone());
            match merged.get_mut(&hit.pk) {
                Some((_, sum)) => *sum += es,
                None => {
                    merged.insert(hit.pk.clone(), (hit, es));
                }
            }
        }
    }
    let token = token.unwrap_or_default();
    let mut hits: Vec<(Hit, f32)> = merged.into_values().collect();
    hits.sort_by(|(a, x), (b, y)| y.total_cmp(x).then_with(|| a.pk.cmp(&b.pk)));
    let hits = hits
        .into_iter()
        .skip(render.from)
        .take(render.size)
        .collect();
    knn_pks.sort();
    knn_pks.dedup();
    let total = count_total(
        gw,
        ctx,
        view,
        fill_ids(count_filter, knn_pks),
        ReadConsistency::AtLeast(token.clone()),
        render.track,
    )
    .await?;
    Ok(Part {
        index: view.name.clone(),
        hits,
        total,
        token,
    })
}

/// A recognised `script_score` (item 4): one exact search. The total is
/// the filter's matches; with a threshold (`min_score`) it is the
/// candidates that pass it among the top `from + size`, `gte` when they
/// fill that window (row T9-5).
async fn run_script(
    gw: &EsGateway,
    ctx: &RequestCtx,
    view: &IndexView,
    request: SearchRequest,
    count_filter: Query,
    render: &RenderSpec,
) -> Result<Part, EsError> {
    let threshold_set = request.score_threshold.is_some();
    let window = render.from.saturating_add(render.size).max(1) as u64;
    let response = run(gw, ctx, view, request, ctx.consistency.clone()).await?;
    let token = response.read_token.clone();
    let total = if threshold_set {
        response.total.map(|total| {
            if total.value >= window {
                TotalHits {
                    value: total.value,
                    relation: TotalRelation::Gte,
                }
            } else {
                total
            }
        })
    } else {
        count_total(
            gw,
            ctx,
            view,
            count_filter,
            ReadConsistency::AtLeast(token.clone()),
            render.track,
        )
        .await?
    };
    Ok(Part {
        index: view.name.clone(),
        hits: scored(response.hits, render, threshold_set),
        total,
        token,
    })
}

/// The order of two hits of a multi-index search by the request's sort
/// keys (a `_score` key compares ES scores).
fn compare_by_keys(keys: &[SortKey], a: (&Hit, f32), b: (&Hit, f32)) -> Ordering {
    let apply = |order: SortOrder, o: Ordering| match order {
        SortOrder::Asc => o,
        SortOrder::Desc => o.reverse(),
    };
    for (i, key) in keys.iter().enumerate() {
        let ordering = match key {
            SortKey::Score { order } => apply(*order, a.1.total_cmp(&b.1)),
            SortKey::Pk { order } => apply(*order, a.0.pk.cmp(&b.0.pk)),
            SortKey::Field { order, missing, .. } => {
                let null = SortValue::Null;
                let x = a.0.sort_values.get(i).unwrap_or(&null);
                let y = b.0.sort_values.get(i).unwrap_or(&null);
                let null_first = match missing {
                    MissingOrder::First => Ordering::Less,
                    MissingOrder::Last => Ordering::Greater,
                };
                match (x, y) {
                    (SortValue::Null, SortValue::Null) => Ordering::Equal,
                    (SortValue::Null, _) => null_first,
                    (_, SortValue::Null) => null_first.reverse(),
                    _ => apply(*order, compare_values(x, y)),
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Why a plan cannot fan out over several indices (Ruling 15).
fn fan_out_refusal(plan: &SearchPlan) -> Option<&'static str> {
    match plan {
        SearchPlan::HybridSum { .. } => Some("hybrid search over several indices"),
        SearchPlan::ScriptScore { .. } => Some("script_score over several indices"),
        SearchPlan::Single { render, .. } if render.score == EsScore::Rrf => {
            Some("rrf over several indices")
        }
        SearchPlan::Single { .. } => None,
    }
}

/// What a multi-index search ran: each index's part, the render spec
/// (the same for every index), the sort keys to merge by (empty: by ES
/// score) and the knn `k` of a knn-only search.
struct Fanned {
    parts: Vec<Part>,
    render: RenderSpec,
    keys: Vec<SortKey>,
    knn_k: Option<usize>,
}

/// A multi-index search (item 2): one search per index with the whole
/// window (a knn search: its `k`), from offset 0.
async fn run_multi(
    gw: &EsGateway,
    ctx: &RequestCtx,
    plans: Vec<(&IndexView, SearchPlan)>,
) -> Result<Fanned, EsError> {
    let mut keys = Vec::new();
    let mut knn_k = None;
    let mut render = None;
    let mut calls = Vec::new();
    for (view, plan) in plans {
        let SearchPlan::Single {
            mut request,
            render: spec,
        } = plan
        else {
            return Err(EsError::unsupported("hybrid search over several indices"));
        };
        match request.retrievers.as_slice() {
            [Retriever::Vector { k, .. }] => {
                knn_k = Some(*k);
                request.limit = *k;
            }
            _ => request.limit = spec.from.saturating_add(spec.size),
        }
        request.offset = 0;
        if spec.user_sort_len > 0 {
            keys = request.sort.clone();
        }
        render.get_or_insert_with(|| spec.clone());
        calls.push(async move { run_single(gw, ctx, view, request, &spec).await });
    }
    // At most `msearch_concurrency` index searches at once, so an `_all`
    // search over many collections does not run them all together (PR #75
    // review); `buffered` keeps the part order.
    let parts = futures::stream::iter(calls)
        .buffered(gw.config().msearch_concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let render = render.ok_or_else(|| EsError::illegal_argument("no index to search"))?;
    Ok(Fanned {
        parts,
        render,
        keys,
        knn_k,
    })
}

/// Runs `body` over `indices` (items 2–5).
pub async fn execute(
    gw: &EsGateway,
    ctx: &RequestCtx,
    indices: &[IndexView],
    body: &Value,
    params: &SearchParams,
) -> Result<SearchOutcome, EsError> {
    let node = gw.config().node_name.as_str();
    if indices.is_empty() {
        return empty(ctx, body, params);
    }
    let now = now_ms();
    let mut plans = Vec::with_capacity(indices.len());
    for view in indices {
        let plan = super::compile(view, body, params, now).map_err(|e| renode(e, node))?;
        plans.push((view, plan));
    }
    if plans.len() > 1 {
        if let Some(why) = plans.iter().find_map(|(_, plan)| fan_out_refusal(plan)) {
            return Err(EsError::unsupported(why));
        }
        let n = plans.len();
        let Fanned {
            parts,
            render: spec,
            keys,
            knn_k,
        } = run_multi(gw, ctx, plans).await?;
        let mut total: Option<TotalHits> = None;
        let mut all: Vec<(String, Hit, f32)> = Vec::new();
        for part in parts {
            if let Some(t) = part.total {
                let sum = total.get_or_insert(TotalHits {
                    value: 0,
                    relation: TotalRelation::Eq,
                });
                sum.value += t.value;
                if t.relation == TotalRelation::Gte {
                    sum.relation = TotalRelation::Gte;
                }
            }
            for (hit, es) in part.hits {
                all.push((part.index.clone(), hit, es));
            }
        }
        let total = total.and_then(|t| {
            let capped = cap_total(t, spec.track)?;
            Some(TotalHits {
                relation: if t.relation == TotalRelation::Gte {
                    TotalRelation::Gte
                } else {
                    capped.relation
                },
                ..capped
            })
        });
        all.sort_by(|(i, a, x), (j, b, y)| {
            let by_keys = if keys.is_empty() {
                y.total_cmp(x)
            } else {
                compare_by_keys(&keys, (a, *x), (b, *y))
            };
            by_keys.then_with(|| a.pk.cmp(&b.pk)).then_with(|| i.cmp(j))
        });
        if let Some(k) = knn_k {
            all.truncate(k);
        }
        let page: Vec<(String, Hit, f32)> =
            all.into_iter().skip(spec.from).take(spec.size).collect();
        let views: HashMap<&str, &IndexView> =
            indices.iter().map(|v| (v.name.as_str(), v)).collect();
        let body = render_page(gw, ctx, &views, page, total, n, &spec).await?;
        return Ok(SearchOutcome {
            body,
            read_token: None,
        });
    }
    let (view, plan) = plans.pop().expect("one plan");
    let (part, render) = match plan {
        SearchPlan::Single { request, render } => {
            (run_single(gw, ctx, view, request, &render).await?, render)
        }
        SearchPlan::HybridSum {
            text,
            vectors,
            count_filter,
            render,
        } => (
            run_hybrid(gw, ctx, view, text, vectors, count_filter, &render).await?,
            render,
        ),
        SearchPlan::ScriptScore {
            request,
            count_filter,
            render,
            ..
        } => (
            run_script(gw, ctx, view, request, count_filter, &render).await?,
            render,
        ),
    };
    let token = part.token.clone();
    let page = part
        .hits
        .into_iter()
        .map(|(hit, es)| (part.index.clone(), hit, es))
        .collect();
    let views: HashMap<&str, &IndexView> = HashMap::from([(view.name.as_str(), view)]);
    let body = render_page(gw, ctx, &views, page, part.total, 1, &render).await?;
    Ok(SearchOutcome {
        body,
        read_token: Some(token),
    })
}

/// The answer over no index (an unmatched wildcard, or only missing names
/// under `ignore_unavailable`).
fn empty(ctx: &RequestCtx, body: &Value, params: &SearchParams) -> Result<SearchOutcome, EsError> {
    let track = track_total_hits(
        params
            .track_total_hits
            .as_ref()
            .or_else(|| body.get("track_total_hits")),
    )?;
    let spec = RenderSpec {
        source: Default::default(),
        score: EsScore::Bm25,
        scores_visible: true,
        sort_keys: Vec::new(),
        user_sort_len: 0,
        version: false,
        seq_no_primary_term: false,
        from: 0,
        size: 0,
        track,
        rest_total_hits_as_int: params.rest_total_hits_as_int,
        min_score: None,
        boost: 1.0,
    };
    let total = cap_total(
        TotalHits {
            value: 0,
            relation: TotalRelation::Eq,
        },
        track,
    );
    Ok(SearchOutcome {
        body: render_response(
            ctx.started.elapsed().as_millis(),
            0,
            total,
            None,
            Vec::new(),
            &spec,
        ),
        read_token: None,
    })
}

/// Renders a page of `(index, hit, ES score)`: `_seq_no` from one `get` per
/// index when `version` or `seq_no_primary_term` asks for it.
async fn render_page(
    gw: &EsGateway,
    ctx: &RequestCtx,
    views: &HashMap<&str, &IndexView>,
    page: Vec<(String, Hit, f32)>,
    total: Option<TotalHits>,
    n_indices: usize,
    spec: &RenderSpec,
) -> Result<Value, EsError> {
    let mut seq_nos: HashMap<(String, PrimaryKey), u64> = HashMap::new();
    if spec.version || spec.seq_no_primary_term {
        let mut by_index: HashMap<&str, Vec<PrimaryKey>> = HashMap::new();
        for (index, hit, _) in &page {
            by_index.entry(index).or_default().push(hit.pk.clone());
        }
        let select = Projection {
            source: IrSourceFilter::None,
            vectors: Vec::new(),
            fields: Vec::new(),
        };
        for (index, pks) in by_index {
            let docs = gw
                .service()
                .get(
                    &ctx.namespace,
                    index,
                    &pks,
                    &select,
                    ctx.consistency.clone(),
                )
                .await
                .map_err(|err| match views.get(index) {
                    Some(view) => search_error(err, view, &[], &gw.config().node_name),
                    None => EsError::from_service(err, ErrorContext::Read),
                })?;
            for (pk, doc) in pks.into_iter().zip(docs) {
                if let Some(doc) = doc {
                    seq_nos.insert((index.to_string(), pk), doc.seq_no);
                }
            }
        }
    }
    let max_score = page
        .iter()
        .map(|(_, _, es)| *es)
        .fold(None, |m: Option<f32>, s| Some(m.map_or(s, |m| m.max(s))));
    let hits = page
        .iter()
        .map(|(index, hit, es)| {
            let seq_no = seq_nos.get(&(index.clone(), hit.pk.clone())).copied();
            render_hit(index, hit, spec, Some(*es), seq_no)
        })
        .collect();
    Ok(render_response(
        ctx.started.elapsed().as_millis(),
        n_indices,
        total,
        max_score,
        hits,
        spec,
    ))
}

/// Counts the matches of `body`'s `query` (or `q`) over `indices` (item
/// 6).
pub async fn count(
    gw: &EsGateway,
    ctx: &RequestCtx,
    indices: &[IndexView],
    body: Option<&Value>,
    params: &SearchParams,
) -> Result<u64, EsError> {
    let query_body = match body {
        None | Some(Value::Null) => None,
        Some(Value::Object(map)) => {
            // ES's `_count` text (row T11-3).
            if let Some(key) = map.keys().find(|key| *key != "query") {
                return Err(EsError::parsing(format!(
                    "request does not support [{key}]"
                )));
            }
            map.get("query").filter(|v| !v.is_null())
        }
        Some(other) => {
            return Err(EsError::parsing(format!(
                "Expected [START_OBJECT] but found [{}]",
                token_name(other)
            )));
        }
    };
    let now = now_ms();
    let mut sum = 0u64;
    for view in indices {
        let qctx = QueryContext::new(view, now);
        let at_shard = |e: EsError| e.at_shard(&view.name, &gw.config().node_name);
        let query = match (&params.q, query_body) {
            (Some(q), _) => Some(url_query(q, params, &qctx).map_err(at_shard)?),
            (None, Some(v)) => {
                let parsed = parse_query(v, &qctx).map_err(at_shard)?;
                if !parsed.knn.is_empty() {
                    return Err(EsError::illegal_argument(
                        "[knn] is not supported by the count API",
                    ));
                }
                if parsed.script.is_some() {
                    return Err(EsError::illegal_argument(
                        "[script_score] is not supported by the count API",
                    ));
                }
                parsed.query
            }
            (None, None) => None,
        };
        let total = count_total(
            gw,
            ctx,
            view,
            query.unwrap_or(Query::MatchAll),
            ctx.consistency.clone(),
            TrackTotalHits::Exact,
        )
        .await?;
        sum += total.map_or(0, |t| t.value);
    }
    Ok(sum)
}

/// The index-expression options of a request's parameters.
pub(crate) fn resolve_options(params: &Params, search: &SearchParams) -> ResolveOptions {
    ResolveOptions {
        ignore_unavailable: search.ignore_unavailable,
        allow_no_indices: search.allow_no_indices,
        allow_wildcards: params
            .list("expand_wildcards")
            .is_none_or(|values| !values.iter().all(|v| v == "none")),
    }
}

/// The mappings of the indices `expr` covers.
pub(crate) async fn views(
    gw: &EsGateway,
    ctx: &RequestCtx,
    expr: &IndexExpr,
    opts: ResolveOptions,
) -> Result<Vec<IndexView>, EsError> {
    let resolved = resolve(gw.service(), &ctx.namespace, expr, opts).await?;
    let mut out = Vec::with_capacity(resolved.len());
    for target in resolved {
        match gw
            .service()
            .get_collection(&ctx.namespace, &target.name)
            .await
        {
            Ok(info) => out.push(IndexView::new(info)),
            Err(ServiceError::NotFound { .. }) if opts.ignore_unavailable => {}
            Err(err) => return Err(EsError::from_service(err, ErrorContext::Read)),
        }
    }
    Ok(out)
}

pub(crate) fn expr_of(index: Option<&str>) -> IndexExpr {
    match index {
        Some(index) => IndexExpr::parse(&crate::http::percent_decode_path(index)),
        None => IndexExpr::All,
    }
}

/// An answer with `Loams-Consistency-Token` when there is one.
fn with_token(mut response: Response, token: Option<&ConsistencyToken>) -> Response {
    if let Some(token) = token
        && let Ok(value) = HeaderValue::from_str(&token.to_string())
    {
        response.headers_mut().insert(TOKEN_HEADER, value);
    }
    response
}

async fn search_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    body: &[u8],
) -> Result<SearchOutcome, EsError> {
    let params = Params::parse(uri.query(), uri.path(), SEARCH_PARAMS)?;
    let search = parse_params(&params)?;
    let body = json_body(body)?.unwrap_or(Value::Null);
    let indices = views(gw, ctx, &expr_of(index), resolve_options(&params, &search)).await?;
    execute(gw, ctx, &indices, &body, &search).await
}

async fn search_answer(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    body: &[u8],
) -> Response {
    match search_request(gw, ctx, uri, index, body).await {
        Ok(outcome) => with_token(
            respond(ctx, 200, &outcome.body),
            outcome.read_token.as_ref(),
        ),
        Err(err) => fail(ctx, &err),
    }
}

/// `GET|POST /_search`.
pub(crate) async fn search_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    search_answer(&gw, &ctx, &uri, None, &body).await
}

/// `GET|POST /{index}/_search`.
pub(crate) async fn search_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    search_answer(&gw, &ctx, &uri, Some(&index), &body).await
}

async fn count_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    body: &[u8],
) -> Result<Value, EsError> {
    let params = Params::parse(uri.query(), uri.path(), COUNT_PARAMS)?;
    for phase_b in ["min_score", "terminate_after"] {
        if params.str(phase_b).is_some() {
            return Err(EsError::unsupported(phase_b));
        }
    }
    let search = parse_params(&params)?;
    let body = json_body(body)?;
    let indices = views(gw, ctx, &expr_of(index), resolve_options(&params, &search)).await?;
    let n = count(gw, ctx, &indices, body.as_ref(), &search).await?;
    Ok(json!({"count": n, "_shards": shards(indices.len())}))
}

async fn count_answer(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    body: &[u8],
) -> Response {
    match count_request(gw, ctx, uri, index, body).await {
        Ok(body) => respond(ctx, 200, &body),
        Err(err) => fail(ctx, &err),
    }
}

/// `GET|POST /_count`.
pub(crate) async fn count_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    count_answer(&gw, &ctx, &uri, None, &body).await
}

/// `GET|POST /{index}/_count`.
pub(crate) async fn count_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    count_answer(&gw, &ctx, &uri, Some(&index), &body).await
}

/// One `_msearch` search: its index expression, options and body, or the
/// error of its malformed pair.
struct MsearchItem {
    expr: IndexExpr,
    opts: ResolveOptions,
    body: Value,
}

/// A header's boolean option (`true`/`false` or their strings).
fn header_bool(header: &Map<String, Value>, key: &str) -> Result<Option<bool>, EsError> {
    match header.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(Value::String(s)) if s == "true" => Ok(Some(true)),
        Some(Value::String(s)) if s == "false" => Ok(Some(false)),
        Some(other) => Err(EsError::illegal_argument(format!(
            "Failed to parse value [{}] as only [true] or [false] are allowed.",
            match other {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        ))),
    }
}

/// A header's string-or-array value, joined by commas.
fn header_list(header: &Map<String, Value>, key: &str) -> Result<Option<String>, EsError> {
    match header.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    EsError::parsing(format!("[{key}] must be a string or an array of strings"))
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|items| Some(items.join(","))),
        Some(other) => Err(EsError::parsing(format!(
            "[{key}] must be a string or an array of strings, found [{}]",
            token_name(other)
        ))),
    }
}

/// One header/body pair of an `_msearch` body (item 7). ES parses the
/// whole body before searching, so an error here fails the request (row
/// T11-3).
fn msearch_item(
    header: &str,
    body: &str,
    path_index: Option<&str>,
    defaults: ResolveOptions,
) -> Result<MsearchItem, EsError> {
    let header = match json_body(header.as_bytes())? {
        None => Map::new(),
        Some(Value::Object(map)) => map,
        Some(other) => {
            return Err(EsError::parsing(format!(
                "Expected [START_OBJECT] but found [{}] in the msearch header",
                token_name(&other)
            )));
        }
    };
    for key in header.keys() {
        if !MSEARCH_HEADER_KEYS.contains(&key.as_str()) {
            return Err(EsError::illegal_argument(format!(
                "key [{key}] is not supported in the metadata section"
            )));
        }
    }
    if let Some(kind) = header.get("search_type") {
        match kind.as_str() {
            Some("query_then_fetch" | "dfs_query_then_fetch") => {}
            _ => {
                return Err(EsError::illegal_argument(format!(
                    "No search type for [{}]",
                    kind.as_str()
                        .map_or_else(|| kind.to_string(), str::to_string)
                )));
            }
        }
    }
    header_bool(&header, "request_cache")?;
    let expr = match header_list(&header, "index")? {
        Some(index) => IndexExpr::parse(&index),
        None => expr_of(path_index),
    };
    let opts = ResolveOptions {
        ignore_unavailable: header_bool(&header, "ignore_unavailable")?
            .unwrap_or(defaults.ignore_unavailable),
        allow_no_indices: header_bool(&header, "allow_no_indices")?
            .unwrap_or(defaults.allow_no_indices),
        allow_wildcards: match header_list(&header, "expand_wildcards")? {
            Some(values) => !values.split(',').all(|v| v == "none"),
            None => defaults.allow_wildcards,
        },
    };
    let body = json_body(body.as_bytes())?.unwrap_or(Value::Null);
    Ok(MsearchItem { expr, opts, body })
}

/// An `_msearch` entry: a search answer with `status`, or an error.
fn msearch_entry(result: Result<SearchOutcome, EsError>) -> Value {
    match result {
        Ok(outcome) => {
            let mut body = outcome.body;
            if let Some(obj) = body.as_object_mut() {
                obj.insert("status".to_string(), json!(200));
            }
            body
        }
        Err(err) => {
            let body = err.to_body();
            json!({"error": body["error"].clone(), "status": err.status})
        }
    }
}

async fn msearch_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    path_index: Option<&str>,
    body: &[u8],
) -> Result<Value, EsError> {
    let params = Params::parse(uri.query(), uri.path(), MSEARCH_PARAMS)?;
    if params.str("typed_keys").is_some() {
        return Err(EsError::unsupported("typed_keys"));
    }
    params.bool("ccs_minimize_roundtrips")?;
    if let Some(kind) = params.str("search_type")
        && !matches!(kind, "query_then_fetch" | "dfs_query_then_fetch")
    {
        return Err(EsError::illegal_argument(format!(
            "No search type for [{kind}]"
        )));
    }
    let search = SearchParams {
        rest_total_hits_as_int: params.bool("rest_total_hits_as_int")?.unwrap_or(false),
        ignore_unavailable: params.bool("ignore_unavailable")?.unwrap_or(false),
        allow_no_indices: params.bool("allow_no_indices")?.unwrap_or(true),
        ..SearchParams::default()
    };
    let defaults = resolve_options(&params, &search);
    let concurrency = params
        .usize("max_concurrent_searches")?
        .unwrap_or(usize::MAX)
        .min(gw.config().msearch_concurrency)
        .max(1);
    let text =
        std::str::from_utf8(body).map_err(|_| EsError::parsing("the msearch body is not UTF-8"))?;
    let lines: Vec<&str> = text
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    // A trailing newline leaves one empty last line.
    let lines = match lines.split_last() {
        Some((last, rest)) if last.trim().is_empty() => rest,
        _ => &lines[..],
    };
    // A last header without a body line is dropped, as ES drops it.
    let pairs: Vec<(&str, &str)> = lines
        .chunks(2)
        .filter_map(|pair| Some((pair[0], *pair.get(1)?)))
        .collect();
    if pairs.is_empty() {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: no requests added;",
        ));
    }
    if body.last() != Some(&b'\n') {
        // ES's text holds a real newline between the brackets.
        return Err(EsError::illegal_argument(
            "The msearch request must be terminated by a newline [\n]",
        ));
    }
    let items: Vec<Result<MsearchItem, EsError>> = pairs
        .into_iter()
        .map(|(header, body)| msearch_item(header, body, path_index, defaults).map(Ok))
        .collect::<Result<_, _>>()?;
    let search = &search;
    let responses: Vec<Value> = futures::stream::iter(items)
        .map(|item| async move {
            let result = match item {
                Ok(item) => match views(gw, ctx, &item.expr, item.opts).await {
                    Ok(indices) => execute(gw, ctx, &indices, &item.body, search).await,
                    Err(err) => Err(err),
                },
                Err(err) => Err(err),
            };
            msearch_entry(result)
        })
        .buffered(concurrency)
        .collect()
        .await;
    Ok(json!({
        "took": u64::try_from(ctx.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "responses": responses,
    }))
}

async fn msearch_answer(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    body: &[u8],
) -> Response {
    match msearch_request(gw, ctx, uri, index, body).await {
        Ok(body) => respond(ctx, 200, &body),
        Err(err) => fail(ctx, &err),
    }
}

/// `GET|POST /_msearch`.
pub(crate) async fn msearch_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    msearch_answer(&gw, &ctx, &uri, None, &body).await
}

/// `GET|POST /{index}/_msearch`.
pub(crate) async fn msearch_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    msearch_answer(&gw, &ctx, &uri, Some(&index), &body).await
}

#[cfg(test)]
mod tests {
    use super::super::body::SEARCH_NODE;
    use super::*;

    #[test]
    fn the_hybrid_count_filter_takes_the_knn_hits() {
        let pks = vec![PrimaryKey::Str("a".into())];
        assert_eq!(
            fill_ids(Query::Ids(Vec::new()), pks.clone()),
            Query::Ids(pks.clone())
        );
        let bool = |ids| Query::Bool {
            must: Vec::new(),
            should: vec![Query::MatchAll, Query::Ids(ids)],
            must_not: Vec::new(),
            filter: Vec::new(),
            minimum_should_match: None,
        };
        assert_eq!(fill_ids(bool(Vec::new()), pks.clone()), bool(pks));
    }

    #[test]
    fn a_search_phase_error_names_the_configured_node() {
        let err = EsError::search_phase(EsError::parsing("x"), "i", SEARCH_NODE);
        let err = renode(err, "node-7");
        assert_eq!(err.extra["failed_shards"][0]["node"], "node-7");
    }

    #[test]
    fn query_strings_are_found_anywhere_in_the_request() {
        let mut request = SearchRequest::new("i");
        request.filter = Some(Query::Bool {
            must: vec![Query::QueryString {
                query: "a:".into(),
                default_fields: Vec::new(),
                default_operator: Default::default(),
            }],
            should: Vec::new(),
            must_not: Vec::new(),
            filter: Vec::new(),
            minimum_should_match: None,
        });
        assert_eq!(request_query_strings(&request), vec!["a:".to_string()]);
    }
}
