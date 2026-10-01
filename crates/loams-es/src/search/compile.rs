//! A search body compiled to a [`SearchPlan`] (plan M1.5 Task 8 items 2,
//! 4, 5, 8 and 9; Rulings 3, 12 and 20).

use loams_collection::Distance;
use loams_query::{
    AnnParams, Fusion, Projection, Query, Retriever, SearchRequest, SortKey, SortOrder,
    SourceFilter as IrSourceFilter, TrackTotalHits,
};
use serde_json::{Map, Value, json};

use super::body::{
    SEARCH_NODE, body_sort, check_keys, count, flag, search_after_filter, track_total_hits,
    url_sort,
};
use super::{EsScore, RenderSpec, SearchParams, SearchPlan};
use crate::doc::SourceFilter;
use crate::dsl::query::token_name;
use crate::dsl::{
    KnnSpec, ParsedQuery, QueryContext, ScriptFunction, parse_filter_list, parse_knn, parse_leaf,
    parse_query,
};
use crate::error::EsError;
use crate::mapping::{EsSimilarity, IndexView};

/// The default `rank_constant` of RRF.
const RANK_CONSTANT: u32 = 60;

fn and(mut parts: Vec<Query>) -> Option<Query> {
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(Query::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: parts,
            minimum_should_match: None,
        }),
    }
}

/// The engine distance of a script function.
fn script_distance(function: ScriptFunction) -> Distance {
    match function {
        ScriptFunction::CosinePlusOne => Distance::Cosine,
        ScriptFunction::InverseOnePlusL2 => Distance::Euclid,
        ScriptFunction::SigmoidDot => Distance::Dot,
    }
}

/// The engine score below which a script's score is under `s`.
fn script_threshold(function: ScriptFunction, s: f32) -> Option<f32> {
    match function {
        ScriptFunction::CosinePlusOne => Some(s - 1.0),
        ScriptFunction::InverseOnePlusL2 => {
            if s <= 0.0 {
                None
            } else {
                Some(1.0 - 1.0 / s)
            }
        }
        ScriptFunction::SigmoidDot => {
            if s <= 0.0 {
                None
            } else if s >= 1.0 {
                Some(f32::MAX)
            } else {
                Some((s / (1.0 - s)).ln())
            }
        }
    }
}

/// The engine score of a knn hit whose ES score is `s` (item 8).
fn knn_min_score(similarity: EsSimilarity, s: f32) -> Result<Option<f32>, EsError> {
    Ok(match similarity {
        EsSimilarity::Cosine | EsSimilarity::DotProduct => Some(2.0 * s - 1.0),
        EsSimilarity::L2Norm => {
            if s <= 0.0 {
                None
            } else if s > 1.0 {
                Some(f32::MAX)
            } else {
                Some(-(1.0 / s - 1.0).sqrt())
            }
        }
        EsSimilarity::MaxInnerProduct => {
            if s <= 0.0 {
                None
            } else if s < 1.0 {
                Some(1.0 - 1.0 / s)
            } else {
                Some(s - 1.0)
            }
        }
        EsSimilarity::L1Norm => return Err(EsError::unsupported("min_score on an l1_norm vector")),
    })
}

/// The engine threshold of `knn.similarity` (item 5).
fn knn_similarity(similarity: EsSimilarity, t: f32) -> Result<f32, EsError> {
    match similarity {
        EsSimilarity::Cosine | EsSimilarity::DotProduct | EsSimilarity::MaxInnerProduct => Ok(t),
        EsSimilarity::L2Norm => Ok(-t),
        EsSimilarity::L1Norm => Err(EsError::unsupported("knn.similarity on an l1_norm vector")),
    }
}

fn stricter(a: Option<f32>, b: Option<f32>) -> Option<f32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn vector_retriever(spec: &KnnSpec, k: usize) -> Retriever {
    Retriever::Vector {
        field: spec.field.clone(),
        query: spec.query_vector.clone(),
        k,
        params: AnnParams {
            ef: Some(u32::try_from(spec.num_candidates).unwrap_or(u32::MAX)),
            ..AnnParams::default()
        },
        filter: spec.filter.clone(),
    }
}

/// Everything the cases share.
struct Shape<'a> {
    view: &'a IndexView,
    select: Projection,
    track: TrackTotalHits,
    render: RenderSpec,
}

impl Shape<'_> {
    fn request(&self) -> SearchRequest {
        let mut request = SearchRequest::new(self.view.name.clone());
        request.select = self.select.clone();
        request.track_total_hits = self.track;
        request
    }

    /// A request of one sub-search of a hybrid sum: its top `k`, uncounted.
    fn part(&self, retriever: Retriever, k: usize) -> SearchRequest {
        let mut request = self.request();
        request.retrievers = vec![retriever];
        request.limit = k;
        request.track_total_hits = TrackTotalHits::None;
        request
    }

    fn render(&self, score: EsScore) -> RenderSpec {
        RenderSpec {
            score,
            ..self.render.clone()
        }
    }
}

/// The `query_string` query of the URL parameter `q` (Ruling 20), with
/// `df` and `default_operator`; `_count` uses it too.
pub(crate) fn url_query(
    q: &str,
    params: &SearchParams,
    ctx: &QueryContext<'_>,
) -> Result<Query, EsError> {
    let mut qs = Map::new();
    qs.insert("query".to_string(), json!(q));
    if let Some(df) = &params.df {
        qs.insert("default_field".to_string(), json!(df));
    }
    if let Some(op) = params.default_operator {
        let op = match op {
            loams_query::BoolOperator::Or => "or",
            loams_query::BoolOperator::And => "and",
        };
        qs.insert("default_operator".to_string(), json!(op));
    }
    parse_leaf(&json!({"query_string": qs}), ctx)
}

/// `boost` when it is positive, else 1 (a threshold divides by it).
fn positive(boost: f32) -> f32 {
    if boost > 0.0 { boost } else { 1.0 }
}

/// Compiles the search `body` (with the URL `params`, Ruling 20) over
/// `index` (Task 8). A failure ES raises on the shard comes wrapped in
/// `search_phase_execution_exception` ([`EsError::at_shard`]).
pub fn compile(
    index: &IndexView,
    body: &Value,
    params: &SearchParams,
    now_ms: i64,
) -> Result<SearchPlan, EsError> {
    compile_body(index, body, params, now_ms).map_err(|e| e.at_shard(&index.name, SEARCH_NODE))
}

fn compile_body(
    index: &IndexView,
    body: &Value,
    params: &SearchParams,
    now_ms: i64,
) -> Result<SearchPlan, EsError> {
    let empty = Map::new();
    let body = match body {
        Value::Null => &empty,
        Value::Object(map) => map,
        other => {
            return Err(EsError::parsing(format!(
                "Expected [START_OBJECT] but found [{}]",
                token_name(other)
            )));
        }
    };
    check_keys(body)?;
    let from = match params.from {
        Some(n) => n,
        None => count(body, "from")?.unwrap_or(0),
    };
    let size = match params.size {
        Some(n) => n,
        None => count(body, "size")?.unwrap_or(10),
    };
    let window = from.saturating_add(size);
    if window > index.es.max_result_window {
        return Err(EsError::search_phase(
            EsError::illegal_argument(format!(
                "Result window is too large, from + size must be less than or equal to: [{}] but \
                 was [{window}]. See the scroll api for a more efficient way to request large \
                 data sets. This limit can be set by changing the [index.max_result_window] \
                 index level setting.",
                index.es.max_result_window
            )),
            &index.name,
            SEARCH_NODE,
        ));
    }
    let source = match &params.source {
        Some(filter) => filter.clone(),
        None => match body.get("_source") {
            Some(v) => SourceFilter::from_body(v)?,
            None => SourceFilter::default(),
        },
    };
    let track = track_total_hits(
        params
            .track_total_hits
            .as_ref()
            .or(body.get("track_total_hits")),
    )?;
    let track_scores = match params.track_scores {
        Some(b) => b,
        None => flag(body, "track_scores")?,
    };
    let min_score = match body.get("min_score") {
        None | Some(Value::Null) => None,
        Some(v) => Some(crate::dsl::query::number_f32("search", "min_score", v)?),
    };
    // Sort (item 3).
    let (sort_given, keys) = match (&params.sort, body.get("sort")) {
        (Some(list), _) => (!list.is_empty(), url_sort(index, list)?),
        (None, Some(v)) if !v.is_null() => (
            !matches!(v, Value::Array(items) if items.is_empty()),
            body_sort(index, v)?,
        ),
        _ => (false, Vec::new()),
    };
    let score_first = matches!(keys.first(), Some(SortKey::Score { .. }));
    let field_first = sort_given && !score_first;
    if keys
        .iter()
        .skip(1)
        .any(|k| matches!(k, SortKey::Score { .. }))
    {
        return Err(EsError::unsupported("sort on _score after another key"));
    }
    // The engine breaks score ties by PK only: a field key after `_score`
    // would be ignored and its sort values unknown (PR #74 review).
    if score_first
        && keys
            .iter()
            .skip(1)
            .any(|k| matches!(k, SortKey::Field { .. }))
    {
        return Err(EsError::unsupported("sort on a field after _score"));
    }
    if field_first && track_scores {
        return Err(EsError::unsupported("track_scores with a field sort"));
    }
    let scores_visible = !field_first;
    let vectors = index
        .es
        .vectors
        .keys()
        .filter(|path| source.keeps_path(path))
        .cloned()
        .collect();
    let select = Projection {
        source: if source.enabled {
            IrSourceFilter::All
        } else {
            IrSourceFilter::None
        },
        vectors,
        fields: Vec::new(),
    };
    let user_sort_len = if sort_given { keys.len() } else { 0 };
    let shape = Shape {
        view: index,
        select,
        track,
        render: RenderSpec {
            source,
            score: EsScore::Bm25,
            scores_visible,
            sort_keys: keys.clone(),
            user_sort_len,
            version: flag(body, "version")?,
            seq_no_primary_term: flag(body, "seq_no_primary_term")?,
            from,
            size,
            track,
            rest_total_hits_as_int: params.rest_total_hits_as_int,
            min_score,
            boost: 1.0,
        },
    };
    let ctx = QueryContext {
        view: index,
        now_ms,
        depth: 0,
        default_k: size,
    };
    // The query (`q` replaces it, Ruling 20).
    let parsed = match (&params.q, body.get("query")) {
        (Some(q), _) => ParsedQuery {
            query: Some(url_query(q, params, &ctx)?),
            knn: Vec::new(),
            script: None,
        },
        (None, Some(v)) if !v.is_null() => parse_query(v, &ctx)?,
        _ => ParsedQuery {
            query: None,
            knn: Vec::new(),
            script: None,
        },
    };
    let has_query = params.q.is_some() || body.get("query").is_some_and(|v| !v.is_null());
    let mut knn = match body.get("knn") {
        Some(v) if !v.is_null() => parse_knn(v, &ctx, size)?,
        _ => Vec::new(),
    };
    knn.extend(parsed.knn);
    // `search_after` (item 6).
    let after = match body.get("search_after") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => {
            if from != 0 {
                return Err(EsError::new(
                    400,
                    "action_request_validation_exception",
                    "Validation Failed: 1: [from] parameter must be set to 0 when [search_after] \
                     is used;",
                ));
            }
            if !field_first {
                return Err(EsError::unsupported("search_after on _score"));
            }
            Some(search_after_filter(index, &keys, values, now_ms)?)
        }
        Some(other) => {
            return Err(EsError::parsing(format!(
                "[search_after] must be an array, found [{}]",
                token_name(other)
            )));
        }
    };
    // `retriever` and legacy `rank` (item 5).
    if let Some(retriever) = body.get("retriever") {
        if has_query {
            return Err(EsError::illegal_argument(
                "cannot specify [retriever] and [query]",
            ));
        }
        if !knn.is_empty() {
            return Err(EsError::illegal_argument(
                "cannot specify [retriever] and [knn]",
            ));
        }
        if body.contains_key("rank") {
            return Err(EsError::illegal_argument(
                "cannot specify [retriever] and [rank]",
            ));
        }
        if sort_given || after.is_some() {
            return Err(EsError::unsupported("sort with a retriever"));
        }
        return compile_retriever(&shape, &ctx, retriever, min_score);
    }
    if let Some(rank) = body.get("rank") {
        if sort_given || after.is_some() {
            return Err(EsError::unsupported("sort with rank"));
        }
        if parsed.script.is_some() {
            return Err(EsError::unsupported("rank with script_score"));
        }
        return legacy_rank(&shape, rank, parsed.query, knn, min_score);
    }
    if let Some(script) = parsed.script {
        if !knn.is_empty() {
            return Err(EsError::unsupported("script_score with knn"));
        }
        if field_first {
            return Err(EsError::unsupported("script_score with a field sort"));
        }
        let mut request = shape.request();
        request.retrievers = vec![Retriever::Vector {
            field: script.field.clone(),
            query: script.query_vector.clone(),
            k: window.max(1),
            params: AnnParams {
                exact: true,
                distance: Some(script_distance(script.function)),
                ..AnnParams::default()
            },
            filter: Some(script.filter.clone()),
        }];
        request.offset = from;
        request.limit = size;
        let boost = positive(script.boost);
        let threshold =
            |s: Option<f32>| s.and_then(|s| script_threshold(script.function, s / boost));
        request.score_threshold = stricter(threshold(script.min_score), threshold(min_score));
        return Ok(SearchPlan::ScriptScore {
            request,
            function: script.function,
            count_filter: script.filter,
            render: RenderSpec {
                boost: script.boost,
                ..shape.render(EsScore::Script(script.function))
            },
        });
    }
    if field_first {
        if !knn.is_empty() {
            return Err(EsError::unsupported("knn with a field sort"));
        }
        if min_score.is_some() {
            return Err(EsError::unsupported("min_score with a field sort"));
        }
        let mut request = shape.request();
        request.filter = and(parsed.query.into_iter().chain(after).collect());
        request.sort = keys.clone();
        if !keys.iter().any(|k| matches!(k, SortKey::Pk { .. })) {
            request.sort.push(SortKey::Pk {
                order: SortOrder::Asc,
            });
        }
        request.offset = from;
        request.limit = size;
        return Ok(SearchPlan::Single {
            request,
            render: shape.render(EsScore::Bm25),
        });
    }
    // A `_score`-first sort keeps every user key, then the PK tie-break.
    let score_sort = |request: &mut SearchRequest| {
        if sort_given {
            request.sort = keys.clone();
            if !keys.iter().any(|k| matches!(k, SortKey::Pk { .. })) {
                request.sort.push(SortKey::Pk {
                    order: SortOrder::Asc,
                });
            }
        }
    };
    match (parsed.query, knn.len()) {
        // Query only, or nothing (match_all).
        (query, 0) => {
            let mut request = shape.request();
            request.retrievers = vec![Retriever::Text {
                query: query.unwrap_or(Query::MatchAll),
                k: window.max(1),
            }];
            request.offset = from;
            request.limit = size;
            request.score_threshold = min_score;
            score_sort(&mut request);
            Ok(SearchPlan::Single {
                request,
                render: shape.render(EsScore::Bm25),
            })
        }
        // One knn search.
        (None, 1) => {
            let spec = &knn[0];
            let similarity = index.es.vectors[&spec.field].similarity;
            let mut request = shape.request();
            request.retrievers = vec![vector_retriever(spec, spec.k)];
            request.offset = from;
            request.limit = size.min(spec.k.saturating_sub(from));
            let by_similarity = spec
                .similarity
                .map(|t| knn_similarity(similarity, t))
                .transpose()?;
            // `min_score` is in boosted ES score space.
            let by_score = match min_score {
                Some(s) => knn_min_score(similarity, s / positive(spec.boost))?,
                None => None,
            };
            request.score_threshold = stricter(by_similarity, by_score);
            score_sort(&mut request);
            Ok(SearchPlan::Single {
                request,
                render: RenderSpec {
                    boost: spec.boost,
                    ..shape.render(EsScore::Knn(similarity))
                },
            })
        }
        // The ES score sum of a query and knn searches (Ruling 3).
        (query, _) => {
            if knn.iter().any(|k| k.similarity.is_some()) {
                return Err(EsError::unsupported(
                    "knn.similarity with several retrievers",
                ));
            }
            if min_score.is_some() {
                return Err(EsError::unsupported("min_score with several retrievers"));
            }
            let total_k: usize = knn.iter().map(|k| k.k).sum();
            let vectors = knn
                .iter()
                .map(|spec| {
                    let similarity = index.es.vectors[&spec.field].similarity;
                    (
                        shape.part(vector_retriever(spec, spec.k), spec.k),
                        similarity,
                        spec.boost,
                    )
                })
                .collect();
            let ids = Query::Ids(Vec::new());
            let (text, count_filter) = match query {
                Some(query) => {
                    let k = window.saturating_add(total_k).max(1);
                    let count_filter = Query::Bool {
                        must: Vec::new(),
                        should: vec![query.clone(), ids],
                        must_not: Vec::new(),
                        filter: Vec::new(),
                        minimum_should_match: None,
                    };
                    (
                        Some((shape.part(Retriever::Text { query, k }, k), 1.0)),
                        count_filter,
                    )
                }
                None => (None, ids),
            };
            Ok(SearchPlan::HybridSum {
                text,
                vectors,
                count_filter,
                render: shape.render(EsScore::Sum),
            })
        }
    }
}

/// `rank_constant` and `rank_window_size` of an rrf object.
fn rrf_params(obj: &Map<String, Value>, window: usize) -> Result<(u32, usize), EsError> {
    let rank_constant = match obj.get("rank_constant") {
        None => RANK_CONSTANT as i64,
        Some(v) => crate::dsl::query::number_int("rrf", "rank_constant", v)?,
    };
    if rank_constant < 1 {
        return Err(EsError::illegal_argument(
            "[rank_constant] must be greater or equal to [1]",
        ));
    }
    let rank_window_size = match obj.get("rank_window_size").or(obj.get("window_size")) {
        None => window.max(10),
        Some(v) => usize::try_from(crate::dsl::query::number_int("rrf", "rank_window_size", v)?)
            .unwrap_or(0),
    };
    if window > rank_window_size {
        return Err(EsError::illegal_argument(
            "[rank_window_size] must be greater than or equal to [from + size]",
        ));
    }
    Ok((
        u32::try_from(rank_constant).unwrap_or(u32::MAX),
        rank_window_size,
    ))
}

/// The single RRF request over `children`.
fn rrf_plan(shape: &Shape<'_>, children: Vec<Retriever>, rank_constant: u32) -> SearchPlan {
    let mut request = shape.request();
    request.retrievers = children;
    request.fusion = Some(Fusion::Rrf { k: rank_constant });
    request.offset = shape.render.from;
    request.limit = shape.render.size;
    SearchPlan::Single {
        request,
        render: shape.render(EsScore::Rrf),
    }
}

/// Legacy `rank: {"rrf": {…}}` with a top-level `query` and `knn` (C25).
fn legacy_rank(
    shape: &Shape<'_>,
    rank: &Value,
    query: Option<Query>,
    knn: Vec<KnnSpec>,
    min_score: Option<f32>,
) -> Result<SearchPlan, EsError> {
    let Some(map) = rank.as_object() else {
        return Err(EsError::parsing("[rank] must be an object"));
    };
    let mut keys = map.iter();
    let rrf = match keys.next() {
        Some((name, v)) if name == "rrf" => v,
        Some((name, _)) => return Err(EsError::unsupported(&format!("rank.{name}"))),
        None => return Err(EsError::parsing("[rank] must name a ranking method")),
    };
    if let Some((name, _)) = keys.next() {
        return Err(EsError::unsupported(&format!("rank.{name}")));
    }
    let empty = Map::new();
    let obj = match rrf {
        Value::Object(obj) => obj,
        Value::Null => &empty,
        _ => return Err(EsError::parsing("[rrf] must be an object")),
    };
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "rank_constant" | "rank_window_size" | "window_size"
        ) {
            return Err(EsError::parsing(format!("[rrf] unknown field [{key}]")));
        }
    }
    if min_score.is_some() {
        return Err(EsError::unsupported("min_score with rrf"));
    }
    let window = shape.render.from.saturating_add(shape.render.size);
    let (rank_constant, rank_window_size) = rrf_params(obj, window)?;
    if usize::from(query.is_some()) + knn.len() < 2 {
        return Err(EsError::illegal_argument(
            "[rank] requires a minimum of [2] result sets using a combination of sub searches \
             and/or knn searches",
        ));
    }
    if knn.iter().any(|k| k.similarity.is_some()) {
        return Err(EsError::unsupported("knn.similarity with rrf"));
    }
    let mut children = Vec::new();
    if let Some(query) = query {
        children.push(Retriever::Text {
            query,
            k: rank_window_size.max(1),
        });
    }
    for spec in &knn {
        children.push(vector_retriever(spec, spec.k.min(rank_window_size).max(1)));
    }
    Ok(rrf_plan(shape, children, rank_constant))
}

/// A body's `retriever` (item 5).
fn compile_retriever(
    shape: &Shape<'_>,
    ctx: &QueryContext<'_>,
    retriever: &Value,
    min_score: Option<f32>,
) -> Result<SearchPlan, EsError> {
    let (kind, obj) = retriever_kind(retriever)?;
    let window = shape.render.from.saturating_add(shape.render.size);
    match kind {
        "rrf" => {
            if min_score.is_some() {
                return Err(EsError::unsupported("min_score with rrf"));
            }
            let (children, rank_constant, _) = rrf_children(ctx, obj, window, None)?;
            Ok(rrf_plan(shape, children, rank_constant))
        }
        "standard" => {
            let query = standard_query(ctx, obj, None)?;
            let mut request = shape.request();
            request.retrievers = vec![Retriever::Text {
                query,
                k: window.max(1),
            }];
            request.offset = shape.render.from;
            request.limit = shape.render.size;
            request.score_threshold = min_score;
            Ok(SearchPlan::Single {
                request,
                render: shape.render(EsScore::Bm25),
            })
        }
        _ => {
            let spec = retriever_knn(ctx, obj, None)?;
            let similarity = shape.view.es.vectors[&spec.field].similarity;
            let mut request = shape.request();
            request.retrievers = vec![vector_retriever(&spec, spec.k)];
            request.offset = shape.render.from;
            request.limit = shape
                .render
                .size
                .min(spec.k.saturating_sub(shape.render.from));
            let by_similarity = spec
                .similarity
                .map(|t| knn_similarity(similarity, t))
                .transpose()?;
            let by_score = match min_score {
                Some(s) => knn_min_score(similarity, s / positive(spec.boost))?,
                None => None,
            };
            request.score_threshold = stricter(by_similarity, by_score);
            Ok(SearchPlan::Single {
                request,
                render: RenderSpec {
                    boost: spec.boost,
                    ..shape.render(EsScore::Knn(similarity))
                },
            })
        }
    }
}

/// The type and body of a retriever object: `standard`, `knn` or `rrf`.
fn retriever_kind(v: &Value) -> Result<(&str, &Map<String, Value>), EsError> {
    let Some(map) = v.as_object() else {
        return Err(EsError::parsing("[retriever] must be an object"));
    };
    let mut keys = map.iter();
    let Some((kind, body)) = keys.next() else {
        return Err(EsError::parsing("retriever malformed, empty clause found"));
    };
    if keys.next().is_some() {
        return Err(EsError::parsing(format!(
            "[{kind}] malformed retriever, expected [END_OBJECT] but found [FIELD_NAME]"
        )));
    }
    if !matches!(kind.as_str(), "standard" | "knn" | "rrf") {
        return Err(EsError::unsupported(&format!("retriever.{kind}")));
    }
    let Some(body) = body.as_object() else {
        return Err(EsError::parsing(format!("[{kind}] must be an object")));
    };
    Ok((kind.as_str(), body))
}

/// A `standard` retriever's query, with its `filter` and the enclosing
/// rrf's.
fn standard_query(
    ctx: &QueryContext<'_>,
    obj: &Map<String, Value>,
    outer: Option<&Query>,
) -> Result<Query, EsError> {
    let mut query = None;
    let mut filters = Vec::new();
    for (key, value) in obj {
        match key.as_str() {
            "query" => query = Some(parse_leaf(value, ctx)?),
            "filter" => filters.extend(parse_filter_list(value, ctx)?),
            "_name" => {}
            _ => return Err(EsError::unsupported(&format!("retriever.standard.{key}"))),
        }
    }
    filters.extend(outer.cloned());
    let query = query.unwrap_or(Query::MatchAll);
    if filters.is_empty() {
        return Ok(query);
    }
    Ok(Query::Bool {
        must: vec![query],
        should: Vec::new(),
        must_not: Vec::new(),
        filter: filters,
        minimum_should_match: None,
    })
}

/// A `knn` retriever, with the enclosing rrf's filter.
fn retriever_knn(
    ctx: &QueryContext<'_>,
    obj: &Map<String, Value>,
    outer: Option<&Query>,
) -> Result<KnnSpec, EsError> {
    let mut specs = parse_knn(&Value::Object(obj.clone()), ctx, ctx.default_k)?;
    let mut spec = specs
        .pop()
        .ok_or_else(|| EsError::parsing("[knn] is empty"))?;
    let mut parts: Vec<Query> = spec.filter.take().into_iter().collect();
    parts.extend(outer.cloned());
    spec.filter = and(parts);
    Ok(spec)
}

/// The children of an rrf retriever, its `rank_constant` and
/// `rank_window_size`.
fn rrf_children(
    ctx: &QueryContext<'_>,
    obj: &Map<String, Value>,
    window: usize,
    outer: Option<&Query>,
) -> Result<(Vec<Retriever>, u32, usize), EsError> {
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "retrievers" | "rank_constant" | "rank_window_size" | "filter" | "_name"
        ) {
            return Err(EsError::unsupported(&format!("retriever.rrf.{key}")));
        }
    }
    let (rank_constant, rank_window_size) = rrf_params(obj, window)?;
    let mut filters: Vec<Query> = match obj.get("filter") {
        Some(v) => parse_filter_list(v, ctx)?.into_iter().collect(),
        None => Vec::new(),
    };
    filters.extend(outer.cloned());
    let filter = and(filters);
    let Some(Value::Array(list)) = obj.get("retrievers") else {
        return Err(EsError::parsing("[rrf] requires [retrievers]"));
    };
    if list.is_empty() {
        return Err(EsError::illegal_argument(
            "[rrf] needs at least one retriever",
        ));
    }
    let mut children = Vec::new();
    for child in list {
        let (kind, body) = retriever_kind(child)?;
        children.push(match kind {
            "standard" => Retriever::Text {
                query: standard_query(ctx, body, filter.as_ref())?,
                k: rank_window_size.max(1),
            },
            "knn" => {
                let spec = retriever_knn(ctx, body, filter.as_ref())?;
                if spec.similarity.is_some() {
                    return Err(EsError::unsupported("knn.similarity with rrf"));
                }
                vector_retriever(&spec, spec.k.min(rank_window_size).max(1))
            }
            _ => {
                let (inputs, k, inner_window) = rrf_children(ctx, body, 0, filter.as_ref())?;
                Retriever::Fused {
                    inputs,
                    fusion: Fusion::Rrf { k },
                    k: inner_window.max(1),
                }
            }
        });
    }
    Ok((children, rank_constant, rank_window_size))
}
