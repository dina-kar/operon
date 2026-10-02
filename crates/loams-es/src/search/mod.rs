//! Search requests (plan M1.5 Tasks 8 and 9): the URL parameters and body
//! of `_search` compiled to a [`SearchPlan`] over one index, executed over
//! every index of the request and rendered with ES scores.
//!
//! - [`body`]: body keys, paging, sort, `search_after` and
//!   `track_total_hits`.
//! - [`compile`](mod@compile): the compilation cases (query, knn, hybrid sum, RRF,
//!   `script_score`).
//! - [`exec`]: `_search`, `_count` and `_msearch` (fan-out, hybrid sum,
//!   totals).
//! - [`render`]: ES scores and the answer's shape.

pub mod body;
pub mod compile;
pub mod exec;
pub mod render;

use loams_query::{BoolOperator, Query, SearchRequest, SortKey, TrackTotalHits};
use serde_json::Value;

pub use compile::compile;
pub use exec::{SearchOutcome, count, execute};
pub use render::{render_hit, to_es_score};

use crate::doc::SourceFilter;
use crate::dsl::ScriptFunction;
use crate::error::EsError;
use crate::http::Params;
use crate::mapping::EsSimilarity;

/// The query parameters `_search` accepts (Rulings 14 and 20); `typed_keys`
/// is absent, so it is refused.
pub const SEARCH_PARAMS: &[&str] = &[
    "from",
    "size",
    "sort",
    "_source",
    "_source_includes",
    "_source_excludes",
    "track_total_hits",
    "q",
    "df",
    "default_operator",
    "analyzer",
    "lenient",
    "rest_total_hits_as_int",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "preference",
    "routing",
    "request_cache",
    "search_type",
    "allow_partial_search_results",
    "ccs_minimize_roundtrips",
    "max_concurrent_shard_requests",
    "pre_filter_shard_size",
    "batched_reduce_size",
    "timeout",
    "track_scores",
];

/// `_search`'s URL parameters that shape the search (Ruling 20: they
/// override the body).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchParams {
    pub from: Option<usize>,
    pub size: Option<usize>,
    /// `f`, `f:asc`, `f:desc`.
    pub sort: Option<Vec<String>>,
    pub source: Option<SourceFilter>,
    /// `true`, `false` or an integer.
    pub track_total_hits: Option<Value>,
    pub q: Option<String>,
    pub df: Option<String>,
    pub default_operator: Option<BoolOperator>,
    pub rest_total_hits_as_int: bool,
    pub ignore_unavailable: bool,
    /// Default true.
    pub allow_no_indices: bool,
    pub track_scores: Option<bool>,
}

/// How the gateway turns engine scores into ES scores (Task 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EsScore {
    /// Engine BM25 is ES's score.
    Bm25,
    /// A knn score converted per similarity.
    Knn(EsSimilarity),
    /// A `script_score` function of the exact engine score.
    Script(ScriptFunction),
    /// The reciprocal-rank sum.
    Rrf,
    /// Σ boost × ES score of a query and knn searches (Ruling 3).
    Sum,
}

/// What rendering a plan's hits needs.
#[derive(Clone, Debug, PartialEq)]
pub struct RenderSpec {
    pub source: SourceFilter,
    pub score: EsScore,
    /// Whether hits carry `_score` (else `null`, `max_score` too).
    pub scores_visible: bool,
    /// The user's sort keys (the PK tie-break excluded).
    pub sort_keys: Vec<SortKey>,
    /// How many `sort` values a hit carries (0 without a user sort).
    pub user_sort_len: usize,
    pub version: bool,
    pub seq_no_primary_term: bool,
    pub from: usize,
    pub size: usize,
    pub track: TrackTotalHits,
    pub rest_total_hits_as_int: bool,
    /// The body's `min_score` (ES score space).
    pub min_score: Option<f32>,
    /// A `script_score`'s `boost`, multiplied into its scores (1.0
    /// otherwise).
    pub boost: f32,
}

/// A compiled search over one index.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchPlan {
    /// One service call: query-only, knn-only (one knn) and RRF.
    Single {
        request: SearchRequest,
        render: RenderSpec,
    },
    /// The ES score sum of a query and knn searches, merged in the gateway
    /// (Ruling 3). `count_filter` counts the total: the query's matches or
    /// the knn hits (its `Ids` is filled at execution).
    HybridSum {
        text: Option<(SearchRequest, f32)>,
        vectors: Vec<(SearchRequest, EsSimilarity, f32)>,
        count_filter: Query,
        render: RenderSpec,
    },
    /// An exact vector search with a metric override (C18–C21).
    ScriptScore {
        request: SearchRequest,
        function: ScriptFunction,
        count_filter: Query,
        render: RenderSpec,
    },
}

impl SearchPlan {
    pub fn render(&self) -> &RenderSpec {
        match self {
            SearchPlan::Single { render, .. }
            | SearchPlan::HybridSum { render, .. }
            | SearchPlan::ScriptScore { render, .. } => render,
        }
    }
}

/// `_search`'s URL parameters (Ruling 20); unknown ones were refused by
/// [`Params::parse`] with [`SEARCH_PARAMS`].
pub fn parse_params(p: &Params) -> Result<SearchParams, EsError> {
    let track_total_hits = match p.str("track_total_hits") {
        None => None,
        Some("" | "true") => Some(Value::Bool(true)),
        Some("false") => Some(Value::Bool(false)),
        Some(n) => Some(Value::from(n.parse::<i64>().map_err(|_| {
            EsError::illegal_argument(format!(
                "Failed to parse int parameter [track_total_hits] with value [{n}]"
            ))
        })?)),
    };
    let default_operator = p
        .str("default_operator")
        .map(|op| crate::dsl::query::operator(&Value::String(op.to_string())))
        .transpose()?;
    if let Some(analyzer) = p.str("analyzer") {
        return Err(EsError::unsupported(&format!("analyzer: {analyzer}")));
    }
    p.bool("lenient")?;
    p.bool("request_cache")?;
    p.bool("allow_partial_search_results")?;
    p.bool("ccs_minimize_roundtrips")?;
    if let Some(kind) = p.str("search_type")
        && !matches!(kind, "query_then_fetch" | "dfs_query_then_fetch")
    {
        return Err(EsError::illegal_argument(format!(
            "No search type for [{kind}]"
        )));
    }
    Ok(SearchParams {
        from: p.usize("from")?,
        size: p.usize("size")?,
        sort: p.list("sort"),
        source: SourceFilter::from_params(p)?,
        track_total_hits,
        q: p.str("q").map(str::to_string),
        df: p.str("df").map(str::to_string),
        default_operator,
        rest_total_hits_as_int: p.bool("rest_total_hits_as_int")?.unwrap_or(false),
        ignore_unavailable: p.bool("ignore_unavailable")?.unwrap_or(false),
        allow_no_indices: p.bool("allow_no_indices")?.unwrap_or(true),
        track_scores: p.bool("track_scores")?,
    })
}
