//! What `_delete_by_query` (Task 10) and `_update_by_query` (Task 9a)
//! share: their URL parameters, their body keys and the per-index loop over
//! the native filter writes (D87, Task 9a rule 7).
//!
//! Each index runs one filter write with `allow_partial: true` and follows
//! its cursor at the same pin until no rows remain or `max_docs` rows were
//! written; `total` is the first call's `matched` (capped by what `max_docs`
//! leaves), and the affected rows and batches are summed.

use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::response::Response;
use loams_query::{FilterWriteOptions, FilterWriteResult, PatchSpec, Query, ServiceError};
use serde_json::{Value, json};

use crate::EsGateway;
use crate::dsl::QueryContext;
use crate::dsl::query::{parse_leaf, token_name};
use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, json_body, respond};
use crate::mapping::IndexView;
use crate::search::SearchParams;
use crate::search::compile::url_query;
use crate::search::exec::{expr_of, now_ms, resolve_options, views};

/// The URL parameters of `_delete_by_query` and `_update_by_query` (Task 10
/// routes).
pub const BY_QUERY_PARAMS: &[&str] = &[
    "refresh",
    "conflicts",
    "wait_for_completion",
    "scroll_size",
    "max_docs",
    "slices",
    "requests_per_second",
    "timeout",
    "scroll",
    "q",
    "df",
    "default_operator",
    "analyzer",
    "lenient",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "routing",
    "preference",
    "search_timeout",
    "wait_for_active_shards",
];

/// The parameters a by-query request acts on.
#[derive(Clone, Debug, PartialEq)]
pub struct DbqParams {
    /// `max_docs` (the body's wins); `None` for every match.
    pub max_docs: Option<u64>,
    /// 1 000, at most 10 000. Accepted and checked; the batches are the
    /// collection service's (`ServiceConfig::filter_write_batch`).
    pub scroll_size: usize,
    /// `refresh=true` or empty; reads are strong, so it changes nothing.
    pub refresh: bool,
    /// `timeout`: each filter write's deadline, the service's default
    /// without it.
    pub timeout: Option<Duration>,
    /// `q`, `df`, `default_operator` and the index options, read as for
    /// `_search`.
    pub search: SearchParams,
}

impl DbqParams {
    /// Checks and reads `p` (validated against [`BY_QUERY_PARAMS`]).
    pub fn parse(p: &Params) -> Result<Self, EsError> {
        if p.bool("wait_for_completion")? == Some(false) {
            return Err(EsError::unsupported("wait_for_completion=false"));
        }
        if let Some(slices) = p.str("slices")
            && !matches!(slices, "1" | "auto")
        {
            return Err(EsError::unsupported(&format!("slices={slices}")));
        }
        if let Some(rate) = p.str("requests_per_second")
            && !matches!(rate, "-1" | "-1.0")
        {
            return Err(EsError::unsupported(&format!("requests_per_second={rate}")));
        }
        if let Some(conflicts) = p.str("conflicts") {
            check_conflicts(conflicts)?;
        }
        let scroll_size = p.usize("scroll_size")?.unwrap_or(1_000);
        check_scroll_size(scroll_size)?;
        let max_docs = match p.str("max_docs") {
            None => None,
            Some(text) => max_docs(&parse_i64("max_docs", text)?)?,
        };
        let timeout = p
            .str("timeout")
            .map(|text| time_value("timeout", text))
            .transpose()?
            .flatten();
        Ok(Self {
            max_docs,
            scroll_size,
            refresh: crate::write::refresh_param(p)?,
            timeout,
            search: crate::search::parse_params(p)?,
        })
    }
}

/// ES's answers to a `scroll_size` of 0 and to one past the result window
/// (the scroll search's own checks; row T11-3).
fn check_scroll_size(scroll_size: usize) -> Result<(), EsError> {
    if scroll_size == 0 {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: [size] cannot be [0] in a scroll context;",
        ));
    }
    if scroll_size > 10_000 {
        return Err(EsError::search_phase(
            EsError::illegal_argument(format!(
                "Batch size is too large, size must be less than or equal to: [10000] but was \
                 [{scroll_size}]. Scroll batch sizes cost as much memory as result windows so \
                 they are controlled by the [index.max_result_window] index level setting."
            )),
            "",
            crate::search::body::SEARCH_NODE,
        ));
    }
    Ok(())
}

fn parse_i64(name: &str, text: &str) -> Result<Value, EsError> {
    text.parse::<i64>().map(Value::from).map_err(|_| {
        EsError::illegal_argument(format!(
            "Failed to parse int parameter [{name}] with value [{text}]"
        ))
    })
}

/// `conflicts`: `abort` or `proceed`; there are no version conflicts to
/// count, so both behave alike.
fn check_conflicts(value: &str) -> Result<(), EsError> {
    match value {
        "abort" | "proceed" => Ok(()),
        other => Err(EsError::illegal_argument(format!(
            "conflicts may only be \"proceed\" or \"abort\" but was [{other}]"
        ))),
    }
}

/// `max_docs`: a positive integer, or `-1` for every match. ES's texts
/// (row T11-3): 0 is below the one slice, another negative is refused as
/// negative.
fn max_docs(value: &Value) -> Result<Option<u64>, EsError> {
    match value.as_i64() {
        Some(-1) => Ok(None),
        Some(n) if n > 0 => Ok(Some(n as u64)),
        Some(0) => Err(EsError::illegal_argument(
            "[max_docs] should be >= [slices]",
        )),
        Some(n) => Err(EsError::illegal_argument(format!(
            "[max_docs] parameter cannot be negative, found [{n}]"
        ))),
        None => Err(EsError::new(
            400,
            "action_request_validation_exception",
            format!(
                "Validation Failed: 1: maxDocs should be greater than 0 if the request is limited to some number of documents or -1 if it isn't but it was [{value}];"
            ),
        )),
    }
}

/// An ES time value (`500ms`, `1s`, `2m`, …); `-1` is none.
pub(crate) fn time_value(name: &str, text: &str) -> Result<Option<Duration>, EsError> {
    if text == "-1" {
        return Ok(None);
    }
    let bad = || {
        EsError::illegal_argument(format!(
            "failed to parse setting [{name}] with value [{text}] as a time value: unit is missing or unrecognized"
        ))
    };
    const UNITS: [(&str, u64); 7] = [
        ("nanos", 1),
        ("micros", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60_000_000_000),
        ("h", 3_600_000_000_000),
        ("d", 86_400_000_000_000),
    ];
    for (unit, nanos) in UNITS {
        if let Some(number) = text.strip_suffix(unit) {
            let n: u64 = number.trim().parse().map_err(|_| bad())?;
            return Ok(Some(Duration::from_nanos(n.saturating_mul(nanos))));
        }
    }
    Err(bad())
}

/// The keys of a by-query body.
pub(crate) struct ByQueryBody<'a> {
    pub query: Option<&'a Value>,
    pub max_docs: Option<Option<u64>>,
    pub script: Option<&'a Value>,
}

/// Reads the body's keys: `query`, `max_docs`, `conflicts` and, when
/// `with_script`, `script`; `slice` and `sort` are Phase A refusals.
pub(crate) fn parse_body(body: &Value, with_script: bool) -> Result<ByQueryBody<'_>, EsError> {
    let mut out = ByQueryBody {
        query: None,
        max_docs: None,
        script: None,
    };
    let map = match body {
        Value::Null => return Ok(out),
        Value::Object(map) => map,
        other => {
            return Err(EsError::parsing(format!(
                "Expected [START_OBJECT] but found [{}]",
                token_name(other)
            )));
        }
    };
    for (key, value) in map {
        match key.as_str() {
            "query" => out.query = Some(value).filter(|v| !v.is_null()),
            "max_docs" => out.max_docs = Some(max_docs(value)?),
            // ES fails a non-string with a 500 class cast; a 400 names it.
            "conflicts" => match value.as_str() {
                Some(text) => check_conflicts(text)?,
                None => {
                    return Err(EsError::parsing(format!(
                        "[conflicts] must be a string, found [{}]",
                        token_name(value)
                    )));
                }
            },
            "script" if with_script => out.script = Some(value).filter(|v| !v.is_null()),
            "slice" | "sort" => return Err(EsError::unsupported(key)),
            // ES's `AbstractBulkByQueryRequest` parser text (row T11-3).
            other => {
                return Err(EsError::parsing(format!(
                    "Unknown key for a {} in [{other}].",
                    token_name(value)
                )));
            }
        }
    }
    Ok(out)
}

/// The query of each index: `q` when given, else the body's `query`, which
/// one of them must give unless `match_all` stands in (knn and
/// `script_score` are refused).
pub(crate) fn compile_queries(
    indices: &[IndexView],
    body_query: Option<&Value>,
    search: &SearchParams,
    match_all: bool,
) -> Result<Vec<Query>, EsError> {
    let now = now_ms();
    indices
        .iter()
        .map(|view| {
            let qctx = QueryContext::new(view, now);
            match (&search.q, body_query) {
                (Some(q), _) => url_query(q, search, &qctx),
                (None, Some(query)) => parse_leaf(query, &qctx),
                (None, None) if match_all => Ok(Query::MatchAll),
                (None, None) => Err(EsError::new(
                    400,
                    "action_request_validation_exception",
                    "Validation Failed: 1: query is missing;",
                )),
            }
        })
        .collect()
}

/// What a by-query request writes to each match.
#[derive(Clone, Debug)]
pub(crate) enum ByFilter {
    /// `_delete_by_query`.
    Delete,
    /// `_update_by_query`.
    Patch(PatchSpec),
}

/// The sums of a by-query request over its indices.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ByQueryTotals {
    pub total: u64,
    pub affected: u64,
    pub batches: u64,
}

/// Runs `op` over each index in turn (Task 9a rule 7): a filter write per
/// index at one pin, following its cursor until no rows remain or
/// `max_docs` rows were written across all indices. A write refused for
/// backpressure past its deadline is 429 (what was written stays).
pub(crate) async fn run(
    gw: &EsGateway,
    ctx: &RequestCtx,
    targets: &[(IndexView, Query)],
    op: &ByFilter,
    max_docs: Option<u64>,
    timeout: Option<Duration>,
    what: &str,
) -> Result<ByQueryTotals, EsError> {
    let mut totals = ByQueryTotals::default();
    let mut left = max_docs.unwrap_or(u64::MAX);
    for (view, query) in targets {
        if left == 0 {
            break;
        }
        let mut cursor = None;
        let mut first = true;
        loop {
            let opts = FilterWriteOptions {
                consistency: ctx.consistency.clone(),
                max_rows: max_docs.map(|_| left),
                allow_partial: true,
                cursor: cursor.take(),
                deadline: timeout,
                ..FilterWriteOptions::default()
            };
            let result = call(gw, ctx, &view.name, query.clone(), op, opts).await?;
            if first {
                totals.total += result.matched.min(left);
                first = false;
            }
            totals.affected += result.affected;
            totals.batches += result.batches;
            left = left.saturating_sub(result.written);
            if let Some(retry_after_ms) = result.retry_after_ms {
                return Err(EsError::from_service(
                    ServiceError::ResourceExhausted {
                        message: format!(
                            "[{what}] was refused for backpressure after {} of its documents were written; retry after {} s",
                            totals.affected,
                            retry_after_ms.div_ceil(1000).max(1)
                        ),
                        retry_after_ms,
                    },
                    ErrorContext::Write,
                ));
            }
            match result.cursor {
                Some(next) if result.rows_remaining && left > 0 => cursor = Some(next),
                _ => break,
            }
        }
    }
    Ok(totals)
}

async fn call(
    gw: &EsGateway,
    ctx: &RequestCtx,
    index: &str,
    query: Query,
    op: &ByFilter,
    opts: FilterWriteOptions,
) -> Result<FilterWriteResult, EsError> {
    let service = gw.service();
    let result = match op {
        // Boxed: the service's filter-write future is deep.
        ByFilter::Delete => {
            Box::pin(service.delete_by_filter(&ctx.namespace, index, query, opts)).await
        }
        ByFilter::Patch(patch) => {
            Box::pin(service.patch_by_filter(&ctx.namespace, index, query, patch.clone(), opts))
                .await
        }
    };
    result.map_err(|err| EsError::from_service(err, ErrorContext::Write))
}

/// `_delete_by_query` over `indices` (Task 10 rule 1, Ruling 6): the
/// body's `query` (or `q`) and `max_docs`, executed per index in name order
/// by `delete_by_filter` at one pin per index (D87).
pub async fn delete_by_query(
    gw: &EsGateway,
    ctx: &RequestCtx,
    indices: &[IndexView],
    body: &Value,
    params: &DbqParams,
) -> Result<Value, EsError> {
    let parsed = parse_body(body, false)?;
    let max_docs = parsed.max_docs.unwrap_or(params.max_docs);
    let mut indices = indices.to_vec();
    indices.sort_by(|a, b| a.name.cmp(&b.name));
    let queries = compile_queries(&indices, parsed.query, &params.search, false)?;
    let targets: Vec<(IndexView, Query)> = indices.into_iter().zip(queries).collect();
    let totals = run(
        gw,
        ctx,
        &targets,
        &ByFilter::Delete,
        max_docs,
        params.timeout,
        "_delete_by_query",
    )
    .await?;
    let fields = [
        ("took", json!(ctx.started.elapsed().as_millis() as u64)),
        ("timed_out", json!(false)),
        ("total", json!(totals.total)),
        ("deleted", json!(totals.affected)),
        ("batches", json!(totals.batches)),
        ("version_conflicts", json!(0)),
        ("noops", json!(0)),
        ("retries", json!({"bulk": 0, "search": 0})),
        ("throttled_millis", json!(0)),
        ("requests_per_second", json!(-1.0)),
        ("throttled_until_millis", json!(0)),
        ("failures", json!([])),
    ];
    Ok(Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    ))
}

async fn delete_by_query_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
    body: &[u8],
) -> Result<Value, EsError> {
    let params = Params::parse(uri.query(), uri.path(), BY_QUERY_PARAMS)?;
    let dbq_params = DbqParams::parse(&params)?;
    let body = json_body(body)?.unwrap_or(Value::Null);
    // The body is read before resolution, so a bad key or a missing query
    // names itself before a missing index does (as `_update_by_query`).
    let parsed = parse_body(&body, false)?;
    if parsed.query.is_none() && dbq_params.search.q.is_none() {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: query is missing;",
        ));
    }
    let indices = views(
        gw,
        ctx,
        &expr_of(Some(index)),
        resolve_options(&params, &dbq_params.search),
    )
    .await?;
    delete_by_query(gw, ctx, &indices, &body, &dbq_params).await
}

/// `POST /{index}/_delete_by_query`.
pub(crate) async fn delete_by_query_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    match delete_by_query_request(&gw, &ctx, &uri, &index, &body).await {
        Ok(body) => respond(&ctx, 200, &body),
        Err(err) => fail(&ctx, &err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_values_parse_with_their_units() {
        assert_eq!(
            time_value("timeout", "500ms").expect("ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            time_value("timeout", "2m").expect("m"),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            time_value("timeout", "1s").expect("s"),
            Some(Duration::from_secs(1))
        );
        assert_eq!(time_value("timeout", "-1").expect("none"), None);
        for bad in ["1", "s", "1x", "-2s"] {
            assert!(time_value("timeout", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_non_string_conflicts_is_a_parse_error() {
        let body = serde_json::json!({"query": {"match_all": {}}, "conflicts": 1});
        let err = parse_body(&body, false).err().expect("refused");
        assert_eq!(err.kind, "parsing_exception");
        assert_eq!(
            err.reason,
            "[conflicts] must be a string, found [VALUE_NUMBER]"
        );
        let body = serde_json::json!({"query": {"match_all": {}}, "conflicts": "proceed"});
        assert!(parse_body(&body, false).is_ok());
    }

    #[test]
    fn max_docs_is_positive_or_minus_one() {
        assert_eq!(max_docs(&Value::from(3)).expect("3"), Some(3));
        assert_eq!(max_docs(&Value::from(-1)).expect("-1"), None);
        for bad in [Value::from(0), Value::from(-2), Value::from("x")] {
            assert!(max_docs(&bad).is_err(), "{bad}");
        }
    }
}
