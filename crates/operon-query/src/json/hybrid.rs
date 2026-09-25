//! The §05 §4 hybrid query body (plan M1.2 Task 1 rule 8).
//!
//! A body is the hybrid form iff it has the key `from` or `retrieve`;
//! otherwise it is a [`SearchRequest`].

use operon_collection::ConsistencyToken;
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::ir::{
    AnnParams, BoolOperator, FieldValue, Fusion, Query, ReadConsistency, Retriever, SearchRequest,
};
use crate::json::pk;
use crate::types::{Projection, SourceFilter};

const MS_PER: [(char, u64); 4] = [
    ('s', 1_000),
    ('m', 60_000),
    ('h', 3_600_000),
    ('d', 86_400_000),
];

fn invalid(message: impl Into<String>) -> ServiceError {
    ServiceError::InvalidArgument(message.into())
}

/// The request a native query body describes, with date math relative to
/// `now_ms`.
pub fn parse_query_body(body: Value, now_ms: u64) -> Result<SearchRequest, ServiceError> {
    let is_hybrid = body
        .as_object()
        .is_some_and(|o| o.contains_key("from") || o.contains_key("retrieve"));
    if !is_hybrid {
        return serde_json::from_value(body)
            .map_err(|err| invalid(format!("invalid search request: {err}")));
    }
    let Value::Object(body) = body else {
        unreachable!("checked above");
    };
    if body.contains_key("expand") {
        return Err(invalid("expand needs graphs, which arrive in M3"));
    }
    if body.contains_key("rerank") {
        return Err(invalid("rerank arrives in M3"));
    }
    const KEYS: [&str; 8] = [
        "from",
        "consistency",
        "retrieve",
        "filter",
        "fuse",
        "select",
        "limit",
        "offset",
    ];
    if let Some(key) = body.keys().find(|key| !KEYS.contains(&key.as_str())) {
        return Err(invalid(format!("unknown key {key} in the query body")));
    }
    let from = body
        .get("from")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("from must be \"collections.<name>\" or \"<name>\""))?;
    let collection = from.strip_prefix("collections.").unwrap_or(from);
    let mut request = SearchRequest::new(collection);
    if let Some(consistency) = body.get("consistency") {
        request.consistency = consistency_from(consistency)?;
    }
    if let Some(retrieve) = body.get("retrieve") {
        request.retrievers = retrieve
            .as_array()
            .ok_or_else(|| invalid("retrieve must be a list"))?
            .iter()
            .map(retriever_from)
            .collect::<Result<_, _>>()?;
    }
    if let Some(filter) = body.get("filter") {
        request.filter = Some(filter_from(filter, now_ms)?);
    }
    if let Some(fuse) = body.get("fuse") {
        request.fusion = Some(fusion_from(fuse)?);
    }
    if let Some(select) = body.get("select") {
        request.select = select_from(select)?;
    }
    if let Some(limit) = body.get("limit") {
        request.limit = usize_from(limit, "limit")?;
    }
    if let Some(offset) = body.get("offset") {
        request.offset = usize_from(offset, "offset")?;
    }
    Ok(request)
}

fn usize_from(v: &Value, what: &str) -> Result<usize, ServiceError> {
    v.as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| invalid(format!("{what} must be an unsigned integer")))
}

/// The only key of a one-key object.
fn single<'a>(v: &'a Value, what: &str) -> Result<(&'a str, &'a Value), ServiceError> {
    match v.as_object() {
        Some(map) if map.len() == 1 => {
            let (key, value) = map.iter().next().expect("one key");
            Ok((key.as_str(), value))
        }
        _ => Err(invalid(format!(
            "{what} must be an object with one key, got {v}"
        ))),
    }
}

fn consistency_from(v: &Value) -> Result<ReadConsistency, ServiceError> {
    match v {
        Value::String(s) if s == "strong" => Ok(ReadConsistency::Strong),
        Value::String(s) if s == "eventual" => Ok(ReadConsistency::Eventual),
        _ => {
            let (key, token) = single(v, "consistency")?;
            let token = token.as_str().filter(|_| key == "token").ok_or_else(|| {
                invalid("consistency must be \"strong\", \"eventual\" or {\"token\": \"v1:…\"}")
            })?;
            let parsed: ConsistencyToken = token
                .parse()
                .map_err(|_| invalid(format!("invalid consistency token {token:?}")))?;
            Ok(ReadConsistency::AtLeast(parsed))
        }
    }
}

/// Reads the keys of a retriever body, refusing unknown ones.
fn fields<'a>(
    v: &'a Value,
    what: &str,
    allowed: &[&str],
) -> Result<&'a Map<String, Value>, ServiceError> {
    let map = v
        .as_object()
        .ok_or_else(|| invalid(format!("{what} must be an object")))?;
    if let Some(key) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid(format!("unknown key {key} in {what}")));
    }
    Ok(map)
}

fn text_of(map: &Map<String, Value>, key: &str, what: &str) -> Result<String, ServiceError> {
    map.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("{what}.{key} must be a string")))
}

fn opt_u32(map: &Map<String, Value>, key: &str, what: &str) -> Result<Option<u32>, ServiceError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| invalid(format!("{what}.{key} must be an unsigned 32-bit integer"))),
    }
}

fn retriever_from(v: &Value) -> Result<Retriever, ServiceError> {
    let (kind, body) = single(v, "a retriever")?;
    match kind {
        "vector" => {
            let map = fields(
                body,
                "vector",
                &[
                    "field",
                    "query",
                    "k",
                    "exact",
                    "nprobes",
                    "refine_factor",
                    "ef",
                ],
            )?;
            let query = map
                .get("query")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("vector.query must be a list of numbers"))?
                .iter()
                .map(|x| {
                    x.as_f64()
                        .map(|x| x as f32)
                        .ok_or_else(|| invalid("vector.query must be a list of numbers"))
                })
                .collect::<Result<_, _>>()?;
            let exact = match map.get("exact") {
                None | Some(Value::Null) => false,
                Some(v) => v
                    .as_bool()
                    .ok_or_else(|| invalid("vector.exact must be a bool"))?,
            };
            Ok(Retriever::Vector {
                field: text_of(map, "field", "vector")?,
                query,
                k: usize_from(map.get("k").unwrap_or(&Value::Null), "vector.k")?,
                params: AnnParams {
                    exact,
                    nprobes: opt_u32(map, "nprobes", "vector")?,
                    refine_factor: opt_u32(map, "refine_factor", "vector")?,
                    ef: opt_u32(map, "ef", "vector")?,
                    ..AnnParams::default()
                },
                filter: None,
            })
        }
        "text" => {
            let map = fields(body, "text", &["field", "query", "k", "operator"])?;
            let operator = match map.get("operator").and_then(Value::as_str) {
                None => BoolOperator::Or,
                Some("or") => BoolOperator::Or,
                Some("and") => BoolOperator::And,
                Some(other) => return Err(invalid(format!("unknown text.operator {other}"))),
            };
            Ok(Retriever::Text {
                query: Query::Match {
                    field: text_of(map, "field", "text")?,
                    text: text_of(map, "query", "text")?,
                    operator,
                    minimum_should_match: None,
                    fuzziness: None,
                    analyzer: None,
                },
                k: usize_from(map.get("k").unwrap_or(&Value::Null), "text.k")?,
            })
        }
        other => Err(invalid(format!("unknown retriever {other}"))),
    }
}

fn bool_query(must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
    Query::Bool {
        must,
        should,
        must_not,
        filter: Vec::new(),
        minimum_should_match: None,
    }
}

fn filter_from(v: &Value, now_ms: u64) -> Result<Query, ServiceError> {
    let (kind, body) = single(v, "a filter")?;
    let list = |body: &Value| -> Result<Vec<Query>, ServiceError> {
        body.as_array()
            .ok_or_else(|| invalid(format!("{kind} must be a list of filters")))?
            .iter()
            .map(|f| filter_from(f, now_ms))
            .collect()
    };
    match kind {
        "and" => Ok(bool_query(list(body)?, Vec::new(), Vec::new())),
        "or" => Ok(bool_query(Vec::new(), list(body)?, Vec::new())),
        "not" => Ok(bool_query(
            Vec::new(),
            Vec::new(),
            vec![filter_from(body, now_ms)?],
        )),
        "term" => {
            let (field, value) = single(body, "term")?;
            Ok(Query::Term {
                field: field.to_string(),
                value: value_from(value, now_ms)?,
            })
        }
        "terms" => {
            let (field, values) = single(body, "terms")?;
            let values = values
                .as_array()
                .ok_or_else(|| invalid("terms values must be a list"))?
                .iter()
                .map(|value| value_from(value, now_ms))
                .collect::<Result<_, _>>()?;
            Ok(Query::Terms {
                field: field.to_string(),
                values,
            })
        }
        "range" => {
            let (field, bounds) = single(body, "range")?;
            let bounds = fields(bounds, "range", &["gt", "gte", "lt", "lte"])?;
            let bound = |key: &str| {
                bounds
                    .get(key)
                    .map(|value| value_from(value, now_ms))
                    .transpose()
            };
            Ok(Query::Range {
                field: field.to_string(),
                gt: bound("gt")?,
                gte: bound("gte")?,
                lt: bound("lt")?,
                lte: bound("lte")?,
            })
        }
        "exists" => Ok(Query::Exists {
            field: body
                .as_str()
                .ok_or_else(|| invalid("exists must name a field"))?
                .to_string(),
        }),
        "ids" => Ok(Query::Ids(
            body.as_array()
                .ok_or_else(|| invalid("ids must be a list"))?
                .iter()
                .map(pk::from_json)
                .collect::<Result<_, _>>()?,
        )),
        "match" => {
            let (field, text) = single(body, "match")?;
            Ok(Query::Match {
                field: field.to_string(),
                text: text
                    .as_str()
                    .ok_or_else(|| invalid("match text must be a string"))?
                    .to_string(),
                operator: BoolOperator::Or,
                minimum_should_match: None,
                fuzziness: None,
                analyzer: None,
            })
        }
        other => Err(invalid(format!("unknown filter {other}"))),
    }
}

/// A JSON scalar, or date math `now`, `now-<n><u>`, `now+<n><u>` with
/// `u ∈ {s, m, h, d}`.
fn value_from(v: &Value, now_ms: u64) -> Result<FieldValue, ServiceError> {
    match v {
        Value::String(s) => Ok(date_math(s, now_ms)?.unwrap_or_else(|| FieldValue::Str(s.clone()))),
        Value::Bool(b) => Ok(FieldValue::Bool(*b)),
        Value::Number(n) => Ok(if let Some(i) = n.as_i64() {
            FieldValue::I64(i)
        } else if let Some(u) = n.as_u64() {
            FieldValue::U64(u)
        } else {
            FieldValue::F64(n.as_f64().unwrap_or_default())
        }),
        other => Err(invalid(format!(
            "a filter value must be a JSON scalar, got {other}"
        ))),
    }
}

/// `Some(Date)` for date math, `None` for any other string.
fn date_math(s: &str, now_ms: u64) -> Result<Option<FieldValue>, ServiceError> {
    let Some(rest) = s.strip_prefix("now") else {
        return Ok(None);
    };
    let now = i128::from(now_ms);
    let ms = if rest.is_empty() {
        now
    } else {
        let (sign, rest) = match rest.as_bytes()[0] {
            b'+' => (1i128, &rest[1..]),
            b'-' => (-1i128, &rest[1..]),
            _ => return Ok(None),
        };
        let Some(unit) = rest.chars().last() else {
            return Err(invalid(format!("invalid date math {s:?}")));
        };
        let per = MS_PER
            .iter()
            .find(|(u, _)| *u == unit)
            .map(|(_, per)| *per)
            .ok_or_else(|| invalid(format!("invalid date math {s:?}: units are s, m, h, d")))?;
        let digits = &rest[..rest.len() - unit.len_utf8()];
        let n: u64 = digits
            .parse()
            .map_err(|_| invalid(format!("invalid date math {s:?}")))?;
        now + sign * i128::from(n) * i128::from(per)
    };
    let micros =
        i64::try_from(ms * 1000).map_err(|_| invalid(format!("date math {s:?} out of range")))?;
    // The JSON date form holds the years 0000–9999 only.
    if crate::json::values::format_date(micros).is_none() {
        return Err(invalid(format!("date math {s:?} out of range")));
    }
    Ok(Some(FieldValue::Date(micros)))
}

fn fusion_from(v: &Value) -> Result<Fusion, ServiceError> {
    let map = v
        .as_object()
        .ok_or_else(|| invalid("fuse must be an object"))?;
    let method = text_of(map, "method", "fuse")?;
    let allowed: &[&str] = match method.as_str() {
        "rrf" => &["method", "k"],
        "dbsf" => &["method"],
        "weighted" => &["method", "weights"],
        other => return Err(invalid(format!("unknown fuse method {other}"))),
    };
    if let Some(key) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid(format!("unknown key {key} in fuse")));
    }
    Ok(match method.as_str() {
        "rrf" => Fusion::Rrf {
            k: opt_u32(map, "k", "fuse")?.unwrap_or(60),
        },
        "dbsf" => Fusion::Dbsf,
        _ => Fusion::WeightedSum {
            weights: map
                .get("weights")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("fuse.weights must be a list of numbers"))?
                .iter()
                .map(|w| {
                    w.as_f64()
                        .map(|w| w as f32)
                        .ok_or_else(|| invalid("fuse.weights must be a list of numbers"))
                })
                .collect::<Result<_, _>>()?,
        },
    })
}

fn select_from(v: &Value) -> Result<Projection, ServiceError> {
    let names = v
        .as_array()
        .ok_or_else(|| invalid("select must be a list of names"))?;
    let mut all = false;
    let mut include = Vec::new();
    let mut vectors = Vec::new();
    for name in names {
        let name = name
            .as_str()
            .ok_or_else(|| invalid("select must be a list of names"))?;
        match name {
            "id" | "_score" => {}
            "_source" => all = true,
            _ => match name.strip_prefix("_vectors.") {
                Some(vector) => vectors.push(vector.to_string()),
                None => include.push(name.to_string()),
            },
        }
    }
    let source = if all {
        SourceFilter::All
    } else if include.is_empty() {
        SourceFilter::None
    } else {
        SourceFilter::Paths {
            include,
            exclude: Vec::new(),
        }
    };
    Ok(Projection {
        source,
        vectors,
        fields: Vec::new(),
    })
}
