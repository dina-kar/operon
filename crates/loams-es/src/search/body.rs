//! A search body's keys, sort, `search_after` and `track_total_hits` (plan
//! M1.5 Task 8 items 1, 3, 6 and 7).

use loams_collection::FieldKind;
use loams_query::{FieldValue, MissingOrder, Query, SortKey, SortOrder, TrackTotalHits};
use serde_json::{Map, Value, json};

use crate::dsl::Rounding;
use crate::dsl::query::token_name;
use crate::dsl::value::{FieldRef, coerce_rounded, es_type, json_class, resolve};
use crate::error::EsError;
use crate::mapping::IndexView;

/// The node a `search_phase_execution_exception` names (Task 9 renders
/// the configured one).
pub const SEARCH_NODE: &str = "loams";

/// Body keys compiled here.
const ACCEPTED: &[&str] = &[
    "query",
    "knn",
    "retriever",
    "rank",
    "from",
    "size",
    "sort",
    "search_after",
    "_source",
    "track_total_hits",
    "track_scores",
    "min_score",
    "version",
    "seq_no_primary_term",
    "timeout",
    "stats",
    "explain",
    "profile",
];

/// Body keys of Phase B (D48).
const PHASE_B: &[&str] = &[
    "aggs",
    "aggregations",
    "highlight",
    "pit",
    "post_filter",
    "collapse",
    "suggest",
    "rescore",
    "indices_boost",
    "runtime_mappings",
    "script_fields",
    "stored_fields",
    "docvalue_fields",
    "fields",
    "slice",
    "ext",
    "terminate_after",
];

/// Refuses the keys of `body` outside Phase A (item 1).
pub fn check_keys(body: &Map<String, Value>) -> Result<(), EsError> {
    for (key, value) in body {
        let key = key.as_str();
        if ACCEPTED.contains(&key) {
            continue;
        }
        if PHASE_B.contains(&key) {
            return Err(EsError::unsupported(key));
        }
        return Err(EsError::parsing(format!(
            "Unknown key for a {} in [{key}].",
            token_name(value)
        )));
    }
    for flag in ["explain", "profile"] {
        match body.get(flag) {
            None | Some(Value::Bool(false)) => {}
            Some(Value::Bool(true)) => return Err(EsError::unsupported(flag)),
            Some(other) => {
                return Err(EsError::parsing(format!(
                    "[{flag}] must be a boolean, found [{}]",
                    token_name(other)
                )));
            }
        }
    }
    Ok(())
}

/// A body flag: absent is false.
pub fn flag(body: &Map<String, Value>, key: &str) -> Result<bool, EsError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(other) => Err(EsError::parsing(format!(
            "[{key}] must be a boolean, found [{}]",
            token_name(other)
        ))),
    }
}

/// A body `from` or `size`.
pub fn count(body: &Map<String, Value>, key: &str) -> Result<Option<usize>, EsError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_i64() {
            Some(n) if n >= 0 => Ok(Some(n as usize)),
            Some(n) => Err(EsError::illegal_argument(format!(
                "[{key}] parameter cannot be negative, found [{n}]"
            ))),
            None => Err(EsError::parsing(format!(
                "[{key}] must be an integer, found [{}]",
                token_name(v)
            ))),
        },
    }
}

/// `track_total_hits` (item 7): absent is up to 10 000.
pub fn track_total_hits(v: Option<&Value>) -> Result<TrackTotalHits, EsError> {
    match v {
        None | Some(Value::Null) => Ok(TrackTotalHits::UpTo(10_000)),
        Some(Value::Bool(true)) => Ok(TrackTotalHits::Exact),
        Some(Value::Bool(false)) => Ok(TrackTotalHits::None),
        Some(Value::Number(n)) => match n.as_i64() {
            Some(-1) => Ok(TrackTotalHits::None),
            Some(n) if n >= 0 => Ok(TrackTotalHits::UpTo(n as u64)),
            _ => Err(EsError::illegal_argument(format!(
                "[track_total_hits] parameter must be positive or equals to -1, got {n}"
            ))),
        },
        Some(other) => Err(EsError::parsing(format!(
            "[track_total_hits] must be a boolean or an integer, found [{}]",
            token_name(other)
        ))),
    }
}

/// The sort keys of the URL form `f:asc,g` (Ruling 20).
pub fn url_sort(view: &IndexView, list: &[String]) -> Result<Vec<SortKey>, EsError> {
    let entries: Vec<Value> = list
        .iter()
        .map(|item| match item.rsplit_once(':') {
            Some((field, order)) => {
                let mut map = Map::new();
                map.insert(field.to_string(), Value::String(order.to_string()));
                Value::Object(map)
            }
            None => Value::String(item.clone()),
        })
        .collect();
    body_sort(view, &Value::Array(entries))
}

fn order_of(v: &Value) -> Result<SortOrder, EsError> {
    match v.as_str().map(str::to_ascii_lowercase).as_deref() {
        Some("asc") => Ok(SortOrder::Asc),
        Some("desc") => Ok(SortOrder::Desc),
        _ => Err(EsError::illegal_argument(format!(
            "Unknown SortOrder [{}]",
            v.as_str().unwrap_or_default()
        ))),
    }
}

/// The body's `sort` (item 3): a string, an object or an array of them.
/// Unmapped keys with `unmapped_type` are dropped.
pub fn body_sort(view: &IndexView, v: &Value) -> Result<Vec<SortKey>, EsError> {
    let mut out = Vec::new();
    let items: Vec<&Value> = match v {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    };
    for item in items {
        match item {
            Value::String(field) => {
                if let Some(key) = sort_key(view, field, None)? {
                    out.push(key);
                }
            }
            Value::Object(map) => {
                for (field, options) in map {
                    if let Some(key) = sort_key(view, field, Some(options))? {
                        out.push(key);
                    }
                }
            }
            other => {
                return Err(EsError::parsing(format!(
                    "[sort] entries must be strings or objects, found [{}]",
                    token_name(other)
                )));
            }
        }
    }
    Ok(out)
}

/// One sort key; `None` when an unmapped field with `unmapped_type` drops
/// it.
fn sort_key(
    view: &IndexView,
    field: &str,
    options: Option<&Value>,
) -> Result<Option<SortKey>, EsError> {
    let mut order = None;
    let mut missing = MissingOrder::Last;
    let mut unmapped_type = false;
    let mut mode = None;
    match options {
        None => {}
        Some(Value::String(_)) => order = Some(order_of(options.unwrap_or(&Value::Null))?),
        Some(Value::Object(map)) => {
            for (key, value) in map {
                match key.as_str() {
                    "order" => order = Some(order_of(value)?),
                    "missing" => {
                        missing = match value.as_str() {
                            Some("_last") => MissingOrder::Last,
                            Some("_first") => MissingOrder::First,
                            _ => {
                                return Err(EsError::unsupported(&format!(
                                    "sort.missing: {value}"
                                )));
                            }
                        }
                    }
                    "unmapped_type" => unmapped_type = true,
                    "mode" => mode = Some(value.as_str().unwrap_or_default().to_string()),
                    "format" => {
                        if value.as_str() != Some("epoch_millis") {
                            return Err(EsError::unsupported(&format!("sort.format: {value}")));
                        }
                    }
                    "numeric_type" => {
                        let wanted = value.as_str().unwrap_or_default();
                        if wanted != es_type(view, field) {
                            return Err(EsError::unsupported(&format!(
                                "sort.numeric_type: {wanted}"
                            )));
                        }
                    }
                    _ => {
                        return Err(EsError::parsing(format!(
                            "[field_sort] unknown field [{key}]"
                        )));
                    }
                }
            }
        }
        Some(other) => {
            return Err(EsError::parsing(format!(
                "[sort] options of [{field}] must be a string or an object, found [{}]",
                token_name(other)
            )));
        }
    }
    match field {
        "_score" => {
            return Ok(Some(SortKey::Score {
                order: order.unwrap_or(SortOrder::Desc),
            }));
        }
        "_doc" | "_shard_doc" => {
            return match order.unwrap_or(SortOrder::Asc) {
                SortOrder::Asc => Ok(Some(SortKey::Pk {
                    order: SortOrder::Asc,
                })),
                SortOrder::Desc => Err(EsError::unsupported(&format!("sort on {field} desc"))),
            };
        }
        _ => {}
    }
    let order = order.unwrap_or(SortOrder::Asc);
    if let Some(mode) = mode {
        let default = if order == SortOrder::Asc {
            "min"
        } else {
            "max"
        };
        if mode != default {
            return Err(EsError::unsupported(&format!("sort.mode: {mode}")));
        }
    }
    let phase = |inner: EsError| EsError::search_phase(inner, &view.name, SEARCH_NODE);
    match resolve(view, field) {
        FieldRef::Unmapped if unmapped_type => Ok(None),
        FieldRef::Unmapped => Err(phase(EsError::new(
            400,
            "query_shard_exception",
            format!("No mapping found for [{field}] in order to sort on"),
        ))),
        FieldRef::Id => Err(phase(EsError::illegal_argument(
            "Fielddata access on the _id field is disallowed, you can re-enable it by updating \
             the dynamic cluster setting: indices.id_field_data.enabled",
        ))),
        FieldRef::Vector(_) => Err(EsError::unsupported("sort on a dense_vector field")),
        FieldRef::Field(spec)
            if matches!(spec.kind, FieldKind::Text { .. }) || es_type(view, field) == "text" =>
        {
            Err(phase(EsError::illegal_argument(format!(
                "Text fields are not optimised for operations that require per-document field \
                 data like aggregations and sorting, so these operations are disabled by \
                 default. Please use a keyword field instead. Alternatively, set fielddata=true \
                 on [{field}] in order to load field data by uninverting the inverted index. \
                 Note that this can use significant memory."
            ))))
        }
        FieldRef::Field(_) if es_type(view, field) == "binary" => {
            Err(EsError::unsupported("sort on a binary field"))
        }
        FieldRef::Field(_) | FieldRef::Json { .. } => Ok(Some(SortKey::Field {
            field: field.to_string(),
            order,
            missing,
        })),
    }
}

/// The value of `search_after` for the sort key on `field`; `None` for a
/// `null` (a missing value echoed back).
fn after_value(
    view: &IndexView,
    field: &str,
    v: &Value,
    now_ms: i64,
) -> Result<Option<FieldValue>, EsError> {
    if v.is_null() {
        return Ok(None);
    }
    // A shard failure in ES, with the value's parse error below it (row
    // T11-3).
    let bad = || {
        let cause = match v {
            Value::String(text) => json!({
                "type": "number_format_exception",
                "reason": format!("For input string: \"{text}\""),
            }),
            other => json!({"type": "illegal_argument_exception", "reason": other.to_string()}),
        };
        EsError::search_phase(
            EsError::illegal_argument(format!(
                "Failed to parse search_after value for field [{field}]."
            ))
            .with("caused_by", cause),
            &view.name,
            SEARCH_NODE,
        )
    };
    match resolve(view, field) {
        FieldRef::Json { .. } => Ok(Some(json_class(v))),
        FieldRef::Field(spec) => {
            let ok = match spec.kind {
                FieldKind::Date => v.is_i64() || v.is_u64() || v.is_string(),
                FieldKind::I64 | FieldKind::F64 => v.is_number(),
                FieldKind::Bool => v.is_boolean() || v.is_string(),
                _ => v.is_string() || v.is_number(),
            };
            if !ok {
                return Err(bad());
            }
            coerce_rounded(view, field, v, now_ms, Rounding::Down).map_err(|_| bad())
        }
        _ => Err(bad()),
    }
}

fn bool_of(
    must: Vec<Query>,
    should: Vec<Query>,
    must_not: Vec<Query>,
    filter: Vec<Query>,
) -> Query {
    Query::Bool {
        must,
        should,
        must_not,
        filter,
        minimum_should_match: None,
    }
}

fn not_exists(field: &str) -> Query {
    bool_of(
        Vec::new(),
        Vec::new(),
        vec![Query::Exists {
            field: field.to_string(),
        }],
        Vec::new(),
    )
}

fn any_of(mut parts: Vec<Query>) -> Query {
    match parts.len() {
        0 => Query::MatchNone,
        1 => parts.pop().unwrap_or(Query::MatchNone),
        _ => bool_of(Vec::new(), parts, Vec::new(), Vec::new()),
    }
}

fn all_of(mut parts: Vec<Query>) -> Query {
    match parts.len() {
        0 => Query::MatchAll,
        1 => parts.pop().unwrap_or(Query::MatchAll),
        _ => bool_of(Vec::new(), Vec::new(), Vec::new(), parts),
    }
}

/// The documents after `values` in the order of `keys` (item 6): the
/// lexicographic `OR_i (AND_{j<i} key_j == v_j) AND key_i beyond v_i`, with
/// missing values placed by each key's `missing`. Documents equal on every
/// key are skipped, as ES skips them without a point in time.
pub fn search_after_filter(
    view: &IndexView,
    keys: &[SortKey],
    values: &[Value],
    now_ms: i64,
) -> Result<Query, EsError> {
    if values.len() != keys.len() {
        return Err(EsError::search_phase(
            EsError::illegal_argument(format!(
                "search_after has {} value(s) but sort has {}.",
                values.len(),
                keys.len()
            )),
            &view.name,
            SEARCH_NODE,
        ));
    }
    let mut disjuncts = Vec::new();
    let mut prefix: Vec<Query> = Vec::new();
    for (key, raw) in keys.iter().zip(values) {
        let (field, order, missing) = match key {
            SortKey::Field {
                field,
                order,
                missing,
            } => (field, *order, *missing),
            SortKey::Score { .. } => return Err(EsError::unsupported("search_after on _score")),
            SortKey::Pk { .. } => return Err(EsError::unsupported("search_after on _doc")),
        };
        let value = after_value(view, field, raw, now_ms)?;
        let beyond = match &value {
            Some(v) => {
                let (gt, lt) = match order {
                    SortOrder::Asc => (Some(v.clone()), None),
                    SortOrder::Desc => (None, Some(v.clone())),
                };
                let range = Query::Range {
                    field: field.clone(),
                    gt,
                    gte: None,
                    lt,
                    lte: None,
                };
                match missing {
                    MissingOrder::Last => Some(any_of(vec![range, not_exists(field)])),
                    MissingOrder::First => Some(range),
                }
            }
            None => match missing {
                MissingOrder::Last => None,
                MissingOrder::First => Some(Query::Exists {
                    field: field.clone(),
                }),
            },
        };
        if let Some(beyond) = beyond {
            let mut parts = prefix.clone();
            parts.push(beyond);
            disjuncts.push(all_of(parts));
        }
        prefix.push(match value {
            Some(v) => Query::Term {
                field: field.clone(),
                value: v,
            },
            None => not_exists(field),
        });
    }
    Ok(any_of(disjuncts))
}
