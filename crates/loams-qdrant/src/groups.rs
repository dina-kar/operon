//! Grouped queries (Task 9, Ruling 11): `query/groups` and the legacy
//! `search/groups` and `recommend/groups`, with `with_lookup`.
//!
//! The gateway runs Qdrant's driver (`qdrant:lib/shard/src/grouping/`)
//! over the compiled query ([`compile_query`]): up to five collect requests,
//! each excluding the full groups, then up to five fill requests
//! restricted to the unsatisfied best groups; every request asks for
//! `groups × group_size` points that carry the key and are not aggregated
//! yet. The groups are then distilled in the query's score order.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use loams_collection::{Distance, PrimaryKey};
use loams_query::{Hit, Query, ReadConsistency, Retriever, SourceFilter};
use serde_json::{Map, Value, json};

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::{and, compile_filter, exclude_ids};
use crate::ids::{PointId, pk_to_json};
use crate::jsonpath::JsonPath;
use crate::model::common::{Record, WithPayload, WithVector};
use crate::model::filter::Filter;
use crate::model::points::PointRequest;
use crate::model::query::{GroupsResult, PointGroup, QueryGroupsRequest, WithLookupInterface};
use crate::query::{QueryPlan, ScoreKind, compile_query, execute, resolve_examples};
use crate::reads;

/// Qdrant's request budgets (`qdrant:lib/shard/src/grouping/driver.rs:7-8`).
const COLLECT_REQUESTS: usize = 5;
const FILL_REQUESTS: usize = 5;

/// A group's key: an integer or a string value at `group_by`. Integers
/// order before strings.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupKey {
    Int(i64),
    Str(String),
}

impl GroupKey {
    /// The key as the group's JSON `id`.
    pub fn to_json(&self) -> Value {
        match self {
            GroupKey::Int(n) => json!(n),
            GroupKey::Str(s) => json!(s),
        }
    }

    /// The key as a point id, for `with_lookup`: a non-negative integer, or
    /// a UUID string.
    fn point_id(&self) -> Option<PointId> {
        match self {
            GroupKey::Int(n) if *n >= 0 => PointId::from_json(&json!(n)).ok(),
            GroupKey::Int(_) => None,
            GroupKey::Str(s) => PointId::from_json(&Value::String(s.clone())).ok(),
        }
    }
}

/// The groups a point joins: each string or integer value at `path`, an
/// array contributing its elements, each key once. Any other value
/// (a float, bool, `null`, object, or nested array) voids the point's
/// keys, as Qdrant's aggregator skips the point
/// (`qdrant:lib/shard/src/grouping/aggregator.rs` `add_point`).
pub fn group_keys(payload: &Map<String, Value>, path: &JsonPath) -> Vec<GroupKey> {
    let mut out = Vec::new();
    for value in path.value_get(payload) {
        let items = match value {
            Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        for item in items {
            let key = match item {
                Value::String(s) => GroupKey::Str(s.clone()),
                Value::Number(n) => match n.as_i64() {
                    Some(n) => GroupKey::Int(n),
                    None => return Vec::new(),
                },
                _ => return Vec::new(),
            };
            if !out.contains(&key) {
                out.push(key);
            }
        }
    }
    out
}

/// The driver's phase (`driver.rs` `State`).
#[derive(Clone, Copy, Debug)]
enum State {
    Collecting { left: usize, fill: usize },
    Filling { left: usize },
    Done,
}

/// The groups found so far (`aggregator.rs` `GroupsAggregator`).
struct Aggregator {
    path: JsonPath,
    limit: usize,
    size: usize,
    larger_is_better: bool,
    groups: HashMap<GroupKey, BTreeMap<PrimaryKey, (Hit, f32)>>,
    full: HashSet<GroupKey>,
    best: HashMap<GroupKey, (f32, PrimaryKey)>,
    ids: BTreeSet<PrimaryKey>,
}

impl Aggregator {
    /// `Less` when score `a` comes before score `b` in the query's order.
    fn order(&self, a: f32, b: f32) -> Ordering {
        if self.larger_is_better {
            b.total_cmp(&a)
        } else {
            a.total_cmp(&b)
        }
    }

    /// Adds each hit to the groups of its keys.
    fn add(&mut self, hits: Vec<(Hit, f32)>) {
        for (hit, score) in hits {
            let keys = hit
                .source
                .as_ref()
                .map(|source| group_keys(source, &self.path))
                .unwrap_or_default();
            for key in keys {
                let group = self.groups.entry(key.clone()).or_default();
                if !group.contains_key(&hit.pk) {
                    group.insert(hit.pk.clone(), (hit.clone(), score));
                    self.ids.insert(hit.pk.clone());
                }
                if group.len() == self.size {
                    self.full.insert(key.clone());
                }
                let better = match self.best.get(&key) {
                    None => true,
                    Some(&(best, _)) => self.order(score, best) == Ordering::Less,
                };
                if better {
                    self.best.insert(key, (score, hit.pk.clone()));
                }
            }
        }
    }

    /// The best `limit` keys: by best hit in the query's order, then key.
    fn best_keys(&self) -> Vec<GroupKey> {
        let mut pairs: Vec<(&GroupKey, f32)> =
            self.best.iter().map(|(k, (s, _))| (k, *s)).collect();
        pairs.sort_by(|a, b| self.order(a.1, b.1).then_with(|| a.0.cmp(b.0)));
        pairs
            .into_iter()
            .take(self.limit)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// The best keys whose group is not full yet.
    fn unfilled_best(&self) -> Vec<GroupKey> {
        self.best_keys()
            .into_iter()
            .filter(|k| !self.full.contains(k))
            .collect()
    }

    /// How many of the best groups are full.
    fn filled_best(&self) -> usize {
        self.best_keys()
            .iter()
            .filter(|k| self.full.contains(*k))
            .count()
    }

    /// The full groups' keys, sorted.
    fn full_keys(&self) -> Vec<GroupKey> {
        let mut keys: Vec<GroupKey> = self.full.iter().cloned().collect();
        keys.sort();
        keys
    }

    /// The best groups, each with its hits in the query's order (then by
    /// key), at most `size` of them.
    fn distill(mut self) -> Vec<(GroupKey, Vec<(Hit, f32)>)> {
        let keys = self.best_keys();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let mut hits: Vec<(Hit, f32)> = self
                .groups
                .remove(&key)
                .unwrap_or_default()
                .into_values()
                .collect();
            hits.sort_by(|a, b| self.order(a.1, b.1).then_with(|| a.0.pk.cmp(&b.0.pk)));
            hits.truncate(self.size);
            out.push((key, hits));
        }
        out
    }
}

/// The match conditions on `key` for `keys`: one `any` of the integers and
/// one of the strings.
fn key_conditions(key: &str, keys: &[GroupKey]) -> Vec<Value> {
    let ints: Vec<i64> = keys
        .iter()
        .filter_map(|k| match k {
            GroupKey::Int(n) => Some(*n),
            GroupKey::Str(_) => None,
        })
        .collect();
    let strs: Vec<&str> = keys
        .iter()
        .filter_map(|k| match k {
            GroupKey::Str(s) => Some(s.as_str()),
            GroupKey::Int(_) => None,
        })
        .collect();
    let mut out = Vec::new();
    if !ints.is_empty() {
        out.push(json!({"key": key, "match": {"any": ints}}));
    }
    if !strs.is_empty() {
        out.push(json!({"key": key, "match": {"any": strs}}));
    }
    out
}

/// Scales every retriever under `r` by `by` (Qdrant's
/// `increase_limit_for_group`: nested prefetch limits × `group_size`).
fn scale_inputs(r: &mut Retriever, by: usize) {
    let inputs: Vec<&mut Retriever> = match r {
        Retriever::Fused { inputs, .. } => inputs.iter_mut().collect(),
        Retriever::Rescore { input, .. } => vec![input.as_mut()],
        Retriever::Vector { .. } | Retriever::Text { .. } | Retriever::Sparse { .. } => Vec::new(),
    };
    for input in inputs {
        match input {
            Retriever::Vector { k, .. }
            | Retriever::Text { k, .. }
            | Retriever::Fused { k, .. }
            | Retriever::Rescore { k, .. }
            | Retriever::Sparse { k, .. } => *k = k.saturating_mul(by),
        }
        scale_inputs(input, by);
    }
}

/// Semantics 1–4: the groups of `req` over `collection`.
pub(crate) async fn run_groups(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    req: QueryGroupsRequest,
) -> Result<GroupsResult, GatewayError> {
    let group_size = req.group_size.unwrap_or(3);
    if group_size == 0 {
        return Err(GatewayError::BadRequest(
            "group_size must be at least 1".to_string(),
        ));
    }
    let limit = req.query.limit.unwrap_or(10);
    if limit == 0 {
        return Err(GatewayError::BadRequest(
            "limit must be at least 1".to_string(),
        ));
    }
    let path: JsonPath = req.group_by.parse()?;
    let info = gw.service().get_collection(&ctx.ns, &collection).await?;
    // Every request is the root query from offset 0 for groups × size.
    let mut query = req.query;
    query.offset = None;
    query.limit = Some(limit.saturating_mul(group_size));
    let examples = resolve_examples(&gw, &ctx, &info, &query).await?;
    let mut plan = compile_query(&query, &info.name, &info.schema, &examples, gw.config())?;
    let post = plan.post().clone();
    if let QueryPlan::Ir { request, .. } = &mut plan {
        request
            .retrievers
            .iter_mut()
            .for_each(|r| scale_inputs(r, group_size));
    }
    // The keys are read from the source; the hits render with the
    // request's selectors at the end.
    for request in plan.requests_mut() {
        request.select.source = SourceFilter::All;
    }
    let compile = |v: Value| -> Result<Option<Query>, GatewayError> {
        let filter: Filter =
            serde_json::from_value(v).map_err(|e| GatewayError::json(e.to_string()))?;
        compile_filter(&filter, &info.schema).map(Some)
    };
    let has_key = compile(json!({"must_not": [{"is_empty": {"key": req.group_by}}]}))?;
    let larger_is_better = !(post.kind == ScoreKind::Distance
        && matches!(post.distance, Distance::Euclid | Distance::Manhattan));
    let mut agg = Aggregator {
        path,
        limit,
        size: group_size,
        larger_is_better,
        groups: HashMap::new(),
        full: HashSet::new(),
        best: HashMap::new(),
        ids: BTreeSet::new(),
    };
    let mut state = State::Collecting {
        left: COLLECT_REQUESTS,
        fill: FILL_REQUESTS,
    };
    let mut consistency = ctx.consistency.clone();
    let mut first = true;
    loop {
        let extra = match state {
            State::Collecting { left: 0, fill } => {
                state = State::Filling { left: fill };
                continue;
            }
            State::Collecting { left, fill } => {
                state = State::Collecting {
                    left: left - 1,
                    fill,
                };
                // Not the groups that are full already.
                let conditions = key_conditions(&req.group_by, &agg.full_keys());
                if conditions.is_empty() {
                    None
                } else {
                    compile(json!({ "must_not": conditions }))?
                }
            }
            State::Filling { left: 0 } | State::Done => break,
            State::Filling { left } => {
                state = State::Filling { left: left - 1 };
                // Only the best groups that are not full yet.
                let conditions = key_conditions(&req.group_by, &agg.unfilled_best());
                if conditions.is_empty() {
                    None
                } else {
                    compile(json!({ "should": conditions }))?
                }
            }
        };
        let ids: Vec<PrimaryKey> = agg.ids.iter().cloned().collect();
        let filter = exclude_ids(and(has_key.clone(), extra), &ids);
        let mut round = plan.clone();
        for request in round.requests_mut() {
            request.filter = and(request.filter.take(), filter.clone());
        }
        let (hits, token) = Box::pin(execute(&gw, &ctx.ns, consistency.clone(), round)).await?;
        if first && let Some(token) = token {
            consistency = ReadConsistency::AtLeast(token);
            first = false;
        }
        let empty = hits.is_empty();
        agg.add(hits);
        let enough = agg.filled_best() >= limit;
        state = match state {
            State::Collecting { .. } if enough => State::Done,
            State::Collecting { fill, .. } if empty => State::Filling { left: fill },
            State::Filling { .. } if enough || empty => State::Done,
            other => other,
        };
    }
    let groups = agg.distill();
    let mut lookups = match &req.with_lookup {
        Some(lookup) => lookup_points(&gw, &ctx, lookup, &groups).await?,
        None => HashMap::new(),
    };
    Ok(GroupsResult {
        groups: groups
            .into_iter()
            .map(|(key, hits)| PointGroup {
                id: key.to_json(),
                hits: hits
                    .into_iter()
                    .map(|(hit, score)| post.render(hit, score))
                    .collect(),
                lookup: lookups.remove(&key),
            })
            .collect(),
    })
}

/// Semantics 4: each group key that is a point id, retrieved from the
/// lookup collection (payload by default, no vectors); keys without a
/// point get nothing. A missing collection is 404 even without ids.
async fn lookup_points(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    lookup: &WithLookupInterface,
    groups: &[(GroupKey, Vec<(Hit, f32)>)],
) -> Result<HashMap<GroupKey, Record>, GatewayError> {
    let (collection, with_payload, with_vector) = match lookup {
        WithLookupInterface::Collection(c) => (c, WithPayload::Bool(true), WithVector::Bool(false)),
        WithLookupInterface::Lookup {
            collection,
            with_payload,
            with_vectors,
        } => (
            collection,
            with_payload.clone().unwrap_or(WithPayload::Bool(true)),
            with_vectors.clone().unwrap_or(WithVector::Bool(false)),
        ),
    };
    let mut by_id: HashMap<String, GroupKey> = HashMap::new();
    for (key, _) in groups {
        if let Some(id) = key.point_id() {
            by_id.insert(pk_to_json(&id.to_pk()).to_string(), key.clone());
        }
    }
    if by_id.is_empty() {
        gw.service().get_collection(&ctx.ns, collection).await?;
        return Ok(HashMap::new());
    }
    let ids: Vec<Value> = groups
        .iter()
        .filter_map(|(key, _)| key.point_id())
        .map(|id| pk_to_json(&id.to_pk()))
        .collect();
    let request = PointRequest {
        ids,
        with_payload: Some(with_payload),
        with_vector: Some(with_vector),
    };
    let records = reads::retrieve(gw.clone(), ctx.clone(), collection.clone(), request).await?;
    Ok(records
        .into_iter()
        .filter_map(|r| by_id.get(&r.id.to_string()).cloned().map(|key| (key, r)))
        .collect())
}
