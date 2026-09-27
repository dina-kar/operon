//! gRPC query messages to the REST model (Task 7 step 5) and scored points
//! back; Task 8 adds the legacy search, recommend and discover messages
//! (semantics 6), Task 9 the group messages and results.

use serde_json::{Map, Value};

use crate::convert::common::{
    vector_output_to_grpc, with_payload_from_grpc, with_vector_from_grpc,
};
use crate::convert::filter::filter_from_grpc;
use crate::convert::points::{id_from_grpc, id_to_grpc, record_to_grpc, vector_input};
use crate::convert::value::map_to_payload;
use crate::error::GatewayError;
use crate::model::common::{ScoredPoint, VectorInput};
use crate::model::filter::OneOrMany;
use crate::model::query::{
    ContextPair, DiscoverInput, FusionName, GroupsResult, IdfParams, IdfScope, LookupLocation, Mmr,
    Prefetch, QuantizationSearchParams, QueryGroupsRequest, QueryInterface, QueryKind,
    QueryRequest, RecommendInput, RecommendStrategy, RrfParams, SearchParams, WithLookupInterface,
    no_discover_input,
};
use crate::proto::qdrant as pb;

/// `v`, or `<what> is required`.
fn required<T>(v: Option<T>, what: &str) -> Result<T, GatewayError> {
    v.ok_or_else(|| GatewayError::BadRequest(format!("{what} is required")))
}

/// `VectorInput`'s oneof: an id, a dense, sparse or multi vector, or an
/// inference object.
pub fn vector_input_from_grpc(v: &pb::VectorInput) -> Result<VectorInput, GatewayError> {
    use pb::vector_input::Variant;
    Ok(match required(v.variant.as_ref(), "VectorInput")? {
        Variant::Id(id) => VectorInput::Id(id_from_grpc(Some(id))?),
        Variant::Dense(d) => VectorInput::Dense(d.data.clone()),
        Variant::Sparse(s) => VectorInput::Sparse {
            indices: s.indices.clone(),
            values: s.values.clone(),
        },
        Variant::MultiDense(m) => {
            VectorInput::Multi(m.vectors.iter().map(|d| d.data.clone()).collect())
        }
        Variant::Document(_) | Variant::Image(_) | Variant::Object(_) => {
            VectorInput::Object(Map::new())
        }
    })
}

/// A required vector input.
fn input(v: Option<&pb::VectorInput>, what: &str) -> Result<VectorInput, GatewayError> {
    vector_input_from_grpc(required(v, what)?)
}

/// A context's pairs; none without a context.
fn pairs(c: Option<&pb::ContextInput>) -> Result<Vec<ContextPair>, GatewayError> {
    c.map_or(&[][..], |c| &c.pairs)
        .iter()
        .map(|p| {
            Ok(ContextPair {
                positive: input(p.positive.as_ref(), "positive")?,
                negative: input(p.negative.as_ref(), "negative")?,
            })
        })
        .collect()
}

/// Several vector inputs.
fn inputs(vs: &[pb::VectorInput]) -> Result<Vec<VectorInput>, GatewayError> {
    vs.iter().map(vector_input_from_grpc).collect()
}

/// `Query`'s oneof; an unset one is no query.
fn query_from_grpc(q: Option<&pb::Query>) -> Result<Option<QueryInterface>, GatewayError> {
    use pb::query::Variant;
    let Some(variant) = q.and_then(|q| q.variant.as_ref()) else {
        return Ok(None);
    };
    let kind = match variant {
        Variant::Nearest(v) => QueryKind::Nearest {
            nearest: vector_input_from_grpc(v)?,
            mmr: None,
        },
        Variant::NearestWithMmr(n) => {
            let mmr = n.mmr.as_ref();
            QueryKind::Nearest {
                nearest: input(n.nearest.as_ref(), "nearest")?,
                mmr: Some(Mmr {
                    diversity: mmr.and_then(|m| m.diversity),
                    candidates_limit: mmr.and_then(|m| m.candidates_limit).map(|n| n as usize),
                }),
            }
        }
        Variant::Recommend(r) => QueryKind::Recommend {
            recommend: RecommendInput {
                positive: inputs(&r.positive)?,
                negative: inputs(&r.negative)?,
                strategy: r.strategy.map(strategy_from_grpc).transpose()?,
            },
        },
        Variant::Discover(d) => QueryKind::Discover {
            discover: DiscoverInput {
                target: input(d.target.as_ref(), "target")?,
                context: Some(OneOrMany::Many(pairs(d.context.as_ref())?)),
            },
        },
        Variant::Context(c) => QueryKind::Context {
            context: OneOrMany::Many(pairs(Some(c))?),
        },
        Variant::Fusion(f) => QueryKind::Fusion {
            fusion: match pb::Fusion::try_from(*f) {
                Ok(pb::Fusion::Rrf) => FusionName::Rrf,
                Ok(pb::Fusion::Dbsf) => FusionName::Dbsf,
                Err(_) => {
                    return Err(GatewayError::BadRequest(format!("unknown fusion {f}")));
                }
            },
        },
        Variant::Rrf(r) => QueryKind::Rrf {
            rrf: RrfParams {
                k: r.k,
                weights: (!r.weights.is_empty()).then(|| r.weights.clone()),
            },
        },
        Variant::OrderBy(_) => QueryKind::OrderBy {
            order_by: Value::Null,
        },
        Variant::Formula(_) => QueryKind::Formula {
            formula: Value::Null,
        },
        Variant::Sample(_) => QueryKind::Sample {
            sample: Value::Null,
        },
        Variant::RelevanceFeedback(_) => QueryKind::RelevanceFeedback {
            relevance_feedback: Value::Null,
        },
    };
    Ok(Some(QueryInterface::Query(kind)))
}

/// A `RecommendStrategy` number.
fn strategy_from_grpc(s: i32) -> Result<RecommendStrategy, GatewayError> {
    match pb::RecommendStrategy::try_from(s) {
        Ok(pb::RecommendStrategy::AverageVector) => Ok(RecommendStrategy::AverageVector),
        Ok(pb::RecommendStrategy::BestScore) => Ok(RecommendStrategy::BestScore),
        Ok(pb::RecommendStrategy::SumScores) => Ok(RecommendStrategy::SumScores),
        Err(_) => Err(GatewayError::BadRequest(format!(
            "unknown recommend strategy {s}"
        ))),
    }
}

/// `SearchParams`; `idf` set without a corpus is `"global"`.
fn params_from_grpc(p: &pb::SearchParams) -> Result<SearchParams, GatewayError> {
    Ok(SearchParams {
        hnsw_ef: p.hnsw_ef.map(|ef| u32::try_from(ef).unwrap_or(u32::MAX)),
        exact: p.exact.unwrap_or(false),
        quantization: p.quantization.as_ref().map(|q| QuantizationSearchParams {
            ignore: q.ignore.unwrap_or(false),
            rescore: q.rescore,
            oversampling: q.oversampling,
        }),
        indexed_only: p.indexed_only.unwrap_or(false),
        acorn: p.acorn.as_ref().map(|_| Value::Bool(true)),
        idf: p
            .idf
            .as_ref()
            .map(|idf| -> Result<IdfParams, GatewayError> {
                Ok(match &idf.corpus {
                    None => IdfParams::Scope(IdfScope::Global),
                    Some(f) => IdfParams::Corpus {
                        corpus: Box::new(filter_from_grpc(f)?),
                    },
                })
            })
            .transpose()?,
    })
}

/// `LookupLocation`; a shard key selector is kept as present.
fn lookup_from_grpc(l: &pb::LookupLocation) -> LookupLocation {
    LookupLocation {
        collection: l.collection_name.clone(),
        vector: l.vector_name.clone(),
        shard_key: l.shard_key_selector.as_ref().map(|_| Value::Bool(true)),
    }
}

/// The prefetches; none when the list is empty.
fn prefetches(ps: &[pb::PrefetchQuery]) -> Result<Option<Vec<Prefetch>>, GatewayError> {
    if ps.is_empty() {
        return Ok(None);
    }
    ps.iter()
        .map(prefetch_from_grpc)
        .collect::<Result<_, _>>()
        .map(Some)
}

/// One `PrefetchQuery`.
pub fn prefetch_from_grpc(p: &pb::PrefetchQuery) -> Result<Prefetch, GatewayError> {
    Ok(Prefetch {
        prefetch: prefetches(&p.prefetch)?,
        query: query_from_grpc(p.query.as_ref())?,
        using: p.using.clone(),
        filter: p.filter.as_ref().map(filter_from_grpc).transpose()?,
        params: p.params.as_ref().map(params_from_grpc).transpose()?,
        score_threshold: p.score_threshold,
        limit: p.limit.map(|l| l as usize),
        lookup_from: p.lookup_from.as_ref().map(lookup_from_grpc),
    })
}

/// `QueryPoints`, with REST's defaults: no payload and no vectors unless
/// asked for. `read_consistency` is ignored (Ruling 14).
pub fn query_request_from_grpc(q: &pb::QueryPoints) -> Result<QueryRequest, GatewayError> {
    Ok(QueryRequest {
        prefetch: prefetches(&q.prefetch)?,
        query: query_from_grpc(q.query.as_ref())?,
        using: q.using.clone(),
        filter: q.filter.as_ref().map(filter_from_grpc).transpose()?,
        params: q.params.as_ref().map(params_from_grpc).transpose()?,
        score_threshold: q.score_threshold,
        limit: q.limit.map(|l| l as usize),
        offset: q.offset.map(|o| o as usize),
        with_payload: Some(with_payload_from_grpc(q.with_payload.as_ref(), false)),
        with_vector: Some(with_vector_from_grpc(q.with_vectors.as_ref(), false)),
        lookup_from: q.lookup_from.as_ref().map(lookup_from_grpc),
        shard_key: q.shard_key_selector.as_ref().map(|_| Value::Bool(true)),
    })
}

/// A `ScoredPoint` as gRPC sends it.
pub fn scored_point_to_grpc(p: &ScoredPoint) -> pb::ScoredPoint {
    pb::ScoredPoint {
        id: Some(id_to_grpc(&p.id)),
        payload: p.payload.as_ref().map(map_to_payload).unwrap_or_default(),
        score: p.score,
        version: p.version,
        vectors: p.vector.as_ref().map(vector_output_to_grpc),
        shard_key: None,
        order_value: None,
    }
}

// ----- legacy methods (Task 8 semantics 6) -----

/// The fields every legacy method shares, with REST's defaults: no
/// payload and no vectors unless asked for; a shard key selector is kept
/// as present (501).
struct Common<'a> {
    filter: Option<&'a pb::Filter>,
    params: Option<&'a pb::SearchParams>,
    limit: u64,
    offset: Option<u64>,
    with_payload: Option<&'a pb::WithPayloadSelector>,
    with_vectors: Option<&'a pb::WithVectorsSelector>,
    shard_key: bool,
}

impl Common<'_> {
    /// A `QueryRequest` with these fields and `query`, `using` and
    /// `lookup_from` unset.
    fn request(&self) -> Result<QueryRequest, GatewayError> {
        Ok(QueryRequest {
            filter: self.filter.map(filter_from_grpc).transpose()?,
            params: self.params.map(params_from_grpc).transpose()?,
            limit: Some(usize::try_from(self.limit).unwrap_or(usize::MAX)),
            offset: self
                .offset
                .map(|o| usize::try_from(o).unwrap_or(usize::MAX)),
            with_payload: Some(with_payload_from_grpc(self.with_payload, false)),
            with_vector: Some(with_vector_from_grpc(self.with_vectors, false)),
            shard_key: self.shard_key.then_some(Value::Bool(true)),
            ..QueryRequest::default()
        })
    }
}

/// A legacy search vector: dense `vector`, or sparse with `sparse_indices`
/// (the values in `vector`).
fn search_input(vector: &[f32], sparse_indices: Option<&pb::SparseIndices>) -> VectorInput {
    match sparse_indices {
        Some(indices) => VectorInput::Sparse {
            indices: indices.data.clone(),
            values: vector.to_vec(),
        },
        None => VectorInput::Dense(vector.to_vec()),
    }
}

/// `SearchPoints`: a nearest query on `vector_name`.
pub fn search_from_grpc(r: &pb::SearchPoints) -> Result<QueryRequest, GatewayError> {
    let common = Common {
        filter: r.filter.as_ref(),
        params: r.params.as_ref(),
        limit: r.limit,
        offset: r.offset,
        with_payload: r.with_payload.as_ref(),
        with_vectors: r.with_vectors.as_ref(),
        shard_key: r.shard_key_selector.is_some(),
    };
    Ok(QueryRequest {
        query: Some(QueryInterface::Vector(search_input(
            &r.vector,
            r.sparse_indices.as_ref(),
        ))),
        using: r.vector_name.clone(),
        score_threshold: r.score_threshold,
        ..common.request()?
    })
}

/// Ids, then vectors, as recommend examples.
fn examples(ids: &[pb::PointId], vectors: &[pb::Vector]) -> Result<Vec<VectorInput>, GatewayError> {
    let mut out = ids
        .iter()
        .map(|id| id_from_grpc(Some(id)).map(VectorInput::Id))
        .collect::<Result<Vec<_>, _>>()?;
    out.extend(vectors.iter().map(vector_input));
    Ok(out)
}

/// `RecommendPoints`: a `recommend` query (ids and vectors).
pub fn recommend_from_grpc(r: &pb::RecommendPoints) -> Result<QueryRequest, GatewayError> {
    let common = Common {
        filter: r.filter.as_ref(),
        params: r.params.as_ref(),
        limit: r.limit,
        offset: r.offset,
        with_payload: r.with_payload.as_ref(),
        with_vectors: r.with_vectors.as_ref(),
        shard_key: r.shard_key_selector.is_some(),
    };
    Ok(QueryRequest {
        query: Some(QueryInterface::Query(QueryKind::Recommend {
            recommend: RecommendInput {
                positive: examples(&r.positive, &r.positive_vectors)?,
                negative: examples(&r.negative, &r.negative_vectors)?,
                strategy: r.strategy.map(strategy_from_grpc).transpose()?,
            },
        })),
        using: r.using.clone(),
        score_threshold: r.score_threshold,
        lookup_from: r.lookup_from.as_ref().map(lookup_from_grpc),
        ..common.request()?
    })
}

/// A `VectorExample`: an id or a vector.
fn example(e: Option<&pb::VectorExample>, what: &str) -> Result<VectorInput, GatewayError> {
    use pb::vector_example::Example;
    match required(e.and_then(|e| e.example.as_ref()), what)? {
        Example::Id(id) => Ok(VectorInput::Id(id_from_grpc(Some(id))?)),
        Example::Vector(v) => Ok(vector_input(v)),
    }
}

/// `DiscoverPoints`: `discover` with a target, else `context`.
pub fn discover_from_grpc(r: &pb::DiscoverPoints) -> Result<QueryRequest, GatewayError> {
    use pb::target_vector::Target;
    let common = Common {
        filter: r.filter.as_ref(),
        params: r.params.as_ref(),
        limit: r.limit,
        offset: r.offset,
        with_payload: r.with_payload.as_ref(),
        with_vectors: r.with_vectors.as_ref(),
        shard_key: r.shard_key_selector.is_some(),
    };
    let context = r
        .context
        .iter()
        .map(|p| {
            Ok(ContextPair {
                positive: example(p.positive.as_ref(), "positive")?,
                negative: example(p.negative.as_ref(), "negative")?,
            })
        })
        .collect::<Result<Vec<_>, GatewayError>>()?;
    let kind = match &r.target {
        None if context.is_empty() => return Err(no_discover_input()),
        None => QueryKind::Context {
            context: OneOrMany::Many(context),
        },
        Some(t) => {
            let single = t.target.as_ref().map(|Target::Single(single)| single);
            QueryKind::Discover {
                discover: DiscoverInput {
                    target: example(single, "target")?,
                    context: Some(OneOrMany::Many(context)),
                },
            }
        }
    };
    Ok(QueryRequest {
        query: Some(QueryInterface::Query(kind)),
        using: r.using.clone(),
        lookup_from: r.lookup_from.as_ref().map(lookup_from_grpc),
        ..common.request()?
    })
}

// ----- groups (Task 9 semantics 5) -----

/// `WithLookup`: payload unless told otherwise, no vectors.
fn with_lookup_from_grpc(l: &pb::WithLookup) -> WithLookupInterface {
    WithLookupInterface::Lookup {
        collection: l.collection.clone(),
        with_payload: Some(with_payload_from_grpc(l.with_payload.as_ref(), true)),
        with_vectors: Some(with_vector_from_grpc(l.with_vectors.as_ref(), false)),
    }
}

/// `QueryPointGroups`: a universal query whose `limit` is the number of
/// groups.
pub fn query_groups_from_grpc(
    g: &pb::QueryPointGroups,
) -> Result<QueryGroupsRequest, GatewayError> {
    let query = QueryRequest {
        prefetch: prefetches(&g.prefetch)?,
        query: query_from_grpc(g.query.as_ref())?,
        using: g.using.clone(),
        filter: g.filter.as_ref().map(filter_from_grpc).transpose()?,
        params: g.params.as_ref().map(params_from_grpc).transpose()?,
        score_threshold: g.score_threshold,
        limit: g.limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
        offset: None,
        with_payload: Some(with_payload_from_grpc(g.with_payload.as_ref(), false)),
        with_vector: Some(with_vector_from_grpc(g.with_vectors.as_ref(), false)),
        lookup_from: g.lookup_from.as_ref().map(lookup_from_grpc),
        shard_key: g.shard_key_selector.as_ref().map(|_| Value::Bool(true)),
    };
    Ok(QueryGroupsRequest {
        query,
        group_by: g.group_by.clone(),
        group_size: g
            .group_size
            .map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
        with_lookup: g.with_lookup.as_ref().map(with_lookup_from_grpc),
    })
}

/// `SearchPointGroups` (legacy).
pub fn search_groups_from_grpc(
    g: &pb::SearchPointGroups,
) -> Result<QueryGroupsRequest, GatewayError> {
    let common = Common {
        filter: g.filter.as_ref(),
        params: g.params.as_ref(),
        limit: u64::from(g.limit),
        offset: None,
        with_payload: g.with_payload.as_ref(),
        with_vectors: g.with_vectors.as_ref(),
        shard_key: g.shard_key_selector.is_some(),
    };
    let query = QueryRequest {
        query: Some(QueryInterface::Vector(search_input(
            &g.vector,
            g.sparse_indices.as_ref(),
        ))),
        using: g.vector_name.clone(),
        score_threshold: g.score_threshold,
        ..common.request()?
    };
    Ok(QueryGroupsRequest {
        query,
        group_by: g.group_by.clone(),
        group_size: Some(g.group_size as usize),
        with_lookup: g.with_lookup.as_ref().map(with_lookup_from_grpc),
    })
}

/// `RecommendPointGroups` (legacy).
pub fn recommend_groups_from_grpc(
    g: &pb::RecommendPointGroups,
) -> Result<QueryGroupsRequest, GatewayError> {
    let common = Common {
        filter: g.filter.as_ref(),
        params: g.params.as_ref(),
        limit: u64::from(g.limit),
        offset: None,
        with_payload: g.with_payload.as_ref(),
        with_vectors: g.with_vectors.as_ref(),
        shard_key: g.shard_key_selector.is_some(),
    };
    let query = QueryRequest {
        query: Some(QueryInterface::Query(QueryKind::Recommend {
            recommend: RecommendInput {
                positive: examples(&g.positive, &g.positive_vectors)?,
                negative: examples(&g.negative, &g.negative_vectors)?,
                strategy: g.strategy.map(strategy_from_grpc).transpose()?,
            },
        })),
        using: g.using.clone(),
        score_threshold: g.score_threshold,
        lookup_from: g.lookup_from.as_ref().map(lookup_from_grpc),
        ..common.request()?
    };
    Ok(QueryGroupsRequest {
        query,
        group_by: g.group_by.clone(),
        group_size: Some(g.group_size as usize),
        with_lookup: g.with_lookup.as_ref().map(with_lookup_from_grpc),
    })
}

/// A group's JSON key as a `GroupId`: a non-negative integer is
/// `unsigned_value`, a negative one `integer_value`, a string
/// `string_value`.
fn group_id_to_grpc(id: &Value) -> pb::GroupId {
    use pb::group_id::Kind;
    let kind = match id {
        Value::String(s) => Some(Kind::StringValue(s.clone())),
        Value::Number(n) => n
            .as_u64()
            .map(Kind::UnsignedValue)
            .or_else(|| n.as_i64().map(Kind::IntegerValue)),
        _ => None,
    };
    pb::GroupId { kind }
}

/// A `GroupsResult` as gRPC sends it.
pub fn groups_to_grpc(r: &GroupsResult) -> pb::GroupsResult {
    pb::GroupsResult {
        groups: r
            .groups
            .iter()
            .map(|g| pb::PointGroup {
                id: Some(group_id_to_grpc(&g.id)),
                hits: g.hits.iter().map(scored_point_to_grpc).collect(),
                lookup: g.lookup.as_ref().map(record_to_grpc),
            })
            .collect(),
    }
}
