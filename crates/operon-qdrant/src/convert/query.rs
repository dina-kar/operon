//! gRPC query messages to the REST model (Task 7 step 5) and scored points
//! back.

use serde_json::{Map, Value};

use crate::convert::common::{
    vector_output_to_grpc, with_payload_from_grpc, with_vector_from_grpc,
};
use crate::convert::filter::filter_from_grpc;
use crate::convert::points::{id_from_grpc, id_to_grpc};
use crate::convert::value::map_to_payload;
use crate::error::GatewayError;
use crate::model::common::{ScoredPoint, VectorInput};
use crate::model::filter::OneOrMany;
use crate::model::query::{
    ContextPair, DiscoverInput, FusionName, IdfParams, IdfScope, LookupLocation, Mmr, Prefetch,
    QuantizationSearchParams, QueryInterface, QueryKind, QueryRequest, RecommendInput,
    RecommendStrategy, RrfParams, SearchParams,
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
                strategy: r
                    .strategy
                    .map(|s| match pb::RecommendStrategy::try_from(s) {
                        Ok(pb::RecommendStrategy::AverageVector) => {
                            Ok(RecommendStrategy::AverageVector)
                        }
                        Ok(pb::RecommendStrategy::BestScore) => Ok(RecommendStrategy::BestScore),
                        Ok(pb::RecommendStrategy::SumScores) => Ok(RecommendStrategy::SumScores),
                        Err(_) => Err(GatewayError::BadRequest(format!(
                            "unknown recommend strategy {s}"
                        ))),
                    })
                    .transpose()?,
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
