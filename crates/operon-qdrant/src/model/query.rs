//! The universal query's request and response types (Task 7).
//!
//! `QueryInterface`, `QueryKind`, `IdfParams` and the lists that may be one
//! item are read by key rather than as `#[serde(untagged)]` unions:
//! `VectorInput` takes any JSON object (an inference object), so an
//! untagged `QueryInterface` would never reach the query kinds, and a
//! malformed filter inside `idf.corpus` keeps its own error text (Ruling 4;
//! rows T5-1, T7-2).

use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::model::common::{ScoredPoint, VectorInput, WithPayload, WithVector};
use crate::model::filter::{Filter, OneOrMany};

/// `T` from a JSON value, with the value's own error text.
fn from_value<T: DeserializeOwned, E: serde::de::Error>(v: Value) -> Result<T, E> {
    T::deserialize(v).map_err(E::custom)
}

/// A list, or one item standing for a list of one; read by shape, so an
/// item's own error text survives.
fn one_or_many<'de, D, T>(d: D) -> Result<Option<OneOrMany<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    match Option::<Value>::deserialize(d)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) => items
            .into_iter()
            .map(from_value)
            .collect::<Result<Vec<T>, _>>()
            .map(|items| Some(OneOrMany::Many(items))),
        Some(item) => from_value(item).map(|item| Some(OneOrMany::One(item))),
    }
}

/// A list, or one item standing for a list of one, as a `Vec` (a
/// prefetch holds prefetches, so it cannot hold one inline).
fn vec_or_one<'de, D, T>(d: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    Ok(one_or_many(d)?.map(|items| match items {
        OneOrMany::Many(items) => items,
        OneOrMany::One(item) => vec![item],
    }))
}

/// `POST /collections/{c}/points/query`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct QueryRequest {
    #[serde(default, deserialize_with = "vec_or_one")]
    pub prefetch: Option<Vec<Prefetch>>,
    #[serde(default)]
    pub query: Option<QueryInterface>,
    #[serde(default)]
    pub using: Option<String>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub params: Option<SearchParams>,
    #[serde(default)]
    pub score_threshold: Option<f32>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub with_payload: Option<WithPayload>,
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
    #[serde(default)]
    pub shard_key: Option<Value>,
}

/// One prefetch: a sub-query whose results the parent query reads.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Prefetch {
    #[serde(default, deserialize_with = "vec_or_one")]
    pub prefetch: Option<Vec<Prefetch>>,
    #[serde(default)]
    pub query: Option<QueryInterface>,
    #[serde(default)]
    pub using: Option<String>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub params: Option<SearchParams>,
    #[serde(default)]
    pub score_threshold: Option<f32>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub lookup_from: Option<LookupLocation>,
}

/// A query: a bare vector (nearest) or one of the query kinds.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryInterface {
    Vector(VectorInput),
    Query(QueryKind),
}

/// The keys that make an object a query kind, in Qdrant's variant order.
const QUERY_KEYS: [&str; 10] = [
    "nearest",
    "recommend",
    "discover",
    "context",
    "fusion",
    "rrf",
    "order_by",
    "formula",
    "sample",
    "relevance_feedback",
];

impl<'de> Deserialize<'de> for QueryInterface {
    /// An object holding a query key is a query kind; anything else is a
    /// vector input (dense, multi, sparse, id or inference object).
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        if let Value::Object(map) = &v
            && QUERY_KEYS.iter().any(|k| map.contains_key(*k))
        {
            return from_value(v).map(QueryInterface::Query);
        }
        from_value(v).map(QueryInterface::Vector)
    }
}

/// The query kinds.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryKind {
    Nearest {
        nearest: VectorInput,
        mmr: Option<Mmr>,
    },
    Recommend {
        recommend: RecommendInput,
    },
    Discover {
        discover: DiscoverInput,
    },
    Context {
        context: OneOrMany<ContextPair>,
    },
    Fusion {
        fusion: FusionName,
    },
    Rrf {
        rrf: RrfParams,
    },
    OrderBy {
        order_by: Value,
    },
    Formula {
        formula: Value,
    },
    Sample {
        sample: Value,
    },
    RelevanceFeedback {
        relevance_feedback: Value,
    },
}

impl QueryKind {
    /// The kind's name in errors (`recommend`, `mmr`, …).
    pub fn name(&self) -> &'static str {
        match self {
            QueryKind::Nearest { mmr: Some(_), .. } => "mmr",
            QueryKind::Nearest { .. } => "nearest",
            QueryKind::Recommend { .. } => "recommend",
            QueryKind::Discover { .. } => "discover",
            QueryKind::Context { .. } => "context",
            QueryKind::Fusion { .. } => "fusion",
            QueryKind::Rrf { .. } => "rrf",
            QueryKind::OrderBy { .. } => "order_by",
            QueryKind::Formula { .. } => "formula",
            QueryKind::Sample { .. } => "sample",
            QueryKind::RelevanceFeedback { .. } => "relevance_feedback",
        }
    }
}

impl<'de> Deserialize<'de> for QueryKind {
    /// By the first query key present, in Qdrant's variant order.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut map = Map::<String, Value>::deserialize(d)?;
        let Some(key) = QUERY_KEYS.iter().find(|k| map.contains_key(**k)) else {
            return Err(D::Error::custom(
                "data did not match any variant of untagged enum QueryInterface",
            ));
        };
        let v = map.remove(*key).unwrap_or(Value::Null);
        Ok(match *key {
            "nearest" => QueryKind::Nearest {
                nearest: from_value(v)?,
                mmr: match map.remove("mmr") {
                    None | Some(Value::Null) => None,
                    Some(m) => Some(from_value(m)?),
                },
            },
            "recommend" => QueryKind::Recommend {
                recommend: from_value(v)?,
            },
            "discover" => QueryKind::Discover {
                discover: from_value(v)?,
            },
            "context" => QueryKind::Context {
                context: one_or_many(v)
                    .map_err(D::Error::custom)?
                    .unwrap_or(OneOrMany::Many(Vec::new())),
            },
            "fusion" => QueryKind::Fusion {
                fusion: from_value(v)?,
            },
            "rrf" => QueryKind::Rrf {
                rrf: from_value(v)?,
            },
            "order_by" => QueryKind::OrderBy { order_by: v },
            "formula" => QueryKind::Formula { formula: v },
            "sample" => QueryKind::Sample { sample: v },
            _ => QueryKind::RelevanceFeedback {
                relevance_feedback: v,
            },
        })
    }
}

/// `mmr` next to `nearest` (Task 8).
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Mmr {
    #[serde(default)]
    pub diversity: Option<f32>,
    #[serde(default)]
    pub candidates_limit: Option<usize>,
}

/// `recommend` (Task 8).
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RecommendInput {
    #[serde(default)]
    pub positive: Vec<VectorInput>,
    #[serde(default)]
    pub negative: Vec<VectorInput>,
    #[serde(default)]
    pub strategy: Option<RecommendStrategy>,
}

/// How `recommend` scores (Task 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendStrategy {
    AverageVector,
    BestScore,
    SumScores,
}

/// `discover` (Task 8).
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DiscoverInput {
    pub target: VectorInput,
    #[serde(default, deserialize_with = "one_or_many")]
    pub context: Option<OneOrMany<ContextPair>>,
}

/// A positive and a negative example.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ContextPair {
    pub positive: VectorInput,
    pub negative: VectorInput,
}

/// `fusion`: `"rrf"` or `"dbsf"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FusionName {
    Rrf,
    Dbsf,
}

/// `rrf`: Qdrant's `k` (default 2) and weights (unsupported).
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RrfParams {
    #[serde(default)]
    pub k: Option<u32>,
    #[serde(default)]
    pub weights: Option<Vec<f32>>,
}

/// Search parameters; for dense vectors they become `AnnParams`
/// (`scoring::ann_params`), for sparse ones only `idf` counts.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SearchParams {
    #[serde(default)]
    pub hnsw_ef: Option<u32>,
    #[serde(default)]
    pub exact: bool,
    #[serde(default)]
    pub quantization: Option<QuantizationSearchParams>,
    #[serde(default)]
    pub indexed_only: bool,
    #[serde(default)]
    pub acorn: Option<Value>,
    #[serde(default)]
    pub idf: Option<IdfParams>,
}

/// Which points sparse IDF statistics count: `"global"` or a filter.
#[derive(Clone, Debug, PartialEq)]
pub enum IdfParams {
    Scope(IdfScope),
    /// Boxed: a `Filter` is about 1.4 KB (clippy `large_enum_variant`).
    Corpus {
        corpus: Box<Filter>,
    },
}

/// `"global"`: every point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdfScope {
    Global,
}

#[derive(Deserialize)]
struct RawCorpus {
    corpus: Filter,
}

impl<'de> Deserialize<'de> for IdfParams {
    /// A string is the scope; an object holds `corpus`.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        if v.is_string() {
            return from_value(v).map(IdfParams::Scope);
        }
        from_value::<RawCorpus, _>(v).map(|raw| IdfParams::Corpus {
            corpus: Box::new(raw.corpus),
        })
    }
}

/// Quantization search parameters; only `oversampling` reaches the IR.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct QuantizationSearchParams {
    #[serde(default)]
    pub ignore: bool,
    #[serde(default)]
    pub rescore: Option<bool>,
    #[serde(default)]
    pub oversampling: Option<f64>,
}

/// Where the ids of example vectors are looked up.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct LookupLocation {
    pub collection: String,
    #[serde(default)]
    pub vector: Option<String>,
    #[serde(default)]
    pub shard_key: Option<Value>,
}

/// `POST /collections/{c}/points/query/batch`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct QueryRequestBatch {
    pub searches: Vec<QueryRequest>,
}

/// `{"points": [...]}`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QueryResponse {
    pub points: Vec<ScoredPoint>,
}
