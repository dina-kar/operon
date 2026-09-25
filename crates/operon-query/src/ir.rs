//! The search IR (overview §6.6) with its exact JSON form (plan M1.2 Task 1).
//!
//! Enums are externally tagged in `snake_case`; fields documented "default"
//! read their default when the key is missing. Every field holding an M1.1
//! type whose derived serde is a wire form (`PrimaryKey`, `Distance`) or that
//! has none (`ConsistencyToken`) goes through the [`crate::json`] adapters.

use std::collections::{BTreeMap, BTreeSet};

use operon_collection::{ConsistencyToken, Distance, PrimaryKey};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use operon_collection::SparseVector;

use crate::hot::HotKind;
use crate::types::Projection;

fn default_limit() -> usize {
    10
}

fn default_rrf_k() -> u32 {
    60
}

fn is_empty_set<T>(set: &BTreeSet<T>) -> bool {
    set.is_empty()
}

fn is_empty_map<K, V>(map: &BTreeMap<K, V>) -> bool {
    map.is_empty()
}

/// One search over one collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    /// A collection name or alias.
    pub collection: String,
    /// Default `Strong` (R11).
    #[serde(default)]
    pub consistency: ReadConsistency,
    #[serde(default)]
    pub retrievers: Vec<Retriever>,
    #[serde(default)]
    pub fusion: Option<Fusion>,
    #[serde(default)]
    pub filter: Option<Query>,
    #[serde(default)]
    pub sort: Vec<SortKey>,
    #[serde(default)]
    pub offset: usize,
    /// Default 10.
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub search_after: Option<Vec<SortValue>>,
    #[serde(default)]
    pub score_threshold: Option<f32>,
    #[serde(default)]
    pub select: Projection,
    /// An ES aggregation request (Tantivy's `Aggregations`).
    #[serde(default)]
    pub aggregations: Option<Value>,
    #[serde(default)]
    pub highlight: Option<Highlight>,
    #[serde(default)]
    pub group_by: Option<GroupBy>,
    #[serde(default)]
    pub track_total_hits: TrackTotalHits,
}

impl SearchRequest {
    /// A request over `collection` with every other field at its default.
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            consistency: ReadConsistency::default(),
            retrievers: Vec::new(),
            fusion: None,
            filter: None,
            sort: Vec::new(),
            offset: 0,
            limit: default_limit(),
            search_after: None,
            score_threshold: None,
            select: Projection::default(),
            aggregations: None,
            highlight: None,
            group_by: None,
            track_total_hits: TrackTotalHits::default(),
        }
    }

    /// The request with every [`SortValue`] and [`FieldValue`] normalized, as
    /// its JSON form reads back.
    pub fn normalized(mut self) -> Self {
        self.retrievers = self
            .retrievers
            .into_iter()
            .map(Retriever::normalized)
            .collect();
        self.filter = self.filter.map(Query::normalized);
        self.search_after = self
            .search_after
            .map(|values| values.into_iter().map(SortValue::normalized).collect());
        self
    }
}

/// How fresh a read must be (overview §6.5, A6).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    /// Every write acknowledged before the read began.
    #[default]
    Strong,
    /// Whatever the tail holds.
    Eventual,
    /// At least the writes of the token.
    AtLeast(#[serde(with = "crate::json::token")] ConsistencyToken),
    /// Exactly a retained manifest plus the token's writes after it.
    Pinned {
        manifest_version: u64,
        #[serde(with = "crate::json::token")]
        token: ConsistencyToken,
    },
}

/// A source of ranked candidates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retriever {
    Vector {
        field: String,
        query: Vec<f32>,
        k: usize,
        #[serde(default)]
        params: AnnParams,
        #[serde(default)]
        filter: Option<Query>,
    },
    Text {
        query: Query,
        k: usize,
    },
    Fused {
        inputs: Vec<Retriever>,
        fusion: Fusion,
        k: usize,
    },
    Rescore {
        input: Box<Retriever>,
        field: String,
        query: Vec<f32>,
        k: usize,
    },
    /// Exact sparse-vector search (overview A29).
    Sparse {
        field: String,
        query: SparseVector,
        k: usize,
        #[serde(default)]
        filter: Option<Query>,
        #[serde(default)]
        params: SparseParams,
    },
}

impl Retriever {
    fn normalized(self) -> Self {
        match self {
            Retriever::Vector {
                field,
                query,
                k,
                params,
                filter,
            } => Retriever::Vector {
                field,
                query,
                k,
                params,
                filter: filter.map(Query::normalized),
            },
            Retriever::Text { query, k } => Retriever::Text {
                query: query.normalized(),
                k,
            },
            Retriever::Fused { inputs, fusion, k } => Retriever::Fused {
                inputs: inputs.into_iter().map(Retriever::normalized).collect(),
                fusion,
                k,
            },
            Retriever::Rescore {
                input,
                field,
                query,
                k,
            } => Retriever::Rescore {
                input: Box::new(input.normalized()),
                field,
                query,
                k,
            },
            Retriever::Sparse {
                field,
                query,
                k,
                filter,
                params,
            } => Retriever::Sparse {
                field,
                query,
                k,
                filter: filter.map(Query::normalized),
                params: SparseParams {
                    idf_corpus: params.idf_corpus.map(Query::normalized),
                },
            },
        }
    }
}

/// Parameters of a sparse retriever.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SparseParams {
    /// The rows IDF statistics count (default: every live row).
    #[serde(default)]
    pub idf_corpus: Option<Query>,
}

/// Parameters of a dense-vector retriever.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AnnParams {
    #[serde(default)]
    pub exact: bool,
    #[serde(default)]
    pub nprobes: Option<u32>,
    #[serde(default)]
    pub refine_factor: Option<u32>,
    #[serde(default)]
    pub ef: Option<u32>,
    #[serde(default)]
    pub oversampling: Option<f32>,
    /// A metric override; only with `exact`.
    #[serde(default, with = "crate::json::schema::distance")]
    pub distance: Option<Distance>,
}

/// How ranked lists are combined (overview §6.6, Rulings 4 and 5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fusion {
    Rrf {
        /// Default 60.
        #[serde(default = "default_rrf_k")]
        k: u32,
    },
    Dbsf,
    WeightedSum {
        weights: Vec<f32>,
    },
}

/// A query over the collection's fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    MatchAll,
    MatchNone,
    Match {
        field: String,
        text: String,
        #[serde(default)]
        operator: BoolOperator,
        #[serde(default)]
        minimum_should_match: Option<String>,
        #[serde(default)]
        fuzziness: Option<Fuzziness>,
        #[serde(default)]
        analyzer: Option<String>,
    },
    MatchPhrase {
        field: String,
        text: String,
        #[serde(default)]
        slop: u32,
    },
    MultiMatch {
        /// `[[name, boost], …]`.
        fields: Vec<(String, f32)>,
        text: String,
        #[serde(default)]
        kind: MultiMatchKind,
        #[serde(default)]
        operator: BoolOperator,
        #[serde(default)]
        tie_breaker: Option<f32>,
    },
    Term {
        field: String,
        value: FieldValue,
    },
    Terms {
        field: String,
        values: Vec<FieldValue>,
    },
    Range {
        field: String,
        #[serde(default)]
        gt: Option<FieldValue>,
        #[serde(default)]
        gte: Option<FieldValue>,
        #[serde(default)]
        lt: Option<FieldValue>,
        #[serde(default)]
        lte: Option<FieldValue>,
    },
    Exists {
        field: String,
    },
    IsNull {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    ValuesCount {
        field: String,
        #[serde(default)]
        gt: Option<u64>,
        #[serde(default)]
        gte: Option<u64>,
        #[serde(default)]
        lt: Option<u64>,
        #[serde(default)]
        lte: Option<u64>,
    },
    Prefix {
        field: String,
        value: String,
    },
    Wildcard {
        field: String,
        pattern: String,
    },
    Fuzzy {
        field: String,
        value: String,
        fuzziness: Fuzziness,
    },
    Ids(#[serde(with = "crate::json::pk::vec")] Vec<PrimaryKey>),
    QueryString {
        query: String,
        #[serde(default)]
        default_fields: Vec<String>,
        #[serde(default)]
        default_operator: BoolOperator,
    },
    Bool {
        #[serde(default)]
        must: Vec<Query>,
        #[serde(default)]
        should: Vec<Query>,
        #[serde(default)]
        must_not: Vec<Query>,
        #[serde(default)]
        filter: Vec<Query>,
        #[serde(default)]
        minimum_should_match: Option<String>,
    },
    Boost {
        query: Box<Query>,
        boost: f32,
    },
    ConstantScore {
        query: Box<Query>,
        score: f32,
    },
}

impl Query {
    /// Every [`FieldValue`] in the query normalized.
    pub fn normalized(self) -> Self {
        let values = |values: Vec<FieldValue>| -> Vec<FieldValue> {
            values.into_iter().map(FieldValue::normalized).collect()
        };
        let queries = |queries: Vec<Query>| -> Vec<Query> {
            queries.into_iter().map(Query::normalized).collect()
        };
        match self {
            Query::Term { field, value } => Query::Term {
                field,
                value: value.normalized(),
            },
            Query::Terms { field, values: v } => Query::Terms {
                field,
                values: values(v),
            },
            Query::Range {
                field,
                gt,
                gte,
                lt,
                lte,
            } => Query::Range {
                field,
                gt: gt.map(FieldValue::normalized),
                gte: gte.map(FieldValue::normalized),
                lt: lt.map(FieldValue::normalized),
                lte: lte.map(FieldValue::normalized),
            },
            Query::Bool {
                must,
                should,
                must_not,
                filter,
                minimum_should_match,
            } => Query::Bool {
                must: queries(must),
                should: queries(should),
                must_not: queries(must_not),
                filter: queries(filter),
                minimum_should_match,
            },
            Query::Boost { query, boost } => Query::Boost {
                query: Box::new(query.normalized()),
                boost,
            },
            Query::ConstantScore { query, score } => Query::ConstantScore {
                query: Box::new(query.normalized()),
                score,
            },
            other => other,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoolOperator {
    #[default]
    Or,
    And,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiMatchKind {
    #[default]
    BestFields,
    MostFields,
    CrossFields,
    Phrase,
    PhrasePrefix,
}

/// Edit distance of a fuzzy match. JSON: `"auto"` or an integer 0–2
/// (serde in [`crate::json::values`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fuzziness {
    Auto,
    Edits(u8),
}

/// A field value in a query or a result. JSON: a string, a bool, an integer
/// (`I64` when it fits, else `U64`), a float, or `{"date": "<RFC 3339>"}`
/// (serde in [`crate::json::values`], Ruling 9).
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    /// µs since the epoch, UTC.
    Date(i64),
}

impl FieldValue {
    /// `U64(n <= i64::MAX)` becomes `I64(n)`, as its JSON reads back.
    pub fn normalized(self) -> Self {
        match self {
            FieldValue::U64(n) => match i64::try_from(n) {
                Ok(n) => FieldValue::I64(n),
                Err(_) => FieldValue::U64(n),
            },
            other => other,
        }
    }
}

/// One key of the effective sort; the PK ascending always breaks ties (R10).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortKey {
    Score {
        #[serde(default = "SortOrder::desc")]
        order: SortOrder,
    },
    Pk {
        #[serde(default = "SortOrder::asc")]
        order: SortOrder,
    },
    Field {
        field: String,
        #[serde(default = "SortOrder::asc")]
        order: SortOrder,
        #[serde(default = "MissingOrder::last")]
        missing: MissingOrder,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn asc() -> Self {
        SortOrder::Asc
    }

    fn desc() -> Self {
        SortOrder::Desc
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingOrder {
    First,
    Last,
}

impl MissingOrder {
    fn last() -> Self {
        MissingOrder::Last
    }
}

/// A sort value of a hit, and of `search_after`. JSON: `null`, a bool, an
/// integer as for [`FieldValue`], a float, a string or `{"uuid": …}`.
#[derive(Clone, Debug, PartialEq)]
pub enum SortValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    Uuid([u8; 16]),
}

impl SortValue {
    /// `U64(n <= i64::MAX)` becomes `I64(n)`, as its JSON reads back.
    pub fn normalized(self) -> Self {
        match self {
            SortValue::U64(n) => match i64::try_from(n) {
                Ok(n) => SortValue::I64(n),
                Err(_) => SortValue::U64(n),
            },
            other => other,
        }
    }

    /// The primary key this PK tie-break value names: `I64(n >= 0)` and
    /// `U64` are `U64`, `Str` is `Str`, `Uuid` is `Uuid`.
    pub fn as_pk(&self) -> Option<PrimaryKey> {
        match self {
            SortValue::I64(n) => u64::try_from(*n).ok().map(PrimaryKey::U64),
            SortValue::U64(n) => Some(PrimaryKey::U64(*n)),
            SortValue::Str(s) => Some(PrimaryKey::Str(s.clone())),
            SortValue::Uuid(bytes) => Some(PrimaryKey::Uuid(*bytes)),
            SortValue::Null | SortValue::Bool(_) | SortValue::F64(_) => None,
        }
    }

    /// The PK tie-break value of `pk`, normalized (the inverse of
    /// [`SortValue::as_pk`]).
    pub fn from_pk(pk: &PrimaryKey) -> Self {
        match pk {
            PrimaryKey::U64(n) => SortValue::U64(*n).normalized(),
            PrimaryKey::Str(s) => SortValue::Str(s.clone()),
            PrimaryKey::Uuid(bytes) => SortValue::Uuid(*bytes),
        }
    }
}

/// Highlighting of text fields (ES `highlight`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Highlight {
    pub fields: Vec<HighlightField>,
}

fn default_pre_tag() -> String {
    "<em>".to_string()
}

fn default_post_tag() -> String {
    "</em>".to_string()
}

fn default_fragment_size() -> usize {
    100
}

fn default_number_of_fragments() -> usize {
    5
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HighlightField {
    pub field: String,
    /// Default `<em>`.
    #[serde(default = "default_pre_tag")]
    pub pre_tag: String,
    /// Default `</em>`.
    #[serde(default = "default_post_tag")]
    pub post_tag: String,
    /// Default 100.
    #[serde(default = "default_fragment_size")]
    pub fragment_size: usize,
    /// Default 5.
    #[serde(default = "default_number_of_fragments")]
    pub number_of_fragments: usize,
}

fn default_group_size() -> usize {
    3
}

/// Groups hits by a field value (Qdrant's `group_by`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GroupBy {
    pub field: String,
    /// Default 3.
    #[serde(default = "default_group_size")]
    pub group_size: usize,
    /// Default 10.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// One group of a grouped search.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HitGroup {
    pub key: FieldValue,
    pub hits: Vec<Hit>,
}

/// Whether and how far the total number of matches is counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackTotalHits {
    #[default]
    None,
    Exact,
    UpTo(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotalHits {
    pub value: u64,
    pub relation: TotalRelation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TotalRelation {
    Eq,
    Gte,
}

/// The answer to a [`SearchRequest`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchResponse {
    pub hits: Vec<Hit>,
    pub total: Option<TotalHits>,
    pub aggregations: Option<Value>,
    pub groups: Option<Vec<HitGroup>>,
    /// The state the read saw (overview §6.5).
    #[serde(with = "crate::json::token")]
    pub read_token: ConsistencyToken,
    /// The hot structures the read used; omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "is_empty_set")]
    pub hot_used: BTreeSet<HotKind>,
}

/// One hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    #[serde(with = "crate::json::pk")]
    pub pk: PrimaryKey,
    pub score: f32,
    pub sort_values: Vec<SortValue>,
    pub source: Option<Map<String, Value>>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "is_empty_map")]
    pub sparse_vectors: BTreeMap<String, SparseVector>,
    pub highlight: BTreeMap<String, Vec<String>>,
    /// Omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "is_empty_map")]
    pub fields: BTreeMap<String, Vec<FieldValue>>,
}
