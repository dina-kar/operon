//! The universal query (Task 7): a Qdrant `QueryRequest` compiled to one
//! IR `SearchRequest` (nearest, sparse nearest, prefetch, fusion, rescore),
//! then post-processed into Qdrant's scores, thresholds and pages. Task 8
//! adds the queries the gateway scores itself (Ruling 10): `recommend`'s
//! `best_score` and `sum_scores`, `discover`, `context` (candidate searches
//! whose union is rescored) and MMR (one candidate search, then Qdrant's
//! greedy selection); `average_vector` stays one IR search.
//!
//! Example vectors given by id are read first (`resolve_examples`); the
//! compiler ([`compile_query`]) is pure, so the crate tests pin its IR.

use std::collections::{BTreeMap, BTreeSet};

use loams_collection::{
    CollectionSchema, ConsistencyToken, Distance, PrimaryKey, SparseModifier, SparseVector,
};
use loams_query::{
    AnnParams, CollectionInfo, Fusion, Hit, Projection, Query, ReadConsistency, Retriever,
    SearchRequest, SourceFilter, SparseParams,
};
use serde_json::Value;

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::{and, compile_filter, exclude_ids};
use crate::ids::{PointId, pk_to_json};
use crate::model::common::{ScoredPoint, VectorInput, VectorValue};
use crate::model::query::{
    ContextPair, FusionName, IdfParams, LookupLocation, Prefetch, QueryInterface, QueryKind,
    QueryRequest, QueryResponse, RecommendStrategy, SearchParams,
};
use crate::reads::{Selectors, projection, render_payload, render_vectors, resolve_selectors};
use crate::scoring::{
    Scorer, ann_params, average_vector, candidate_k, check_sparse, check_vector, mmr_select,
    passes_threshold, to_qdrant_score,
};
use crate::{QdrantConfig, QdrantGateway};

/// Qdrant's text for `params.idf` on a vector without the IDF modifier.
const IDF_NEEDS_MODIFIER: &str =
    "search param `idf` requires a sparse vector with the `idf` modifier";

/// A stored example vector.
#[derive(Clone, Debug, PartialEq)]
pub enum Example {
    Dense(Vec<f32>),
    Sparse(SparseVector),
}

/// The example vectors a request names by id, by `(collection, vector,
/// key)`, and the ids to leave out of the results: those looked up in the
/// queried collection itself (no `lookup_from`, or one naming it).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResolvedExamples {
    pub vectors: BTreeMap<(String, String, PrimaryKey), Example>,
    pub exclude: BTreeSet<PrimaryKey>,
}

/// How a query runs.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryPlan {
    /// One IR search, then [`PostProcess`].
    Ir {
        request: SearchRequest,
        post: PostProcess,
    },
    /// Task 8 semantics 3: the union of the `legs`' hits, each scored by
    /// `scorer` over its `using` vector, then [`PostProcess`].
    Scored {
        legs: Vec<SearchRequest>,
        scorer: Scorer,
        using: String,
        post: PostProcess,
    },
    /// Task 8 semantics 4: MMR over one nearest search's candidates (their
    /// `using` vectors), keeping each pick's nearest score.
    Mmr {
        candidates: SearchRequest,
        query: Vec<f32>,
        lambda: f32,
        using: String,
        post: PostProcess,
    },
}

impl QueryPlan {
    /// What the gateway does with the plan's hits.
    pub fn post(&self) -> &PostProcess {
        match self {
            QueryPlan::Ir { post, .. }
            | QueryPlan::Scored { post, .. }
            | QueryPlan::Mmr { post, .. } => post,
        }
    }

    /// Every IR search the plan runs (Task 9 adds the group filters).
    pub(crate) fn requests_mut(&mut self) -> Vec<&mut SearchRequest> {
        match self {
            QueryPlan::Ir { request, .. } => vec![request],
            QueryPlan::Scored { legs, .. } => legs.iter_mut().collect(),
            QueryPlan::Mmr { candidates, .. } => vec![candidates],
        }
    }
}

/// What the gateway does with the IR's hits (semantics step 3).
#[derive(Clone, Debug, PartialEq)]
pub struct PostProcess {
    pub distance: Distance,
    pub kind: ScoreKind,
    pub threshold: Option<f32>,
    pub offset: usize,
    pub limit: usize,
    pub selectors: Selectors,
    pub exclude: BTreeSet<PrimaryKey>,
}

/// What a result's score is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScoreKind {
    /// A vector score: converted per distance (Ruling 7).
    Distance,
    /// A fused score (RRF, DBSF).
    Fusion,
    /// A recommend, discover or context score (Task 8).
    Custom,
    /// No query: every score is `0.0`.
    Filter,
}

/// The fields a query level shares with a prefetch.
struct Level<'a> {
    prefetch: &'a [Prefetch],
    query: Option<&'a QueryInterface>,
    using: &'a str,
    lookup: Option<&'a LookupLocation>,
}

impl<'a> Level<'a> {
    /// The request's root level.
    fn root(req: &'a QueryRequest) -> Self {
        Self {
            prefetch: req.prefetch.as_deref().unwrap_or_default(),
            query: req.query.as_ref(),
            using: req.using.as_deref().unwrap_or(""),
            lookup: req.lookup_from.as_ref(),
        }
    }

    /// A prefetch's level.
    fn of(p: &'a Prefetch) -> Self {
        Self {
            prefetch: p.prefetch.as_deref().unwrap_or_default(),
            query: p.query.as_ref(),
            using: p.using.as_deref().unwrap_or(""),
            lookup: p.lookup_from.as_ref(),
        }
    }

    /// Where this level's example ids are looked up: `(collection,
    /// vector)`.
    fn location(&self, collection: &str) -> (String, String) {
        match self.lookup {
            Some(l) => (
                l.collection.clone(),
                l.vector.clone().unwrap_or_else(|| self.using.to_string()),
            ),
            None => (collection.to_string(), self.using.to_string()),
        }
    }
}

/// Every vector input of a query.
fn inputs(q: &QueryInterface) -> Vec<&VectorInput> {
    match q {
        QueryInterface::Vector(v) => vec![v],
        QueryInterface::Query(kind) => match kind {
            QueryKind::Nearest { nearest, .. } => vec![nearest],
            QueryKind::Recommend { recommend } => recommend
                .positive
                .iter()
                .chain(&recommend.negative)
                .collect(),
            QueryKind::Discover { discover } => std::iter::once(&discover.target)
                .chain(
                    discover
                        .context
                        .iter()
                        .flat_map(|c| c.as_slice())
                        .flat_map(|p| [&p.positive, &p.negative]),
                )
                .collect(),
            QueryKind::Context { context } => context
                .as_slice()
                .iter()
                .flat_map(|p| [&p.positive, &p.negative])
                .collect(),
            _ => Vec::new(),
        },
    }
}

type Groups = BTreeMap<(String, String), Vec<PrimaryKey>>;

/// Semantics step 1: the ids of `level` and its prefetches, grouped by
/// location.
fn gather(
    collection: &str,
    level: &Level<'_>,
    groups: &mut Groups,
    exclude: &mut BTreeSet<PrimaryKey>,
) -> Result<(), GatewayError> {
    if level.lookup.is_some_and(|l| l.shard_key.is_some()) {
        return Err(GatewayError::Unsupported("shard_key".to_string()));
    }
    for input in level.query.map(inputs).unwrap_or_default() {
        // An object is an inference object, refused when compiled.
        if let VectorInput::Id(v) = input
            && !v.is_object()
        {
            let pk = PointId::from_json(v)?.to_pk();
            let list = groups.entry(level.location(collection)).or_default();
            if !list.contains(&pk) {
                list.push(pk.clone());
            }
            // Qdrant excludes the id unless `lookup_from` names another
            // collection (row T8-4).
            if level.lookup.is_none_or(|l| l.collection == collection) {
                exclude.insert(pk);
            }
        }
    }
    for p in level.prefetch {
        gather(collection, &Level::of(p), groups, exclude)?;
    }
    Ok(())
}

/// A key as Qdrant shows it in messages.
fn shown(pk: &PrimaryKey) -> String {
    match pk_to_json(pk) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Qdrant's text for an unknown vector name.
fn not_existing(name: &str) -> GatewayError {
    GatewayError::BadRequest(format!("Not existing vector name error: {name}"))
}

/// Semantics step 1: one `get` per location, reading only the vector. A
/// missing id is `PointsNotFound`; a found point without the vector is
/// `Vector <name> is not found for point <id>`.
pub(crate) async fn resolve_examples(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    req: &QueryRequest,
) -> Result<ResolvedExamples, GatewayError> {
    let mut groups = Groups::new();
    let mut out = ResolvedExamples::default();
    gather(&info.name, &Level::root(req), &mut groups, &mut out.exclude)?;
    for ((collection, name), pks) in groups {
        let schema = if collection == info.name {
            info.schema.clone()
        } else {
            gw.service()
                .get_collection(&ctx.ns, &collection)
                .await?
                .schema
        };
        let known = schema.vectors.iter().any(|s| s.name == name)
            || schema.sparse_vectors.iter().any(|s| s.name == name);
        if !known {
            return Err(not_existing(&name));
        }
        let select = Projection {
            source: SourceFilter::None,
            vectors: vec![name.clone()],
            fields: Vec::new(),
        };
        let docs = gw
            .service()
            .get(&ctx.ns, &collection, &pks, &select, ctx.consistency.clone())
            .await?;
        for (pk, doc) in pks.into_iter().zip(docs) {
            let Some(mut doc) = doc else {
                return Err(GatewayError::PointsNotFound(shown(&pk)));
            };
            let example = if let Some(v) = doc.vectors.remove(&name) {
                Example::Dense(v)
            } else if let Some(v) = doc.sparse_vectors.remove(&name) {
                Example::Sparse(v)
            } else {
                return Err(GatewayError::BadRequest(format!(
                    "Vector {name} is not found for point {}",
                    shown(&pk)
                )));
            };
            out.vectors
                .insert((collection.clone(), name.clone(), pk), example);
        }
    }
    Ok(out)
}

/// A `limit` (default 10), at least 1.
fn positive_limit(limit: Option<usize>) -> Result<usize, GatewayError> {
    match limit.unwrap_or(10) {
        0 => Err(GatewayError::BadRequest(
            "limit must be at least 1".to_string(),
        )),
        n => Ok(n),
    }
}

/// The input of a nearest query: a bare vector, or `nearest` without
/// `mmr`.
fn nearest_of(q: &QueryInterface) -> Option<&VectorInput> {
    match q {
        QueryInterface::Vector(v) => Some(v),
        QueryInterface::Query(QueryKind::Nearest { nearest, mmr: None }) => Some(nearest),
        QueryInterface::Query(_) => None,
    }
}

/// The IR fusion of a `fusion` or `rrf` query (Ruling 9): Qdrant's RRF `k`
/// (default 2) is sent as `k - 1`; weights are unsupported.
fn fusion_of(q: &QueryInterface) -> Option<Result<Fusion, GatewayError>> {
    let QueryInterface::Query(kind) = q else {
        return None;
    };
    let qdrant_k = match kind {
        QueryKind::Fusion {
            fusion: FusionName::Dbsf,
        } => return Some(Ok(Fusion::Dbsf)),
        QueryKind::Fusion {
            fusion: FusionName::Rrf,
        } => None,
        QueryKind::Rrf { rrf } => {
            if rrf.weights.as_ref().is_some_and(|w| !w.is_empty()) {
                return Some(Err(GatewayError::Unsupported("weighted RRF".to_string())));
            }
            rrf.k
        }
        _ => return None,
    };
    Some(match qdrant_k.unwrap_or(2) {
        0 => Err(GatewayError::BadRequest("k must be at least 1".to_string())),
        k => Ok(Fusion::Rrf { k: k - 1 }),
    })
}

/// Qdrant's checks of one level, root or prefetch, in its order
/// (`qdrant:lib/collection/src/operations/universal_query/collection_query.rs`
/// `validation`; rows T8-1 to T8-3): prefetches and a `score_threshold`
/// need a query, and a fusion takes no `using`.
fn validate(level: &Level<'_>, threshold: Option<f32>) -> Result<(), GatewayError> {
    let bad = |m: &str| Err(GatewayError::BadRequest(m.to_string()));
    if !level.prefetch.is_empty() && level.query.is_none() {
        return bad(
            "A query is needed to merge the prefetches. Can't have prefetches without defining a query.",
        );
    }
    if threshold.is_some() {
        match level.query {
            None => {
                return bad(
                    "A query is needed to use the score_threshold. Can't have score_threshold without defining a query.",
                );
            }
            Some(QueryInterface::Query(QueryKind::OrderBy { .. })) => {
                return bad("Can't use score_threshold with an order_by query.");
            }
            Some(_) => {}
        }
    }
    if matches!(
        level.query,
        Some(QueryInterface::Query(
            QueryKind::Fusion { .. } | QueryKind::Rrf { .. }
        ))
    ) && !level.using.is_empty()
    {
        return bad("Fusion queries cannot be combined with the 'using' field.");
    }
    Ok(())
}

/// A root query the gateway scores itself (Task 8): `recommend`,
/// `discover`, `context`, or `nearest` with `mmr`.
fn gateway_kind(q: Option<&QueryInterface>) -> Option<&QueryKind> {
    match q {
        Some(QueryInterface::Query(
            kind @ (QueryKind::Recommend { .. }
            | QueryKind::Discover { .. }
            | QueryKind::Context { .. }
            | QueryKind::Nearest { mmr: Some(_), .. }),
        )) => Some(kind),
        _ => None,
    }
}

/// Neither a positive nor, where it may stand alone, a negative example.
fn no_positive() -> GatewayError {
    GatewayError::BadRequest("No positive examples given".to_string())
}

/// A fusion without prefetches.
fn fusion_needs_prefetch() -> GatewayError {
    GatewayError::BadRequest("Fusion query requires prefetch".to_string())
}

/// A retriever's `k`.
fn retriever_k(r: &Retriever) -> usize {
    match r {
        Retriever::Vector { k, .. }
        | Retriever::Text { k, .. }
        | Retriever::Fused { k, .. }
        | Retriever::Rescore { k, .. }
        | Retriever::Sparse { k, .. } => *k,
    }
}

/// A query vector the compiler can use.
enum QueryVector {
    Dense(Vec<f32>),
    Sparse(SparseVector),
}

/// A compiled level: its retriever, what its scores are, and the distance
/// that converts them.
type Compiled = (Retriever, ScoreKind, Distance);

struct Compiler<'a> {
    collection: &'a str,
    schema: &'a CollectionSchema,
    examples: &'a ResolvedExamples,
}

impl Compiler<'_> {
    /// Whether `name` is a sparse vector.
    fn is_sparse(&self, name: &str) -> bool {
        self.schema.sparse_vectors.iter().any(|s| s.name == name)
    }

    /// A request filter compiled against the schema.
    fn filter(
        &self,
        f: Option<&crate::model::filter::Filter>,
    ) -> Result<Option<Query>, GatewayError> {
        f.map(|f| compile_filter(f, self.schema)).transpose()
    }

    /// `v` as a vector; an id is its resolved example vector.
    fn value(&self, level: &Level<'_>, v: &VectorInput) -> Result<QueryVector, GatewayError> {
        Ok(match v.clone().resolve(level.using)? {
            VectorValue::Dense(d) => QueryVector::Dense(d),
            VectorValue::Sparse(s) => QueryVector::Sparse(s),
            VectorValue::Id(id) => {
                let (collection, name) = level.location(self.collection);
                match self.examples.vectors.get(&(collection, name, id.to_pk())) {
                    Some(Example::Dense(d)) => QueryVector::Dense(d.clone()),
                    Some(Example::Sparse(s)) => QueryVector::Sparse(s.clone()),
                    None => {
                        return Err(GatewayError::Service(loams_query::ServiceError::Internal(
                            format!("example {} was not resolved", shown(&id.to_pk())),
                        )));
                    }
                }
            }
        })
    }

    /// A checked dense query on `level.using` (normalized for Cosine).
    fn dense(
        &self,
        level: &Level<'_>,
        v: &VectorInput,
    ) -> Result<(Vec<f32>, Distance), GatewayError> {
        let using = level.using;
        let mut query = match self.value(level, v)? {
            QueryVector::Dense(d) => d,
            QueryVector::Sparse(_) if self.is_sparse(using) => {
                return Err(GatewayError::Unsupported("sparse rescoring".to_string()));
            }
            QueryVector::Sparse(_) => {
                if self.schema.vectors.iter().any(|s| s.name == using) {
                    return Err(GatewayError::BadRequest(format!(
                        "Vector {using} is a dense vector"
                    )));
                }
                return Err(not_existing(using));
            }
        };
        check_vector(self.schema, using, &mut query)?;
        let distance = self
            .schema
            .vectors
            .iter()
            .find(|s| s.name == using)
            .map_or(Distance::Dot, |s| s.distance);
        Ok((query, distance))
    }

    /// A leaf nearest: `Vector` on a dense `using`, `Sparse` on a sparse
    /// one (Ruling 21).
    fn leaf(
        &self,
        level: &Level<'_>,
        v: &VectorInput,
        k: usize,
        params: Option<&SearchParams>,
        filter: Option<Query>,
    ) -> Result<(Retriever, Distance), GatewayError> {
        let using = level.using;
        let idf = params.and_then(|p| p.idf.as_ref());
        if let Some(spec) = self.schema.sparse_vectors.iter().find(|s| s.name == using) {
            let query = match self.value(level, v)? {
                QueryVector::Sparse(s) => check_sparse(
                    self.schema,
                    using,
                    s.indices().to_vec(),
                    s.values().to_vec(),
                )?,
                QueryVector::Dense(_) => {
                    return Err(GatewayError::BadRequest(format!(
                        "Vector {using} is a sparse vector"
                    )));
                }
            };
            let idf_corpus = match idf {
                None => None,
                Some(_) if spec.modifier != SparseModifier::Idf => {
                    return Err(GatewayError::BadRequest(IDF_NEEDS_MODIFIER.to_string()));
                }
                Some(IdfParams::Scope(_)) => None,
                Some(IdfParams::Corpus { corpus }) => Some(compile_filter(corpus, self.schema)?),
            };
            let retriever = Retriever::Sparse {
                field: using.to_string(),
                query,
                k,
                filter,
                params: SparseParams { idf_corpus },
            };
            return Ok((retriever, Distance::Dot));
        }
        if !self.schema.vectors.iter().any(|s| s.name == using) {
            return Err(not_existing(using));
        }
        if idf.is_some() {
            return Err(GatewayError::BadRequest(IDF_NEEDS_MODIFIER.to_string()));
        }
        let (query, distance) = self.dense(level, v)?;
        let retriever = Retriever::Vector {
            field: using.to_string(),
            query,
            k,
            params: ann_params(params),
            filter,
        };
        Ok((retriever, distance))
    }

    /// A nearest over prefetches: the union of `children` rescored exactly
    /// against `level.using` (dense only).
    fn rescore(
        &self,
        level: &Level<'_>,
        v: &VectorInput,
        k: usize,
        children: &[Prefetch],
        ancestors: Option<&Query>,
    ) -> Result<Compiled, GatewayError> {
        if self.is_sparse(level.using) {
            return Err(GatewayError::Unsupported("sparse rescoring".to_string()));
        }
        let (query, distance) = self.dense(level, v)?;
        let mut inputs = self.children(children, ancestors)?;
        let input = if inputs.len() == 1 {
            inputs.remove(0)
        } else {
            // The fusion order does not matter: k keeps every candidate.
            let k = inputs.iter().map(retriever_k).sum();
            Retriever::Fused {
                inputs,
                fusion: Fusion::Rrf { k: 1 },
                k,
            }
        };
        let retriever = Retriever::Rescore {
            input: Box::new(input),
            field: level.using.to_string(),
            query,
            k,
        };
        Ok((retriever, ScoreKind::Distance, distance))
    }

    /// The compiled children of a prefetch or the root.
    fn children(
        &self,
        children: &[Prefetch],
        ancestors: Option<&Query>,
    ) -> Result<Vec<Retriever>, GatewayError> {
        children
            .iter()
            .map(|c| self.prefetch(c, ancestors).map(|(r, _, _)| r))
            .collect()
    }

    /// A fusion over `children`, cut at `k`.
    fn fused(
        &self,
        fusion: Fusion,
        k: usize,
        children: &[Prefetch],
        ancestors: Option<&Query>,
    ) -> Result<Compiled, GatewayError> {
        if children.is_empty() {
            return Err(fusion_needs_prefetch());
        }
        let inputs = self.children(children, ancestors)?;
        Ok((
            Retriever::Fused { inputs, fusion, k },
            ScoreKind::Fusion,
            Distance::Dot,
        ))
    }

    /// Prefetch compile: the filter of every ancestor prefetch is ANDed
    /// into the leaves (the root's is the request's filter).
    fn prefetch(&self, p: &Prefetch, ancestors: Option<&Query>) -> Result<Compiled, GatewayError> {
        let level = Level::of(p);
        validate(&level, p.score_threshold)?;
        if p.score_threshold.is_some() {
            return Err(GatewayError::Unsupported(
                "prefetch score_threshold".to_string(),
            ));
        }
        let filter = and(ancestors.cloned(), self.filter(p.filter.as_ref())?);
        let k = positive_limit(p.limit)?;
        let Some(q) = level.query else {
            // A leaf (validated): Qdrant scrolls `limit` points in id order,
            // which no IR retriever expresses (row T8-1).
            return Err(GatewayError::Unsupported(
                "a prefetch without a query".to_string(),
            ));
        };
        if let Some(v) = nearest_of(q) {
            if level.prefetch.is_empty() {
                let (r, distance) = self.leaf(&level, v, k, p.params.as_ref(), filter)?;
                return Ok((r, ScoreKind::Distance, distance));
            }
            return self.rescore(&level, v, k, level.prefetch, filter.as_ref());
        }
        if let Some(fusion) = fusion_of(q) {
            return self.fused(fusion?, k, level.prefetch, filter.as_ref());
        }
        let QueryInterface::Query(kind) = q else {
            unreachable!("a bare vector is a nearest query")
        };
        Err(GatewayError::Unsupported(match kind {
            QueryKind::Nearest { .. } => "mmr inside a prefetch".to_string(),
            QueryKind::Recommend { .. }
            | QueryKind::Discover { .. }
            | QueryKind::Context { .. } => {
                format!("{} inside a prefetch", kind.name())
            }
            other => other.name().to_string(),
        }))
    }

    /// Semantics step 2 at the root: the retrievers, their score kind and
    /// distance.
    fn root(
        &self,
        level: &Level<'_>,
        params: Option<&SearchParams>,
        k_root: usize,
    ) -> Result<(Vec<Retriever>, ScoreKind, Distance), GatewayError> {
        let one = |(r, kind, distance): Compiled| (vec![r], kind, distance);
        let Some(q) = level.query else {
            // No prefetch (validated): filter order, score 0.0.
            return Ok((Vec::new(), ScoreKind::Filter, Distance::Dot));
        };
        if let Some(v) = nearest_of(q) {
            if level.prefetch.is_empty() {
                let (r, distance) = self.leaf(level, v, k_root, params, None)?;
                return Ok((vec![r], ScoreKind::Distance, distance));
            }
            return self
                .rescore(level, v, k_root, level.prefetch, None)
                .map(one);
        }
        if let Some(fusion) = fusion_of(q) {
            // E3: one `Fused` over the prefetches, cut at offset + limit.
            return self.fused(fusion?, k_root, level.prefetch, None).map(one);
        }
        let QueryInterface::Query(kind) = q else {
            unreachable!("a bare vector is a nearest query")
        };
        // `compile_query` plans the gateway-scored kinds (Task 8).
        Err(GatewayError::Unsupported(kind.name().to_string()))
    }

    /// Task 8 semantics 1, 2 and 4: a root `recommend`, `discover`,
    /// `context` or `nearest` with `mmr`, over the dense `level.using`.
    /// Candidate searches read `candidate_k` points (MMR: its
    /// `candidates_limit`, default `limit`), under the request's `filter`
    /// (with the example ids excluded), fetching the `using` vector.
    fn gateway(
        &self,
        level: &Level<'_>,
        kind: &QueryKind,
        params: Option<&SearchParams>,
        filter: Option<Query>,
        mut post: PostProcess,
        config: &QdrantConfig,
    ) -> Result<QueryPlan, GatewayError> {
        let name = kind.name();
        if !level.prefetch.is_empty() {
            return Err(GatewayError::Unsupported(format!("prefetch under {name}")));
        }
        let using = level.using;
        if self.is_sparse(using) {
            return Err(GatewayError::Unsupported(format!("sparse {name}")));
        }
        let Some(spec) = self.schema.vectors.iter().find(|s| s.name == using) else {
            return Err(not_existing(using));
        };
        post.distance = spec.distance;
        let dense = |v: &VectorInput| self.dense(level, v).map(|(v, _)| v);
        let dense_all = |vs: &[VectorInput]| vs.iter().map(dense).collect::<Result<Vec<_>, _>>();
        let pairs = |ps: &[ContextPair]| {
            ps.iter()
                .map(|p| Ok((dense(&p.positive)?, dense(&p.negative)?)))
                .collect::<Result<Vec<_>, GatewayError>>()
        };
        let mut select = projection(&post.selectors);
        if !select.vectors.iter().any(|v| v == using) {
            select.vectors.push(using.to_string());
        }
        let search_with = |query: Vec<f32>, k: usize, params: AnnParams| {
            let mut request = SearchRequest::new(self.collection);
            request.retrievers = vec![Retriever::Vector {
                field: using.to_string(),
                query,
                k,
                params,
                filter: None,
            }];
            request.filter = filter.clone();
            request.offset = 0;
            request.limit = k;
            request.select = select.clone();
            request
        };
        let search = |query: Vec<f32>, k: usize| search_with(query, k, ann_params(params));
        let k = candidate_k(post.offset, post.limit, config.max_candidates);
        let scored = |legs: Vec<Vec<f32>>, scorer: Scorer, mut post: PostProcess| {
            post.kind = ScoreKind::Custom;
            QueryPlan::Scored {
                legs: legs.into_iter().map(|q| search(q, k)).collect(),
                scorer,
                using: using.to_string(),
                post,
            }
        };
        // Without positives the best scores belong to the points farthest
        // from the negatives, which no negative's neighbourhood holds: the
        // legs search away from the negatives instead (review of #57).
        let away = |neg: &[Vec<f32>], scorer: Scorer, mut post: PostProcess| {
            post.kind = ScoreKind::Custom;
            QueryPlan::Scored {
                legs: away_from(post.distance, neg, ann_params(params))
                    .into_iter()
                    .map(|(q, p)| search_with(q, k, p))
                    .collect(),
                scorer,
                using: using.to_string(),
                post,
            }
        };
        match kind {
            QueryKind::Recommend { recommend } => {
                let pos = dense_all(&recommend.positive)?;
                let neg = dense_all(&recommend.negative)?;
                match recommend
                    .strategy
                    .unwrap_or(RecommendStrategy::AverageVector)
                {
                    RecommendStrategy::AverageVector => {
                        let mut avg = average_vector(&pos, &neg)?;
                        check_vector(self.schema, using, &mut avg)?;
                        let mut request = search(avg, post.offset.saturating_add(post.limit));
                        request.select = projection(&post.selectors);
                        post.kind = ScoreKind::Distance;
                        Ok(QueryPlan::Ir { request, post })
                    }
                    RecommendStrategy::BestScore => {
                        if pos.is_empty() && neg.is_empty() {
                            return Err(no_positive());
                        }
                        if pos.is_empty() {
                            let legs = neg.clone();
                            return Ok(away(&legs, Scorer::BestScore { pos, neg }, post));
                        }
                        let legs = pos.clone();
                        Ok(scored(legs, Scorer::BestScore { pos, neg }, post))
                    }
                    RecommendStrategy::SumScores => {
                        if pos.is_empty() && neg.is_empty() {
                            return Err(no_positive());
                        }
                        // Negatives alone are accepted, as in Qdrant (owner
                        // ruling on row T8-7), searched away from them.
                        if pos.is_empty() {
                            let legs = neg.clone();
                            return Ok(away(&legs, Scorer::SumScores { pos, neg }, post));
                        }
                        let legs = pos.clone();
                        Ok(scored(legs, Scorer::SumScores { pos, neg }, post))
                    }
                }
            }
            QueryKind::Discover { discover } => {
                let target = dense(&discover.target)?;
                let pairs = pairs(discover.context.as_ref().map_or(&[], |c| c.as_slice()))?;
                let legs = std::iter::once(target.clone())
                    .chain(pairs.iter().map(|(p, _)| p.clone()))
                    .collect();
                Ok(scored(legs, Scorer::Discover { target, pairs }, post))
            }
            QueryKind::Context { context } => {
                let pairs = pairs(context.as_slice())?;
                if pairs.is_empty() {
                    // Qdrant accepts an empty context and scores every point
                    // 0 (owner ruling on row T8-7): one filter-only leg of
                    // `candidate_k` points.
                    let mut leg = search(Vec::new(), k);
                    leg.retrievers.clear();
                    post.kind = ScoreKind::Custom;
                    return Ok(QueryPlan::Scored {
                        legs: vec![leg],
                        scorer: Scorer::Context { pairs },
                        using: using.to_string(),
                        post,
                    });
                }
                let legs = pairs.iter().map(|(p, _)| p.clone()).collect();
                Ok(scored(legs, Scorer::Context { pairs }, post))
            }
            QueryKind::Nearest {
                nearest,
                mmr: Some(mmr),
            } => {
                let diversity = mmr.diversity.unwrap_or(0.5);
                if !(0.0..=1.0).contains(&diversity) {
                    return Err(GatewayError::BadRequest(format!(
                        "mmr.diversity must be in the range [0, 1], got {diversity}"
                    )));
                }
                let query = dense(nearest)?;
                let k = mmr
                    .candidates_limit
                    .unwrap_or(post.limit)
                    .min(config.max_candidates);
                post.kind = ScoreKind::Distance;
                Ok(QueryPlan::Mmr {
                    candidates: search(query.clone(), k),
                    query,
                    lambda: 1.0 - diversity,
                    using: using.to_string(),
                    post,
                })
            }
            other => Err(GatewayError::Unsupported(other.name().to_string())),
        }
    }
}

/// Semantics step 2: the IR request (or, Task 8, the gateway-scored plan)
/// and its post-processing. `collection` is the collection's name (example
/// ids are keyed by it).
pub fn compile_query(
    req: &QueryRequest,
    collection: &str,
    schema: &CollectionSchema,
    examples: &ResolvedExamples,
    config: &QdrantConfig,
) -> Result<QueryPlan, GatewayError> {
    if req.shard_key.is_some() {
        return Err(GatewayError::Unsupported("shard_key".to_string()));
    }
    let limit = positive_limit(req.limit)?;
    let offset = req.offset.unwrap_or(0);
    let k_root = offset.saturating_add(limit);
    let selectors = resolve_selectors(
        schema,
        req.with_payload.as_ref(),
        false,
        req.with_vector.as_ref(),
    )?;
    let compiler = Compiler {
        collection,
        schema,
        examples,
    };
    let level = Level::root(req);
    validate(&level, req.score_threshold)?;
    let mut post = PostProcess {
        distance: Distance::Dot,
        kind: ScoreKind::Filter,
        threshold: req.score_threshold,
        offset,
        limit,
        selectors,
        exclude: examples.exclude.clone(),
    };
    if let Some(kind) = gateway_kind(level.query) {
        let exclude: Vec<PrimaryKey> = examples.exclude.iter().cloned().collect();
        let filter = exclude_ids(compiler.filter(req.filter.as_ref())?, &exclude);
        return compiler.gateway(&level, kind, req.params.as_ref(), filter, post, config);
    }
    let (retrievers, kind, distance) = compiler.root(&level, req.params.as_ref(), k_root)?;
    let filter = compiler.filter(req.filter.as_ref())?;
    let exclude: Vec<PrimaryKey> = examples.exclude.iter().cloned().collect();
    let mut request = SearchRequest::new(collection);
    request.retrievers = retrievers;
    request.filter = exclude_ids(filter, &exclude);
    request.offset = 0;
    request.limit = k_root;
    request.select = projection(&post.selectors);
    post.kind = kind;
    post.distance = distance;
    Ok(QueryPlan::Ir { request, post })
}

impl PostProcess {
    /// Semantics steps 3.2, 3.3 and 3.5 for one hit: `None` for an example
    /// id or a score that fails the threshold, else its Qdrant score.
    pub fn keep(&self, hit: &Hit) -> Option<f32> {
        if self.exclude.contains(&hit.pk) {
            return None;
        }
        let score = to_qdrant_score(self.distance, &self.kind, hit.score);
        match self.threshold {
            Some(t) if !passes_threshold(self.distance, &self.kind, score, t) => None,
            _ => Some(score),
        }
    }

    /// Semantics steps 3.2–3.6: the kept hits with their Qdrant scores,
    /// `offset` skipped and at most `limit`. The input order is Qdrant's
    /// (larger-is-better IR scores are ascending distances).
    pub fn select(&self, hits: Vec<Hit>) -> Vec<(Hit, f32)> {
        hits.into_iter()
            .filter_map(|hit| self.keep(&hit).map(|score| (hit, score)))
            .skip(self.offset)
            .take(self.limit)
            .collect()
    }

    /// Semantics step 3.7: a hit rendered with the request's selectors.
    pub fn render(&self, hit: Hit, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: pk_to_json(&hit.pk),
            version: 0,
            score,
            payload: render_payload(&self.selectors, hit.source),
            vector: render_vectors(&self.selectors, hit.vectors, hit.sparse_vectors),
        }
    }

    /// [`PostProcess::select`], then [`PostProcess::render`].
    pub fn apply(&self, hits: Vec<Hit>) -> Vec<ScoredPoint> {
        self.select(hits)
            .into_iter()
            .map(|(hit, score)| self.render(hit, score))
            .collect()
    }
}

/// Runs a compiled plan at `consistency` (later searches of a plan at
/// `AtLeast` the first one's read token): the kept hits with their Qdrant
/// scores, unrendered, and the first search's read token (`None` when the
/// plan searched nothing).
pub(crate) async fn execute(
    gw: &QdrantGateway,
    ns: &str,
    consistency: ReadConsistency,
    plan: QueryPlan,
) -> Result<(Vec<(Hit, f32)>, Option<ConsistencyToken>), GatewayError> {
    match plan {
        QueryPlan::Ir { mut request, post } => {
            request.consistency = consistency;
            let response = Box::pin(gw.service().search(ns, request)).await?;
            Ok((post.select(response.hits), Some(response.read_token)))
        }
        QueryPlan::Scored {
            legs,
            scorer,
            using,
            post,
        } => {
            // Semantics 3.1–3.2: the legs in order, the first hit of each
            // key kept.
            let mut consistency = consistency;
            let mut token = None;
            let mut union: BTreeMap<PrimaryKey, Hit> = BTreeMap::new();
            for mut leg in legs {
                leg.consistency = consistency.clone();
                let response = Box::pin(gw.service().search(ns, leg)).await?;
                if token.is_none() {
                    consistency = ReadConsistency::AtLeast(response.read_token.clone());
                    token = Some(response.read_token);
                }
                for hit in response.hits {
                    union.entry(hit.pk.clone()).or_insert(hit);
                }
            }
            // Semantics 3.3–3.6.
            let mut hits: Vec<Hit> = union
                .into_values()
                .filter_map(|mut hit| {
                    hit.score = scorer.score(post.distance, hit.vectors.get(&using)?);
                    Some(hit)
                })
                .collect();
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.pk.cmp(&b.pk)));
            Ok((post.select(hits), token))
        }
        QueryPlan::Mmr {
            mut candidates,
            query,
            lambda,
            using,
            post,
        } => {
            // `candidates_limit: 0` gathers nothing, as in Qdrant.
            if candidates.limit == 0 {
                return Ok((Vec::new(), None));
            }
            candidates.consistency = consistency;
            let response = Box::pin(gw.service().search(ns, candidates)).await?;
            // Semantics 4.2: the example ids dropped, the threshold applied
            // to the nearest scores.
            let kept: Vec<(Hit, f32)> = response
                .hits
                .into_iter()
                .filter(|hit| hit.vectors.contains_key(&using))
                .filter_map(|hit| post.keep(&hit).map(|score| (hit, score)))
                .collect();
            let vectors: Vec<(PrimaryKey, Vec<f32>)> = kept
                .iter()
                .map(|(hit, _)| (hit.pk.clone(), hit.vectors[&using].clone()))
                .collect();
            // Semantics 4.3–4.4.
            let picks = mmr_select(
                post.distance,
                &query,
                &vectors,
                lambda,
                post.offset.saturating_add(post.limit),
            );
            let mut slots: Vec<Option<(Hit, f32)>> = kept.into_iter().map(Some).collect();
            let hits = picks
                .into_iter()
                .skip(post.offset)
                .take(post.limit)
                .filter_map(|i| slots[i].take())
                .collect();
            Ok((hits, Some(response.read_token)))
        }
    }
}

/// Runs one query: examples, compile, search, post-process.
pub(crate) async fn run_query(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    req: QueryRequest,
) -> Result<Vec<ScoredPoint>, GatewayError> {
    let info = gw.service().get_collection(&ctx.ns, &collection).await?;
    let examples = resolve_examples(&gw, &ctx, &info, &req).await?;
    let plan = compile_query(&req, &info.name, &info.schema, &examples, gw.config())?;
    let post = plan.post().clone();
    let (hits, _) = Box::pin(execute(&gw, &ctx.ns, ctx.consistency.clone(), plan)).await?;
    Ok(hits
        .into_iter()
        .map(|(hit, score)| post.render(hit, score))
        .collect())
}

/// Semantics step 4: the requests in order, with the same context; the
/// first failure fails the batch.
pub(crate) async fn run_batch(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    requests: Vec<QueryRequest>,
) -> Result<Vec<QueryResponse>, GatewayError> {
    let max = gw.config().max_batch_queries;
    let len = requests.len();
    crate::check_request_len("The query batch", len, max)?;
    let mut out = Vec::new();
    for req in requests {
        let points = Box::pin(run_query(gw.clone(), ctx.clone(), collection.clone(), req)).await?;
        out.push(QueryResponse { points });
    }
    Ok(out)
}

/// The legs of a negatives-only `best_score` or `sum_scores` query. On
/// Cosine and Dot: one per negative, plus one for their sum when there are
/// several; the nearest points to `-v` are exactly the ones least similar
/// to `v`, and the nearest to `-Σ v` minimize `Σ sim(c, v)` (the negated
/// `sum_scores`), so the ANN index finds them. No index answers "farthest"
/// on Euclid or Manhattan: there the query runs one exact scan by dot
/// product with `-Σ v` (or `-v` of the first negative when the sum is
/// zero or overflows f32), which leans away from the negatives but may miss a far point of
/// small norm (candidate-bounded, as Ruling 10 says). One scan per query,
/// whatever the number of negatives, bounds the work as an `exact` search
/// does (review of #60).
fn away_from(
    distance: Distance,
    neg: &[Vec<f32>],
    params: AnnParams,
) -> Vec<(Vec<f32>, AnnParams)> {
    let negate = |v: &[f32]| v.iter().map(|x| -x).collect::<Vec<f32>>();
    let dim = neg.first().map_or(0, Vec::len);
    let sum = (0..dim)
        .map(|i| neg.iter().map(|v| v[i]).sum::<f32>())
        .collect::<Vec<f32>>();
    // Cosine example vectors are normalized, so this is Σ v̂. A sum that
    // overflows f32 is not searched (review of #62).
    let sum = (neg.len() > 1 && sum.iter().all(|x| x.is_finite()) && sum.iter().any(|x| *x != 0.0))
        .then(|| negate(&sum));
    match distance {
        Distance::Cosine | Distance::Dot => neg
            .iter()
            .map(|v| negate(v))
            .chain(sum)
            .map(|q| (q, params.clone()))
            .collect(),
        Distance::Euclid | Distance::Manhattan => {
            let exact = AnnParams {
                exact: true,
                distance: Some(Distance::Dot),
                ..params
            };
            sum.or_else(|| neg.first().map(|v| negate(v)))
                .into_iter()
                .map(|q| (q, exact.clone()))
                .collect()
        }
    }
}
