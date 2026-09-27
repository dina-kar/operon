//! The universal query (Task 7): a Qdrant `QueryRequest` compiled to one
//! IR `SearchRequest` (nearest, sparse nearest, prefetch, fusion, rescore),
//! then post-processed into Qdrant's scores, thresholds and pages.
//!
//! Example vectors given by id are read first ([`resolve_examples`]); the
//! compiler ([`compile_query`]) is pure, so the crate tests pin its IR.

use std::collections::{BTreeMap, BTreeSet};

use operon_collection::{CollectionSchema, Distance, PrimaryKey, SparseModifier, SparseVector};
use operon_query::{
    CollectionInfo, Fusion, Hit, Projection, Query, Retriever, SearchRequest, SourceFilter,
    SparseParams,
};
use serde_json::Value;

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::{and, compile_filter, exclude_ids};
use crate::ids::{PointId, pk_to_json};
use crate::model::common::{ScoredPoint, VectorInput, VectorValue};
use crate::model::query::{
    FusionName, IdfParams, LookupLocation, Prefetch, QueryInterface, QueryKind, QueryRequest,
    QueryResponse, SearchParams,
};
use crate::reads::{Selectors, projection, render_payload, render_vectors, resolve_selectors};
use crate::scoring::{ann_params, check_sparse, check_vector, passes_threshold, to_qdrant_score};
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
/// queried collection itself (no `lookup_from`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResolvedExamples {
    pub vectors: BTreeMap<(String, String, PrimaryKey), Example>,
    pub exclude: BTreeSet<PrimaryKey>,
}

/// How a query runs. Task 8 adds the gateway-scored plans.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryPlan {
    /// One IR search, then [`PostProcess`].
    Ir {
        request: SearchRequest,
        post: PostProcess,
    },
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
    fn root(req: &'a QueryRequest) -> Self {
        Self {
            prefetch: req.prefetch.as_deref().unwrap_or_default(),
            query: req.query.as_ref(),
            using: req.using.as_deref().unwrap_or(""),
            lookup: req.lookup_from.as_ref(),
        }
    }

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
            if level.lookup.is_none() {
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

fn several_prefetches() -> GatewayError {
    GatewayError::BadRequest("A query is required when there are several prefetches".to_string())
}

fn fusion_needs_prefetch() -> GatewayError {
    GatewayError::BadRequest("Fusion query requires prefetch".to_string())
}

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
    fn is_sparse(&self, name: &str) -> bool {
        self.schema.sparse_vectors.iter().any(|s| s.name == name)
    }

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
                        return Err(GatewayError::Service(operon_query::ServiceError::Internal(
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
        if p.score_threshold.is_some() {
            return Err(GatewayError::Unsupported(
                "prefetch score_threshold".to_string(),
            ));
        }
        let level = Level::of(p);
        let filter = and(ancestors.cloned(), self.filter(p.filter.as_ref())?);
        let k = positive_limit(p.limit)?;
        let Some(q) = level.query else {
            return match level.prefetch {
                [] => Err(GatewayError::Unsupported(
                    "a prefetch without a query".to_string(),
                )),
                [one] => self.prefetch(one, filter.as_ref()),
                _ => Err(several_prefetches()),
            };
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
            return match level.prefetch {
                [] => Ok((Vec::new(), ScoreKind::Filter, Distance::Dot)),
                [p] => self.prefetch(p, None).map(one),
                _ => Err(several_prefetches()),
            };
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
        let name = kind.name();
        Err(GatewayError::Unsupported(match kind {
            QueryKind::Nearest { .. }
            | QueryKind::Recommend { .. }
            | QueryKind::Discover { .. }
            | QueryKind::Context { .. } => {
                if !level.prefetch.is_empty() {
                    format!("prefetch under {name}")
                } else if self.is_sparse(level.using) {
                    format!("sparse {name}")
                } else {
                    // Task 8 scores these in the gateway.
                    format!("{name} queries")
                }
            }
            _ => name.to_string(),
        }))
    }
}

/// Semantics step 2: the IR request and its post-processing. `collection`
/// is the collection's name (example ids are keyed by it).
pub fn compile_query(
    req: &QueryRequest,
    collection: &str,
    schema: &CollectionSchema,
    examples: &ResolvedExamples,
    _config: &QdrantConfig,
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
    let (retrievers, kind, distance) =
        compiler.root(&Level::root(req), req.params.as_ref(), k_root)?;
    let filter = compiler.filter(req.filter.as_ref())?;
    let exclude: Vec<PrimaryKey> = examples.exclude.iter().cloned().collect();
    let mut request = SearchRequest::new(collection);
    request.retrievers = retrievers;
    request.filter = exclude_ids(filter, &exclude);
    request.offset = 0;
    request.limit = k_root;
    request.select = projection(&selectors);
    Ok(QueryPlan::Ir {
        request,
        post: PostProcess {
            distance,
            kind,
            threshold: req.score_threshold,
            offset,
            limit,
            selectors,
            exclude: examples.exclude.clone(),
        },
    })
}

impl PostProcess {
    /// Semantics step 3.2–3.7: drop the example ids, convert the scores,
    /// keep the hits that pass the threshold, skip `offset`, take `limit`,
    /// render. The IR's order is already Qdrant's (larger-is-better IR
    /// scores are ascending distances).
    pub fn apply(&self, hits: Vec<Hit>) -> Vec<ScoredPoint> {
        let mut out = Vec::with_capacity(self.limit.min(hits.len()));
        let mut skipped = 0;
        for hit in hits {
            if out.len() == self.limit {
                break;
            }
            if self.exclude.contains(&hit.pk) {
                continue;
            }
            let score = to_qdrant_score(self.distance, &self.kind, hit.score);
            if let Some(t) = self.threshold
                && !passes_threshold(self.distance, &self.kind, score, t)
            {
                continue;
            }
            if skipped < self.offset {
                skipped += 1;
                continue;
            }
            out.push(ScoredPoint {
                id: pk_to_json(&hit.pk),
                version: 0,
                score,
                payload: render_payload(&self.selectors, hit.source),
                vector: render_vectors(&self.selectors, hit.vectors, hit.sparse_vectors),
            });
        }
        out
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
    let QueryPlan::Ir { mut request, post } =
        compile_query(&req, &info.name, &info.schema, &examples, gw.config())?;
    request.consistency = ctx.consistency.clone();
    let response = Box::pin(gw.service().search(&ctx.ns, request)).await?;
    Ok(post.apply(response.hits))
}

/// Semantics step 4: the requests in order, with the same context; the
/// first failure fails the batch.
pub(crate) async fn run_batch(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    requests: Vec<QueryRequest>,
) -> Result<Vec<QueryResponse>, GatewayError> {
    let mut out = Vec::with_capacity(requests.len());
    for req in requests {
        let points = Box::pin(run_query(gw.clone(), ctx.clone(), collection.clone(), req)).await?;
        out.push(QueryResponse { points });
    }
    Ok(out)
}
