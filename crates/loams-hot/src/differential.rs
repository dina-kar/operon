//! The hot on/off differential harness (plan M1.3 Task 12; Ruling 17; R12).
//!
//! One collection, two [`CollectionService`]s over one
//! [`CollectionContext`]: one reads through the [`HotTierImpl`], the other
//! has no hot tier. The harness writes a seeded workload, drives link apply,
//! index builds, maintenance and artifact builds itself (phase by phase),
//! and runs every generated query at one pinned read
//! ([`ReadConsistency::Pinned`]) on the cold service, the hot service, and a
//! third time on a per-query choice of service and hot switch. Exact
//! queries must answer identically (R12, scores included); approximate
//! vector queries must return rows that match their filter, exact scores,
//! and a recall@10 of at least `min_recall` on each tier.
//!
//! Only with the `test-util` feature. M1.7 runs it at scale.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use loams_collection::{
    CollectionContext, CollectionManifest, CollectionSchema, ConsistencyToken, Distance, DocOp,
    Document, DynamicMapping, FieldKind, FieldSpec, HnswParams, IndexBuildSource,
    LanceCompactionSource, PatchMode, PrimaryKey, SparseModifier, SparseVector, SparseVectorSpec,
    SplitMergeSource, VectorElement, VectorIndexSpec, VectorSpec, apply_patch, live_manifest,
};
use loams_common::meta::{Consistency, HotConfig, MetaStore};
use loams_common::{CollectionId, NamespaceId};
use loams_link::LinkApplySource;
use loams_query::hot::{self, HotTier, HotUsed, RequestHot};
use loams_query::types::WriteOptions;
use loams_query::{
    AnnParams, CollectionService, FieldValue, Fusion, MultiMatchKind, Projection, Query,
    ReadConsistency, Retriever, SearchRequest, SearchResponse, SortKey, SortOrder,
};
use loams_worker::{RunResult, TaskOutcome, TaskSource, run_once};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value, json};

use crate::{HotBuildSource, HotTierImpl};

/// The name of the harness's dense vector column.
pub const VECTOR: &str = "vec";
/// The Lance column of [`VECTOR`], which names its artifact and views.
const VECTOR_COLUMN: &str = "_vector_0";
/// The name of its sparse vector.
pub const SPARSE: &str = "sp";
/// The phases, in order (rule 2).
pub const PHASES: [&str; 5] = ["loaded", "tail", "applied", "maintained", "rebuilt"];

/// Words in the text vocabulary.
const VOCABULARY: usize = 500;
/// Indices a sparse vector draws from.
const SPARSE_INDICES: usize = 1_000;
/// The Zipf exponent of words and sparse indices.
const ZIPF: f64 = 1.1;
/// Documents per `loaded` batch: `docs / BATCHES`.
const BATCHES: usize = 20;
/// The recall depth of rule 6.
const RECALL_AT: usize = 10;
/// How long one wait for the pipeline may take.
const WAIT: Duration = Duration::from_secs(120);
/// The lease TTL of the harness's task runs.
const TTL: Duration = Duration::from_secs(60);
/// `ts` values start here (2024-01-01T00:00:00Z) and spread over 60 days.
const TS_BASE_MS: i64 = 1_704_067_200_000;
const TS_SPAN_MS: i64 = 60 * 86_400_000;

#[derive(Clone, Debug)]
pub struct DiffConfig {
    pub seed: u64,
    pub docs: usize,
    pub queries_per_phase: usize,
    pub dim: u32,
    pub min_recall: f64,
    /// Relative: a score may differ from the exact one by
    /// `score_tolerance × max(1, |exact|)`.
    pub score_tolerance: f32,
    /// Addition (row 12.8): per-phase query counts that replace
    /// `queries_per_phase` for the phases they name.
    pub phase_queries: BTreeMap<&'static str, usize>,
}

impl DiffConfig {
    /// The defaults for `seed`: 2 000 documents, 200 queries per phase,
    /// dimension 16, recall 0.95, tolerance 1e-5.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            docs: 2_000,
            queries_per_phase: 200,
            dim: 16,
            min_recall: 0.95,
            score_tolerance: 1e-5,
            phase_queries: BTreeMap::new(),
        }
    }

    fn queries_in(&self, phase: &str) -> usize {
        self.phase_queries
            .get(phase)
            .copied()
            .unwrap_or(self.queries_per_phase)
    }
}

impl Default for DiffConfig {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryClass {
    Identical,
    Approximate,
}

#[derive(Clone, Debug)]
pub struct GeneratedQuery {
    pub id: u32,
    pub class: QueryClass,
    pub op: DiffOp,
}

// The plan fixes the variants; queries are built once per run.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum DiffOp {
    /// The request's `collection` and `consistency` are set when it runs.
    Search(SearchRequest),
    Get(Vec<PrimaryKey>, Projection),
    Count(Option<Query>),
    /// A filter and a page size; paged to the end.
    Scroll(Option<Query>, usize),
}

#[derive(Clone, Debug, Default)]
pub struct PhaseReport {
    pub phase: &'static str,
    pub identical: u32,
    pub approximate: u32,
    pub hot_recall: f64,
    pub cold_recall: f64,
    /// Growth of the tier's counters over the hot pass.
    pub ann_served: u64,
    pub split_files_served: u64,
}

#[derive(Clone, Debug)]
pub struct Mismatch {
    pub phase: &'static str,
    pub query: GeneratedQuery,
    pub reason: String,
    pub hot: String,
    pub cold: String,
}

#[derive(Clone, Debug, Default)]
pub struct DiffReport {
    pub seed: u64,
    pub phases: Vec<PhaseReport>,
    pub mismatches: Vec<Mismatch>,
}

impl DiffReport {
    pub fn is_ok(&self) -> bool {
        self.mismatches.is_empty()
    }

    /// The seed, one line per phase, and every mismatch with both bodies
    /// (cut at 2 000 characters each).
    pub fn describe(&self) -> String {
        use fmt::Write;
        let mut out = format!(
            "differential seed {}: {} mismatches\n",
            self.seed,
            self.mismatches.len()
        );
        for p in &self.phases {
            let _ = writeln!(
                out,
                "  {:<10} identical {:>4} approximate {:>4} recall hot {:.3} cold {:.3} \
                 ann_served +{} split_files_served +{}",
                p.phase,
                p.identical,
                p.approximate,
                p.hot_recall,
                p.cold_recall,
                p.ann_served,
                p.split_files_served
            );
        }
        for m in &self.mismatches {
            let _ = writeln!(
                out,
                "- [{}] query {} ({:?}): {}\n  op:   {}\n  hot:  {}\n  cold: {}",
                m.phase,
                m.query.id,
                m.query.class,
                m.reason,
                cut(&format!("{:?}", m.query.op)),
                cut(&m.hot),
                cut(&m.cold)
            );
        }
        out
    }
}

fn cut(s: &str) -> String {
    const MAX: usize = 2_000;
    match s.char_indices().nth(MAX) {
        Some((at, _)) => format!("{}… ({} bytes)", &s[..at], s.len()),
        None => s.to_string(),
    }
}

/// The collection as the harness wrote it (latest-wins fold), for filters
/// and exact top-k.
#[derive(Clone, Debug, Default)]
pub struct Model {
    docs: BTreeMap<PrimaryKey, Document>,
    /// The next key an insert uses.
    next_key: u64,
}

impl Model {
    /// Applies one op as link apply does.
    pub fn apply(&mut self, op: &DocOp) {
        let pk = op.pk().clone();
        if let PrimaryKey::U64(k) = pk {
            self.next_key = self.next_key.max(k + 1);
        }
        match apply_patch(self.docs.get(&pk), op) {
            Some(doc) => {
                self.docs.insert(pk, doc);
            }
            None => {
                self.docs.remove(&pk);
            }
        }
    }

    pub fn get(&self, pk: &PrimaryKey) -> Option<&Document> {
        self.docs.get(pk)
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    fn keys(&self) -> Vec<PrimaryKey> {
        self.docs.keys().cloned().collect()
    }

    /// The exact top `k` scores of the documents with a vector that
    /// `filter` matches (every filter the generator gives approximate
    /// queries is one [`matches`] decides).
    fn exact_top(&self, query: &[f32], filter: Option<&Query>, k: usize) -> Vec<(f32, PrimaryKey)> {
        let mut scored: Vec<(f32, PrimaryKey)> = self
            .docs
            .values()
            .filter(|doc| filter.is_none_or(|f| matches(f, doc) == Some(true)))
            .filter_map(|doc| {
                let v = doc.vectors.get(VECTOR)?;
                Some((
                    loams_hnsw::exact_score(Distance::Cosine, query, v),
                    doc.pk.clone(),
                ))
            })
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        scored.truncate(k);
        scored
    }
}

/// Whether `doc` matches `query`, for the queries the generator builds
/// over `tag`, `tags`, `n`, `ts` and `flag`; `None` for anything else.
pub fn matches(query: &Query, doc: &Document) -> Option<bool> {
    let values = |field: &str| -> Vec<&Value> {
        match doc.source.get(field) {
            Some(Value::Array(items)) => items.iter().collect(),
            Some(Value::Null) | None => Vec::new(),
            Some(value) => vec![value],
        }
    };
    let eq = |value: &Value, want: &FieldValue| match (value, want) {
        (Value::String(s), FieldValue::Str(w)) => s == w,
        (Value::Bool(b), FieldValue::Bool(w)) => b == w,
        (Value::Number(n), FieldValue::I64(w)) => n.as_i64() == Some(*w),
        _ => false,
    };
    // A comparable number: `n` as is, `ts` (epoch ms) in µs, as dates are.
    let number = |field: &str, value: &Value| -> Option<i64> {
        let n = value.as_i64()?;
        Some(if field == "ts" { n * 1_000 } else { n })
    };
    let bound = |value: &FieldValue| -> Option<i64> {
        match value {
            FieldValue::I64(n) => Some(*n),
            FieldValue::Date(us) => Some(*us),
            _ => None,
        }
    };
    Some(match query {
        Query::MatchAll => true,
        Query::Term { field, value } => values(field).iter().any(|v| eq(v, value)),
        Query::Terms {
            field,
            values: wants,
        } => values(field).iter().any(|v| wants.iter().any(|w| eq(v, w))),
        Query::Exists { field } => !values(field).is_empty(),
        Query::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } => {
            let (gt, gte, lt, lte) = (
                gt.as_ref().map(bound),
                gte.as_ref().map(bound),
                lt.as_ref().map(bound),
                lte.as_ref().map(bound),
            );
            if [gt, gte, lt, lte].iter().any(|b| matches!(b, Some(None))) {
                return None;
            }
            values(field).iter().any(|v| {
                number(field, v).is_some_and(|n| {
                    gt.flatten().is_none_or(|b| n > b)
                        && gte.flatten().is_none_or(|b| n >= b)
                        && lt.flatten().is_none_or(|b| n < b)
                        && lte.flatten().is_none_or(|b| n <= b)
                })
            })
        }
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            minimum_should_match: None,
        } if should.is_empty() => {
            for q in must.iter().chain(filter) {
                if !matches(q, doc)? {
                    return Some(false);
                }
            }
            for q in must_not {
                if matches(q, doc)? {
                    return Some(false);
                }
            }
            true
        }
        _ => return None,
    })
}

// ----- Seeded generation -----

/// A Zipf distribution over `0..n` (exponent [`ZIPF`]).
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize) -> Self {
        let weights: Vec<f64> = (1..=n).map(|r| 1.0 / (r as f64).powf(ZIPF)).collect();
        let total: f64 = weights.iter().sum();
        let mut acc = 0.0;
        let cdf = weights
            .into_iter()
            .map(|w| {
                acc += w / total;
                acc
            })
            .collect();
        Self { cdf }
    }

    fn sample(&self, rng: &mut ChaCha8Rng) -> usize {
        let u: f64 = rng.random();
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }
}

/// Word `i` of the vocabulary: three consonant-vowel syllables, so no two
/// words share a stem.
fn word(i: usize) -> String {
    const C: &[u8] = b"bdfgklmnprtvz";
    const V: &[u8] = b"aeiou";
    let mut n = i;
    let mut out = String::new();
    for _ in 0..3 {
        out.push(C[n % C.len()] as char);
        n /= C.len();
        out.push(V[n % V.len()] as char);
        n /= V.len();
    }
    out
}

/// A seeded RNG for `(seed, what)`.
fn rng_for(seed: u64, what: &str) -> ChaCha8Rng {
    let salt = xxhash_rust::xxh3::xxh3_64(what.as_bytes());
    ChaCha8Rng::seed_from_u64(seed ^ salt)
}

struct Gen {
    rng: ChaCha8Rng,
    words: Zipf,
    indices: Zipf,
    dim: u32,
}

impl Gen {
    fn new(seed: u64, what: &str, dim: u32) -> Self {
        Self {
            rng: rng_for(seed, what),
            words: Zipf::new(VOCABULARY),
            indices: Zipf::new(SPARSE_INDICES),
            dim,
        }
    }

    fn chance(&mut self, p: f64) -> bool {
        self.rng.random_bool(p)
    }

    fn range(&mut self, range: std::ops::Range<usize>) -> usize {
        self.rng.random_range(range)
    }

    fn words(&mut self, count: usize) -> String {
        (0..count)
            .map(|_| word(self.words.sample(&mut self.rng)))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..self.dim)
            .map(|_| self.rng.random_range(-1.0f32..1.0))
            .collect()
    }

    /// 1–`max` distinct Zipf indices with weights in [0, 1), about one in
    /// ten of them zero.
    fn sparse(&mut self, max: usize) -> SparseVector {
        let count = self.range(1..max + 1);
        let mut indices = BTreeSet::new();
        while indices.len() < count {
            indices.insert(self.indices.sample(&mut self.rng) as u32);
        }
        let values = indices
            .iter()
            .map(|_| match self.chance(0.1) {
                true => 0.0,
                false => self.rng.random_range(0.0f32..1.0),
            })
            .collect();
        SparseVector::new(indices.into_iter().collect(), values).expect("a valid sparse vector")
    }

    fn tag(&mut self) -> String {
        format!("t{}", self.range(0..10))
    }

    fn tags_value(&mut self) -> String {
        format!("g{}", self.range(0..20))
    }

    /// Rule 1's document for key `k`.
    fn document(&mut self, k: u64) -> Document {
        let mut source = Map::new();
        source.insert("title".into(), json!(self.words_n(2, 6)));
        source.insert("txt".into(), json!(self.words_n(8, 40)));
        source.insert("tag".into(), json!(self.tag()));
        let tags: BTreeSet<String> = (0..self.range(0..4)).map(|_| self.tags_value()).collect();
        if !tags.is_empty() {
            source.insert("tags".into(), json!(tags));
        }
        source.insert("n".into(), json!(self.range(0..1_000) as i64));
        source.insert("f".into(), json!(self.rng.random_range(0.0f64..100.0)));
        source.insert(
            "ts".into(),
            json!(TS_BASE_MS + self.rng.random_range(0..TS_SPAN_MS)),
        );
        source.insert("flag".into(), json!(self.chance(0.5)));
        let mut vectors = BTreeMap::new();
        if !self.chance(0.05) {
            vectors.insert(VECTOR.to_string(), self.vector());
        }
        let mut sparse_vectors = BTreeMap::new();
        let roll: f64 = self.rng.random();
        if roll >= 0.06 {
            sparse_vectors.insert(SPARSE.to_string(), self.sparse(20));
        } else if roll >= 0.05 {
            sparse_vectors.insert(
                SPARSE.to_string(),
                SparseVector::new(Vec::new(), Vec::new()).expect("empty"),
            );
        }
        Document {
            pk: PrimaryKey::U64(k),
            source,
            vectors,
            sparse_vectors,
        }
    }

    fn words_n(&mut self, min: usize, max: usize) -> String {
        let n = self.range(min..max + 1);
        self.words(n)
    }

    // ----- Queries (rule 3) -----

    fn one_filter(&mut self) -> Query {
        match self.range(0..6) {
            0 => Query::Term {
                field: "tag".into(),
                value: FieldValue::Str(self.tag()),
            },
            1 => Query::Terms {
                field: "tags".into(),
                values: (0..self.range(1..4))
                    .map(|_| FieldValue::Str(self.tags_value()))
                    .collect(),
            },
            2 => {
                let lo = self.range(0..800) as i64;
                Query::Range {
                    field: "n".into(),
                    gt: None,
                    gte: Some(FieldValue::I64(lo)),
                    lt: Some(FieldValue::I64(lo + self.range(100..400) as i64)),
                    lte: None,
                }
            }
            3 => {
                let lo = TS_BASE_MS + self.rng.random_range(0..TS_SPAN_MS / 2);
                let hi = lo + self.rng.random_range(TS_SPAN_MS / 10..TS_SPAN_MS / 2);
                Query::Range {
                    field: "ts".into(),
                    gt: None,
                    gte: Some(FieldValue::Date(lo * 1_000)),
                    lt: None,
                    lte: Some(FieldValue::Date(hi * 1_000)),
                }
            }
            4 => Query::Exists {
                field: "tags".into(),
            },
            _ => Query::Term {
                field: "flag".into(),
                value: FieldValue::Bool(self.chance(0.5)),
            },
        }
    }

    /// One or two filters.
    fn filter(&mut self) -> Query {
        match self.chance(0.5) {
            true => self.one_filter(),
            false => Query::Bool {
                must: Vec::new(),
                should: Vec::new(),
                must_not: Vec::new(),
                filter: vec![self.one_filter(), self.one_filter()],
                minimum_should_match: None,
            },
        }
    }

    /// A filter that keeps an approximate query on the ANN path: at least
    /// about a tenth of the rows.
    fn broad_filter(&mut self) -> Query {
        match self.range(0..3) {
            0 => Query::Term {
                field: "tag".into(),
                value: FieldValue::Str(self.tag()),
            },
            1 => {
                let lo = self.range(0..700) as i64;
                Query::Range {
                    field: "n".into(),
                    gt: None,
                    gte: Some(FieldValue::I64(lo)),
                    lt: Some(FieldValue::I64(lo + 300)),
                    lte: None,
                }
            }
            _ => Query::Term {
                field: "flag".into(),
                value: FieldValue::Bool(self.chance(0.5)),
            },
        }
    }

    fn maybe_filter(&mut self) -> Option<Query> {
        self.chance(0.5).then(|| self.filter())
    }

    fn text_query(&mut self) -> Query {
        match self.range(0..3) {
            0 => {
                let n = self.range(1..4);
                Query::Match {
                    field: if self.chance(0.7) { "txt" } else { "title" }.into(),
                    text: self.words(n),
                    operator: Default::default(),
                    minimum_should_match: None,
                    fuzziness: None,
                    analyzer: None,
                }
            }
            1 => Query::MatchPhrase {
                field: "txt".into(),
                text: self.words(2),
                slop: self.range(0..3) as u32,
            },
            _ => {
                let n = self.range(1..4);
                Query::MultiMatch {
                    fields: vec![("title".into(), 2.0), ("txt".into(), 1.0)],
                    text: self.words(n),
                    kind: MultiMatchKind::BestFields,
                    operator: Default::default(),
                    tie_breaker: None,
                }
            }
        }
    }

    fn page(&mut self, request: &mut SearchRequest) {
        request.limit = self.range(1..51);
        request.offset = self.range(0..21);
    }

    fn depth(request: &SearchRequest) -> usize {
        request.limit + request.offset
    }

    fn exact_vector(&mut self, filter: Option<Query>, k: usize) -> Retriever {
        Retriever::Vector {
            field: VECTOR.into(),
            query: self.vector(),
            k,
            params: AnnParams {
                exact: true,
                ..AnnParams::default()
            },
            filter,
        }
    }

    fn sort(&mut self) -> Vec<SortKey> {
        match self.range(0..3) {
            0 => vec![SortKey::Score {
                order: SortOrder::Desc,
            }],
            1 => vec![SortKey::Field {
                field: "n".into(),
                order: SortOrder::Asc,
                missing: loams_query::MissingOrder::Last,
            }],
            _ => vec![SortKey::Field {
                field: "ts".into(),
                order: SortOrder::Desc,
                missing: loams_query::MissingOrder::Last,
            }],
        }
    }

    fn query(&mut self, id: u32, model: &Model) -> GeneratedQuery {
        let mut request = SearchRequest::new("");
        let roll = self.range(0..100);
        let identical = |op| GeneratedQuery {
            id,
            class: QueryClass::Identical,
            op,
        };
        match roll {
            // Text (30 %).
            0..30 => {
                self.page(&mut request);
                request.retrievers = vec![Retriever::Text {
                    query: self.text_query(),
                    k: Self::depth(&request),
                }];
                identical(DiffOp::Search(request))
            }
            // A filter-only or scored query with a sort (15 %).
            30..45 => {
                self.page(&mut request);
                request.filter = Some(self.filter());
                if self.chance(0.5) {
                    request.retrievers = vec![Retriever::Text {
                        query: self.text_query(),
                        k: Self::depth(&request).max(10),
                    }];
                }
                request.sort = self.sort();
                identical(DiffOp::Search(request))
            }
            // Aggregations (10 %).
            45..55 => {
                request.limit = self.range(0..6);
                request.filter = self.maybe_filter();
                request.aggregations = Some(match self.range(0..3) {
                    0 => json!({"a": {"terms": {"field": "tag"}}}),
                    1 => json!({"a": {"stats": {"field": "n"}}}),
                    _ => json!({"a": {"date_histogram": {"field": "ts", "fixed_interval": "7d"}}}),
                });
                identical(DiffOp::Search(request))
            }
            // Get, count, scroll (10 %).
            55..65 => match self.range(0..3) {
                0 => {
                    let keys = model.keys();
                    let mut pks: Vec<PrimaryKey> = (0..self.range(1..11))
                        .map(|_| match keys.is_empty() || self.chance(0.1) {
                            true => PrimaryKey::U64(1_000_000 + self.range(0..100) as u64),
                            false => keys[self.range(0..keys.len())].clone(),
                        })
                        .collect();
                    pks.dedup();
                    identical(DiffOp::Get(pks, Projection::default()))
                }
                1 => identical(DiffOp::Count(self.maybe_filter())),
                _ => {
                    let page = self.range(50..400);
                    identical(DiffOp::Scroll(self.maybe_filter(), page))
                }
            },
            // Exact vector, with and without a filter (10 %).
            65..75 => {
                self.page(&mut request);
                let filter = self.maybe_filter();
                request.retrievers = vec![self.exact_vector(filter, Self::depth(&request))];
                identical(DiffOp::Search(request))
            }
            // Hybrid text + exact vector (5 %).
            75..80 => {
                self.page(&mut request);
                let k = Self::depth(&request).max(10);
                request.retrievers = vec![
                    Retriever::Text {
                        query: self.text_query(),
                        k,
                    },
                    self.exact_vector(None, k),
                ];
                request.fusion = Some(Fusion::Rrf { k: 60 });
                identical(DiffOp::Search(request))
            }
            // Sparse, alone or fused with an exact vector (5 %).
            80..85 => {
                self.page(&mut request);
                let k = Self::depth(&request).max(10);
                let sparse = Retriever::Sparse {
                    field: SPARSE.into(),
                    query: self.sparse(5),
                    k,
                    filter: self.maybe_filter(),
                    params: Default::default(),
                };
                request.retrievers = vec![sparse];
                if self.chance(0.5) {
                    request.retrievers.push(self.exact_vector(None, k));
                    request.fusion = Some(match self.chance(0.5) {
                        true => Fusion::Rrf { k: 60 },
                        false => Fusion::Dbsf,
                    });
                }
                identical(DiffOp::Search(request))
            }
            // Approximate vector, with and without a filter (15 %).
            _ => {
                request.limit = RECALL_AT;
                let filter = self.chance(0.5).then(|| self.broad_filter());
                request.retrievers = vec![Retriever::Vector {
                    field: VECTOR.into(),
                    query: self.vector(),
                    k: RECALL_AT,
                    params: AnnParams::default(),
                    filter,
                }];
                GeneratedQuery {
                    id,
                    class: QueryClass::Approximate,
                    op: DiffOp::Search(request),
                }
            }
        }
    }
}

/// `count` queries of `phase` for `seed` over `model` (rule 3). The
/// vector dimension is the harness's default (16); see [`generate_with`].
pub fn generate(seed: u64, phase: &str, model: &Model, count: usize) -> Vec<GeneratedQuery> {
    generate_with(seed, phase, model, count, DiffConfig::default().dim)
}

/// [`generate`] with vectors of `dim` dimensions.
pub fn generate_with(
    seed: u64,
    phase: &str,
    model: &Model,
    count: usize,
    dim: u32,
) -> Vec<GeneratedQuery> {
    let mut g = Gen::new(seed, &format!("queries/{phase}"), dim);
    (0..count as u32).map(|id| g.query(id, model)).collect()
}

/// Rule 1's schema with vectors of `dim` dimensions. The vector's
/// `full_scan_threshold_kb` is 1, so filtered approximate queries reach the
/// ANN path at this collection size (row 12.2).
pub fn schema(dim: u32) -> CollectionSchema {
    let field = |name: &str, kind: FieldKind| FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    };
    let text = || FieldKind::Text {
        analyzer: "english".to_string(),
        positions: true,
    };
    let schema = CollectionSchema::new(
        vec![
            field("title", text()),
            field("txt", text()),
            field("tag", FieldKind::Keyword),
            field("tags", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field("f", FieldKind::F64),
            field("ts", FieldKind::Date),
            field("flag", FieldKind::Bool),
        ],
        vec![VectorSpec {
            name: VECTOR.to_string(),
            dim,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::IvfPq {
                num_partitions: Some(16),
                num_sub_vectors: None,
                num_bits: 8,
            },
            hnsw: HnswParams {
                full_scan_threshold_kb: 1,
                ..HnswParams::default()
            },
            quantization: None,
        }],
        DynamicMapping::Strict,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: SPARSE.to_string(),
        modifier: SparseModifier::Idf,
    }]);
    schema.validate().expect("the differential schema is valid");
    schema
}

// ----- The fixture and the harness -----

/// Moves the metastore clock (a `ManualClock` in tests). `loams-hot` does
/// not depend on `loams-meta` (D47), so the fixture hands its clock over
/// through this trait (row 12.1).
pub trait DiffClock: Send + Sync + fmt::Debug {
    fn advance(&self, by: Duration);
}

/// Everything the harness drives; built by the test over one
/// [`CollectionContext`].
#[derive(Debug)]
pub struct DiffFixture {
    pub meta: Arc<dyn MetaStore>,
    pub clock: Arc<dyn DiffClock>,
    pub ctx: CollectionContext,
    pub ns: String,
    pub collection: String,
    /// Reads through `tier` (or a wrapper of it).
    pub hot: Arc<CollectionService>,
    /// No hot tier.
    pub cold: Arc<CollectionService>,
    pub tier: HotTierImpl,
    pub link: LinkApplySource,
    pub index: IndexBuildSource,
    pub merge: SplitMergeSource,
    pub compaction: LanceCompactionSource,
    pub build: HotBuildSource,
}

#[derive(Debug)]
pub struct DiffHarness {
    f: DiffFixture,
    model: Model,
    /// Every write's token, merged.
    written: ConsistencyToken,
    ids: Option<(NamespaceId, CollectionId)>,
}

/// A phase's recall counts, for the pools of phases too small to judge.
#[derive(Clone, Copy, Debug, Default)]
struct Pooled {
    queries: u64,
    hot_found: usize,
    cold_found: usize,
    expected: usize,
    /// The hot tier's recall was judged on the phase alone
    /// ([`MIN_JUDGED`]).
    hot_judged: bool,
    /// The cold tier's recall was judged on the phase alone
    /// ([`MIN_POOLED`]).
    cold_judged: bool,
}

/// One tier's pool of phases too small to judge on their own.
#[derive(Clone, Copy, Debug, Default)]
struct Pool {
    queries: u64,
    found: usize,
    expected: usize,
}

impl Pool {
    fn add(&mut self, queries: u64, found: usize, expected: usize) {
        self.queries += queries;
        self.found += found;
        self.expected += expected;
    }
}

/// A phase's recall is judged on its own from this many approximate
/// queries.
const MIN_JUDGED: u64 = 20;

/// The cold tier's recall is judged only from this many approximate
/// queries, in a phase alone or in the pool of smaller phases (Ruling C4);
/// the hot tier's is judged from [`MIN_JUDGED`] in a phase and from any
/// number in the pool.
///
/// The seeds fix every document, query and third-pass pick, but Lance
/// 12.0.0 trains the cold tier's IVF centroids and PQ codebooks with
/// k-means seeded from the OS (`KMeansParams::seed` is `None`, and neither
/// `IvfBuildParams` nor `PQBuildParams` exposes it), so the cold index and
/// its recall differ from run to run; making it deterministic would mean
/// patching Lance or supplying trained centroids and codebooks. In
/// `random_per_request_disabling_matches_cold` the pool held nine queries
/// (90 hits), where five misses read 0.944 and failed CI (seed
/// 3512381933) while the same run's 139-query phase read 0.988; five local
/// runs read 0.977-0.985 there, and one of them pooled 0.944 again. At 50
/// queries (500 hits) 0.95 needs 25 misses, against 6-12 at the measured
/// rates.
///
/// Judging the cold tier on a phase alone from [`MIN_JUDGED`] queries
/// failed the same way: the default phases hold 25-40 approximate queries,
/// and the `applied` phase read 0.948 in CI twice (seed 3 with 33 queries,
/// seed 1 with 27: 17 and 14 misses where 16 and 13 pass) while the same
/// runs' other phases read 0.979-0.995. So a phase's cold recall is judged
/// alone only from [`MIN_POOLED`] queries too (the 139- and 1000-query
/// `applied` phases of `random_per_request_disabling_matches_cold` still
/// are), and the default phases pool: about 170 queries at 0.977-0.986 in
/// those runs, where 0.95 needs about twice their misses.
///
/// The hot tier keeps the old rule: it is what the harness guards, and
/// its test engines are deterministic (`FlatEngine` is exact, so hot
/// recall is 1.0). The broken tiers are caught by it on small pools: the
/// deleted-row tier reads 0.0 over about 20 queries. The cold tier's ANN
/// recall has its own tests (`loams-query` `ann`), and its larger phases
/// are still judged on their own from [`MIN_POOLED`] queries.
const MIN_POOLED: u64 = 50;

/// Which tiers a phase with `approximate` approximate queries is judged on
/// by itself: `(hot, cold)`.
fn tiers_judged(approximate: u64) -> (bool, bool) {
    (approximate >= MIN_JUDGED, approximate >= MIN_POOLED)
}

#[cfg(test)]
mod judged_tests {
    use super::*;

    #[test]
    fn the_thresholds_are_inclusive_and_the_cold_tier_needs_more() {
        assert_eq!(tiers_judged(MIN_JUDGED - 1), (false, false));
        assert_eq!(tiers_judged(MIN_JUDGED), (true, false));
        assert_eq!(tiers_judged(MIN_POOLED - 1), (true, false));
        assert_eq!(tiers_judged(MIN_POOLED), (true, true));
    }
}

/// One answer, as compared.
struct Answer {
    /// The canonical body: the response without `hot_used`, or the error.
    body: Value,
    /// A search's typed response, for the approximate checks.
    search: Option<SearchResponse>,
}

impl Answer {
    fn text(&self) -> String {
        self.body.to_string()
    }
}

/// Where a query of the third pass runs.
#[derive(Clone, Copy, Debug)]
enum Pick {
    Cold,
    Hot,
    /// The hot service with the request's hot switch off.
    HotOff,
}

impl DiffHarness {
    pub async fn new(fixture: DiffFixture) -> Self {
        Self {
            f: fixture,
            model: Model::default(),
            written: ConsistencyToken::default(),
            ids: None,
        }
    }

    /// The fixture, for tests that inspect the pipeline afterwards.
    pub fn fixture(&self) -> &DiffFixture {
        &self.f
    }

    pub fn model(&self) -> &Model {
        &self.model
    }

    /// Creates the collection, pins it, and runs the five phases.
    pub async fn run(&mut self, config: &DiffConfig) -> DiffReport {
        boxed(self.run_phases(config)).await
    }

    async fn run_phases(&mut self, config: &DiffConfig) -> DiffReport {
        let mut report = DiffReport {
            seed: config.seed,
            ..DiffReport::default()
        };
        self.create(config).await;
        let mut docs = Gen::new(config.seed, "documents", config.dim);
        let debug = std::env::var("DIFF_DEBUG").is_ok();
        let (mut hot_pool, mut cold_pool) = (Pool::default(), Pool::default());
        for phase in PHASES {
            let started = Instant::now();
            match phase {
                "loaded" => self.load(config, &mut docs).await,
                "tail" => self.write_tail(config, &mut docs).await,
                "applied" => {
                    self.apply_link().await;
                    self.reconcile(false).await;
                }
                "maintained" => {
                    self.maintain().await;
                    self.reconcile(false).await;
                }
                _ => self.rebuild().await,
            }
            let prepared = started.elapsed();
            let (phase_report, mismatches, pooled) = self.compare(config, phase).await;
            if !pooled.hot_judged {
                hot_pool.add(pooled.queries, pooled.hot_found, pooled.expected);
            }
            if !pooled.cold_judged {
                cold_pool.add(pooled.queries, pooled.cold_found, pooled.expected);
            }
            if debug {
                eprintln!(
                    "{phase}: prepared in {prepared:?}, compared in {:?}",
                    started.elapsed() - prepared
                );
            }
            report.phases.push(phase_report);
            report.mismatches.extend(mismatches);
        }
        let mut tiers = vec![("hot", hot_pool, MIN_JUDGED)];
        if cold_pool.queries >= MIN_POOLED {
            tiers.push(("cold", cold_pool, MIN_POOLED));
        }
        for (tier, pool, below) in tiers {
            if pool.expected > 0 {
                let value = pool.found as f64 / pool.expected as f64;
                if value < config.min_recall {
                    report.mismatches.push(Mismatch {
                        phase: "pooled",
                        query: GeneratedQuery {
                            id: u32::MAX,
                            class: QueryClass::Approximate,
                            op: DiffOp::Count(None),
                        },
                        reason: format!(
                            "recall@{RECALL_AT} of the {tier} tier over the phases with fewer \
                             than {below} approximate queries is {value:.3} < {}",
                            config.min_recall
                        ),
                        hot: String::new(),
                        cold: String::new(),
                    });
                }
            }
        }
        report
    }

    fn ids(&self) -> (NamespaceId, CollectionId) {
        self.ids.expect("the collection exists")
    }

    async fn create(&mut self, config: &DiffConfig) {
        let f = &self.f;
        boxed(
            f.hot
                .create_collection(&f.ns, &f.collection, schema(config.dim), Some(2)),
        )
        .await
        .expect("create the collection");
        let ns = f
            .meta
            .namespace_by_name(Consistency::Linearizable, &f.ns)
            .await
            .expect("read the namespace")
            .expect("the namespace")
            .id;
        let collection = f
            .meta
            .resolve_collection(Consistency::Linearizable, ns, &f.collection)
            .await
            .expect("resolve")
            .expect("the collection");
        f.meta
            .set_collection_hot(
                ns,
                collection.id,
                HotConfig {
                    vectors: true,
                    text: true,
                    fragments: true,
                },
            )
            .await
            .expect("pin the collection");
        // Both services must see the new collection before they are read.
        let deadline = Instant::now() + WAIT;
        while boxed(f.cold.get_collection(&f.ns, &f.collection))
            .await
            .is_err()
        {
            assert!(
                Instant::now() < deadline,
                "the cold service never saw the collection"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.ids = Some((ns, collection.id));
    }

    async fn write(&mut self, ops: Vec<DocOp>) {
        let f = &self.f;
        let result = boxed(f.hot.write(
            &f.ns,
            &f.collection,
            ops.clone(),
            WriteOptions {
                atomic: true,
                ..WriteOptions::default()
            },
        ))
        .await
        .expect("write");
        assert!(
            result.positions.iter().all(Option::is_some),
            "an op was not written: {:?}",
            result.results
        );
        self.written.merge(&result.token);
        for op in &ops {
            self.model.apply(op);
        }
    }

    async fn live(&self) -> Arc<CollectionManifest> {
        let (ns, cid) = self.ids();
        live_manifest(
            &*self.f.meta,
            &self.f.ctx.store,
            &self.f.ctx.manifests,
            ns,
            cid,
            Consistency::Linearizable,
        )
        .await
        .expect("read the live manifest")
        .map(|(_, manifest)| manifest)
        .expect("a live manifest")
    }

    /// Runs `source` once; true when every task it proposed was idle.
    async fn run_source(&self, owner: &str, source: &dyn TaskSource) -> bool {
        let results = run_once(self.f.meta.clone(), owner, TTL, source)
            .await
            .unwrap_or_else(|err| panic!("{owner}: {err}"));

        results.iter().all(|(key, result)| match result {
            RunResult::Ran(Ok(TaskOutcome::Idle)) => true,
            RunResult::Ran(Ok(_)) | RunResult::LeaseHeld => false,
            RunResult::Ran(Err(err)) => panic!("{owner} {key}: {err}"),
        })
    }

    /// Link apply until the live manifest applied every write.
    async fn apply_link(&self) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.run_source("diff-linker", &self.f.link).await;
            let applied = self.live_applied().await;
            let done =
                self.written.0.iter().all(|(_, partition, offset)| {
                    applied.get(partition).is_some_and(|a| a >= offset)
                });
            if done {
                return;
            }
            assert!(Instant::now() < deadline, "link apply never caught up");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn live_applied(&self) -> BTreeMap<u32, u64> {
        let (ns, cid) = self.ids();
        live_manifest(
            &*self.f.meta,
            &self.f.ctx.store,
            &self.f.ctx.manifests,
            ns,
            cid,
            Consistency::Linearizable,
        )
        .await
        .expect("read the live manifest")
        .map(|(_, manifest)| manifest.applied.clone())
        .unwrap_or_default()
    }

    /// Rule 2 `loaded`.
    async fn load(&mut self, config: &DiffConfig, docs: &mut Gen) {
        let per_batch = config.docs.div_ceil(BATCHES).max(1);
        let mut k = 0u64;
        while (k as usize) < config.docs {
            let end = (k as usize + per_batch).min(config.docs) as u64;
            let ops = (k..end).map(|k| DocOp::Upsert(docs.document(k))).collect();
            self.write(ops).await;
            self.apply_link().await;
            k = end;
        }
        let deadline = Instant::now() + WAIT;
        while !self.run_source("diff-indexer", &self.f.index).await {
            assert!(
                Instant::now() < deadline,
                "the index builds never went idle"
            );
        }
        self.build_artifact(None).await;
        self.reconcile(true).await;
    }

    /// Runs the hot build until the live manifest references an artifact
    /// of [`VECTOR`] other than `replacing`.
    async fn build_artifact(&self, replacing: Option<String>) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.run_source("diff-builder", &self.f.build).await;
            let live = self.live().await;
            let built = live
                .hot_artifacts
                .iter()
                .any(|a| a.column == VECTOR_COLUMN && Some(&a.prefix) != replacing.as_ref());
            if built {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the artifact was never committed"
            );
        }
    }

    /// Reconciles until the tier serves a view of the live manifest (whose
    /// artifact is current when `current`) and has pinned every split.
    async fn reconcile(&self, current: bool) {
        let (ns, cid) = self.ids();
        let deadline = Instant::now() + WAIT;
        loop {
            let report = self.f.tier.reconcile_once().await.expect("reconcile");
            let live = self.live().await;
            let view = self
                .f
                .tier
                .column_view(ns, cid, VECTOR_COLUMN, live.version);
            let served = view.is_some_and(|v| {
                !current || loams_query::hot::HotAnn::source_version(&*v) == live.version
            });
            let pinned = live
                .splits
                .iter()
                .all(|split| self.f.tier.split_file(ns, cid, split.ulid).is_some());
            if served && pinned {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the tier never served the live manifest {} (served {served}, pinned {pinned}): {:?}",
                live.version,
                report.failures
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Rule 2 `tail`: 2 % upserts (half of them new keys), 1 % deletes and
    /// 1 % patches of distinct keys, not applied.
    async fn write_tail(&mut self, config: &DiffConfig, docs: &mut Gen) {
        let n = config.docs;
        let (upserts, deletes, patches) = ((n / 50).max(2), (n / 100).max(1), (n / 100).max(1));
        let mut keys = self.model.keys();
        let mut ops = Vec::new();
        let take = |g: &mut Gen, keys: &mut Vec<PrimaryKey>| -> Option<PrimaryKey> {
            (!keys.is_empty()).then(|| {
                let i = g.range(0..keys.len());
                keys.swap_remove(i)
            })
        };
        let mut next = self.model.next_key;
        for i in 0..upserts {
            let k = match i % 2 {
                0 => {
                    next += 1;
                    next - 1
                }
                _ => match take(docs, &mut keys) {
                    Some(PrimaryKey::U64(k)) => k,
                    _ => continue,
                },
            };
            ops.push(DocOp::Upsert(docs.document(k)));
        }
        for _ in 0..deletes {
            if let Some(pk) = take(docs, &mut keys) {
                ops.push(DocOp::Delete(pk));
            }
        }
        for _ in 0..patches {
            let Some(pk) = take(docs, &mut keys) else {
                continue;
            };
            let mut source = Map::new();
            source.insert("n".into(), json!(docs.range(0..1_000) as i64));
            source.insert("tag".into(), json!(docs.tag()));
            let mut vectors = BTreeMap::new();
            match docs.range(0..10) {
                0 => {
                    vectors.insert(VECTOR.to_string(), None);
                }
                1..4 => {
                    vectors.insert(VECTOR.to_string(), Some(docs.vector()));
                }
                _ => {}
            }
            ops.push(DocOp::Patch {
                pk,
                mode: PatchMode::MergeTop,
                source,
                delete_keys: Vec::new(),
                vectors,
                sparse_vectors: BTreeMap::new(),
                upsert: None,
            });
        }
        self.write(ops).await;
    }

    /// Rule 2 `maintained`: merges and compactions until both are idle.
    async fn maintain(&self) {
        let deadline = Instant::now() + WAIT;
        loop {
            let merges_idle = self.run_source("diff-merger", &self.f.merge).await;
            let compactions_idle = self.run_source("diff-compactor", &self.f.compaction).await;
            if merges_idle && compactions_idle {
                return;
            }
            assert!(Instant::now() < deadline, "maintenance never went idle");
        }
    }

    /// Rule 2 `rebuilt`: past `rebuild_max_staleness`, a new artifact, and
    /// a view that is current.
    async fn rebuild(&self) {
        let before = self
            .live()
            .await
            .hot_artifacts
            .iter()
            .find(|a| a.column == VECTOR_COLUMN)
            .map(|a| a.prefix.clone());
        let staleness = self.f.build.config().rebuild_max_staleness;
        self.f.clock.advance(staleness + Duration::from_secs(1));
        self.build_artifact(before).await;
        self.reconcile(true).await;
    }

    // ----- Comparing -----

    async fn execute(
        &self,
        pick: Pick,
        query: &GeneratedQuery,
        consistency: &ReadConsistency,
    ) -> Answer {
        let f = &self.f;
        let (service, switch) = match pick {
            Pick::Cold => (&f.cold, None),
            Pick::Hot => (&f.hot, None),
            Pick::HotOff => (&f.hot, Some(false)),
        };
        let run = boxed(run_op(
            service,
            &f.ns,
            &f.collection,
            &query.op,
            consistency,
        ));
        match switch {
            None => run.await,
            Some(enabled) => {
                hot::scope(
                    RequestHot {
                        enabled,
                        used: HotUsed::default(),
                    },
                    run,
                )
                .await
            }
        }
    }

    async fn compare(
        &self,
        config: &DiffConfig,
        phase: &'static str,
    ) -> (PhaseReport, Vec<Mismatch>, Pooled) {
        let f = &self.f;
        let pinned = boxed(f.hot.pin(&f.ns, &f.collection))
            .await
            .expect("pin a read");
        let consistency = ReadConsistency::Pinned {
            manifest_version: pinned.manifest_version,
            token: pinned.token,
        };
        let queries = generate_with(
            config.seed,
            phase,
            &self.model,
            config.queries_in(phase),
            config.dim,
        );
        let mut report = PhaseReport {
            phase,
            ..PhaseReport::default()
        };
        let mut mismatches = Vec::new();
        let mismatch =
            |query: &GeneratedQuery, reason: String, hot: String, cold: String| Mismatch {
                phase,
                query: query.clone(),
                reason,
                hot,
                cold,
            };

        // Pass 1: cold. Neither counter may move.
        let before = f.tier.counters();
        let mut cold = Vec::with_capacity(queries.len());
        for query in &queries {
            cold.push(self.execute(Pick::Cold, query, &consistency).await);
        }
        let after_cold = f.tier.counters();
        // Pass 2: hot.
        let mut hot = Vec::with_capacity(queries.len());
        for query in &queries {
            hot.push(self.execute(Pick::Hot, query, &consistency).await);
        }
        let after_hot = f.tier.counters();
        report.ann_served = after_hot.ann_served - after_cold.ann_served;
        report.split_files_served = after_hot.split_files_served - after_cold.split_files_served;

        let marker = GeneratedQuery {
            id: u32::MAX,
            class: QueryClass::Identical,
            op: DiffOp::Count(None),
        };
        if after_cold.ann_served != before.ann_served
            || after_cold.split_files_served != before.split_files_served
        {
            mismatches.push(mismatch(
                &marker,
                "the cold pass touched the hot tier".into(),
                format!("{after_cold:?}"),
                format!("{before:?}"),
            ));
        }

        // Pass 3: a seeded choice of service and switch per query.
        let mut picks = rng_for(config.seed, &format!("picks/{phase}"));
        let mut approximate = 0u64;
        let (mut hot_found, mut cold_found, mut expected) = (0usize, 0usize, 0usize);
        for (i, query) in queries.iter().enumerate() {
            let pick = match picks.random_range(0..3) {
                0 => Pick::Cold,
                1 => Pick::Hot,
                _ => Pick::HotOff,
            };
            let third = self.execute(pick, query, &consistency).await;
            let (c, h) = (&cold[i], &hot[i]);
            match query.class {
                QueryClass::Identical => {
                    report.identical += 1;
                    if c.body != h.body {
                        mismatches.push(mismatch(
                            query,
                            "hot and cold differ".into(),
                            h.text(),
                            c.text(),
                        ));
                    } else if third.body != c.body {
                        mismatches.push(mismatch(
                            query,
                            format!("the third pass ({pick:?}) differs"),
                            third.text(),
                            c.text(),
                        ));
                    }
                }
                QueryClass::Approximate => {
                    report.approximate += 1;
                    approximate += 1;
                    let same = match pick {
                        Pick::Hot => h,
                        Pick::Cold | Pick::HotOff => c,
                    };
                    if third.body != same.body {
                        mismatches.push(mismatch(
                            query,
                            format!("the third pass ({pick:?}) differs from its tier's pass"),
                            third.text(),
                            same.text(),
                        ));
                    }
                    let (found_c, want) =
                        self.check_approximate(config, query, c, "cold", &mut mismatches, phase);
                    let (found_h, _) =
                        self.check_approximate(config, query, h, "hot", &mut mismatches, phase);
                    cold_found += found_c;
                    hot_found += found_h;
                    expected += want;
                }
            }
        }
        let recall = |found: usize| match expected {
            0 => 1.0,
            _ => found as f64 / expected as f64,
        };
        report.hot_recall = recall(hot_found);
        report.cold_recall = recall(cold_found);
        // A phase with few approximate queries is judged only in the pool
        // of such phases (row 12.5; the cold tier from `MIN_POOLED` queries,
        // Ruling C4): one miss in three queries is 0.933.
        let (hot_judged, cold_judged) = tiers_judged(approximate);
        for (tier, value, judged) in [
            ("hot", report.hot_recall, hot_judged),
            ("cold", report.cold_recall, cold_judged),
        ] {
            if judged && value < config.min_recall {
                let example = queries
                    .iter()
                    .find(|q| q.class == QueryClass::Approximate)
                    .unwrap_or(&marker);
                mismatches.push(mismatch(
                    example,
                    format!(
                        "recall@{RECALL_AT} of the {tier} tier is {value:.3} < {}",
                        config.min_recall
                    ),
                    format!("{}", report.hot_recall),
                    format!("{}", report.cold_recall),
                ));
            }
        }

        // Rule 7: the hot tier was exercised.
        if matches!(phase, "loaded" | "rebuilt") {
            let wanted = (approximate as f64 * 0.9).ceil() as u64;
            if report.ann_served < wanted || report.split_files_served == 0 {
                mismatches.push(mismatch(
                    &marker,
                    "the hot tier was not exercised".into(),
                    format!(
                        "ann_served +{} (want >= {wanted}), split_files_served +{}",
                        report.ann_served, report.split_files_served
                    ),
                    String::new(),
                ));
            }
        }
        let pooled = Pooled {
            queries: approximate,
            hot_found,
            cold_found,
            expected,
            hot_judged,
            cold_judged,
        };
        (report, mismatches, pooled)
    }

    /// Rule 6 for one response of one tier: every hit is a model row that
    /// the filter matches, with its exact score. Returns the hits among the
    /// exact top 10 (ties with the 10th included) and how many there are.
    fn check_approximate(
        &self,
        config: &DiffConfig,
        query: &GeneratedQuery,
        answer: &Answer,
        tier: &str,
        mismatches: &mut Vec<Mismatch>,
        phase: &'static str,
    ) -> (usize, usize) {
        let DiffOp::Search(request) = &query.op else {
            return (0, 0);
        };
        let Some(Retriever::Vector {
            query: q, filter, ..
        }) = request.retrievers.first()
        else {
            return (0, 0);
        };
        let push = |mismatches: &mut Vec<Mismatch>, reason: String| {
            mismatches.push(Mismatch {
                phase,
                query: query.clone(),
                reason: format!("{tier}: {reason}"),
                hot: answer.text(),
                cold: String::new(),
            });
        };
        let exact = self.model.exact_top(q, filter.as_ref(), RECALL_AT);
        let Some(response) = &answer.search else {
            push(mismatches, "the search failed".into());
            return (0, exact.len());
        };
        let tenth = exact.last().map(|(score, _)| *score);
        let top: BTreeSet<&PrimaryKey> = exact.iter().map(|(_, pk)| pk).collect();
        let mut found = 0;
        for hit in &response.hits {
            let Some(doc) = self.model.get(&hit.pk) else {
                push(mismatches, format!("hit {:?} is not in the model", hit.pk));
                continue;
            };
            if let Some(filter) = filter
                && matches(filter, doc) == Some(false)
            {
                push(
                    mismatches,
                    format!("hit {:?} does not match the filter", hit.pk),
                );
            }
            let Some(v) = doc.vectors.get(VECTOR) else {
                push(mismatches, format!("hit {:?} has no vector", hit.pk));
                continue;
            };
            let want = loams_hnsw::exact_score(Distance::Cosine, q, v);
            let tolerance = config.score_tolerance * want.abs().max(1.0);
            if (hit.score - want).abs() > tolerance {
                push(
                    mismatches,
                    format!("hit {:?} scores {} but exactly {want}", hit.pk, hit.score),
                );
            }
            let tied = tenth.is_some_and(|t| (want - t).abs() <= tolerance);
            if top.contains(&hit.pk) || tied {
                found += 1;
            }
        }
        (found.min(exact.len()), exact.len())
    }
}

/// `fut` as a trait object: the service's futures are deep enough that a
/// caller's future type overflows rustc's query depth otherwise (row 10.6).
fn boxed<'a, T>(
    fut: impl std::future::Future<Output = T> + Send + 'a,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>> {
    Box::pin(fut)
}

/// Runs `op` on `service` at `consistency`.
async fn run_op(
    service: &CollectionService,
    ns: &str,
    name: &str,
    op: &DiffOp,
    consistency: &ReadConsistency,
) -> Answer {
    fn body<T: serde::Serialize, E: fmt::Display>(result: &Result<T, E>) -> Value {
        match result {
            Ok(value) => serde_json::to_value(value).expect("a JSON answer"),
            Err(err) => json!({ "error": err.to_string() }),
        }
    }
    match op {
        DiffOp::Search(request) => {
            let mut request = request.clone();
            request.collection = name.to_string();
            request.consistency = consistency.clone();
            let result = boxed(service.search(ns, request)).await;
            let mut value = body(&result);
            if let Value::Object(map) = &mut value {
                // Which structures served a read is the only allowed
                // difference (R12).
                map.remove("hot_used");
            }
            Answer {
                body: value,
                search: result.ok(),
            }
        }
        DiffOp::Get(pks, projection) => {
            let result =
                boxed(service.get_with_token(ns, name, pks, projection, consistency.clone())).await;
            Answer {
                body: body(&result.map(|(docs, token)| (docs, format!("{token:?}")))),
                search: None,
            }
        }
        DiffOp::Count(filter) => {
            let result =
                boxed(service.count_with_token(ns, name, filter.clone(), consistency.clone()))
                    .await;
            Answer {
                body: body(&result.map(|(count, token)| (count, format!("{token:?}")))),
                search: None,
            }
        }
        DiffOp::Scroll(filter, page) => {
            let mut pages = Vec::new();
            let mut after = None;
            let all = Projection::default();
            let result = loop {
                match boxed(service.scroll_with_token(
                    ns,
                    name,
                    filter.clone(),
                    after.clone(),
                    *page,
                    &all,
                    consistency.clone(),
                ))
                .await
                {
                    Ok(((docs, next), token)) => {
                        pages.push(json!({
                            "docs": serde_json::to_value(&docs).expect("docs"),
                            "token": format!("{token:?}"),
                        }));
                        match next {
                            Some(next) if pages.len() < 1_000 => after = Some(next),
                            _ => break Ok(pages),
                        }
                    }
                    Err(err) => break Err(err),
                }
            };
            Answer {
                body: body(&result),
                search: None,
            }
        }
    }
}
