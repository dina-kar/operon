//! The M1.2 contracts the Qdrant gateway relies on (plan M1.4 Task 0,
//! S1–S12), checked against the `CollectionService` of an in-process
//! `Server`.
//!
//! Every read check runs twice: while the documents are still in the live
//! tail, and again once the link has applied them to splits, so the tail
//! and the durable path must agree.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use loams::{Server, ServerConfig};
use loams_collection::{
    CollectionSchema, Distance, DocOp, Document, DynamicMapping, FieldKind, FieldSpec, HnswParams,
    PrimaryKey, SparseModifier, SparseVector, SparseVectorSpec, VectorElement, VectorIndexSpec,
    VectorSpec,
};
use loams_query::{
    AnnParams, BoolOperator, CollectionService, FieldValue, Fusion, OpResult, Projection, Query,
    ReadConsistency, Retriever, SearchRequest, ServiceError, SourceFilter, SparseParams,
    WriteOptions,
};
use serde_json::{Map, Value, json};
use tempfile::TempDir;

const NS: &str = "qdrant";

/// The two phases every read check runs in.
#[derive(Clone, Copy, Debug)]
enum Phase {
    Tail,
    Indexed,
}

const PHASES: [Phase; 2] = [Phase::Tail, Phase::Indexed];

// ----- fixture -----

struct Fx {
    server: Server,
    svc: Arc<CollectionService>,
    _dir: TempDir,
}

impl Fx {
    async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = ServerConfig::new(dir.path());
        config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.log.flush_interval = Duration::from_millis(20);
        config.worker_poll_interval = Duration::from_millis(50);
        config.link.batch_interval = Duration::ZERO;
        let server = Server::start(config).await.expect("start");
        let svc = server.collections();
        Self {
            server,
            svc,
            _dir: dir,
        }
    }

    async fn create(&self, name: &str, schema: CollectionSchema) {
        schema.validate().expect("valid schema");
        self.svc
            .create_collection(NS, name, schema, None)
            .await
            .expect("create collection");
    }

    async fn write(&self, name: &str, ops: Vec<DocOp>) {
        let result = self
            .svc
            .write(NS, name, ops, WriteOptions::default())
            .await
            .expect("write");
        for op in &result.results {
            assert!(!matches!(op, OpResult::Rejected(_)), "{op:?}");
        }
    }

    async fn upsert(&self, name: &str, docs: Vec<Document>) {
        self.write(name, docs.into_iter().map(DocOp::Upsert).collect())
            .await;
    }

    /// Waits until the link has applied every record of `name`.
    async fn indexed(&self, name: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let info = self.svc.get_collection(NS, name).await.expect("info");
            if info.link_lag_records == 0 && info.manifest_version > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{name} was not indexed: {info:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Runs `phase`: nothing for `Tail`, the wait for `Indexed`.
    async fn enter(&self, name: &str, phase: Phase) {
        if let Phase::Indexed = phase {
            self.indexed(name).await;
        }
    }

    async fn search(&self, request: SearchRequest) -> Vec<(PrimaryKey, f32)> {
        self.svc
            .search(NS, request)
            .await
            .expect("search")
            .hits
            .into_iter()
            .map(|hit| (hit.pk, hit.score))
            .collect()
    }

    /// The keys a filter-only request over `name` returns (at most 100).
    async fn matching(&self, name: &str, filter: Query) -> BTreeSet<u64> {
        let mut request = SearchRequest::new(name);
        request.filter = Some(filter);
        request.limit = 100;
        self.search(request)
            .await
            .into_iter()
            .map(|(pk, _)| match pk {
                PrimaryKey::U64(n) => n,
                other => panic!("unexpected key {other:?}"),
            })
            .collect()
    }

    async fn shutdown(self) {
        self.server.shutdown().await.expect("shutdown");
    }
}

// ----- builders -----

/// The catch-all `payload` field of every gateway collection (Ruling 5).
fn payload_field() -> FieldSpec {
    FieldSpec {
        name: "payload".to_string(),
        source_path: String::new(),
        kind: FieldKind::Json,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

fn dense(name: &str, dim: u32, distance: Distance) -> VectorSpec {
    VectorSpec {
        name: name.to_string(),
        dim,
        distance,
        element: VectorElement::F32,
        index: VectorIndexSpec::Auto,
        hnsw: HnswParams::default(),
        quantization: None,
    }
}

fn sparse_spec(name: &str, modifier: SparseModifier) -> SparseVectorSpec {
    SparseVectorSpec {
        name: name.to_string(),
        modifier,
    }
}

/// A gateway-shaped schema: `payload` only, plus the given vectors.
fn schema(vectors: Vec<VectorSpec>, sparse: Vec<SparseVectorSpec>) -> CollectionSchema {
    CollectionSchema::new(vec![payload_field()], vectors, DynamicMapping::Ignore)
        .with_sparse_vectors(sparse)
}

fn object(source: Value) -> Map<String, Value> {
    match source {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

fn doc(pk: impl Into<Key>, source: Value) -> Document {
    Document {
        pk: pk.into().0,
        source: object(source),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    }
}

/// A key literal: a u64 or a ready `PrimaryKey`.
struct Key(PrimaryKey);

impl From<u64> for Key {
    fn from(n: u64) -> Self {
        Key(PrimaryKey::U64(n))
    }
}

impl From<PrimaryKey> for Key {
    fn from(pk: PrimaryKey) -> Self {
        Key(pk)
    }
}

fn with_dense(mut doc: Document, name: &str, vector: &[f32]) -> Document {
    doc.vectors.insert(name.to_string(), vector.to_vec());
    doc
}

fn sv(pairs: &[(u32, f32)]) -> SparseVector {
    let (indices, values) = pairs.iter().copied().unzip();
    SparseVector::new(indices, values).expect("sparse vector")
}

fn with_sparse(mut doc: Document, name: &str, pairs: &[(u32, f32)]) -> Document {
    doc.sparse_vectors.insert(name.to_string(), sv(pairs));
    doc
}

/// An exact dense retriever.
fn knn(field: &str, query: &[f32], k: usize) -> Retriever {
    Retriever::Vector {
        field: field.to_string(),
        query: query.to_vec(),
        k,
        params: AnnParams {
            exact: true,
            ..AnnParams::default()
        },
        filter: None,
    }
}

fn sparse(field: &str, pairs: &[(u32, f32)], k: usize, idf_corpus: Option<Query>) -> Retriever {
    Retriever::Sparse {
        field: field.to_string(),
        query: sv(pairs),
        k,
        filter: None,
        params: SparseParams { idf_corpus },
    }
}

fn request(collection: &str, retrievers: Vec<Retriever>, fusion: Option<Fusion>) -> SearchRequest {
    let mut request = SearchRequest::new(collection);
    request.retrievers = retrievers;
    request.fusion = fusion;
    request.limit = 100;
    request
}

/// One `Retriever::Fused` over `inputs`, as the gateway compiles a Qdrant
/// root fusion (E3): top-level `retrievers` with `fusion` would truncate the
/// fused list to the largest input `k` (M1.2 Task 6 rule 2), while Qdrant
/// fuses the whole union and then applies `limit`.
fn fused(collection: &str, inputs: Vec<Retriever>, fusion: Fusion) -> SearchRequest {
    let k = 100;
    request(
        collection,
        vec![Retriever::Fused { inputs, fusion, k }],
        None,
    )
}

fn set(keys: &[u64]) -> BTreeSet<u64> {
    keys.iter().copied().collect()
}

fn term(field: &str, value: FieldValue) -> Query {
    Query::Term {
        field: field.to_string(),
        value,
    }
}

fn range(field: &str, gte: Option<FieldValue>, lt: Option<FieldValue>) -> Query {
    Query::Range {
        field: field.to_string(),
        gt: None,
        gte,
        lt,
        lte: None,
    }
}

fn values_count(field: &str, gte: Option<u64>, lte: Option<u64>) -> Query {
    Query::ValuesCount {
        field: field.to_string(),
        gt: None,
        gte,
        lt: None,
        lte,
    }
}

// ----- reference scorers (Qdrant's formulas, "Qdrant protocol facts") -----

/// Qdrant's RRF: `Σ 1/(pos + k)` with 0-based positions.
fn qdrant_rrf(lists: &[Vec<(PrimaryKey, f32)>], qdrant_k: u32) -> BTreeMap<PrimaryKey, f32> {
    let mut fused = BTreeMap::new();
    for list in lists {
        for (pos, (pk, _)) in list.iter().enumerate() {
            *fused.entry(pk.clone()).or_insert(0.0f32) += 1.0 / (pos as f32 + qdrant_k as f32);
        }
    }
    fused
}

/// Qdrant's DBSF: per list, mean and sample standard deviation (Welford),
/// `(s - (μ - 3σ)) / (6σ)`, 0.5 for a list of one or a degenerate range;
/// summed per point.
fn qdrant_dbsf(lists: &[Vec<(PrimaryKey, f32)>]) -> BTreeMap<PrimaryKey, f32> {
    let mut fused = BTreeMap::new();
    for list in lists {
        let normalized: Vec<f64> = if list.len() < 2 {
            vec![0.5; list.len()]
        } else {
            let (mut mean, mut m2) = (0.0f64, 0.0f64);
            for (i, (_, score)) in list.iter().enumerate() {
                let x = f64::from(*score);
                let delta = x - mean;
                mean += delta / (i + 1) as f64;
                m2 += delta * (x - mean);
            }
            let sigma = (m2 / (list.len() - 1) as f64).sqrt();
            let (low, high) = (mean - 3.0 * sigma, mean + 3.0 * sigma);
            if low == high {
                vec![0.5; list.len()]
            } else {
                list.iter()
                    .map(|(_, s)| (f64::from(*s) - low) / (high - low))
                    .collect()
            }
        };
        for ((pk, _), value) in list.iter().zip(normalized) {
            *fused.entry(pk.clone()).or_insert(0.0f32) += value as f32;
        }
    }
    fused
}

/// Asserts that `hits` are exactly `want`'s keys, ordered by (score desc,
/// pk asc), each within `tolerance` of its reference score.
fn assert_fused(hits: &[(PrimaryKey, f32)], want: &BTreeMap<PrimaryKey, f32>, tolerance: f32) {
    let keys: BTreeSet<&PrimaryKey> = hits.iter().map(|(pk, _)| pk).collect();
    assert_eq!(keys, want.keys().collect(), "{hits:?} vs {want:?}");
    for (pk, score) in hits {
        let reference = want[pk];
        assert!(
            (score - reference).abs() <= tolerance,
            "{pk:?}: {score} vs {reference} ({hits:?})"
        );
    }
    assert_descending(hits);
}

/// Scores descending, equal scores by pk ascending.
fn assert_descending(hits: &[(PrimaryKey, f32)]) {
    for pair in hits.windows(2) {
        let ((a, sa), (b, sb)) = (&pair[0], &pair[1]);
        assert!(
            sa > sb || (sa == sb && a < b),
            "order: {a:?} {sa} before {b:?} {sb} ({hits:?})"
        );
    }
}

/// A sparse corpus entry: key, vector (if any), whether it is in the IDF
/// corpus of the query.
struct SparseDoc<'a> {
    pk: u64,
    vector: Option<&'a [(u32, f32)]>,
    in_corpus: bool,
}

/// Qdrant's sparse score ("Qdrant protocol facts" → "Sparse vectors"):
/// `Σ qᵢ·wᵢ` over shared indices, each query weight multiplied by
/// `ln((N − df + 0.5)/(df + 0.5) + 1)` under IDF, where `N` counts the
/// corpus documents with a non-empty vector and `df` those holding index
/// `i`. A document sharing no index is not a candidate.
fn sparse_reference(
    docs: &[SparseDoc<'_>],
    query: &[(u32, f32)],
    use_idf: bool,
) -> BTreeMap<PrimaryKey, f32> {
    let corpus: Vec<&[(u32, f32)]> = docs
        .iter()
        .filter(|d| d.in_corpus)
        .filter_map(|d| d.vector)
        .filter(|v| !v.is_empty())
        .collect();
    let n = corpus.len() as f64;
    let idf = |index: u32| {
        let df = corpus
            .iter()
            .filter(|v| v.iter().any(|(i, _)| *i == index))
            .count() as f64;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    };
    let mut scores = BTreeMap::new();
    for d in docs {
        let Some(vector) = d.vector else { continue };
        let mut shared = false;
        let mut score = 0.0f64;
        for (qi, qw) in query {
            if let Some((_, w)) = vector.iter().find(|(i, _)| i == qi) {
                shared = true;
                let weight = if use_idf {
                    f64::from(*qw) * idf(*qi)
                } else {
                    f64::from(*qw)
                };
                score += weight * f64::from(*w);
            }
        }
        if shared {
            scores.insert(PrimaryKey::U64(d.pk), score as f32);
        }
    }
    scores
}

/// The live documents 1–5 of a sparse table, in the corpus when `filter`
/// holds.
fn corpus<'a>(
    table: &[(u64, &'a [(u32, f32)])],
    filter: impl Fn(u64) -> bool,
) -> Vec<SparseDoc<'a>> {
    (1..=5u64)
        .map(|pk| SparseDoc {
            pk,
            vector: table.iter().find(|(k, _)| *k == pk).map(|(_, v)| *v),
            in_corpus: filter(pk),
        })
        .collect()
}

// ----- S1, S2: fusion -----

/// Six documents with two Dot vectors whose rankings differ: `a` ranks
/// 6, 5, 4, … for `[1, 0]`; `b` ranks 4, 1, 5, 2, … for `[0, 1]`.
async fn fusion_fixture(fx: &Fx, name: &str) {
    fx.create(
        name,
        schema(
            vec![dense("a", 2, Distance::Dot), dense("b", 2, Distance::Dot)],
            vec![],
        ),
    )
    .await;
    let docs = (1..=6u64)
        .map(|i| {
            let d = doc(i, json!({"i": i}));
            let d = with_dense(d, "a", &[i as f32, 0.0]);
            with_dense(d, "b", &[0.0, ((i * 5) % 7) as f32])
        })
        .collect();
    fx.upsert(name, docs).await;
}

#[tokio::test]
async fn rrf_fusion_uses_one_based_ranks() {
    let fx = Fx::start().await;
    fusion_fixture(&fx, "rrf").await;
    let inputs = || vec![knn("a", &[1.0, 0.0], 4), knn("b", &[0.0, 1.0], 4)];
    for phase in PHASES {
        fx.enter("rrf", phase).await;
        let mut lists = Vec::new();
        for input in inputs() {
            lists.push(fx.search(request("rrf", vec![input], None)).await);
        }
        assert_eq!(lists[0].len(), 4, "{phase:?}");
        // IR k = Qdrant k − 1 (Ruling 9): k 1 is Qdrant's default 2, k 0 is
        // Qdrant's smallest k 1.
        for k in [0u32, 1, 60] {
            let fused = fx.search(fused("rrf", inputs(), Fusion::Rrf { k })).await;
            assert_fused(&fused, &qdrant_rrf(&lists, k + 1), 1e-6);
        }
        // Top-level retrievers with a fusion keep only the largest input k
        // of the six fused documents, which is why the gateway uses
        // `Retriever::Fused` (E3).
        let windowed = fx
            .search(request("rrf", inputs(), Some(Fusion::Rrf { k: 1 })))
            .await;
        assert_eq!(windowed.len(), 4, "{phase:?}: {windowed:?}");
    }
    fx.shutdown().await;
}

#[tokio::test]
async fn dbsf_fusion_matches_the_qdrant_formula() {
    let fx = Fx::start().await;
    fusion_fixture(&fx, "dbsf").await;
    // The third input holds one hit, which normalizes to 0.5.
    let inputs = || {
        vec![
            knn("a", &[1.0, 0.0], 4),
            knn("b", &[0.0, 1.0], 5),
            knn("a", &[-1.0, 0.0], 1),
        ]
    };
    for phase in PHASES {
        fx.enter("dbsf", phase).await;
        let mut lists = Vec::new();
        for input in inputs() {
            lists.push(fx.search(request("dbsf", vec![input], None)).await);
        }
        assert_eq!(lists[2].len(), 1, "{phase:?}");
        let fused = fx.search(fused("dbsf", inputs(), Fusion::Dbsf)).await;
        assert_fused(&fused, &qdrant_dbsf(&lists), 1e-5);
    }
    fx.shutdown().await;
}

// ----- S3: ties -----

#[tokio::test]
async fn equal_scores_order_by_pk() {
    let fx = Fx::start().await;
    fx.create("ties", schema(vec![dense("v", 2, Distance::Dot)], vec![]))
        .await;
    let keys = [
        PrimaryKey::U64(9),
        PrimaryKey::Str("b".to_string()),
        PrimaryKey::Uuid([0x10; 16]),
        PrimaryKey::U64(2),
        PrimaryKey::Str("B".to_string()),
        PrimaryKey::Uuid([0x01; 16]),
        PrimaryKey::Str("a".to_string()),
    ];
    let make = |pk: &PrimaryKey| with_dense(doc(pk.clone(), json!({"t": 1})), "v", &[1.0, 1.0]);
    // Canonical bytes: numbers, then UUIDs, then strings bytewise.
    let mut sorted = keys.to_vec();
    sorted.sort_by_key(PrimaryKey::canonical);
    // The first four go to splits, the rest stay in the tail.
    fx.upsert("ties", keys[..4].iter().map(make).collect())
        .await;
    fx.indexed("ties").await;
    fx.upsert("ties", keys[4..].iter().map(make).collect())
        .await;
    for phase in PHASES {
        fx.enter("ties", phase).await;
        let hits = fx
            .search(request("ties", vec![knn("v", &[1.0, 0.0], 10)], None))
            .await;
        let order: Vec<PrimaryKey> = hits.iter().map(|(pk, _)| pk.clone()).collect();
        assert_eq!(order, sorted, "{phase:?}: equal vector scores");
        for filter in [None, Some(Query::MatchAll)] {
            let mut filter_only = request("ties", vec![], None);
            filter_only.filter = filter.clone();
            let order: Vec<PrimaryKey> = fx
                .search(filter_only)
                .await
                .into_iter()
                .map(|(pk, _)| pk)
                .collect();
            assert_eq!(order, sorted, "{phase:?}: filter-only {filter:?}");
        }
    }
    fx.shutdown().await;
}

// ----- S4, S5: JSON paths -----

/// `payload.n`: 0 → 1, 1 → "1", 2 → 1.0, 3 → 1.5, 5 → [3, "1"].
/// `payload.k`: 0 → [1, 2], 1 → null, 2 → [], 3 → [1, null], else absent.
async fn paths_fixture(fx: &Fx, name: &str) {
    fx.create(name, schema(vec![], vec![])).await;
    fx.upsert(
        name,
        vec![
            doc(0, json!({"n": 1, "k": [1, 2], "s": "abc"})),
            doc(1, json!({"n": "1", "k": null})),
            doc(2, json!({"n": 1.0, "k": []})),
            doc(3, json!({"n": 1.5, "k": [1, null]})),
            doc(
                4,
                json!({"tags": ["a", "b"], "o": {"k": [1, 2], "deep": {"x": true}}}),
            ),
            doc(5, json!({"n": [3, "1"], "s": "abd"})),
        ],
    )
    .await;
}

#[tokio::test]
async fn json_field_paths_are_type_strict() {
    let fx = Fx::start().await;
    paths_fixture(&fx, "paths").await;
    for phase in PHASES {
        fx.enter("paths", phase).await;
        let hits = |query: Query| fx.matching("paths", query);
        // Carried in from M1.2 Task 3: Tantivy indexes the integral float
        // 1.0 as the integer 1, so `I64(1)` matches `1.0` (Qdrant would
        // not); a string never matches a number.
        assert_eq!(
            hits(term("payload.n", FieldValue::I64(1))).await,
            set(&[0, 2]),
            "{phase:?}"
        );
        assert_eq!(
            hits(term("payload.n", FieldValue::Str("1".into()))).await,
            set(&[1, 5]),
            "{phase:?}"
        );
        assert_eq!(
            hits(term("payload.n", FieldValue::F64(1.5))).await,
            set(&[3]),
            "{phase:?}"
        );
        let terms = Query::Terms {
            field: "payload.n".to_string(),
            values: vec![FieldValue::I64(3), FieldValue::Str("x".into())],
        };
        assert_eq!(hits(terms).await, set(&[5]), "{phase:?}: any element");
        // F64 bounds compare integers and floats; strings never match.
        assert_eq!(
            hits(range(
                "payload.n",
                Some(FieldValue::F64(0.5)),
                Some(FieldValue::F64(2.0))
            ))
            .await,
            set(&[0, 2, 3]),
            "{phase:?}"
        );
        assert_eq!(
            hits(range("payload.n", Some(FieldValue::F64(2.5)), None)).await,
            set(&[5]),
            "{phase:?}: an array matches when any element does"
        );
        assert_eq!(
            hits(term("payload.tags", FieldValue::Str("a".into()))).await,
            set(&[4]),
            "{phase:?}"
        );
        assert_eq!(
            hits(term("payload.o.k", FieldValue::I64(2))).await,
            set(&[4]),
            "{phase:?}: nested objects are dotted paths"
        );
        assert_eq!(
            hits(term("payload.o.deep.x", FieldValue::Bool(true))).await,
            set(&[4]),
            "{phase:?}"
        );
        assert_eq!(
            hits(Query::Exists {
                field: "payload.n".to_string()
            })
            .await,
            set(&[0, 1, 2, 3, 5]),
            "{phase:?}"
        );
        assert_eq!(
            hits(Query::Prefix {
                field: "payload.s".to_string(),
                value: "ab".to_string()
            })
            .await,
            set(&[0, 5]),
            "{phase:?}"
        );
    }
    fx.shutdown().await;
}

#[tokio::test]
async fn is_empty_and_is_null_follow_qdrant() {
    let fx = Fx::start().await;
    paths_fixture(&fx, "empty").await;
    for phase in PHASES {
        fx.enter("empty", phase).await;
        let hits = |query: Query| fx.matching("empty", query);
        let field = || "payload.k".to_string();
        assert_eq!(
            hits(Query::IsEmpty { field: field() }).await,
            set(&[1, 2, 4, 5]),
            "{phase:?}: missing, null or []"
        );
        assert_eq!(
            hits(Query::IsNull { field: field() }).await,
            set(&[1, 3]),
            "{phase:?}: null or an array holding null, not a missing key"
        );
        let none = hits(values_count("payload.k", None, Some(0))).await;
        for pk in [2, 4, 5] {
            assert!(none.contains(&pk), "{phase:?}: {pk} counts 0: {none:?}");
        }
        assert!(!none.contains(&0), "{phase:?}: {none:?}");
        let two = hits(values_count("payload.k", Some(2), None)).await;
        assert!(two.contains(&0), "{phase:?}: [1, 2] counts 2: {two:?}");
        assert!(!two.contains(&2) && !two.contains(&4), "{phase:?}: {two:?}");
    }
    fx.shutdown().await;
}

// ----- S6: add_fields -----

#[tokio::test]
async fn add_fields_is_not_backfilled_and_is_lenient() {
    let fx = Fx::start().await;
    fx.create("idx", schema(vec![], vec![])).await;
    fx.upsert("idx", vec![doc(1, json!({"k": 5}))]).await;
    // Doc 1 reaches a split under schema version 1.
    fx.indexed("idx").await;
    let spec = FieldSpec {
        name: "payload_index.k".to_string(),
        source_path: "k".to_string(),
        kind: FieldKind::I64,
        indexed: true,
        fast: true,
        ignore_malformed: true,
    };
    let schema = fx
        .svc
        .add_fields(NS, "idx", vec![spec.clone()], vec![], BTreeMap::new())
        .await
        .expect("add fields");
    assert!(
        schema.fields.contains(&spec),
        "returned at once: {schema:?}"
    );
    assert_eq!(schema.version, 2);
    // A value of the wrong type is skipped and the write succeeds.
    fx.upsert(
        "idx",
        vec![doc(2, json!({"k": 7})), doc(3, json!({"k": "seven"}))],
    )
    .await;
    let select = Projection {
        source: SourceFilter::None,
        vectors: vec![],
        fields: vec!["payload_index.k".to_string()],
    };
    for phase in PHASES {
        fx.enter("idx", phase).await;
        assert_eq!(
            fx.matching("idx", term("payload_index.k", FieldValue::I64(5)))
                .await,
            set(&[]),
            "{phase:?}: doc 1's split is not backfilled"
        );
        assert_eq!(
            fx.matching("idx", term("payload_index.k", FieldValue::I64(7)))
                .await,
            set(&[2]),
            "{phase:?}"
        );
        let docs = fx
            .svc
            .get(
                NS,
                "idx",
                &[PrimaryKey::U64(1), PrimaryKey::U64(2), PrimaryKey::U64(3)],
                &select,
                ReadConsistency::Strong,
            )
            .await
            .expect("get");
        let values = |i: usize| {
            docs[i]
                .as_ref()
                .expect("present")
                .fields
                .get("payload_index.k")
                .cloned()
                .unwrap_or_default()
        };
        // `StoredDoc.fields` is extracted from `_source` under the current
        // schema at read time (M1.2 Task 7 rule 10), so doc 1 shows the
        // value its split never indexed.
        assert_eq!(values(0), vec![FieldValue::I64(5)], "{phase:?}");
        assert_eq!(values(1), vec![FieldValue::I64(7)], "{phase:?}");
        assert_eq!(values(2), vec![], "{phase:?}: skipped as malformed");
        // The catch-all field still sees every document (Ruling 5).
        assert_eq!(
            fx.matching("idx", term("payload.k", FieldValue::I64(5)))
                .await,
            set(&[1]),
            "{phase:?}"
        );
    }
    fx.shutdown().await;
}

// ----- S7: namespaces -----

#[tokio::test]
async fn create_collection_creates_the_namespace() {
    let fx = Fx::start().await;
    let absent = "absent-ns";
    assert!(
        fx.svc
            .list_collections(absent)
            .await
            .expect("list")
            .is_empty(),
        "an absent namespace lists nothing"
    );
    match fx.svc.get_collection(absent, "c").await {
        Err(ServiceError::NotFound { kind, .. }) => assert_eq!(kind, "collection"),
        other => panic!("expected a missing collection, got {other:?}"),
    }
    assert!(!fx.svc.drop_collection(absent, "c").await.expect("drop"));
    assert!(
        !fx.svc
            .namespace_names()
            .await
            .expect("names")
            .iter()
            .any(|n| n == absent)
    );
    // Creating a collection creates the namespace.
    let info = fx
        .svc
        .create_collection("fresh-ns", "c", schema(vec![], vec![]), None)
        .await
        .expect("create in a new namespace");
    assert_eq!(info.name, "c");
    assert!(
        fx.svc
            .namespace_names()
            .await
            .expect("names")
            .iter()
            .any(|n| n == "fresh-ns")
    );
    assert_eq!(
        fx.svc
            .count("fresh-ns", "c", None, ReadConsistency::Strong)
            .await
            .expect("count"),
        0
    );
    fx.shutdown().await;
}

// ----- S8: scroll and get -----

#[tokio::test]
async fn scroll_after_is_exclusive() {
    let fx = Fx::start().await;
    fx.create("scroll", schema(vec![], vec![])).await;
    let keys = vec![
        PrimaryKey::Str("y".to_string()),
        PrimaryKey::U64(300),
        PrimaryKey::Uuid([0xaa; 16]),
        PrimaryKey::U64(1),
        PrimaryKey::Str("x".to_string()),
        PrimaryKey::U64(5),
        PrimaryKey::Uuid([0x0b; 16]),
    ];
    let make = |pk: &PrimaryKey| doc(pk.clone(), json!({"t": 1}));
    fx.upsert("scroll", keys[..4].iter().map(make).collect())
        .await;
    fx.indexed("scroll").await;
    fx.upsert("scroll", keys[4..].iter().map(make).collect())
        .await;
    let mut sorted = keys.clone();
    sorted.sort_by_key(PrimaryKey::canonical);
    let select = Projection::default();
    for phase in PHASES {
        fx.enter("scroll", phase).await;
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let (page, _) = fx
                .svc
                .scroll(
                    NS,
                    "scroll",
                    None,
                    after.clone(),
                    3,
                    &select,
                    ReadConsistency::Strong,
                )
                .await
                .expect("scroll");
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 3);
            if let Some(after) = &after {
                assert!(page.iter().all(|d| d.pk > *after), "{phase:?}: exclusive");
            }
            after = page.last().map(|d| d.pk.clone());
            seen.extend(page.into_iter().map(|d| d.pk));
        }
        assert_eq!(seen, sorted, "{phase:?}: every key once, in PK order");
        // An absent key starts the page at the next present one.
        let (page, _) = fx
            .svc
            .scroll(
                NS,
                "scroll",
                None,
                Some(PrimaryKey::U64(2)),
                2,
                &select,
                ReadConsistency::Strong,
            )
            .await
            .expect("scroll");
        let pks: Vec<PrimaryKey> = page.into_iter().map(|d| d.pk).collect();
        assert_eq!(pks, vec![PrimaryKey::U64(5), PrimaryKey::U64(300)]);
        // get: one entry per requested key, in request order.
        let wanted = [
            PrimaryKey::Str("y".to_string()),
            PrimaryKey::U64(999),
            PrimaryKey::U64(1),
            PrimaryKey::Uuid([0xaa; 16]),
        ];
        let docs = fx
            .svc
            .get(NS, "scroll", &wanted, &select, ReadConsistency::Strong)
            .await
            .expect("get");
        assert_eq!(docs.len(), wanted.len());
        for (want, got) in wanted.iter().zip(&docs) {
            match got {
                Some(d) => assert_eq!(&d.pk, want, "{phase:?}"),
                None => assert_eq!(want, &PrimaryKey::U64(999), "{phase:?}"),
            }
        }
    }
    fx.shutdown().await;
}

// ----- S9, S10: rescore and projections -----

/// `a` and `b` as in [`fusion_fixture`], and a Cosine vector `z` that is
/// zero for doc 1.
async fn rescore_fixture(fx: &Fx, name: &str) {
    fx.create(
        name,
        schema(
            vec![
                dense("a", 2, Distance::Dot),
                dense("b", 2, Distance::Dot),
                dense("z", 2, Distance::Cosine),
            ],
            vec![],
        ),
    )
    .await;
    let docs = (1..=6u64)
        .map(|i| {
            let d = doc(i, json!({"i": i}));
            let d = with_dense(d, "a", &[i as f32, 0.0]);
            let d = with_dense(d, "b", &[0.0, ((i * 5) % 7) as f32]);
            let z = if i == 1 { [0.0, 0.0] } else { [1.0, i as f32] };
            with_dense(d, "z", &z)
        })
        .collect();
    fx.upsert(name, docs).await;
}

fn rescore(input: Retriever, field: &str, query: &[f32], k: usize) -> Retriever {
    Retriever::Rescore {
        input: Box::new(input),
        field: field.to_string(),
        query: query.to_vec(),
        k,
    }
}

#[tokio::test]
async fn rescore_scores_candidates_exactly() {
    let fx = Fx::start().await;
    rescore_fixture(&fx, "rescore").await;
    for phase in PHASES {
        fx.enter("rescore", phase).await;
        // The input's candidates are docs 6, 5 and 4; their b scores are
        // 2, 4 and 6. Doc 3 (b = 1) and the others are not candidates.
        let hits = fx
            .search(request(
                "rescore",
                vec![rescore(knn("a", &[1.0, 0.0], 3), "b", &[0.0, 1.0], 3)],
                None,
            ))
            .await;
        let want = vec![
            (PrimaryKey::U64(4), 6.0),
            (PrimaryKey::U64(5), 4.0),
            (PrimaryKey::U64(6), 2.0),
        ];
        assert_eq!(hits, want, "{phase:?}");
        let top = fx
            .search(request(
                "rescore",
                vec![rescore(knn("a", &[1.0, 0.0], 3), "b", &[0.0, 1.0], 2)],
                None,
            ))
            .await;
        assert_eq!(top, want[..2].to_vec(), "{phase:?}: k bounds the output");
        // A Cosine score with a zero vector is 0.0 or NaN, never an error.
        for query in [[0.0, 0.0], [1.0, 0.0]] {
            let hits = fx
                .search(request(
                    "rescore",
                    vec![rescore(knn("a", &[1.0, 0.0], 6), "z", &query, 6)],
                    None,
                ))
                .await;
            assert_eq!(hits.len(), 6, "{phase:?}");
            for (pk, score) in &hits {
                if query == [0.0, 0.0] || *pk == PrimaryKey::U64(1) {
                    assert!(*score == 0.0 || score.is_nan(), "{phase:?}: {pk:?} {score}");
                }
            }
        }
    }
    fx.shutdown().await;
}

#[tokio::test]
async fn hits_carry_requested_vectors_and_source() {
    let fx = Fx::start().await;
    rescore_fixture(&fx, "proj").await;
    let cases = [
        (SourceFilter::All, vec!["b"]),
        (SourceFilter::All, vec!["a", "z"]),
        (SourceFilter::None, vec![]),
    ];
    for phase in PHASES {
        fx.enter("proj", phase).await;
        for (source, vectors) in &cases {
            let mut req = request("proj", vec![knn("a", &[1.0, 0.0], 3)], None);
            req.select = Projection {
                source: source.clone(),
                vectors: vectors.iter().map(|v| v.to_string()).collect(),
                fields: vec![],
            };
            let response = fx.svc.search(NS, req).await.expect("search");
            assert_eq!(response.hits.len(), 3);
            for hit in &response.hits {
                let names: Vec<&str> = hit.vectors.keys().map(String::as_str).collect();
                assert_eq!(&names, vectors, "{phase:?}");
                match source {
                    SourceFilter::All => {
                        let source = hit.source.as_ref().expect("source");
                        assert!(source.contains_key("i"), "{phase:?}: {source:?}");
                    }
                    _ => assert_eq!(hit.source, None, "{phase:?}"),
                }
            }
        }
    }
    fx.shutdown().await;
}

// ----- S11: text and dates on JSON paths -----

#[tokio::test]
async fn json_paths_support_text_and_date_conditions() {
    let fx = Fx::start().await;
    fx.create("text", schema(vec![], vec![])).await;
    fx.upsert(
        "text",
        vec![
            doc(1, json!({"body": "The quick brown fox"})),
            doc(2, json!({"body": "brown quick"})),
            doc(3, json!({"when": "2024-01-02"})),
            doc(4, json!({"when": "yesterday"})),
            doc(5, json!({"when": "2023-12-31T23:00:00Z"})),
        ],
    )
    .await;
    let matches = |text: &str, operator: BoolOperator| Query::Match {
        field: "payload.body".to_string(),
        text: text.to_string(),
        operator,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    };
    let phrase = |text: &str| Query::MatchPhrase {
        field: "payload.body".to_string(),
        text: text.to_string(),
        slop: 0,
    };
    // 2024-01-01T00:00:00Z in µs.
    let new_year = FieldValue::Date(1_704_067_200_000_000);
    for phase in PHASES {
        fx.enter("text", phase).await;
        let hits = |query: Query| fx.matching("text", query);
        assert_eq!(
            hits(matches("Quick FOX", BoolOperator::Or)).await,
            set(&[1, 2]),
            "{phase:?}: standard analyzer lowercases"
        );
        assert_eq!(
            hits(matches("Quick FOX", BoolOperator::And)).await,
            set(&[1]),
            "{phase:?}"
        );
        assert_eq!(hits(phrase("quick brown")).await, set(&[1]), "{phase:?}");
        assert_eq!(hits(phrase("brown quick")).await, set(&[2]), "{phase:?}");
        assert_eq!(
            hits(range("payload.when", Some(new_year.clone()), None)).await,
            set(&[3]),
            "{phase:?}: date-like strings only"
        );
        assert_eq!(
            hits(range("payload.when", None, Some(new_year.clone()))).await,
            set(&[5]),
            "{phase:?}"
        );
    }
    fx.shutdown().await;
}

// ----- S12: sparse vectors -----

#[tokio::test]
async fn sparse_retriever_matches_the_reference_scorer() {
    let fx = Fx::start().await;
    fx.create(
        "sparse",
        schema(
            vec![dense("d", 2, Distance::Dot)],
            vec![
                sparse_spec("s", SparseModifier::Idf),
                sparse_spec("t", SparseModifier::None),
            ],
        ),
    )
    .await;
    let s: [(u64, &[(u32, f32)]); 4] = [
        (1, &[(1, 1.0), (2, 0.5)]),
        (2, &[(2, 1.0), (3, 1.0)]),
        (3, &[(3, 2.0)]),
        (4, &[]),
    ];
    let t: [(u64, &[(u32, f32)]); 4] = [
        (1, &[(1, 2.0)]),
        (2, &[(5, 0.0)]),
        (3, &[(1, 1.0), (5, 3.0)]),
        (4, &[(7, 1.0)]),
    ];
    let group = |pk: u64| if pk <= 2 { "x" } else { "y" };
    let d = |pk: u64| [1.0 / pk as f32, pk as f32 / 10.0];
    let mut docs = Vec::new();
    for pk in 1..=5u64 {
        let mut document = with_dense(doc(pk, json!({"g": group(pk)})), "d", &d(pk));
        if let Some((_, pairs)) = s.iter().find(|(k, _)| *k == pk) {
            document = with_sparse(document, "s", pairs);
        }
        if let Some((_, pairs)) = t.iter().find(|(k, _)| *k == pk) {
            document = with_sparse(document, "t", pairs);
        }
        docs.push(document);
    }
    // Doc 6 is deleted: live-only IDF statistics never count it.
    docs.push(with_sparse(
        doc(6, json!({"g": "x"})),
        "s",
        &[(2, 4.0), (3, 4.0)],
    ));
    fx.upsert("sparse", docs).await;
    fx.write("sparse", vec![DocOp::Delete(PrimaryKey::U64(6))])
        .await;
    let everyone = |_: u64| true;
    let group_x = |pk: u64| group(pk) == "x";
    let t_query: &[(u32, f32)] = &[(1, 1.0), (5, 2.0)];
    let s_query: &[(u32, f32)] = &[(2, 1.0), (3, 0.5)];
    let x_filter = term("payload.g", FieldValue::Str("x".into()));
    for phase in PHASES {
        fx.enter("sparse", phase).await;
        // No modifier: plain dot over shared indices; doc 2's stored zero
        // weight is a shared index, so it scores 0 and is returned.
        let hits = fx
            .search(request(
                "sparse",
                vec![sparse("t", t_query, 10, None)],
                None,
            ))
            .await;
        let want = sparse_reference(&corpus(&t, everyone), t_query, false);
        assert!(want.contains_key(&PrimaryKey::U64(2)));
        assert_fused(&hits, &want, 1e-5);
        // IDF over the live documents with a non-empty vector.
        let hits = fx
            .search(request(
                "sparse",
                vec![sparse("s", s_query, 10, None)],
                None,
            ))
            .await;
        assert_fused(
            &hits,
            &sparse_reference(&corpus(&s, everyone), s_query, true),
            1e-5,
        );
        // IDF over `idf_corpus`.
        let hits = fx
            .search(request(
                "sparse",
                vec![sparse("s", s_query, 10, Some(x_filter.clone()))],
                None,
            ))
            .await;
        assert_fused(
            &hits,
            &sparse_reference(&corpus(&s, group_x), s_query, true),
            1e-5,
        );
        // An empty query returns nothing.
        let hits = fx
            .search(request("sparse", vec![sparse("t", &[], 10, None)], None))
            .await;
        assert!(hits.is_empty(), "{phase:?}: {hits:?}");
        // Fusion with a dense retriever, under RRF and DBSF.
        let inputs = || vec![knn("d", &[1.0, 0.0], 5), sparse("t", t_query, 5, None)];
        let mut lists = Vec::new();
        for input in inputs() {
            lists.push(fx.search(request("sparse", vec![input], None)).await);
        }
        let rrf = fx
            .search(fused("sparse", inputs(), Fusion::Rrf { k: 1 }))
            .await;
        assert_fused(&rrf, &qdrant_rrf(&lists, 2), 1e-6);
        let dbsf = fx.search(fused("sparse", inputs(), Fusion::Dbsf)).await;
        assert_fused(&dbsf, &qdrant_dbsf(&lists), 1e-5);
        // Projections fill `Hit.sparse_vectors` and `StoredDoc.sparse_vectors`.
        let select = Projection {
            source: SourceFilter::None,
            vectors: vec!["s".to_string(), "t".to_string(), "d".to_string()],
            fields: vec![],
        };
        let mut req = request("sparse", vec![sparse("t", t_query, 10, None)], None);
        req.select = select.clone();
        let response = fx.svc.search(NS, req).await.expect("search");
        let one = response
            .hits
            .iter()
            .find(|hit| hit.pk == PrimaryKey::U64(1))
            .expect("doc 1");
        assert_eq!(one.sparse_vectors.get("s"), Some(&sv(s[0].1)), "{phase:?}");
        assert_eq!(one.sparse_vectors.get("t"), Some(&sv(t[0].1)), "{phase:?}");
        assert_eq!(one.vectors.get("d"), Some(&d(1).to_vec()), "{phase:?}");
        let docs = fx
            .svc
            .get(
                NS,
                "sparse",
                &[PrimaryKey::U64(3)],
                &select,
                ReadConsistency::Strong,
            )
            .await
            .expect("get");
        let three = docs[0].as_ref().expect("doc 3");
        assert_eq!(
            three.sparse_vectors.get("s"),
            Some(&sv(s[2].1)),
            "{phase:?}"
        );
        assert_eq!(
            three.sparse_vectors.get("t"),
            Some(&sv(t[2].1)),
            "{phase:?}"
        );
    }
    fx.shutdown().await;
}
