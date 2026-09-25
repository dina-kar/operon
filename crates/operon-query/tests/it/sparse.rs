//! Exact sparse vector search (plan M1.2 Task 6 rule 7; Ruling 21,
//! overview A29): `SparseExec` over the splits and the tail, with live-only
//! IDF statistics.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::{TailFixture, field, vector};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use operon_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, PrimaryKey, SparseModifier,
    SparseVector, SparseVectorSpec, sparse_postings_field, sparse_weights_field, split_path,
};
use operon_common::{CollectionId, NamespaceId};
use operon_query::exec::{AnnExec, FilterBitmapExec, Ranked, RowSet, SparseExec, batch_to_ranked};
use operon_query::hot::{HotAnn, HotKind, HotTier, RequestHot};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::sparse::{SparseStats, idf, sparse_score};
use operon_query::tail::TailConfig;
use operon_query::text::{StatsCache, open_splits};
use operon_query::vector::AnnConfig;
use operon_query::{AnnParams, FieldValue, Query, ReadConsistency, ServiceError};
use operon_quickwit::doc_mapper::{FastFieldWarmupInfo, WarmupInfo};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value, json};

// ----- fixtures -----

/// A sparse index nothing random draws: the deleted doc's own index.
const DELETED_INDEX: u32 = 10_000;
/// The index of the doc updated five times.
const UPDATED_INDEX: u32 = 10_001;
/// The doc deleted in the tail, the doc updated five times and a doc
/// whose `s` is empty.
const DELETED_DOC: u64 = 5_000;
const UPDATED_DOC: u64 = 5_001;
const EMPTY_DOC: u64 = 5_002;

/// `tag` Keyword; sparse `s` (Idf) and `t` (None); dense `d` (dim 2).
fn sparse_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword)],
        vec![vector("d", 2)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![
        SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::Idf,
        },
        SparseVectorSpec {
            name: "t".to_string(),
            modifier: SparseModifier::None,
        },
    ]);
    schema.validate().expect("valid schema");
    schema
}

/// Zipf(1.1) over 500 indices.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new() -> Self {
        let mut cdf = Vec::with_capacity(500);
        let mut total = 0.0;
        for rank in 1..=500 {
            total += 1.0 / f64::powf(f64::from(rank), 1.1);
            cdf.push(total);
        }
        for value in &mut cdf {
            *value /= total;
        }
        Self { cdf }
    }

    fn index(&self, rng: &mut ChaCha8Rng) -> u32 {
        let x: f64 = rng.random();
        self.cdf.partition_point(|c| *c < x).min(499) as u32
    }

    /// `min..=max` distinct indices, values in (0, 1], about 5 % zeros.
    fn vector(&self, rng: &mut ChaCha8Rng, min: usize, max: usize) -> SparseVector {
        let n = rng.random_range(min..=max);
        let mut indices = BTreeSet::new();
        while indices.len() < n {
            indices.insert(self.index(rng));
        }
        let values = indices
            .iter()
            .map(|_| {
                if rng.random_bool(0.05) {
                    0.0
                } else {
                    rng.random_range(0.01f32..1.0)
                }
            })
            .collect();
        SparseVector::new(indices.into_iter().collect(), values).expect("valid")
    }
}

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("valid")
}

fn doc(pk: u64, tag: &str, s: Option<SparseVector>, t: Option<SparseVector>) -> DocOp {
    let Value::Object(source) = json!({ "tag": tag }) else {
        unreachable!()
    };
    let mut sparse_vectors = BTreeMap::new();
    if let Some(s) = s {
        sparse_vectors.insert("s".to_string(), s);
    }
    if let Some(t) = t {
        sparse_vectors.insert("t".to_string(), t);
    }
    DocOp::Upsert(Document {
        pk: PrimaryKey::U64(pk),
        source,
        vectors: BTreeMap::new(),
        sparse_vectors,
    })
}

fn tag_of(pk: u64) -> &'static str {
    ["a", "b", "c"][(pk % 3) as usize]
}

/// A random doc: 5 % without `s`, 2 % with an empty `s`.
fn random_doc(zipf: &Zipf, rng: &mut ChaCha8Rng, pk: u64) -> DocOp {
    let roll: f64 = rng.random();
    let s = if roll < 0.05 {
        None
    } else if roll < 0.07 {
        Some(sparse(&[], &[]))
    } else {
        Some(zipf.vector(rng, 1, 30))
    };
    let t = Some(zipf.vector(rng, 1, 30));
    doc(pk, tag_of(pk), s, t)
}

fn updated_version(version: u32) -> DocOp {
    doc(
        UPDATED_DOC,
        "a",
        Some(sparse(&[3, UPDATED_INDEX], &[0.5, 1.0 + version as f32])),
        None,
    )
}

/// The Task 6 sparse corpus: 2 000 docs over 3 splits plus a tail; 100
/// updated (50 durably, 50 in the tail), 40 deleted (20 and 20), and the
/// special docs.
async fn sparse_fixture() -> TailFixture {
    let fixture = TailFixture::start(sparse_schema(), 2).await;
    let zipf = Zipf::new();
    let mut rng = ChaCha8Rng::seed_from_u64(21);
    let docs: Vec<DocOp> = (0..2_000)
        .map(|pk| random_doc(&zipf, &mut rng, pk))
        .collect();
    let delete = |pk: u64| DocOp::Delete(PrimaryKey::U64(pk));

    let mut c1: Vec<DocOp> = docs[..700].to_vec();
    c1.push(doc(
        DELETED_DOC,
        "a",
        Some(sparse(&[3, DELETED_INDEX], &[0.5, 2.0])),
        None,
    ));
    c1.push(doc(EMPTY_DOC, "a", Some(sparse(&[], &[])), None));
    c1.push(updated_version(0));
    let mut c2: Vec<DocOp> = docs[700..1_400].to_vec();
    c2.extend((0..50).map(|pk| random_doc(&zipf, &mut rng, pk)));
    c2.extend((100..120).map(delete));
    c2.push(updated_version(1));
    let mut c3: Vec<DocOp> = docs[1_400..1_800].to_vec();
    c3.push(updated_version(2));
    for commit in [c1, c2, c3] {
        fixture.append_all(&commit).await;
        fixture.apply_link().await;
    }
    let mut tail: Vec<DocOp> = docs[1_800..].to_vec();
    tail.extend((200..250).map(|pk| random_doc(&zipf, &mut rng, pk)));
    tail.extend((300..320).map(delete));
    tail.push(delete(DELETED_DOC));
    for version in 3..5 {
        tail.push(updated_version(version));
    }
    fixture.append_all(&tail).await;
    fixture.append(&updated_version(5)).await;
    fixture
}

fn reads(fixture: &TailFixture) -> Reads {
    fixture.reads(TailConfig::default(), ReadConfig::default())
}

async fn strong_view(fixture: &TailFixture, reads: &Reads) -> Arc<ReadView> {
    Arc::new(
        fixture
            .view(reads, &ReadConsistency::Strong)
            .await
            .expect("view"),
    )
}

/// One live doc of the view: its key, source and sparse vectors.
#[derive(Clone, Debug)]
struct Live {
    pk: PrimaryKey,
    source: Map<String, Value>,
    sparse: BTreeMap<String, SparseVector>,
}

async fn live_docs(view: &ReadView) -> Vec<Live> {
    let mut out = Vec::new();
    for stored in view.snapshot.scan_all().await.expect("scan") {
        if !view.is_shadowed(stored.row_id) {
            out.push(Live {
                pk: stored.pk,
                source: stored.source,
                sparse: stored.sparse_vectors,
            });
        }
    }
    for doc in view.tail.live_docs() {
        let document = doc.doc.as_ref().expect("live");
        out.push(Live {
            pk: doc.pk.clone(),
            source: document.source.clone(),
            sparse: document.sparse_vectors.clone(),
        });
    }
    out
}

/// Live-doc statistics of `field` over the docs `corpus` keeps.
fn brute_stats(docs: &[Live], field: &str, corpus: &dyn Fn(&Live) -> bool) -> SparseStats {
    let mut n = 0;
    let mut df: BTreeMap<u32, u64> = BTreeMap::new();
    for live in docs.iter().filter(|d| corpus(d)) {
        let Some(v) = live.sparse.get(field).filter(|v| !v.is_empty()) else {
            continue;
        };
        n += 1;
        for index in v.indices() {
            *df.entry(*index).or_default() += 1;
        }
    }
    SparseStats { n, df }
}

/// The in-test brute force: (key, score bits) of the best `k` over `docs`
/// (those `allowed` keeps), the query weighted by `stats` when given.
fn brute(
    docs: &[Live],
    field: &str,
    query: &SparseVector,
    stats: Option<&SparseStats>,
    k: usize,
    allowed: &dyn Fn(&Live) -> bool,
) -> Vec<(PrimaryKey, u32)> {
    let query = match stats {
        Some(stats) => {
            let values: Vec<f32> = query
                .indices()
                .iter()
                .zip(query.values())
                .map(|(i, v)| v * idf(stats.n, stats.df.get(i).copied().unwrap_or(0)))
                .collect();
            sparse(query.indices(), &values)
        }
        None => query.clone(),
    };
    let mut scored: Vec<(f32, PrimaryKey)> = docs
        .iter()
        .filter(|d| allowed(d))
        .filter_map(|d| {
            let v = d.sparse.get(field)?;
            Some((sparse_score(&query, v)?, d.pk.clone()))
        })
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(s, pk)| (pk, s.to_bits()))
        .collect()
}

fn bits(hits: &[Ranked]) -> Vec<(PrimaryKey, u32)> {
    hits.iter()
        .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
        .collect()
}

fn exec(
    view: &Arc<ReadView>,
    field: &str,
    query: SparseVector,
    k: usize,
    filter: Option<Query>,
    corpus: Option<Query>,
) -> SparseExec {
    let bitmap = |query: Query| Arc::new(FilterBitmapExec::new(view.clone(), query, 8));
    SparseExec::new(
        view.clone(),
        field.to_string(),
        query,
        k,
        filter.map(bitmap),
        corpus.map(bitmap),
        StatsCache::new(100_000),
        8,
    )
}

async fn search(exec: &SparseExec) -> Vec<Ranked> {
    exec.search().await.expect("search")
}

fn tag(value: &str) -> Query {
    Query::Term {
        field: "tag".to_string(),
        value: FieldValue::Str(value.to_string()),
    }
}

/// Warms the whole `_sparse.s` postings and the columns sparse search
/// reads.
fn warm_sparse(schema: &tantivy::schema::Schema) -> Result<WarmupInfo, ServiceError> {
    let mut info = WarmupInfo::default();
    if let Ok(field) = schema.get_field(&sparse_postings_field("s")) {
        info.term_dict_fields.insert(field);
    }
    for name in ["_rowid", "_pk", sparse_weights_field("s").as_str()] {
        if schema.get_field(name).is_ok() {
            info.fast_fields.insert(FastFieldWarmupInfo {
                name: name.to_string(),
                with_subfields: false,
            });
        }
    }
    Ok(info)
}

async fn stats_of(view: &ReadView, indices: &[u32], corpus: Option<&RowSet>) -> SparseStats {
    let splits = open_splits(view, &warm_sparse).await.expect("splits");
    SparseStats::compute(view, &splits, "s", indices, corpus, &StatsCache::new(1_000))
        .await
        .expect("stats")
}

// ----- the kernel -----

#[test]
fn sparse_score_matches_qdrant() {
    assert_eq!(
        sparse_score(&sparse(&[1, 5], &[2.0, 0.0]), &sparse(&[5, 9], &[3.0, 1.0])),
        Some(0.0)
    );
    assert_eq!(
        sparse_score(&sparse(&[1], &[1.0]), &sparse(&[2], &[1.0])),
        None
    );
    assert_eq!(
        sparse_score(
            &sparse(&[1, 2, 7], &[0.5, 2.0, 3.0]),
            &sparse(&[2, 7], &[4.0, 0.25])
        ),
        Some(2.0f32 * 4.0 + 3.0 * 0.25)
    );
    let expected = ((10.0f32 - 3.0 + 0.5) / (3.0 + 0.5) + 1.0).ln();
    assert_eq!(idf(10, 3).to_bits(), expected.to_bits());
    assert_eq!(idf(0, 0), 2.0f32.ln());
}

// ----- search -----

#[tokio::test]
async fn sparse_search_equals_brute_force_over_live_docs() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    assert!(view.snapshot.splits().len() >= 3);
    assert!(view.tail.live_count() > 0);
    assert!(!view.tail.shadow().is_empty());
    let docs = live_docs(&view).await;
    let zipf = Zipf::new();
    let mut rng = ChaCha8Rng::seed_from_u64(22);
    for round in 0..30 {
        let field = if round % 2 == 0 { "s" } else { "t" };
        let query = zipf.vector(&mut rng, 1, 10);
        let stats = (field == "s").then(|| brute_stats(&docs, "s", &|_| true));
        for k in [1, 10, 100] {
            let hits = search(&exec(&view, field, query.clone(), k, None, None)).await;
            assert_eq!(
                bits(&hits),
                brute(&docs, field, &query, stats.as_ref(), k, &|_| true),
                "{field} {query:?} k={k}"
            );
        }
    }
    // The operator protocol: one ranked batch through DataFusion.
    let query = zipf.vector(&mut rng, 3, 8);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(exec(&view, "s", query.clone(), 10, None, None));
    let batches =
        datafusion::physical_plan::collect(plan.clone(), Arc::new(TaskContext::default()))
            .await
            .expect("collect");
    let through: Vec<Ranked> = batches
        .iter()
        .flat_map(|batch| batch_to_ranked(batch).expect("ranked"))
        .collect();
    let stats = brute_stats(&docs, "s", &|_| true);
    assert_eq!(
        bits(&through),
        brute(&docs, "s", &query, Some(&stats), 10, &|_| true)
    );
    assert_eq!(plan.name(), "SparseExec");
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn idf_statistics_count_live_docs_only() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let docs = live_docs(&view).await;
    let mut indices: Vec<u32> = (0..500).collect();
    indices.extend([DELETED_INDEX, UPDATED_INDEX]);
    let stats = stats_of(&view, &indices, None).await;
    // The doc updated five times counts once; the deleted doc and the doc
    // with an empty vector count 0.
    assert_eq!(stats.df[&UPDATED_INDEX], 1);
    assert_eq!(stats.df[&DELETED_INDEX], 0);
    let expected = brute_stats(&docs, "s", &|_| true);
    assert_eq!(stats.n, expected.n);
    assert!(docs.iter().any(|d| d.pk == PrimaryKey::U64(EMPTY_DOC)));
    for index in &indices {
        assert_eq!(
            stats.df[index],
            expected.df.get(index).copied().unwrap_or(0),
            "df({index})"
        );
    }

    // A single-split rebuild of the same live docs has the same statistics.
    let rebuilt = TailFixture::start(sparse_schema(), 2).await;
    let ops: Vec<DocOp> = docs
        .iter()
        .map(|d| {
            DocOp::Upsert(Document {
                pk: d.pk.clone(),
                source: d.source.clone(),
                vectors: BTreeMap::new(),
                sparse_vectors: d.sparse.clone(),
            })
        })
        .collect();
    rebuilt.append_all(&ops).await;
    rebuilt.apply_link().await;
    let rebuilt_reads = self::reads(&rebuilt);
    let one = strong_view(&rebuilt, &rebuilt_reads).await;
    assert_eq!(one.snapshot.splits().len(), 1);
    assert_eq!(one.tail.live_count(), 0);
    assert_eq!(stats_of(&one, &indices, None).await, stats);
    rebuilt_reads.shutdown().await;
    rebuilt.shutdown().await;
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn docs_without_a_shared_index_are_never_returned() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    for field in ["s", "t"] {
        let query = sparse(&[20_000, 20_001], &[1.0, 1.0]);
        assert!(
            search(&exec(&view, field, query, 100, None, None))
                .await
                .is_empty()
        );
    }
    // Only docs sharing an index come back, even when k exceeds them.
    let query = sparse(&[DELETED_INDEX, UPDATED_INDEX], &[1.0, 1.0]);
    let hits = search(&exec(&view, "s", query, 100, None, None)).await;
    assert_eq!(
        hits.iter().map(|h| h.pk.clone()).collect::<Vec<_>>(),
        vec![PrimaryKey::U64(UPDATED_DOC)]
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_empty_query_returns_nothing_and_opens_no_split() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let prefixes: Vec<String> = view
        .snapshot
        .splits()
        .iter()
        .map(|split| split_path(fixture.ns, fixture.cid, split.ulid))
        .collect();
    let gets = || -> u64 { prefixes.iter().map(|p| fixture.paths.gets_under(p)).sum() };
    let before = gets();
    for field in ["s", "t"] {
        let hits = search(&exec(&view, field, sparse(&[], &[]), 10, None, None)).await;
        assert!(hits.is_empty());
    }
    assert_eq!(gets(), before, "no split was read");
    // A non-empty query does read them.
    search(&exec(&view, "s", sparse(&[3], &[1.0]), 10, None, None)).await;
    assert!(gets() > before);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn idf_corpus_narrows_the_statistics() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let docs = live_docs(&view).await;
    let tagged = |value: &'static str| {
        move |d: &Live| d.source.get("tag").and_then(Value::as_str) == Some(value)
    };
    let corpus_stats = brute_stats(&docs, "s", &tagged("a"));
    let all_stats = brute_stats(&docs, "s", &|_| true);
    let zipf = Zipf::new();
    let mut rng = ChaCha8Rng::seed_from_u64(23);
    let mut changed = false;
    for _ in 0..10 {
        let query = zipf.vector(&mut rng, 2, 8);
        let hits = search(&exec(&view, "s", query.clone(), 20, None, Some(tag("a")))).await;
        let expected = brute(&docs, "s", &query, Some(&corpus_stats), 20, &|_| true);
        assert_eq!(bits(&hits), expected, "{query:?}");
        changed |= expected != brute(&docs, "s", &query, Some(&all_stats), 20, &|_| true);
    }
    assert!(changed, "the corpus changes some scores");

    // The statistics themselves, and a corpus matching nothing.
    let indices: Vec<u32> = (0..50).collect();
    let rows = FilterBitmapExec::new(view.clone(), tag("a"), 8)
        .rows()
        .await
        .expect("rows");
    let stats = stats_of(&view, &indices, Some(&rows)).await;
    assert_eq!(stats.n, corpus_stats.n);
    for index in &indices {
        assert_eq!(
            stats.df[index],
            corpus_stats.df.get(index).copied().unwrap_or(0)
        );
    }
    let nothing = FilterBitmapExec::new(view.clone(), tag("zzz"), 8)
        .rows()
        .await
        .expect("rows");
    assert_eq!(nothing, RowSet::Rows(Default::default()));
    let stats = stats_of(&view, &indices, Some(&nothing)).await;
    assert_eq!(stats.n, 0);
    assert!(stats.df.values().all(|df| *df == 0));
    assert_eq!(stats.df.len(), indices.len());

    // `idf_corpus` needs the Idf modifier.
    let err = exec(&view, "t", sparse(&[3], &[1.0]), 10, None, Some(tag("a")))
        .search()
        .await
        .expect_err("t has no idf modifier");
    assert_eq!(
        err,
        ServiceError::InvalidArgument(
            "idf_corpus needs a sparse vector with the idf modifier".to_string()
        )
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn filters_apply_before_the_top_k() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let docs = live_docs(&view).await;
    let chosen: BTreeSet<PrimaryKey> = [3u64, 17, 250, 404, 777, 1_234, 1_500, 1_801, 1_999, 205]
        .into_iter()
        .map(PrimaryKey::U64)
        .collect();
    let filter = Query::Ids(chosen.iter().cloned().collect());
    let stats = brute_stats(&docs, "s", &|_| true);
    let zipf = Zipf::new();
    let mut rng = ChaCha8Rng::seed_from_u64(24);
    for _ in 0..10 {
        let query = zipf.vector(&mut rng, 5, 20);
        let hits = search(&exec(
            &view,
            "s",
            query.clone(),
            100,
            Some(filter.clone()),
            None,
        ))
        .await;
        assert!(hits.len() <= 10);
        assert!(hits.iter().all(|hit| chosen.contains(&hit.pk)));
        assert_eq!(
            bits(&hits),
            brute(&docs, "s", &query, Some(&stats), 100, &|d| chosen
                .contains(&d.pk))
        );
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn shadowed_and_deleted_docs_never_score_or_count() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let docs = live_docs(&view).await;
    let deleted: BTreeSet<PrimaryKey> = (100..120)
        .chain(300..320)
        .chain([DELETED_DOC])
        .map(PrimaryKey::U64)
        .collect();
    assert!(docs.iter().all(|d| !deleted.contains(&d.pk)));
    // Index 3 is common and every special doc has it.
    let query = sparse(&[3, DELETED_INDEX, UPDATED_INDEX], &[1.0, 1.0, 1.0]);
    let hits = search(&exec(&view, "s", query.clone(), 2_000, None, None)).await;
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|hit| !view.is_shadowed(hit.row_id)));
    assert!(hits.iter().all(|hit| !deleted.contains(&hit.pk)));
    let updated: Vec<&Ranked> = hits
        .iter()
        .filter(|hit| hit.pk == PrimaryKey::U64(UPDATED_DOC))
        .collect();
    assert_eq!(updated.len(), 1, "one version of the updated doc");
    let stats = stats_of(&view, &[3, DELETED_INDEX, UPDATED_INDEX], None).await;
    assert_eq!(stats.df[&DELETED_INDEX], 0);
    assert_eq!(stats.df[&UPDATED_INDEX], 1);
    // Its score is the latest version's: 3 → 0.5, UPDATED_INDEX → 6.0.
    let latest = sparse(&[3, UPDATED_INDEX], &[0.5, 6.0]);
    let weighted = stats.weigh(&query);
    assert_eq!(
        updated[0].score.to_bits(),
        sparse_score(&weighted, &latest).expect("shared").to_bits()
    );
    let expected = brute_stats(&docs, "s", &|_| true);
    assert_eq!(
        bits(&hits),
        brute(&docs, "s", &query, Some(&expected), 2_000, &|_| true)
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_dense_name_in_a_sparse_retriever_is_invalid() {
    let fixture = TailFixture::start(sparse_schema(), 1).await;
    fixture
        .append(&doc(1, "a", Some(sparse(&[1], &[1.0])), None))
        .await;
    let reads = reads(&fixture);
    let view = strong_view(&fixture, &reads).await;
    let invalid = |message: &str| ServiceError::InvalidArgument(message.to_string());
    let err = exec(&view, "d", sparse(&[1], &[1.0]), 10, None, None)
        .search()
        .await
        .expect_err("dense");
    assert_eq!(err, invalid("d is a dense vector"));
    let err = exec(&view, "nope", sparse(&[1], &[1.0]), 10, None, None)
        .search()
        .await
        .expect_err("unknown");
    assert_eq!(err, invalid("unknown sparse vector nope"));
    // The reverse: a sparse name in a dense retriever.
    let err = AnnExec::new(
        view.clone(),
        "s".to_string(),
        vec![1.0, 0.0],
        10,
        AnnParams::default(),
        None,
        AnnConfig::default(),
    )
    .search()
    .await
    .expect_err("sparse");
    assert_eq!(err, invalid("s is a sparse vector"));
    // And the tail alone answers a valid query.
    let hits = search(&exec(&view, "s", sparse(&[1], &[1.0]), 10, None, None)).await;
    assert_eq!(hits.len(), 1);
    reads.shutdown().await;
    fixture.shutdown().await;
}

// ----- the hot tier -----

/// A hot tier pinning local copies of splits and counting `ann` calls.
#[derive(Debug)]
struct LocalSplits {
    files: BTreeMap<ulid::Ulid, PathBuf>,
    ann_calls: Arc<AtomicUsize>,
}

impl HotTier for LocalSplits {
    fn ann(&self, _: NamespaceId, _: CollectionId, _: &str, _: u64) -> Option<Arc<dyn HotAnn>> {
        self.ann_calls.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn split_file(&self, _: NamespaceId, _: CollectionId, split: ulid::Ulid) -> Option<PathBuf> {
        self.files.get(&split).cloned()
    }

    fn record_access(&self, _: NamespaceId, _: CollectionId) {}
}

#[tokio::test]
async fn pinned_split_files_give_identical_sparse_results() {
    let fixture = sparse_fixture().await;
    let reads = reads(&fixture);
    let cold = strong_view(&fixture, &reads).await;
    let dir = tempfile::TempDir::new().expect("temp dir");
    let mut files = BTreeMap::new();
    for split in cold.snapshot.splits() {
        let (bytes, _) = fixture
            .store
            .get(&split_path(fixture.ns, fixture.cid, split.ulid))
            .await
            .expect("split bytes");
        let path = dir.path().join(format!("{}.split", split.ulid));
        std::fs::write(&path, &bytes).expect("write");
        files.insert(split.ulid, path);
    }
    let ann_calls = Arc::new(AtomicUsize::new(0));
    let tier: Arc<dyn HotTier> = Arc::new(LocalSplits {
        files,
        ann_calls: ann_calls.clone(),
    });
    let hot = RequestHot {
        enabled: true,
        used: Default::default(),
    };
    let collection = fixture.collection().await;
    let warm = Arc::new(
        reads
            .view(
                fixture.ns,
                &collection,
                &ReadConsistency::Strong,
                &hot,
                tier,
            )
            .await
            .expect("view"),
    );
    let zipf = Zipf::new();
    let mut rng = ChaCha8Rng::seed_from_u64(25);
    for round in 0..10 {
        let field = if round % 2 == 0 { "s" } else { "t" };
        let query = zipf.vector(&mut rng, 1, 10);
        let corpus = (field == "s" && round % 4 == 0).then(|| tag("b"));
        let cold_hits = search(&exec(&cold, field, query.clone(), 50, None, corpus.clone())).await;
        let warm_hits = search(&exec(&warm, field, query, 50, None, corpus)).await;
        assert!(!cold_hits.is_empty());
        assert_eq!(bits(&cold_hits), bits(&warm_hits));
    }
    assert_eq!(
        warm.hot_used.kinds().into_iter().collect::<Vec<_>>(),
        vec![HotKind::Splits]
    );
    assert!(cold.hot_used.kinds().is_empty());
    assert_eq!(ann_calls.load(Ordering::SeqCst), 0);
    reads.shutdown().await;
    fixture.shutdown().await;
}
