//! Text search over splits and the tail (plan M1.2 Task 5):
//! `TantivySearchExec`, `FilterBitmapExec` and global live-only BM25
//! statistics (R6, Ruling 2).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::{TailFixture, field};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use operon_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, PrimaryKey, split_path,
};
use operon_common::{CollectionId, NamespaceId};
use operon_query::exec::{
    EffectiveSort, FilterBitmapExec, Ranked, RowSet, TantivySearchExec, batch_to_ranked,
};
use operon_query::hot::{HotAnn, HotKind, HotTier, RequestHot};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::TailConfig;
use operon_query::text::{FIELD_NORMS_TABLE, GlobalStats, StatsCache, norm_mid2, open_splits};
use operon_query::{
    BoolOperator, FieldValue, MissingOrder, Query, ReadConsistency, Retriever, SearchRequest,
    ServiceError, SortKey, SortOrder,
};
use operon_quickwit::doc_mapper::WarmupInfo;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

// ----- fixtures -----

const VOCABULARY: [&str; 60] = [
    "apple", "river", "stone", "cloud", "table", "green", "light", "north", "piano", "quiet",
    "radio", "sugar", "tiger", "under", "vivid", "water", "yellow", "zebra", "anchor", "bridge",
    "candle", "desert", "engine", "forest", "garden", "harbor", "island", "jungle", "kettle",
    "ladder", "market", "needle", "orange", "pepper", "rabbit", "saddle", "tunnel", "violin",
    "window", "basket", "copper", "dragon", "falcon", "ginger", "hammer", "lemon", "mirror",
    "nickel", "oyster", "pillow", "rocket", "silver", "timber", "velvet", "walnut", "button",
    "carpet", "donkey", "feather", "helmet",
];

/// `body` Text english, `m` absent; `tag` Keyword fast, `n` I64 fast,
/// `payload` Json; unmapped paths are ignored.
fn body_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            field(
                "body",
                FieldKind::Text {
                    analyzer: "english".to_string(),
                    positions: true,
                },
            ),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field("payload", FieldKind::Json),
        ],
        vec![],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

fn doc_of(pk: PrimaryKey, source: Value) -> DocOp {
    let Value::Object(source) = source else {
        panic!("not an object");
    };
    DocOp::Upsert(Document {
        pk,
        source,
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    })
}

fn body(pk: u64, text: &str) -> DocOp {
    doc_of(PrimaryKey::U64(pk), json!({"body": text}))
}

fn delete(pk: u64) -> DocOp {
    DocOp::Delete(PrimaryKey::U64(pk))
}

fn words(rng: &mut ChaCha8Rng, vocabulary: &[&str], n: usize) -> String {
    (0..n)
        .map(|_| *vocabulary.choose(rng).expect("a word"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A query of 1–4 distinct words.
fn random_query(rng: &mut ChaCha8Rng, vocabulary: &[&str]) -> Query {
    let n = rng.random_range(1..=4);
    let mut chosen: Vec<&str> = Vec::new();
    while chosen.len() < n {
        let word = *vocabulary.choose(rng).expect("a word");
        if !chosen.contains(&word) {
            chosen.push(word);
        }
    }
    matching(&chosen.join(" "))
}

fn matching(text: &str) -> Query {
    Query::Match {
        field: "body".to_string(),
        text: text.to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

fn strong() -> ReadConsistency {
    ReadConsistency::Strong
}

async fn view(fixture: &TailFixture, reads: &Reads) -> Arc<ReadView> {
    Arc::new(fixture.view(reads, &strong()).await.expect("view"))
}

fn reads(fixture: &TailFixture) -> Reads {
    fixture.reads(TailConfig::default(), ReadConfig::default())
}

fn exec(view: &Arc<ReadView>, query: Query, k: usize, sort: EffectiveSort) -> TantivySearchExec {
    TantivySearchExec::new(
        view.clone(),
        query,
        None,
        k,
        sort,
        None,
        StatsCache::new(1_000_000),
        8,
    )
}

async fn top(view: &Arc<ReadView>, query: Query, k: usize) -> Vec<Ranked> {
    exec(view, query, k, EffectiveSort::by_score())
        .search()
        .await
        .expect("search")
}

/// (key, score bits) of each hit.
fn bits(hits: &[Ranked]) -> Vec<(PrimaryKey, u32)> {
    hits.iter()
        .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
        .collect()
}

/// The field-mode sort of `keys` (no retriever).
fn field_sort(keys: Vec<SortKey>) -> EffectiveSort {
    EffectiveSort::of(&SearchRequest {
        sort: keys,
        ..SearchRequest::new("docs")
    })
    .expect("sort")
}

/// The english analyzer's tokens of `text`.
fn tokens(text: &str) -> Vec<String> {
    tokens_of("english", text)
}

fn tokens_of(name: &str, text: &str) -> Vec<String> {
    let mut analyzer = operon_text::tokenizer_manager()
        .get(name)
        .expect("analyzer");
    let mut out = Vec::new();
    analyzer
        .token_stream(text)
        .process(&mut |token| out.push(token.text.clone()));
    out
}

fn fieldnorm_id(len: usize) -> u8 {
    let len = u32::try_from(len).expect("short");
    (FIELD_NORMS_TABLE.partition_point(|bound| *bound <= len) - 1) as u8
}

/// The documents of a view: durable rows that are not shadowed, plus the
/// tail's live docs, by row id.
async fn rows_of(view: &ReadView) -> BTreeMap<u64, (PrimaryKey, Value)> {
    let mut out = BTreeMap::new();
    for stored in view.snapshot.scan_all().await.expect("scan") {
        if !view.is_shadowed(stored.row_id) {
            out.insert(stored.row_id, (stored.pk, Value::Object(stored.source)));
        }
    }
    for doc in view.tail.live_docs() {
        let source = doc.doc.as_ref().expect("live").source.clone();
        out.insert(doc.row_id, (doc.pk.clone(), Value::Object(source)));
    }
    out
}

/// Warms every field norm and the text fields' dictionaries and postings,
/// which statistics read.
fn warm_text(schema: &tantivy::schema::Schema) -> Result<WarmupInfo, ServiceError> {
    let term_dict_fields = ["body", "m"]
        .iter()
        .filter_map(|name| schema.get_field(name).ok())
        .collect();
    Ok(WarmupInfo {
        field_norms: true,
        term_dict_fields,
        ..WarmupInfo::default()
    })
}

// ----- global statistics -----

/// The R6 corpus: 200 final documents, `(key, body)`.
fn corpus() -> Vec<(u64, String)> {
    let mut rng = ChaCha8Rng::seed_from_u64(6);
    (0..200)
        .map(|pk| {
            let n = rng.random_range(3..=12);
            (pk, words(&mut rng, &VOCABULARY, n))
        })
        .collect()
}

#[tokio::test]
async fn global_statistics_equal_a_single_split() {
    let final_docs: Vec<DocOp> = corpus().iter().map(|(pk, text)| body(*pk, text)).collect();
    let mut rng = ChaCha8Rng::seed_from_u64(60);
    let queries: Vec<Query> = (0..10)
        .map(|_| random_query(&mut rng, &VOCABULARY))
        .collect();

    // A: the 200 docs in one commit (one split).
    let a = TailFixture::start(body_schema(), 2).await;
    a.append_all(&final_docs).await;
    a.apply_link().await;
    let a_reads = reads(&a);
    let a_view = view(&a, &a_reads).await;
    assert_eq!(a_view.snapshot.splits().len(), 1);
    assert_eq!(a_view.tail.live_count(), 0);

    // B: five commits, 15 docs updated twice before their final value, and
    // 20 docs plus 7 final values never applied.
    let b = TailFixture::start(body_schema(), 2).await;
    let early = |version: &str| -> Vec<DocOp> {
        (0..15)
            .map(|pk| body(pk, &format!("{version} apple river {pk}")))
            .collect()
    };
    let mut c1: Vec<DocOp> = early("first");
    c1.extend(final_docs[15..36].iter().cloned());
    let mut c2: Vec<DocOp> = early("second stone");
    c2.extend(final_docs[36..72].iter().cloned());
    let mut c3: Vec<DocOp> = final_docs[0..8].to_vec();
    c3.extend(final_docs[72..108].iter().cloned());
    for commit in [
        c1,
        c2,
        c3,
        final_docs[108..144].to_vec(),
        final_docs[144..180].to_vec(),
    ] {
        b.append_all(&commit).await;
        b.apply_link().await;
    }
    let mut tail: Vec<DocOp> = final_docs[180..200].to_vec();
    tail.extend(final_docs[8..15].iter().cloned());
    b.append_all(&tail).await;
    let b_reads = reads(&b);
    let b_view = view(&b, &b_reads).await;
    assert!(b_view.snapshot.splits().len() >= 5);
    assert!(
        b_view
            .snapshot
            .splits()
            .iter()
            .any(|split| split.delete_bitmap.is_some())
    );
    assert_eq!(b_view.tail.shadow().len(), 7);
    assert_eq!(b_view.live_rows(), 200);
    assert_eq!(a_view.live_rows(), 200);

    for query in &queries {
        let a_hits = top(&a_view, query.clone(), 50).await;
        let b_hits = top(&b_view, query.clone(), 50).await;
        assert!(!a_hits.is_empty(), "{query:?}");
        assert_eq!(bits(&a_hits), bits(&b_hits), "{query:?}");
    }

    // The operator protocol: one batch of ranked rows through DataFusion.
    let plan: Arc<dyn ExecutionPlan> = Arc::new(exec(
        &b_view,
        queries[0].clone(),
        10,
        EffectiveSort::by_score(),
    ));
    let batches =
        datafusion::physical_plan::collect(plan.clone(), Arc::new(TaskContext::default()))
            .await
            .expect("collect");
    let through_df: Vec<Ranked> = batches
        .iter()
        .flat_map(|batch| batch_to_ranked(batch).expect("ranked"))
        .collect();
    assert_eq!(
        bits(&through_df),
        bits(&top(&b_view, queries[0].clone(), 10).await)
    );
    let metrics = plan.metrics().expect("metrics");
    assert_eq!(metrics.output_rows(), Some(through_df.len()));
    assert_eq!(plan.name(), "TantivySearchExec");

    a_reads.shutdown().await;
    b_reads.shutdown().await;
    a.shutdown().await;
    b.shutdown().await;
}

fn and_matching(text: &str) -> Query {
    Query::Match {
        field: "body".to_string(),
        text: text.to_string(),
        operator: BoolOperator::And,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

/// Plan row 15.1: Tantivy sums a conjunction in per-segment cost order and
/// a disjunction in an order that changes as its scorers run out, so
/// without canonical rescoring these scores differ in their last bits
/// between one split and many.
#[tokio::test]
async fn conjunctions_and_long_disjunctions_score_identically_across_layouts() {
    // A small vocabulary, so conjunctions of 3–4 words match many docs.
    let vocabulary = &VOCABULARY[..10];
    let mut rng = ChaCha8Rng::seed_from_u64(15);
    let final_docs: Vec<DocOp> = (0..240u64)
        .map(|pk| {
            let n = rng.random_range(4..=14);
            doc_of(
                PrimaryKey::U64(pk),
                json!({"body": words(&mut rng, vocabulary, n), "n": pk % 5}),
            )
        })
        .collect();
    let a = TailFixture::start(body_schema(), 2).await;
    a.append_all(&final_docs).await;
    a.apply_link().await;
    let b = TailFixture::start(body_schema(), 2).await;
    let early: Vec<DocOp> = (0..30).map(|pk| body(pk, "apple river stone")).collect();
    b.append_all(&early).await;
    b.apply_link().await;
    for chunk in [&final_docs[..17], &final_docs[17..90], &final_docs[90..150]] {
        b.append_all(chunk).await;
        b.apply_link().await;
    }
    b.append_all(&final_docs[150..]).await;
    let (a_reads, b_reads) = (reads(&a), reads(&b));
    let (a_view, b_view) = (view(&a, &a_reads).await, view(&b, &b_reads).await);
    assert_eq!(a_view.snapshot.splits().len(), 1);
    assert!(b_view.snapshot.splits().len() > 1);
    assert_eq!(b_view.live_rows(), 240);

    let mut queries = Vec::new();
    for _ in 0..12 {
        let n = rng.random_range(3..=4);
        queries.push(and_matching(&words(&mut rng, vocabulary, n)));
        let n = rng.random_range(5..=7);
        queries.push(matching(&words(&mut rng, vocabulary, n)));
    }
    queries.push(Query::Bool {
        must: vec![matching("apple"), matching("river"), matching("stone")],
        should: vec![matching("cloud"), matching("table"), matching("green")],
        must_not: vec![],
        filter: vec![],
        minimum_should_match: None,
    });
    let by_field_then_score = || {
        EffectiveSort::of(&SearchRequest {
            retrievers: vec![Retriever::Text {
                query: Query::MatchAll,
                k: 60,
            }],
            sort: vec![
                SortKey::Field {
                    field: "n".to_string(),
                    order: SortOrder::Asc,
                    missing: MissingOrder::Last,
                },
                SortKey::Score {
                    order: SortOrder::Desc,
                },
            ],
            ..SearchRequest::new("docs")
        })
        .expect("sort")
    };
    for query in &queries {
        let a_hits = top(&a_view, query.clone(), 60).await;
        assert!(!a_hits.is_empty(), "{query:?}");
        assert_eq!(
            bits(&a_hits),
            bits(&top(&b_view, query.clone(), 60).await),
            "{query:?}"
        );
        let field = |view| exec(view, query.clone(), 60, by_field_then_score());
        let a_field = field(&a_view).search().await.expect("search");
        let b_field = field(&b_view).search().await.expect("search");
        assert_eq!(bits(&a_field), bits(&b_field), "field mode {query:?}");
    }
    a_reads.shutdown().await;
    b_reads.shutdown().await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn superseded_versions_do_not_count_in_statistics() {
    let fixture = TailFixture::start(body_schema(), 1).await;
    let others: Vec<DocOp> = (0..10).map(|pk| body(pk, "apple river")).collect();
    fixture.append_all(&others).await;
    for version in 0..5 {
        fixture
            .append(&body(99, &format!("zebra stone v{version}")))
            .await;
        if version < 3 {
            fixture.apply_link().await;
        }
    }
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let splits = open_splits(&view, &warm_text).await.expect("splits");
    let key = ("body".to_string(), b"zebra".to_vec());
    let stats = GlobalStats::compute(
        &view,
        &splits,
        &BTreeSet::from([key.clone()]),
        &BTreeSet::from(["body".to_string()]),
        &StatsCache::new(1_000),
        8,
    )
    .await
    .expect("stats");
    assert_eq!(stats.doc_freq[&key], 1);
    assert_eq!(stats.num_docs, 11);
    // body: 10 × "apple river" + "zebra stone v4", all under 40 tokens.
    let expected = 10 * tokens("apple river").len() + tokens("zebra stone v4").len();
    assert_eq!(stats.tokens["body"], expected as u64);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_split_without_the_field_contributes_no_tokens() {
    let fixture = TailFixture::start(body_schema(), 1).await;
    let before: Vec<DocOp> = (0..5)
        .map(|pk| {
            doc_of(
                PrimaryKey::U64(pk),
                json!({"body": "apple", "m": "one two three four"}),
            )
        })
        .collect();
    fixture.append_all(&before).await;
    fixture.apply_link().await;
    let mut next = body_schema();
    next.fields.push(field(
        "m",
        FieldKind::Text {
            analyzer: "standard".to_string(),
            positions: false,
        },
    ));
    next.version = 2;
    fixture
        .meta
        .client
        .update_collection_schema(fixture.cid, 1, next)
        .await
        .expect("add m");
    let texts = [
        "red",
        "red green",
        "a b c d e",
        "x y z",
        "long text here now",
    ];
    let after: Vec<DocOp> = texts
        .iter()
        .zip(5u64..)
        .map(|(text, pk)| doc_of(PrimaryKey::U64(pk), json!({"body": "river", "m": text})))
        .collect();
    fixture.append_all(&after).await;
    fixture.apply_link().await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let splits = open_splits(&view, &warm_text).await.expect("splits");
    assert_eq!(splits.len(), 2);
    assert!(splits[0].searcher.schema().get_field("m").is_err());
    let stats = GlobalStats::compute(
        &view,
        &splits,
        &BTreeSet::new(),
        &BTreeSet::from(["m".to_string()]),
        &StatsCache::new(1_000),
        8,
    )
    .await
    .expect("stats");
    let expected2: u64 = texts
        .iter()
        .map(|text| norm_mid2(fieldnorm_id(tokens_of("standard", text).len())))
        .sum();
    assert_eq!(stats.tokens["m"], expected2 / 2);
    assert_eq!(stats.tokens["m"], 15);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn bm25_matches_a_reference_implementation() {
    let mut rng = ChaCha8Rng::seed_from_u64(25);
    let vocabulary = &VOCABULARY[..12];
    let texts: Vec<String> = (0..30)
        .map(|_| {
            let n = rng.random_range(1..=45);
            words(&mut rng, vocabulary, n)
        })
        .collect();
    let fixture = TailFixture::start(body_schema(), 2).await;
    // Earlier versions (deleted and shadowed), then the final texts in two
    // commits and the tail.
    let stale: Vec<DocOp> = (0..30).map(|pk| body(pk, "apple apple apple")).collect();
    fixture.append_all(&stale).await;
    fixture.apply_link().await;
    let finals: Vec<DocOp> = texts
        .iter()
        .zip(0u64..)
        .map(|(text, pk)| body(pk, text))
        .collect();
    fixture.append_all(&finals[..20]).await;
    fixture.apply_link().await;
    fixture.append_all(&finals[20..]).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    assert_eq!(view.live_rows(), 30);

    // The reference.
    let analyzed: Vec<Vec<String>> = texts.iter().map(|text| tokens(text)).collect();
    let n = analyzed.len() as f64;
    let sum2: u64 = analyzed
        .iter()
        .map(|t| norm_mid2(fieldnorm_id(t.len())))
        .sum();
    let avgdl = (sum2 / 2) as f64 / n;
    let (k1, b) = (1.2f64, 0.75f64);
    for _ in 0..5 {
        let query = random_query(&mut rng, vocabulary);
        let Query::Match { text, .. } = &query else {
            unreachable!()
        };
        let query_tokens = tokens(text);
        let mut expected: Vec<(PrimaryKey, f64)> = Vec::new();
        for (pk, doc) in analyzed.iter().enumerate() {
            let dl = f64::from(FIELD_NORMS_TABLE[usize::from(fieldnorm_id(doc.len()))]);
            let mut score = 0.0;
            let mut matched = false;
            for token in &query_tokens {
                let tf = doc.iter().filter(|t| *t == token).count() as f64;
                if tf == 0.0 {
                    continue;
                }
                matched = true;
                let df = analyzed.iter().filter(|d| d.contains(token)).count() as f64;
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                score += idf * tf * (k1 + 1.0) / (tf + k1 * (1.0 - b + b * dl / avgdl));
            }
            if matched {
                expected.push((PrimaryKey::U64(pk as u64), score));
            }
        }
        let hits = top(&view, query.clone(), 100).await;
        assert_eq!(hits.len(), expected.len(), "{query:?}");
        let got: BTreeMap<PrimaryKey, f32> =
            hits.iter().map(|hit| (hit.pk.clone(), hit.score)).collect();
        for (pk, score) in expected {
            let actual = f64::from(got[&pk]);
            assert!(
                (actual - score).abs() <= 1e-5 * score.max(1.0),
                "{query:?} {pk:?}: {actual} vs {score}"
            );
        }
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn top_k_equals_exhaustive_scoring() {
    let mut rng = ChaCha8Rng::seed_from_u64(2000);
    let vocabulary = &VOCABULARY[..25];
    let docs: Vec<DocOp> = (0..2000u64)
        .map(|pk| {
            let n = rng.random_range(2..=8);
            body(pk, &words(&mut rng, vocabulary, n))
        })
        .collect();
    let fixture = TailFixture::start(body_schema(), 2).await;
    for chunk in docs[..1800].chunks(600) {
        fixture.append_all(chunk).await;
        fixture.apply_link().await;
    }
    fixture.append_all(&docs[1800..]).await;
    // Updates in the tail shadow durable rows.
    let updates: Vec<DocOp> = (0..50u64)
        .map(|pk| body(pk * 7, &words(&mut rng, vocabulary, 3)))
        .collect();
    fixture.append_all(&updates).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    assert_eq!(view.snapshot.splits().len(), 3);
    assert!(view.tail.live_count() > 0);
    for _ in 0..20 {
        let query = random_query(&mut rng, vocabulary);
        let mut all = top(&view, query.clone(), 10_000).await;
        all.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.pk.cmp(&b.pk)));
        for k in [1, 5, 50] {
            let hits = top(&view, query.clone(), k).await;
            assert_eq!(
                bits(&hits),
                bits(&all[..k.min(all.len())]),
                "{query:?} k={k}"
            );
        }
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn ties_at_the_kth_score_are_kept_and_ordered_by_pk() {
    let fixture = TailFixture::start(body_schema(), 2).await;
    let mut keys: Vec<PrimaryKey> = (0..4u64).map(|n| PrimaryKey::U64(n * 1_000)).collect();
    keys.extend((0..18).map(|i| PrimaryKey::Str(format!("key-{:02}", 17 - i))));
    keys.extend((0..18u8).map(|i| PrimaryKey::Uuid([i.wrapping_mul(37); 16])));
    let ops: Vec<DocOp> = keys
        .iter()
        .map(|pk| doc_of(pk.clone(), json!({"body": "apple river stone"})))
        .collect();
    fixture.append_all(&ops[..25]).await;
    fixture.apply_link().await;
    fixture.append_all(&ops[25..]).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let mut expected = keys.clone();
    expected.sort();
    for query in [matching("apple"), matching("apple river")] {
        let hits = top(&view, query.clone(), 10).await;
        assert_eq!(
            hits.iter().map(|hit| hit.pk.clone()).collect::<Vec<_>>(),
            expected[..10].to_vec(),
            "{query:?}"
        );
        assert!(hits.iter().all(|hit| hit.score == hits[0].score));
        assert!(matches!(hits[0].pk, PrimaryKey::U64(_)));
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn deleted_and_shadowed_docs_never_score_or_count() {
    let fixture = TailFixture::start(body_schema(), 2).await;
    let ops: Vec<DocOp> = (0..40).map(|pk| body(pk, "apple river")).collect();
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    // Deleted through a commit (delete bitmap).
    let deletes: Vec<DocOp> = (0..10).map(delete).collect();
    fixture.append_all(&deletes).await;
    fixture.apply_link().await;
    // Deleted and rewritten without the word in the tail (shadowed).
    let mut tail: Vec<DocOp> = (10..15).map(delete).collect();
    tail.extend((15..20).map(|pk| body(pk, "stone only")));
    tail.extend((40..45).map(|pk| body(pk, "apple in the tail")));
    fixture.append_all(&tail).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    assert!(view.tail.shadow().len() >= 10);
    let expected: BTreeSet<PrimaryKey> = (20..45).map(PrimaryKey::U64).collect();

    let hits = top(&view, matching("apple"), 100).await;
    let found: BTreeSet<PrimaryKey> = hits.iter().map(|hit| hit.pk.clone()).collect();
    assert_eq!(found, expected);
    assert_eq!(hits.len(), expected.len());
    let count = exec(&view, matching("apple"), 100, EffectiveSort::by_score())
        .count()
        .await
        .expect("count");
    assert_eq!(count, expected.len() as u64);
    let all = exec(&view, Query::MatchAll, 100, EffectiveSort::by_score())
        .count()
        .await
        .expect("count");
    assert_eq!(all, view.live_rows());
    assert_eq!(all, 30);
    let everything = exec(
        &view,
        matching("apple stone river"),
        100,
        EffectiveSort::by_score(),
    )
    .count()
    .await
    .expect("count");
    assert_eq!(everything, 30);

    let RowSet::Rows(rows) = FilterBitmapExec::new(view.clone(), matching("apple"), 8)
        .rows()
        .await
        .expect("rows")
    else {
        panic!("a clause is not All");
    };
    let by_row = rows_of(&view).await;
    let keys: BTreeSet<PrimaryKey> = rows.iter().map(|row| by_row[&row].0.clone()).collect();
    assert_eq!(keys, expected);
    reads.shutdown().await;
    fixture.shutdown().await;
}

// ----- field sorts -----

fn n_doc(pk: u64, n: Option<Value>) -> DocOp {
    let mut source = json!({"body": "apple"});
    if let Some(n) = n {
        source["n"] = n;
    }
    doc_of(PrimaryKey::U64(pk), source)
}

async fn keys_sorted(view: &Arc<ReadView>, keys: Vec<SortKey>) -> Vec<PrimaryKey> {
    exec(view, Query::MatchAll, 100, field_sort(keys))
        .search()
        .await
        .expect("search")
        .into_iter()
        .map(|hit| hit.pk)
        .collect()
}

fn keys(pks: &[u64]) -> Vec<PrimaryKey> {
    pks.iter().copied().map(PrimaryKey::U64).collect()
}

#[tokio::test]
async fn field_sort_orders_by_value_then_pk_with_missing_last_and_first() {
    let fixture = TailFixture::start(body_schema(), 2).await;
    let values = [
        (1, Some(json!(5))),
        (2, None),
        (3, Some(json!(-2))),
        (4, Some(json!(5))),
        (5, None),
        (6, Some(json!(9))),
        (7, Some(json!(5))),
    ];
    let ops: Vec<DocOp> = values.iter().map(|(pk, n)| n_doc(*pk, n.clone())).collect();
    fixture.append_all(&ops[..4]).await;
    fixture.apply_link().await;
    fixture.append_all(&ops[4..]).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let by = |order, missing| SortKey::Field {
        field: "n".to_string(),
        order,
        missing,
    };
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Asc, MissingOrder::Last)]).await,
        keys(&[3, 1, 4, 7, 6, 2, 5])
    );
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Asc, MissingOrder::First)]).await,
        keys(&[2, 5, 3, 1, 4, 7, 6])
    );
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Desc, MissingOrder::Last)]).await,
        keys(&[6, 1, 4, 7, 3, 2, 5])
    );
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Desc, MissingOrder::First)]).await,
        keys(&[2, 5, 6, 1, 4, 7, 3])
    );
    // PK descending as the tie-break, and a bounded k.
    let hits = exec(
        &view,
        Query::MatchAll,
        3,
        field_sort(vec![
            by(SortOrder::Asc, MissingOrder::Last),
            SortKey::Pk {
                order: SortOrder::Desc,
            },
        ]),
    )
    .search()
    .await
    .expect("search");
    assert_eq!(
        hits.iter().map(|h| h.pk.clone()).collect::<Vec<_>>(),
        keys(&[3, 7, 4])
    );
    assert!(hits.iter().all(|h| h.score == 0.0));
    // No sort and no retriever: PK order.
    assert_eq!(
        keys_sorted(&view, vec![]).await,
        keys(&[1, 2, 3, 4, 5, 6, 7])
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn multi_valued_fields_sort_by_min_ascending_and_max_descending() {
    let fixture = TailFixture::start(body_schema(), 1).await;
    let ops = [
        n_doc(1, Some(json!([3, 9]))),
        n_doc(2, Some(json!([5]))),
        n_doc(3, Some(json!([1, 4]))),
        n_doc(4, Some(json!([6, 7]))),
    ];
    fixture.append_all(&ops[..2]).await;
    fixture.apply_link().await;
    fixture.append_all(&ops[2..]).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let by = |order| SortKey::Field {
        field: "n".to_string(),
        order,
        missing: MissingOrder::Last,
    };
    // Minimums 3, 5, 1, 6.
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Asc)]).await,
        keys(&[3, 1, 2, 4])
    );
    // Maximums 9, 5, 4, 7.
    assert_eq!(
        keys_sorted(&view, vec![by(SortOrder::Desc)]).await,
        keys(&[1, 4, 2, 3])
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_sort_on_a_non_fast_field_is_invalid() {
    let fixture = TailFixture::start(body_schema(), 1).await;
    fixture.append(&body(1, "apple")).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let by = |field: &str| {
        vec![SortKey::Field {
            field: field.to_string(),
            order: SortOrder::Asc,
            missing: MissingOrder::Last,
        }]
    };
    for (field, message) in [
        ("body", "sort field body is not a fast field"),
        ("payload", "sort field payload is not a fast field"),
        ("missing", "sort field missing is not a fast field"),
        ("payload.x", "sorting on JSON paths is not supported in M1"),
    ] {
        let result = exec(&view, Query::MatchAll, 10, field_sort(by(field)))
            .search()
            .await;
        assert_eq!(
            result,
            Err(ServiceError::InvalidArgument(message.to_string())),
            "{field}"
        );
    }
    // A vector retriever cannot be combined with a field sort.
    let request = SearchRequest {
        retrievers: vec![Retriever::Vector {
            field: "v".to_string(),
            query: vec![1.0],
            k: 1,
            params: Default::default(),
            filter: None,
        }],
        sort: by("n"),
        ..SearchRequest::new("docs")
    };
    assert!(matches!(
        EffectiveSort::of(&request),
        Err(ServiceError::InvalidArgument(_))
    ));
    reads.shutdown().await;
    fixture.shutdown().await;
}

// ----- filter bitmaps -----

fn random_filter(rng: &mut ChaCha8Rng, depth: u32) -> Query {
    let tags = ["a", "b", "c"];
    match rng.random_range(0..if depth == 0 { 4 } else { 6 }) {
        0 => Query::Term {
            field: "tag".to_string(),
            value: FieldValue::Str(tags.choose(rng).expect("tag").to_string()),
        },
        1 => {
            let low = rng.random_range(-5..5);
            Query::Range {
                field: "n".to_string(),
                gt: None,
                gte: Some(FieldValue::I64(low)),
                lt: Some(FieldValue::I64(low + rng.random_range(1..6))),
                lte: None,
            }
        }
        2 => Query::Exists {
            field: "n".to_string(),
        },
        3 => Query::MatchAll,
        _ => Query::Bool {
            must: (0..rng.random_range(0..2))
                .map(|_| random_filter(rng, depth - 1))
                .collect(),
            should: vec![],
            must_not: (0..rng.random_range(0..2))
                .map(|_| random_filter(rng, depth - 1))
                .collect(),
            filter: (0..rng.random_range(1..3))
                .map(|_| random_filter(rng, depth - 1))
                .collect(),
            minimum_should_match: None,
        },
    }
}

/// `filter` evaluated on a document's source.
fn eval(filter: &Query, source: &Value) -> bool {
    match filter {
        Query::MatchAll => true,
        Query::Term {
            field,
            value: FieldValue::Str(value),
        } => source.get(field) == Some(&json!(value)),
        Query::Range { field, gte, lt, .. } => {
            let Some(n) = source.get(field).and_then(Value::as_i64) else {
                return false;
            };
            let (Some(FieldValue::I64(low)), Some(FieldValue::I64(high))) = (gte, lt) else {
                unreachable!()
            };
            *low <= n && n < *high
        }
        Query::Exists { field } => source.get(field).is_some_and(|v| !v.is_null()),
        Query::Bool {
            must,
            must_not,
            filter,
            ..
        } => {
            must.iter().chain(filter).all(|q| eval(q, source))
                && !must_not.iter().any(|q| eval(q, source))
        }
        other => unreachable!("{other:?}"),
    }
}

#[tokio::test]
async fn filter_bitmaps_cover_splits_and_tail() {
    let mut rng = ChaCha8Rng::seed_from_u64(30);
    let fixture = TailFixture::start(body_schema(), 2).await;
    let random_doc = |pk: u64, rng: &mut ChaCha8Rng| {
        let mut source = json!({"body": "apple"});
        if rng.random_bool(0.7) {
            source["tag"] = json!(["a", "b", "c"].choose(rng).expect("tag"));
        }
        if rng.random_bool(0.7) {
            source["n"] = json!(rng.random_range(-5..5));
        }
        doc_of(PrimaryKey::U64(pk), source)
    };
    let first: Vec<DocOp> = (0..60).map(|pk| random_doc(pk, &mut rng)).collect();
    fixture.append_all(&first).await;
    fixture.apply_link().await;
    let mut second: Vec<DocOp> = (40..90).map(|pk| random_doc(pk, &mut rng)).collect();
    second.extend((0..5).map(delete));
    fixture.append_all(&second).await;
    fixture.apply_link().await;
    let mut tail: Vec<DocOp> = (80..110).map(|pk| random_doc(pk, &mut rng)).collect();
    tail.extend((10..14).map(delete));
    fixture.append_all(&tail).await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let rows = rows_of(&view).await;
    assert!(view.tail.live_count() > 0 && !view.tail.shadow().is_empty());
    for _ in 0..30 {
        let filter = random_filter(&mut rng, 2);
        let expected: BTreeSet<u64> = rows
            .iter()
            .filter(|(_, (_, source))| eval(&filter, source))
            .map(|(row, _)| *row)
            .collect();
        let exec = FilterBitmapExec::new(view.clone(), filter.clone(), 8);
        match exec.rows().await.expect("rows") {
            RowSet::All => {
                assert!(matches!(filter, Query::MatchAll), "{filter:?}");
                assert_eq!(expected.len(), rows.len());
            }
            RowSet::Rows(found) => {
                assert_eq!(
                    found.iter().collect::<BTreeSet<_>>(),
                    expected,
                    "{filter:?}"
                );
            }
        }
        // Through DataFusion: ascending row ids; All expands to every row.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(exec);
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("collect");
        let emitted: Vec<u64> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                    .expect("u64")
                    .clone();
                column.values().to_vec()
            })
            .collect();
        assert_eq!(emitted, expected.iter().copied().collect::<Vec<_>>());
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

// ----- the hot tier -----

/// A hot tier that pins local copies of splits.
#[derive(Debug)]
struct LocalSplits {
    files: BTreeMap<ulid::Ulid, PathBuf>,
}

impl HotTier for LocalSplits {
    fn ann(&self, _: NamespaceId, _: CollectionId, _: &str, _: u64) -> Option<Arc<dyn HotAnn>> {
        None
    }

    fn split_file(&self, _: NamespaceId, _: CollectionId, split: ulid::Ulid) -> Option<PathBuf> {
        self.files.get(&split).cloned()
    }

    fn record_access(&self, _: NamespaceId, _: CollectionId) {}
}

#[tokio::test]
async fn the_hot_split_file_gives_identical_results() {
    let mut rng = ChaCha8Rng::seed_from_u64(7);
    let fixture = TailFixture::start(body_schema(), 2).await;
    let docs: Vec<DocOp> = (0..120u64)
        .map(|pk| body(pk, &words(&mut rng, &VOCABULARY[..20], 5)))
        .collect();
    fixture.append_all(&docs[..60]).await;
    fixture.apply_link().await;
    fixture.append_all(&docs[60..100]).await;
    fixture.apply_link().await;
    fixture.append_all(&docs[100..]).await;
    let reads = reads(&fixture);
    let collection = fixture.collection().await;
    let cold = view(&fixture, &reads).await;

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
    let hot = RequestHot {
        enabled: true,
        used: Default::default(),
    };
    let tier: Arc<dyn HotTier> = Arc::new(LocalSplits { files });
    let warm = Arc::new(
        reads
            .view(fixture.ns, &collection, &strong(), &hot, tier)
            .await
            .expect("view"),
    );
    for _ in 0..5 {
        let query = random_query(&mut rng, &VOCABULARY[..20]);
        assert_eq!(
            bits(&top(&cold, query.clone(), 20).await),
            bits(&top(&warm, query.clone(), 20).await),
            "{query:?}"
        );
    }
    assert_eq!(
        warm.hot_used.kinds().into_iter().collect::<Vec<_>>(),
        vec![HotKind::Splits]
    );
    assert!(cold.hot_used.kinds().is_empty());
    // A file that vanished after the lookup falls back to the cache.
    drop(dir);
    let hot = RequestHot {
        enabled: true,
        used: Default::default(),
    };
    let gone = Arc::new(
        reads
            .view(fixture.ns, &collection, &strong(), &hot, warm.hot.clone())
            .await
            .expect("view"),
    );
    let query = matching("apple river");
    assert_eq!(
        bits(&top(&cold, query.clone(), 20).await),
        bits(&top(&gone, query, 20).await)
    );
    assert!(gone.hot_used.kinds().is_empty());
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn term_sets_and_query_string_leaves_are_warmed() {
    let fixture = TailFixture::start(body_schema(), 2).await;
    let docs: Vec<DocOp> = (0..300u64)
        .map(|pk| {
            let tag = ["alpha", "beta", "gamma", "delta"][(pk % 4) as usize];
            doc_of(
                PrimaryKey::U64(pk),
                json!({"body": format!("apple {tag}"), "tag": tag, "n": (pk % 10) as i64}),
            )
        })
        .collect();
    fixture.append_all(&docs).await;
    fixture.apply_link().await;
    let reads = reads(&fixture);
    let view = view(&fixture, &reads).await;
    let string = |query: &str| Query::QueryString {
        query: query.to_string(),
        default_fields: vec!["body".to_string()],
        default_operator: BoolOperator::Or,
    };
    let cases = [
        (
            Query::Terms {
                field: "tag".to_string(),
                values: vec![
                    FieldValue::Str("alpha".into()),
                    FieldValue::Str("delta".into()),
                ],
            },
            150,
        ),
        (
            Query::Ids((0..300u64).step_by(3).map(PrimaryKey::U64).collect()),
            100,
        ),
        (string("n:[0 TO 2]"), 90),
        (string("tag:[beta TO delta]"), 150),
        (string("tag: IN [gamma beta]"), 150),
        (string("n:>7"), 60),
    ];
    for (query, expected) in cases {
        // A fresh view opens every split cold, warmed for this query only.
        let count = exec(&view, query.clone(), 1_000, EffectiveSort::by_score())
            .count()
            .await
            .unwrap_or_else(|err| panic!("{query:?}: {err}"));
        assert_eq!(count, expected, "{query:?}");
        let hits = top(&view, query.clone(), 1_000).await;
        assert_eq!(hits.len() as u64, expected, "{query:?}");
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}
