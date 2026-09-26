//! Search assembly (plan M1.2 Task 7): fusion, document fetch, tail merge,
//! the planner, paging, groups, get, count and scroll.

use operon_collection::PrimaryKey;
use operon_query::Fusion;
use operon_query::exec::{Ranked, fuse};

// ----- fusion -----

fn ranked(pk: u64, score: f32) -> Ranked {
    Ranked {
        row_id: pk,
        pk: PrimaryKey::U64(pk),
        score,
        sort: Vec::new(),
    }
}

/// A = [d1 0.9, d2 0.8, d3 0.7], B = [d3 10, d1 5] (M1.4 S1, S2).
fn lists() -> Vec<Vec<Ranked>> {
    vec![
        vec![ranked(1, 0.9), ranked(2, 0.8), ranked(3, 0.7)],
        vec![ranked(3, 10.0), ranked(1, 5.0)],
    ]
}

fn scores(fused: &[Ranked]) -> Vec<(u64, f32)> {
    fused
        .iter()
        .map(|hit| match hit.pk {
            PrimaryKey::U64(pk) => (pk, hit.score),
            _ => panic!("u64 keys"),
        })
        .collect()
}

fn assert_close(actual: &[(u64, f32)], expected: &[(u64, f32)]) {
    assert_eq!(actual.len(), expected.len(), "{actual:?}");
    for ((pk, score), (want_pk, want)) in actual.iter().zip(expected) {
        assert_eq!(pk, want_pk, "{actual:?}");
        assert!((score - want).abs() <= 1e-7, "{pk}: {score} vs {want}");
    }
}

#[test]
fn rrf_golden_values() {
    let fused = fuse(&lists(), &Fusion::Rrf { k: 60 });
    assert_close(
        &scores(&fused),
        &[(1, 0.032522473), (3, 0.03226646), (2, 0.016129032)],
    );
    let fused = fuse(&lists(), &Fusion::Rrf { k: 1 });
    assert_close(
        &scores(&fused),
        &[(1, 0.8333334), (3, 0.75), (2, 0.33333334)],
    );
}

#[test]
fn dbsf_golden_values() {
    let fused = fuse(&lists(), &Fusion::Dbsf);
    assert_close(
        &scores(&fused),
        &[(1, 1.0488155), (3, 0.9511845), (2, 0.50000006)],
    );
    // A list of one hit normalizes to 0.5.
    let fused = fuse(&[lists()[0].clone(), vec![ranked(4, 3.0)]], &Fusion::Dbsf);
    let d4 = scores(&fused)
        .into_iter()
        .find(|(pk, _)| *pk == 4)
        .expect("d4");
    assert_eq!(d4.1, 0.5);
}

#[test]
fn dbsf_is_not_clamped() {
    let mut outlier: Vec<Ranked> = vec![ranked(1, 100.0)];
    outlier.extend((2..=20).map(|pk| ranked(pk, 1.0)));
    let fused = fuse(&[outlier], &Fusion::Dbsf);
    assert_eq!(scores(&fused)[0].0, 1);
    assert!(fused[0].score > 1.0, "{}", fused[0].score);
}

#[test]
fn weighted_sum_golden_values() {
    let fused = fuse(
        &lists(),
        &Fusion::WeightedSum {
            weights: vec![2.0, 0.5],
        },
    );
    assert_close(&scores(&fused), &[(3, 6.4), (1, 4.3), (2, 1.6)]);
}

// ----- fixtures -----

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::common::{TailFixture, field, obj, vector};
use datafusion::physical_plan::ExecutionPlan;
use operon_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, SparseModifier, SparseVector,
    SparseVectorSpec,
};
use operon_query::exec::{
    AnnExec, DocFetchExec, EffectiveSort, FetchColumns, SearchConfig, SearchPlanner, SparseExec,
    TantivySearchExec, fetched_schema, filter_source, ranked_to_batch,
};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::TailConfig;
use operon_query::text::StatsCache;
use operon_query::vector::{AnnConfig, score};
use operon_query::{
    AnnParams, BoolOperator, FieldValue, GroupBy, Hit, Projection, Query, ReadConsistency,
    Retriever, SearchRequest, SearchResponse, ServiceError, SortKey, SortOrder, SortValue,
    SourceFilter, SparseParams, TotalHits, TotalRelation, TrackTotalHits,
};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

/// `body` Text english, `tag` Keyword, `n` I64, `payload` Json; dense `a`
/// (dim 3) and `b` (dim 2), sparse `s`.
fn search_schema() -> CollectionSchema {
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
        vec![vector("a", 3), vector("b", 2)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    schema.validate().expect("valid schema");
    schema
}

fn upsert_doc(
    pk: PrimaryKey,
    source: Value,
    vectors: &[(&str, Vec<f32>)],
    sparse: &[(&str, SparseVector)],
) -> DocOp {
    DocOp::Upsert(Document {
        pk,
        source: obj(source),
        vectors: vectors
            .iter()
            .map(|(name, v)| (name.to_string(), v.clone()))
            .collect(),
        sparse_vectors: sparse
            .iter()
            .map(|(name, v)| (name.to_string(), v.clone()))
            .collect(),
    })
}

fn plain(pk: u64, source: Value) -> DocOp {
    upsert_doc(PrimaryKey::U64(pk), source, &[], &[])
}

fn delete(pk: PrimaryKey) -> DocOp {
    DocOp::Delete(pk)
}

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("sparse")
}

fn matching(field: &str, text: &str) -> Query {
    Query::Match {
        field: field.to_string(),
        text: text.to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

fn term(field: &str, value: FieldValue) -> Query {
    Query::Term {
        field: field.to_string(),
        value,
    }
}

fn text(query: Query, k: usize) -> Retriever {
    Retriever::Text { query, k }
}

/// A collection written in `commits` applied commits, then `tail` ops the
/// link has not applied.
struct Setup {
    fixture: TailFixture,
    reads: Reads,
}

impl Setup {
    async fn new(schema: CollectionSchema, commits: Vec<Vec<DocOp>>, tail: Vec<DocOp>) -> Self {
        let fixture = TailFixture::start(schema, 2).await;
        for commit in commits {
            fixture.append_all(&commit).await;
            fixture.apply_link().await;
        }
        if !tail.is_empty() {
            fixture.append_all(&tail).await;
        }
        let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
        Self { fixture, reads }
    }

    async fn view(&self) -> Arc<ReadView> {
        Arc::new(
            self.fixture
                .view(&self.reads, &ReadConsistency::Strong)
                .await
                .expect("view"),
        )
    }

    async fn shutdown(self) {
        self.reads.shutdown().await;
        self.fixture.shutdown().await;
    }
}

fn planner() -> SearchPlanner {
    SearchPlanner::new(SearchConfig::default(), AnnConfig::default())
}

async fn search(view: &Arc<ReadView>, request: SearchRequest) -> SearchResponse {
    planner()
        .search(view.clone(), request)
        .await
        .expect("search")
}

fn request() -> SearchRequest {
    SearchRequest::new("docs")
}

fn pks(hits: &[Hit]) -> Vec<PrimaryKey> {
    hits.iter().map(|hit| hit.pk.clone()).collect()
}

fn u64s(hits: &[Hit]) -> Vec<u64> {
    hits.iter()
        .map(|hit| match hit.pk {
            PrimaryKey::U64(pk) => pk,
            _ => panic!("u64 keys"),
        })
        .collect()
}

/// Splits `ops` into `n` nearly equal commits.
fn commits_of(ops: Vec<DocOp>, n: usize) -> Vec<Vec<DocOp>> {
    let size = ops.len().div_ceil(n);
    ops.chunks(size).map(<[DocOp]>::to_vec).collect()
}

// ----- constant-score text retrievers (rule 11) -----

#[tokio::test]
async fn constant_score_top_k_walks_pk_order() {
    let mut rng = ChaCha8Rng::seed_from_u64(71);
    let mut keys: Vec<u64> = (0..6_000).collect();
    keys.shuffle(&mut rng);
    let tag_of = |pk: u64| if pk.is_multiple_of(6) { "b" } else { "a" };
    let ops: Vec<DocOp> = keys
        .iter()
        .map(|pk| plain(*pk, json!({"tag": tag_of(*pk), "n": pk})))
        .collect();
    let mut state: BTreeMap<u64, &str> = keys.iter().map(|pk| (*pk, tag_of(*pk))).collect();
    // The tail deletes two "a" keys, turns an "a" into a "b" and a "b" into
    // an "a", and writes new keys in both.
    let tail = vec![
        delete(PrimaryKey::U64(1)),
        delete(PrimaryKey::U64(2)),
        plain(3, json!({"tag": "b", "n": 3})),
        plain(6, json!({"tag": "a", "n": 6})),
        plain(6_001, json!({"tag": "a", "n": 6_001})),
        plain(6_002, json!({"tag": "a", "n": 6_002})),
    ];
    state.remove(&1);
    state.remove(&2);
    state.insert(3, "b");
    state.insert(6, "a");
    state.insert(6_001, "a");
    state.insert(6_002, "a");
    let setup = Setup::new(search_schema(), commits_of(ops, 3), tail).await;
    let view = setup.view().await;
    let query = Query::Terms {
        field: "tag".to_string(),
        values: vec![FieldValue::Str("a".to_string())],
    };
    assert_eq!(state.values().filter(|tag| **tag == "a").count(), 5_000);

    for (filter, lowest) in [
        (None, 0u64),
        (
            Some(Query::Range {
                field: "n".to_string(),
                gt: None,
                gte: Some(FieldValue::I64(100)),
                lt: None,
                lte: None,
            }),
            100,
        ),
    ] {
        let expected: Vec<u64> = state
            .iter()
            .filter(|(pk, tag)| **tag == "a" && **pk >= lowest)
            .map(|(pk, _)| *pk)
            .take(10)
            .collect();
        let response = search(
            &view,
            SearchRequest {
                retrievers: vec![text(query.clone(), 10)],
                filter: filter.clone(),
                ..request()
            },
        )
        .await;
        assert_eq!(u64s(&response.hits), expected, "{filter:?}");
        assert!(response.hits.iter().all(|hit| hit.score == 1.0));
        let scored = TantivySearchExec::new(
            view.clone(),
            query.clone(),
            filter.clone(),
            10,
            EffectiveSort::by_score(),
            None,
            StatsCache::new(1_000),
            8,
        )
        .search()
        .await
        .expect("scored");
        let scored: Vec<(PrimaryKey, u32)> = scored
            .iter()
            .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
            .collect();
        let walked: Vec<(PrimaryKey, u32)> = response
            .hits
            .iter()
            .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
            .collect();
        assert_eq!(walked, scored, "{filter:?}");
    }
    setup.shutdown().await;
}

// ----- ties and PK order (S3) -----

/// Six docs that tie on every score, keyed by every key kind, spread over
/// two commits and the tail, and one doc that matches only the vector.
fn tie_keys() -> Vec<PrimaryKey> {
    vec![
        PrimaryKey::U64(3),
        PrimaryKey::U64(40),
        PrimaryKey::Uuid([1; 16]),
        PrimaryKey::Uuid([2; 16]),
        PrimaryKey::Str("a".to_string()),
        PrimaryKey::Str("zz".to_string()),
    ]
}

#[tokio::test]
async fn equal_scores_order_by_pk_on_every_path() {
    let tie = |pk: &PrimaryKey| {
        upsert_doc(
            pk.clone(),
            json!({"body": "same words", "tag": "t"}),
            &[("a", vec![1.0, 0.0, 0.0])],
            &[],
        )
    };
    let keys = tie_keys();
    // Out of key order across the layers.
    let c1 = vec![tie(&keys[5]), tie(&keys[1])];
    let c2 = vec![
        tie(&keys[3]),
        upsert_doc(
            PrimaryKey::U64(1),
            json!({"body": "other text"}),
            &[("a", vec![0.0, 1.0, 0.0])],
            &[],
        ),
    ];
    let tail = vec![tie(&keys[0]), tie(&keys[4]), tie(&keys[2])];
    let setup = Setup::new(search_schema(), vec![c1, c2], tail).await;
    let view = setup.view().await;
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(sorted, keys, "U64s, then Uuids, then Strs");

    let text_retriever = text(matching("body", "same"), 10);
    let exact_vector = Retriever::Vector {
        field: "a".to_string(),
        query: vec![1.0, 0.0, 0.0],
        k: 10,
        params: AnnParams {
            exact: true,
            ..AnnParams::default()
        },
        filter: None,
    };
    let requests = [
        ("text", vec![text_retriever.clone()], None, None),
        ("exact vector", vec![exact_vector.clone()], None, None),
        (
            "fused",
            vec![text_retriever, exact_vector],
            Some(operon_query::Fusion::Dbsf),
            None,
        ),
        (
            "filter only",
            vec![],
            None,
            Some(term("tag", FieldValue::Str("t".to_string()))),
        ),
    ];
    for (name, retrievers, fusion, filter) in requests {
        let response = search(
            &view,
            SearchRequest {
                retrievers,
                fusion,
                filter,
                ..request()
            },
        )
        .await;
        let hits = &response.hits[..6];
        assert_eq!(pks(hits), keys, "{name}");
        assert!(
            hits.iter()
                .all(|hit| hit.score.to_bits() == hits[0].score.to_bits()),
            "{name}: {:?}",
            hits.iter().map(|h| h.score).collect::<Vec<_>>()
        );
    }
    setup.shutdown().await;
}

/// 100 docs keyed 0..100 in random order over three commits, with `n = pk
/// % 10`, `tag` by parity and `body` of `pk % 7 + 1` words "apple"; the
/// tail rewrites ten of them and deletes five.
async fn hundred() -> (Setup, BTreeMap<u64, Value>) {
    let mut rng = ChaCha8Rng::seed_from_u64(73);
    let mut keys: Vec<u64> = (0..100).collect();
    keys.shuffle(&mut rng);
    let source = |pk: u64, version: u32| {
        json!({
            "body": vec!["apple"; (pk % 7 + 1) as usize].join(" ") + " pear",
            "tag": if pk.is_multiple_of(2) { "even" } else { "odd" },
            "n": pk % 10,
            "version": version,
        })
    };
    let mut state: BTreeMap<u64, Value> = BTreeMap::new();
    let ops: Vec<DocOp> = keys
        .iter()
        .map(|pk| {
            state.insert(*pk, source(*pk, 0));
            plain(*pk, source(*pk, 0))
        })
        .collect();
    let mut tail = Vec::new();
    for pk in (0..100).step_by(10) {
        state.insert(pk, source(pk, 1));
        tail.push(plain(pk, source(pk, 1)));
    }
    for pk in [5, 15, 33, 71, 99] {
        state.remove(&pk);
        tail.push(delete(PrimaryKey::U64(pk)));
    }
    let _ = rng.random::<u8>();
    (
        Setup::new(search_schema(), commits_of(ops, 3), tail).await,
        state,
    )
}

#[tokio::test]
async fn a_filter_only_request_is_in_pk_order() {
    let (setup, state) = hundred().await;
    let view = setup.view().await;
    let even: Vec<u64> = state.keys().copied().filter(|pk| pk % 2 == 0).collect();
    let response = search(
        &view,
        SearchRequest {
            filter: Some(term("tag", FieldValue::Str("even".to_string()))),
            limit: 100,
            ..request()
        },
    )
    .await;
    assert_eq!(u64s(&response.hits), even);
    let response = search(
        &view,
        SearchRequest {
            offset: 3,
            limit: 4,
            ..request()
        },
    )
    .await;
    let all: Vec<u64> = state.keys().copied().collect();
    assert_eq!(u64s(&response.hits), all[3..7].to_vec());
    // Sources are the newest versions.
    for hit in &response.hits {
        let PrimaryKey::U64(pk) = hit.pk else {
            panic!()
        };
        assert_eq!(
            hit.source
                .as_ref()
                .map(|s| Value::Object(s.clone()))
                .as_ref(),
            state.get(&pk)
        );
        assert_eq!(hit.sort_values, vec![SortValue::I64(pk as i64)]);
    }
    setup.shutdown().await;
}

// ----- rescoring (S9) and projection (S10) -----

#[tokio::test]
async fn rescore_scores_candidates_exactly() {
    let vectors: Vec<(u64, Option<Vec<f32>>)> = vec![
        (1, Some(vec![1.0, 0.0, 0.0])),
        (2, Some(vec![0.5, 0.5, 0.1])),
        (3, Some(vec![0.0, 0.0, 0.0])),
        (4, None),
        (5, Some(vec![-1.0, 0.2, 0.3])),
        (6, Some(vec![0.3, 0.9, 0.0])),
    ];
    let ops: Vec<DocOp> = vectors
        .iter()
        .map(|(pk, v)| {
            let vectors: Vec<(&str, Vec<f32>)> = v.iter().map(|v| ("a", v.clone())).collect();
            upsert_doc(
                PrimaryKey::U64(*pk),
                json!({"body": format!("apple {}", "pear ".repeat(*pk as usize))}),
                &vectors,
                &[],
            )
        })
        .collect();
    let setup = Setup::new(search_schema(), vec![ops[..3].to_vec()], ops[3..].to_vec()).await;
    let view = setup.view().await;
    let query = vec![0.6, 0.8, 0.0];
    let response = search(
        &view,
        SearchRequest {
            retrievers: vec![Retriever::Rescore {
                input: Box::new(text(matching("body", "apple"), 10)),
                field: "a".to_string(),
                query: query.clone(),
                k: 4,
            }],
            ..request()
        },
    )
    .await;
    let mut expected: Vec<(u64, f32)> = vectors
        .iter()
        .filter_map(|(pk, v)| {
            Some((
                *pk,
                score(operon_collection::Distance::Cosine, &query, v.as_ref()?),
            ))
        })
        .collect();
    expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    expected.truncate(4);
    let got: Vec<(u64, f32)> = response
        .hits
        .iter()
        .map(|hit| (u64s(std::slice::from_ref(hit))[0], hit.score))
        .collect();
    assert_eq!(got, expected);
    // The zero vector scores 0.0 and the doc without the vector is gone.
    let all = search(
        &view,
        SearchRequest {
            retrievers: vec![Retriever::Rescore {
                input: Box::new(text(matching("body", "apple"), 10)),
                field: "a".to_string(),
                query: query.clone(),
                k: 10,
            }],
            ..request()
        },
    )
    .await;
    assert_eq!(all.hits.len(), 5);
    let zero = all
        .hits
        .iter()
        .find(|hit| hit.pk == PrimaryKey::U64(3))
        .expect("the zero vector");
    assert_eq!(zero.score, 0.0);
    assert!(all.hits.iter().all(|hit| hit.pk != PrimaryKey::U64(4)));
    // A wrong dimension is refused.
    let err = planner()
        .search(
            view.clone(),
            SearchRequest {
                retrievers: vec![Retriever::Rescore {
                    input: Box::new(text(matching("body", "apple"), 10)),
                    field: "a".to_string(),
                    query: vec![1.0],
                    k: 10,
                }],
                ..request()
            },
        )
        .await
        .expect_err("wrong dimension");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");
    setup.shutdown().await;
}

#[tokio::test]
async fn hits_carry_requested_vectors_and_source() {
    let doc = |pk: u64| {
        upsert_doc(
            PrimaryKey::U64(pk),
            json!({"body": "apple", "n": pk}),
            &[("a", vec![1.0, 2.0, 3.0]), ("b", vec![0.5, 0.5])],
            &[("s", sparse(&[4, 1], &[0.5, 2.0]))],
        )
    };
    let setup = Setup::new(search_schema(), vec![vec![doc(1)]], vec![doc(2)]).await;
    let view = setup.view().await;
    let select = |vectors: &[&str], source: SourceFilter| Projection {
        source,
        vectors: vectors.iter().map(|v| v.to_string()).collect(),
        fields: vec!["n".to_string()],
    };
    let response = search(
        &view,
        SearchRequest {
            select: select(&["a"], SourceFilter::All),
            ..request()
        },
    )
    .await;
    assert_eq!(response.hits.len(), 2);
    for hit in &response.hits {
        assert_eq!(hit.vectors.keys().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(hit.vectors["a"], vec![1.0, 2.0, 3.0]);
        assert!(hit.sparse_vectors.is_empty());
        assert_eq!(hit.source.as_ref().expect("source")["body"], "apple");
        let PrimaryKey::U64(pk) = hit.pk else {
            panic!()
        };
        assert_eq!(hit.fields["n"], vec![FieldValue::I64(pk as i64)]);
    }
    let response = search(
        &view,
        SearchRequest {
            select: select(&["a", "s"], SourceFilter::None),
            ..request()
        },
    )
    .await;
    for hit in &response.hits {
        assert_eq!(hit.vectors.keys().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(hit.sparse_vectors.keys().collect::<Vec<_>>(), vec!["s"]);
        assert_eq!(hit.sparse_vectors["s"], sparse(&[1, 4], &[2.0, 0.5]));
        assert_eq!(hit.source, None);
        // The source is still read for `fields`.
        assert_eq!(hit.fields.len(), 1);
    }
    let err = planner()
        .search(
            view.clone(),
            SearchRequest {
                select: select(&["nope"], SourceFilter::All),
                ..request()
            },
        )
        .await
        .expect_err("unknown vector");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");

    // The operator form: DocFetchExec over ranked rows of both layers.
    let ranked = TantivySearchExec::new(
        view.clone(),
        matching("body", "apple"),
        None,
        10,
        EffectiveSort::by_score(),
        None,
        StatsCache::new(1_000),
        8,
    )
    .search()
    .await
    .expect("ranked");
    assert_eq!(ranked.len(), 2);
    let input: Arc<dyn ExecutionPlan> = Arc::new(ValuesExec(ranked.clone()));
    let fetch: Arc<dyn ExecutionPlan> = Arc::new(DocFetchExec::new(
        input,
        view.clone(),
        FetchColumns {
            source: true,
            vectors: vec![1],
            sparse: vec![0],
        },
    ));
    let batches = datafusion::physical_plan::collect(
        fetch,
        datafusion::prelude::SessionContext::new().task_ctx(),
    )
    .await
    .expect("fetch");
    assert_eq!(batches[0].schema(), fetched_schema());
    assert_eq!(batches[0].num_rows(), 2);
    let sources = batches[0]
        .column_by_name("_source")
        .expect("_source")
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("utf8");
    let first: Value = serde_json::from_str(sources.value(0)).expect("json");
    assert_eq!(first["body"], "apple");
    let vectors = batches[0]
        .column_by_name("_vectors")
        .expect("_vectors")
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryArray>()
        .expect("binary");
    let decoded: BTreeMap<String, Vec<f32>> =
        postcard::from_bytes(vectors.value(1)).expect("postcard");
    assert_eq!(decoded, BTreeMap::from([("b".to_string(), vec![0.5, 0.5])]));
    let sparse_column = batches[0]
        .column_by_name("_sparse")
        .expect("_sparse")
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryArray>()
        .expect("binary");
    let decoded: BTreeMap<String, SparseVector> =
        postcard::from_bytes(sparse_column.value(0)).expect("postcard");
    assert_eq!(decoded["s"], sparse(&[1, 4], &[2.0, 0.5]));
    setup.shutdown().await;
}

/// A plan that outputs fixed ranked rows.
#[derive(Debug)]
struct ValuesExec(Vec<Ranked>);

impl datafusion::physical_plan::DisplayAs for ValuesExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ValuesExec")
    }
}

impl ExecutionPlan for ValuesExec {
    fn name(&self) -> &str {
        "ValuesExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        static PROPERTIES: std::sync::LazyLock<Arc<datafusion::physical_plan::PlanProperties>> =
            std::sync::LazyLock::new(|| {
                Arc::new(datafusion::physical_plan::PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(
                        operon_query::exec::ranked_schema(),
                    ),
                    datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Final,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                ))
            });
        &PROPERTIES
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let batch = ranked_to_batch(&self.0);
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                operon_query::exec::ranked_schema(),
                futures::stream::once(async move { Ok(batch) }),
            ),
        ))
    }
}

#[tokio::test]
async fn dense_and_sparse_retrievers_fuse() {
    // The M1.6 fixture collection `sp` (steps 22–25).
    let schema = CollectionSchema::new(vec![], vec![vector("e", 2)], DynamicMapping::Ignore)
        .with_sparse_vectors(vec![SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::None,
        }]);
    schema.validate().expect("valid");
    let docs = [
        upsert_doc(
            PrimaryKey::U64(1),
            json!({}),
            &[("e", vec![1.0, 0.0])],
            &[("s", sparse(&[5, 1], &[2.0, 1.0]))],
        ),
        upsert_doc(
            PrimaryKey::U64(2),
            json!({}),
            &[("e", vec![0.0, 1.0])],
            &[("s", sparse(&[5], &[0.5]))],
        ),
        upsert_doc(
            PrimaryKey::U64(3),
            json!({}),
            &[("e", vec![0.8, 0.6])],
            &[("s", sparse(&[7], &[3.0]))],
        ),
    ];
    let setup = Setup::new(schema, vec![docs[..2].to_vec()], docs[2..].to_vec()).await;
    let view = setup.view().await;
    let sparse_retriever = Retriever::Sparse {
        field: "s".to_string(),
        query: sparse(&[5], &[1.0]),
        k: 10,
        filter: None,
        params: SparseParams::default(),
    };
    let dense_retriever = Retriever::Vector {
        field: "e".to_string(),
        query: vec![1.0, 0.0],
        k: 10,
        params: AnnParams::default(),
        filter: None,
    };
    let only_sparse = search(
        &view,
        SearchRequest {
            retrievers: vec![sparse_retriever.clone()],
            ..request()
        },
    )
    .await;
    assert_eq!(u64s(&only_sparse.hits), vec![1, 2]);
    assert_eq!(only_sparse.hits[0].score, 2.0);
    assert_eq!(only_sparse.hits[1].score, 0.5);

    let fused = |fusion: operon_query::Fusion| SearchRequest {
        retrievers: vec![sparse_retriever.clone(), dense_retriever.clone()],
        fusion: Some(fusion),
        limit: 3,
        ..request()
    };
    let rrf = search(&view, fused(operon_query::Fusion::Rrf { k: 60 })).await;
    assert_eq!(u64s(&rrf.hits), vec![1, 2, 3]);
    let expected = [
        1.0f32 / 61.0 + 1.0f32 / 61.0,
        1.0f32 / 62.0 + 1.0f32 / 63.0,
        1.0f32 / 62.0,
    ];
    for (hit, want) in rrf.hits.iter().zip(expected) {
        assert!((hit.score - want).abs() <= 1e-7, "{} vs {want}", hit.score);
    }

    // DBSF equals `fuse` over the two operators' lists.
    let sparse_list = SparseExec::new(
        view.clone(),
        "s".to_string(),
        sparse(&[5], &[1.0]),
        10,
        None,
        None,
        StatsCache::new(1_000),
        8,
    )
    .search()
    .await
    .expect("sparse");
    let dense_list = AnnExec::new(
        view.clone(),
        "e".to_string(),
        vec![1.0, 0.0],
        10,
        AnnParams::default(),
        None,
        AnnConfig::default(),
    )
    .search()
    .await
    .expect("dense");
    let want = fuse(&[sparse_list, dense_list], &operon_query::Fusion::Dbsf);
    let dbsf = search(&view, fused(operon_query::Fusion::Dbsf)).await;
    let got: Vec<(PrimaryKey, u32)> = dbsf
        .hits
        .iter()
        .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
        .collect();
    let want: Vec<(PrimaryKey, u32)> = want
        .iter()
        .take(3)
        .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
        .collect();
    assert_eq!(got, want);

    // A sparse retriever as the input of a dense rescore.
    let rescored = search(
        &view,
        SearchRequest {
            retrievers: vec![Retriever::Rescore {
                input: Box::new(sparse_retriever),
                field: "e".to_string(),
                query: vec![0.6, 0.8],
                k: 10,
            }],
            ..request()
        },
    )
    .await;
    assert_eq!(u64s(&rescored.hits), vec![2, 1]);
    assert_eq!(
        rescored.hits[0].score,
        score(
            operon_collection::Distance::Cosine,
            &[0.6, 0.8],
            &[0.0, 1.0]
        )
    );
    setup.shutdown().await;
}

#[test]
fn source_paths_filter_like_es() {
    let source = obj(json!({
        "title": "t",
        "body": "b",
        "meta": {"a": 1, "secret": 2, "deep": {"z": 3, "secret": 4}},
        "metadata": {"x": 1},
        "list": [{"meta": 1}],
        "tags": [{"meta": {"a": 5, "secret": 6}}, {"other": 7}]
    }));
    let filter = SourceFilter::Paths {
        include: vec!["meta.*".to_string(), "title".to_string()],
        exclude: vec!["meta.secret".to_string()],
    };
    assert_eq!(
        filter_source(&source, &filter).map(Value::Object),
        Some(json!({
            "title": "t",
            "meta": {"a": 1, "deep": {"z": 3, "secret": 4}}
        }))
    );
    // Arrays are transparent and `*` spans dots.
    let filter = SourceFilter::Paths {
        include: vec!["tags.*".to_string()],
        exclude: vec!["*.secret".to_string()],
    };
    assert_eq!(
        filter_source(&source, &filter).map(Value::Object),
        Some(json!({"tags": [{"meta": {"a": 5}}, {"other": 7}]}))
    );
    let filter = SourceFilter::Paths {
        include: vec![],
        exclude: vec!["meta".to_string(), "list".to_string(), "tags".to_string()],
    };
    assert_eq!(
        filter_source(&source, &filter).map(Value::Object),
        Some(json!({"title": "t", "body": "b", "metadata": {"x": 1}}))
    );
    assert_eq!(
        filter_source(&source, &SourceFilter::All),
        Some(source.clone())
    );
    assert_eq!(filter_source(&source, &SourceFilter::None), None);
}

// ----- paging -----

fn by_n() -> Vec<SortKey> {
    vec![SortKey::Field {
        field: "n".to_string(),
        order: SortOrder::Asc,
        missing: operon_query::MissingOrder::Last,
    }]
}

#[tokio::test]
async fn search_after_prefix_skips_equal_values() {
    let (setup, state) = hundred().await;
    let view = setup.view().await;
    let response = search(
        &view,
        SearchRequest {
            sort: by_n(),
            search_after: Some(vec![SortValue::I64(3)]),
            limit: 100,
            ..request()
        },
    )
    .await;
    let mut expected: Vec<(u64, u64)> = state
        .keys()
        .map(|pk| (pk % 10, *pk))
        .filter(|(n, _)| *n > 3)
        .collect();
    expected.sort();
    let expected: Vec<u64> = expected.into_iter().map(|(_, pk)| pk).collect();
    assert_eq!(u64s(&response.hits), expected);
    setup.shutdown().await;
}

#[tokio::test]
async fn search_after_with_the_pk_pages_exactly() {
    let (setup, state) = hundred().await;
    let view = setup.view().await;
    let mut seen: Vec<u64> = Vec::new();
    let mut after: Option<Vec<SortValue>> = None;
    loop {
        let response = search(
            &view,
            SearchRequest {
                sort: by_n(),
                search_after: after.clone(),
                limit: 7,
                ..request()
            },
        )
        .await;
        if response.hits.is_empty() {
            break;
        }
        assert!(response.hits.len() <= 7);
        after = Some(response.hits.last().expect("a hit").sort_values.clone());
        seen.extend(u64s(&response.hits));
    }
    let mut expected: Vec<(u64, u64)> = state.keys().map(|pk| (pk % 10, *pk)).collect();
    expected.sort();
    let expected: Vec<u64> = expected.into_iter().map(|(_, pk)| pk).collect();
    assert_eq!(seen, expected);
    assert_eq!(seen.iter().collect::<BTreeSet<_>>().len(), state.len());
    setup.shutdown().await;
}

#[tokio::test]
async fn offset_limit_and_threshold() {
    let (setup, _) = hundred().await;
    let view = setup.view().await;
    let query = matching("body", "apple");
    let full = search(
        &view,
        SearchRequest {
            retrievers: vec![text(query.clone(), 50)],
            limit: 50,
            ..request()
        },
    )
    .await;
    assert_eq!(full.hits.len(), 50);
    let page = search(
        &view,
        SearchRequest {
            retrievers: vec![text(query.clone(), 50)],
            offset: 2,
            limit: 3,
            ..request()
        },
    )
    .await;
    assert_eq!(pks(&page.hits), pks(&full.hits[2..5]));
    let threshold = full.hits[20].score;
    let above = search(
        &view,
        SearchRequest {
            retrievers: vec![text(query.clone(), 50)],
            score_threshold: Some(threshold),
            limit: 50,
            ..request()
        },
    )
    .await;
    let expected: Vec<PrimaryKey> = full
        .hits
        .iter()
        .filter(|hit| hit.score >= threshold)
        .map(|hit| hit.pk.clone())
        .collect();
    assert!(expected.len() > 20 && expected.len() < 50);
    assert_eq!(pks(&above.hits), expected);
    // A threshold cuts the total too (review #21).
    let counted = search(
        &view,
        SearchRequest {
            retrievers: vec![text(query.clone(), 50)],
            score_threshold: Some(threshold),
            track_total_hits: TrackTotalHits::Exact,
            limit: 50,
            ..request()
        },
    )
    .await;
    assert_eq!(
        counted.total,
        Some(TotalHits {
            value: expected.len() as u64,
            relation: TotalRelation::Eq,
        })
    );
    setup.shutdown().await;
}

#[tokio::test]
async fn total_hits_exact_up_to_and_none() {
    let (setup, state) = hundred().await;
    let view = setup.view().await;
    let with =
        |track: TrackTotalHits, retrievers: Vec<Retriever>, filter: Option<Query>| SearchRequest {
            retrievers,
            filter,
            track_total_hits: track,
            ..request()
        };
    let apple = || vec![text(matching("body", "apple"), 10)];
    let all = state.len() as u64;
    let eq = |value| {
        Some(TotalHits {
            value,
            relation: TotalRelation::Eq,
        })
    };
    assert_eq!(
        search(&view, with(TrackTotalHits::None, apple(), None))
            .await
            .total,
        None
    );
    assert_eq!(
        search(&view, with(TrackTotalHits::Exact, apple(), None))
            .await
            .total,
        eq(all)
    );
    assert_eq!(
        search(&view, with(TrackTotalHits::UpTo(20), apple(), None))
            .await
            .total,
        Some(TotalHits {
            value: 20,
            relation: TotalRelation::Gte
        })
    );
    assert_eq!(
        search(&view, with(TrackTotalHits::UpTo(1_000), apple(), None))
            .await
            .total,
        eq(all)
    );
    // With the request filter.
    let odd = Some(term("tag", FieldValue::Str("odd".to_string())));
    let odd_count = state.keys().filter(|pk| *pk % 2 == 1).count() as u64;
    assert_eq!(
        search(&view, with(TrackTotalHits::Exact, apple(), odd.clone()))
            .await
            .total,
        eq(odd_count)
    );
    // Filter only: the filter's matches.
    assert_eq!(
        search(&view, with(TrackTotalHits::Exact, vec![], odd.clone()))
            .await
            .total,
        eq(odd_count)
    );
    // A vector retriever: its distinct candidates.
    let schema_less = vec![Retriever::Fused {
        inputs: vec![
            text(matching("body", "apple"), 7),
            text(matching("body", "pear"), 9),
        ],
        fusion: operon_query::Fusion::Rrf { k: 60 },
        k: 30,
    }];
    let fused = search(&view, with(TrackTotalHits::Exact, schema_less, None)).await;
    assert!(fused.total.expect("total").value <= 16);
    setup.shutdown().await;
}

// ----- groups -----

#[tokio::test]
async fn group_by_groups_by_value() {
    let key_of = |pk: u64| -> Value {
        match pk % 12 {
            k if k < 6 => json!(format!("k{k}")),
            k => json!(k),
        }
    };
    let ops: Vec<DocOp> = (0..60)
        .map(|pk| {
            plain(
                pk,
                json!({
                    "body": format!("apple {}", "pear ".repeat((pk % 9) as usize)),
                    "payload": {"g": key_of(pk)},
                }),
            )
        })
        .collect();
    let setup = Setup::new(
        search_schema(),
        commits_of(ops[..40].to_vec(), 2),
        ops[40..].to_vec(),
    )
    .await;
    let view = setup.view().await;
    let ranking = search(
        &view,
        SearchRequest {
            retrievers: vec![text(matching("body", "apple"), 60)],
            limit: 60,
            ..request()
        },
    )
    .await;
    assert_eq!(ranking.hits.len(), 60);
    // The grouping of the exact ranking, in test.
    let mut expected: Vec<(FieldValue, Vec<u64>)> = Vec::new();
    for pk in u64s(&ranking.hits) {
        let key = match key_of(pk) {
            Value::String(s) => FieldValue::Str(s),
            Value::Number(n) => FieldValue::I64(n.as_i64().expect("int")),
            _ => unreachable!(),
        };
        match expected.iter().position(|(k, _)| *k == key) {
            Some(at) if expected[at].1.len() < 3 => expected[at].1.push(pk),
            Some(_) => {}
            None if expected.len() < 5 => expected.push((key, vec![pk])),
            None => {}
        }
    }
    let response = search(
        &view,
        SearchRequest {
            retrievers: vec![text(matching("body", "apple"), 60)],
            group_by: Some(GroupBy {
                field: "payload.g".to_string(),
                group_size: 3,
                limit: 5,
            }),
            ..request()
        },
    )
    .await;
    assert!(response.hits.is_empty());
    let got: Vec<(FieldValue, Vec<u64>)> = response
        .groups
        .expect("groups")
        .into_iter()
        .map(|group| (group.key, u64s(&group.hits)))
        .collect();
    assert_eq!(got, expected);
    setup.shutdown().await;
}

#[tokio::test]
async fn field_mode_groups_fill_past_the_page_window() {
    // Sorted by `n`, the first 40 matches all share group key 0, so a
    // window of `limit × group_size` (6) fills only one group.
    let ops: Vec<DocOp> = (0..60)
        .map(|pk| {
            let g = if pk < 40 { 0 } else { pk % 3 };
            plain(pk, json!({"body": "apple", "n": pk, "payload": {"g": g}}))
        })
        .collect();
    let setup = Setup::new(search_schema(), commits_of(ops, 2), vec![]).await;
    let view = setup.view().await;
    let response = search(
        &view,
        SearchRequest {
            retrievers: vec![text(matching("body", "apple"), 10)],
            sort: by_n(),
            group_by: Some(GroupBy {
                field: "payload.g".to_string(),
                group_size: 2,
                limit: 3,
            }),
            ..request()
        },
    )
    .await;
    let got: Vec<(FieldValue, Vec<u64>)> = response
        .groups
        .expect("groups")
        .into_iter()
        .map(|group| (group.key, u64s(&group.hits)))
        .collect();
    assert_eq!(
        got,
        vec![
            (FieldValue::I64(0), vec![0, 1]),
            (FieldValue::I64(1), vec![40, 43]),
            (FieldValue::I64(2), vec![41, 44]),
        ]
    );
    setup.shutdown().await;
}

#[tokio::test]
async fn field_mode_group_windows_stop_at_the_max_window() {
    // The same data as above, with `max_window` 10: the window grows from 6
    // to 10 and stops there, so only key 0's group is found (the first 40
    // matches in `n` order all share it).
    let ops: Vec<DocOp> = (0..60)
        .map(|pk| {
            let g = if pk < 40 { 0 } else { pk % 3 };
            plain(pk, json!({"body": "apple", "n": pk, "payload": {"g": g}}))
        })
        .collect();
    let setup = Setup::new(search_schema(), commits_of(ops, 2), vec![]).await;
    let view = setup.view().await;
    let planner = SearchPlanner::new(
        SearchConfig {
            limits: operon_query::SearchLimits {
                max_window: 10,
                ..Default::default()
            },
            ..SearchConfig::default()
        },
        AnnConfig::default(),
    );
    let response = planner
        .search(
            view.clone(),
            SearchRequest {
                retrievers: vec![text(matching("body", "apple"), 10)],
                sort: by_n(),
                limit: 10,
                group_by: Some(GroupBy {
                    field: "payload.g".to_string(),
                    group_size: 2,
                    limit: 3,
                }),
                ..request()
            },
        )
        .await
        .expect("search");
    let got: Vec<(FieldValue, Vec<u64>)> = response
        .groups
        .expect("groups")
        .into_iter()
        .map(|group| (group.key, u64s(&group.hits)))
        .collect();
    assert_eq!(got, vec![(FieldValue::I64(0), vec![0, 1])]);
    setup.shutdown().await;
}

#[tokio::test]
async fn a_field_sort_with_a_vector_retriever_is_invalid() {
    let setup = Setup::new(search_schema(), vec![], vec![]).await;
    let view = setup.view().await;
    let err = planner()
        .search(
            view,
            SearchRequest {
                retrievers: vec![Retriever::Vector {
                    field: "a".to_string(),
                    query: vec![1.0, 0.0, 0.0],
                    k: 10,
                    params: AnnParams::default(),
                    filter: None,
                }],
                sort: by_n(),
                ..request()
            },
        )
        .await
        .expect_err("invalid");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");
    setup.shutdown().await;
}

// ----- scroll, get and count -----

/// 500 keys (400 random u64s and 100 strings) over four commits, with
/// durable updates and deletes across them, then a tail of new keys,
/// updates and deletes.
async fn five_hundred() -> (Setup, BTreeMap<PrimaryKey, Value>) {
    let (setup, state, _) = five_hundred_with_deletes().await;
    (setup, state)
}

/// [`five_hundred`], and the keys deleted durably or in the tail.
async fn five_hundred_with_deletes() -> (Setup, BTreeMap<PrimaryKey, Value>, Vec<PrimaryKey>) {
    let mut removed: Vec<PrimaryKey> = Vec::new();
    let mut rng = ChaCha8Rng::seed_from_u64(75);
    let mut keys: Vec<PrimaryKey> = (0..400)
        .map(|_| PrimaryKey::U64(rng.random_range(0..1_000_000)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    keys.extend((0..100).map(|i| PrimaryKey::Str(format!("k{:03}", i * 7 % 100))));
    keys.shuffle(&mut rng);
    let source = |i: usize, version: u32| json!({"n": i, "version": version});
    let mut state: BTreeMap<PrimaryKey, Value> = BTreeMap::new();
    let put = |state: &mut BTreeMap<PrimaryKey, Value>, pk: &PrimaryKey, i: usize, v: u32| {
        state.insert(pk.clone(), source(i, v));
        upsert_doc(pk.clone(), source(i, v), &[], &[])
    };
    let mut commits: Vec<Vec<DocOp>> = vec![Vec::new(); 4];
    let base = keys.len() - 30;
    for (i, pk) in keys[..base].iter().enumerate() {
        commits[i % 4].push(put(&mut state, pk, i, 0));
    }
    // Durable updates and deletes of keys of earlier commits.
    for (i, pk) in keys[..40].iter().enumerate() {
        if i % 4 < 3 {
            let op = if i % 2 == 0 {
                put(&mut state, pk, i, 1)
            } else {
                state.remove(pk);
                removed.push(pk.clone());
                delete(pk.clone())
            };
            commits[3].push(op);
        }
    }
    let mut tail = Vec::new();
    for (i, pk) in keys[base..].iter().enumerate() {
        tail.push(put(&mut state, pk, base + i, 0));
    }
    for (i, pk) in keys[40..100].iter().enumerate() {
        if i % 3 == 0 {
            tail.push(put(&mut state, pk, 40 + i, 2));
        } else if i % 3 == 1 {
            state.remove(pk);
            removed.push(pk.clone());
            tail.push(delete(pk.clone()));
        }
    }
    // A key deleted in the tail and written again.
    let again = keys[41].clone();
    tail.push(put(&mut state, &again, 41, 3));
    removed.retain(|pk| !state.contains_key(pk));
    (
        Setup::new(search_schema(), commits, tail).await,
        state,
        removed,
    )
}

#[tokio::test]
async fn scroll_pages_cover_every_key_once_across_tail_and_splits() {
    let (setup, state) = five_hundred().await;
    let view = setup.view().await;
    assert!(view.snapshot.splits().len() >= 4);
    let planner = planner();
    let mut after = None;
    let mut seen: Vec<(PrimaryKey, Value)> = Vec::new();
    loop {
        let (docs, next) = planner
            .scroll(
                view.clone(),
                None,
                after.clone(),
                13,
                &Projection::default(),
            )
            .await
            .expect("scroll");
        assert!(docs.len() <= 13);
        seen.extend(
            docs.into_iter()
                .map(|doc| (doc.pk, Value::Object(doc.source.expect("source")))),
        );
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let expected: Vec<(PrimaryKey, Value)> = state.into_iter().collect();
    assert_eq!(seen.len(), expected.len());
    assert_eq!(seen, expected);
    setup.shutdown().await;
}

#[tokio::test]
async fn scroll_after_is_exclusive_and_ordered() {
    let (setup, state) = five_hundred().await;
    let view = setup.view().await;
    let keys: Vec<PrimaryKey> = state.keys().cloned().collect();
    let planner = planner();
    // After an existing key: the next keys, exclusive.
    let (docs, next) = planner
        .scroll(
            view.clone(),
            None,
            Some(keys[10].clone()),
            5,
            &Projection::default(),
        )
        .await
        .expect("scroll");
    let got: Vec<PrimaryKey> = docs.iter().map(|doc| doc.pk.clone()).collect();
    assert_eq!(got, keys[11..16].to_vec());
    assert_eq!(next, Some(keys[15].clone()));
    // After a key that does not exist.
    let PrimaryKey::U64(k) = keys[20] else {
        panic!()
    };
    let (docs, _) = planner
        .scroll(
            view.clone(),
            None,
            Some(PrimaryKey::U64(k - 1)),
            2,
            &Projection::default(),
        )
        .await
        .expect("scroll");
    assert_eq!(docs[0].pk, keys[20]);
    // The last page has no next key.
    let (docs, next) = planner
        .scroll(
            view.clone(),
            None,
            Some(keys[keys.len() - 3].clone()),
            5,
            &Projection::default(),
        )
        .await
        .expect("scroll");
    assert_eq!(docs.len(), 2);
    assert_eq!(next, None);
    // Exactly `limit` left: no next key either.
    let (docs, next) = planner
        .scroll(
            view.clone(),
            None,
            Some(keys[keys.len() - 3].clone()),
            2,
            &Projection::default(),
        )
        .await
        .expect("scroll");
    assert_eq!(docs.len(), 2);
    assert_eq!(next, None);
    setup.shutdown().await;
}

#[tokio::test]
async fn a_selective_filtered_scroll_sorts_the_matches() {
    let (setup, state) = five_hundred().await;
    let view = setup.view().await;
    let filter = Query::Range {
        field: "n".to_string(),
        gt: None,
        gte: Some(FieldValue::I64(100)),
        lt: Some(FieldValue::I64(160)),
        lte: None,
    };
    let expected: Vec<PrimaryKey> = state
        .iter()
        .filter(|(_, v)| (100..160).contains(&v["n"].as_i64().expect("n")))
        .map(|(pk, _)| pk.clone())
        .collect();
    assert!(expected.len() > 20);
    let pages = |planner: SearchPlanner| {
        let view = view.clone();
        let filter = filter.clone();
        async move {
            let mut after = None;
            let mut seen = Vec::new();
            loop {
                let (docs, next) = planner
                    .scroll(
                        view.clone(),
                        Some(filter.clone()),
                        after,
                        7,
                        &Projection::default(),
                    )
                    .await
                    .expect("scroll");
                seen.extend(docs.into_iter().map(|doc| doc.pk));
                match next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            seen
        }
    };
    let sorted = pages(planner()).await;
    let walked = pages(SearchPlanner::new(
        SearchConfig {
            scroll_sort_threshold: 0,
            ..SearchConfig::default()
        },
        AnnConfig::default(),
    ))
    .await;
    assert_eq!(sorted, expected);
    assert_eq!(walked, expected);
    setup.shutdown().await;
}

#[tokio::test]
async fn get_returns_request_order_with_misses() {
    let (setup, state, removed) = five_hundred_with_deletes().await;
    let view = setup.view().await;
    // Keys from every layer, deleted keys (durably and in the tail), a key
    // that never existed and a repeat.
    let present: Vec<PrimaryKey> = state.keys().take(40).cloned().collect();
    assert!(removed.len() >= 20);
    let missing = PrimaryKey::Str("never".to_string());
    let mut request_keys = vec![present[5].clone(), missing.clone(), present[0].clone()];
    request_keys.extend(removed.iter().cloned());
    request_keys.extend(present[10..30].iter().cloned());
    request_keys.push(present[5].clone());
    let docs = planner()
        .get(&view, &request_keys, &Projection::default())
        .await
        .expect("get");
    assert_eq!(docs.len(), request_keys.len());
    for (pk, doc) in request_keys.iter().zip(&docs) {
        match state.get(pk) {
            Some(source) => {
                let doc = doc.as_ref().expect("present");
                assert_eq!(&doc.pk, pk);
                assert_eq!(doc.source.clone().map(Value::Object).as_ref(), Some(source));
            }
            None => assert!(doc.is_none(), "{pk:?}"),
        }
    }
    // Every key of the fixture, deleted ones included.
    let all: Vec<PrimaryKey> = state.keys().cloned().collect();
    let docs = planner()
        .get(
            &view,
            &all,
            &Projection {
                source: SourceFilter::None,
                vectors: vec![],
                fields: vec!["n".to_string()],
            },
        )
        .await
        .expect("get");
    for (pk, doc) in all.iter().zip(docs) {
        let doc = doc.expect("present");
        assert_eq!(doc.source, None);
        assert_eq!(
            doc.fields["n"],
            vec![FieldValue::I64(state[pk]["n"].as_i64().expect("n"))]
        );
    }
    setup.shutdown().await;
}

#[tokio::test]
async fn count_matches_the_exact_total() {
    let (setup, state) = five_hundred().await;
    let view = setup.view().await;
    let planner = planner();
    assert_eq!(
        planner.count(view.clone(), None).await.expect("count"),
        state.len() as u64
    );
    let filter = Query::Range {
        field: "n".to_string(),
        gt: None,
        gte: Some(FieldValue::I64(200)),
        lt: None,
        lte: None,
    };
    let count = planner
        .count(view.clone(), Some(filter.clone()))
        .await
        .expect("count");
    let response = planner
        .search(
            view.clone(),
            SearchRequest {
                filter: Some(filter),
                track_total_hits: TrackTotalHits::Exact,
                ..request()
            },
        )
        .await
        .expect("search");
    assert_eq!(response.total.expect("total").value, count);
    assert_eq!(
        count,
        state
            .values()
            .filter(|v| v["n"].as_i64().expect("n") >= 200)
            .count() as u64
    );
    setup.shutdown().await;
}
