//! Aggregations and highlighting (plan M1.2 Task 8).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::{TailFixture, field, obj, vector};
use operon_collection::{CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, PrimaryKey};
use operon_query::exec::{AggDomain, SearchConfig, SearchPlanner, aggregate};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::TailConfig;
use operon_query::vector::AnnConfig;
use operon_query::{
    AnnParams, BoolOperator, Highlight, HighlightField, Query, ReadConsistency, Retriever,
    SearchRequest, SearchResponse, ServiceError,
};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

// ----- fixtures -----

const VOCABULARY: [&str; 24] = [
    "apple", "river", "stone", "cloud", "table", "green", "light", "north", "piano", "quiet",
    "radio", "sugar", "tiger", "under", "vivid", "water", "yellow", "zebra", "anchor", "bridge",
    "candle", "desert", "engine", "forest",
];

const TAGS: [&str; 8] = ["t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"];

/// `body` Text english, `title` Text standard, `tag` Keyword, `n` I64 and
/// vector `a` (dim 3).
fn agg_schema() -> CollectionSchema {
    let text = |name: &str, analyzer: &str| {
        field(
            name,
            FieldKind::Text {
                analyzer: analyzer.to_string(),
                positions: true,
            },
        )
    };
    let schema = CollectionSchema::new(
        vec![
            text("body", "english"),
            text("title", "standard"),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
        ],
        vec![vector("a", 3)],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

fn upsert(pk: u64, source: Value, a: Option<Vec<f32>>) -> DocOp {
    DocOp::Upsert(Document {
        pk: PrimaryKey::U64(pk),
        source: obj(source),
        vectors: a.into_iter().map(|v| ("a".to_string(), v)).collect(),
        sparse_vectors: BTreeMap::new(),
    })
}

fn delete(pk: u64) -> DocOp {
    DocOp::Delete(PrimaryKey::U64(pk))
}

struct Setup {
    fixture: TailFixture,
    reads: Reads,
}

impl Setup {
    async fn with(fixture: TailFixture, commits: Vec<Vec<DocOp>>, tail: Vec<DocOp>) -> Self {
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

    async fn new(commits: Vec<Vec<DocOp>>, tail: Vec<DocOp>) -> Self {
        Self::with(TailFixture::start(agg_schema(), 2).await, commits, tail).await
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

/// The final documents of fixtures A and B: 200 docs with random bodies,
/// tags, `n` and titles.
fn final_docs() -> Vec<(u64, Value)> {
    let mut rng = ChaCha8Rng::seed_from_u64(81);
    (0..200)
        .map(|pk| {
            let words: Vec<&str> = (0..rng.random_range(3..=10))
                .map(|_| *VOCABULARY.choose(&mut rng).expect("a word"))
                .collect();
            let source = json!({
                "body": words.join(" "),
                "title": format!("title {pk} {}", words[0]),
                "tag": TAGS[rng.random_range(0..TAGS.len())],
                "n": rng.random_range(0..1_000),
            });
            (pk, source)
        })
        .collect()
}

/// Task 5's fixtures A and B: A has the 200 docs in one commit (one
/// split); B has five commits, 15 docs written twice with other values
/// before their final one, and 20 docs plus 7 final values in the tail.
async fn fixtures() -> (Setup, Setup, BTreeMap<u64, Value>) {
    let docs = final_docs();
    let finals: Vec<DocOp> = docs
        .iter()
        .map(|(pk, source)| upsert(*pk, source.clone(), None))
        .collect();
    let a = Setup::new(vec![finals.clone()], vec![]).await;
    let early = |version: &str| -> Vec<DocOp> {
        (0..15)
            .map(|pk| {
                upsert(
                    pk,
                    json!({"body": format!("{version} apple river"), "title": "early",
                           "tag": "early", "n": 5_000 + pk}),
                    None,
                )
            })
            .collect()
    };
    let mut c1 = early("first");
    c1.extend(finals[15..36].iter().cloned());
    let mut c2 = early("second");
    c2.extend(finals[36..72].iter().cloned());
    let mut c3 = finals[0..8].to_vec();
    c3.extend(finals[72..108].iter().cloned());
    let mut tail = finals[180..200].to_vec();
    tail.extend(finals[8..15].iter().cloned());
    let b = Setup::new(
        vec![
            c1,
            c2,
            c3,
            finals[108..144].to_vec(),
            finals[144..180].to_vec(),
        ],
        tail,
    )
    .await;
    (a, b, docs.into_iter().collect())
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

async fn run(view: &ReadView, request: &Value, domain: AggDomain) -> Result<Value, ServiceError> {
    aggregate(view, request, domain, &SearchConfig::default()).await
}

/// `doc_count` by key of a `terms` result.
fn counts(result: &Value) -> BTreeMap<String, u64> {
    result["buckets"]
        .as_array()
        .expect("buckets")
        .iter()
        .map(|bucket| {
            (
                bucket["key"].as_str().expect("key").to_string(),
                bucket["doc_count"].as_u64().expect("count"),
            )
        })
        .collect()
}

// ----- aggregations -----

#[tokio::test]
async fn aggregations_over_splits_and_tail_equal_a_single_split() {
    let (a, b, docs) = fixtures().await;
    let (a_view, b_view) = (a.view().await, b.view().await);
    assert_eq!(a_view.snapshot.splits().len(), 1);
    assert!(b_view.snapshot.splits().len() >= 5);
    assert!(b_view.tail.live_count() > 0);
    let request = json!({
        "tags": {"terms": {"field": "tag", "size": 20}},
        "n_stats": {"stats": {"field": "n"}},
        "n_hist": {"histogram": {"field": "n", "interval": 100}},
        "n_range": {"range": {"field": "n", "ranges": [
            {"to": 100}, {"from": 100, "to": 500}, {"from": 500}
        ]}},
        "tag_card": {"cardinality": {"field": "tag"}},
        "n_pct": {"percentiles": {"field": "n"}}
    });
    let on_a = run(&a_view, &request, AggDomain::Filter(None))
        .await
        .expect("a");
    let on_b = run(&b_view, &request, AggDomain::Filter(None))
        .await
        .expect("b");
    assert_eq!(on_a, on_b);
    // And they count the final documents.
    let mut expected: BTreeMap<String, u64> = BTreeMap::new();
    for source in docs.values() {
        *expected
            .entry(source["tag"].as_str().expect("tag").to_string())
            .or_default() += 1;
    }
    assert_eq!(counts(&on_b["tags"]), expected);
    assert_eq!(on_b["n_stats"]["count"], 200);
    let sum: i64 = docs.values().map(|s| s["n"].as_i64().expect("n")).sum();
    assert_eq!(on_b["n_stats"]["sum"].as_f64(), Some(sum as f64));
    // Through the planner: a filter-only request aggregates its filter.
    let response = search(
        &b_view,
        SearchRequest {
            aggregations: Some(request.clone()),
            ..SearchRequest::new("docs")
        },
    )
    .await;
    assert_eq!(response.aggregations, Some(on_a));
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn deleted_and_shadowed_docs_are_not_aggregated() {
    let docs: Vec<DocOp> = (0..20)
        .map(|pk| upsert(pk, json!({"tag": "x", "n": pk}), None))
        .collect();
    let mut tail: Vec<DocOp> = (5..10)
        .map(|pk| upsert(pk, json!({"tag": "y", "n": 100 + pk}), None))
        .collect();
    tail.extend((10..12).map(delete));
    let setup = Setup::new(vec![docs, (0..5).map(delete).collect()], tail).await;
    let view = setup.view().await;
    let result = run(
        &view,
        &json!({"tags": {"terms": {"field": "tag"}}, "n": {"stats": {"field": "n"}}}),
        AggDomain::Filter(None),
    )
    .await
    .expect("aggregate");
    assert_eq!(
        counts(&result["tags"]),
        BTreeMap::from([("x".to_string(), 8), ("y".to_string(), 5)])
    );
    assert_eq!(result["n"]["count"], 13);
    let sum: u64 = (12..20).sum::<u64>() + (105..110).sum::<u64>();
    assert_eq!(result["n"]["sum"].as_f64(), Some(sum as f64));
    setup.shutdown().await;
}

#[tokio::test]
async fn the_aggregation_domain_follows_the_retrievers() {
    let mut rng = ChaCha8Rng::seed_from_u64(83);
    let docs: Vec<(u64, Value, Vec<f32>)> = (0..60)
        .map(|pk| {
            let body = if pk % 3 == 0 {
                "apple pie"
            } else {
                "river stone"
            };
            let a = vec![
                rng.random_range(-1.0..1.0),
                rng.random_range(-1.0..1.0),
                1.0,
            ];
            (
                pk,
                json!({"body": body, "tag": TAGS[(pk % 4) as usize], "n": pk}),
                a,
            )
        })
        .collect();
    let ops: Vec<DocOp> = docs
        .iter()
        .map(|(pk, source, a)| upsert(*pk, source.clone(), Some(a.clone())))
        .collect();
    let setup = Setup::new(vec![ops[..40].to_vec()], ops[40..].to_vec()).await;
    let view = setup.view().await;
    let aggs = json!({"tags": {"terms": {"field": "tag"}}});
    let total = |response: &SearchResponse| -> u64 {
        counts(&response.aggregations.as_ref().expect("aggregations")["tags"])
            .values()
            .sum()
    };
    // A text query: every match, not only its k.
    let text = search(
        &view,
        SearchRequest {
            retrievers: vec![Retriever::Text {
                query: matching("body", "apple"),
                k: 3,
            }],
            aggregations: Some(aggs.clone()),
            ..SearchRequest::new("docs")
        },
    )
    .await;
    assert_eq!(total(&text), 20);
    // A vector search: its candidates.
    let vector = search(
        &view,
        SearchRequest {
            retrievers: vec![Retriever::Vector {
                field: "a".to_string(),
                query: vec![0.0, 0.0, 1.0],
                k: 7,
                params: AnnParams {
                    exact: true,
                    ..AnnParams::default()
                },
                filter: None,
            }],
            aggregations: Some(aggs.clone()),
            ..SearchRequest::new("docs")
        },
    )
    .await;
    assert_eq!(total(&vector), 7);
    // Filter only: the filter's matches.
    let filtered = search(
        &view,
        SearchRequest {
            filter: Some(Query::Range {
                field: "n".to_string(),
                gt: None,
                gte: Some(operon_query::FieldValue::I64(50)),
                lt: None,
                lte: None,
            }),
            aggregations: Some(aggs.clone()),
            ..SearchRequest::new("docs")
        },
    )
    .await;
    assert_eq!(total(&filtered), 10);
    setup.shutdown().await;
}

#[tokio::test]
async fn top_hits_carry_id_score_and_source() {
    let (_, b, docs) = fixtures().await;
    let view = b.view().await;
    let request = json!({
        "tags": {
            "terms": {"field": "tag", "size": 20},
            "aggs": {"top": {"top_hits": {
                "size": 2,
                "sort": [{"n": "desc"}],
                "_source": {"includes": ["title"]},
                "docvalue_fields": ["n"]
            }}}
        }
    });
    let result = run(&view, &request, AggDomain::Filter(None))
        .await
        .expect("aggregate");
    let buckets = result["tags"]["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), TAGS.len());
    for bucket in buckets {
        let tag = bucket["key"].as_str().expect("key");
        let mut expected: Vec<(i64, u64)> = docs
            .iter()
            .filter(|(_, s)| s["tag"] == tag)
            .map(|(pk, s)| (s["n"].as_i64().expect("n"), *pk))
            .collect();
        expected.sort_by(|a, b| b.cmp(a));
        let hits = bucket["top"]["hits"].as_array().expect("hits");
        assert_eq!(hits.len(), 2, "{tag}");
        for (hit, (n, pk)) in hits.iter().zip(&expected) {
            // Ties on `n` may come back in either order; `n` itself may not.
            let id = hit["_id"].as_u64().expect("_id");
            assert_eq!(docs[&id]["n"].as_i64(), Some(*n), "{tag}: {hit}");
            let _ = pk;
            assert_eq!(hit["_score"], Value::Null);
            assert_eq!(
                hit["_source"],
                json!({"title": docs[&id]["title"].clone()}),
                "{hit}"
            );
            assert_eq!(hit["fields"], json!({"n": [n]}));
            assert!(hit.get("sort").is_some());
        }
    }
    b.shutdown().await;
}

#[tokio::test]
async fn an_aggregation_over_a_field_added_later_skips_old_splits() {
    let fixture = TailFixture::start(agg_schema(), 1).await;
    let before: Vec<DocOp> = (0..5)
        .map(|pk| upsert(pk, json!({"tag": "old", "m": 1_000}), None))
        .collect();
    fixture.append_all(&before).await;
    fixture.apply_link().await;
    let mut next = agg_schema();
    next.fields.push(field("m", FieldKind::I64));
    next.version = 2;
    fixture
        .meta
        .client
        .update_collection_schema(fixture.cid, 1, next)
        .await
        .expect("add m");
    let after: Vec<DocOp> = (5..9)
        .map(|pk| upsert(pk, json!({"tag": "new", "m": pk}), None))
        .collect();
    let setup = Setup::with(fixture, vec![after], vec![upsert(9, json!({"m": 9}), None)]).await;
    let view = setup.view().await;
    assert_eq!(view.snapshot.splits().len(), 2);
    let result = run(
        &view,
        &json!({"m": {"stats": {"field": "m"}}}),
        AggDomain::Filter(None),
    )
    .await
    .expect("aggregate");
    assert_eq!(result["m"]["count"], 5);
    assert_eq!(
        result["m"]["sum"].as_f64(),
        Some((5 + 6 + 7 + 8 + 9) as f64)
    );
    setup.shutdown().await;
}

#[tokio::test]
async fn an_invalid_aggregation_is_invalid_argument() {
    let setup = Setup::new(
        vec![vec![upsert(1, json!({"tag": "x", "n": 1}), None)]],
        vec![],
    )
    .await;
    let view = setup.view().await;
    for request in [
        json!({"x": {"nope": {"field": "n"}}}),
        json!({"x": {"histogram": {"field": "n"}}}),
        json!({"x": {"terms": {"field": "tag"}, "aggs": {"y": {"top_hits": {"_source": 3}}}}}),
        json!({"x": {"terms": {"field": "tag"}, "aggs": {"y": {"top_hits": 3}}}}),
        json!({"x": {"terms": {"field": "tag"}, "aggs": {"y": {"top_hits": null}}}}),
        json!([1, 2]),
    ] {
        let err = run(&view, &request, AggDomain::Filter(None))
            .await
            .expect_err("invalid");
        match err {
            ServiceError::InvalidArgument(message) => {
                assert!(message.starts_with("aggregations: "), "{message}")
            }
            other => panic!("{request}: {other:?}"),
        }
    }
    setup.shutdown().await;
}

#[tokio::test]
async fn a_non_fast_aggregation_field_is_invalid_argument() {
    let setup = Setup::new(
        vec![vec![upsert(1, json!({"body": "apple", "n": 1}), None)]],
        vec![],
    )
    .await;
    let view = setup.view().await;
    for (request, field) in [
        (json!({"x": {"terms": {"field": "body"}}}), "body"),
        (json!({"x": {"stats": {"field": "nope"}}}), "nope"),
        (
            json!({"x": {"terms": {"field": "tag"}, "aggs": {"y": {"max": {"field": "title"}}}}}),
            "title",
        ),
    ] {
        let err = run(&view, &request, AggDomain::Filter(None))
            .await
            .expect_err("not fast");
        assert_eq!(
            err,
            ServiceError::InvalidArgument(format!("aggregation field {field} is not a fast field"))
        );
    }
    setup.shutdown().await;
}

// ----- highlighting -----

fn highlight_field(field: &str, pre: &str, post: &str, size: usize, n: usize) -> HighlightField {
    HighlightField {
        field: field.to_string(),
        pre_tag: pre.to_string(),
        post_tag: post.to_string(),
        fragment_size: size,
        number_of_fragments: n,
    }
}

fn highlighted(query: Query, fields: Vec<HighlightField>) -> SearchRequest {
    SearchRequest {
        retrievers: vec![Retriever::Text { query, k: 100 }],
        highlight: Some(Highlight { fields }),
        limit: 100,
        ..SearchRequest::new("docs")
    }
}

#[tokio::test]
async fn highlight_marks_query_terms_with_the_tags() {
    let setup = Setup::new(
        vec![vec![upsert(
            1,
            json!({"title": "The quick brown fox"}),
            None,
        )]],
        vec![upsert(2, json!({"title": "A slow turtle"}), None)],
    )
    .await;
    let view = setup.view().await;
    let response = search(
        &view,
        highlighted(
            matching("title", "quick fox"),
            vec![highlight_field("title", "<b>", "</b>", 100, 5)],
        ),
    )
    .await;
    assert_eq!(response.hits.len(), 1);
    assert_eq!(
        response.hits[0].highlight,
        BTreeMap::from([(
            "title".to_string(),
            vec!["The <b>quick</b> brown <b>fox</b>".to_string()]
        )])
    );
    // No HTML escaping, and a non-text field is refused.
    let err = planner()
        .search(
            view.clone(),
            highlighted(
                matching("title", "quick"),
                vec![highlight_field("tag", "<b>", "</b>", 100, 5)],
            ),
        )
        .await
        .expect_err("not text");
    assert_eq!(
        err,
        ServiceError::InvalidArgument("highlighting needs a text field: tag".to_string())
    );
    setup.shutdown().await;
}

#[tokio::test]
async fn number_of_fragments_zero_highlights_whole_values() {
    let long = "the fox ran far away from the barn and then the dog chased the fox again.";
    let setup = Setup::new(
        vec![vec![upsert(
            1,
            json!({"title": [long, "no match here", "a fox."]}),
            None,
        )]],
        vec![],
    )
    .await;
    let view = setup.view().await;
    let whole = search(
        &view,
        highlighted(
            matching("title", "fox"),
            vec![highlight_field("title", "[", "]", 20, 0)],
        ),
    )
    .await;
    assert_eq!(
        whole.hits[0].highlight["title"],
        vec![
            "the [fox] ran far away from the barn and then the dog chased the [fox] again."
                .to_string(),
            "a [fox].".to_string(),
        ]
    );
    // With fragments: one fragment of at most ~20 chars per value, the
    // first `number_of_fragments` of them.
    let fragments = search(
        &view,
        highlighted(
            matching("title", "fox"),
            vec![highlight_field("title", "[", "]", 20, 1)],
        ),
    )
    .await;
    let snippets = &fragments.hits[0].highlight["title"];
    assert_eq!(snippets.len(), 1);
    assert!(snippets[0].contains("[fox]"), "{snippets:?}");
    assert!(snippets[0].len() < long.len());
    setup.shutdown().await;
}

#[tokio::test]
async fn highlights_are_identical_across_placements() {
    let (a, b, _) = fixtures().await;
    let (a_view, b_view) = (a.view().await, b.view().await);
    let request = highlighted(
        matching("body", "apple river stone"),
        vec![
            highlight_field("body", "<em>", "</em>", 18, 2),
            highlight_field("title", "<em>", "</em>", 100, 5),
        ],
    );
    let on_a = search(&a_view, request.clone()).await;
    let on_b = search(&b_view, request).await;
    assert!(!on_a.hits.is_empty());
    let by_pk = |response: &SearchResponse| -> BTreeMap<PrimaryKey, BTreeMap<String, Vec<String>>> {
        response
            .hits
            .iter()
            .map(|hit| (hit.pk.clone(), hit.highlight.clone()))
            .collect()
    };
    assert!(
        on_a.hits
            .iter()
            .all(|hit| hit.highlight.contains_key("body"))
    );
    assert_eq!(by_pk(&on_a), by_pk(&on_b));
    a.shutdown().await;
    b.shutdown().await;
}
