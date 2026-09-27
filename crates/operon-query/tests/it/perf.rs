//! M1.6 Task 10 (D92): the per-response `performance` block of
//! `CollectionService::search`.

use std::sync::Arc;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::json;

use crate::common::{
    Fixture, GATE_NS, PLACEMENTS, Probe, battery, build_placements, random_history, response_json,
    tail_schema, upsert,
};
use operon_collection::{CollectionConfig, DocOp, PrimaryKey, SparseVector};
use operon_common::meta::{Consistency, MetaStore};
use operon_query::exec::SearchPlanner;
use operon_query::hot::{HotTier, NoHotTier, RequestHot};
use operon_query::perf::Performance;
use operon_query::{
    AnnParams, BoolOperator, CollectionService, FieldValue, Fusion, Query, ReadConsistency,
    Retriever, SearchRequest, SearchResponse, ServiceConfig, SparseParams, WriteOptions,
};

const NS: &str = "acme";
const DOCS: &str = "docs";

async fn with_docs() -> (Fixture, Arc<CollectionService>) {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, DOCS, tail_schema(), None)
        .await
        .expect("create the collection");
    (f, service)
}

async fn write(service: &CollectionService, ops: Vec<DocOp>) {
    service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
}

fn alpha_docs(range: std::ops::Range<u64>) -> Vec<DocOp> {
    range
        .map(|i| upsert(i, json!({"t": format!("alpha doc {i}"), "n": i as i64})))
        .collect()
}

fn alpha() -> Query {
    Query::Match {
        field: "t".to_string(),
        text: "alpha".to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

fn alpha_search(consistency: ReadConsistency) -> SearchRequest {
    let mut request = SearchRequest::new(DOCS);
    request.retrievers = vec![Retriever::Text {
        query: alpha(),
        k: 100,
    }];
    request.limit = 100;
    request.consistency = consistency;
    request
}

async fn search(service: &CollectionService, consistency: ReadConsistency) -> SearchResponse {
    service
        .search(NS, alpha_search(consistency))
        .await
        .expect("search")
}

/// Whether `ms` has at most microsecond precision.
fn microseconds(ms: f64) -> bool {
    let us = ms * 1000.0;
    (us - us.round()).abs() < 1e-6
}

#[tokio::test]
async fn timings_are_present_and_consistent() {
    let (f, service) = with_docs().await;
    write(&service, alpha_docs(0..5)).await;
    let response = search(&service, ReadConsistency::Strong).await;
    let p = &response.performance;
    assert!(p.server_total_ms > 0.0, "{p:?}");
    assert!(p.planning_ms > 0.0 && p.execution_ms > 0.0, "{p:?}");
    assert_eq!(p.queue_ms, 0.0, "no queue in M1");
    assert!(p.server_total_ms >= p.planning_ms + p.execution_ms, "{p:?}");
    for ms in [p.server_total_ms, p.queue_ms, p.planning_ms, p.execution_ms] {
        assert!(ms >= 0.0 && microseconds(ms), "{ms} in {p:?}");
    }
    // The block serializes under its JSON keys.
    let value = serde_json::to_value(&response).expect("serialize");
    for key in [
        "server_total_ms",
        "queue_ms",
        "planning_ms",
        "execution_ms",
        "manifest_version",
        "tail_records",
        "stale_records",
        "rows_scanned",
        "cache",
        "object_store_requests",
    ] {
        assert!(
            value["performance"].get(key).is_some(),
            "{key} in {}",
            value["performance"]
        );
    }
    assert_eq!(
        serde_json::from_value::<SearchResponse>(value)
            .expect("round trip")
            .performance,
        response.performance
    );
    f.shutdown().await;
}

#[tokio::test]
async fn tail_records_counts_the_unapplied_writes() {
    let (f, service) = with_docs().await;
    write(&service, alpha_docs(0..5)).await;
    let before = search(&service, ReadConsistency::Strong).await.performance;
    assert_eq!(before.tail_records, 5, "{before:?}");
    assert_eq!(before.manifest_version, 0, "nothing applied yet");

    f.settle().await;
    let after = search(&service, ReadConsistency::Strong).await.performance;
    assert_eq!(after.tail_records, 0, "{after:?}");
    assert!(after.manifest_version > 0, "{after:?}");

    // A delete is an overlay entry too.
    write(&service, vec![DocOp::Delete(PrimaryKey::U64(1))]).await;
    let deleted = search(&service, ReadConsistency::Strong).await.performance;
    assert_eq!(deleted.tail_records, 1, "{deleted:?}");
    f.shutdown().await;
}

#[tokio::test]
async fn stale_records_is_set_only_for_eventual() {
    let (f, service) = with_docs().await;
    write(&service, alpha_docs(0..2)).await;
    let strong = search(&service, ReadConsistency::Strong).await.performance;
    assert_eq!(strong.stale_records, None, "{strong:?}");

    let cid = service.get_collection(NS, DOCS).await.expect("info").id;
    let tail = service
        .reads()
        .tail(cid)
        .expect("the strong read runs the tail");
    tail.pause_fetch(true);
    tail.wait_held().await;
    write(&service, alpha_docs(2..5)).await;
    let eventual = search(&service, ReadConsistency::Eventual)
        .await
        .performance;
    assert_eq!(eventual.stale_records, Some(3), "{eventual:?}");
    assert_eq!(eventual.tail_records, 2, "{eventual:?}");

    tail.pause_fetch(false);
    let strong = search(&service, ReadConsistency::Strong).await.performance;
    assert_eq!(strong.stale_records, None, "{strong:?}");
    assert_eq!(strong.tail_records, 5, "{strong:?}");
    f.shutdown().await;
}

fn kinds(p: &Performance) -> Vec<&str> {
    p.rows_scanned.iter().map(|r| r.kind.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_scanned_has_one_entry_per_retriever() {
    let mut rng = ChaCha8Rng::seed_from_u64(10);
    let history = random_history(&mut rng, 100);
    let f = Fixture::start().await;
    build_placements(&f, &history, &[50], 60).await;
    let service = f.service();
    let title = Query::Match {
        field: "title".to_string(),
        text: "w00 w01 w02".to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    };
    let sparse = SparseVector::new(vec![1, 4, 7, 12, 20], vec![0.5, 0.8, 0.3, 1.0, 0.6])
        .expect("a sparse vector");
    let mut request = SearchRequest::new("split");
    request.retrievers = vec![
        Retriever::Text {
            query: title.clone(),
            k: 30,
        },
        Retriever::Vector {
            field: "v".to_string(),
            query: vec![0.3, -0.5, 0.8, 0.1],
            k: 30,
            params: AnnParams::default(),
            filter: None,
        },
        Retriever::Sparse {
            field: "s".to_string(),
            query: sparse,
            k: 30,
            filter: None,
            params: SparseParams { idf_corpus: None },
        },
    ];
    request.fusion = Some(Fusion::Rrf { k: 60 });
    request.limit = 10;
    let hybrid = service
        .search(GATE_NS, request)
        .await
        .expect("hybrid")
        .performance;
    assert_eq!(kinds(&hybrid), ["text", "vector", "sparse"], "{hybrid:?}");
    for rows in &hybrid.rows_scanned {
        assert!(rows.candidates > 0, "{rows:?}");
        assert!(rows.brute_force <= rows.candidates, "{rows:?}");
        // `split` holds applied and unapplied writes: the tail is scored
        // without an index.
        assert!(rows.brute_force > 0, "{rows:?}");
    }
    // No vector index was built: every durable candidate is brute force.
    assert_eq!(
        hybrid.rows_scanned[1].brute_force, hybrid.rows_scanned[1].candidates,
        "{hybrid:?}"
    );

    let mut text_only = SearchRequest::new("split");
    text_only.retrievers = vec![Retriever::Text {
        query: title,
        k: 30,
    }];
    let text = service.search(GATE_NS, text_only).await.expect("text");
    assert_eq!(kinds(&text.performance), ["text"]);

    let mut filtered = SearchRequest::new("split");
    filtered.filter = Some(Query::Term {
        field: "tag".to_string(),
        value: FieldValue::Str("red".to_string()),
    });
    let filter = service.search(GATE_NS, filtered).await.expect("filter");
    assert_eq!(kinds(&filter.performance), ["filter"]);
    assert!(
        filter.performance.rows_scanned[0].candidates > 0,
        "{:?}",
        filter.performance
    );
    f.shutdown().await;
}

#[tokio::test]
async fn a_split_only_text_query_counts_its_cache_reads_exactly() {
    let (f, service) = with_docs().await;
    write(&service, alpha_docs(0..20)).await;
    f.settle().await;
    let cold = f
        .cold_service(CollectionConfig::default(), ServiceConfig::default())
        .await;

    let first = search(&cold, ReadConsistency::Strong).await.performance;
    assert_eq!(first.tail_records, 0, "only splits: {first:?}");
    assert!(first.cache.miss_bytes > 0, "{first:?}");
    assert!(first.object_store_requests.get > 0, "{first:?}");

    let second = search(&cold, ReadConsistency::Strong).await.performance;
    assert_eq!(second.cache.miss_bytes, 0, "{second:?}");
    assert!(second.cache.hit_bytes > 0, "{second:?}");
    assert!(second.cache.hit_bytes <= first.cache.miss_bytes + first.cache.hit_bytes);
    assert_eq!(second.cache.hit_ratio, Some(1.0), "{second:?}");
    assert_eq!(second.object_store_requests.get, 0, "{second:?}");
    assert_eq!(second.object_store_requests.head, 0, "{second:?}");

    // A read of nothing cached and nothing fetched has no ratio.
    let empty = cold
        .search(NS, SearchRequest::new(DOCS))
        .await
        .expect("match all")
        .performance;
    if empty.cache.hit_bytes + empty.cache.miss_bytes == 0 {
        assert_eq!(empty.cache.hit_ratio, None, "{empty:?}");
    }
    cold.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_does_not_change_results() {
    let mut rng = ChaCha8Rng::seed_from_u64(55);
    let history = random_history(&mut rng, 100);
    let f = Fixture::start().await;
    build_placements(&f, &history, &[30, 70], 60).await;
    let service = f.service();
    let config = ServiceConfig::default();
    let planner = SearchPlanner::new(config.search.clone(), config.ann.clone());
    let ns = f
        .meta
        .client
        .namespace_by_name(Consistency::Local, GATE_NS)
        .await
        .expect("read")
        .expect("the namespace")
        .id;
    let mut probes = 0;
    for placement in PLACEMENTS {
        let collection = f
            .meta
            .client
            .resolve_collection(Consistency::Local, ns, placement)
            .await
            .expect("read")
            .expect("the collection");
        for (name, probe) in battery(placement) {
            let Probe::Search(request) = probe else {
                continue;
            };
            let scoped = service.search(GATE_NS, (*request).clone()).await;
            // The planner alone: no service, so no scope.
            let hot = RequestHot {
                enabled: false,
                used: Default::default(),
            };
            let tier: Arc<dyn HotTier> = Arc::new(NoHotTier);
            let view = service
                .reads()
                .view(ns, &collection, &request.consistency, &hot, tier)
                .await
                .expect("view");
            let bare = planner.search(Arc::new(view), (*request).clone()).await;
            match (scoped, bare) {
                (Ok(scoped), Ok(bare)) => {
                    assert_eq!(
                        response_json(&scoped),
                        response_json(&bare),
                        "{name} over {placement}"
                    );
                    probes += 1;
                }
                (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{name}"),
                (a, b) => panic!("{name} over {placement}: {a:?} vs {b:?}"),
            }
        }
    }
    assert!(probes > 50, "{probes} probes compared");
    f.shutdown().await;
}
