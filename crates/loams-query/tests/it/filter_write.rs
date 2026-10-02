//! Native delete-by-filter and patch-by-filter (plan M1.5 Task 9a, D87):
//! one pin, atomic batches in primary-key order, the limit, the cursor, the
//! deadline and backpressure.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::common::{Fixture, WAIT, tail_schema, upsert};
use loams_collection::{CollectionConfig, DocOp, PrimaryKey};
use loams_query::filter_write::over_limit;
use loams_query::{
    BackpressureConfig, CollectionService, FieldValue, FilterWriteOptions, FilterWriteResult,
    PatchSpec, Projection, Query, ReadConsistency, ServiceConfig, ServiceError, WriteOptions,
};
use serde_json::{Value, json};

const NS: &str = "acme";
const DOCS: &str = "docs";

fn tag(value: &str) -> Query {
    Query::Term {
        field: "tag".to_string(),
        value: FieldValue::Str(value.to_string()),
    }
}

fn batches_of(n: usize) -> ServiceConfig {
    ServiceConfig {
        filter_write_batch: n,
        ..ServiceConfig::default()
    }
}

async fn with_docs(config: ServiceConfig) -> (Fixture, Arc<CollectionService>) {
    let f = Fixture::start_with(config).await;
    let service = f.service();
    service
        .create_collection(NS, DOCS, tail_schema(), None)
        .await
        .expect("create the collection");
    (f, service)
}

/// Upserts `keys` with `tag` and `n = key`.
async fn put(service: &CollectionService, keys: impl IntoIterator<Item = u64>, tag: &str) {
    let ops: Vec<DocOp> = keys
        .into_iter()
        .map(|i| {
            upsert(
                i,
                json!({"t": format!("doc {i}"), "tag": tag, "n": i as i64}),
            )
        })
        .collect();
    service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
}

async fn count(service: &CollectionService, filter: Option<Query>) -> u64 {
    service
        .count(NS, DOCS, filter, ReadConsistency::Strong)
        .await
        .expect("count")
}

async fn source(service: &CollectionService, key: u64) -> Option<Value> {
    let docs = service
        .get(
            NS,
            DOCS,
            &[PrimaryKey::U64(key)],
            &Projection::default(),
            ReadConsistency::Strong,
        )
        .await
        .expect("get");
    docs.into_iter()
        .next()
        .flatten()
        .map(|doc| Value::Object(doc.source.unwrap_or_default()))
}

fn partial(max_rows: u64) -> FilterWriteOptions {
    FilterWriteOptions {
        max_rows: Some(max_rows),
        allow_partial: true,
        ..FilterWriteOptions::default()
    }
}

fn next(result: &FilterWriteResult, max_rows: u64) -> FilterWriteOptions {
    FilterWriteOptions {
        max_rows: Some(max_rows),
        cursor: result.cursor.clone(),
        ..FilterWriteOptions::default()
    }
}

#[tokio::test]
async fn delete_by_filter_deletes_exactly_the_matches_at_the_pin() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    let (matching, others): (Vec<u64>, Vec<u64>) = (0..30).partition(|i| i % 5 < 2);
    assert_eq!(matching.len(), 12);
    put(&service, matching.clone(), "a").await;
    put(&service, others.clone(), "b").await;
    let result = service
        .delete_by_filter(NS, DOCS, tag("a"), FilterWriteOptions::default())
        .await
        .expect("delete by filter");
    assert_eq!(result.matched, 12);
    assert_eq!(result.affected, 12);
    assert_eq!(result.batches, 1);
    assert!(!result.rows_remaining);
    assert_eq!(result.cursor, None);
    let at = ReadConsistency::AtLeast(result.token.clone());
    let left = service
        .count(NS, DOCS, Some(tag("a")), at.clone())
        .await
        .expect("count");
    assert_eq!(left, 0);
    let all = service.count(NS, DOCS, None, at).await.expect("count");
    assert_eq!(all, 18);
    for key in others {
        assert!(source(&service, key).await.is_some(), "{key} stays");
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn documents_written_after_the_pin_are_not_touched() {
    let (f, service) = with_docs(batches_of(500)).await;
    put(&service, 0..3_000, "a").await;
    let deleting = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .delete_by_filter(NS, DOCS, tag("a"), FilterWriteOptions::default())
                .await
        })
    };
    // After the first batch, 100 more matching documents.
    let deadline = std::time::Instant::now() + WAIT;
    while count(&service, Some(tag("a"))).await == 3_000 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    put(&service, 10_000..10_100, "a").await;
    let result = deleting.await.expect("join").expect("delete by filter");
    assert_eq!(result.matched, 3_000);
    assert_eq!(result.affected, 3_000);
    assert_eq!(result.batches, 6);
    assert_eq!(count(&service, Some(tag("a"))).await, 100);
    for key in [10_000, 10_050, 10_099] {
        assert!(source(&service, key).await.is_some(), "{key} stays");
    }
    f.shutdown().await;
}

#[tokio::test]
async fn a_key_updated_after_the_pin_is_still_deleted_in_m1() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    put(&service, [1, 2], "a").await;
    let first = service
        .delete_by_filter(NS, DOCS, tag("a"), partial(1))
        .await
        .expect("first call");
    assert_eq!(first.affected, 1);
    assert!(first.rows_remaining);
    // Key 2 stops matching after the pin: M1 deletes it anyway (rule 5).
    put(&service, [2], "b").await;
    let second = service
        .delete_by_filter(NS, DOCS, tag("a"), next(&first, 1))
        .await
        .expect("second call");
    assert_eq!(second.affected, 1);
    assert!(!second.rows_remaining);
    assert_eq!(source(&service, 2).await, None);
    f.shutdown().await;
}

#[tokio::test]
async fn patch_by_filter_merges_into_the_current_document_and_never_creates() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    put(&service, 1..=5, "a").await;
    put(&service, [9], "b").await;
    let patch = PatchSpec::merge_deep(
        json!({"extra": "x"})
            .as_object()
            .cloned()
            .expect("an object"),
    );
    let first = service
        .patch_by_filter(NS, DOCS, tag("a"), patch.clone(), partial(2))
        .await
        .expect("first call");
    assert_eq!((first.matched, first.affected), (5, 2));
    // Between the calls: key 3 is deleted, key 4 changes another field.
    service
        .write(
            NS,
            DOCS,
            vec![DocOp::Delete(PrimaryKey::U64(3))],
            WriteOptions::default(),
        )
        .await
        .expect("delete");
    service
        .write(
            NS,
            DOCS,
            vec![upsert(4, json!({"t": "changed", "tag": "a", "n": 40}))],
            WriteOptions::default(),
        )
        .await
        .expect("update");
    let second = service
        .patch_by_filter(NS, DOCS, tag("a"), patch, next(&first, 10))
        .await
        .expect("second call");
    // Key 3 is `NotFound` and not counted; it stays deleted.
    assert_eq!((second.matched, second.affected, second.written), (5, 2, 3));
    assert!(!second.rows_remaining);
    assert_eq!(source(&service, 3).await, None);
    assert_eq!(
        source(&service, 4).await,
        Some(json!({"t": "changed", "tag": "a", "n": 40, "extra": "x"}))
    );
    for key in [1, 2, 5] {
        assert_eq!(
            source(&service, key).await,
            Some(json!({"t": format!("doc {key}"), "tag": "a", "n": key, "extra": "x"}))
        );
    }
    assert_eq!(
        source(&service, 9).await,
        Some(json!({"t": "doc 9", "tag": "b", "n": 9}))
    );
    f.shutdown().await;
}

#[tokio::test]
async fn over_the_limit_fails_before_writing_unless_allow_partial() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    put(&service, 0..25, "a").await;
    let err = service
        .delete_by_filter(
            NS,
            DOCS,
            tag("a"),
            FilterWriteOptions {
                max_rows: Some(10),
                ..FilterWriteOptions::default()
            },
        )
        .await
        .expect_err("over the limit");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");
    assert_eq!(over_limit(&err), Some((25, 10)), "{err}");
    assert_eq!(count(&service, None).await, 25);
    let result = service
        .delete_by_filter(NS, DOCS, tag("a"), partial(10))
        .await
        .expect("partial");
    assert_eq!((result.matched, result.affected), (25, 10));
    assert!(result.rows_remaining);
    assert!(result.cursor.is_some());
    assert_eq!(count(&service, None).await, 15);
    f.shutdown().await;
}

#[tokio::test]
async fn the_cursor_continues_at_the_same_pin() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    put(&service, (0..25).map(|i| i * 2), "a").await;
    let mut opts = partial(10);
    let mut affected = Vec::new();
    let mut late = 1;
    loop {
        let result = service
            .delete_by_filter(NS, DOCS, tag("a"), opts)
            .await
            .expect("filter write");
        assert_eq!(result.matched, 25);
        affected.push(result.affected);
        // Documents written between the calls, between the pinned keys.
        put(&service, [late, late + 2], "a").await;
        late += 4;
        if !result.rows_remaining {
            assert_eq!(result.cursor, None);
            break;
        }
        opts = next(&result, 10);
    }
    assert_eq!(affected, vec![10, 10, 5]);
    assert_eq!(count(&service, Some(tag("a"))).await, 6);
    f.shutdown().await;
}

#[tokio::test]
async fn a_cursor_whose_pin_expired_is_not_found_pin() {
    let expiring = CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = Fixture::start_configured(ServiceConfig::default(), expiring).await;
    let service = f.service();
    service
        .create_collection(NS, DOCS, tail_schema(), None)
        .await
        .expect("create the collection");
    put(&service, 0..25, "a").await;
    f.settle().await;
    let first = service
        .delete_by_filter(NS, DOCS, tag("a"), partial(10))
        .await
        .expect("first call");
    assert!(first.rows_remaining);
    // Two more commits: the pinned manifest is no longer retained.
    for round in 0..2 {
        put(&service, [100 + round], "b").await;
        count(&service, None).await;
        f.settle().await;
    }
    let err = service
        .delete_by_filter(NS, DOCS, tag("a"), next(&first, 10))
        .await
        .expect_err("the pin expired");
    assert!(
        matches!(err, ServiceError::NotFound { kind: "pin", .. }),
        "{err:?}"
    );
    f.shutdown().await;
}

#[tokio::test]
async fn each_batch_is_atomic() {
    let (f, service) = with_docs(batches_of(10)).await;
    put(&service, 0..25, "a").await;
    // `n` is a long: the patch fails validation, and no key of the batch is
    // written.
    let patch = PatchSpec {
        delete_keys: vec!["t".to_string()],
        ..PatchSpec::merge_deep(
            json!({"n": "not a number"})
                .as_object()
                .cloned()
                .expect("an object"),
        )
    };
    let err = service
        .patch_by_filter(NS, DOCS, tag("a"), patch, FilterWriteOptions::default())
        .await
        .expect_err("a schema violation");
    assert!(
        matches!(err, ServiceError::SchemaViolation { .. }),
        "{err:?}"
    );
    for key in 0..25 {
        assert_eq!(
            source(&service, key).await,
            Some(json!({"t": format!("doc {key}"), "tag": "a", "n": key})),
            "{key}"
        );
    }
    f.shutdown().await;
}

#[tokio::test]
async fn the_deadline_returns_rows_remaining_with_a_cursor() {
    let (f, service) = with_docs(batches_of(10)).await;
    put(&service, 0..25, "a").await;
    let result = service
        .delete_by_filter(
            NS,
            DOCS,
            tag("a"),
            FilterWriteOptions {
                deadline: Some(Duration::ZERO),
                ..FilterWriteOptions::default()
            },
        )
        .await
        .expect("filter write");
    // One batch always goes out; then the deadline has passed.
    assert_eq!(
        (result.matched, result.affected, result.batches),
        (25, 10, 1)
    );
    assert!(result.rows_remaining);
    assert_eq!(result.retry_after_ms, None);
    let cursor = result.cursor.clone().expect("a cursor");
    assert_eq!(cursor.after, PrimaryKey::U64(9));
    assert_eq!(cursor.manifest_version, result.pin.manifest_version);
    let rest = service
        .delete_by_filter(
            NS,
            DOCS,
            tag("a"),
            FilterWriteOptions {
                cursor: Some(cursor),
                ..FilterWriteOptions::default()
            },
        )
        .await
        .expect("the rest");
    assert_eq!((rest.affected, rest.batches), (15, 2));
    assert!(!rest.rows_remaining);
    assert_eq!(count(&service, None).await, 0);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_batch_waits_for_retry_after() {
    let config = ServiceConfig {
        filter_write_batch: 10,
        backpressure: BackpressureConfig {
            max_unapplied_records: 15,
            refresh_interval: Duration::ZERO,
            min_retry_after: Duration::from_millis(50),
            max_retry_after: Duration::from_millis(200),
            ..BackpressureConfig::default()
        },
        ..ServiceConfig::default()
    };
    let (f, service) = with_docs(config).await;
    for keys in [0..10, 10..20, 20..30] {
        put(&service, keys, "a").await;
        f.settle().await;
    }
    // The link runs now and then every 500 ms: the third batch meets 20
    // unapplied records and waits for it.
    f.start_worker_every(Duration::from_millis(500));
    let result = service
        .delete_by_filter(NS, DOCS, tag("a"), FilterWriteOptions::default())
        .await
        .expect("filter write");
    assert_eq!((result.affected, result.batches), (30, 3));
    assert!(!result.rows_remaining);
    assert!(
        service
            .backlog_monitor()
            .counters()
            .throttled_writes
            .load(Ordering::Relaxed)
            >= 1
    );
    f.shutdown().await;
}

#[tokio::test]
async fn eventual_and_pinned_consistency_are_refused() {
    let (f, service) = with_docs(ServiceConfig::default()).await;
    put(&service, 0..3, "a").await;
    let pinned = service.pin(NS, DOCS).await.expect("pin").consistency();
    for consistency in [ReadConsistency::Eventual, pinned] {
        let err = service
            .delete_by_filter(
                NS,
                DOCS,
                tag("a"),
                FilterWriteOptions {
                    consistency,
                    ..FilterWriteOptions::default()
                },
            )
            .await
            .expect_err("refused");
        assert!(
            matches!(&err, ServiceError::InvalidArgument(m) if m.contains("fresh pin")),
            "{err:?}"
        );
    }
    let err = service
        .delete_by_filter(NS, DOCS, tag("a"), partial(0))
        .await
        .expect_err("max_rows 0");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");
    assert_eq!(count(&service, None).await, 3);
    f.shutdown().await;
}
