//! The unapplied-data budget and write backpressure (plan M1.3 Task 15,
//! D86).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::common::{Fixture, TailFixture, field, tail_schema, upsert};
use loams_collection::{CollectionSchema, DocOp, DynamicMapping, FieldKind, PrimaryKey};
use loams_common::meta::{Collection, Consistency, EntryKind, IndexEntry, MetaStore};
use loams_common::{CollectionId, NamespaceId};
use loams_query::backlog::{retry_after, unapplied_bytes};
use loams_query::read::{ReadConfig, ReadView};
use loams_query::tail::{TailConfig, TailState};
use loams_query::{
    Backlog, BacklogMonitor, BackpressureConfig, BackpressureState, CollectionService, Override,
    ReadConsistency, ServiceConfig, ServiceError, WriteOptions,
};
use serde_json::{Value, json};

const NS: &str = "acme";
const DOCS: &str = "docs";

/// Backpressure with `adjust` applied, measured afresh before every
/// admission.
fn budget(adjust: impl FnOnce(&mut BackpressureConfig)) -> ServiceConfig {
    let mut backpressure = BackpressureConfig {
        refresh_interval: Duration::ZERO,
        ..BackpressureConfig::default()
    };
    adjust(&mut backpressure);
    ServiceConfig {
        backpressure,
        ..ServiceConfig::default()
    }
}

/// A fixture with collection `acme/docs` of `schema` under `config`.
async fn with_docs(
    config: ServiceConfig,
    schema: CollectionSchema,
) -> (Fixture, Arc<CollectionService>, NamespaceId, Collection) {
    let f = Fixture::start_with(config).await;
    let service = f.service();
    service
        .create_collection(NS, DOCS, schema, None)
        .await
        .expect("create the collection");
    let (ns, collection) = resolve(&f).await;
    (f, service, ns, collection)
}

async fn resolve(f: &Fixture) -> (NamespaceId, Collection) {
    let ns = f
        .meta
        .client
        .namespace_by_name(Consistency::Linearizable, NS)
        .await
        .expect("read")
        .expect("the namespace")
        .id;
    let collection = f
        .meta
        .client
        .resolve_collection(Consistency::Linearizable, ns, DOCS)
        .await
        .expect("read")
        .expect("the collection");
    (ns, collection)
}

fn ops(keys: std::ops::Range<u64>) -> Vec<DocOp> {
    keys.map(|i| upsert(i, json!({"t": format!("doc {i}"), "n": i as i64})))
        .collect()
}

async fn write(service: &CollectionService, ops: Vec<DocOp>) -> Result<Backlog, ServiceError> {
    write_with(service, ops, Override::None).await
}

async fn write_with(
    service: &CollectionService,
    ops: Vec<DocOp>,
    backpressure: Override,
) -> Result<Backlog, ServiceError> {
    service
        .write(
            NS,
            DOCS,
            ops,
            WriteOptions {
                backpressure,
                ..WriteOptions::default()
            },
        )
        .await
        .map(|result| result.backlog)
}

async fn high_watermarks(f: &Fixture, cid: CollectionId) -> Vec<u64> {
    f.meta
        .client
        .collection_head(Consistency::Linearizable, cid)
        .await
        .expect("read")
        .expect("the collection")
        .high_watermarks
}

fn retry_ms(err: &ServiceError) -> u64 {
    match err {
        ServiceError::ResourceExhausted { retry_after_ms, .. } => *retry_after_ms,
        other => panic!("expected ResourceExhausted, got {other:?}"),
    }
}

#[tokio::test]
async fn backlog_counts_records_and_bytes_past_applied() {
    let (f, service, ns, collection) = with_docs(budget(|_| {}), tail_schema()).await;
    for i in 0..3 {
        write(&service, ops(i * 10..i * 10 + 4))
            .await
            .expect("write");
    }
    let monitor = service.backlog_monitor();
    let backlog = monitor.backlog(ns, &collection).await.expect("backlog");
    assert_eq!(backlog.records, 12);
    // Every index entry of the stream, since nothing is applied.
    let mut bytes = 0;
    for partition in 0..collection.partitions {
        if let Some(index) = f
            .meta
            .client
            .partition_index(
                Consistency::Linearizable,
                collection.stream,
                partition,
                0,
                None,
            )
            .await
            .expect("index")
        {
            bytes += index
                .entries()
                .map(|e| e.byte_range.end - e.byte_range.start)
                .sum::<u64>();
        }
    }
    assert!(bytes > 0);
    assert_eq!(backlog.bytes, bytes);

    f.apply_link().await;
    let backlog = monitor.backlog(ns, &collection).await.expect("backlog");
    assert_eq!(backlog, Backlog::default());
    f.shutdown().await;
}

#[test]
fn bytes_are_pro_rata_for_an_entry_that_straddles_applied() {
    let entry = |base_offset: u64, records: u32, start: u64, end: u64| IndexEntry {
        kind: EntryKind::Wal,
        base_offset,
        records,
        object: "wal/x".to_string(),
        byte_range: start..end,
        max_timestamp_ms: 0,
    };
    let entries = [
        entry(0, 10, 0, 1000),    // [0, 10)
        entry(10, 3, 1000, 1100), // [10, 13)
        entry(13, 7, 5000, 5700), // [13, 20)
    ];
    // Nothing applied: every byte.
    assert_eq!(unapplied_bytes(&entries, 0), 1800);
    // 4 of the first entry's 10 records applied: 6/10 of 1000.
    assert_eq!(unapplied_bytes(&entries, 4), 600 + 100 + 700);
    // 1 of 3 records of the second entry above applied 12: ceil(100/3).
    assert_eq!(unapplied_bytes(&entries, 12), 34 + 700);
    // An entry ending at applied adds nothing.
    assert_eq!(unapplied_bytes(&entries, 13), 700);
    assert_eq!(unapplied_bytes(&entries, 20), 0);
}

#[tokio::test]
async fn a_write_at_the_record_budget_is_refused_with_retry_after() {
    let (f, service, _, collection) =
        with_docs(budget(|b| b.max_unapplied_records = 10), tail_schema()).await;
    write(&service, ops(0..10)).await.expect("under the budget");
    let before = high_watermarks(&f, collection.id).await;
    let err = write(&service, ops(10..11))
        .await
        .expect_err("at the budget");
    let wait = retry_ms(&err);
    assert!((1_000..=30_000).contains(&wait), "{wait}");
    assert!(err.is_retryable());
    assert!(
        err.to_string().contains("10 records") && err.to_string().contains("budget 10 records"),
        "{err}"
    );
    assert_eq!(high_watermarks(&f, collection.id).await, before);
    assert_eq!(
        service
            .backlog_monitor()
            .counters()
            .throttled_writes
            .load(Ordering::Relaxed),
        1
    );
    f.apply_link().await;
    write(&service, ops(10..11)).await.expect("applied");
    f.shutdown().await;
}

#[tokio::test]
async fn a_write_at_the_byte_budget_is_refused() {
    let (f, service, ns, collection) = with_docs(budget(|_| {}), tail_schema()).await;
    write(&service, ops(0..5)).await.expect("write");
    let bytes = service
        .backlog_monitor()
        .backlog(ns, &collection)
        .await
        .expect("backlog")
        .bytes;
    f.shutdown().await;

    // A byte budget the first write reaches exactly.
    let (f, service, _, _) =
        with_docs(budget(|b| b.max_unapplied_bytes = bytes), tail_schema()).await;
    write(&service, ops(0..5)).await.expect("under the budget");
    let err = write(&service, ops(5..6))
        .await
        .expect_err("at the byte budget");
    retry_ms(&err);
    f.shutdown().await;
}

#[tokio::test]
async fn a_refused_write_proposes_no_dynamic_field() {
    let schema = CollectionSchema::new(
        vec![field("t", FieldKind::Keyword)],
        Vec::new(),
        DynamicMapping::Map,
    );
    let (f, service, _, _) = with_docs(budget(|b| b.max_unapplied_records = 1), schema).await;
    write(&service, vec![upsert(1, json!({"t": "a"}))])
        .await
        .expect("under the budget");
    let version = service
        .get_collection(NS, DOCS)
        .await
        .expect("info")
        .schema
        .version;
    let err = write(&service, vec![upsert(2, json!({"t": "b", "fresh": 7}))])
        .await
        .expect_err("at the budget");
    retry_ms(&err);
    let schema = service.get_collection(NS, DOCS).await.expect("info").schema;
    assert_eq!(schema.version, version);
    assert!(schema.fields.iter().all(|f| f.name != "fresh"));
    f.shutdown().await;
}

#[tokio::test]
async fn a_bulk_override_is_admitted_up_to_the_factor() {
    let (f, service, _, _) = with_docs(
        budget(|b| {
            b.max_unapplied_records = 2;
            b.override_factor = 4;
        }),
        tail_schema(),
    )
    .await;
    write(&service, ops(0..2)).await.expect("under the budget");
    write(&service, ops(2..4))
        .await
        .expect_err("plain, at the budget");
    // Backlogs 2, 4 and 6 are under 4 × 2.
    for i in 0..3 {
        write_with(&service, ops(10 + 2 * i..12 + 2 * i), Override::Bulk)
            .await
            .expect("bulk under the factor");
    }
    let err = write_with(&service, ops(20..22), Override::Bulk)
        .await
        .expect_err("bulk at 4 × the budget");
    retry_ms(&err);
    let counters = service.backlog_monitor().counters();
    assert_eq!(counters.override_writes.load(Ordering::Relaxed), 3);
    assert_eq!(counters.throttled_writes.load(Ordering::Relaxed), 2);
    f.shutdown().await;
}

#[tokio::test]
async fn one_write_larger_than_the_budget_is_admitted_on_an_empty_backlog() {
    let (f, service, ns, collection) =
        with_docs(budget(|b| b.max_unapplied_records = 2), tail_schema()).await;
    let backlog = write(&service, ops(0..50)).await.expect("an empty backlog");
    assert_eq!(backlog, Backlog::default());
    let after = service
        .backlog_monitor()
        .backlog(ns, &collection)
        .await
        .expect("backlog");
    assert_eq!(after.records, 50);
    write(&service, ops(50..51)).await.expect_err("now over");
    f.shutdown().await;
}

#[test]
fn retry_after_follows_the_apply_rate() {
    let config = BackpressureConfig {
        max_unapplied_records: 1_000,
        max_unapplied_bytes: 1 << 30,
        ..BackpressureConfig::default()
    };
    // 500 records over 90 % of the budget.
    let backlog = Backlog {
        records: 1_400,
        bytes: 1_400 * 100,
    };
    assert_eq!(retry_after(&config, backlog, 100.0), Duration::from_secs(5));
    assert_eq!(
        retry_after(&config, backlog, 0.0),
        Duration::from_secs(30),
        "a stalled link"
    );
    assert_eq!(
        retry_after(&config, backlog, 1e6),
        Duration::from_secs(1),
        "a fast link"
    );
    // The byte excess can dominate: 1 000 bytes per record, 100 records/s.
    let config = BackpressureConfig {
        max_unapplied_records: 1_000_000,
        max_unapplied_bytes: 1_000_000,
        ..BackpressureConfig::default()
    };
    let backlog = Backlog {
        records: 1_700,
        bytes: 1_700_000,
    };
    // (1 700 000 − 900 000) / (100 × 1 000) = 8 s.
    assert_eq!(retry_after(&config, backlog, 100.0), Duration::from_secs(8));
}

#[test]
fn an_inverted_retry_after_range_does_not_panic() {
    let config = BackpressureConfig {
        min_retry_after: Duration::from_secs(30),
        max_retry_after: Duration::from_secs(1),
        ..BackpressureConfig::default()
    };
    let backlog = Backlog {
        records: 2_000_000,
        bytes: 1 << 20,
    };
    for rate in [0.0, 100.0, 1e9] {
        assert_eq!(retry_after(&config, backlog, rate), Duration::from_secs(1));
    }
}

#[tokio::test]
async fn disabled_backpressure_never_refuses() {
    let (f, service, _, _) = with_docs(
        budget(|b| {
            b.enabled = false;
            b.max_unapplied_records = 1;
        }),
        tail_schema(),
    )
    .await;
    for i in 0..4 {
        write(&service, ops(i * 3..i * 3 + 3))
            .await
            .expect("never refused");
    }
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.backpressure.state, BackpressureState::Disabled);
    assert_eq!(info.backpressure.unapplied_records, 12);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_measurement_is_cached_for_the_refresh_interval() {
    let (f, service, ns, collection) = with_docs(ServiceConfig::default(), tail_schema()).await;
    write(&service, ops(0..3)).await.expect("write");
    // A monitor of its own, so the write's measurement does not count; a
    // long refresh interval, so a slow runner cannot make one go stale.
    let monitor = Arc::new(BacklogMonitor::new(
        f.storage.ctx.clone(),
        BackpressureConfig {
            refresh_interval: Duration::from_secs(60),
            ..BackpressureConfig::default()
        },
    ));
    let admissions: Vec<_> = (0..100)
        .map(|_| {
            let (monitor, collection) = (monitor.clone(), collection.clone());
            tokio::spawn(async move { monitor.admit(ns, &collection, Override::None).await })
        })
        .collect();
    for admission in admissions {
        let backlog = admission.await.expect("join").expect("admitted");
        assert_eq!(backlog.records, 3);
    }
    assert_eq!(monitor.refreshes(), 1, "one collection_head read");
    f.shutdown().await;
}

#[tokio::test]
async fn collection_info_reports_the_backlog_and_state() {
    let (f, service, _, _) =
        with_docs(budget(|b| b.max_unapplied_records = 5), tail_schema()).await;
    write(&service, ops(0..2)).await.expect("write");
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.backpressure.state, BackpressureState::Open);
    assert_eq!(info.backpressure.unapplied_records, 2);
    assert_eq!(info.backpressure.max_unapplied_records, 5);
    assert!(info.unapplied_bytes > 0);
    assert_eq!(info.unapplied_bytes, info.backpressure.unapplied_bytes);
    let json = serde_json::to_value(&info).expect("json");
    assert_eq!(json["backpressure"]["state"], json!("open"));

    write(&service, ops(2..5)).await.expect("write");
    let listed = service.list_collections(NS).await.expect("list");
    assert_eq!(listed[0].backpressure.state, BackpressureState::Throttling);
    assert_eq!(listed[0].backpressure.unapplied_records, 5);

    f.apply_link().await;
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.backpressure.state, BackpressureState::Open);
    assert_eq!(info.unapplied_bytes, 0);
    f.shutdown().await;
}

/// Every document of `view` (durable rows not shadowed, then the tail's).
async fn docs(view: &ReadView) -> BTreeMap<PrimaryKey, Value> {
    let mut out = BTreeMap::new();
    for stored in view.snapshot.scan_all().await.expect("scan") {
        if !view.is_shadowed(stored.row_id) {
            out.insert(stored.pk, Value::Object(stored.source));
        }
    }
    for doc in view.tail.live_docs() {
        let source = doc.doc.as_ref().expect("live").source.clone();
        out.insert(doc.pk.clone(), Value::Object(source));
    }
    out
}

/// Rule 7 (D86): with the live tail over its bound, an `Eventual` read
/// takes the durable state plus the tail up to its head, reading nothing of
/// the log, while a `Strong` read falls back to a range tail.
#[tokio::test]
async fn eventual_never_builds_a_range_tail() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let filler = "x".repeat(1_000);
    let early: Vec<DocOp> = (0..40)
        .map(|i| upsert(i, json!({"t": "early", "n": i as i64})))
        .collect();
    fixture.append_all(&early).await;
    fixture.apply_link().await;
    let backlog: Vec<DocOp> = (20..300)
        .map(|i| upsert(i, json!({"t": format!("{filler} {i}"), "n": i as i64})))
        .collect();
    fixture.append_all(&backlog).await;
    let monitor = BacklogMonitor::new(fixture.ctx.clone(), BackpressureConfig::default());
    let collection = fixture.collection().await;
    let measured = monitor
        .backlog(fixture.ns, &collection)
        .await
        .expect("backlog");
    assert!(measured.bytes > 64 << 10, "{measured:?}");

    let small = fixture.reads(
        TailConfig {
            max_bytes: 64 << 10,
            ..TailConfig::default()
        },
        ReadConfig::default(),
    );
    let large = fixture.reads(TailConfig::default(), ReadConfig::default());
    let expected = docs(
        &fixture
            .view(&large, &ReadConsistency::Strong)
            .await
            .expect("large"),
    )
    .await;
    assert_eq!(expected.len(), 300);
    let strong = fixture
        .view(&small, &ReadConsistency::Strong)
        .await
        .expect("strong");
    assert!(
        matches!(
            small.tail_state(fixture.cid),
            Some(TailState::Overflow { .. })
        ),
        "{:?}",
        small.tail_state(fixture.cid)
    );
    assert_eq!(docs(&strong).await, expected, "the range tail");

    // Fetching stopped at the overflow: later writes reach only strong
    // reads, through range tails.
    let later: Vec<DocOp> = (300..350)
        .map(|i| upsert(i, json!({"t": format!("{filler} {i}"), "n": i as i64})))
        .collect();
    fixture.append_all(&later).await;
    let log_gets = fixture.paths.gets_under("wal/");
    let eventual = fixture
        .view(&small, &ReadConsistency::Eventual)
        .await
        .expect("eventual");
    // Exactly the 300 the tail held: no range tail over the later 50.
    assert_eq!(
        docs(&eventual).await,
        expected,
        "durable plus the tail's head"
    );
    assert_eq!(fixture.paths.gets_under("wal/"), log_gets, "no log read");
    let strong = fixture
        .view(&small, &ReadConsistency::Strong)
        .await
        .expect("strong");
    assert_eq!(docs(&strong).await.len(), 350, "a range tail");
    small.shutdown().await;
    large.shutdown().await;
    fixture.shutdown().await;
}

/// Rule 8 (D86): a backlog at the byte budget, half the live tail's bound,
/// fits the tail, so a strong read needs no range tail.
#[tokio::test]
async fn a_strong_read_at_the_budget_is_served_from_the_live_tail() {
    // The defaults' ratio (a 128 MiB budget, a 256 MiB tail) at 1/1024 scale.
    let tail_max = 256 << 10;
    let config = BackpressureConfig {
        max_unapplied_bytes: (tail_max / 2) as u64,
        refresh_interval: Duration::ZERO,
        ..BackpressureConfig::default()
    };
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let monitor = BacklogMonitor::new(fixture.ctx.clone(), config);
    let collection = fixture.collection().await;
    let filler = "y".repeat(500);
    let mut next = 0u64;
    loop {
        match monitor.admit(fixture.ns, &collection, Override::None).await {
            Ok(_) => {
                let batch: Vec<DocOp> = (next..next + 10)
                    .map(|i| upsert(i, json!({"t": format!("{filler} {i}"), "n": i as i64})))
                    .collect();
                fixture.append_all(&batch).await;
                next += 10;
            }
            Err(err) => {
                retry_ms(&err);
                break;
            }
        }
    }
    let at = monitor
        .backlog(fixture.ns, &collection)
        .await
        .expect("backlog");
    assert!(at.bytes >= (tail_max / 2) as u64, "{at:?}");
    let reads = fixture.reads(
        TailConfig {
            max_bytes: tail_max,
            ..TailConfig::default()
        },
        ReadConfig::default(),
    );
    let view = fixture
        .view(&reads, &ReadConsistency::Strong)
        .await
        .expect("strong");
    assert_eq!(reads.tail_state(fixture.cid), Some(TailState::Following));
    let log_gets = fixture.paths.gets_under("wal/");
    assert_eq!(docs(&view).await.len() as u64, next);
    assert_eq!(fixture.paths.gets_under("wal/"), log_gets);
    reads.shutdown().await;
    fixture.shutdown().await;
}
