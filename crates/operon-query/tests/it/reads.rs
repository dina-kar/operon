//! Read views (plan M1.2 Task 4): consistency resolution, pinned reads,
//! range tails and the read caches.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{TailFixture, WAIT, tail_schema, upsert};
use operon_collection::{ConsistencyToken, DocOp, PrimaryKey, lance_prefix};
use operon_query::read::{ReadConfig, ReadView};
use operon_query::tail::{TailConfig, TailLookup, TailState};
use operon_query::{ReadConsistency, ServiceError};
use serde_json::{Value, json};

/// Every document of `view`: the durable rows that are not shadowed, then
/// the tail's live docs over them.
async fn docs(view: &ReadView) -> BTreeMap<PrimaryKey, Value> {
    let mut out = BTreeMap::new();
    for stored in view.snapshot.scan_all().await.expect("scan") {
        if !view.is_shadowed(stored.row_id) {
            out.insert(stored.pk, Value::Object(stored.source));
        }
    }
    for doc in view.tail.live_docs() {
        let source = doc.doc.as_ref().expect("live").source.clone();
        assert!(
            out.insert(doc.pk.clone(), Value::Object(source)).is_none(),
            "{:?} is both durable and in the tail",
            doc.pk
        );
    }
    assert_eq!(out.len() as u64, view.live_rows(), "live_rows");
    out
}

fn delete(pk: u64) -> DocOp {
    DocOp::Delete(PrimaryKey::U64(pk))
}

fn doc_n(i: u64) -> DocOp {
    upsert(i, json!({"t": format!("doc {i}"), "n": i as i64}))
}

fn strong() -> ReadConsistency {
    ReadConsistency::Strong
}

fn pinned(version: u64, token: &ConsistencyToken) -> ReadConsistency {
    ReadConsistency::Pinned {
        manifest_version: version,
        token: token.clone(),
    }
}

/// The token naming (partition, offset + 1) of `(partition, offset)`.
fn token_after(fixture: &TailFixture, at: (u32, u64)) -> ConsistencyToken {
    ConsistencyToken(vec![(fixture.stream, at.0, at.1 + 1)])
}

fn pin_not_found(result: Result<ReadView, ServiceError>) {
    match result {
        Err(ServiceError::NotFound { kind: "pin", .. }) => {}
        Err(other) => panic!("expected NotFound(pin), got {other}"),
        Ok(_) => panic!("expected NotFound(pin), got a view"),
    }
}

#[tokio::test]
async fn a_strong_read_sees_an_acknowledged_write_before_link_apply() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    fixture.append(&doc_n(1)).await;
    let view = fixture.view(&reads, &strong()).await.expect("view");
    assert!(matches!(
        view.tail.get(&PrimaryKey::U64(1)),
        TailLookup::Present(_)
    ));
    assert_eq!(
        docs(&view).await.get(&PrimaryKey::U64(1)),
        Some(&json!({"t": "doc 1", "n": 1}))
    );
    // A second write is seen by the next strong read.
    fixture.append(&delete(1)).await;
    let view = fixture.view(&reads, &strong()).await.expect("view");
    assert!(docs(&view).await.is_empty());
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn at_least_waits_for_the_token_offsets() {
    let fixture = TailFixture::start(tail_schema(), 1).await;
    let reads = Arc::new(fixture.reads(TailConfig::default(), ReadConfig::default()));
    fixture.view(&reads, &strong()).await.expect("view");
    let tail = reads.tail(fixture.cid).expect("the tail runs");
    tail.pause_fetch(true);
    tail.wait_held().await;
    let at = fixture.append(&doc_n(7)).await;
    let token = token_after(&fixture, (fixture.home(&PrimaryKey::U64(7)), at));
    let collection = fixture.collection().await;
    let started = Instant::now();
    let read = {
        let (reads, collection) = (reads.clone(), collection.clone());
        let consistency = ReadConsistency::AtLeast(token);
        let hot = operon_query::hot::RequestHot {
            enabled: false,
            used: Default::default(),
        };
        let ns = fixture.ns;
        tokio::spawn(async move {
            let tier: Arc<dyn operon_query::hot::HotTier> = Arc::new(operon_query::hot::NoHotTier);
            reads
                .view(ns, &collection, &consistency, &hot, tier)
                .await
                .map(|view| view.tail.get(&PrimaryKey::U64(7)))
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!read.is_finished(), "the read did not wait for the token");
    tail.pause_fetch(false);
    let lookup = read.await.expect("join").expect("view");
    assert!(matches!(lookup, TailLookup::Present(_)));
    assert!(started.elapsed() < ReadConfig::default().consistency_wait);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_strong_read_times_out_when_the_log_cannot_be_read() {
    let fixture = TailFixture::start(tail_schema(), 1).await;
    let reads = fixture.reads(
        TailConfig::default(),
        ReadConfig {
            consistency_wait: Duration::from_millis(300),
            ..ReadConfig::default()
        },
    );
    fixture.view(&reads, &strong()).await.expect("view");
    fixture.paths.fail_gets_under("wal/");
    fixture.append(&doc_n(1)).await;
    let started = Instant::now();
    let result = fixture.view(&reads, &strong()).await;
    assert!(
        matches!(
            result,
            Err(ServiceError::Timeout | ServiceError::Unavailable(_))
        ),
        "{:?}",
        result.map(|_| ())
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    fixture.paths.clear_failures();
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn eventual_reads_what_is_in_memory() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    fixture.append_all(&[doc_n(1), doc_n(2)]).await;
    fixture.apply_link().await;
    fixture.append(&doc_n(3)).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    assert!(reads.tail_state(fixture.cid).is_none());
    let view = fixture
        .view(&reads, &ReadConsistency::Eventual)
        .await
        .expect("view");
    // Durable only: no tail ran yet.
    assert_eq!(view.tail.live_count(), 0);
    assert_eq!(
        docs(&view).await.keys().cloned().collect::<Vec<_>>(),
        vec![PrimaryKey::U64(1), PrimaryKey::U64(2)]
    );
    let applied = fixture.applied().await;
    for (p, offset) in &applied {
        assert_eq!(view.read_token.offset(fixture.stream, *p), Some(*offset));
    }
    // The read started the tail; once it caught up, eventual reads see it.
    assert!(reads.tail_state(fixture.cid).is_some());
    let deadline = Instant::now() + WAIT;
    loop {
        let view = fixture
            .view(&reads, &ReadConsistency::Eventual)
            .await
            .expect("view");
        if docs(&view).await.contains_key(&PrimaryKey::U64(3)) {
            break;
        }
        assert!(Instant::now() < deadline, "the tail never caught up");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn read_tokens_cover_the_read() {
    let fixture = TailFixture::start(tail_schema(), 3).await;
    let ops: Vec<DocOp> = (0..12).map(doc_n).collect();
    fixture.append_all(&ops[..6]).await;
    fixture.apply_link().await;
    fixture.append_all(&ops[6..]).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let view = fixture.view(&reads, &strong()).await.expect("view");
    let hwm = fixture.high_watermarks().await;
    assert_eq!(view.read_token.0.len(), 3);
    for p in 0..3 {
        let head = view.tail.head().get(&p).copied().unwrap_or(0);
        assert_eq!(view.read_token.offset(fixture.stream, p), Some(head));
        assert_eq!(head, hwm[&p]);
    }
    assert_eq!(docs(&view).await.len(), 12);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn pinned_reads_ignore_later_writes() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let ops: Vec<DocOp> = (0..20).map(doc_n).collect();
    fixture.append_all(&ops[..12]).await;
    fixture.apply_link().await;
    fixture.append_all(&ops[12..]).await;
    let collection = fixture.collection().await;
    let (version, token) = reads.pin(&collection).await.expect("pin");
    assert!(version >= 1);
    let at_pin = fixture
        .view(&reads, &pinned(version, &token))
        .await
        .expect("view");
    let expected = docs(&at_pin).await;
    assert_eq!(expected.len(), 20);
    assert_eq!(at_pin.read_token, token);

    // 10 upserts and 5 deletes, committed through the link twice.
    let later: Vec<DocOp> = (0..10)
        .map(|i| upsert(i, json!({"t": "changed", "n": -1})))
        .collect();
    fixture.append_all(&later).await;
    fixture.apply_link().await;
    let deletes: Vec<DocOp> = (10..15).map(delete).collect();
    fixture.append_all(&deletes).await;
    fixture.apply_link().await;

    let view = fixture
        .view(&reads, &pinned(version, &token))
        .await
        .expect("view");
    assert_eq!(docs(&view).await, expected);
    assert_eq!(view.live_rows(), at_pin.live_rows());
    assert_eq!(view.snapshot.manifest().version, version);
    // A fresh node (no cached range tail) reads the same.
    let fresh = fixture.reads(TailConfig::default(), ReadConfig::default());
    let view = fixture
        .view(&fresh, &pinned(version, &token))
        .await
        .expect("view");
    assert_eq!(docs(&view).await, expected);
    // The live state moved on.
    let now = fixture.view(&reads, &strong()).await.expect("view");
    assert_eq!(docs(&now).await.len(), 15);
    fresh.shutdown().await;
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_pinned_read_of_a_gone_manifest_is_not_found() {
    let mut fixture = TailFixture::start(tail_schema(), 1).await;
    fixture.ctx.config.keep_manifests = 1;
    fixture.ctx.config.time_travel_retention = Duration::ZERO;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    fixture.append(&doc_n(0)).await;
    fixture.apply_link().await;
    let collection = fixture.collection().await;
    let (version, token) = reads.pin(&collection).await.expect("pin");
    fixture
        .view(&reads, &pinned(version, &token))
        .await
        .expect("retained while live");
    for i in 1..=3 {
        fixture.append(&doc_n(i)).await;
        fixture.apply_link().await;
    }
    pin_not_found(fixture.view(&reads, &pinned(version, &token)).await);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_pinned_token_of_another_collection_is_not_found() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    fixture.append(&doc_n(0)).await;
    fixture.apply_link().await;
    let (other, _, _) = fixture
        .meta
        .client
        .create_collection(fixture.ns, "other", tail_schema(), 2)
        .await
        .expect("create other");
    let other = fixture
        .ctx
        .meta
        .collection(operon_common::meta::Consistency::Linearizable, other)
        .await
        .expect("read")
        .expect("exists");
    let (_, other_token) = reads.pin(&other).await.expect("pin other");
    let (version, token) = reads
        .pin(&fixture.collection().await)
        .await
        .expect("pin docs");
    pin_not_found(fixture.view(&reads, &pinned(version, &other_token)).await);
    // A token that also names another stream, or misses a partition.
    let mut mixed = token.clone();
    mixed.0.extend(other_token.0.iter().copied());
    pin_not_found(fixture.view(&reads, &pinned(version, &mixed)).await);
    let partial = ConsistencyToken(token.0[..1].to_vec());
    pin_not_found(fixture.view(&reads, &pinned(version, &partial)).await);
    fixture
        .view(&reads, &pinned(version, &token))
        .await
        .expect("its own token");
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_pin_before_the_first_commit_reads_from_offset_zero() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let ops: Vec<DocOp> = (0..8).map(doc_n).collect();
    fixture.append_all(&ops).await;
    let (version, token) = reads.pin(&fixture.collection().await).await.expect("pin");
    assert_eq!(version, 0);
    let at_pin = fixture
        .view(&reads, &pinned(0, &token))
        .await
        .expect("view");
    let expected = docs(&at_pin).await;
    assert_eq!(expected.len(), 8);
    fixture.append_all(&[delete(0), doc_n(9)]).await;
    fixture.apply_link().await;
    let fresh = fixture.reads(TailConfig::default(), ReadConfig::default());
    for reads in [&reads, &fresh] {
        let view = fixture.view(reads, &pinned(0, &token)).await.expect("view");
        assert_eq!(view.snapshot.manifest().version, 0);
        assert_eq!(docs(&view).await, expected);
    }
    fresh.shutdown().await;
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_pinned_read_after_drop_and_recreate_is_not_found() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    fixture.append(&doc_n(0)).await;
    fixture.apply_link().await;
    let (version, token) = reads.pin(&fixture.collection().await).await.expect("pin");
    fixture
        .meta
        .client
        .drop_collection(fixture.ns, "docs")
        .await
        .expect("drop");
    reads.stop_collection(fixture.cid).await;
    let (cid, _, _) = fixture
        .meta
        .client
        .create_collection(fixture.ns, "docs", tail_schema(), 2)
        .await
        .expect("re-create");
    let recreated = fixture
        .ctx
        .meta
        .collection(operon_common::meta::Consistency::Linearizable, cid)
        .await
        .expect("read")
        .expect("exists");
    pin_not_found(
        fixture
            .view_of(&reads, &recreated, &pinned(version, &token))
            .await,
    );
    pin_not_found(
        fixture
            .view_of(&reads, &recreated, &pinned(0, &token))
            .await,
    );
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_overflowed_tail_falls_back_to_a_range_tail_with_identical_results() {
    let fixture = TailFixture::start(tail_schema(), 2).await;
    let filler = "x".repeat(1_000);
    let ops: Vec<DocOp> = (0..40)
        .map(|i| upsert(i, json!({"t": "early", "n": i as i64})))
        .collect();
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    let ops: Vec<DocOp> = (20..300)
        .map(|i| upsert(i, json!({"t": format!("{filler} {i}"), "n": i as i64})))
        .chain((0..10).map(delete))
        .collect();
    fixture.append_all(&ops).await;

    let small = fixture.reads(
        TailConfig {
            max_bytes: 64 << 10,
            ..TailConfig::default()
        },
        ReadConfig::default(),
    );
    let large = fixture.reads(TailConfig::default(), ReadConfig::default());
    let expected = docs(&fixture.view(&large, &strong()).await.expect("large view")).await;
    assert_eq!(expected.len(), 290);
    let view = fixture.view(&small, &strong()).await.expect("small view");
    assert!(
        matches!(
            small.tail_state(fixture.cid),
            Some(TailState::Overflow { .. })
        ),
        "{:?}",
        small.tail_state(fixture.cid)
    );
    assert_eq!(docs(&view).await, expected);
    let hwm = fixture.high_watermarks().await;
    for (p, hwm) in hwm {
        assert_eq!(view.read_token.offset(fixture.stream, p), Some(hwm));
    }
    small.shutdown().await;
    large.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_strong_read_through_a_lagging_follower_is_still_strong() {
    let fixture = TailFixture::start_cluster(tail_schema(), 2).await;
    let reads = fixture.follower_reads(0, TailConfig::default(), ReadConfig::default());
    for i in 0..50u64 {
        fixture
            .append(&upsert(i % 7, json!({"t": format!("v{i}"), "n": i as i64})))
            .await;
        let view = fixture.view(&reads, &strong()).await.expect("view");
        let TailLookup::Present(doc) = view.tail.get(&PrimaryKey::U64(i % 7)) else {
            panic!("iteration {i}: the write is missing");
        };
        assert_eq!(
            doc.doc.as_ref().expect("present").source["t"],
            json!(format!("v{i}")),
            "iteration {i}"
        );
    }
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn snapshots_are_cached_per_manifest_version() {
    let fixture = TailFixture::start(tail_schema(), 1).await;
    let ops: Vec<DocOp> = (0..5).map(doc_n).collect();
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let versions = format!("{}_versions/", lance_prefix(fixture.ns, fixture.cid));
    let before = fixture.paths.gets_under(&versions);
    let first = fixture.view(&reads, &strong()).await.expect("view");
    let after_first = fixture.paths.gets_under(&versions);
    let second = fixture.view(&reads, &strong()).await.expect("view");
    assert_eq!(
        first.snapshot.manifest().version,
        second.snapshot.manifest().version
    );
    assert_eq!(fixture.paths.gets_under(&versions), after_first);
    assert!(after_first >= before);
    reads.shutdown().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn delete_bitmaps_are_read_once_per_node() {
    let fixture = TailFixture::start(tail_schema(), 1).await;
    let ops: Vec<DocOp> = (0..6).map(doc_n).collect();
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    fixture
        .append_all(&[upsert(1, json!({"t": "again"})), delete(2)])
        .await;
    fixture.apply_link().await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let mut path = None;
    for _ in 0..2 {
        let view = fixture.view(&reads, &strong()).await.expect("view");
        let split = view
            .snapshot
            .splits()
            .iter()
            .find(|split| split.delete_bitmap.is_some())
            .expect("the first split has a delete bitmap")
            .clone();
        let deleted = view
            .bitmaps
            .deleted_docs(&view.snapshot, &split)
            .await
            .expect("bitmap");
        assert_eq!(deleted.len(), 2);
        path = split.delete_bitmap.clone();
    }
    assert_eq!(fixture.paths.gets_under(&path.expect("path")), 1);
    reads.shutdown().await;
    fixture.shutdown().await;
}
