//! The tail index (plan M1.2 Task 3, H3): it folds the implicit stream like
//! the link, reads the durable state for patches, keeps its shadow rows
//! exact across manifest commits, bounds its memory, restarts from the live
//! manifest and builds range tails.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::{TailFixture, WAIT, doc, field, obj, patch, tail_schema, upsert};
use bytes::Bytes;
use operon_collection::{
    CollectionSchema, CollectionSnapshot, DocOp, DynamicMapping, FieldKind, PatchMode, PrimaryKey,
    SparseModifier, SparseVector, SparseVectorSpec, decode_sparse_weights, fold_stream,
};
use operon_common::meta::Consistency;
use operon_log::Record;
use operon_query::tail::{
    TAIL_ROWID_BASE, TailConfig, TailError, TailLookup, TailSnapshot, TailState, build_range_tail,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;
use tantivy::{Searcher, Term};

/// A lookup without row ids: kind, document and (partition, offset).
fn view(lookup: &TailLookup) -> (u8, Option<Value>, Option<(u32, u64)>) {
    match lookup {
        TailLookup::Present(d) => (
            1,
            d.doc.as_ref().map(|doc| Value::Object(doc.source.clone())),
            Some((d.partition, d.offset)),
        ),
        TailLookup::Deleted(d) => (2, None, Some((d.partition, d.offset))),
        TailLookup::Absent => (0, None, None),
    }
}

fn present(lookup: TailLookup) -> Arc<operon_query::tail::TailDoc> {
    match lookup {
        TailLookup::Present(d) => d,
        other => panic!("expected Present, got {other:?}"),
    }
}

fn source_of(lookup: TailLookup) -> Value {
    Value::Object(
        present(lookup)
            .doc
            .as_ref()
            .expect("present")
            .source
            .clone(),
    )
}

fn term_count(searcher: &Searcher, field: &str, value: &str) -> usize {
    let field = searcher.schema().get_field(field).expect("field");
    let query = TermQuery::new(
        Term::from_field_text(field, value),
        IndexRecordOption::Basic,
    );
    searcher.search(&query, &Count).expect("search")
}

fn random_source(rng: &mut ChaCha8Rng) -> Value {
    let words = ["alpha", "beta", "gamma", "delta"];
    let mut source = serde_json::Map::new();
    if rng.random_bool(0.7) {
        source.insert(
            "t".into(),
            json!(format!(
                "{} {}",
                words[rng.random_range(0..4)],
                words[rng.random_range(0..4)]
            )),
        );
    }
    if rng.random_bool(0.6) {
        source.insert("tag".into(), json!(words[rng.random_range(0..4)]));
    }
    if rng.random_bool(0.6) {
        source.insert("n".into(), json!(rng.random_range(-5..5)));
    }
    if rng.random_bool(0.5) {
        let mut o = serde_json::Map::new();
        if rng.random_bool(0.5) {
            o.insert("x".into(), json!(rng.random_range(0..3)));
        }
        if rng.random_bool(0.5) {
            o.insert("y".into(), json!(rng.random_range(0..3)));
        }
        source.insert("o".into(), Value::Object(o));
    }
    Value::Object(source)
}

fn random_vector(rng: &mut ChaCha8Rng) -> Vec<f32> {
    (0..3).map(|_| rng.random_range(0..4) as f32).collect()
}

/// A valid op on one of 20 keys: every kind, every patch mode, with and
/// without `upsert`, with vector sets and deletes.
fn random_op(rng: &mut ChaCha8Rng) -> DocOp {
    let pk = rng.random_range(0..20u64);
    let full = |rng: &mut ChaCha8Rng| {
        let mut d = doc(pk, random_source(rng));
        if rng.random_bool(0.5) {
            d.vectors.insert("v".into(), random_vector(rng));
        }
        d
    };
    match rng.random_range(0..10) {
        0..=3 => DocOp::Upsert(full(rng)),
        4 => DocOp::Delete(PrimaryKey::U64(pk)),
        _ => {
            let mode = [
                PatchMode::MergeDeep,
                PatchMode::MergeTop,
                PatchMode::Replace,
            ][rng.random_range(0..3)];
            let mut vectors = BTreeMap::new();
            match rng.random_range(0..4) {
                0 => {
                    vectors.insert("v".to_string(), Some(random_vector(rng)));
                }
                1 => {
                    vectors.insert("v".to_string(), None);
                }
                _ => {}
            }
            let delete_keys = if rng.random_bool(0.2) {
                vec!["o.x".to_string()]
            } else {
                Vec::new()
            };
            let source = obj(random_source(rng));
            let upsert = rng.random_bool(0.3).then(|| full(rng));
            DocOp::Patch {
                pk: PrimaryKey::U64(pk),
                mode,
                source,
                delete_keys,
                vectors,
                sparse_vectors: BTreeMap::new(),
                upsert,
            }
        }
    }
}

#[tokio::test]
async fn the_tail_folds_ops_like_the_link() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let mut rng = ChaCha8Rng::seed_from_u64(0x7a11);
    let ops: Vec<DocOp> = (0..300).map(|_| random_op(&mut rng)).collect();
    f.append_all(&ops).await;
    let snapshot = f.sync(&tail).await;
    let expected = f.expected().await;
    for key in 0..20 {
        let pk = PrimaryKey::U64(key);
        match (expected.get(&pk), snapshot.get(&pk)) {
            (Some(expected), TailLookup::Present(found)) => {
                assert_eq!(found.doc.as_deref(), Some(&expected.doc), "key {key}");
                assert_eq!(
                    (found.partition, found.offset),
                    (expected.partition, expected.offset),
                    "key {key}"
                );
            }
            (None, TailLookup::Deleted(_) | TailLookup::Absent) => {}
            (expected, found) => panic!("key {key}: expected {expected:?}, found {found:?}"),
        }
    }
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_in_the_tail_reads_the_durable_document() {
    let f = TailFixture::start(tail_schema(), 2).await;
    f.append(&upsert(1, json!({"a": 1, "o": {"x": 1}}))).await;
    f.apply_link().await;
    let row = f.row_of(1).await.expect("durable");
    let tail = f.tail(TailConfig::default()).await;
    f.append(&patch(1, json!({"o": {"y": 2}}))).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(
        source_of(snapshot.get(&PrimaryKey::U64(1))),
        json!({"a": 1, "o": {"x": 1, "y": 2}})
    );
    assert!(snapshot.shadow().contains(row), "{:?}", snapshot.shadow());
    assert_eq!(snapshot.shadow().len(), 1);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_of_a_key_missing_everywhere_is_a_noop() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    f.append(&patch(5, json!({"a": 1}))).await;
    let snapshot = f.sync(&tail).await;
    assert!(matches!(
        snapshot.get(&PrimaryKey::U64(5)),
        TailLookup::Absent
    ));
    assert_eq!(snapshot.entry_count(), 0);
    assert!(snapshot.shadow().is_empty());
    assert!(snapshot.keys_after(None, 10).is_empty());
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_with_upsert_inserts_when_missing() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys,
        vectors,
        sparse_vectors,
        ..
    } = patch(5, json!({"a": 1}))
    else {
        unreachable!()
    };
    let op = DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys,
        vectors,
        sparse_vectors,
        upsert: Some(doc(5, json!({"b": 2}))),
    };
    f.append(&op).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(
        source_of(snapshot.get(&PrimaryKey::U64(5))),
        json!({"b": 2})
    );
    assert_eq!(snapshot.live_count(), 1);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn an_unchanged_upsert_is_a_new_version_and_an_unchanged_patch_appends_nothing() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let pk = PrimaryKey::U64(3);
    f.append(&upsert(3, json!({"a": 1}))).await;
    let second = f.append(&upsert(3, json!({"a": 1}))).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(present(snapshot.get(&pk)).offset, second);
    assert_eq!(snapshot.entry_count(), 2);
    let noop = f.append(&patch(3, json!({"a": 1}))).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(present(snapshot.get(&pk)).offset, second);
    assert_eq!(snapshot.entry_count(), 2);
    assert!(snapshot.head()[&f.home(&pk)] > noop);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn the_tail_drops_covered_entries_when_a_manifest_lands() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let ops: Vec<DocOp> = (0..50)
        .map(|n| upsert(n, json!({"tag": "x", "n": n})))
        .collect();
    f.append_all(&ops).await;
    let before = f.sync(&tail).await;
    assert_eq!(before.keys_after(None, 100).len(), 50);
    f.apply_link().await;
    let version = f.manifest().await.1.version;
    let after = f.adopted(&tail, version).await;
    assert!(after.keys_after(None, 100).is_empty());
    assert!(after.shadow().is_empty());
    assert!(after.live().is_empty());
    assert_eq!(after.garbage(), 50);
    for n in 0..50 {
        assert!(matches!(after.get(&PrimaryKey::U64(n)), TailLookup::Absent));
    }
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn in_flight_snapshots_survive_a_manifest_adoption() {
    let f = TailFixture::start(tail_schema(), 2).await;
    // Compact as soon as there is garbage.
    let config = TailConfig {
        compact_min_entries: 1,
        ..TailConfig::default()
    };
    let tail = f.tail(config).await;
    let ops: Vec<DocOp> = (0..10).map(|n| upsert(n, json!({"tag": "x"}))).collect();
    f.append_all(&ops).await;
    let held = f.sync(&tail).await;
    f.apply_link().await;
    let version = f.manifest().await.1.version;
    let current = f.adopted(&tail, version).await;
    // Adopted and compacted: the new generation is empty.
    assert_eq!(current.entry_count(), 0);
    assert!(current.searcher().is_none());
    for n in 0..10 {
        assert_eq!(
            source_of(held.get(&PrimaryKey::U64(n))),
            json!({"tag": "x"})
        );
    }
    let searcher = held.searcher().expect("the held snapshot searches");
    assert_eq!(term_count(searcher, "tag", "x"), 10);
    assert_eq!(held.live_docs().len(), 10);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn shadowed_rows_follow_pk_deltas() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let pk = PrimaryKey::U64(7);
    let p = f.home(&pk);
    f.append(&upsert(7, json!({"a": 1}))).await;
    f.apply_link().await;
    let r1 = f.row_of(7).await.expect("durable");
    let tail = f.tail(TailConfig::default()).await;
    let o1 = f.append(&upsert(7, json!({"a": 2}))).await;
    let o2 = f.append(&patch(7, json!({"b": 3}))).await;
    let snapshot = f.sync(&tail).await;
    assert!(snapshot.shadow().contains(r1));
    // The link commits the upsert at o1 < o2 only.
    let mut upto = f.applied().await;
    upto.insert(p, o1 + 1);
    let version = f.commit_upto(&upto).await;
    let r2 = f.row_of(7).await.expect("durable");
    assert_ne!(r1, r2);
    let after = f.adopted(&tail, version).await;
    let found = present(after.get(&pk));
    assert_eq!(found.offset, o2);
    assert_eq!(
        Value::Object(found.doc.as_ref().unwrap().source.clone()),
        json!({"a": 2, "b": 3})
    );
    assert_eq!(after.shadow().iter().collect::<Vec<_>>(), vec![r2]);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_across_a_manifest_boundary_folds_once() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let pk = PrimaryKey::U64(9);
    let p = f.home(&pk);
    let tail = f.tail(TailConfig::default()).await;
    let o1 = f.append(&upsert(9, json!({"a": 1}))).await;
    f.append(&patch(9, json!({"b": 2}))).await;
    f.sync(&tail).await;
    let mut upto = BTreeMap::new();
    upto.insert(p, o1 + 1);
    let version = f.commit_upto(&upto).await;
    let row = f.row_of(9).await.expect("durable");
    let after = f.adopted(&tail, version).await;
    assert_eq!(source_of(after.get(&pk)), json!({"a": 1, "b": 2}));
    assert_eq!(after.keys_after(None, 10).len(), 1);
    assert_eq!(after.live_count(), 1);
    assert_eq!(after.shadow().iter().collect::<Vec<_>>(), vec![row]);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_missing_pk_delta_reresolves_every_overlay_key() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let keys: Vec<u64> = (0..6).collect();
    let first: Vec<DocOp> = keys.iter().map(|n| upsert(*n, json!({"a": 0}))).collect();
    f.append_all(&first).await;
    f.apply_link().await;
    let tail = f.tail(TailConfig::default()).await;
    f.sync(&tail).await;
    // New rows for every key, then a patch of each that stays in the tail.
    let upserts: Vec<DocOp> = keys.iter().map(|n| upsert(*n, json!({"a": 1}))).collect();
    let positions = f.append_all(&upserts).await;
    let patches: Vec<DocOp> = keys.iter().map(|n| patch(*n, json!({"b": 2}))).collect();
    f.append_all(&patches).await;
    let before = f.sync(&tail).await;
    assert_eq!(before.keys_after(None, 100).len(), keys.len());

    tail.pause_fetch(true);
    tail.wait_held().await;
    let mut upto = f.applied().await;
    for (partition, offset) in positions {
        let slot = upto.entry(partition).or_insert(0);
        *slot = (*slot).max(offset + 1);
    }
    let version = f.commit_upto(&upto).await;
    let (_, manifest) = f.manifest().await;
    let delta = manifest
        .pk_delta
        .clone()
        .expect("the commit wrote a pk delta");
    f.store.delete(&delta).await.expect("delete the pk delta");
    tail.pause_fetch(false);

    let after = f.adopted(&tail, version).await;
    let fresh: BTreeSet<u64> = f
        .snapshot()
        .await
        .get_by_pk(&keys.iter().map(|n| PrimaryKey::U64(*n)).collect::<Vec<_>>())
        .await
        .expect("get")
        .into_iter()
        .map(|stored| stored.expect("durable").row_id)
        .collect();
    let old: BTreeSet<u64> = before.shadow().iter().collect();
    assert_ne!(fresh, old, "the commit gave the keys new rows");
    assert_eq!(after.shadow().iter().collect::<BTreeSet<u64>>(), fresh);
    assert_eq!(after.keys_after(None, 100).len(), keys.len());
    tail.stop().await;
    f.shutdown().await;
}

/// #18 review: an adoption whose re-resolution fails is retried, and its
/// manifest is never published with the older manifest's shadow rows.
#[tokio::test]
async fn a_failed_reresolve_is_retried_before_publishing() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let keys: Vec<u64> = (0..6).collect();
    let first: Vec<DocOp> = keys.iter().map(|n| upsert(*n, json!({"a": 0}))).collect();
    f.append_all(&first).await;
    f.apply_link().await;
    let tail = f.tail(TailConfig::default()).await;
    f.sync(&tail).await;
    let upserts: Vec<DocOp> = keys.iter().map(|n| upsert(*n, json!({"a": 1}))).collect();
    let positions = f.append_all(&upserts).await;
    let patches: Vec<DocOp> = keys.iter().map(|n| patch(*n, json!({"b": 2}))).collect();
    f.append_all(&patches).await;
    let before = f.sync(&tail).await;

    tail.pause_fetch(true);
    tail.wait_held().await;
    let mut upto = f.applied().await;
    for (partition, offset) in positions {
        let slot = upto.entry(partition).or_insert(0);
        *slot = (*slot).max(offset + 1);
    }
    let version = f.commit_upto(&upto).await;
    let (_, manifest) = f.manifest().await;
    let delta = manifest.pk_delta.clone().expect("a pk delta");
    f.store.delete(&delta).await.expect("delete the pk delta");
    // Re-resolution reads the new Lance version. With every such read
    // failing, the held follower runs exactly one iteration: one failed
    // attempt, which neither publishes the new manifest nor resets.
    let lance = operon_collection::lance_prefix(f.ns, f.cid);
    f.paths.fail_gets_under(&lance);
    tail.step().await;
    assert!(
        f.paths.failures_under(&lance) > 0,
        "the attempt read the Lance version and failed"
    );
    assert!(
        tail.current().manifest().version < version,
        "a failed re-resolution publishes nothing"
    );
    // The retry succeeds and adopts the manifest over the same overlay: the
    // tail's row ids are unchanged, which a reset (a refold with new row
    // ids) would not give.
    f.paths.clear_failures();
    tail.pause_fetch(false);

    let after = f.adopted(&tail, version).await;
    let fresh: BTreeSet<u64> = f
        .snapshot()
        .await
        .get_by_pk(&keys.iter().map(|n| PrimaryKey::U64(*n)).collect::<Vec<_>>())
        .await
        .expect("get")
        .into_iter()
        .map(|stored| stored.expect("durable").row_id)
        .collect();
    assert_ne!(fresh, before.shadow().iter().collect::<BTreeSet<u64>>());
    assert_eq!(
        after.live().iter().collect::<Vec<u64>>(),
        before.live().iter().collect::<Vec<u64>>(),
        "the adoption kept the overlay's row ids"
    );
    assert_eq!(after.shadow().iter().collect::<BTreeSet<u64>>(), fresh);
    assert_eq!(after.keys_after(None, 100).len(), keys.len());
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn the_tail_restarts_from_the_live_manifest() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let first: Vec<DocOp> = (0..8).map(|n| upsert(n, json!({"a": n}))).collect();
    f.append_all(&first).await;
    f.apply_link().await;
    let later = vec![
        patch(1, json!({"b": 1})),
        DocOp::Delete(PrimaryKey::U64(2)),
        upsert(3, json!({"c": 3})),
        upsert(20, json!({"new": true})),
        patch(21, json!({"missing": true})),
    ];
    f.append_all(&later).await;
    let tail = f.tail(TailConfig::default()).await;
    let one = f.sync(&tail).await;
    let views = |snapshot: &TailSnapshot| -> Vec<_> {
        (0..25)
            .map(|n| view(&snapshot.get(&PrimaryKey::U64(n))))
            .collect()
    };
    let before = views(&one);
    tail.stop().await;
    assert!(tail.is_stopped());
    let again = f.tail(TailConfig::default()).await;
    let two = f.sync(&again).await;
    assert_eq!(views(&two), before);
    assert_eq!(two.shadow(), one.shadow());
    again.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn the_memory_bound_puts_the_tail_in_overflow() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let config = TailConfig {
        max_bytes: 64 << 10,
        ..TailConfig::default()
    };
    let tail = f.tail(config).await;
    let filler = "x".repeat(200);
    let ops: Vec<DocOp> = (0..1000)
        .map(|n| upsert(n, json!({"t": filler, "n": n})))
        .collect();
    f.append_all(&ops).await;
    let deadline = std::time::Instant::now() + WAIT;
    while !matches!(tail.state(), TailState::Overflow { .. }) {
        assert!(std::time::Instant::now() < deadline, "never overflowed");
        tail.notify();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f.append(&upsert(5000, json!({"n": 1}))).await;
    let targets = f.high_watermarks().await;
    let beyond = tail
        .sync(&targets, tokio::time::Instant::now() + WAIT)
        .await;
    assert!(
        matches!(beyond, Err(TailError::Overflow { .. })),
        "{beyond:?}"
    );

    f.apply_link().await;
    let deadline = std::time::Instant::now() + WAIT;
    while tail.state() != TailState::Following {
        assert!(std::time::Instant::now() < deadline, "never recovered");
        tail.notify();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f.append(&upsert(5001, json!({"n": 2}))).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(
        source_of(snapshot.get(&PrimaryKey::U64(5001))),
        json!({"n": 2})
    );
    assert!(snapshot.bytes() < 64 << 10);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn undecodable_and_invalid_records_are_skipped() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let pk = PrimaryKey::U64(4);
    let p = f.home(&pk);
    let written = f.append(&upsert(4, json!({"n": 1}))).await;
    let bad = f
        .append_raw(
            p,
            Record {
                key: Some(Bytes::from_static(b"junk")),
                value: Some(Bytes::from_static(b"not a doc op")),
                headers: vec![],
                timestamp_ms: 1,
            },
        )
        .await;
    let invalid = f.append(&patch(4, json!({"n": "abc"}))).await;
    let snapshot = f.sync(&tail).await;
    let found = present(snapshot.get(&pk));
    assert_eq!(found.offset, written);
    assert_eq!(
        Value::Object(found.doc.as_ref().unwrap().source.clone()),
        json!({"n": 1})
    );
    assert!(snapshot.head()[&p] > bad.max(invalid));
    assert_eq!(snapshot.entry_count(), 1);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn the_tail_indexes_sparse_vectors() {
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword)],
        vec![],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    let f = TailFixture::start(schema, 2).await;
    let tail = f.tail(TailConfig::default()).await;
    let mut first = doc(1, json!({"tag": "x"}));
    first.sparse_vectors.insert(
        "s".into(),
        SparseVector::new(vec![1, 5], vec![2.0, 0.0]).unwrap(),
    );
    f.append(&DocOp::Upsert(first)).await;
    let patched = SparseVector::new(vec![7], vec![1.0]).unwrap();
    let DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys,
        vectors,
        upsert,
        ..
    } = patch(1, json!({}))
    else {
        unreachable!()
    };
    f.append(&DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys,
        vectors,
        sparse_vectors: BTreeMap::from([("s".to_string(), Some(patched.clone()))]),
        upsert,
    })
    .await;
    let snapshot = f.sync(&tail).await;
    let found = present(snapshot.get(&PrimaryKey::U64(1)));
    assert_eq!(found.doc.as_ref().unwrap().sparse_vectors["s"], patched);
    let searcher = snapshot.searcher().expect("a live doc");
    let postings = searcher.schema().get_field("_sparse.s").unwrap();
    let hits = |index: u64| {
        let query = TermQuery::new(
            Term::from_field_u64(postings, index),
            IndexRecordOption::Basic,
        );
        searcher.search(&query, &DocSetCollector).unwrap()
    };
    assert!(hits(1).is_empty() && hits(5).is_empty());
    let with_7: Vec<_> = hits(7).into_iter().collect();
    assert_eq!(with_7.len(), 1);
    let address = with_7[0];
    let segment = searcher.segment_reader(address.segment_ord);
    let rowid = segment
        .fast_fields()
        .u64("_rowid")
        .unwrap()
        .first(address.doc_id)
        .unwrap();
    assert_eq!(rowid, found.row_id);
    let weights = segment
        .fast_fields()
        .bytes("_sparse_w.s")
        .unwrap()
        .expect("the weights column");
    let ord = weights.term_ords(address.doc_id).next().expect("weights");
    let mut bytes = Vec::new();
    weights.ord_to_bytes(ord, &mut bytes).unwrap();
    assert_eq!(decode_sparse_weights(&bytes).unwrap(), patched);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_schema_change_rebuilds_the_tail_index() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let tail = f.tail(TailConfig::default()).await;
    f.append(&upsert(1, json!({"m": "before"}))).await;
    f.sync(&tail).await;
    let mut next = tail_schema();
    next.fields.push(field("m", FieldKind::Keyword));
    next.version = 2;
    f.meta
        .client
        .update_collection_schema(f.cid, 1, next)
        .await
        .expect("add a field");
    f.append(&upsert(2, json!({"m": "after"}))).await;
    let deadline = std::time::Instant::now() + WAIT;
    let snapshot = loop {
        let snapshot = f.sync(&tail).await;
        if snapshot.schema_version() == 2 {
            break snapshot;
        }
        assert!(std::time::Instant::now() < deadline, "never rebuilt");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let searcher = snapshot.searcher().expect("live docs");
    assert_eq!(term_count(searcher, "m", "before"), 1);
    assert_eq!(term_count(searcher, "m", "after"), 1);
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn tail_row_ids_are_above_every_lance_row_id() {
    let f = TailFixture::start(tail_schema(), 2).await;
    let ops: Vec<DocOp> = (0..10).map(|n| upsert(n, json!({"n": n}))).collect();
    f.append_all(&ops).await;
    f.apply_link().await;
    let tail = f.tail(TailConfig::default()).await;
    let more: Vec<DocOp> = (5..15).map(|n| upsert(n, json!({"n": n + 1}))).collect();
    f.append_all(&more).await;
    let snapshot = f.sync(&tail).await;
    assert_eq!(snapshot.live_count(), 10);
    assert!(snapshot.live().iter().all(|row| row >= TAIL_ROWID_BASE));
    assert_eq!(snapshot.shadow().len(), 5);
    assert!(snapshot.shadow().iter().all(|row| row < TAIL_ROWID_BASE));
    for n in 0..10 {
        assert!(f.row_of(n).await.expect("durable") < TAIL_ROWID_BASE);
    }
    tail.stop().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_range_tail_stops_exactly_at_its_upper_bound() {
    let f = TailFixture::start(tail_schema(), 1).await;
    let ops = vec![
        upsert(1, json!({"a": 1})),
        upsert(2, json!({"a": 2})),
        patch(1, json!({"b": 1})),
        DocOp::Delete(PrimaryKey::U64(2)),
        upsert(3, json!({"a": 3})),
        // Offset 5 and later are beyond the bound.
        patch(3, json!({"b": 3})),
        upsert(4, json!({"a": 4})),
        upsert(2, json!({"a": 22})),
        DocOp::Delete(PrimaryKey::U64(1)),
        upsert(5, json!({"a": 5})),
    ];
    f.append_all(&ops).await;
    let collection = f.collection().await;
    let durable = CollectionSnapshot::open(&f.ctx, f.ns, f.cid, Consistency::Linearizable)
        .await
        .expect("snapshot");
    let upper = BTreeMap::from([(0, 5)]);
    let range = build_range_tail(
        f.ns,
        &collection,
        &f.ctx,
        &f.reader,
        &durable,
        &upper,
        64 << 20,
    )
    .await
    .expect("range tail");
    assert_eq!(range.head()[&0], 5);
    let records: Vec<_> = f
        .records()
        .await
        .into_iter()
        .filter(|(_, r)| r.offset < 5)
        .collect();
    let expected = fold_stream(&collection.schema, 1, &records);
    for n in 1..=5 {
        let pk = PrimaryKey::U64(n);
        match (expected.get(&pk), range.get(&pk)) {
            (Some(expected), TailLookup::Present(found)) => {
                assert_eq!(found.doc.as_deref(), Some(&expected.doc), "key {n}");
                assert_eq!(found.offset, expected.offset, "key {n}");
            }
            (None, TailLookup::Deleted(_) | TailLookup::Absent) => {}
            (expected, found) => panic!("key {n}: expected {expected:?}, found {found:?}"),
        }
    }
    f.shutdown().await;
}
