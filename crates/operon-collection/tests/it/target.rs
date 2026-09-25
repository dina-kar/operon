//! The collection link target (plan M1.1 Task 10): link apply folds a
//! collection's implicit stream into Lance, Tantivy splits, delete bitmaps
//! and the PK index under one manifest.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::common::{TargetFixture, doc, field, home, patch, schema, sparse, upsert, vector};
use bytes::Bytes;
use futures::FutureExt;
use operon_collection::{
    CollectionCommitHook, CollectionCommitStep, CollectionConfig, CollectionSchema, DocOp,
    DynamicMapping, FieldKind, PatchMode, PrimaryKey, SPARSE_PRESENT, SparseModifier,
    SparseVectorSpec, StoredDoc, decode_dead_letters, decode_sparse_weights, sparse_postings_field,
    sparse_weights_field,
};
use operon_link::{CommitError, LinkError, LinkTargetFactory};
use operon_log::Record;
use operon_meta::{Consistency, collection_pk_prefix, collection_pointer_key};
use operon_pk::{PkIndex, PkIndexConfig};
use operon_worker::{RunResult, TaskError, TaskKey, TaskOutcome, TaskSource};
use serde_json::json;
use tantivy::collector::DocSetCollector;
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;

/// Fields `tag` (keyword) and `n` (i64); unmapped paths are ignored.
fn tagged() -> CollectionSchema {
    schema(
        vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
        DynamicMapping::Ignore,
    )
}

/// No typed fields; every path is ignored.
fn untyped() -> CollectionSchema {
    schema(vec![], DynamicMapping::Ignore)
}

/// The one result of a run, which must have run.
fn ran(results: Vec<(TaskKey, RunResult)>) -> Result<TaskOutcome, TaskError> {
    let [(_, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the task did not run");
    };
    result
}

fn assert_ran_ok(results: Vec<(TaskKey, RunResult)>) {
    let result = ran(results);
    assert!(result.is_ok(), "{result:?}");
}

/// The link error a failed run ended with.
fn link_error(result: Result<TaskOutcome, TaskError>) -> LinkError {
    match result {
        Err(TaskError::Failed(err)) => match err.downcast::<LinkError>() {
            Ok(err) => *err,
            Err(other) => panic!("not a link error: {other}"),
        },
        other => panic!("expected a failed run, got {other:?}"),
    }
}

fn assert_verified(problems: Vec<String>) {
    assert!(problems.is_empty(), "{problems:#?}");
}

/// The live documents by key.
fn by_key(docs: Vec<StoredDoc>) -> BTreeMap<PrimaryKey, StoredDoc> {
    docs.into_iter().map(|d| (d.pk.clone(), d)).collect()
}

/// A hook that runs `f` once, the first time a commit reaches `step`.
fn once_at<F>(step: CollectionCommitStep, f: F) -> CollectionCommitHook
where
    F: Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static,
{
    let done = Arc::new(AtomicBool::new(false));
    Arc::new(move |at, _fence| {
        if at == step && !done.swap(true, Ordering::SeqCst) {
            f()
        } else {
            futures::future::ready(()).boxed()
        }
    })
}

/// 50 upserts over 3 partitions are one commit, and the collection is the
/// stream's fold.
#[tokio::test]
async fn upserts_are_applied_and_readable() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write(
        (0..50)
            .map(|n| upsert(n, json!({ "tag": format!("t{}", n % 3), "n": n })))
            .collect(),
    )
    .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.applied, f.high_watermarks().await);
    assert_eq!(manifest.applied.len(), 3, "every partition has records");
    assert_eq!(manifest.live_doc_count, 50);
    assert_eq!(manifest.splits.len(), 1);
    assert_eq!(manifest.schema_version, 1);
    assert!(manifest.pk_delta.is_some());
    assert!(manifest.dead_letters.is_none());
    assert_eq!(f.snapshot().await.scan_all().await.unwrap().len(), 50);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Review focus 3: several ops on one key in one batch fold latest-wins
/// into a single row.
#[tokio::test]
async fn one_batch_with_upsert_patch_delete_on_one_key_leaves_the_fold_result() {
    let f = TargetFixture::start(untyped(), 2).await;
    let k = PrimaryKey::U64(7);
    f.write(vec![
        upsert(7, json!({ "a": 1 })),
        patch(k.clone(), json!({ "b": 2 })),
        DocOp::Delete(k.clone()),
        upsert(7, json!({ "c": 3 })),
    ])
    .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs.len(), 1, "{docs:?}");
    assert_eq!(docs[0].pk, k);
    assert_eq!(docs[0].source, crate::common::obj(json!({ "c": 3 })));
    assert_eq!(f.manifest().await.version, 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn an_upsert_of_an_existing_key_replaces_its_row() {
    let f = TargetFixture::start(tagged(), 2).await;
    let source = f.source(f.factory());
    f.write((1..=5).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let first = f.snapshot().await;
    let old_row = by_key(first.scan_all().await.unwrap())[&PrimaryKey::U64(3)].row_id;

    f.write(vec![upsert(3, json!({ "n": 33 }))]).await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let second = f.snapshot().await;
    let docs = by_key(second.scan_all().await.unwrap());
    assert_eq!(docs.len(), 5);
    let replaced = &docs[&PrimaryKey::U64(3)];
    assert_eq!(replaced.source, crate::common::obj(json!({ "n": 33 })));
    assert_ne!(replaced.row_id, old_row);
    // The old row is deleted in Lance …
    assert_eq!(second.take_rows(&[old_row]).await.unwrap(), vec![None]);
    // … and its split doc is in the first split's new delete bitmap.
    let (split, doc_id) = second.locate_row(old_row).expect("the old row's split doc");
    assert_eq!(split, 0);
    let deleted = second.deleted_docs(&second.splits()[0]).await.unwrap();
    assert_eq!(deleted.iter().collect::<Vec<_>>(), [doc_id]);
    assert!(second.splits()[0].delete_bitmap.is_some());
    assert_eq!(second.splits()[0].deleted_count, 1);
    assert_eq!(second.manifest().live_doc_count, 5);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_of_a_missing_key_is_a_noop() {
    let f = TargetFixture::start(untyped(), 2).await;
    f.write(vec![patch(PrimaryKey::U64(9), json!({ "a": 1 }))])
        .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.applied, f.high_watermarks().await);
    assert_eq!(manifest.live_doc_count, 0);
    assert_eq!(manifest.lance_version, 0, "nothing was written to Lance");
    assert!(manifest.splits.is_empty());
    assert!(manifest.pk_delta.is_none());
    assert_eq!(manifest.dead_letters_total, 0);
    assert!(f.snapshot().await.scan_all().await.unwrap().is_empty());
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_with_upsert_inserts_when_missing() {
    let f = TargetFixture::start(untyped(), 2).await;
    let k = PrimaryKey::U64(9);
    f.write(vec![DocOp::Patch {
        pk: k.clone(),
        mode: PatchMode::MergeDeep,
        source: crate::common::obj(json!({ "a": 1 })),
        delete_keys: vec![],
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: Some(doc(k.clone(), json!({ "u": 1 }))),
    }])
    .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].source, crate::common::obj(json!({ "u": 1 })));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_reads_the_committed_document() {
    let f = TargetFixture::start(untyped(), 2).await;
    let source = f.source(f.factory());
    f.write(vec![upsert(1, json!({ "a": 1, "n": { "x": 1 } }))])
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    f.write(vec![patch(PrimaryKey::U64(1), json!({ "n": { "y": 2 } }))])
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].source,
        crate::common::obj(json!({ "a": 1, "n": { "x": 1, "y": 2 } }))
    );
    assert_eq!(f.manifest().await.version, 2);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn an_undecodable_record_is_dead_lettered() {
    let f = TargetFixture::start(untyped(), 2).await;
    f.write(vec![upsert(1, json!({ "a": 1 }))]).await;
    let (key, value) = (b"\x09not a key".to_vec(), b"\x01garbage".to_vec());
    let offset = f
        .append_raw(
            1,
            Record {
                key: Some(Bytes::from(key.clone())),
                value: Some(Bytes::from(value.clone())),
                headers: vec![],
                timestamp_ms: -1,
            },
        )
        .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.dead_letters_total, 1);
    assert_eq!(manifest.applied, f.high_watermarks().await);
    let path = manifest.dead_letters.expect("a dead-letter object");
    let (bytes, _) = f.store.get(&path).await.unwrap();
    let letters = decode_dead_letters(&bytes).unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!((letters[0].partition, letters[0].offset), (1, offset));
    assert_eq!(letters[0].key.as_deref(), Some(key.as_slice()));
    assert_eq!(letters[0].value.as_deref(), Some(value.as_slice()));
    assert!(
        letters[0].reason.starts_with("undecodable: "),
        "{}",
        letters[0].reason
    );
    assert_eq!(f.snapshot().await.scan_all().await.unwrap().len(), 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn a_patch_that_breaks_the_schema_is_dead_lettered() {
    let f = TargetFixture::start(tagged(), 2).await;
    let source = f.source(f.factory());
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    // Bypasses the writer, which would refuse it.
    let k = PrimaryKey::U64(1);
    f.append_op(home(&k, 2), &patch(k.clone(), json!({ "n": "abc" })))
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 2);
    assert_eq!(manifest.dead_letters_total, 1);
    let (bytes, _) = f
        .store
        .get(manifest.dead_letters.as_deref().expect("dead letters"))
        .await
        .unwrap();
    let letters = decode_dead_letters(&bytes).unwrap();
    assert!(
        letters[0].reason.starts_with("schema: n: "),
        "{}",
        letters[0].reason
    );
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs[0].source, crate::common::obj(json!({ "n": 1 })));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Review focus 5: a vector added after documents exist.
#[tokio::test]
async fn documents_written_before_a_vector_was_added_read_it_as_absent() {
    let f = TargetFixture::start(tagged(), 2).await;
    let source = f.source(f.factory());
    f.write((1..=3).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let with_vector = CollectionSchema::new(
        tagged().fields,
        vec![vector("v2", 2)],
        DynamicMapping::Ignore,
    );
    let version = f
        .meta
        .client
        .update_collection_schema(f.cid, 1, with_vector)
        .await
        .expect("add a vector");
    assert_eq!(version, 2);
    let ops = (4..=5)
        .map(|n| {
            let mut d = doc(PrimaryKey::U64(n), json!({ "n": n }));
            d.vectors.insert("v2".to_string(), vec![n as f32, 1.0]);
            DocOp::Upsert(d)
        })
        .collect();
    f.write(ops).await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let snapshot = f.snapshot().await;
    let docs = by_key(snapshot.scan_all().await.unwrap());
    for n in 1..=3 {
        assert!(docs[&PrimaryKey::U64(n)].vectors.is_empty(), "doc {n}");
    }
    for n in 4..=5 {
        assert_eq!(
            docs[&PrimaryKey::U64(n)].vectors,
            BTreeMap::from([("v2".to_string(), vec![n as f32, 1.0])])
        );
    }
    let splits = snapshot.splits();
    assert_eq!(splits.len(), 2);
    assert_eq!(splits[0].schema_version, 1);
    assert_eq!(splits[1].schema_version, 2);
    assert_eq!(snapshot.manifest().schema_version, 2);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test]
async fn a_fully_deleted_split_leaves_the_manifest() {
    let f = TargetFixture::start(tagged(), 2).await;
    let source = f.source(f.factory());
    f.write(vec![upsert(1, json!({})), upsert(2, json!({}))])
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    f.write(vec![upsert(3, json!({}))]).await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let kept = f.manifest().await.splits[1].ulid;
    f.write(vec![
        DocOp::Delete(PrimaryKey::U64(1)),
        DocOp::Delete(PrimaryKey::U64(2)),
    ])
    .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(
        manifest.splits.iter().map(|s| s.ulid).collect::<Vec<_>>(),
        [kept]
    );
    assert_eq!(manifest.live_doc_count, 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The live keys of the docs of `split` (in `snapshot`) matching `term` in
/// field `field`, and how many matching docs are deleted.
async fn postings(
    snapshot: &operon_collection::CollectionSnapshot,
    field: &str,
    term: u64,
) -> (BTreeSet<PrimaryKey>, usize) {
    let docs = by_key(snapshot.scan_all().await.expect("scan"));
    let rows: BTreeMap<u64, PrimaryKey> = docs.values().map(|d| (d.row_id, d.pk.clone())).collect();
    let mut live = BTreeSet::new();
    let mut masked = 0;
    for split in snapshot.splits() {
        let index = snapshot.open_split(split).await.expect("open split");
        let searcher = operon_text::warm_up_all(&index).await.expect("warm");
        let deleted = snapshot.deleted_docs(split).await.expect("bitmap");
        let field = searcher.schema().get_field(field).expect("field");
        let query = TermQuery::new(
            tantivy::Term::from_field_u64(field, term),
            IndexRecordOption::Basic,
        );
        let row_ids = searcher
            .segment_reader(0)
            .fast_fields()
            .u64("_rowid")
            .expect("_rowid");
        for hit in searcher.search(&query, &DocSetCollector).expect("search") {
            if deleted.contains(hit.doc_id) {
                masked += 1;
                continue;
            }
            let row = row_ids.first(hit.doc_id).expect("a row id");
            live.insert(rows[&row].clone());
        }
    }
    (live, masked)
}

/// Overview A28: a sparse vector is a Lance column and postings plus weights
/// in the split, and deleted docs' postings are masked by the bitmaps.
#[tokio::test]
async fn sparse_vectors_are_committed_to_lance_and_the_split() {
    let schema =
        CollectionSchema::new(vec![], vec![], DynamicMapping::Ignore).with_sparse_vectors(vec![
            SparseVectorSpec {
                name: "s".to_string(),
                modifier: SparseModifier::Idf,
            },
        ]);
    let f = TargetFixture::start(schema, 2).await;
    let source = f.source(f.factory());
    let with_s = |n: u64| {
        let mut d = doc(PrimaryKey::U64(n), json!({ "n": n }));
        d.sparse_vectors
            .insert("s".to_string(), sparse(&[n as u32, 100], &[1.0, n as f32]));
        DocOp::Upsert(d)
    };
    f.write((1..=4).map(with_s).collect()).await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let sparse_patch = |n: u64, s| DocOp::Patch {
        pk: PrimaryKey::U64(n),
        mode: PatchMode::MergeDeep,
        source: crate::common::obj(json!({})),
        delete_keys: vec![],
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::from([("s".to_string(), s)]),
        upsert: None,
    };
    f.write(vec![
        sparse_patch(2, Some(sparse(&[7], &[0.5]))),
        sparse_patch(3, None),
        upsert(4, json!({ "n": 4 })),
    ])
    .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    assert_verified(f.verify().await);

    let snapshot = f.snapshot().await;
    let docs = by_key(snapshot.scan_all().await.unwrap());
    // Lance: key 1 keeps its vector, 2 has the new one, 3 and 4 have none.
    assert_eq!(
        docs[&PrimaryKey::U64(2)].sparse_vectors["s"],
        sparse(&[7], &[0.5])
    );
    assert!(docs[&PrimaryKey::U64(3)].sparse_vectors.is_empty());
    assert!(docs[&PrimaryKey::U64(4)].sparse_vectors.is_empty());
    // The split weights of every live doc are its vector.
    for doc in docs.values() {
        let (split, doc_id) = snapshot.locate_row(doc.row_id).unwrap();
        let index = snapshot
            .open_split(&snapshot.splits()[split])
            .await
            .unwrap();
        let searcher = operon_text::warm_up_all(&index).await.unwrap();
        let weights = searcher
            .segment_reader(0)
            .fast_fields()
            .bytes(&sparse_weights_field("s"))
            .unwrap()
            .expect("a weights column");
        let mut ords = weights.term_ords(doc_id);
        match doc.sparse_vectors.get("s") {
            Some(vector) => {
                let mut bytes = Vec::new();
                weights
                    .ord_to_bytes(ords.next().expect("weights"), &mut bytes)
                    .unwrap();
                assert_eq!(decode_sparse_weights(&bytes).unwrap(), *vector);
            }
            None => assert!(ords.next().is_none(), "{:?} has no weights", doc.pk),
        }
    }
    // Postings: only live docs match, and the old docs of 2, 3 and 4 are
    // masked by the new delete bitmap.
    let field = sparse_postings_field("s");
    let keys = |ns: &[u64]| {
        ns.iter()
            .map(|n| PrimaryKey::U64(*n))
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(postings(&snapshot, &field, 1).await, (keys(&[1]), 0));
    assert_eq!(postings(&snapshot, &field, 2).await, (keys(&[]), 1));
    assert_eq!(postings(&snapshot, &field, 7).await, (keys(&[2]), 0));
    assert_eq!(postings(&snapshot, &field, 100).await, (keys(&[1]), 3));
    assert_eq!(
        postings(&snapshot, &field, SPARSE_PRESENT).await,
        (keys(&[1, 2]), 3)
    );
    f.shutdown().await;
}

#[tokio::test]
async fn a_batch_of_only_dead_letters_advances_applied() {
    let f = TargetFixture::start(untyped(), 2).await;
    f.append_raw(
        0,
        Record {
            key: None,
            value: Some(Bytes::from_static(b"\x01")),
            headers: vec![],
            timestamp_ms: -1,
        },
    )
    .await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.applied, f.high_watermarks().await);
    assert_eq!(manifest.dead_letters_total, 1);
    assert_eq!(manifest.lance_version, 0);
    assert!(manifest.splits.is_empty());
    assert!(manifest.pk_delta.is_none());
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Step 11: a commit that took longer than `max_commit_delay` is refused
/// by the target itself, before any CAS (the metastore's own freshness
/// refusal would say "stale object").
#[tokio::test]
async fn a_commit_past_max_commit_delay_is_blocked_and_logged() {
    let config = CollectionConfig {
        max_commit_delay: Duration::from_secs(1),
        ..CollectionConfig::default()
    };
    let f = TargetFixture::start_with(tagged(), 2, config).await;
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    let hook = once_at(CollectionCommitStep::AfterManifestPut, || {
        tokio::time::sleep(Duration::from_millis(1_200)).boxed()
    });
    let source = f.source(f.hooked_factory(hook));
    let err = link_error(ran(f.run_once(&source, "w1").await));
    let LinkError::Blocked(why) = &err else {
        panic!("expected Blocked, got {err:?}");
    };
    assert!(
        why.contains("longer than"),
        "not the target's own check: {why}"
    );
    assert!(
        !why.contains("stale object"),
        "the CAS was attempted: {why}"
    );
    let key = collection_pointer_key(f.cid);
    let pointer = f
        .meta
        .client
        .read(Consistency::Linearizable, |s| {
            s.pointer(f.ns, &key).cloned()
        })
        .await
        .unwrap();
    assert_eq!(pointer, None, "nothing was committed");
    assert_ran_ok(f.run_once(&source, "w1").await);
    assert_eq!(f.manifest().await.version, 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The CAS is applied but its acknowledgement is lost: the retry sees the
/// pointer at our manifest, and the commit reports success at exactly +1.
#[tokio::test]
async fn a_lost_cas_ack_is_recognised() {
    let f = TargetFixture::start(tagged(), 2).await;
    let meta = f.meta.client.clone();
    // The next write after the manifest PUT is the CAS.
    let hook = once_at(CollectionCommitStep::AfterManifestPut, move || {
        meta.inject_lost_ack();
        futures::future::ready(()).boxed()
    });
    let factory = f.hooked_factory(hook);
    let target = factory
        .open(&f.meta.client.clone().into(), &f.link().await)
        .unwrap();
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    let state = target.load().await.unwrap();
    let batch = f.batch_after(&state).await;
    let fence = f.fence("w1").await;
    let result = target.commit(0, batch, &fence).await;
    assert!(matches!(result, Ok(1)), "{result:?}");
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 1);
    // The commit went on past the CAS: the PK index reflects it.
    assert_eq!(f.pk_watermark().await.applied, manifest.applied);
    f.write(vec![upsert(2, json!({ "n": 2 }))]).await;
    assert_ran_ok(f.run_once(&f.source(factory), "w1").await);
    assert_eq!(f.manifest().await.version, 2);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// `load` opens no writer: an outside writer opened after it fences
/// nothing the target holds, so the target's commit is not `Fenced`.
#[tokio::test]
async fn load_opens_no_writer() {
    let f = TargetFixture::start(tagged(), 2).await;
    // Another factory's target commits first, so the index exists and that
    // target holds a (soon fenced) handle.
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    assert_ran_ok(f.run_once(&f.source(f.factory()), "w1").await);

    let factory = f.factory();
    let link = f.link().await;
    let target = factory.open(&f.meta.client.clone().into(), &link).unwrap();
    let state = target.load().await.unwrap();
    assert_eq!(state.version, 1);
    let outside = PkIndex::open(
        &f.store,
        &collection_pk_prefix(f.ns, f.cid),
        PkIndexConfig::default(),
    )
    .await
    .unwrap();
    outside.close().await.unwrap();

    f.write(vec![upsert(2, json!({ "n": 2 }))]).await;
    let batch = f.batch_after(&state).await;
    let fence = f.fence("w9").await;
    let result = target.commit(state.version, batch, &fence).await;
    assert!(!matches!(result, Err(CommitError::Fenced)), "{result:?}");
    assert_eq!(result.unwrap(), 2);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Review focus 4: a collection dropped while its task runs. The commit's
/// CAS is refused, the task fails, and the link is no longer a candidate.
#[tokio::test]
async fn a_task_of_a_dropped_collection_stops() {
    let f = TargetFixture::start(tagged(), 2).await;
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    let (meta, ns) = (f.meta.client.clone(), f.ns);
    let dropped = Arc::new(AtomicBool::new(false));
    let hook = {
        let dropped = dropped.clone();
        once_at(CollectionCommitStep::AfterManifestPut, move || {
            let (meta, dropped) = (meta.clone(), dropped.clone());
            async move {
                meta.drop_collection(ns, "docs").await.expect("drop");
                dropped.store(true, Ordering::SeqCst);
            }
            .boxed()
        })
    };
    let factory = f.hooked_factory(hook);
    let source = f.source(factory.clone());
    let link = f.link().await;
    let err = link_error(ran(f.run_once(&source, "w1").await));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(err, LinkError::NotFound(_)), "{err:?}");
    // The link went with the collection: no candidate, and a target of it
    // finds nothing.
    assert!(source.candidates(&f.meta.client).await.unwrap().is_empty());
    let err = factory
        .open(&f.meta.client.clone().into(), &link)
        .unwrap()
        .load()
        .await
        .unwrap_err();
    assert!(matches!(err, LinkError::NotFound(_)), "{err:?}");
    f.shutdown().await;
}

/// A record on a partition other than its key's (controller ruling P8) is
/// dead-lettered: latest-wins per key needs one partition per key.
#[tokio::test]
async fn a_record_on_the_wrong_partition_is_dead_lettered() {
    let f = TargetFixture::start(tagged(), 3).await;
    let k = PrimaryKey::U64(4);
    let wrong = (home(&k, 3) + 1) % 3;
    f.write(vec![upsert(4, json!({ "n": 1 }))]).await;
    let offset = f.append_op(wrong, &upsert(4, json!({ "n": 2 }))).await;
    let source = f.source(f.factory());
    assert_ran_ok(f.run_once(&source, "w1").await);
    let manifest = f.manifest().await;
    assert_eq!(manifest.dead_letters_total, 1);
    let (bytes, _) = f
        .store
        .get(manifest.dead_letters.as_deref().expect("dead letters"))
        .await
        .unwrap();
    let letters = decode_dead_letters(&bytes).unwrap();
    assert_eq!((letters[0].partition, letters[0].offset), (wrong, offset));
    assert!(
        letters[0].reason.starts_with("wrong_partition"),
        "{}",
        letters[0].reason
    );
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs[0].source, crate::common::obj(json!({ "n": 1 })));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A patch whose result equals the committed document is not a write: the
/// row, its `seq_no` and the Lance version stay (so the result does not
/// depend on how records were batched).
#[tokio::test]
async fn a_patch_that_changes_nothing_keeps_the_row() {
    let f = TargetFixture::start(untyped(), 2).await;
    let source = f.source(f.factory());
    f.write(vec![upsert(1, json!({ "a": 1, "n": { "x": 1 } }))])
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let before = f.snapshot().await;
    let row = before.scan_all().await.unwrap().remove(0);
    f.write(vec![patch(PrimaryKey::U64(1), json!({ "n": { "x": 1 } }))])
        .await;
    assert_ran_ok(f.run_once(&source, "w1").await);
    let after = f.snapshot().await;
    assert_eq!(after.manifest().version, 2);
    assert_eq!(
        after.manifest().lance_version,
        before.manifest().lance_version
    );
    assert_eq!(after.scan_all().await.unwrap(), vec![row]);
    assert!(after.manifest().pk_delta.is_none());
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Ruling 19's envelope on PK deltas and dead letters, and the bare PK
/// index values (controller ruling P16).
#[test]
fn pk_values_deltas_and_dead_letters_round_trip_and_reject_corruption() {
    use operon_collection::{
        DEAD_LETTERS_MAGIC, DeadLetter, PK_DELTA_MAGIC, decode_pk_delta, encode_dead_letters,
        encode_pk_delta, parse_pk_value, pk_value,
    };
    let value = pk_value(0x0102_0304_0506_0708);
    assert_eq!(value, [1, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(parse_pk_value(&value).unwrap(), 0x0102_0304_0506_0708);
    assert!(parse_pk_value(&value[..8]).is_err());
    assert!(parse_pk_value(&[2, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());

    // Sorted by key on encode.
    let entries = vec![
        (vec![3, b'b'], None),
        (vec![1, 0, 0, 0, 0, 0, 0, 0, 1], Some(7)),
    ];
    let bytes = encode_pk_delta(&entries);
    assert_eq!(&bytes[..4], PK_DELTA_MAGIC);
    assert_eq!(&bytes[4..6], &1u16.to_le_bytes());
    let mut sorted = entries.clone();
    sorted.sort();
    assert_eq!(decode_pk_delta(&bytes).unwrap(), sorted);
    for i in 0..bytes.len() {
        let mut flipped = bytes.to_vec();
        flipped[i] ^= 0x10;
        assert!(decode_pk_delta(&flipped).is_err(), "byte {i}");
    }
    assert!(decode_pk_delta(&bytes[..bytes.len() - 1]).is_err());

    let letters = vec![DeadLetter {
        partition: 2,
        offset: 9,
        key: None,
        value: Some(vec![1, 2, 3]),
        reason: "undecodable: missing record key".to_string(),
    }];
    let bytes = encode_dead_letters(&letters);
    assert_eq!(&bytes[..4], DEAD_LETTERS_MAGIC);
    assert_eq!(decode_dead_letters(&bytes).unwrap(), letters);
    for i in 0..bytes.len() {
        let mut flipped = bytes.to_vec();
        flipped[i] ^= 0x01;
        assert!(decode_dead_letters(&flipped).is_err(), "byte {i}");
    }
}

/// A dropped collection's target (and its open PK index writer) is evicted
/// by the factory on the next poll, although its link is never a candidate
/// again.
#[tokio::test]
async fn a_dropped_collections_target_is_evicted_and_closed() {
    let f = TargetFixture::start(tagged(), 2).await;
    f.write(vec![upsert(1, json!({ "n": 1 }))]).await;
    let factory = f.factory();
    let source = f.source(factory.clone());
    assert_ran_ok(f.run_once(&source, "w1").await);
    assert_eq!(factory.cached_links(), [f.link]);
    f.meta
        .client
        .drop_collection(f.ns, "docs")
        .await
        .expect("drop");
    assert!(source.candidates(&f.meta.client).await.unwrap().is_empty());
    assert!(factory.cached_links().is_empty());
    f.shutdown().await;
}
