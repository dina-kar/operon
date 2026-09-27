//! Bulk import from object storage (D1 Task 8): the operation, the mapping
//! and `_id` rules, conditional reads, `on_error`, retries, cancel, resume
//! and the limits. The source is an in-memory store that counts reads, the
//! sink an in-memory map by `_id` (`operon`'s tests import into a real
//! collection).

mod import_harness;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use import_harness::{Harness, SOURCE, config, parquet_bytes, rows, until};
use operon_durable::OperationState;
use operon_durable::import::{
    FILE_PREFIX, IdType, ImportError, SinkError, Step, StepHook, derived_id,
};
use serde_json::json;

fn parquet_body() -> serde_json::Value {
    json!({ "source": SOURCE, "format": "parquet" })
}

fn ndjson(rows: impl IntoIterator<Item = serde_json::Value>) -> bytes::Bytes {
    let mut out = String::new();
    for row in rows {
        out.push_str(&row.to_string());
        out.push('\n');
    }
    bytes::Bytes::from(out)
}

// ─── imports_parquet_and_ndjson ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn imports_parquet_and_ndjson() {
    let h = Harness::start(config()).await;
    h.put("a.parquet", parquet_bytes(&rows("a", 0, 10, true), 4))
        .await;
    h.put("b.parquet", parquet_bytes(&rows("b", 0, 5, true), 4))
        .await;
    h.put("skip.txt", "not a file of the import".into()).await;
    let (id, created) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "pattern": "*.parquet" }),
            None,
        )
        .await;
    assert!(created);
    let op = h.succeeded(&id).await;
    let result = op.result.expect("result");
    assert_eq!(result["files_total"], 2, "{result}");
    assert_eq!(result["files_done"], 2, "{result}");
    assert_eq!(result["files_failed"], 0, "{result}");
    assert_eq!(result["rows_written"], 15, "{result}");
    assert!(result["bytes_read"].as_u64().unwrap() > 0, "{result}");
    // The merged token covers every write: the largest counter.
    assert_eq!(
        result["token"],
        h.sink.writes.load(Ordering::SeqCst).to_string(),
        "{result}"
    );
    assert_eq!(h.sink.len(), 15);
    let a3 = h.sink.doc("a3").expect("a3");
    assert_eq!(a3.row["n"], 3);
    assert_eq!(a3.row["t"], "row 3");
    assert_eq!(h.counting.reads("skip.txt"), 0, "the pattern filters");
    assert_eq!(op.target, json!({ "collection": "docs" }));
    assert_eq!(op.kind, "collection.import");
    assert_eq!(op.progress["files_total"], 2, "{:?}", op.progress);
    assert_eq!(op.progress["files_done"], 2, "{:?}", op.progress);
    assert_eq!(op.progress["rows_written"], 15, "{:?}", op.progress);

    // NDJSON (slice boundaries: `ndjson_slices_split_at_newlines`).
    let lines = (0..30).map(|n| json!({ "_id": format!("j{n}"), "n": n, "t": "x".repeat(n % 7) }));
    h.put("lines.ndjson", ndjson(lines)).await;
    let before = h.sink.len();
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "ndjson", "pattern": "*.ndjson" }),
            None,
        )
        .await;
    let op = h.succeeded(&id).await;
    let result = op.result.expect("result");
    assert_eq!(result["rows_written"], 30, "{result}");
    assert_eq!(h.sink.len(), before + 30);
    for n in 0..30 {
        let doc = h.sink.doc(&format!("j{n}")).expect("imported");
        assert_eq!(doc.writes, 1, "j{n} written once");
        assert_eq!(doc.row["n"], n);
    }
    h.stop().await;
}

// ─── ndjson_slices_split_at_newlines ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn ndjson_slices_split_at_newlines() {
    for slice_bytes in [1_u64, 7, 23, 24, 25, 64, 1 << 20] {
        let mut config = config();
        config.ndjson_slice_bytes = slice_bytes;
        let h = Harness::start(config).await;
        let lines =
            (0..17).map(|n| json!({ "_id": format!("r{n}"), "pad": "y".repeat(n * 3 % 11) }));
        h.put("f.ndjson", ndjson(lines)).await;
        let (id, _) = h
            .submit(json!({ "source": SOURCE, "format": "ndjson" }), None)
            .await;
        let op = h.succeeded(&id).await;
        let result = op.result.expect("result");
        assert_eq!(
            result["rows_written"], 17,
            "slices of {slice_bytes}: {result}"
        );
        let ids: BTreeSet<String> = h.sink.ids().into_iter().collect();
        let expected: BTreeSet<String> = (0..17).map(|n| format!("r{n}")).collect();
        assert_eq!(ids, expected, "slices of {slice_bytes}");
        assert!(
            h.sink
                .docs
                .lock()
                .expect("lock")
                .values()
                .all(|d| d.writes == 1),
            "slices of {slice_bytes}: a line was read by two slices"
        );
        h.stop().await;
    }
}

// ─── rows_without_id_get_deterministic_ids ──────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn rows_without_id_get_deterministic_ids() {
    let h = Harness::start(config()).await;
    h.put("0.parquet", parquet_bytes(&rows("", 0, 6, false), 4))
        .await;
    h.put("1.parquet", parquet_bytes(&rows("", 6, 3, false), 4))
        .await;
    let (id, _) = h.submit(parquet_body(), None).await;
    h.succeeded(&id).await;
    let mut expected = BTreeSet::new();
    // File 0: row groups of 4 and 2 rows; file 1: one of 3.
    for (file, slice, count) in [(0, 0, 4), (0, 1, 2), (1, 0, 3)] {
        for row in 0..count {
            expected.insert(derived_id(&id, file, slice, row));
        }
    }
    let ids: BTreeSet<String> = h.sink.ids().into_iter().collect();
    assert_eq!(ids, expected);
    let doc = h.sink.doc(ids.iter().next().unwrap()).unwrap();
    assert_eq!(doc.id_type, Some(IdType::Uuid));
    h.stop().await;
}

// ─── id_column_becomes_the_id ───────────────────────────────────────────────

/// A file with `key` (text), `n` and `t`, and a null `key` in row `null`.
fn keyed(null: Option<usize>) -> RecordBatch {
    let keys: Vec<Option<String>> = (0..4)
        .map(|n| (Some(n) != null).then(|| format!("k{n}")))
        .collect();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("key", Arc::new(StringArray::from(keys))),
        ("n", Arc::new(Int64Array::from(vec![0, 1, 2, 3]))),
        ("t", Arc::new(StringArray::from(vec!["a", "b", "c", "d"]))),
    ];
    RecordBatch::try_from_iter(columns).expect("batch")
}

#[tokio::test(flavor = "multi_thread")]
async fn id_column_becomes_the_id() {
    let h = Harness::start(config()).await;
    h.put("f.parquet", parquet_bytes(&keyed(None), 10)).await;
    let mapping = json!({ "id_column": "key", "id_type": "str", "columns": { "t": "title" } });
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "mapping": mapping }),
            None,
        )
        .await;
    h.succeeded(&id).await;
    assert_eq!(h.sink.ids(), ["k0", "k1", "k2", "k3"]);
    let doc = h.sink.doc("k2").unwrap();
    assert_eq!(doc.row["title"], "c", "{doc:?}");
    assert!(doc.row.get("t").is_none(), "renamed: {doc:?}");
    assert!(doc.row.get("key").is_none(), "renamed to _id: {doc:?}");
    assert_eq!(doc.id_type, Some(IdType::Str));
    h.stop().await;

    // A file without the id_column fails with id_column_missing.
    let h = Harness::start(config()).await;
    h.put("f.parquet", parquet_bytes(&rows("x", 0, 3, false), 10))
        .await;
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "mapping": { "id_column": "key" } }),
            None,
        )
        .await;
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    assert_eq!(
        op.error.as_ref().unwrap().code,
        "id_column_missing",
        "{op:?}"
    );
    assert_eq!(h.sink.len(), 0);

    // id_column with a columns target of _id, and two columns with one
    // target: 400 before submit.
    for mapping in [
        json!({ "id_column": "key", "columns": { "other": "_id" } }),
        json!({ "columns": { "a": "x", "b": "x" } }),
        json!({ "id_column": "key", "columns": { "key": "k" } }),
        json!({ "columns": {}, "unknown": 1 }),
    ] {
        let body = json!({ "source": SOURCE, "format": "parquet", "mapping": mapping });
        let err = operon_durable::import::submit(&h.ops, &h.env, "default", "docs", body, None)
            .await
            .expect_err("refused");
        assert!(matches!(err, ImportError::Invalid(_)), "{mapping}: {err:?}");
    }

    // A target that collides with an unrenamed column: mapping_conflict.
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "mapping": { "columns": { "n": "t" } } }),
            None,
        )
        .await;
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    assert_eq!(
        op.error.as_ref().unwrap().code,
        "mapping_conflict",
        "{op:?}"
    );
    h.stop().await;

    // A null _id (the id_column's) is a row error; no id is generated.
    let h = Harness::start(config()).await;
    h.put("f.parquet", parquet_bytes(&keyed(Some(2)), 10)).await;
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "mapping": { "id_column": "key" } }),
            None,
        )
        .await;
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    let error = op.error.unwrap();
    assert_eq!(error.code, "row_error", "{error:?}");
    assert!(
        error.message.contains("the primary key is null"),
        "{error:?}"
    );
    assert_eq!(h.sink.len(), 0, "no generated id replaced the null");
    h.stop().await;

    // Without id_column, the file's own _id is the id (O5): no UUIDs.
    let h = Harness::start(config()).await;
    h.put("f.parquet", parquet_bytes(&rows("own", 0, 3, true), 10))
        .await;
    let (id, _) = h.submit(parquet_body(), None).await;
    h.succeeded(&id).await;
    assert_eq!(h.sink.ids(), ["own0", "own1", "own2"]);
    h.stop().await;
}

// ─── file_changed_fails_its_branch ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn file_changed_fails_its_branch() {
    // After file 1's layout was read, file 1 is rewritten: its slices'
    // reads carry the planned etag and fail.
    let slot: Arc<std::sync::Mutex<Option<operon_store::Store>>> = Arc::default();
    let hook_slot = slot.clone();
    let hook: StepHook = Arc::new(move |step| {
        if *step == (Step::Layout { file: 1 })
            && let Some(store) = hook_slot.lock().expect("lock").take()
        {
            let bytes = parquet_bytes(&rows("new", 0, 2, true), 10);
            futures::executor::block_on(store.put("1.parquet", bytes)).expect("rewrite");
        }
        false
    });
    let h = Harness::start_with(config(), Some(hook)).await;
    *slot.lock().expect("lock") = Some(h.store.clone());
    for (i, prefix) in ["a", "b", "c"].iter().enumerate() {
        h.put(
            &format!("{i}.parquet"),
            parquet_bytes(&rows(prefix, 0, 3, true), 10),
        )
        .await;
    }
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "on_error": "skip_file",
                    "max_parallel_files": 1 }),
            None,
        )
        .await;
    let op = h.succeeded(&id).await;
    let result = op.result.unwrap();
    assert_eq!(result["files_failed"], 1, "{result}");
    assert_eq!(result["failures"][0]["code"], "file_changed", "{result}");
    assert_eq!(result["failures"][0]["key"], "1.parquet", "{result}");
    let ids: BTreeSet<String> = h.sink.ids().into_iter().collect();
    assert!(ids.contains("a0") && ids.contains("c2"), "{ids:?}");
    assert!(
        !ids.iter()
            .any(|i| i.starts_with('b') || i.starts_with("new")),
        "{ids:?}"
    );
    h.stop().await;
}

// ─── skip_file_on_error_counts_failures ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn skip_file_on_error_counts_failures() {
    let h = Harness::start(config()).await;
    h.put("0.parquet", parquet_bytes(&keyed(None), 10)).await;
    h.put("1.parquet", parquet_bytes(&keyed(Some(1)), 10)).await;
    h.put("2.parquet", parquet_bytes(&keyed(None), 10)).await;
    // A row error under skip_file: the file is skipped, the operation goes on.
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "on_error": "skip_file",
                    "mapping": { "id_column": "key" } }),
            None,
        )
        .await;
    let op = h.succeeded(&id).await;
    let result = op.result.unwrap();
    assert_eq!(result["files_total"], 3, "{result}");
    assert_eq!(result["files_done"], 2, "{result}");
    assert_eq!(result["files_failed"], 1, "{result}");
    assert_eq!(result["failures"][0]["key"], "1.parquet", "{result}");
    assert_eq!(result["failures"][0]["code"], "row_error", "{result}");
    assert_eq!(op.progress["files_failed"], 1, "{:?}", op.progress);
    h.stop().await;

    // The same file under on_error: fail fails the operation.
    let h = Harness::start(config()).await;
    h.put("0.parquet", parquet_bytes(&keyed(None), 10)).await;
    h.put("1.parquet", parquet_bytes(&keyed(Some(1)), 10)).await;
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "on_error": "fail",
                    "mapping": { "id_column": "key" } }),
            None,
        )
        .await;
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    let error = op.error.unwrap();
    assert_eq!(error.code, "row_error", "{error:?}");
    assert!(error.message.contains("1.parquet"), "{error:?}");
    h.stop().await;
}

// ─── backpressure_is_retried_not_failed ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn backpressure_is_retried_not_failed() {
    let h = Harness::start(config()).await;
    // The first 5 writes are refused as over the unapplied budget (429).
    h.sink.refuse(|attempt| {
        (attempt < 5).then(|| SinkError::Retry {
            message: "resource exhausted: the unapplied budget is full".into(),
            after_ms: 20,
        })
    });
    h.put("0.parquet", parquet_bytes(&rows("a", 0, 8, true), 4))
        .await;
    let (id, _) = h.submit(parquet_body(), None).await;
    h.succeeded(&id).await;
    assert_eq!(h.sink.len(), 8);
    assert_eq!(
        h.sink.attempts.load(Ordering::SeqCst),
        7,
        "5 refused + 2 slices"
    );
    h.stop().await;

    // A refusal that outlasts retry_for fails the file with unavailable.
    let mut short = config();
    short.retry_for = Duration::from_millis(300);
    let h = Harness::start(short).await;
    h.sink.refuse(|_| {
        Some(SinkError::Retry {
            message: "unavailable".into(),
            after_ms: 0,
        })
    });
    h.put("0.parquet", parquet_bytes(&rows("a", 0, 2, true), 4))
        .await;
    let (id, _) = h.submit(parquet_body(), None).await;
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    assert_eq!(op.error.unwrap().code, "unavailable");
    h.stop().await;
}

// ─── cancel_stops_between_slices ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn cancel_stops_between_slices() {
    let h = Harness::start(config()).await;
    // One file of 3 slices; the second slice's write waits.
    h.put("0.parquet", parquet_bytes(&rows("a", 0, 6, true), 2))
        .await;
    h.sink.pause_at(1);
    let (id, _) = h.submit(parquet_body(), None).await;
    until("the second slice's write", || {
        h.sink.paused.load(Ordering::SeqCst)
    })
    .await;
    h.ops.cancel(&id).await.expect("cancel");
    h.sink.release();
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Canceled, "{op:?}");
    // The file branch sees the cancel before the third slice.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.sink.writes.load(Ordering::SeqCst), 2, "slice 3 never ran");
    assert_eq!(h.sink.len(), 4);
    h.stop().await;
}

// ─── idempotent_resubmit_resumes ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_resubmit_resumes() {
    let h = Harness::start(config()).await;
    h.put("0.parquet", parquet_bytes(&keyed(None), 10)).await;
    h.put("1.parquet", parquet_bytes(&keyed(None), 10)).await;
    h.put("2.parquet", parquet_bytes(&keyed(Some(0)), 10)).await;
    let body = json!({ "source": SOURCE, "format": "parquet", "on_error": "fail",
                       "max_parallel_files": 1, "mapping": { "id_column": "key" } });
    let (id, created) = h.submit(body.clone(), Some("load-1")).await;
    assert!(created);
    let op = h.finished(&id).await;
    assert_eq!(op.state, OperationState::Failed, "{op:?}");
    let reads = h.counting.all_reads();
    assert!(reads["0.parquet"] > 0 && reads["1.parquet"] > 0);

    // Fix file 2 and resubmit with the same key: the same operation resumes.
    h.put("2.parquet", parquet_bytes(&keyed(None), 10)).await;
    let (again, created) = h.submit(body.clone(), Some("load-1")).await;
    assert_eq!(again, id);
    assert!(created, "a resume starts the operation again");
    let op = h.succeeded(&id).await;
    let result = op.result.unwrap();
    assert_eq!(result["files_done"], 3, "{result}");
    assert_eq!(
        h.counting.reads("0.parquet"),
        reads["0.parquet"],
        "file 0 not re-read"
    );
    assert_eq!(
        h.counting.reads("1.parquet"),
        reads["1.parquet"],
        "file 1 not re-read"
    );
    assert!(h.counting.reads("2.parquet") > reads["2.parquet"]);

    // A resubmit of the finished operation is the same operation.
    let (late, created) = h.submit(body, Some("load-1")).await;
    assert_eq!((late, created), (id.clone(), false));
    // Other parameters under the key: refused.
    let err = operon_durable::import::submit(
        &h.ops,
        &h.env,
        "default",
        "docs",
        json!({ "source": SOURCE, "format": "ndjson" }),
        Some("load-1"),
    )
    .await
    .expect_err("reused");
    assert!(
        matches!(
            err,
            ImportError::Ops(operon_durable::OpsError::IdempotencyKeyReused(_))
        ),
        "{err:?}"
    );
    h.stop().await;
}

// ─── the limits ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn too_many_files_is_400() {
    let mut config = config();
    config.max_files = 2;
    let h = Harness::start(config).await;
    for i in 0..3 {
        h.put(
            &format!("{i}.parquet"),
            parquet_bytes(&rows("a", 0, 1, true), 10),
        )
        .await;
    }
    let err =
        operon_durable::import::submit(&h.ops, &h.env, "default", "docs", parquet_body(), None)
            .await
            .expect_err("too many");
    match err {
        ImportError::Invalid(message) => assert!(message.contains("prefixes"), "{message}"),
        other => panic!("{other:?}"),
    }
    // Narrowed by the pattern, it goes.
    let (id, _) = h
        .submit(
            json!({ "source": SOURCE, "format": "parquet", "pattern": "0.*" }),
            None,
        )
        .await;
    h.succeeded(&id).await;
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn too_many_operations_is_429() {
    let mut config = config();
    config.max_concurrent_operations = 1;
    let h = Harness::start(config).await;
    h.put("0.parquet", parquet_bytes(&rows("a", 0, 2, true), 10))
        .await;
    h.sink.pause_at(0);
    let (first, _) = h.submit(parquet_body(), Some("first")).await;
    let err =
        operon_durable::import::submit(&h.ops, &h.env, "default", "docs", parquet_body(), None)
            .await
            .expect_err("limit");
    assert!(
        matches!(err, ImportError::TooManyOperations { limit: 1 }),
        "{err:?}"
    );
    // An idempotent repeat is not a new operation.
    let (again, created) = h.submit(parquet_body(), Some("first")).await;
    assert_eq!((again, created), (first.clone(), false));
    // Another namespace has its own limit.
    operon_durable::import::submit(&h.ops, &h.env, "other", "docs", parquet_body(), None)
        .await
        .expect("another namespace");
    h.sink.release();
    h.succeeded(&first).await;
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_refuses_bad_requests() {
    let h = Harness::start(config()).await;
    for body in [
        json!({ "source": "gs://bucket/", "format": "parquet" }),
        json!({ "source": "not a url", "format": "parquet" }),
        json!({ "source": SOURCE, "format": "csv" }),
        json!({ "source": SOURCE, "format": "parquet", "max_parallel_files": 0 }),
        json!({ "source": SOURCE, "format": "parquet", "extra": true }),
    ] {
        let err =
            operon_durable::import::submit(&h.ops, &h.env, "default", "docs", body.clone(), None)
                .await
                .expect_err("refused");
        assert!(matches!(err, ImportError::Invalid(_)), "{body}: {err:?}");
    }
    let err =
        operon_durable::import::submit(&h.ops, &h.env, "default", "missing", parquet_body(), None)
            .await
            .expect_err("no collection");
    assert!(matches!(err, ImportError::NotFound(_)), "{err:?}");
    // An empty source succeeds at once.
    let (id, _) = h.submit(parquet_body(), None).await;
    let op = h.succeeded(&id).await;
    assert_eq!(op.result.unwrap()["files_total"], 0);
    h.stop().await;
}

// ─── file roots ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn files_are_tagged_roots_of_their_own() {
    let seen = Arc::new(AtomicBool::new(false));
    let h = Harness::start(config()).await;
    h.put("0.parquet", parquet_bytes(&rows("a", 0, 2, true), 10))
        .await;
    let (id, _) = h.submit(parquet_body(), None).await;
    h.succeeded(&id).await;
    let answer = h
        .server
        .client()
        .process(json!({ "kind": "promise.search", "data": {
            "tags": { "loam:op": id.as_str(), "loam:kind": "file" }, "limit": 10 } }))
        .await
        .expect("search");
    let files = answer["data"]["promises"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{answer}");
    let file = &files[0];
    assert!(
        file["id"].as_str().unwrap().starts_with(FILE_PREFIX),
        "{file}"
    );
    assert_eq!(file["tags"]["loam:file"], "0");
    assert_eq!(file["tags"]["resonate:origin"], file["id"]);
    seen.store(true, Ordering::SeqCst);
    // Retention removes the file roots with their operation.
    let pruned = h.ops.prune_finished(i64::MAX / 2).await.expect("prune");
    assert_eq!(pruned, 1);
    let answer = h
        .server
        .client()
        .process(json!({ "kind": "promise.search", "data": {
            "tags": { "loam:op": id.as_str() }, "limit": 10 } }))
        .await
        .expect("search");
    assert_eq!(answer["data"]["promises"], json!([]), "{answer}");
    assert!(seen.load(Ordering::SeqCst));
    h.stop().await;
}
