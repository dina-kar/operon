//! R1 plan Task 8: documents, tables and index maintenance on TiKV. Every
//! test needs a cluster and skips without `OPERON_TEST_PD`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use operon_live::catalog::{self, IndexSpec};
use operon_live::docs::{self, index_key_range};
use operon_live::{
    AppKeys, Doc, DocId, IndexId, IndexRange, KeyRange, Limits, LiveError, LiveValue, Order,
    TableDef, WriteRecord, pb,
};
use operon_tikv::testing::{self, TEST_LIVE};
use operon_tikv::{CommitMode, Tikv, TxnError, TxnOptions};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

// ---- helpers ----

/// A body's result: a Live error that is a storage error goes back to the
/// runner (which retries conflicts); any other stays in the value.
fn lift<T>(r: Result<T, LiveError>) -> Result<Result<T, LiveError>, TxnError> {
    match r {
        Ok(v) => Ok(Ok(v)),
        Err(e) => e.into_txn().map(Err),
    }
}

/// Runs `$body` (a block returning `Result<_, LiveError>`, with `$txn`
/// bound to the transaction) in one committed run; the listed variables
/// are cloned into each attempt.
macro_rules! in_txn {
    ($tikv:expr, [$($v:ident),*], |$txn:ident| $body:block) => {{
        $(let $v = $v.clone();)*
        let mut opts = TxnOptions::new("test");
        opts.commit_mode = Some(CommitMode::TwoPc);
        $tikv
            .run(opts, move |$txn| {
                $(let $v = $v.clone();)*
                Box::pin(async move { lift(async { $body }.await) })
            })
            .await
            .expect("the transaction commits")
            .value
    }};
}

fn fields(pairs: &[(&str, LiveValue)]) -> BTreeMap<String, LiveValue> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn int(v: i64) -> LiveValue {
    LiveValue::I64(v)
}

fn text(v: &str) -> LiveValue {
    LiveValue::Str(v.to_string())
}

/// A handle on the test Live keyspace under a fresh root, or `None`.
async fn live() -> Option<Tikv> {
    let cluster = testing::cluster().await?;
    Some(cluster.connect(TEST_LIVE).await)
}

async fn define(tikv: &Tikv, name: &str, indexes: &[(&str, &[&str])]) -> TableDef {
    let name = name.to_string();
    let specs: Vec<IndexSpec> = indexes
        .iter()
        .map(|(n, f)| IndexSpec {
            name: (*n).to_string(),
            fields: f.iter().map(|s| (*s).to_string()).collect(),
        })
        .collect();
    in_txn!(tikv, [name, specs], |txn| {
        catalog::define_table(
            txn,
            &AppKeys::dedicated(),
            &name,
            &specs,
            &Limits::default(),
        )
        .await
    })
    .expect("the table is defined")
}

async fn insert(
    tikv: &Tikv,
    table: &TableDef,
    f: BTreeMap<String, LiveValue>,
) -> (DocId, WriteRecord) {
    try_insert(tikv, table, f).await.expect("inserted")
}

async fn try_insert(
    tikv: &Tikv,
    table: &TableDef,
    f: BTreeMap<String, LiveValue>,
) -> Result<(DocId, WriteRecord), LiveError> {
    let table = table.clone();
    in_txn!(tikv, [table, f], |txn| {
        docs::insert(txn, &AppKeys::dedicated(), &table, f, &Limits::default()).await
    })
}

async fn patch(
    tikv: &Tikv,
    table: &TableDef,
    id: DocId,
    f: BTreeMap<String, LiveValue>,
) -> WriteRecord {
    let table = table.clone();
    in_txn!(tikv, [table, f], |txn| {
        docs::patch(
            txn,
            &AppKeys::dedicated(),
            &table,
            id,
            f,
            &Limits::default(),
        )
        .await
    })
    .expect("patched")
}

async fn snapshot(tikv: &Tikv) -> operon_tikv::Snap {
    tikv.snapshot(tikv.now().await.expect("a timestamp"))
        .await
        .expect("a snapshot")
}

async fn scan(tikv: &Tikv, table: &TableDef, range: &IndexRange) -> (Vec<Doc>, KeyRange) {
    let mut snap = snapshot(tikv).await;
    docs::scan(
        &mut snap,
        &AppKeys::dedicated(),
        table,
        range,
        &Limits::default(),
    )
    .await
    .expect("a scan")
}

/// Every key of `range` at a fresh snapshot.
async fn keys_in(tikv: &Tikv, range: &KeyRange) -> BTreeSet<Vec<u8>> {
    let mut snap = snapshot(tikv).await;
    let (lo, hi) = range.bounds();
    snap.scan(lo, hi, usize::MAX)
        .await
        .expect("a scan")
        .into_iter()
        .map(|(k, _)| k)
        .collect()
}

fn set(keys: &[Vec<u8>]) -> BTreeSet<Vec<u8>> {
    keys.iter().cloned().collect()
}

// ---- tests ----

/// The reference order of design §20 §4.3 for the values this test uses.
fn reference(a: &LiveValue, b: &LiveValue) -> Ordering {
    let rank = |v: &LiveValue| match v {
        LiveValue::Null => 0,
        LiveValue::I64(_) => 1,
        LiveValue::F64(_) => 2,
        LiveValue::Bool(_) => 3,
        LiveValue::Str(_) => 4,
        _ => 5,
    };
    match (a, b) {
        (LiveValue::I64(x), LiveValue::I64(y)) => x.cmp(y),
        (LiveValue::F64(x), LiveValue::F64(y)) => x.total_cmp(y),
        (LiveValue::Bool(x), LiveValue::Bool(y)) => x.cmp(y),
        (LiveValue::Str(x), LiveValue::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        _ => rank(a).cmp(&rank(b)),
    }
}

fn scalar() -> impl Strategy<Value = LiveValue> {
    prop_oneof![
        Just(LiveValue::Null),
        (-3i64..3).prop_map(LiveValue::I64),
        any::<i64>().prop_map(LiveValue::I64),
        // Finite and without -0.0, so total_cmp is the index order.
        (-1e6f64..1e6).prop_map(|f| LiveValue::F64(if f == 0.0 { 0.0 } else { f })),
        any::<bool>().prop_map(LiveValue::Bool),
        "[ab\u{0}]{0,3}".prop_map(LiveValue::Str),
    ]
}

/// Documents inserted with random values of `a`, read back through the index
/// on `a` in both directions, come out in value order (ties by creation time
/// and id), and a bounded range returns exactly the documents inside it.
#[test]
fn index_scan_order_matches_value_order() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    let Some(tikv) = rt.block_on(live()) else {
        return;
    };
    let mut runner = TestRunner::new(Config {
        cases: 12,
        ..Config::default()
    });
    let strategy = (
        proptest::collection::vec(scalar(), 1..24),
        scalar(),
        scalar(),
    );
    runner
        .run(&strategy, |(values, lo, hi)| {
            rt.block_on(async {
                let name = format!("t{}", rand_suffix());
                let table = define(&tikv, &name, &[("by_a", &["a"])]).await;
                let mut inserted = Vec::new();
                for v in &values {
                    let (id, _) = insert(&tikv, &table, fields(&[("a", v.clone())])).await;
                    inserted.push((v.clone(), id));
                }
                let index = table.index_id("by_a").expect("the index");
                for order in [Order::Asc, Order::Desc] {
                    let range = IndexRange {
                        order,
                        ..IndexRange::all(table.id, index)
                    };
                    let (docs, _) = scan(&tikv, &table, &range).await;
                    assert_eq!(docs.len(), values.len());
                    let mut seen: Vec<(LiveValue, u64, [u8; 16])> = docs
                        .iter()
                        .map(|d| (d.fields["a"].clone(), d.creation_ms, d.id.bytes))
                        .collect();
                    if order == Order::Desc {
                        seen.reverse();
                    }
                    for pair in seen.windows(2) {
                        let o = reference(&pair[0].0, &pair[1].0)
                            .then(pair[0].1.cmp(&pair[1].1))
                            .then(pair[0].2.cmp(&pair[1].2));
                        assert_eq!(o, Ordering::Less, "{:?} before {:?}", pair[0], pair[1]);
                    }
                }
                // A bounded range: lo <= a < hi.
                let range = IndexRange {
                    lower: Bound::Included(lo.clone()),
                    upper: Bound::Excluded(hi.clone()),
                    ..IndexRange::all(table.id, index)
                };
                let (docs, _) = scan(&tikv, &table, &range).await;
                let got: BTreeSet<DocId> = docs.iter().map(|d| d.id).collect();
                let want: BTreeSet<DocId> = inserted
                    .iter()
                    .filter(|(v, _)| {
                        reference(v, &lo) != Ordering::Less && reference(v, &hi) == Ordering::Less
                    })
                    .map(|(_, id)| *id)
                    .collect();
                assert_eq!(got, want, "[{lo:?}, {hi:?})");
            });
            Ok(())
        })
        .expect("index order holds");
}

fn rand_suffix() -> u64 {
    rand::random::<u64>() % 1_000_000_000
}

#[test]
fn patch_moves_index_entries() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let table = define(&tikv, "msgs", &[("by_channel", &["channel"])]).await;
        let index = table.index_id("by_channel").expect("index");
        let (id, _) = insert(
            &tikv,
            &table,
            fields(&[("channel", text("a")), ("body", text("hi"))]),
        )
        .await;
        let in_channel = |c: &str| IndexRange {
            eq: vec![text(c)],
            ..IndexRange::all(table.id, index)
        };
        assert_eq!(scan(&tikv, &table, &in_channel("a")).await.0.len(), 1);
        patch(&tikv, &table, id, fields(&[("channel", text("b"))])).await;
        assert!(scan(&tikv, &table, &in_channel("a")).await.0.is_empty());
        let (docs, _) = scan(&tikv, &table, &in_channel("b")).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, id);
        // The other field survived the patch.
        assert_eq!(docs[0].fields["body"], text("hi"));
        // One entry per index, no stale one.
        let entries = keys_in(&tikv, &AppKeys::dedicated().table_indexes(table.id)).await;
        assert_eq!(entries.len(), 2, "by_creation_time and by_channel");
    });
}

#[test]
fn delete_removes_all_index_entries() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let table = define(&tikv, "t", &[("by_a", &["a"]), ("by_a_b", &["a", "b"])]).await;
        let app = AppKeys::dedicated();
        let (id, rec) = insert(&tikv, &table, fields(&[("a", int(1)), ("b", int(2))])).await;
        let (other, _) = insert(&tikv, &table, fields(&[("a", int(1))])).await;
        assert_eq!(rec.index_keys_added.len(), 3);
        let table2 = table.clone();
        let deleted = in_txn!(tikv, [table2], |txn| {
            docs::delete(txn, &AppKeys::dedicated(), &table2, id, &Limits::default()).await
        })
        .expect("deleted")
        .expect("it existed");
        assert_eq!(deleted.kind, pb::WriteKind::WRITE_KIND_DELETE);
        assert_eq!(set(&deleted.index_keys_removed), set(&rec.index_keys_added));
        assert!(deleted.index_keys_added.is_empty());
        // Nothing of it is left; the other document is whole.
        let entries = keys_in(&tikv, &app.table_indexes(table.id)).await;
        assert_eq!(entries.len(), 3, "the other document's three entries");
        assert!(entries.iter().all(|k| k.ends_with(&other.bytes)));
        let mut snap = snapshot(&tikv).await;
        assert!(
            docs::get(&mut snap, &app, id)
                .await
                .expect("a read")
                .is_none()
        );
        assert!(
            docs::get(&mut snap, &app, other)
                .await
                .expect("a read")
                .is_some()
        );
        // Deleting again finds nothing.
        let table2 = table.clone();
        let again = in_txn!(tikv, [table2], |txn| {
            docs::delete(txn, &AppKeys::dedicated(), &table2, id, &Limits::default()).await
        })
        .expect("a delete");
        assert!(again.is_none());
    });
}

#[test]
fn write_record_lists_old_and_new_index_keys() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let table = define(&tikv, "t", &[("by_a", &["a"]), ("by_b", &["b"])]).await;
        let app = AppKeys::dedicated();
        let (id, ins) = insert(&tikv, &table, fields(&[("a", int(1)), ("b", int(1))])).await;
        assert_eq!(ins.kind, pb::WriteKind::WRITE_KIND_INSERT);
        assert_eq!(ins.table_id, table.id.0);
        assert_eq!(ins.doc_id, id.bytes.to_vec());
        assert!(ins.index_keys_removed.is_empty());
        assert_eq!(
            set(&ins.index_keys_added),
            keys_in(&tikv, &app.table_indexes(table.id)).await
        );
        // A patch moving `a` only: removed lists every old entry, added every
        // new one; the unchanged entries are in both.
        let rec = patch(&tikv, &table, id, fields(&[("a", int(2))])).await;
        assert_eq!(rec.kind, pb::WriteKind::WRITE_KIND_REPLACE);
        assert_eq!(set(&rec.index_keys_removed), set(&ins.index_keys_added));
        assert_eq!(
            set(&rec.index_keys_added),
            keys_in(&tikv, &app.table_indexes(table.id)).await
        );
        let both: BTreeSet<_> = set(&rec.index_keys_removed)
            .intersection(&set(&rec.index_keys_added))
            .cloned()
            .collect();
        assert_eq!(both.len(), 2, "by_creation_time and by_b did not move");
        let by_a = app.index_prefix(table.id, table.index_id("by_a").expect("by_a"));
        let moved_out: Vec<_> = rec
            .index_keys_removed
            .iter()
            .filter(|k| !both.contains(*k))
            .collect();
        let moved_in: Vec<_> = rec
            .index_keys_added
            .iter()
            .filter(|k| !both.contains(*k))
            .collect();
        assert_eq!(moved_out.len(), 1);
        assert_eq!(moved_in.len(), 1);
        assert!(moved_out[0].starts_with(&by_a) && moved_in[0].starts_with(&by_a));
        // A replace keeps the id and the creation time.
        let table2 = table.clone();
        let replaced = in_txn!(tikv, [table2], |txn| {
            docs::replace(
                txn,
                &AppKeys::dedicated(),
                &table2,
                id,
                fields(&[("b", int(9))]),
                &Limits::default(),
            )
            .await
        })
        .expect("replaced");
        assert_eq!(
            set(&replaced.index_keys_removed),
            set(&rec.index_keys_added)
        );
        let mut snap = snapshot(&tikv).await;
        let doc = docs::get(&mut snap, &app, id)
            .await
            .expect("a read")
            .expect("present");
        assert_eq!(doc.fields, fields(&[("b", int(9))]));
        let (_, first) = app
            .doc_of_index_entry(table.id, &ins.index_keys_added[0])
            .expect("an entry");
        assert_eq!(doc.creation_ms, first);
    });
}

#[test]
fn limit_bounded_scan_reports_range_to_last_key() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let table = define(&tikv, "t", &[("by_n", &["n"])]).await;
        let index = table.index_id("by_n").expect("index");
        for n in 0..10 {
            insert(&tikv, &table, fields(&[("n", int(n))])).await;
        }
        let app = AppKeys::dedicated();
        let full = index_key_range(&app, &table, &IndexRange::all(table.id, index)).expect("range");
        for order in [Order::Asc, Order::Desc] {
            let range = IndexRange {
                order,
                limit: Some(3),
                ..IndexRange::all(table.id, index)
            };
            let (docs, read) = scan(&tikv, &table, &range).await;
            let ns: Vec<LiveValue> = docs.iter().map(|d| d.fields["n"].clone()).collect();
            let entries = keys_in(&tikv, &read).await;
            assert_eq!(
                entries.len(),
                3,
                "the read range holds exactly the 3 entries read"
            );
            if order == Order::Asc {
                assert_eq!(ns, vec![int(0), int(1), int(2)]);
                assert_eq!(read.lo, full.lo);
            } else {
                assert_eq!(ns, vec![int(9), int(8), int(7)]);
                assert_eq!(read.hi, full.hi);
            }
            // An insert past the last key read is outside the read range; one
            // inside it is inside.
            let (_, past) = insert(
                &tikv,
                &table,
                fields(&[("n", int(if order == Order::Asc { 5 } else { 4 }))]),
            )
            .await;
            assert!(!read.contains(&past.index_keys_added[1]));
            let (_, within) = insert(
                &tikv,
                &table,
                fields(&[("n", int(if order == Order::Asc { 1 } else { 8 }))]),
            )
            .await;
            assert!(read.contains(&within.index_keys_added[1]));
        }
        // A range the limit does not fill reports the whole range.
        let range = IndexRange {
            limit: Some(100),
            ..IndexRange::all(table.id, index)
        };
        let (docs, read) = scan(&tikv, &table, &range).await;
        assert_eq!(docs.len(), 14);
        assert_eq!(read, full);
        // A limit above the scan limit is refused.
        let mut snap = snapshot(&tikv).await;
        let limits = Limits {
            max_scanned_docs: 5,
            ..Limits::default()
        };
        let too_many = IndexRange {
            limit: Some(6),
            ..IndexRange::all(table.id, index)
        };
        assert!(matches!(
            docs::scan(&mut snap, &app, &table, &too_many, &limits).await,
            Err(LiveError::LimitExceeded {
                limit: "max_scanned_docs",
                ..
            })
        ));
        // So is an unlimited range holding more.
        assert!(matches!(
            docs::scan(
                &mut snap,
                &app,
                &table,
                &IndexRange::all(table.id, index),
                &limits
            )
            .await,
            Err(LiveError::LimitExceeded {
                limit: "max_scanned_docs",
                ..
            })
        ));
    });
}

#[test]
fn document_over_1_mib_is_refused() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let table = define(&tikv, "t", &[]).await;
        let big = LiveValue::Bytes(vec![7; 1024 * 1024]);
        let err = try_insert(&tikv, &table, fields(&[("blob", big)]))
            .await
            .expect_err("over 1 MiB");
        assert!(
            matches!(
                err,
                LiveError::LimitExceeded {
                    limit: "max_document_bytes",
                    ..
                }
            ),
            "{err}"
        );
        assert_eq!(err.code(), pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED);
        // Nothing was written; just under the limit is fine.
        let app = AppKeys::dedicated();
        assert!(keys_in(&tikv, &app.documents(table.id)).await.is_empty());
        let fits = LiveValue::Bytes(vec![7; 1024 * 1024 - 64]);
        let (id, _) = insert(&tikv, &table, fields(&[("blob", fits.clone())])).await;
        let mut snap = snapshot(&tikv).await;
        let doc = docs::get(&mut snap, &app, id)
            .await
            .expect("a read")
            .expect("present");
        assert_eq!(doc.fields["blob"], fits);
    });
}

#[test]
fn tables_are_created_on_first_insert_and_ids_name_their_table() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let app = AppKeys::dedicated();
        let before = tikv.now().await.expect("ts");
        let (a, b, id) = in_txn!(tikv, [], |txn| {
            let limits = Limits::default();
            let a = catalog::table_for_insert(txn, &AppKeys::dedicated(), "a", &limits).await?;
            let again = catalog::table_for_insert(txn, &AppKeys::dedicated(), "a", &limits).await?;
            assert_eq!(a, again);
            let b = catalog::table_for_insert(txn, &AppKeys::dedicated(), "b", &limits).await?;
            let (id, _) = docs::insert(
                txn,
                &AppKeys::dedicated(),
                &b,
                fields(&[("x", int(1))]),
                &limits,
            )
            .await?;
            Ok::<_, LiveError>((a, b, id))
        })
        .expect("created");
        assert_ne!(a.id, b.id);
        assert!(a.indexes.is_empty());
        assert_eq!(id.table, b.id);
        let mut snap = snapshot(&tikv).await;
        let tables = catalog::list_tables(&mut snap, &app).await.expect("tables");
        assert_eq!(tables, vec![a.clone(), b.clone()]);
        let doc = docs::get(&mut snap, &app, id)
            .await
            .expect("a read")
            .expect("present");
        // _creationTime is the start timestamp's physical time.
        let lo = Tikv::physical_ms(&before);
        let hi = Tikv::physical_ms(&tikv.now().await.expect("ts"));
        assert!(
            (lo..=hi).contains(&doc.creation_ms),
            "{lo} <= {} <= {hi}",
            doc.creation_ms
        );
        // An id of table b is refused on table a.
        let a2 = a.clone();
        let wrong = in_txn!(tikv, [a2], |txn| {
            docs::patch(
                txn,
                &AppKeys::dedicated(),
                &a2,
                id,
                fields(&[]),
                &Limits::default(),
            )
            .await
        });
        assert!(
            matches!(wrong, Err(LiveError::InvalidArgument(_))),
            "{wrong:?}"
        );
        // A by_id scan reads documents by id, bounded by id text.
        let range = IndexRange {
            lower: Bound::Included(LiveValue::Str(id.to_string())),
            upper: Bound::Included(LiveValue::Str(id.to_string())),
            ..IndexRange::all(b.id, IndexId::BY_ID)
        };
        let (docs, _) = scan(&tikv, &b, &range).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, id);
    });
}

#[test]
fn index_change_on_non_empty_table_is_refused() {
    let rt = tokio::runtime::Runtime::new().expect("a runtime");
    rt.block_on(async {
        let Some(tikv) = live().await else { return };
        let empty = define(&tikv, "t", &[("by_a", &["a"])]).await;
        // On an empty table an index change is fine, and the same definition
        // is a no-op; a changed index gets a new id.
        let same = define(&tikv, "t", &[("by_a", &["a"])]).await;
        assert_eq!(same, empty);
        let changed = define(&tikv, "t", &[("by_a", &["a", "b"]), ("by_c", &["c"])]).await;
        assert_ne!(changed.index_id("by_a"), empty.index_id("by_a"));
        assert_eq!(changed.id, empty.id);
        insert(&tikv, &changed, fields(&[("a", int(1))])).await;
        let specs = vec![IndexSpec {
            name: "by_d".to_string(),
            fields: vec!["d".to_string()],
        }];
        let refused = in_txn!(tikv, [specs], |txn| {
            catalog::define_table(txn, &AppKeys::dedicated(), "t", &specs, &Limits::default()).await
        });
        match refused {
            Err(LiveError::FailedPrecondition(m)) => {
                assert!(m.contains("index changes need an empty table in R1"), "{m}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        // Unchanged indexes on a non-empty table are fine.
        let again = define(&tikv, "t", &[("by_a", &["a", "b"]), ("by_c", &["c"])]).await;
        assert_eq!(again, changed);
    });
}
