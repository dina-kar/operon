//! Split merges (plan M1.3 Task 1, Ruling 4; Task 0 E20, E52): Quickwit's
//! stable log merge policy over the manifest's splits, re-indexing the live
//! docs of the inputs in row-id order, committed by the fenced,
//! freshness-checked CAS with rebase.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::common::{TargetFixture, WAIT, doc, field, patch, text, upsert};
use futures::FutureExt;
use operon_collection::{
    CollectionCommitHook, CollectionCommitStep, CollectionManifest, CollectionSchema, CommitKind,
    DocOp, DynamicMapping, FieldKind, MERGE_TASK_PREFIX, MaintenanceConfig, MergePlan, PK_FIELD,
    PrimaryKey, ROWID_FIELD, SPARSE_PRESENT, SparseModifier, SparseVectorSpec, SplitMergeSource,
    SplitRef, plan_merges, row_id_runs,
};
use operon_meta::{Clock, ManualClock, SystemClock};
use operon_quickwit::merge_policy::StableLogMergePolicyConfig;
use operon_worker::{RunResult, TaskError, TaskKey, TaskOutcome, run_once};
use proptest::prelude::*;
use serde_json::json;
use tantivy::collector::DocSetCollector;
use tantivy::query::{ExistsQuery, PhraseQuery, Query, RangeQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};
use tantivy::{Searcher, Term};

/// The lease TTL of a merge run.
const TTL: Duration = Duration::from_secs(30);

/// The lease TTL of a run that crashes: short, so a successor soon takes over.
const CRASHED_TTL: Duration = Duration::from_millis(300);

/// The plan's test policy; every poll proposes the collection.
fn config() -> MaintenanceConfig {
    policy(10, 3, 5)
}

fn policy(
    min_level_num_docs: usize,
    merge_factor: usize,
    max_merge_factor: usize,
) -> MaintenanceConfig {
    MaintenanceConfig {
        merge_policy: StableLogMergePolicyConfig {
            min_level_num_docs,
            merge_factor,
            max_merge_factor,
            maturation_period: Duration::from_hours(48),
        },
        split_num_docs_target: 10_000,
        poll_interval: Duration::ZERO,
        ..MaintenanceConfig::default()
    }
}

/// A keyword, an i64, a text field with positions, and a sparse vector.
fn merge_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            text("body"),
        ],
        vec![],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    schema.validate().expect("valid schema");
    schema
}

const WORDS: [&str; 6] = ["quick", "brown", "fox", "lazy", "dog", "jumps"];

/// Key `k`'s document of generation `g`: a tag, a number, three words of
/// body and a sparse vector.
fn document(k: u64, g: u64) -> DocOp {
    let n = (k * 7 + g * 13) % 50;
    let words: Vec<&str> = (0..3).map(|i| WORDS[((n + i) % 6) as usize]).collect();
    let mut d = doc(
        PrimaryKey::U64(k),
        json!({ "tag": format!("t{}", n % 3), "n": n as i64 - 10, "body": words.join(" ") }),
    );
    let first = (n % 5) as u32;
    d.sparse_vectors.insert(
        "s".to_string(),
        crate::common::sparse(&[first, 10 + (k % 3) as u32], &[0.5 + g as f32, 1.0]),
    );
    DocOp::Upsert(d)
}

/// Writes `ops` and applies them in one link commit.
async fn commit(f: &TargetFixture, ops: Vec<DocOp>) {
    f.write(ops).await;
    f.apply_all(&f.source(f.factory()), "linker").await;
}

/// `count` commits of `size` new keys each, from key `first`.
async fn commits(f: &TargetFixture, first: u64, count: u64, size: u64) {
    for c in 0..count {
        let start = first + c * size;
        commit(f, (start..start + size).map(|k| document(k, 0)).collect()).await;
    }
}

fn outcome(results: Vec<(TaskKey, RunResult)>) -> Result<TaskOutcome, TaskError> {
    let [(key, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the merge task did not run");
    };
    assert!(key.key.starts_with(MERGE_TASK_PREFIX), "{key}");
    result
}

async fn merge_once(
    f: &TargetFixture,
    source: &SplitMergeSource,
) -> Result<TaskOutcome, TaskError> {
    outcome(
        run_once(&f.meta.client, "merger", TTL, source)
            .await
            .expect("run"),
    )
}

/// Runs merges until the policy plans none; every run must succeed.
async fn merge_all(f: &TargetFixture, source: &SplitMergeSource, config: &MaintenanceConfig) {
    for _ in 0..50 {
        if plan(f, config).await.is_empty() {
            return;
        }
        let result = merge_once(f, source).await;
        assert!(result.is_ok(), "{result:?}");
    }
    panic!("merges never ran out");
}

async fn plan(f: &TargetFixture, config: &MaintenanceConfig) -> Vec<MergePlan> {
    plan_merges(&f.manifest().await, config, f.ctx.meta.now_ms())
}

/// The manifest's one split.
fn only_split(manifest: CollectionManifest) -> SplitRef {
    let [split] = <[SplitRef; 1]>::try_from(manifest.splits).expect("one split");
    split
}

fn ulids(splits: &[SplitRef]) -> Vec<ulid::Ulid> {
    splits.iter().map(|s| s.ulid).collect()
}

fn assert_verified(problems: Vec<String>) {
    assert!(problems.is_empty(), "{problems:#?}");
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A hook that holds the first merge at `step` until `release` is
/// notified, and says when it got there.
fn hold_at(
    step: CollectionCommitStep,
) -> (
    CollectionCommitHook,
    Arc<AtomicBool>,
    Arc<tokio::sync::Notify>,
) {
    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let (flag, gate) = (reached.clone(), release.clone());
    let hook: CollectionCommitHook = Arc::new(move |at, _fence| {
        if at == step && !flag.swap(true, Ordering::SeqCst) {
            let gate = gate.clone();
            async move { gate.notified().await }.boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    (hook, reached, release)
}

/// The row id of key `k` in the live version.
async fn row_of(f: &TargetFixture, k: u64) -> u64 {
    f.snapshot()
        .await
        .get_by_pk(&[PrimaryKey::U64(k)])
        .await
        .expect("get")[0]
        .as_ref()
        .expect("the key exists")
        .row_id
}

/// The key whose live row is `row`.
async fn key_at(f: &TargetFixture, row: u64) -> u64 {
    let docs = f.snapshot().await.scan_all().await.expect("scan");
    match docs.iter().find(|d| d.row_id == row).expect("a row").pk {
        PrimaryKey::U64(k) => k,
        ref other => panic!("{other:?}"),
    }
}

/// A live split doc's fast-field values.
#[derive(Clone, Debug, PartialEq)]
struct Fast {
    row_id: u64,
    tag: Option<String>,
    n: Option<i64>,
    weights: Option<Vec<u8>>,
}

fn pk_of(searcher: &Searcher, doc: u32) -> PrimaryKey {
    let pks = searcher
        .segment_reader(0)
        .fast_fields()
        .bytes(PK_FIELD)
        .expect("fast field")
        .expect("pk column");
    let mut bytes = Vec::new();
    let ord = pks.term_ords(doc).next().expect("a pk");
    pks.ord_to_bytes(ord, &mut bytes).expect("bytes");
    PrimaryKey::from_canonical(&bytes).expect("a primary key")
}

fn fast_of(searcher: &Searcher, doc: u32) -> Fast {
    let fast = searcher.segment_reader(0).fast_fields();
    let tag = fast.str("tag").expect("fast field").and_then(|column| {
        let ord = column.term_ords(doc).next()?;
        let mut out = String::new();
        column.ord_to_str(ord, &mut out).expect("str");
        Some(out)
    });
    let weights = fast
        .bytes("_sparse_w.s")
        .expect("fast field")
        .and_then(|column| {
            let ord = column.term_ords(doc).next()?;
            let mut out = Vec::new();
            column.ord_to_bytes(ord, &mut out).expect("bytes");
            Some(out)
        });
    Fast {
        row_id: fast
            .u64(ROWID_FIELD)
            .expect("rowid")
            .first(doc)
            .expect("a row id"),
        tag,
        n: fast.i64("n").expect("n").first(doc),
        weights,
    }
}

type MakeQuery = Box<dyn Fn(&Schema) -> Box<dyn Query>>;

fn term_query(name: &'static str, value: &'static str) -> MakeQuery {
    Box::new(move |schema| {
        let field = schema.get_field(name).expect("field");
        Box::new(TermQuery::new(
            Term::from_field_text(field, value),
            IndexRecordOption::Basic,
        ))
    })
}

fn sparse_query(index: u64) -> MakeQuery {
    Box::new(move |schema| {
        let field = schema.get_field("_sparse.s").expect("field");
        Box::new(TermQuery::new(
            Term::from_field_u64(field, index),
            IndexRecordOption::Basic,
        ))
    })
}

fn n_range(low: i64, high: i64) -> MakeQuery {
    Box::new(move |schema| {
        let field = schema.get_field("n").expect("field");
        Box::new(RangeQuery::new(
            Bound::Included(Term::from_field_i64(field, low)),
            Bound::Included(Term::from_field_i64(field, high)),
        ))
    })
}

fn phrase(words: &'static [&'static str]) -> MakeQuery {
    Box::new(move |schema| {
        let field = schema.get_field("body").expect("field");
        Box::new(PhraseQuery::new(
            words
                .iter()
                .map(|w| Term::from_field_text(field, w))
                .collect(),
        ))
    })
}

/// The plan's 12 queries: keyword terms, i64 ranges, `ExistsQuery`,
/// phrases, a body term and sparse postings.
fn queries() -> Vec<MakeQuery> {
    vec![
        term_query("tag", "t0"),
        term_query("tag", "t1"),
        term_query("tag", "t2"),
        n_range(0, 20),
        n_range(-10, -1),
        Box::new(|_| Box::new(ExistsQuery::new("n".into(), false))),
        Box::new(|_| Box::new(ExistsQuery::new("tag".into(), false))),
        phrase(&["quick", "brown"]),
        phrase(&["fox", "lazy", "dog"]),
        term_query("body", "jumps"),
        sparse_query(3),
        sparse_query(SPARSE_PRESENT),
    ]
}

/// For each query, the keys of the live split docs it matches; and every
/// live split doc's fast-field values by key.
async fn split_view(f: &TargetFixture) -> (Vec<BTreeSet<PrimaryKey>>, BTreeMap<PrimaryKey, Fast>) {
    let snapshot = f.snapshot().await;
    let queries = queries();
    let mut matching = vec![BTreeSet::new(); queries.len()];
    let mut fast = BTreeMap::new();
    for split in snapshot.splits() {
        let index = snapshot.open_split(split).await.expect("open split");
        let searcher = operon_text::warm_up_all(&index).await.expect("warm");
        let deleted = snapshot.deleted_docs(split).await.expect("deleted");
        let doc_count = u32::try_from(split.doc_count).expect("a u32 doc count");
        for doc in (0..doc_count).filter(|d| !deleted.contains(*d)) {
            let previous = fast.insert(pk_of(&searcher, doc), fast_of(&searcher, doc));
            assert!(previous.is_none(), "a key is live in two splits");
        }
        for (query, keys) in queries.iter().zip(&mut matching) {
            let query = query(&index.schema());
            for address in searcher.search(&*query, &DocSetCollector).expect("search") {
                if !deleted.contains(address.doc_id) {
                    keys.insert(pk_of(&searcher, address.doc_id));
                }
            }
        }
    }
    (matching, fast)
}

/// 7 splits of 20 docs: one merge of the 5 newest (`max_merge_factor`);
/// then the level of 20-doc splits holds 2 and the 100-doc split is alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_merges_follows_the_stable_log_policy() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = config();
    commits(&f, 0, 7, 20).await;
    let before = f.manifest().await;
    assert_eq!(before.splits.len(), 7);
    let mut newest = ulids(&before.splits);
    newest.sort_unstable();
    let plans = plan(&f, &config).await;
    assert_eq!(
        plans,
        vec![MergePlan {
            inputs: newest[2..].to_vec(),
            purge: false
        }]
    );

    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let after = f.manifest().await;
    assert_eq!(after.version, before.version + 1);
    assert_eq!(after.parent_version, before.version);
    assert_eq!(after.kind, CommitKind::Maintenance);
    assert_eq!(
        (
            after.applied.clone(),
            after.live_doc_count,
            after.lance_version
        ),
        (
            before.applied.clone(),
            before.live_doc_count,
            before.lance_version
        )
    );
    assert_eq!(after.pk_delta, None);
    assert_eq!(after.splits.len(), 3);
    let merged = after
        .splits
        .iter()
        .find(|s| s.merge_ops == 1)
        .expect("a merged split");
    assert_eq!((merged.doc_count, merged.deleted_count), (100, 0));
    let rows: Vec<u64> = before
        .splits
        .iter()
        .filter(|s| newest[2..].contains(&s.ulid))
        .flat_map(|s| s.row_id_ranges.iter().flat_map(|r| r.clone()))
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .collect();
    assert_eq!(merged.row_id_ranges, row_id_runs(&rows));
    assert!(plan(&f, &config).await.is_empty());
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// 3 splits at schema version 1 and 3 at version 2: two merges, each
/// within one version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn splits_of_different_schema_versions_never_merge() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = config();
    commits(&f, 0, 3, 20).await;
    let mut next = merge_schema();
    next.fields.push(field("extra", FieldKind::Keyword));
    next.version = 2;
    f.meta
        .client
        .update_collection_schema(f.cid, 1, next)
        .await
        .expect("add a field");
    commits(&f, 100, 3, 20).await;
    let manifest = f.manifest().await;
    let version_of: BTreeMap<ulid::Ulid, u64> = manifest
        .splits
        .iter()
        .map(|s| (s.ulid, s.schema_version))
        .collect();
    let plans = plan(&f, &config).await;
    assert_eq!(plans.len(), 2, "{plans:?}");
    for (plan, version) in plans.iter().zip([1, 2]) {
        assert_eq!(plan.inputs.len(), 3);
        assert!(
            plan.inputs.iter().all(|u| version_of[u] == version),
            "{plan:?}"
        );
    }

    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(
        merge_once(&f, &source).await.unwrap(),
        TaskOutcome::MoreWork
    );
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let splits = f.manifest().await.splits;
    let versions: Vec<u64> = splits.iter().map(|s| s.schema_version).collect();
    assert_eq!(versions, vec![1, 2]);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A split with 40 % deleted docs and no merge partner is rewritten alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_with_many_deletes_is_purged_alone() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = MaintenanceConfig {
        purge_min_deleted: 1,
        ..config()
    };
    commits(&f, 0, 1, 10).await;
    commit(
        &f,
        (0..4).map(|k| DocOp::Delete(PrimaryKey::U64(k))).collect(),
    )
    .await;
    let split = only_split(f.manifest().await);
    assert_eq!((split.doc_count, split.deleted_count), (10, 4));
    assert_eq!(
        plan(&f, &config).await,
        vec![MergePlan {
            inputs: vec![split.ulid],
            purge: true
        }]
    );
    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let purged = only_split(f.manifest().await);
    assert_ne!(purged.ulid, split.ulid);
    assert_eq!((purged.doc_count, purged.deleted_count), (6, 0));
    assert_eq!(purged.delete_bitmap, None);
    assert_eq!(purged.merge_ops, 1);
    assert!(plan(&f, &config).await.is_empty());
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// 6 commits of upserts, patches and deletes over 60 keys, merged until no
/// merge remains: every query matches the same keys, and every live doc has
/// the same fast-field values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_preserves_matching_docs_and_fast_fields() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = config();
    for g in 0..6u64 {
        let mut ops: Vec<DocOp> = (0..60)
            .filter(|k| (k + g) % 3 == 0 || g == 0)
            .map(|k| document(k, g))
            .collect();
        if g > 0 {
            let touched: BTreeSet<u64> = (0..60).filter(|k| (k + g) % 3 == 0).collect();
            ops.extend(
                (0..60u64)
                    .filter(|k| (k + g) % 7 == 1 && !touched.contains(k))
                    .map(|k| patch(PrimaryKey::U64(k), json!({ "n": (k * g) as i64 % 30 }))),
            );
            ops.extend(
                (0..60u64)
                    .filter(|k| (k * 5 + g) % 11 == 0 && !touched.contains(k))
                    .map(|k| DocOp::Delete(PrimaryKey::U64(k))),
            );
        }
        commit(&f, ops).await;
    }
    let before_splits = f.manifest().await.splits.len();
    let (matching, fast) = split_view(&f).await;
    assert!(matching.iter().all(|keys| !keys.is_empty()), "{matching:?}");

    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    merge_all(&f, &source, &config).await;
    let manifest = f.manifest().await;
    assert!(manifest.splits.len() < before_splits, "nothing merged");
    assert!(manifest.splits.iter().any(|s| s.merge_ops > 0));
    let (matching_after, fast_after) = split_view(&f).await;
    for (i, (a, b)) in matching.iter().zip(&matching_after).enumerate() {
        assert_eq!(a, b, "query {i}");
    }
    assert_eq!(fast, fast_after);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[test]
fn row_id_runs_are_maximal() {
    assert_eq!(row_id_runs(&[]), Vec::<std::ops::Range<u64>>::new());
    assert_eq!(row_id_runs(&[3, 4, 5, 7, 9, 10]), vec![3..6, 7..8, 9..11]);
}

/// Inputs `[0,10)`, `[10,20)`, `[20,30)` with rows 5 and 15 deleted merge
/// into `[0,5), [6,15), [16,30)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_row_id_ranges_are_maximal_runs() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = config();
    commits(&f, 0, 3, 10).await;
    let ranges: Vec<Vec<std::ops::Range<u64>>> = f
        .manifest()
        .await
        .splits
        .iter()
        .map(|s| s.row_id_ranges.clone())
        .collect();
    assert_eq!(ranges, vec![vec![0..10], vec![10..20], vec![20..30]]);
    let (five, fifteen) = (key_at(&f, 5).await, key_at(&f, 15).await);
    commit(
        &f,
        vec![
            DocOp::Delete(PrimaryKey::U64(five)),
            DocOp::Delete(PrimaryKey::U64(fifteen)),
        ],
    )
    .await;
    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let merged = only_split(f.manifest().await);
    assert_eq!(merged.row_id_ranges, vec![0..5, 6..15, 16..30]);
    assert_eq!((merged.doc_count, merged.deleted_count), (28, 0));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A `[0,10)` and C `[15,25)` merge first (B `[10,15)` has the fewest docs,
/// so the policy leaves it), then the result merges with B: one split
/// whose doc *i* is row *i*, which `RowLocator` agrees with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_generation_merge_keeps_row_id_order() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = policy(100, 2, 2);
    commits(&f, 0, 1, 10).await;
    commits(&f, 10, 1, 5).await;
    commits(&f, 15, 1, 10).await;
    let splits = f.manifest().await.splits;
    let (a, b, c) = (splits[0].ulid, splits[1].ulid, splits[2].ulid);
    assert_eq!(splits[1].row_id_ranges, vec![10..15]);
    assert_eq!(
        plan(&f, &config).await,
        vec![MergePlan {
            inputs: vec![a, c],
            purge: false
        }]
    );
    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(
        merge_once(&f, &source).await.unwrap(),
        TaskOutcome::MoreWork
    );
    let splits = f.manifest().await.splits;
    assert_eq!(splits.len(), 2);
    assert_eq!(splits[0].row_id_ranges, vec![0..10, 15..25]);
    assert_eq!(splits[1].ulid, b);
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);

    let snapshot = f.snapshot().await;
    let [merged] = snapshot.splits() else {
        panic!("one split: {:?}", snapshot.splits());
    };
    assert_eq!(merged.row_id_ranges, vec![0..25]);
    assert_eq!(merged.merge_ops, 2);
    let index = snapshot.open_split(merged).await.expect("open");
    let searcher = operon_text::warm_up_all(&index).await.expect("warm");
    let rows = searcher
        .segment_reader(0)
        .fast_fields()
        .u64(ROWID_FIELD)
        .expect("rowid");
    for doc in 0..25u32 {
        assert_eq!(rows.first(doc), Some(u64::from(doc)), "doc {doc}");
        assert_eq!(snapshot.locate_row(u64::from(doc)), Some((0, doc)));
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Two splits written before `add_fields(extra)` and one after: their merge
/// has no `extra`, so a term on `extra` still matches only the docs written
/// after the addition (A4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fields_added_after_a_split_stay_absent_after_its_merge() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = policy(10, 2, 2);
    let tagged = |k: u64| upsert(k, json!({ "tag": "t", "extra": "x" }));
    commit(&f, (0..5).map(tagged).collect()).await;
    commit(&f, (5..10).map(tagged).collect()).await;
    let mut next = merge_schema();
    next.fields.push(field("extra", FieldKind::Keyword));
    next.version = 2;
    f.meta
        .client
        .update_collection_schema(f.cid, 1, next)
        .await
        .expect("add a field");
    commit(&f, (10..13).map(tagged).collect()).await;
    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);

    let snapshot = f.snapshot().await;
    let splits = snapshot.splits();
    assert_eq!(splits.len(), 2);
    let mut matched = BTreeSet::new();
    for split in splits {
        let index = snapshot.open_split(split).await.expect("open");
        let schema = index.schema();
        match split.schema_version {
            1 => {
                assert_eq!(split.merge_ops, 1);
                assert!(schema.get_field("extra").is_err(), "the merge added extra");
            }
            _ => {
                let searcher = operon_text::warm_up_all(&index).await.expect("warm");
                let query = TermQuery::new(
                    Term::from_field_text(schema.get_field("extra").unwrap(), "x"),
                    IndexRecordOption::Basic,
                );
                for address in searcher.search(&query, &DocSetCollector).unwrap() {
                    matched.insert(pk_of(&searcher, address.doc_id));
                }
            }
        }
    }
    let want: BTreeSet<PrimaryKey> = (10..13).map(PrimaryKey::U64).collect();
    assert_eq!(matched, want);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A merge held after its split PUT while a link commit deletes one doc of
/// an input: its CAS conflicts, and the rebase carries the delete into a
/// delete bitmap of the merged split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_during_a_merge_is_carried_into_the_merged_split() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    commits(&f, 0, 3, 10).await;
    let (hook, reached, release) = hold_at(CollectionCommitStep::AfterSplitPut);
    let source = SplitMergeSource::new(f.ctx.clone(), config()).with_hook(hook);
    let meta = f.meta.client.clone();
    let run = tokio::spawn(async move { run_once(&meta, "merger", TTL, &source).await });
    wait_for("the merged split", || reached.load(Ordering::SeqCst)).await;

    let row = row_of(&f, 12).await;
    commit(&f, vec![DocOp::Delete(PrimaryKey::U64(12))]).await;
    let link_version = f.manifest().await.version;
    release.notify_one();
    let result = outcome(run.await.expect("join").expect("run"));
    assert!(result.is_ok(), "{result:?}");

    let manifest = f.manifest().await;
    assert_eq!(manifest.version, link_version + 1);
    assert_eq!(manifest.kind, CommitKind::Maintenance);
    let [merged] = manifest.splits.as_slice() else {
        panic!("one split: {:?}", manifest.splits);
    };
    assert_eq!((merged.doc_count, merged.deleted_count), (30, 1));
    assert!(merged.delete_bitmap.is_some());
    let snapshot = f.snapshot().await;
    let (_, doc) = snapshot.locate_row(row).expect("located");
    assert!(snapshot.deleted_docs(merged).await.unwrap().contains(doc));
    assert_eq!(manifest.live_doc_count, 29);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Every doc of an input is deleted while the merge is held, so the link
/// commit removes that split: the merge abandons, and the next run plans
/// again over what is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_whose_input_vanished_is_abandoned() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    commits(&f, 0, 4, 10).await;
    let (hook, reached, release) = hold_at(CollectionCommitStep::AfterSplitPut);
    let source = SplitMergeSource::new(f.ctx.clone(), config()).with_hook(hook);
    let meta = f.meta.client.clone();
    let held = source.clone();
    let run = tokio::spawn(async move { run_once(&meta, "merger", TTL, &held).await });
    wait_for("the merged split", || reached.load(Ordering::SeqCst)).await;

    commit(
        &f,
        (10..20)
            .map(|k| DocOp::Delete(PrimaryKey::U64(k)))
            .collect(),
    )
    .await;
    let after_link = f.manifest().await;
    assert_eq!(after_link.splits.len(), 3);
    release.notify_one();
    let result = outcome(run.await.expect("join").expect("run"));
    assert_eq!(result.unwrap(), TaskOutcome::MoreWork);
    assert_eq!(f.manifest().await, after_link);

    assert_eq!(plan(&f, &config()).await.len(), 1);
    assert_eq!(merge_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let merged = only_split(f.manifest().await);
    assert_eq!((merged.doc_count, merged.merge_ops), (30, 1));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The merge is dropped at `step` (a crash): the live manifest is
/// unchanged, and once its lease expires the next run merges.
async fn crash_at(step: CollectionCommitStep) {
    let f = TargetFixture::start(merge_schema(), 2).await;
    commits(&f, 0, 3, 10).await;
    let before = f.manifest().await;
    let (hook, reached, _release) = hold_at(step);
    let source = SplitMergeSource::new(f.ctx.clone(), config()).with_hook(hook);
    let meta = f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        let _ = run_once(&meta, "crashed", CRASHED_TTL, &source).await;
    });
    wait_for("the crash point", || reached.load(Ordering::SeqCst)).await;
    crashed.abort();
    let _ = crashed.await;
    assert_eq!(f.manifest().await, before);

    let source = SplitMergeSource::new(f.ctx.clone(), config());
    let deadline = Instant::now() + WAIT;
    loop {
        let results = run_once(&f.meta.client, "w2", TTL, &source)
            .await
            .expect("run");
        match results.as_slice() {
            [(_, RunResult::LeaseHeld)] => {
                assert!(Instant::now() < deadline, "the crashed lease never expired");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            _ => {
                assert_eq!(outcome(results).expect("merged"), TaskOutcome::Idle);
                break;
            }
        }
    }
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, before.version + 1);
    assert_eq!(manifest.splits.len(), 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_split_put_leaves_the_manifest_unchanged() {
    crash_at(CollectionCommitStep::AfterSplitPut).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_manifest_put_leaves_the_manifest_unchanged() {
    crash_at(CollectionCommitStep::AfterManifestPut).await;
}

/// The merge's lease is taken over after its manifest PUT: its CAS is
/// refused as `Fenced` and the manifest is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_merge_changes_nothing() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    commits(&f, 0, 3, 10).await;
    let before = f.manifest().await;
    let meta = f.meta.client.clone();
    let taken = Arc::new(AtomicBool::new(false));
    let flag = taken.clone();
    let hook: CollectionCommitHook = Arc::new(move |at, fence| {
        let meta = meta.clone();
        let flag = flag.clone();
        async move {
            if at == CollectionCommitStep::AfterManifestPut && !flag.swap(true, Ordering::SeqCst) {
                meta.release_lease(&fence.lease, "merger", fence.epoch)
                    .await
                    .expect("release");
                meta.acquire_lease(&fence.lease, "thief", TTL)
                    .await
                    .expect("take over");
            }
        }
        .boxed()
    });
    let source = SplitMergeSource::new(f.ctx.clone(), config()).with_hook(hook);
    let result = merge_once(&f, &source).await;
    assert!(matches!(result, Err(TaskError::Fenced)), "{result:?}");
    assert!(taken.load(Ordering::SeqCst));
    assert_eq!(f.manifest().await, before);
    f.shutdown().await;
}

/// With `merge` off, nothing is proposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merges_off_proposes_nothing() {
    let f = TargetFixture::start(merge_schema(), 2).await;
    commits(&f, 0, 3, 10).await;
    let config = MaintenanceConfig {
        merge: false,
        ..config()
    };
    let source = SplitMergeSource::new(f.ctx.clone(), config);
    let results = run_once(&f.meta.client, "merger", TTL, &source)
        .await
        .expect("run");
    assert!(results.is_empty());
    f.shutdown().await;
}

/// A split older than the maturation period (on the metastore clock) is
/// mature and never merges.
#[test]
fn old_splits_are_mature_on_the_metastore_clock() {
    let split = |i: u64, created_at_ms: u64| SplitRef {
        ulid: ulid::Ulid::from_parts(created_at_ms, u128::from(i)),
        doc_count: 20,
        deleted_count: 0,
        size_bytes: 1_000,
        footer_range: 900..1_000,
        row_id_ranges: vec![std::ops::Range {
            start: i * 20,
            end: i * 20 + 20,
        }],
        delete_bitmap: None,
        schema_version: 1,
        created_at_ms,
        merge_ops: 0,
    };
    let mut manifest = CollectionManifest::empty(operon_common::CollectionId(1));
    manifest.splits = (0..3).map(|i| split(i, 1_000)).collect();
    let config = config();
    let hour = 3_600_000;
    assert_eq!(plan_merges(&manifest, &config, 0).len(), 1);
    assert_eq!(plan_merges(&manifest, &config, 1_000 + 47 * hour).len(), 1);
    assert!(plan_merges(&manifest, &config, 1_000 + 48 * hour).is_empty());
}

/// A split of `docs` live docs and `size_bytes` bytes, created at 1 000 ms.
fn sized_split(i: u64, docs: u64, size_bytes: u64) -> SplitRef {
    SplitRef {
        ulid: ulid::Ulid::from_parts(1_000, u128::from(i)),
        doc_count: docs,
        deleted_count: 0,
        size_bytes,
        footer_range: 0..size_bytes.min(100),
        row_id_ranges: vec![std::ops::Range {
            start: i * 1_000,
            end: i * 1_000 + docs,
        }],
        delete_bitmap: None,
        schema_version: 1,
        created_at_ms: 1_000,
        merge_ops: 0,
    }
}

/// A policy operation is cut to its smallest inputs within
/// `max_merge_docs` and `max_merge_bytes`, and dropped when fewer than two
/// fit (PR #32 review; plan row R32.1).
#[test]
fn a_merge_plan_is_bounded() {
    let mut manifest = CollectionManifest::empty(operon_common::CollectionId(1));
    manifest.splits = vec![
        sized_split(0, 40, 4_000),
        sized_split(1, 20, 2_000),
        sized_split(2, 35, 9_000),
        sized_split(3, 25, 2_500),
    ];
    let ulid = |i: usize| manifest.splits[i].ulid;
    let unbounded = plan_merges(&manifest, &config(), 0);
    assert_eq!(
        unbounded,
        vec![MergePlan {
            inputs: (0..4).map(ulid).collect(),
            purge: false
        }]
    );
    let by_docs = MaintenanceConfig {
        max_merge_docs: 80,
        ..config()
    };
    // 20 + 25 + 35 = 80 fit; the 40-doc split waits.
    let want = |mut inputs: Vec<ulid::Ulid>| {
        inputs.sort_unstable();
        vec![MergePlan {
            inputs,
            purge: false,
        }]
    };
    assert_eq!(
        plan_merges(&manifest, &by_docs, 0),
        want(vec![ulid(1), ulid(3), ulid(2)])
    );
    let by_bytes = MaintenanceConfig {
        max_merge_bytes: 5_000,
        ..config()
    };
    // 2 000 + 2 500 fit; the 35-doc split's 9 000 bytes do not.
    assert_eq!(
        plan_merges(&manifest, &by_bytes, 0),
        want(vec![ulid(1), ulid(3)])
    );
    let one_fits = MaintenanceConfig {
        max_merge_docs: 44,
        ..config()
    };
    assert!(plan_merges(&manifest, &one_fits, 0).is_empty());
    for plan in plan_merges(&manifest, &by_docs, 0) {
        let docs: u64 = plan
            .inputs
            .iter()
            .map(|u| {
                manifest
                    .splits
                    .iter()
                    .find(|s| s.ulid == *u)
                    .unwrap()
                    .doc_count
            })
            .sum();
        assert!(docs <= 80);
    }
}

/// A split too big in bytes is skipped, not the end of the scan: the inputs
/// are ordered by live docs, so a later split may still fit (PR #34 review).
#[test]
fn a_split_over_the_byte_bound_does_not_block_smaller_ones() {
    let mut manifest = CollectionManifest::empty(operon_common::CollectionId(1));
    manifest.splits = vec![
        sized_split(0, 1, 900),
        sized_split(1, 2, 100),
        sized_split(2, 3, 100),
    ];
    let bytes = MaintenanceConfig {
        max_merge_bytes: 250,
        ..config()
    };
    let mut inputs = vec![manifest.splits[1].ulid, manifest.splits[2].ulid];
    inputs.sort_unstable();
    assert_eq!(
        plan_merges(&manifest, &bytes, 0),
        vec![MergePlan {
            inputs,
            purge: false
        }]
    );
}

/// The metastore clock passes `commit_delay` while the merged split is being
/// built: the commit's clock starts after the build, so the merge still
/// commits, and the split and manifest carry the post-build time (PR #32
/// review; plan row R32.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_build_still_commits() {
    let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
    let f = TargetFixture::start_with_clock(
        merge_schema(),
        2,
        operon_collection::CollectionConfig::default(),
        clock.clone(),
    )
    .await;
    commits(&f, 0, 3, 10).await;
    let before = f.manifest().await;
    let config = MaintenanceConfig {
        commit_delay: Duration::from_secs(1),
        ..config()
    };
    let built_at = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (slow, at) = (clock.clone(), built_at.clone());
    let hook: CollectionCommitHook = Arc::new(move |step, _fence| {
        if step == CollectionCommitStep::AfterSplitBuild {
            // Ten commit delays of building, well within the lease's TTL.
            slow.advance(Duration::from_secs(10));
            at.store(slow.now_ms(), Ordering::SeqCst);
        }
        futures::future::ready(()).boxed()
    });
    let source = SplitMergeSource::new(f.ctx.clone(), config).with_hook(hook);
    let result = merge_once(&f, &source).await;
    assert_eq!(result.expect("merged"), TaskOutcome::Idle);
    let built_at = built_at.load(Ordering::SeqCst);
    assert!(built_at > 0, "the hook never ran");
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, before.version + 1);
    assert!(manifest.created_at_ms >= built_at);
    let merged = only_split(manifest);
    assert!(merged.created_at_ms >= built_at);
    assert_eq!(merged.ulid.timestamp_ms(), merged.created_at_ms);
    assert_eq!(merged.doc_count, 30);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// One random op over 30 keys.
#[derive(Clone, Debug)]
enum RandomOp {
    Upsert(u64, u64),
    Patch(u64, i64),
    Delete(u64),
}

impl RandomOp {
    fn op(&self) -> DocOp {
        match *self {
            RandomOp::Upsert(k, g) => document(k, g),
            RandomOp::Patch(k, n) => patch(PrimaryKey::U64(k), json!({ "n": n })),
            RandomOp::Delete(k) => DocOp::Delete(PrimaryKey::U64(k)),
        }
    }
}

fn random_op() -> impl Strategy<Value = RandomOp> {
    prop_oneof![
        4 => (0u64..30, 0u64..5).prop_map(|(k, g)| RandomOp::Upsert(k, g)),
        1 => (0u64..30, -20i64..20).prop_map(|(k, n)| RandomOp::Patch(k, n)),
        2 => (0u64..30).prop_map(RandomOp::Delete),
    ]
}

/// After each batch: nothing, a merge run, or a merge held at a step while
/// the next batch commits.
#[derive(Clone, Copy, Debug)]
enum Then {
    Nothing,
    Merge,
    Hold(usize),
}

const HOLD_STEPS: [CollectionCommitStep; 2] = [
    CollectionCommitStep::AfterSplitPut,
    CollectionCommitStep::AfterManifestPut,
];

fn then() -> impl Strategy<Value = Then> {
    prop_oneof![
        1 => Just(Then::Nothing),
        2 => Just(Then::Merge),
        2 => (0..HOLD_STEPS.len()).prop_map(Then::Hold),
    ]
}

fn workload() -> impl Strategy<Value = Vec<(Vec<RandomOp>, Then)>> {
    prop::collection::vec((prop::collection::vec(random_op(), 1..12), then()), 4..=8)
}

/// A merge run in the background.
type Run = tokio::task::JoinHandle<Result<Vec<(TaskKey, RunResult)>, TaskError>>;

async fn random_batches_and_merges(steps: Vec<(Vec<RandomOp>, Then)>) {
    let f = TargetFixture::start(merge_schema(), 2).await;
    let config = MaintenanceConfig {
        purge_min_deleted: 2,
        ..policy(10, 2, 3)
    };
    let mut held: Option<(Run, Arc<tokio::sync::Notify>)> = None;
    for (batch, then) in steps {
        commit(&f, batch.iter().map(RandomOp::op).collect()).await;
        if let Some((run, release)) = held.take() {
            release.notify_one();
            let result = outcome(run.await.expect("join").expect("run"));
            assert!(result.is_ok(), "{result:?}");
            assert_verified(f.verify().await);
        }
        match then {
            Then::Nothing => {}
            Then::Merge => {
                let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
                let result = merge_once(&f, &source).await;
                assert!(result.is_ok(), "{result:?}");
            }
            Then::Hold(step) if !plan(&f, &config).await.is_empty() => {
                let (hook, reached, release) = hold_at(HOLD_STEPS[step]);
                let source = SplitMergeSource::new(f.ctx.clone(), config.clone()).with_hook(hook);
                let meta = f.meta.client.clone();
                let run =
                    tokio::spawn(async move { run_once(&meta, "merger", TTL, &source).await });
                wait_for("the hold", || reached.load(Ordering::SeqCst)).await;
                held = Some((run, release));
            }
            Then::Hold(_) => {}
        }
        assert_verified(f.verify().await);
    }
    if let Some((run, release)) = held.take() {
        release.notify_one();
        let result = outcome(run.await.expect("join").expect("run"));
        assert!(result.is_ok(), "{result:?}");
    }
    let source = SplitMergeSource::new(f.ctx.clone(), config.clone());
    merge_all(&f, &source, &config).await;
    assert_verified(f.verify().await);
    f.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Random link batches interleaved with merges and held merges always
    /// verify.
    #[test]
    fn random_batches_and_merges_verify(steps in workload()) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(random_batches_and_merges(steps));
    }
}
