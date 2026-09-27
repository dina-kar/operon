//! R1 plan Task 11: the read-set index, journal invalidation, query reruns,
//! shared subscriptions and the safety rerun (design §20 §8.2–§8.3). The
//! interval-index tests run without a cluster; the rest need TiKV and skip
//! without `OPERON_TEST_PD`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use operon_live::catalog::{self, IndexSpec};
use operon_live::system::{self, GET, INSERT, PATCH, QUERY};
use operon_live::{
    AppKeys, DocId, FnKind, Function, Janitor, KeyRange, Limits, LiveConfig, LiveError, LiveTxn,
    LiveValue, ReadSet, ReadSetIndex, Runner, SubId, SubKey, SubResult, SubsConfig, Subscriptions,
    TableId, Tick, pb,
};
use operon_tikv::testing::{self, TEST_LIVE};
use operon_tikv::{CommitMode, Timestamp, TimestampExt, TxnOptions};
use proptest::prelude::*;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

// ---- the read-set index, against a brute-force model ----

#[derive(Debug, Clone)]
enum Op {
    Insert(u64, ReadSet),
    Remove(u64),
    Stab(Vec<u8>),
}

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..4, 0..4)
}

fn read_set() -> impl Strategy<Value = ReadSet> {
    (
        prop::collection::btree_set(key(), 0..3),
        prop::collection::vec((key(), key()), 0..4),
    )
        .prop_map(|(points, ranges)| ReadSet {
            points,
            ranges: ranges
                .into_iter()
                .map(|(lo, hi)| KeyRange { lo, hi })
                .collect(),
        })
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u64..16, read_set()).prop_map(|(id, rs)| Op::Insert(id, rs)),
        1 => (0u64..16).prop_map(Op::Remove),
        4 => key().prop_map(Op::Stab),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every stab returns exactly the subscriptions whose read set covers
    /// the key (`ReadSet::covers`), under any sequence of inserts (which
    /// replace), removes and stabs; empty `hi` is unbounded and empty
    /// ranges hold nothing.
    #[test]
    fn interval_index_matches_brute_force(ops in prop::collection::vec(op(), 0..160)) {
        let mut index = ReadSetIndex::new(AppKeys::dedicated());
        let mut model: HashMap<u64, ReadSet> = HashMap::new();
        for op in ops {
            match op {
                Op::Insert(id, rs) => {
                    index.insert(SubId(id), &rs);
                    model.insert(id, rs);
                }
                Op::Remove(id) => {
                    index.remove(SubId(id));
                    model.remove(&id);
                }
                Op::Stab(key) => {
                    let mut got = HashSet::new();
                    index.stab_key(&key, &mut got);
                    let want: HashSet<SubId> = model
                        .iter()
                        .filter(|(_, rs)| rs.covers(&key))
                        .map(|(id, _)| SubId(*id))
                        .collect();
                    prop_assert_eq!(got, want, "key {:?}", key);
                }
            }
            prop_assert_eq!(index.len(), model.len());
            prop_assert_eq!(index.check_invariants(), Ok(()));
        }
    }
}

/// A write is matched by its document key and by each removed and added
/// index key.
#[test]
fn stab_matches_the_document_and_index_keys_of_a_write() {
    let app = AppKeys::dedicated();
    let id = DocId {
        table: TableId(7),
        bytes: [9; 16],
    };
    let mut index = ReadSetIndex::new(app.clone());
    let mut by_doc = ReadSet::default();
    by_doc.points.insert(app.document(&id));
    let old_key = vec![0x03, 0, 0, 0, 7, 0, 0, 0, 2, 0x10];
    let new_key = vec![0x03, 0, 0, 0, 7, 0, 0, 0, 2, 0x50];
    let around = |k: &[u8]| {
        let mut hi = k.to_vec();
        hi.push(0);
        ReadSet {
            points: BTreeSet::new(),
            ranges: vec![KeyRange { lo: k.to_vec(), hi }],
        }
    };
    index.insert(SubId(1), &by_doc);
    index.insert(SubId(2), &around(&old_key));
    index.insert(SubId(3), &around(&new_key));
    index.insert(SubId(4), &around(&[0x03, 0, 0, 0, 8]));
    let w = pb::WriteRecord {
        table_id: 7,
        doc_id: id.bytes.to_vec(),
        kind: pb::WriteKind::WRITE_KIND_REPLACE.into(),
        index_keys_removed: vec![old_key],
        index_keys_added: vec![new_key],
        ..Default::default()
    };
    let mut out = HashSet::new();
    index.stab(&w, &mut out);
    assert_eq!(out, HashSet::from([SubId(1), SubId(2), SubId(3)]));
}

// ---- helpers ----

fn s(v: &str) -> LiveValue {
    LiveValue::Str(v.to_string())
}

fn int(v: i64) -> LiveValue {
    LiveValue::I64(v)
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn sys(name: &str) -> Arc<dyn Function> {
    system::lookup(name).expect("a system function")
}

fn config(cluster: &testing::TestCluster) -> LiveConfig {
    LiveConfig {
        app: "t11".to_string(),
        tikv: cluster.config(TEST_LIVE),
        limits: Limits::default(),
        journal_shards: operon_live::DEFAULT_JOURNAL_SHARDS,
    }
}

async fn open() -> Option<Runner> {
    let cluster = testing::cluster().await?;
    let tikv = cluster.connect(TEST_LIVE).await;
    Some(
        Runner::open(tikv, &config(&cluster))
            .await
            .expect("the runner opens"),
    )
}

/// The test settings: the plan's, with no safety rerun unless asked.
fn subs_config() -> SubsConfig {
    SubsConfig {
        safety_rerun: Duration::from_secs(3600),
        ..SubsConfig::default()
    }
}

fn spawn(r: &Runner, config: SubsConfig) -> Subscriptions {
    Subscriptions::spawn(r.clone(), config, CancellationToken::new())
}

async fn mutate(r: &Runner, f: Arc<dyn Function>, args: LiveValue) -> operon_live::Mutated {
    r.mutate(f, args, None).await.expect("the mutation commits")
}

async fn insert(r: &Runner, table: &str, doc: &[(&str, LiveValue)]) -> (DocId, Timestamp) {
    let m = mutate(
        r,
        sys(INSERT),
        obj(&[("table", s(table)), ("fields", obj(doc))]),
    )
    .await;
    let LiveValue::Str(id) = m.result else {
        panic!("insert returns an id, not {:?}", m.result)
    };
    (id.parse().expect("a document id"), m.commit_ts)
}

async fn patch(r: &Runner, id: DocId, doc: &[(&str, LiveValue)]) -> Timestamp {
    mutate(
        r,
        sys(PATCH),
        obj(&[("id", s(&id.to_string())), ("fields", obj(doc))]),
    )
    .await
    .commit_ts
}

async fn define(r: &Runner, table: &str, indexes: &[(&str, &[&str])]) {
    let table = table.to_string();
    let specs: Vec<IndexSpec> = indexes
        .iter()
        .map(|(n, f)| IndexSpec {
            name: (*n).to_string(),
            fields: f.iter().map(|s| (*s).to_string()).collect(),
        })
        .collect();
    let mut opts = TxnOptions::new("test.define");
    opts.commit_mode = Some(CommitMode::TwoPc);
    r.tikv()
        .run(opts, move |txn| {
            let (table, specs) = (table.clone(), specs.clone());
            Box::pin(async move {
                catalog::define_table(
                    txn,
                    &AppKeys::dedicated(),
                    &table,
                    &specs,
                    &Limits::default(),
                )
                .await
                .map_err(|e| operon_tikv::TxnError::Fatal(e.to_string()))
            })
        })
        .await
        .expect("the table is defined");
}

fn docs(result: &SubResult) -> Vec<LiveValue> {
    match &result.result {
        Ok(LiveValue::Array(docs)) => docs.clone(),
        other => panic!("a query result is an array, not {other:?}"),
    }
}

fn field(doc: &LiveValue, name: &str) -> LiveValue {
    match doc {
        LiveValue::Object(f) => f.get(name).cloned().unwrap_or(LiveValue::Null),
        other => panic!("a document is an object, not {other:?}"),
    }
}

/// Receives ticks until one at or after `ts`; returns them all.
async fn until(rx: &mut broadcast::Receiver<Tick>, ts: &Timestamp) -> Vec<Tick> {
    let mut ticks = Vec::new();
    loop {
        let tick = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("a tick within 20 s")
            .expect("the tick channel");
        let done = tick.at.version() >= ts.version();
        ticks.push(tick);
        if done {
            return ticks;
        }
    }
}

/// The newest result of `id` in `ticks`, if any tick changed it.
fn latest(ticks: &[Tick], id: SubId) -> Option<Arc<SubResult>> {
    ticks.iter().rev().find_map(|t| {
        t.changed
            .iter()
            .find(|(c, _)| *c == id)
            .map(|(_, r)| r.clone())
    })
}

/// A subscription as the checker holds it: its function, its arguments and
/// its latest result.
type Held = (Arc<dyn Function>, LiveValue, Arc<SubResult>);

/// A query that counts its calls.
struct Counting {
    inner: Arc<dyn Function>,
    calls: Arc<AtomicUsize>,
}

impl Counting {
    fn wrap(inner: Arc<dyn Function>) -> (Arc<dyn Function>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Counting {
                inner,
                calls: calls.clone(),
            }),
            calls,
        )
    }
}

impl Function for Counting {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn kind(&self) -> FnKind {
        FnKind::Query
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.call(txn, args)
    }
}

async fn subscribe(
    subs: &Subscriptions,
    f: Arc<dyn Function>,
    args: LiveValue,
) -> (SubId, Arc<SubResult>) {
    subs.subscribe(SubKey::new(f.name(), &args), f, args)
        .await
        .expect("subscribed")
}

fn table_query(table: &str) -> LiveValue {
    obj(&[("table", s(table))])
}

fn n_range(table: &str, lo: i64, hi: i64) -> LiveValue {
    obj(&[
        ("table", s(table)),
        ("index", s("by_n")),
        ("lower", obj(&[("value", int(lo))])),
        (
            "upper",
            obj(&[("value", int(hi)), ("inclusive", LiveValue::Bool(false))]),
        ),
    ])
}

// ---- the plan's tests ----

/// An insert whose index key falls in a subscribed range invalidates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_into_range_invalidates() {
    let Some(r) = open().await else { return };
    define(&r, "items", &[("by_n", &["n"])]).await;
    let subs = spawn(&r, subs_config());
    let (id, first) = subscribe(&subs, sys(QUERY), n_range("items", 0, 10)).await;
    assert!(docs(&first).is_empty());
    let mut rx = subs.updates();
    let (_, ts) = insert(&r, "items", &[("n", int(5))]).await;
    let ticks = until(&mut rx, &ts).await;
    let now = latest(&ticks, id).expect("the subscription changed");
    assert_eq!(docs(&now).len(), 1);
    assert_eq!(field(&docs(&now)[0], "n"), int(5));
}

/// A patch that moves a document's index key out of a subscribed range
/// invalidates it through the removed key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_moving_out_of_range_invalidates() {
    let Some(r) = open().await else { return };
    define(&r, "items", &[("by_n", &["n"])]).await;
    let (doc, _) = insert(&r, "items", &[("n", int(5))]).await;
    let subs = spawn(&r, subs_config());
    let (id, first) = subscribe(&subs, sys(QUERY), n_range("items", 0, 10)).await;
    assert_eq!(docs(&first).len(), 1);
    let mut rx = subs.updates();
    let ts = patch(&r, doc, &[("n", int(50))]).await;
    let ticks = until(&mut rx, &ts).await;
    let now = latest(&ticks, id).expect("the subscription changed");
    assert!(docs(&now).is_empty(), "{now:?}");
}

/// A write outside every read set reruns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrelated_write_does_not_rerun() {
    let Some(r) = open().await else { return };
    insert(&r, "a", &[("v", int(1))]).await;
    insert(&r, "b", &[("v", int(1))]).await;
    let subs = spawn(&r, subs_config());
    let (f, calls) = Counting::wrap(sys(QUERY));
    let (id, _) = subscribe(&subs, f, table_query("a")).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut rx = subs.updates();
    let (_, ts) = insert(&r, "b", &[("v", int(2))]).await;
    let ticks = until(&mut rx, &ts).await;
    assert!(latest(&ticks, id).is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no rerun");
    assert_eq!(subs.stats().reruns, 0);
}

/// A range that stopped at its limit depends only on the keys up to the
/// last one returned: an insert past it reruns nothing, a write inside the
/// page does. PD's TSO advances its physical part every 50 ms, so documents
/// created within one window share `_creationTime` and sort by id; the
/// test patches the documents the page actually holds and waits out the
/// window before the insert that must sort past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_bounded_range_ignores_inserts_past_last_key() {
    let Some(r) = open().await else { return };
    for i in 0..3 {
        insert(&r, "page", &[("i", int(i))]).await;
    }
    let subs = spawn(&r, subs_config());
    let (f, calls) = Counting::wrap(sys(QUERY));
    let args = obj(&[("table", s("page")), ("limit", int(2))]);
    let (id, first) = subscribe(&subs, f, args).await;
    let page = docs(&first);
    assert_eq!(page.len(), 2);
    let mut rx = subs.updates();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let (_, ts) = insert(&r, "page", &[("i", int(3))]).await;
    let ticks = until(&mut rx, &ts).await;
    assert!(latest(&ticks, id).is_none());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an insert past the page reruns nothing"
    );
    let LiveValue::Str(last) = field(&page[1], "_id") else {
        panic!("a document has an id")
    };
    let ts = patch(&r, last.parse().expect("an id"), &[("i", int(10))]).await;
    let ticks = until(&mut rx, &ts).await;
    let now = latest(&ticks, id).expect("a write inside the page reruns");
    assert_eq!(field(&docs(&now)[1], "i"), int(10));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

/// Subscriptions with one key share one entry and one rerun; the last
/// unsubscribe removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_subscriptions_share_one_rerun() {
    let Some(r) = open().await else { return };
    insert(&r, "room", &[("m", s("hi"))]).await;
    let subs = spawn(&r, subs_config());
    let (f, calls) = Counting::wrap(sys(QUERY));
    let args = table_query("room");
    let (a, ra) = subscribe(&subs, f.clone(), args.clone()).await;
    let (b, rb) = subscribe(&subs, f.clone(), args.clone()).await;
    let (c, _) = subscribe(&subs, f.clone(), args.clone()).await;
    assert_eq!((a, a), (b, c));
    assert!(Arc::ptr_eq(&ra, &rb), "one shared result");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(subs.stats().subscriptions, 1);
    let mut rx = subs.updates();
    let (_, ts) = insert(&r, "room", &[("m", s("again"))]).await;
    let ticks = until(&mut rx, &ts).await;
    assert_eq!(docs(&latest(&ticks, a).expect("changed")).len(), 2);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one rerun for three subscribers"
    );
    subs.unsubscribe(a);
    subs.unsubscribe(a);
    let (d, _) = subscribe(&subs, f.clone(), args.clone()).await;
    assert_eq!(d, a, "still held by one reference");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    subs.unsubscribe(a);
    subs.unsubscribe(a);
    let (e, _) = subscribe(&subs, f, args).await;
    assert_ne!(
        e, a,
        "the last unsubscribe removed it; a new one evaluates again"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(subs.stats().subscriptions, 1);
}

/// A burst of writes reruns a subscription far fewer times than it has
/// writes, and the last tick holds every write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_writes_coalesces_to_newest_tick() {
    let Some(r) = open().await else { return };
    insert(&r, "burst", &[("i", int(-1))]).await;
    let subs = spawn(&r, subs_config());
    let (f, calls) = Counting::wrap(sys(QUERY));
    let (id, _) = subscribe(&subs, f, table_query("burst")).await;
    let mut rx = subs.updates();
    const WRITES: usize = 64;
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let (r, next) = (r.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            let mut last: Option<Timestamp> = None;
            loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= WRITES {
                    return last;
                }
                let (_, ts) = insert(&r, "burst", &[("i", int(i as i64))]).await;
                if last.as_ref().is_none_or(|l| ts.version() > l.version()) {
                    last = Some(ts);
                }
            }
        }));
    }
    let mut newest: Option<Timestamp> = None;
    for t in tasks {
        if let Some(ts) = t.await.expect("a writer")
            && newest.as_ref().is_none_or(|n| ts.version() > n.version())
        {
            newest = Some(ts);
        }
    }
    let ticks = until(&mut rx, &newest.expect("a commit")).await;
    assert_eq!(
        docs(&latest(&ticks, id).expect("changed")).len(),
        WRITES + 1
    );
    let reruns = calls.load(Ordering::SeqCst) - 1;
    eprintln!("{WRITES} writes by 16 writers: {reruns} reruns");
    assert!((1..WRITES / 2).contains(&reruns), "{reruns} reruns");
}

/// A dropped journal batch (the test hook) leaves a stale result; the
/// safety rerun finds it, counts it and publishes the repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_rerun_detects_injected_miss() {
    let Some(r) = open().await else { return };
    insert(&r, "safe", &[("i", int(0))]).await;
    let subs = spawn(
        &r,
        SubsConfig {
            safety_rerun: Duration::from_millis(400),
            ..SubsConfig::default()
        },
    );
    let (id, first) = subscribe(&subs, sys(QUERY), table_query("safe")).await;
    assert_eq!(docs(&first).len(), 1);
    let mut rx = subs.updates();
    subs.drop_next_batch();
    let (_, ts) = insert(&r, "safe", &[("i", int(1))]).await;
    let mut ticks = until(&mut rx, &ts).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while subs.stats().missed_invalidations == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no safety pass found the miss"
        );
        let at = r.tikv().now().await.expect("now");
        ticks.extend(until(&mut rx, &at).await);
    }
    assert_eq!(subs.stats().missed_invalidations, 1);
    let now = latest(&ticks, id).expect("the repair is published");
    assert_eq!(docs(&now).len(), 2);
}

// ---- beyond the plan's list ----

/// Carry T9 (row T9-6): when the janitor trimmed past the tailer, the
/// manager restarts at a fresh timestamp and reruns every subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_journal_resyncs_every_subscription() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_LIVE).await;
    let cfg = config(&cluster);
    let r = Runner::open(tikv.clone(), &cfg).await.expect("the runner");
    // A second runner on the same app: its commits do not wake the manager.
    let other = Runner::open(tikv.clone(), &cfg).await.expect("a runner");
    insert(&r, "trim", &[("i", int(0))]).await;
    let subs = spawn(
        &r,
        SubsConfig {
            poll_min: Duration::from_secs(3600),
            poll_max: Duration::from_secs(3600),
            ..subs_config()
        },
    );
    let (id, first) = subscribe(&subs, sys(QUERY), table_query("trim")).await;
    assert_eq!(docs(&first).len(), 1);
    let mut rx = subs.updates();
    insert(&other, "trim", &[("i", int(1))]).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let journal = r.journal().await.expect("the journal");
    let report = Janitor::new(tikv.clone(), journal)
        .with_retention(Duration::ZERO)
        .run_once()
        .await
        .expect("the janitor runs");
    assert!(report.deleted >= 2, "{report:?}");
    subs.wake();
    let at = r.tikv().now().await.expect("now");
    let ticks = until(&mut rx, &at).await;
    assert!(ticks.iter().any(|t| t.resynced), "a resync tick");
    assert_eq!(subs.stats().resyncs, 1);
    let now = latest(&ticks, id).expect("the rerun result is published");
    assert_eq!(docs(&now).len(), 2);
    // Ticking goes on from the new position.
    let (_, ts) = insert(&r, "trim", &[("i", int(2))]).await;
    let ticks = until(&mut rx, &ts).await;
    assert_eq!(docs(&latest(&ticks, id).expect("changed")).len(), 3);
}

/// A query of a table that does not exist yet depends on the tables not
/// created yet (row T10-8), so the insert that creates it invalidates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_to_a_missing_table_sees_its_first_insert() {
    let Some(r) = open().await else { return };
    insert(&r, "other", &[]).await;
    let subs = spawn(&r, subs_config());
    let (id, first) = subscribe(&subs, sys(QUERY), table_query("later")).await;
    assert!(docs(&first).is_empty());
    let mut rx = subs.updates();
    let (_, ts) = insert(&r, "later", &[("v", int(1))]).await;
    let ticks = until(&mut rx, &ts).await;
    assert_eq!(docs(&latest(&ticks, id).expect("changed")).len(), 1);
}

/// Review Focus 1, no missed update: under concurrent inserts, patches and
/// point reads by several writers, after every tick every subscription's
/// held result equals a fresh evaluation at the tick's timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_update_is_missed_under_concurrent_writes() {
    let Some(r) = open().await else { return };
    define(&r, "items", &[("by_n", &["n"])]).await;
    let mut seeded = Vec::new();
    for i in 0..12 {
        seeded.push(insert(&r, "items", &[("n", int(i))]).await.0);
    }
    let subs = spawn(&r, subs_config());
    let mut queries: Vec<LiveValue> = vec![
        table_query("items"),
        n_range("items", 3, 9),
        n_range("items", 10, 30),
        obj(&[
            ("table", s("items")),
            ("index", s("by_n")),
            ("lower", obj(&[("value", int(5))])),
            ("limit", int(3)),
        ]),
        obj(&[
            ("table", s("items")),
            ("index", s("by_n")),
            ("eq", LiveValue::Array(vec![int(7)])),
        ]),
        table_query("ghost"),
    ];
    let mut gets = Vec::new();
    for id in seeded.iter().take(3) {
        gets.push(obj(&[("id", s(&id.to_string()))]));
    }
    let mut held: BTreeMap<SubId, Held> = BTreeMap::new();
    for args in queries.drain(..) {
        let (id, result) = subscribe(&subs, sys(QUERY), args.clone()).await;
        held.insert(id, (sys(QUERY), args, result));
    }
    for args in gets {
        let (id, result) = subscribe(&subs, sys(GET), args.clone()).await;
        held.insert(id, (sys(GET), args, result));
    }
    let mut rx = subs.updates();
    let newest = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::new();
    for w in 0..4u64 {
        let (r, seeded, newest) = (r.clone(), seeded.clone(), newest.clone());
        writers.push(tokio::spawn(async move {
            for i in 0..30u64 {
                let x = (w * 131 + i * 17) % 29;
                let ts = match i % 5 {
                    0 | 1 => insert(&r, "items", &[("n", int(x as i64))]).await.1,
                    2 | 3 => {
                        let doc = seeded[usize::try_from((w + i) % 12).expect("small")];
                        patch(&r, doc, &[("n", int(x as i64))]).await
                    }
                    _ if w == 0 && i == 14 => insert(&r, "ghost", &[("n", int(1))]).await.1,
                    _ => insert(&r, "noise", &[("n", int(x as i64))]).await.1,
                };
                newest.fetch_max(ts.version(), Ordering::SeqCst);
            }
        }));
    }
    // The checker stops at the first tick at or after `target`, the newest
    // commit, which is set once the writers are done (0 until then).
    let target = Arc::new(AtomicU64::new(0));
    let checker = {
        let (r, target) = (r.clone(), target.clone());
        tokio::spawn(async move {
            let mut checked = 0usize;
            loop {
                let tick = tokio::time::timeout(Duration::from_secs(30), rx.recv())
                    .await
                    .expect("a tick")
                    .expect("the tick channel");
                for (id, result) in &tick.changed {
                    if let Some(h) = held.get_mut(id) {
                        h.2 = result.clone();
                    }
                }
                for (id, (f, args, result)) in &held {
                    let fresh = r
                        .query(&**f, args.clone(), tick.at.clone())
                        .await
                        .expect("a fresh evaluation");
                    assert_eq!(
                        result.result.as_ref(),
                        Ok(&fresh.result),
                        "{id} at tick {} (held since {})",
                        tick.at.version(),
                        result.ts.version()
                    );
                }
                checked += 1;
                let t = target.load(Ordering::SeqCst);
                if t != 0 && tick.at.version() >= t {
                    return checked;
                }
            }
        })
    };
    for w in writers {
        w.await.expect("a writer");
    }
    target.store(newest.load(Ordering::SeqCst), Ordering::SeqCst);
    let checked = checker.await.expect("the checker");
    eprintln!("checked every subscription at {checked} ticks");
    assert!(checked >= 2);
    assert_eq!(subs.stats().missed_invalidations, 0);
}

/// With a consumer id, the tailer checkpoints with a time to live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tailer_checkpoints_under_its_consumer() {
    let Some(r) = open().await else { return };
    insert(&r, "cp", &[]).await;
    let subs = spawn(
        &r,
        SubsConfig {
            consumer: Some("node-a".into()),
            ..subs_config()
        },
    );
    let mut rx = subs.updates();
    let at = r.tikv().now().await.expect("now");
    until(&mut rx, &at).await;
    // The checkpoint is written right after a tick is published.
    let journal = r.journal().await.expect("the journal");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let checkpoint = loop {
        let at = r.tikv().now().await.expect("now");
        let mut snap = r.tikv().snapshot(at).await.expect("a snapshot");
        if let Some(c) = journal
            .load_checkpoint(&mut snap, "node-a")
            .await
            .expect("read")
        {
            break c;
        }
        assert!(tokio::time::Instant::now() < deadline, "no checkpoint");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(checkpoint.expires_ms.is_some());
    assert_eq!(checkpoint.positions.iter().sum::<u64>(), 1);
}

/// Only queries can be subscribed to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mutation_cannot_be_subscribed() {
    let Some(r) = open().await else { return };
    let subs = spawn(&r, subs_config());
    let args = obj(&[("table", s("x")), ("fields", obj(&[]))]);
    let e = subs
        .subscribe(SubKey::new(INSERT, &args), sys(INSERT), args)
        .await;
    assert!(matches!(e, Err(LiveError::InvalidArgument(_))), "{e:?}");
}
