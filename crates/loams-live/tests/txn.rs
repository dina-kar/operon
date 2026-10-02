//! R1 plan Task 10: `LiveTxn` with read sets and point-read promotion, the
//! mutation runner with retries and idempotency keys, and the built-in
//! `_system:*` functions (design §20 §5.1–§5.2, §8.1). The key-layout and
//! shard-draw tests run without a cluster; the rest need TiKV and skip
//! without `LOAMS_TEST_PD`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use buffa::Message;
use futures::future::BoxFuture;
use loams_live::keys::{IDEMPOTENCY, KIND_APP};
use loams_live::system::{self, DELETE, GET, INSERT, PATCH, QUERY, REPLACE};
use loams_live::txn::{MUTATION_OP, idempotency_hash};
use loams_live::{
    AppKeys, DEFAULT_JOURNAL_SHARDS, DocId, FnKind, Function, IndexId, IndexRange, Janitor,
    Journal, Limits, LiveConfig, LiveError, LiveTxn, LiveValue, Mutated, Runner, RunnerOptions,
    TableId, Tailer, pb,
};
use loams_tikv::testing::{self, TEST_LIVE};
use loams_tikv::{Fault, FaultPlan, FaultPoint, TimestampExt, TxnOptions};
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::{Barrier, Notify};

// ---- helpers ----

fn s(v: &str) -> LiveValue {
    LiveValue::Str(v.to_string())
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn fields(pairs: &[(&str, LiveValue)]) -> BTreeMap<String, LiveValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn sys(name: &str) -> Arc<dyn Function> {
    system::lookup(name).expect("a system function")
}

fn config(cluster: &testing::TestCluster, shards: u16, limits: Limits) -> LiveConfig {
    LiveConfig {
        app: "t10".to_string(),
        tikv: cluster.config(TEST_LIVE),
        limits,
        journal_shards: shards,
    }
}

async fn open_with(shards: u16, limits: Limits, options: RunnerOptions) -> Option<Runner> {
    let cluster = testing::cluster().await?;
    let tikv = cluster.connect(TEST_LIVE).await;
    Some(
        Runner::open_with(tikv, &config(&cluster, shards, limits), options)
            .await
            .expect("the runner opens"),
    )
}

async fn open() -> Option<Runner> {
    open_with(
        DEFAULT_JOURNAL_SHARDS,
        Limits::default(),
        RunnerOptions::default(),
    )
    .await
}

async fn mutate(r: &Runner, f: Arc<dyn Function>, args: LiveValue) -> Mutated {
    r.mutate(f, args, None).await.expect("the mutation commits")
}

async fn insert(r: &Runner, table: &str, doc: &[(&str, LiveValue)]) -> DocId {
    let m = mutate(
        r,
        sys(INSERT),
        obj(&[("table", s(table)), ("fields", obj(doc))]),
    )
    .await;
    let LiveValue::Str(id) = m.result else {
        panic!("insert returns an id, not {:?}", m.result)
    };
    id.parse().expect("a document id")
}

async fn query_now(r: &Runner, f: &dyn Function, args: LiveValue) -> LiveValue {
    let at = r.tikv().now().await.expect("now");
    r.query(f, args, at).await.expect("the query runs").result
}

async fn get(r: &Runner, id: DocId) -> LiveValue {
    query_now(r, &*sys(GET), obj(&[("id", s(&id.to_string()))])).await
}

async fn all(r: &Runner, table: &str) -> Vec<LiveValue> {
    match query_now(r, &*sys(QUERY), obj(&[("table", s(table))])).await {
        LiveValue::Array(docs) => docs,
        other => panic!("a query returns an array, not {other:?}"),
    }
}

fn field(doc: &LiveValue, name: &str) -> LiveValue {
    match doc {
        LiveValue::Object(f) => f.get(name).cloned().unwrap_or(LiveValue::Null),
        other => panic!("a document is an object, not {other:?}"),
    }
}

async fn heads(r: &Runner) -> Vec<u64> {
    let journal = r.journal().await.expect("the journal");
    let at = r.tikv().now().await.expect("now");
    let mut snap = r.tikv().snapshot(at).await.expect("a snapshot");
    journal.heads(&mut snap).await.expect("heads")
}

fn arg_id(args: &LiveValue, name: &str) -> DocId {
    match field(args, name) {
        LiveValue::Str(id) => id.parse().expect("an id"),
        other => panic!("'{name}' is an id, not {other:?}"),
    }
}

fn is_on(doc: &Option<loams_live::docs::Doc>) -> bool {
    doc.as_ref()
        .is_some_and(|d| d.fields.get("on") == Some(&LiveValue::Bool(true)))
}

/// Waits at `barrier` the first time it is called, so two mutations both
/// read before either commits; reruns do not wait.
struct FirstCall {
    barrier: Arc<Barrier>,
    waited: AtomicBool,
}

impl FirstCall {
    fn new(barrier: Arc<Barrier>) -> Self {
        FirstCall {
            barrier,
            waited: AtomicBool::new(false),
        }
    }

    async fn wait(&self) {
        if !self.waited.swap(true, Ordering::SeqCst) {
            self.barrier.wait().await;
        }
    }
}

/// `{ me, other }`: reads both documents by id and turns `me` off if
/// `other` is still on (the on-call invariant: at least one stays on).
struct GoOffByIds(FirstCall);

impl Function for GoOffByIds {
    fn name(&self) -> &str {
        "test:go_off_by_ids"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let (me, other) = (arg_id(&args, "me"), arg_id(&args, "other"));
            let mine = txn.get(me).await?;
            let theirs = txn.get(other).await?;
            self.0.wait().await;
            if is_on(&mine) && is_on(&theirs) {
                txn.patch(me, fields(&[("on", LiveValue::Bool(false))]))
                    .await?;
                return Ok(LiveValue::Bool(true));
            }
            Ok(LiveValue::Bool(false))
        })
    }
}

/// `{ me }`: reads the whole `oncall` table by range and turns `me` off if
/// at least two are on.
struct GoOffByRange(FirstCall);

impl Function for GoOffByRange {
    fn name(&self) -> &str {
        "test:go_off_by_range"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let me = arg_id(&args, "me");
            let table = txn.table("oncall").await?.expect("the table exists");
            let docs = txn
                .query(IndexRange::all(table.id, IndexId::BY_CREATION_TIME))
                .await?;
            let on = docs
                .iter()
                .filter(|d| d.fields.get("on") == Some(&LiveValue::Bool(true)))
                .count();
            self.0.wait().await;
            if on >= 2 {
                txn.patch(me, fields(&[("on", LiveValue::Bool(false))]))
                    .await?;
                return Ok(LiveValue::Bool(true));
            }
            Ok(LiveValue::Bool(false))
        })
    }
}

/// `{ table, n }`: inserts `n` documents; with `fail`, then throws.
struct InsertMany;

impl Function for InsertMany {
    fn name(&self) -> &str {
        "test:insert_many"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let LiveValue::Str(table) = field(&args, "table") else {
                panic!("a table")
            };
            let LiveValue::I64(n) = field(&args, "n") else {
                panic!("a count")
            };
            for i in 0..n {
                txn.insert(&table, fields(&[("i", LiveValue::I64(i))]))
                    .await?;
            }
            if field(&args, "fail") == LiveValue::Bool(true) {
                return Err(LiveError::InvalidArgument("the function threw".into()));
            }
            Ok(LiveValue::Null)
        })
    }
}

/// `{ table, limits: [n…] }` (query or mutation): one range read of the
/// whole table per entry of `limits` (`0`: no limit); returns the counts.
struct Scans(FnKind);

impl Function for Scans {
    fn name(&self) -> &str {
        "test:scans"
    }
    fn kind(&self) -> FnKind {
        self.0
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let LiveValue::Str(name) = field(&args, "table") else {
                panic!("a table")
            };
            let LiveValue::Array(limits) = field(&args, "limits") else {
                panic!("limits")
            };
            if self.0 == FnKind::Mutation {
                txn.insert(&name, fields(&[("scanner", LiveValue::Bool(true))]))
                    .await?;
            }
            let table = txn.table(&name).await?.expect("the table exists");
            let mut counts = Vec::new();
            for limit in limits {
                let LiveValue::I64(limit) = limit else {
                    panic!("an int")
                };
                let mut range = IndexRange::all(table.id, IndexId::BY_CREATION_TIME);
                range.limit = (limit > 0).then_some(limit as u32);
                counts.push(LiveValue::I64(txn.query(range).await?.len() as i64));
            }
            Ok(LiveValue::Array(counts))
        })
    }
}

/// A query reading two documents by id and one index range.
struct Reader;

impl Function for Reader {
    fn name(&self) -> &str {
        "test:reader"
    }
    fn kind(&self) -> FnKind {
        FnKind::Query
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let found = txn.get(arg_id(&args, "found")).await?;
            let missing = txn.get(arg_id(&args, "missing")).await?;
            assert!(found.is_some() && missing.is_none());
            let table = txn.table("items").await?.expect("items exists");
            let docs = txn
                .query(IndexRange::all(table.id, IndexId::BY_CREATION_TIME))
                .await?;
            Ok(LiveValue::I64(docs.len() as i64))
        })
    }
}

/// A mutation that only reads a document.
struct ReadOnly;

impl Function for ReadOnly {
    fn name(&self) -> &str {
        "test:read_only"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let doc = txn.get(arg_id(&args, "id")).await?;
            Ok(LiveValue::Bool(doc.is_some()))
        })
    }
}

/// `{ id }`: reads `n`, then (first call only) tells the test it has read
/// and waits for its go, then writes `n + 1`. Records every `n` it read.
struct Increment {
    read: Notify,
    go: Notify,
    first: AtomicBool,
    seen: Mutex<Vec<i64>>,
}

impl Function for Increment {
    fn name(&self) -> &str {
        "test:increment"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let id = arg_id(&args, "id");
            let doc = txn.get(id).await?.expect("the counter exists");
            let LiveValue::I64(n) = doc.fields["n"] else {
                panic!("n is an int")
            };
            self.seen.lock().expect("seen").push(n);
            if !self.first.swap(true, Ordering::SeqCst) {
                self.read.notify_one();
                self.go.notified().await;
            }
            txn.patch(id, fields(&[("n", LiveValue::I64(n + 1))]))
                .await?;
            Ok(LiveValue::I64(n + 1))
        })
    }
}

/// Loses the acknowledgement of the first mutation attempt that reaches
/// `point`, once.
struct LoseOnce {
    point: FaultPoint,
    armed: AtomicBool,
}

impl FaultPlan for LoseOnce {
    fn at(&self, op: &str, point: FaultPoint, _attempt: u32) -> Option<Fault> {
        (op == MUTATION_OP && point == self.point && self.armed.swap(false, Ordering::SeqCst))
            .then_some(Fault::LoseAck)
    }
}

// ---- without a cluster ----

#[test]
fn idempotency_and_app_keys_follow_the_layout() {
    let app = AppKeys::dedicated();
    let hash = idempotency_hash("order-17").expect("a key");
    let key = app.idempotency(&hash);
    assert_eq!(key[0], IDEMPOTENCY);
    assert_eq!(&key[1..], &hash);
    assert!(app.idempotency_records().contains(&key));
    assert!(!app.journal().contains(&key));
    assert_eq!(app.app_def(), vec![0x01, KIND_APP]);
    for k in [
        app.journal_head(3),
        app.journal_entry(3, 9),
        app.journal_checkpoint(3, "n"),
    ] {
        assert!(app.journal().contains(&k));
    }
    // A table not created yet: its index entries are in `tables_from`.
    let later = app.index_entry(
        IndexId::BY_CREATION_TIME,
        &[],
        7,
        &DocId {
            table: TableId(9),
            bytes: [1; 16],
        },
    );
    assert!(app.tables_from(TableId(5)).contains(&later));
    assert!(!app.tables_from(TableId(10)).contains(&later));
}

/// Owner ruling on T9-7 (row T10-2): a rerun draws among the other shards.
#[test]
fn a_rerun_draws_a_journal_shard_other_than_the_last() {
    let journal = Journal::new(AppKeys::dedicated(), 16).expect("16 shards");
    let mut rng = StdRng::seed_from_u64(10);
    for skip in 0..16 {
        let drawn: BTreeSet<u16> = (0..2000)
            .map(|_| journal.pick_other(&mut rng, Some(skip)))
            .collect();
        assert!(!drawn.contains(&skip), "shard {skip} is excluded");
        assert_eq!(drawn.len(), 15, "every other shard is drawn");
    }
    let one = Journal::new(AppKeys::dedicated(), 1).expect("1 shard");
    assert_eq!(one.pick_other(&mut rng, Some(0)), 0, "one shard: no choice");
}

// ---- on TiKV ----

/// Semantics 1: a conflict reruns the function from scratch at a new
/// snapshot, which sees the write it conflicted with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rerun_on_conflict_sees_new_snapshot() {
    let Some(r) = open().await else { return };
    let id = insert(&r, "counters", &[("n", LiveValue::I64(0))]).await;
    let f = Arc::new(Increment {
        read: Notify::new(),
        go: Notify::new(),
        first: AtomicBool::new(false),
        seen: Mutex::new(Vec::new()),
    });
    let task = {
        let (r, f) = (r.clone(), f.clone());
        tokio::spawn(async move {
            r.mutate(f, obj(&[("id", s(&id.to_string()))]), None)
                .await
                .expect("the increment commits")
        })
    };
    f.read.notified().await;
    mutate(
        &r,
        sys(PATCH),
        obj(&[
            ("id", s(&id.to_string())),
            ("fields", obj(&[("n", LiveValue::I64(10))])),
        ]),
    )
    .await;
    f.go.notify_one();
    let m = task.await.expect("the task");
    assert_eq!(m.attempts, 2, "one rerun");
    assert_eq!(*f.seen.lock().expect("seen"), vec![0, 10]);
    assert_eq!(m.result, LiveValue::I64(11));
    assert_eq!(field(&get(&r, id).await, "n"), LiveValue::I64(11));
}

/// Semantics 2 (§20 §5.2): two mutations that read each other's document by
/// id and write their own cannot both commit; one reruns, and the invariant
/// (at least one on call) holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_read_promotion_prevents_write_skew() {
    let Some(r) = open().await else { return };
    let on = [("on", LiveValue::Bool(true))];
    let (x, y) = (
        insert(&r, "oncall", &on).await,
        insert(&r, "oncall", &on).await,
    );
    let barrier = Arc::new(Barrier::new(2));
    let run = |me: DocId, other: DocId| {
        let r = r.clone();
        let f = Arc::new(GoOffByIds(FirstCall::new(barrier.clone())));
        tokio::spawn(async move {
            r.mutate(
                f,
                obj(&[("me", s(&me.to_string())), ("other", s(&other.to_string()))]),
                None,
            )
            .await
            .expect("commits")
        })
    };
    let (a, b) = (run(x, y), run(y, x));
    let (a, b) = (a.await.expect("a"), b.await.expect("b"));
    assert!(
        a.attempts + b.attempts >= 3,
        "one of them reran: {} + {}",
        a.attempts,
        b.attempts
    );
    let went_off = [&a, &b]
        .iter()
        .filter(|m| m.result == LiveValue::Bool(true))
        .count();
    assert_eq!(went_off, 1, "exactly one went off");
    let still_on = [get(&r, x).await, get(&r, y).await]
        .iter()
        .filter(|d| field(d, "on") == LiveValue::Bool(true))
        .count();
    assert_eq!(still_on, 1, "the invariant holds");
}

/// Two mutations that each read the table by range and turn their own
/// document off, on a journal of 1 024 shards (so their heads rarely meet).
/// Returns how many documents are still on and the attempts.
async fn range_skew_round(r: &Runner) -> (usize, u32) {
    let on = [("on", LiveValue::Bool(true))];
    let (x, y) = (
        insert(r, "oncall", &on).await,
        insert(r, "oncall", &on).await,
    );
    let barrier = Arc::new(Barrier::new(2));
    let run = |me: DocId| {
        let r = r.clone();
        let f = Arc::new(GoOffByRange(FirstCall::new(barrier.clone())));
        tokio::spawn(async move {
            r.mutate(f, obj(&[("me", s(&me.to_string()))]), None)
                .await
                .expect("commits")
        })
    };
    let (a, b) = (run(x), run(y));
    let (a, b) = (a.await.expect("a"), b.await.expect("b"));
    let still_on = [get(r, x).await, get(r, y).await]
        .iter()
        .filter(|d| field(d, "on") == LiveValue::Bool(true))
        .count();
    // Leave nothing on for the next round's count.
    for id in [x, y] {
        mutate(r, sys(DELETE), obj(&[("id", s(&id.to_string()))])).await;
    }
    (still_on, a.attempts + b.attempts)
}

/// §20 §5.2 and Q31: index-range reads are snapshot reads, so two mutations
/// that each read a range the other writes into can both commit (write
/// skew). This pins the documented R1 behaviour until Q31 is decided.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_write_skew_is_possible_and_documented() {
    let Some(r) = open_with(1024, Limits::default(), RunnerOptions::default()).await else {
        return;
    };
    let mut anomalies = 0;
    for _ in 0..3 {
        let (still_on, _) = range_skew_round(&r).await;
        if still_on == 0 {
            anomalies += 1;
        }
    }
    assert!(
        anomalies > 0,
        "range write skew occurred in at least one of 3 rounds under snapshot isolation"
    );
}

/// Row T10-4: with `serializable_ranges`, a range-reading mutation that
/// writes locks every journal head, so of two write-skewing mutations one
/// must abort and rerun, and the invariant holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_write_skew_is_prevented_with_serializable_ranges() {
    let options = RunnerOptions {
        serializable_ranges: true,
        ..RunnerOptions::default()
    };
    let Some(r) = open_with(1024, Limits::default(), options).await else {
        return;
    };
    for _ in 0..3 {
        let (still_on, attempts) = range_skew_round(&r).await;
        assert_eq!(still_on, 1, "the invariant holds");
        assert!(attempts >= 3, "one of the two reran ({attempts} attempts)");
    }
}

/// Review Focus 2: an idempotent mutation applies once across a lost
/// acknowledgement (resolved through the commit token), across an unknown
/// outcome that the token fences (rerun, applied once), and across the
/// client's retry of the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_mutate_applies_once_across_lost_ack() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    for (point, key) in [
        (FaultPoint::AfterCommit, "after-commit"),
        (FaultPoint::BeforeCommit, "before-commit"),
    ] {
        let plan = Arc::new(LoseOnce {
            point,
            armed: AtomicBool::new(true),
        });
        let tikv = cluster.connect(TEST_LIVE).await.with_faults(plan.clone());
        let r = Runner::open(tikv, &config(&cluster, 16, Limits::default()))
            .await
            .expect("opens");
        let args = obj(&[
            ("table", s("orders")),
            ("fields", obj(&[("n", LiveValue::I64(1))])),
        ]);
        let first = r
            .mutate(sys(INSERT), args.clone(), Some(key.to_string()))
            .await
            .expect("commits");
        assert!(!plan.armed.load(Ordering::SeqCst), "{key}: the fault fired");
        match point {
            FaultPoint::AfterCommit => {
                assert!(first.earlier_unknown, "{key}: resolved as committed");
                assert_eq!(first.attempts, 1);
            }
            _ => assert_eq!(first.attempts, 2, "{key}: fenced, then rerun"),
        }
        assert!(!first.replayed);
        assert_eq!(all(&r, "orders").await.len(), 1, "{key}: applied once");

        let again = r
            .mutate(sys(INSERT), args.clone(), Some(key.to_string()))
            .await
            .expect("replays");
        assert!(again.replayed, "{key}: the retry is a replay");
        assert_eq!(again.result, first.result, "{key}: the same result");
        assert!(again.journal.is_none());
        assert!(again.commit_ts.version() >= first.commit_ts.version());
        assert_eq!(all(&r, "orders").await.len(), 1, "{key}: still once");

        // Another function under the same key is refused.
        let reused = r
            .mutate(
                sys(DELETE),
                obj(&[("id", first.result.clone())]),
                Some(key.to_string()),
            )
            .await;
        assert!(
            matches!(reused, Err(LiveError::InvalidArgument(_))),
            "{reused:?}"
        );
    }
}

/// Idempotent retries of one key, sequential and concurrent, apply the
/// mutation exactly once and all return its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_retry_of_the_same_key_applies_once() {
    let Some(r) = open().await else { return };
    let args = obj(&[
        ("table", s("payments")),
        ("fields", obj(&[("cents", LiveValue::I64(500))])),
    ]);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (r, args) = (r.clone(), args.clone());
        tasks.push(tokio::spawn(async move {
            r.mutate(sys(INSERT), args, Some("pay-1".to_string()))
                .await
                .expect("commits or replays")
        }));
    }
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.expect("a task"));
    }
    let ran: Vec<&Mutated> = results.iter().filter(|m| !m.replayed).collect();
    assert_eq!(ran.len(), 1, "one call ran the function");
    assert!(results.iter().all(|m| m.result == ran[0].result));
    let later = r
        .mutate(sys(INSERT), args, Some("pay-1".to_string()))
        .await
        .expect("replays");
    assert!(later.replayed);
    assert_eq!(later.result, ran[0].result);
    assert_eq!(all(&r, "payments").await.len(), 1);
    // Another key applies again.
    let other = r
        .mutate(
            sys(INSERT),
            obj(&[("table", s("payments")), ("fields", obj(&[]))]),
            Some("pay-2".to_string()),
        )
        .await
        .expect("commits");
    assert!(!other.replayed);
    assert_eq!(all(&r, "payments").await.len(), 2);
}

/// Semantics 4 and §20 §8.1: a query's read set holds the document keys it
/// read by id (found or not) and its index ranges, and covers the keys a
/// later write to them touches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_set_covers_points_and_ranges() {
    let Some(r) = open().await else { return };
    let found = insert(&r, "items", &[("k", LiveValue::I64(1))]).await;
    let missing = DocId {
        table: found.table,
        bytes: [7; 16],
    };
    let at = r.tikv().now().await.expect("now");
    let q = r
        .query(
            &Reader,
            obj(&[
                ("found", s(&found.to_string())),
                ("missing", s(&missing.to_string())),
            ]),
            at.clone(),
        )
        .await
        .expect("the query");
    assert_eq!(q.result, LiveValue::I64(1));
    assert_eq!(q.ts, at);
    let app = r.app();
    let points: BTreeSet<Vec<u8>> = [app.document(&found), app.document(&missing)].into();
    assert_eq!(q.read_set.points, points);
    assert_eq!(q.read_set.ranges.len(), 1);
    let doc = get(&r, found).await;
    let LiveValue::I64(created) = field(&doc, "_creationTime") else {
        panic!("a creation time")
    };
    let entry = app.index_entry(IndexId::BY_CREATION_TIME, &[], created as u64, &found);
    assert!(
        q.read_set.covers(&entry),
        "the range holds the document's entry"
    );
    assert!(q.read_set.covers(&app.document(&missing)));
    assert_eq!(q.usage.scanned_docs, 3);
    assert_eq!(q.usage.index_ranges, 1);

    // A query of a table that does not exist yet depends on its creation.
    let at = r.tikv().now().await.expect("now");
    let empty = r
        .query(&*sys(QUERY), obj(&[("table", s("later"))]), at)
        .await
        .expect("the query");
    assert_eq!(empty.result, LiveValue::Array(Vec::new()));
    let later = insert(&r, "later", &[]).await;
    let doc = get(&r, later).await;
    let LiveValue::I64(created) = field(&doc, "_creationTime") else {
        panic!("a creation time")
    };
    let entry = app.index_entry(IndexId::BY_CREATION_TIME, &[], created as u64, &later);
    assert!(
        empty.read_set.covers(&entry),
        "the insert creating the table invalidates the empty result"
    );
}

/// Row T9-10: a read-only mutation writes no journal entry (and no
/// idempotency record).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_mutation_writes_no_journal_entry() {
    let Some(r) = open().await else { return };
    let id = insert(&r, "notes", &[]).await;
    let before = heads(&r).await;
    let m = r
        .mutate(
            Arc::new(ReadOnly),
            obj(&[("id", s(&id.to_string()))]),
            Some("ro-1".to_string()),
        )
        .await
        .expect("commits");
    assert_eq!(m.result, LiveValue::Bool(true));
    assert!(m.journal.is_none());
    assert_eq!(heads(&r).await, before, "no head moved");
    let hash = idempotency_hash("ro-1").expect("a key");
    let at = r.tikv().now().await.expect("now");
    let mut snap = r.tikv().snapshot(at).await.expect("a snapshot");
    assert_eq!(
        snap.get(&r.app().idempotency(&hash)).await.expect("read"),
        None
    );
    let w = insert(&r, "notes", &[]).await;
    assert_ne!(w, id);
    assert_eq!(
        heads(&r).await.iter().sum::<u64>(),
        before.iter().sum::<u64>() + 1,
        "a writing mutation moves one head by one"
    );
}

/// Semantics 5: the scanned-document limit is counted over every read of
/// one attempt; the written-document limit likewise, and an exceeded limit
/// rolls the attempt back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_limit_is_enforced() {
    let limits = Limits {
        max_scanned_docs: 5,
        max_written_docs: 10,
        ..Limits::default()
    };
    let Some(r) = open_with(16, limits, RunnerOptions::default()).await else {
        return;
    };
    let many = |n: i64| obj(&[("table", s("rows")), ("n", LiveValue::I64(n))]);
    mutate(&r, Arc::new(InsertMany), many(8)).await;
    let scans = |limits: &[i64]| {
        obj(&[
            ("table", s("rows")),
            (
                "limits",
                LiveValue::Array(limits.iter().map(|&l| LiveValue::I64(l)).collect()),
            ),
        ])
    };
    let at = r.tikv().now().await.expect("now");
    let q = r
        .query(&Scans(FnKind::Query), scans(&[3]), at.clone())
        .await
        .expect("3 of 5");
    assert_eq!(q.result, LiveValue::Array(vec![LiveValue::I64(3)]));
    for over in [&[0][..], &[3, 3][..], &[5, 1][..]] {
        let e = r
            .query(&Scans(FnKind::Query), scans(over), at.clone())
            .await
            .expect_err("over the limit");
        assert!(
            matches!(
                e,
                LiveError::LimitExceeded {
                    limit: "max_scanned_docs",
                    ..
                }
            ),
            "{over:?}: {e:?}"
        );
    }
    // In a mutation the exceeded limit rolls back the attempt's insert.
    let e = r
        .mutate(Arc::new(Scans(FnKind::Mutation)), scans(&[3, 3]), None)
        .await
        .expect_err("over the limit");
    assert!(
        matches!(
            e,
            LiveError::LimitExceeded {
                limit: "max_scanned_docs",
                ..
            }
        ),
        "{e:?}"
    );
    let e = r
        .mutate(Arc::new(InsertMany), many(11), None)
        .await
        .expect_err("over the write limit");
    assert!(
        matches!(
            e,
            LiveError::LimitExceeded {
                limit: "max_written_docs",
                ..
            }
        ),
        "{e:?}"
    );
    let at = r.tikv().now().await.expect("now");
    let q = r
        .query(&Scans(FnKind::Query), scans(&[5]), at)
        .await
        .expect("5 of 5");
    assert_eq!(q.result, LiveValue::Array(vec![LiveValue::I64(5)]));
    let heads = heads(&r).await;
    assert_eq!(
        heads.iter().sum::<u64>(),
        1,
        "only the first insert committed"
    );
}

/// A function error rolls its writes back and reaches the caller as is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_function_error_rolls_back_its_writes() {
    let Some(r) = open().await else { return };
    let e = r
        .mutate(
            Arc::new(InsertMany),
            obj(&[
                ("table", s("drafts")),
                ("n", LiveValue::I64(3)),
                ("fail", LiveValue::Bool(true)),
            ]),
            Some("draft-1".to_string()),
        )
        .await
        .expect_err("the function threw");
    assert_eq!(e, LiveError::InvalidArgument("the function threw".into()));
    assert!(all(&r, "drafts").await.is_empty());
    assert_eq!(heads(&r).await.iter().sum::<u64>(), 0);
}

/// The built-in functions end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_functions_round_trip() {
    let Some(r) = open().await else { return };
    let id = insert(&r, "people", &[("a", LiveValue::I64(1))]).await;
    let doc = get(&r, id).await;
    assert_eq!(field(&doc, "a"), LiveValue::I64(1));
    assert_eq!(field(&doc, "_id"), s(&id.to_string()));
    assert!(matches!(field(&doc, "_creationTime"), LiveValue::I64(ms) if ms > 0));

    let idv = s(&id.to_string());
    mutate(
        &r,
        sys(PATCH),
        obj(&[
            ("id", idv.clone()),
            ("fields", obj(&[("b", LiveValue::I64(2))])),
        ]),
    )
    .await;
    let doc = get(&r, id).await;
    assert_eq!(
        (field(&doc, "a"), field(&doc, "b")),
        (LiveValue::I64(1), LiveValue::I64(2))
    );
    mutate(
        &r,
        sys(REPLACE),
        obj(&[
            ("id", idv.clone()),
            ("fields", obj(&[("c", LiveValue::I64(3))])),
        ]),
    )
    .await;
    let doc = get(&r, id).await;
    assert_eq!(field(&doc, "a"), LiveValue::Null);
    assert_eq!(field(&doc, "c"), LiveValue::I64(3));

    insert(&r, "people", &[("a", LiveValue::I64(9))]).await;
    let desc = query_now(
        &r,
        &*sys(QUERY),
        obj(&[
            ("table", s("people")),
            ("order", s("desc")),
            ("limit", LiveValue::I64(1)),
        ]),
    )
    .await;
    let LiveValue::Array(desc) = desc else {
        panic!("an array")
    };
    assert_eq!(desc.len(), 1);
    assert_eq!(field(&desc[0], "a"), LiveValue::I64(9), "newest first");
    let by_id = query_now(
        &r,
        &*sys(QUERY),
        obj(&[("table", s("people")), ("index", s("by_id"))]),
    )
    .await;
    assert!(matches!(by_id, LiveValue::Array(ref d) if d.len() == 2));
    let unknown = r
        .query(
            &*sys(QUERY),
            obj(&[("table", s("people")), ("index", s("by_name"))]),
            r.tikv().now().await.expect("now"),
        )
        .await;
    assert!(
        matches!(unknown, Err(LiveError::NotFound(_))),
        "{unknown:?}"
    );

    mutate(&r, sys(DELETE), obj(&[("id", idv.clone())])).await;
    assert_eq!(get(&r, id).await, LiveValue::Null);
    for (f, args) in [
        (DELETE, obj(&[("id", idv.clone())])),
        (PATCH, obj(&[("id", idv.clone()), ("fields", obj(&[]))])),
    ] {
        let e = r.mutate(sys(f), args, None).await;
        assert!(matches!(e, Err(LiveError::NotFound(_))), "{f}: {e:?}");
    }
    assert_eq!(all(&r, "nobody").await, Vec::<LiveValue>::new());

    // Kinds: a query cannot run as a mutation, nor the other way round.
    let e = r.mutate(sys(GET), obj(&[("id", idv.clone())]), None).await;
    assert!(matches!(e, Err(LiveError::InvalidArgument(_))), "{e:?}");
    let e = r
        .query(&*sys(INSERT), obj(&[]), r.tikv().now().await.expect("now"))
        .await;
    assert!(matches!(e, Err(LiveError::InvalidArgument(_))), "{e:?}");
    let e = r
        .mutate(
            sys(INSERT),
            obj(&[("table", s("people")), ("extra", LiveValue::Null)]),
            None,
        )
        .await;
    assert!(matches!(e, Err(LiveError::InvalidArgument(_))), "{e:?}");
    assert!(system::lookup("_system:nope").is_none());
}

/// Row T10-1 (owner ruling on T9-7): the shard count is per app, stored in
/// its catalog, and changes only while the journal is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_shard_count_is_stored_per_app_and_changes_only_while_empty() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_LIVE).await;
    let r = Runner::open(tikv.clone(), &config(&cluster, 4, Limits::default()))
        .await
        .expect("opens");
    assert_eq!(r.journal().await.expect("journal").shards(), 4);
    let again = Runner::open(tikv.clone(), &config(&cluster, 16, Limits::default()))
        .await
        .expect("reopens");
    assert_eq!(
        again.journal().await.expect("journal").shards(),
        4,
        "the stored count wins"
    );
    assert!(matches!(
        r.set_journal_shards(0).await,
        Err(LiveError::InvalidArgument(_))
    ));
    r.set_journal_shards(8).await.expect("the journal is empty");
    assert_eq!(r.journal().await.expect("journal").shards(), 8);
    for _ in 0..12 {
        let m = mutate(
            &r,
            sys(INSERT),
            obj(&[("table", s("t")), ("fields", obj(&[]))]),
        )
        .await;
        let (shard, _) = m.journal.expect("an entry");
        assert!(shard < 8, "a mutation uses the stored count");
    }
    let e = r.set_journal_shards(16).await;
    assert!(matches!(e, Err(LiveError::FailedPrecondition(_))), "{e:?}");
    r.set_journal_shards(8)
        .await
        .expect("the same count is a no-op");
    assert_eq!(heads(&r).await.len(), 8);
}

/// Semantics 3: an expired idempotency record no longer replays, and the
/// janitor deletes expired records and keeps live ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn janitor_sweeps_expired_idempotency_records() {
    let Some(r) = open().await else { return };
    let app = r.app().clone();
    let put_expired = |key: &str| {
        let k = app.idempotency(&idempotency_hash(key).expect("a key"));
        let v = pb::IdempotencyRecord {
            format: 1,
            result: LiveValue::Null.to_proto().into(),
            expires_ms: 1,
            function: INSERT.to_string(),
            ..Default::default()
        }
        .encode_to_vec();
        let tikv = r.tikv().clone();
        async move {
            let key = k.clone();
            tikv.run(TxnOptions::new("test.put"), move |txn| {
                let (k, v) = (k.clone(), v.clone());
                Box::pin(async move { txn.put(&k, v).await })
            })
            .await
            .expect("written");
            key
        }
    };
    let insert_args = obj(&[("table", s("t")), ("fields", obj(&[]))]);
    r.mutate(sys(INSERT), insert_args.clone(), Some("live".to_string()))
        .await
        .expect("commits");
    put_expired("stale").await;
    let rerun = r
        .mutate(sys(INSERT), insert_args.clone(), Some("stale".to_string()))
        .await
        .expect("runs");
    assert!(!rerun.replayed, "an expired record does not replay");
    let expired = put_expired("expired").await;

    let journal = r.journal().await.expect("journal");
    let report = Janitor::new(r.tikv().clone(), journal)
        .run_once()
        .await
        .expect("a pass");
    assert_eq!(report.expired_idempotency, 1);
    let at = r.tikv().now().await.expect("now");
    let mut snap = r.tikv().snapshot(at).await.expect("snap");
    assert_eq!(snap.get(&expired).await.expect("read"), None);
    for kept in ["live", "stale"] {
        let key = app.idempotency(&idempotency_hash(kept).expect("a key"));
        assert!(
            snap.get(&key).await.expect("read").is_some(),
            "{kept} is kept"
        );
    }
    assert_eq!(all(&r, "t").await.len(), 2);
}

/// Review of #76: a record the sweep cannot decode (corrupt, or a newer
/// format) is kept, since deleting it would free its key for a second run,
/// and the sweep goes on to the records after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn janitor_keeps_an_undecodable_idempotency_record_and_sweeps_on() {
    let Some(r) = open().await else { return };
    let app = r.app().clone();
    // The first possible record key, so the sweep meets it first.
    let garbage = app.idempotency(&[0; loams_live::keys::IDEMPOTENCY_HASH_BYTES]);
    let expired = app.idempotency(&idempotency_hash("expired").expect("a key"));
    let record = pb::IdempotencyRecord {
        format: 1,
        result: LiveValue::Null.to_proto().into(),
        expires_ms: 1,
        function: INSERT.to_string(),
        ..Default::default()
    }
    .encode_to_vec();
    let (g, e) = (garbage.clone(), expired.clone());
    r.tikv()
        .run(TxnOptions::new("test.put"), move |txn| {
            let (g, e, record) = (g.clone(), e.clone(), record.clone());
            Box::pin(async move {
                txn.put(&g, vec![0xff, 0xff, 0xff]).await?;
                txn.put(&e, record).await
            })
        })
        .await
        .expect("written");
    let journal = r.journal().await.expect("journal");
    let report = Janitor::new(r.tikv().clone(), journal)
        .run_once()
        .await
        .expect("the pass does not stop at the undecodable record");
    assert_eq!(report.expired_idempotency, 1);
    let at = r.tikv().now().await.expect("now");
    let mut snap = r.tikv().snapshot(at).await.expect("snap");
    assert_eq!(snap.get(&expired).await.expect("read"), None);
    assert!(snap.get(&garbage).await.expect("read").is_some(), "kept");
}

/// Review of #76: a live idempotency key reused with other arguments is
/// refused, not replayed; the same call still replays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_key_reused_with_other_arguments_is_refused() {
    let Some(r) = open().await else { return };
    let call = |cents: i64| {
        obj(&[
            ("table", s("payments")),
            ("fields", obj(&[("cents", LiveValue::I64(cents))])),
        ])
    };
    let key = Some("pay-1".to_string());
    let first = r
        .mutate(sys(INSERT), call(500), key.clone())
        .await
        .expect("commits");
    let other = r.mutate(sys(INSERT), call(900), key.clone()).await;
    assert!(
        matches!(&other, Err(LiveError::InvalidArgument(m)) if m.contains("other arguments")),
        "{other:?}"
    );
    let again = r
        .mutate(sys(INSERT), call(500), key)
        .await
        .expect("replays");
    assert!(again.replayed);
    assert_eq!(again.result, first.result);
    assert_eq!(all(&r, "payments").await.len(), 1);
}

/// Owner rulings on T9-7 and T10-3 (rows T10-2, T10-3, T11-1): 32 writers
/// and 2 000 mutations on the default 64 shards all commit within the
/// default budget of 16 attempts, with no `Conflict` failure, while a
/// tailer ticks continuously beside them (the tailer's cost at 64 shards).
/// Prints the rerun rate and the tick counts and latencies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutations_under_contention_complete_within_the_default_budget() {
    let Some(r) = open().await else { return };
    insert(&r, "load", &[]).await;
    const WRITERS: usize = 32;
    const MUTATIONS: usize = 2000;
    let journal = r.journal().await.expect("the journal");
    assert_eq!(journal.shards(), DEFAULT_JOURNAL_SHARDS);
    let at = r.tikv().now().await.expect("now");
    let mut tailer = Tailer::start(r.tikv().clone(), journal, at)
        .await
        .expect("a tailer");
    let stop = Arc::new(AtomicBool::new(false));
    let tailing = {
        let (r, stop) = (r.clone(), stop.clone());
        tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut entries = 0usize;
            let mut moved = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let started = std::time::Instant::now();
                let at = r.tikv().now().await.expect("now");
                let batch = tailer.tick(at).await.expect("a tick");
                tailer.ack(&batch).expect("ack");
                latencies.push(started.elapsed());
                entries += batch.entries.len();
                moved += batch
                    .from
                    .iter()
                    .zip(&batch.heads)
                    .filter(|(f, h)| f != h)
                    .count();
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            (latencies, entries, moved)
        })
    };
    let started = std::time::Instant::now();
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..WRITERS {
        let (r, next) = (r.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            let mut done = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= MUTATIONS {
                    return done;
                }
                let m = r
                    .mutate(
                        sys(INSERT),
                        obj(&[
                            ("table", s("load")),
                            ("fields", obj(&[("i", LiveValue::I64(i as i64))])),
                        ]),
                        None,
                    )
                    .await;
                done.push(m);
            }
        }));
    }
    let mut results = Vec::new();
    for t in tasks {
        results.extend(t.await.expect("a writer"));
    }
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let (mut latencies, entries, moved) = tailing.await.expect("the tailer");
    let failures: Vec<&LiveError> = results.iter().filter_map(|m| m.as_ref().err()).collect();
    assert!(failures.is_empty(), "failures: {failures:?}");
    let attempts: Vec<u32> = results
        .iter()
        .map(|m| m.as_ref().expect("ok").attempts)
        .collect();
    let reruns: u32 = attempts.iter().map(|a| a - 1).sum();
    let max = attempts.iter().max().copied().unwrap_or(0);
    latencies.sort();
    let pct = |p: usize| latencies[(latencies.len() - 1) * p / 100];
    eprintln!(
        "{MUTATIONS} mutations by {WRITERS} writers on {DEFAULT_JOURNAL_SHARDS} shards in {elapsed:?}: \
         {reruns} reruns ({:.3} per mutation), at most {max} attempts; tailer: {} ticks, \
         {entries} entries, {:.1} shards moved per tick, tick latency p50 {:?} p99 {:?} max {:?}",
        f64::from(reruns) / MUTATIONS as f64,
        latencies.len(),
        moved as f64 / latencies.len().max(1) as f64,
        pct(50),
        pct(99),
        pct(100),
    );
    assert!(max <= 16);
    assert_eq!(heads(&r).await.iter().sum::<u64>(), MUTATIONS as u64 + 1);
}

/// A patch through `LiveTxn` of a document id whose table does not exist is
/// `NotFound`, and `patch(id)` finds the table by the id's table part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_by_id_finds_the_table_of_the_id() {
    let Some(r) = open().await else { return };
    let a = insert(&r, "alpha", &[("v", LiveValue::I64(1))]).await;
    let b = insert(&r, "beta", &[("v", LiveValue::I64(1))]).await;
    assert_ne!(a.table, b.table);
    mutate(
        &r,
        sys(PATCH),
        obj(&[
            ("id", s(&b.to_string())),
            ("fields", obj(&[("v", LiveValue::I64(2))])),
        ]),
    )
    .await;
    assert_eq!(field(&get(&r, b).await, "v"), LiveValue::I64(2));
    assert_eq!(field(&get(&r, a).await, "v"), LiveValue::I64(1));
    let ghost = DocId {
        table: TableId(999),
        bytes: [3; 16],
    };
    let e = r
        .mutate(
            sys(PATCH),
            obj(&[("id", s(&ghost.to_string())), ("fields", obj(&[]))]),
            None,
        )
        .await;
    assert!(matches!(e, Err(LiveError::NotFound(_))), "{e:?}");
}

/// Review of #73 (Task 13's deploy gate): `try_quiesce` is refused while a
/// mutation is in flight, and while it is held a new mutation waits at
/// admission until it drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesce_refuses_in_flight_mutations_and_holds_new_ones() {
    let Some(r) = open().await else { return };
    let id = insert(&r, "counters", &[("n", LiveValue::I64(0))]).await;
    let f = Arc::new(Increment {
        read: Notify::new(),
        go: Notify::new(),
        first: AtomicBool::new(false),
        seen: Mutex::new(Vec::new()),
    });
    let in_flight = {
        let (r, f) = (r.clone(), f.clone());
        tokio::spawn(async move {
            r.mutate(f, obj(&[("id", s(&id.to_string()))]), None)
                .await
                .expect("commits")
        })
    };
    f.read.notified().await;
    assert!(r.try_quiesce().is_none(), "a mutation is in flight");
    f.go.notify_one();
    in_flight.await.expect("the task");

    let gate = r.try_quiesce().expect("nothing in flight");
    let waiting = {
        let r = r.clone();
        tokio::spawn(async move {
            mutate(
                &r,
                sys(PATCH),
                obj(&[
                    ("id", s(&id.to_string())),
                    ("fields", obj(&[("n", LiveValue::I64(5))])),
                ]),
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "held at admission");
    assert_eq!(field(&get(&r, id).await, "n"), LiveValue::I64(1));
    drop(gate);
    waiting.await.expect("the task");
    assert_eq!(field(&get(&r, id).await, "n"), LiveValue::I64(5));
}
