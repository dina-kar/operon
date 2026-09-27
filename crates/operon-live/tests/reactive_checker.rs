//! R1 plan Task 16 semantics 1: the reactive correctness checker.
//!
//! A seeded run drives a [`LiveServer`] through the generated connect-rust
//! client: `sessions` sessions watch random index ranges, limits, equality
//! reads, whole tables (one of them missing until a writer creates it) and
//! point reads of documents; `writers` writers run random inserts, patches,
//! replaces and deletes through `Mutate`. The server's TiKV handle carries a
//! random [`FaultPlan`] (Task 2's faults: refusals, conflicts, lost
//! acknowledgements and delays). For every Transition each session checks:
//! 1. it applies at the session's current version (a gap is a violation),
//!    and versions never move back (`ts` and the query-set version only
//!    grow; a Transition with updates changes the version);
//! 2. every result the session holds equals a fresh `Runner::query` at the
//!    Transition's timestamp, on a separate handle without faults: the
//!    updated queries (no stale result) and the others (no missed update),
//!    so a committed write that touches a subscribed range is in the first
//!    Transition at or after its commit timestamp;
//! 3. the results are exactly the query set of the version's query-set
//!    version (`ModifyQuerySet` adds and removes land).
//!
//! Sessions also resume at random: the first Transition after a resume must
//! start at the session's last version (not a fresh session's zero version,
//! which the TypeScript restart test cannot tell apart, row T14-13), carry
//! every query's result and pass check 2. After the writers stop, every
//! session sends a mutation with its session header, follows the ts-only
//! Transitions to its commit timestamp, and resumes once more: it must
//! converge to fresh results there. The server's safety rerun must count no
//! missed invalidation.
//!
//! Settings: `OPERON_CHECKER_SEED` (default random, printed),
//! `OPERON_CHECKER_SECS` (60; 1 800 with `OPERON_TEST_NIGHTLY`),
//! `OPERON_CHECKER_FAULTS=0` (no fault plan), `OPERON_CHECKER_NEMESIS=1`
//! (under `scripts/tikv/nemesis.sh`: availability errors are counted, not
//! failures, and a resume may fall back to a fresh session),
//! `OPERON_NEMESIS_EXPECT_REBUILD=1` (the server's TSO supervisor must have
//! rebuilt its client, Task 16 semantics 3) and
//! `OPERON_CHECKER_TICK_READ_LAG_MS` (the server's tick read lag, default
//! 200 ms).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use buffa::MessageField;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use operon_live::catalog::{self, IndexSpec};
use operon_live::pb::__buffa::oneof::query_set_change::Change;
use operon_live::pb::__buffa::oneof::watch_request::Start;
use operon_live::session::{ClientState, QueryResult, SESSION_HEADER, SessionConfig, Version};
use operon_live::system::{self, DELETE, GET, INSERT, PATCH, QUERY, REPLACE};
use operon_live::{
    AppKeys, Limits, LiveConfig, LiveError, LiveHandle, LiveServer, LiveValue, Runner, SubsConfig,
    pb,
};
use operon_tikv::testing::{self, TEST_LIVE};
use operon_tikv::{
    CommitMode, Fault, FaultPlan, FaultPoint, Tikv, TikvStats, Timestamp, TimestampExt, TxnOptions,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Watch = connectrpc::client::ServerStream<
    <HttpClient as connectrpc::client::ClientTransport>::ResponseBody,
    pb::__buffa::view::TransitionView<'static>,
>;

/// Whether this is the nightly run (`OPERON_TEST_NIGHTLY`), without
/// `testing::nightly`'s skip line: the checkers run every time and only
/// last longer at night.
fn nightly() -> bool {
    std::env::var(testing::NIGHTLY_ENV).is_ok_and(|v| !v.trim().is_empty())
}

/// The tables the sessions watch; `c` does not exist until a writer's first
/// insert creates it. `noise` is written and never watched.
const WATCHED: [&str; 3] = ["a", "b", "c"];
const NOISE: &str = "noise";
/// Index values are drawn from `0..N_RANGE`, so ranges overlap writes.
const N_RANGE: i64 = 24;

// ---- settings ----

#[derive(Debug, Clone)]
struct Opts {
    seed: u64,
    duration: Duration,
    sessions: usize,
    writers: usize,
    faults: bool,
    nemesis: bool,
    expect_rebuild: bool,
    tick_read_lag: Duration,
    safety_rerun: Duration,
    /// The broken-build hook of `checker_catches_injected_stale_result`:
    /// the server drops a journal batch every 250 ms.
    drop_batches: bool,
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1")
}

impl Opts {
    fn from_env() -> Self {
        let seed = std::env::var("OPERON_CHECKER_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(rand::random);
        let default_secs = if nightly() { 1_800 } else { 60 };
        let secs = std::env::var("OPERON_CHECKER_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_secs);
        let lag = std::env::var("OPERON_CHECKER_TICK_READ_LAG_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map_or(
                operon_live::subs::DEFAULT_TICK_READ_LAG,
                Duration::from_millis,
            );
        Opts {
            seed,
            duration: Duration::from_secs(secs),
            sessions: 4,
            writers: 4,
            faults: std::env::var("OPERON_CHECKER_FAULTS").map_or(true, |v| v != "0"),
            nemesis: env_flag("OPERON_CHECKER_NEMESIS"),
            expect_rebuild: env_flag("OPERON_NEMESIS_EXPECT_REBUILD"),
            tick_read_lag: lag,
            safety_rerun: Duration::from_secs(10),
            drop_batches: false,
        }
    }
}

// ---- the fault plan ----

/// Task 2's faults at random: about one attempt in `1/p` of the first two
/// gets one (later attempts run clean, so runs finish).
struct RandomFaults {
    rng: Mutex<ChaCha8Rng>,
    p: f64,
    injected: AtomicU64,
}

impl FaultPlan for RandomFaults {
    fn at(&self, _op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        if attempt > 2 {
            return None;
        }
        let mut rng = self
            .rng
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !rng.random_bool(self.p) {
            return None;
        }
        let fault = match (point, rng.random_range(0..4)) {
            (FaultPoint::BeforeBegin, _) => {
                Fault::Delay(Duration::from_millis(rng.random_range(1..40)))
            }
            (FaultPoint::BeforePrewrite, 0) => Fault::Refuse,
            (FaultPoint::BeforePrewrite | FaultPoint::BeforeCommit, 1) => Fault::Conflict,
            (FaultPoint::BeforeCommit, 0) => Fault::LoseAck,
            (FaultPoint::AfterCommit, 0 | 1) => Fault::LoseAck,
            _ => Fault::Delay(Duration::from_millis(rng.random_range(1..20))),
        };
        self.injected.fetch_add(1, Ordering::Relaxed);
        Some(fault)
    }
}

// ---- counters and the report ----

#[derive(Debug, Default)]
struct Counters {
    transitions: AtomicU64,
    checks: AtomicU64,
    unchecked: AtomicU64,
    resumes: AtomicU64,
    resume_fallbacks: AtomicU64,
    modifies: AtomicU64,
    mutations: AtomicU64,
    refused: AtomicU64,
    mutation_errors: AtomicU64,
    stream_errors: AtomicU64,
    /// Held results that were a retryable `UNAVAILABLE` error while a fresh
    /// evaluation succeeded (a storage outage the server reported, T11-8).
    held_unavailable: AtomicU64,
    /// Extra sync rounds a session needed to converge.
    resyncs: AtomicU64,
}

fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug)]
#[allow(dead_code)] // read through Debug
struct Report {
    seed: u64,
    secs: f64,
    transitions: u64,
    checks: u64,
    unchecked: u64,
    resumes: u64,
    resume_fallbacks: u64,
    modifies: u64,
    mutations: u64,
    refused: u64,
    mutation_errors: u64,
    stream_errors: u64,
    held_unavailable: u64,
    resyncs: u64,
    faults_injected: u64,
    ticks: u64,
    reruns: u64,
    safety_passes: u64,
    missed_invalidations: u64,
    server: TikvStats,
    checker: TikvStats,
}

// ---- the shared state of a run ----

struct Ctx {
    opts: Opts,
    http1: Client,
    http2: Client,
    /// Fresh evaluations, on a handle without faults.
    runner: Runner,
    counters: Counters,
    /// Documents the writers made and have not deleted: (table, id).
    known: Mutex<Vec<(String, String)>>,
    /// Point-read targets: the seeded documents (never deleted).
    seeded: Vec<String>,
    /// Each session's current id, for the writers' session headers.
    session_ids: Mutex<Vec<String>>,
    deadline: Instant,
    /// Cancelled when the writers are done.
    writers_done: CancellationToken,
    /// Cancelled on the first violation.
    failed: CancellationToken,
    violation: Mutex<Option<String>>,
}

impl Ctx {
    fn fail(&self, message: String) -> String {
        let mut v = self.violation.lock().expect("the violation lock");
        if v.is_none() {
            *v = Some(message.clone());
        }
        self.failed.cancel();
        message
    }
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn s(v: &str) -> LiveValue {
    LiveValue::Str(v.to_string())
}

fn int(v: i64) -> LiveValue {
    LiveValue::I64(v)
}

fn client(addr: std::net::SocketAddr, http2: bool) -> Client {
    let transport = if http2 {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };
    let uri = format!("http://{addr}").parse().expect("a uri");
    pb::LiveServiceClient::new(transport, ClientConfig::new(uri))
}

async fn mutate(
    c: &Client,
    function: &str,
    args: LiveValue,
    session: Option<&str>,
) -> Result<pb::MutateResponse, ConnectError> {
    let req = pb::MutateRequest {
        function: function.into(),
        args: MessageField::some(args.to_proto()),
        ..Default::default()
    };
    let mut options = CallOptions::default();
    if let Some(id) = session {
        options = options.with_header(SESSION_HEADER, id);
    }
    Ok(c.mutate_with_options(req, options).await?.into_owned())
}

fn doc_id(r: &pb::MutateResponse) -> Option<String> {
    let v = LiveValue::from_proto(r.result.clone().into_option()?).ok()?;
    match v {
        LiveValue::Str(id) => Some(id),
        _ => None,
    }
}

async fn define(tikv: &Tikv, table: &str) {
    let table = table.to_string();
    let specs = vec![IndexSpec {
        name: "by_n".into(),
        fields: vec!["n".into()],
    }];
    let mut opts = TxnOptions::new("test.define");
    opts.commit_mode = Some(CommitMode::TwoPc);
    tikv.run(opts, move |txn| {
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

// ---- queries ----

/// A watched query: its function and arguments.
type Spec = (&'static str, LiveValue);

fn random_query(rng: &mut ChaCha8Rng, seeded: &[String]) -> Spec {
    let table = |rng: &mut ChaCha8Rng| s(["a", "b"][rng.random_range(0..2)]);
    let lo = rng.random_range(0..N_RANGE);
    let hi = lo + rng.random_range(1..10);
    let order = if rng.random_bool(0.3) { "desc" } else { "asc" };
    match rng.random_range(0..10) {
        0..=3 => (
            QUERY,
            obj(&[
                ("table", table(rng)),
                ("index", s("by_n")),
                ("lower", obj(&[("value", int(lo))])),
                (
                    "upper",
                    obj(&[("value", int(hi)), ("inclusive", LiveValue::Bool(false))]),
                ),
                ("order", s(order)),
            ]),
        ),
        4 => (
            QUERY,
            obj(&[
                ("table", table(rng)),
                ("index", s("by_n")),
                ("lower", obj(&[("value", int(lo))])),
                ("order", s(order)),
                ("limit", int(rng.random_range(1..4))),
            ]),
        ),
        5 => (
            QUERY,
            obj(&[
                ("table", table(rng)),
                ("index", s("by_n")),
                ("eq", LiveValue::Array(vec![int(lo)])),
            ]),
        ),
        6 => (
            QUERY,
            obj(&[("table", s(WATCHED[rng.random_range(0..WATCHED.len())]))]),
        ),
        _ => (
            GET,
            obj(&[("id", s(&seeded[rng.random_range(0..seeded.len())]))]),
        ),
    }
}

fn set_proto(version: u64, set: &BTreeMap<u32, Spec>) -> pb::QuerySet {
    pb::QuerySet {
        version,
        queries: set.iter().map(|(id, q)| spec_proto(*id, q)).collect(),
        ..Default::default()
    }
}

fn spec_proto(id: u32, (function, args): &Spec) -> pb::QuerySpec {
    pb::QuerySpec {
        query_id: id,
        function: (*function).into(),
        args: MessageField::some(args.to_proto()),
        ..Default::default()
    }
}

/// Evaluates `spec` at `ts` on the checker's runner, retrying storage
/// errors; `None` when the cluster did not answer (counted, not a failure).
async fn fresh(ctx: &Ctx, spec: &Spec, ts: u64) -> Option<Result<LiveValue, LiveError>> {
    let f = system::lookup(spec.0).expect("a system function");
    for attempt in 0..30u32 {
        match ctx
            .runner
            .query(&*f, spec.1.clone(), Timestamp::from_version(ts))
            .await
        {
            Ok(q) => return Some(Ok(q.result)),
            Err(LiveError::Txn(e)) => {
                if attempt == 29 {
                    tracing::warn!(error = %e, "a fresh evaluation failed; unchecked");
                }
                tokio::time::sleep(Duration::from_millis(100 + 50 * u64::from(attempt))).await;
            }
            Err(e) => return Some(Err(e)),
        }
    }
    None
}

// ---- sessions ----

enum Phase {
    Running,
    /// Following the ts-only Transitions to this session's last commit.
    Syncing {
        commit: u64,
        until: Instant,
    },
    /// Resumed after the sync; done once its full Transition checks out.
    FinalResume {
        until: Instant,
    },
}

struct SessionRun<'a> {
    ctx: &'a Ctx,
    idx: usize,
    rng: ChaCha8Rng,
    next_query_id: u32,
    set_version: u64,
    set: BTreeMap<u32, Spec>,
    sets: BTreeMap<u64, BTreeMap<u32, Spec>>,
    state: ClientState,
    session_id: String,
    /// The version a resume started from, until its first Transition.
    resumed_from: Option<Version>,
    /// Updates received in the chunks of the Transition in progress.
    chunk_updates: usize,
    mid_chunk: bool,
    /// Whether the last checked Transition held an `UNAVAILABLE` result.
    unavailable: bool,
    /// Sync rounds so far (a round with an `UNAVAILABLE` result repeats).
    sync_rounds: u32,
}

impl SessionRun<'_> {
    fn violation(&self, what: String) -> String {
        self.ctx.fail(format!(
            "seed {}: session {} ({}): {what}",
            self.ctx.opts.seed, self.idx, self.session_id
        ))
    }

    fn client(&self) -> &Client {
        &self.ctx.http2
    }

    async fn open(&mut self, resume: bool) -> Result<Watch, ConnectError> {
        let set = set_proto(self.set_version, &self.set);
        let start = if resume {
            self.resumed_from = Some(self.state.version);
            Start::Resume(Box::new(pb::Resume {
                last_version: MessageField::some(self.state.version.to_proto()),
                query_set: MessageField::some(set),
                ..Default::default()
            }))
        } else {
            self.resumed_from = None;
            self.state = ClientState::new();
            Start::Initial(Box::new(set))
        };
        self.chunk_updates = 0;
        self.mid_chunk = false;
        self.client()
            .watch(pb::WatchRequest {
                start: Some(start),
                ..Default::default()
            })
            .await
    }

    /// Reopens after a lost stream: a resume, or (under the nemesis) a
    /// fresh session when the resume is refused.
    async fn reopen(&mut self, resume: bool) -> Result<Watch, String> {
        let mut tries = 0u32;
        loop {
            match self.open(resume && tries < 3).await {
                Ok(w) => return Ok(w),
                Err(e) if self.ctx.opts.nemesis && tries < 60 => {
                    inc(&self.ctx.counters.stream_errors);
                    tracing::info!(error = %e, "reopening a session");
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(self.violation(format!("Watch did not reopen: {e}"))),
            }
        }
    }

    /// Checks and applies one Transition (the module docs' checks 1–3).
    async fn handle(&mut self, t: pb::Transition) -> Result<(), String> {
        inc(&self.ctx.counters.transitions);
        let start = Version::from_proto(t.start.as_option());
        let end = Version::from_proto(t.end.as_option());
        if end.ts < start.ts || end.query_set < start.query_set {
            return Err(self.violation(format!("a Transition moves back: {start:?} → {end:?}")));
        }
        // A resume may end at the version it starts from (no tick since,
        // row T12-4); its full results are then checked like any others.
        if start == end && !t.updates.is_empty() && self.resumed_from.is_none() {
            return Err(self.violation(format!(
                "a Transition with {} updates keeps the version {start:?}",
                t.updates.len()
            )));
        }
        if !t.session_id.is_empty() && t.session_id != self.session_id {
            self.session_id = t.session_id.clone();
            self.ctx.session_ids.lock().expect("ids")[self.idx] = self.session_id.clone();
        }
        let before = self.state.version;
        self.chunk_updates += t.updates.len();
        match self.state.apply(&t) {
            Err(e) => {
                let resumed = self.resumed_from.map_or(String::new(), |v| {
                    format!(" (the first Transition after a resume from {v:?})")
                });
                return Err(self.violation(format!("{e}{resumed}")));
            }
            Ok(false) => {
                self.mid_chunk = true;
                return Ok(());
            }
            Ok(true) => self.mid_chunk = false,
        }
        let updates = std::mem::take(&mut self.chunk_updates);
        let resumed = self.resumed_from.take();
        if let Some(last) = resumed {
            let wanted = self.sets.get(&end.query_set).map_or(0, BTreeMap::len);
            if updates < wanted {
                return Err(self.violation(format!(
                    "a resume from {last:?} sent {updates} results, not all {wanted}"
                )));
            }
            // As the TypeScript client does (row T14-6): a resume carries
            // the full results of the set it sent, so results of queries
            // outside it (a removal whose Transition the old stream never
            // delivered) are dropped.
            if let Some(set) = self.sets.get(&end.query_set) {
                self.state.results.retain(|id, _| set.contains_key(id));
            }
        }
        // A heartbeat changes nothing; everything else is checked.
        if resumed.is_none() && updates == 0 && before == self.state.version {
            return Ok(());
        }
        let Some(set) = self.sets.get(&end.query_set) else {
            return Err(self.violation(format!(
                "a Transition ends at query-set version {}, which this client never sent",
                end.query_set
            )));
        };
        let held: BTreeSet<u32> = self.state.results.keys().copied().collect();
        let wanted: BTreeSet<u32> = set.keys().copied().collect();
        if held != wanted {
            return Err(self.violation(format!(
                "at {end:?} the results are for queries {held:?}, not the set's {wanted:?}"
            )));
        }
        self.unavailable = false;
        for (id, result) in &self.state.results {
            let spec = &set[id];
            let Some(f) = fresh(self.ctx, spec, end.ts).await else {
                inc(&self.ctx.counters.unchecked);
                continue;
            };
            inc(&self.ctx.counters.checks);
            let same = match (result, &f) {
                (QueryResult::Value(v), Ok(fv)) => v == fv,
                (QueryResult::Error(e), Err(fe)) => e.code.as_known() == Some(fe.code()),
                // A storage outage the server reported for this query
                // (row T11-8): not a wrong answer, and it must clear by the
                // final sync.
                (QueryResult::Error(e), Ok(_))
                    if e.code.as_known() == Some(pb::ErrorCode::ERROR_CODE_UNAVAILABLE) =>
                {
                    inc(&self.ctx.counters.held_unavailable);
                    self.unavailable = true;
                    true
                }
                _ => false,
            };
            if !same {
                return Err(self.violation(format!(
                    "query {id} {}({:?}) at ts {} (resumed: {}) differs from a fresh evaluation:\n  held:  {result:?}\n  fresh: {f:?}",
                    spec.0,
                    spec.1,
                    end.ts,
                    resumed.is_some()
                )));
            }
        }
        Ok(())
    }

    /// A random `ModifyQuerySet`: add a query or remove one.
    async fn modify(&mut self) -> Result<(), String> {
        let mut next = self.set.clone();
        let change = if next.len() > 1 && self.rng.random_bool(0.5) {
            let ids: Vec<u32> = next.keys().copied().collect();
            let id = ids[self.rng.random_range(0..ids.len())];
            next.remove(&id);
            Change::Remove(id)
        } else {
            let id = self.next_query_id;
            self.next_query_id += 1;
            let q = random_query(&mut self.rng, &self.ctx.seeded);
            let spec = spec_proto(id, &q);
            next.insert(id, q);
            Change::Add(Box::new(spec))
        };
        let version = self.set_version + 1;
        let req = pb::ModifyQuerySetRequest {
            session_id: self.session_id.clone(),
            base_version: self.set_version,
            new_version: version,
            changes: vec![pb::QuerySetChange {
                change: Some(change),
                ..Default::default()
            }],
            ..Default::default()
        };
        match self.client().modify_query_set(req).await {
            Ok(_) => {
                inc(&self.ctx.counters.modifies);
                self.set = next;
                self.set_version = version;
                self.sets.insert(version, self.set.clone());
                Ok(())
            }
            // The session ended (its stream error arrives next).
            Err(e) if e.code == ErrorCode::NotFound || self.ctx.opts.nemesis => {
                tracing::info!(error = %e, "ModifyQuerySet refused");
                Ok(())
            }
            Err(e) => Err(self.violation(format!("ModifyQuerySet failed: {e}"))),
        }
    }

    async fn run(mut self) -> Result<(), String> {
        let n = self.rng.random_range(3..6);
        for _ in 0..n {
            let id = self.next_query_id;
            self.next_query_id += 1;
            let q = random_query(&mut self.rng, &self.ctx.seeded);
            self.set.insert(id, q);
        }
        self.sets.insert(self.set_version, self.set.clone());
        let mut w = self.reopen(false).await?;
        let mut phase = Phase::Running;
        let mut next_action =
            Instant::now() + Duration::from_millis(self.rng.random_range(300..2_000));
        let sync_budget = if self.ctx.opts.nemesis {
            Duration::from_secs(240)
        } else {
            Duration::from_secs(60)
        };
        loop {
            if self.ctx.failed.is_cancelled() {
                return Err("another task found a violation".into());
            }
            let got = tokio::time::timeout(Duration::from_millis(500), w.message()).await;
            match got {
                Err(_) => {}
                Ok(Ok(Some(view))) => self.handle(view.to_owned_message()).await?,
                Ok(Ok(None)) | Ok(Err(_)) => {
                    let why = match got {
                        Ok(Err(e)) => e.to_string(),
                        _ => "the stream ended".into(),
                    };
                    inc(&self.ctx.counters.stream_errors);
                    if self.resumed_from.is_some() {
                        // A refused resume: only the nemesis may cause one.
                        if !self.ctx.opts.nemesis {
                            return Err(self.violation(format!("a resume was refused: {why}")));
                        }
                        inc(&self.ctx.counters.resume_fallbacks);
                        w = self.reopen(false).await?;
                    } else {
                        if !self.ctx.opts.nemesis {
                            return Err(self.violation(format!("the stream failed: {why}")));
                        }
                        w = self
                            .reopen(!self.mid_chunk && self.state.version != Version::default())
                            .await?;
                    }
                    continue;
                }
            }
            if self.mid_chunk || self.resumed_from.is_some() {
                continue;
            }
            match phase {
                Phase::Running if self.ctx.writers_done.is_cancelled() => {
                    // Sync: a mutation of this session on a table nobody
                    // watches; ts-only Transitions carry it to the commit.
                    let mut commit = None;
                    for _ in 0..120 {
                        match mutate(
                            &self.ctx.http1,
                            INSERT,
                            obj(&[("table", s(NOISE)), ("fields", obj(&[("n", int(0))]))]),
                            Some(&self.session_id),
                        )
                        .await
                        {
                            Ok(r) => {
                                commit = Some(r.commit_ts);
                                break;
                            }
                            Err(e) => {
                                tracing::info!(error = %e, "the sync mutation failed; retrying");
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }
                    let Some(commit) = commit else {
                        return Err(self.violation("the sync mutation never committed".into()));
                    };
                    phase = Phase::Syncing {
                        commit,
                        until: Instant::now() + sync_budget,
                    };
                }
                Phase::Running => {
                    if Instant::now() >= next_action {
                        match self.rng.random_range(0..10) {
                            0..=2 => {
                                inc(&self.ctx.counters.resumes);
                                w = self.reopen(true).await?;
                            }
                            3..=6 => self.modify().await?,
                            _ => {}
                        }
                        next_action = Instant::now()
                            + Duration::from_millis(self.rng.random_range(300..3_000));
                    }
                }
                Phase::Syncing { commit, until } => {
                    if self.state.version.ts >= commit {
                        inc(&self.ctx.counters.resumes);
                        w = self.reopen(true).await?;
                        phase = Phase::FinalResume {
                            until: Instant::now() + sync_budget,
                        };
                    } else if Instant::now() > until {
                        return Err(self.violation(format!(
                            "the session stayed at ts {} for {sync_budget:?}, below its commit {commit}",
                            self.state.version.ts
                        )));
                    }
                }
                // The resumed stream's first Transition passed `handle`;
                // one with an UNAVAILABLE result syncs again (the nemesis
                // may still be injecting faults).
                Phase::FinalResume { .. } if self.state.version.ts > 0 && self.unavailable => {
                    self.sync_rounds += 1;
                    inc(&self.ctx.counters.resyncs);
                    if self.sync_rounds > 20 {
                        return Err(self.violation(
                            "results stayed UNAVAILABLE through 20 sync rounds".into(),
                        ));
                    }
                    phase = Phase::Running;
                }
                Phase::FinalResume { .. } if self.state.version.ts > 0 => return Ok(()),
                Phase::FinalResume { until } => {
                    if Instant::now() > until {
                        return Err(self.violation("the final resume sent nothing".into()));
                    }
                }
            }
        }
    }
}

// ---- writers ----

async fn writer(ctx: Arc<Ctx>, idx: usize) {
    let mut rng = ChaCha8Rng::seed_from_u64(ctx.opts.seed ^ (0x5752_4954_4552 + idx as u64));
    let c = if idx.is_multiple_of(2) {
        &ctx.http1
    } else {
        &ctx.http2
    };
    let halfway = ctx.deadline - ctx.opts.duration / 2;
    let mut created_c = false;
    while Instant::now() < ctx.deadline && !ctx.failed.is_cancelled() {
        let session = if rng.random_bool(0.2) {
            let ids = ctx.session_ids.lock().expect("ids");
            let id = ids[rng.random_range(0..ids.len())].clone();
            (!id.is_empty()).then_some(id)
        } else {
            None
        };
        let n = int(rng.random_range(0..N_RANGE));
        let pick = |rng: &mut ChaCha8Rng, take: bool| {
            let mut known = ctx.known.lock().expect("known");
            if known.is_empty() {
                return None;
            }
            let i = rng.random_range(0..known.len());
            Some(if take {
                known.swap_remove(i)
            } else {
                known[i].clone()
            })
        };
        let (function, args, table) = if idx == 0 && !created_c && Instant::now() >= halfway {
            created_c = true;
            (
                INSERT,
                obj(&[("table", s("c")), ("fields", obj(&[("n", n)]))]),
                Some("c"),
            )
        } else {
            match rng.random_range(0..20) {
                0..=6 => {
                    let t = ["a", "b"][rng.random_range(0..2)];
                    (
                        INSERT,
                        obj(&[("table", s(t)), ("fields", obj(&[("n", n)]))]),
                        Some(t),
                    )
                }
                7..=12 => match pick(&mut rng, false) {
                    Some((_, id)) => (
                        PATCH,
                        obj(&[("id", s(&id)), ("fields", obj(&[("n", n)]))]),
                        None,
                    ),
                    None => continue,
                },
                13..=14 => match pick(&mut rng, false) {
                    Some((_, id)) => (
                        REPLACE,
                        obj(&[
                            ("id", s(&id)),
                            ("fields", obj(&[("n", n), ("r", LiveValue::Bool(true))])),
                        ]),
                        None,
                    ),
                    None => continue,
                },
                15..=16 => match pick(&mut rng, true) {
                    Some((_, id)) => (DELETE, obj(&[("id", s(&id))]), None),
                    None => continue,
                },
                _ => (
                    INSERT,
                    obj(&[("table", s(NOISE)), ("fields", obj(&[("n", n)]))]),
                    None,
                ),
            }
        };
        match mutate(c, function, args, session.as_deref()).await {
            Ok(r) => {
                inc(&ctx.counters.mutations);
                if let (Some(t), Some(id)) = (table, doc_id(&r)) {
                    ctx.known.lock().expect("known").push((t.to_string(), id));
                }
            }
            // A patch or delete of a document another writer deleted.
            Err(e) if e.code == ErrorCode::NotFound => inc(&ctx.counters.refused),
            Err(e) => {
                inc(&ctx.counters.mutation_errors);
                tracing::info!(error = %e, function, "a mutation failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

// ---- a run ----

async fn run(cluster: &testing::TestCluster, opts: Opts) -> Result<Report, String> {
    eprintln!("reactive checker: {opts:?}");
    let started = Instant::now();
    let tikv_config = cluster.config(TEST_LIVE);
    let plan = Arc::new(RandomFaults {
        rng: Mutex::new(ChaCha8Rng::seed_from_u64(opts.seed ^ 0xFA17)),
        p: 0.04,
        injected: AtomicU64::new(0),
    });
    let mut server_tikv = Tikv::connect(tikv_config.clone())
        .await
        .expect("the server's handle connects");
    if opts.faults {
        server_tikv = server_tikv.with_faults(plan.clone());
    }
    let checker_tikv = Tikv::connect(tikv_config.clone())
        .await
        .expect("the checker's handle connects");
    let mut config = LiveConfig::with_tikv("t16", tikv_config);
    config.listen = "127.0.0.1:0".parse().expect("an address");
    config.subs = SubsConfig {
        safety_rerun: opts.safety_rerun,
        tick_read_lag: opts.tick_read_lag,
        ..SubsConfig::default()
    };
    config.session = SessionConfig {
        heartbeat: Duration::from_secs(3),
        ts_only_interval: Duration::from_millis(200),
        ..SessionConfig::default()
    };
    let runner = Runner::open(checker_tikv.clone(), &config)
        .await
        .expect("the checker's runner opens");
    for t in ["a", "b"] {
        define(&checker_tikv, t).await;
    }
    let shutdown = CancellationToken::new();
    let handle: LiveHandle = LiveServer::start_on(server_tikv, config, shutdown.clone())
        .await
        .expect("the server starts");
    let http1 = client(handle.addr, false);
    let http2 = client(handle.addr, true);
    let mut known = Vec::new();
    let mut seeded = Vec::new();
    for (i, t) in ["a", "b"].iter().cycle().take(20).enumerate() {
        let r = mutate(
            &http1,
            INSERT,
            obj(&[
                ("table", s(t)),
                ("fields", obj(&[("n", int(i as i64 % N_RANGE))])),
            ]),
            None,
        )
        .await
        .expect("a seed insert commits");
        let id = doc_id(&r).expect("an id");
        seeded.push(id.clone());
        // The first half may be patched and replaced, never deleted.
        if i >= 10 {
            known.push(((*t).to_string(), id));
        }
    }
    let ctx = Arc::new(Ctx {
        counters: Counters::default(),
        known: Mutex::new(known),
        seeded,
        session_ids: Mutex::new(vec![String::new(); opts.sessions]),
        deadline: Instant::now() + opts.duration,
        writers_done: CancellationToken::new(),
        failed: CancellationToken::new(),
        violation: Mutex::new(None),
        http1,
        http2,
        runner,
        opts: opts.clone(),
    });
    let mut sessions = tokio::task::JoinSet::new();
    for idx in 0..opts.sessions {
        let ctx = ctx.clone();
        sessions.spawn(async move {
            let rng = ChaCha8Rng::seed_from_u64(ctx.opts.seed ^ (0x5E55 + idx as u64));
            SessionRun {
                ctx: &ctx,
                idx,
                rng,
                next_query_id: 1,
                set_version: 1,
                set: BTreeMap::new(),
                sets: BTreeMap::new(),
                state: ClientState::new(),
                session_id: String::new(),
                resumed_from: None,
                chunk_updates: 0,
                mid_chunk: false,
                unavailable: false,
                sync_rounds: 0,
            }
            .run()
            .await
        });
    }
    let mut writers = tokio::task::JoinSet::new();
    for idx in 0..opts.writers {
        writers.spawn(writer(ctx.clone(), idx));
    }
    // The broken-build hook: drop a journal batch every 250 ms.
    let done = CancellationToken::new();
    let drops = async {
        if !opts.drop_batches {
            return;
        }
        loop {
            tokio::select! {
                () = done.cancelled() => return,
                () = ctx.failed.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(250)) => {
                    handle.subscriptions().drop_next_batch();
                }
            }
        }
    };
    let work = async {
        while writers.join_next().await.is_some() {}
        ctx.writers_done.cancel();
        let mut result = Ok(());
        while let Some(joined) = sessions.join_next().await {
            let r = joined
                .map_err(|e| format!("a session panicked: {e}"))
                .and_then(|r| r);
            if let Err(e) = r
                && result.is_ok()
            {
                result = Err(e);
                ctx.failed.cancel();
            }
        }
        done.cancel();
        result
    };
    let (outcome, ()) = tokio::join!(work, drops);
    let outcome = match outcome {
        Ok(()) => Ok(()),
        Err(e) => Err(ctx
            .violation
            .lock()
            .expect("the violation lock")
            .clone()
            .unwrap_or(e)),
    };
    let stats = handle.stats();
    let server = handle.tikv().stats();
    let checker = checker_tikv.stats();
    handle.stop().await;
    outcome?;
    let c = &ctx.counters;
    let report = Report {
        seed: opts.seed,
        secs: started.elapsed().as_secs_f64(),
        transitions: c.transitions.load(Ordering::Relaxed),
        checks: c.checks.load(Ordering::Relaxed),
        unchecked: c.unchecked.load(Ordering::Relaxed),
        resumes: c.resumes.load(Ordering::Relaxed),
        resume_fallbacks: c.resume_fallbacks.load(Ordering::Relaxed),
        modifies: c.modifies.load(Ordering::Relaxed),
        mutations: c.mutations.load(Ordering::Relaxed),
        refused: c.refused.load(Ordering::Relaxed),
        mutation_errors: c.mutation_errors.load(Ordering::Relaxed),
        stream_errors: c.stream_errors.load(Ordering::Relaxed),
        held_unavailable: c.held_unavailable.load(Ordering::Relaxed),
        resyncs: c.resyncs.load(Ordering::Relaxed),
        faults_injected: plan.injected.load(Ordering::Relaxed),
        ticks: stats.ticks,
        reruns: stats.reruns,
        safety_passes: stats.safety_passes,
        missed_invalidations: stats.missed_invalidations,
        server,
        checker,
    };
    eprintln!("reactive checker report: {report:#?}");
    if report.missed_invalidations > 0 {
        return Err(format!(
            "seed {}: the safety rerun counted {} missed invalidations",
            opts.seed, report.missed_invalidations
        ));
    }
    if report.checks == 0 || report.mutations == 0 {
        return Err(format!(
            "seed {}: the run checked nothing: {report:?}",
            opts.seed
        ));
    }
    if opts.expect_rebuild && report.server.client_rebuilds == 0 {
        return Err(format!(
            "seed {}: the nemesis stalled PD, but the server's TSO supervisor rebuilt no client",
            opts.seed
        ));
    }
    Ok(report)
}

/// The gate: 60 s per PR, 30 min nightly (settings in the module docs).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reactive_checker() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    if let Err(e) = run(&cluster, Opts::from_env()).await {
        panic!("{e}");
    }
}

/// The checker fails on a deliberately broken build: the server drops
/// journal batches (a lost invalidation), and with the safety rerun out of
/// the way the sessions hold stale results.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn checker_catches_injected_stale_result() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let opts = Opts {
        duration: Duration::from_secs(20),
        faults: false,
        nemesis: false,
        expect_rebuild: false,
        safety_rerun: Duration::from_secs(3_600),
        drop_batches: true,
        ..Opts::from_env()
    };
    let err = run(&cluster, opts)
        .await
        .expect_err("the checker must catch the stale results of dropped journal batches");
    eprintln!("caught: {err}");
    assert!(
        err.contains("differs from a fresh evaluation") || err.contains("missed invalidations"),
        "{err}"
    );
}
