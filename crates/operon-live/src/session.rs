//! Sessions (design §20 §7.1, §8.2; R1 plan Task 12): the query set of one
//! `Watch` stream, its versioned [`pb::Transition`]s, and the bounded,
//! merging queue they wait in.
//!
//! - **Versions.** A session's state is a [`Version`] (query-set version,
//!   identity version, ts). Every Transition goes from the session's current
//!   version (`start`) to a new one (`end`); a client applies it only from
//!   its own current version ([`ClientState::apply`]). The first Transition
//!   of a new session starts at the zero version and carries every result; a
//!   resumed session's first Transition starts at the client's last version
//!   and carries every result too (R1 sends no diffs, Ruling 9).
//! - **One timestamp.** Every result of a session is valid at its version's
//!   `ts`, a tick of the app's [`Subscriptions`]. The session holds an
//!   updates receiver from before its first subscription, and the manager
//!   publishes each tick before it answers subscribers or moves `current`,
//!   so a session that drains its receiver after reading `current()` holds
//!   every change up to it (row T12-3).
//! - **Backpressure.** Transitions wait in an [`Outbox`] of `queue` entries;
//!   when it is full, the queued Transitions are merged into one ([`merge`]).
//!   A session whose client has not taken a Transition for `blocked_limit`
//!   since its queue filled is closed with `RESOURCE_EXHAUSTED`; the client
//!   resumes.
//! - **Heartbeats.** An empty Transition (`start == end`) after `heartbeat`
//!   without one; while a mutation the session made (the [`SESSION_HEADER`]
//!   on `Mutate`) is not yet visible at the session's `ts`, a ts-only
//!   Transition at most once per `ts_only_interval`.
//! - **Chunks.** A Transition over `max_transition_bytes` goes out as several
//!   messages with one `end`, all but the last with `more = true` ([`chunks`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use buffa::Message;
use operon_tikv::{Timestamp, TimestampExt};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::pb::__buffa::oneof::query_set_change::Change;
use crate::pb::__buffa::oneof::query_update::Update;
use crate::pb::__buffa::oneof::watch_request::Start as StartOneof;
use crate::readset::SubId;
use crate::subs::{SubKey, SubResult, Subscriptions, Tick};
use crate::{Function, LiveError, LiveValue, pb};

/// The request header that names the session a `Mutate` belongs to, so the
/// session sends ts-only Transitions until the mutation is visible (§20
/// §7.1, row T12-6).
pub const SESSION_HEADER: &str = "loam-session-id";

/// The largest Transition message; larger ones are chunked (4 MiB).
pub const MAX_TRANSITION_BYTES: usize = 4 * 1024 * 1024;

/// Queries per session (§20 §8.2's quota).
pub const MAX_SESSION_QUERIES: usize = 1000;

/// How long a resumed session waits for a tick at or after the client's
/// last timestamp before it refuses the resume.
pub const RESUME_WAIT: Duration = Duration::from_secs(30);

/// A session's state version (§20 §7.1). The zero version is the state of a
/// client with no results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Version {
    pub query_set: u64,
    pub identity: u64,
    /// The TSO timestamp (its version) every result is valid at.
    pub ts: u64,
}

impl Version {
    /// The wire form.
    pub fn to_proto(self) -> pb::StateVersion {
        pb::StateVersion {
            query_set: self.query_set,
            identity: self.identity,
            ts: self.ts,
            ..Default::default()
        }
    }

    /// The version of `v`; an unset version is the zero version.
    pub fn from_proto(v: Option<&pb::StateVersion>) -> Self {
        v.map_or_else(Version::default, |v| Version {
            query_set: v.query_set,
            identity: v.identity,
            ts: v.ts,
        })
    }
}

/// A session's settings (the `session_*` keys of [`LiveConfig`](crate::LiveConfig)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    /// Transitions queued before they are merged (16).
    pub queue: usize,
    /// An empty Transition after this long without one (15 s).
    pub heartbeat: Duration,
    /// A client that takes no Transition for this long after its queue
    /// filled is disconnected (30 s).
    pub blocked_limit: Duration,
    /// The largest Transition message ([`MAX_TRANSITION_BYTES`]).
    pub max_transition_bytes: usize,
    /// Queries per session ([`MAX_SESSION_QUERIES`]).
    pub max_queries: usize,
    /// The shortest time between two ts-only Transitions (1 s).
    pub ts_only_interval: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig {
            queue: 16,
            heartbeat: Duration::from_secs(15),
            blocked_limit: Duration::from_secs(30),
            max_transition_bytes: MAX_TRANSITION_BYTES,
            max_queries: MAX_SESSION_QUERIES,
            ts_only_interval: Duration::from_secs(1),
        }
    }
}

// ---- the client's side ----

/// One query's result as a client holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Value(LiveValue),
    Error(pb::LiveError),
}

/// A client's view of a session: its version and its results. The
/// reference for the generated clients' session layer (Task 14), and what
/// the tests apply Transitions to.
#[derive(Debug, Clone, Default)]
pub struct ClientState {
    pub version: Version,
    pub results: BTreeMap<u32, QueryResult>,
    /// The chunks of a Transition whose last chunk has not arrived.
    partial: Option<(Version, Version, Vec<pb::QueryUpdate>)>,
}

impl ClientState {
    /// A client with no results, at the zero version.
    pub fn new() -> Self {
        ClientState::default()
    }

    /// A client that last saw `version` (a reconnect).
    pub fn at(version: Version) -> Self {
        ClientState {
            version,
            ..ClientState::default()
        }
    }

    /// Applies `t` if it starts at the client's version (for a chunk, if it
    /// continues the chunks received so far). Returns `true` when the
    /// Transition is complete and applied, `false` for a chunk with more to
    /// come. A gap is [`LiveError::FailedPrecondition`] and changes nothing:
    /// the client must resume.
    pub fn apply(&mut self, t: &pb::Transition) -> Result<bool, LiveError> {
        let start = Version::from_proto(t.start.as_option());
        let end = Version::from_proto(t.end.as_option());
        let mut updates = match self.partial.take() {
            Some((s, e, updates)) if s == start && e == end => updates,
            Some((s, e, updates)) => {
                let err = LiveError::FailedPrecondition(format!(
                    "a chunk of {start:?} → {end:?} arrived inside {s:?} → {e:?}"
                ));
                self.partial = Some((s, e, updates));
                return Err(err);
            }
            None if start == self.version => Vec::new(),
            None => {
                return Err(LiveError::FailedPrecondition(format!(
                    "a Transition from {start:?} does not apply at {:?}",
                    self.version
                )));
            }
        };
        updates.extend(t.updates.iter().cloned());
        if t.more {
            self.partial = Some((start, end, updates));
            return Ok(false);
        }
        let mut results = self.results.clone();
        for u in updates {
            match u.update {
                Some(Update::Value(v)) => {
                    let v = LiveValue::from_proto(*v)
                        .map_err(|e| LiveError::Corrupt(format!("query {}: {e}", u.query_id)))?;
                    results.insert(u.query_id, QueryResult::Value(v));
                }
                Some(Update::Error(e)) => {
                    results.insert(u.query_id, QueryResult::Error(*e));
                }
                Some(Update::Removed(_)) => {
                    results.remove(&u.query_id);
                }
                None => {
                    return Err(LiveError::Corrupt(format!(
                        "query {} has an update with nothing set",
                        u.query_id
                    )));
                }
            }
        }
        self.results = results;
        self.version = end;
        Ok(true)
    }
}

// ---- merging and chunking ----

/// One Transition from `a.start` to `b.end` with the updates of both, `b`'s
/// winning per query (`a.end` must be `b.start`, as in a session's queue).
pub fn merge(a: pb::Transition, b: pb::Transition) -> pb::Transition {
    let mut order: Vec<u32> = Vec::new();
    let mut by_id: HashMap<u32, pb::QueryUpdate> = HashMap::new();
    for u in a.updates.into_iter().chain(b.updates) {
        if by_id.insert(u.query_id, u.clone()).is_none() {
            order.push(u.query_id);
        }
    }
    pb::Transition {
        session_id: b.session_id,
        start: a.start,
        end: b.end,
        updates: order
            .into_iter()
            .filter_map(|id| by_id.remove(&id))
            .collect(),
        more: false,
        ..Default::default()
    }
}

/// `t` as messages of at most `max_bytes` each (one update larger than that
/// goes alone): all with `t`'s `start` and `end`, all but the last with
/// `more = true`.
pub fn chunks(t: pb::Transition, max_bytes: usize) -> Vec<pb::Transition> {
    if t.encoded_len() as usize <= max_bytes || t.updates.len() <= 1 {
        return vec![t];
    }
    let shell = pb::Transition {
        session_id: t.session_id.clone(),
        start: t.start.clone(),
        end: t.end.clone(),
        ..Default::default()
    };
    let base = shell.encoded_len() as usize + 2;
    let mut out: Vec<pb::Transition> = Vec::new();
    let mut current = shell.clone();
    let mut size = base;
    for u in t.updates {
        // The field tag and length prefix of the update.
        let len = u.encoded_len() as usize + 6;
        if !current.updates.is_empty() && size + len > max_bytes {
            current.more = true;
            out.push(std::mem::replace(&mut current, shell.clone()));
            size = base;
        }
        size += len;
        current.updates.push(u);
    }
    out.push(current);
    out
}

// ---- the queue ----

/// A session's bounded outbound queue (§20 §8.2): at most `capacity`
/// Transitions; a push into a full queue merges everything queued into one.
/// Closed when the client disconnects, the session ends, or the client
/// stays blocked past `blocked_limit`.
pub struct Outbox {
    state: Mutex<OutState>,
    notify: Notify,
    capacity: usize,
    blocked_limit: Duration,
}

#[derive(Default)]
struct OutState {
    queue: VecDeque<pb::Transition>,
    /// When the queue first filled since the client last took a Transition.
    full_since: Option<Instant>,
    /// The error the client gets next (the queue is dropped).
    error: Option<LiveError>,
    /// No more pushes; the client gets what is queued, then the end.
    closed: bool,
    /// The client's stream is gone.
    gone: bool,
}

impl fmt::Debug for Outbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox")
            .field("len", &self.len())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl Outbox {
    /// An empty queue of `capacity` (at least 1) Transitions.
    pub fn new(capacity: usize, blocked_limit: Duration) -> Self {
        Outbox {
            state: Mutex::default(),
            notify: Notify::new(),
            capacity: capacity.max(1),
            blocked_limit,
        }
    }

    fn lock(&self) -> MutexGuard<'_, OutState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Queues `t`, merging the queue when it is full. Returns `false` (and
    /// drops `t`) when the queue is closed, including when this push finds
    /// the client blocked past the limit.
    pub fn push(&self, t: pb::Transition) -> bool {
        let mut s = self.lock();
        if s.closed || s.gone || self.blocked(&mut s) {
            return false;
        }
        if s.queue.len() >= self.capacity {
            let mut merged: Option<pb::Transition> = None;
            for q in s.queue.drain(..).chain(std::iter::once(t)) {
                merged = Some(match merged {
                    None => q,
                    Some(m) => merge(m, q),
                });
            }
            s.queue.extend(merged);
            s.full_since.get_or_insert_with(Instant::now);
        } else {
            s.queue.push_back(t);
            if s.queue.len() >= self.capacity {
                s.full_since.get_or_insert_with(Instant::now);
            }
        }
        drop(s);
        self.notify.notify_one();
        true
    }

    /// Closes the queue if its client has been blocked past the limit;
    /// returns whether the queue is closed.
    pub fn check_blocked(&self) -> bool {
        let mut s = self.lock();
        let closed = self.blocked(&mut s) || s.closed || s.gone;
        drop(s);
        self.notify.notify_one();
        closed
    }

    fn blocked(&self, s: &mut OutState) -> bool {
        if s.closed {
            return false;
        }
        match s.full_since {
            Some(since) if since.elapsed() > self.blocked_limit => {
                s.queue.clear();
                s.closed = true;
                s.error = Some(LiveError::LimitExceeded {
                    limit: "blocked_limit",
                    message: format!(
                        "the client took no Transition for {:?} while its queue was full; \
                         reconnect and resume",
                        self.blocked_limit
                    ),
                });
                true
            }
            _ => false,
        }
    }

    /// Ends the queue: the client gets what is queued (or `error`, if
    /// given, instead), then the end of the stream.
    pub fn close(&self, error: Option<LiveError>) {
        let mut s = self.lock();
        if !s.closed {
            s.closed = true;
            if error.is_some() {
                s.queue.clear();
                s.error = error;
            }
        }
        drop(s);
        self.notify.notify_one();
    }

    /// The client's stream is gone; the session ends.
    pub fn client_gone(&self) {
        self.lock().gone = true;
        self.notify.notify_one();
    }

    /// Whether the queue takes no more Transitions.
    pub fn is_closed(&self) -> bool {
        let s = self.lock();
        s.closed || s.gone
    }

    /// Transitions queued.
    pub fn len(&self) -> usize {
        self.lock().queue.len()
    }

    /// Whether nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The next Transition, the closing error, or `None` at the end.
    pub async fn pop(&self) -> Option<Result<pb::Transition, LiveError>> {
        loop {
            let notified = self.notify.notified();
            {
                let mut s = self.lock();
                if let Some(t) = s.queue.pop_front() {
                    s.full_since = None;
                    return Some(Ok(t));
                }
                if let Some(e) = s.error.take() {
                    return Some(Err(e));
                }
                if s.closed || s.gone {
                    return None;
                }
            }
            notified.await;
        }
    }
}

// ---- sessions ----

/// Resolves a function name to the function the app serves.
pub type Resolve = Arc<dyn Fn(&str) -> Result<Arc<dyn Function>, LiveError> + Send + Sync>;

/// How a `Watch` starts.
#[derive(Debug, Clone)]
pub enum Start {
    /// A new session with this query set.
    Initial(pb::QuerySet),
    /// A reconnect: the client's last version and its query set.
    Resume { last: Version, set: pb::QuerySet },
}

impl Start {
    /// The start a `WatchRequest` asks for.
    pub fn from_request(req: pb::WatchRequest) -> Result<Self, LiveError> {
        match req.start {
            Some(StartOneof::Initial(set)) => Ok(Start::Initial(*set)),
            Some(StartOneof::Resume(r)) => {
                let r = *r;
                Ok(Start::Resume {
                    last: Version::from_proto(r.last_version.as_option()),
                    set: r.query_set.into_option().unwrap_or_default(),
                })
            }
            None => Err(LiveError::invalid(
                "a WatchRequest needs `initial` or `resume`",
            )),
        }
    }

    fn set(&self) -> &pb::QuerySet {
        match self {
            Start::Initial(set) | Start::Resume { set, .. } => set,
        }
    }
}

/// An open session: its id and the queue its stream reads.
#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub outbox: Arc<Outbox>,
}

enum Cmd {
    Modify {
        req: pb::ModifyQuerySetRequest,
        reply: oneshot::Sender<Result<(), LiveError>>,
    },
    /// A mutation of the session committed at this timestamp (version).
    Committed(u64),
}

/// The sessions of one app on this node.
#[derive(Clone)]
pub struct Sessions {
    inner: Arc<Inner>,
}

struct Inner {
    subs: Arc<Subscriptions>,
    resolve: Resolve,
    config: SessionConfig,
    node: String,
    shutdown: CancellationToken,
    map: Mutex<HashMap<String, mpsc::UnboundedSender<Cmd>>>,
}

impl fmt::Debug for Sessions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sessions")
            .field("node", &self.inner.node)
            .field("open", &self.len())
            .finish_non_exhaustive()
    }
}

impl Sessions {
    /// Sessions over `subs`, resolving function names with `resolve`, until
    /// `shutdown` (which ends every open session's stream). Session ids
    /// start with `node`.
    pub fn new(
        subs: Arc<Subscriptions>,
        resolve: Resolve,
        config: SessionConfig,
        node: String,
        shutdown: CancellationToken,
    ) -> Self {
        Sessions {
            inner: Arc::new(Inner {
                subs,
                resolve,
                config,
                node,
                shutdown,
                map: Mutex::default(),
            }),
        }
    }

    fn map(&self) -> MutexGuard<'_, HashMap<String, mpsc::UnboundedSender<Cmd>>> {
        self.inner
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open sessions.
    pub fn len(&self) -> usize {
        self.map().len()
    }

    /// Whether no session is open.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Opens a session: validates the query set, then subscribes it in the
    /// session's own task, whose first Transition arrives in the returned
    /// outbox.
    pub fn open(&self, start: Start) -> Result<Session, LiveError> {
        check_set(&self.inner.config, start.set())?;
        let id = format!(
            "{}-{:016x}{:016x}",
            self.inner.node,
            rand::random::<u64>(),
            rand::random::<u64>()
        );
        let outbox = Arc::new(Outbox::new(
            self.inner.config.queue,
            self.inner.config.blocked_limit,
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        self.map().insert(id.clone(), tx);
        let task = SessionTask {
            sessions: self.clone(),
            id: id.clone(),
            outbox: outbox.clone(),
            cmds: rx,
            updates: self.inner.subs.updates(),
            queries: BTreeMap::new(),
            by_sub: HashMap::new(),
            version: Version::default(),
            latest: 0,
            dirty: BTreeSet::new(),
            last_push: Instant::now(),
            last_ts_only: None,
            pending: None,
        };
        tokio::spawn(task.run(start));
        Ok(Session { id, outbox })
    }

    /// Applies a `ModifyQuerySet` to its session; answers once the
    /// Transition that reflects it is queued.
    pub async fn modify(&self, req: pb::ModifyQuerySetRequest) -> Result<(), LiveError> {
        let tx = self.map().get(&req.session_id).cloned().ok_or_else(|| {
            LiveError::NotFound(format!(
                "session {:?} is not open on this node",
                req.session_id
            ))
        })?;
        let (reply, answer) = oneshot::channel();
        tx.send(Cmd::Modify { req, reply })
            .map_err(|_| LiveError::NotFound("the session has ended".into()))?;
        answer
            .await
            .map_err(|_| LiveError::NotFound("the session has ended".into()))?
    }

    /// A mutation that named `session_id` committed at `commit_ts` (a TSO
    /// version): the session sends ts-only Transitions until its `ts`
    /// reaches it. An unknown session is ignored.
    pub fn mutation_committed(&self, session_id: &str, commit_ts: u64) {
        if let Some(tx) = self.map().get(session_id) {
            let _ = tx.send(Cmd::Committed(commit_ts));
        }
    }
}

fn check_set(config: &SessionConfig, set: &pb::QuerySet) -> Result<(), LiveError> {
    if set.queries.len() > config.max_queries {
        return Err(LiveError::limit(
            "max_queries",
            format!(
                "a session holds at most {} queries, not {}",
                config.max_queries,
                set.queries.len()
            ),
        ));
    }
    let mut ids = BTreeSet::new();
    for q in &set.queries {
        if !ids.insert(q.query_id) {
            return Err(LiveError::invalid(format!(
                "query id {} appears twice in the query set",
                q.query_id
            )));
        }
    }
    Ok(())
}

/// One query of a session.
struct Query {
    /// The subscription and what it was made from; `None` for a query that
    /// failed before it could subscribe (its error is fixed).
    sub: Option<(SubId, SubKey, Arc<dyn Function>, LiveValue)>,
    held: Held,
}

enum Held {
    Result(Arc<SubResult>),
    Fixed(pb::LiveError),
}

impl Held {
    fn update(&self, query_id: u32) -> pb::QueryUpdate {
        let update = match self {
            Held::Result(r) => match &r.result {
                Ok(v) => Update::Value(Box::new(v.to_proto())),
                Err(e) => Update::Error(Box::new(e.to_proto())),
            },
            Held::Fixed(e) => Update::Error(Box::new(e.clone())),
        };
        pb::QueryUpdate {
            query_id,
            update: Some(update),
            ..Default::default()
        }
    }
}

struct SessionTask {
    sessions: Sessions,
    id: String,
    outbox: Arc<Outbox>,
    cmds: mpsc::UnboundedReceiver<Cmd>,
    updates: broadcast::Receiver<Tick>,
    queries: BTreeMap<u32, Query>,
    by_sub: HashMap<SubId, BTreeSet<u32>>,
    /// The version of the last queued Transition.
    version: Version,
    /// The newest tick applied: every held result is valid at it.
    latest: u64,
    /// Queries whose held result changed since the last Transition.
    dirty: BTreeSet<u32>,
    last_push: Instant,
    last_ts_only: Option<Instant>,
    /// The newest commit of a mutation of this session that the session's
    /// `ts` has not reached.
    pending: Option<u64>,
}

impl SessionTask {
    fn subs(&self) -> &Subscriptions {
        &self.sessions.inner.subs
    }

    fn config(&self) -> &SessionConfig {
        &self.sessions.inner.config
    }

    async fn run(mut self, start: Start) {
        match self.begin(start).await {
            Ok(()) => self.serve().await,
            Err(e) => self.outbox.close(Some(e)),
        }
        for q in std::mem::take(&mut self.queries).into_values() {
            if let Some((id, ..)) = q.sub {
                self.sessions.inner.subs.unsubscribe(id);
            }
        }
        self.sessions.map().remove(&self.id);
        self.outbox.close(None);
    }

    /// Subscribes the query set and queues the first Transition.
    async fn begin(&mut self, start: Start) -> Result<(), LiveError> {
        let (from, set, resume_at) = match start {
            Start::Initial(set) => (Version::default(), set, None),
            Start::Resume { last, set } => (last, set, Some(last.ts)),
        };
        self.add(set.queries).await;
        if self.sync() {
            self.resubscribe().await;
        }
        if let Some(ts) = resume_at {
            self.wait_for(ts).await?;
        }
        self.version = from;
        self.dirty = self.queries.keys().copied().collect();
        let end = Version {
            query_set: set.version,
            identity: 0,
            ts: self.latest,
        };
        self.push(end, Vec::new());
        Ok(())
    }

    /// Waits (waking the manager) until a tick at or after `ts` is applied.
    async fn wait_for(&mut self, ts: u64) -> Result<(), LiveError> {
        let deadline = tokio::time::Instant::now() + RESUME_WAIT;
        while self.latest < ts {
            self.sessions.inner.subs.wake();
            let tick = tokio::select! {
                () = self.sessions.inner.shutdown.cancelled() => {
                    return Err(LiveError::Txn(operon_tikv::TxnError::NotApplied(
                        "the server is shutting down".into(),
                    )));
                }
                t = tokio::time::timeout_at(deadline, self.updates.recv()) => t,
            };
            match tick {
                Err(_) => {
                    return Err(LiveError::FailedPrecondition(format!(
                        "no tick reached the resumed session's timestamp {ts} within {RESUME_WAIT:?}"
                    )));
                }
                Ok(Ok(tick)) => self.apply_tick(&tick),
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => self.resubscribe().await,
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(LiveError::Internal(
                        "the subscription manager has stopped".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn serve(&mut self) {
        let c = self.config().clone();
        let period = [c.heartbeat, c.ts_only_interval, c.blocked_limit]
            .into_iter()
            .min()
            .unwrap_or(Duration::from_secs(1))
            .div_f64(4.0)
            .clamp(Duration::from_millis(10), Duration::from_millis(250));
        let mut housekeeping = tokio::time::interval(period);
        housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let shutdown = self.sessions.inner.shutdown.clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                tick = self.updates.recv() => match tick {
                    Ok(tick) => {
                        self.apply_tick(&tick);
                        self.push_changes();
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::debug!(session = %self.id, missed, "the session lagged; resubscribing");
                        self.resubscribe().await;
                        self.push_changes();
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        self.outbox.close(Some(LiveError::Internal(
                            "the subscription manager has stopped".into(),
                        )));
                        return;
                    }
                },
                cmd = self.cmds.recv() => match cmd {
                    None => return,
                    Some(Cmd::Modify { req, reply }) => {
                        let result = self.modify(req).await;
                        let _ = reply.send(result);
                    }
                    Some(Cmd::Committed(ts)) => {
                        if ts > self.version.ts {
                            self.pending = Some(self.pending.map_or(ts, |p| p.max(ts)));
                        }
                    }
                },
                _ = housekeeping.tick() => {}
            }
            if self.outbox.check_blocked() {
                return;
            }
            self.ts_only();
            if self.last_push.elapsed() >= self.config().heartbeat {
                let v = self.version;
                self.push(v, Vec::new());
            }
        }
    }

    /// Takes a tick's changed results for this session's queries.
    fn apply_tick(&mut self, tick: &Tick) {
        self.latest = self.latest.max(tick.at.version());
        for (id, result) in &tick.changed {
            let Some(qs) = self.by_sub.get(id) else {
                continue;
            };
            for q in qs.clone() {
                self.take(q, result);
            }
        }
    }

    /// Holds `result` for query `q` if it is newer than what is held.
    fn take(&mut self, q: u32, result: &Arc<SubResult>) {
        let Some(query) = self.queries.get_mut(&q) else {
            return;
        };
        if let Held::Result(held) = &query.held
            && (Arc::ptr_eq(held, result) || held.ts.version() > result.ts.version())
        {
            return;
        }
        query.held = Held::Result(result.clone());
        self.dirty.insert(q);
    }

    /// Applies every tick already in the receiver and the manager's current
    /// tick: every held result is then valid at `latest` (row T12-3).
    /// Returns `true` when the receiver lagged (ticks were dropped): tokio
    /// reports a lag once, so the caller must resubscribe to recover the
    /// dropped ticks' changes (review of #88).
    #[must_use]
    fn sync(&mut self) -> bool {
        if let Some(current) = self.subs().current() {
            self.latest = self.latest.max(current.version());
        }
        loop {
            match self.updates.try_recv() {
                Ok(tick) => self.apply_tick(&tick),
                Err(broadcast::error::TryRecvError::Lagged(_)) => return true,
                Err(_) => return false,
            }
        }
    }

    /// Subscribes `specs`; a query that cannot subscribe holds its error.
    async fn add(&mut self, specs: Vec<pb::QuerySpec>) {
        let resolve = self.sessions.inner.resolve.clone();
        let jobs = specs.into_iter().map(|spec| {
            let subs = self.sessions.inner.subs.clone();
            let resolve = resolve.clone();
            async move {
                let prepared = (|| {
                    let f = resolve(&spec.function)?;
                    let args = args_of(spec.args.into_option())?;
                    Ok::<_, LiveError>((f, args))
                })();
                let subscribed = match prepared {
                    Ok((f, args)) => {
                        let key = SubKey::new(f.name(), &args);
                        subs.subscribe(key.clone(), f.clone(), args.clone())
                            .await
                            .map(|(id, result)| (id, key, f, args, result))
                    }
                    Err(e) => Err(e),
                };
                (spec.query_id, subscribed)
            }
        });
        for (query_id, subscribed) in futures::future::join_all(jobs).await {
            self.remove(query_id);
            let query = match subscribed {
                Ok((id, key, f, args, result)) => {
                    self.by_sub.entry(id).or_default().insert(query_id);
                    Query {
                        sub: Some((id, key, f, args)),
                        held: Held::Result(result),
                    }
                }
                Err(e) => Query {
                    sub: None,
                    held: Held::Fixed(e.to_proto()),
                },
            };
            self.queries.insert(query_id, query);
            self.dirty.insert(query_id);
        }
    }

    /// Drops query `q` and its subscription reference; returns whether it
    /// was held.
    fn remove(&mut self, q: u32) -> bool {
        let Some(query) = self.queries.remove(&q) else {
            return false;
        };
        if let Some((id, ..)) = query.sub {
            if let Some(qs) = self.by_sub.get_mut(&id) {
                qs.remove(&q);
                if qs.is_empty() {
                    self.by_sub.remove(&id);
                }
            }
            self.sessions.inner.subs.unsubscribe(id);
        }
        self.dirty.remove(&q);
        true
    }

    /// Subscribes every query again (a lagged receiver may have missed
    /// changes), then drops the old references.
    async fn resubscribe(&mut self) {
        loop {
            let mut jobs = Vec::new();
            for (q, query) in &self.queries {
                if let Some((id, key, f, args)) = &query.sub {
                    jobs.push((*q, *id, key.clone(), f.clone(), args.clone()));
                }
            }
            for (q, old, key, f, args) in jobs {
                match self.sessions.inner.subs.subscribe(key, f, args).await {
                    Ok((id, result)) => {
                        if id != old {
                            if let Some(qs) = self.by_sub.get_mut(&old) {
                                qs.remove(&q);
                                if qs.is_empty() {
                                    self.by_sub.remove(&old);
                                }
                            }
                            self.by_sub.entry(id).or_default().insert(q);
                            if let Some(Query {
                                sub: Some((sub_id, ..)),
                                ..
                            }) = self.queries.get_mut(&q)
                            {
                                *sub_id = id;
                            }
                        }
                        self.sessions.inner.subs.unsubscribe(old);
                        self.take(q, &result);
                    }
                    Err(e) => {
                        tracing::warn!(session = %self.id, error = %e, "resubscribing a lagged session failed");
                    }
                }
            }
            if !self.sync() {
                break;
            }
        }
    }

    async fn modify(&mut self, req: pb::ModifyQuerySetRequest) -> Result<(), LiveError> {
        if req.base_version != self.version.query_set {
            return Err(LiveError::FailedPrecondition(format!(
                "the session's query set is at version {}, not {}",
                self.version.query_set, req.base_version
            )));
        }
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for c in req.changes {
            match c.change {
                Some(Change::Add(spec)) => adds.push(*spec),
                Some(Change::Remove(id)) => removes.push(id),
                None => return Err(LiveError::invalid("a QuerySetChange has nothing set")),
            }
        }
        let after: BTreeSet<u32> = self
            .queries
            .keys()
            .copied()
            .filter(|q| !removes.contains(q))
            .chain(adds.iter().map(|a| a.query_id))
            .collect();
        check_set(
            self.config(),
            &pb::QuerySet {
                queries: adds.clone(),
                ..Default::default()
            },
        )?;
        if after.len() > self.config().max_queries {
            return Err(LiveError::limit(
                "max_queries",
                format!(
                    "a session holds at most {} queries",
                    self.config().max_queries
                ),
            ));
        }
        let mut removed = Vec::new();
        for q in removes {
            if self.remove(q) {
                removed.push(q);
            }
        }
        self.add(adds).await;
        if self.sync() {
            self.resubscribe().await;
        }
        let end = Version {
            query_set: req.new_version,
            identity: self.version.identity,
            ts: self.latest.max(self.version.ts),
        };
        self.push(end, removed);
        Ok(())
    }

    /// Queues a Transition with the changed queries, if any.
    fn push_changes(&mut self) {
        if self.dirty.is_empty() {
            return;
        }
        let end = Version {
            ts: self.latest.max(self.version.ts),
            ..self.version
        };
        self.push(end, Vec::new());
    }

    /// A ts-only Transition while a mutation of the session is not visible.
    fn ts_only(&mut self) {
        let Some(pending) = self.pending else {
            return;
        };
        if self.version.ts >= pending {
            self.pending = None;
            return;
        }
        if self.latest <= self.version.ts
            || self
                .last_ts_only
                .is_some_and(|t| t.elapsed() < self.config().ts_only_interval)
        {
            return;
        }
        self.last_ts_only = Some(Instant::now());
        let end = Version {
            ts: self.latest,
            ..self.version
        };
        self.push(end, Vec::new());
    }

    /// Queues the Transition from the session's version to `end` with the
    /// dirty queries' results and `removed`.
    fn push(&mut self, end: Version, removed: Vec<u32>) {
        let mut updates: Vec<pb::QueryUpdate> = removed
            .into_iter()
            .map(|query_id| pb::QueryUpdate {
                query_id,
                update: Some(Update::Removed(Box::default())),
                ..Default::default()
            })
            .collect();
        for q in std::mem::take(&mut self.dirty) {
            if let Some(query) = self.queries.get(&q) {
                updates.push(query.held.update(q));
            }
        }
        let t = pb::Transition {
            session_id: self.id.clone(),
            start: buffa::MessageField::some(self.version.to_proto()),
            end: buffa::MessageField::some(end.to_proto()),
            updates,
            more: false,
            ..Default::default()
        };
        self.version = end;
        if self.pending.is_some_and(|p| end.ts >= p) {
            self.pending = None;
        }
        self.last_push = Instant::now();
        self.outbox.push(t);
    }
}

/// A request's arguments: unset is `null`.
pub fn args_of(args: Option<pb::Value>) -> Result<LiveValue, LiveError> {
    args.map(LiveValue::from_proto)
        .transpose()
        .map(|a| a.unwrap_or(LiveValue::Null))
}

/// The timestamp of a TSO version.
pub(crate) fn ts_of(version: u64) -> Timestamp {
    Timestamp::from_version(version)
}
