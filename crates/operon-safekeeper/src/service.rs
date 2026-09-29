//! The WAL service: the safekeeper's Postgres-protocol listener over a
//! [`WalStore`] (§28 §6.3, §6.7, §6.9).
//!
//! - `START_WAL_PUSH` (walproposer): a reader task queues the proposer's
//!   CopyData messages; the acceptor task takes every `AppendRequest` queued
//!   while its previous store write was in flight as one group commit, and
//!   answers with one `AppendResponse`.
//! - `START_REPLICATION` (vanilla protocol): streams WAL up to `commit_lsn`,
//!   or up to `flush_lsn` within a term for walproposer's recovery reader,
//!   and records the pageserver's `remote_consistent_lsn` from its feedback.
//!   The pageserver's *interpreted* protocol (decoded, sharded records) is
//!   refused for now: it needs Neon's `wal_decoder` (Q112).
//! - `IDENTIFY_SYSTEM`, `TIMELINE_STATUS`.
//!
//! The service is stateless: every instance can serve any timeline, and the
//! store's term fence orders concurrent proposers. Readers on the same
//! instance as the proposer are woken by an in-process registry; others poll
//! the store.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::acceptor::Acceptor;
use crate::pgwire::{self, Startup};
use crate::proto::{AcceptorMessage, Command, PROTO_VERSION, ProposerMessage};
use crate::store::WalStore;
use crate::types::{Id, Lsn, NodeId, PageserverFeedback, Term, TimelineId};
use crate::{AcceptorState, Error};

/// Bytes of queued `AppendRequest`s folded into one store write (8 requests
/// of 128 KiB: TiKV's `raft-max-size-per-msg`, §28 §6.5).
pub const MAX_BATCH_BYTES: usize = 1 << 20;
/// Proposer messages queued per connection.
const QUEUE: usize = 256;

/// Settings of a [`WalService`].
#[derive(Clone, Debug)]
pub struct WalServiceConfig {
    /// The node id walproposer sees (the pool's logical id).
    pub node_id: NodeId,
    /// How often a heartbeat-only commit LSN is persisted.
    pub commit_flush_interval: Duration,
    /// Keepalive interval on replication streams.
    pub keepalive_interval: Duration,
    /// How often a reader polls the store when no local proposer wakes it.
    pub poll_interval: Duration,
    /// Feed committed WAL to this stock safekeeper, which serves the
    /// pageserver (the interim path of [`crate::feeder`]).
    pub feeder: Option<crate::feeder::FeederConfig>,
}

impl Default for WalServiceConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            commit_flush_interval: Duration::from_secs(1),
            keepalive_interval: Duration::from_secs(1),
            poll_interval: Duration::from_millis(20),
            feeder: None,
        }
    }
}

/// The in-memory view readers wait on: the proposer's latest state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub term: Term,
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    /// A proposer is streaming to this instance: the fields are current.
    /// Otherwise readers take the state from the store.
    pub active: bool,
}

impl Progress {
    fn of(st: &AcceptorState) -> Self {
        Self {
            term: st.term,
            flush_lsn: st.wal_end(),
            commit_lsn: st.commit_lsn,
            peer_horizon_lsn: st.peer_horizon_lsn,
            active: true,
        }
    }
}

/// The in-memory tail of recently appended WAL (§28 §6.7, source 1), so that
/// readers on this instance do not go to the store for WAL just written.
#[derive(Debug, Default)]
struct Tail {
    chunks: std::collections::VecDeque<(Lsn, bytes::Bytes)>,
    bytes: usize,
}

/// The tail keeps at most this much WAL per timeline.
const TAIL_BYTES: usize = 64 << 20;

impl Tail {
    fn end(&self) -> Option<Lsn> {
        self.chunks.back().map(|(l, b)| Lsn(l.0 + b.len() as u64))
    }

    fn push(&mut self, begin: Lsn, data: bytes::Bytes) {
        if data.is_empty() {
            return;
        }
        match self.end() {
            Some(end) if end == begin => {}
            Some(end) if end > begin => {
                // A retried prefix: keep what is new.
                let skip = (end.0 - begin.0) as usize;
                if skip >= data.len() {
                    return;
                }
                return self.push(end, data.slice(skip..));
            }
            _ => {
                self.chunks.clear();
                self.bytes = 0;
            }
        }
        self.bytes += data.len();
        self.chunks.push_back((begin, data));
        while self.bytes > TAIL_BYTES {
            match self.chunks.pop_front() {
                Some((_, b)) => self.bytes -= b.len(),
                None => break,
            }
        }
    }

    fn truncate(&mut self, at: Lsn) {
        while let Some((l, b)) = self.chunks.back_mut() {
            if *l >= at {
                self.bytes -= b.len();
                self.chunks.pop_back();
            } else {
                let keep = (at.0 - l.0) as usize;
                if keep < b.len() {
                    self.bytes -= b.len() - keep;
                    *b = b.slice(..keep);
                }
                break;
            }
        }
    }

    /// WAL from `from`, if the tail covers it.
    fn read(&self, from: Lsn, max: usize) -> Option<Vec<(Lsn, bytes::Bytes)>> {
        let (first, _) = self.chunks.front()?;
        if from < *first || Some(from) > self.end() {
            return None;
        }
        let mut out = Vec::new();
        let mut at = from.0;
        let mut budget = max;
        for (l, b) in &self.chunks {
            let end = l.0 + b.len() as u64;
            if end <= at {
                continue;
            }
            if budget == 0 {
                break;
            }
            let lo = (at - l.0) as usize;
            let hi = b.len().min(lo.saturating_add(budget));
            out.push((Lsn(at), b.slice(lo..hi)));
            budget -= hi - lo;
            at = l.0 + hi as u64;
        }
        Some(out)
    }
}

#[derive(Debug, Default)]
struct Registry {
    timelines: Mutex<HashMap<TimelineId, watch::Sender<Progress>>>,
    sessions: Mutex<HashMap<TimelineId, u32>>,
    tails: Mutex<HashMap<TimelineId, Tail>>,
    fed: Mutex<std::collections::HashSet<TimelineId>>,
}

impl Registry {
    fn sender(&self, tl: TimelineId) -> watch::Sender<Progress> {
        let mut m = self.timelines.lock().unwrap_or_else(|p| p.into_inner());
        m.entry(tl)
            .or_insert_with(|| watch::channel(Progress::default()).0)
            .clone()
    }

    fn publish(&self, tl: TimelineId, p: Progress) {
        self.sender(tl).send_if_modified(|cur| {
            if p.term < cur.term {
                return false;
            }
            let next = Progress {
                commit_lsn: if p.term == cur.term {
                    cur.commit_lsn.max(p.commit_lsn)
                } else {
                    p.commit_lsn
                },
                active: cur.active,
                ..p
            };
            let changed = next != *cur;
            *cur = next;
            changed
        });
    }

    /// A proposer session starts (`+1`) or ends (`-1`) on this instance.
    fn session(&self, tl: TimelineId, delta: i32) {
        let n = {
            let mut m = self.sessions.lock().unwrap_or_else(|p| p.into_inner());
            let n = m.entry(tl).or_default();
            *n = n.saturating_add_signed(delta);
            *n
        };
        self.sender(tl).send_if_modified(|cur| {
            let active = n > 0;
            let changed = cur.active != active;
            cur.active = active;
            changed
        });
    }

    fn tail<T>(&self, tl: TimelineId, f: impl FnOnce(&mut Tail) -> T) -> T {
        let mut m = self.tails.lock().unwrap_or_else(|p| p.into_inner());
        f(m.entry(tl).or_default())
    }
}

/// The WAL service over a store.
#[derive(Debug)]
pub struct WalService<S> {
    store: Arc<S>,
    config: WalServiceConfig,
    registry: Registry,
}

impl<S: WalStore> WalService<S> {
    pub fn new(store: Arc<S>, config: WalServiceConfig) -> Arc<Self> {
        Arc::new(Self {
            store,
            config,
            registry: Registry::default(),
        })
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn config(&self) -> &WalServiceConfig {
        &self.config
    }

    /// The latest progress this instance has seen for `tl`.
    pub fn progress(&self, tl: TimelineId) -> Progress {
        *self.registry.sender(tl).borrow()
    }

    /// WAL from `from` (at most `max` bytes): from the in-memory tail when
    /// it covers `from`, else from the store.
    pub async fn read_wal(
        &self,
        tl: TimelineId,
        from: Lsn,
        max: usize,
    ) -> Result<Vec<(Lsn, bytes::Bytes)>, Error> {
        if let Some(out) = self.registry.tail(tl, |t| t.read(from, max)) {
            return Ok(out);
        }
        self.store.read(&tl, from, max).await
    }

    /// The timeline's progress: this instance's in-memory view while a
    /// proposer streams here, else the store's head.
    pub async fn current(&self, tl: TimelineId) -> Result<Progress, Error> {
        let local = self.progress(tl);
        if local.active {
            return Ok(local);
        }
        let st = self.store.load(&tl).await?.ok_or(Error::NotFound(tl))?;
        Ok(Progress {
            active: false,
            ..Progress::of(&st)
        })
    }

    /// Wakes on every change of this instance's view of `tl`.
    pub fn subscribe(&self, tl: TimelineId) -> watch::Receiver<Progress> {
        self.registry.sender(tl).subscribe()
    }

    /// Start the feeder for `tl`, once per process.
    pub fn ensure_feeder(self: &Arc<Self>, tl: TimelineId) {
        let Some(cfg) = self.config.feeder.clone() else {
            return;
        };
        let mut fed = self.registry.fed.lock().unwrap_or_else(|p| p.into_inner());
        if fed.insert(tl) {
            tokio::spawn(crate::feeder::run(self.clone(), tl, cfg));
        }
    }

    /// Accept connections until `shutdown` resolves.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error> {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                acc = listener.accept() => {
                    let (stream, peer) = acc.map_err(|e| Error::Io(e.to_string()))?;
                    let svc = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = svc.handle(stream).await {
                            debug!(%peer, error = %e, "connection ended");
                        }
                    });
                }
            }
        }
    }

    async fn handle(self: Arc<Self>, stream: TcpStream) -> Result<(), Error> {
        let _ = stream.set_nodelay(true);
        let (rd, wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);
        let Some(startup) = pgwire::read_startup(&mut rd, &mut wr).await? else {
            return Ok(());
        };
        let tl = timeline_of(&startup)?;
        let mut buf = BytesMut::new();
        pgwire::put_login_ok(&mut buf);
        send(&mut wr, &mut buf).await?;

        loop {
            let Some((tag, body)) = pgwire::read_message(&mut rd).await? else {
                return Ok(());
            };
            match tag {
                b'X' => return Ok(()),
                b'Q' => {}
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected message {:?}",
                        other as char
                    )));
                }
            }
            let q = String::from_utf8_lossy(body.strip_suffix(b"\0").unwrap_or(&body)).into_owned();
            let cmd = match Command::parse(&q) {
                Ok(c) => c,
                Err(e) => {
                    pgwire::put_error(&mut buf, "0A000", &e.to_string());
                    pgwire::put_ready(&mut buf);
                    send(&mut wr, &mut buf).await?;
                    continue;
                }
            };
            let res = match cmd {
                Command::SetDatestyle => {
                    pgwire::put_command_complete(&mut buf, "SELECT 1");
                    Ok(())
                }
                Command::IdentifySystem => self.identify_system(tl, &mut buf).await,
                Command::TimelineStatus => self.timeline_status(tl, &mut buf).await,
                Command::StartWalPush {
                    proto_version,
                    allow_timeline_creation,
                } => {
                    // Takes over the connection until the stream ends.
                    return self
                        .wal_push(tl, proto_version, allow_timeline_creation, rd, wr)
                        .await;
                }
                Command::StartReplication { start_lsn, term } => {
                    let tl = tl.ok_or_else(|| Error::Protocol("no timeline_id".into()))?;
                    let protocol = startup.options().get("protocol").cloned();
                    if protocol
                        .as_deref()
                        .is_some_and(|p| p.contains("interpreted"))
                    {
                        Err(Error::Protocol(
                            "the interpreted WAL protocol is not served yet (§28 Q112)".into(),
                        ))
                    } else {
                        return self.replicate(tl, start_lsn, term, rd, wr).await;
                    }
                }
            };
            if let Err(e) = res {
                pgwire::put_error(&mut buf, "XX000", &e.to_string());
            }
            pgwire::put_ready(&mut buf);
            send(&mut wr, &mut buf).await?;
        }
    }

    async fn identify_system(
        &self,
        tl: Option<TimelineId>,
        buf: &mut BytesMut,
    ) -> Result<(), Error> {
        let tl = tl.ok_or_else(|| Error::Protocol("no timeline_id".into()))?;
        let st = self.store.load(&tl).await?.ok_or(Error::NotFound(tl))?;
        let commit = st.commit_lsn.max(self.progress(tl).commit_lsn);
        pgwire::put_row_description(
            buf,
            &["systemid", "timeline", "xlogpos", "dbname"],
            &["timeline"],
        );
        let sysid = st.server.system_id.to_string();
        let lsn = commit.to_string();
        pgwire::put_data_row(buf, &[Some(&sysid), Some("1"), Some(&lsn), None]);
        pgwire::put_command_complete(buf, "IDENTIFY_SYSTEM");
        Ok(())
    }

    async fn timeline_status(
        &self,
        tl: Option<TimelineId>,
        buf: &mut BytesMut,
    ) -> Result<(), Error> {
        let tl = tl.ok_or_else(|| Error::Protocol("no timeline_id".into()))?;
        pgwire::put_row_description(buf, &["flush_lsn", "commit_lsn"], &[]);
        if let Some(st) = self.store.load(&tl).await? {
            let commit = st.commit_lsn.max(self.progress(tl).commit_lsn);
            pgwire::put_data_row(
                buf,
                &[Some(&st.wal_end().to_string()), Some(&commit.to_string())],
            );
        }
        pgwire::put_command_complete(buf, "TIMELINE_STATUS");
        Ok(())
    }

    async fn wal_push<R, W>(
        self: Arc<Self>,
        tl: Option<TimelineId>,
        proto_version: u32,
        allow_timeline_creation: bool,
        rd: R,
        mut wr: W,
    ) -> Result<(), Error>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let mut buf = BytesMut::new();
        if proto_version != PROTO_VERSION {
            pgwire::put_error(
                &mut buf,
                "0A000",
                &format!(
                    "safekeeper protocol {proto_version} is not served; set neon.safekeeper_proto_version = 3"
                ),
            );
            send(&mut wr, &mut buf).await?;
            return Ok(());
        }
        pgwire::put_copy_both(&mut buf);
        send(&mut wr, &mut buf).await?;

        let (tx, mut rx) = mpsc::channel::<ProposerMessage>(QUEUE);
        let reader = tokio::spawn(read_proposer(rd, tx));
        let mut session: Option<TimelineId> = None;

        let res = async {
            let Some(ProposerMessage::Greeting(g)) = rx.recv().await else {
                return Err(Error::Protocol("expected ProposerGreeting first".into()));
            };
            if let Some(tl) = tl
                && tl != g.timeline()
            {
                return Err(Error::Protocol(format!(
                    "greeting for {} on a connection for {tl}",
                    g.timeline()
                )));
            }
            let (mut acc, reply) = Acceptor::greet(
                self.store.clone(),
                self.config.node_id,
                &g,
                allow_timeline_creation,
            )
            .await?;
            reply_to(&mut wr, &mut buf, &reply).await?;
            let tl = acc.timeline();
            session = Some(tl);
            self.registry.session(tl, 1);
            self.registry.publish(tl, Progress::of(&acc.state()));
            self.ensure_feeder(tl);

            let mut tick = tokio::time::interval(self.config.commit_flush_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut pending: Option<ProposerMessage> = None;
            let mut stats = PushStats::default();
            loop {
                let msg = match pending.take() {
                    Some(m) => m,
                    None => tokio::select! {
                        m = rx.recv() => match m {
                            Some(m) => m,
                            None => break,
                        },
                        _ = tick.tick() => {
                            acc.refresh().await?;
                            acc.persist_commit_lsn().await?;
                            continue;
                        }
                    },
                };
                match msg {
                    ProposerMessage::Greeting(_) => {
                        return Err(Error::Protocol("a second ProposerGreeting".into()));
                    }
                    ProposerMessage::VoteRequest(v) => {
                        let r = acc.handle_vote(&v).await?;
                        reply_to(&mut wr, &mut buf, &r).await?;
                    }
                    ProposerMessage::Elected(e) => {
                        acc.handle_elected(&e).await?;
                        if acc.state().term == e.term {
                            self.registry.tail(tl, |t| t.truncate(e.start_streaming_at));
                        }
                    }
                    ProposerMessage::Append(first) => {
                        // Group commit: everything queued behind this one.
                        let term = first.h.term;
                        let mut bytes = first.wal.len();
                        let mut batch = vec![first];
                        while bytes < MAX_BATCH_BYTES {
                            match rx.try_recv() {
                                Ok(ProposerMessage::Append(a)) if a.h.term == term => {
                                    bytes += a.wal.len();
                                    batch.push(a);
                                }
                                Ok(other) => {
                                    pending = Some(other);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        let t0 = std::time::Instant::now();
                        acc.observe_term(self.progress(tl).term);
                        let r = acc.handle_appends(&batch).await?;
                        if let AcceptorMessage::AppendResponse(resp) = &r
                            && resp.term == term
                        {
                            self.registry.tail(tl, |t| {
                                for a in batch.iter().filter(|a| a.h.end_lsn <= resp.flush_lsn) {
                                    t.push(a.h.begin_lsn, a.wal.clone());
                                }
                            });
                        }
                        reply_to(&mut wr, &mut buf, &r).await?;
                        if bytes > 0 {
                            stats.record(tl, batch.len(), bytes, t0.elapsed());
                        }
                    }
                }
                self.registry.publish(tl, Progress::of(&acc.state()));
            }
            acc.persist_commit_lsn().await?;
            Ok(())
        }
        .await;
        reader.abort();
        if let Some(tl) = session {
            self.registry.session(tl, -1);
        }
        if let Err(e) = &res {
            warn!(error = %e, "WAL push ended");
            pgwire::put_error(&mut buf, "XX000", &e.to_string());
            let _ = send(&mut wr, &mut buf).await;
        }
        res
    }

    async fn replicate<R, W>(
        self: Arc<Self>,
        tl: TimelineId,
        start_lsn: Lsn,
        term: Option<Term>,
        rd: R,
        mut wr: W,
    ) -> Result<(), Error>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let mut buf = BytesMut::new();
        let head = self.store.load(&tl).await?.ok_or(Error::NotFound(tl))?;
        if start_lsn < head.trimmed_lsn {
            return Err(Error::Trimmed {
                from: start_lsn,
                trimmed: head.trimmed_lsn,
            });
        }
        pgwire::put_copy_both(&mut buf);
        send(&mut wr, &mut buf).await?;
        info!(%tl, start = %start_lsn, ?term, "replication started");

        let (fb_tx, mut fb_rx) = mpsc::channel::<Lsn>(16);
        let reader = tokio::spawn(read_feedback(rd, fb_tx));
        let mut watch = self.registry.sender(tl).subscribe();
        let mut keepalive = tokio::time::interval(self.config.keepalive_interval);
        let mut at = start_lsn;
        let mut last_fb_write = tokio::time::Instant::now();
        let mut pending_fb: Option<Lsn> = None;

        let res: Result<(), Error> = async {
            loop {
                // The readable end: committed WAL, or the term's WAL for the
                // compute's recovery reader.
                watch.borrow_and_update();
                let st = self.current(tl).await?;
                let end = match term {
                    None => st.commit_lsn.min(st.flush_lsn),
                    Some(t) if st.term == t => st.flush_lsn,
                    Some(_) => return Ok(()), // the term moved on: done
                };
                if at < end {
                    for (lsn, bytes) in self.read_wal(tl, at, MAX_BATCH_BYTES).await? {
                        let take = bytes.len().min(end.0.saturating_sub(lsn.0) as usize);
                        if take == 0 {
                            break;
                        }
                        let mut msg = BytesMut::with_capacity(25 + take);
                        msg.put_u8(b'w');
                        msg.put_u64(lsn.0);
                        msg.put_u64(end.0);
                        msg.put_i64(pgwire::pg_now_us());
                        msg.put_slice(&bytes[..take]);
                        pgwire::put_copy_data(&mut buf, &msg);
                        at = Lsn(lsn.0 + take as u64);
                    }
                    send(&mut wr, &mut buf).await?;
                    continue;
                }
                tokio::select! {
                    _ = watch.changed() => {}
                    _ = tokio::time::sleep(self.config.poll_interval) => {}
                    _ = keepalive.tick() => {
                        let mut msg = BytesMut::with_capacity(18);
                        msg.put_u8(b'k');
                        msg.put_u64(end.0);
                        msg.put_i64(pgwire::pg_now_us());
                        msg.put_u8(0);
                        pgwire::put_copy_data(&mut buf, &msg);
                        send(&mut wr, &mut buf).await?;
                    }
                    fb = fb_rx.recv() => match fb {
                        Some(lsn) => pending_fb = Some(pending_fb.map_or(lsn, |p| p.max(lsn))),
                        None => return Ok(()), // the reader hung up
                    },
                }
                // remote_consistent_lsn, coalesced to one write a second.
                if let Some(lsn) = pending_fb
                    && last_fb_write.elapsed() >= Duration::from_secs(1)
                {
                    self.store.record_remote_consistent_lsn(&tl, lsn).await?;
                    pending_fb = None;
                    last_fb_write = tokio::time::Instant::now();
                }
            }
        }
        .await;
        reader.abort();
        if let Some(lsn) = pending_fb {
            let _ = self.store.record_remote_consistent_lsn(&tl, lsn).await;
        }
        res
    }
}

/// Append latency on one WAL push stream, logged every 10 s.
#[derive(Debug, Default)]
struct PushStats {
    since: Option<std::time::Instant>,
    micros: Vec<u64>,
    requests: usize,
    bytes: usize,
}

impl PushStats {
    fn record(&mut self, tl: TimelineId, requests: usize, bytes: usize, took: Duration) {
        let since = *self.since.get_or_insert_with(std::time::Instant::now);
        self.micros.push(took.as_micros() as u64);
        self.requests += requests;
        self.bytes += bytes;
        if since.elapsed() >= Duration::from_secs(10) {
            self.micros.sort_unstable();
            let q = |f: f64| {
                self.micros[((self.micros.len() as f64 * f) as usize).min(self.micros.len() - 1)]
            };
            info!(
                %tl,
                writes = self.micros.len(),
                requests = self.requests,
                kib = self.bytes / 1024,
                p50_us = q(0.5),
                p99_us = q(0.99),
                max_us = q(1.0),
                "append latency (store write + ack)"
            );
            *self = PushStats::default();
        }
    }
}

async fn send<W: AsyncWrite + Unpin>(wr: &mut W, buf: &mut BytesMut) -> Result<(), Error> {
    wr.write_all(buf)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    wr.flush().await.map_err(|e| Error::Io(e.to_string()))?;
    buf.clear();
    Ok(())
}

async fn reply_to<W: AsyncWrite + Unpin>(
    wr: &mut W,
    buf: &mut BytesMut,
    msg: &AcceptorMessage,
) -> Result<(), Error> {
    let mut payload = BytesMut::with_capacity(128);
    msg.serialize(&mut payload);
    pgwire::put_copy_data(buf, &payload);
    send(wr, buf).await
}

/// The proposer's CopyData stream into the queue; ends on CopyDone, EOF or
/// a malformed message (the connection is then dropped).
async fn read_proposer<R: AsyncRead + Unpin>(mut rd: R, tx: mpsc::Sender<ProposerMessage>) {
    loop {
        match pgwire::read_message(&mut rd).await {
            Ok(Some((b'd', body))) => match ProposerMessage::parse(body) {
                Ok(m) => {
                    if tx.send(m).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "bad proposer message");
                    return;
                }
            },
            Ok(Some((b'c', _))) | Ok(Some((b'X', _))) | Ok(None) => return,
            Ok(Some((tag, _))) => {
                warn!(tag = %(tag as char), "unexpected message in WAL push");
                return;
            }
            Err(e) => {
                debug!(error = %e, "proposer connection closed");
                return;
            }
        }
    }
}

/// Replication feedback: `'r'` standby status, `'h'` hot standby, and the
/// pageserver's `'z'` status update, whose `remote_consistent_lsn` is sent on.
async fn read_feedback<R: AsyncRead + Unpin>(mut rd: R, tx: mpsc::Sender<Lsn>) {
    loop {
        match pgwire::read_message(&mut rd).await {
            Ok(Some((b'd', body))) => match body.first() {
                Some(b'z') if body.len() > 9 => {
                    if let Ok(fb) = PageserverFeedback::parse(body.slice(9..))
                        && tx.send(fb.remote_consistent_lsn).await.is_err()
                    {
                        return;
                    }
                }
                Some(b'r') | Some(b'h') => {}
                _ => debug!("unexpected replication feedback"),
            },
            Ok(Some((b'c', _))) | Ok(Some((b'X', _))) | Ok(None) | Err(_) => return,
            Ok(Some(_)) => {}
        }
    }
}

/// The timeline named in the startup options (`tenant_id` / `ztenantid`,
/// `timeline_id` / `ztimelineid`).
fn timeline_of(startup: &Startup) -> Result<Option<TimelineId>, Error> {
    let opts = startup.options();
    let get = |a: &str, b: &str| opts.get(a).or_else(|| opts.get(b)).cloned();
    match (
        get("tenant_id", "ztenantid"),
        get("timeline_id", "ztimelineid"),
    ) {
        (Some(t), Some(l)) => Ok(Some(TimelineId::new(t.parse::<Id>()?, l.parse::<Id>()?))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn b(s: &'static [u8]) -> Bytes {
        Bytes::from_static(s)
    }

    fn flat(v: Option<Vec<(Lsn, Bytes)>>) -> Option<Vec<u8>> {
        v.map(|v| v.into_iter().flat_map(|(_, b)| b.to_vec()).collect())
    }

    #[test]
    fn tail_reads_truncates_and_skips_retries() {
        let mut t = Tail::default();
        t.push(Lsn(10), b(b"abc"));
        t.push(Lsn(13), b(b"def"));
        t.push(Lsn(12), b(b"cdefg")); // retried prefix
        assert_eq!(flat(t.read(Lsn(11), usize::MAX)).unwrap(), b"bcdefg");
        assert_eq!(flat(t.read(Lsn(11), 3)).unwrap(), b"bcd");
        assert_eq!(flat(t.read(Lsn(17), 10)).unwrap(), b"");
        assert!(t.read(Lsn(9), 10).is_none());
        assert!(t.read(Lsn(18), 10).is_none());
        t.truncate(Lsn(14));
        assert_eq!(flat(t.read(Lsn(10), usize::MAX)).unwrap(), b"abcd");
        // A gap resets the tail.
        t.push(Lsn(100), b(b"z"));
        assert!(t.read(Lsn(10), 1).is_none());
        assert_eq!(flat(t.read(Lsn(100), 1)).unwrap(), b"z");
    }

    #[test]
    fn registry_progress_follows_terms_and_sessions() {
        let r = Registry::default();
        let tl = TimelineId::default();
        let p = |term, flush, commit| Progress {
            term,
            flush_lsn: Lsn(flush),
            commit_lsn: Lsn(commit),
            peer_horizon_lsn: Lsn(0),
            active: true,
        };
        r.session(tl, 1);
        r.publish(tl, p(1, 10, 5));
        r.publish(tl, p(1, 12, 4)); // commit never goes back within a term
        assert_eq!(*r.sender(tl).borrow(), p(1, 12, 5));
        r.publish(tl, p(0, 99, 99)); // an older term is ignored
        assert_eq!(r.sender(tl).borrow().flush_lsn, Lsn(12));
        r.publish(tl, p(2, 8, 3)); // a new term replaces the view
        assert_eq!(*r.sender(tl).borrow(), p(2, 8, 3));
        r.session(tl, -1);
        assert!(!r.sender(tl).borrow().active);
    }
}
