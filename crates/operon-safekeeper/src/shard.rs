//! The compio data path of Arm A (§28 §7.2, D263): thread-per-core shards
//! that own timelines.
//!
//! - **Shards.** Each shard is one thread running a compio runtime (io_uring,
//!   optionally with SQPOLL) with its own [`NvmeWalStore`] and journal. A
//!   timeline belongs to shard `hash(timeline) % shards`; the shard count is
//!   fixed at first start (`<data>/SHARDS`).
//! - **The io_uring engine** ([`run_uring_engine`]) is a task on the shard:
//!   it takes the journal's flush units and writes them with compio's
//!   positional writes on `O_DIRECT | O_DSYNC` descriptors, so each write is
//!   one durable I/O (FUA where the device has it), keeping up to `depth` in
//!   flight. Where the device has no FUA it can write, then `fdatasync`
//!   ([`UringSync::Fsync`]). compio 0.19 exposes neither `IO_LINK` nor
//!   registered buffers to safe code, and the workspace forbids `unsafe`, so
//!   the pair is two back-to-back operations and buffers are not registered.
//! - **Connections.** The tokio service reads the startup packet and
//!   authenticates, then hands a `START_WAL_PUSH` socket to the owning shard
//!   ([`Shards::handoff`]); the shard runs the whole push there: reading
//!   proposer messages, the acceptor, journal writes, durable acks. Readers
//!   (the feeder, replication, the HTTP API) stay on tokio and reach the
//!   shards' stores through [`ShardedStore`].

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteAtExt, AsyncWriteExt};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, info, warn};

use crate::acceptor::{Acceptor, Appends, PendingAppends};
use crate::journal::{AlignedBuf, Journal, Unit};
use crate::nvme::NvmeWalStore;
use crate::pgwire;
use crate::proto::{AcceptorMessage, Command, PROTO_VERSION, ProposerElected, ProposerMessage};
use crate::service::{Handoff, MAX_BATCH_BYTES, WalService};
use crate::store::{AppendBatch, Deposed, WalStore};
use crate::types::{AcceptorState, Configuration, Lsn, ServerInfo, Term, TimelineId};
use crate::{Error, journal};

impl compio::buf::IoBuf for AlignedBuf {
    fn as_init(&self) -> &[u8] {
        self
    }
}

/// Events queued between a push's reader, its timers and its loop: a full
/// queue stops the socket being read.
const QUEUE: usize = 256;

/// The longest `START_WAL_PUSH` query accepted.
const MAX_QUERY: usize = 4096;

/// How the io_uring engine makes a unit durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UringSync {
    /// Descriptors opened with `O_DSYNC`: every write is durable (FUA on a
    /// device that has it, write + flush otherwise).
    Dsync,
    /// A plain write, then `fdatasync` (devices without FUA).
    Fsync,
}

/// The shard a timeline belongs to.
pub fn shard_of(tl: &TimelineId, shards: usize) -> usize {
    let b = tl.to_bytes();
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for x in b {
        h ^= u64::from(x);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % shards.max(1) as u64) as usize
}

/// Fix the shard count at first start: `<data>/SHARDS`.
pub fn check_shard_count(data: &Path, shards: usize) -> Result<(), Error> {
    let io =
        |what: &str, e: std::io::Error| Error::Store(format!("{}: {what}: {e}", data.display()));
    let p = data.join("SHARDS");
    match std::fs::read_to_string(&p) {
        Ok(s) if s.trim() == shards.to_string() => Ok(()),
        Ok(s) => Err(Error::Store(format!(
            "{} was created with {} shards, not {shards}",
            data.display(),
            s.trim()
        ))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A data directory used by the tokio layout holds its journal in
            // `journal/`: opening shards there would start from nothing.
            if data.join("journal").exists() {
                return Err(Error::Store(format!(
                    "{} holds a tokio-layout journal; it cannot be opened as shards",
                    data.display()
                )));
            }
            // Create the marker whole or not at all.
            let tmp = data.join("SHARDS.tmp");
            let mut f = std::fs::File::create(&tmp).map_err(|e| io("create SHARDS", e))?;
            std::io::Write::write_all(&mut f, shards.to_string().as_bytes())
                .map_err(|e| io("write SHARDS", e))?;
            f.sync_all().map_err(|e| io("sync SHARDS", e))?;
            std::fs::rename(&tmp, &p).map_err(|e| io("rename SHARDS", e))?;
            std::fs::File::open(data)
                .and_then(|d| d.sync_all())
                .map_err(|e| io("sync the data directory", e))
        }
        Err(e) => Err(io("read SHARDS", e)),
    }
}

/// The io_uring journal engine, on the shard that owns the journal.
pub async fn run_uring_engine(j: Journal, depth: usize, sync: UringSync) {
    let notify = Arc::new(Notify::new());
    {
        let n = notify.clone();
        j.set_waker(Arc::new(move || n.notify_one()));
    }
    let direct = j.direct();
    let mut files: HashMap<u64, Rc<compio::fs::File>> = HashMap::new();
    let mut inflight = FuturesUnordered::new();
    loop {
        if j.is_closed() {
            return;
        }
        while inflight.len() < depth.max(1) {
            let Some(u) = j.try_take() else { break };
            let f = match open_for(&mut files, &u, direct, sync).await {
                Ok(f) => f,
                Err(e) => {
                    j.complete(vec![u], Err(e));
                    continue;
                }
            };
            inflight.push(write_unit(f, u, sync));
        }
        if inflight.is_empty() {
            notify.notified().await;
            continue;
        }
        let next = inflight.next();
        let woke = notify.notified();
        futures::pin_mut!(woke);
        match futures::future::select(next, woke).await {
            futures::future::Either::Left((Some((u, res)), _)) => j.complete(vec![u], res),
            futures::future::Either::Left((None, _)) | futures::future::Either::Right(_) => {}
        }
    }
}

async fn open_for(
    files: &mut HashMap<u64, Rc<compio::fs::File>>,
    u: &Unit,
    direct: bool,
    sync: UringSync,
) -> std::io::Result<Rc<compio::fs::File>> {
    if let Some(f) = files.get(&u.seg.seq) {
        return Ok(f.clone());
    }
    let mut flags = 0;
    if direct {
        flags |= journal::segments::o_direct();
    }
    if sync == UringSync::Dsync {
        flags |= rustix::fs::OFlags::DSYNC.bits() as i32;
    }
    let f = compio::fs::OpenOptions::new()
        .write(true)
        .custom_flags(flags)
        .open(&u.seg.path)
        .await?;
    // Keep the current segment and the previous one.
    if files.len() >= 2 {
        let oldest = files.keys().min().copied();
        if let Some(k) = oldest {
            files.remove(&k);
        }
    }
    let f = Rc::new(f);
    files.insert(u.seg.seq, f.clone());
    Ok(f)
}

async fn write_unit(
    f: Rc<compio::fs::File>,
    u: Unit,
    sync: UringSync,
) -> (Unit, std::io::Result<()>) {
    let Unit { id, seg, off, buf } = u;
    let mut w = &*f;
    let BufResult(mut res, buf) = w.write_all_at(buf, off).await;
    if res.is_ok() && sync == UringSync::Fsync {
        res = f.sync_data().await;
    }
    (Unit { id, seg, off, buf }, res)
}

/// A connection handed to a shard.
#[derive(Debug)]
struct Conn {
    tl: TimelineId,
    stream: std::net::TcpStream,
}

/// Settings of the shards.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub shards: usize,
    pub depth: usize,
    pub sync: UringSync,
    /// Run the io_uring journal engine on the shard (the `uring` tier);
    /// otherwise the journal's own threads write.
    pub uring: bool,
    /// SQPOLL with this idle time.
    pub sqpoll: Option<Duration>,
    /// The acceptor's node id and commit-flush interval (from the service).
    pub node_id: u64,
    pub commit_flush_interval: Duration,
}

/// The running shards.
#[derive(Debug, Clone)]
pub struct Shards {
    txs: Vec<mpsc::UnboundedSender<Conn>>,
}

impl Shards {
    /// Start one thread per store, each running a compio runtime with the
    /// store's io_uring engine; connections come through [`Self::handoff`].
    /// The service is set later ([`Self::serve_with`]), since it is built
    /// over the stores' router.
    pub fn start(
        stores: Vec<Arc<NvmeWalStore>>,
        cfg: ShardConfig,
    ) -> Result<(Shards, ShardStarter), Error> {
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for _ in &stores {
            let (tx, rx) = mpsc::unbounded_channel();
            txs.push(tx);
            rxs.push(rx);
        }
        Ok((Shards { txs }, ShardStarter { stores, rxs, cfg }))
    }

    /// The service's handoff: route a `START_WAL_PUSH` to its shard.
    pub fn handoff(&self) -> Handoff {
        let txs = self.txs.clone();
        Handoff(Arc::new(move |tl, stream| {
            let k = shard_of(&tl, txs.len());
            txs[k]
                .send(Conn { tl, stream })
                .map_err(|_| Error::Io(format!("shard {k} is gone")))
        }))
    }
}

/// The second step of [`Shards::start`]: run the shard threads once the
/// service exists.
#[derive(Debug)]
pub struct ShardStarter {
    stores: Vec<Arc<NvmeWalStore>>,
    rxs: Vec<mpsc::UnboundedReceiver<Conn>>,
    cfg: ShardConfig,
}

impl ShardStarter {
    pub fn run(self, svc: Arc<WalService<ShardedStore>>) -> Result<(), Error> {
        // Shard threads run inside the caller's tokio context, so code they
        // reach that needs a reactor or a spawner (the feeder) finds one.
        let tokio_rt = tokio::runtime::Handle::try_current().ok();
        let mut started = Vec::new();
        for (k, (store, mut rx)) in self.stores.into_iter().zip(self.rxs).enumerate() {
            let cfg = self.cfg.clone();
            let svc = svc.clone();
            let tokio_rt = tokio_rt.clone();
            let (up_tx, up_rx) = std::sync::mpsc::channel::<Result<(), String>>();
            started.push((k, up_rx));
            std::thread::Builder::new()
                .name(format!("wal-shard-{k}"))
                .spawn(move || {
                    let _tokio = tokio_rt.as_ref().map(|h| h.enter());
                    let mut proactor = compio::driver::ProactorBuilder::new();
                    if let Some(idle) = cfg.sqpoll {
                        proactor.sqpoll_idle(idle);
                    }
                    let rt = match compio::runtime::RuntimeBuilder::new()
                        .with_proactor(proactor)
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            warn!(shard = k, error = %e, "could not start the compio runtime");
                            let _ = up_tx.send(Err(e.to_string()));
                            return;
                        }
                    };
                    let _ = up_tx.send(Ok(()));
                    info!(shard = k, driver = ?rt.driver_type(), sqpoll = ?cfg.sqpoll, depth = cfg.depth, sync = ?cfg.sync, "shard started");
                    rt.block_on(async move {
                        if cfg.uring {
                            compio::runtime::spawn(run_uring_engine(
                                store.journal().clone(),
                                cfg.depth,
                                cfg.sync,
                            ))
                            .detach();
                        }
                        while let Some(c) = rx.recv().await {
                            let (svc, store, cfg) = (svc.clone(), store.clone(), cfg.clone());
                            compio::runtime::spawn(async move {
                                if let Err(e) = push(svc, store, cfg, c).await {
                                    debug!(error = %e, "shard WAL push ended");
                                }
                            })
                            .detach();
                        }
                    });
                })
                .map_err(|e| Error::Io(format!("spawn shard {k}: {e}")))?;
        }
        // Report a shard that could not start instead of serving without it.
        for (k, up) in started {
            match up.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(Error::Io(format!("shard {k}: compio runtime: {e}"))),
                Err(_) => return Err(Error::Io(format!("shard {k} exited while starting"))),
            }
        }
        Ok(())
    }
}

/// What the push loop waits on.
#[derive(Debug)]
enum Ev {
    Msg(ProposerMessage),
    Tick,
    Closed,
}

async fn send(wr: &mut compio::net::TcpStream, out: &mut Vec<u8>) -> Result<(), Error> {
    let buf = std::mem::take(out);
    let BufResult(res, mut buf) = wr.write_all(buf).await;
    res.map_err(|e| Error::Io(e.to_string()))?;
    buf.clear();
    *out = buf;
    Ok(())
}

fn put_reply(out: &mut Vec<u8>, msg: &AcceptorMessage) {
    let mut payload = BytesMut::with_capacity(128);
    msg.serialize(&mut payload);
    let mut b = BytesMut::new();
    pgwire::put_copy_data(&mut b, &payload);
    out.extend_from_slice(&b);
}

/// Read tagged messages from the proposer into the event channel.
async fn read_proposer(mut rd: compio::net::TcpStream, tx: mpsc::Sender<Ev>) {
    let mut acc = BytesMut::new();
    let mut chunk = Vec::with_capacity(256 << 10);
    loop {
        // Every complete message in the buffer.
        while acc.len() >= 5 {
            let len = u32::from_be_bytes([acc[1], acc[2], acc[3], acc[4]]) as usize;
            if !(4..=(8 << 20)).contains(&len) {
                let _ = tx.send(Ev::Closed).await;
                return;
            }
            if acc.len() < 1 + len {
                break;
            }
            let tag = acc[0];
            acc.advance(5);
            let body = acc.split_to(len - 4).freeze();
            match tag {
                b'd' => match ProposerMessage::parse(body) {
                    Ok(m) => {
                        if tx.send(Ev::Msg(m)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "bad proposer message");
                        let _ = tx.send(Ev::Closed).await;
                        return;
                    }
                },
                _ => {
                    let _ = tx.send(Ev::Closed).await;
                    return;
                }
            }
        }
        chunk.clear();
        let BufResult(res, c) = rd.read(chunk).await;
        chunk = c;
        match res {
            Ok(0) | Err(_) => {
                let _ = tx.send(Ev::Closed).await;
                return;
            }
            Ok(n) => acc.extend_from_slice(&chunk[..n]),
        }
    }
}

/// Read the `START_WAL_PUSH` query the tokio prelude left on the socket.
async fn read_query(rd: &mut compio::net::TcpStream) -> Result<String, Error> {
    let mut acc = Vec::new();
    loop {
        if acc.len() >= 5 {
            let len = u32::from_be_bytes([acc[1], acc[2], acc[3], acc[4]]) as usize;
            if !(4..=MAX_QUERY).contains(&len) {
                return Err(Error::Protocol("bad START_WAL_PUSH length".into()));
            }
            if acc.len() > len {
                if acc[0] != b'Q' || acc.len() > 1 + len {
                    return Err(Error::Protocol("expected only START_WAL_PUSH".into()));
                }
                let body = &acc[5..];
                let body = body.strip_suffix(b"\0").unwrap_or(body);
                return Ok(String::from_utf8_lossy(body).into_owned());
            }
        }
        // Exactly the missing bytes, so nothing after the query is consumed.
        let want = if acc.len() < 5 {
            5 - acc.len()
        } else {
            1 + u32::from_be_bytes([acc[1], acc[2], acc[3], acc[4]]) as usize - acc.len()
        };
        let BufResult(res, b) = rd.read(Vec::with_capacity(want)).await;
        match res {
            Ok(0) => return Err(Error::Io("closed before START_WAL_PUSH".into())),
            Ok(_) => acc.extend_from_slice(&b),
            Err(e) => return Err(Error::Io(e.to_string())),
        }
    }
}

/// One `START_WAL_PUSH` on its shard: the pipelined loop of the tokio
/// service, on compio.
async fn push(
    svc: Arc<WalService<ShardedStore>>,
    store: Arc<NvmeWalStore>,
    cfg: ShardConfig,
    c: Conn,
) -> Result<(), Error> {
    let _ = c.stream.set_nodelay(true);
    let stream =
        compio::net::TcpStream::from_std(c.stream).map_err(|e| Error::Io(e.to_string()))?;
    let (mut rd, mut wr) = stream.into_split();
    let mut out = Vec::with_capacity(4096);
    let q = read_query(&mut rd).await?;
    let (proto_version, allow) = match Command::parse(&q)? {
        Command::StartWalPush {
            proto_version,
            allow_timeline_creation,
        } => (proto_version, allow_timeline_creation),
        _ => return Err(Error::Protocol(format!("unexpected query on a shard: {q}"))),
    };
    let mut b = BytesMut::new();
    if proto_version != PROTO_VERSION {
        pgwire::put_error(
            &mut b,
            "0A000",
            &format!("safekeeper protocol {proto_version} is not served"),
        );
        out.extend_from_slice(&b);
        return send(&mut wr, &mut out).await;
    }
    pgwire::put_copy_both(&mut b);
    out.extend_from_slice(&b);
    send(&mut wr, &mut out).await?;

    let (tx, mut ev) = mpsc::channel(QUEUE);
    let reader = compio::runtime::spawn(read_proposer(rd, tx.clone()));
    let ttx = tx;
    let interval = cfg.commit_flush_interval;
    let ticker = compio::runtime::spawn(async move {
        loop {
            compio::time::sleep(interval).await;
            if ttx.send(Ev::Tick).await.is_err() {
                return;
            }
        }
    });

    let tl = c.tl;
    let mut session = false;
    let res = async {
        let Some(Ev::Msg(ProposerMessage::Greeting(g))) = ev.recv().await else {
            return Err(Error::Protocol("expected ProposerGreeting first".into()));
        };
        if g.timeline() != tl {
            return Err(Error::Protocol(format!(
                "greeting for {} on a connection for {tl}",
                g.timeline()
            )));
        }
        let (mut acc, reply) = Acceptor::greet(store.clone(), cfg.node_id, &g, allow).await?;
        put_reply(&mut out, &reply);
        send(&mut wr, &mut out).await?;
        session = true;
        svc.session(tl, 1);
        svc.publish(tl, &acc.state());
        svc.ensure_feeder(tl);
        let mut pending: Option<ProposerMessage> = None;
        // Appends in flight, oldest first: issued in LSN order and answered in
        // that order, so every response's flush LSN covers completed writes
        // (the same loop as the tokio service, on this shard).
        let depth = store.max_in_flight().max(1);
        let mut flights: VecDeque<Flight> = VecDeque::new();
        loop {
            let must_land = match &pending {
                Some(ProposerMessage::Append(_)) => flights.len() >= depth,
                Some(_) => !flights.is_empty(),
                None => false,
            };
            if must_land {
                let (f, r) = land(&mut flights).await;
                answer(&svc, tl, &mut acc, f, r, &mut wr, &mut out).await?;
                continue;
            }
            let msg = match pending.take() {
                Some(m) => m,
                None => {
                    let has_flights = !flights.is_empty();
                    let can_take = flights.len() < depth;
                    let landing = async {
                        if !has_flights {
                            std::future::pending::<()>().await;
                        }
                        land(&mut flights).await
                    };
                    let next = async {
                        if !can_take {
                            std::future::pending::<()>().await;
                        }
                        ev.recv().await
                    };
                    futures::pin_mut!(landing, next);
                    match futures::future::select(landing, next).await {
                        futures::future::Either::Left(((f, r), _)) => {
                            answer(&svc, tl, &mut acc, f, r, &mut wr, &mut out).await?;
                            continue;
                        }
                        futures::future::Either::Right((e, _)) => match e {
                            None | Some(Ev::Closed) => break,
                            Some(Ev::Tick) => {
                                acc.refresh().await?;
                                acc.persist_commit_lsn().await?;
                                continue;
                            }
                            Some(Ev::Msg(m)) => m,
                        },
                    }
                }
            };
            match msg {
                ProposerMessage::Greeting(_) => {
                    return Err(Error::Protocol("a second ProposerGreeting".into()));
                }
                ProposerMessage::VoteRequest(v) => {
                    let r = acc.handle_vote(&v).await?;
                    put_reply(&mut out, &r);
                    send(&mut wr, &mut out).await?;
                }
                ProposerMessage::Elected(e) => {
                    acc.handle_elected(&e).await?;
                    if acc.state().term == e.term {
                        svc.tail_truncate(tl, e.start_streaming_at);
                    }
                }
                ProposerMessage::Append(first) => {
                    // Group commit: everything queued behind this one.
                    let term = first.h.term;
                    let mut bytes = first.wal.len();
                    let mut batch = vec![first];
                    let mut closed = false;
                    while bytes < MAX_BATCH_BYTES {
                        match ev.try_recv() {
                            Ok(Ev::Msg(ProposerMessage::Append(a))) if a.h.term == term => {
                                bytes += a.wal.len();
                                batch.push(a);
                            }
                            Ok(Ev::Msg(other)) => {
                                pending = Some(other);
                                break;
                            }
                            Ok(Ev::Tick) => {}
                            Ok(Ev::Closed) | Err(mpsc::error::TryRecvError::Disconnected) => {
                                closed = true;
                                break;
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                        }
                    }
                    acc.observe_term(svc.progress(tl).term);
                    match acc.begin_appends(&batch)? {
                        Appends::Reply(r) => {
                            // Answers stay in order: after the writes in flight.
                            while !flights.is_empty() {
                                let (f, res) = land(&mut flights).await;
                                answer(&svc, tl, &mut acc, f, res, &mut wr, &mut out).await?;
                            }
                            put_reply(&mut out, &r);
                            send(&mut wr, &mut out).await?;
                        }
                        Appends::Pending(p) => {
                            let task = p.batch.clone().map(|b| {
                                let store = store.clone();
                                compio::runtime::spawn(async move { store.append(&tl, &b).await })
                            });
                            flights.push_back(Flight {
                                pending: p,
                                reqs: batch,
                                task,
                            });
                        }
                    }
                    if closed {
                        // The proposer hung up mid-batch: what was issued is
                        // answered below, then the push ends.
                        break;
                    }
                }
            }
            svc.publish(tl, &acc.state());
        }
        while !flights.is_empty() {
            let (f, res) = land(&mut flights).await;
            answer(&svc, tl, &mut acc, f, res, &mut wr, &mut out).await?;
        }
        acc.persist_commit_lsn().await?;
        Ok(())
    }
    .await;
    drop(reader);
    drop(ticker);
    if session {
        svc.session(tl, -1);
    }
    if let Err(e) = &res {
        warn!(%tl, error = %e, "shard WAL push ended");
        let mut b = BytesMut::new();
        pgwire::put_error(&mut b, "XX000", &e.to_string());
        out.clear();
        out.extend_from_slice(&b);
        let _ = send(&mut wr, &mut out).await;
    }
    res
}

type StoreAppend = Result<Result<AcceptorState, Deposed>, Error>;

/// One append between issue and answer.
struct Flight {
    pending: PendingAppends,
    reqs: Vec<crate::proto::AppendRequest>,
    /// The store write; `None` for heartbeats, which land at once.
    task: Option<compio::runtime::JoinHandle<StoreAppend>>,
}

/// Waits for the oldest append in flight and takes it off the queue.
/// Cancel-safe: the queue only changes once the write has completed.
async fn land(flights: &mut VecDeque<Flight>) -> (Flight, Option<StoreAppend>) {
    let res = match flights.front_mut().and_then(|f| f.task.as_mut()) {
        Some(task) => Some(
            task.await
                .unwrap_or_else(|_| Err(Error::Store("append task panicked".into()))),
        ),
        None => None,
    };
    let f = flights
        .pop_front()
        .unwrap_or_else(|| unreachable!("land() on an empty queue"));
    (f, res)
}

/// Answer one landed append: adopt its outcome, refresh the tail readers
/// use, and reply.
async fn answer(
    svc: &Arc<WalService<ShardedStore>>,
    tl: TimelineId,
    acc: &mut Acceptor<NvmeWalStore>,
    f: Flight,
    res: Option<StoreAppend>,
    wr: &mut compio::net::TcpStream,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    let term = f.pending.term();
    let r = acc.finish_appends(f.pending, res).await?;
    if let AcceptorMessage::AppendResponse(resp) = &r
        && resp.term == term
    {
        for a in f.reqs.iter().filter(|a| a.h.end_lsn <= resp.flush_lsn) {
            svc.tail_push(tl, a.h.begin_lsn, a.wal.clone());
        }
    }
    put_reply(out, &r);
    send(wr, out).await?;
    svc.publish(tl, &acc.state());
    Ok(())
}

/// The shards' stores behind one [`WalStore`], routed by timeline: what the
/// tokio side (readers, the feeder, the HTTP API) uses.
#[derive(Debug, Clone)]
pub struct ShardedStore {
    stores: Vec<Arc<NvmeWalStore>>,
}

impl ShardedStore {
    pub fn new(stores: Vec<Arc<NvmeWalStore>>) -> Self {
        Self { stores }
    }

    fn of(&self, tl: &TimelineId) -> &NvmeWalStore {
        &self.stores[shard_of(tl, self.stores.len())]
    }
}

#[async_trait]
impl WalStore for ShardedStore {
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error> {
        self.of(tl).load(tl).await
    }
    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error> {
        self.of(tl).create(tl, server, start_lsn).await
    }
    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error> {
        self.of(tl).update_meta(tl, server, mconf).await
    }
    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error> {
        self.of(tl).vote(tl, term).await
    }
    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        self.of(tl).elected(tl, msg).await
    }
    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        self.of(tl).append(tl, batch).await
    }
    fn max_in_flight(&self) -> usize {
        self.stores.first().map_or(1, |s| s.max_in_flight())
    }
    async fn record_commit_lsn(
        &self,
        tl: &TimelineId,
        term: Term,
        commit_lsn: Lsn,
    ) -> Result<Result<(), Deposed>, Error> {
        self.of(tl).record_commit_lsn(tl, term, commit_lsn).await
    }
    async fn record_backup_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        self.of(tl).record_backup_lsn(tl, lsn).await
    }
    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        self.of(tl).record_remote_consistent_lsn(tl, lsn).await
    }
    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error> {
        self.of(tl).read(tl, from, max_bytes).await
    }
    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error> {
        self.of(tl).trim(tl, lsn).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Id;

    #[test]
    fn timelines_spread_over_shards_and_stay_put() {
        let mut seen = [0usize; 4];
        for i in 0..400u32 {
            let mut t = [0u8; 16];
            t[..4].copy_from_slice(&i.to_le_bytes());
            let tl = TimelineId::new(Id([7; 16]), Id(t));
            let k = shard_of(&tl, 4);
            assert_eq!(k, shard_of(&tl, 4));
            seen[k] += 1;
        }
        assert!(seen.iter().all(|&n| n > 50), "{seen:?}");
    }

    #[test]
    fn the_shard_count_is_fixed_at_first_start() {
        let d = tempfile::tempdir().unwrap();
        check_shard_count(d.path(), 2).unwrap();
        check_shard_count(d.path(), 2).unwrap();
        assert!(check_shard_count(d.path(), 3).is_err());
    }
}
