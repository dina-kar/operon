//! The raw TiKV [`WalStore`]: blind, pipelined WAL appends fenced once per
//! election (§28 §7.3).
//!
//! The transactional store ([`crate::tikv`]) pays, per append, a TSO round
//! trip, a read of the head and a 1PC prewrite, and it serialises every
//! append of a timeline on the head key. This store takes the fence off the
//! write path:
//!
//! - **WAL chunks are blind RawKV puts** under term-tagged keys
//!   `root ‖ tl ‖ 'W' ‖ writer term ‖ begin LSN`. No TSO, no read before the
//!   write, and no key shared between appends, so many appends of one
//!   timeline can be in flight at once ([`WalStore::max_in_flight`]).
//! - **The head** (`root ‖ tl ‖ 'H'`) holds the acceptor state plus the
//!   *segments*: which writer term's keys hold which LSN range. It changes
//!   only by compare-and-swap, at votes, elections and the periodic
//!   bookkeeping writes (commit, backup and pageserver LSNs, trims).
//! - **The fence** is a read of the head *after* the put completed: the
//!   append is acknowledged only if the head still names the writer's term.
//!   A vote first swaps the new term into the head and only *then* scans for
//!   the WAL end it reports. So if the check read the old term, it ran before
//!   the vote's swap, the put completed before the check, and the vote's scan
//!   sees the put: every acknowledged byte is inside the WAL the new
//!   proposer adopts. A put that lands after the scan is never acknowledged
//!   (its check sees the new term) and lies above the point where the new
//!   head's segments end that writer's range, so readers never return it.
//!   No timing assumption, no lease.
//! - **Readers** follow the segments: for each LSN they read only the keys of
//!   the writer term whose segment covers it, stitch chunks, and stop at the
//!   first hole. Stale-term keys are garbage, deleted by [`WalStore::trim`].
//!
//! The acknowledged WAL end is the *contiguous* end of completed appends, so
//! out-of-order completions never acknowledge a hole. The durable head keeps
//! only a lower bound of the WAL end (`flush_lsn`, advanced with the commit
//! LSN); [`WalStore::load`] and votes find the true end by scanning the last
//! segment from there.
//!
//! [`RawKv`] is the slice of TiKV's RawKV API the store needs;
//! [`TikvRawKv`] (feature `tikv`) implements it on `tikv-client`, with
//! blind puts on a plain client and compare-and-swap on an atomic-mode
//! client (TiKV requires that the two never touch the same key: they do
//! not, the head is only ever swapped). [`MemRawKv`] is a linearizable
//! in-memory model for the tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::proto::ProposerElected;
use crate::store::{
    AppendBatch, Deposed, WalStore, apply_commit_lsn, apply_elected, apply_vote, trim_bound,
};
use crate::types::{AcceptorState, Configuration, Lsn, ServerInfo, Term, TimelineId};

/// The largest WAL value written: one walproposer `AppendRequest`
/// (`MAX_SEND_SIZE`). Longer chunks are split, so a reader finds the chunk
/// covering an LSN within this distance below it.
pub const MAX_CHUNK: usize = 128 * 1024;

/// Appends in flight per timeline by default (TiKV's store writer batches
/// their Raft log writes into shared fsyncs).
pub const DEFAULT_PIPELINE_DEPTH: usize = 8;

const RAW_HEAD_VERSION: u8 = 2;
/// Chunks per scan page (at most 8 MiB of values).
const SCAN_PAGE: u32 = 64;
/// Head swaps retried per call before giving up (each retry re-reads).
const CAS_RETRIES: u32 = 64;

/// The RawKV operations of the store. Every operation is linearizable, as
/// TiKV's are on the region leader.
#[async_trait]
pub trait RawKv: Send + Sync + 'static {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error>;
    /// Blind puts (non-atomic mode).
    async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error>;
    /// Sets `key` to `new` iff its value is `expected`; returns the value
    /// before the call and whether it was swapped.
    async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, bool), Error>;
    /// Pairs in `[from, to)`, ascending, at most `limit`.
    async fn scan(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error>;
    async fn delete_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), Error>;
    /// Ask for region boundaries at `keys` (best effort; the default does
    /// nothing).
    async fn pre_split(&self, _keys: Vec<Vec<u8>>) -> Result<(), Error> {
        Ok(())
    }
}

/// A writer term's LSN range: from `start` to the next segment's start (the
/// last one: to the WAL end).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub term: Term,
    pub start: Lsn,
}

/// The durable head of a timeline.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawHead {
    /// The acceptor state; `flush_lsn` is a lower bound of the WAL end.
    pub state: AcceptorState,
    /// Ascending by `start` and by `term`.
    pub segments: Vec<Segment>,
}

impl RawHead {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = vec![RAW_HEAD_VERSION];
        out.extend(
            postcard::to_stdvec(self).map_err(|e| Error::Store(format!("encode head: {e}")))?,
        );
        Ok(out)
    }

    fn decode(v: &[u8]) -> Result<Self, Error> {
        match v.split_first() {
            Some((&RAW_HEAD_VERSION, rest)) => {
                postcard::from_bytes(rest).map_err(|e| Error::Store(format!("decode head: {e}")))
            }
            _ => Err(Error::Store("unknown raw head version".into())),
        }
    }

    /// `(term, start, end)` of every segment; the last one ends at `end`.
    fn ranges(&self, end: Lsn) -> impl Iterator<Item = (Term, Lsn, Lsn)> + '_ {
        self.segments.iter().enumerate().map(move |(i, s)| {
            let to = self.segments.get(i + 1).map_or(end, |n| n.start);
            (s.term, s.start, to.max(s.start))
        })
    }
}

/// Stitch `chunks` (one writer term, ascending by begin, possibly
/// overlapping with identical bytes) from `at`, up to `end` and `budget`
/// bytes; stops at the first hole. Returns the pieces and where it stopped.
fn stitch(
    chunks: &[(u64, Bytes)],
    mut at: u64,
    end: u64,
    mut budget: u64,
) -> (Vec<(Lsn, Bytes)>, u64) {
    let mut out = Vec::new();
    for (begin, c) in chunks {
        if at >= end || budget == 0 {
            break;
        }
        let c_end = (begin + c.len() as u64).min(end);
        if c_end <= at {
            continue;
        }
        if *begin > at {
            break; // a hole
        }
        let lo = at - begin;
        let hi = (c_end - begin).min(lo.saturating_add(budget));
        out.push((Lsn(at), c.slice(lo as usize..hi as usize)));
        budget -= hi - lo;
        at = begin + hi;
    }
    (out, at)
}

/// Completed appends of the current writer term on this instance.
#[derive(Debug)]
struct Tracker {
    term: Term,
    /// Every byte below is durable (contiguous from the segment start).
    contiguous: u64,
    /// Completed ranges above `contiguous`: begin → end.
    done: BTreeMap<u64, u64>,
    /// The in-memory commit and peer horizon LSNs (persisted by
    /// `record_commit_lsn`).
    commit_lsn: Lsn,
    peer_horizon_lsn: Lsn,
}

impl Tracker {
    fn complete(&mut self, begin: u64, end: u64) {
        if end <= self.contiguous {
            return;
        }
        let e = self.done.entry(begin).or_insert(end);
        *e = (*e).max(end);
        while let Some((&b, &e)) = self.done.first_key_value() {
            if b > self.contiguous {
                break;
            }
            self.done.pop_first();
            self.contiguous = self.contiguous.max(e);
        }
    }
}

#[derive(Debug, Default)]
struct TlCache {
    /// The head as last read or written: bytes (the swap's expected value)
    /// and decoded.
    head: Option<(Vec<u8>, RawHead)>,
    tracker: Option<Tracker>,
}

/// What a head update decided.
enum Step<T> {
    /// Leave the head as it is.
    Keep(T),
    /// Swap in the mutated head.
    Write(T),
}

/// The raw TiKV-backed [`WalStore`] over any [`RawKv`].
#[derive(Debug)]
pub struct RawWalStore<K> {
    kv: Arc<K>,
    root: Vec<u8>,
    depth: usize,
    cache: Mutex<HashMap<TimelineId, TlCache>>,
}

impl<K: RawKv> RawWalStore<K> {
    /// A store whose keys live under `root`, with up to `depth` appends in
    /// flight per timeline.
    pub fn new(kv: Arc<K>, root: Vec<u8>, depth: usize) -> Self {
        Self {
            kv,
            root,
            depth: depth.max(1),
            cache: Mutex::default(),
        }
    }

    pub fn kv(&self) -> &Arc<K> {
        &self.kv
    }

    fn prefix(&self, tl: &TimelineId) -> Vec<u8> {
        let mut k = Vec::with_capacity(self.root.len() + 50);
        k.extend_from_slice(&self.root);
        k.extend_from_slice(&tl.to_bytes());
        k
    }

    fn head_key(&self, tl: &TimelineId) -> Vec<u8> {
        let mut k = self.prefix(tl);
        k.push(b'H');
        k
    }

    fn wal_key(&self, tl: &TimelineId, term: Term, lsn: u64) -> Vec<u8> {
        let mut k = self.prefix(tl);
        k.push(b'W');
        k.extend_from_slice(&term.to_be_bytes());
        k.extend_from_slice(&lsn.to_be_bytes());
        k
    }

    fn wal_lsn(&self, key: &[u8]) -> Result<u64, Error> {
        let at = self.root.len() + 32 + 1 + 8;
        key.get(at..at + 8)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| Error::Store("malformed WAL key".into()))
    }

    fn with_cache<T>(&self, tl: &TimelineId, f: impl FnOnce(&mut TlCache) -> T) -> T {
        let mut m = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        f(m.entry(*tl).or_default())
    }

    fn remember(&self, tl: &TimelineId, bytes: Vec<u8>, head: RawHead) {
        self.with_cache(tl, |c| {
            // Terms only grow: never replace a newer cached head by an older
            // read that raced with it.
            let newer = c
                .head
                .as_ref()
                .is_none_or(|(_, h)| h.state.term <= head.state.term);
            if newer {
                c.head = Some((bytes, head));
            }
        });
    }

    /// The head from the store (a linearizable read).
    async fn fetch(&self, tl: &TimelineId) -> Result<Option<(Vec<u8>, RawHead)>, Error> {
        match self.kv.get(self.head_key(tl)).await? {
            None => Ok(None),
            Some(b) => {
                let h = RawHead::decode(&b)?;
                self.remember(tl, b.clone(), h.clone());
                Ok(Some((b, h)))
            }
        }
    }

    /// Read-modify-swap of the head. `f` sees the current head (the cached
    /// one first: terms and LSNs only grow, so a `Keep` decided on a stale
    /// copy also holds for the current one, and a stale `Write` fails its
    /// swap and is retried on the value the swap returns).
    async fn update<T>(
        &self,
        tl: &TimelineId,
        mut f: impl FnMut(&mut RawHead) -> Result<Step<T>, Error>,
    ) -> Result<T, Error> {
        let mut cur = match self.with_cache(tl, |c| c.head.clone()) {
            Some(h) => h,
            None => self.fetch(tl).await?.ok_or(Error::NotFound(*tl))?,
        };
        for _ in 0..CAS_RETRIES {
            let mut next = cur.1.clone();
            let out = match f(&mut next)? {
                Step::Keep(out) => return Ok(out),
                Step::Write(out) => out,
            };
            let bytes = next.encode()?;
            let (prev, swapped) = self
                .kv
                .compare_and_swap(self.head_key(tl), Some(cur.0.clone()), bytes.clone())
                .await?;
            if swapped {
                self.remember(tl, bytes, next);
                return Ok(out);
            }
            let prev = prev.ok_or(Error::NotFound(*tl))?;
            let h = RawHead::decode(&prev)?;
            self.remember(tl, prev.clone(), h.clone());
            cur = (prev, h);
        }
        Err(Error::Store("head swap kept failing".into()))
    }

    /// Chunks of writer `term` from key LSN `from` on, one page.
    async fn chunks(
        &self,
        tl: &TimelineId,
        term: Term,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, Bytes)>, Error> {
        let pairs = self
            .kv
            .scan(
                self.wal_key(tl, term, from),
                self.wal_key(tl, term, to),
                SCAN_PAGE,
            )
            .await?;
        pairs
            .into_iter()
            .map(|(k, v)| Ok((self.wal_lsn(&k)?, Bytes::from(v))))
            .collect()
    }

    /// Stitched WAL of one segment `[at, end)`, at most `budget` bytes;
    /// returns the pieces (when `keep`) and where it stopped: a hole, `end`
    /// or the budget.
    #[allow(clippy::too_many_arguments)]
    async fn read_segment(
        &self,
        tl: &TimelineId,
        term: Term,
        seg_start: Lsn,
        mut at: u64,
        end: u64,
        budget: u64,
        keep: bool,
    ) -> Result<(Vec<(Lsn, Bytes)>, u64), Error> {
        let mut out = Vec::new();
        let mut left = budget;
        // The chunk covering `at` begins at most MAX_CHUNK - 1 below it.
        let mut cursor = at.saturating_sub(MAX_CHUNK as u64 - 1).max(seg_start.0);
        loop {
            if at >= end || left == 0 {
                return Ok((out, at));
            }
            let page = self.chunks(tl, term, cursor, end).await?;
            let Some(&(last_begin, _)) = page.last() else {
                return Ok((out, at));
            };
            let (pieces, next) = stitch(&page, at, end, left);
            left -= pieces.iter().map(|(_, b)| b.len() as u64).sum::<u64>();
            if keep {
                out.extend(pieces);
            }
            at = next;
            let hole = at < end && left > 0 && page.iter().any(|(b, _)| *b > at);
            if hole || (page.len() as u32) < SCAN_PAGE {
                return Ok((out, at));
            }
            cursor = last_begin + 1;
        }
    }

    /// The contiguous WAL end of the last segment, scanning from `from`.
    async fn scan_end(&self, tl: &TimelineId, head: &RawHead, from: Lsn) -> Result<Lsn, Error> {
        let Some(last) = head.segments.last() else {
            return Ok(head.state.flush_lsn);
        };
        let at = from.max(last.start).max(head.state.flush_lsn);
        let (_, end) = self
            .read_segment(tl, last.term, last.start, at.0, u64::MAX, u64::MAX, false)
            .await?;
        Ok(Lsn(end))
    }

    /// The head with the true WAL end in `state.flush_lsn`: scanned from
    /// this instance's contiguous end when it writes the last segment, else
    /// from the stored lower bound.
    async fn with_end(&self, tl: &TimelineId, mut head: RawHead) -> Result<RawHead, Error> {
        let local = self.with_cache(tl, |c| {
            c.tracker
                .as_ref()
                .filter(|t| head.segments.last().is_some_and(|s| s.term == t.term))
                .map(|t| Lsn(t.contiguous))
        });
        let end = self
            .scan_end(tl, &head, local.unwrap_or(Lsn::INVALID))
            .await?;
        head.state.flush_lsn = head.state.flush_lsn.max(end);
        Ok(head)
    }

    /// The state an append reports: the cached head with this instance's
    /// contiguous end and in-memory commit LSN.
    fn reported(head: &RawHead, t: &Tracker) -> AcceptorState {
        let mut st = head.state.clone();
        st.flush_lsn = st.flush_lsn.max(Lsn(t.contiguous));
        st.commit_lsn = st.commit_lsn.max(t.commit_lsn);
        st.peer_horizon_lsn = st.peer_horizon_lsn.max(t.peer_horizon_lsn);
        st
    }
}

#[async_trait]
impl<K: RawKv> WalStore for RawWalStore<K> {
    fn max_in_flight(&self) -> usize {
        self.depth
    }

    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error> {
        match self.fetch(tl).await? {
            None => Ok(None),
            Some((_, h)) => Ok(Some(self.with_end(tl, h).await?.state)),
        }
    }

    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error> {
        if let Some(st) = self.load(tl).await? {
            return Ok(st);
        }
        // One region per timeline (§28 §6.5): its head and WAL share a
        // leader, and its Raft log is its own.
        let mut end = self.prefix(tl);
        successor(&mut end);
        if let Err(e) = self.kv.pre_split(vec![self.prefix(tl), end]).await {
            tracing::warn!(%tl, error = %e, "timeline pre-split failed");
        }
        let head = RawHead {
            state: AcceptorState::new(server, start_lsn),
            segments: Vec::new(),
        };
        let bytes = head.encode()?;
        let (prev, swapped) = self
            .kv
            .compare_and_swap(self.head_key(tl), None, bytes.clone())
            .await?;
        if swapped {
            self.remember(tl, bytes, head.clone());
            return Ok(head.state);
        }
        let prev = prev.ok_or_else(|| Error::Store("create: swap failed on no value".into()))?;
        let h = RawHead::decode(&prev)?;
        self.remember(tl, prev, h.clone());
        Ok(self.with_end(tl, h).await?.state)
    }

    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error> {
        let head = self
            .update(tl, |h| {
                h.state.server = server;
                if let Some(m) = &mconf {
                    h.state.mconf = m.clone();
                }
                Ok(Step::Write(h.clone()))
            })
            .await?;
        Ok(self.with_end(tl, head).await?.state)
    }

    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error> {
        let (given, head) = self
            .update(tl, |h| {
                let given = apply_vote(&mut h.state, term);
                let out = (given, h.clone());
                Ok(if given {
                    Step::Write(out)
                } else {
                    Step::Keep(out)
                })
            })
            .await?;
        // The fence is in place (the swap above, or a stored term already at
        // or above `term`): only now is the WAL end read, so it covers every
        // append acknowledged under an older term.
        let head = self.with_end(tl, head).await?;
        Ok((given, head.state))
    }

    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        for _ in 0..CAS_RETRIES {
            let (expected, head) = self.fetch(tl).await?.ok_or(Error::NotFound(*tl))?;
            if head.state.term > msg.term {
                return Ok(Err(Deposed {
                    current: head.state.term,
                }));
            }
            if head.state.term < msg.term {
                // Fence first, as a vote does, then look at the WAL.
                self.vote(tl, msg.term).await?;
                continue;
            }
            let head = self.with_end(tl, head).await?;
            let mut next = head.clone();
            let at = match apply_elected(&mut next.state, msg)? {
                Err(d) => return Ok(Err(d)),
                Ok(at) => at,
            };
            // Cut the writer segments at `at` and open this term's.
            next.segments.retain(|s| s.start < at || s.term == msg.term);
            match next.segments.last() {
                Some(s) if s.term == msg.term => {
                    if s.start > at {
                        return Err(Error::Protocol(format!(
                            "ProposerElected of term {} at {at}, below the term's own start {}",
                            msg.term, s.start
                        )));
                    }
                }
                _ => next.segments.push(Segment {
                    term: msg.term,
                    start: at,
                }),
            }
            let bytes = next.encode()?;
            // Expected: the head as fetched, before the fence was known to
            // hold (`with_end` only changed the in-memory flush_lsn).
            let (_, swapped) = self
                .kv
                .compare_and_swap(self.head_key(tl), Some(expected), bytes.clone())
                .await?;
            if !swapped {
                continue;
            }
            self.remember(tl, bytes, next.clone());
            self.with_cache(tl, |c| {
                let keep = c
                    .tracker
                    .as_ref()
                    .is_some_and(|t| t.term == msg.term && t.contiguous >= at.0);
                if !keep {
                    c.tracker = Some(Tracker {
                        term: msg.term,
                        contiguous: at.0,
                        done: BTreeMap::new(),
                        commit_lsn: next.state.commit_lsn,
                        peer_horizon_lsn: next.state.peer_horizon_lsn,
                    });
                }
            });
            return Ok(Ok(next.state));
        }
        Err(Error::Store("elected: head swap kept failing".into()))
    }

    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        let end = batch.end_lsn()?;
        // 1. Check the term against this instance's view, and plan the keys.
        enum Plan {
            Deposed(Term),
            Reload,
            Write(Vec<(Vec<u8>, Vec<u8>)>, u64),
        }
        let plan = |c: &mut TlCache| -> Result<Plan, Error> {
            let Some((_, head)) = &c.head else {
                return Ok(Plan::Reload);
            };
            if head.state.term > batch.term {
                return Ok(Plan::Deposed(head.state.term));
            }
            let Some(t) = c.tracker.as_ref().filter(|t| t.term == batch.term) else {
                return Ok(Plan::Reload);
            };
            if head.state.term < batch.term {
                return Ok(Plan::Reload);
            }
            // Skip what is already durable (a retried prefix).
            let from = batch.begin_lsn.0.max(t.contiguous).min(end.0);
            let mut skip = from - batch.begin_lsn.0;
            let mut at = from;
            let mut puts = Vec::new();
            for c in &batch.wal {
                let len = c.len() as u64;
                if skip >= len {
                    skip -= len;
                    continue;
                }
                let rest = c.slice(skip as usize..);
                skip = 0;
                for piece in rest.chunks(MAX_CHUNK) {
                    puts.push((self.wal_key(tl, batch.term, at), piece.to_vec()));
                    at += piece.len() as u64;
                }
            }
            Ok(Plan::Write(puts, from))
        };
        let mut reloaded = false;
        let (puts, from) = loop {
            match self.with_cache(tl, plan)? {
                Plan::Deposed(current) => return Ok(Err(Deposed { current })),
                Plan::Write(p, f) => break (p, f),
                Plan::Reload if !reloaded => {
                    reloaded = true;
                    let (_, head) = self.fetch(tl).await?.ok_or(Error::NotFound(*tl))?;
                    if head.state.term == batch.term
                        && head.segments.last().is_some_and(|s| s.term == batch.term)
                    {
                        let head = self.with_end(tl, head).await?;
                        self.with_cache(tl, |c| {
                            if c.tracker.as_ref().is_none_or(|t| t.term != batch.term) {
                                c.tracker = Some(Tracker {
                                    term: batch.term,
                                    contiguous: head.state.flush_lsn.0,
                                    done: BTreeMap::new(),
                                    commit_lsn: head.state.commit_lsn,
                                    peer_horizon_lsn: head.state.peer_horizon_lsn,
                                });
                            }
                        });
                    }
                }
                Plan::Reload => {
                    return Err(Error::Protocol(format!(
                        "AppendRequest of term {} before ProposerElected",
                        batch.term
                    )));
                }
            }
        };

        // 2. The blind write.
        if !puts.is_empty() {
            self.kv.batch_put(puts).await?;
        }

        // 3. The fence: the head, read after the write, still names the term.
        let (_, head) = self.fetch(tl).await?.ok_or(Error::NotFound(*tl))?;
        if head.state.term != batch.term {
            if head.state.term > batch.term {
                return Ok(Err(Deposed {
                    current: head.state.term,
                }));
            }
            return Err(Error::Store(format!(
                "stored term {} below the writer's {}",
                head.state.term, batch.term
            )));
        }

        // 4. Acknowledge only the contiguous end.
        self.with_cache(tl, |c| {
            let Some(t) = c.tracker.as_mut().filter(|t| t.term == batch.term) else {
                return Err(Error::Store("append: the term's tracker is gone".into()));
            };
            t.complete(from, end.0);
            if batch.commit_lsn != Lsn::INVALID {
                t.commit_lsn = t.commit_lsn.max(batch.commit_lsn.min(Lsn(t.contiguous)));
            }
            t.peer_horizon_lsn = t.peer_horizon_lsn.max(batch.truncate_lsn);
            let (_, h) = c.head.as_ref().ok_or(Error::NotFound(*tl))?;
            Ok(Ok(Self::reported(h, t)))
        })
    }

    async fn record_commit_lsn(
        &self,
        tl: &TimelineId,
        term: Term,
        commit_lsn: Lsn,
    ) -> Result<Result<(), Deposed>, Error> {
        let local = self.with_cache(tl, |c| {
            c.tracker
                .as_ref()
                .filter(|t| t.term == term)
                .map(|t| Lsn(t.contiguous))
        });
        self.update(tl, |h| {
            // Clamp to what is known durable: this instance's contiguous end
            // or the stored lower bound.
            let mut st = h.state.clone();
            st.flush_lsn = st.flush_lsn.max(local.unwrap_or(Lsn::INVALID));
            let before = h.state.commit_lsn;
            if let Err(d) = apply_commit_lsn(&mut st, term, commit_lsn)? {
                return Ok(Step::Keep(Err(d)));
            }
            if st.commit_lsn == before {
                return Ok(Step::Keep(Ok(())));
            }
            h.state.commit_lsn = st.commit_lsn;
            // The commit LSN is durable WAL of this history: a valid scan start.
            let last_start = h.segments.last().map_or(Lsn::INVALID, |s| s.start);
            h.state.flush_lsn = h.state.flush_lsn.max(st.commit_lsn.max(last_start));
            Ok(Step::Write(Ok(())))
        })
        .await
    }

    async fn record_backup_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        self.update(tl, |h| {
            let b = h
                .state
                .backup_lsn
                .max(lsn.min(h.state.commit_lsn.max(h.state.flush_lsn)));
            if b == h.state.backup_lsn {
                return Ok(Step::Keep(()));
            }
            h.state.backup_lsn = b;
            Ok(Step::Write(()))
        })
        .await
    }

    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        self.update(tl, |h| {
            if lsn <= h.state.remote_consistent_lsn {
                return Ok(Step::Keep(()));
            }
            h.state.remote_consistent_lsn = lsn;
            Ok(Step::Write(()))
        })
        .await
    }

    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error> {
        let (_, head) = self.fetch(tl).await?.ok_or(Error::NotFound(*tl))?;
        if from < head.state.trimmed_lsn {
            return Err(Error::Trimmed {
                from,
                trimmed: head.state.trimmed_lsn,
            });
        }
        let mut out = Vec::new();
        let mut at = from.0;
        let mut budget = max_bytes as u64;
        let ranges: Vec<_> = head.ranges(Lsn(u64::MAX)).collect();
        for (term, start, end) in ranges {
            if budget == 0 || at < start.0 {
                break;
            }
            if at >= end.0 {
                continue;
            }
            let (pieces, next) = self
                .read_segment(tl, term, start, at, end.0, budget, true)
                .await?;
            budget -= pieces.iter().map(|(_, b)| b.len() as u64).sum::<u64>();
            out.extend(pieces);
            if next < end.0 {
                break; // a hole or the budget: the end of what is readable
            }
            at = next;
        }
        Ok(out)
    }

    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error> {
        let local = self.with_cache(tl, |c| c.tracker.as_ref().map(|t| (t.term, t.contiguous)));
        let (bound, head) = self
            .update(tl, |h| {
                let mut st = h.state.clone();
                if let (Some((t, c)), Some(last)) = (local, h.segments.last())
                    && t == last.term
                {
                    st.flush_lsn = st.flush_lsn.max(Lsn(c));
                }
                let bound = trim_bound(&st, lsn);
                if bound == h.state.trimmed_lsn {
                    return Ok(Step::Keep((bound, h.clone())));
                }
                h.state.trimmed_lsn = bound;
                // Segments wholly below the bound are dropped (the last is kept).
                while h.segments.len() > 1 && h.segments[1].start <= bound {
                    h.segments.remove(0);
                }
                Ok(Step::Write((bound, h.clone())))
            })
            .await?;
        // Delete below the bound, keeping any chunk that may contain it, and
        // every stale writer term's keys below the kept segments.
        let keep_from = bound.0.saturating_sub(MAX_CHUNK as u64);
        let mut prev_term = 0u64;
        for (i, s) in head.segments.iter().enumerate() {
            // Terms between the previous kept one and this one: garbage.
            let lo = if i == 0 { 0 } else { prev_term + 1 };
            if lo < s.term {
                self.kv
                    .delete_range(self.wal_key(tl, lo, 0), self.wal_key(tl, s.term, 0))
                    .await?;
            }
            if i == 0 && keep_from > 0 {
                self.kv
                    .delete_range(
                        self.wal_key(tl, s.term, 0),
                        self.wal_key(tl, s.term, keep_from),
                    )
                    .await?;
            }
            prev_term = s.term;
        }
        Ok(bound)
    }
}

/// The next key prefix after `k` (increment with carry).
pub(crate) fn successor(k: &mut Vec<u8>) {
    while let Some(last) = k.last_mut() {
        if *last == 0xFF {
            k.pop();
        } else {
            *last += 1;
            return;
        }
    }
}

/// A linearizable in-memory [`RawKv`] for tests and models.
#[derive(Debug, Default)]
pub struct MemRawKv {
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemRawKv {
    pub fn new() -> Self {
        Self::default()
    }

    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<Vec<u8>, Vec<u8>>> {
        self.map.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Every key, for assertions.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.map().keys().cloned().collect()
    }
}

#[async_trait]
impl RawKv for MemRawKv {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.map().get(&key).cloned())
    }

    async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        self.map().extend(pairs);
        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, bool), Error> {
        let mut m = self.map();
        let prev = m.get(&key).cloned();
        if prev == expected {
            m.insert(key, new);
            Ok((prev, true))
        } else {
            Ok((prev, false))
        }
    }

    async fn scan(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        if from >= to {
            return Ok(Vec::new());
        }
        Ok(self
            .map()
            .range(from..to)
            .take(limit as usize)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn delete_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
        if from < to {
            let mut m = self.map();
            let doomed: Vec<_> = m.range(from..to).map(|(k, _)| k.clone()).collect();
            for k in doomed {
                m.remove(&k);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "tikv")]
pub use self::tikv_kv::{TikvRawKv, TikvRawWalStore};

#[cfg(feature = "tikv")]
mod tikv_kv {
    use super::*;

    /// [`RawKv`] on `tikv-client`: blind writes, reads, scans and range
    /// deletes on a plain client; compare-and-swap on an atomic-mode one.
    #[derive(Clone)]
    pub struct TikvRawKv {
        plain: tikv_client::RawClient,
        atomic: tikv_client::RawClient,
        /// For PD's HTTP API (region pre-splits).
        tikv: operon_tikv::Tikv,
    }

    impl std::fmt::Debug for TikvRawKv {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TikvRawKv")
                .field("tikv", &self.tikv)
                .finish_non_exhaustive()
        }
    }

    /// The raw TiKV store.
    pub type TikvRawWalStore = RawWalStore<TikvRawKv>;

    fn err(e: tikv_client::Error) -> Error {
        Error::Store(e.to_string())
    }

    impl TikvRawKv {
        /// Connects to the keyspace of `config` (which must exist, on API v2).
        pub async fn connect(config: operon_tikv::TikvConfig) -> Result<Self, Error> {
            let client_config = tikv_client::Config::default()
                .with_timeout(config.request_timeout)
                .with_keyspace(&config.keyspace);
            let plain = tikv_client::RawClient::new_with_config(config.pd.clone(), client_config)
                .await
                .map_err(err)?;
            let atomic = plain.with_atomic_for_cas();
            let tikv = operon_tikv::Tikv::connect(config)
                .await
                .map_err(|e| Error::Store(e.to_string()))?;
            Ok(Self {
                plain,
                atomic,
                tikv,
            })
        }

        /// The root prefix of the handle's configuration.
        pub fn root(&self) -> &[u8] {
            self.tikv.root()
        }

        /// A raw store with `depth` appends in flight per timeline.
        pub fn into_store(self, depth: usize) -> TikvRawWalStore {
            let root = self.root().to_vec();
            RawWalStore::new(Arc::new(self), root, depth)
        }
    }

    #[async_trait]
    impl RawKv for TikvRawKv {
        async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
            self.plain.get(key).await.map_err(err)
        }

        async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
            self.plain.batch_put(pairs).await.map_err(err)
        }

        async fn compare_and_swap(
            &self,
            key: Vec<u8>,
            expected: Option<Vec<u8>>,
            new: Vec<u8>,
        ) -> Result<(Option<Vec<u8>>, bool), Error> {
            self.atomic
                .compare_and_swap(key, expected, new)
                .await
                .map_err(err)
        }

        async fn scan(
            &self,
            from: Vec<u8>,
            to: Vec<u8>,
            limit: u32,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
            if from >= to {
                return Ok(Vec::new());
            }
            let pairs = self.plain.scan(from..to, limit).await.map_err(err)?;
            Ok(pairs.into_iter().map(|p| (Vec::from(p.0), p.1)).collect())
        }

        async fn delete_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
            if from >= to {
                return Ok(());
            }
            self.plain.delete_range(from..to).await.map_err(err)
        }

        async fn pre_split(&self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
            self.tikv
                .split_raw_regions(&keys)
                .await
                .map_err(|e| Error::Store(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests;
