//! The local-NVMe [`WalStore`] of Arm A (§28 §7.2, D264–D268): one acceptor's
//! timelines in a shared [`Journal`], with heads in a [`MetaStore`].
//!
//! - **The fence is in memory.** This process is the only writer of its
//!   acceptors, so the rules of [`crate::store`] run under a per-timeline
//!   lock against the in-memory head. A vote or an election first waits until
//!   the timeline's written WAL is durable, then stores the new head in the
//!   [`MetaStore`] before answering.
//! - **Appends are pipelined** ([`WalStore::max_in_flight`]): an append
//!   packs its WAL into the journal's open unit under the timeline's lock,
//!   then waits for the unit outside it, so several appends of one timeline
//!   share flush units and the response reports the contiguous durable end.
//!   Reads and `load` only ever see durable WAL.
//! - **Recovery** rebuilds each timeline from its stored head and a replay of
//!   the journal: Append records extend the WAL contiguous from the trimmed
//!   point, Truncate records cut it, Progress records raise the lazily kept
//!   LSNs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, info};

use crate::Error;
use crate::journal::format::{Kind, RecordHeader};
use crate::journal::{Journal, JournalConfig};
use crate::meta::MetaStore;
use crate::proto::ProposerElected;
use crate::store::{
    AppendBatch, Deposed, WalStore, apply_append, apply_commit_lsn, apply_elected, apply_vote,
    remaining_chunks, trim_bound,
};
use crate::types::{AcceptorState, Configuration, Lsn, ServerInfo, Term, TimelineId};

/// Where one run of a timeline's WAL sits in the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    lsn: u64,
    len: u64,
    seq: u64,
    off: u64,
    /// The flush unit that carries it (0: replayed, durable).
    unit: u64,
}

impl Entry {
    fn end(&self) -> u64 {
        self.lsn + self.len
    }
}

#[derive(Debug)]
struct Tl {
    /// `flush_lsn` is the end of the WAL written (maybe not yet durable).
    head: AcceptorState,
    /// Contiguous, ascending.
    index: VecDeque<Entry>,
    /// Written, not yet known durable: (unit, WAL end after it).
    pending: VecDeque<(u64, Lsn)>,
    /// The end of the durable WAL.
    durable_end: Lsn,
    /// The unit of this timeline's latest record of any kind.
    last_unit: u64,
}

impl Tl {
    fn new(head: AcceptorState) -> Tl {
        let durable_end = head.flush_lsn;
        Tl {
            head,
            index: VecDeque::new(),
            pending: VecDeque::new(),
            durable_end,
            last_unit: 0,
        }
    }

    /// Advance `durable_end` over pending writes the journal made durable.
    fn settle(&mut self, durable: u64) {
        while let Some(&(unit, end)) = self.pending.front() {
            if unit > durable {
                return;
            }
            self.durable_end = end;
            self.pending.pop_front();
        }
        if self.pending.is_empty() {
            self.durable_end = self.head.flush_lsn;
        }
    }

    /// The head as readers and a new connection see it: durable WAL only.
    fn durable_head(&self) -> AcceptorState {
        let mut st = self.head.clone();
        st.flush_lsn = self.durable_end;
        st.commit_lsn = st.commit_lsn.min(st.wal_end());
        st
    }

    /// Drop WAL at and above `at` (a Truncate).
    fn truncate(&mut self, at: u64) {
        while let Some(e) = self.index.back_mut() {
            if e.lsn >= at {
                self.index.pop_back();
            } else {
                if e.end() > at {
                    e.len = at - e.lsn;
                }
                break;
            }
        }
    }

    /// Drop WAL below `below` (a trim).
    fn trim_front(&mut self, below: u64) {
        while let Some(e) = self.index.front_mut() {
            if e.end() <= below {
                self.index.pop_front();
            } else {
                if e.lsn < below {
                    let skip = below - e.lsn;
                    e.lsn += skip;
                    e.off += skip;
                    e.len -= skip;
                }
                break;
            }
        }
    }

    /// Replay one Append record: extend the contiguous WAL.
    fn replay_append(&mut self, seq: u64, off: u64, h: &RecordHeader) {
        let (begin, end) = (h.lsn.0, h.lsn.0 + u64::from(h.len));
        let cur = self.head.flush_lsn.0;
        if end <= cur || begin > cur {
            if begin > cur {
                debug!(tl = %h.tl, begin, cur, "replay: WAL after a gap is ignored");
            }
            return;
        }
        let skip = cur - begin;
        self.index.push_back(Entry {
            lsn: cur,
            len: end - cur,
            seq,
            off: off + skip,
            unit: 0,
        });
        self.head.flush_lsn = Lsn(end);
        if h.aux != 0 {
            self.head.commit_lsn = self.head.commit_lsn.max(Lsn(h.aux));
        }
        self.head.peer_horizon_lsn = self.head.peer_horizon_lsn.max(Lsn(h.aux2));
    }
}

/// The local-NVMe store: one shard's journal and the acceptors in it.
pub struct NvmeWalStore {
    journal: Journal,
    meta: Arc<dyn MetaStore>,
    tls: StdMutex<HashMap<TimelineId, Arc<Mutex<Tl>>>>,
    /// Every timeline's trimmed LSN, for freeing segments without locking
    /// each timeline.
    trimmed: StdMutex<HashMap<TimelineId, u64>>,
    /// Timelines with Progress records not yet stored in their heads.
    dirty: StdMutex<HashSet<TimelineId>>,
    /// Woken after every write, for appends waiting on a predecessor.
    applied: Notify,
}

/// Appends of one timeline the service may have in flight.
const MAX_IN_FLIGHT: usize = 16;

/// How long an append that leaves a gap waits for its predecessor.
#[cfg(not(test))]
const GAP_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const GAP_WAIT: std::time::Duration = std::time::Duration::from_millis(200);

impl std::fmt::Debug for NvmeWalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NvmeWalStore")
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

fn record(
    kind: Kind,
    tl: &TimelineId,
    term: Term,
    lsn: Lsn,
    aux: u64,
    aux2: u64,
    len: usize,
) -> RecordHeader {
    RecordHeader {
        kind,
        tl: *tl,
        term,
        lsn,
        aux,
        aux2,
        len: len as u32,
    }
}

impl NvmeWalStore {
    /// Open the journal in `cfg.dir` and rebuild every timeline from `meta`
    /// and the replay. Thread-tier engines are started; a
    /// [`crate::journal::Tier::Uring`] journal waits for its shard.
    pub async fn open(cfg: JournalConfig, meta: Arc<dyn MetaStore>) -> Result<NvmeWalStore, Error> {
        Self::open_owning(cfg, meta, |_| true).await
    }

    /// [`Self::open`] for one shard: only the timelines `owns` accepts.
    pub async fn open_owning(
        cfg: JournalConfig,
        meta: Arc<dyn MetaStore>,
        owns: impl Fn(&TimelineId) -> bool,
    ) -> Result<NvmeWalStore, Error> {
        let heads: Vec<_> = meta
            .load_all()
            .await?
            .into_iter()
            .filter(|(tl, _)| owns(tl))
            .collect();
        let mut tls: HashMap<TimelineId, Tl> = heads
            .into_iter()
            .map(|(tl, mut head)| {
                // The WAL comes from the journal, contiguous from the trimmed
                // point (where a fresh or trimmed acceptor's WAL starts).
                head.flush_lsn = head.trimmed_lsn.max(head.local_start_lsn);
                (tl, Tl::new(head))
            })
            .collect();
        let journal = Journal::open(cfg, |seq, h, off| {
            let Some(t) = tls.get_mut(&h.tl) else {
                return;
            };
            match h.kind {
                Kind::Append => t.replay_append(seq, off, h),
                Kind::Truncate => {
                    if h.lsn < t.head.flush_lsn {
                        t.truncate(h.lsn.0);
                        t.head.flush_lsn = h.lsn;
                    }
                }
                Kind::Progress => {
                    t.head.commit_lsn = t.head.commit_lsn.max(h.lsn);
                    t.head.backup_lsn = t.head.backup_lsn.max(Lsn(h.aux));
                    t.head.remote_consistent_lsn = t.head.remote_consistent_lsn.max(Lsn(h.aux2));
                }
            }
        })?;
        let trimmed = tls
            .iter()
            .map(|(k, t)| (*k, t.head.trimmed_lsn.0))
            .collect();
        let tls = tls
            .into_iter()
            .map(|(k, mut t)| {
                t.head.commit_lsn = t.head.commit_lsn.min(t.head.wal_end());
                t.head.backup_lsn = t.head.backup_lsn.min(t.head.wal_end());
                t.durable_end = t.head.flush_lsn;
                info!(tl = %k, term = t.head.term, flush_lsn = %t.head.flush_lsn, "timeline recovered");
                (k, Arc::new(Mutex::new(t)))
            })
            .collect();
        journal.start();
        Ok(NvmeWalStore {
            journal,
            meta,
            tls: StdMutex::new(tls),
            trimmed: StdMutex::new(trimmed),
            dirty: StdMutex::new(HashSet::new()),
            applied: Notify::new(),
        })
    }

    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    fn get(&self, tl: &TimelineId) -> Result<Arc<Mutex<Tl>>, Error> {
        self.tls
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(tl)
            .cloned()
            .ok_or(Error::NotFound(*tl))
    }

    /// Wait until everything written for the timeline is durable.
    async fn flush(&self, t: &mut Tl) -> Result<(), Error> {
        self.journal.wait(t.last_unit).await?;
        t.settle(self.journal.durable().unit);
        Ok(())
    }

    fn progress(&self, tl: &TimelineId, t: &mut Tl) -> Result<u64, Error> {
        let h = record(
            Kind::Progress,
            tl,
            t.head.term,
            t.head.commit_lsn,
            t.head.backup_lsn.0,
            t.head.remote_consistent_lsn.0,
            0,
        );
        let p = self.journal.append(&h, &[])?;
        t.last_unit = p.unit;
        self.dirty
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(*tl);
        Ok(p.unit)
    }

    /// Store the durable head of every timeline whose Progress records were
    /// written since its head was last stored, so recycling the journal
    /// segments that carry them loses nothing.
    async fn persist_progress(&self) -> Result<(), Error> {
        let dirty: Vec<TimelineId> = self
            .dirty
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        let mut todo = dirty.into_iter();
        while let Some(tl) = todo.next() {
            let Ok(t) = self.get(&tl) else {
                continue;
            };
            let mut t = t.lock().await;
            let durable = self.journal.durable().unit;
            t.settle(durable);
            if let Err(e) = self.meta.put(&tl, &t.durable_head()).await {
                // Keep the failed timeline and every one not reached yet, so
                // the next trim persists them before any segment is freed.
                let mut d = self.dirty.lock().unwrap_or_else(|e| e.into_inner());
                d.insert(tl);
                d.extend(todo);
                return Err(e);
            }
            if t.last_unit > durable {
                // The latest Progress record is not on disk yet.
                self.dirty
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(tl);
            }
        }
        Ok(())
    }

    /// Pack a batch into the journal under the timeline's lock. The ticket
    /// is the unit to wait for, `None` when everything is already durable.
    fn write(
        &self,
        tl: &TimelineId,
        t: &mut Tl,
        batch: &AppendBatch,
    ) -> Result<Result<Option<u64>, Deposed>, Error> {
        let mut next = t.head.clone();
        let plan = match apply_append(&mut next, batch)? {
            Err(d) => return Ok(Err(d)),
            Ok(plan) => plan,
        };
        let chunks = remaining_chunks(batch, &plan);
        let mut at = plan.write_from.0;
        let mut ticket = None;
        let max = self.journal.config().max_payload();
        // Pack the chunks into records of at most `max` bytes each.
        let mut group: Vec<Bytes> = Vec::new();
        let mut group_len = 0usize;
        let mut flush_group = |group: &mut Vec<Bytes>,
                               group_len: &mut usize,
                               t: &mut Tl,
                               at: &mut u64|
         -> Result<(), Error> {
            if *group_len == 0 {
                return Ok(());
            }
            let h = record(
                Kind::Append,
                tl,
                batch.term,
                Lsn(*at),
                next.commit_lsn.0,
                next.peer_horizon_lsn.0,
                *group_len,
            );
            let slices: Vec<&[u8]> = group.iter().map(|b| &b[..]).collect();
            let p = self.journal.append(&h, &slices)?;
            t.index.push_back(Entry {
                lsn: *at,
                len: *group_len as u64,
                seq: p.seq,
                off: p.payload_off,
                unit: p.unit,
            });
            *at += *group_len as u64;
            t.pending.push_back((p.unit, Lsn(*at)));
            t.last_unit = p.unit;
            ticket = Some(p.unit);
            group.clear();
            *group_len = 0;
            Ok(())
        };
        for c in chunks {
            let mut c = c;
            while !c.is_empty() {
                let room = max - group_len;
                if room == 0 {
                    flush_group(&mut group, &mut group_len, t, &mut at)?;
                    continue;
                }
                let take = room.min(c.len());
                group.push(c.split_to(take));
                group_len += take;
            }
        }
        flush_group(&mut group, &mut group_len, t, &mut at)?;
        if ticket.is_none() {
            // A retry of WAL already written: it is acknowledged once what is
            // still pending is durable.
            t.settle(self.journal.durable().unit);
            ticket = t.pending.back().map(|&(unit, _)| unit);
        }
        t.head = next;
        Ok(Ok(ticket))
    }

    /// Close the journal (tests and shutdown).
    pub fn close(&self) {
        self.journal.close();
    }
}

#[async_trait]
impl WalStore for NvmeWalStore {
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error> {
        let Ok(t) = self.get(tl) else {
            return Ok(None);
        };
        let mut t = t.lock().await;
        t.settle(self.journal.durable().unit);
        Ok(Some(t.durable_head()))
    }

    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error> {
        if let Ok(t) = self.get(tl) {
            return Ok(t.lock().await.durable_head());
        }
        let head = AcceptorState::new(server, start_lsn);
        self.meta.put(tl, &head).await?;
        let mut map = self.tls.lock().unwrap_or_else(|p| p.into_inner());
        let t = map
            .entry(*tl)
            .or_insert_with(|| Arc::new(Mutex::new(Tl::new(head.clone()))))
            .clone();
        drop(map);
        self.trimmed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(*tl)
            .or_insert(head.trimmed_lsn.0);
        Ok(t.try_lock().map_or(head, |t| t.durable_head()))
    }

    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        let mut next = t.head.clone();
        next.server = server;
        if let Some(m) = mconf {
            next.mconf = m;
        }
        self.meta.put(tl, &next).await?;
        t.head = next;
        Ok(t.durable_head())
    }

    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        // VoteResponse.flush_lsn must be what the disk holds.
        self.flush(&mut t).await?;
        let mut next = t.head.clone();
        let given = apply_vote(&mut next, term);
        if given {
            self.meta.put(tl, &next).await?;
            t.head = next;
        }
        Ok((given, t.durable_head()))
    }

    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        self.flush(&mut t).await?;
        let mut next = t.head.clone();
        let at = match apply_elected(&mut next, msg)? {
            Err(d) => return Ok(Err(d)),
            Ok(at) => at,
        };
        if at < t.head.flush_lsn {
            // The truncation is durable before the new history.
            let h = record(Kind::Truncate, tl, msg.term, at, 0, 0, 0);
            let p = self.journal.append(&h, &[])?;
            t.last_unit = p.unit;
            self.journal.wait(p.unit).await?;
            t.truncate(at.0);
        }
        self.meta.put(tl, &next).await?;
        t.head = next;
        t.pending.clear();
        t.durable_end = t.head.flush_lsn;
        Ok(Ok(t.durable_head()))
    }

    fn max_in_flight(&self) -> usize {
        MAX_IN_FLIGHT
    }

    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        let t = self.get(tl)?;
        // Writes are issued in LSN order, but the tasks that carry them may
        // reach the store out of order: one that leaves a gap waits for its
        // predecessor.
        let ticket = loop {
            let arrived = self.applied.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            {
                let mut g = t.lock().await;
                let gap = !batch.is_empty()
                    && g.head.term == batch.term
                    && g.head.flush_lsn != Lsn::INVALID
                    && batch.begin_lsn > g.head.flush_lsn;
                if !gap {
                    let r = self.write(tl, &mut g, batch)?;
                    self.applied.notify_waiters();
                    match r {
                        Err(d) => return Ok(Err(d)),
                        Ok(ticket) => break ticket,
                    }
                }
            }
            if tokio::time::timeout(GAP_WAIT, arrived).await.is_err() {
                return Err(Error::Protocol(format!(
                    "AppendRequest at {} leaves a gap that nothing filled",
                    batch.begin_lsn
                )));
            }
        };
        if let Some(unit) = ticket {
            self.journal.wait(unit).await?;
        }
        let mut g = t.lock().await;
        g.settle(self.journal.durable().unit);
        Ok(Ok(g.durable_head()))
    }

    async fn record_commit_lsn(
        &self,
        tl: &TimelineId,
        term: Term,
        commit_lsn: Lsn,
    ) -> Result<Result<(), Deposed>, Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        let mut next = t.head.clone();
        if let Err(d) = apply_commit_lsn(&mut next, term, commit_lsn)? {
            return Ok(Err(d));
        }
        if next.commit_lsn != t.head.commit_lsn {
            t.head = next;
            let unit = self.progress(tl, &mut t)?;
            self.journal.wait(unit).await?;
        }
        Ok(Ok(()))
    }

    async fn record_backup_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        let v = t.head.backup_lsn.max(lsn.min(t.durable_end));
        if v != t.head.backup_lsn {
            t.head.backup_lsn = v;
            self.progress(tl, &mut t)?;
        }
        Ok(())
    }

    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        let t = self.get(tl)?;
        let mut t = t.lock().await;
        if lsn > t.head.remote_consistent_lsn {
            t.head.remote_consistent_lsn = lsn;
            self.progress(tl, &mut t)?;
        }
        Ok(())
    }

    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error> {
        let entries = {
            let t = self.get(tl)?;
            let mut t = t.lock().await;
            t.settle(self.journal.durable().unit);
            if from < t.head.trimmed_lsn {
                return Err(Error::Trimmed {
                    from,
                    trimmed: t.head.trimmed_lsn,
                });
            }
            let end = t.durable_end.0;
            let start = t.index.partition_point(|e| e.end() <= from.0);
            let mut out = Vec::new();
            let mut budget = max_bytes as u64;
            let mut at = from.0;
            for e in t.index.range(start..) {
                if budget == 0 || at >= end {
                    break;
                }
                if e.lsn > at {
                    return Err(Error::Store(format!("WAL hole at {}", Lsn(at))));
                }
                let lo = at - e.lsn;
                let hi = (e.end().min(end) - e.lsn).min(lo + budget);
                if hi <= lo {
                    break;
                }
                out.push((Lsn(at), e.seq, e.off + lo, (hi - lo) as usize));
                budget -= hi - lo;
                at = e.lsn + hi;
            }
            out
        };
        entries
            .into_iter()
            .map(|(lsn, seq, off, len)| Ok((lsn, Bytes::from(self.journal.read(seq, off, len)?))))
            .collect()
    }

    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error> {
        let bound = {
            let t = self.get(tl)?;
            let mut t = t.lock().await;
            t.settle(self.journal.durable().unit);
            let mut head = t.durable_head();
            let bound = trim_bound(&head, lsn);
            if bound <= t.head.trimmed_lsn {
                return Ok(t.head.trimmed_lsn);
            }
            head.trimmed_lsn = bound;
            // The stored head carries the lazily kept LSNs too, since the
            // journal records that held them may be recycled next.
            self.meta.put(tl, &head).await?;
            t.head.trimmed_lsn = bound;
            t.trim_front(bound.0);
            bound
        };
        let trimmed = {
            let mut m = self.trimmed.lock().unwrap_or_else(|p| p.into_inner());
            m.insert(*tl, bound.0);
            m.clone()
        };
        // A freed segment may hold the only copy of another timeline's lazily
        // kept LSNs: put every durable one in the stored heads first.
        self.persist_progress().await?;
        self.journal
            .free(|tl, end| trimmed.get(tl).is_some_and(|&t| end <= t));
        Ok(bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::Tier;
    use crate::meta::LocalMeta;
    use crate::types::{Id, TermHistory, TermLsn};

    fn tl() -> TimelineId {
        TimelineId::new(Id([4; 16]), Id([5; 16]))
    }

    fn cfg(dir: &std::path::Path, tier: Tier) -> JournalConfig {
        JournalConfig {
            segment_size: 256 << 10,
            unit_capacity: 64 << 10,
            ..JournalConfig::new(dir.join("journal"), tier)
        }
    }

    async fn open(dir: &std::path::Path, tier: Tier) -> NvmeWalStore {
        let meta = Arc::new(LocalMeta::open(&dir.join("meta")).unwrap());
        NvmeWalStore::open(cfg(dir, tier), meta).await.unwrap()
    }

    fn elected(term: Term, start: u64, th: &[(Term, u64)]) -> ProposerElected {
        ProposerElected {
            generation: 0,
            term,
            start_streaming_at: Lsn(start),
            term_history: TermHistory(
                th.iter()
                    .map(|&(t, l)| TermLsn {
                        term: t,
                        lsn: Lsn(l),
                    })
                    .collect(),
            ),
        }
    }

    fn batch(term: Term, begin: u64, data: &[u8], commit: u64) -> AppendBatch {
        AppendBatch {
            term,
            begin_lsn: Lsn(begin),
            wal: vec![Bytes::copy_from_slice(data)],
            commit_lsn: Lsn(commit),
            truncate_lsn: Lsn::INVALID,
        }
    }

    async fn read_all(s: &NvmeWalStore, from: u64) -> Vec<u8> {
        s.read(&tl(), Lsn(from), usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b.to_vec())
            .collect()
    }

    #[tokio::test]
    async fn the_protocol_rules_hold_and_survive_a_restart() {
        for tier in [Tier::Buffered, Tier::Pwritev2 { depth: 2 }] {
            let d = tempfile::tempdir().unwrap();
            let s = open(d.path(), tier).await;
            s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
                .await
                .unwrap();
            assert!(s.vote(&tl(), 1).await.unwrap().0);
            assert!(!s.vote(&tl(), 1).await.unwrap().0);
            s.elected(&tl(), &elected(1, 100, &[(1, 100)]))
                .await
                .unwrap()
                .unwrap();
            let st = s
                .append(&tl(), &batch(1, 100, b"abcdef", 0))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(st.flush_lsn, Lsn(106));
            // A retried prefix and a gap.
            s.append(&tl(), &batch(1, 103, b"defgh", 104))
                .await
                .unwrap()
                .unwrap();
            assert!(s.append(&tl(), &batch(1, 200, b"z", 0)).await.is_err());
            assert_eq!(read_all(&s, 100).await, b"abcdefgh");
            // Term 2 truncates the uncommitted tail.
            s.vote(&tl(), 2).await.unwrap();
            assert_eq!(
                s.append(&tl(), &batch(1, 108, b"x", 0)).await.unwrap(),
                Err(Deposed { current: 2 })
            );
            s.elected(&tl(), &elected(2, 105, &[(1, 100), (2, 105)]))
                .await
                .unwrap()
                .unwrap();
            s.append(&tl(), &batch(2, 105, b"XYZ", 106))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(read_all(&s, 100).await, b"abcdeXYZ");
            s.record_remote_consistent_lsn(&tl(), Lsn(103))
                .await
                .unwrap();
            s.record_backup_lsn(&tl(), Lsn(104)).await.unwrap();
            s.record_commit_lsn(&tl(), 2, Lsn(107))
                .await
                .unwrap()
                .unwrap();
            s.close();
            drop(s);

            let s = open(d.path(), tier).await;
            let st = s.load(&tl()).await.unwrap().unwrap();
            assert_eq!(st.term, 2);
            assert_eq!(st.flush_lsn, Lsn(108));
            assert_eq!(st.commit_lsn, Lsn(107));
            assert_eq!(st.backup_lsn, Lsn(104));
            assert_eq!(st.remote_consistent_lsn, Lsn(103));
            assert_eq!(st.last_log_term(), 2);
            assert_eq!(read_all(&s, 100).await, b"abcdeXYZ");
            s.close();
        }
    }

    #[tokio::test]
    async fn an_append_returns_when_durable_and_a_retry_waits_for_it_too() {
        let d = tempfile::tempdir().unwrap();
        // No engine: nothing becomes durable until the test drives it.
        let meta = Arc::new(LocalMeta::open(&d.path().join("meta")).unwrap());
        let s = Arc::new(
            NvmeWalStore::open(cfg(d.path(), Tier::Uring { depth: 1 }), meta)
                .await
                .unwrap(),
        );
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        let append = |s: &Arc<NvmeWalStore>| {
            let s = s.clone();
            tokio::spawn(async move { s.append(&tl(), &batch(1, 0, b"hello", 0)).await })
        };
        let first = append(&s);
        // Placed in a unit, not durable: the call has not returned.
        let u = loop {
            if let Some(u) = s.journal().try_take() {
                break u;
            }
            tokio::task::yield_now().await;
        };
        assert!(!first.is_finished());
        assert_eq!(s.load(&tl()).await.unwrap().unwrap().flush_lsn, Lsn(0));
        assert!(read_all(&s, 0).await.is_empty());
        // A retry of the same WAL waits for that unit as well.
        let retry = append(&s);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!retry.is_finished(), "a retry is not acknowledged early");
        crate::journal::pwrite_dsync(&u.seg.write, &u.buf, u.off).unwrap();
        s.journal().complete(vec![u], Ok(()));
        for h in [first, retry] {
            let st = h.await.unwrap().unwrap().unwrap();
            assert_eq!(st.flush_lsn, Lsn(5));
        }
        assert_eq!(read_all(&s, 0).await, b"hello");
        s.close();
    }

    #[tokio::test]
    async fn appends_that_arrive_out_of_order_are_applied_in_order() {
        let d = tempfile::tempdir().unwrap();
        let s = Arc::new(open(d.path(), Tier::Buffered).await);
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 100, &[(1, 100)]))
            .await
            .unwrap()
            .unwrap();
        let s2 = s.clone();
        let later =
            tokio::spawn(async move { s2.append(&tl(), &batch(1, 105, b"world", 0)).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!later.is_finished(), "waits for the WAL before it");
        s.append(&tl(), &batch(1, 100, b"hello", 0))
            .await
            .unwrap()
            .unwrap();
        let st = later.await.unwrap().unwrap().unwrap();
        assert_eq!(st.flush_lsn, Lsn(110));
        assert_eq!(read_all(&s, 100).await, b"helloworld");
        s.close();
    }

    #[tokio::test]
    async fn trim_frees_segments_and_reads_below_it_fail() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path(), Tier::Buffered).await;
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        let chunk = vec![1u8; 30_000];
        for i in 0..40u64 {
            s.append(&tl(), &batch(1, i * 30_000, &chunk, i * 30_000))
                .await
                .unwrap()
                .unwrap();
        }
        let before = s.journal().live_segments().len();
        assert!(before > 3);
        let end = 40 * 30_000;
        s.record_commit_lsn(&tl(), 1, Lsn(end))
            .await
            .unwrap()
            .unwrap();
        s.record_backup_lsn(&tl(), Lsn(end - 100_000))
            .await
            .unwrap();
        s.record_remote_consistent_lsn(&tl(), Lsn(end))
            .await
            .unwrap();
        let t = s.trim(&tl(), Lsn(end)).await.unwrap();
        assert_eq!(t, Lsn(end - 100_000));
        assert!(s.journal().live_segments().len() < before);
        assert!(s.read(&tl(), Lsn(0), 10).await.is_err());
        assert_eq!(read_all(&s, end - 100_000).await.len(), 100_000);
        s.close();
        drop(s);
        let s = open(d.path(), Tier::Buffered).await;
        let st = s.load(&tl()).await.unwrap().unwrap();
        assert_eq!(
            (st.trimmed_lsn, st.flush_lsn),
            (Lsn(end - 100_000), Lsn(end))
        );
        assert_eq!(read_all(&s, end - 100_000).await.len(), 100_000);
        s.close();
    }

    #[tokio::test]
    async fn a_large_batch_is_split_into_records() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path(), Tier::Pwritev2 { depth: 4 }).await;
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let b = AppendBatch {
            term: 1,
            begin_lsn: Lsn(0),
            wal: data.chunks(50_000).map(Bytes::copy_from_slice).collect(),
            commit_lsn: Lsn(0),
            truncate_lsn: Lsn(0),
        };
        s.append(&tl(), &b).await.unwrap().unwrap();
        assert_eq!(read_all(&s, 0).await, data);
        s.close();
    }
}
