//! The storage behind the acceptor.
//!
//! A [`WalStore`] keeps, per timeline, the head ([`AcceptorState`]) and the
//! WAL chunks. Every mutating call is one atomic, durable step that re-checks
//! the proposer's term against the stored head: the fence of §28 §6.4. TiKV
//! implements it with one 1PC transaction per call (P4a); [`MemWalStore`]
//! drives the protocol tests.
//!
//! The state transitions themselves are the pure functions in this module
//! ([`apply_vote`], [`apply_elected`], [`apply_append`]), so that every store
//! applies exactly the same rules inside its own atomic section.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Error;
use crate::proto::ProposerElected;
use crate::types::{AcceptorState, Configuration, Lsn, ServerInfo, Term, TermHistory, TimelineId};

/// The proposer's term is older than the stored one: it was deposed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deposed {
    /// The stored (higher) term.
    pub current: Term,
}

/// A contiguous run of WAL from one proposer term: one or more queued
/// `AppendRequest`s folded into one durable write (group commit).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendBatch {
    pub term: Term,
    pub begin_lsn: Lsn,
    /// Contiguous chunks, each at most `MAX_SEND_SIZE`; together they cover
    /// `[begin_lsn, begin_lsn + total)`.
    pub wal: Vec<Bytes>,
    /// The proposer's commit LSN (0 when unknown).
    pub commit_lsn: Lsn,
    /// The proposer's `truncate_lsn`.
    pub truncate_lsn: Lsn,
}

impl AppendBatch {
    pub fn len(&self) -> u64 {
        self.wal.iter().map(|c| c.len() as u64).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.wal.iter().all(Bytes::is_empty)
    }

    pub fn end_lsn(&self) -> Result<Lsn, Error> {
        self.begin_lsn.checked_add(self.len())
    }
}

/// What [`apply_append`] decided: the WAL bytes still to write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendPlan {
    /// Write the batch's bytes from this LSN on (a retried prefix is skipped).
    pub write_from: Lsn,
    /// How many leading bytes of the batch are already stored.
    pub skip: u64,
}

/// The durable storage of acceptor heads and WAL.
#[async_trait]
pub trait WalStore: Send + Sync + 'static {
    /// The head, if the timeline exists.
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error>;

    /// Create the timeline, or return the existing head unchanged.
    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error>;

    /// Record what the greeting taught: the server info (a system id or a
    /// Postgres version learned) and a higher membership configuration.
    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error>;

    /// Grant a vote if `term` is above the stored term; durable before
    /// returning. Returns whether the vote was given.
    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error>;

    /// Adopt the elected proposer's history and truncate the WAL at
    /// `start_streaming_at`, in one step ([`apply_elected`]).
    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error>;

    /// Fenced, contiguous append ([`apply_append`]); durable on `Ok(Ok(_))`.
    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error>;

    /// Persist a commit LSN learned from heartbeats (never above the WAL end,
    /// never lowered). Off the commit path: the acceptor coalesces these.
    async fn record_commit_lsn(&self, tl: &TimelineId, commit_lsn: Lsn) -> Result<(), Error>;

    /// Persist the pageserver's `remote_consistent_lsn` (never lowered).
    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error>;

    /// WAL from `from` on, up to `max_bytes` and never past the stored WAL
    /// end, as `(start_lsn, bytes)` pieces in order. Empty when `from` is at
    /// the end. An error if `from` was trimmed.
    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error>;

    /// Delete WAL below `lsn`, clamped to what the bucket copy, the
    /// pageserver and the commit point have passed. Returns the new low mark.
    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error>;
}

/// The vote rule: grant iff `term` is higher. Mutates the head on a grant.
pub fn apply_vote(st: &mut AcceptorState, term: Term) -> bool {
    if st.term < term {
        st.term = term;
        true
    } else {
        false
    }
}

/// The `ProposerElected` rule (Neon's `handle_elected`). Mutates the head and
/// returns the LSN to truncate the stored WAL at, or `Deposed` (head
/// untouched). On an error the caller discards the mutated copy, as an
/// aborted transaction would.
pub fn apply_elected(
    st: &mut AcceptorState,
    msg: &ProposerElected,
) -> Result<Result<Lsn, Deposed>, Error> {
    if st.term > msg.term {
        return Ok(Err(Deposed { current: st.term }));
    }
    st.term = msg.term;

    let lcp = TermHistory::find_highest_common_point(
        &msg.term_history,
        &st.stored_term_history(),
        st.wal_end(),
    )?;
    let lcp =
        match lcp {
            Some(p) => p,
            None => *msg.term_history.0.first().ok_or_else(|| {
                Error::Protocol("ProposerElected with an empty term history".into())
            })?,
        };
    if lcp.lsn != msg.start_streaming_at {
        return Err(Error::Protocol(format!(
            "ProposerElected: truncation point {} differs from the common point {:?} \
             (acceptor history {:?}, WAL end {}); the proposer reconnects",
            msg.start_streaming_at,
            lcp,
            st.stored_term_history(),
            st.wal_end()
        )));
    }
    if msg.start_streaming_at < st.commit_lsn {
        return Err(Error::Protocol(format!(
            "ProposerElected would truncate committed WAL: start {} < commit {}",
            msg.start_streaming_at, st.commit_lsn
        )));
    }

    let start = msg.term_history.0.first().map_or(Lsn::INVALID, |e| e.lsn);
    if st.timeline_start_lsn == Lsn::INVALID {
        st.timeline_start_lsn = start;
    }
    if st.peer_horizon_lsn == Lsn::INVALID {
        st.peer_horizon_lsn = st.timeline_start_lsn;
    }
    // The first WAL this acceptor will hold (Neon keys this on
    // `local_start_lsn == 0`; an empty WAL is the sturdier test).
    if st.flush_lsn == Lsn::INVALID {
        if st.local_start_lsn == Lsn::INVALID {
            st.local_start_lsn = msg.start_streaming_at;
        }
        if st.trimmed_lsn == Lsn::INVALID {
            st.trimmed_lsn = st.local_start_lsn;
        }
    }
    st.commit_lsn = st.commit_lsn.max(st.timeline_start_lsn);
    st.backup_lsn = st.backup_lsn.max(st.timeline_start_lsn);
    st.remote_consistent_lsn = st.remote_consistent_lsn.max(st.timeline_start_lsn);
    st.flush_lsn = msg.start_streaming_at;
    st.term_history = msg.term_history.clone();
    Ok(Ok(msg.start_streaming_at))
}

/// The append rule (Neon's `handle_append_request`, plus retry idempotence).
///
/// - A higher stored term deposes the proposer.
/// - A lower stored term means no `ProposerElected` was seen: an error.
/// - WAL already stored (a retried write of the same term) is skipped.
/// - WAL starting above the stored end is a gap: an error.
///
/// Mutates the head (`flush_lsn`, `commit_lsn`, `peer_horizon_lsn`) and
/// returns which bytes the store still has to write.
pub fn apply_append(
    st: &mut AcceptorState,
    batch: &AppendBatch,
) -> Result<Result<AppendPlan, Deposed>, Error> {
    if st.term > batch.term {
        return Ok(Err(Deposed { current: st.term }));
    }
    if st.term < batch.term {
        return Err(Error::Protocol(format!(
            "AppendRequest of term {} before ProposerElected (acceptor term {})",
            batch.term, st.term
        )));
    }
    let end = batch.end_lsn()?;
    let flush = st.flush_lsn;
    let plan = if batch.is_empty() || end <= flush {
        AppendPlan {
            write_from: end,
            skip: batch.len(),
        }
    } else if batch.begin_lsn < flush {
        AppendPlan {
            write_from: flush,
            skip: flush.0 - batch.begin_lsn.0,
        }
    } else if batch.begin_lsn > flush && flush != Lsn::INVALID {
        return Err(Error::Protocol(format!(
            "AppendRequest at {} leaves a gap after the WAL end {flush}",
            batch.begin_lsn
        )));
    } else {
        AppendPlan {
            write_from: batch.begin_lsn,
            skip: 0,
        }
    };
    st.flush_lsn = st.flush_lsn.max(end);
    if batch.commit_lsn != Lsn::INVALID {
        st.commit_lsn = st.commit_lsn.max(batch.commit_lsn.min(st.wal_end()));
    }
    st.peer_horizon_lsn = st.peer_horizon_lsn.max(batch.truncate_lsn);
    Ok(Ok(plan))
}

/// The bytes of `batch` from `plan.skip` on, as chunks.
pub fn remaining_chunks(batch: &AppendBatch, plan: &AppendPlan) -> Vec<Bytes> {
    let mut skip = plan.skip;
    let mut out = Vec::with_capacity(batch.wal.len());
    for c in &batch.wal {
        let len = c.len() as u64;
        if skip >= len {
            skip -= len;
            continue;
        }
        out.push(c.slice(skip as usize..));
        skip = 0;
    }
    out
}

/// The trim bound: never above what the bucket copy, the pageserver and the
/// commit point have all passed, nor above the WAL end.
pub fn trim_bound(st: &AcceptorState, want: Lsn) -> Lsn {
    want.min(st.backup_lsn)
        .min(st.remote_consistent_lsn)
        .min(st.commit_lsn)
        .min(st.wal_end())
        .max(st.trimmed_lsn)
}

#[derive(Debug, Default)]
struct MemTimeline {
    head: AcceptorState,
    /// begin LSN → chunk.
    wal: BTreeMap<u64, Bytes>,
}

impl MemTimeline {
    fn truncate(&mut self, at: Lsn) {
        self.wal.split_off(&at.0);
        if let Some((&begin, chunk)) = self.wal.iter_mut().next_back() {
            let end = begin + chunk.len() as u64;
            if end > at.0 {
                *chunk = chunk.slice(..(at.0 - begin) as usize);
            }
        }
    }
}

/// An in-memory [`WalStore`], for tests and the protocol harness. Nothing is
/// durable across a process restart.
#[derive(Debug, Default)]
pub struct MemWalStore {
    inner: Mutex<HashMap<TimelineId, MemTimeline>>,
}

impl MemWalStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn with<T>(
        &self,
        tl: &TimelineId,
        f: impl FnOnce(&mut MemTimeline) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut map = self
            .inner
            .lock()
            .map_err(|_| Error::Store("poisoned lock".into()))?;
        let t = map.get_mut(tl).ok_or(Error::NotFound(*tl))?;
        // Apply to a copy, so that an error leaves the head untouched (as an
        // aborted transaction would).
        let mut copy = MemTimeline {
            head: t.head.clone(),
            wal: t.wal.clone(),
        };
        let out = f(&mut copy)?;
        *t = copy;
        Ok(out)
    }
}

#[async_trait]
impl WalStore for MemWalStore {
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error> {
        let map = self
            .inner
            .lock()
            .map_err(|_| Error::Store("poisoned lock".into()))?;
        Ok(map.get(tl).map(|t| t.head.clone()))
    }

    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error> {
        let mut map = self
            .inner
            .lock()
            .map_err(|_| Error::Store("poisoned lock".into()))?;
        let t = map.entry(*tl).or_insert_with(|| MemTimeline {
            head: AcceptorState::new(server, start_lsn),
            wal: BTreeMap::new(),
        });
        Ok(t.head.clone())
    }

    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error> {
        self.with(tl, |t| {
            t.head.server = server;
            if let Some(m) = mconf {
                t.head.mconf = m;
            }
            Ok(t.head.clone())
        })
    }

    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error> {
        self.with(tl, |t| {
            let given = apply_vote(&mut t.head, term);
            Ok((given, t.head.clone()))
        })
    }

    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        self.with(tl, |t| match apply_elected(&mut t.head, msg)? {
            Err(d) => Ok(Err(d)),
            Ok(at) => {
                t.truncate(at);
                Ok(Ok(t.head.clone()))
            }
        })
    }

    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        self.with(tl, |t| match apply_append(&mut t.head, batch)? {
            Err(d) => Ok(Err(d)),
            Ok(plan) => {
                let mut at = plan.write_from.0;
                for c in remaining_chunks(batch, &plan) {
                    let len = c.len() as u64;
                    if len > 0 {
                        t.wal.insert(at, c);
                    }
                    at += len;
                }
                Ok(Ok(t.head.clone()))
            }
        })
    }

    async fn record_commit_lsn(&self, tl: &TimelineId, commit_lsn: Lsn) -> Result<(), Error> {
        self.with(tl, |t| {
            t.head.commit_lsn = t.head.commit_lsn.max(commit_lsn.min(t.head.wal_end()));
            Ok(())
        })
    }

    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        self.with(tl, |t| {
            t.head.remote_consistent_lsn = t.head.remote_consistent_lsn.max(lsn);
            Ok(())
        })
    }

    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error> {
        let map = self
            .inner
            .lock()
            .map_err(|_| Error::Store("poisoned lock".into()))?;
        let t = map.get(tl).ok_or(Error::NotFound(*tl))?;
        read_chunks(
            &t.head,
            t.wal.range(..).map(|(k, v)| (*k, v.clone())),
            from,
            max_bytes,
        )
    }

    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error> {
        self.with(tl, |t| {
            let bound = trim_bound(&t.head, lsn);
            // Keep the chunk that contains `bound`.
            let keep_from = t
                .wal
                .range(..=bound.0)
                .next_back()
                .filter(|(b, c)| *b + c.len() as u64 > bound.0)
                .map_or(bound.0, |(b, _)| *b);
            t.wal = t.wal.split_off(&keep_from);
            t.head.trimmed_lsn = bound;
            Ok(bound)
        })
    }
}

/// Slice stored chunks (ascending by begin LSN) into the pieces a reader
/// wants: from `from`, at most `max_bytes`, never past `head.flush_lsn`.
pub fn read_chunks(
    head: &AcceptorState,
    chunks: impl IntoIterator<Item = (u64, Bytes)>,
    from: Lsn,
    max_bytes: usize,
) -> Result<Vec<(Lsn, Bytes)>, Error> {
    if from < head.trimmed_lsn {
        return Err(Error::Trimmed {
            from,
            trimmed: head.trimmed_lsn,
        });
    }
    let end = head.flush_lsn;
    let mut out = Vec::new();
    let mut at = from.0;
    let mut budget = max_bytes as u64;
    for (begin, chunk) in chunks {
        if budget == 0 || at >= end.0 {
            break;
        }
        let chunk_end = (begin + chunk.len() as u64).min(end.0);
        if chunk_end <= at {
            continue;
        }
        if begin > at {
            return Err(Error::Store(format!(
                "WAL hole at {} (next chunk at {begin:#x})",
                Lsn(at)
            )));
        }
        let lo = at - begin;
        let hi = (chunk_end - begin).min(lo.saturating_add(budget));
        out.push((Lsn(at), chunk.slice(lo as usize..hi as usize)));
        budget -= hi - lo;
        at = begin + hi;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Id, TermLsn};

    fn tl() -> TimelineId {
        TimelineId::new(Id([1; 16]), Id([2; 16]))
    }

    fn elected(term: Term, start: u64, history: &[(Term, u64)]) -> ProposerElected {
        ProposerElected {
            generation: 0,
            term,
            start_streaming_at: Lsn(start),
            term_history: TermHistory(
                history
                    .iter()
                    .map(|&(t, l)| TermLsn {
                        term: t,
                        lsn: Lsn(l),
                    })
                    .collect(),
            ),
        }
    }

    fn batch(term: Term, begin: u64, data: &'static [u8], commit: u64) -> AppendBatch {
        AppendBatch {
            term,
            begin_lsn: Lsn(begin),
            wal: vec![Bytes::from_static(data)],
            commit_lsn: Lsn(commit),
            truncate_lsn: Lsn::INVALID,
        }
    }

    async fn read_all(s: &MemWalStore, from: u64) -> Vec<u8> {
        s.read(&tl(), Lsn(from), usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b.to_vec())
            .collect()
    }

    #[tokio::test]
    async fn vote_is_durable_and_once_per_term() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        assert!(s.vote(&tl(), 1).await.unwrap().0);
        // A "restart": state comes from the store.
        assert_eq!(s.load(&tl()).await.unwrap().unwrap().term, 1);
        assert!(!s.vote(&tl(), 1).await.unwrap().0);
        assert!(s.vote(&tl(), 2).await.unwrap().0);
    }

    #[tokio::test]
    async fn append_after_elected_and_fencing() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 10, &[(1, 10)]))
            .await
            .unwrap()
            .unwrap();
        let st = s
            .append(&tl(), &batch(1, 10, b"abcde", 0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.flush_lsn, Lsn(15));
        assert_eq!(st.commit_lsn, Lsn(10));

        // A new proposer votes term 2: the old one is fenced.
        s.vote(&tl(), 2).await.unwrap();
        let d = s.append(&tl(), &batch(1, 15, b"fg", 15)).await.unwrap();
        assert_eq!(d, Err(Deposed { current: 2 }));
        assert_eq!(read_all(&s, 10).await, b"abcde");
    }

    #[tokio::test]
    async fn append_before_elected_is_an_error() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        assert!(s.append(&tl(), &batch(3, 0, b"x", 0)).await.is_err());
    }

    #[tokio::test]
    async fn retried_append_is_idempotent_and_gaps_fail() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 0, b"abc", 0))
            .await
            .unwrap()
            .unwrap();
        // Exact retry and an overlapping retry.
        s.append(&tl(), &batch(1, 0, b"abc", 0))
            .await
            .unwrap()
            .unwrap();
        let st = s
            .append(&tl(), &batch(1, 1, b"bcdef", 0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.flush_lsn, Lsn(6));
        assert_eq!(read_all(&s, 0).await, b"abcdef");
        // A gap: the head is unchanged.
        assert!(s.append(&tl(), &batch(1, 9, b"z", 0)).await.is_err());
        assert_eq!(s.load(&tl()).await.unwrap().unwrap().flush_lsn, Lsn(6));
    }

    #[tokio::test]
    async fn commit_lsn_never_passes_the_wal_end_nor_goes_back() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        let st = s
            .append(&tl(), &batch(1, 0, b"abc", 99))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.commit_lsn, Lsn(3));
        s.record_commit_lsn(&tl(), Lsn(1)).await.unwrap();
        assert_eq!(s.load(&tl()).await.unwrap().unwrap().commit_lsn, Lsn(3));
    }

    #[tokio::test]
    async fn elected_truncates_uncommitted_wal_of_an_old_term() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 0, b"abcdef", 2))
            .await
            .unwrap()
            .unwrap();
        s.vote(&tl(), 2).await.unwrap();
        // The new proposer's history says term 1 ended at 4.
        let st = s
            .elected(&tl(), &elected(2, 4, &[(1, 0), (2, 4)]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.flush_lsn, Lsn(4));
        assert_eq!(read_all(&s, 0).await, b"abcd");
        s.append(&tl(), &batch(2, 4, b"XY", 0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_all(&s, 0).await, b"abcdXY");
        assert_eq!(s.load(&tl()).await.unwrap().unwrap().last_log_term(), 2);
    }

    #[tokio::test]
    async fn elected_refuses_to_truncate_committed_wal() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 0, b"abcdef", 5))
            .await
            .unwrap()
            .unwrap();
        s.vote(&tl(), 2).await.unwrap();
        assert!(
            s.elected(&tl(), &elected(2, 4, &[(1, 0), (2, 4)]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn elected_with_wrong_start_is_refused() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 0, &[(1, 0)]))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 0, b"abcdef", 0))
            .await
            .unwrap()
            .unwrap();
        s.vote(&tl(), 2).await.unwrap();
        // The common point is 6 (the acceptor's end), not 5.
        assert!(
            s.elected(&tl(), &elected(2, 5, &[(1, 0), (2, 7)]))
                .await
                .is_err()
        );
        s.elected(&tl(), &elected(2, 6, &[(1, 0), (2, 7)]))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn read_respects_budget_and_end_and_trim() {
        let s = MemWalStore::new();
        s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
            .await
            .unwrap();
        s.vote(&tl(), 1).await.unwrap();
        s.elected(&tl(), &elected(1, 100, &[(1, 100)]))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 100, b"0123", 0))
            .await
            .unwrap()
            .unwrap();
        s.append(&tl(), &batch(1, 104, b"4567", 0))
            .await
            .unwrap()
            .unwrap();
        let got = s.read(&tl(), Lsn(102), 3).await.unwrap();
        assert_eq!(
            got,
            vec![
                (Lsn(102), Bytes::from_static(b"23")),
                (Lsn(104), Bytes::from_static(b"4"))
            ]
        );
        assert!(s.read(&tl(), Lsn(108), 10).await.unwrap().is_empty());

        // Trim is clamped by commit, backup and remote_consistent LSNs.
        s.record_commit_lsn(&tl(), Lsn(106)).await.unwrap();
        assert_eq!(s.trim(&tl(), Lsn(106)).await.unwrap(), Lsn(100));
        s.with(&tl(), |t| {
            t.head.backup_lsn = Lsn(106);
            t.head.remote_consistent_lsn = Lsn(105);
            Ok(())
        })
        .unwrap();
        assert_eq!(s.trim(&tl(), Lsn(106)).await.unwrap(), Lsn(105));
        assert!(s.read(&tl(), Lsn(104), 10).await.is_err());
        assert_eq!(read_all(&s, 105).await, b"567");
    }
}

#[cfg(test)]
mod props {
    use proptest::prelude::*;

    use super::*;
    use crate::types::{Id, TermLsn};

    proptest! {
        /// Any split of a WAL stream into requests, with any retried
        /// (overlapping) prefixes, stores exactly the stream.
        #[test]
        fn chunked_appends_with_retries_store_the_stream(
            data in proptest::collection::vec(any::<u8>(), 1..400),
            cuts in proptest::collection::vec(1usize..64, 1..20),
            retries in proptest::collection::vec(0usize..16, 1..20),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async {
                let tl = TimelineId::new(Id([3; 16]), Id([4; 16]));
                let s = MemWalStore::new();
                s.create(&tl, ServerInfo::default(), Lsn::INVALID).await.unwrap();
                s.vote(&tl, 1).await.unwrap();
                let start = 1000u64;
                let el = ProposerElected {
                    generation: 0,
                    term: 1,
                    start_streaming_at: Lsn(start),
                    term_history: TermHistory(vec![TermLsn { term: 1, lsn: Lsn(start) }]),
                };
                s.elected(&tl, &el).await.unwrap().unwrap();
                let mut at = 0usize;
                let mut i = 0usize;
                while at < data.len() {
                    let len = cuts[i % cuts.len()].min(data.len() - at);
                    let back = retries[i % retries.len()].min(at);
                    let from = at - back;
                    let b = AppendBatch {
                        term: 1,
                        begin_lsn: Lsn(start + from as u64),
                        wal: vec![Bytes::copy_from_slice(&data[from..at + len])],
                        commit_lsn: Lsn::INVALID,
                        truncate_lsn: Lsn::INVALID,
                    };
                    s.append(&tl, &b).await.unwrap().unwrap();
                    at += len;
                    i += 1;
                }
                let got: Vec<u8> = s
                    .read(&tl, Lsn(start), usize::MAX)
                    .await
                    .unwrap()
                    .into_iter()
                    .flat_map(|(_, b)| b.to_vec())
                    .collect();
                assert_eq!(got, data);
            });
        }
    }

    #[test]
    fn head_postcard_round_trip() {
        let mut st = AcceptorState::new(
            ServerInfo {
                pg_version: 160_009,
                system_id: 7,
                wal_seg_size: 16 << 20,
            },
            Lsn(0x16B_5A00),
        );
        st.term = 3;
        st.term_history = TermHistory(vec![TermLsn {
            term: 3,
            lsn: Lsn(0x16B_5A00),
        }]);
        let bytes = postcard::to_stdvec(&st).unwrap();
        assert_eq!(postcard::from_bytes::<AcceptorState>(&bytes).unwrap(), st);
    }
}
