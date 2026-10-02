//! The acceptor: one proposer connection's view of a timeline.
//!
//! Ported from Neon's `SafeKeeper::process_msg` (`safekeeper/src/safekeeper.rs`,
//! Apache-2.0), with the durable state in a [`WalStore`] instead of a control
//! file and a local WAL directory. Two things differ by design (§28 §6.3–§6.5):
//!
//! - **Group commit.** The caller hands [`Acceptor::handle_appends`] every
//!   `AppendRequest` queued while the previous write was in flight; they become
//!   one fenced store write and one `AppendResponse`, as Neon's safekeeper
//!   fsyncs once when its queue drains.
//! - **Heartbeats stay off the store.** An empty `AppendRequest` only carries
//!   `commit_lsn`. It is applied in memory and persisted with the next WAL
//!   write, by [`Acceptor::persist_commit_lsn`] on a timer, or at once when it
//!   crosses the term start (which `--sync-safekeepers` waits for). Neon keeps
//!   `commit_lsn` in memory the same way.

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::Error;
use crate::proto::{
    AcceptorGreeting, AcceptorMessage, AppendRequest, AppendResponse, ProposerElected,
    ProposerGreeting, VoteRequest, VoteResponse,
};
use crate::store::{AppendBatch, WalStore};
use crate::types::{AcceptorState, HotStandbyFeedback, Lsn, NodeId, ServerInfo, TimelineId};

/// One proposer connection's acceptor.
#[derive(Debug)]
pub struct Acceptor<S> {
    store: Arc<S>,
    node_id: NodeId,
    tl: TimelineId,
    /// The head as last read from or written to the store.
    state: AcceptorState,
    /// The commit LSN known in memory (at or above `state.commit_lsn`).
    commit_lsn: Lsn,
    /// The proposer's `truncate_lsn`, in memory.
    peer_horizon_lsn: Lsn,
    /// Where the current term's WAL starts; `None` before `ProposerElected`.
    term_start_lsn: Option<Lsn>,
    /// The end of the last append handed to the store (in flight or done).
    issued_end: Lsn,
}

impl PendingAppends {
    /// The proposer term the append was issued under.
    pub fn term(&self) -> crate::types::Term {
        self.term
    }
}

/// What [`Acceptor::begin_appends`] decided.
#[derive(Debug)]
pub enum Appends {
    /// Answer now; the store is not involved.
    Reply(AcceptorMessage),
    /// Write `batch` (if any), then call [`Acceptor::finish_appends`].
    Pending(PendingAppends),
}

/// An append between [`Acceptor::begin_appends`] and
/// [`Acceptor::finish_appends`].
#[derive(Debug)]
pub struct PendingAppends {
    /// The store write; `None` for heartbeats.
    pub batch: Option<AppendBatch>,
    term: crate::types::Term,
    term_start_lsn: Lsn,
    commit_lsn: Lsn,
    truncate_lsn: Lsn,
}

impl<S: WalStore> Acceptor<S> {
    /// Handle the `ProposerGreeting` that opens a `START_WAL_PUSH` stream:
    /// load (or, if allowed, create) the timeline, check the server, and
    /// answer with the acceptor's term.
    pub async fn greet(
        store: Arc<S>,
        node_id: NodeId,
        msg: &ProposerGreeting,
        allow_timeline_creation: bool,
    ) -> Result<(Self, AcceptorMessage), Error> {
        let tl = msg.timeline();
        let server = ServerInfo {
            pg_version: msg.pg_version,
            system_id: msg.system_id,
            wal_seg_size: msg.wal_seg_size,
        };
        let mut state = match store.load(&tl).await? {
            Some(st) => st,
            None if allow_timeline_creation => store.create(&tl, server, Lsn::INVALID).await?,
            None => return Err(Error::NotFound(tl)),
        };

        if let (Some(have), Some(got)) = (state.server.pg_major(), server.pg_major())
            && have != got
        {
            return Err(Error::Protocol(format!(
                "timeline {tl} is Postgres {have}, the compute is Postgres {got}"
            )));
        }
        if state.server.wal_seg_size != msg.wal_seg_size {
            return Err(Error::Protocol(format!(
                "timeline {tl} has wal_seg_size {}, the compute {}",
                state.server.wal_seg_size, msg.wal_seg_size
            )));
        }

        // sync-safekeepers sends system_id 0; ignore that.
        let mut new_server = state.server;
        if msg.system_id != 0 && state.server.system_id != msg.system_id {
            if state.server.system_id != 0 {
                warn!(%tl, have = state.server.system_id, got = msg.system_id, "system id changed");
            }
            new_server.system_id = msg.system_id;
            if msg.pg_version != 0 {
                new_server.pg_version = msg.pg_version;
            }
        }
        let new_mconf = if msg.mconf.generation > state.mconf.generation {
            if !msg.mconf.contains(node_id) {
                return Err(Error::Protocol(format!(
                    "refusing generation {}: node {node_id} is not a member",
                    msg.mconf.generation
                )));
            }
            Some(msg.mconf.clone())
        } else {
            None
        };
        if new_server != state.server || new_mconf.is_some() {
            state = store.update_meta(&tl, new_server, new_mconf).await?;
        }

        let reply = AcceptorMessage::Greeting(AcceptorGreeting {
            node_id,
            mconf: state.mconf.clone(),
            term: state.term,
        });
        info!(%tl, term = state.term, flush_lsn = %state.flush_lsn, "greeted walproposer");
        let commit_lsn = state.commit_lsn;
        let peer_horizon_lsn = state.peer_horizon_lsn;
        Ok((
            Self {
                store,
                node_id,
                tl,
                state,
                commit_lsn,
                peer_horizon_lsn,
                term_start_lsn: None,
                issued_end: Lsn::INVALID,
            },
            reply,
        ))
    }

    pub fn timeline(&self) -> TimelineId {
        self.tl
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The head as last seen, with the in-memory commit LSN.
    pub fn state(&self) -> AcceptorState {
        let mut st = self.state.clone();
        st.commit_lsn = self.commit_lsn;
        st.peer_horizon_lsn = self.peer_horizon_lsn;
        st
    }

    fn check_generation(&self, generation: u32, what: &str) -> Result<(), Error> {
        if generation != self.state.mconf.generation {
            return Err(Error::Protocol(format!(
                "refusing {what} of generation {generation}: acceptor generation {}",
                self.state.mconf.generation
            )));
        }
        Ok(())
    }

    /// `VoteRequest`: vote at most once per term, durably.
    pub async fn handle_vote(&mut self, msg: &VoteRequest) -> Result<AcceptorMessage, Error> {
        self.check_generation(msg.generation, "VoteRequest")?;
        let (given, st) = self.store.vote(&self.tl, msg.term).await?;
        self.adopt(st);
        let resp = VoteResponse {
            generation: self.state.mconf.generation,
            term: self.state.term,
            vote_given: given,
            flush_lsn: self.state.wal_end(),
            truncate_lsn: self.peer_horizon_lsn,
            term_history: self.state.stored_term_history(),
        };
        info!(tl = %self.tl, term = msg.term, given, flush_lsn = %resp.flush_lsn, "vote");
        Ok(AcceptorMessage::VoteResponse(resp))
    }

    /// `ProposerElected`: adopt the history and truncate. No reply; a
    /// deposed proposer learns the higher term from the next response.
    pub async fn handle_elected(&mut self, msg: &ProposerElected) -> Result<(), Error> {
        self.check_generation(msg.generation, "ProposerElected")?;
        match self.store.elected(&self.tl, msg).await? {
            Err(d) => {
                self.state.term = d.current;
                debug!(tl = %self.tl, term = msg.term, current = d.current, "stale ProposerElected");
            }
            Ok(st) => {
                self.adopt(st);
                self.issued_end = self.state.flush_lsn;
                self.term_start_lsn = Some(
                    msg.term_history
                        .0
                        .last()
                        .map(|e| e.lsn)
                        .ok_or_else(|| Error::Protocol("empty term history".into()))?,
                );
                info!(tl = %self.tl, term = msg.term, start = %msg.start_streaming_at, "elected");
            }
        }
        Ok(())
    }

    /// A run of queued `AppendRequest`s: one fenced store write for their WAL,
    /// and one response. Heartbeats (no WAL) do not touch the store.
    pub async fn handle_appends(
        &mut self,
        reqs: &[AppendRequest],
    ) -> Result<AcceptorMessage, Error> {
        match self.begin_appends(reqs)? {
            Appends::Reply(r) => Ok(r),
            Appends::Pending(p) => {
                let res = match &p.batch {
                    Some(b) => Some(self.store.append(&self.tl, b).await),
                    None => None,
                };
                self.finish_appends(p, res).await
            }
        }
    }

    /// The first half of [`Self::handle_appends`], without the store: check
    /// the requests and build the store write. The service issues the write
    /// (several may be in flight, [`WalStore::max_in_flight`]) and hands its
    /// outcome to [`Self::finish_appends`], in issue order.
    pub fn begin_appends(&mut self, reqs: &[AppendRequest]) -> Result<Appends, Error> {
        let Some(first) = reqs.first() else {
            return Ok(Appends::Reply(AcceptorMessage::AppendResponse(
                self.append_response(),
            )));
        };
        for r in reqs {
            self.check_generation(r.h.generation, "AppendRequest")?;
            if r.h.begin_lsn.checked_add(r.wal.len() as u64)? != r.h.end_lsn {
                return Err(Error::Protocol(format!(
                    "AppendRequest [{}, {}) carries {} WAL bytes",
                    r.h.begin_lsn,
                    r.h.end_lsn,
                    r.wal.len()
                )));
            }
            if r.h.term != first.h.term {
                return Err(Error::Protocol(
                    "AppendRequests of two terms in one batch".into(),
                ));
            }
        }
        let term = first.h.term;
        if self.state.term > term {
            return Ok(Appends::Reply(AcceptorMessage::AppendResponse(
                AppendResponse::term_only(self.state.mconf.generation, self.state.term),
            )));
        }
        let Some(term_start_lsn) = self.term_start_lsn.filter(|_| self.state.term == term) else {
            return Err(Error::Protocol(format!(
                "AppendRequest of term {term} before ProposerElected"
            )));
        };

        let commit_lsn = reqs
            .iter()
            .map(|r| r.h.commit_lsn)
            .max()
            .unwrap_or_default();
        let truncate_lsn = reqs
            .iter()
            .map(|r| r.h.truncate_lsn)
            .max()
            .unwrap_or_default();
        let data: Vec<&AppendRequest> = reqs.iter().filter(|r| !r.wal.is_empty()).collect();
        let mut batch = None;
        if let Some(d0) = data.first() {
            let mut at = d0.h.begin_lsn;
            for r in &data {
                if r.h.begin_lsn != at {
                    return Err(Error::Protocol(format!(
                        "non-contiguous AppendRequests: {} after {at}",
                        r.h.begin_lsn
                    )));
                }
                at = r.h.end_lsn;
            }
            // Appends in flight have not reached `state` yet: the stream
            // continues from the end of the last one issued.
            if self.issued_end != Lsn::INVALID && d0.h.begin_lsn > self.issued_end {
                return Err(Error::Protocol(format!(
                    "AppendRequest at {} leaves a gap after the WAL end {}",
                    d0.h.begin_lsn, self.issued_end
                )));
            }
            self.issued_end = self.issued_end.max(at);
            batch = Some(AppendBatch {
                term,
                begin_lsn: d0.h.begin_lsn,
                wal: data.iter().map(|r| r.wal.clone()).collect(),
                commit_lsn: commit_lsn.max(self.commit_lsn),
                truncate_lsn: truncate_lsn.max(self.peer_horizon_lsn),
            });
        }
        Ok(Appends::Pending(PendingAppends {
            batch,
            term,
            term_start_lsn,
            commit_lsn,
            truncate_lsn,
        }))
    }

    /// The second half of [`Self::handle_appends`]: adopt the store write's
    /// outcome (`None` when there was no WAL to write) and answer.
    pub async fn finish_appends(
        &mut self,
        p: PendingAppends,
        res: Option<Result<Result<AcceptorState, crate::store::Deposed>, Error>>,
    ) -> Result<AcceptorMessage, Error> {
        match res {
            None => {}
            Some(Err(e)) => return Err(e),
            Some(Ok(Err(d))) => {
                self.state.term = self.state.term.max(d.current);
                return Ok(AcceptorMessage::AppendResponse(AppendResponse::term_only(
                    self.state.mconf.generation,
                    self.state.term,
                )));
            }
            Some(Ok(Ok(st))) => {
                // Completions may report a flush LSN below one already
                // adopted (a pipelined store's contiguous end): never go back.
                let flush = self.state.flush_lsn.max(st.flush_lsn);
                self.adopt(st);
                self.state.flush_lsn = flush;
            }
        }
        // An earlier completion in the pipeline deposed this proposer.
        if self.state.term != p.term {
            return Ok(AcceptorMessage::AppendResponse(AppendResponse::term_only(
                self.state.mconf.generation,
                self.state.term,
            )));
        }

        self.peer_horizon_lsn = self.peer_horizon_lsn.max(p.truncate_lsn);
        if p.commit_lsn != Lsn::INVALID {
            let c = p.commit_lsn.max(self.commit_lsn).min(self.state.wal_end());
            self.commit_lsn = self.commit_lsn.max(c);
        }
        // sync-safekeepers waits for commit_lsn to reach the term start; make
        // that durable at once, as Neon does.
        if self.commit_lsn >= p.term_start_lsn && self.state.commit_lsn < p.term_start_lsn {
            self.persist_commit_lsn().await?;
        }
        Ok(AcceptorMessage::AppendResponse(self.append_response()))
    }

    /// The store, for the service's in-flight writes.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// Persist the in-memory commit LSN if it is ahead of the store's,
    /// fenced by the current term (a deposed proposer's is dropped).
    pub async fn persist_commit_lsn(&mut self) -> Result<(), Error> {
        if self.commit_lsn > self.state.commit_lsn {
            match self
                .store
                .record_commit_lsn(&self.tl, self.state.term, self.commit_lsn)
                .await?
            {
                Ok(()) => self.state.commit_lsn = self.commit_lsn,
                Err(d) => {
                    self.state.term = d.current;
                    self.commit_lsn = self.state.commit_lsn;
                }
            }
        }
        Ok(())
    }

    /// Learn a higher term seen elsewhere on this instance (another
    /// connection's proposer). Heartbeats never touch the store, so without
    /// this a deposed, idle proposer would keep hearing its own term.
    pub fn observe_term(&mut self, term: crate::types::Term) {
        self.state.term = self.state.term.max(term);
    }

    /// Re-read the head and adopt a higher stored term (a proposer elected on
    /// another instance). The service calls this on its commit timer, so a
    /// deposed proposer that only sends heartbeats learns within one interval.
    pub async fn refresh(&mut self) -> Result<(), Error> {
        if let Some(st) = self.store.load(&self.tl).await?
            && st.term > self.state.term
        {
            info!(tl = %self.tl, term = self.state.term, stored = st.term, "deposed");
            self.state.term = st.term;
        }
        Ok(())
    }

    /// Whether [`Self::persist_commit_lsn`] has anything to write.
    pub fn commit_lsn_dirty(&self) -> bool {
        self.commit_lsn > self.state.commit_lsn
    }

    /// The current `AppendResponse` (without feedback).
    pub fn append_response(&self) -> AppendResponse {
        AppendResponse {
            generation: self.state.mconf.generation,
            term: self.state.term,
            flush_lsn: self.state.wal_end(),
            commit_lsn: self.commit_lsn,
            hs_feedback: HotStandbyFeedback::default(),
            pageserver_feedback: None,
        }
    }

    fn adopt(&mut self, st: AcceptorState) {
        self.commit_lsn = self.commit_lsn.max(st.commit_lsn);
        self.peer_horizon_lsn = self.peer_horizon_lsn.max(st.peer_horizon_lsn);
        self.state = st;
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::proto::AppendRequestHeader;
    use crate::store::MemWalStore;
    use crate::types::{Configuration, Id, TermHistory, TermLsn};

    const SEG: u32 = 16 << 20;

    fn greeting() -> ProposerGreeting {
        ProposerGreeting {
            tenant_id: Id([1; 16]),
            timeline_id: Id([1; 16]),
            mconf: Configuration::default(),
            pg_version: 160_009,
            system_id: 42,
            wal_seg_size: SEG,
        }
    }

    async fn open(store: &Arc<MemWalStore>) -> Acceptor<MemWalStore> {
        Acceptor::greet(store.clone(), 1, &greeting(), true)
            .await
            .unwrap()
            .0
    }

    fn append(term: u64, begin: u64, data: &'static [u8], commit: u64) -> AppendRequest {
        AppendRequest {
            h: AppendRequestHeader {
                generation: 0,
                term,
                begin_lsn: Lsn(begin),
                end_lsn: Lsn(begin + data.len() as u64),
                commit_lsn: Lsn(commit),
                truncate_lsn: Lsn(0),
            },
            wal: Bytes::from_static(data),
        }
    }

    fn elected(term: u64, start: u64, th: &[(u64, u64)]) -> ProposerElected {
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

    fn resp(m: AcceptorMessage) -> AppendResponse {
        match m {
            AcceptorMessage::AppendResponse(r) => r,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn greeting_creates_or_refuses() {
        let store = Arc::new(MemWalStore::new());
        let err = Acceptor::greet(store.clone(), 1, &greeting(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
        let (_, reply) = Acceptor::greet(store.clone(), 1, &greeting(), true)
            .await
            .unwrap();
        assert_eq!(
            reply,
            AcceptorMessage::Greeting(AcceptorGreeting {
                node_id: 1,
                mconf: Configuration::default(),
                term: 0
            })
        );
        // A different major version or segment size is refused.
        let mut g = greeting();
        g.pg_version = 170_005;
        assert!(Acceptor::greet(store.clone(), 1, &g, true).await.is_err());
        let mut g = greeting();
        g.wal_seg_size = 1 << 20;
        assert!(Acceptor::greet(store.clone(), 1, &g, true).await.is_err());
    }

    // Neon's test_voting.
    #[tokio::test]
    async fn voting() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        assert!(
            a.handle_vote(&VoteRequest {
                generation: 42,
                term: 1
            })
            .await
            .is_err()
        );
        let r = a
            .handle_vote(&VoteRequest {
                generation: 0,
                term: 1,
            })
            .await
            .unwrap();
        assert!(matches!(
            r,
            AcceptorMessage::VoteResponse(VoteResponse {
                vote_given: true,
                ..
            })
        ));
        // "Reboot": a new connection reads the term from the store.
        let mut a = open(&store).await;
        let r = a
            .handle_vote(&VoteRequest {
                generation: 0,
                term: 1,
            })
            .await
            .unwrap();
        assert!(matches!(
            r,
            AcceptorMessage::VoteResponse(VoteResponse {
                vote_given: false,
                ..
            })
        ));
    }

    // Neon's test_last_log_term_switch.
    #[tokio::test]
    async fn last_log_term_switch() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 2,
        })
        .await
        .unwrap();
        let mut bad = elected(2, 1, &[(1, 1), (2, 3)]);
        bad.generation = 42;
        assert!(a.handle_elected(&bad).await.is_err());
        a.handle_elected(&elected(2, 1, &[(1, 1), (2, 3)]))
            .await
            .unwrap();
        a.handle_appends(&[append(2, 1, b"b", 0)]).await.unwrap();
        assert_eq!(a.state().last_log_term(), 1);
        a.handle_appends(&[append(2, 2, b"b", 0)]).await.unwrap();
        assert_eq!(a.state().last_log_term(), 2);
    }

    // Neon's test_non_consecutive_write.
    #[tokio::test]
    async fn non_consecutive_write() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 1,
        })
        .await
        .unwrap();
        a.handle_elected(&elected(1, 1, &[(1, 1)])).await.unwrap();
        let mut bad = append(1, 1, b"b", 0);
        bad.h.generation = 42;
        assert!(a.handle_appends(&[bad]).await.is_err());
        a.handle_appends(&[append(1, 1, b"b", 0)]).await.unwrap();
        assert!(a.handle_appends(&[append(1, 4, b"b", 0)]).await.is_err());
        // Within one batch, too.
        assert!(
            a.handle_appends(&[append(1, 2, b"b", 0), append(1, 5, b"c", 0)])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn append_before_elected_is_refused() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 1,
        })
        .await
        .unwrap();
        assert!(a.handle_appends(&[append(1, 1, b"b", 0)]).await.is_err());
    }

    #[tokio::test]
    async fn group_commit_and_heartbeats() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 1,
        })
        .await
        .unwrap();
        a.handle_elected(&elected(1, 100, &[(1, 100)]))
            .await
            .unwrap();
        // Three queued requests: one write, one response at their end.
        let r = resp(
            a.handle_appends(&[
                append(1, 100, b"aa", 0),
                append(1, 102, b"bb", 0),
                append(1, 104, b"cc", 0),
            ])
            .await
            .unwrap(),
        );
        assert_eq!(r.flush_lsn, Lsn(106));
        assert_eq!(r.term, 1);

        // A heartbeat moves commit_lsn in memory only, clamped to the WAL end.
        let r = resp(a.handle_appends(&[append(1, 106, b"", 200)]).await.unwrap());
        assert_eq!(r.commit_lsn, Lsn(106));
        assert!(a.commit_lsn_dirty());
        a.persist_commit_lsn().await.unwrap();
        assert_eq!(
            store.load(&a.timeline()).await.unwrap().unwrap().commit_lsn,
            Lsn(106)
        );
    }

    #[tokio::test]
    async fn commit_reaching_term_start_is_persisted_at_once() {
        let store = Arc::new(MemWalStore::new());
        let mut a = open(&store).await;
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 1,
        })
        .await
        .unwrap();
        a.handle_elected(&elected(1, 10, &[(1, 10)])).await.unwrap();
        a.handle_appends(&[append(1, 10, b"abc", 0)]).await.unwrap();
        a.handle_vote(&VoteRequest {
            generation: 0,
            term: 2,
        })
        .await
        .unwrap();
        a.handle_elected(&elected(2, 13, &[(1, 10), (2, 13)]))
            .await
            .unwrap();
        a.handle_appends(&[append(2, 13, b"", 13)]).await.unwrap();
        assert!(!a.commit_lsn_dirty());
        assert_eq!(
            store.load(&a.timeline()).await.unwrap().unwrap().commit_lsn,
            Lsn(13)
        );
    }

    #[tokio::test]
    async fn deposed_proposer_gets_term_only_and_writes_nothing() {
        let store = Arc::new(MemWalStore::new());
        let mut old = open(&store).await;
        old.handle_vote(&VoteRequest {
            generation: 0,
            term: 1,
        })
        .await
        .unwrap();
        old.handle_elected(&elected(1, 0, &[(1, 0)])).await.unwrap();
        old.handle_appends(&[append(1, 0, b"ab", 0)]).await.unwrap();

        // A second compute (another pool instance) is elected in term 2.
        let mut new = open(&store).await;
        new.handle_vote(&VoteRequest {
            generation: 0,
            term: 2,
        })
        .await
        .unwrap();
        new.handle_elected(&elected(2, 2, &[(1, 0), (2, 2)]))
            .await
            .unwrap();

        // The old connection's cached term is 1, but the store fences it.
        let r = resp(old.handle_appends(&[append(1, 2, b"XX", 2)]).await.unwrap());
        assert_eq!(r, AppendResponse::term_only(0, 2));
        let r = resp(new.handle_appends(&[append(2, 2, b"cd", 0)]).await.unwrap());
        assert_eq!(r.flush_lsn, Lsn(4));
        let wal: Vec<u8> = store
            .read(&new.timeline(), Lsn(0), 100)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b.to_vec())
            .collect();
        assert_eq!(wal, b"abcd");
    }

    #[tokio::test]
    async fn greeting_learns_system_id_and_higher_generation() {
        let store = Arc::new(MemWalStore::new());
        let mut g = greeting();
        g.system_id = 0; // sync-safekeepers
        Acceptor::greet(store.clone(), 7, &g, true).await.unwrap();
        let (a, _) = Acceptor::greet(store.clone(), 7, &greeting(), true)
            .await
            .unwrap();
        assert_eq!(a.state().server.system_id, 42);

        let mut g = greeting();
        g.mconf = Configuration {
            generation: 2,
            members: vec![crate::types::SafekeeperId {
                id: 8,
                host: "x".into(),
                pg_port: 1,
            }],
            new_members: None,
        };
        assert!(Acceptor::greet(store.clone(), 7, &g, true).await.is_err());
        g.mconf.members[0].id = 7;
        let (a, _) = Acceptor::greet(store.clone(), 7, &g, true).await.unwrap();
        assert_eq!(a.state().mconf.generation, 2);
    }
}
