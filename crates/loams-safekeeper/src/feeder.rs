//! The pageserver feeder: an interim way to get committed WAL to an
//! unmodified pageserver (§28 Q112).
//!
//! The pageserver only ingests WAL through Neon's *interpreted* protocol:
//! records decoded, sharded and compressed by the sender with Neon's
//! `wal_decoder`, which needs Postgres server headers at build time. Until the
//! WAL service links it, the feeder plays walproposer towards one stock Neon
//! safekeeper and streams it the **committed** WAL from the [`WalStore`]. That
//! safekeeper then publishes the timeline to the storage broker and serves
//! the pageserver as it always does.
//!
//! The commit path does not touch it: computes are acknowledged by the WAL
//! service once TiKV has the WAL, and the feeder copies it afterwards. The
//! feeder's safekeeper holds no state the WAL service needs (it is refilled
//! from the store after a loss), and the pageserver's
//! `remote_consistent_lsn`, which it relays in its responses, is recorded in
//! the head for trimming.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::{info, warn};

use crate::Error;
use crate::pgwire::client;
use crate::proto::{
    AcceptorMessage, AppendRequest, AppendRequestHeader, MAX_SEND_SIZE, ProposerElected,
    ProposerGreeting, ProposerMessage, VoteRequest,
};
use crate::service::{Progress, WalService};
use crate::store::WalStore;
use crate::types::{AcceptorState, Lsn, TermHistory, TimelineId};

/// How a feeder reaches its safekeeper.
#[derive(Clone, Debug)]
pub struct FeederConfig {
    /// The safekeeper's Postgres listener, `host:port`.
    pub safekeeper: String,
    /// The pause before reconnecting after an error.
    pub retry: Duration,
    /// How often the store is polled when no local proposer wakes the feeder.
    pub poll: Duration,
}

/// Feed `tl` until the process stops: reconnect on every error or term change.
pub async fn run<S: WalStore>(svc: Arc<WalService<S>>, tl: TimelineId, cfg: FeederConfig) {
    loop {
        match feed_once(&svc, tl, &cfg).await {
            Ok(()) => info!(%tl, "feeder: term changed, reconnecting"),
            Err(e) => {
                warn!(%tl, error = %e, "feeder: reconnecting");
                tokio::time::sleep(cfg.retry).await;
            }
        }
    }
}

async fn send(wr: &mut OwnedWriteHalf, m: &ProposerMessage) -> Result<(), Error> {
    let mut buf = BytesMut::new();
    m.serialize(&mut buf);
    client::send_copy_data(wr, &buf).await
}

async fn recv(rd: &mut OwnedReadHalf) -> Result<AcceptorMessage, Error> {
    let body = client::recv_copy_data(rd)
        .await?
        .ok_or_else(|| Error::Io("safekeeper closed the stream".into()))?;
    AcceptorMessage::parse(body)
}

/// Wait until the timeline has an elected term with WAL.
async fn elected_head<S: WalStore>(
    svc: &WalService<S>,
    tl: TimelineId,
    poll: Duration,
) -> Result<AcceptorState, Error> {
    loop {
        if let Some(st) = svc.store().load(&tl).await?
            && st.term > 0
            && !st.term_history.0.is_empty()
            && st.term_history.0.last().is_some_and(|e| e.term == st.term)
        {
            return Ok(st);
        }
        tokio::time::sleep(poll).await;
    }
}

async fn feed_once<S: WalStore>(
    svc: &Arc<WalService<S>>,
    tl: TimelineId,
    cfg: &FeederConfig,
) -> Result<(), Error> {
    let head = elected_head(svc, tl, cfg.poll).await?;
    let term = head.term;

    let mut s = TcpStream::connect(&cfg.safekeeper)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    let _ = s.set_nodelay(true);
    let options = format!("-c timeline_id={} tenant_id={}", tl.timeline, tl.tenant);
    client::startup(
        &mut s,
        &[
            ("user", "loams-wal"),
            ("dbname", "replication"),
            ("options", &options),
        ],
    )
    .await?;
    client::query(
        &mut s,
        "START_WAL_PUSH (proto_version '3', allow_timeline_creation 'true')",
    )
    .await?;
    client::expect_copy_both(&mut s).await?;
    let (mut rd, mut wr) = s.into_split();

    send(
        &mut wr,
        &ProposerMessage::Greeting(ProposerGreeting {
            tenant_id: tl.tenant,
            timeline_id: tl.timeline,
            mconf: Default::default(),
            pg_version: head.server.pg_version,
            system_id: head.server.system_id,
            wal_seg_size: head.server.wal_seg_size,
        }),
    )
    .await?;
    let AcceptorMessage::Greeting(g) = recv(&mut rd).await? else {
        return Err(Error::Protocol("feeder: expected AcceptorGreeting".into()));
    };
    if g.term > term {
        return Err(Error::Protocol(format!(
            "feeder: the safekeeper's term {} is above the WAL service's {term}",
            g.term
        )));
    }
    send(
        &mut wr,
        &ProposerMessage::VoteRequest(VoteRequest {
            generation: 0,
            term,
        }),
    )
    .await?;
    let AcceptorMessage::VoteResponse(v) = recv(&mut rd).await? else {
        return Err(Error::Protocol("feeder: expected VoteResponse".into()));
    };
    if v.term != term {
        return Err(Error::Protocol(format!(
            "feeder: safekeeper at term {}",
            v.term
        )));
    }

    // Where the safekeeper's WAL and ours diverge, as walproposer computes it.
    let history: TermHistory = head.term_history.clone();
    let start =
        match TermHistory::find_highest_common_point(&history, &v.term_history, v.flush_lsn)? {
            Some(p) => p.lsn,
            None => history.0.first().map(|e| e.lsn).unwrap_or_default(),
        };
    if start < head.trimmed_lsn {
        return Err(Error::Trimmed {
            from: start,
            trimmed: head.trimmed_lsn,
        });
    }
    send(
        &mut wr,
        &ProposerMessage::Elected(ProposerElected {
            generation: 0,
            term,
            start_streaming_at: start,
            term_history: history,
        }),
    )
    .await?;
    info!(%tl, term, start = %start, safekeeper = %cfg.safekeeper, "feeder streaming");

    // Responses carry the pageserver's feedback; record its progress.
    let store = svc.store().clone();
    let acks = tokio::spawn(async move {
        let mut last = Lsn::INVALID;
        loop {
            match recv(&mut rd).await {
                Ok(AcceptorMessage::AppendResponse(r)) => {
                    if r.term > term {
                        return;
                    }
                    if let Some(fb) = r.pageserver_feedback
                        && fb.remote_consistent_lsn > last
                    {
                        last = fb.remote_consistent_lsn;
                        let _ = store.record_remote_consistent_lsn(&tl, last).await;
                    }
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    let mut at = start;
    let mut sent_commit = Lsn::INVALID;
    let mut watch = svc.subscribe(tl);
    let res = async {
        loop {
            if acks.is_finished() {
                return Err(Error::Io("feeder: safekeeper stream ended".into()));
            }
            watch.borrow_and_update();
            let st: Progress = svc.current(tl).await?;
            if st.term != term {
                return Ok(());
            }
            let commit = st.commit_lsn.min(st.flush_lsn);
            if at < commit {
                for (lsn, bytes) in svc.read_wal(tl, at, 8 * MAX_SEND_SIZE).await? {
                    if lsn >= commit {
                        break;
                    }
                    let mut off = 0usize;
                    let len = bytes.len().min((commit.0 - lsn.0) as usize);
                    while off < len {
                        let n = (len - off).min(MAX_SEND_SIZE);
                        let begin = Lsn(lsn.0 + off as u64);
                        let req = AppendRequest {
                            h: AppendRequestHeader {
                                generation: 0,
                                term,
                                begin_lsn: begin,
                                end_lsn: Lsn(begin.0 + n as u64),
                                commit_lsn: commit,
                                truncate_lsn: st.peer_horizon_lsn.min(commit),
                            },
                            wal: bytes.slice(off..off + n),
                        };
                        send(&mut wr, &ProposerMessage::Append(req)).await?;
                        off += n;
                    }
                    at = Lsn(lsn.0 + len as u64);
                }
                continue;
            }
            // Caught up: tell the safekeeper the commit point, then wait.
            if commit > sent_commit {
                sent_commit = commit;
                let hb = AppendRequest {
                    h: AppendRequestHeader {
                        generation: 0,
                        term,
                        begin_lsn: at,
                        end_lsn: at,
                        commit_lsn: commit,
                        truncate_lsn: st.peer_horizon_lsn.min(commit),
                    },
                    wal: Default::default(),
                };
                send(&mut wr, &ProposerMessage::Append(hb)).await?;
            }
            tokio::select! {
                _ = watch.changed() => {}
                _ = tokio::time::sleep(cfg.poll) => {}
            }
        }
    }
    .await;
    acks.abort();
    res
}
