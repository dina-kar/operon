//! The metastore's private HTTP routes (M1.3 Task 9): openraft's RPCs, and
//! the few requests a networked [`MetaClient`](crate::MetaClient) and a
//! joining node send to the leader.
//!
//! These routes belong to the openraft backend (postcard [`Command`]s, read
//! indexes, membership); they are not a `MetaStore` protocol (M1.3 E64). A
//! remote `MetaStore` client (D64, M2.x) is a separate implementation.
//!
//! Every route is a POST with a postcard body and answers 200 with a
//! postcard body, 400 for an undecodable request, or 503 while the node is
//! shutting down (it then did nothing). Bodies are limited to 64 MiB, except
//! snapshots (1 GiB).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use operon_common::meta::{ApplyError, MetaError};
use serde::{Deserialize, Serialize};

use crate::command::{Command, Reply};
use crate::node::{MembershipChange, MetaNode};
use crate::raft::{NodeId, TypeConfig};
use crate::transport::{HttpTransport, PostError, decode_snapshot_request, encode};

pub const RAFT_APPEND: &str = "/internal/v1/raft/append";
pub const RAFT_VOTE: &str = "/internal/v1/raft/vote";
pub const RAFT_PRE_VOTE: &str = "/internal/v1/raft/pre-vote";
pub const RAFT_SNAPSHOT: &str = "/internal/v1/raft/snapshot";
pub const META_WRITE: &str = "/internal/v1/meta/write";
pub const META_READ_INDEX: &str = "/internal/v1/meta/read-index";
pub const META_JOIN: &str = "/internal/v1/meta/join";
pub const META_LEAVE: &str = "/internal/v1/meta/leave";
pub const META_STATUS: &str = "/internal/v1/meta/status";

/// The body limit of every route but the snapshot route.
pub const BODY_LIMIT: usize = 64 << 20;
/// The body limit of [`RAFT_SNAPSHOT`].
pub const SNAPSHOT_BODY_LIMIT: usize = 1 << 30;

/// A leader as a node knows it: its id and, when the membership has one, its
/// address.
pub type LeaderHint = Option<(NodeId, Option<String>)>;

/// The answer to a [`META_WRITE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireWrite {
    /// The command was applied at `log_index` (a rejected command too).
    Applied {
        reply: Result<Reply, ApplyError>,
        log_index: u64,
    },
    /// The node is not the leader. `refused` is true when it refused before
    /// proposing, so the write did not apply; false when openraft answered
    /// after proposing, so its outcome is unknown (M1.3 E46).
    NotLeader { leader: LeaderHint, refused: bool },
    /// The write did not finish in time; its outcome is unknown.
    Timeout,
    /// Raft could not make progress; the outcome is unknown.
    Unavailable(String),
    /// The leader refused the command's time stamp before proposing it.
    ClockSkew { stamped_ms: u64, leader_ms: u64 },
}

/// The answer to a [`META_READ_INDEX`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireReadIndex {
    Index(u64),
    NotLeader { leader: LeaderHint },
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub addr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaveRequest {
    pub node_id: NodeId,
}

/// The answer to a [`META_JOIN`] or [`META_LEAVE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireMembership {
    Done { changed: bool },
    NotLeader { leader: LeaderHint },
    Refused(String),
    Unavailable(String),
}

/// The answer to a [`META_STATUS`]: the receiving node's view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireStatus {
    pub node_id: NodeId,
    pub leader: Option<NodeId>,
    pub last_applied: Option<u64>,
    pub voters: BTreeMap<NodeId, String>,
    pub learners: BTreeMap<NodeId, String>,
}

/// The routes above, served for `node`.
pub fn router(node: MetaNode) -> axum::Router {
    let small = axum::Router::new()
        .route(RAFT_APPEND, post(append))
        .route(RAFT_VOTE, post(vote))
        .route(RAFT_PRE_VOTE, post(pre_vote))
        .route(META_WRITE, post(meta_write))
        .route(META_READ_INDEX, post(read_index))
        .route(META_JOIN, post(join_route))
        .route(META_LEAVE, post(leave_route))
        .route(META_STATUS, post(status))
        .layer(DefaultBodyLimit::max(BODY_LIMIT));
    let snapshots = axum::Router::new()
        .route(RAFT_SNAPSHOT, post(snapshot))
        .layer(DefaultBodyLimit::max(SNAPSHOT_BODY_LIMIT));
    small.merge(snapshots).with_state(node)
}

fn ok<T: Serialize>(value: &T) -> Response {
    (StatusCode::OK, encode(value)).into_response()
}

fn bad_request(err: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, err.to_string()).into_response()
}

/// 503 while the node cannot handle requests: shut down, or cut off by a test.
fn refuse(node: &MetaNode) -> Option<Response> {
    (node.is_partitioned() || !node.is_running())
        .then(|| (StatusCode::SERVICE_UNAVAILABLE, "meta node unavailable").into_response())
}

fn decode<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(body)
}

async fn append(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let rpc: AppendEntriesRequest<TypeConfig> = match decode(&body) {
        Ok(rpc) => rpc,
        Err(err) => return bad_request(err),
    };
    ok(&node.raft().append_entries(rpc).await)
}

async fn vote(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let rpc: VoteRequest<TypeConfig> = match decode(&body) {
        Ok(rpc) => rpc,
        Err(err) => return bad_request(err),
    };
    ok(&node.raft().vote(rpc).await)
}

async fn pre_vote(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let rpc: VoteRequest<TypeConfig> = match decode(&body) {
        Ok(rpc) => rpc,
        Err(err) => return bad_request(err),
    };
    ok(&node.raft().pre_vote(rpc).await)
}

async fn snapshot(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let (vote, snapshot) = match decode_snapshot_request(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(err),
    };
    ok(&node.raft().install_full_snapshot(vote, snapshot).await)
}

/// `leader` with its address, if this node's membership has one.
fn hint(node: &MetaNode, leader: Option<NodeId>) -> LeaderHint {
    let leader = leader?;
    let addr = node
        .membership()
        .voters
        .get(&leader)
        .cloned()
        .filter(|addr| !addr.is_empty());
    Some((leader, addr))
}

async fn meta_write(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let command: Command = match decode(&body) {
        Ok(command) => command,
        Err(err) => return bad_request(err),
    };
    let answer = match node.write_indexed_attempt(command).await {
        Ok((reply, log_index)) => WireWrite::Applied { reply, log_index },
        Err(attempt) => match attempt.error {
            MetaError::NotLeader { leader } => WireWrite::NotLeader {
                leader: hint(&node, leader),
                refused: attempt.refused,
            },
            MetaError::Timeout => WireWrite::Timeout,
            MetaError::ClockSkew {
                stamped_ms,
                leader_ms,
            } => WireWrite::ClockSkew {
                stamped_ms,
                leader_ms,
            },
            other => WireWrite::Unavailable(other.to_string()),
        },
    };
    ok(&answer)
}

async fn read_index(State(node): State<MetaNode>) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let answer = match node.read_index().await {
        Ok(index) => WireReadIndex::Index(index),
        Err(MetaError::NotLeader { leader }) => WireReadIndex::NotLeader {
            leader: hint(&node, leader),
        },
        Err(other) => WireReadIndex::Unavailable(other.to_string()),
    };
    ok(&answer)
}

fn membership_answer(node: &MetaNode, change: Result<MembershipChange, MetaError>) -> Response {
    let answer = match change {
        Ok(MembershipChange::Done { changed }) => WireMembership::Done { changed },
        Ok(MembershipChange::NotLeader) => WireMembership::NotLeader {
            leader: hint(node, node.leader().map(|(id, _)| id)),
        },
        Ok(MembershipChange::Refused(reason)) => WireMembership::Refused(reason),
        Err(err) => WireMembership::Unavailable(err.to_string()),
    };
    ok(&answer)
}

async fn join_route(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let request: JoinRequest = match decode(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(err),
    };
    let change = node.join_member(request.node_id, &request.addr).await;
    membership_answer(&node, change)
}

async fn leave_route(State(node): State<MetaNode>, body: Bytes) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let request: LeaveRequest = match decode(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(err),
    };
    let change = node.leave_member(request.node_id).await;
    membership_answer(&node, change)
}

async fn status(State(node): State<MetaNode>) -> Response {
    if let Some(refused) = refuse(&node) {
        return refused;
    }
    let raft = node.status();
    let membership = node.membership();
    ok(&WireStatus {
        node_id: node.id(),
        leader: raft.leader,
        last_applied: raft.last_applied,
        voters: membership.voters,
        learners: membership.learners,
    })
}

/// The longest wait between two rounds of [`join`] or [`leave`].
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Asks `seeds` in turn (following `NotLeader` addresses) to add this node
/// as a learner or update its address (M1.3 Task 9 rule 5); true if the
/// membership changed. Retries with backoff until `deadline`.
pub async fn join(
    transport: &HttpTransport,
    seeds: &[String],
    request: JoinRequest,
    deadline: Duration,
) -> Result<bool, MetaError> {
    membership_request(transport, seeds, META_JOIN, &request, deadline).await
}

/// Asks `seeds` in turn to remove this learner; true if the membership
/// changed. A voter is refused ([`MetaError::Config`]): voters change in M2.
pub async fn leave(
    transport: &HttpTransport,
    seeds: &[String],
    request: LeaveRequest,
    deadline: Duration,
) -> Result<bool, MetaError> {
    membership_request(transport, seeds, META_LEAVE, &request, deadline).await
}

async fn membership_request<Req: Serialize>(
    transport: &HttpTransport,
    seeds: &[String],
    path: &str,
    request: &Req,
    deadline: Duration,
) -> Result<bool, MetaError> {
    if seeds.is_empty() {
        return Err(MetaError::Config("no seed addresses".to_string()));
    }
    let deadline = Instant::now() + deadline;
    // Adding a learner blocks until it has caught up, possibly from a
    // snapshot, so a membership request may take a snapshot's time.
    let timeout = transport.config().snapshot_timeout;
    let mut backoff = Duration::from_millis(50);
    let mut last = MetaError::Timeout;
    loop {
        let mut targets: Vec<String> = seeds.to_vec();
        let mut i = 0;
        while i < targets.len() {
            let addr = targets[i].clone();
            i += 1;
            let remaining = deadline.saturating_duration_since(Instant::now());
            match transport
                .post::<_, WireMembership>(
                    &addr,
                    path,
                    request,
                    timeout.min(remaining.max(MIN_ATTEMPT)),
                )
                .await
            {
                Ok(WireMembership::Done { changed }) => return Ok(changed),
                Ok(WireMembership::Refused(reason)) => return Err(MetaError::Config(reason)),
                Ok(WireMembership::NotLeader { leader }) => {
                    last = MetaError::NotLeader {
                        leader: leader.as_ref().map(|(id, _)| *id),
                    };
                    // Ask the hinted leader next, once per round.
                    if let Some((_, Some(leader))) = leader
                        && !targets[i..].contains(&leader)
                        && targets.len() < seeds.len() * 2
                    {
                        targets.insert(i, leader);
                    }
                }
                Ok(WireMembership::Unavailable(msg)) => last = MetaError::Unavailable(msg),
                Err(err) => last = post_error(err),
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(last);
        }
        tracing::debug!(%last, ?backoff, "membership request failed; retrying");
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Each attempt gets at least this long, even at the deadline.
const MIN_ATTEMPT: Duration = Duration::from_millis(100);

/// A failed POST as a retryable metastore error.
pub(crate) fn post_error(err: PostError) -> MetaError {
    match err {
        PostError::Unknown(msg) => {
            tracing::debug!(msg, "metastore request outcome unknown");
            MetaError::Timeout
        }
        other => MetaError::Unavailable(other.to_string()),
    }
}
