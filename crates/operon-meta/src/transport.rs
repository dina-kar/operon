//! How meta nodes reach each other: in process ([`Router`]) or over HTTP
//! ([`HttpTransport`], M1.3 Task 9).
//!
//! Over HTTP, openraft's RPCs travel as postcard bodies on the node's
//! listen address, on the private routes of [`crate::rpc`]. The
//! [`HttpNetworkFactory`] connects to each peer at the address the effective
//! membership gives it.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use openraft::error::{
    Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::VoteOf;
use operon_common::meta::MetaError;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::network::Router;
use crate::raft::{NodeId, Snapshot, SnapshotData, TypeConfig};
use crate::rpc::{META_WRITE, RAFT_APPEND, RAFT_PRE_VOTE, RAFT_SNAPSHOT, RAFT_VOTE};

/// How a meta node reaches its peers.
#[derive(Clone, Debug)]
pub enum Transport {
    /// Direct calls between the nodes of one process (M0).
    InProcess(Router),
    /// openraft RPCs over HTTP, to the addresses in the membership.
    Http(HttpTransport),
}

/// Timeouts of an [`HttpTransport`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpTransportConfig {
    /// Establishing a TCP connection. Default 1 s.
    pub connect_timeout: Duration,
    /// One request and its response, except snapshots. Default 10 s.
    pub request_timeout: Duration,
    /// One full snapshot transfer. Default 300 s.
    pub snapshot_timeout: Duration,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(10),
            snapshot_timeout: Duration::from_secs(300),
        }
    }
}

/// An HTTP client for the metastore's private routes. Cheap to clone; clones
/// share the connection pool and the test hooks.
#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    config: HttpTransportConfig,
    /// Test hook: responses of meta writes still to discard.
    dropped_responses: Arc<AtomicU32>,
}

impl fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpTransport")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Why a POST failed.
#[derive(Debug)]
pub(crate) enum PostError {
    /// The request never reached the peer (no connection): it did nothing.
    NotSent(String),
    /// The peer refused the request before handling it (503: shutting down,
    /// or cut off by a test): it did nothing.
    Refused(String),
    /// The request may have reached the peer, but no usable answer came
    /// back (a timeout, a broken connection, another status): its outcome
    /// is unknown.
    Unknown(String),
    /// The body of a 200 answer did not decode.
    Decode(String),
}

impl fmt::Display for PostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostError::NotSent(m) | PostError::Refused(m) | PostError::Unknown(m) => f.write_str(m),
            PostError::Decode(m) => write!(f, "undecodable response: {m}"),
        }
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    // Every message is plain data; postcard cannot fail on it.
    postcard::to_allocvec(value).expect("postcard encodes metastore messages")
}

impl HttpTransport {
    pub fn new(config: HttpTransportConfig) -> Result<Self, MetaError> {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| MetaError::Config(format!("HTTP transport: {e}")))?;
        Ok(Self {
            client,
            config,
            dropped_responses: Arc::new(AtomicU32::new(0)),
        })
    }

    pub fn config(&self) -> HttpTransportConfig {
        self.config
    }

    /// For tests: the next meta write sent through this transport (or a
    /// clone) reaches the leader and is handled there, but its response is
    /// discarded, as if the connection broke after the request was sent.
    /// Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn drop_next_response(&self) {
        self.dropped_responses.fetch_add(1, Ordering::SeqCst);
    }

    fn take_dropped_response(&self) -> bool {
        self.dropped_responses
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }

    /// POSTs `body` to `http://<addr><path>` and returns the body of a 200
    /// answer.
    pub(crate) async fn post_raw(
        &self,
        addr: &str,
        path: &str,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<Bytes, PostError> {
        let url = format!("http://{addr}{path}");
        let response = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .timeout(timeout)
            .body(body)
            .send()
            .await
            .map_err(|e| match e.is_connect() {
                true => PostError::NotSent(format!("POST {url}: {e}")),
                false => PostError::Unknown(format!("POST {url}: {e}")),
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(PostError::Refused(format!("POST {url}: {status}")));
        }
        if status != reqwest::StatusCode::OK {
            return Err(PostError::Unknown(format!("POST {url}: {status}")));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|e| PostError::Unknown(format!("POST {url}: {e}")))?;
        if path == META_WRITE && self.take_dropped_response() {
            return Err(PostError::Unknown(format!(
                "POST {url}: response dropped (test hook)"
            )));
        }
        Ok(bytes)
    }

    /// [`HttpTransport::post_raw`] with a postcard request and response.
    pub(crate) async fn post<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        addr: &str,
        path: &str,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp, PostError> {
        let bytes = self.post_raw(addr, path, encode(request), timeout).await?;
        postcard::from_bytes(&bytes).map_err(|e| PostError::Decode(e.to_string()))
    }
}

/// Each peer's address in the effective membership. openraft keeps a
/// replication stream (and its connection) when a membership change only
/// updates a node's address, so connections look the address up here on
/// every RPC; the node keeps it current ([`watch_addresses`]).
pub(crate) type AddressBook = Arc<RwLock<BTreeMap<NodeId, String>>>;

/// Keeps `book` equal to the addresses of `raft`'s effective membership,
/// until Raft stops.
pub(crate) fn watch_addresses(raft: &crate::network::MetaRaft, book: AddressBook) {
    use openraft::async_runtime::WatchReceiver;
    let mut metrics = raft.metrics();
    tokio::spawn(async move {
        let mut seen = None;
        loop {
            {
                let m = metrics.borrow_watched();
                let membership = &m.membership_config;
                if seen.as_ref() != Some(membership.log_id()) {
                    seen = Some(*membership.log_id());
                    let mut book = book.write().unwrap_or_else(PoisonError::into_inner);
                    for (id, node) in membership.nodes() {
                        if !node.addr.is_empty() {
                            book.insert(*id, node.addr.clone());
                        }
                    }
                }
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    });
}

/// Creates HTTP connections from one node to its peers.
pub(crate) struct HttpNetworkFactory {
    pub(crate) transport: HttpTransport,
    pub(crate) from: NodeId,
    pub(crate) addresses: AddressBook,
    /// Test hook ([`crate::MetaNode::set_partitioned`]): drop every
    /// outgoing RPC while set.
    pub(crate) partitioned: Arc<AtomicBool>,
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> HttpConnection {
        if !node.addr.is_empty() {
            self.addresses
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(target, node.addr.clone());
        }
        HttpConnection {
            transport: self.transport.clone(),
            from: self.from,
            target,
            addresses: self.addresses.clone(),
            partitioned: self.partitioned.clone(),
        }
    }
}

/// One node's connection to one peer.
pub(crate) struct HttpConnection {
    transport: HttpTransport,
    from: NodeId,
    target: NodeId,
    addresses: AddressBook,
    partitioned: Arc<AtomicBool>,
}

impl HttpConnection {
    /// The target's current address.
    fn check(&self) -> Result<String, Unreachable<TypeConfig>> {
        let addr = self
            .addresses
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.target)
            .cloned();
        let Some(addr) = addr else {
            return Err(Unreachable::from_string(format!(
                "node {} has no address in the membership",
                self.target
            )));
        };
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(Unreachable::from_string(format!(
                "node {} is partitioned (test hook)",
                self.from
            )));
        }
        Ok(addr)
    }

    /// Sends one append, vote or pre-vote RPC, mapping errors as the
    /// in-process `Connection` does.
    async fn call<Req, Resp>(
        &self,
        path: &str,
        rpc: &Req,
        option: &RPCOption,
    ) -> Result<Resp, RPCError<TypeConfig>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let addr = self.check()?;
        let timeout = option.hard_ttl().min(self.transport.config.request_timeout);
        let answer: Result<Resp, RaftError<TypeConfig>> = self
            .transport
            .post(&addr, path, rpc, timeout)
            .await
            .map_err(rpc_error)?;
        answer.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

fn rpc_error(err: PostError) -> RPCError<TypeConfig> {
    match err {
        PostError::Decode(_) => RPCError::Network(NetworkError::from_string(err)),
        _ => RPCError::Unreachable(Unreachable::from_string(err)),
    }
}

impl RaftNetworkV2<TypeConfig> for HttpConnection {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call(RAFT_APPEND, &rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call(RAFT_VOTE, &rpc, &option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call(RAFT_PRE_VOTE, &rpc, &option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let addr = self.check()?;
        let body = encode_snapshot_request(&vote, snapshot);
        let send = self.transport.post_raw(
            &addr,
            RAFT_SNAPSHOT,
            body,
            self.transport.config.snapshot_timeout,
        );
        let bytes = tokio::select! {
            sent = send => sent,
            _ = cancel => {
                return Err(StreamingError::Closed(ReplicationClosed::new("cancelled")));
            }
        };
        let bytes = bytes.map_err(|e| match e {
            PostError::Decode(_) => StreamingError::Network(NetworkError::from_string(e)),
            _ => StreamingError::Unreachable(Unreachable::from_string(e)),
        })?;
        let answer: Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>> =
            postcard::from_bytes(&bytes)
                .map_err(|e| StreamingError::Network(NetworkError::from_string(e)))?;
        answer.map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
    }
}

/// `u32 LE header length | postcard((vote, meta)) | snapshot bytes`.
pub(crate) fn encode_snapshot_request(vote: &VoteOf<TypeConfig>, snapshot: Snapshot) -> Vec<u8> {
    let header = encode(&(vote, &snapshot.meta));
    let data = snapshot.snapshot.into_inner();
    let len = u32::try_from(header.len()).expect("a snapshot header fits in 4 GiB");
    let mut body = Vec::with_capacity(4 + header.len() + data.len());
    body.extend_from_slice(&len.to_le_bytes());
    body.extend_from_slice(&header);
    body.extend_from_slice(&data);
    body
}

/// Splits a snapshot request body ([`encode_snapshot_request`]).
pub(crate) fn decode_snapshot_request(
    body: &[u8],
) -> Result<(VoteOf<TypeConfig>, Snapshot), String> {
    let (len, rest) = body
        .split_first_chunk::<4>()
        .ok_or("a snapshot request shorter than its header length")?;
    let len = usize::try_from(u32::from_le_bytes(*len)).map_err(|e| e.to_string())?;
    if rest.len() < len {
        return Err("a snapshot request shorter than its header".to_string());
    }
    let (header, data) = rest.split_at(len);
    let (vote, meta) = postcard::from_bytes(header).map_err(|e| e.to_string())?;
    Ok((
        vote,
        Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data.to_vec()),
        },
    ))
}
