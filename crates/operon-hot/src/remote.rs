//! Read forwarding (plan M1.3 Task 10 rules 4–6; Ruling 14; B2, B3, E33,
//! E55): [`RemoteReadsImpl`] sends a read to its owner over
//! `POST /internal/v1/reads/<op>`, and [`serve_forwarded`] runs it on the
//! receiving node through the `*_local` methods, which never forward, so a
//! forwarded read is never forwarded again.
//!
//! The body is JSON (Ruling 14): `{"ns", "hot", "args"}` in, and
//! `{"ok": <result>, "hot_used": [..]}` or `{"err": <ServiceError body>}`
//! out. Get, count and scroll carry the owner's read token (E55).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use operon_collection::{ConsistencyToken, PrimaryKey};
use operon_query::error::ServiceError;
use operon_query::hot::{self, HotKind, HotUsed, RequestHot};
use operon_query::ir::{Query, ReadConsistency, SearchRequest, SearchResponse};
use operon_query::placement::{FORWARDED_HEADER, Owner, RemoteReads};
use operon_query::types::{Projection, StoredDoc};
use operon_query::{CollectionService, ScrollPage};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::TierError;
use crate::placement::PlacementImpl;

/// The internal route prefix; the op (`search`, `get`, `count`, `scroll`)
/// follows.
pub const READS_PATH: &str = "/internal/v1/reads/";

/// The ops [`serve_forwarded`] knows.
pub const READ_OPS: [&str; 4] = ["search", "get", "count", "scroll"];

/// Timeouts of a forwarded read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteReadsConfig {
    /// 500 ms.
    pub connect_timeout: Duration,
    /// 30 s.
    pub request_timeout: Duration,
}

impl Default for RemoteReadsConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(500),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// A snapshot of a node's forwarding counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ForwardCounters {
    /// Reads this node sent to an owner (every attempt).
    pub forwarded_out: u64,
    /// Forwarded reads this node received and ran.
    pub forwarded_in: u64,
    /// Forwards that failed in transport, so the read ran locally.
    pub fallbacks_signalled: u64,
}

/// Shared atomic counters behind [`ForwardCounters`] (one per node).
#[derive(Debug, Default)]
pub struct ForwardStats {
    forwarded_out: AtomicU64,
    forwarded_in: AtomicU64,
    fallbacks_signalled: AtomicU64,
}

impl ForwardStats {
    pub fn snapshot(&self) -> ForwardCounters {
        ForwardCounters {
            forwarded_out: self.forwarded_out.load(Ordering::Relaxed),
            forwarded_in: self.forwarded_in.load(Ordering::Relaxed),
            fallbacks_signalled: self.fallbacks_signalled.load(Ordering::Relaxed),
        }
    }
}

/// A forwarded error: [`ServiceError`]'s own JSON body (E33).
pub type WireError = Value;

/// `err`'s JSON body.
pub fn to_wire(err: &ServiceError) -> WireError {
    err.to_body()
}

/// The error `err` describes; a body that names no known error is
/// `Internal`.
pub fn from_wire(err: WireError) -> ServiceError {
    ServiceError::from_body(&err)
        .unwrap_or_else(|| ServiceError::Internal(format!("undecodable forwarded error: {err}")))
}

// ----- The wire forms -----

#[derive(Serialize, Deserialize)]
struct Envelope<A> {
    ns: String,
    #[serde(default)]
    hot: Option<bool>,
    args: A,
}

#[derive(Serialize, Deserialize)]
struct SearchArgs {
    req: SearchRequest,
}

#[derive(Serialize, Deserialize)]
struct GetArgs {
    name: String,
    #[serde(with = "operon_query::json::pk::vec")]
    pks: Vec<PrimaryKey>,
    select: Projection,
    consistency: ReadConsistency,
}

#[derive(Serialize, Deserialize)]
struct CountArgs {
    name: String,
    filter: Option<Query>,
    consistency: ReadConsistency,
}

#[derive(Serialize, Deserialize)]
struct ScrollArgs {
    name: String,
    filter: Option<Query>,
    #[serde(with = "operon_query::json::pk::opt")]
    after: Option<PrimaryKey>,
    limit: usize,
    select: Projection,
    consistency: ReadConsistency,
}

#[derive(Serialize, Deserialize)]
struct Docs {
    docs: Vec<Option<StoredDoc>>,
    #[serde(with = "operon_query::json::token")]
    read_token: ConsistencyToken,
}

#[derive(Serialize, Deserialize)]
struct Count {
    count: u64,
    #[serde(with = "operon_query::json::token")]
    read_token: ConsistencyToken,
}

#[derive(Serialize, Deserialize)]
struct Page {
    docs: Vec<StoredDoc>,
    #[serde(with = "operon_query::json::pk::opt")]
    next: Option<PrimaryKey>,
    #[serde(with = "operon_query::json::token")]
    read_token: ConsistencyToken,
}

#[derive(Deserialize)]
struct Reply<T> {
    #[serde(default = "Option::default")]
    ok: Option<T>,
    #[serde(default)]
    err: Option<Value>,
    #[serde(default)]
    hot_used: Vec<HotKind>,
}

// ----- The sending side -----

/// [`RemoteReads`] over `POST http://<owner>/internal/v1/reads/<op>`.
#[derive(Debug)]
pub struct RemoteReadsImpl {
    client: reqwest::Client,
    placement: Arc<PlacementImpl>,
    stats: Arc<ForwardStats>,
}

impl RemoteReadsImpl {
    pub fn new(
        placement: Arc<PlacementImpl>,
        stats: Arc<ForwardStats>,
        config: RemoteReadsConfig,
    ) -> Result<Self, TierError> {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| {
                TierError::Other(format!("building the read forwarding client: {err}"))
            })?;
        Ok(Self {
            client,
            placement,
            stats,
        })
    }

    /// Rule 4: one forwarded call. A transport failure marks the owner
    /// suspect and answers `Unavailable`, which makes the service read
    /// locally (B3).
    async fn call<A: Serialize, T: DeserializeOwned>(
        &self,
        to: &Owner,
        op: &str,
        ns: &str,
        args: A,
    ) -> Result<T, ServiceError> {
        let Owner::Remote { node_id, addr } = to else {
            return Err(ServiceError::Unavailable(
                "a forwarded read needs a remote owner".to_string(),
            ));
        };
        let scope = hot::current();
        let envelope = Envelope {
            ns: ns.to_string(),
            hot: scope.as_ref().map(|hot| hot.enabled),
            args,
        };
        self.stats.forwarded_out.fetch_add(1, Ordering::Relaxed);
        let url = format!("http://{addr}{READS_PATH}{op}");
        let unreachable = |reason: String| {
            self.placement.mark_unreachable(*node_id);
            self.stats
                .fallbacks_signalled
                .fetch_add(1, Ordering::Relaxed);
            ServiceError::Unavailable(format!("owner {node_id} unreachable: {reason}"))
        };
        let response = self
            .client
            .post(&url)
            .header(FORWARDED_HEADER, "1")
            .json(&envelope)
            .send()
            .await
            .map_err(|err| unreachable(err.to_string()))?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(unreachable(format!("status {}", response.status())));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| unreachable(err.to_string()))?;
        let reply: Reply<T> = serde_json::from_slice(&body)
            .map_err(|err| unreachable(format!("undecodable answer: {err}")))?;
        match (reply.ok, reply.err) {
            (Some(ok), None) => {
                if let Some(scope) = &scope {
                    for kind in reply.hot_used {
                        scope.used.record(kind);
                    }
                }
                Ok(ok)
            }
            (None, Some(err)) => Err(from_wire(err)),
            _ => Err(unreachable(
                "the answer has neither or both of ok and err".to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl RemoteReads for RemoteReadsImpl {
    async fn search(
        &self,
        to: &Owner,
        ns: &str,
        req: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        self.call(to, "search", ns, SearchArgs { req }).await
    }

    async fn get(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        pks: Vec<PrimaryKey>,
        select: Projection,
        consistency: ReadConsistency,
    ) -> Result<(Vec<Option<StoredDoc>>, ConsistencyToken), ServiceError> {
        let args = GetArgs {
            name: name.to_string(),
            pks,
            select,
            consistency,
        };
        let docs: Docs = self.call(to, "get", ns, args).await?;
        Ok((docs.docs, docs.read_token))
    }

    async fn count(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        consistency: ReadConsistency,
    ) -> Result<(u64, ConsistencyToken), ServiceError> {
        let args = CountArgs {
            name: name.to_string(),
            filter,
            consistency,
        };
        let count: Count = self.call(to, "count", ns, args).await?;
        Ok((count.count, count.read_token))
    }

    async fn scroll(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        after: Option<PrimaryKey>,
        limit: usize,
        select: Projection,
        consistency: ReadConsistency,
    ) -> Result<ScrollPage, ServiceError> {
        let args = ScrollArgs {
            name: name.to_string(),
            filter,
            after,
            limit,
            select,
            consistency,
        };
        let page: Page = self.call(to, "scroll", ns, args).await?;
        Ok(((page.docs, page.next), page.read_token))
    }
}

// ----- The receiving side -----

fn answer(status: StatusCode, body: Value) -> (StatusCode, Bytes) {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    (status, Bytes::from(bytes))
}

fn refuse(status: StatusCode, err: ServiceError) -> (StatusCode, Bytes) {
    answer(status, json!({ "err": to_wire(&err) }))
}

fn decode<A: DeserializeOwned>(body: &[u8]) -> Result<Envelope<A>, ServiceError> {
    serde_json::from_slice(body)
        .map_err(|err| ServiceError::InvalidArgument(format!("malformed forwarded read: {err}")))
}

/// Rule 5: decodes a forwarded read (`400` when malformed, `404` for an
/// unknown op), counts it, and runs the matching `*_local` method of
/// `service` inside a hot scope with the sender's switch (the service
/// default when absent). The receiver does not check that it owns the
/// collection: after ownership churn it serves the read cold.
pub async fn serve_forwarded(
    service: &CollectionService,
    op: &str,
    body: Bytes,
    stats: &ForwardStats,
) -> (StatusCode, Bytes) {
    match op {
        "search" => {
            let envelope = match decode::<SearchArgs>(&body) {
                Ok(envelope) => envelope,
                Err(err) => return refuse(StatusCode::BAD_REQUEST, err),
            };
            let (hot, ns) = (envelope.hot, envelope.ns);
            let req = envelope.args.req;
            Box::pin(run(service, stats, hot, service.search_local(&ns, req))).await
        }
        "get" => {
            let envelope = match decode::<GetArgs>(&body) {
                Ok(envelope) => envelope,
                Err(err) => return refuse(StatusCode::BAD_REQUEST, err),
            };
            let (hot, ns, a) = (envelope.hot, envelope.ns, envelope.args);
            let call = async {
                service
                    .get_local_with_token(&ns, &a.name, &a.pks, &a.select, a.consistency.clone())
                    .await
                    .map(|(docs, read_token)| Docs { docs, read_token })
            };
            Box::pin(run(service, stats, hot, call)).await
        }
        "count" => {
            let envelope = match decode::<CountArgs>(&body) {
                Ok(envelope) => envelope,
                Err(err) => return refuse(StatusCode::BAD_REQUEST, err),
            };
            let (hot, ns, a) = (envelope.hot, envelope.ns, envelope.args);
            let call = async {
                service
                    .count_local_with_token(&ns, &a.name, a.filter, a.consistency)
                    .await
                    .map(|(count, read_token)| Count { count, read_token })
            };
            Box::pin(run(service, stats, hot, call)).await
        }
        "scroll" => {
            let envelope = match decode::<ScrollArgs>(&body) {
                Ok(envelope) => envelope,
                Err(err) => return refuse(StatusCode::BAD_REQUEST, err),
            };
            let (hot, ns, a) = (envelope.hot, envelope.ns, envelope.args);
            let call = async {
                service
                    .scroll_local_with_token(
                        &ns,
                        &a.name,
                        a.filter,
                        a.after,
                        a.limit,
                        &a.select,
                        a.consistency,
                    )
                    .await
                    .map(|((docs, next), read_token)| Page {
                        docs,
                        next,
                        read_token,
                    })
            };
            Box::pin(run(service, stats, hot, call)).await
        }
        other => refuse(
            StatusCode::NOT_FOUND,
            ServiceError::NotFound {
                kind: "object",
                name: format!("{READS_PATH}{other}"),
            },
        ),
    }
}

/// Counts a decoded forwarded read and runs it in its hot scope.
async fn run<T: Serialize>(
    service: &CollectionService,
    stats: &ForwardStats,
    hot: Option<bool>,
    call: impl Future<Output = Result<T, ServiceError>>,
) -> (StatusCode, Bytes) {
    stats.forwarded_in.fetch_add(1, Ordering::Relaxed);
    let used = HotUsed::default();
    let scope = RequestHot {
        enabled: hot.unwrap_or(service.config().hot_default),
        used: used.clone(),
    };
    match hot::scope(scope, call).await {
        Ok(ok) => answer(
            StatusCode::OK,
            json!({ "ok": ok, "hot_used": used.kinds() }),
        ),
        Err(err) => answer(StatusCode::OK, json!({ "err": to_wire(&err) })),
    }
}
