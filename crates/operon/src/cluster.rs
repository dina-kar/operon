//! `operon cluster` (plan M1.3 Task 11): the metastore replica over HTTP,
//! the node's one metastore handle (E64), the router that answers `503`
//! until the node has started, and the `meta-membership` task that removes
//! learners whose node lease has long expired.
//!
//! This module names the openraft implementation (`MetaNode`, `MetaClient`,
//! the HTTP transport); every other component gets `Arc<dyn MetaStore>`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use operon_common::meta::{Consistency, MetaStore};
use operon_hot::node_lease_key;
use operon_meta::rpc::{self, LeaveRequest};
use operon_meta::{HttpTransport, MetaClient, MetaClientConfig, MetaNode, SystemClock};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::api::internal::NodeInfo;

/// The task key of the learner eviction task (`TaskKey::cluster`).
pub const MEMBERSHIP_TASK: &str = "meta-membership";

/// How long one eviction may take to reach the leader.
const LEAVE_DEADLINE: Duration = Duration::from_secs(30);

/// The node's one metastore handle (E64; M1.2a Ruling 12): a networked
/// `MetaClient` over the local replica. A later remote or external backend
/// replaces this function, and no other component names `MetaClient`.
pub fn metastore(node: &MetaNode, transport: &HttpTransport) -> (MetaClient, Arc<dyn MetaStore>) {
    let client = MetaClient::networked(
        node.clone(),
        transport.clone(),
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    let store: Arc<dyn MetaStore> = client.clone().into();
    (client, store)
}

/// A router that answers `503 unavailable` until [`LateRouter::set`] is
/// called (rule 3), and again after [`LateRouter::close`] (shutdown).
#[derive(Clone, Debug, Default)]
pub struct LateRouter {
    router: Arc<OnceLock<axum::Router>>,
    closed: Arc<AtomicBool>,
}

impl LateRouter {
    /// Serves `router` from now on (only the first call counts).
    pub fn set(&self, router: axum::Router) {
        if self.router.set(router).is_err() {
            tracing::warn!("the late router was already set");
        }
    }

    pub fn is_set(&self) -> bool {
        self.router.get().is_some()
    }

    /// Answers `503` again, for good.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Routes `request` to the router once set and not closed.
    pub async fn handle(self, request: Request) -> Response {
        match self.router.get() {
            Some(router) if !self.closed.load(Ordering::SeqCst) => {
                match router.clone().oneshot(request).await {
                    Ok(response) => response,
                    Err(never) => match never {},
                }
            }
            _ => crate::api::ApiError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "the node is not serving yet (starting or stopping)",
            )
            .into_response(),
        }
    }
}

/// The metastore view of `GET /internal/v1/node/stats`.
#[derive(Debug)]
pub struct ClusterInfo {
    pub node: MetaNode,
    /// What this node's start-up join reported.
    pub join_changed: bool,
}

impl NodeInfo for ClusterInfo {
    fn meta(&self) -> Value {
        let membership = self.node.membership();
        json!({
            "leader": self.node.leader().map(|(id, _)| id),
            "voters": membership.voters.keys().collect::<Vec<_>>(),
            "learners": membership.learners.keys().collect::<Vec<_>>(),
            "membership_index": self.node.membership_log_index(),
            "join_changed": self.join_changed,
        })
    }
}

/// Rule 5: every `interval`, one run removes each learner whose lease
/// `node/<id>` exists and expired more than `learner_expiry` ago. A learner
/// with no lease record is left alone (it may be joining).
#[derive(Clone)]
pub struct MembershipSource {
    node: MetaNode,
    transport: HttpTransport,
    seeds: Vec<String>,
    learner_expiry: Duration,
    interval: Duration,
    last_run: Arc<Mutex<Option<Instant>>>,
}

impl fmt::Debug for MembershipSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MembershipSource")
            .field("seeds", &self.seeds)
            .field("learner_expiry", &self.learner_expiry)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl MembershipSource {
    pub fn new(
        node: MetaNode,
        transport: HttpTransport,
        seeds: Vec<String>,
        learner_expiry: Duration,
        interval: Duration,
    ) -> Self {
        Self {
            node,
            transport,
            seeds,
            learner_expiry,
            interval,
            last_run: Arc::new(Mutex::new(None)),
        }
    }

    /// The learners this run would remove: lease expired for longer than
    /// `learner_expiry` at the metastore's `now_ms`.
    pub async fn stale_learners(&self, meta: &dyn MetaStore) -> Result<Vec<u64>, TaskError> {
        let learners: Vec<u64> = self.node.membership().learners.into_keys().collect();
        let now = meta.now_ms();
        let expiry = u64::try_from(self.learner_expiry.as_millis()).unwrap_or(u64::MAX);
        let mut stale = Vec::new();
        for id in learners {
            if let Some(lease) = meta.lease(Consistency::Local, &node_lease_key(id)).await?
                && lease.deadline_ms.saturating_add(expiry) < now
            {
                stale.push(id);
            }
        }
        Ok(stale)
    }
}

#[async_trait]
impl TaskSource for MembershipSource {
    fn priority(&self) -> Priority {
        Priority::Maintenance
    }

    async fn candidates(&self, _meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let due = self
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none_or(|last| last.elapsed() >= self.interval);
        Ok(match due {
            true => vec![(
                TaskKey::cluster(MEMBERSHIP_TASK),
                Arc::new(MembershipTask {
                    source: self.clone(),
                }) as Arc<dyn Task>,
            )],
            false => Vec::new(),
        })
    }
}

struct MembershipTask {
    source: MembershipSource,
}

#[async_trait]
impl Task for MembershipTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        *self
            .source
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        let stale = self.source.stale_learners(&*ctx.meta).await?;
        for id in stale {
            match rpc::leave(
                &self.source.transport,
                &self.source.seeds,
                LeaveRequest { node_id: id },
                LEAVE_DEADLINE,
            )
            .await
            {
                Ok(changed) => {
                    tracing::info!(node = id, changed, "evicted a stale metastore learner")
                }
                Err(err) => tracing::warn!(node = id, %err, "evicting a stale metastore learner"),
            }
        }
        Ok(TaskOutcome::Done)
    }
}

/// `--peers id=host:port,…`.
pub fn parse_peers(s: &str) -> Result<BTreeMap<u64, String>, String> {
    let mut peers = BTreeMap::new();
    for item in s.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        let (id, addr) = item
            .split_once('=')
            .ok_or_else(|| format!("expected id=host:port, got {item:?}"))?;
        let id: u64 = id
            .trim()
            .parse()
            .map_err(|_| format!("bad node id in {item:?}"))?;
        if peers.insert(id, addr.trim().to_string()).is_some() {
            return Err(format!("node {id} is listed twice in --peers"));
        }
    }
    Ok(peers)
}

/// Whether `s` is `host:port` with a non-empty host and a port.
pub fn is_host_port(s: &str) -> bool {
    if s.parse::<std::net::SocketAddr>().is_ok() {
        return true;
    }
    match s.rsplit_once(':') {
        Some((host, port)) => {
            !host.is_empty() && !host.contains(':') && port.parse::<u16>().is_ok()
        }
        None => false,
    }
}
