//! The node registry (plan M1.3 Task 10 rule 1; Ruling 12): each node holds
//! the metastore lease `node/<node_id>`, whose owner string is its
//! [`NodeDescriptor`], and every node reads the held leases to learn which
//! nodes are live. Liveness is lease expiry; no command is added.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use operon_common::meta::{ApplyError, Consistency, MetaError, MetaStore};
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::TierError;

/// Every node's lease is `node/<node_id>`.
pub const NODE_LEASE_PREFIX: &str = "node/";

/// How long `register` waits between attempts while an earlier incarnation
/// holds the lease.
const REGISTER_RETRY: Duration = Duration::from_secs(1);

/// The roles a node runs (plan M1.3 Task 11 rule 2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Roles {
    pub meta: bool,
    pub log: bool,
    pub query: bool,
    pub worker: bool,
    pub gateway: bool,
}

impl Roles {
    /// Every role.
    pub fn all() -> Self {
        Self {
            meta: true,
            log: true,
            query: true,
            worker: true,
            gateway: true,
        }
    }

    /// A comma-separated subset of `meta,log,query,worker,gateway`, in any
    /// order; empty or unknown names are an error.
    pub fn parse(s: &str) -> Result<Self, TierError> {
        let mut roles = Roles::default();
        for name in s.split(',').map(str::trim) {
            let role = match name {
                "meta" => &mut roles.meta,
                "log" => &mut roles.log,
                "query" => &mut roles.query,
                "worker" => &mut roles.worker,
                "gateway" => &mut roles.gateway,
                "" => {
                    return Err(TierError::Other(format!(
                        "empty role in {s:?} (expected meta, log, query, worker or gateway)"
                    )));
                }
                other => {
                    return Err(TierError::Other(format!(
                        "unknown role {other:?} (expected meta, log, query, worker or gateway)"
                    )));
                }
            };
            *role = true;
        }
        Ok(roles)
    }

    /// Whether no role is set.
    pub fn is_empty(&self) -> bool {
        *self == Roles::default()
    }
}

impl fmt::Display for Roles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (self.meta, "meta"),
            (self.log, "log"),
            (self.query, "query"),
            (self.worker, "worker"),
            (self.gateway, "gateway"),
        ];
        let set: Vec<&str> = names
            .iter()
            .filter(|(on, _)| *on)
            .map(|(_, name)| *name)
            .collect();
        f.write_str(&set.join(","))
    }
}

impl FromStr for Roles {
    type Err = TierError;

    fn from_str(s: &str) -> Result<Self, TierError> {
        Roles::parse(s)
    }
}

/// What the registry knows of a node: the owner string of its lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub node_id: u64,
    /// New on every process start, so a restarted node's lease owner differs
    /// from its predecessor's.
    pub incarnation: Ulid,
    /// Where other nodes reach it (`--advertise`, an IP address, E49).
    pub addr: SocketAddr,
    pub roles: Roles,
    /// Empty when unset.
    pub zone: String,
}

impl NodeDescriptor {
    /// `v1;<incarnation>;<addr>;<roles>;<zone>`.
    pub fn encode(&self) -> String {
        format!(
            "v1;{};{};{};{}",
            self.incarnation, self.addr, self.roles, self.zone
        )
    }

    /// The descriptor of node `node_id` from its lease owner string.
    pub fn decode(node_id: u64, owner: &str) -> Result<Self, TierError> {
        let malformed = |why: &str| {
            TierError::Other(format!(
                "malformed descriptor of node {node_id} ({why}): {owner:?}"
            ))
        };
        let mut parts = owner.splitn(5, ';');
        let (Some(version), Some(incarnation), Some(addr), Some(roles), Some(zone)) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(malformed("expected 5 fields"));
        };
        if version != "v1" {
            return Err(malformed("unknown version"));
        }
        let incarnation =
            Ulid::from_string(incarnation).map_err(|_| malformed("bad incarnation"))?;
        let addr = addr.parse().map_err(|_| malformed("bad address"))?;
        let roles = Roles::parse(roles).map_err(|_| malformed("bad roles"))?;
        Ok(Self {
            node_id,
            incarnation,
            addr,
            roles,
            zone: zone.to_string(),
        })
    }

    /// The lease key of this node.
    pub fn lease_key(&self) -> String {
        node_lease_key(self.node_id)
    }
}

/// `node/<node_id>`.
pub fn node_lease_key(node_id: u64) -> String {
    format!("{NODE_LEASE_PREFIX}{node_id}")
}

/// Registry timing (Ruling 12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegistryConfig {
    /// The lease TTL (10 s).
    pub lease_ttl: Duration,
    /// How often the lease is renewed (3 s).
    pub renew_every: Duration,
    /// How often the live set is re-read (1 s).
    pub refresh_every: Duration,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(10),
            renew_every: Duration::from_secs(3),
            refresh_every: Duration::from_secs(1),
        }
    }
}

/// This node's lease and the live nodes.
pub struct NodeRegistry {
    /// `None` for a standalone registry.
    meta: Option<Arc<dyn MetaStore>>,
    me: NodeDescriptor,
    config: RegistryConfig,
    /// The epoch of this node's lease.
    epoch: Mutex<u64>,
    live: RwLock<Arc<Vec<NodeDescriptor>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl fmt::Debug for NodeRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeRegistry")
            .field("me", &self.me)
            .field("config", &self.config)
            .field("standalone", &self.meta.is_none())
            .finish_non_exhaustive()
    }
}

impl NodeRegistry {
    /// Acquires `node/<id>` (retrying while an earlier incarnation holds it,
    /// up to 2 × ttl), then renews and refreshes in the background.
    pub async fn register(
        meta: Arc<dyn MetaStore>,
        me: NodeDescriptor,
        config: RegistryConfig,
    ) -> Result<Arc<Self>, TierError> {
        let key = me.lease_key();
        let owner = me.encode();
        let ttl_ms = u64::try_from(config.lease_ttl.as_millis()).unwrap_or(u64::MAX);
        let give_up_at = meta.now_ms().saturating_add(ttl_ms.saturating_mul(2));
        let grant = loop {
            match meta.acquire_lease(&key, &owner, config.lease_ttl).await {
                Ok(grant) => break grant,
                Err(MetaError::Rejected(ApplyError::LeaseHeld { owner: held, .. })) => {
                    if meta.now_ms() >= give_up_at {
                        return Err(TierError::Other(format!(
                            "node {} is still registered by {held:?} after {:?}",
                            me.node_id,
                            config.lease_ttl * 2
                        )));
                    }
                    tracing::info!(node = me.node_id, %held, "waiting for an earlier incarnation's lease");
                    tokio::time::sleep(REGISTER_RETRY).await;
                }
                Err(
                    err @ (MetaError::NotLeader { .. }
                    | MetaError::Timeout
                    | MetaError::Unavailable(_)),
                ) => {
                    if meta.now_ms() >= give_up_at {
                        return Err(err.into());
                    }
                    tokio::time::sleep(REGISTER_RETRY).await;
                }
                Err(err) => return Err(err.into()),
            }
        };
        let registry = Arc::new(Self {
            meta: Some(meta),
            live: RwLock::new(Arc::new(vec![me.clone()])),
            me,
            config,
            epoch: Mutex::new(grant.epoch),
            tasks: Mutex::new(Vec::new()),
        });
        if let Err(err) = registry.refresh().await {
            tracing::warn!(%err, "the first registry refresh failed");
        }
        let renew = tokio::spawn(renew_loop(Arc::downgrade(&registry)));
        let refresh = tokio::spawn(refresh_loop(Arc::downgrade(&registry)));
        registry
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend([renew, refresh]);
        Ok(registry)
    }

    /// A registry of one node that never touches the metastore (single-node
    /// modes and tests).
    pub fn standalone(me: NodeDescriptor) -> Arc<Self> {
        Self::fixed(me.clone(), vec![me])
    }

    /// A registry whose live set is `nodes` (sorted by id) and never changes
    /// unless [`NodeRegistry::set_live`] is called; no metastore.
    pub fn fixed(me: NodeDescriptor, mut nodes: Vec<NodeDescriptor>) -> Arc<Self> {
        nodes.sort_by_key(|node| node.node_id);
        Arc::new(Self {
            meta: None,
            me,
            config: RegistryConfig::default(),
            epoch: Mutex::new(0),
            live: RwLock::new(Arc::new(nodes)),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn me(&self) -> &NodeDescriptor {
        &self.me
    }

    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    /// Held leases at the last refresh, sorted by `node_id`.
    pub fn live(&self) -> Arc<Vec<NodeDescriptor>> {
        self.live
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the live set (tests; a fixed registry's only way to change).
    pub fn set_live(&self, mut nodes: Vec<NodeDescriptor>) {
        nodes.sort_by_key(|node| node.node_id);
        *self.live.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(nodes);
    }

    /// Re-reads the held `node/` leases (one `Local` read) and publishes the
    /// ones whose owner decodes. A standalone registry does nothing.
    pub async fn refresh(&self) -> Result<(), TierError> {
        let Some(meta) = &self.meta else {
            return Ok(());
        };
        let leases = meta
            .leases_with_prefix(Consistency::Local, NODE_LEASE_PREFIX)
            .await?;
        let now = meta.now_ms();
        let mut nodes = Vec::new();
        for (key, lease) in leases {
            if !lease.is_held_at(now) {
                continue;
            }
            let Some(owner) = &lease.owner else { continue };
            let id = key[NODE_LEASE_PREFIX.len()..].parse::<u64>();
            match id
                .map_err(|err| TierError::Other(format!("bad node lease key {key:?}: {err}")))
                .and_then(|id| NodeDescriptor::decode(id, owner))
            {
                Ok(node) => nodes.push(node),
                Err(err) => tracing::warn!(%err, "skipping a node lease"),
            }
        }
        self.set_live(nodes);
        Ok(())
    }

    /// Stops the background tasks without releasing the lease: it expires
    /// after its TTL, as when the process dies.
    pub fn stop(&self) {
        for task in self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
        {
            task.abort();
        }
    }

    /// Stops renewing and releases the lease.
    pub async fn deregister(&self) {
        self.stop();
        let Some(meta) = &self.meta else { return };
        let epoch = *self.epoch.lock().unwrap_or_else(PoisonError::into_inner);
        if let Err(err) = meta
            .release_lease(&self.me.lease_key(), &self.me.encode(), epoch)
            .await
        {
            tracing::warn!(node = self.me.node_id, %err, "releasing the node lease");
        }
    }

    /// One renewal: extends the lease, or takes it again once it was lost
    /// (a partitioned node that comes back registers again).
    async fn renew(&self) -> Result<(), TierError> {
        let Some(meta) = &self.meta else {
            return Ok(());
        };
        let key = self.me.lease_key();
        let owner = self.me.encode();
        let epoch = *self.epoch.lock().unwrap_or_else(PoisonError::into_inner);
        let grant = match meta
            .renew_lease(&key, &owner, epoch, self.config.lease_ttl)
            .await
        {
            Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {
                tracing::warn!(
                    node = self.me.node_id,
                    "the node lease was lost; registering again"
                );
                meta.acquire_lease(&key, &owner, self.config.lease_ttl)
                    .await?
            }
            other => other?,
        };
        *self.epoch.lock().unwrap_or_else(PoisonError::into_inner) = grant.epoch;
        Ok(())
    }
}

impl Drop for NodeRegistry {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn renew_loop(registry: Weak<NodeRegistry>) {
    loop {
        let every = match registry.upgrade() {
            Some(registry) => registry.config.renew_every,
            None => return,
        };
        tokio::time::sleep(every).await;
        let Some(registry) = registry.upgrade() else {
            return;
        };
        if let Err(err) = registry.renew().await {
            tracing::warn!(node = registry.me.node_id, %err, "renewing the node lease");
        }
    }
}

async fn refresh_loop(registry: Weak<NodeRegistry>) {
    loop {
        let every = match registry.upgrade() {
            Some(registry) => registry.config.refresh_every,
            None => return,
        };
        tokio::time::sleep(every).await;
        let Some(registry) = registry.upgrade() else {
            return;
        };
        if let Err(err) = registry.refresh().await {
            tracing::warn!(node = registry.me.node_id, %err, "refreshing the node registry");
        }
    }
}
