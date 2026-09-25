//! A metastore client that follows the leader and retries.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use operon_common::schema::CollectionSchema;
use operon_common::{CollectionId, NamespaceId, StreamId};
use tokio::sync::watch;

use crate::clock::Clock;
use crate::command::{ApplyError, Command, Reply};
use crate::error::MetaError;
use crate::node::{Consistency, MetaNode};
use crate::raft::NodeId;
use crate::state::MetaState;
use crate::types::{
    AliasAction, Fence, Freshness, LeaseGrant, LinkId, Retention, TargetRef, WalChunk, WalClass,
};

/// The longest wait between two attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How a [`MetaClient`] retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetaClientConfig {
    /// How long a request keeps being retried. An attempt that is in flight
    /// when the deadline passes still finishes (each attempt is bounded by the
    /// node's request timeout). Zero means a single attempt. Default 10 s.
    pub retry_deadline: Duration,
    /// The first wait between attempts; it doubles after each attempt, up to
    /// 1 s. Default 50 ms.
    pub backoff: Duration,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            retry_deadline: Duration::from_secs(10),
            backoff: Duration::from_millis(50),
        }
    }
}

struct Inner {
    /// The local node first, then its peers.
    nodes: Vec<MetaNode>,
    clock: Arc<dyn Clock>,
    config: MetaClientConfig,
    /// The node that last accepted a write or confirmed a linearizable read.
    leader: Mutex<Option<NodeId>>,
    /// Test hook: the next successful writes report `Timeout` instead.
    lost_acks: AtomicU32,
}

/// A metastore client for a process that runs a meta node: reads are served
/// by the local node, and writes and linearizable reads go to the leader.
///
/// Requests that fail with [`MetaError::NotLeader`], [`MetaError::Timeout`]
/// or [`MetaError::Unavailable`] are retried, following the leader hint of a
/// `NotLeader` and otherwise trying the next node, until
/// [`MetaClientConfig::retry_deadline`]. That is safe because every command
/// is retry-safe (each [`Command`] says how); a retried write may therefore
/// report its first attempt's effect, such as
/// [`ApplyError::NamespaceExists`](crate::ApplyError::NamespaceExists). When
/// the deadline passes, the last error is returned, and a write's outcome is
/// unknown. Cheap to clone.
#[derive(Clone)]
pub struct MetaClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for MetaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ids: Vec<NodeId> = self.inner.nodes.iter().map(MetaNode::id).collect();
        f.debug_struct("MetaClient")
            .field("nodes", &ids)
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn is_retryable(err: &MetaError) -> bool {
    matches!(
        err,
        MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_)
    )
}

impl MetaClient {
    /// A client of `local`, which can reach the leader among `peers` (empty
    /// for a single-node metastore). `clock` stamps commands that carry time.
    pub fn new(
        local: MetaNode,
        peers: Vec<MetaNode>,
        clock: Arc<dyn Clock>,
        config: MetaClientConfig,
    ) -> Self {
        let local_id = local.id();
        let mut nodes = vec![local];
        nodes.extend(peers.into_iter().filter(|p| p.id() != local_id));
        Self {
            inner: Arc::new(Inner {
                nodes,
                clock,
                config,
                leader: Mutex::new(None),
                lost_acks: AtomicU32::new(0),
            }),
        }
    }

    /// The local meta node.
    pub fn local(&self) -> &MetaNode {
        &self.inner.nodes[0]
    }

    /// The current time from the client's clock, in ms since the epoch.
    pub fn now_ms(&self) -> u64 {
        self.inner.clock.now_ms()
    }

    /// Watches the local node's last applied log index
    /// ([`MetaNode::watch_applied`]).
    pub fn watch_applied(&self) -> watch::Receiver<u64> {
        self.local().watch_applied()
    }

    /// For tests: makes the next successful write through this client (or
    /// any clone) report [`MetaError::Timeout`] although it was applied, like
    /// a lost acknowledgement. The client then retries as it would after a
    /// real one. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn inject_lost_ack(&self) {
        self.inner.lost_acks.fetch_add(1, Ordering::SeqCst);
    }

    fn take_lost_ack(&self) -> bool {
        self.inner
            .lost_acks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }

    fn position(&self, id: NodeId) -> Option<usize> {
        self.inner.nodes.iter().position(|n| n.id() == id)
    }

    fn remember_leader(&self, id: NodeId) {
        *self
            .inner
            .leader
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(id);
    }

    /// Where to send the first attempt: the last known leader, else the local node.
    fn first_target(&self) -> usize {
        let leader = *self
            .inner
            .leader
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        leader.and_then(|id| self.position(id)).unwrap_or(0)
    }

    /// Runs `op` against the leader, retrying as the type docs describe.
    /// Also returns whether an attempt before the last one failed with an
    /// error that leaves a write's outcome unknown.
    async fn on_leader<T, F, Fut>(&self, op: F) -> (Result<T, MetaError>, bool)
    where
        F: Fn(MetaNode) -> Fut,
        Fut: Future<Output = Result<T, MetaError>>,
    {
        let deadline = Instant::now() + self.inner.config.retry_deadline;
        let mut backoff = self.inner.config.backoff;
        let mut target = self.first_target();
        let mut followed_hint = false;
        let mut earlier_unknown = false;
        let count = self.inner.nodes.len();
        loop {
            let node = self.inner.nodes[target].clone();
            let err = match op(node.clone()).await {
                Ok(value) => {
                    self.remember_leader(node.id());
                    return (Ok(value), earlier_unknown);
                }
                Err(err) if !is_retryable(&err) => return (Err(err), earlier_unknown),
                Err(err) => err,
            };
            // A fresh hint to another node is followed at once; anything else
            // waits out the backoff first, so two nodes that disagree about the
            // leader cannot make the client spin.
            let hinted = match &err {
                MetaError::NotLeader {
                    leader: Some(leader),
                } => self.position(*leader).filter(|&i| i != target),
                _ => None,
            };
            if let Some(next) = hinted
                && !followed_hint
            {
                target = next;
                followed_hint = true;
                earlier_unknown = true;
                continue;
            }
            target = hinted.unwrap_or((target + 1) % count);
            followed_hint = false;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return (Err(err), earlier_unknown);
            }
            earlier_unknown = true;
            tracing::debug!(%err, ?backoff, "metastore request failed; retrying");
            tokio::time::sleep(backoff.min(remaining)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Proposes `command` on the leader and waits until it is applied.
    pub async fn write(&self, command: Command) -> Result<Reply, MetaError> {
        self.write_tracked(command).await.0
    }

    /// Like [`MetaClient::write`], and also says whether an earlier attempt of
    /// this call failed with an error that leaves its outcome unknown
    /// (`NotLeader`, `Timeout` or `Unavailable`). When it did, a rejection of
    /// the final attempt does not prove the command was never applied: for
    /// example a retried `CommitWal` whose commit record has since been pruned
    /// is rejected as stale although the first attempt committed it.
    pub async fn write_tracked(&self, command: Command) -> (Result<Reply, MetaError>, bool) {
        self.on_leader(|node| {
            let command = command.clone();
            async move {
                let reply = node.write(command).await?;
                if self.take_lost_ack() {
                    return Err(MetaError::Timeout);
                }
                Ok(reply)
            }
        })
        .await
    }

    /// Runs `f` against the metastore state: the local node's for
    /// [`Consistency::Local`], the leader's after it confirms its leadership
    /// for [`Consistency::Linearizable`].
    pub async fn read<T>(
        &self,
        consistency: Consistency,
        f: impl FnOnce(&MetaState) -> T,
    ) -> Result<T, MetaError> {
        match consistency {
            Consistency::Local => self.local().read(Consistency::Local, f).await,
            Consistency::Linearizable => {
                let leader = self
                    .on_leader(|node| async move {
                        node.read(Consistency::Linearizable, |_| ()).await?;
                        Ok(node)
                    })
                    .await
                    .0?;
                // The leader has applied everything up to the confirmed read
                // index, and its state only moves forward.
                leader.read(Consistency::Local, f).await
            }
        }
    }

    pub async fn create_namespace(&self, name: &str) -> Result<NamespaceId, MetaError> {
        let command = Command::CreateNamespace {
            name: name.to_string(),
        };
        match self.write(command).await? {
            Reply::NamespaceCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Creates a stream that keeps its records forever.
    pub async fn create_stream(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
    ) -> Result<StreamId, MetaError> {
        self.create_stream_with_retention(namespace, name, partitions, class, Retention::default())
            .await
    }

    /// Creates a stream with a retention policy, in one command.
    pub async fn create_stream_with_retention(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> Result<StreamId, MetaError> {
        let command = Command::CreateStream {
            namespace,
            name: name.to_string(),
            partitions,
            class,
            retention,
        };
        match self.write(command).await? {
            Reply::StreamCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Declares a link from `source` into `target` ([`Command::CreateLink`]).
    /// A retry after a lost acknowledgement reports
    /// [`ApplyError::LinkExists`](crate::ApplyError::LinkExists) with the id.
    pub async fn create_link(
        &self,
        namespace: NamespaceId,
        name: &str,
        source: StreamId,
        target: TargetRef,
        options: std::collections::BTreeMap<String, String>,
    ) -> Result<LinkId, MetaError> {
        let command = Command::CreateLink {
            namespace,
            name: name.to_string(),
            source,
            target,
            options,
        };
        match self.write(command).await? {
            Reply::LinkCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Commits a durable WAL object created at `created_at_ms` (its ULID
    /// time); returns each chunk's base offset.
    pub async fn commit_wal(
        &self,
        object: &str,
        created_at_ms: u64,
        chunks: Vec<WalChunk>,
    ) -> Result<Vec<u64>, MetaError> {
        let command = Command::CommitWal {
            object: object.to_string(),
            created_at_ms,
            chunks,
        };
        match self.write(command).await? {
            Reply::WalCommitted { base_offsets } => Ok(base_offsets),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Replaces WAL index entries with a segment entry
    /// ([`Command::SwapSegment`]), stamped with the client's clock.
    #[allow(clippy::too_many_arguments)]
    pub async fn swap_segment(
        &self,
        stream: StreamId,
        partition: u32,
        replaces: Vec<(u64, String)>,
        segment: &str,
        byte_range: Range<u64>,
        max_timestamp_ms: i64,
        fence: Option<Fence>,
        fresh: Freshness,
    ) -> Result<(), MetaError> {
        let command = Command::SwapSegment {
            stream,
            partition,
            replaces,
            segment: segment.to_string(),
            byte_range,
            max_timestamp_ms,
            fence,
            now_ms: self.now_ms(),
            fresh,
        };
        match self.write(command).await? {
            Reply::SegmentSwapped => Ok(()),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Trims a partition below `before_offset`, fenced by `fence` if given;
    /// returns the new log start.
    pub async fn trim_partition(
        &self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
    ) -> Result<u64, MetaError> {
        let command = Command::TrimPartition {
            stream,
            partition,
            before_offset,
            fence,
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::Trimmed { log_start_offset } => Ok(log_start_offset),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn set_retention(
        &self,
        stream: StreamId,
        retention: Retention,
    ) -> Result<(), MetaError> {
        match self
            .write(Command::SetRetention { stream, retention })
            .await?
        {
            Reply::RetentionSet => Ok(()),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Prunes old WAL commit records, fenced by `fence` if given; returns how
    /// many were removed.
    pub async fn prune_wal_commits(&self, fence: Option<Fence>) -> Result<u32, MetaError> {
        let command = Command::PruneWalCommits {
            fence,
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::Pruned { removed } => Ok(removed),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Removes collected objects from the retired set, fenced by `fence` if
    /// given; returns how many were there.
    pub async fn forget_objects(
        &self,
        objects: Vec<String>,
        fence: Option<Fence>,
    ) -> Result<u32, MetaError> {
        match self
            .write(Command::ForgetObjects { objects, fence })
            .await?
        {
            Reply::Forgotten { removed } => Ok(removed),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn acquire_lease(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseGrant, MetaError> {
        let command = Command::AcquireLease {
            key: key.to_string(),
            owner: owner.to_string(),
            ttl_ms: millis(ttl),
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::Lease(grant) => Ok(grant),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn renew_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<LeaseGrant, MetaError> {
        let command = Command::RenewLease {
            key: key.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms: millis(ttl),
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::Lease(grant) => Ok(grant),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Extends a lease `owner` still holds at `epoch`, even if it expired,
    /// as long as nobody else took it ([`Command::ReacquireLease`]).
    pub async fn reacquire_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<LeaseGrant, MetaError> {
        let command = Command::ReacquireLease {
            key: key.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms: millis(ttl),
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::Lease(grant) => Ok(grant),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn release_lease(&self, key: &str, owner: &str, epoch: u64) -> Result<(), MetaError> {
        let command = Command::ReleaseLease {
            key: key.to_string(),
            owner: owner.to_string(),
            epoch,
        };
        match self.write(command).await? {
            Reply::LeaseReleased => Ok(()),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Compare-and-swap on a pointer; returns the new version.
    pub async fn cas_pointer(
        &self,
        namespace: NamespaceId,
        key: &str,
        expected: Option<u64>,
        value: &str,
        fence: Option<Fence>,
    ) -> Result<u64, MetaError> {
        self.cas_pointer_fresh(namespace, key, expected, value, fence, None)
            .await
    }

    /// [`MetaClient::cas_pointer`], refused with
    /// [`ApplyError::StaleObject`](crate::ApplyError::StaleObject) once the
    /// objects the value references are no longer `fresh`.
    pub async fn cas_pointer_fresh(
        &self,
        namespace: NamespaceId,
        key: &str,
        expected: Option<u64>,
        value: &str,
        fence: Option<Fence>,
        fresh: Option<Freshness>,
    ) -> Result<u64, MetaError> {
        let command = Command::CasPointer {
            namespace,
            key: key.to_string(),
            expected,
            value: value.to_string(),
            fence,
            fresh,
        };
        match self.write(command).await? {
            Reply::PointerSet { version } => Ok(version),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Creates a collection with its implicit stream and link
    /// ([`Command::CreateCollection`]); returns their ids.
    ///
    /// A retry after a lost acknowledgement that finds the collection
    /// ([`ApplyError::CollectionExists`]) returns the first attempt's ids.
    /// Without an earlier attempt of unknown outcome, `CollectionExists` means
    /// the collection was there before this call, and is returned as is.
    pub async fn create_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
        schema: CollectionSchema,
        partitions: u32,
    ) -> Result<(CollectionId, StreamId, LinkId), MetaError> {
        let command = Command::CreateCollection {
            namespace,
            name: name.to_string(),
            schema,
            partitions,
        };
        match self.write_tracked(command).await {
            (Ok(Reply::CollectionCreated { id, stream, link }), _) => Ok((id, stream, link)),
            (Ok(other), _) => Err(MetaError::UnexpectedReply(other)),
            (Err(MetaError::Rejected(ApplyError::CollectionExists(id))), true) => {
                self.created_collection(id).await
            }
            (Err(err), _) => Err(err),
        }
    }

    /// The ids of collection `id`, which a retried create found. The local
    /// node may lag the leader that applied the create; the leader then has it.
    async fn created_collection(
        &self,
        id: CollectionId,
    ) -> Result<(CollectionId, StreamId, LinkId), MetaError> {
        let ids = |state: &MetaState| state.collection(id).map(|c| (c.id, c.stream, c.link));
        if let Some(ids) = self.read(Consistency::Local, ids).await? {
            return Ok(ids);
        }
        self.read(Consistency::Linearizable, ids)
            .await?
            // Dropped since: report what the metastore said.
            .ok_or(MetaError::Rejected(ApplyError::CollectionExists(id)))
    }

    /// Drops a collection ([`Command::DropCollection`]), stamped with the
    /// client's clock; returns its id, or `None` if there was none of that
    /// name (also when a retry after a lost acknowledgement finds it
    /// already dropped).
    pub async fn drop_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
    ) -> Result<Option<CollectionId>, MetaError> {
        let command = Command::DropCollection {
            namespace,
            name: name.to_string(),
            now_ms: self.now_ms(),
        };
        match self.write(command).await? {
            Reply::CollectionDropped(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Replaces a collection's schema at `expected_version` with an additive
    /// extension ([`Command::UpdateCollectionSchema`]); returns the new version.
    pub async fn update_collection_schema(
        &self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> Result<u64, MetaError> {
        let command = Command::UpdateCollectionSchema {
            collection,
            expected_version,
            schema,
        };
        match self.write(command).await? {
            Reply::SchemaUpdated { version } => Ok(version),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Applies alias actions atomically ([`Command::UpdateAliases`]).
    pub async fn update_aliases(
        &self,
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> Result<(), MetaError> {
        match self
            .write(Command::UpdateAliases { namespace, actions })
            .await?
        {
            Reply::AliasesUpdated => Ok(()),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }
}
