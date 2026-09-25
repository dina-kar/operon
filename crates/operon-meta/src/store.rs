//! [`MetaStore`] for the openraft [`MetaClient`] (M1.2a plan, Task 4).
//!
//! Writes delegate to the client's inherent methods, or, for the tracked
//! writes, build the [`Command`] and call [`MetaClient::write_tracked`]. Each
//! read is exactly one [`MetaClient::read`] closure over [`MetaState`], so it
//! sees one consistent state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use operon_common::meta::{
    AliasAction, ChangeWait, Collection, CollectionHead, CollectionRoots, Consistency, Fence,
    Lease, LeaseGrant, Link, LinkHead, LinkId, MetaChanges, MetaError, MetaResult, MetaStopped,
    MetaStore, Namespace, PartitionBounds, PartitionIndex, Pointer, PointerCas, Retention,
    SegmentSwap, Stream, StreamState, TargetRef, Tracked, WalClass, WalCommit,
    collection_pointer_key, link_pointer_key,
};
use operon_common::schema::CollectionSchema;
use operon_common::{CollectionId, NamespaceId, StreamId};
use tokio::sync::watch;

use crate::client::MetaClient;
use crate::command::{Command, Reply};
use crate::node::MetaNode;
use crate::state::MetaState;
use crate::types::PartitionState;

/// The change watch over the local node's applied index: armed by
/// `borrow_and_update` when created, and stopped when the node stops (its
/// Raft shut down or failed) or the channel closes.
#[derive(Debug)]
struct AppliedChanges {
    applied: watch::Receiver<u64>,
    node: MetaNode,
}

#[async_trait]
impl ChangeWait for AppliedChanges {
    async fn changed(&mut self) -> Result<(), MetaStopped> {
        tokio::select! {
            biased;
            changed = self.applied.changed() => changed.map_err(|_| MetaStopped),
            () = self.node.stopped() => Err(MetaStopped),
        }
    }
}

/// Maps a tracked write's reply with `expect`; any other reply is
/// [`MetaError::UnexpectedReply`].
fn tracked<T>(
    (result, earlier_unknown): (Result<Reply, MetaError>, bool),
    expect: impl FnOnce(Reply) -> Result<T, Reply>,
) -> Tracked<T> {
    let result = result.and_then(|reply| {
        expect(reply).map_err(|other| MetaError::UnexpectedReply(format!("{other:?}")))
    });
    Tracked {
        result,
        earlier_unknown,
    }
}

/// A collection with its pointer, its implicit stream's bounds (0 for a
/// partition without state) and the clock, from `state`.
fn collection_head(state: &MetaState, collection: &Collection) -> CollectionHead {
    let per_partition = |bound: fn(&PartitionState) -> u64| {
        (0..collection.partitions)
            .map(|p| state.partition(collection.stream, p).map_or(0, bound))
            .collect()
    };
    CollectionHead {
        collection: collection.clone(),
        pointer: state
            .pointer(collection.namespace, &collection_pointer_key(collection.id))
            .cloned(),
        log_start_offsets: per_partition(PartitionState::log_start_offset),
        high_watermarks: per_partition(PartitionState::high_watermark),
        clock_ms: state.clock_ms(),
    }
}

#[async_trait]
impl MetaStore for MetaClient {
    // ----- Clock, changes, readiness -----

    fn now_ms(&self) -> u64 {
        MetaClient::now_ms(self)
    }

    fn watch_changes(&self) -> MetaChanges {
        let mut applied = self.watch_applied();
        applied.borrow_and_update();
        MetaChanges::new(AppliedChanges {
            applied,
            node: self.local().clone(),
        })
    }

    fn is_ready(&self) -> bool {
        self.local().status().leader.is_some()
    }

    async fn clock_ms(&self, consistency: Consistency) -> MetaResult<u64> {
        self.read(consistency, |s| s.clock_ms()).await
    }

    // ----- Catalog: namespaces, streams, links -----

    async fn create_namespace(&self, name: &str) -> MetaResult<NamespaceId> {
        MetaClient::create_namespace(self, name).await
    }

    async fn namespace_by_name(
        &self,
        consistency: Consistency,
        name: &str,
    ) -> MetaResult<Option<Namespace>> {
        self.read(consistency, |s| s.namespace_by_name(name).cloned())
            .await
    }

    async fn namespaces(&self, consistency: Consistency) -> MetaResult<Vec<Namespace>> {
        self.read(consistency, |s| s.namespaces().cloned().collect())
            .await
    }

    async fn create_stream(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> MetaResult<StreamId> {
        self.create_stream_with_retention(namespace, name, partitions, class, retention)
            .await
    }

    async fn set_retention(&self, stream: StreamId, retention: Retention) -> MetaResult<()> {
        MetaClient::set_retention(self, stream, retention).await
    }

    async fn stream(&self, consistency: Consistency, id: StreamId) -> MetaResult<Option<Stream>> {
        self.read(consistency, |s| s.stream(id).cloned()).await
    }

    async fn stream_by_name(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Stream>> {
        self.read(consistency, |s| s.stream_by_name(namespace, name).cloned())
            .await
    }

    async fn streams(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Stream>> {
        self.read(consistency, |s| match namespace {
            Some(namespace) => s.streams(namespace).cloned().collect(),
            None => s.all_streams().cloned().collect(),
        })
        .await
    }

    async fn stream_state(
        &self,
        consistency: Consistency,
        id: StreamId,
    ) -> MetaResult<Option<StreamState>> {
        self.read(consistency, |s| {
            let stream = s.stream(id)?.clone();
            let partitions = (0..stream.partitions)
                .map(|p| {
                    s.partition(id, p).map(|state| PartitionBounds {
                        log_start_offset: state.log_start_offset(),
                        high_watermark: state.high_watermark(),
                        bytes: state.bytes(),
                    })
                })
                .collect();
            Some(StreamState { stream, partitions })
        })
        .await
    }

    async fn create_link(
        &self,
        namespace: NamespaceId,
        name: &str,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> MetaResult<LinkId> {
        MetaClient::create_link(self, namespace, name, source, target, options).await
    }

    async fn link_by_name(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Link>> {
        self.read(consistency, |s| s.link_by_name(namespace, name).cloned())
            .await
    }

    async fn links(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Link>> {
        self.read(consistency, |s| match namespace {
            Some(namespace) => s.links(namespace).cloned().collect(),
            None => s.all_links().cloned().collect(),
        })
        .await
    }

    async fn links_with_pointers(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<LinkHead>> {
        self.read(consistency, |s| {
            s.links(namespace)
                .map(|link| LinkHead {
                    link: link.clone(),
                    pointer: s.pointer(namespace, &link_pointer_key(link.id)).cloned(),
                })
                .collect()
        })
        .await
    }

    // ----- Sequencer and offset index -----

    async fn commit_wal(&self, commit: WalCommit) -> Tracked<Vec<u64>> {
        let command = Command::CommitWal {
            object: commit.object,
            created_at_ms: commit.created_at_ms,
            chunks: commit.chunks,
        };
        tracked(self.write_tracked(command).await, |reply| match reply {
            Reply::WalCommitted { base_offsets } => Ok(base_offsets),
            other => Err(other),
        })
    }

    async fn swap_segment(&self, swap: SegmentSwap) -> Tracked<()> {
        let command = Command::SwapSegment {
            stream: swap.stream,
            partition: swap.partition,
            replaces: swap.replaces,
            segment: swap.segment,
            byte_range: swap.byte_range,
            max_timestamp_ms: swap.max_timestamp_ms,
            fence: swap.fence,
            now_ms: MetaClient::now_ms(self),
            fresh: swap.fresh,
        };
        tracked(self.write_tracked(command).await, |reply| match reply {
            Reply::SegmentSwapped => Ok(()),
            other => Err(other),
        })
    }

    async fn trim_partition(
        &self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
    ) -> MetaResult<u64> {
        MetaClient::trim_partition(self, stream, partition, before_offset, fence).await
    }

    async fn partition_index(
        &self,
        consistency: Consistency,
        stream: StreamId,
        partition: u32,
        from_offset: u64,
        max_bytes: Option<u64>,
    ) -> MetaResult<Option<PartitionIndex>> {
        self.read(consistency, |s| {
            s.stream(stream)?;
            let state = s.partition(stream, partition)?;
            // The fetch plan's loop (as built in `operon-log`'s reader).
            let mut entries = Vec::new();
            let mut bytes = 0u64;
            for entry in state.entries_from(from_offset) {
                if let Some(max_bytes) = max_bytes
                    && !entries.is_empty()
                    && bytes >= max_bytes
                {
                    break;
                }
                bytes += entry.byte_range.end - entry.byte_range.start;
                entries.push(entry.clone());
            }
            Some(PartitionIndex::new(
                state.log_start_offset(),
                state.next_offset(),
                state.bytes(),
                entries,
            ))
        })
        .await
    }

    // ----- Leases and fencing -----

    async fn acquire_lease(&self, key: &str, owner: &str, ttl: Duration) -> MetaResult<LeaseGrant> {
        MetaClient::acquire_lease(self, key, owner, ttl).await
    }

    async fn renew_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        MetaClient::renew_lease(self, key, owner, epoch, ttl).await
    }

    async fn reacquire_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        MetaClient::reacquire_lease(self, key, owner, epoch, ttl).await
    }

    async fn release_lease(&self, key: &str, owner: &str, epoch: u64) -> MetaResult<()> {
        MetaClient::release_lease(self, key, owner, epoch).await
    }

    async fn lease(&self, consistency: Consistency, key: &str) -> MetaResult<Option<Lease>> {
        self.read(consistency, |s| s.lease(key).cloned()).await
    }

    // ----- Manifest pointers -----

    async fn cas_pointer(&self, cas: PointerCas) -> Tracked<u64> {
        let command = Command::CasPointer {
            namespace: cas.namespace,
            key: cas.key,
            expected: cas.expected,
            value: cas.value,
            fence: cas.fence,
            fresh: cas.fresh,
        };
        tracked(self.write_tracked(command).await, |reply| match reply {
            Reply::PointerSet { version } => Ok(version),
            other => Err(other),
        })
    }

    async fn pointer(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        key: &str,
    ) -> MetaResult<Option<Pointer>> {
        self.read(consistency, |s| s.pointer(namespace, key).cloned())
            .await
    }

    // ----- Collections -----

    async fn create_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
        schema: CollectionSchema,
        partitions: u32,
    ) -> MetaResult<(CollectionId, StreamId, LinkId)> {
        MetaClient::create_collection(self, namespace, name, schema, partitions).await
    }

    async fn drop_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<CollectionId>> {
        MetaClient::drop_collection(self, namespace, name).await
    }

    async fn update_collection_schema(
        &self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> MetaResult<u64> {
        MetaClient::update_collection_schema(self, collection, expected_version, schema).await
    }

    async fn update_aliases(
        &self,
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> MetaResult<()> {
        MetaClient::update_aliases(self, namespace, actions).await
    }

    async fn collection(
        &self,
        consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<Collection>> {
        self.read(consistency, |s| s.collection(id).cloned()).await
    }

    async fn resolve_collection(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name_or_alias: &str,
    ) -> MetaResult<Option<Collection>> {
        self.read(consistency, |s| {
            s.resolve_collection(namespace, name_or_alias).cloned()
        })
        .await
    }

    async fn collection_for_link(
        &self,
        consistency: Consistency,
        link: LinkId,
    ) -> MetaResult<Option<Collection>> {
        self.read(consistency, |s| s.collection_for_link(link).cloned())
            .await
    }

    async fn collections(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Collection>> {
        self.read(consistency, |s| match namespace {
            Some(namespace) => s.collections(namespace).cloned().collect(),
            None => s.all_collections().cloned().collect(),
        })
        .await
    }

    async fn aliases(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<(String, CollectionId)>> {
        self.read(consistency, |s| {
            s.aliases(namespace)
                .map(|(alias, id)| (alias.to_string(), id))
                .collect()
        })
        .await
    }

    async fn collection_head(
        &self,
        consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<CollectionHead>> {
        self.read(consistency, |s| {
            s.collection(id)
                .map(|collection| collection_head(s, collection))
        })
        .await
    }

    async fn collection_heads(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<CollectionHead>> {
        self.read(consistency, |s| match namespace {
            Some(namespace) => s
                .collections(namespace)
                .map(|collection| collection_head(s, collection))
                .collect(),
            None => s
                .all_collections()
                .map(|collection| collection_head(s, collection))
                .collect(),
        })
        .await
    }

    // ----- Garbage collection (always Linearizable) -----

    async fn retired_expired(&self, grace_ms: u64) -> MetaResult<Vec<String>> {
        self.read(Consistency::Linearizable, |s| {
            let now = s.clock_ms();
            s.retired()
                .filter(|(_, at)| at.saturating_add(grace_ms) <= now)
                .map(|(path, _)| path.to_string())
                .collect()
        })
        .await
    }

    async fn forget_objects(&self, objects: Vec<String>, fence: Option<Fence>) -> MetaResult<u32> {
        MetaClient::forget_objects(self, objects, fence).await
    }

    async fn prune_wal_commits(&self, fence: Option<Fence>) -> MetaResult<u32> {
        MetaClient::prune_wal_commits(self, fence).await
    }

    async fn orphan_wal_objects(
        &self,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.read(Consistency::Linearizable, |s| {
            let now = s.clock_ms();
            let retired: BTreeSet<&str> = s.retired().map(|(path, _)| path).collect();
            candidates
                .iter()
                .filter(|(_, created)| created.saturating_add(min_age_ms) <= now)
                .map(|(path, _)| path)
                .filter(|path| {
                    s.wal_live_chunks(path).is_none() && !retired.contains(path.as_str())
                })
                .take(limit)
                .cloned()
                .collect()
        })
        .await
    }

    async fn orphan_segments(
        &self,
        namespace: NamespaceId,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.read(Consistency::Linearizable, |s| {
            let now = s.clock_ms();
            let mut kept: BTreeSet<&str> = s.retired().map(|(path, _)| path).collect();
            for stream in s.streams(namespace) {
                for partition in 0..stream.partitions {
                    if let Some(state) = s.partition(stream.id, partition) {
                        kept.extend(state.entries().map(|e| e.object.as_str()));
                    }
                }
            }
            candidates
                .iter()
                .filter(|(_, created)| created.saturating_add(min_age_ms) <= now)
                .map(|(path, _)| path)
                .filter(|path| !kept.contains(path.as_str()))
                .take(limit)
                .cloned()
                .collect()
        })
        .await
    }

    async fn segment_referenced(
        &self,
        stream: StreamId,
        partition: u32,
        object: &str,
    ) -> MetaResult<bool> {
        self.read(Consistency::Linearizable, |s| {
            s.retired().any(|(path, _)| path == object)
                || s.partition(stream, partition)
                    .is_some_and(|state| state.entries().any(|e| e.object == object))
        })
        .await
    }

    async fn collection_roots(
        &self,
        namespace: NamespaceId,
        under: &str,
    ) -> MetaResult<CollectionRoots> {
        self.read(Consistency::Linearizable, |s| CollectionRoots {
            clock_ms: s.clock_ms(),
            collections: s
                .collections(namespace)
                .map(|c| {
                    let pointer = s.pointer(namespace, &collection_pointer_key(c.id)).cloned();
                    (c.clone(), pointer)
                })
                .collect(),
            retired_prefixes: s
                .retired()
                .map(|(path, _)| path)
                .filter(|path| path.starts_with(under) && path.ends_with('/'))
                .map(str::to_string)
                .collect(),
        })
        .await
    }
}

impl From<MetaClient> for Arc<dyn MetaStore> {
    fn from(client: MetaClient) -> Self {
        Arc::new(client)
    }
}

/// Clones the client (cheap: one `Arc`); the clone shares its state.
impl From<&MetaClient> for Arc<dyn MetaStore> {
    fn from(client: &MetaClient) -> Self {
        Arc::new(client.clone())
    }
}
