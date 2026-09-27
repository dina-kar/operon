//! `impl MetaStore for TikvMeta`: the trait surface, the clock and the change
//! watch. The log, garbage-collection and change-counter methods are Task 5's
//! and answer [`MetaError::Unavailable`] until then (R1 plan Task 4
//! semantics 5).

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use operon_common::meta::{
    AliasAction, ChangeWait, Collection, CollectionHead, CollectionRoots, Consistency, Fence,
    HotConfig, Lease, LeaseGrant, Link, LinkHead, LinkId, MetaChanges, MetaError, MetaResult,
    MetaStopped, MetaStore, Namespace, PartitionIndex, Pointer, PointerCas, Retention, SegmentSwap,
    Stream, StreamState, TargetRef, Tracked, WalClass, WalCommit,
};
use operon_common::schema::CollectionSchema;
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_tikv::Tikv;

use crate::{TikvMeta, tikv_error};

/// What Task 5's methods answer until it lands.
pub(crate) const NOT_YET: &str = "not implemented in R1 Task 4";

fn not_yet<T>() -> MetaResult<T> {
    Err(MetaError::Unavailable(NOT_YET.to_string()))
}

/// A change watch: wakes on this handle's own writes at once, and every
/// `poll` in any case (spurious wake-ups are allowed; Task 5 adds the
/// per-scope change counters that make the poll exact).
#[derive(Debug)]
struct PollWait {
    changes: tokio::sync::watch::Receiver<u64>,
    poll: Duration,
}

#[async_trait]
impl ChangeWait for PollWait {
    async fn changed(&mut self) -> Result<(), MetaStopped> {
        tokio::select! {
            changed = self.changes.changed() => changed.map_err(|_| MetaStopped),
            () = tokio::time::sleep(self.poll) => Ok(()),
        }
    }
}

#[async_trait]
impl MetaStore for TikvMeta {
    // ----- Clock, changes, readiness -----

    fn now_ms(&self) -> u64 {
        self.now_estimate()
    }

    fn watch_changes(&self) -> MetaChanges {
        let mut changes = self.inner.changes.subscribe();
        changes.mark_unchanged();
        MetaChanges::new(PollWait {
            changes,
            poll: self.inner.poll,
        })
    }

    fn is_ready(&self) -> bool {
        // Opening fetched a TSO timestamp; each call reaches the cluster on
        // its own after that.
        true
    }

    async fn clock_ms(&self, _consistency: Consistency) -> MetaResult<u64> {
        let ts = self.inner.tikv.now().await.map_err(tikv_error)?;
        let physical = Tikv::physical_ms(&ts);
        self.inner
            .last_now
            .fetch_max(physical, std::sync::atomic::Ordering::AcqRel);
        Ok(physical)
    }

    // ----- Catalog -----

    async fn create_namespace(&self, name: &str) -> MetaResult<NamespaceId> {
        self.create_namespace_impl(name).await
    }

    async fn namespace_by_name(
        &self,
        _consistency: Consistency,
        name: &str,
    ) -> MetaResult<Option<Namespace>> {
        self.namespace_by_name_impl(name).await
    }

    async fn namespaces(&self, _consistency: Consistency) -> MetaResult<Vec<Namespace>> {
        self.namespaces_impl().await
    }

    async fn create_stream(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> MetaResult<StreamId> {
        self.create_stream_impl(namespace, name, partitions, class, retention)
            .await
    }

    async fn set_retention(&self, stream: StreamId, retention: Retention) -> MetaResult<()> {
        self.set_retention_impl(stream, retention).await
    }

    async fn stream(&self, _consistency: Consistency, id: StreamId) -> MetaResult<Option<Stream>> {
        self.stream_impl(id).await
    }

    async fn stream_by_name(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Stream>> {
        self.stream_by_name_impl(namespace, name).await
    }

    async fn streams(
        &self,
        _consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Stream>> {
        self.streams_impl(namespace).await
    }

    async fn stream_state(
        &self,
        _consistency: Consistency,
        _id: StreamId,
    ) -> MetaResult<Option<StreamState>> {
        not_yet()
    }

    async fn create_link(
        &self,
        namespace: NamespaceId,
        name: &str,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> MetaResult<LinkId> {
        self.create_link_impl(namespace, name, source, target, options)
            .await
    }

    async fn link_by_name(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Link>> {
        self.link_by_name_impl(namespace, name).await
    }

    async fn links(
        &self,
        _consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Link>> {
        self.links_impl(namespace).await
    }

    async fn links_with_pointers(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<LinkHead>> {
        self.links_with_pointers_impl(namespace).await
    }

    // ----- Sequencer and offset index (Task 5) -----

    async fn commit_wal(&self, _commit: WalCommit) -> Tracked<Vec<u64>> {
        Tracked {
            result: not_yet(),
            earlier_unknown: false,
        }
    }

    async fn swap_segment(&self, _swap: SegmentSwap) -> Tracked<()> {
        Tracked {
            result: not_yet(),
            earlier_unknown: false,
        }
    }

    async fn trim_partition(
        &self,
        _stream: StreamId,
        _partition: u32,
        _before_offset: u64,
        _fence: Option<Fence>,
    ) -> MetaResult<u64> {
        not_yet()
    }

    async fn partition_index(
        &self,
        _consistency: Consistency,
        _stream: StreamId,
        _partition: u32,
        _from_offset: u64,
        _max_bytes: Option<u64>,
    ) -> MetaResult<Option<PartitionIndex>> {
        not_yet()
    }

    // ----- Leases and fencing -----

    async fn acquire_lease(&self, key: &str, owner: &str, ttl: Duration) -> MetaResult<LeaseGrant> {
        self.acquire_lease_impl(key, owner, ttl).await
    }

    async fn renew_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        self.renew_lease_impl(key, owner, epoch, ttl).await
    }

    async fn reacquire_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        self.reacquire_lease_impl(key, owner, epoch, ttl).await
    }

    async fn release_lease(&self, key: &str, owner: &str, epoch: u64) -> MetaResult<()> {
        self.release_lease_impl(key, owner, epoch).await
    }

    async fn lease(&self, _consistency: Consistency, key: &str) -> MetaResult<Option<Lease>> {
        self.lease_impl(key).await
    }

    async fn leases_with_prefix(
        &self,
        _consistency: Consistency,
        prefix: &str,
    ) -> MetaResult<Vec<(String, Lease)>> {
        self.leases_with_prefix_impl(prefix).await
    }

    // ----- Manifest pointers -----

    async fn cas_pointer(&self, cas: PointerCas) -> Tracked<u64> {
        self.cas_pointer_impl(cas).await
    }

    async fn pointer(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        key: &str,
    ) -> MetaResult<Option<Pointer>> {
        self.pointer_impl(namespace, key).await
    }

    // ----- Collections -----

    async fn create_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
        schema: CollectionSchema,
        partitions: u32,
    ) -> MetaResult<(CollectionId, StreamId, LinkId)> {
        self.create_collection_impl(namespace, name, schema, partitions)
            .await
    }

    async fn drop_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<CollectionId>> {
        self.drop_collection_impl(namespace, name).await
    }

    async fn update_collection_schema(
        &self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> MetaResult<u64> {
        self.update_collection_schema_impl(collection, expected_version, schema)
            .await
    }

    async fn update_aliases(
        &self,
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> MetaResult<()> {
        self.update_aliases_impl(namespace, actions).await
    }

    async fn collection(
        &self,
        _consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<Collection>> {
        self.collection_impl(id).await
    }

    async fn resolve_collection(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        name_or_alias: &str,
    ) -> MetaResult<Option<Collection>> {
        self.resolve_collection_impl(namespace, name_or_alias).await
    }

    async fn collection_for_link(
        &self,
        _consistency: Consistency,
        link: LinkId,
    ) -> MetaResult<Option<Collection>> {
        self.collection_for_link_impl(link).await
    }

    async fn collections(
        &self,
        _consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Collection>> {
        self.collections_impl(namespace).await
    }

    async fn aliases(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<(String, CollectionId)>> {
        self.aliases_impl(namespace).await
    }

    async fn collection_head(
        &self,
        _consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<CollectionHead>> {
        self.collection_head_impl(id).await
    }

    async fn collection_heads(
        &self,
        _consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<CollectionHead>> {
        self.collection_heads_impl(namespace).await
    }

    async fn set_collection_hot(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
        hot: HotConfig,
    ) -> MetaResult<()> {
        self.set_collection_hot_impl(namespace, collection, hot)
            .await
    }

    async fn collection_hot(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        collection: CollectionId,
    ) -> MetaResult<HotConfig> {
        self.collection_hot_impl(namespace, collection).await
    }

    // ----- Garbage collection (Task 5) -----

    async fn retired_expired(&self, _grace_ms: u64) -> MetaResult<Vec<String>> {
        not_yet()
    }

    async fn forget_objects(
        &self,
        _objects: Vec<String>,
        _fence: Option<Fence>,
    ) -> MetaResult<u32> {
        not_yet()
    }

    async fn prune_wal_commits(&self, _fence: Option<Fence>) -> MetaResult<u32> {
        not_yet()
    }

    async fn orphan_wal_objects(
        &self,
        _candidates: Vec<(String, u64)>,
        _min_age_ms: u64,
        _limit: usize,
    ) -> MetaResult<Vec<String>> {
        not_yet()
    }

    async fn orphan_segments(
        &self,
        _namespace: NamespaceId,
        _candidates: Vec<(String, u64)>,
        _min_age_ms: u64,
        _limit: usize,
    ) -> MetaResult<Vec<String>> {
        not_yet()
    }

    async fn segment_referenced(
        &self,
        _stream: StreamId,
        _partition: u32,
        _object: &str,
    ) -> MetaResult<bool> {
        not_yet()
    }

    async fn collection_roots(
        &self,
        _namespace: NamespaceId,
        _under: &str,
    ) -> MetaResult<CollectionRoots> {
        not_yet()
    }
}
