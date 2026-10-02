//! `impl MetaStore for TikvMeta`: the trait surface and the clock. Each
//! method delegates to its domain module (`catalog`, `leases`, `pointers`,
//! `log`, `gc`, `changes`).

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use loams_common::meta::{
    AliasAction, AliasTargetAction, AliasTargets, Collection, CollectionHead, CollectionRoots,
    Consistency, Fence, HotConfig, IdempotencyClaim, IdempotencyCompletion, IdempotencyEntry,
    IdempotencyKey, IdempotencyState, Lease, LeaseGrant, Link, LinkHead, LinkId, MetaChanges,
    MetaResult, MetaStore, NameTarget, Namespace, PartitionIndex, Pointer, PointerCas, Retention,
    SegmentSwap, Stream, StreamState, TargetRef, Tracked, WalClass, WalCommit,
};
use loams_common::schema::CollectionSchema;
use loams_common::{CollectionId, NamespaceId, StreamId};
use loams_tikv::Tikv;

use crate::{TikvMeta, tikv_error};

#[async_trait]
impl MetaStore for TikvMeta {
    // ----- Clock, changes, readiness -----

    fn now_ms(&self) -> u64 {
        self.now_estimate()
    }

    fn watch_changes(&self) -> MetaChanges {
        self.watch()
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
        id: StreamId,
    ) -> MetaResult<Option<StreamState>> {
        self.stream_state_impl(id).await
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

    // ----- Sequencer and offset index -----

    async fn commit_wal(&self, commit: WalCommit) -> Tracked<Vec<u64>> {
        self.commit_wal_impl(commit).await
    }

    async fn swap_segment(&self, swap: SegmentSwap) -> Tracked<()> {
        self.swap_segment_impl(swap).await
    }

    async fn trim_partition(
        &self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
    ) -> MetaResult<u64> {
        self.trim_partition_impl(stream, partition, before_offset, fence)
            .await
    }

    async fn partition_index(
        &self,
        _consistency: Consistency,
        stream: StreamId,
        partition: u32,
        from_offset: u64,
        max_bytes: Option<u64>,
    ) -> MetaResult<Option<PartitionIndex>> {
        self.partition_index_impl(stream, partition, from_offset, max_bytes)
            .await
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

    async fn update_alias_targets(
        &self,
        namespace: NamespaceId,
        actions: Vec<AliasTargetAction>,
    ) -> MetaResult<()> {
        self.update_alias_targets_impl(namespace, actions).await
    }

    async fn alias_targets(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<(String, AliasTargets)>> {
        self.alias_targets_impl(namespace).await
    }

    async fn resolve_name(
        &self,
        _consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<NameTarget>> {
        self.resolve_name_impl(namespace, name).await
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

    // ----- Garbage collection (always Linearizable) -----

    async fn retired_expired(&self, grace_ms: u64) -> MetaResult<Vec<String>> {
        self.retired_expired_impl(grace_ms).await
    }

    async fn forget_objects(&self, objects: Vec<String>, fence: Option<Fence>) -> MetaResult<u32> {
        self.forget_objects_impl(objects, fence).await
    }

    async fn prune_wal_commits(&self, fence: Option<Fence>) -> MetaResult<u32> {
        self.prune_wal_commits_impl(fence).await
    }

    async fn claim_idempotency_keys(
        &self,
        claim: IdempotencyClaim,
    ) -> MetaResult<Vec<IdempotencyState>> {
        self.claim_idempotency_keys_impl(claim).await
    }

    async fn complete_idempotency_keys(&self, completion: IdempotencyCompletion) -> MetaResult<()> {
        self.complete_idempotency_keys_impl(completion).await
    }

    async fn release_idempotency_keys(
        &self,
        stream: StreamId,
        owner: &str,
        keys: Vec<IdempotencyKey>,
    ) -> MetaResult<()> {
        self.release_idempotency_keys_impl(stream, owner, keys)
            .await
    }

    async fn idempotency_key(
        &self,
        consistency: Consistency,
        stream: StreamId,
        key: IdempotencyKey,
    ) -> MetaResult<Option<IdempotencyEntry>> {
        self.idempotency_key_impl(consistency, stream, key).await
    }

    async fn prune_idempotency_keys(&self, fence: Option<Fence>) -> MetaResult<u32> {
        self.prune_idempotency_keys_impl(fence).await
    }

    async fn orphan_wal_objects(
        &self,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.orphan_wal_objects_impl(candidates, min_age_ms, limit)
            .await
    }

    async fn orphan_segments(
        &self,
        namespace: NamespaceId,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.orphan_segments_impl(namespace, candidates, min_age_ms, limit)
            .await
    }

    async fn segment_referenced(
        &self,
        stream: StreamId,
        partition: u32,
        object: &str,
    ) -> MetaResult<bool> {
        self.segment_referenced_impl(stream, partition, object)
            .await
    }

    async fn collection_roots(
        &self,
        namespace: NamespaceId,
        under: &str,
    ) -> MetaResult<CollectionRoots> {
        self.collection_roots_impl(namespace, under).await
    }
}
