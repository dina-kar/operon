//! The metastore as every Operon crate outside the composition roots sees it
//! (D47): the [`MetaStore`] trait, its [`Consistency`] levels, the
//! [`Tracked`] result of writes whose outcome may be unknown, and the
//! [`MetaChanges`] watch.
//!
//! # Contract
//!
//! - **Semantic, not raw key-value.** Every read is a named domain query that
//!   returns owned records, and every write is one metastore operation taking
//!   typed arguments or a request struct ([`WalCommit`], [`SegmentSwap`],
//!   [`PointerCas`]). No method takes a closure, raw bytes or an untyped
//!   key-value pair, so each backend can serve each call with one native query
//!   or transaction. Pointers are the manifest-pointer domain object:
//!   versioned, compare-and-swap, fenced, freshness-checked, with their keys
//!   validated and `collection/<id>` keys checked against the catalog.
//! - **One call, one snapshot.** Every read returns data from one consistent
//!   state. Composite reads ([`MetaStore::collection_head`],
//!   [`MetaStore::collection_roots`], the garbage-collection queries) exist
//!   where the parts must come from the same state.
//! - **Unknown outcomes stay visible.** [`MetaStore::commit_wal`],
//!   [`MetaStore::swap_segment`] and [`MetaStore::cas_pointer`] return
//!   [`Tracked`], whose `earlier_unknown` says that an earlier attempt within
//!   the call ended with an unknown outcome. A rejection of the final attempt
//!   then does not prove that nothing was applied. Every other write returns
//!   [`MetaResult`].
//! - **Changes are watched, not polled.** [`MetaStore::watch_changes`] returns
//!   a [`MetaChanges`] armed when it is created: any state change applied after
//!   `watch_changes` returns makes [`MetaChanges::changed`] complete. Spurious
//!   wake-ups are allowed; `Err(MetaStopped)` means the metastore stopped and
//!   nothing will change any more.
//! - **Consistency.** [`Consistency::Linearizable`] reflects every write
//!   acknowledged before the call began. [`Consistency::Local`] returns one
//!   consistent state that may be stale; successive `Local` reads through one
//!   handle never go backwards. A backend may serve `Local` as
//!   `Linearizable`.
//! - **Retries live inside the implementation.** A method returns
//!   [`MetaError::NotLeader`], [`MetaError::Timeout`] or
//!   [`MetaError::Unavailable`] only after the implementation's own retry
//!   budget is spent; a write's outcome is then unknown. Every write is
//!   retry-safe, and a retried write may report its first attempt's effect;
//!   each method says how.
//!
//! Rejections arrive as [`MetaError::Rejected`] carrying an [`ApplyError`]; a
//! rejected write changes nothing. Each write lists the rejections it can
//! return besides [`ApplyError::InvalidArgument`] (a malformed name, key,
//! partition count, range or schema), which every write can return.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

use crate::meta::error::MetaResult;
#[cfg(doc)]
use crate::meta::error::{ApplyError, MetaError};
use crate::meta::types::{
    AliasAction, Collection, Fence, HotConfig, Lease, LeaseGrant, Link, LinkId, Namespace, Pointer,
    Retention, Stream, TargetRef, WalClass,
};
use crate::meta::views::{
    CollectionHead, CollectionRoots, LinkHead, PartitionIndex, PointerCas, SegmentSwap,
    StreamState, WalCommit,
};
use crate::schema::CollectionSchema;
use crate::{CollectionId, NamespaceId, StreamId};

/// How fresh a metastore read must be.
///
/// `Linearizable` reflects every write acknowledged before the read began.
/// `Local` returns one consistent state that may be stale; successive
/// `Local` reads through one handle never go backwards. A backend may serve
/// `Local` as `Linearizable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Reflects every write acknowledged before the call began. Served only by
    /// the leader, after it confirms its leadership with a quorum.
    Linearizable,
    /// Whatever this node has applied so far; may be stale on a follower or on
    /// a leader that has been cut off.
    Local,
}

/// A write whose callers must tell "rejected on the first attempt" from
/// "rejected after an attempt of unknown outcome".
#[derive(Debug)]
#[must_use]
pub struct Tracked<T> {
    /// The final attempt's result.
    pub result: MetaResult<T>,
    /// Whether an earlier attempt within the call ended with an unknown
    /// outcome (not the leader, a timeout, or unavailable). When it did, a
    /// rejection in `result` does not prove the write was never applied.
    pub earlier_unknown: bool,
}

impl<T> Tracked<T> {
    /// The result, for callers that do not care about earlier attempts.
    pub fn into_result(self) -> MetaResult<T> {
        self.result
    }
}

/// The metastore stopped: no change will ever be applied again.
#[derive(Debug, thiserror::Error)]
#[error("the metastore stopped")]
pub struct MetaStopped;

/// What a [`MetaChanges`] waits on; each backend supplies its own.
#[async_trait]
pub trait ChangeWait: Send + Sync + fmt::Debug {
    /// Completes once a state change is applied after the previous call
    /// returned (for the first call, after the wait was created). May
    /// complete spuriously.
    async fn changed(&mut self) -> Result<(), MetaStopped>;
}

/// A watch of metastore changes, from [`MetaStore::watch_changes`].
///
/// Armed when created: a change applied after creation completes `changed()`.
/// Spurious wake-ups are allowed.
#[derive(Debug)]
pub struct MetaChanges(Box<dyn ChangeWait>);

impl MetaChanges {
    /// Wraps a backend's wait.
    pub fn new(inner: impl ChangeWait + 'static) -> Self {
        Self(Box::new(inner))
    }

    /// Waits for a change applied since this watch was created or since the
    /// previous call returned; `Err(MetaStopped)` once the metastore stopped.
    pub async fn changed(&mut self) -> Result<(), MetaStopped> {
        self.0.changed().await
    }
}

/// The metastore, as every Operon crate outside the composition roots sees it
/// (D47). Every method follows the contract stated in this module's docs
/// (`operon-common/src/meta/store.rs`): semantic operations only, one
/// consistent state per call, unknown outcomes surfaced through [`Tracked`],
/// changes watched through [`MetaChanges`], [`Consistency`] as documented, and
/// retries inside the implementation.
///
/// Methods are grouped by domain: clock, changes and readiness; the catalog
/// (namespaces, streams, links); the sequencer and offset index; leases and
/// fencing; manifest pointers; collections; garbage collection. Lists are
/// ordered by id unless a method says otherwise.
#[async_trait]
pub trait MetaStore: Send + Sync + fmt::Debug + 'static {
    // ----- Clock, changes, readiness -----

    /// The proposer's clock, in ms since the epoch: stamps commands and
    /// freshness deadlines.
    fn now_ms(&self) -> u64;

    /// A watch armed now: any change applied after this returns completes its
    /// [`MetaChanges::changed`]. Subscribe before reading, so a change between
    /// the read and the wait still wakes the waiter.
    fn watch_changes(&self) -> MetaChanges;

    /// Whether the metastore can serve writes now (the readiness probe).
    fn is_ready(&self) -> bool;

    /// The metastore clock: the latest time stamp of any applied write. It
    /// never goes backwards.
    async fn clock_ms(&self, consistency: Consistency) -> MetaResult<u64>;

    // ----- Catalog: namespaces, streams, links -----

    /// Creates a namespace; names are unique. Rejected with
    /// [`ApplyError::NamespaceExists`] (carrying the existing id) when the
    /// name is taken, which is also what a retry after a lost acknowledgement
    /// reports, with the id the first attempt created.
    async fn create_namespace(&self, name: &str) -> MetaResult<NamespaceId>;

    /// The namespace named `name`.
    async fn namespace_by_name(
        &self,
        consistency: Consistency,
        name: &str,
    ) -> MetaResult<Option<Namespace>>;

    /// Every namespace, by id.
    async fn namespaces(&self, consistency: Consistency) -> MetaResult<Vec<Namespace>>;

    /// Creates a stream of `partitions` partitions
    /// (1..=[`MAX_PARTITIONS`](crate::meta::MAX_PARTITIONS)) in `namespace`,
    /// with its retention policy. Names are unique within the namespace and
    /// never start with `_`. Rejected with [`ApplyError::NamespaceNotFound`],
    /// or [`ApplyError::StreamExists`] (carrying the existing id) when the
    /// name is taken, which is also what a retry after a lost acknowledgement
    /// reports, with the id the first attempt created.
    async fn create_stream(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> MetaResult<StreamId>;

    /// Sets a stream's retention policy. Rejected with
    /// [`ApplyError::StreamNotFound`], or [`ApplyError::InvalidArgument`] for a
    /// collection's implicit stream (only its collection trims it). Setting
    /// the same policy again is a no-op, so a retry is safe.
    async fn set_retention(&self, stream: StreamId, retention: Retention) -> MetaResult<()>;

    /// The stream `id`.
    async fn stream(&self, consistency: Consistency, id: StreamId) -> MetaResult<Option<Stream>>;

    /// The stream named `name` in `namespace`.
    async fn stream_by_name(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Stream>>;

    /// The streams of `namespace` (`None`: of every namespace), by id.
    async fn streams(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Stream>>;

    /// The stream `id` with the bounds of each of its partitions, from one
    /// state.
    async fn stream_state(
        &self,
        consistency: Consistency,
        id: StreamId,
    ) -> MetaResult<Option<StreamState>>;

    /// Declares a link from stream `source` (in `namespace`) into `target`.
    /// Names are unique within the namespace and never start with `_`.
    /// Rejected with [`ApplyError::NamespaceNotFound`],
    /// [`ApplyError::StreamNotFound`], or [`ApplyError::LinkExists`] (carrying
    /// the existing id) when the name is taken, which is also what a retry
    /// after a lost acknowledgement reports, with the id the first attempt
    /// created.
    async fn create_link(
        &self,
        namespace: NamespaceId,
        name: &str,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> MetaResult<LinkId>;

    /// The link named `name` in `namespace`.
    async fn link_by_name(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Link>>;

    /// The links of `namespace` (`None`: of every namespace), by id.
    async fn links(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Link>>;

    /// The links of `namespace`, by id, each with its `link/<id>` pointer,
    /// from one state.
    async fn links_with_pointers(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<LinkHead>>;

    // ----- Sequencer and offset index -----

    /// Assigns offsets to every chunk of a durable WAL object and appends the
    /// chunks to their partitions' offset indexes, atomically; returns each
    /// chunk's base offset, in the order the chunks were given.
    ///
    /// Committing the same object again returns the first commit's offsets
    /// and changes nothing. Rejected with [`ApplyError::StreamNotFound`],
    /// [`ApplyError::PartitionNotFound`], or [`ApplyError::StaleCommit`] when
    /// the object has no commit record and `created_at_ms` is more than
    /// [`WAL_COMMIT_WINDOW_MS`](crate::meta::WAL_COMMIT_WINDOW_MS) behind the
    /// metastore clock. A retry therefore either returns the first commit's
    /// offsets or is rejected; it never commits the object twice. A
    /// `StaleCommit` with `earlier_unknown` does not mean the first attempt
    /// failed: its commit record may have been pruned since. A
    /// `created_at_ms` too far ahead of the metastore's own clock is refused
    /// with [`MetaError::ClockSkew`].
    async fn commit_wal(&self, commit: WalCommit) -> Tracked<Vec<u64>>;

    /// Replaces a contiguous run of WAL index entries of one partition with one
    /// segment entry covering the same offsets, stamped with
    /// [`MetaStore::now_ms`].
    ///
    /// A retry after a lost acknowledgement finds the segment entry in place
    /// and succeeds without changing anything. Otherwise rejected with
    /// [`ApplyError::StreamNotFound`], [`ApplyError::PartitionNotFound`],
    /// [`ApplyError::Fenced`] when the fence is broken,
    /// [`ApplyError::IndexMismatch`] when a replaced entry is no longer a WAL
    /// entry of the named object (a concurrent swap or trim moved it), or
    /// [`ApplyError::StaleObject`] once the segment is no longer `fresh`
    /// (garbage collection may already have deleted it).
    async fn swap_segment(&self, swap: SegmentSwap) -> Tracked<()>;

    /// Makes offsets below `before_offset` (capped at the high watermark)
    /// unreadable and drops the index entries wholly below it; returns the new
    /// log start. Stamped with [`MetaStore::now_ms`].
    ///
    /// Rejected with [`ApplyError::StreamNotFound`],
    /// [`ApplyError::PartitionNotFound`], or [`ApplyError::Fenced`] when the
    /// fence is broken (nothing changes). The log start only moves forward, so
    /// a retry returns the same log start; a retry whose fence was broken
    /// after the first attempt applied is rejected, but that trim stays.
    async fn trim_partition(
        &self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
    ) -> MetaResult<u64>;

    /// Bounds plus the entries of `entries_from(from_offset)`: the partition's
    /// index entries starting with the one holding `from_offset` (or the first
    /// after it; offsets below the log start count as the log start),
    /// stopping once at least one entry is returned and their byte ranges sum
    /// to >= `max_bytes` (`None`: to the end). `None` if the stream or
    /// partition is unknown.
    async fn partition_index(
        &self,
        consistency: Consistency,
        stream: StreamId,
        partition: u32,
        from_offset: u64,
        max_bytes: Option<u64>,
    ) -> MetaResult<Option<PartitionIndex>>;

    // ----- Leases and fencing -----

    /// Takes a free or expired lease for `ttl`
    /// (at most [`MAX_LEASE_TTL_MS`](crate::meta::MAX_LEASE_TTL_MS)), bumping
    /// its epoch; rejected with [`ApplyError::LeaseHeld`] while another owner
    /// holds it. If `owner` already holds it, extends it and keeps the epoch,
    /// so a retry after a lost acknowledgement gets the same epoch back; if it
    /// expired between the attempts, the retry takes it again at the next
    /// epoch. Owners must be unique per process incarnation.
    async fn acquire_lease(&self, key: &str, owner: &str, ttl: Duration) -> MetaResult<LeaseGrant>;

    /// Extends a held, unexpired lease by `ttl` from now. Rejected with
    /// [`ApplyError::LeaseLost`] when `owner` no longer holds it at `epoch` or
    /// it expired; a retry after a lost acknowledgement extends it again, or
    /// fails the same way if it expired in between.
    async fn renew_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant>;

    /// Re-takes a lease `owner` still holds at `epoch` (not released and not
    /// taken over), expired or not: its deadline becomes now + `ttl` and the
    /// epoch stays, so fences at `epoch` stay valid. Rejected with
    /// [`ApplyError::LeaseLost`] otherwise. A retry extends the deadline
    /// again, so it is safe.
    async fn reacquire_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant>;

    /// Releases a lease. Rejected with [`ApplyError::LeaseLost`] when `owner`
    /// does not hold it at `epoch`; releasing an already-released lease at the
    /// same epoch succeeds, so a retry is safe.
    async fn release_lease(&self, key: &str, owner: &str, epoch: u64) -> MetaResult<()>;

    /// The lease on `key`, if it was ever taken.
    async fn lease(&self, consistency: Consistency, key: &str) -> MetaResult<Option<Lease>>;

    /// Every lease whose key starts with `prefix`, in key order, released
    /// and expired ones included (callers check [`Lease::is_held_at`]), from
    /// one state (M1.3 Ruling 12).
    async fn leases_with_prefix(
        &self,
        consistency: Consistency,
        prefix: &str,
    ) -> MetaResult<Vec<(String, Lease)>>;

    // ----- Manifest pointers -----

    /// Sets a pointer if its current version is `expected` (`None`: it must
    /// not exist yet) and the fence, if given, holds; returns the new version
    /// (`expected + 1`, or 1 for a new pointer).
    ///
    /// Rejected with [`ApplyError::NamespaceNotFound`],
    /// [`ApplyError::CollectionNotFound`] for a `collection/<id>` key whose
    /// collection is not in the namespace, [`ApplyError::Fenced`],
    /// [`ApplyError::StaleObject`] once `fresh` has expired, or
    /// [`ApplyError::VersionMismatch`] carrying the current pointer. A retry
    /// after a lost acknowledgement reports `VersionMismatch`: a current
    /// pointer holding the caller's value at `expected + 1` means the first
    /// attempt *may* have succeeded (another writer may have written the same
    /// value), so callers that must know write a unique value.
    async fn cas_pointer(&self, cas: PointerCas) -> Tracked<u64>;

    /// The pointer `key` in `namespace`, if it has been set.
    async fn pointer(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        key: &str,
    ) -> MetaResult<Option<Pointer>>;

    // ----- Collections -----

    /// Creates a collection with its implicit stream and link (both named
    /// [`implicit_name`](crate::meta::implicit_name), class `Standard`,
    /// default retention), in one step; returns their ids. Names are unique
    /// within the namespace across collections and aliases and never start
    /// with `_`; `schema` must be valid and at version 1.
    ///
    /// Rejected with [`ApplyError::NamespaceNotFound`],
    /// [`ApplyError::CollectionExists`] (carrying the existing id) when the
    /// collection existed before the call, or [`ApplyError::NameTaken`] when
    /// the name is held by a collection with another schema or partition
    /// count, or by an alias. A retry after a lost acknowledgement that finds
    /// the collection returns the first attempt's ids.
    async fn create_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
        schema: CollectionSchema,
        partitions: u32,
    ) -> MetaResult<(CollectionId, StreamId, LinkId)>;

    /// Drops the collection `name` (not an alias): removes it, its aliases,
    /// its implicit stream and link and its manifest pointer, and retires its
    /// object and primary-key prefixes. Stamped with [`MetaStore::now_ms`].
    /// Returns its id, or `None` when there is no such collection, which is
    /// also what a retry after a lost acknowledgement returns.
    async fn drop_collection(
        &self,
        namespace: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<CollectionId>>;

    /// Replaces a collection's schema at `expected_version` with an additive
    /// extension; returns the new version (`expected_version + 1`). Rejected
    /// with [`ApplyError::CollectionNotFound`],
    /// [`ApplyError::SchemaVersionMismatch`] for a stale `expected_version`,
    /// or [`ApplyError::IncompatibleSchema`]. A retry after a lost
    /// acknowledgement finds the same schema at `expected_version + 1` and
    /// succeeds.
    async fn update_collection_schema(
        &self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> MetaResult<u64>;

    /// Applies 1..=100 alias actions in order, atomically: if any fails,
    /// nothing changes. Rejected with [`ApplyError::NamespaceNotFound`],
    /// [`ApplyError::NameTaken`] for an alias named like a collection, or
    /// [`ApplyError::UnknownCollection`]. Creating an existing alias re-points
    /// it and deleting a missing one is a no-op, so a retry succeeds.
    async fn update_aliases(
        &self,
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> MetaResult<()>;

    /// The collection `id`, whatever its namespace: callers that must match a
    /// namespace check it themselves.
    async fn collection(
        &self,
        consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<Collection>>;

    /// The collection named `name_or_alias` in `namespace`, directly or
    /// through an alias.
    async fn resolve_collection(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        name_or_alias: &str,
    ) -> MetaResult<Option<Collection>>;

    /// The collection whose implicit link is `link`.
    async fn collection_for_link(
        &self,
        consistency: Consistency,
        link: LinkId,
    ) -> MetaResult<Option<Collection>>;

    /// The collections of `namespace` (`None`: of every namespace), by id.
    async fn collections(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<Collection>>;

    /// The aliases of `namespace` with the collections they point at, by alias
    /// name.
    async fn aliases(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
    ) -> MetaResult<Vec<(String, CollectionId)>>;

    /// The collection `id` (whatever its namespace) with its manifest pointer,
    /// its implicit stream's bounds and the metastore clock, from one state.
    async fn collection_head(
        &self,
        consistency: Consistency,
        id: CollectionId,
    ) -> MetaResult<Option<CollectionHead>>;

    /// [`MetaStore::collection_head`] of every collection of `namespace`
    /// (`None`: of every namespace), by id, from one state.
    async fn collection_heads(
        &self,
        consistency: Consistency,
        namespace: Option<NamespaceId>,
    ) -> MetaResult<Vec<CollectionHead>>;

    /// Sets the hot configuration of collection `collection` of `namespace`
    /// (M1.3 Task 4; E63); an all-false `hot` clears it. Rejected with
    /// [`ApplyError::CollectionNotFound`] when there is no such collection in
    /// `namespace`. Setting the same value again succeeds, so a retry is
    /// safe.
    async fn set_collection_hot(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
        hot: HotConfig,
    ) -> MetaResult<()>;

    /// The hot configuration of collection `collection` of `namespace`: the
    /// default (all false) when none is set. Rejected with
    /// [`ApplyError::CollectionNotFound`] when there is no such collection in
    /// `namespace`.
    async fn collection_hot(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        collection: CollectionId,
    ) -> MetaResult<HotConfig>;

    // ----- Garbage collection -----
    //
    // Always `Linearizable`; each result is computed against the metastore
    // clock of the same state as the references it checks.

    /// Retired paths whose retirement time + `grace_ms` <= the metastore
    /// clock, in path order.
    async fn retired_expired(&self, grace_ms: u64) -> MetaResult<Vec<String>>;

    /// Removes collected objects from the retired set; returns how many were
    /// there. Unknown paths are ignored, so a retry is safe. Rejected with
    /// [`ApplyError::Fenced`] when the fence is broken.
    async fn forget_objects(&self, objects: Vec<String>, fence: Option<Fence>) -> MetaResult<u32>;

    /// Forgets WAL commit records older than twice
    /// [`WAL_COMMIT_WINDOW_MS`](crate::meta::WAL_COMMIT_WINDOW_MS); returns
    /// how many were removed. Stamped with [`MetaStore::now_ms`]. A retry
    /// removes nothing more. Rejected with [`ApplyError::Fenced`] when the
    /// fence is broken.
    async fn prune_wal_commits(&self, fence: Option<Fence>) -> MetaResult<u32>;

    /// Of `candidates` (path, created_at_ms), in order, at most `limit` that
    /// are at least `min_age_ms` old by the metastore clock, have no live WAL
    /// chunk, and are not retired.
    async fn orphan_wal_objects(
        &self,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>>;

    /// Of `candidates` (path, created_at_ms), in order, at most `limit` that
    /// are at least `min_age_ms` old by the metastore clock, not retired, and
    /// not named by any index entry of any stream of `namespace`.
    async fn orphan_segments(
        &self,
        namespace: NamespaceId,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>>;

    /// Whether `object` is retired or named by an index entry of the
    /// partition.
    async fn segment_referenced(
        &self,
        stream: StreamId,
        partition: u32,
        object: &str,
    ) -> MetaResult<bool>;

    /// The metastore clock, the collections of `namespace` (by id) with their
    /// manifest pointers, and the retired prefixes (paths ending in `/`) that
    /// start with `under`, from one state.
    async fn collection_roots(
        &self,
        namespace: NamespaceId,
        under: &str,
    ) -> MetaResult<CollectionRoots>;
}
