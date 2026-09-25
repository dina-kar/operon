mod catalog;
mod collections;
mod invariants;
mod leases;
mod links;
mod pointers;
mod retention;
mod segments;
mod sequencer;

use std::collections::BTreeMap;

use operon_common::{CollectionId, NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use crate::command::{ApplyError, Command, Reply};
use crate::types::{
    Collection, Lease, Link, LinkId, Namespace, PartitionState, Pointer, Stream, WalCommitRecord,
};

/// Longest namespace or stream name, in bytes.
pub const MAX_NAME_LEN: usize = 255;
/// Most partitions a stream may have.
pub const MAX_PARTITIONS: u32 = 10_000;
/// Longest object path, lease key or pointer key, in bytes.
pub const MAX_KEY_LEN: usize = 1024;
/// Longest lease a holder may take or renew for: one hour.
pub const MAX_LEASE_TTL_MS: u64 = 3_600_000;

/// The metastore state machine.
///
/// `apply` must be deterministic: it reads nothing but the state and the
/// command (time arrives inside commands), and it keeps everything in ordered
/// maps, so replicas that apply the same log hold identical state and encode
/// identical snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaState {
    /// The latest `now_ms` of any applied command. Time never goes backwards,
    /// even when a new leader's clock is behind the old one's.
    clock_ms: u64,
    last_namespace_id: u64,
    last_stream_id: u64,
    namespaces: BTreeMap<NamespaceId, Namespace>,
    namespace_names: BTreeMap<String, NamespaceId>,
    streams: BTreeMap<StreamId, Stream>,
    stream_names: BTreeMap<(NamespaceId, String), StreamId>,
    partitions: BTreeMap<(StreamId, u32), PartitionState>,
    /// Base offsets assigned to each committed WAL object, for idempotent
    /// retries. Pruned by `PruneWalCommits` once older than twice the commit
    /// window.
    wal_commits: BTreeMap<String, WalCommitRecord>,
    /// Per WAL object, how many of its chunks are still `Wal` index entries.
    /// An object leaves this map (for `retired`) when the count reaches zero.
    wal_live_chunks: BTreeMap<String, u32>,
    /// Objects no index entry references any more (WAL objects whose chunks
    /// were all segmented or trimmed, and trimmed segments), with the
    /// metastore clock when they were retired. Garbage collection deletes
    /// them after a grace period and then forgets them.
    retired: BTreeMap<String, u64>,
    leases: BTreeMap<String, Lease>,
    pointers: BTreeMap<(NamespaceId, String), Pointer>,
    last_link_id: u64,
    links: BTreeMap<LinkId, Link>,
    link_names: BTreeMap<(NamespaceId, String), LinkId>,
    last_collection_id: u64,
    collections: BTreeMap<CollectionId, Collection>,
    collection_names: BTreeMap<(NamespaceId, String), CollectionId>,
    /// Alias name → the collection it points at. An alias never has the
    /// name of a collection of its namespace.
    aliases: BTreeMap<(NamespaceId, String), CollectionId>,
}

impl MetaState {
    /// Applies one command. On error the state is unchanged.
    pub fn apply(&mut self, command: Command) -> Result<Reply, ApplyError> {
        match command {
            Command::CreateNamespace { name } => self.create_namespace(name),
            Command::CreateStream {
                namespace,
                name,
                partitions,
                class,
                retention,
            } => self.create_stream(namespace, name, partitions, class, retention),
            Command::CreateLink {
                namespace,
                name,
                source,
                target,
                options,
            } => self.create_link(namespace, name, source, target, options),
            Command::CommitWal {
                object,
                created_at_ms,
                chunks,
            } => self.commit_wal(object, created_at_ms, chunks),
            Command::SetRetention { stream, retention } => self.set_retention(stream, retention),
            Command::SwapSegment {
                stream,
                partition,
                replaces,
                segment,
                byte_range,
                max_timestamp_ms,
                fence,
                now_ms,
                fresh,
            } => self.swap_segment(
                stream,
                partition,
                replaces,
                segment,
                byte_range,
                max_timestamp_ms,
                fence,
                now_ms,
                fresh,
            ),
            Command::TrimPartition {
                stream,
                partition,
                before_offset,
                fence,
                now_ms,
            } => self.trim_partition(stream, partition, before_offset, fence, now_ms),
            Command::PruneWalCommits { fence, now_ms } => self.prune_wal_commits(fence, now_ms),
            Command::ForgetObjects { objects, fence } => self.forget_objects(objects, fence),
            Command::AcquireLease {
                key,
                owner,
                ttl_ms,
                now_ms,
            } => self.acquire_lease(key, owner, ttl_ms, now_ms),
            Command::RenewLease {
                key,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            } => self.renew_lease(key, owner, epoch, ttl_ms, now_ms),
            Command::ReacquireLease {
                key,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            } => self.reacquire_lease(key, owner, epoch, ttl_ms, now_ms),
            Command::ReleaseLease { key, owner, epoch } => self.release_lease(key, owner, epoch),
            Command::CasPointer {
                namespace,
                key,
                expected,
                value,
                fence,
                fresh,
            } => self.cas_pointer(namespace, key, expected, value, fence, fresh),
            Command::CreateCollection {
                namespace,
                name,
                schema,
                partitions,
            } => self.create_collection(namespace, name, schema, partitions),
            Command::DropCollection {
                namespace,
                name,
                now_ms,
            } => self.drop_collection(namespace, name, now_ms),
            Command::UpdateCollectionSchema {
                collection,
                expected_version,
                schema,
            } => self.update_collection_schema(collection, expected_version, schema),
            Command::UpdateAliases { namespace, actions } => {
                self.update_aliases(namespace, actions)
            }
        }
    }

    /// The metastore clock: the latest `now_ms` of any applied command.
    pub fn clock_ms(&self) -> u64 {
        self.clock_ms
    }
}

/// Names are 1..=255 bytes of ASCII letters, digits, `-`, `_` and `.`, and are
/// not `.` or `..`, so they are safe as object path segments.
fn validate_name(kind: &str, name: &str) -> Result<(), ApplyError> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(ApplyError::InvalidArgument(format!(
            "invalid {kind} name {name:?}"
        )))
    }
}

/// Names starting with `_` belong to implicit objects (a collection's stream
/// and link); users cannot create them.
fn refuse_reserved(name: &str) -> Result<(), ApplyError> {
    if name.starts_with('_') {
        return Err(ApplyError::InvalidArgument(
            "names starting with '_' are reserved for implicit objects".to_string(),
        ));
    }
    Ok(())
}

/// Keys (object paths, lease keys, pointer keys) are 1..=1024 bytes.
fn validate_key(kind: &str, key: &str) -> Result<(), ApplyError> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(ApplyError::InvalidArgument(format!(
            "{kind} must be 1..={MAX_KEY_LEN} bytes, got {}",
            key.len()
        )));
    }
    Ok(())
}
