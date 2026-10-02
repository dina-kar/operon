mod catalog;
mod collections;
mod hot;
mod idempotency;
mod invariants;
mod leases;
mod links;
mod pointers;
mod retention;
mod segments;
mod sequencer;

use std::collections::BTreeMap;

use loams_common::meta::{
    AliasTargets, ApplyError, Collection, HotConfig, IdempotencyEntry, IdempotencyKey, Lease, Link,
    LinkId, MAX_KEY_LEN, MAX_NAME_LEN, Namespace, Pointer, Stream,
};
use loams_common::{CollectionId, NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use crate::command::{Command, Reply};
use crate::types::{PartitionState, WalCommitRecord};

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
    /// Hot configuration per collection (M1.3 Ruling 7), non-default values
    /// only. Serde skips it, so the derived encoding stays M1.1's; a snapshot
    /// carries it only in format 6, written while it is non-empty (Ruling
    /// 20).
    #[serde(skip)]
    collection_hot: BTreeMap<CollectionId, HotConfig>,
    /// Every alias that is not exactly one unset member (M1.5 Ruling 22):
    /// several members, or one whose `is_write_index` is set. An alias is
    /// in exactly one of `aliases` and this map, and no entry is empty.
    /// Serde skips it, so the derived encoding stays M1.1's; a snapshot
    /// carries it only in format 7, written while it is non-empty.
    #[serde(skip)]
    alias_targets: BTreeMap<(NamespaceId, String), AliasTargets>,
    /// The stream ingest ledger (design §02 §7.4, D270): per stream and
    /// idempotency key, a pending claim or where the event was appended,
    /// until it lapses and is pruned. Serde skips it, so the derived
    /// encoding stays M1.1's; a snapshot carries it only in format 8,
    /// written while it is non-empty.
    #[serde(skip)]
    idempotency: BTreeMap<(StreamId, IdempotencyKey), IdempotencyEntry>,
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
            Command::SetCollectionHot { collection, hot } => {
                self.set_collection_hot(collection, hot)
            }
            Command::UpdateAliasTargets { namespace, actions } => {
                self.update_alias_targets(namespace, actions)
            }
            Command::ClaimIdempotencyKeys {
                stream,
                owner,
                keys,
                ttl_ms,
                now_ms,
            } => self.claim_idempotency_keys(stream, owner, keys, ttl_ms, now_ms),
            Command::CompleteIdempotencyKeys {
                stream,
                owner,
                done,
                window_ms,
                now_ms,
            } => self.complete_idempotency_keys(stream, owner, done, window_ms, now_ms),
            Command::ReleaseIdempotencyKeys {
                stream,
                owner,
                keys,
            } => self.release_idempotency_keys(stream, owner, keys),
            Command::PruneIdempotencyKeys { fence, now_ms } => {
                self.prune_idempotency_keys(fence, now_ms)
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
