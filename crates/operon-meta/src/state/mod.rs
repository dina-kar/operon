mod catalog;
mod leases;
mod pointers;
mod sequencer;

use std::collections::BTreeMap;

use operon_common::{NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use crate::command::{ApplyError, Command, Reply};
use crate::types::{Lease, Namespace, PartitionState, Pointer, Stream};

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
    /// Base offsets assigned to each committed WAL object, for idempotent retries.
    /// Entries are removed when the segmenter retires the object (M0.3).
    wal_commits: BTreeMap<String, Vec<u64>>,
    leases: BTreeMap<String, Lease>,
    pointers: BTreeMap<(NamespaceId, String), Pointer>,
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
            } => self.create_stream(namespace, name, partitions, class),
            Command::CommitWal { object, chunks } => self.commit_wal(object, chunks),
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
            Command::ReleaseLease { key, owner, epoch } => self.release_lease(key, owner, epoch),
            Command::CasPointer {
                namespace,
                key,
                expected,
                value,
                fence,
            } => self.cas_pointer(namespace, key, expected, value, fence),
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
