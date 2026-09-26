//! The Operon metastore.
//!
//! [`MetaState`] is the deterministic state machine: namespaces, streams, the
//! stream sequencer and offset index, leases, manifest pointers, links, and
//! the collection catalog. Every change is a [`Command`] applied in Raft log
//! order, so every replica computes the same state (design §01 §3.2, §02 §3).
//!
//! [`MetaClient`] implements [`operon_common::meta::MetaStore`], the semantic
//! trait every crate outside the composition roots uses (D47).

mod client;
mod clock;
mod codec;
mod command;
mod db;
mod log_store;
mod network;
mod node;
mod raft;
mod state;
mod state_machine;
mod store;
mod types;

pub use client::{MetaClient, MetaClientConfig};
pub use clock::{Clock, ManualClock, SystemClock};
#[cfg(feature = "test-util")]
pub use codec::{snapshot_bytes, snapshot_round_trip, state_from_snapshot_bytes};
pub use command::{Command, Reply};
pub use db::LocalDb;
pub use log_store::LogStore;
pub use network::Router;
pub use node::{MetaConfig, MetaNode, RaftStatus};
pub use operon_common::meta::{
    AliasAction, ApplyError, COLLECTION_KIND, Collection, Consistency, EntryKind, Fence, Freshness,
    HotConfig, IndexEntry, Lease, LeaseGrant, Link, LinkId, MAX_COLLECTION_NAME_LEN, MAX_KEY_LEN,
    MAX_LEASE_TTL_MS, MAX_NAME_LEN, MAX_PARTITIONS, MetaError, Namespace, Pointer, Retention,
    StaleLag, Stream, TargetRef, WAL_COMMIT_WINDOW_MS, WalChunk, WalClass, collection_pk_prefix,
    collection_pointer_key, collection_prefix, implicit_name, log_stale_object,
};
pub use raft::{EntryReply, NodeId, SnapshotData, TypeConfig};
pub use state::MetaState;
pub use state_machine::StateMachineStore;
pub use types::{PartitionState, WalCommitRecord};

/// Evaluates a named failpoint (M0.4 Task 5). With the `failpoints` feature
/// the `fail` crate may act on it (the crash gate aborts the process there);
/// without it, this expands to nothing.
macro_rules! failpoint {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name);
    };
}
pub(crate) use failpoint;
