//! The Operon metastore.
//!
//! [`MetaState`] is the deterministic state machine: namespaces, streams, the
//! stream sequencer and offset index, leases, manifest pointers, links, and
//! the collection catalog. Every change is a [`Command`] applied in Raft log
//! order, so every replica computes the same state (design §01 §3.2, §02 §3).

mod client;
mod clock;
mod codec;
mod command;
mod db;
mod error;
mod log_store;
mod network;
mod node;
mod raft;
mod state;
mod state_machine;
mod types;

pub use client::{MetaClient, MetaClientConfig};
pub use clock::{Clock, ManualClock, SystemClock};
#[cfg(feature = "test-util")]
pub use codec::snapshot_round_trip;
pub use command::{ApplyError, Command, Reply, StaleLag, log_stale_object};
pub use db::LocalDb;
pub use error::MetaError;
pub use log_store::LogStore;
pub use network::Router;
pub use node::{Consistency, MetaConfig, MetaNode, RaftStatus};
pub use raft::{EntryReply, NodeId, SnapshotData, TypeConfig};
pub use state::{MAX_KEY_LEN, MAX_LEASE_TTL_MS, MAX_NAME_LEN, MAX_PARTITIONS, MetaState};
pub use state_machine::StateMachineStore;
pub use types::{
    AliasAction, COLLECTION_KIND, Collection, EntryKind, Fence, Freshness, IndexEntry, Lease,
    LeaseGrant, Link, LinkId, MAX_COLLECTION_NAME_LEN, Namespace, PartitionState, Pointer,
    Retention, Stream, TargetRef, WAL_COMMIT_WINDOW_MS, WalChunk, WalClass, WalCommitRecord,
    collection_pk_prefix, collection_pointer_key, collection_prefix, implicit_name,
};

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
