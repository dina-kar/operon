//! The Operon metastore.
//!
//! [`MetaState`] is the deterministic state machine: namespaces, streams, the
//! stream sequencer and offset index, leases, and manifest pointers. Every change
//! is a [`Command`] applied in Raft log order, so every replica computes the same
//! state (design §01 §3.2, §02 §3).

mod codec;
mod command;
mod db;
mod log_store;
mod raft;
mod state;
mod state_machine;
mod types;

pub use command::{ApplyError, Command, Reply};
pub use db::LocalDb;
pub use log_store::LogStore;
pub use raft::{EntryReply, NodeId, SnapshotData, TypeConfig};
pub use state::{MAX_KEY_LEN, MAX_LEASE_TTL_MS, MAX_NAME_LEN, MAX_PARTITIONS, MetaState};
pub use state_machine::StateMachineStore;
pub use types::{
    Fence, IndexEntry, Lease, LeaseGrant, Namespace, PartitionState, Pointer, Stream, WalChunk,
    WalClass,
};
