//! The Operon metastore.
//!
//! [`MetaState`] is the deterministic state machine: namespaces, streams, the
//! stream sequencer and offset index, leases, and manifest pointers. Every change
//! is a [`Command`] applied in Raft log order, so every replica computes the same
//! state (design §01 §3.2, §02 §3).

mod command;
mod state;
mod types;

pub use command::{ApplyError, Command, Reply};
pub use state::{MAX_KEY_LEN, MAX_NAME_LEN, MAX_PARTITIONS, MetaState};
pub use types::{IndexEntry, Namespace, PartitionState, Stream, WalChunk, WalClass};
