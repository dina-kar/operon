//! Loam Live, the reactive document database on TiKV (design §20; R1 plan
//! Tasks 8–13).
//!
//! This crate holds the data model of one Live app in one keyspace (Task 8):
//! [`LiveValue`] and its protobuf and index encodings, [`DocId`] and its
//! checksummed text form, tables and indexes ([`TableDef`], [`IndexDef`] and
//! the catalog operations in [`catalog`]), the key layout of §20 §4.3
//! ([`AppKeys`]), and the document operations with index maintenance in
//! [`docs`], which run inside an `operon-tikv` transaction and return the
//! [`WriteRecord`]s the commit journal needs. [`Limits`] holds R1's document
//! and mutation limits. The sharded, sequenced commit journal (Task 9), its
//! [`Tailer`] and its [`Janitor`] are in [`journal`]. [`LiveTxn`], its
//! [`ReadSet`], the [`Function`] trait and the mutation and query
//! [`Runner`] (Task 10) are in [`txn`]; the built-in `_system:*` functions
//! are in [`system`], their argument shapes in [`query`].

pub mod catalog;
mod config;
pub mod docs;
mod error;
pub mod ids;
pub mod journal;
pub mod keys;
mod limits;
pub mod query;
pub mod system;
pub mod txn;
mod value;

/// The `loam.live.v1` protobuf messages.
pub use operon_live_proto::loam::live::v1 as pb;

pub use catalog::{IndexDef, IndexSpec, TableDef};
pub use config::{DEFAULT_JOURNAL_SHARDS, KEYSPACE_PREFIX, LiveConfig, keyspace_of};
pub use docs::{Doc, IndexRange, Order, Reads, WriteRecord};
pub use error::LiveError;
pub use ids::{DocId, IndexId, TableId};
pub use journal::{Batch, Checkpoint, Janitor, JanitorReport, Journal, Tailer};
pub use keys::{AppKeys, KeyRange};
pub use limits::Limits;
pub use txn::{FnKind, Function, LiveTxn, Mutated, Queried, ReadSet, Runner, RunnerOptions, Usage};
pub use value::{LiveValue, fields_from_proto, fields_to_proto};
