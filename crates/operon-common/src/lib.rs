//! Identifier and schema types shared by Operon crates.
//!
//! The metastore allocates ids from per-kind counters, so ids are dense, never
//! reused, and render as plain decimal numbers in object paths (for example
//! `ns/42/streams/7/0/...`, design §01 §6).
//!
//! [`schema`] holds the collection schema types: the metastore's commands
//! carry them, and `operon-collection` re-exports them.

mod id;
pub mod schema;

pub use id::{CollectionId, NamespaceId, ParseIdError, StreamId};
