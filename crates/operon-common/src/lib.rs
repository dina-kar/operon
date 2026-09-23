//! Identifier types shared by Operon crates.
//!
//! The metastore allocates ids from per-kind counters, so ids are dense, never
//! reused, and render as plain decimal numbers in object paths (for example
//! `ns/42/streams/7/0/...`, design §01 §6).

mod id;

pub use id::{NamespaceId, ParseIdError, StreamId};
