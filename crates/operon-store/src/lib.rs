//! Object storage access for Operon.
//!
//! [`Store`] wraps an [`object_store::ObjectStore`] and exposes the small set of
//! operations Operon relies on: create-only writes, compare-and-swap writes,
//! whole-object and range reads, idempotent deletes, and listing.
//! [`FaultyStore`] injects failures for crash and fault testing.

mod error;
mod fault;
mod store;

pub use error::StoreError;
pub use fault::{Fault, FaultRates, FaultyStore, Op};
pub use store::{ObjectInfo, ObjectVersion, Store};
