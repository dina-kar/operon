//! Object storage access for Operon.
//!
//! [`Store`] wraps an [`object_store::ObjectStore`] and exposes the small set of
//! operations Operon relies on: create-only writes, compare-and-swap writes,
//! whole-object and range reads, idempotent deletes, and listing.

mod error;
mod store;

pub use error::StoreError;
pub use store::{ObjectInfo, ObjectVersion, Store};
