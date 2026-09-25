//! Metastore types shared by every backend and every caller: catalog
//! records, coordination primitives (leases, fences, pointers), the errors a
//! metastore operation can return, and the consistency levels a read may ask
//! for (M1.2a plan).
//!
//! These are moved verbatim from `operon-meta`, which keeps everything
//! openraft-specific (`Command`, `Reply`, `MetaState`, the codec, `MetaNode`,
//! ...) and re-exports every name here at its crate root, so
//! `operon_meta::X` keeps compiling for existing callers.

mod error;
mod store;
mod types;

pub use error::{ApplyError, MetaError, MetaResult, StaleLag, log_stale_object};
pub use store::Consistency;
pub use types::{
    AliasAction, COLLECTION_KIND, COLLECTION_POINTER_PREFIX, Collection, EntryKind, Fence,
    Freshness, IndexEntry, Lease, LeaseGrant, Link, LinkId, MAX_COLLECTION_NAME_LEN, MAX_KEY_LEN,
    MAX_LEASE_TTL_MS, MAX_NAME_LEN, MAX_PARTITIONS, Namespace, Pointer, Retention, Stream,
    TargetRef, WAL_COMMIT_WINDOW_MS, WalChunk, WalClass, collection_pk_prefix,
    collection_pointer_key, collection_prefix, implicit_name, link_pointer_key,
};
