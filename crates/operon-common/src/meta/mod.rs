//! The metastore as every backend and every caller shares it: the semantic
//! [`MetaStore`] trait (D47) with its read models and requests, catalog
//! records, coordination primitives (leases, fences, pointers), the errors a
//! metastore operation can return, and the consistency levels a read may ask
//! for (M1.2a plan).
//!
//! The records and errors are moved verbatim from `operon-meta`, which keeps everything
//! openraft-specific (`Command`, `Reply`, `MetaState`, the codec, `MetaNode`,
//! ...) and re-exports every name here at its crate root, so
//! `operon_meta::X` keeps compiling for existing callers.

mod error;
mod store;
mod types;
mod views;

pub use error::{ApplyError, MetaError, MetaResult, StaleLag, log_stale_object};
pub use store::{ChangeWait, Consistency, MetaChanges, MetaStopped, MetaStore, Tracked};
pub use types::{
    AliasAction, COLLECTION_KIND, COLLECTION_POINTER_PREFIX, Collection, EntryKind, Fence,
    Freshness, HotConfig, IndexEntry, Lease, LeaseGrant, Link, LinkId, MAX_COLLECTION_NAME_LEN,
    MAX_KEY_LEN, MAX_LEASE_TTL_MS, MAX_NAME_LEN, MAX_PARTITIONS, Namespace, Pointer, Retention,
    Stream, TargetRef, WAL_COMMIT_WINDOW_MS, WalChunk, WalClass, collection_pk_prefix,
    collection_pointer_key, collection_prefix, implicit_name, link_pointer_key,
};
pub use views::{
    CollectionHead, CollectionRoots, LinkHead, PartitionBounds, PartitionIndex, PointerCas,
    SegmentSwap, StreamState, WalCommit,
};
