//! The cases, one `pub async fn <case>(backend: &dyn Backend)` each, grouped
//! by domain as the trait is (Ruling 1). Every case starts its own fresh
//! metastore, so names and keys never collide across cases; they are still
//! unique within the suite, so a failure names its case. Checks read
//! `Linearizable`, so they hold through any handle of a replicated backend.

mod catalog;
mod changes;
mod collections;
mod faults;
mod gc;
mod hot;
mod leases;
mod linearizable;
mod pointers;
mod sequencer;

use std::fmt::Debug;
use std::ops::Range;

use operon_common::meta::{
    ApplyError, Consistency, MetaError, MetaResult, MetaStore, PointerCas, Retention, WalChunk,
    WalClass, WalCommit,
};
use operon_common::schema::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec};
use operon_common::{NamespaceId, StreamId};

pub use catalog::*;
pub use changes::*;
pub use collections::*;
pub use faults::*;
pub use gc::*;
pub use hot::*;
pub use leases::*;
pub use linearizable::*;
pub use pointers::*;
pub use sequencer::*;

/// Every case name, in suite order.
pub const CASES: &[&str] = crate::for_each_case!([crate::__case_names]);

/// The rejection in `result`; panics on success or on any other error.
#[track_caller]
fn rejected<T: Debug>(result: MetaResult<T>) -> ApplyError {
    match result {
        Err(MetaError::Rejected(err)) => err,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// Prints the skip line of a case that needs fault injection.
fn skip_without_faults(case: &str) {
    println!("skipped: {case} needs fault injection");
}

async fn namespace(meta: &dyn MetaStore, name: &str) -> NamespaceId {
    meta.create_namespace(name).await.expect("create namespace")
}

async fn stream(meta: &dyn MetaStore, ns: NamespaceId, name: &str, partitions: u32) -> StreamId {
    meta.create_stream(
        ns,
        name,
        partitions,
        WalClass::Standard,
        Retention::default(),
    )
    .await
    .expect("create stream")
}

fn chunk(stream: StreamId, partition: u32, records: u32, bytes: Range<u64>) -> WalChunk {
    WalChunk {
        stream,
        partition,
        records,
        byte_range: bytes,
        max_timestamp_ms: 0,
    }
}

/// Commits `object` created now; returns its base offsets.
async fn commit(meta: &dyn MetaStore, object: &str, chunks: Vec<WalChunk>) -> Vec<u64> {
    meta.commit_wal(WalCommit {
        object: object.to_string(),
        created_at_ms: meta.now_ms(),
        chunks,
    })
    .await
    .into_result()
    .expect("commit WAL")
}

/// Moves the metastore clock up to the proposer's clock, with a stamped write
/// that changes nothing else.
async fn stamp(meta: &dyn MetaStore) {
    meta.prune_wal_commits(None)
        .await
        .expect("prune WAL commits");
}

/// The high watermark of a partition, `Linearizable`.
async fn high_watermark(meta: &dyn MetaStore, stream: StreamId, partition: u32) -> u64 {
    meta.partition_index(
        Consistency::Linearizable,
        stream,
        partition,
        u64::MAX,
        Some(0),
    )
    .await
    .expect("partition index")
    .expect("the partition exists")
    .high_watermark()
}

/// Every retired path whose retirement is at least as old as the metastore
/// clock (with no grace: every retired path).
async fn retired(meta: &dyn MetaStore) -> Vec<String> {
    meta.retired_expired(0).await.expect("retired")
}

fn cas(ns: NamespaceId, key: &str, expected: Option<u64>, value: &str) -> PointerCas {
    PointerCas {
        namespace: ns,
        key: key.to_string(),
        expected,
        value: value.to_string(),
        fence: None,
        fresh: None,
    }
}

fn keyword(name: &str) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        kind: FieldKind::Keyword,
        indexed: true,
        fast: false,
        ignore_malformed: false,
    }
}

/// A version 1 schema with one keyword field.
fn schema() -> CollectionSchema {
    CollectionSchema::new(vec![keyword("title")], Vec::new(), DynamicMapping::Ignore)
}
