//! Service types beside the search IR: projections, collection info, write
//! results, stored documents, manifests and pins (overview §6.7; plan M1.2
//! Task 1, Rulings 10, 12, 13 and 14).

use std::collections::BTreeMap;

use loams_collection::{CollectionSchema, ConsistencyToken, PrimaryKey};
use loams_common::{CollectionId, StreamId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use loams_common::meta::{AliasAction, AliasTargetAction};

use crate::backlog::{Backlog, BackpressureStatus, Override};
use crate::error::ServiceError;
use crate::hot::HotStatus;
use crate::ir::{FieldValue, ReadConsistency, SparseVector};

fn is_empty_map<K, V>(map: &BTreeMap<K, V>) -> bool {
    map.is_empty()
}

/// What a read returns of each document.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    /// Default `All`.
    #[serde(default)]
    pub source: SourceFilter,
    /// The dense and sparse vectors to return, by name.
    #[serde(default)]
    pub vectors: Vec<String>,
    /// The typed fields to return (`Hit.fields`), by name.
    #[serde(default)]
    pub fields: Vec<String>,
}

/// Which part of `_source` a read returns (Ruling 12). JSON: `"all"`,
/// `"none"` or `{"include": [..], "exclude": [..]}` (serde in
/// [`crate::json::values`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SourceFilter {
    #[default]
    All,
    None,
    /// ES `_source` include and exclude patterns.
    Paths {
        include: Vec<String>,
        exclude: Vec<String>,
    },
}

/// `GET …/collections/{c}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub id: CollectionId,
    pub name: String,
    pub namespace: String,
    #[serde(with = "crate::json::schema")]
    pub schema: CollectionSchema,
    pub partitions: u32,
    /// Sorted.
    pub aliases: Vec<String>,
    pub stream: StreamId,
    /// 0 before the first commit.
    pub manifest_version: u64,
    /// The live manifest's `live_doc_count`.
    pub live_doc_count: u64,
    pub size_bytes: u64,
    /// From the `loams.created_at_ms` annotation (Ruling 13); 0 without it.
    pub created_at_ms: u64,
    /// Σ over partitions of (high watermark − applied).
    pub link_lag_records: u64,
    pub hot: HotStatus,
    /// The bytes of the offset index entries past `applied` (Task 15).
    #[serde(default)]
    pub unapplied_bytes: u64,
    /// The write budget and whether a plain write is refused now (Task 15).
    #[serde(default)]
    pub backpressure: BackpressureStatus,
}

/// Options of a collection write (Ruling 10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WriteOptions {
    /// Report `Created`/`Updated`/`Deleted`/`NotFound`/`Noop` instead of
    /// `Accepted`.
    pub report_existence: bool,
    /// Validate every op first and write nothing if one fails (Ruling 16).
    pub atomic: bool,
    /// `Bulk` admits the write up to `override_factor` × the budget
    /// (`Loams-Backpressure: off`, Task 15).
    pub backpressure: Override,
}

/// The answer to a collection write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WriteResult {
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
    /// One per op, in input order.
    pub results: Vec<OpResult>,
    /// One per op: where it was appended, `None` when it was not.
    pub positions: Vec<Option<OpPosition>>,
    /// The collection's backlog measured at admission, before this write's
    /// records (Task 15).
    #[serde(default)]
    pub backlog: Backlog,
}

/// The outcome of one op (Ruling 10).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpResult {
    Created,
    Updated,
    Deleted,
    NotFound,
    /// A patch whose result equals the present document.
    Noop,
    /// Written without existence reporting.
    Accepted,
    Rejected(ServiceError),
}

impl OpResult {
    /// Whether the key existed before the op: `Created` and `NotFound` →
    /// `Some(false)`; `Updated`, `Deleted` and `Noop` → `Some(true)`;
    /// `Accepted` and `Rejected` → `None` (M1.4's view).
    pub fn existed(&self) -> Option<bool> {
        match self {
            OpResult::Created | OpResult::NotFound => Some(false),
            OpResult::Updated | OpResult::Deleted | OpResult::Noop => Some(true),
            OpResult::Accepted | OpResult::Rejected(_) => None,
        }
    }
}

/// Where an op was appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpPosition {
    pub partition: u32,
    pub seq_no: u64,
}

/// A document as a get or scroll returns it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredDoc {
    #[serde(with = "crate::json::pk")]
    pub pk: PrimaryKey,
    pub source: Option<Map<String, Value>>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "is_empty_map")]
    pub sparse_vectors: BTreeMap<String, SparseVector>,
    pub fields: BTreeMap<String, Vec<FieldValue>>,
    pub seq_no: u64,
    pub partition: u32,
}

/// One retained manifest version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestInfo {
    pub version: u64,
    pub created_at_ms: u64,
    pub size_bytes: u64,
    pub live_doc_count: u64,
    pub lance_version: u64,
}

/// A pinned view of a collection (Ruling 14): it holds nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedRead {
    pub collection: CollectionId,
    pub name: String,
    pub manifest_version: u64,
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
}

impl PinnedRead {
    /// `ReadConsistency::Pinned { manifest_version, token }`.
    pub fn consistency(&self) -> ReadConsistency {
        ReadConsistency::Pinned {
            manifest_version: self.manifest_version,
            token: self.token.clone(),
        }
    }
}

/// One member of an alias (M1.5 Task 0a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AliasMember {
    /// The member collection's name.
    pub collection: String,
    /// Its `is_write_index` setting: `Some(true)`, `Some(false)` or unset.
    pub is_write_index: Option<bool>,
}

/// An alias with its members (M1.5 Task 0a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AliasInfo {
    pub alias: String,
    /// By collection name.
    pub members: Vec<AliasMember>,
    /// The name of the member writes through the alias go to (M1.5 Ruling
    /// 9): the member set to true, else the only member when it is unset,
    /// else none.
    pub write_target: Option<String>,
}

/// What a name of a namespace names (M1.5 Task 0a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NameInfo {
    /// A collection, by name.
    Collection(String),
    Alias(AliasInfo),
}
