//! Operon collections (design §03 §3, §06): documents, the collection catalog
//! client, Lance and Tantivy storage under one manifest, and the collection
//! link target.
//!
//! So far (plan M1.1 Task 5): [`PrimaryKey`] and [`partition_of`], the
//! canonical [`SparseVector`], [`DocOp`] and its record codec
//! ([`encode`]/[`decode`]), patches ([`apply_patch`]) and the per-key
//! latest-wins [`fold`], and the [`ConsistencyToken`] a write returns.
//!
//! Task 6: value [`extract`]ion and [`coerce`]ion, document validation
//! ([`check_document`], [`check_patch`]) and ES dynamic mapping
//! ([`propose_dynamic_fields`]), and the [`CollectionWriter`].
//!
//! Task 7: the Lance integration: the Arrow schema ([`arrow_schema()`],
//! [`to_record_batch`], [`row_from_batch`]), [`LanceEnv`] (the Operon
//! object-store provider, version 1) and [`LanceCommitter`], which commits
//! every later Lance version detached from the mainline (R7, Ruling 1).
//!
//! The collection schema types live in `operon_common::schema`, because the
//! metastore's commands carry them (plan M1.1 Ruling 6); they are re-exported
//! here, both as [`schema`] and at the crate root.

mod arrow_schema;
mod codec;
mod config;
mod doc;
mod dynamic;
mod error;
mod lance;
mod pk;
mod resolve;
mod token;
mod values;
mod writer;

pub use operon_common::schema;
pub use operon_common::schema::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, KNOWN_ANALYZERS, MAX_VECTOR_DIM, Quantization, SchemaError, SparseModifier,
    SparseVectorSpec, VectorElement, VectorIndexSpec, VectorSpec,
};

pub use crate::lance::{LANCE_SCHEME, LanceCommitter, LanceEnv};
pub use arrow_schema::{
    INGEST_OFFSET_COLUMN, INGEST_PARTITION_COLUMN, NewRow, PK_COLUMN, SOURCE_COLUMN, StoredRow,
    arrow_schema, base_arrow_schema, row_from_batch, sparse_column, to_record_batch, vector_column,
};
pub use codec::{CODEC_VERSION, MAX_RECORD_VALUE_BYTES, decode, encode};
pub use config::LanceConfig;
pub use doc::{DocOp, Document, PatchMode, SparseVector, apply_patch};
pub use dynamic::{DynamicMappingError, propose_dynamic_fields};
pub use error::{CodecError, CollectionError, SparseVectorError};
pub use pk::{MAX_STR_PK_BYTES, PrimaryKey, partition_of};
pub use resolve::{fold, needs_current};
pub use token::{CONSISTENCY_TOKEN_HEADER, ConsistencyToken, TokenParseError};
pub use values::{
    DocRejection, ExtractedDoc, IndexValue, Violation, check_document, check_patch, coerce,
    extract, parse_date, unmapped_paths,
};
pub use writer::{CollectionWriter, MAX_WRITE_OPS, OpError, OpResult, WriteError, WriteOutcome};
