//! Operon collections (design §03 §3, §06): documents, the collection catalog
//! client, Lance and Tantivy storage under one manifest, and the collection
//! link target.
//!
//! So far (plan M1.1 Task 5): [`PrimaryKey`] and [`partition_of`], the
//! canonical [`SparseVector`], [`DocOp`] and its record codec
//! ([`encode`]/[`decode`]), patches ([`apply_patch`]) and the per-key
//! latest-wins [`fold`], and the [`ConsistencyToken`] a write returns.
//!
//! The collection schema types live in `operon_common::schema`, because the
//! metastore's commands carry them (plan M1.1 Ruling 6); they are re-exported
//! here, both as [`schema`] and at the crate root.

mod codec;
mod doc;
mod error;
mod pk;
mod resolve;
mod token;

pub use operon_common::schema;
pub use operon_common::schema::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, KNOWN_ANALYZERS, MAX_VECTOR_DIM, Quantization, SchemaError, SparseModifier,
    SparseVectorSpec, VectorElement, VectorIndexSpec, VectorSpec,
};

pub use codec::{CODEC_VERSION, MAX_RECORD_VALUE_BYTES, decode, encode};
pub use doc::{DocOp, Document, PatchMode, SparseVector, apply_patch};
pub use error::{CodecError, SparseVectorError};
pub use pk::{MAX_STR_PK_BYTES, PrimaryKey, partition_of};
pub use resolve::{fold, needs_current};
pub use token::{CONSISTENCY_TOKEN_HEADER, ConsistencyToken, TokenParseError};
