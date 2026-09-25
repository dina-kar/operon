//! `operon-query`: the read side of collections and the one facade every
//! gateway uses (plan M1.2).
//!
//! This crate holds the search IR of overview §6.6 with its exact JSON form
//! ([`ir`], [`json`]), the service types and errors ([`types`], [`error`]),
//! schema-free request validation ([`validate`]), and the hot-tier and
//! placement hooks ([`hot`], [`placement`]).
//!
//! Task 2: the query compiler from the IR to Tantivy queries per split
//! schema ([`text`]). Task 3: the in-memory tail index of writes the live
//! manifest does not reflect yet ([`tail`], H3). Task 4: read views per
//! consistency level ([`read`]). Task 5: text search over splits and the
//! tail, filter bitmaps and global BM25 statistics ([`exec`], [`text`]).
//! Task 6: dense vector search over Lance, the tail and the hot tier, and
//! exact sparse vector search ([`vector`], [`sparse`], [`exec`]).
//! Task 7: search assembly, get, count and scroll ([`exec::planner`]).
//! Task 8: aggregations and highlighting ([`exec::aggs`],
//! [`text::highlight`]).

pub mod error;
pub mod exec;
pub mod hot;
pub mod ir;
pub mod json;
pub mod placement;
pub mod read;
pub mod sparse;
pub mod tail;
pub mod text;
pub mod types;
pub mod validate;
pub mod vector;

pub use error::{NOT_FOUND_KINDS, ServiceError};
pub use ir::{
    AnnParams, BoolOperator, FieldValue, Fusion, Fuzziness, GroupBy, Highlight, HighlightField,
    Hit, HitGroup, MissingOrder, MultiMatchKind, Query, ReadConsistency, Retriever, SearchRequest,
    SearchResponse, SortKey, SortOrder, SortValue, SparseParams, SparseVector, TotalHits,
    TotalRelation, TrackTotalHits,
};
pub use json::alias_actions_from_json;
pub use types::{
    AliasAction, CollectionInfo, ManifestInfo, OpPosition, OpResult, PinnedRead, Projection,
    SourceFilter, StoredDoc, WriteOptions, WriteResult,
};
pub use validate::{SearchLimits, validate_request};
