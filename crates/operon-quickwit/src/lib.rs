//! Quickwit's split, directory, query and merge-policy code, vendored from
//! quickwit-oss/quickwit af0591a3 and adapted to crates.io Tantivy 0.26.2 (design §11 §2).
//!
//! Every vendored file keeps its Datadog Apache-2.0 header and says, on the line below
//! it, where it came from and how it was modified; the files are also reformatted with
//! Operon's `rustfmt.toml`. [`shim`] is Operon's stand-in for the Quickwit crates the
//! vendored files import (`quickwit-common`, `-proto`, `-config`, `-metastore`). See
//! `NOTICE`.
//!
//! The workspace enables `serde_json/preserve_order` (M1.3 row E58), so `serde_json::Map`
//! keeps insertion order; the vendored code does not rely on sorted maps
//! (`tests/it/canary.rs`).

pub mod datetime;
pub mod directories;
pub mod doc_mapper;
pub mod merge_policy;
pub mod query;
pub mod search;
pub mod shim;
pub mod storage;
