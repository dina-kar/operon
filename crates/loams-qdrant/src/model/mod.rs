//! The hand-written serde model of Qdrant's REST types (Ruling 3). Unions
//! are `#[serde(untagged)]` in Qdrant's variant order, `None` is skipped on
//! output, and unknown request fields are ignored (Ruling 4).

pub mod collections;
pub mod common;
pub mod filter;
pub mod points;
pub mod query;
