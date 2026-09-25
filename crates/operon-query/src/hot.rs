//! The hot tier as the query engine sees it (plan M1.2 Task 1; overview
//! A13, A19, A23): what a read reports about the hot structures it used.

use serde::{Deserialize, Serialize};

/// A hot structure a read can use; declaration order is the sorted order of
/// the `Operon-Hot-Used` header. Fragment prefetch (H1) is never reported
/// (overview A19).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotKind {
    Hnsw,
    Splits,
}

/// The `hot` value of `CollectionInfo`. M1.3's `GET …/collections/{c}`
/// handler replaces it with the owner's full status, a superset whose
/// `vectors`, `text` and `fragments` objects each carry these `state` and
/// `source_version` keys.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotStatus {
    pub vectors: HotState,
    pub text: HotState,
    pub fragments: HotState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotState {
    pub state: HotStateKind,
    /// The manifest version the structure reflects.
    pub source_version: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotStateKind {
    #[default]
    Off,
    Building,
    Ready,
}
