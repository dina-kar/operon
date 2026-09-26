//! Point request and response types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `POST /collections/{c}/points/count`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CountRequest {
    /// Compiled by Task 4; until then only an absent or `null` filter is
    /// served.
    #[serde(default)]
    pub filter: Option<Value>,
    /// Counts are always exact.
    #[serde(default)]
    pub exact: Option<bool>,
}

/// `{"count": n}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct CountResult {
    pub count: u64,
}
