//! Point request and response types.

use serde::{Deserialize, Serialize};

use crate::model::filter::Filter;

/// `POST /collections/{c}/points/count`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CountRequest {
    #[serde(default)]
    pub filter: Option<Filter>,
    /// Counts are always exact.
    #[serde(default)]
    pub exact: Option<bool>,
}

/// `{"count": n}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct CountResult {
    pub count: u64,
}
