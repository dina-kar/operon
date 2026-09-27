//! Collection request and response types.

use serde::Serialize;

/// `GET /collections`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CollectionDescription {
    pub name: String,
}
