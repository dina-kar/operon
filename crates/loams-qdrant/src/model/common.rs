//! The REST types every endpoint shares: selectors, vector inputs and
//! outputs, update results and points.

use std::collections::BTreeMap;

use loams_collection::SparseVector;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::GatewayError;
use crate::ids::PointId;

/// `with_payload`: all or nothing, a list of keys, or a selector.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum WithPayload {
    Bool(bool),
    Include(Vec<String>),
    Selector(PayloadSelector),
}

/// `{"include": [...]}` or `{"exclude": [...]}`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum PayloadSelector {
    Include { include: Vec<String> },
    Exclude { exclude: Vec<String> },
}

/// `with_vector(s)`: all or nothing, or a list of vector names.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum WithVector {
    Bool(bool),
    Names(Vec<String>),
}

/// A vector as a request gives it: dense, multi, sparse, a point id, or an
/// object (an inference object, or anything else).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum VectorInput {
    Dense(Vec<f32>),
    Multi(Vec<Vec<f32>>),
    Sparse { indices: Vec<u32>, values: Vec<f32> },
    Id(Value),
    Object(Map<String, Value>),
}

/// A [`VectorInput`] the gateway can use.
#[derive(Clone, Debug, PartialEq)]
pub enum VectorValue {
    Dense(Vec<f32>),
    Sparse(SparseVector),
    /// The stored vector of this point.
    Id(PointId),
}

impl VectorInput {
    /// Multivectors and objects are unsupported (Ruling 15); a sparse vector
    /// goes through `SparseVector::new` (Ruling 21); an id through
    /// [`PointId::from_json`]. `name` names the vector in errors.
    pub fn resolve(self, name: &str) -> Result<VectorValue, GatewayError> {
        match self {
            VectorInput::Dense(v) => Ok(VectorValue::Dense(v)),
            VectorInput::Multi(_) => Err(GatewayError::Unsupported("multivectors".to_string())),
            VectorInput::Sparse { indices, values } => SparseVector::new(indices, values)
                .map(VectorValue::Sparse)
                .map_err(|err| GatewayError::BadRequest(format!("Sparse vector {name}: {err}"))),
            // `Id` takes any JSON value, so objects land here too.
            VectorInput::Id(Value::Object(_)) | VectorInput::Object(_) => {
                Err(GatewayError::Unsupported("inference objects".to_string()))
            }
            VectorInput::Id(v) => PointId::from_json(&v).map(VectorValue::Id),
        }
    }
}

/// The result of a write.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UpdateResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<u64>,
    pub status: UpdateStatus,
}

/// `acknowledged` without `wait`, `completed` with it (Ruling 14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    Acknowledged,
    Completed,
}

/// A point as retrieve and scroll return it.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record {
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorOutput>,
}

/// A point as searches return it; `version` is always 0 (Ruling 20).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScoredPoint {
    pub id: Value,
    pub version: u64,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorOutput>,
}

/// A bare list when exactly the default vector `""` is selected, else a map
/// by name (`qdrant:lib/segment/src/data_types/vectors.rs:569-590`).
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum VectorOutput {
    Single(Vec<f32>),
    Named(BTreeMap<String, NamedVectorOutput>),
}

/// One named vector; a sparse one is sorted by index.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum NamedVectorOutput {
    Dense(Vec<f32>),
    Sparse { indices: Vec<u32>, values: Vec<f32> },
}

impl From<&SparseVector> for NamedVectorOutput {
    fn from(v: &SparseVector) -> Self {
        NamedVectorOutput::Sparse {
            indices: v.indices().to_vec(),
            values: v.values().to_vec(),
        }
    }
}
