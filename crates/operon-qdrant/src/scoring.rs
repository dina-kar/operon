//! Vector checks and Qdrant's distance arithmetic. Task 5 adds the write
//! side: vector names, dimensions, finiteness and cosine normalization
//! (Ruling 8), and sparse values (Ruling 21).

use std::collections::BTreeMap;

use operon_collection::{CollectionSchema, Distance, SparseVector};
use serde_json::Value;

use crate::error::GatewayError;
use crate::model::common::VectorInput;

/// Qdrant's `cosine_preprocess`: divides by the length, unless the squared
/// length is below `f32::EPSILON` or within `1e-6` of 1
/// (`qdrant:lib/segment/src/spaces/simple.rs:228-235`, `tools.rs:14-16`).
pub fn cosine_normalize(v: &mut [f32]) {
    let squared: f32 = v.iter().map(|x| x * x).sum();
    if squared < f32::EPSILON || (squared - 1.0).abs() <= 1.0e-6 {
        return;
    }
    let length = squared.sqrt();
    v.iter_mut().for_each(|x| *x /= length);
}

fn not_existing(name: &str) -> GatewayError {
    GatewayError::BadRequest(format!("Not existing vector name error: {name}"))
}

/// A dense value for `name`: the name must be a dense schema vector, the
/// length its dimension, every value finite; Cosine vectors are then
/// normalized.
pub fn check_vector(
    schema: &CollectionSchema,
    name: &str,
    v: &mut [f32],
) -> Result<(), GatewayError> {
    let Some(spec) = schema.vectors.iter().find(|s| s.name == name) else {
        if schema.sparse_vectors.iter().any(|s| s.name == name) {
            return Err(GatewayError::BadRequest(format!(
                "Vector {name} is a sparse vector"
            )));
        }
        return Err(not_existing(name));
    };
    if v.len() != spec.dim as usize {
        return Err(GatewayError::BadRequest(format!(
            "Vector dimension error: expected dim: {}, got {}",
            spec.dim,
            v.len()
        )));
    }
    if v.iter().any(|x| !x.is_finite()) {
        return Err(GatewayError::BadRequest(
            "Vector contains NaN or infinite values".to_string(),
        ));
    }
    if spec.distance == Distance::Cosine {
        cosine_normalize(v);
    }
    Ok(())
}

/// A sparse value for `name`: the name must be a sparse schema vector; the
/// value goes through `SparseVector::new` (sorted, zero weights kept).
pub fn check_sparse(
    schema: &CollectionSchema,
    name: &str,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> Result<SparseVector, GatewayError> {
    if !schema.sparse_vectors.iter().any(|s| s.name == name) {
        if schema.vectors.iter().any(|s| s.name == name) {
            return Err(GatewayError::BadRequest(format!(
                "Vector {name} is a dense vector"
            )));
        }
        return Err(not_existing(name));
    }
    SparseVector::new(indices, values)
        .map_err(|err| GatewayError::BadRequest(format!("Sparse vector {name}: {err}")))
}

/// Checked vectors, split by kind.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CheckedVectors {
    pub dense: BTreeMap<String, Vec<f32>>,
    pub sparse: BTreeMap<String, SparseVector>,
}

/// Qdrant's text for a named value that is no vector at all.
pub(crate) fn not_a_vector() -> GatewayError {
    GatewayError::json("data did not match any variant of untagged enum VectorStruct")
}

/// Named values checked one by one ([`check_vector`], [`check_sparse`]);
/// multivectors and inference objects are unsupported (Ruling 15).
pub(crate) fn check_named(
    schema: &CollectionSchema,
    named: BTreeMap<String, VectorInput>,
) -> Result<CheckedVectors, GatewayError> {
    let mut out = CheckedVectors::default();
    for (name, input) in named {
        match input {
            VectorInput::Dense(mut v) => {
                check_vector(schema, &name, &mut v)?;
                out.dense.insert(name, v);
            }
            VectorInput::Sparse { indices, values } => {
                let v = check_sparse(schema, &name, indices, values)?;
                out.sparse.insert(name, v);
            }
            VectorInput::Multi(_) => {
                return Err(GatewayError::Unsupported("multivectors".to_string()));
            }
            VectorInput::Id(Value::Object(_)) | VectorInput::Object(_) => {
                return Err(GatewayError::Unsupported("inference objects".to_string()));
            }
            VectorInput::Id(_) => return Err(not_a_vector()),
        }
    }
    Ok(out)
}
