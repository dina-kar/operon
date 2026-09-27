//! The shared protobuf conversions: selectors and vector outputs.

use crate::model::common::{
    NamedVectorOutput, PayloadSelector, VectorOutput, WithPayload, WithVector,
};
use crate::proto::qdrant as pb;

/// A gRPC payload selector; an absent one (or one without options) is
/// `WithPayload::Bool(default)`.
pub fn with_payload_from_grpc(s: Option<&pb::WithPayloadSelector>, default: bool) -> WithPayload {
    use pb::with_payload_selector::SelectorOptions;
    match s.and_then(|s| s.selector_options.as_ref()) {
        None => WithPayload::Bool(default),
        Some(SelectorOptions::Enable(on)) => WithPayload::Bool(*on),
        Some(SelectorOptions::Include(i)) => WithPayload::Selector(PayloadSelector::Include {
            include: i.fields.clone(),
        }),
        Some(SelectorOptions::Exclude(e)) => WithPayload::Selector(PayloadSelector::Exclude {
            exclude: e.fields.clone(),
        }),
    }
}

/// A gRPC vectors selector; an absent one is `WithVector::Bool(default)`.
pub fn with_vector_from_grpc(s: Option<&pb::WithVectorsSelector>, default: bool) -> WithVector {
    use pb::with_vectors_selector::SelectorOptions;
    match s.and_then(|s| s.selector_options.as_ref()) {
        None => WithVector::Bool(default),
        Some(SelectorOptions::Enable(on)) => WithVector::Bool(*on),
        Some(SelectorOptions::Include(i)) => WithVector::Names(i.names.clone()),
    }
}

/// An output vector in the `VectorOutput.vector` oneof, with the deprecated
/// `data` left empty (`qdrant:lib/api/src/conversions/vectors.rs:102-130`).
fn named_to_grpc(v: &NamedVectorOutput) -> pb::VectorOutput {
    use pb::vector_output::Vector;
    let vector = match v {
        NamedVectorOutput::Dense(data) => Vector::Dense(pb::DenseVector { data: data.clone() }),
        NamedVectorOutput::Sparse { indices, values } => Vector::Sparse(pb::SparseVector {
            values: values.clone(),
            indices: indices.clone(),
        }),
    };
    pb::VectorOutput {
        vector: Some(vector),
        ..Default::default()
    }
}

/// A point's output vectors as gRPC sends them.
pub fn vector_output_to_grpc(v: &VectorOutput) -> pb::VectorsOutput {
    use pb::vectors_output::VectorsOptions;
    let options = match v {
        VectorOutput::Single(data) => {
            VectorsOptions::Vector(named_to_grpc(&NamedVectorOutput::Dense(data.clone())))
        }
        VectorOutput::Named(map) => VectorsOptions::Vectors(pb::NamedVectorsOutput {
            vectors: map
                .iter()
                .map(|(name, v)| (name.clone(), named_to_grpc(v)))
                .collect(),
        }),
    };
    pb::VectorsOutput {
        vectors_options: Some(options),
    }
}
