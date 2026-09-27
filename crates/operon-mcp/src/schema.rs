//! The memory collection (plan M1.6 Task 8 rule 5, Ruling 9): created on
//! first use; its vector is added on the first write that carries one.

use std::collections::BTreeMap;

use operon_collection::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, VectorElement, VectorIndexSpec, VectorSpec,
};

/// The memory collection's vector.
pub const MEMORY_VECTOR: &str = "embedding";

fn field(name: &str, kind: FieldKind, fast: bool) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        kind,
        indexed: true,
        fast,
        ignore_malformed: false,
    }
}

/// The schema of a new memory collection; with `vector_dim`, it has the
/// `embedding` vector too.
pub fn memory_schema(vector_dim: Option<u32>) -> CollectionSchema {
    CollectionSchema {
        version: 1,
        fields: vec![
            field(
                "text",
                FieldKind::Text {
                    analyzer: "standard".to_string(),
                    positions: true,
                },
                false,
            ),
            field("tags", FieldKind::Keyword, true),
            field("author", FieldKind::Keyword, true),
            field("created_at", FieldKind::Date, true),
            field("metadata", FieldKind::Json, false),
        ],
        vectors: vector_dim.map(memory_vector).into_iter().collect(),
        sparse_vectors: Vec::new(),
        dynamic: DynamicMapping::Ignore,
        max_fields: DEFAULT_MAX_FIELDS,
        annotations: BTreeMap::new(),
    }
}

/// The `embedding` vector of `dim` dimensions, cosine.
pub fn memory_vector(dim: u32) -> VectorSpec {
    VectorSpec {
        name: MEMORY_VECTOR.to_string(),
        dim,
        distance: Distance::Cosine,
        element: VectorElement::F32,
        index: VectorIndexSpec::Auto,
        hnsw: HnswParams::default(),
        quantization: None,
    }
}

/// Whether `schema` has a text field named `text` (annotations are
/// ignored, row E18).
pub fn is_memory_collection(schema: &CollectionSchema) -> bool {
    schema
        .fields
        .iter()
        .any(|f| f.name == "text" && matches!(f.kind, FieldKind::Text { .. }))
}
