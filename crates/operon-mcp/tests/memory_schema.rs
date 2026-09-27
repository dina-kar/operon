//! The memory collection's schema (plan M1.6 Task 8 rule 5, Ruling 9).

use std::collections::BTreeMap;

use operon_collection::{
    Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, VectorElement, VectorIndexSpec,
    VectorSpec,
};
use operon_mcp::schema::{MEMORY_VECTOR, is_memory_collection, memory_schema, memory_vector};

fn field(name: &str, kind: FieldKind, fast: bool) -> FieldSpec {
    FieldSpec {
        name: name.into(),
        source_path: name.into(),
        kind,
        indexed: true,
        fast,
        ignore_malformed: false,
    }
}

#[test]
fn memory_schema_is_exact() {
    let schema = memory_schema(None);
    assert_eq!(schema.version, 1);
    assert_eq!(
        schema.fields,
        vec![
            field(
                "text",
                FieldKind::Text {
                    analyzer: "standard".into(),
                    positions: true
                },
                false
            ),
            field("tags", FieldKind::Keyword, true),
            field("author", FieldKind::Keyword, true),
            field("created_at", FieldKind::Date, true),
            field("metadata", FieldKind::Json, false),
        ]
    );
    assert!(schema.vectors.is_empty());
    assert!(schema.sparse_vectors.is_empty());
    assert_eq!(schema.dynamic, DynamicMapping::Ignore);
    assert_eq!(schema.max_fields, 1000);
    assert_eq!(schema.annotations, BTreeMap::new());
}

#[test]
fn memory_schema_with_a_dimension_has_the_embedding_vector() {
    let schema = memory_schema(Some(384));
    assert_eq!(schema.vectors, vec![memory_vector(384)]);
    assert_eq!(
        memory_vector(3),
        VectorSpec {
            name: MEMORY_VECTOR.into(),
            dim: 3,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::Auto,
            hnsw: HnswParams::default(),
            quantization: None,
        }
    );
    assert_eq!(MEMORY_VECTOR, "embedding");
    assert_eq!(schema.fields, memory_schema(None).fields);
}

#[test]
fn is_memory_collection_needs_a_text_field_named_text() {
    let mut schema = memory_schema(None);
    assert!(is_memory_collection(&schema));
    // Annotations (such as `operon.created_at_ms`) do not matter (row E18).
    schema
        .annotations
        .insert("operon.created_at_ms".into(), "1".into());
    assert!(is_memory_collection(&schema));
    schema.fields[0].kind = FieldKind::Keyword;
    assert!(!is_memory_collection(&schema));
    schema.fields[0] = field(
        "body",
        FieldKind::Text {
            analyzer: "standard".into(),
            positions: true,
        },
        false,
    );
    assert!(!is_memory_collection(&schema));
    // A field named `text` read from another path would not index what
    // `memory_write` stores at `source.text` (PR #99 review).
    schema.fields[0] = FieldSpec {
        source_path: "body".into(),
        ..field(
            "text",
            FieldKind::Text {
                analyzer: "standard".into(),
                positions: true,
            },
            false,
        )
    };
    assert!(!is_memory_collection(&schema));
}
