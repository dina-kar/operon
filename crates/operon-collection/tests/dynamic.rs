//! ES dynamic mapping as a pure function.

mod common;

use common::{field, obj, schema, text, vector};
use operon_collection::{
    CollectionSchema, DynamicMapping, DynamicMappingError, FieldKind, FieldSpec, SparseModifier,
    SparseVectorSpec, propose_dynamic_fields,
};
use serde_json::{Map, Value, json};

/// A proposed field: `name` from `source_path`.
fn proposed(name: &str, source_path: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: source_path.to_string(),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    }
}

fn text_kind() -> FieldKind {
    FieldKind::Text {
        analyzer: "standard".to_string(),
        positions: true,
    }
}

fn propose(schema: &CollectionSchema, sources: &[Value]) -> Vec<FieldSpec> {
    let sources: Vec<Map<String, Value>> = sources.iter().cloned().map(obj).collect();
    let refs: Vec<&Map<String, Value>> = sources.iter().collect();
    propose_dynamic_fields(schema, &refs).expect("proposal")
}

#[test]
fn dynamic_mapping_follows_es_rules() {
    let empty = schema(vec![], DynamicMapping::Map);
    let source = json!({
        "title": "x", "n": 1, "f": 1.5, "b": true, "d": "2024-01-02",
        "o": {"k": "v"}, "arr": [null, 2],
    });
    assert_eq!(
        propose(&empty, &[source]),
        [
            proposed("arr", "arr", FieldKind::I64),
            proposed("b", "b", FieldKind::Bool),
            proposed("d", "d", FieldKind::Date),
            proposed("f", "f", FieldKind::F64),
            proposed("n", "n", FieldKind::I64),
            proposed("o.k", "o.k", text_kind()),
            proposed("o.k.keyword", "o.k", FieldKind::Keyword),
            proposed("title", "title", text_kind()),
            proposed("title.keyword", "title", FieldKind::Keyword),
        ]
    );

    // Mapped paths are left alone; an existing `.keyword` name is skipped;
    // a digit-only string is text, not a date; nulls and empty arrays map
    // nothing; the first source that has a path decides its type.
    let partial = schema(
        vec![text("title"), field("s.keyword", FieldKind::I64)],
        DynamicMapping::Map,
    );
    let sources = [
        json!({"title": 5, "s": "x", "digits": "1704153600000", "z": null, "e": []}),
        json!({"s": 1, "later": [null, {"x": 2.5}]}),
    ];
    assert_eq!(
        propose(&partial, &sources),
        [
            proposed("digits", "digits", text_kind()),
            proposed("digits.keyword", "digits", FieldKind::Keyword),
            proposed("s", "s", text_kind()),
            proposed("later.x", "later.x", FieldKind::F64),
        ]
    );
    // Every proposal extends the schema validly.
    let mut next = partial.clone();
    next.fields.extend(propose(&partial, &sources));
    next.version += 1;
    next.validate().unwrap();
    partial.check_additive(&next).unwrap();
    // Nothing unmapped, nothing proposed.
    assert!(propose(&next, &sources).is_empty());
}

#[test]
fn dynamic_mapping_is_deterministic() {
    let empty = schema(vec![], DynamicMapping::Map);
    let sources = [
        json!({"b": {"y": "2024/01/02", "x": [1, 2]}, "a": false}),
        json!({"c": "text", "b": {"x": 1.5, "w": "2024-01-02T03:04:05Z"}}),
    ];
    let first = propose(&empty, &sources);
    for _ in 0..10 {
        assert_eq!(propose(&empty, &sources), first);
    }
    assert_eq!(
        first,
        [
            proposed("a", "a", FieldKind::Bool),
            proposed("b.x", "b.x", FieldKind::I64),
            proposed("b.y", "b.y", FieldKind::Date),
            proposed("b.w", "b.w", FieldKind::Date),
            proposed("c", "c", text_kind()),
            proposed("c.keyword", "c", FieldKind::Keyword),
        ]
    );
}

#[test]
fn dynamic_mapping_respects_the_field_limit() {
    // One field, one dense and one sparse vector: 3 of 6.
    let mut limited = CollectionSchema::new(
        vec![field("n", FieldKind::I64)],
        vec![vector("v", 2)],
        DynamicMapping::Map,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    limited.max_fields = 6;
    limited.validate().unwrap();

    let fits = [json!({"a": 1, "b": 2, "c": 3})];
    assert_eq!(propose(&limited, &fits).len(), 3);

    // A string is two fields (text and keyword): 3 + 4 > 6.
    let over = [obj(json!({"a": 1, "b": 2, "t": "x"}))];
    let refs: Vec<&Map<String, Value>> = over.iter().collect();
    assert_eq!(
        propose_dynamic_fields(&limited, &refs),
        Err(DynamicMappingError::TooManyFields { limit: 6 })
    );
}

#[test]
fn paths_that_are_invalid_field_names_are_not_mapped() {
    let empty = schema(vec![], DynamicMapping::Map);
    let long = "k".repeat(256);
    let source = json!({
        "_meta": {"x": 1}, "-dash": 1, "": 2, "a.": 3, "ok": 4, long.clone(): 5,
        "o": {"_inner": true},
    });
    assert_eq!(
        propose(&empty, &[source]),
        [
            proposed("o._inner", "o._inner", FieldKind::Bool),
            proposed("ok", "ok", FieldKind::I64),
        ]
    );
    // A text path whose `.keyword` name would be too long keeps its text field.
    let almost = "t".repeat(250);
    assert_eq!(
        propose(&empty, &[json!({ almost.clone(): "x" })]),
        [proposed(&almost, &almost, text_kind())]
    );
}
