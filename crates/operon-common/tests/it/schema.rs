//! `CollectionSchema` validation and additive evolution.

use operon_common::schema::{
    CollectionSchema, DEFAULT_MAX_FIELDS, Distance, DynamicMapping, FieldKind, FieldSpec,
    HnswParams, KNOWN_ANALYZERS, MAX_VECTOR_DIM, Quantization, SchemaError, SparseModifier,
    SparseVectorSpec, VectorElement, VectorIndexSpec, VectorSpec,
};

fn field(name: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        kind,
        indexed: true,
        fast: false,
        ignore_malformed: false,
    }
}

fn text(name: &str) -> FieldSpec {
    field(
        name,
        FieldKind::Text {
            analyzer: "standard".to_string(),
            positions: true,
        },
    )
}

fn vector(name: &str, dim: u32, distance: Distance, index: VectorIndexSpec) -> VectorSpec {
    VectorSpec {
        name: name.to_string(),
        dim,
        distance,
        element: VectorElement::F32,
        index,
        hnsw: HnswParams::default(),
        quantization: None,
    }
}

fn sparse(name: &str) -> SparseVectorSpec {
    SparseVectorSpec {
        name: name.to_string(),
        modifier: SparseModifier::Idf,
    }
}

/// A schema that exercises every accepted edge: a Json field over the whole
/// `_source`, the unnamed default vector, Manhattan without an index, every
/// index and quantization kind, a sparse vector and annotations.
fn valid() -> CollectionSchema {
    let mut payload = field("payload", FieldKind::Json);
    payload.source_path = String::new();
    let mut tag = field("meta.tag", FieldKind::Keyword);
    tag.source_path = "meta.tag".to_string();
    tag.fast = true;
    let mut count = field("count", FieldKind::I64);
    count.indexed = false;
    count.fast = true;
    let mut pq = vector(
        "pq",
        16,
        Distance::Dot,
        VectorIndexSpec::IvfPq {
            num_partitions: Some(1),
            num_sub_vectors: Some(4),
            num_bits: 8,
        },
    );
    pq.quantization = Some(Quantization::Product {
        compression_ratio: 16,
        always_ram: false,
    });
    let mut rq = vector(
        "rq",
        16,
        Distance::Euclid,
        VectorIndexSpec::IvfRq {
            num_partitions: Some(65_536),
            num_bits: 1,
        },
    );
    rq.quantization = Some(Quantization::Scalar {
        quantile_ppm: Some(500_000),
        always_ram: true,
    });
    let mut hnsw = vector(
        "hnsw",
        MAX_VECTOR_DIM,
        Distance::Cosine,
        VectorIndexSpec::IvfHnswSq {
            num_partitions: None,
        },
    );
    hnsw.quantization = Some(Quantization::Binary { always_ram: false });
    let mut schema = CollectionSchema::new(
        vec![
            text("title"),
            tag,
            count,
            payload,
            field("when", FieldKind::Date),
        ],
        vec![
            vector("", 4, Distance::Cosine, VectorIndexSpec::Auto),
            vector("taxi", 3, Distance::Manhattan, VectorIndexSpec::None),
            pq,
            rq,
            hnsw,
        ],
        DynamicMapping::Strict,
    )
    .with_sparse_vectors(vec![sparse("splade")]);
    schema
        .annotations
        .insert("es.mapping".to_string(), "{}".to_string());
    schema
        .annotations
        .insert("qdrant.payload_index.a".to_string(), "keyword".to_string());
    schema
}

#[track_caller]
fn assert_invalid(schema: &CollectionSchema) {
    match schema.validate() {
        Err(SchemaError::Invalid(message)) => assert!(!message.is_empty()),
        other => panic!("expected Invalid, got {other:?}"),
    }
}

/// Validates `valid()` changed by `change`.
#[track_caller]
fn assert_invalid_with(change: impl FnOnce(&mut CollectionSchema)) {
    let mut schema = valid();
    change(&mut schema);
    assert_invalid(&schema);
}

/// Appends a text field `name` read from the valid source path `x`.
fn with_field(name: &str) -> impl FnOnce(&mut CollectionSchema) {
    let mut spec = text(name);
    spec.source_path = "x".to_string();
    move |s| s.fields.push(spec)
}

#[test]
fn a_valid_schema_passes() {
    let schema = valid();
    assert_eq!(schema.validate(), Ok(()));
    assert_eq!(schema.version, 1);
    assert_eq!(schema.max_fields, DEFAULT_MAX_FIELDS);
    assert_eq!(
        KNOWN_ANALYZERS,
        ["standard", "english", "simple", "whitespace", "keyword"]
    );
    for analyzer in KNOWN_ANALYZERS {
        let mut schema = valid();
        schema.fields[0] = field(
            "title",
            FieldKind::Text {
                analyzer: analyzer.to_string(),
                positions: false,
            },
        );
        assert_eq!(schema.validate(), Ok(()), "{analyzer}");
    }
    let defaults = HnswParams::default();
    assert_eq!(
        (
            defaults.m,
            defaults.ef_construct,
            defaults.full_scan_threshold_kb,
            defaults.payload_m,
            defaults.on_disk
        ),
        (16, 100, 10_000, None, false)
    );
    let empty = CollectionSchema::new(vec![], vec![], DynamicMapping::Ignore);
    assert_eq!(empty.validate(), Ok(()));
    assert!(empty.sparse_vectors.is_empty() && empty.annotations.is_empty());
}

#[test]
fn lookups_find_fields_and_vectors_by_name() {
    let schema = valid();
    assert_eq!(schema.field("meta.tag").unwrap().kind, FieldKind::Keyword);
    assert!(schema.field("missing").is_none());
    assert_eq!(schema.vector("").unwrap().0, 0);
    assert_eq!(schema.vector("pq").unwrap().0, 2);
    assert!(schema.vector("missing").is_none());
    assert_eq!(schema.sparse_vector("splade").unwrap().0, 0);
    assert!(schema.sparse_vector("").is_none());
}

#[test]
fn rejects_version_zero() {
    assert_invalid_with(|s| s.version = 0);
}

#[test]
fn rejects_max_fields_out_of_range() {
    assert_invalid_with(|s| s.max_fields = 0);
    assert_invalid_with(|s| s.max_fields = 100_001);
    let mut schema = CollectionSchema::new(vec![], vec![], DynamicMapping::Map);
    schema.max_fields = 100_000;
    assert_eq!(schema.validate(), Ok(()));
}

#[test]
fn rejects_more_fields_and_vectors_than_max_fields() {
    // 5 fields + 5 vectors + 1 sparse vector.
    let mut schema = valid();
    schema.max_fields = 11;
    assert_eq!(schema.validate(), Ok(()));
    schema.max_fields = 10;
    assert_invalid(&schema);
}

#[test]
fn rejects_an_empty_field_name() {
    assert_invalid_with(with_field(""));
}

#[test]
fn rejects_a_field_name_over_255_bytes() {
    assert_eq!(
        {
            let mut s = valid();
            with_field(&"a".repeat(255))(&mut s);
            s.validate()
        },
        Ok(())
    );
    assert_invalid_with(with_field(&"a".repeat(256)));
}

#[test]
fn rejects_a_field_name_with_a_control_character() {
    assert_invalid_with(with_field("ti\ntle"));
    assert_invalid_with(with_field("x\u{7f}"));
}

#[test]
fn rejects_a_field_name_starting_with_underscore() {
    assert_invalid_with(with_field("_id"));
}

#[test]
fn rejects_a_field_name_starting_with_a_dash() {
    assert_invalid_with(with_field("-x"));
}

#[test]
fn rejects_a_field_name_starting_or_ending_with_a_dot() {
    assert_invalid_with(with_field(".x"));
    assert_invalid_with(with_field("x."));
}

#[test]
fn rejects_a_field_name_containing_two_dots() {
    assert_invalid_with(with_field("a..b"));
}

#[test]
fn rejects_duplicate_field_names() {
    assert_invalid_with(with_field("title"));
}

#[test]
fn rejects_an_empty_source_path_outside_json_fields() {
    assert_invalid_with(|s| s.fields[0].source_path = String::new());
}

#[test]
fn rejects_a_source_path_with_an_empty_segment() {
    for path in ["a..b", ".a", "a.", "."] {
        assert_invalid_with(|s| s.fields[0].source_path = path.to_string());
    }
}

#[test]
fn rejects_an_unknown_analyzer() {
    assert_invalid_with(|s| {
        s.fields[0].kind = FieldKind::Text {
            analyzer: "klingon".to_string(),
            positions: true,
        }
    });
}

#[test]
fn rejects_fast_text() {
    assert_invalid_with(|s| s.fields[0].fast = true);
}

#[test]
fn rejects_a_field_neither_indexed_nor_fast() {
    assert_invalid_with(|s| {
        s.fields[1].indexed = false;
        s.fields[1].fast = false;
    });
}

#[test]
fn rejects_a_vector_name_over_255_bytes() {
    assert_invalid_with(|s| s.vectors[1].name = "v".repeat(256));
}

#[test]
fn rejects_a_vector_name_with_a_control_character() {
    assert_invalid_with(|s| s.vectors[1].name = "v\t".to_string());
}

#[test]
fn rejects_duplicate_vector_names() {
    assert_invalid_with(|s| s.vectors[1].name = "pq".to_string());
}

#[test]
fn rejects_a_dim_out_of_range() {
    assert_invalid_with(|s| s.vectors[0].dim = 0);
    assert_invalid_with(|s| s.vectors[0].dim = MAX_VECTOR_DIM + 1);
}

#[test]
fn rejects_an_empty_sparse_vector_name() {
    assert_invalid_with(|s| s.sparse_vectors.push(sparse("")));
}

#[test]
fn rejects_a_sparse_vector_name_over_255_bytes() {
    assert_invalid_with(|s| s.sparse_vectors.push(sparse(&"s".repeat(256))));
}

#[test]
fn rejects_a_sparse_vector_name_with_a_control_character() {
    assert_invalid_with(|s| s.sparse_vectors.push(sparse("s\0")));
}

#[test]
fn rejects_duplicate_sparse_vector_names() {
    assert_invalid_with(|s| s.sparse_vectors.push(sparse("splade")));
}

#[test]
fn rejects_a_sparse_vector_named_like_a_dense_vector() {
    assert_invalid_with(|s| s.sparse_vectors.push(sparse("pq")));
}

#[test]
fn rejects_manhattan_with_ivf() {
    for index in [
        VectorIndexSpec::IvfPq {
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: 8,
        },
        VectorIndexSpec::IvfRq {
            num_partitions: None,
            num_bits: 1,
        },
        VectorIndexSpec::IvfHnswSq {
            num_partitions: None,
        },
    ] {
        assert_invalid_with(|s| s.vectors[1].index = index);
    }
    let mut schema = valid();
    schema.vectors[1].index = VectorIndexSpec::Auto;
    assert_eq!(schema.validate(), Ok(()));
}

#[test]
fn rejects_pq_num_bits_other_than_4_or_8() {
    for num_bits in [0, 1, 7, 16] {
        assert_invalid_with(|s| {
            s.vectors[2].index = VectorIndexSpec::IvfPq {
                num_partitions: None,
                num_sub_vectors: None,
                num_bits,
            }
        });
    }
}

#[test]
fn rejects_pq_sub_vectors_not_dividing_dim() {
    for num_sub_vectors in [0, 3, 32] {
        assert_invalid_with(|s| {
            s.vectors[2].index = VectorIndexSpec::IvfPq {
                num_partitions: None,
                num_sub_vectors: Some(num_sub_vectors),
                num_bits: 4,
            }
        });
    }
}

#[test]
fn rejects_rq_num_bits_out_of_range() {
    for num_bits in [0, 9] {
        assert_invalid_with(|s| {
            s.vectors[3].index = VectorIndexSpec::IvfRq {
                num_partitions: None,
                num_bits,
            }
        });
    }
}

#[test]
fn rejects_num_partitions_out_of_range() {
    for num_partitions in [0, 65_537] {
        let p = Some(num_partitions);
        for index in [
            VectorIndexSpec::IvfPq {
                num_partitions: p,
                num_sub_vectors: None,
                num_bits: 8,
            },
            VectorIndexSpec::IvfRq {
                num_partitions: p,
                num_bits: 8,
            },
            VectorIndexSpec::IvfHnswSq { num_partitions: p },
        ] {
            assert_invalid_with(|s| s.vectors[0].index = index);
        }
    }
}

#[test]
fn rejects_an_unknown_pq_compression_ratio() {
    for compression_ratio in [0, 2, 12, 128] {
        assert_invalid_with(|s| {
            s.vectors[0].quantization = Some(Quantization::Product {
                compression_ratio,
                always_ram: false,
            })
        });
    }
}

#[test]
fn rejects_a_quantile_out_of_range() {
    for quantile_ppm in [0, 499_999, 1_000_001] {
        assert_invalid_with(|s| {
            s.vectors[0].quantization = Some(Quantization::Scalar {
                quantile_ppm: Some(quantile_ppm),
                always_ram: false,
            })
        });
    }
}

#[test]
fn rejects_more_annotations_than_max_fields_plus_256() {
    let mut schema = CollectionSchema::new(vec![], vec![], DynamicMapping::Map);
    schema.max_fields = 1;
    for i in 0..257 {
        schema
            .annotations
            .insert(format!("operon.{i}"), String::new());
    }
    assert_eq!(schema.validate(), Ok(()));
    schema
        .annotations
        .insert("operon.one-too-many".to_string(), String::new());
    assert_invalid(&schema);
}

#[test]
fn rejects_an_annotation_key_outside_the_gateway_namespaces() {
    for key in ["", "es", "esx.a", "other.a", "ES.a"] {
        assert_invalid_with(|s| {
            s.annotations.insert(key.to_string(), String::new());
        });
    }
}

#[test]
fn rejects_an_annotation_key_over_255_bytes() {
    assert_invalid_with(|s| {
        s.annotations
            .insert(format!("es.{}", "k".repeat(253)), String::new());
    });
}

#[test]
fn rejects_an_annotation_value_over_65536_bytes() {
    let mut schema = valid();
    schema
        .annotations
        .insert("es.big".to_string(), "v".repeat(65_536));
    assert_eq!(schema.validate(), Ok(()));
    assert_invalid_with(|s| {
        s.annotations
            .insert("es.big".to_string(), "v".repeat(65_537));
    });
}

#[track_caller]
fn assert_incompatible(old: &CollectionSchema, next: &CollectionSchema) {
    match old.check_additive(next) {
        Err(SchemaError::Incompatible(message)) => assert!(!message.is_empty()),
        other => panic!("expected Incompatible, got {other:?}"),
    }
}

#[test]
fn appending_fields_and_vectors_is_additive() {
    let old = valid();
    let mut next = old.clone();
    next.version = 2;
    next.fields.push(field("added", FieldKind::Bool));
    next.vectors
        .push(vector("added", 2, Distance::Dot, VectorIndexSpec::Auto));
    assert_eq!(old.check_additive(&next), Ok(()));
    assert_eq!(old.check_additive(&old), Ok(()));
    // The field limit may shrink, but not below what the schema holds.
    next.max_fields = 13;
    assert_eq!(old.check_additive(&next), Ok(()));
    next.max_fields = 12;
    assert_incompatible(&old, &next);
}

#[test]
fn changing_a_field_is_incompatible() {
    let old = valid();
    let mut next = old.clone();
    next.fields[1].fast = false;
    assert_incompatible(&old, &next);
    let mut reordered = old.clone();
    reordered.fields.swap(0, 1);
    assert_incompatible(&old, &reordered);
    let mut vector_changed = old.clone();
    vector_changed.vectors[0].dim = 8;
    assert_incompatible(&old, &vector_changed);
}

#[test]
fn removing_a_vector_is_incompatible() {
    let old = valid();
    let mut next = old.clone();
    next.vectors.pop();
    assert_incompatible(&old, &next);
    let mut no_field = old.clone();
    no_field.fields.pop();
    assert_incompatible(&old, &no_field);
}

#[test]
fn annotations_and_dynamic_may_change() {
    let old = valid();
    let mut next = old.clone();
    next.dynamic = DynamicMapping::Map;
    next.annotations.clear();
    next.annotations
        .insert("operon.note".to_string(), "x".to_string());
    assert_eq!(old.check_additive(&next), Ok(()));
    assert!(!old.same_ignoring_version(&next));
    let mut bumped = old.clone();
    bumped.version = 7;
    assert!(old.same_ignoring_version(&bumped));
}

#[test]
fn adding_or_changing_a_sparse_vector_is_incompatible() {
    let old = valid();
    let mut added = old.clone();
    added.sparse_vectors.push(sparse("bm25"));
    assert_incompatible(&old, &added);
    let mut changed = old.clone();
    changed.sparse_vectors[0].modifier = SparseModifier::None;
    assert_incompatible(&old, &changed);
    let mut removed = old.clone();
    removed.sparse_vectors.clear();
    assert_incompatible(&old, &removed);
}

#[test]
fn schemas_round_trip_through_postcard() {
    let schema = valid();
    let bytes = postcard::to_stdvec(&schema).unwrap();
    let back: CollectionSchema = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(back, schema);
}
