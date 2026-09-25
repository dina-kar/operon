//! Value extraction, coercion, date parsing and document validation.

mod common;

use std::collections::BTreeMap;

use common::{doc, field, json, obj, patch, schema, sparse, text, vector};
use operon_collection::{
    CollectionSchema, DocOp, DocRejection, DynamicMapping, ExtractedDoc, FieldKind, IndexValue,
    PatchMode, PrimaryKey, SparseModifier, SparseVectorSpec, Violation, check_document,
    check_patch, coerce, extract, parse_date, unmapped_paths,
};
use serde_json::{Value, json};

fn extracted(source: &Value, path: &str) -> Vec<Value> {
    extract(&obj(source.clone()), path)
        .into_iter()
        .map(|value| value.into_owned())
        .collect()
}

fn violation(field: &str, message: &str) -> Violation {
    Violation {
        field: field.to_string(),
        message: message.to_string(),
    }
}

fn violations(list: &[(&str, &str)]) -> DocRejection {
    DocRejection::Violations(list.iter().map(|(f, m)| violation(f, m)).collect())
}

fn pk() -> PrimaryKey {
    PrimaryKey::U64(1)
}

// ---------------------------------------------------------------------------
// Extraction

#[test]
fn extract_follows_dotted_keys_and_nested_objects() {
    let source = json!({
        "a": {"b": 1, "c": {"d": "deep"}},
        "a.b": 2,
        "x.y": {"z": true},
        "top": "t",
    });
    assert_eq!(extracted(&source, "a.b"), [json!(1), json!(2)]);
    assert_eq!(extracted(&source, "a.c.d"), [json!("deep")]);
    assert_eq!(extracted(&source, "x.y.z"), [json!(true)]);
    assert_eq!(extracted(&source, "top"), [json!("t")]);
    // A path that ends in an object gives the object; a missing path nothing.
    assert_eq!(extracted(&source, "a.c"), [json!({"d": "deep"})]);
    assert!(extracted(&source, "a.missing").is_empty());
    assert!(extracted(&source, "top.more").is_empty());
    // Nulls are extracted and coerce to nothing.
    assert_eq!(extracted(&json!({"n": null}), "n"), [Value::Null]);
}

#[test]
fn extract_flattens_arrays_of_objects() {
    let source = json!({"a": [{"b": 1}, {"b": [2, 3]}]});
    assert_eq!(extracted(&source, "a.b"), [json!(1), json!(2), json!(3)]);
    // Nested arrays are flattened at the end of the path too.
    let nested = json!({"t": [["x", ["y"]], "z"], "e": []});
    assert_eq!(
        extracted(&nested, "t"),
        [json!("x"), json!("y"), json!("z")]
    );
    assert!(extracted(&nested, "e").is_empty());
}

#[test]
fn extract_of_the_empty_path_is_the_source() {
    let source = json!({"a": 1, "b": {"c": [2]}});
    assert_eq!(extracted(&source, ""), [source]);
}

// ---------------------------------------------------------------------------
// Coercion

fn kind_text() -> FieldKind {
    FieldKind::Text {
        analyzer: "standard".to_string(),
        positions: true,
    }
}

#[test]
fn coercion_follows_es_rules() {
    const UUID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    let uuid_bytes = *uuid::Uuid::parse_str(UUID).unwrap().as_bytes();
    let ok = |v: IndexValue| Ok(Some(v));
    let bad = || Err(());
    // (kind, input, expected): per kind a valid, a coercible and a malformed
    // input, then null, an object and a Json field.
    let cases: Vec<(FieldKind, Value, Result<Option<IndexValue>, ()>)> = vec![
        (
            kind_text(),
            json!("hello"),
            ok(IndexValue::Text("hello".into())),
        ),
        (kind_text(), json!(1.5), ok(IndexValue::Text("1.5".into()))),
        (kind_text(), json!(["a"]), bad()),
        (
            FieldKind::Keyword,
            json!("k"),
            ok(IndexValue::Keyword("k".into())),
        ),
        (
            FieldKind::Keyword,
            json!(true),
            ok(IndexValue::Keyword("true".into())),
        ),
        (FieldKind::Keyword, json!({"k": 1}), bad()),
        (FieldKind::I64, json!(-42), ok(IndexValue::I64(-42))),
        (FieldKind::I64, json!("17"), ok(IndexValue::I64(17))),
        (FieldKind::I64, json!("abc"), bad()),
        (FieldKind::F64, json!(2.25), ok(IndexValue::F64(2.25))),
        (FieldKind::F64, json!("1e3"), ok(IndexValue::F64(1000.0))),
        (FieldKind::F64, json!("NaN"), bad()),
        (FieldKind::Bool, json!(false), ok(IndexValue::Bool(false))),
        (FieldKind::Bool, json!("true"), ok(IndexValue::Bool(true))),
        (FieldKind::Bool, json!(1), bad()),
        (
            FieldKind::Date,
            json!("2024-01-02"),
            ok(IndexValue::Date(1_704_153_600_000)),
        ),
        (
            FieldKind::Date,
            json!("1704153600000"),
            ok(IndexValue::Date(1_704_153_600_000)),
        ),
        (FieldKind::Date, json!("yesterday"), bad()),
        (
            FieldKind::Uuid,
            json!(UUID),
            ok(IndexValue::Uuid(uuid_bytes)),
        ),
        (
            FieldKind::Uuid,
            json!(UUID.replace('-', "").to_uppercase()),
            ok(IndexValue::Uuid(uuid_bytes)),
        ),
        (FieldKind::Uuid, json!("not-a-uuid"), bad()),
        (FieldKind::I64, Value::Null, Ok(None)),
        (FieldKind::I64, json!(3.5), ok(IndexValue::I64(3))),
        (FieldKind::Json, json!({"a": 1}), bad()),
    ];
    assert_eq!(cases.len(), 24);
    for (kind, input, expected) in cases {
        let got = coerce(&kind, &input).map_err(|message| {
            assert!(!message.is_empty(), "{kind:?} {input}: empty message");
        });
        assert_eq!(got, expected, "{kind:?} {input}");
    }
    // ES 8 `coerce: true`: fractions are truncated toward zero (controller
    // ruling P27); values out of range and non-numeric strings are refused.
    for (input, expected) in [
        (json!(3.0), 3),
        (json!(-3.7), -3),
        (json!("1.0"), 1),
        (json!("3.5"), 3),
        (json!("-0.5"), 0),
    ] {
        assert_eq!(
            coerce(&FieldKind::I64, &input),
            Ok(Some(IndexValue::I64(expected))),
            "{input}"
        );
    }
    for input in [
        json!(u64::MAX),
        json!(1e19),
        json!(-1e19),
        json!("9223372036854775808"),
        json!("1e19"),
        json!("NaN"),
        json!("inf"),
        json!(""),
        json!("3 apples"),
    ] {
        assert!(coerce(&FieldKind::I64, &input).is_err(), "{input}");
    }
    assert_eq!(
        coerce(&FieldKind::Bool, &json!("")),
        Ok(Some(IndexValue::Bool(false)))
    );
    for input in [json!("yes"), json!("TRUE"), json!(0)] {
        assert!(coerce(&FieldKind::Bool, &input).is_err(), "{input}");
    }
    assert_eq!(
        coerce(&FieldKind::I64, &json!(i64::MIN)),
        Ok(Some(IndexValue::I64(i64::MIN)))
    );
    for input in [json!("inf"), json!("-infinity"), json!("x"), json!([1.0])] {
        assert!(coerce(&FieldKind::F64, &input).is_err(), "{input}");
    }
}

#[test]
fn ignore_malformed_skips_instead_of_rejecting() {
    let strict = field("n", FieldKind::I64);
    let lenient = operon_collection::FieldSpec {
        ignore_malformed: true,
        ..field("m", FieldKind::I64)
    };
    // Ignore: the object inside `m` also has an unmapped leaf, `m.o`.
    let schema = schema(vec![strict, lenient], DynamicMapping::Ignore);
    let good = doc(pk(), json!({"n": 1, "m": ["x", 2, {"o": 1}, 3]}));
    assert_eq!(
        check_document(&schema, &good),
        Ok(ExtractedDoc {
            values: vec![
                (0, vec![IndexValue::I64(1)]),
                (1, vec![IndexValue::I64(2), IndexValue::I64(3)]),
            ],
        })
    );
    let bad = doc(pk(), json!({"n": "x", "m": "y"}));
    let Err(DocRejection::Violations(found)) = check_document(&schema, &bad) else {
        panic!("expected a violation");
    };
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].field, "n");
}

// ---------------------------------------------------------------------------
// Dates

#[test]
fn dates_parse_in_every_accepted_format() {
    let cases = [
        (json!(1_704_153_600_000_i64), 1_704_153_600_000),
        (json!("1704153600000"), 1_704_153_600_000),
        (json!("2024-01-02T03:04:05Z"), 1_704_164_645_000),
        (json!("2024-01-02T03:04:05.123+02:00"), 1_704_157_445_123),
        (json!("2024-01-02T03:04:05.5"), 1_704_164_645_500),
        (json!("2024-01-02"), 1_704_153_600_000),
        (json!("2024/01/02 03:04:05"), 1_704_164_645_000),
        (json!("2024/01/02"), 1_704_153_600_000),
    ];
    for (input, millis) in cases {
        assert_eq!(parse_date(&input), Ok(millis), "{input}");
    }
    assert_eq!(parse_date(&json!(-1)), Ok(-1));
    assert_eq!(parse_date(&json!("1969-12-31")), Ok(-86_400_000));
    for input in [
        json!("2024-1-2"),
        json!("2024-01-02T03:04"),
        json!("02/01/2024"),
        json!("2024-13-01"),
        json!(" 2024-01-02"),
        json!("2024-01-02 03:04:05"),
        json!("99999999999999999999"),
        json!(""),
        json!(1.5),
        json!(u64::MAX),
        json!(true),
        json!({"d": 1}),
    ] {
        assert!(parse_date(&input).is_err(), "{input}");
    }
}

/// Tantivy stores a date as i64 nanoseconds, so only epoch milliseconds in
/// `i64::MIN / 10^6 ..= i64::MAX / 10^6` (1677-09-21 ..= 2262-04-11) are
/// dates; anything beyond is malformed, never wrapped.
#[test]
fn a_date_beyond_tantivys_range_is_malformed() {
    const MAX: i64 = i64::MAX / 1_000_000;
    const MIN: i64 = i64::MIN / 1_000_000;
    for millis in [MIN, -1, 0, MAX] {
        assert_eq!(parse_date(&json!(millis)), Ok(millis), "{millis}");
    }
    assert_eq!(parse_date(&json!(MAX.to_string())), Ok(MAX));
    assert_eq!(parse_date(&json!("2262-04-11")), Ok(9_223_286_400_000));
    assert_eq!(parse_date(&json!("1677-09-22")), Ok(-9_223_286_400_000));
    for input in [
        json!(MAX + 1),
        json!(MIN - 1),
        json!(i64::MAX),
        json!(i64::MIN),
        json!((MAX + 1).to_string()),
        json!("2262-04-12"),
        json!("1677-09-21"),
        json!("9999-12-31T23:59:59Z"),
        json!("0001-01-01"),
    ] {
        assert!(parse_date(&input).is_err(), "{input}");
    }

    let mut date = field("d", FieldKind::Date);
    assert!(coerce(&date.kind, &json!("9999-01-01")).is_err());
    let d = doc(pk(), json!({"d": ["9999-01-01", "2024-01-02"]}));
    let found = check_document(&schema(vec![date.clone()], DynamicMapping::Strict), &d);
    assert!(
        matches!(found, Err(DocRejection::Violations(_))),
        "{found:?}"
    );
    date.ignore_malformed = true;
    let found = check_document(&schema(vec![date], DynamicMapping::Strict), &d).unwrap();
    assert_eq!(
        found.values,
        [(0, vec![IndexValue::Date(1_704_153_600_000)])]
    );
}

// ---------------------------------------------------------------------------
// Unmapped paths and dynamic modes

#[test]
fn strict_mapping_rejects_unmapped_paths() {
    let schema = schema(vec![text("title")], DynamicMapping::Strict);
    let d = doc(
        pk(),
        json!({"title": "t", "extra": 1, "o": {"k": [null, "v"]}, "n": null, "e": [], "empty": {}}),
    );
    assert_eq!(unmapped_paths(&schema, &d.source), ["extra", "o.k"]);
    assert_eq!(
        check_document(&schema, &d),
        Err(violations(&[
            ("extra", "strict dynamic mapping: extra is not mapped"),
            ("o.k", "strict dynamic mapping: o.k is not mapped"),
        ]))
    );
    assert!(check_document(&schema, &doc(pk(), json!({"title": "t"}))).is_ok());
}

#[test]
fn ignore_mapping_keeps_unmapped_paths_in_source() {
    let schema = schema(vec![text("title")], DynamicMapping::Ignore);
    let d = doc(pk(), json!({"title": "t", "extra": {"k": 1}}));
    assert_eq!(unmapped_paths(&schema, &d.source), ["extra.k"]);
    assert_eq!(
        check_document(&schema, &d),
        Ok(ExtractedDoc {
            values: vec![(0, vec![IndexValue::Text("t".into())])],
        })
    );
}

#[test]
fn map_mode_reports_dynamic_mapping_required() {
    let schema = schema(vec![text("title")], DynamicMapping::Map);
    let d = doc(
        pk(),
        json!({"title": "t", "z": 1, "a": {"b": true}, "_meta": 1}),
    );
    // `_meta` cannot be a field name, so it stays in `_source` only.
    assert_eq!(
        check_document(&schema, &d),
        Err(DocRejection::DynamicMappingRequired {
            paths: vec!["a.b".to_string(), "z".to_string()],
        })
    );
    // A patch brings its own paths.
    assert_eq!(
        check_patch(&schema, &patch(pk(), json!({"title": "u", "q": "x"}))),
        Err(DocRejection::DynamicMappingRequired {
            paths: vec!["q".to_string()],
        })
    );
    // A violation wins over a mapping request.
    let typed = common::schema(vec![field("n", FieldKind::I64)], DynamicMapping::Map);
    assert_eq!(
        check_document(&typed, &doc(pk(), json!({"n": "x", "z": 1}))),
        Err(violations(&[("n", "cannot index \"x\" as i64")]))
    );
}

#[test]
fn a_json_field_covers_its_subtree() {
    let schema = schema(vec![json("meta", "meta")], DynamicMapping::Strict);
    let d = doc(
        pk(),
        json!({"meta": {"a": 1, "b": {"c": "x"}}, "metadata": 1}),
    );
    assert_eq!(unmapped_paths(&schema, &d.source), ["metadata"]);
    let d = doc(
        pk(),
        json!({"meta": {"a": 1, "b": [{"c": "x"}]}, "meta.d": 2}),
    );
    assert!(unmapped_paths(&schema, &d.source).is_empty());
    // Json fields are not typed: nothing is extracted for them.
    assert_eq!(
        check_document(&schema, &d),
        Ok(ExtractedDoc { values: vec![] })
    );
}

#[test]
fn the_catch_all_json_field_covers_everything() {
    let schema = schema(vec![json("payload", "")], DynamicMapping::Strict);
    let d = doc(pk(), json!({"a": 1, "b": {"c": [true, "x"]}, "_meta": 1}));
    assert!(unmapped_paths(&schema, &d.source).is_empty());
    assert!(check_document(&schema, &d).is_ok());
}

// ---------------------------------------------------------------------------
// Vectors

fn vector_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(vec![], vec![vector("v", 3)], DynamicMapping::Ignore)
        .with_sparse_vectors(vec![SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::None,
        }]);
    schema.validate().expect("valid schema");
    schema
}

#[test]
fn vectors_with_the_wrong_dimension_are_violations() {
    let schema = vector_schema();
    let mut d = doc(pk(), json!({}));
    d.vectors.insert("v".to_string(), vec![1.0, 2.0]);
    assert_eq!(
        check_document(&schema, &d),
        Err(violations(&[("vectors.v", "expected 3 dimensions, got 2")]))
    );
    d.vectors.insert("v".to_string(), vec![1.0, f32::NAN, 2.0]);
    let Err(DocRejection::Violations(found)) = check_document(&schema, &d) else {
        panic!("a non-finite value is a violation");
    };
    assert_eq!(found[0].field, "vectors.v");
    // A correct vector, and a missing one, are fine.
    d.vectors.insert("v".to_string(), vec![1.0, 2.0, 3.0]);
    assert!(check_document(&schema, &d).is_ok());
    assert!(check_document(&schema, &doc(pk(), json!({}))).is_ok());
    // A patch's vectors are checked the same way; a deletion is not.
    let mut p = patch(pk(), json!({}));
    if let DocOp::Patch { vectors, .. } = &mut p {
        vectors.insert("v".to_string(), Some(vec![1.0]));
        vectors.insert("gone".to_string(), None);
    }
    assert_eq!(
        check_patch(&schema, &p),
        Err(violations(&[("vectors.v", "expected 3 dimensions, got 1")]))
    );
}

#[test]
fn unknown_vectors_are_violations() {
    let schema = vector_schema();
    let mut d = doc(pk(), json!({}));
    d.vectors.insert("w".to_string(), vec![1.0]);
    assert_eq!(
        check_document(&schema, &d),
        Err(violations(&[("vectors.w", "unknown vector")]))
    );
    // A dense name is not a sparse one.
    let mut d = doc(pk(), json!({}));
    d.vectors.insert("s".to_string(), vec![1.0, 2.0, 3.0]);
    assert_eq!(
        check_document(&schema, &d),
        Err(violations(&[("vectors.s", "unknown vector")]))
    );
}

#[test]
fn unknown_sparse_vectors_are_violations() {
    let schema = vector_schema();
    let mut d = doc(pk(), json!({}));
    d.sparse_vectors
        .insert("x".to_string(), sparse(&[1], &[0.5]));
    d.sparse_vectors
        .insert("s".to_string(), sparse(&[1], &[0.5]));
    assert_eq!(
        check_document(&schema, &d),
        Err(violations(&[("sparse_vectors.x", "unknown sparse vector")]))
    );
    let mut p = patch(pk(), json!({}));
    if let DocOp::Patch { sparse_vectors, .. } = &mut p {
        sparse_vectors.insert("v".to_string(), Some(sparse(&[2], &[1.0])));
    }
    assert_eq!(
        check_patch(&schema, &p),
        Err(violations(&[("sparse_vectors.v", "unknown sparse vector")]))
    );
    // The patch's upsert document is checked in full.
    let mut upsert = doc(pk(), json!({}));
    upsert
        .sparse_vectors
        .insert("y".to_string(), sparse(&[], &[]));
    let p = DocOp::Patch {
        pk: pk(),
        mode: PatchMode::MergeTop,
        source: obj(json!({})),
        delete_keys: vec![],
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: Some(upsert),
    };
    assert_eq!(
        check_patch(&schema, &p),
        Err(violations(&[("sparse_vectors.y", "unknown sparse vector")]))
    );
}

#[test]
fn an_empty_sparse_vector_is_valid() {
    let schema = vector_schema();
    let mut d = doc(pk(), json!({}));
    d.sparse_vectors.insert("s".to_string(), sparse(&[], &[]));
    assert!(check_document(&schema, &d).is_ok());
    let mut p = patch(pk(), json!({}));
    if let DocOp::Patch { sparse_vectors, .. } = &mut p {
        sparse_vectors.insert("s".to_string(), Some(sparse(&[], &[])));
    }
    assert_eq!(check_patch(&schema, &p), Ok(()));
}

#[test]
fn patch_delete_keys_must_be_paths() {
    let schema = vector_schema();
    for bad in ["", "a..b", ".a", "a."] {
        let mut p = patch(pk(), json!({}));
        if let DocOp::Patch { delete_keys, .. } = &mut p {
            delete_keys.push("ok.path".to_string());
            delete_keys.push(bad.to_string());
        }
        let Err(DocRejection::Violations(found)) = check_patch(&schema, &p) else {
            panic!("{bad:?} is not a path");
        };
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].field, "delete_keys");
    }
    // A delete needs nothing but its key.
    assert_eq!(check_patch(&schema, &DocOp::Delete(pk())), Ok(()));
}
