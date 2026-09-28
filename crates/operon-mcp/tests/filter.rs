//! The flat MCP filter (plan M1.6 Task 8 rule 2, Ruling 8).

use operon_mcp::filter::{field_value, translate_filter};
use operon_query::{FieldValue, Query};
use serde_json::{Map, Value, json};

fn filter(value: Value) -> Map<String, Value> {
    value.as_object().expect("object").clone()
}

fn one(value: Value) -> Query {
    translate_filter(&filter(value))
        .expect("translates")
        .expect("a clause")
}

fn term(field: &str, value: FieldValue) -> Query {
    Query::Term {
        field: field.into(),
        value,
    }
}

#[test]
fn a_scalar_is_a_term() {
    assert_eq!(
        one(json!({"tenant": "a"})),
        term("tenant", FieldValue::Str("a".into()))
    );
    assert_eq!(one(json!({"n": 3})), term("n", FieldValue::I64(3)));
    assert_eq!(one(json!({"ok": true})), term("ok", FieldValue::Bool(true)));
}

#[test]
fn a_list_is_terms() {
    assert_eq!(
        one(json!({"tenant": ["a", "b"]})),
        Query::Terms {
            field: "tenant".into(),
            values: vec![FieldValue::Str("a".into()), FieldValue::Str("b".into())],
        }
    );
}

#[test]
fn an_object_of_bounds_is_a_range() {
    assert_eq!(
        one(json!({"n": {"gte": 2, "lt": 5.5}})),
        Query::Range {
            field: "n".into(),
            gt: None,
            gte: Some(FieldValue::I64(2)),
            lt: Some(FieldValue::F64(5.5)),
            lte: None,
        }
    );
    assert_eq!(
        one(json!({"d": {"gt": "2026-01-01", "lte": "2026-12-31"}})),
        Query::Range {
            field: "d".into(),
            gt: Some(FieldValue::Str("2026-01-01".into())),
            gte: None,
            lt: None,
            lte: Some(FieldValue::Str("2026-12-31".into())),
        }
    );
}

#[test]
fn exists_true_and_false() {
    let exists = Query::Exists {
        field: "tags".into(),
    };
    assert_eq!(one(json!({"tags": {"exists": true}})), exists);
    assert_eq!(
        one(json!({"tags": {"exists": false}})),
        Query::Bool {
            must: vec![],
            should: vec![],
            must_not: vec![exists],
            filter: vec![],
            minimum_should_match: None,
        }
    );
}

#[test]
fn null_is_is_null() {
    assert_eq!(
        one(json!({"author": null})),
        Query::IsNull {
            field: "author".into()
        }
    );
}

#[test]
fn several_entries_are_anded_in_key_order() {
    assert_eq!(
        one(json!({"b": 1, "a": "x"})),
        Query::Bool {
            must: vec![],
            should: vec![],
            must_not: vec![],
            filter: vec![
                term("a", FieldValue::Str("x".into())),
                term("b", FieldValue::I64(1)),
            ],
            minimum_should_match: None,
        }
    );
}

#[test]
fn no_entries_is_no_filter() {
    assert_eq!(translate_filter(&Map::new()).expect("ok"), None);
}

#[test]
fn an_empty_list_or_unknown_object_is_rejected() {
    for bad in [
        json!({"tags": []}),
        json!({"tags": [["nested"]]}),
        json!({"tags": [{"a": 1}]}),
        json!({"tags": [null]}),
        json!({"n": {}}),
        json!({"n": {"between": [1, 2]}}),
        json!({"n": {"gte": 1, "exists": true}}),
        json!({"n": {"gte": [1]}}),
        json!({"n": {"exists": "yes"}}),
    ] {
        let err = translate_filter(&filter(bad.clone())).expect_err(&bad.to_string());
        assert_eq!(err.code, "invalid_argument", "{bad}");
        assert!(err.message.starts_with("filter on `"), "{}", err.message);
    }
}

#[test]
fn integers_floats_and_bools_map_to_field_values() {
    assert_eq!(field_value(&json!(1)).unwrap(), FieldValue::I64(1));
    assert_eq!(field_value(&json!(-7)).unwrap(), FieldValue::I64(-7));
    assert_eq!(field_value(&json!(1.5)).unwrap(), FieldValue::F64(1.5));
    // Above i64::MAX is a u64 (row E18), as the native API reads it.
    assert_eq!(
        field_value(&json!(9223372036854775808u64)).unwrap(),
        FieldValue::U64(9_223_372_036_854_775_808)
    );
    assert_eq!(field_value(&json!(true)).unwrap(), FieldValue::Bool(true));
    assert_eq!(
        field_value(&json!("x")).unwrap(),
        FieldValue::Str("x".into())
    );
    assert_eq!(
        field_value(&json!(null)).unwrap_err().code,
        "invalid_argument"
    );
    assert_eq!(
        field_value(&json!([1])).unwrap_err().code,
        "invalid_argument"
    );
}
