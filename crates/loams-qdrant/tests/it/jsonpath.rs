//! Qdrant's payload key paths (plan M1.4 Task 4).

use loams_qdrant::GatewayError;
use loams_qdrant::jsonpath::{
    JsonPath, PathItem, select_exclude, select_include, value_remove, value_set,
};
use serde_json::{Map, Value, json};

fn path(s: &str) -> JsonPath {
    s.parse().unwrap_or_else(|e| panic!("{s}: {e}"))
}

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().cloned().expect("an object")
}

#[test]
fn paths_parse_like_qdrant() {
    assert_eq!(
        path("a.b[].c"),
        JsonPath {
            first: "a".into(),
            rest: vec![
                PathItem::Key("b".into()),
                PathItem::Wildcard,
                PathItem::Key("c".into())
            ],
        }
    );
    assert_eq!(
        path("a[0]"),
        JsonPath {
            first: "a".into(),
            rest: vec![PathItem::Index(0)],
        }
    );
    assert_eq!(
        path("\"k.x\".y"),
        JsonPath {
            first: "k.x".into(),
            rest: vec![PathItem::Key("y".into())],
        }
    );
    assert_eq!(
        path("foo[1][50].bar-baz[].\"qux[.]quux\"").to_string(),
        "foo[1][50].bar-baz[].\"qux[.]quux\""
    );
    for bad in ["", "a.", ".a", "a[x]", "a[", "a b", "\"a", "a..b"] {
        let err = bad.parse::<JsonPath>().expect_err(bad);
        assert_eq!(
            err.to_string(),
            format!("Format error in JSON body: Invalid json path: '{bad}'")
        );
    }
}

#[test]
fn normalized_drops_wildcards_and_rejects_indexes() {
    assert_eq!(path("a.b[].c").normalized().unwrap(), "a.b.c");
    assert_eq!(path("a[]").normalized().unwrap(), "a");
    assert_eq!(path("\"k\".y").normalized().unwrap(), "k.y");
    for unsupported in ["a[0]", "a.b[3].c", "\"k.x\".y", "a.\"b.c\""] {
        assert!(
            matches!(
                path(unsupported).normalized(),
                Err(GatewayError::Unsupported(_))
            ),
            "{unsupported}"
        );
    }
}

#[test]
fn value_get_does_not_descend_arrays_without_a_wildcard() {
    let p = obj(json!({"a": [{"b": 1}, {"b": 2}], "c": {"d": [3, 4]}}));
    assert!(path("a.b").value_get(&p).is_empty());
    assert_eq!(path("a[].b").value_get(&p), vec![&json!(1), &json!(2)]);
    assert_eq!(path("a[1].b").value_get(&p), vec![&json!(2)]);
    assert_eq!(path("c.d").value_get(&p), vec![&json!([3, 4])]);
}

#[test]
fn include_keeps_paths_and_ancestors() {
    let p = obj(json!({"a": {"b": 1, "c": 2}, "d": 3}));
    assert_eq!(
        Value::Object(select_include(&p, &[path("a.b")])),
        json!({"a": {"b": 1}})
    );
    assert_eq!(
        Value::Object(select_include(&p, &[path("a")])),
        json!({"a": {"b": 1, "c": 2}})
    );
}

#[test]
fn exclude_removes_exactly_the_subtree() {
    let p = obj(json!({"a": {"b": 1, "c": 2}, "d": 3}));
    let out = select_exclude(&p, &[path("a.b")]);
    assert_eq!(Value::Object(out.clone()), json!({"a": {"c": 2}, "d": 3}));
    // Key order is kept (E5).
    assert_eq!(out.keys().collect::<Vec<_>>(), ["a", "d"]);
}

#[test]
fn include_through_arrays_needs_wildcard() {
    let p = obj(json!({"a": [{"b": 1, "c": 2}]}));
    assert_eq!(
        Value::Object(select_include(&p, &[path("a[].b")])),
        json!({"a": [{"b": 1}]})
    );
    assert_eq!(
        Value::Object(select_include(&p, &[path("a.b")])),
        json!({"a": []})
    );
}

#[test]
fn value_set_merges_at_path_top_level() {
    let mut dest = obj(json!({"a": {"b": {"x": {"z": 2}, "w": 3}}}));
    value_set(Some(&path("a.b")), &mut dest, &obj(json!({"x": {"y": 1}})));
    assert_eq!(
        Value::Object(dest),
        json!({"a": {"b": {"x": {"y": 1}, "w": 3}}})
    );
}

#[test]
fn value_set_creates_missing_objects() {
    let mut dest = obj(json!({"k": 1, "a": 5}));
    value_set(Some(&path("a.b.c")), &mut dest, &obj(json!({"x": 1})));
    assert_eq!(
        Value::Object(dest),
        json!({"k": 1, "a": {"b": {"c": {"x": 1}}}})
    );

    let mut dest = obj(json!({"k": 1}));
    value_set(Some(&path("n")), &mut dest, &obj(json!({"x": 1})));
    assert_eq!(Value::Object(dest), json!({"k": 1, "n": {"x": 1}}));

    // No path: a top-level merge, where `null` removes the key.
    let mut dest = obj(json!({"k": 1, "m": 2, "z": 3}));
    value_set(None, &mut dest, &obj(json!({"m": null, "k": 9, "new": 0})));
    assert_eq!(
        Value::Object(dest.clone()),
        json!({"k": 9, "z": 3, "new": 0})
    );
    assert_eq!(dest.keys().collect::<Vec<_>>(), ["k", "z", "new"]);

    // Through a wildcard: every element.
    let mut dest = obj(json!({"a": [{"q": 1}, {"q": 2}]}));
    value_set(Some(&path("a[]")), &mut dest, &obj(json!({"x": 0})));
    assert_eq!(
        Value::Object(dest),
        json!({"a": [{"q": 1, "x": 0}, {"q": 2, "x": 0}]})
    );
}

#[test]
fn value_remove_through_wildcard() {
    let mut dest = obj(json!({"a": [{"b": 1, "c": 2}, {"b": 3}], "d": 4}));
    value_remove(&path("a[].b"), &mut dest);
    assert_eq!(
        Value::Object(dest.clone()),
        json!({"a": [{"c": 2}, {}], "d": 4})
    );

    value_remove(&path("a[]"), &mut dest);
    assert_eq!(Value::Object(dest.clone()), json!({"a": [], "d": 4}));

    // A trailing index removes nothing (not idempotent in Qdrant).
    let mut dest = obj(json!({"a": [1, 2]}));
    value_remove(&path("a[0]"), &mut dest);
    assert_eq!(Value::Object(dest.clone()), json!({"a": [1, 2]}));

    let mut dest = obj(json!({"x": 1, "a": 2, "y": 3}));
    value_remove(&path("a"), &mut dest);
    assert_eq!(dest.keys().collect::<Vec<_>>(), ["x", "y"]);
}
