//! Task 6: `_source` filtering (ES `XContentMapValues.filter`).

use loams_es::Params;
use loams_es::doc::{SourceFilter, glob_match, glob_prefix_alive};
use serde_json::{Map, Value, json};

fn object(value: Value) -> Map<String, Value> {
    value.as_object().cloned().expect("object")
}

fn filter(includes: &[&str], excludes: &[&str]) -> SourceFilter {
    SourceFilter {
        enabled: true,
        includes: includes.iter().map(|s| s.to_string()).collect(),
        excludes: excludes.iter().map(|s| s.to_string()).collect(),
    }
}

fn apply(f: &SourceFilter, source: Value) -> Value {
    Value::Object(f.apply(object(source)))
}

fn doc() -> Value {
    json!({"metadata": {"page": 1, "src": "a"}, "text": "t", "vector": [1]})
}

#[test]
fn includes_keep_whole_subtrees() {
    let f = filter(&["metadata", "text"], &[]);
    assert_eq!(
        apply(&f, doc()),
        json!({"metadata": {"page": 1, "src": "a"}, "text": "t"})
    );
}

#[test]
fn wildcards_cross_dots() {
    let f = filter(&["meta*"], &[]);
    assert_eq!(
        apply(&f, doc()),
        json!({"metadata": {"page": 1, "src": "a"}})
    );
    let f = filter(&[], &["*.src"]);
    assert_eq!(
        apply(&f, doc()),
        json!({"metadata": {"page": 1}, "text": "t", "vector": [1]})
    );
    let f = filter(&["*.page"], &[]);
    assert_eq!(apply(&f, doc()), json!({"metadata": {"page": 1}}));
}

#[test]
fn leaf_includes_keep_only_the_leaf() {
    let f = filter(&["metadata.page"], &[]);
    assert_eq!(apply(&f, doc()), json!({"metadata": {"page": 1}}));
}

#[test]
fn arrays_of_objects_are_filtered_per_element() {
    let f = filter(&["a.x"], &[]);
    assert_eq!(
        apply(&f, json!({"a": [{"x": 1, "y": 2}, {"x": 3}]})),
        json!({"a": [{"x": 1}, {"x": 3}]})
    );
    // An element left empty is dropped, and so is an array left empty.
    let f = filter(&["a.y"], &[]);
    assert_eq!(
        apply(
            &f,
            json!({"a": [{"x": 1, "y": 2}, {"x": 3}], "b": [{"x": 1}]})
        ),
        json!({"a": [{"y": 2}]})
    );
    // Scalars of an array follow their key.
    let f = filter(&["tags"], &[]);
    assert_eq!(
        apply(&f, json!({"tags": ["a", "b"], "n": [1]})),
        json!({"tags": ["a", "b"]})
    );
}

#[test]
fn excludes_win_over_includes() {
    let f = filter(&["metadata"], &["metadata.src"]);
    assert_eq!(apply(&f, doc()), json!({"metadata": {"page": 1}}));
    let f = filter(&["metadata.page"], &["metadata"]);
    assert_eq!(apply(&f, doc()), json!({}));
    let f = filter(&["*"], &["vector"]);
    assert_eq!(
        apply(&f, doc()),
        json!({"metadata": {"page": 1, "src": "a"}, "text": "t"})
    );
}

#[test]
fn an_object_matching_nothing_is_dropped() {
    let f = filter(&["metadata.nope", "text"], &[]);
    assert_eq!(apply(&f, doc()), json!({"text": "t"}));
    // An object an include names itself is kept, even when empty.
    let f = filter(&["e"], &[]);
    assert_eq!(apply(&f, json!({"e": {}, "f": {}})), json!({"e": {}}));
    // An object the excludes empty is dropped, even when an include matched
    // it; one that was empty already stays (ES 8.19, row T11-3).
    let f = filter(&["metadata"], &["metadata.*"]);
    assert_eq!(apply(&f, doc()), json!({}));
    let doc = json!({"a": {"b": 1}, "e": {}, "arr": [], "m": {"n": {"o": 1}}, "x": 1});
    assert_eq!(
        apply(&filter(&[], &["a.b"]), doc.clone()),
        json!({"e": {}, "arr": [], "m": {"n": {"o": 1}}, "x": 1})
    );
    assert_eq!(apply(&filter(&["m"], &["m.n.o"]), doc.clone()), json!({}));
    assert_eq!(
        apply(&filter(&["e", "arr", "x"], &[]), doc.clone()),
        json!({"e": {}, "arr": [], "x": 1})
    );
    assert_eq!(apply(&filter(&["a", "x"], &["a.b"]), doc), json!({"x": 1}));
}

#[test]
fn dotted_keys_are_matched_by_their_full_path() {
    let f = filter(&["metadata.page"], &[]);
    assert_eq!(
        apply(
            &f,
            json!({"metadata.page": 2, "metadata": {"page": 1, "x": 0}})
        ),
        json!({"metadata.page": 2, "metadata": {"page": 1}})
    );
}

#[test]
fn key_order_is_kept() {
    let f = filter(&[], &["b"]);
    let out = apply(&f, json!({"z": 1, "b": 2, "a": 3}));
    assert_eq!(out.to_string(), r#"{"z":1,"a":3}"#);
}

#[test]
fn excluded_vectors_are_not_fetched() {
    let f = filter(&["text"], &[]);
    assert!(!f.keeps_path("vector"));
    assert!(f.keeps_path("text"));
    let f = filter(&["metadata"], &[]);
    assert!(f.keeps_path("metadata.emb"));
    assert!(!f.keeps_path("metadataemb"));
    let f = filter(&[], &["vec*"]);
    assert!(!f.keeps_path("vector"));
    assert!(f.keeps_path("text"));
    let f = filter(&[], &["a"]);
    assert!(!f.keeps_path("a.v"));
    let off = SourceFilter {
        enabled: false,
        ..filter(&[], &[])
    };
    assert!(!off.keeps_path("text"));
}

#[test]
fn globs_match_across_dots() {
    assert!(glob_match("*", "a.b"));
    assert!(glob_match("a*c", "a.b.c"));
    assert!(glob_match("*.src", "metadata.src"));
    assert!(!glob_match("*.src", "metadata.srcx"));
    assert!(glob_match("a**b", "ab"));
    assert!(!glob_match("a", "ab"));
    assert!(glob_match("", ""));
    assert!(glob_prefix_alive("metadata.page", "metadata."));
    assert!(glob_prefix_alive("*.page", "anything."));
    assert!(!glob_prefix_alive("metadata.page", "text."));
    assert!(glob_prefix_alive("a*z", "abc"));
    assert!(!glob_prefix_alive("ab", "abc"));
}

fn params(query: &str) -> Params {
    Params::parse(
        Some(query),
        "/i/_doc/1",
        &["_source", "_source_includes", "_source_excludes"],
    )
    .expect("params")
}

#[test]
fn parameters_follow_es() {
    assert_eq!(
        SourceFilter::from_params(&Params::default()).expect("none"),
        None
    );
    let f = SourceFilter::from_params(&params("_source=false&_source_includes=a"))
        .expect("parse")
        .expect("some");
    assert!(!f.enabled);
    let f = SourceFilter::from_params(&params("_source=true"))
        .expect("parse")
        .expect("some");
    assert_eq!(f, filter(&[], &[]));
    let f = SourceFilter::from_params(&params("_source=a,b&_source_excludes=c"))
        .expect("parse")
        .expect("some");
    assert_eq!(f, filter(&["a", "b"], &["c"]));
    // `_source_includes` replaces the `_source` list.
    let f = SourceFilter::from_params(&params("_source=a&_source_includes=x"))
        .expect("parse")
        .expect("some");
    assert_eq!(f, filter(&["x"], &[]));
}

#[test]
fn body_forms_follow_es() {
    let parse = |v: Value| SourceFilter::from_body(&v).expect("parse");
    assert_eq!(parse(json!(true)), filter(&[], &[]));
    assert!(!parse(json!(false)).enabled);
    assert_eq!(parse(json!("a")), filter(&["a"], &[]));
    assert_eq!(parse(json!(["a", "b"])), filter(&["a", "b"], &[]));
    assert_eq!(
        parse(json!({"includes": ["a"], "excludes": "b"})),
        filter(&["a"], &["b"])
    );
    assert_eq!(
        parse(json!({"include": "a", "exclude": ["b"]})),
        filter(&["a"], &["b"])
    );
    let e = SourceFilter::from_body(&json!(1)).expect_err("number");
    assert_eq!(e.status, 400);
    let e = SourceFilter::from_body(&json!({"nope": 1})).expect_err("key");
    assert_eq!(e.status, 400);
}
