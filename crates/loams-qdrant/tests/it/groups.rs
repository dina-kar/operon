//! Group keys (plan M1.4 Task 9, "Grouping" in the protocol facts): string
//! and integer values, arrays flattened one level, and Qdrant's rule that
//! one value of another type voids the point.

use loams_qdrant::groups::{GroupKey, group_keys};
use loams_qdrant::jsonpath::JsonPath;
use serde_json::{Map, Value, json};

fn keys(payload: Value, path: &str) -> Vec<GroupKey> {
    let map: Map<String, Value> = serde_json::from_value(payload).expect("object");
    let path: JsonPath = path.parse().expect("path");
    group_keys(&map, &path)
}

fn s(v: &str) -> GroupKey {
    GroupKey::Str(v.to_string())
}

#[test]
fn group_keys_follow_qdrant() {
    assert_eq!(keys(json!({"d": "a"}), "d"), [s("a")]);
    assert_eq!(keys(json!({"d": -4}), "d"), [GroupKey::Int(-4)]);
    // Arrays give each element once, in order.
    assert_eq!(
        keys(json!({"d": ["b", 1, "b", 1]}), "d"),
        [s("b"), GroupKey::Int(1)]
    );
    // Nested paths.
    assert_eq!(keys(json!({"m": {"d": 3}}), "m.d"), [GroupKey::Int(3)]);
    assert_eq!(
        keys(json!({"m": [{"d": "x"}, {"d": "y"}]}), "m[].d"),
        [s("x"), s("y")]
    );
    // No key, or one value that is no key: the point joins no group.
    for payload in [
        json!({}),
        json!({"d": null}),
        json!({"d": 1.5}),
        json!({"d": 2.0}),
        json!({"d": true}),
        json!({"d": {"x": 1}}),
        json!({"d": []}),
        json!({"d": ["a", false]}),
        json!({"d": ["a", ["b"]]}),
    ] {
        assert!(keys(payload.clone(), "d").is_empty(), "{payload}");
    }
    // Integers order before strings.
    assert!(GroupKey::Int(i64::MAX) < s(""));
    assert_eq!(GroupKey::Int(7).to_json(), json!(7));
    assert_eq!(s("k").to_json(), json!("k"));
}
