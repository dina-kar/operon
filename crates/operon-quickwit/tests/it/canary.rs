//! Vendored Quickwit code relies on `serde_json::Map` being a sorted map. This fails if
//! feature unification ever enables `serde_json/preserve_order` in the workspace.

use serde_json::{Map, Value};

#[test]
fn serde_json_maps_are_sorted() {
    let map: Map<String, Value> = serde_json::from_str(r#"{"b":1,"a":2}"#).unwrap();
    let keys: Vec<&str> = map.keys().map(String::as_str).collect();
    assert_eq!(keys, vec!["a", "b"]);
}
