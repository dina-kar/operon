//! The workspace enables `serde_json/preserve_order` (qdrant-edge needs it, and
//! Cargo unifies features; M1.3 row E58), so every build has one map type:
//! `serde_json::Map` keeps insertion order. The vendored Quickwit code does not
//! rely on sorted maps (its tests pass with the feature on). This fails if the
//! feature is ever dropped, which would give `-p` builds a different map type.

use serde_json::{Map, Value};

#[test]
fn serde_json_maps_keep_insertion_order() {
    let map: Map<String, Value> = serde_json::from_str(r#"{"b":1,"a":2}"#).unwrap();
    let keys: Vec<&str> = map.keys().map(String::as_str).collect();
    assert_eq!(keys, vec!["b", "a"]);
}
