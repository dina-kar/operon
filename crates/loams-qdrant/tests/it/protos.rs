//! The vendored Qdrant protos and the generated stubs (plan M1.4 Task 1;
//! Ruling 2).

use loams_qdrant::convert::value::{json_to_value, value_to_json};
use loams_qdrant::proto;
use serde_json::json;

/// crc32c of each vendored file: a change to a proto must change this
/// table (and re-copy all nine files from one Qdrant tag).
const PINNED: [(&str, u32); 9] = [
    ("collections.proto", 0xa92169a6),
    ("collections_service.proto", 0x58e97db5),
    ("health_check.proto", 0xe4c774e8),
    ("json_with_int.proto", 0xfc9a352f),
    ("points.proto", 0xd3ca8e5d),
    ("points_service.proto", 0x1e149ed9),
    ("qdrant.proto", 0x7b40fe4b),
    ("qdrant_common.proto", 0xef98714f),
    ("snapshots_service.proto", 0x0cc40352),
];

#[test]
fn vendored_protos_are_pinned() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .expect("proto dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let pinned: Vec<&str> = PINNED.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, pinned, "exactly the nine vendored files");
    let mut actual = Vec::new();
    for (name, _) in PINNED {
        let bytes = std::fs::read(dir.join(name)).expect(name);
        actual.push((name, crc32c::crc32c(&bytes)));
    }
    assert_eq!(actual, PINNED, "a vendored proto changed");
}

#[test]
fn generated_services_exist() {
    fn named<T>() -> &'static str {
        std::any::type_name::<T>()
    }
    struct Unused;
    let names = [
        named::<proto::qdrant::points_server::PointsServer<Unused>>(),
        named::<proto::qdrant::collections_server::CollectionsServer<Unused>>(),
        named::<proto::qdrant::snapshots_server::SnapshotsServer<Unused>>(),
        named::<proto::qdrant::qdrant_server::QdrantServer<Unused>>(),
        named::<proto::health::health_server::HealthServer<Unused>>(),
    ];
    assert!(
        names.iter().all(|name| name.contains("Server")),
        "{names:?}"
    );
}

#[test]
fn grpc_values_round_trip_json() {
    let input = json!({"i": 3, "f": 1.5, "big": 18446744073709551615u64, "n": null, "o": {"a": [1, "x", true]}});
    let back = value_to_json(&json_to_value(&input));
    let mut expected = input.clone();
    // Over i64, so it travels as a double.
    expected["big"] = json!(18446744073709551615u64 as f64);
    assert_eq!(back, expected);
    assert!(back["big"].is_f64());
    assert!(back["i"].is_i64());
    assert!(back["f"].is_f64());
}
