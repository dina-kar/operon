//! The scan plan route over the native API (plan M1.2 Task 14, D53).

use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{Native, TOKEN, kb};

const PLAN_KEYS: [&str; 17] = [
    "collection",
    "collection_id",
    "columns",
    "durable_token",
    "expires_at_ms",
    "fragments",
    "lance",
    "live_rows",
    "manifest_version",
    "namespace",
    "offsets",
    "pin",
    "pk_encoding",
    "planned_at_ms",
    "schema_version",
    "tail",
    "tail_records",
];

fn keys(value: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

#[tokio::test]
async fn scan_route_speaks_the_documented_json() {
    let api = Native::start().await;
    kb(&api, "w").await;
    api.post(
        "/v1/namespaces/w/aliases",
        json!({"actions": [{"create": {"alias": "kb-live", "collection": "kb"}}]}),
    )
    .await
    .expect(StatusCode::OK);
    // Wait until the link applied every write.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = api
            .get("/v1/namespaces/w/collections/kb")
            .await
            .expect(StatusCode::OK);
        if info["link_lag_records"] == 0 && info["manifest_version"].as_u64() > Some(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the link never caught up: {info}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let reply = api
        .post("/v1/namespaces/w/collections/kb-live/scan", json!({}))
        .await;
    let header = reply.header(TOKEN).map(str::to_string);
    let plan = reply.expect(StatusCode::OK);
    assert_eq!(keys(&plan), PLAN_KEYS, "{plan}");
    assert_eq!(plan["namespace"], "w");
    assert_eq!(plan["collection"], "kb");
    assert_eq!(plan["pk_encoding"], "operon_canonical_v1");
    assert_eq!(plan["tail"], false);
    assert_eq!(plan["live_rows"], 6);
    assert_eq!(plan["durable_token"], plan["pin"]["token"]);
    assert_eq!(header.as_deref(), plan["pin"]["token"].as_str());
    assert_eq!(plan["offsets"].as_array().map(Vec::len), Some(2));
    let lance = &plan["lance"];
    assert_eq!(
        keys(lance),
        [
            "manifest_path",
            "stable_row_ids",
            "storage_format",
            "uri",
            "version"
        ]
    );
    assert!(lance["version"].is_u64(), "{lance}");
    let bucket = url::Url::from_directory_path(
        api.data_dir()
            .join("bucket")
            .canonicalize()
            .expect("the bucket directory"),
    )
    .expect("a file URL")
    .to_string();
    let uri = lance["uri"].as_str().expect("a uri");
    assert!(uri.starts_with(&bucket), "{uri} under {bucket}");
    assert!(
        uri.ends_with(&format!("/collections/{}/lance", plan["collection_id"])),
        "{uri}"
    );
    let fragment = &plan["fragments"][0];
    assert_eq!(
        keys(fragment),
        [
            "deleted_rows",
            "deletion_file",
            "files",
            "id",
            "lance",
            "live_rows",
            "physical_rows"
        ]
    );
    assert_eq!(plan["columns"][0]["role"], "pk");
    assert_eq!(plan["columns"][4]["vector"], "embedding");

    // A manifest version and an explicit current plan the same state.
    let version = plan["manifest_version"].clone();
    let pinned = api
        .post(
            "/v1/namespaces/w/collections/kb/scan",
            json!({"at": {"manifest_version": version}}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(pinned["fragments"], plan["fragments"]);
    let current = api
        .post(
            "/v1/namespaces/w/collections/kb/scan",
            json!({"at": "current"}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(current["lance"], plan["lance"]);

    // Tags arrive in M2; unknown forms and keys are refused.
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/scan",
            json!({"at": {"tag": "x"}}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument", "{body}");
    assert_eq!(
        body["message"], "invalid argument: tags arrive in M2 (D52)",
        "{body}"
    );
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/scan",
            json!({"at": {"manifest": 1}}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(
        body["message"],
        r#"invalid argument: unknown scan point {"manifest":1}"#
    );
    api.post(
        "/v1/namespaces/w/collections/kb/scan",
        json!({"when": "current"}),
    )
    .await
    .expect(StatusCode::BAD_REQUEST);

    // An unknown collection and a gone manifest are 404.
    let body = api
        .post("/v1/namespaces/w/collections/nope/scan", json!({}))
        .await
        .expect(StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found", "{body}");
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/scan",
            json!({"at": {"manifest_version": 1_000_000}}),
        )
        .await
        .expect(StatusCode::NOT_FOUND);
    assert_eq!(body["kind"], "pin", "{body}");
    api.shutdown().await;
}
