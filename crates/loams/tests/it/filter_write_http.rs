//! The native filter-write routes (plan M1.5 Task 9a rule 6, D87).

use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{Native, TOKEN, kb};

const KB: &str = "/v1/namespaces/w/collections/kb";

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

fn tenant(value: &str) -> Value {
    json!({"term": {"field": "tenant", "value": value}})
}

async fn count(api: &Native, filter: Value, token: &str) -> u64 {
    api.post_with(
        &format!("{KB}/documents/count"),
        &[(TOKEN, token)],
        json!({"filter": filter}),
    )
    .await
    .expect(StatusCode::OK)["count"]
        .as_u64()
        .expect("a count")
}

async fn source(api: &Native, id: u64) -> Value {
    let body = api
        .post(&format!("{KB}/documents/get"), json!({"ids": [id]}))
        .await
        .expect(StatusCode::OK);
    body["documents"][0]["source"].clone()
}

#[tokio::test]
async fn delete_by_filter_route_speaks_the_documented_json() {
    let api = Native::start().await;
    kb(&api, "w").await;
    // A partial call: one of the three `c` documents, and a cursor.
    let reply = api
        .post(
            &format!("{KB}/documents/delete_by_filter"),
            json!({"filter": tenant("c"), "max_rows": 1, "allow_partial": true}),
        )
        .await;
    let header = reply.header(TOKEN).expect("token header").to_string();
    let first = reply.expect(StatusCode::OK);
    assert_eq!(
        keys(&first),
        [
            "affected",
            "batches",
            "cursor",
            "matched",
            "pin",
            "rows_remaining",
            "token",
            "written"
        ]
    );
    assert_eq!(first["token"], header.as_str());
    assert_eq!(
        (&first["matched"], &first["affected"], &first["batches"]),
        (&json!(3), &json!(1), &json!(1))
    );
    assert_eq!(first["rows_remaining"], true);
    assert_eq!(keys(&first["pin"]), ["manifest_version", "token"]);
    assert_eq!(
        keys(&first["cursor"]),
        ["after", "manifest_version", "token"]
    );
    assert_eq!(first["cursor"]["token"], first["pin"]["token"]);
    // The cursor finishes the write at the same pin.
    let rest = api
        .post(
            &format!("{KB}/documents/delete_by_filter"),
            json!({"filter": tenant("c"), "cursor": first["cursor"]}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        (&rest["matched"], &rest["affected"]),
        (&json!(3), &json!(2))
    );
    assert_eq!(rest["rows_remaining"], false);
    assert_eq!(rest["cursor"], Value::Null);
    assert_eq!(rest["pin"], first["pin"]);
    let token = rest["token"].as_str().expect("a token");
    assert_eq!(count(&api, tenant("c"), token).await, 0);
    assert_eq!(count(&api, json!("match_all"), token).await, 3);

    // A single-target alias names its collection.
    api.post(
        "/v1/namespaces/w/aliases",
        json!({"actions": [{"create": {"alias": "kb_live", "collection": "kb"}}]}),
    )
    .await
    .expect(StatusCode::OK);
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb_live/documents/delete_by_filter",
            json!({"filter": tenant("b")}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["affected"], 1);
    // Unknown keys are refused.
    let reply = api
        .post(
            &format!("{KB}/documents/delete_by_filter"),
            json!({"filter": tenant("a"), "patch": {}}),
        )
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_by_filter_route_applies_the_patch() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let body = api
        .post(
            &format!("{KB}/documents/patch_by_filter"),
            json!({
                "filter": tenant("a"),
                "patch": {"mode": "merge_deep", "source": {"extra": {"x": 1}}, "delete_keys": ["n"]}
            }),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        (&body["matched"], &body["affected"]),
        (&json!(2), &json!(2))
    );
    assert_eq!(
        source(&api, 1).await,
        json!({"body": "refund policy", "tenant": "a", "extra": {"x": 1}})
    );
    assert_eq!(
        source(&api, 3).await,
        json!({"body": "refund window", "tenant": "b", "n": 3})
    );
    // `null` removes a vector; an unknown mode and a patch without its
    // object are refused.
    let body = api
        .post(
            &format!("{KB}/documents/patch_by_filter"),
            json!({"filter": tenant("b"), "patch": {"vectors": {"embedding": null}}}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["affected"], 1);
    for bad in [
        json!({"filter": tenant("a"), "patch": {"mode": "other"}}),
        json!({"filter": tenant("a")}),
        json!({"filter": tenant("a"), "patch": {"upsert": {}}}),
    ] {
        let reply = api
            .post(&format!("{KB}/documents/patch_by_filter"), bad.clone())
            .await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{bad}");
    }
}

#[tokio::test]
async fn over_the_limit_is_400_with_matched_and_limit() {
    let api = Native::start().await;
    let token = kb(&api, "w").await;
    let body = api
        .post(
            &format!("{KB}/documents/delete_by_filter"),
            json!({"filter": tenant("c"), "max_rows": 2}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    assert_eq!((&body["matched"], &body["limit"]), (&json!(3), &json!(2)));
    assert_eq!(count(&api, tenant("c"), &token).await, 3);
    // `max_rows: 0` is refused as well, without the extras.
    let body = api
        .post(
            &format!("{KB}/documents/delete_by_filter"),
            json!({"filter": tenant("c"), "max_rows": 0, "allow_partial": true}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    assert!(body.get("matched").is_none(), "{body}");
}

#[tokio::test]
async fn the_token_header_covers_the_filter_write() {
    let api = Native::start().await;
    let written = kb(&api, "w").await;
    let reply = api
        .post_with(
            &format!("{KB}/documents/delete_by_filter"),
            &[(TOKEN, written.as_str()), ("Loams-Backpressure", "off")],
            json!({"filter": tenant("a")}),
        )
        .await;
    let token = reply.header(TOKEN).expect("token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert_eq!(body["affected"], 2);
    // A read at the token sees the deletions, and every earlier write.
    assert_eq!(count(&api, tenant("a"), &token).await, 0);
    assert_eq!(count(&api, json!("match_all"), &token).await, 4);
    // A bad header is 400.
    let reply = api
        .post_with(
            &format!("{KB}/documents/delete_by_filter"),
            &[("Loams-Backpressure", "maybe")],
            json!({"filter": tenant("b")}),
        )
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let reply = api
        .post_with(
            &format!("{KB}/documents/delete_by_filter"),
            &[(TOKEN, "not a token")],
            json!({"filter": tenant("b")}),
        )
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
}
