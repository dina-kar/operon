//! SQL over the native API (plan M1.2 Task 11).

use reqwest::StatusCode;
use serde_json::json;

use crate::common::{Native, TOKEN, kb};

#[tokio::test]
async fn sql_over_http_returns_columns_and_rows() {
    let api = Native::start().await;
    let token = kb(&api, "w").await;
    // Fixture step 17 runs after one of the six documents is deleted.
    api.post(
        "/v1/namespaces/w/collections/kb/documents",
        json!({"ops": [{"delete": {"id": 2}}]}),
    )
    .await
    .expect(StatusCode::OK);
    let body = api
        .post(
            "/v1/namespaces/w/sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        body,
        json!({"columns": [{"name": "n", "type": "Int64"}], "rows": [[5]], "truncated": false})
    );
    // Typed columns, with a consistency token in the header.
    let body = api
        .post_with(
            "/v1/namespaces/w/sql",
            &[(TOKEN, &token)],
            json!({"query": "SELECT n, tenant FROM kb WHERE n IS NOT NULL ORDER BY n", "consistency": "eventual"}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["columns"][0]["name"], "n");
    assert_eq!(body["rows"], json!([[1, "a"], [3, "b"]]));
    api.shutdown().await;
}

#[tokio::test]
async fn sql_ddl_is_400() {
    let api = Native::start().await;
    kb(&api, "w").await;
    for statement in [
        "CREATE TABLE t (x INT)",
        "DROP TABLE kb",
        "INSERT INTO kb (n) VALUES (1)",
    ] {
        let body = api
            .post("/v1/namespaces/w/sql", json!({"query": statement}))
            .await
            .expect(StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_argument", "{statement}: {body}");
    }
    api.shutdown().await;
}
