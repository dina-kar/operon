//! Task 1: the HTTP plumbing, the error envelope, the info endpoints, index
//! expressions and the server wiring of the Elasticsearch gateway.

use loams_es::names::{
    IndexExpr, ResolveOptions, Resolved, WriteTarget, resolve, resolve_single, resolve_write,
};
use loams_query::AliasTargetAction;
use reqwest::{Method, StatusCode};
use serde_json::json;

use crate::harness::Es;

const PRODUCT: &str = "Elasticsearch";

#[tokio::test]
async fn root_returns_es_8_info_with_product_header() {
    let es = Es::start().await;
    let a = es.get("/").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    assert_eq!(a.header("content-type"), "application/json");
    assert_eq!(a.body["version"]["number"], "8.19.0");
    assert_eq!(a.body["version"]["build_flavor"], "default");
    assert_eq!(a.body["tagline"], "You Know, for Search");
    assert_eq!(a.body["name"], "loams");
    assert_eq!(a.body["cluster_name"], "loams");
    let a = es.head("/").await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    assert_eq!(a.header("content-length"), "0");
    assert!(a.text.is_empty());
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn errors_and_head_carry_the_product_header() {
    let es = Es::start().await;
    let a = es.head("/missing").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND);
    assert!(a.text.is_empty(), "{}", a.text);
    assert_eq!(a.header("content-length"), "0");
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    let a = es.get("/missing/_doc/1").await;
    a.assert_error(
        404,
        "index_not_found_exception",
        Some("no such index [missing]"),
    );
    assert_eq!(a.body["error"]["index"], "missing");
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    // ES's shape for an unknown route: a string `error`, no `status`
    // (checked against the 8.19 oracle, row T11-3).
    let a = es.get("/nope/_x/y/z?p=1").await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST, "{}", a.text);
    assert_eq!(
        a.body,
        json!({"error": "no handler found for uri [/nope/_x/y/z?p=1] and method [GET]"})
    );
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_wrong_method_is_405_with_the_allowed_list() {
    let es = Es::start().await;
    let a = es.send(Method::DELETE, "/_license", None, &[]).await;
    assert_eq!(a.status, StatusCode::METHOD_NOT_ALLOWED, "{}", a.text);
    assert_eq!(
        a.body,
        json!({
            "error": "Incorrect HTTP method for uri [/_license] and method [DELETE], allowed: [GET]",
            "status": 405,
        })
    );
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    // ES's order, and `HEAD` only where ES registers it (row T11-3).
    Es::ok(es.put("/i", None).await);
    for (method, path, allowed) in [
        (Method::PATCH, "/i", "GET, PUT, DELETE, HEAD"),
        (Method::PATCH, "/i/_doc/1", "GET, POST, PUT, DELETE, HEAD"),
        (Method::DELETE, "/i/_mapping", "GET, POST, PUT"),
        (Method::DELETE, "/i/_search", "GET, POST"),
    ] {
        let a = es.send(method.clone(), path, None, &[]).await;
        assert_eq!(
            a.body["error"],
            format!(
                "Incorrect HTTP method for uri [{path}] and method [{method}], allowed: [{allowed}]"
            ),
            "{}",
            a.text
        );
    }
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn compat_media_types_are_accepted_and_echoed() {
    let es = Es::start().await;
    let compat = "application/vnd.elasticsearch+json; compatible-with=8";
    let a = es
        .send(
            Method::POST,
            "/_ml/trained_models/m/_infer",
            Some(json!({"docs": []})),
            &[("accept", compat)],
        )
        .await;
    assert!(
        a.header("content-type")
            .starts_with("application/vnd.elasticsearch+json"),
        "{:?}",
        a.headers
    );
    let a = es
        .send_raw(
            Method::GET,
            "/",
            Some((compat, b"{}".to_vec())),
            &[("accept", compat)],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(
        a.header("content-type"),
        "application/vnd.elasticsearch+json;compatible-with=8"
    );
    let v9 = "application/vnd.elasticsearch+json; compatible-with=9";
    let a = es.send(Method::GET, "/", None, &[("accept", v9)]).await;
    a.assert_error(400, "media_type_header_exception", None);
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_body_of_another_media_type_is_406() {
    let es = Es::start().await;
    let a = es
        .send_raw(
            Method::POST,
            "/_ml/trained_models/m/_infer",
            Some(("text/plain", b"{}".to_vec())),
            &[],
        )
        .await;
    assert_eq!(a.status, StatusCode::NOT_ACCEPTABLE, "{}", a.text);
    assert_eq!(
        a.body,
        json!({"error": "Content-Type header [text/plain] is not supported", "status": 406})
    );
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn authorization_headers_are_ignored() {
    let es = Es::start().await;
    let a = es
        .send(
            Method::GET,
            "/",
            None,
            &[("authorization", "Basic ZWxhc3RpYzpjaGFuZ2VtZQ==")],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn license_is_active_enterprise() {
    let es = Es::start().await;
    let a = es.get("/_license").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.body["license"]["type"], "enterprise");
    assert_eq!(a.body["license"]["status"], "active");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cluster_health_is_green() {
    let es = Es::start().await;
    let a = es
        .get("/_cluster/health?wait_for_status=yellow&timeout=1s")
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.body["status"], "green");
    assert_eq!(a.body["timed_out"], false);
    assert_eq!(a.body["active_primary_shards"], 0);
    es.create_raw("default", "a", Some(2)).await;
    es.create_raw("default", "b", Some(3)).await;
    let a = es.get("/_cluster/health").await;
    assert_eq!(a.body["active_primary_shards"], 5, "{}", a.text);
    assert_eq!(a.body["active_shards"], 5);
    let a = es.get("/_cluster/health/b").await;
    assert_eq!(a.body["active_primary_shards"], 3, "{}", a.text);
    // A missing index: 408 red, as ES answers when its wait times out
    // (O-M15-7, row T11-3).
    let a = es.get("/_cluster/health/missing?timeout=1s").await;
    assert_eq!(a.status.as_u16(), 408, "{}", a.text);
    assert_eq!(a.body["status"], "red");
    assert_eq!(a.body["timed_out"], true);
    assert_eq!(a.body["active_primary_shards"], 0);
    assert_eq!(a.body["unassigned_primary_shards"], 0);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn trained_model_infer_is_404() {
    let es = Es::start().await;
    let a = es
        .post(
            "/_ml/trained_models/non-existing%20model%20ID/_infer",
            json!({"docs": []}),
        )
        .await;
    a.assert_error(
        404,
        "resource_not_found_exception",
        Some("Could not find trained model [non-existing model ID]"),
    );
    let a = es
        .post(
            "/_ml/trained_models/m/deployment/_infer",
            json!({"docs": []}),
        )
        .await;
    a.assert_error(404, "resource_not_found_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn unknown_parameters_are_refused() {
    let es = Es::start().await;
    let a = es.get("/?foo=1").await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("request [/] contains unrecognized parameter: [foo]"),
    );
    let a = es.get("/?filter_path=version").await;
    a.assert_error(400, "illegal_argument_exception", None);
    let a = es.get("/?pretty").await;
    assert_eq!(a.status, StatusCode::OK);
    assert!(a.text.contains('\n'), "{}", a.text);
    let a = es.get("/?human&error_trace=true").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert!(!a.text.contains('\n'), "{}", a.text);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_hot_header_is_honoured() {
    let es = Es::start().await;
    es.create_raw("default", "i", None).await;
    let a = es
        .send(
            Method::GET,
            "/_cluster/health/i",
            None,
            &[("loams-hot", "off")],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.header("loams-hot-used"), "none");
    let a = es
        .send(
            Method::GET,
            "/_cluster/health/i",
            None,
            &[("loams-hot", "maybe")],
        )
        .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("invalid Loams-Hot header [maybe] (expected on or off)"),
    );
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    // `/i/_search` (Task 9): the search itself runs without hot structures.
    let a = es
        .send(Method::GET, "/i/_search", None, &[("loams-hot", "off")])
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.header("loams-hot-used"), "none");
    let a = es
        .send(Method::GET, "/i/_search", None, &[("loams-hot", "maybe")])
        .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("invalid Loams-Hot header [maybe] (expected on or off)"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_namespace_header_selects_the_namespace() {
    let es = Es::start().await;
    es.create_raw("tenant1", "x", None).await;
    let a = es.get("/_cluster/health/x").await;
    assert_eq!(a.status.as_u16(), 408, "{}", a.text);
    let a = es.head("/x").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND);
    let tenant = [("loams-namespace", "tenant1")];
    let a = es
        .send(Method::GET, "/_cluster/health/x", None, &tenant)
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.body["active_primary_shards"], 4);
    // `HEAD /x` (Task 3) sees the index only in its namespace.
    let a = es.send(Method::HEAD, "/x", None, &tenant).await;
    assert_eq!(a.status, StatusCode::OK);
    assert!(a.text.is_empty(), "{}", a.text);
    let a = es
        .send(
            Method::GET,
            "/",
            None,
            &[("loams-consistency-token", "junk")],
        )
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_body_over_the_limit_is_413() {
    let es = Es::start_with(|config| {
        if let Some(es) = config.es.as_mut() {
            es.max_body_bytes = 1024;
        }
    })
    .await;
    let line = "{\"index\":{\"_index\":\"i\"}}\n{\"field\":\"value\"}\n";
    let body = line.repeat(2048 / line.len() + 1).into_bytes();
    let a = es
        .send_raw(
            Method::POST,
            "/_bulk",
            Some(("application/x-ndjson", body.clone())),
            &[],
        )
        .await;
    a.assert_error(
        413,
        "content_too_long_exception",
        Some(&format!(
            "entity content is too long [{}] for the configured buffer limit [1024]",
            body.len()
        )),
    );
    assert_eq!(a.header("x-elastic-product"), PRODUCT);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn index_expressions_resolve_names_aliases_and_wildcards() {
    let es = Es::start().await;
    let ns = "default";
    for name in ["logs-a", "logs-b", "other"] {
        es.create_raw(ns, name, Some(1)).await;
    }
    let service = es.server.collections();
    let add = |alias: &str, collection: &str, is_write_index| AliasTargetAction::Add {
        alias: alias.to_string(),
        collection: collection.to_string(),
        is_write_index,
    };
    service
        .update_alias_targets(
            ns,
            vec![
                add("both", "logs-a", None),
                add("both", "logs-b", None),
                add("writer", "logs-a", None),
                add("writer", "other", Some(true)),
                add("single", "other", None),
            ],
        )
        .await
        .expect("aliases");
    let names = |resolved: Vec<Resolved>| {
        resolved
            .into_iter()
            .map(|r| (r.name, r.via_alias))
            .collect::<Vec<_>>()
    };
    let opts = ResolveOptions::default();
    let got = resolve(&service, ns, &IndexExpr::parse("other,both"), opts)
        .await
        .expect("resolve");
    assert_eq!(
        names(got),
        vec![
            ("logs-a".to_string(), Some("both".to_string())),
            ("logs-b".to_string(), Some("both".to_string())),
            ("other".to_string(), None),
        ]
    );
    let got = resolve(&service, ns, &IndexExpr::parse("logs-*"), opts)
        .await
        .expect("wildcard");
    assert_eq!(names(got).len(), 2);
    let got = resolve(&service, ns, &IndexExpr::All, opts)
        .await
        .expect("all");
    assert_eq!(names(got).len(), 3);
    let got = resolve(&service, ns, &IndexExpr::parse("nothing-*"), opts)
        .await
        .expect("allow_no_indices");
    assert!(got.is_empty());
    let strict = ResolveOptions {
        allow_no_indices: false,
        ..opts
    };
    let e = resolve(&service, ns, &IndexExpr::parse("nothing-*"), strict)
        .await
        .expect_err("no match");
    assert_eq!(e.kind, "index_not_found_exception");
    let e = resolve(&service, ns, &IndexExpr::parse("other,missing"), opts)
        .await
        .expect_err("missing");
    assert_eq!(e.reason, "no such index [missing]");
    let lenient = ResolveOptions {
        ignore_unavailable: true,
        ..opts
    };
    let got = resolve(&service, ns, &IndexExpr::parse("other,missing"), lenient)
        .await
        .expect("ignore_unavailable");
    assert_eq!(names(got), vec![("other".to_string(), None)]);

    // Writes (Ruling 9).
    assert_eq!(
        resolve_write(&service, ns, "writer").await.expect("writer"),
        Some(WriteTarget {
            name: "other".to_string(),
            via_alias: Some("writer".to_string()),
        })
    );
    assert_eq!(
        resolve_write(&service, ns, "single").await.expect("single"),
        Some(WriteTarget {
            name: "other".to_string(),
            via_alias: Some("single".to_string()),
        })
    );
    assert_eq!(
        resolve_write(&service, ns, "logs-a")
            .await
            .expect("index")
            .map(|t| t.name),
        Some("logs-a".to_string())
    );
    assert_eq!(
        resolve_write(&service, ns, "new-index")
            .await
            .expect("auto-create"),
        None
    );
    let e = resolve_write(&service, ns, "both")
        .await
        .expect_err("no write index");
    assert_eq!(e.kind, "illegal_argument_exception");
    assert!(
        e.reason
            .starts_with("no write index is defined for alias [both]."),
        "{}",
        e.reason
    );
    let e = resolve_write(&service, ns, "logs-a,other")
        .await
        .expect_err("a list");
    assert_eq!(e.kind, "invalid_index_name_exception");

    // Single-document reads (Ruling 9).
    let single = resolve_single(&service, ns, "single")
        .await
        .expect("single");
    assert_eq!(single.name, "other");
    let e = resolve_single(&service, ns, "both")
        .await
        .expect_err("two members");
    assert_eq!(
        e.reason,
        "alias [both] has more than one index associated with it [logs-a, logs-b], can't \
         execute a single index op"
    );
    let e = resolve_single(&service, ns, "missing")
        .await
        .expect_err("missing");
    assert_eq!(e.kind, "index_not_found_exception");
    es.server.shutdown().await.expect("shutdown");
}

#[test]
fn loams_dev_serves_es_on_its_own_port() {
    let dev = Es::dev(true);
    let addr = dev
        .addr("loams es listening on http://")
        .unwrap_or_else(|| panic!("no ES line in {:?}", dev.lines));
    let es_at = dev
        .lines
        .iter()
        .position(|l| l.starts_with("loams es listening on "));
    let http_at = dev
        .lines
        .iter()
        .position(|l| l.starts_with("loams listening on "));
    assert!(es_at < http_at, "{:?}", dev.lines);
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let response = runtime.block_on(async {
        reqwest::get(format!("http://{addr}/"))
            .await
            .expect("GET /")
    });
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-elastic-product")
            .and_then(|v| v.to_str().ok()),
        Some(PRODUCT)
    );
    drop(dev);
    let dev = Es::dev(false);
    assert!(
        dev.addr("loams es listening on ").is_none(),
        "{:?}",
        dev.lines
    );
}
