//! M1.6 Task 10 (D92): the `performance` block and `Server-Timing` on the
//! native query and SQL responses.

use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{Native, Reply, kb};

/// `Server-Timing`'s `total`, `plan` and `exec` durations, in that order.
fn server_timing(reply: &Reply) -> Vec<(String, f64)> {
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let header = reply
        .header("server-timing")
        .expect("a Server-Timing header");
    header
        .split(',')
        .map(|metric| {
            let (name, dur) = metric
                .trim()
                .split_once(";dur=")
                .unwrap_or_else(|| panic!("a metric with a duration: {header}"));
            (
                name.to_string(),
                dur.parse().unwrap_or_else(|_| panic!("a number: {header}")),
            )
        })
        .collect()
}

fn names(timing: &[(String, f64)]) -> Vec<&str> {
    timing.iter().map(|(name, _)| name.as_str()).collect()
}

fn ms(value: &Value) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| panic!("a number: {value}"))
}

#[tokio::test]
async fn query_response_carries_performance_and_server_timing() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let reply = api
        .post(
            "/v1/namespaces/w/query",
            json!({
                "collection": "kb",
                "retrievers": [
                    {"text": {"query": {"match": {"field": "body", "text": "refund"}}, "k": 10}},
                    {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}}
                ],
                "fusion": {"rrf": {"k": 60}},
                "limit": 5
            }),
        )
        .await;
    let timing = server_timing(&reply);
    let body = reply.expect(StatusCode::OK);
    let p = &body["performance"];
    for key in ["server_total_ms", "queue_ms", "planning_ms", "execution_ms"] {
        assert!(ms(&p[key]) >= 0.0, "{key}: {p}");
    }
    assert!(
        ms(&p["server_total_ms"]) >= ms(&p["planning_ms"]) + ms(&p["execution_ms"]),
        "{p}"
    );
    // The worker applies the link as it likes: at most the six writes.
    assert!(p["tail_records"].as_u64().is_some_and(|n| n <= 6), "{p}");
    assert!(p["manifest_version"].is_u64(), "{p}");
    assert_eq!(p["stale_records"], Value::Null, "a strong read: {p}");
    let kinds: Vec<&str> = p["rows_scanned"]
        .as_array()
        .expect("rows_scanned")
        .iter()
        .map(|rows| rows["kind"].as_str().expect("kind"))
        .collect();
    assert_eq!(kinds, ["text", "vector"], "{p}");
    for key in ["hit_bytes", "miss_bytes", "hit_ratio"] {
        assert!(p["cache"].get(key).is_some(), "{key}: {p}");
    }
    for key in ["get", "head", "list"] {
        assert!(p["object_store_requests"][key].is_u64(), "{key}: {p}");
    }
    assert_eq!(names(&timing), ["total", "plan", "exec"]);
    // The header rounds the block's timings to three decimals.
    for ((_, dur), key) in timing
        .iter()
        .zip(["server_total_ms", "planning_ms", "execution_ms"])
    {
        assert!((dur - ms(&p[key])).abs() < 0.001, "{key}: {dur} vs {p}");
    }

    // An eventual read reports its staleness.
    let body = api
        .post(
            "/v1/namespaces/w/query",
            json!({"collection": "kb", "consistency": "eventual"}),
        )
        .await
        .expect(StatusCode::OK);
    assert!(body["performance"]["stale_records"].is_u64(), "{body}");
    api.shutdown().await;
}

#[tokio::test]
async fn sql_response_carries_the_timings() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let reply = api
        .post(
            "/v1/namespaces/w/sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await;
    let timing = server_timing(&reply);
    let body = reply.expect(StatusCode::OK);
    assert_eq!(body["rows"], json!([[6]]));
    let p = body["performance"].as_object().expect("performance");
    let mut keys: Vec<&str> = p.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["execution_ms", "planning_ms", "queue_ms", "server_total_ms"],
        "the four timings only"
    );
    assert!(
        ms(&p["planning_ms"]) > 0.0 && ms(&p["execution_ms"]) > 0.0,
        "{body}"
    );
    assert!(
        ms(&p["server_total_ms"]) >= ms(&p["planning_ms"]) + ms(&p["execution_ms"]),
        "{body}"
    );
    assert_eq!(ms(&p["queue_ms"]), 0.0);
    assert_eq!(names(&timing), ["total", "plan", "exec"]);
    api.shutdown().await;
}
