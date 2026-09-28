//! The shared SDK wire fixtures (plan M1.6 Task 1): `sdks/fixtures/` pins the
//! native API's request and response shapes; this module runs them against
//! an in-process server, and both SDKs' test suites read the same files.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use operon_collection::ConsistencyToken;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::common::{Native, Reply, TOKEN};

const SCENARIO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../sdks/fixtures/scenario.json"
));
const QUERIES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../sdks/fixtures/queries.json"
));

/// The namespace the scenario runs in (`{ns}`).
const NS: &str = "wire";

/// Replaces `{ns}` in every string of `value`.
fn with_namespace(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace("{ns}", NS)),
        Value::Array(items) => Value::Array(items.iter().map(with_namespace).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), with_namespace(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Replaces every `{token:<step>}` in `header` with that step's token header.
fn with_tokens(header: &str, tokens: &BTreeMap<String, String>) -> String {
    let mut out = header.to_string();
    while let Some(start) = out.find("{token:") {
        let end = start + out[start..].find('}').expect("closing brace");
        let step = &out[start + "{token:".len()..end];
        let token = tokens
            .get(step)
            .unwrap_or_else(|| panic!("no token header recorded for step {step:?}"));
        out.replace_range(start..=end, token);
    }
    out
}

fn str_list(step: &Value, key: &str) -> Vec<String> {
    step[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} must be a list: {step}"))
        .iter()
        .map(|v| v.as_str().expect("a string").to_string())
        .collect()
}

/// Checks one step's reply against its `status`, `keys`, `header_token` and
/// `expect`; returns the failures.
fn check(step: &Value, reply: &Reply) -> Vec<String> {
    let mut failures = Vec::new();
    let statuses: Vec<u64> = step["status"]
        .as_array()
        .expect("status")
        .iter()
        .map(|s| s.as_u64().expect("a status"))
        .collect();
    if !statuses.contains(&u64::from(reply.status.as_u16())) {
        failures.push(format!(
            "status {} not in {statuses:?}",
            reply.status.as_u16()
        ));
    }
    for key in str_list(step, "keys") {
        if reply.body.get(&key).is_none() {
            failures.push(format!("missing key {key:?}"));
        }
    }
    if step["header_token"].as_bool().expect("header_token") {
        match reply.header(TOKEN) {
            None => failures.push("no Operon-Consistency-Token header".into()),
            Some(text) => {
                if let Err(e) = ConsistencyToken::from_str(text) {
                    failures.push(format!("token header {text:?} does not parse: {e}"));
                }
            }
        }
    }
    for entry in step["expect"].as_array().expect("expect") {
        let pointer = entry["pointer"].as_str().expect("pointer");
        let got = reply.body.pointer(pointer);
        if entry.get("absent") == Some(&Value::Bool(true))
            && let Some(got) = got
        {
            failures.push(format!("{pointer} should be absent, got {got}"));
        }
        if let Some(want) = entry.get("equals")
            && got != Some(want)
        {
            failures.push(format!("{pointer}: want {want}, got {got:?}"));
        }
        if let Some(options) = entry.get("one_of") {
            let options = options.as_array().expect("one_of");
            if !got.is_some_and(|got| options.contains(got)) {
                failures.push(format!("{pointer}: want one of {options:?}, got {got:?}"));
            }
        }
    }
    failures
}

#[test]
fn the_fixture_files_are_well_formed() {
    let scenario: Value = serde_json::from_str(SCENARIO).expect("scenario.json");
    assert_eq!(scenario["version"], 1);
    let steps = scenario["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 30);
    let names: BTreeSet<&str> = steps
        .iter()
        .map(|s| s["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names.len(), steps.len(), "step names are unique");
    let keys = [
        "name",
        "method",
        "path",
        "headers",
        "body",
        "status",
        "keys",
        "header_token",
        "expect",
    ];
    for step in steps {
        let got: BTreeSet<&str> = step
            .as_object()
            .expect("step")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(got, keys.into_iter().collect(), "{step}");
    }

    let queries: Value = serde_json::from_str(QUERIES).expect("queries.json");
    assert_eq!(queries["version"], 1);
    let names: Vec<&str> = queries["queries"]
        .as_array()
        .expect("queries")
        .iter()
        .map(|q| q["name"].as_str().expect("name"))
        .collect();
    assert_eq!(
        names,
        [
            "match_all",
            "match_none",
            "match",
            "match_phrase",
            "multi_match",
            "term",
            "terms",
            "range",
            "exists",
            "is_null",
            "is_empty",
            "values_count",
            "prefix",
            "wildcard",
            "fuzzy",
            "ids",
            "query_string",
            "bool",
            "boost",
            "constant_score",
            "match_fuzzy_auto",
            "range_dates",
        ]
    );
}

#[tokio::test]
async fn the_wire_scenario_passes_against_the_server() {
    let scenario: Value = serde_json::from_str(SCENARIO).expect("scenario.json");
    let api = Native::start().await;
    let mut tokens = BTreeMap::new();
    let mut bodies: BTreeMap<String, Value> = BTreeMap::new();
    let mut failures = Vec::new();
    for step in scenario["steps"].as_array().expect("steps") {
        let name = step["name"].as_str().expect("name");
        let method = Method::from_str(step["method"].as_str().expect("method")).expect("method");
        let path = step["path"].as_str().expect("path").replace("{ns}", NS);
        let headers: Vec<(String, String)> = step["headers"]
            .as_object()
            .expect("headers")
            .iter()
            .map(|(k, v)| (k.clone(), with_tokens(v.as_str().expect("header"), &tokens)))
            .collect();
        let body = match &step["body"] {
            Value::Null => None,
            body => Some(with_namespace(body)),
        };
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let reply = api
            .call(method.clone(), &path, &header_refs, body.clone())
            .await;
        let mut problems = check(step, &reply);
        // Step 7: an identical re-create answers the same collection id.
        if name == "create_collection_again" {
            let first = &bodies["create_collection"]["id"];
            if &reply.body["id"] != first {
                problems.push(format!(
                    "/id {} differs from step 6's {first}",
                    reply.body["id"]
                ));
            }
        }
        if !problems.is_empty() {
            failures.push(format!(
                "step {name}: {}\n  request: {method} {path} headers {headers:?} body {}\n  response: {} headers {:?} body {}",
                problems.join("; "),
                body.unwrap_or(Value::Null),
                reply.status,
                reply.headers,
                reply.body,
            ));
        }
        if let Some(token) = reply.header(TOKEN) {
            tokens.insert(name.to_string(), token.to_string());
        }
        bodies.insert(name.to_string(), reply.body);
    }
    api.shutdown().await;
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

/// Creates `qv` (a field of every kind the queries use) with two documents
/// in namespace `q`.
async fn qv(api: &Native) {
    let field = |name: &str, kind: Value| json!({"name": name, "source_path": name, "kind": kind, "indexed": true, "fast": false});
    let schema = json!({
        "fields": [
            field("title", json!({"text": {"analyzer": "standard", "positions": true}})),
            field("tag", json!("keyword")),
            field("n", json!("i64")),
            field("ts", json!("date")),
            field("flag", json!("bool")),
            field("meta", json!("json")),
        ],
        "vectors": [],
        "sparse_vectors": [],
        "dynamic": "strict",
        "max_fields": 1000
    });
    api.post(
        "/v1/namespaces/q/collections",
        json!({"name": "qv", "schema": schema}),
    )
    .await
    .expect(StatusCode::CREATED);
    let upsert = |id: Value, source: Value| json!({"upsert": {"id": id, "source": source, "vectors": {}, "sparse_vectors": {}}});
    api.post(
        "/v1/namespaces/q/collections/qv/documents",
        json!({"ops": [
            upsert(json!(1), json!({"title": "hello world", "tag": "a", "n": 1,
                                    "ts": "2026-03-01T12:00:00Z", "flag": true, "meta": {"k": "v"}})),
            upsert(json!("k-str"), json!({"title": "goodbye moon", "tag": "b", "n": 2,
                                          "ts": "2025-06-01T00:00:00Z", "flag": false, "meta": {"k": 2}})),
        ], "report_existence": false}),
    )
    .await
    .expect(StatusCode::OK);
}

/// Posts W12 over `qv` for every fixture query, built into a request by
/// `request`; returns the queries that were not answered 200.
async fn run_queries(api: &Native, request: impl Fn(&Value) -> Value) -> Vec<String> {
    let queries: Value = serde_json::from_str(QUERIES).expect("queries.json");
    let mut failures = Vec::new();
    for entry in queries["queries"].as_array().expect("queries") {
        let body = request(&entry["query"]);
        let reply = api.post("/v1/namespaces/q/query", body.clone()).await;
        if reply.status != StatusCode::OK {
            failures.push(format!(
                "{}: {} {}\n  request: {body}",
                entry["name"], reply.status, reply.body
            ));
        }
    }
    failures
}

fn search(retrievers: Value, filter: Value) -> Value {
    json!({
        "collection": "qv", "consistency": "strong", "retrievers": retrievers, "fusion": null,
        "filter": filter, "sort": [], "offset": 0, "limit": 10, "search_after": null,
        "score_threshold": null, "select": {"source": "all", "vectors": [], "fields": []},
        "aggregations": null, "highlight": null, "group_by": null, "track_total_hits": "none"
    })
}

#[tokio::test]
async fn every_fixture_query_is_accepted_as_a_filter() {
    let api = Native::start().await;
    qv(&api).await;
    let failures = run_queries(&api, |query| search(json!([]), query.clone())).await;
    api.shutdown().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn every_fixture_query_is_accepted_as_a_text_retriever() {
    let api = Native::start().await;
    qv(&api).await;
    let failures = run_queries(&api, |query| {
        search(json!([{"text": {"query": query, "k": 10}}]), Value::Null)
    })
    .await;
    api.shutdown().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
