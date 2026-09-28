//! The contract, the seed and the mock agree (design §19 §8).

use std::collections::BTreeSet;
use std::time::SystemTime;

use httpmock::MockServer;
use operon_console_mock::{CONTRACT, ENGINE_TEMPLATES, Route, load_seed, register, routes};
use serde_json::Value;

fn contract() -> Value {
    serde_json::from_str(CONTRACT).expect("the contract is JSON")
}

fn all_routes() -> Vec<Route> {
    let seed = load_seed(SystemTime::now()).expect("the seed loads");
    let mut all = routes(&seed, true).expect("routes build");
    all.extend(routes(&seed, false).expect("signed-out routes build"));
    all
}

fn operations(contract: &Value) -> BTreeSet<(String, String)> {
    let mut ops = BTreeSet::new();
    for (path, item) in contract["paths"].as_object().expect("paths") {
        for method in item.as_object().expect("path item").keys() {
            ops.insert((method.to_uppercase(), path.clone()));
        }
    }
    ops
}

#[test]
fn every_operation_has_a_mock_and_every_mock_an_operation() {
    let contract = contract();
    let ops = operations(&contract);
    let mocked: BTreeSet<(String, String)> = all_routes()
        .into_iter()
        .filter(|r| !ENGINE_TEMPLATES.contains(&r.template))
        .map(|r| (r.method.to_string(), r.template.to_string()))
        .collect();
    let missing: Vec<_> = ops.difference(&mocked).collect();
    let extra: Vec<_> = mocked.difference(&ops).collect();
    assert!(
        missing.is_empty(),
        "contract operations without a mock: {missing:?}"
    );
    assert!(
        extra.is_empty(),
        "mocks for paths the contract lacks: {extra:?}"
    );
}

#[test]
fn every_mocked_body_matches_its_response_schema() {
    let contract = contract();
    let mut checked = 0;
    for route in all_routes() {
        if ENGINE_TEMPLATES.contains(&route.template) {
            continue;
        }
        let op = &contract["paths"][route.template][route.method.to_lowercase()];
        let response = &op["responses"][route.status.to_string()];
        let schema = &response["content"]["application/json"]["schema"];
        match (&route.body, schema.is_null()) {
            (None, true) => {}
            (Some(body), false) => {
                let mut errors = Vec::new();
                validate(&contract, schema, body, "$", &mut errors);
                assert!(
                    errors.is_empty(),
                    "{} {} ({}): {}",
                    route.method,
                    route.path,
                    route.status,
                    errors.join("; ")
                );
                checked += 1;
            }
            (Some(body), true) if route.status >= 400 => {
                // An error status must be declared, exactly or as a range.
                let range = format!("{}XX", route.status / 100);
                let declared =
                    &op["responses"][range.as_str()]["content"]["application/json"]["schema"];
                assert!(
                    !declared.is_null(),
                    "{} {} answers {} but the contract declares neither it nor {range}",
                    route.method,
                    route.path,
                    route.status,
                );
                let mut errors = Vec::new();
                validate(&contract, declared, body, "$", &mut errors);
                assert!(
                    errors.is_empty(),
                    "{} {}: {}",
                    route.method,
                    route.path,
                    errors.join("; ")
                );
            }
            (body, _) => panic!(
                "{} {} ({}): body {} but the contract {} a JSON response",
                route.method,
                route.path,
                route.status,
                if body.is_some() { "present" } else { "absent" },
                if schema.is_null() {
                    "has no"
                } else {
                    "declares"
                },
            ),
        }
    }
    assert!(checked > 100, "only {checked} bodies checked");
}

#[test]
fn seed_times_are_resolved() {
    let seed = load_seed(SystemTime::now()).unwrap();
    let text = seed.to_string();
    assert!(!text.contains("\"@"), "an unresolved placeholder remains");
    // A live token expires in the future.
    let token = &seed["tokens"]["agt_docs_qa"][0];
    let expires = humantime::parse_rfc3339(token["expires_at"].as_str().unwrap()).unwrap();
    assert!(expires > SystemTime::now());
}

#[tokio::test]
async fn the_server_answers_from_the_seed() {
    let seed = load_seed(SystemTime::now()).unwrap();
    let server = MockServer::start_async().await;
    register(&server, &routes(&seed, true).unwrap())
        .await
        .unwrap();
    let client = reqwest::Client::new();

    let projects: Value = client
        .get(server.url("/api/v1/projects"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let slugs: Vec<&str> = projects["projects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["slug"].as_str().unwrap())
        .collect();
    assert_eq!(slugs, ["docs-search", "support-assistant", "code-index"]);

    // The project filter on the audit log matches before the unfiltered mock.
    let filtered: Value = client
        .get(server.url("/api/v1/audit?project=code-index"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let events = filtered["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert!(events.iter().all(|e| e["project"] == "code-index"));

    // An environment's collections come from the engine's data API.
    let cols: Value = client
        .get(server.url("/v1/namespaces/docs-search-production/collections"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cols["collections"][0]["name"], "docs");

    let status = client
        .delete(server.url("/api/v1/session"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 204);
}

#[test]
fn the_validator_rejects_bodies_that_break_the_schema() {
    let contract = contract();
    let agent = serde_json::json!({"$ref": "#/components/schemas/Agent"});
    let seed = load_seed(SystemTime::now()).unwrap();
    let good = seed["agents"][0].clone();
    let mut errors = Vec::new();
    validate(&contract, &agent, &good, "$", &mut errors);
    assert!(errors.is_empty(), "{errors:?}");

    for (field, bad) in [
        ("status", serde_json::json!("sleeping")),
        ("max_token_ttl_s", serde_json::json!(86_400)),
        ("owner", serde_json::json!("usr_01")),
        ("surprise", serde_json::json!(true)),
    ] {
        let mut body = good.clone();
        body[field] = bad;
        let mut errors = Vec::new();
        validate(&contract, &agent, &body, "$", &mut errors);
        assert!(!errors.is_empty(), "{field} should fail");
    }
    let mut missing = good.clone();
    missing.as_object_mut().unwrap().remove("policy");
    let mut errors = Vec::new();
    validate(&contract, &agent, &missing, "$", &mut errors);
    assert_eq!(errors, ["$: missing required policy"]);
}

/// The JSON Schema subset the contract uses: `$ref`, `type` (one or a list),
/// `enum`, `properties`, `required`, `additionalProperties: false`, `items`,
/// `anyOf`, `minimum` and `maximum`.
fn validate(root: &Value, schema: &Value, value: &Value, at: &str, errors: &mut Vec<String>) {
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        let name = r.trim_start_matches("#/components/schemas/");
        let target = &root["components"]["schemas"][name];
        assert!(!target.is_null(), "unknown $ref {r}");
        return validate(root, target, value, at, errors);
    }
    if let Some(any) = schema.get("anyOf").and_then(Value::as_array) {
        let ok = any.iter().any(|s| {
            let mut e = Vec::new();
            validate(root, s, value, at, &mut e);
            e.is_empty()
        });
        if !ok {
            errors.push(format!("{at}: matches none of anyOf"));
        }
        return;
    }
    if let Some(t) = schema.get("type") {
        let types: Vec<&str> = match t {
            Value::String(s) => vec![s.as_str()],
            Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        let actual = match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };
        let ok = types.contains(&actual) || (actual == "integer" && types.contains(&"number"));
        if !ok {
            errors.push(format!("{at}: expected {types:?}, got {actual}"));
            return;
        }
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        errors.push(format!("{at}: {value} is not one of {allowed:?}"));
    }
    if let Some(n) = value.as_f64()
        && let Some(min) = schema.get("minimum").and_then(Value::as_f64)
        && n < min
    {
        errors.push(format!("{at}: {n} < minimum {min}"));
    }
    if let Some(n) = value.as_f64()
        && let Some(max) = schema.get("maximum").and_then(Value::as_f64)
        && n > max
    {
        errors.push(format!("{at}: {n} > maximum {max}"));
    }
    if let Value::Object(map) = value {
        let props = schema.get("properties").and_then(Value::as_object);
        for key in schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let key = key.as_str().unwrap_or_default();
            if !map.contains_key(key) {
                errors.push(format!("{at}: missing required {key}"));
            }
        }
        for (key, v) in map {
            match props.and_then(|p| p.get(key)) {
                Some(s) => validate(root, s, v, &format!("{at}.{key}"), errors),
                None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                    errors.push(format!("{at}: unexpected property {key}"));
                }
                None => {}
            }
        }
    }
    if let (Value::Array(list), Some(item)) = (value, schema.get("items")) {
        for (i, v) in list.iter().enumerate() {
            validate(root, item, v, &format!("{at}[{i}]"), errors);
        }
    }
}
