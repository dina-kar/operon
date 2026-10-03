//! The gate that matters most: a credential never appears in an error, a
//! `Debug`, a header rendering, a log line or a tool result.

use std::sync::Arc;

use loams_web_bridge::testing::{FakeCdp, FakeTransport, read_page};
use loams_web_bridge::{
    BrowserProvider, BrowserRunProvider, EgressPolicy, FillValue, OpenRequest, ProviderKind,
    SecretRef, SecretResolver, SecretValue, WebBridgeConfig, local_provider, redact,
};
use serde_json::{Value, json};

/// A token that appears nowhere else, so finding it anywhere means a leak.
const CANARY_TOKEN: &str = "canary-cf-token-2f7c1d9e4b6a8053";
/// A password typed into a field, likewise.
const CANARY_PASSWORD: &str = "canary-password-9d3a71fe05b2";

fn config() -> WebBridgeConfig {
    let mut config = WebBridgeConfig {
        provider: ProviderKind::BrowserRun,
        ..WebBridgeConfig::default()
    };
    config.browser_run.account_id = "0123456789abcdef0123456789abcdef".to_string();
    config.browser_run.token_secret_ref = Some("env:cloudflare#api_token".to_string());
    config.browser_run.plan = loams_web_bridge::Plan::Paid;
    config.browser_run.min_session_interval_ms = Some(0);
    config.credentials.allow_secret_fills = true;
    config.credentials.allowed_fill_hosts = vec!["example.com".to_string()];
    config
}

fn resolver() -> Arc<loams_web_bridge::MapSecretResolver> {
    Arc::new(
        loams_web_bridge::MapSecretResolver::new()
            .with(
                SecretRef::parse("env:cloudflare#api_token").unwrap_or_else(|_| panic!("ref")),
                CANARY_TOKEN,
            )
            .with(
                SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref")),
                CANARY_PASSWORD,
            ),
    )
}

fn answers() -> std::collections::HashMap<String, Value> {
    let mut answers = std::collections::HashMap::new();
    answers.insert(
        "Target.createTarget".to_string(),
        json!({ "targetId": "target-1" }),
    );
    answers.insert(
        "Target.attachToTarget".to_string(),
        json!({ "sessionId": "session-1" }),
    );
    answers.insert(
        "Accessibility.getFullAXTree".to_string(),
        json!({ "nodes": [
            {"nodeId": "1", "role": {"value": "RootWebArea"}, "name": {"value": "Example"},
             "childIds": ["2"], "backendDOMNodeId": 1},
            {"nodeId": "2", "role": {"value": "textbox"}, "name": {"value": "Password"},
             "value": {"value": CANARY_PASSWORD},
             "properties": [{"name": "protected", "value": {"value": true}}],
             "backendDOMNodeId": 7}
        ]}),
    );
    answers.insert(
        "Runtime.evaluate".to_string(),
        read_page("https://example.com/login", "Example", "Sign in"),
    );
    answers.insert(
        "DOM.resolveNode".to_string(),
        json!({ "object": { "objectId": "obj-1" } }),
    );
    // A browser that is rude enough to echo the request back in its error.
    answers.insert(
        "Runtime.callFunctionOn".to_string(),
        json!({ "exceptionDetails": { "text": "refused" } }),
    );
    answers.insert("Page.navigate".to_string(), json!({ "frameId": "frame-1" }));
    answers
}

/// Everything a caller could print after a session, as one string.
async fn observable_output() -> String {
    let mut collected = String::new();

    // A browser that refuses to create a target, with the token in its message.
    let rude = Arc::new(FakeCdp::new(answers()));
    {
        let mut rude_inner = FakeCdp::new(answers());
        rude_inner.fail_create_target = Some(format!(
            "connect failed, Authorization: Bearer {CANARY_TOKEN}"
        ));
        let transport = Arc::new(FakeTransport::new(Arc::new(rude_inner)));
        let config = config();
        let failing = BrowserRunProvider::new(
            config.browser_run.clone(),
            EgressPolicy::public_web(),
            config.credentials.clone(),
            resolver(),
            transport,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        match failing.open(OpenRequest::blank()).await {
            Ok(_) => collected.push_str("the rude browser opened a session\n"),
            Err(error) => {
                collected.push_str(&format!("{error}\n"));
                collected.push_str(&format!("{error:?}\n"));
            }
        }
    }

    // A refused fill, a refused navigation, and a real session's own output.
    let transport = Arc::new(FakeTransport::new(Arc::clone(&rude)));
    let config = config();
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        config.egress().unwrap_or_else(|error| panic!("{error}")),
        config.credentials.clone(),
        resolver(),
        transport,
    )
    .unwrap_or_else(|error| panic!("{error}"));

    collected.push_str(&format!("provider: {provider:?}\n"));
    collected.push_str(&format!("transport: {provider:?}\n"));

    match provider.open(OpenRequest::blank()).await {
        Ok(_) => collected.push_str("the rude browser opened a session\n"),
        Err(error) => {
            collected.push_str(&format!("{error}\n"));
            collected.push_str(&format!("{error:?}\n"));
        }
    }

    let ok_transport = Arc::new(FakeTransport::new(Arc::new(FakeCdp::new(answers()))));
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        config.egress().unwrap_or_else(|error| panic!("{error}")),
        config.credentials.clone(),
        resolver(),
        ok_transport.clone(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let page = provider
        .open(OpenRequest::at(
            url::Url::parse("https://example.com/login").unwrap_or_else(|_| panic!("url")),
        ))
        .await
        .unwrap_or_else(|error| panic!("{error}"));

    // The handshake itself: the one place the token must be present.
    for (_, headers) in ok_transport.handshakes() {
        collected.push_str(&format!("headers: {headers:?}\n"));
    }

    if let Ok(snapshot) = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
    {
        collected.push_str(&snapshot.render(loams_web_bridge::tool::DEFAULT_BUDGET_CHARS));
        collected.push('\n');
        let uid = snapshot.nodes[1].uid.clone();
        let reference = SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref"));
        let outcome = provider
            .fill(&page, &uid, &FillValue::Secret(reference))
            .await;
        match outcome {
            Ok(outcome) => collected.push_str(&outcome.render()),
            Err(error) => {
                collected.push_str(&format!("{error}\n"));
                collected.push_str(&format!("{error:?}\n"));
            }
        }
    }

    // A navigation the egress policy refuses, and one the browser refuses.
    if let Err(error) = provider
        .navigate(
            &page,
            &url::Url::parse("http://169.254.169.254/latest/meta-data/")
                .unwrap_or_else(|_| panic!("url")),
        )
        .await
    {
        collected.push_str(&format!("{error}\n"));
        collected.push_str(&format!("{error:?}\n"));
    }

    let missing = SecretRef::parse("env:nothing#here").unwrap_or_else(|_| panic!("ref"));
    match resolver().resolve(&missing).await {
        Ok(_) => collected.push_str("an absent secret resolved\n"),
        Err(error) => {
            collected.push_str(&format!("{error}\n"));
            collected.push_str(&format!("{error:?}\n"));
        }
    }

    collected.push_str(&format!("{rude:?}\n"));
    collected
}

#[tokio::test]
async fn the_api_token_never_appears_in_an_error_a_log_or_a_rendering() {
    {
        let config = config();
        BrowserRunProvider::new(
            config.browser_run.clone(),
            config.egress().unwrap_or_else(|error| panic!("{error}")),
            config.credentials.clone(),
            resolver(),
            Arc::new(FakeTransport::new(Arc::new(FakeCdp::new(answers())))),
        )
        .unwrap_or_else(|error| panic!("{error}"))
    };
    let output = observable_output().await;

    assert!(
        !output.contains(CANARY_TOKEN),
        "the token leaked into:\n{output}"
    );
    assert!(
        output.contains(redact::REDACTED),
        "the redaction marker should be there instead:\n{output}"
    );
}

#[tokio::test]
async fn a_typed_password_never_appears_in_a_snapshot_or_an_action_result() {
    let config = config();
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        config.egress().unwrap_or_else(|error| panic!("{error}")),
        config.credentials.clone(),
        resolver(),
        Arc::new(FakeTransport::new(Arc::new(FakeCdp::new(answers())))),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let page = provider
        .open(OpenRequest::at(
            url::Url::parse("https://example.com/login").unwrap_or_else(|_| panic!("url")),
        ))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        !snapshot
            .render(loams_web_bridge::tool::DEFAULT_BUDGET_CHARS)
            .contains(CANARY_PASSWORD)
    );

    let reference = SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref"));
    let outcome = provider
        .fill(&page, &snapshot.nodes[1].uid, &FillValue::Secret(reference))
        .await;
    let rendered = match outcome {
        Ok(outcome) => outcome.render(),
        Err(error) => format!("{error}\n{error:?}"),
    };
    assert!(!rendered.contains(CANARY_PASSWORD), "{rendered}");
}

#[tokio::test]
async fn the_local_driver_never_sees_a_secret_in_its_own_log() {
    let driver = Arc::new(loams_web_bridge::testing::FakeDriver::new(
        loams_web_bridge::testing::login_page("https://example.com/login"),
    ));
    let config = {
        let mut config = config();
        config.provider = ProviderKind::Local;
        config.credentials.allow_secret_fills = true;
        config.credentials.allowed_fill_hosts = vec!["example.com".to_string()];
        config
    };
    let provider = local_provider(&config, driver.clone(), resolver())
        .unwrap_or_else(|error| panic!("{error}"));
    let page = provider
        .open(OpenRequest::at(
            url::Url::parse("https://example.com/login").unwrap_or_else(|_| panic!("url")),
        ))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let reference = SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref"));
    provider
        .fill(&page, &snapshot.nodes[0].uid, &FillValue::Secret(reference))
        .await
        .unwrap_or_else(|error| panic!("{error}"));

    let actions = driver.actions().join("\n");
    assert!(actions.contains("fill"), "{actions}");
    assert!(!actions.contains(CANARY_PASSWORD), "{actions}");
    assert!(
        actions.contains("bytes"),
        "only a length crosses: {actions}"
    );
}

#[test]
fn a_secret_value_cannot_be_printed_by_accident() {
    let value = SecretValue::new(CANARY_TOKEN);
    assert_eq!(format!("{value}"), redact::REDACTED);
    assert_eq!(format!("{value:?}"), "SecretValue(<redacted>)");
    assert!(redact::is_registered(CANARY_TOKEN));
    assert_eq!(value.expose(), CANARY_TOKEN, "the wire path still works");
}
