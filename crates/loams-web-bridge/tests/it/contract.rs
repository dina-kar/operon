//! One conformance suite, run against both providers.
//!
//! This is the point of the abstraction: the same assertions hold whichever
//! provider a deployment selects, and the local provider cannot drift from the
//! remote one without a test failing.

use std::collections::HashMap;
use std::sync::Arc;

use loams_web_bridge::testing::{
    FakeCdp, FakeDriver, FakeTransport, ax_tree_with_button, login_page, read_page,
};
use loams_web_bridge::{
    BrowserProvider, BrowserRunProvider, CredentialPolicy, MapSecretResolver, OpenRequest, PageRef,
    ProviderKind, SecretRef, SnapshotRequest, WaitCondition, WaitRequest, WebBridgeConfig,
    local_provider,
};
use serde_json::{Value, json};

const URL: &str = "https://example.com/login";

fn url(raw: &str) -> url::Url {
    url::Url::parse(raw).unwrap_or_else(|_| panic!("{raw} parses"))
}

fn resolver() -> Arc<MapSecretResolver> {
    Arc::new(MapSecretResolver::new().with(
        SecretRef::parse("env:cloudflare#api_token").unwrap_or_else(|_| panic!("ref")),
        "canary-token-4f8b2c19d7e3",
    ))
}

fn remote() -> Box<dyn BrowserProvider> {
    let mut config = WebBridgeConfig {
        provider: ProviderKind::BrowserRun,
        ..WebBridgeConfig::default()
    };
    config.browser_run.account_id = "0123456789abcdef0123456789abcdef".to_string();
    config.browser_run.token_secret_ref = Some("env:cloudflare#api_token".to_string());
    config.browser_run.plan = loams_web_bridge::Plan::Paid;
    config.browser_run.min_session_interval_ms = Some(0);
    let mut answers: HashMap<String, Value> = HashMap::new();
    answers.insert(
        "Target.createTarget".to_string(),
        json!({ "targetId": "t1" }),
    );
    answers.insert(
        "Target.attachToTarget".to_string(),
        json!({ "sessionId": "s1" }),
    );
    answers.insert(
        "Accessibility.getFullAXTree".to_string(),
        ax_tree_with_button("Sign in", 42),
    );
    answers.insert(
        "Runtime.evaluate".to_string(),
        read_page(URL, "Sign in", "Sign in to continue"),
    );
    answers.insert(
        "DOM.resolveNode".to_string(),
        json!({ "object": { "objectId": "o1" } }),
    );
    answers.insert("Page.navigate".to_string(), json!({ "frameId": "f1" }));
    let transport = Arc::new(FakeTransport::new(Arc::new(FakeCdp::new(answers))));
    Box::new(
        BrowserRunProvider::new(
            config.browser_run.clone(),
            config.egress().unwrap_or_else(|error| panic!("{error}")),
            config.credentials.clone(),
            resolver(),
            transport,
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn local() -> Box<dyn BrowserProvider> {
    let driver = Arc::new(FakeDriver::new(login_page(URL)));
    let mut config = WebBridgeConfig {
        provider: ProviderKind::Local,
        ..WebBridgeConfig::default()
    };
    config.local.artifact_dir = None;
    local_provider(&config, driver, resolver()).unwrap_or_else(|error| panic!("{error}"))
}

async fn contract(provider: &dyn BrowserProvider) {
    // A handle names its provider.
    let page = provider
        .open(OpenRequest::at(url(URL)))
        .await
        .unwrap_or_else(|error| panic!("{} open: {error}", provider.kind().as_str()));
    assert_eq!(page.provider(), provider.kind());

    // A snapshot is text with uids, and it carries the page's shape.
    let snapshot = provider
        .snapshot(&page, &SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("snapshot: {error}"));
    assert!(!snapshot.nodes.is_empty(), "a snapshot has nodes");
    assert!(
        snapshot
            .nodes
            .iter()
            .all(|node| node.uid.as_str().starts_with('s')),
        "every uid names its snapshot"
    );
    let rendered = snapshot.render(SnapshotRequest::new().budget());
    assert!(!rendered.contains("hunter2"), "a password never renders");

    // A uid from an older snapshot is a self-healing error, whichever
    // provider minted it.
    let first_uid = snapshot.nodes[1].uid.clone();
    provider
        .snapshot(&page, &SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("a second snapshot: {error}"));
    let error = provider
        .click(&page, &first_uid)
        .await
        .err()
        .unwrap_or_else(|| panic!("a stale uid is refused"));
    assert!(error.to_string().contains("take_snapshot"), "{error}");

    // find answers with matches, not with the page.
    let found = provider
        .find(&page, &loams_web_bridge::FindQuery::text("Sign in"))
        .await
        .unwrap_or_else(|error| panic!("find: {error}"));
    assert!(!found.is_empty(), "find returns matches");

    // An action answers in one line plus a change summary.
    let current = provider
        .snapshot(&page, &SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let uid = current.nodes[1].uid.clone();
    let outcome = provider
        .click(&page, &uid)
        .await
        .unwrap_or_else(|error| panic!("click: {error}"));
    assert!(outcome.message.contains("clicked"), "{}", outcome.message);
    assert!(!outcome.render().is_empty());

    // A wait for text that is on the page succeeds, and one that never will
    // does not lie about having succeeded.
    let outcome = provider
        .wait_for(
            &page,
            &WaitRequest::new(WaitCondition::Text("Sign in".to_string())).with_timeout_ms(50),
        )
        .await
        .unwrap_or_else(|error| panic!("wait_for: {error}"));
    assert!(outcome.matched, "{outcome:?}");
    let missed = provider
        .wait_for(
            &page,
            &WaitRequest::new(WaitCondition::Text("never appears".to_string())).with_timeout_ms(1),
        )
        .await
        .unwrap_or_else(|error| panic!("wait_for: {error}"));
    assert!(!missed.matched);
    assert!(missed.note.contains("take_snapshot"), "{missed:?}");

    // Extraction answers with the page's text.
    let extracted = provider
        .extract(&page)
        .await
        .unwrap_or_else(|error| panic!("extract: {error}"));
    assert!(!extracted.title.is_empty(), "a title");

    // A private address is refused by both providers, in the same way.
    for blocked in [
        "http://127.0.0.1/",
        "http://169.254.169.254/latest/meta-data/",
        "file:///etc/passwd",
    ] {
        let error = provider
            .navigate(&page, &url(blocked))
            .await
            .err()
            .unwrap_or_else(|| panic!("{blocked} is refused"));
        assert!(
            matches!(error, loams_web_bridge::BridgeError::EgressDenied(_)),
            "{blocked}: {error:?}"
        );
    }

    // A closed handle is an error, and closing twice is not.
    provider
        .close(&page)
        .await
        .unwrap_or_else(|error| panic!("close: {error}"));
    provider
        .close(&page)
        .await
        .unwrap_or_else(|error| panic!("closing twice is quiet: {error}"));
    assert!(
        provider
            .snapshot(&page, &SnapshotRequest::new())
            .await
            .is_err(),
        "a closed handle cannot be snapshotted"
    );
}

#[tokio::test]
async fn the_local_provider_satisfies_the_contract() {
    contract(local().as_ref()).await;
}

#[tokio::test]
async fn the_remote_provider_satisfies_the_same_contract() {
    contract(remote().as_ref()).await;
}

#[tokio::test]
async fn the_providers_report_different_engines_and_different_trust() {
    let local = local();
    let remote = remote();

    assert_eq!(local.engine(), None);
    assert_eq!(remote.engine(), Some(loams_web_bridge::Engine::Chromium));

    let local_caps = local.capabilities();
    let remote_caps = remote.capabilities();
    assert!(local_caps.credentials_stay_local);
    assert!(!remote_caps.credentials_stay_local);
    assert!(local_caps.authenticated_sessions);
    assert!(
        remote_caps.authenticated_sessions,
        "chromium keeps a session"
    );
    assert!(!remote_caps.guardrails, "no allow list was configured");

    // Both are swappable behind the one trait.
    let providers: Vec<Arc<dyn BrowserProvider>> = vec![Arc::from(local), Arc::from(remote)];
    assert_eq!(providers.len(), 2);
    for provider in providers {
        assert!(provider.capabilities().core);
    }
}

#[tokio::test]
async fn a_handle_is_opaque_and_says_only_what_it_must() {
    let page = PageRef::new(ProviderKind::BrowserRun, "remote-target-1");
    let rendered = format!("{page:?}");
    assert!(rendered.contains("remote-target-1"));
    assert!(!rendered.contains("session"), "{rendered}");
    assert!(!rendered.contains("token"), "{rendered}");
}

#[tokio::test]
async fn the_remote_provider_honours_an_allow_list_it_was_given() {
    let mut config = WebBridgeConfig {
        provider: ProviderKind::BrowserRun,
        ..WebBridgeConfig::default()
    };
    config.browser_run.account_id = "0123456789abcdef0123456789abcdef".to_string();
    config.browser_run.token_secret_ref = Some("env:cloudflare#api_token".to_string());
    config.browser_run.plan = loams_web_bridge::Plan::Paid;
    config.browser_run.min_session_interval_ms = Some(0);
    config.allowed_domains = vec!["*.example.com".to_string()];
    let mut answers: HashMap<String, Value> = HashMap::new();
    answers.insert(
        "Target.createTarget".to_string(),
        json!({ "targetId": "t1" }),
    );
    answers.insert(
        "Target.attachToTarget".to_string(),
        json!({ "sessionId": "s1" }),
    );
    answers.insert(
        "Accessibility.getFullAXTree".to_string(),
        ax_tree_with_button("Sign in", 42),
    );
    answers.insert(
        "Runtime.evaluate".to_string(),
        read_page(URL, "Sign in", "Sign in"),
    );
    answers.insert("Page.navigate".to_string(), json!({ "frameId": "f1" }));
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        config.egress().unwrap_or_else(|error| panic!("{error}")),
        CredentialPolicy::default(),
        resolver(),
        Arc::new(FakeTransport::new(Arc::new(FakeCdp::new(answers)))),
    )
    .unwrap_or_else(|error| panic!("{error}"));

    let page = provider
        .open(OpenRequest::at(url("https://app.example.com/login")))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let error = provider
        .navigate(&page, &url("https://elsewhere.test/"))
        .await
        .err()
        .unwrap_or_else(|| panic!("off the allow list"));
    assert!(error.to_string().contains("allow list"), "{error}");
    assert!(provider.capabilities().guardrails);
}
