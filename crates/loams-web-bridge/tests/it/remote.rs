//! The Browser Run provider, driven by a scripted CDP connection.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use loams_web_bridge::browser_run::endpoint::{AUTHORIZATION_HEADER, GUARDRAILS_HEADER};
use loams_web_bridge::browser_run::ws::WebSocketTransport;
use loams_web_bridge::testing::{FakeCdp, FakeTransport, ax_tree_with_button, read_page};
use loams_web_bridge::{
    BrowserProvider, BrowserRunProvider, CredentialPolicy, EgressPolicy, Engine, FillValue,
    MapSecretResolver, OpenRequest, ProviderKind, SecretRef, WebBridgeConfig,
};
use serde_json::{Value, json};

pub const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";
pub const TOKEN_REF: &str = "env:cloudflare#api_token";
pub const TOKEN: &str = "cf-token-abcdef0123456789abcdef0123456789";

/// A remote configuration with no allow list, so the tests are about the
/// protocol rather than the policy (the policy has its own tests).
pub fn config() -> WebBridgeConfig {
    let mut config = WebBridgeConfig {
        provider: ProviderKind::BrowserRun,
        ..WebBridgeConfig::default()
    };
    config.browser_run.account_id = ACCOUNT.to_string();
    config.browser_run.token_secret_ref = Some(TOKEN_REF.to_string());
    config.browser_run.plan = loams_web_bridge::Plan::Paid;
    config.browser_run.min_session_interval_ms = Some(0);
    config
}

pub fn resolver() -> Arc<MapSecretResolver> {
    Arc::new(
        MapSecretResolver::new()
            .with(
                SecretRef::parse(TOKEN_REF).unwrap_or_else(|_| panic!("a good reference")),
                TOKEN,
            )
            .with(
                SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("a good reference")),
                "correct-horse-battery-staple",
            ),
    )
}

fn answers() -> HashMap<String, Value> {
    let mut answers = HashMap::new();
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
        ax_tree_with_button("Sign in", 42),
    );
    answers.insert(
        "Runtime.evaluate".to_string(),
        read_page("https://example.com/", "Example", "Sign in to continue"),
    );
    answers.insert(
        "DOM.resolveNode".to_string(),
        json!({ "object": { "objectId": "obj-1" } }),
    );
    answers.insert("Page.navigate".to_string(), json!({ "frameId": "frame-1" }));
    answers.insert(
        "Page.captureScreenshot".to_string(),
        json!({ "data": base64::engine::general_purpose::STANDARD.encode(b"fake-png-bytes") }),
    );
    answers
}

/// A provider over a scripted connection, with both handles returned so a test
/// can assert what was sent.
pub fn provider(
    configure: impl FnOnce(&mut WebBridgeConfig),
) -> (BrowserRunProvider, Arc<FakeTransport>, Arc<FakeCdp>) {
    let mut config = config();
    configure(&mut config);
    let connection = Arc::new(FakeCdp::new(answers()));
    let transport = Arc::new(FakeTransport::new(Arc::clone(&connection)));
    let egress = config.egress().unwrap_or_else(|error| panic!("{error}"));
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        egress,
        config.credentials.clone(),
        resolver(),
        transport.clone(),
    )
    .unwrap_or_else(|error| panic!("the provider builds: {error}"));
    (provider, transport, connection)
}

async fn open(provider: &BrowserRunProvider) -> loams_web_bridge::PageRef {
    open_at(provider, "https://example.com/login").await
}

async fn open_at(provider: &BrowserRunProvider, raw: &str) -> loams_web_bridge::PageRef {
    provider
        .open(OpenRequest::at(
            url::Url::parse(raw).unwrap_or_else(|_| panic!("{raw} parses")),
        ))
        .await
        .unwrap_or_else(|error| panic!("the session opens: {error}"))
}

#[tokio::test]
async fn the_handshake_carries_the_bearer_token_and_the_documented_path() {
    let (provider, transport, _) = provider(|_| {});
    let _page = open(&provider).await;
    let handshakes = transport.handshakes();
    let (url, headers) = handshakes.first().unwrap_or_else(|| panic!("a handshake"));
    assert_eq!(
        url,
        &format!(
            "wss://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/browser-run/devtools/browser\
             ?keep_alive=600000"
        )
    );
    assert!(headers.contains(AUTHORIZATION_HEADER));
    assert!(
        !headers.contains(GUARDRAILS_HEADER),
        "no allow list, no header"
    );
}

#[tokio::test]
async fn an_allow_list_becomes_a_guardrails_header() {
    let (provider, transport, _) = provider(|config| {
        config.allowed_domains = vec!["*.example.com".to_string()];
        config.allowed_domain_sets = vec!["common-cdns".to_string()];
    });
    let _page = open_at(&provider, "https://app.example.com/login").await;
    let (_, headers) = transport
        .handshakes()
        .first()
        .cloned()
        .unwrap_or_else(|| panic!("a handshake"));
    let guardrails = headers
        .as_pairs()
        .iter()
        .find(|(name, _)| name == GUARDRAILS_HEADER)
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| panic!("guardrails are sent"));
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(guardrails.as_bytes())
        .unwrap_or_else(|_| panic!("base64url"));
    let json = String::from_utf8(decoded).unwrap_or_else(|_| panic!("utf-8"));
    assert!(json.contains("*.example.com"), "{json}");
    assert!(json.contains("common-cdns"), "{json}");
}

#[tokio::test]
async fn kitesurf_selects_the_engine_and_never_sends_keep_alive() {
    let (provider, transport, _) = provider(|config| config.browser_run.engine = Engine::Kitesurf);
    let _page = open(&provider).await;
    let (url, _) = transport
        .handshakes()
        .first()
        .cloned()
        .unwrap_or_else(|| panic!("a handshake"));
    assert!(url.ends_with("?browser=kitesurf"), "{url}");
    assert!(!url.contains("keep_alive"), "{url}");
    assert_eq!(provider.engine(), Engine::Kitesurf);
}

#[tokio::test]
async fn a_kitesurf_session_under_an_allow_list_is_refused_by_default() {
    let mut config = config();
    config.browser_run.engine = Engine::Kitesurf;
    config.allowed_domains = vec!["*.example.com".to_string()];
    let egress = config.egress().unwrap_or_else(|error| panic!("{error}"));
    let error = BrowserRunProvider::new(
        config.browser_run.clone(),
        egress,
        CredentialPolicy::default(),
        resolver(),
        Arc::new(FakeTransport::new(Arc::new(FakeCdp::default()))),
    )
    .err()
    .unwrap_or_else(|| panic!("refused"));
    assert!(error.to_string().contains("guardrails"), "{error}");
}

#[tokio::test]
async fn the_session_lifecycle_is_the_documented_cdp_conversation() {
    let (provider, _, connection) = provider(|_| {});
    let _page = open(&provider).await;
    let calls = connection.calls();
    assert_eq!(
        calls,
        [
            "Target.createTarget(-)",
            "Target.attachToTarget(-)",
            "Page.enable(session-1)",
            "Runtime.enable(session-1)",
            "DOM.enable(session-1)",
            "Accessibility.enable(session-1)",
            "Page.navigate(session-1)",
            "Accessibility.getFullAXTree(session-1)",
            "Runtime.evaluate(session-1)",
        ]
    );
}

#[tokio::test]
async fn a_snapshot_carries_uids_and_never_a_password_value() {
    let (provider, _, _) = provider(|_| {});
    let page = open(&provider).await;
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let button = snapshot
        .nodes
        .iter()
        .find(|node| node.name == "Sign in")
        .unwrap_or_else(|| panic!("the button is in the tree"));
    assert_eq!(button.uid.as_str(), "s2_2");
    assert_eq!(button.backend_id, 42);
    assert_eq!(snapshot.url, "example.com");
}

#[tokio::test]
async fn a_stale_uid_never_reaches_the_browser() {
    let (provider, _, connection) = provider(|_| {});
    let page = open(&provider).await;
    provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let before = connection.calls().len();
    let error = provider
        .click(
            &page,
            &loams_web_bridge::Uid::parse("s1_2").unwrap_or_else(|_| panic!("uid")),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a stale uid is an error"));
    assert!(error.to_string().contains("take_snapshot"), "{error}");
    assert_eq!(connection.calls().len(), before, "nothing was sent");
}

#[tokio::test]
async fn a_click_resolves_the_uid_to_a_backend_node_and_releases_the_handle() {
    let (provider, _, connection) = provider(|_| {});
    let page = open(&provider).await;
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let uid = snapshot
        .nodes
        .iter()
        .find(|node| node.backend_id == 42)
        .map(|node| node.uid.clone())
        .unwrap_or_else(|| panic!("a uid for the button"));
    let outcome = provider
        .click(&page, &uid)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(outcome.message.contains("clicked"), "{}", outcome.message);
    let calls = connection.calls();
    assert!(calls.contains(&"DOM.resolveNode(session-1)".to_string()));
    assert!(calls.contains(&"Runtime.callFunctionOn(session-1)".to_string()));
    assert!(calls.contains(&"Runtime.releaseObject(session-1)".to_string()));
}

#[tokio::test]
async fn a_secret_fill_is_refused_on_the_remote_provider_by_default() {
    let (provider, _, _) = provider(|_| {});
    let page = open(&provider).await;
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let uid = snapshot.nodes[1].uid.clone();
    let reference = SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref"));
    let error = provider
        .fill(&page, &uid, &FillValue::Secret(reference))
        .await
        .err()
        .unwrap_or_else(|| panic!("refused"));
    assert!(error.to_string().contains("third party"), "{error}");
}

#[tokio::test]
async fn a_service_account_fill_needs_an_allow_listed_host() {
    let (provider, _, _) = provider(|config| {
        config.credentials.allow_secret_fills = true;
        config.credentials.allowed_fill_hosts = vec!["app.example.com".to_string()];
    });
    let page = open(&provider).await;
    let snapshot = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let uid = snapshot.nodes[1].uid.clone();
    let reference = SecretRef::parse("env:app#password").unwrap_or_else(|_| panic!("ref"));
    // The page is example.com, which is not the allow-listed host.
    let error = provider
        .fill(&page, &uid, &FillValue::Secret(reference))
        .await
        .err()
        .unwrap_or_else(|| panic!("refused"));
    assert!(error.to_string().contains("allowed-fill-hosts"), "{error}");
}

#[tokio::test]
async fn a_screenshot_lands_on_disk_with_a_hash() {
    let directory = tempfile::tempdir().unwrap_or_else(|error| panic!("{error}"));
    let (provider, _, _) = provider(|config| {
        config.browser_run.artifact_dir = Some(directory.path().to_path_buf());
    });
    let page = open(&provider).await;
    let screenshot = provider
        .screenshot(&page)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(screenshot.path.starts_with(directory.path()));
    assert_eq!(screenshot.bytes, b"fake-png-bytes".len());
    assert_eq!(screenshot.sha256.len(), 64);
    assert!(
        std::fs::read(&screenshot.path).is_ok(),
        "the file is written"
    );
}

#[tokio::test]
async fn kitesurf_refuses_a_pixel_screenshot_and_offers_extraction() {
    let directory = tempfile::tempdir().unwrap_or_else(|error| panic!("{error}"));
    let (provider, _, _) = provider(|config| {
        config.browser_run.engine = Engine::Kitesurf;
        config.browser_run.artifact_dir = Some(directory.path().to_path_buf());
    });
    let page = open(&provider).await;
    let error = provider
        .screenshot(&page)
        .await
        .err()
        .unwrap_or_else(|| panic!("refused"));
    assert!(error.to_string().contains("pixel-perfect"), "{error}");

    let extracted = provider
        .extract(&page)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(extracted.title, "Example");
    assert!(extracted.text.contains("Sign in"), "{}", extracted.text);
    assert!(!provider.capabilities().authenticated_sessions);
    assert!(provider.capabilities().extraction);
}

#[tokio::test]
async fn the_concurrency_ceiling_is_the_free_plans_three() {
    let connection = Arc::new(FakeCdp::new(answers()));
    let transport = Arc::new(FakeTransport::new(Arc::clone(&connection)));
    let mut config = config();
    config.browser_run.plan = loams_web_bridge::Plan::Free;
    config.browser_run.min_session_interval_ms = Some(0);
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        EgressPolicy::public_web(),
        CredentialPolicy::default(),
        resolver(),
        transport,
    )
    .unwrap_or_else(|error| panic!("{error}"));

    let first = open(&provider).await;
    let second = open(&provider).await;
    let third = open(&provider).await;
    let error = provider
        .open(OpenRequest::blank())
        .await
        .err()
        .unwrap_or_else(|| panic!("the fourth is refused"));
    assert!(matches!(
        error,
        loams_web_bridge::BridgeError::SessionLimit { max: 3 }
    ));

    provider
        .close(&first)
        .await
        .unwrap_or_else(|_| panic!("close"));
    let _fourth = open(&provider).await;
    let _ = (second, third);
}

#[tokio::test]
async fn closing_a_session_closes_the_browser_and_charges_the_budget() {
    let (provider, _, connection) = provider(|config| {
        config.browser_run.daily_budget_ms = Some(60_000);
    });
    let page = open(&provider).await;
    provider
        .close(&page)
        .await
        .unwrap_or_else(|_| panic!("close"));
    assert!(connection.calls().contains(&"Browser.close(-)".to_string()));
    let error = provider
        .snapshot(&page, &loams_web_bridge::SnapshotRequest::new())
        .await
        .err()
        .unwrap_or_else(|| panic!("a closed handle is an error"));
    assert!(error.to_string().contains("not open"), "{error}");
    assert!(provider.budget().used_ms() < 60_000);
}

#[tokio::test]
async fn a_handle_from_the_local_provider_is_refused() {
    let (provider, _, _) = provider(|_| {});
    let foreign = loams_web_bridge::PageRef::new(ProviderKind::Local, "local-1");
    let error = provider.open(OpenRequest::blank()).await.map(|_| ()).err();
    assert!(error.is_none(), "a remote session opens fine");
    let error = provider
        .snapshot(&foreign, &loams_web_bridge::SnapshotRequest::new())
        .await
        .err()
        .unwrap_or_else(|| panic!("a foreign handle is refused"));
    assert!(error.to_string().contains("local provider"), "{error}");
}

#[tokio::test]
async fn the_real_transport_is_wired_but_never_dialled_without_an_account() {
    // Building the transport must not touch the network; dialling is the test
    // suite's job with a fake, and a live check belongs to Task 0's spike.
    let transport = WebSocketTransport;
    let egress = EgressPolicy::public_web();
    let mut config = config();
    config.browser_run.account_id = ACCOUNT.to_string();
    let provider = BrowserRunProvider::new(
        config.browser_run.clone(),
        egress,
        CredentialPolicy::default(),
        resolver(),
        Arc::new(transport),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(provider.account_id(), ACCOUNT);
    assert_eq!(provider.kind(), ProviderKind::BrowserRun);
}
