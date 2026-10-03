//! The configuration surface: what TOML and the environment may set, and what
//! is refused.
use std::collections::HashMap;

use loams_web_bridge::{ConfigError, Engine, Plan, ProviderKind, WebBridgeConfig};

const REMOTE_TOML: &str = r#"
provider = "browser-run"

allowed_domains = ["*.example.com"]
allowed_domain_sets = ["common-cdns"]

[browser_run]
account_id = "0123456789abcdef0123456789abcdef"
token_secret_ref = "env:cloudflare#api_token"
plan = "free"

[local]
default_profile = "acme-staging/zulip"
"#;

#[test]
fn a_toml_section_becomes_a_checked_configuration() {
    let config = WebBridgeConfig::load(REMOTE_TOML).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(config.provider, ProviderKind::BrowserRun);
    assert_eq!(config.browser_run.engine, Engine::Chromium);
    assert_eq!(config.browser_run.plan, Plan::Free);
    assert_eq!(
        config.browser_run.token_secret_ref.as_deref(),
        Some("env:cloudflare#api_token")
    );
    assert_eq!(
        config.local.default_profile.as_deref(),
        Some("acme-staging/zulip")
    );

    let egress = config.egress().unwrap_or_else(|error| panic!("{error}"));
    assert!(egress.is_allow_listed());
    let guardrails = egress.guardrails().unwrap_or_else(|| panic!("guardrails"));
    assert_eq!(guardrails.allowed_domains, ["*.example.com"]);
    assert_eq!(guardrails.allowed_domain_sets, ["common-cdns"]);
}

#[test]
fn the_environment_names_the_account_and_the_engine_but_never_the_token() {
    let environment: HashMap<&str, &str> = HashMap::from([
        (
            "LOAMS_WEB_BROWSER_RUN_ACCOUNT_ID",
            "abcdef0123456789abcdef0123456789",
        ),
        ("LOAMS_WEB_BROWSER_RUN_ENGINE", "kitesurf"),
        (
            "LOAMS_WEB_BROWSER_RUN_TOKEN_SECRET_REF",
            "env:cloudflare#api_token",
        ),
    ]);
    let mut config = WebBridgeConfig::default();
    config
        .apply_env_from(&|name| environment.get(name).map(|value| (*value).to_string()))
        .unwrap_or_else(|error| panic!("{error}"));

    assert_eq!(
        config.browser_run.account_id,
        "abcdef0123456789abcdef0123456789"
    );
    assert_eq!(config.browser_run.engine, Engine::Kitesurf);
    assert_eq!(
        config.browser_run.token_secret_ref.as_deref(),
        Some("env:cloudflare#api_token"),
        "only a reference, never a token"
    );
    assert!(
        environment
            .keys()
            .all(|name| !name.to_string().contains("TOKEN_VALUE")),
        "the environment has no place for a token value"
    );
}

#[test]
fn an_unknown_field_is_a_typo_not_a_silent_default() {
    let error = WebBridgeConfig::load("provider = \"browser-run\"\nacount-id = \"x\"")
        .err()
        .unwrap_or_else(|| panic!("an unknown field is refused"));
    assert!(matches!(error, ConfigError::Toml(_)), "{error:?}");
}

#[test]
fn kitesurf_under_an_allow_list_is_refused_because_guardrails_are_unavailable() {
    let mut config = WebBridgeConfig::load(REMOTE_TOML).unwrap_or_else(|error| panic!("{error}"));
    config.browser_run.engine = Engine::Kitesurf;
    let error = config.validate().err().unwrap_or_else(|| panic!("refused"));
    assert!(
        matches!(
            error,
            ConfigError::Unsupported {
                field: "browser_run.engine",
                ..
            }
        ),
        "{error:?}"
    );
    assert!(error.to_string().contains("guardrails"), "{error}");
}

#[test]
fn an_operator_may_accept_an_unenforced_engine_explicitly() {
    let mut config = WebBridgeConfig::load(REMOTE_TOML).unwrap_or_else(|error| panic!("{error}"));
    config.browser_run.engine = Engine::Kitesurf;
    config.allow_unenforced_guardrails = true;
    config
        .validate()
        .unwrap_or_else(|error| panic!("explicitly accepted: {error}"));
}

#[test]
fn keep_alive_is_bounded_and_never_combined_with_kitesurf() {
    let mut config = WebBridgeConfig::load(REMOTE_TOML).unwrap_or_else(|error| panic!("{error}"));
    config.browser_run.keep_alive_ms = Some(9_999);
    assert!(config.validate().is_err(), "below Browser Run's floor");

    config.browser_run.keep_alive_ms = Some(600_000);
    config.validate().unwrap_or_else(|error| panic!("{error}"));

    config.browser_run.engine = Engine::Kitesurf;
    config.allow_unenforced_guardrails = true;
    assert!(config.validate().is_err(), "kitesurf takes no keep_alive");
}

#[test]
fn the_free_plan_supplies_its_documented_limits() {
    let config = WebBridgeConfig::load(REMOTE_TOML).unwrap_or_else(|error| panic!("{error}"));
    let run = &config.browser_run;
    assert_eq!(run.max_concurrent_sessions(), 3);
    assert_eq!(
        run.min_session_interval(),
        Some(std::time::Duration::from_millis(20_000))
    );
    assert_eq!(
        run.daily_budget(),
        Some(std::time::Duration::from_millis(600_000))
    );
    assert_eq!(
        run.keep_alive(),
        Some(std::time::Duration::from_millis(60_000))
    );
}

#[test]
fn a_secret_fill_needs_a_host_to_be_allowed_on() {
    let mut config = WebBridgeConfig::default();
    config.credentials.allow_secret_fills = true;
    assert!(
        config.validate().is_err(),
        "an empty host list allows nothing"
    );

    config.credentials.allowed_fill_hosts = vec!["app.example.com".to_string()];
    config.validate().unwrap_or_else(|error| panic!("{error}"));
    assert!(config.credentials.allows_fill_on("app.example.com"));
    assert!(!config.credentials.allows_fill_on("evil.example.com"));
}
