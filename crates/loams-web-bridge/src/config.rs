//! The configuration surface of the web bridge: which provider runs, how the
//! remote provider reaches Browser Run, what it is allowed to open, and how
//! much browser time it may spend.
//!
//! Everything has a default, and the defaults are the conservative ones: the
//! remote provider is **off** until it is configured, the engine is
//! **Chromium**, private hosts are refused, and a session under an allow list
//! may not run on an engine that cannot enforce the allow list.

use std::path::PathBuf;
use std::time::Duration;

use crate::egress::{EgressPolicy, HostPattern, MAX_ALLOWED_DOMAINS};
use crate::error::ConfigError;
use crate::secret::SecretRef;

/// The provider the bridge uses (D565: the owner picks, configuration decides).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// The person's own machine runs the browser (the desktop bridge, or a
    /// headless local driver).
    #[default]
    Local,
    /// Cloudflare Browser Run, over its CDP endpoint (D566).
    BrowserRun,
}

impl ProviderKind {
    /// The name as written in configuration.
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderKind::Local => "local",
            ProviderKind::BrowserRun => "browser-run",
        }
    }
}

/// Which browser pool a Browser Run session asks for.
///
/// Chromium is the default and the right answer for anything that needs
/// state, a real TLS stack or pixel rendering. Kitesurf is a stateless Workers
/// engine that is cheap and fast for a throwaway render, and Cloudflare's own
/// documentation rules it out for a long-running authenticated session
/// (read on 2026-10-03).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Engine {
    /// Browser Run's default Chromium-backed pool. Stateful within a session.
    #[default]
    Chromium,
    /// Cloudflare's Kitesurf: a Workers V8 isolate, ephemeral and stateless.
    Kitesurf,
}

impl Engine {
    /// The value of the `browser` query parameter, if any. Omitting the
    /// parameter is what selects the default pool, so `Chromium` sends none.
    pub fn browser_param(&self) -> Option<&'static str> {
        match self {
            Engine::Chromium => None,
            Engine::Kitesurf => Some("kitesurf"),
        }
    }

    /// The engine's name, for messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            Engine::Chromium => "chromium",
            Engine::Kitesurf => "kitesurf",
        }
    }
}

/// The Cloudflare plan the account is on. It only sets defaults; every one of
/// them can be overridden, because Cloudflare can raise an account's limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Plan {
    /// Workers Free: 10 minutes of browser time a day, 3 concurrent browsers,
    /// one new browser every 20 seconds, a 60 second idle timeout.
    #[default]
    Free,
    /// Workers Paid: billed browser hours and concurrent browsers.
    Paid,
}

/// The lowest `keep_alive` Browser Run accepts, in milliseconds (read on
/// 2026-10-03).
pub const KEEP_ALIVE_MIN_MS: u64 = 10_000;

/// The highest `keep_alive` this provider sends, in milliseconds.
///
/// The CDP page's table says the parameter accepts up to 1 200 000 ms; the
/// same page's FAQ and limits page say ten minutes. Ten minutes is accepted by
/// both readings, so that is what we send (Q567).
pub const KEEP_ALIVE_MAX_MS: u64 = 600_000;

/// Free tier: 10 minutes of browser time per day.
pub const FREE_DAILY_BUDGET_MS: u64 = 600_000;

/// Free tier: one new browser every 20 seconds.
pub const FREE_MIN_SESSION_INTERVAL_MS: u64 = 20_000;

/// Free tier: 3 concurrent browsers.
pub const FREE_MAX_CONCURRENT_SESSIONS: usize = 3;

/// Paid tier: 10 concurrent browsers are included, and the account may ask for
/// more. The default matches the included number.
pub const PAID_MAX_CONCURRENT_SESSIONS: usize = 10;

/// How the remote provider reaches Browser Run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserRunConfig {
    /// The Cloudflare account id. Not a secret, but never a good default:
    /// an unset account is an error rather than a guess.
    #[serde(default)]
    pub account_id: String,

    /// Which pool to ask for.
    #[serde(default)]
    pub engine: Engine,

    /// The plan the account is on; it fills the unset limits below.
    #[serde(default)]
    pub plan: Plan,

    /// Where the API token lives. The token itself never appears in
    /// configuration, in a log or in an error.
    #[serde(default)]
    pub token_secret_ref: Option<String>,

    /// How long one browser session may idle, in milliseconds. `None` uses the
    /// plan default; Kitesurf must leave it unset.
    #[serde(default)]
    pub keep_alive_ms: Option<u64>,

    /// How long one tool call may take before it is abandoned, in
    /// milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// How many browser sessions may be open at once.
    #[serde(default)]
    pub max_concurrent_sessions: Option<usize>,

    /// The shortest gap between two new sessions, in milliseconds.
    #[serde(default)]
    pub min_session_interval_ms: Option<u64>,

    /// The local ceiling on browser time per UTC day, in milliseconds. `0`
    /// turns the local guard off; Cloudflare still enforces its own.
    #[serde(default)]
    pub daily_budget_ms: Option<u64>,

    /// Where screenshots and downloads are written.
    #[serde(default)]
    pub artifact_dir: Option<PathBuf>,
}

fn default_request_timeout_ms() -> u64 {
    60_000
}

impl Default for BrowserRunConfig {
    fn default() -> Self {
        Self {
            account_id: String::new(),
            engine: Engine::default(),
            plan: Plan::default(),
            token_secret_ref: None,
            keep_alive_ms: None,
            request_timeout_ms: default_request_timeout_ms(),
            max_concurrent_sessions: None,
            min_session_interval_ms: None,
            daily_budget_ms: None,
            artifact_dir: None,
        }
    }
}

impl BrowserRunConfig {
    /// The `keep_alive` to send, if any.
    pub fn keep_alive(&self) -> Option<Duration> {
        match self.engine {
            Engine::Kitesurf => None,
            Engine::Chromium => Some(Duration::from_millis(self.keep_alive_ms.unwrap_or(
                match self.plan {
                    Plan::Free => 60_000,
                    Plan::Paid => KEEP_ALIVE_MAX_MS,
                },
            ))),
        }
    }

    /// The concurrency ceiling.
    pub fn max_concurrent_sessions(&self) -> usize {
        self.max_concurrent_sessions.unwrap_or(match self.plan {
            Plan::Free => FREE_MAX_CONCURRENT_SESSIONS,
            Plan::Paid => PAID_MAX_CONCURRENT_SESSIONS,
        })
    }

    /// The shortest gap between new sessions.
    pub fn min_session_interval(&self) -> Option<Duration> {
        self.min_session_interval_ms.map(Duration::from_millis).or({
            match self.plan {
                Plan::Free => Some(Duration::from_millis(FREE_MIN_SESSION_INTERVAL_MS)),
                Plan::Paid => None,
            }
        })
    }

    /// The local daily ceiling, if there is one.
    pub fn daily_budget(&self) -> Option<Duration> {
        self.daily_budget_ms
            .map(Duration::from_millis)
            .or(match self.plan {
                Plan::Free => Some(Duration::from_millis(FREE_DAILY_BUDGET_MS)),
                Plan::Paid => None,
            })
    }

    /// Where artifacts are written.
    pub fn artifacts_dir(&self) -> PathBuf {
        self.artifact_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("loams-web-bridge").join("remote"))
    }

    /// The timeout for one tool call.
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    /// The parsed token reference.
    pub fn token_ref(&self) -> Result<Option<SecretRef>, ConfigError> {
        match &self.token_secret_ref {
            None => Ok(None),
            Some(raw) => SecretRef::parse(raw)
                .map(Some)
                .map_err(|error| ConfigError::Invalid {
                    field: "browser_run.token_secret_ref",
                    reason: error.to_string(),
                }),
        }
    }
}

/// Which secret fills the bridge may make, and where (D567).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialPolicy {
    /// Whether `fill` may resolve a secret at all on this provider. Off by
    /// default, and **always off on the remote provider**: a remote browser is
    /// a third party and never receives a person's credential. What the remote
    /// provider may do is fill a *service-account* secret into an allow-listed
    /// host, and only when the host list below says so.
    #[serde(default)]
    pub allow_secret_fills: bool,

    /// The hosts a service-account secret may be typed into. Empty means none.
    #[serde(default)]
    pub allowed_fill_hosts: Vec<String>,
}

impl CredentialPolicy {
    /// Whether a secret may be typed into `host`.
    pub fn allows_fill_on(&self, host: &str) -> bool {
        self.allowed_fill_hosts
            .iter()
            .filter_map(|raw| HostPattern::parse(raw).ok())
            .any(|pattern| pattern.matches(host))
    }
}

/// The local (agent-driven) provider's settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// The profile a session opens by default, named `<environment>/<site>` as
    /// §18.14.4 D506 has it.
    #[serde(default)]
    pub default_profile: Option<String>,

    /// Where screenshots and downloads are written.
    #[serde(default)]
    pub artifact_dir: Option<PathBuf>,
}

impl LocalConfig {
    /// Where artifacts go when nothing says otherwise.
    pub fn artifacts_dir(&self) -> PathBuf {
        self.artifact_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("loams-web-bridge").join("local"))
    }
}

/// The whole `[web_bridge]` section.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebBridgeConfig {
    /// Which provider to build.
    #[serde(default)]
    pub provider: ProviderKind,

    /// The local provider's settings.
    #[serde(default)]
    pub local: LocalConfig,

    /// The remote provider's settings.
    #[serde(default)]
    pub browser_run: BrowserRunConfig,

    /// Hostnames to allow or refuse.
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    /// Cloudflare domain sets to allow.
    #[serde(default)]
    pub allowed_domain_sets: Vec<String>,
    /// Hostnames to refuse even if allowed above.
    #[serde(default)]
    pub denied_domains: Vec<String>,
    /// Permit private and loopback addresses. Development only.
    #[serde(default)]
    pub allow_private_hosts: bool,
    /// Permit an engine that cannot enforce guardrails to run under an allow
    /// list. Development only.
    #[serde(default)]
    pub allow_unenforced_guardrails: bool,

    /// Which secret fills are permitted.
    #[serde(default)]
    pub credentials: CredentialPolicy,
}

impl WebBridgeConfig {
    /// A configuration with the local provider and no remote credentials: the
    /// default, and the one a fresh install gets.
    pub fn local_default() -> Self {
        Self::default()
    }

    /// The egress policy the configuration describes.
    pub fn egress(&self) -> Result<EgressPolicy, ConfigError> {
        let mut policy = EgressPolicy::public_web();
        policy.set_allow_private_hosts(self.allow_private_hosts);
        policy.set_allow_unenforced_guardrails(self.allow_unenforced_guardrails);
        for host in &self.allowed_domains {
            policy.allow(host)?;
        }
        for set in &self.allowed_domain_sets {
            policy.allow_domain_set(set.clone())?;
        }
        for host in &self.denied_domains {
            policy.deny(host)?;
        }
        Ok(policy)
    }

    /// Whether the remote provider may run at all.
    pub fn is_remote_enabled(&self) -> bool {
        self.provider == ProviderKind::BrowserRun
    }

    /// Read a configuration from TOML.
    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        toml::from_str(raw).map_err(|error| ConfigError::Toml(error.to_string()))
    }

    /// Overlay `LOAMS_WEB_BRIDGE_*` environment variables.
    pub fn apply_env(&mut self) -> Result<(), ConfigError> {
        self.apply_env_from(&|name| env_var(name))
    }

    /// Overlay environment variables from an arbitrary lookup.
    ///
    /// The environment names the account and the engine; it never names a
    /// token. The token is a reference resolved through the host's secret
    /// store, so an environment dump cannot leak it.
    pub fn apply_env_from(
        &mut self,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigError> {
        if let Some(raw) = lookup("LOAMS_WEB_BRIDGE_PROVIDER") {
            self.provider = match raw.as_str() {
                "local" => ProviderKind::Local,
                "browser-run" => ProviderKind::BrowserRun,
                other => {
                    return Err(ConfigError::Invalid {
                        field: "LOAMS_WEB_BRIDGE_PROVIDER",
                        reason: format!("{other:?} is not local or browser-run"),
                    });
                }
            };
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_ACCOUNT_ID") {
            self.browser_run.account_id = raw;
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_ENGINE") {
            self.browser_run.engine = match raw.as_str() {
                "chromium" | "default" => Engine::Chromium,
                "kitesurf" => Engine::Kitesurf,
                other => {
                    return Err(ConfigError::Invalid {
                        field: "LOAMS_WEB_BROWSER_RUN_ENGINE",
                        reason: format!("{other:?} is not chromium or kitesurf"),
                    });
                }
            };
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_TOKEN_SECRET_REF") {
            self.browser_run.token_secret_ref = Some(raw);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_KEEP_ALIVE_MS") {
            self.browser_run.keep_alive_ms =
                Some(parse_u64("LOAMS_WEB_BROWSER_RUN_KEEP_ALIVE_MS", &raw)?);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_MAX_SESSIONS") {
            self.browser_run.max_concurrent_sessions =
                Some(parse_u64("LOAMS_WEB_BROWSER_RUN_MAX_SESSIONS", &raw)? as usize);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BROWSER_RUN_DAILY_BUDGET_MS") {
            self.browser_run.daily_budget_ms =
                Some(parse_u64("LOAMS_WEB_BROWSER_RUN_DAILY_BUDGET_MS", &raw)?);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BRIDGE_ARTIFACT_DIR") {
            self.browser_run.artifact_dir = Some(PathBuf::from(raw));
        }
        if let Some(raw) = lookup("LOAMS_WEB_BRIDGE_ALLOWED_DOMAINS") {
            self.allowed_domains = split_list(&raw);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BRIDGE_ALLOWED_DOMAIN_SETS") {
            self.allowed_domain_sets = split_list(&raw);
        }
        if let Some(raw) = lookup("LOAMS_WEB_BRIDGE_DENIED_DOMAINS") {
            self.denied_domains = split_list(&raw);
        }
        Ok(())
    }

    /// Read the configuration from TOML, overlay the environment and check it.
    pub fn load(raw_toml: &str) -> Result<Self, ConfigError> {
        let mut config = Self::from_toml_str(raw_toml)?;
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }

    /// Check everything the provider will need, so a mistake is a startup
    /// error rather than a failed session.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let policy = self.egress()?;
        if self.allowed_domains.len() > MAX_ALLOWED_DOMAINS {
            return Err(ConfigError::Invalid {
                field: "allowed_domains",
                reason: format!("at most {MAX_ALLOWED_DOMAINS} are accepted"),
            });
        }
        for host in &self.credentials.allowed_fill_hosts {
            HostPattern::parse(host).map_err(|error| ConfigError::Invalid {
                field: "credentials.allowed_fill_hosts",
                reason: error.to_string(),
            })?;
        }
        if self.credentials.allow_secret_fills && self.credentials.allowed_fill_hosts.is_empty() {
            return Err(ConfigError::Invalid {
                field: "credentials.allowed_fill_hosts",
                reason: "listing no host means no host may be typed into; \
                         allow_secret_fills has nothing to allow"
                    .to_string(),
            });
        }
        if self.provider != ProviderKind::BrowserRun {
            return Ok(());
        }

        let run = &self.browser_run;
        if run.account_id.trim().is_empty() {
            return Err(ConfigError::Missing {
                field: "browser_run.account_id",
            });
        }
        if !is_account_id(&run.account_id) {
            return Err(ConfigError::Invalid {
                field: "browser_run.account_id",
                reason: "a Cloudflare account id is 32 hexadecimal characters".to_string(),
            });
        }
        if run.token_ref()?.is_none() {
            return Err(ConfigError::Missing {
                field: "browser_run.token_secret_ref",
            });
        }
        if run.engine == Engine::Kitesurf && run.keep_alive_ms.is_some() {
            return Err(ConfigError::Unsupported {
                field: "browser_run.keep_alive_ms",
                reason: "Browser Run documents that browser=kitesurf must not be combined with \
                         keep_alive, lab or recording"
                    .to_string(),
            });
        }
        if let Some(keep_alive) = run.keep_alive_ms
            && !(KEEP_ALIVE_MIN_MS..=KEEP_ALIVE_MAX_MS).contains(&keep_alive)
        {
            return Err(ConfigError::Invalid {
                field: "browser_run.keep_alive_ms",
                reason: format!("must be between {KEEP_ALIVE_MIN_MS} and {KEEP_ALIVE_MAX_MS}"),
            });
        }
        if run.engine == Engine::Kitesurf
            && policy.is_allow_listed()
            && !policy.allows_unenforced_engine()
        {
            return Err(ConfigError::Unsupported {
                field: "browser_run.engine",
                reason: "Browser Run guardrails are not supported on Kitesurf, so an allow list \
                         cannot be enforced for the session's sub-resources and redirects; use the \
                         chromium engine, or set allow_unenforced_guardrails to accept that"
                    .to_string(),
            });
        }
        if run.max_concurrent_sessions() == 0 {
            return Err(ConfigError::Invalid {
                field: "browser_run.max_concurrent_sessions",
                reason: "must be at least 1".to_string(),
            });
        }
        if run.request_timeout_ms < 1_000 {
            return Err(ConfigError::Invalid {
                field: "browser_run.request_timeout_ms",
                reason: "must be at least 1000".to_string(),
            });
        }
        Ok(())
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_u64(field: &'static str, raw: &str) -> Result<u64, ConfigError> {
    raw.trim().parse::<u64>().map_err(|_| ConfigError::Invalid {
        field,
        reason: format!("{raw:?} is not a number"),
    })
}

/// A Cloudflare account id is 32 hexadecimal characters. Checking it here
/// catches the common mistake of pasting the API token where the account goes.
fn is_account_id(raw: &str) -> bool {
    raw.len() == 32 && raw.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_the_local_provider_and_no_remote_credentials() {
        let config = WebBridgeConfig::default();
        assert_eq!(config.provider, ProviderKind::Local);
        assert_eq!(config.browser_run.engine, Engine::Chromium);
        assert!(!config.is_remote_enabled());
        config.validate().unwrap_or_else(|error| panic!("{error}"));
    }

    #[test]
    fn the_free_plan_supplies_its_own_limits() {
        let run = BrowserRunConfig::default();
        assert_eq!(run.max_concurrent_sessions(), FREE_MAX_CONCURRENT_SESSIONS);
        assert_eq!(run.daily_budget(), Some(Duration::from_millis(600_000)));
    }

    #[test]
    fn a_remote_configuration_needs_an_account_and_a_token_reference() {
        let mut config = WebBridgeConfig {
            provider: ProviderKind::BrowserRun,
            ..WebBridgeConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Missing {
                field: "browser_run.account_id"
            })
        ));
        config.browser_run.account_id = "0123456789abcdef0123456789abcdef".to_string();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Missing {
                field: "browser_run.token_secret_ref"
            })
        ));
    }

    #[test]
    fn an_api_token_pasted_as_an_account_is_refused() {
        let mut config = WebBridgeConfig {
            provider: ProviderKind::BrowserRun,
            ..WebBridgeConfig::default()
        };
        config.browser_run.account_id =
            "v1.0-abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string();
        config.browser_run.token_secret_ref = Some("env:cloudflare#api_token".to_string());
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid {
                field: "browser_run.account_id",
                ..
            })
        ));
    }

    fn remote_config() -> WebBridgeConfig {
        let mut config = WebBridgeConfig {
            provider: ProviderKind::BrowserRun,
            ..WebBridgeConfig::default()
        };
        config.browser_run.account_id = "0123456789abcdef0123456789abcdef".to_string();
        config.browser_run.token_secret_ref = Some("env:cloudflare#api_token".to_string());
        config
    }

    #[test]
    fn kitesurf_may_not_carry_keep_alive() {
        let mut config = remote_config();
        config.browser_run.engine = Engine::Kitesurf;
        config.browser_run.keep_alive_ms = Some(60_000);
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Unsupported {
                field: "browser_run.keep_alive_ms",
                ..
            })
        ));
    }
}
