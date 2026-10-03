//! Building the Browser Run request: the WebSocket endpoint, the headers, and
//! the guardrails policy.
//!
//! Facts read on Cloudflare's own documentation on 2026-10-03
//! (<https://developers.cloudflare.com/browser-run/cdp/>):
//!
//! * `wss://api.cloudflare.com/client/v4/accounts/{account_id}/browser-run/devtools/browser`
//!   acquires a session and speaks CDP over the same socket. Some older pages
//!   and the Puppeteer examples use the `browser-rendering` path, which is the
//!   same endpoint under its former product name; we send `browser-run`.
//! * `keep_alive` is the session lifetime in milliseconds and defaults to
//!   60 000. The page's table allows 10 000 to 1 200 000; its FAQ and limits
//!   page say ten minutes. We cap at ten minutes, which both readings accept.
//! * `browser=kitesurf` selects Kitesurf and **must not** be combined with
//!   `keep_alive`, `lab` or `recording`.
//! * `cf-brapi-guardrails` carries a base64url-encoded JSON object with
//!   `allowedDomains` (50 at most) and `allowedDomainSets` (four at most). It
//!   is **not supported on Kitesurf**.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::config::Engine;
use crate::egress::{Guardrails, MAX_ALLOWED_DOMAIN_SETS, MAX_ALLOWED_DOMAINS};
use crate::error::{BridgeError, ConfigError};
use crate::secret::SecretValue;

/// The header Cloudflare reads the session guardrails from.
pub const GUARDRAILS_HEADER: &str = "cf-brapi-guardrails";

/// The authorization header name.
pub const AUTHORIZATION_HEADER: &str = "Authorization";

/// The CDP WebSocket endpoint for an account.
pub fn websocket_endpoint(
    account_id: &str,
    engine: Engine,
    keep_alive: Option<Duration>,
) -> String {
    let mut endpoint = format!(
        "wss://api.cloudflare.com/client/v4/accounts/{account_id}/browser-run/devtools/browser"
    );
    let mut query: Vec<String> = Vec::new();
    if let Some(keep_alive) = keep_alive {
        query.push(format!("keep_alive={}", keep_alive.as_millis()));
    }
    if let Some(browser) = engine.browser_param() {
        query.push(format!("browser={browser}"));
    }
    if !query.is_empty() {
        endpoint.push('?');
        endpoint.push_str(&query.join("&"));
    }
    endpoint
}

/// The headers of the WebSocket handshake.
///
/// `Debug` is written by hand: the authorization header is the API token, and
/// a `Debug` of a header list must never be able to print it.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RequestHeaders(Vec<(String, String)>);

impl RequestHeaders {
    /// The headers as pairs, for a transport that wants them.
    pub fn as_pairs(&self) -> &[(String, String)] {
        &self.0
    }

    /// Whether a header is present.
    pub fn contains(&self, name: &str) -> bool {
        self.0.iter().any(|(key, _)| key == name)
    }
}

impl std::fmt::Debug for RequestHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (name, _) in &self.0 {
            map.entry(&name, &crate::redact::REDACTED);
        }
        map.finish()
    }
}

/// The handshake headers: the bearer token, and guardrails when there is an
/// allow list and the engine can enforce them.
pub fn headers(token: &SecretValue, guardrails: Option<&Guardrails>) -> RequestHeaders {
    let mut headers = vec![(
        AUTHORIZATION_HEADER.to_string(),
        format!("Bearer {}", token.expose()),
    )];
    if let Some(guardrails) = guardrails {
        headers.push((GUARDRAILS_HEADER.to_string(), encode_guardrails(guardrails)));
    }
    RequestHeaders(headers)
}

/// Encode a guardrails policy the way Cloudflare reads it.
pub fn encode_guardrails(guardrails: &Guardrails) -> String {
    let json = serde_json::to_string(guardrails).unwrap_or_else(|_| "{}".to_string());
    URL_SAFE_NO_PAD.encode(json)
}

/// Check a policy against Cloudflare's own limits before sending it, so a
/// mistake is a local error instead of a `400` from the API.
pub fn check_guardrails(guardrails: &Guardrails) -> Result<(), ConfigError> {
    if guardrails.allowed_domains.len() > MAX_ALLOWED_DOMAINS {
        return Err(ConfigError::Invalid {
            field: "egress.allowed_domains",
            reason: format!("Browser Run accepts at most {MAX_ALLOWED_DOMAINS}"),
        });
    }
    if guardrails.allowed_domain_sets.len() > MAX_ALLOWED_DOMAIN_SETS {
        return Err(ConfigError::Invalid {
            field: "egress.allowed_domain_sets",
            reason: format!("Browser Run accepts at most {MAX_ALLOWED_DOMAIN_SETS}"),
        });
    }
    for domain in &guardrails.allowed_domains {
        crate::egress::HostPattern::parse(domain)?;
    }
    Ok(())
}

/// Refuse the combinations Cloudflare's documentation rules out.
pub fn check_endpoint(engine: Engine, keep_alive: Option<Duration>) -> Result<(), BridgeError> {
    if engine == Engine::Kitesurf && keep_alive.is_some() {
        return Err(BridgeError::Unsupported {
            engine: "kitesurf",
            what: "browser=kitesurf must not be combined with keep_alive".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chromium_sends_keep_alive_and_no_browser_parameter() {
        let endpoint = websocket_endpoint("acct", Engine::Chromium, Some(Duration::from_secs(60)));
        assert_eq!(
            endpoint,
            "wss://api.cloudflare.com/client/v4/accounts/acct/browser-run/devtools/browser\
             ?keep_alive=60000"
        );
    }

    #[test]
    fn kitesurf_selects_the_engine_and_never_sends_keep_alive() {
        let endpoint = websocket_endpoint("acct", Engine::Kitesurf, None);
        assert!(endpoint.ends_with("?browser=kitesurf"), "{endpoint}");
        assert!(!endpoint.contains("keep_alive"), "{endpoint}");
        assert!(
            check_endpoint(Engine::Kitesurf, Some(Duration::from_secs(60))).is_err(),
            "keep_alive with kitesurf is refused"
        );
    }

    #[test]
    fn the_handshake_headers_hide_the_token() {
        let token = SecretValue::new("cf-api-token-0123456789abcdef");
        let guardrails = Guardrails {
            allowed_domains: vec!["example.com".to_string()],
            allowed_domain_sets: Vec::new(),
        };
        let headers = headers(&token, Some(&guardrails));
        let rendered = format!("{headers:?}");
        assert!(!rendered.contains("0123456789abcdef"), "{rendered}");
        assert!(rendered.contains("cf-brapi-guardrails"), "{rendered}");
    }

    #[test]
    fn guardrails_are_base64url_without_padding() {
        let guardrails = Guardrails {
            allowed_domains: vec!["example.com".to_string()],
            allowed_domain_sets: vec!["common-cdns".to_string()],
        };
        let encoded = encode_guardrails(&guardrails);
        assert!(!encoded.contains('='), "{encoded}");
        assert!(!encoded.contains('+'), "{encoded}");
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .unwrap_or_else(|_| panic!("the encoding round-trips"));
        let json = String::from_utf8(decoded).unwrap_or_else(|_| panic!("utf-8"));
        assert!(json.contains("example.com"), "{json}");
        assert!(json.contains("common-cdns"), "{json}");
    }
}
