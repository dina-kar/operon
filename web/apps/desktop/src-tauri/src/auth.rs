//! Sign-in to a Loams instance through Authentik (§37 §6.5, D431, AP1
//! Task 7, Rulings 7–8; §38 for Authentik itself).
//!
//! 1. Discovery: the instance's `GetInstance` names the Authentik issuer and
//!    the public client id (`loams-desktop`), and the Loams gateway
//!    (`issuer`) whose token endpoint does the exchange.
//! 2. OIDC authorization code with PKCE (S256) in the **system browser**,
//!    redirecting to `http://127.0.0.1:<port 0>/callback` (RFC 8252 §7.3).
//!    The listener accepts exactly one request, checks `state`, answers a
//!    static page and closes. No embedded webview sign-in, ever.
//! 3. The Authentik token is exchanged at the Loams gateway (RFC 8693) for
//!    Loams's own access and refresh tokens; Authentik's tokens are dropped.
//! 4. The refresh token goes to the OS keychain; the access token stays in
//!    Rust memory (the bridge adds it); no command returns either.
//!
//! The unified auth plan implements the gateway's exchange (Q438); until
//! then this runs against mocks (the tests play Authentik and the gateway).

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use url::Url;

use crate::keychain::{KeyStore, account};

/// The public OAuth client of Loams Desktop at every instance's Authentik.
pub const CLIENT_ID: &str = "loams-desktop";
/// How long the browser step may take.
pub const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("this instance offers no Authentik sign-in")]
    NoSignIn,
    #[error("discovery failed: {0}")]
    Discovery(String),
    #[error("the sign-in callback was invalid: {0}")]
    Callback(&'static str),
    #[error("the browser sign-in timed out")]
    Timeout,
    #[error("the token endpoint refused: {0}")]
    Token(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("keychain: {0}")]
    Keychain(#[from] crate::keychain::KeyError),
}

/// A PKCE verifier and its S256 challenge (RFC 7636).
#[derive(Debug, Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

impl Pkce {
    #[must_use]
    pub fn new() -> Self {
        Self::from_verifier(random_token())
    }

    #[must_use]
    pub fn from_verifier(verifier: String) -> Self {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Self {
            verifier,
            challenge,
        }
    }
}

impl Default for Pkce {
    fn default() -> Self {
        Self::new()
    }
}

/// The instance's sign-in configuration, from `GetInstance` (Connect JSON).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceAuth {
    pub instance_id: String,
    /// The Loams gateway (the authorization server for Loams tokens).
    pub gateway: Url,
    /// The Authentik issuer for this app.
    pub authentik_issuer: Url,
    pub client_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetInstanceJson {
    #[serde(default)]
    instance_id: String,
    #[serde(default)]
    issuer: String,
    #[serde(default)]
    sign_in_methods: Vec<SignInMethodJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignInMethodJson {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    issuer: String,
    #[serde(default)]
    client_id: String,
}

/// Reads `GetInstance` over Connect's JSON codec (`POST`, `{}`).
///
/// # Errors
///
/// An unreachable instance, or one without Authentik sign-in.
pub async fn discover_instance(
    client: &reqwest::Client,
    base: &Url,
) -> Result<InstanceAuth, AuthError> {
    let url = base
        .join("loams.instance.v1.InstanceService/GetInstance")
        .map_err(|e| AuthError::Discovery(e.to_string()))?;
    let info: GetInstanceJson = client
        .post(url)
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .body("{}")
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| AuthError::Discovery(e.to_string()))?
        .json()
        .await
        .map_err(|e| AuthError::Discovery(e.to_string()))?;
    let method = info
        .sign_in_methods
        .iter()
        .find(|m| m.kind == "SIGN_IN_KIND_AUTHENTIK" && !m.issuer.is_empty())
        .ok_or(AuthError::NoSignIn)?;
    let parse = |s: &str| Url::parse(s).map_err(|e| AuthError::Discovery(e.to_string()));
    Ok(InstanceAuth {
        instance_id: info.instance_id,
        gateway: parse(&info.issuer)?,
        authentik_issuer: parse(&method.issuer)?,
        client_id: if method.client_id.is_empty() {
            CLIENT_ID.into()
        } else {
            method.client_id.clone()
        },
    })
}

/// The OIDC endpoints of an issuer.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Oidc {
    pub authorization_endpoint: Url,
    pub token_endpoint: Url,
}

/// OIDC discovery (`/.well-known/openid-configuration`).
///
/// # Errors
///
/// An unreachable issuer or an invalid document.
pub async fn discover_oidc(client: &reqwest::Client, issuer: &Url) -> Result<Oidc, AuthError> {
    let mut url = issuer.clone();
    let path = format!(
        "{}/.well-known/openid-configuration",
        url.path().trim_end_matches('/')
    );
    url.set_path(&path);
    client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| AuthError::Discovery(e.to_string()))?
        .json()
        .await
        .map_err(|e| AuthError::Discovery(e.to_string()))
}

/// The authorization request (system browser). `max_age=0` forces a fresh
/// authentication for step-up (§37 §6.5).
#[must_use]
pub fn authorize_url(
    oidc: &Oidc,
    client_id: &str,
    redirect_uri: &str,
    pkce: &Pkce,
    state: &str,
    nonce: &str,
    step_up: bool,
) -> Url {
    let mut url = oidc.authorization_endpoint.clone();
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("scope", "openid profile email offline_access")
            .append_pair("code_challenge", &pkce.challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", state)
            .append_pair("nonce", nonce);
        if step_up {
            q.append_pair("max_age", "0");
        }
    }
    url
}

/// The loopback redirect listener (RFC 8252 §7.3).
#[derive(Debug)]
pub struct Loopback {
    listener: TcpListener,
    pub redirect_uri: String,
}

impl Loopback {
    /// Binds `127.0.0.1:0`.
    ///
    /// # Errors
    ///
    /// If loopback cannot be bound.
    pub async fn bind() -> Result<Self, AuthError> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let port = listener.local_addr()?.port();
        Ok(Self {
            listener,
            redirect_uri: format!("http://127.0.0.1:{port}/callback"),
        })
    }

    /// Accepts exactly one request; returns the code if its `state` matches.
    /// The connection is answered and closed either way.
    ///
    /// # Errors
    ///
    /// Wrong path, state mismatch, an `error` parameter, or the timeout.
    pub async fn wait(self, state: &str, timeout: Duration) -> Result<String, AuthError> {
        let (mut socket, _) = tokio::time::timeout(timeout, self.listener.accept())
            .await
            .map_err(|_| AuthError::Timeout)??;
        drop(self.listener);
        let mut buf = vec![0u8; 8192];
        let n = tokio::time::timeout(Duration::from_secs(10), socket.read(&mut buf))
            .await
            .map_err(|_| AuthError::Timeout)??;
        let request = String::from_utf8_lossy(&buf[..n]);
        let result = parse_callback(&request, state);
        let (status, body) = match &result {
            Ok(_) => ("200 OK", "Signed in to Loams. You can close this tab."),
            Err(_) => (
                "400 Bad Request",
                "Sign-in failed. Return to Loams and try again.",
            ),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: text/plain; charset=utf-8\r\ncache-control: no-store\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
        result
    }
}

/// Parses `GET /callback?code=…&state=… HTTP/1.1`.
fn parse_callback(request: &str, expected_state: &str) -> Result<String, AuthError> {
    let line = request
        .lines()
        .next()
        .ok_or(AuthError::Callback("empty request"))?;
    let mut parts = line.split_whitespace();
    if parts.next() != Some("GET") {
        return Err(AuthError::Callback("not a GET"));
    }
    let target = parts.next().ok_or(AuthError::Callback("no target"))?;
    let url = Url::parse(&format!("http://127.0.0.1{target}"))
        .map_err(|_| AuthError::Callback("bad target"))?;
    if url.path() != "/callback" {
        return Err(AuthError::Callback("wrong path"));
    }
    let query = |k: &str| {
        url.query_pairs()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.into_owned())
    };
    if query("error").is_some() {
        return Err(AuthError::Callback(
            "the identity provider returned an error",
        ));
    }
    if query("state").as_deref() != Some(expected_state) {
        return Err(AuthError::Callback("state mismatch"));
    }
    query("code")
        .filter(|c| !c.is_empty())
        .ok_or(AuthError::Callback("no code"))
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

async fn token_request(
    client: &reqwest::Client,
    endpoint: &Url,
    form: &[(&str, &str)],
) -> Result<TokenResponse, AuthError> {
    let response = client
        .post(endpoint.clone())
        .form(form)
        .send()
        .await
        .map_err(|e| AuthError::Token(e.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        // Never echo the body: it may contain tokens on some servers.
        return Err(AuthError::Token(format!("HTTP {status}")));
    }
    response
        .json()
        .await
        .map_err(|e| AuthError::Token(e.to_string()))
}

/// Loams's tokens after the exchange. Never serialized to JavaScript.
#[derive(Clone)]
pub struct LoamsTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: Option<u64>,
}

impl std::fmt::Debug for LoamsTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoamsTokens")
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

/// The gateway's token endpoint (§37 §7.2.2).
///
/// # Errors
///
/// A gateway URL that cannot be joined.
pub fn gateway_token_endpoint(gateway: &Url) -> Result<Url, AuthError> {
    let mut url = gateway.clone();
    let path = format!("{}/api/v1/oauth/token", url.path().trim_end_matches('/'));
    url.set_path(&path);
    Ok(url)
}

/// What the console learns about a sign-in: never a token.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SignedIn {
    pub instance_id: String,
    /// False when the keychain is unavailable (session-only sign-in).
    pub persistent: bool,
}

/// The whole flow. `open_browser` opens the authorization URL in the
/// system browser (the opener plugin; a test drives the callback itself).
///
/// # Errors
///
/// Any step of the flow; nothing is stored on failure.
pub async fn sign_in(
    client: &reqwest::Client,
    base: &Url,
    principal_hint: &str,
    keystore: &dyn KeyStore,
    open_browser: impl FnOnce(Url),
    step_up: bool,
    timeout: Duration,
) -> Result<(SignedIn, LoamsTokens), AuthError> {
    let instance = discover_instance(client, base).await?;
    let oidc = discover_oidc(client, &instance.authentik_issuer).await?;
    let pkce = Pkce::new();
    let state = random_token();
    let nonce = random_token();
    let loopback = Loopback::bind().await?;
    let url = authorize_url(
        &oidc,
        &instance.client_id,
        &loopback.redirect_uri,
        &pkce,
        &state,
        &nonce,
        step_up,
    );
    let redirect_uri = loopback.redirect_uri.clone();
    open_browser(url);
    let code = loopback.wait(&state, timeout).await?;
    let idp = token_request(
        client,
        &oidc.token_endpoint,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", &redirect_uri),
            ("client_id", &instance.client_id),
            ("code_verifier", &pkce.verifier),
        ],
    )
    .await?;
    // RFC 8693: Authentik's access token for Loams's tokens. Authentik's
    // own tokens are dropped here (`exchange_discards_idp_tokens`).
    let loams = token_request(
        client,
        &gateway_token_endpoint(&instance.gateway)?,
        &[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token", &idp.access_token),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("client_id", CLIENT_ID),
        ],
    )
    .await?;
    drop(idp);
    if let Some(refresh) = &loams.refresh_token {
        keystore.set(&account(&instance.instance_id, principal_hint), refresh)?;
    }
    Ok((
        SignedIn {
            instance_id: instance.instance_id,
            persistent: keystore.persistent(),
        },
        LoamsTokens {
            access_token: loams.access_token,
            refresh_token: loams.refresh_token,
            expires_in: loams.expires_in,
        },
    ))
}

/// Refreshes Loams's tokens, rotating the stored refresh token.
///
/// # Errors
///
/// No stored refresh token, or the gateway refuses.
pub async fn refresh(
    client: &reqwest::Client,
    gateway: &Url,
    instance_id: &str,
    principal: &str,
    keystore: &dyn KeyStore,
) -> Result<LoamsTokens, AuthError> {
    let key = account(instance_id, principal);
    let current = keystore
        .get(&key)?
        .ok_or(AuthError::Token("not signed in".into()))?;
    let answer = token_request(
        client,
        &gateway_token_endpoint(gateway)?,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", &current),
            ("client_id", CLIENT_ID),
        ],
    )
    .await?;
    if let Some(next) = &answer.refresh_token {
        keystore.set(&key, next)?;
    }
    Ok(LoamsTokens {
        access_token: answer.access_token,
        refresh_token: answer.refresh_token,
        expires_in: answer.expires_in,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_s256_matches_rfc7636_appendix_b() {
        let pkce = Pkce::from_verifier("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".into());
        assert_eq!(
            pkce.challenge,
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        let random = Pkce::new();
        assert_eq!(random.verifier.len(), 43);
    }

    #[test]
    fn callback_parsing() {
        let ok = "GET /callback?code=abc&state=s1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert_eq!(parse_callback(ok, "s1").unwrap(), "abc");
        for (req, why) in [
            (
                "GET /callback?code=abc&state=s2 HTTP/1.1\r\n",
                "state mismatch",
            ),
            ("GET /other?code=abc&state=s1 HTTP/1.1\r\n", "wrong path"),
            ("POST /callback?code=abc&state=s1 HTTP/1.1\r\n", "not a GET"),
            (
                "GET /callback?error=access_denied&state=s1 HTTP/1.1\r\n",
                "the identity provider returned an error",
            ),
            ("GET /callback?state=s1 HTTP/1.1\r\n", "no code"),
        ] {
            match parse_callback(req, "s1") {
                Err(AuthError::Callback(reason)) => assert_eq!(reason, why, "{req}"),
                other => panic!("{req}: {other:?}"),
            }
        }
    }

    #[test]
    fn authorize_url_carries_pkce_and_step_up() {
        let oidc = Oidc {
            authorization_endpoint: Url::parse("https://auth.example/application/o/authorize/")
                .unwrap(),
            token_endpoint: Url::parse("https://auth.example/application/o/token/").unwrap(),
        };
        let pkce = Pkce::from_verifier("v".repeat(43));
        let url = authorize_url(
            &oidc,
            CLIENT_ID,
            "http://127.0.0.1:5555/callback",
            &pkce,
            "st",
            "no",
            true,
        );
        let q: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["code_challenge"], pkce.challenge);
        assert_eq!(q["client_id"], "loams-desktop");
        assert_eq!(q["max_age"], "0");
        assert_eq!(q["redirect_uri"], "http://127.0.0.1:5555/callback");
    }

    #[test]
    fn tokens_never_print() {
        let t = LoamsTokens {
            access_token: "secret-access".into(),
            refresh_token: Some("secret-refresh".into()),
            expires_in: Some(3600),
        };
        let shown = format!("{t:?}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
