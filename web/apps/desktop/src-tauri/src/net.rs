//! The network bridge (§37 §6.4, D430, AP1 Task 5, Ruling 6).
//!
//! Every console request goes through `net_fetch`: Rust performs it and
//! streams the response back over a Tauri `Channel` as `head`, `chunk`s and
//! `end`, which `@loams/platform-tauri` turns into a standard `Response`
//! whose body is a `ReadableStream`. So connect-es server streams work, and
//! **tokens never reach JavaScript**:
//!
//! - only the active environment's origin is reachable;
//! - plain `http` only to loopback, and never with credentials; a remote
//!   environment is `https` and gets `Authorization: Bearer` from Rust;
//! - `Cookie`, `Authorization`, `Proxy-*` and other credential or hop headers
//!   set by JavaScript are dropped;
//! - redirects are followed only to the same origin and scheme, at most 3
//!   times; anything else reaches JavaScript as an error, so no credential
//!   is sent where the allowlist did not admit.
//!
//! The policy functions are pure and unit-tested; `perform` is the transport.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use url::{Host, Url};

use crate::envs::{EnvKind, Environment};

/// At most this many same-origin redirects.
pub const MAX_REDIRECTS: usize = 3;

/// What JavaScript asks for (`tauriFetch`'s arguments).
#[derive(Debug, Clone, Deserialize)]
pub struct FetchRequest {
    pub url: String,
    #[serde(default = "get")]
    pub method: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// The request body, base64 (TODO: raw IPC bodies, AP1 Task 5's spike).
    #[serde(default)]
    pub body: Option<String>,
}

fn get() -> String {
    "GET".into()
}

/// What streams back to JavaScript.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FetchEvent {
    Head {
        status: u16,
        headers: Vec<(String, String)>,
    },
    /// A body chunk, base64.
    Chunk {
        data: String,
    },
    End,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum NetError {
    #[error("no active environment")]
    NoEnvironment,
    #[error("the environment has no address yet (is the local stack running?)")]
    NotReady,
    #[error("refused: {0} is not this environment's origin")]
    ForeignOrigin(String),
    #[error("refused: plain http is allowed only to loopback ({0})")]
    InsecureRemote(String),
    #[error("refused a redirect to {0}")]
    Redirect(String),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("{0}")]
    Transport(String),
}

/// Whether a request may carry the environment's credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Credentials {
    None,
    Bearer,
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(d)) => d == "localhost",
        None => false,
    }
}

/// Decides whether `url` may be fetched for `env`, and with what credential.
///
/// # Errors
///
/// A foreign origin, or plain `http` to anything but loopback.
pub fn check_url(env: &Environment, url: &Url) -> Result<Credentials, NetError> {
    let Some(origin) = env.origin() else {
        return Err(NetError::NotReady);
    };
    if url.origin() != origin {
        return Err(NetError::ForeignOrigin(url.origin().ascii_serialization()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(NetError::BadRequest("credentials in the URL".into()));
    }
    match url.scheme() {
        "https" => Ok(match env.kind {
            EnvKind::Remote => Credentials::Bearer,
            EnvKind::Local => Credentials::None,
        }),
        "http" if is_loopback(url) => Ok(Credentials::None),
        "http" => Err(NetError::InsecureRemote(url.origin().ascii_serialization())),
        other => Err(NetError::BadRequest(format!("scheme {other}"))),
    }
}

/// Headers JavaScript may not set: credentials, proxies, hop-by-hop.
const STRIPPED: [&str; 9] = [
    "authorization",
    "cookie",
    "cookie2",
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "dpop",
    "upgrade",
];

/// Drops credential and hop-by-hop headers set by JavaScript.
#[must_use]
pub fn sanitize_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            let n = name.to_ascii_lowercase();
            !STRIPPED.contains(&n.as_str()) && !n.starts_with("proxy-") && !n.starts_with("sec-")
        })
        .cloned()
        .collect()
}

/// Whether to follow a redirect from `from` to `to` (hop number `hop`).
///
/// # Errors
///
/// Another origin, a downgrade from `https`, or too many hops.
pub fn check_redirect(from: &Url, to: &Url, hop: usize) -> Result<(), NetError> {
    if hop >= MAX_REDIRECTS {
        return Err(NetError::TooManyRedirects);
    }
    if to.origin() != from.origin() || to.scheme() != from.scheme() {
        return Err(NetError::Redirect(to.origin().ascii_serialization()));
    }
    Ok(())
}

/// The bridge's shared state: one HTTP client and the in-memory tokens.
#[derive(Debug)]
pub struct Net {
    client: reqwest::Client,
    /// Environment id → access token (memory only, AP1 Ruling 8).
    tokens: Mutex<HashMap<String, String>>,
}

impl Net {
    /// # Errors
    ///
    /// If the TLS backend cannot start.
    pub fn new() -> Result<Self, NetError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("loams-desktop/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| NetError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            tokens: Mutex::new(HashMap::new()),
        })
    }

    pub fn set_token(&self, env: &str, token: Option<String>) {
        let mut tokens = self
            .tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match token {
            Some(t) => tokens.insert(env.into(), t),
            None => tokens.remove(env),
        };
    }

    #[must_use]
    pub fn has_token(&self, env: &str) -> bool {
        self.tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(env)
    }

    fn token(&self, env: &str) -> Option<String> {
        self.tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(env)
            .cloned()
    }

    /// Performs `request` for `env`, streaming events into `sink`.
    ///
    /// # Errors
    ///
    /// A policy refusal or a transport failure before the head arrives;
    /// failures after it are sent as `FetchEvent::Error`.
    pub async fn perform(
        &self,
        env: &Environment,
        request: FetchRequest,
        mut sink: impl FnMut(FetchEvent) + Send,
    ) -> Result<(), NetError> {
        let mut url = Url::parse(&request.url).map_err(|e| NetError::BadRequest(e.to_string()))?;
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|e| NetError::BadRequest(e.to_string()))?;
        let body = request
            .body
            .as_deref()
            .map(|b| STANDARD.decode(b))
            .transpose()
            .map_err(|e| NetError::BadRequest(e.to_string()))?;
        let headers = sanitize_headers(&request.headers);
        let mut hop = 0;
        let response = loop {
            let credentials = check_url(env, &url)?;
            let mut builder = self.client.request(method.clone(), url.clone());
            for (name, value) in &headers {
                builder = builder.header(name, value);
            }
            if credentials == Credentials::Bearer
                && let Some(token) = self.token(&env.id)
            {
                builder = builder.bearer_auth(token);
            }
            if let Some(body) = &body {
                builder = builder.body(body.clone());
            }
            let response = builder
                .send()
                .await
                .map_err(|e| NetError::Transport(e.to_string()))?;
            if !response.status().is_redirection() {
                break response;
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|l| l.to_str().ok())
                .ok_or_else(|| NetError::Redirect("(no location)".into()))?;
            let next = url
                .join(location)
                .map_err(|e| NetError::BadRequest(e.to_string()))?;
            check_redirect(&url, &next, hop)?;
            hop += 1;
            url = next;
        };
        let head = FetchEvent::Head {
            status: response.status().as_u16(),
            headers: response
                .headers()
                .iter()
                .filter(|(name, _)| name.as_str() != "set-cookie")
                .filter_map(|(n, v)| Some((n.as_str().to_owned(), v.to_str().ok()?.to_owned())))
                .collect(),
        };
        sink(head);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => sink(FetchEvent::Chunk {
                    data: STANDARD.encode(&bytes),
                }),
                Err(e) => {
                    sink(FetchEvent::Error {
                        message: e.to_string(),
                    });
                    return Ok(());
                }
            }
        }
        sink(FetchEvent::End);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(kind: EnvKind, url: &str) -> Environment {
        Environment {
            id: "e".into(),
            name: "e".into(),
            kind,
            base_url: Url::parse(url).ok(),
        }
    }

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn bridge_refuses_foreign_origin() {
        let local = env(EnvKind::Local, "http://127.0.0.1:8084/");
        assert!(matches!(
            check_url(&local, &u("http://127.0.0.1:9999/x")),
            Err(NetError::ForeignOrigin(_))
        ));
        assert!(matches!(
            check_url(&local, &u("https://evil.example/x")),
            Err(NetError::ForeignOrigin(_))
        ));
    }

    #[test]
    fn bridge_allows_loopback_http_without_credentials() {
        let local = env(EnvKind::Local, "http://127.0.0.1:8084/");
        assert_eq!(
            check_url(
                &local,
                &u("http://127.0.0.1:8084/loams.instance.v1.InstanceService/GetInstance")
            ),
            Ok(Credentials::None)
        );
    }

    #[test]
    fn bridge_refuses_remote_plain_http() {
        // A Local profile pointed at a non-loopback http address is refused.
        let lan = env(EnvKind::Local, "http://192.168.1.20:8080/");
        assert!(matches!(
            check_url(&lan, &u("http://192.168.1.20:8080/x")),
            Err(NetError::InsecureRemote(_))
        ));
        let remote = env(EnvKind::Remote, "https://loams.example/");
        assert_eq!(
            check_url(&remote, &u("https://loams.example/x")),
            Ok(Credentials::Bearer)
        );
        assert!(check_url(&remote, &u("http://loams.example/x")).is_err());
    }

    #[test]
    fn bridge_strips_js_auth_headers() {
        let headers: Vec<(String, String)> = [
            ("Authorization", "Bearer stolen"),
            ("Cookie", "session=1"),
            ("Proxy-Authorization", "x"),
            ("DPoP", "proof"),
            ("Sec-Fetch-Site", "none"),
            ("Content-Type", "application/proto"),
            ("Connect-Protocol-Version", "1"),
        ]
        .map(|(a, b)| (a.into(), b.into()))
        .to_vec();
        let kept: Vec<String> = sanitize_headers(&headers)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(kept, ["Content-Type", "Connect-Protocol-Version"]);
    }

    #[test]
    fn redirects_stay_on_origin_and_scheme() {
        let from = u("https://loams.example/a");
        assert!(check_redirect(&from, &u("https://loams.example/b"), 0).is_ok());
        assert!(matches!(
            check_redirect(&from, &u("https://other.example/b"), 0),
            Err(NetError::Redirect(_))
        ));
        assert!(matches!(
            check_redirect(&from, &u("http://loams.example/b"), 0),
            Err(NetError::Redirect(_))
        ));
        assert_eq!(
            check_redirect(&from, &u("https://loams.example/c"), MAX_REDIRECTS),
            Err(NetError::TooManyRedirects)
        );
    }

    #[test]
    fn no_address_means_not_ready() {
        let local = Environment {
            base_url: None,
            ..env(EnvKind::Local, "http://127.0.0.1:1/")
        };
        assert_eq!(
            check_url(&local, &u("http://127.0.0.1:1/")),
            Err(NetError::NotReady)
        );
    }
}
