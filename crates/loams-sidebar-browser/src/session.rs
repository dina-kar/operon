//! The credential boundary: what Loams keeps and what the engine is given.
//!
//! # The constraint
//!
//! SF1's Global Constraint is **"embeds never hold a Loams token."** D620
//! supersedes E1 and ships the embed anyway, so the constraint has to hold in
//! code rather than by the accident of an ephemeral store — which is the only
//! reason E1 could claim it was "easy to keep" (SF1 Task 0, Decision 1).
//!
//! # The mechanism
//!
//! Three mechanisms, in order of how much they can catch:
//!
//! 1. **The engine is never asked to authenticate.** Loams runs the
//!    OIDC/Authentik ceremony itself, with its own OIDC client, and only the
//!    resulting app session cookies cross into [`ScopedSessionCookie`].
//!    Obscura has no WebAuthn implementation at all, so a browser-driven
//!    ceremony is not merely undesirable here, it is impossible; the design
//!    does not have a browser-held-credential path to leak.
//! 2. **Cookies are host-only and origin-scoped by construction.**
//!    [`ScopedSessionCookie::new`] refuses any cookie whose domain is not
//!    exactly the embedded origin's host, refuses a `Domain` attribute
//!    outright, and enforces the `__Host-` / `__Secure-` cookie prefixes that
//!    Zulip's `__Host-sessionid` depends on (SF1 Decision 2, ruling E6). There
//!    is no API that hands the engine a cookie for a second origin.
//! 3. **The invariant is audited, not assumed.** [`crate::boundary`] scans the
//!    engine's cookie jar and the whole profile directory for a given secret.
//!    [`crate::boundary::assert_no_loams_token`] is what makes this a test
//!    rather than a claim, and it is the check that would fail if someone
//!    later "helpfully" injected the Loams bearer token to avoid a second
//!    login.
//!
//! # What a naive persistent profile would have done
//!
//! Pointing the engine at a persistent directory with no injection boundary
//! would have been the easy version of this feature: the app's own login flow
//! runs inside the embed, whatever the page decides to write to its cookie jar
//! and its `localStorage` lands on disk, and the Loams console session —
//! which the desktop already holds for the API — would sit in the same jar one
//! bad redirect away. The whole point of the ledger in this module is that
//! Loams writes the app's cookies itself, from a value it obtained out of
//! band, and can enumerate exactly what the engine was given.

use std::fmt;

use url::Url;

use crate::error::{Result, SidebarBrowserError};

/// A Loams API credential.
///
/// This type exists so that "the Loams token" is a name in the code rather than
/// a `String` that can be passed anywhere. It deliberately has **no**
/// `Serialize`, no `Display`, and no method that produces anything a
/// [`ScopedSessionCookie`] can be built from. Its `Debug` is redacted, matching
/// SF1's Global Constraint that `Secret` has no `Serialize` and a redacted
/// `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct LoamsCredential {
    token: String,
}

impl LoamsCredential {
    /// Wrap a Loams API token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// The raw token, for Loams's own HTTP stack.
    ///
    /// The doc comment is the point: every caller of this method is Loams code
    /// talking to the Loams API. Nothing in this crate calls it, and that is
    /// asserted by `tests/token_boundary.rs`, which greps the crate's own
    /// sources for it.
    pub fn expose(&self) -> &str {
        &self.token
    }

    /// A short, non-reversible fingerprint, safe to put in a log line or an
    /// error message when you need to say *which* token without saying it.
    pub fn fingerprint(&self) -> String {
        // FNV-1a over the token, rendered as hex. Not a security primitive and
        // not used as one: it only distinguishes tokens in diagnostics.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in self.token.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("loams-credential:{hash:016x}")
    }
}

impl fmt::Debug for LoamsCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LoamsCredential(<redacted>)")
    }
}

/// The scheme, host and port of one embedded app, with no path, query,
/// fragment or userinfo.
///
/// One profile is bound to one origin. A cookie for any other origin cannot be
/// constructed (see [`ScopedSessionCookie::new`]), which is what stops a
/// shared-cookie-domain shortcut that ruling E6 forbids.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EmbeddedOrigin {
    url: Url,
}

impl EmbeddedOrigin {
    /// Parse and validate an origin. `https://chat.example.com` and
    /// `https://chat.example.com/` are the same origin; anything with a
    /// non-root path, a query, a fragment or userinfo is refused, because
    /// those cannot be an origin and silently truncating them would widen the
    /// scope of what the cookie is valid for.
    pub fn parse(value: &str) -> Result<Self> {
        let url = Url::parse(value).map_err(|error| {
            SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} is not a URL: {error}"
            ))
        })?;
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} has scheme {:?}, only http and https are embeddable",
                url.scheme()
            )));
        }
        if url.username() != "" || url.password().is_some() {
            return Err(SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} carries userinfo"
            )));
        }
        if url.path() != "/" {
            return Err(SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} has path {:?}; an origin has none",
                url.path()
            )));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} carries a query or fragment"
            )));
        }
        if url.host().is_none() {
            return Err(SidebarBrowserError::Rejected(format!(
                "embedded origin {value:?} has no host"
            )));
        }
        Ok(Self { url })
    }

    /// The origin as a URL, e.g. `https://chat.example.com/`.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The scheme, e.g. `https`.
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// The host, e.g. `chat.example.com`.
    pub fn host(&self) -> &str {
        self.url.host_str().unwrap_or_default()
    }

    /// The host as a [`Host`], for the IP comparisons in
    /// [`crate::net_policy`].
    pub fn host_enum(&self) -> Option<url::Host<&str>> {
        self.url.host()
    }

    /// The port, or the scheme's default when the URL omits it.
    pub fn port_or_known_default(&self) -> u16 {
        self.url
            .port_or_known_default()
            .unwrap_or(if self.url.scheme() == "https" {
                443
            } else {
                80
            })
    }

    /// Whether this origin is the secure scheme. A cookie for an `https` origin
    /// must be `Secure`; [`ScopedSessionCookie::new`] requires it.
    pub fn is_secure(&self) -> bool {
        self.url.scheme() == "https"
    }

    /// The origin rendered as `scheme://host[:port]`, without the trailing
    /// slash. Used in log lines and in the ledger.
    pub fn as_display(&self) -> String {
        let mut out = format!("{}://{}", self.url.scheme(), self.host());
        if let Some(port) = self.url.port() {
            out.push_str(&format!(":{port}"));
        }
        out
    }
}

impl fmt::Debug for EmbeddedOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EmbeddedOrigin")
            .field(&self.as_display())
            .finish()
    }
}

impl fmt::Display for EmbeddedOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_display())
    }
}

/// `SameSite` as the engine reports and accepts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SameSite {
    /// No attribute. Engines treat this as `Lax` in practice; Obscura stores an
    /// empty string for it, which is why [`SameSite::as_str`] is not the same
    /// as `Default::default()`'s `Display`.
    #[default]
    Unspecified,
    /// `Lax`. All of Zulip, Plane CE and Forgejo ship `Lax`, which works
    /// same-site (SF1 Decision 2).
    Lax,
    /// `Strict`.
    Strict,
    /// `None`. Requires `Secure`. SF1 forbids setting this on the apps'
    /// sessions, so nothing in this crate constructs it; the variant exists
    /// only to read a cookie back faithfully.
    None,
}

impl SameSite {
    /// The CDP spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            SameSite::Unspecified => "",
            SameSite::Lax => "Lax",
            SameSite::Strict => "Strict",
            SameSite::None => "None",
        }
    }

    /// Parse a CDP spelling. Obscura stores an absent attribute as `""`
    /// (`obscura-cdp/src/cookie_params.rs`), so that maps to
    /// [`SameSite::Unspecified`] rather than being an error.
    pub fn parse(value: &str) -> Self {
        match value {
            "Lax" => SameSite::Lax,
            "Strict" => SameSite::Strict,
            "None" => SameSite::None,
            _ => SameSite::Unspecified,
        }
    }
}

/// The attributes of a cookie other than its name, value and origin.
///
/// Grouped so that constructing a [`ScopedSessionCookie`] reads as one call with
/// a named set of attributes rather than eight positional arguments, where
/// swapping `secure` and `http_only` would compile and quietly do the wrong
/// thing.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct CookieAttributes {
    /// The cookie path. Must be rooted at `/`.
    pub path: String,
    /// Whether the cookie carries `Secure`.
    pub secure: bool,
    /// Whether the cookie carries `HttpOnly`.
    pub http_only: bool,
    /// The cookie's `SameSite`.
    pub same_site: SameSite,
    /// Absolute expiry in seconds since the Unix epoch, as CDP spells it.
    pub expires: Option<f64>,
}

impl CookieAttributes {
    /// Attributes for a session cookie at the origin's root: `Path=/`,
    /// `HttpOnly`, `SameSite=Lax`, no expiry.
    ///
    /// `secure` is taken from the origin rather than from the caller, because an
    /// `https` origin must be `Secure` and an `http` one may be either; leaving
    /// the caller to pass the right value is a mistake waiting to happen.
    pub fn session(origin: &EmbeddedOrigin) -> Self {
        Self {
            path: "/".to_string(),
            secure: origin.is_secure(),
            http_only: true,
            same_site: SameSite::Lax,
            expires: None,
        }
    }

    /// Set an absolute expiry, in seconds since the Unix epoch.
    pub fn with_expires(mut self, expires: f64) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Set the cookie path.
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Set whether the cookie is `Secure`.
    pub fn with_secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    /// Set whether the cookie is `HttpOnly`.
    pub fn with_http_only(mut self, http_only: bool) -> Self {
        self.http_only = http_only;
        self
    }

    /// Set the cookie's `SameSite`.
    pub fn with_same_site(mut self, same_site: SameSite) -> Self {
        self.same_site = same_site;
        self
    }
}

/// One embedded app's session cookie, scoped to one origin.
///
/// This is the **only** type in the crate that crosses into the engine. It
/// has no public field, no `Deserialize`, and no constructor that takes
/// anything but an [`EmbeddedOrigin`] and the cookie's own parts — so there is
/// no path by which a [`LoamsCredential`] becomes one.
///
/// Persistence ([`CookieLedger`]) deliberately does not derive `Serialize`
/// here either: it has private wire types instead, so the on-disk format is a
/// separate, versioned concern rather than a side effect of this type's shape.
#[derive(Clone, PartialEq)]
pub struct ScopedSessionCookie {
    origin: EmbeddedOrigin,
    name: String,
    value: String,
    path: String,
    secure: bool,
    http_only: bool,
    same_site: SameSite,
    expires: Option<f64>,
}

impl ScopedSessionCookie {
    /// Build a cookie for `origin`, enforcing every scoping invariant.
    ///
    /// Refused:
    ///
    /// - a name outside RFC 6265's cookie-name grammar;
    /// - a path that is not rooted, or a `__Host-` cookie whose path is not
    ///   exactly `/`;
    /// - a cookie for an `https` origin that is not `Secure`, or a
    ///   `__Secure-` / `__Host-` cookie that is not `Secure`;
    /// - an empty value, or one containing a control character or `;`.
    ///
    /// Note what is *not* a parameter: there is no `domain` argument. A
    /// `Domain` attribute cannot be expressed, which is how ruling E6's
    /// "Zulip's `__Host-` prefix forbids any `Domain` attribute" is enforced
    /// rather than merely observed.
    pub fn new(
        origin: EmbeddedOrigin,
        name: impl Into<String>,
        value: impl Into<String>,
        attributes: CookieAttributes,
    ) -> Result<Self> {
        let name = name.into();
        let value = value.into();
        let CookieAttributes {
            path,
            secure,
            http_only,
            same_site,
            expires,
        } = attributes;
        let reject = |reason: String| {
            Err(SidebarBrowserError::Rejected(format!(
                "cookie {name:?} for {origin} refused: {reason}"
            )))
        };

        if name.is_empty() {
            return reject("the name is empty".into());
        }
        if !name.bytes().all(is_cookie_name_byte) {
            return reject("the name is outside RFC 6265 cookie-name".into());
        }
        if value.is_empty() {
            return reject("the value is empty".into());
        }
        if value
            .bytes()
            .any(|byte| byte < 0x20 || byte == 0x7f || byte == b';')
        {
            return reject("the value contains a control character or `;`".into());
        }
        if !path.starts_with('/') {
            return reject(format!("the path {path:?} is not rooted at `/`"));
        }
        if origin.is_secure() && !secure {
            return reject(format!("{origin} is https, so the cookie must be Secure"));
        }
        if same_site == SameSite::None && !secure {
            return reject("`SameSite=None` requires Secure".into());
        }
        if name.starts_with("__Host-") && (path != "/" || !secure) {
            return reject("a `__Host-` cookie needs `Secure` and `Path=/`".into());
        }
        if name.starts_with("__Secure-") && !secure {
            return reject("a `__Secure-` cookie needs `Secure`".into());
        }

        Ok(Self {
            origin,
            name,
            value,
            path,
            secure,
            http_only,
            same_site,
            expires,
        })
    }

    /// Build the session cookie an OIDC ceremony for `origin` produced.
    ///
    /// The ordinary path: an app's session cookie for its own origin, with the
    /// attributes that app's stack sets. Zulip's is `__Host-sessionid`,
    /// `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`; Plane's is `session-id`
    /// and Forgejo's is `session`, with the same attribute set (SF1 Task 0,
    /// Decision 2's findings table).
    pub fn from_oidc_session(
        origin: EmbeddedOrigin,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self> {
        let attributes = CookieAttributes::session(&origin);
        Self::new(origin, name, value, attributes)
    }

    /// The origin this cookie is valid for.
    pub fn origin(&self) -> &EmbeddedOrigin {
        &self.origin
    }

    /// The cookie name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The cookie value.
    ///
    /// This is the app's session, not a Loams token; see the module docs.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// The cookie path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Whether the cookie is `Secure`.
    pub fn secure(&self) -> bool {
        self.secure
    }

    /// Whether the cookie is `HttpOnly`.
    pub fn http_only(&self) -> bool {
        self.http_only
    }

    /// The cookie's `SameSite`.
    pub fn same_site(&self) -> SameSite {
        self.same_site
    }

    /// Absolute expiry, in seconds since the Unix epoch, as CDP spells it.
    pub fn expires(&self) -> Option<f64> {
        self.expires
    }

    /// The CDP `Network.CookieParam` for this cookie.
    ///
    /// `domain` is **deliberately absent** and `url` is present instead.
    /// Obscura decides host-only-ness from exactly that choice
    /// (`obscura-cdp/src/cookie_params.rs`: `host_only: explicit_domain.is_none()`),
    /// so sending `url` is what keeps the cookie off sibling subdomains, and
    /// sending `domain` would both break `__Host-` and re-open the shared
    /// cookie domain that ruling E6 rules out.
    pub fn to_cdp_json(&self) -> serde_json::Value {
        let mut out = serde_json::json!({
            "name": self.name,
            "value": self.value,
            "url": self.origin.as_display(),
            "path": self.path,
            "secure": self.secure,
            "httpOnly": self.http_only,
            "sameSite": self.same_site.as_str(),
        });
        if let Some(expires) = self.expires {
            out["expires"] = serde_json::json!(expires);
        }
        out
    }
}

impl fmt::Debug for ScopedSessionCookie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedSessionCookie")
            .field("origin", &self.origin.as_display())
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .field("path", &self.path)
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("same_site", &self.same_site)
            .field("expires", &self.expires)
            .finish()
    }
}

fn is_cookie_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}
