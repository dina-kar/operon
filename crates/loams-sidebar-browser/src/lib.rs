//! The docked sidebar browser (design §37 §18, plan [SF1](../../docs/plans/2026-10-02-sf1-collab-ui-plugins.md), decision **D620**).
//!
//! # What this is
//!
//! SF1 Task 0 declared the sidebar browser "does not ship on any operating
//! system" (ruling E1), because the per-platform webview it would have been
//! built on has an ephemeral website-data store everywhere, no WebAuthn on the
//! GTK and WPE WebKit ports, and no Windows implementation at all. The owner
//! has overridden E1. This crate is the replacement, and it is a different
//! shape: **no webview is forked or embedded at all.** The rendering engine is
//! Obscura (Apache-2.0, Rust, headless, no Chromium), run as a child process
//! and driven over the Chrome DevTools Protocol.
//!
//! Forking was rejected, and it is worth saying why in the crate's own docs
//! because it is the load-bearing reason for the whole shape: WKWebView and
//! WebView2 are proprietary and cannot be forked, and forking WebKit is a
//! multi-year effort. A separate process over a documented protocol is the only
//! option that is both Apache-2.0-clean and one code path for Linux, macOS and
//! Windows.
//!
//! # How each of E1's three blockers is closed, in code
//!
//! | E1's blocker | Closed by |
//! |---|---|
//! | Ephemeral store on every platform | [`profile::ProfileKey`] gives each `(environment, app)` pair its own directory, and [`ledger::CookieLedger`] persists the app's scoped session cookies there. Loams re-injects them on the next launch. Tests: `tests/persistence.rs`, `tests/isolation.rs`. |
//! | No WebAuthn in WebKitGTK | Loams runs the OIDC/Authentik ceremony itself and hands the engine only post-auth scoped session cookies ([`session`]). The engine never performs a WebAuthn ceremony — and could not: the pinned engine has no WebAuthn implementation at all. Test: `tests/token_boundary.rs`. |
//! | No Windows browser | There is no per-platform webview to be missing. [`engine`] builds one command line that is the same on all three targets. Test: `tests/launch_spec.rs`. |
//!
//! # The security constraint
//!
//! SF1's Global Constraint is "embeds never hold a Loams token", and a naive
//! persistent profile would break it. [`boundary`] is the check that would
//! catch that, and `tests/token_boundary.rs` runs it against a real profile
//! directory holding a real Loams token.
//!
//! # What this is not
//!
//! Not a general-purpose human-interactive browser. See [`panel`] and the
//! crate README's "Fidelity" section: the surface is agent-driven, frames come
//! from a software rasteriser through `Page.startScreencast`, and interaction
//! is routed back as synthetic input events. The embedded targets are Zulip,
//! Plane and Forgejo, which bounds what that costs, and the reason it is
//! bounded is written down rather than assumed.
//!
//! # Engine version
//!
//! Written against Obscura **0.2.3** ([`engine::PINNED_ENGINE_VERSION`]),
//! verified by reading that tag's source. [`engine::check_version`] compares at
//! startup. The README says which release asset to install, and which ones not
//! to.

#![warn(missing_debug_implementations)]

pub mod boundary;
pub mod cdp;
pub mod engine;
pub mod error;
pub mod frame;
pub mod ledger;
pub mod navigate;
pub mod net_policy;
pub mod panel;
pub mod profile;
pub mod session;

pub use boundary::{assert_cookies_exclude, audit_profile};
pub use cdp::{CdpClient, CdpEvent, DEFAULT_CALL_TIMEOUT};
pub use engine::{
    BIND_HOST, DEFAULT_CDP_PORT, DEFAULT_STARTUP_TIMEOUT, EngineLaunchSpec, OBSCURA_BIN_ENV,
    ObscuraEngine, PINNED_ENGINE_VERSION, check_version, resolve_program,
};
pub use error::{Result, SidebarBrowserError};
pub use frame::{
    DecodedFrame, FrameFormat, MAX_FRAME_BYTES, decode_frame, png_dimensions, png_fixture,
};
pub use ledger::{COOKIE_LEDGER, COOKIE_LEDGER_VERSION, CookieLedger};
pub use navigate::NavigationAllowlist;
pub use net_policy::{
    CLOUD_METADATA_ADDR, PrivateNetworkDecision, decide, decide_host, decide_origin,
};
pub use panel::{FramePump, PanelInput, PanelState, start_screencast_params};
pub use profile::{PROFILE_MANIFEST, ProfileKey};
pub use session::{
    CookieAttributes, EmbeddedOrigin, LoamsCredential, SameSite, ScopedSessionCookie,
};

/// Everything one docked embed needs, assembled and checked.
///
/// [`SidebarBrowser::prepare`] is the entry point: it derives the profile
/// directory, decides the network policy, refuses anything it must refuse, and
/// returns a [`LaunchPlan`] the caller can execute (or, in a test, assert on
/// without starting anything).
///
/// The credential does not come in through this type. That is deliberate:
/// `prepare` has no parameter that could hold a Loams token, so the thing that
/// launches the engine cannot be handed one. The app session cookies come in
/// separately, through [`LaunchPlan::inject`], and
/// [`session::ScopedSessionCookie`] is the only thing that can cross it.
#[derive(Clone, Debug)]
pub struct SidebarBrowser {
    key: ProfileKey,
    origin: EmbeddedOrigin,
    allowlist: NavigationAllowlist,
    plan: LaunchPlan,
}

impl SidebarBrowser {
    /// Derive the launch plan for one `(environment, app)` embed.
    ///
    /// `base` is the desktop's data directory; the profile directory is
    /// `<base>/<environment>/<app>`.
    pub fn prepare(
        base: impl AsRef<std::path::Path>,
        key: ProfileKey,
        origin: EmbeddedOrigin,
        program: impl Into<std::path::PathBuf>,
        port: u16,
    ) -> Result<Self> {
        let profile_dir = key.ensure_profile_dir(base)?;
        let plan = LaunchPlan::build(&key, &profile_dir, &origin, program, port)?;
        Ok(Self {
            allowlist: NavigationAllowlist::new(origin.clone()),
            key,
            origin,
            plan,
        })
    }

    /// Allow an extra origin, typically the identity provider the sign-in
    /// redirect goes through.
    pub fn allow_origin(mut self, origin: EmbeddedOrigin) -> Self {
        self.allowlist = self.allowlist.allow_origin(origin);
        self
    }

    /// The profile key.
    pub fn key(&self) -> &ProfileKey {
        &self.key
    }

    /// The embedded origin.
    pub fn origin(&self) -> &EmbeddedOrigin {
        &self.origin
    }

    /// The navigation allowlist.
    pub fn allowlist(&self) -> &NavigationAllowlist {
        &self.allowlist
    }

    /// The plan, which has not been executed.
    pub fn plan(&self) -> &LaunchPlan {
        &self.plan
    }

    /// Inject cookies into a connected client and load anything already
    /// persisted for this profile.
    ///
    /// Both sources go in: the caller's `fresh` cookies (from the OIDC
    /// ceremony Loams just performed) and the ledger's (from the last launch).
    /// `fresh` wins, because a cookie the app has just rotated must not be
    /// overwritten by the copy from last time.
    pub async fn inject(
        &self,
        client: &mut CdpClient,
        session_id: &str,
        fresh: &[ScopedSessionCookie],
    ) -> Result<usize> {
        let mut ledger = CookieLedger::load_for(self.plan.profile_dir(), &self.key)?;
        for cookie in fresh {
            if cookie.origin() != &self.origin {
                return Err(SidebarBrowserError::Rejected(format!(
                    "cookie {:?} is for {}, but this embed is {}",
                    cookie.name(),
                    cookie.origin(),
                    self.origin
                )));
            }
            ledger.upsert(cookie.clone());
        }
        let cookies = ledger.for_origin_cookies(&self.origin);
        if !cookies.is_empty() {
            let params = serde_json::json!({
                "cookies": cookies.iter().map(|cookie| cookie.to_cdp_json()).collect::<Vec<_>>(),
            });
            client
                .call("Storage.setCookies", params, Some(session_id))
                .await?;
        }
        // Persist after injecting, so a ledger written this launch is the one
        // that survives it.
        ledger.store(self.plan.profile_dir(), &self.origin)?;
        Ok(cookies.len())
    }

    /// Read the engine's cookies back and assert none of them is a Loams
    /// token.
    ///
    /// Takes the token as an argument rather than holding one, so this crate
    /// has no field that could keep it alive past the check.
    pub async fn verify_boundary(
        &self,
        client: &mut CdpClient,
        session_id: &str,
        token: &LoamsCredential,
    ) -> Result<()> {
        let response = client
            .call(
                "Storage.getCookies",
                serde_json::json!({}),
                Some(session_id),
            )
            .await?;
        let cookies = cookies_from_cdp(&response, &self.origin)?;
        assert_cookies_exclude(&cookies, token.expose())?;
        audit_profile(self.plan.profile_dir(), token.expose())?;
        Ok(())
    }
}

/// A checked, not-yet-executed engine launch.
#[derive(Clone, Debug)]
pub struct LaunchPlan {
    spec: EngineLaunchSpec,
    profile_dir: std::path::PathBuf,
    network: PrivateNetworkDecision,
}

impl LaunchPlan {
    fn build(
        key: &ProfileKey,
        profile_dir: &std::path::Path,
        origin: &EmbeddedOrigin,
        program: impl Into<std::path::PathBuf>,
        port: u16,
    ) -> Result<Self> {
        let decision = net_policy::decide_origin(origin)?;
        net_policy::engine_relaxation(origin, &decision).map_err(|error| match &decision {
            PrivateNetworkDecision::Refused { reason } => SidebarBrowserError::NetworkRefused {
                reason: format!("{key} embeds {origin}, which Loams refuses: {reason}"),
            },
            _ => error,
        })?;
        let spec = EngineLaunchSpec::for_origin(program, profile_dir, port, origin)?;
        spec.assert_loopback()?;
        Ok(Self {
            spec,
            profile_dir: profile_dir.to_path_buf(),
            network: decision,
        })
    }

    /// The engine invocation.
    pub fn spec(&self) -> &EngineLaunchSpec {
        &self.spec
    }

    /// The profile directory.
    pub fn profile_dir(&self) -> &std::path::Path {
        &self.profile_dir
    }

    /// The network decision behind the invocation's flags.
    pub fn network(&self) -> &PrivateNetworkDecision {
        &self.network
    }
}

/// Parse the engine's `Storage.getCookies` response into this crate's cookie
/// type, keeping only cookies scoped to `origin`.
///
/// Cookies for other origins are dropped rather than refused: the engine's jar
/// is shared across a profile and a stale cookie for a sibling subdomain would
/// otherwise make every read fail. The CDP parameter is `url`, never `domain`,
/// for the reason in [`ScopedSessionCookie::to_cdp_json`].
pub fn cookies_from_cdp(
    response: &serde_json::Value,
    origin: &EmbeddedOrigin,
) -> Result<Vec<ScopedSessionCookie>> {
    let Some(list) = response
        .get("cookies")
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for entry in list {
        let Some(name) = entry.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let value = entry
            .get("value")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let Some(cookie_origin) = cookie_origin(entry, origin) else {
            continue;
        };
        if &cookie_origin != origin {
            continue;
        }
        let built = ScopedSessionCookie::new(
            cookie_origin,
            name,
            value,
            session::CookieAttributes {
                path: entry
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("/")
                    .to_string(),
                secure: entry
                    .get("secure")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                http_only: entry
                    .get("httpOnly")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                same_site: session::SameSite::parse(
                    entry
                        .get("sameSite")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(""),
                ),
                expires: entry.get("expires").and_then(serde_json::Value::as_f64),
            },
        );
        match built {
            Ok(cookie) => out.push(cookie),
            // A cookie the engine holds that this crate would refuse to build,
            // e.g. one with a `Domain` attribute or a non-rooted path. It is
            // not silently accepted, and it is not fatal to the read; it is
            // worth a log line because it means the page set something outside
            // the boundary.
            Err(error) => {
                tracing::warn!(
                    name,
                    error = %error,
                    "the engine holds a cookie for the embedded origin that Loams would not build; \
                     ignoring it"
                );
            }
        }
    }
    Ok(out)
}

fn cookie_origin(entry: &serde_json::Value, default: &EmbeddedOrigin) -> Option<EmbeddedOrigin> {
    if let Some(origin) = entry
        .get("url")
        .and_then(serde_json::Value::as_str)
        .and_then(|url| EmbeddedOrigin::parse(url).ok())
    {
        return Some(origin);
    }
    let domain = entry.get("domain").and_then(serde_json::Value::as_str)?;
    // A `Domain` attribute means the cookie was not host-only, which is
    // exactly the shape `__Host-` forbids and this crate never writes. Keep it
    // as its registrable host so it can be compared and dropped, rather than
    // guessing at a scope.
    let trimmed = domain.trim_start_matches('.');
    let port = default.port_or_known_default();
    let authority = if port == 443 || port == 80 {
        trimmed.to_string()
    } else {
        format!("{trimmed}:{port}")
    };
    EmbeddedOrigin::parse(&format!("{}://{authority}", default.scheme())).ok()
}
