//! What the embed is allowed to navigate to.
//!
//! # Where this comes from
//!
//! SF1 Task 0's spike found that zeron's browser module already solves
//! navigation control, and the mechanism it recorded is the model here
//! (`crates/ui/src/browser/model.rs:131-138` in that project): `http` and
//! `https` only, a host is required, and any URL carrying userinfo is
//! refused. D620 replaces the webview but not that policy — the policy is what
//! makes a browser pointed at an agent-controlled surface safe to leave running.
//!
//! On top of zeron's three rules this adds the one zeron did not need:
//!
//! - **The host must be in the allowlist.** The embedded origin, plus the
//!   identity provider's origins, which is what lets the Authentik redirect
//!   work while the OIDC ceremony itself happens in Loams. Anything else is
//!   refused rather than opened in the system browser, so a link in a chat
//!   message cannot quietly turn the embed into a general-purpose browser.
//!
//! This is also the compensating control for [`crate::net_policy`]: the
//! engine's private-network switch is process-wide, so the per-request scoping
//! has to live here.

use url::Url;

use crate::error::{Result, SidebarBrowserError};
use crate::session::EmbeddedOrigin;

/// The set of origins one embed may navigate within.
///
/// One embedded app's origin, plus the identity providers it may bounce through
/// during sign-in. Nothing is a wildcard: an allowlist entry is a whole origin,
/// compared by `(scheme, host, port)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NavigationAllowlist {
    origins: Vec<EmbeddedOrigin>,
}

impl NavigationAllowlist {
    /// An allowlist containing only `app`.
    pub fn new(app: EmbeddedOrigin) -> Self {
        Self { origins: vec![app] }
    }

    /// Add an origin the embed may also visit, typically the identity provider.
    ///
    /// Duplicates are collapsed, so calling this twice with the same provider
    /// is not an error.
    pub fn allow_origin(mut self, origin: EmbeddedOrigin) -> Self {
        if !self.origins.contains(&origin) {
            self.origins.push(origin);
        }
        self
    }

    /// Add an identity provider origin from a URL string.
    pub fn allow_url(self, url: &str) -> Result<Self> {
        Ok(self.allow_origin(EmbeddedOrigin::parse(url)?))
    }

    /// The allowed origins.
    pub fn origins(&self) -> &[EmbeddedOrigin] {
        &self.origins
    }

    /// Whether `url` may be navigated to, and why not when it may not.
    pub fn check(&self, url: &Url) -> Result<()> {
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(SidebarBrowserError::NavigationRefused {
                origin: self.describe(),
                url: url.to_string(),
            });
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(SidebarBrowserError::NavigationRefused {
                origin: self.describe(),
                url: url.to_string(),
            });
        }
        let Some(host) = url.host() else {
            return Err(SidebarBrowserError::NavigationRefused {
                origin: self.describe(),
                url: url.to_string(),
            });
        };
        let matches = self.origins.iter().any(|allowed| {
            allowed.scheme() == url.scheme()
                && allowed.port_or_known_default() == url.port_or_known_default().unwrap_or(0)
                && allowed.host_enum() == Some(host.clone())
        });
        if matches {
            return Ok(());
        }
        // A host that is the right name on the wrong scheme or the wrong port
        // is still a different origin and is refused. The message says so,
        // because "it worked in the browser" is the usual report for exactly
        // this case.
        Err(SidebarBrowserError::NavigationRefused {
            origin: self.describe(),
            url: url.to_string(),
        })
    }

    fn describe(&self) -> String {
        self.origins
            .iter()
            .map(EmbeddedOrigin::as_display)
            .collect::<Vec<_>>()
            .join(", ")
    }
}
