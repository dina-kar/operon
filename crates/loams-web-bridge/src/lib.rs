//! The web toolbox's browser providers (design §37 §18.14, §42 §4; decisions
//! D565–D567; plan AP1c).
//!
//! One tool contract — a snapshot with uids, `find`, `click`, `fill`,
//! `wait_for`, a screenshot, a content extraction — and two providers behind
//! it:
//!
//! * [`local::LocalProvider`] drives the browser on the machine the agent
//!   already runs on. It is the default, and the only path for credentialed
//!   work, because the credentials never leave that machine.
//! * [`browser_run::BrowserRunProvider`] drives Cloudflare Browser Run over its
//!   CDP endpoint, for unattended public-web work when no local browser is
//!   available.
//!
//! Which one runs is configuration (`WebBridgeConfig::provider`), not a fork,
//! so a deployment can change its mind without touching its callers.
//!
//! **Two rules shape the remote provider**, and both come from Cloudflare's own
//! documentation rather than from taste:
//!
//! * **Chromium is the default engine.** Kitesurf is a stateless Workers
//!   isolate: Cloudflare states it cannot start "a long-running, authenticated
//!   session that requires persistent state" and cannot negotiate a bot
//!   challenge's TLS fingerprint. It is offered for the throwaway work its
//!   statelessness suits — a screenshot, a scrape, an extraction — and refused
//!   for anything that needs state.
//! * **The egress policy is enforced twice.** In the provider, before every
//!   navigation, and in Browser Run itself through session guardrails. Guardrails
//!   are a Chromium-pool feature, so a Kitesurf session under a hostname allow
//!   list is refused by default instead of running ungoverned.
//!
//! A Cloudflare API token is a secret: it is named by a `secret_ref`, resolved
//! through the host's [`secret::SecretResolver`] at the moment it is needed,
//! registered for scrubbing before it goes on the wire, and never logged,
//! returned, traced or formatted.
//!
//! Nothing here has been run against a live Cloudflare account.
//! `docs/remote-browser-provider.md` records what is verified from the
//! documentation and what is not verified at all.

pub mod artifact;
pub mod browser_run;
pub mod config;
pub mod egress;
pub mod error;
pub mod local;
pub mod page;
pub mod provider;
pub mod redact;
pub mod secret;
pub mod tool;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

pub use browser_run::BrowserRunProvider;
pub use config::{BrowserRunConfig, CredentialPolicy, Engine, Plan, ProviderKind, WebBridgeConfig};
pub use egress::{EgressError, EgressPolicy, Guardrails, HostPattern};
pub use error::{BridgeError, ConfigError};
pub use local::{DriverAction, DriverPage, LocalProvider, PageDriver};
pub use page::PageState;
pub use provider::{BrowserProvider, Capabilities, OpenRequest, PageRef};
pub use secret::{MapSecretResolver, SecretRef, SecretResolver, SecretValue};
pub use tool::{
    ActionOutcome, AxNode, Extracted, FillValue, FindQuery, Navigation, Screenshot, Snapshot,
    SnapshotMode, SnapshotNode, SnapshotRequest, Uid, WaitCondition, WaitOutcome, WaitRequest,
};

use std::sync::Arc;

/// Build the provider the configuration selects.
///
/// The remote provider is the only one that needs credentials, so it resolves
/// them through `secrets`; nothing else in the crate reads an environment
/// variable or a file for a token.
pub fn provider(
    config: &WebBridgeConfig,
    transport: Arc<dyn browser_run::cdp::CdpTransport>,
    secrets: Arc<dyn SecretResolver>,
) -> Result<Box<dyn BrowserProvider>, BridgeError> {
    let egress = config.egress()?;
    match config.provider {
        ProviderKind::Local => Err(BridgeError::policy(
            "the local provider needs a PageDriver from the host; use local_provider()",
        )),
        ProviderKind::BrowserRun => Ok(Box::new(BrowserRunProvider::new(
            config.browser_run.clone(),
            egress,
            config.credentials.clone(),
            secrets,
            transport,
        )?)),
    }
}

/// Build the local provider over a host's driver.
pub fn local_provider(
    config: &WebBridgeConfig,
    driver: Arc<dyn PageDriver>,
    secrets: Arc<dyn SecretResolver>,
) -> Result<Box<dyn BrowserProvider>, BridgeError> {
    Ok(Box::new(LocalProvider::new(
        driver,
        config.egress()?,
        config.credentials.clone(),
        secrets,
        config.local.artifacts_dir(),
    )))
}
