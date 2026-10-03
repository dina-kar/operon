//! Launching and supervising the engine process.
//!
//! # The pinned version
//!
//! [`PINNED_ENGINE_VERSION`] is **0.2.3**, verified by reading that tag's
//! source. It is checked at startup rather than assumed, because the two
//! things this crate depends on most are exactly the things that move: the
//! CDP method set, and the `--allow-private-network` spelling. Both changed
//! during the engine's development (`--allow-private-network` replaced an
//! env-var-only path in issue #33 of that project), so a binary from a
//! different release can silently lose a capability rather than fail loudly.
//!
//! **The release assets that matter are the unsuffixed ones.** The project
//! publishes four variants per target and the `-no-render` ones do not have
//! the `render` feature, which is what `Page.startScreencast`,
//! `Page.stopScreencast`, `Page.captureScreenshot` and `Page.printToPDF` are
//! compiled behind. A `-no-render` binary answers those methods with "requires
//! a build with the render feature", so the install instructions in this
//! crate's README are explicit about which asset to take.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use crate::error::{Result, SidebarBrowserError};
use crate::net_policy::{self, PrivateNetworkDecision};
use crate::session::EmbeddedOrigin;

/// The engine release this crate is written against.
pub const PINNED_ENGINE_VERSION: &str = "0.2.3";

/// The default CDP port, matching the engine's own `--port` default.
pub const DEFAULT_CDP_PORT: u16 = 9222;

/// The interface the engine binds. Loopback only.
///
/// Obscura's `--host` also defaults to `127.0.0.1`, but Loams states it
/// explicitly rather than inheriting a default: the CDP endpoint has no
/// authentication, so anything that can open a WebSocket to it can read the
/// profile's cookies and drive the engine. Binding it is the only control.
pub const BIND_HOST: &str = "127.0.0.1";

/// The environment variable that overrides the engine binary's path.
pub const OBSCURA_BIN_ENV: &str = "LOAMS_OBSCURA_BIN";

/// How long to wait for the engine to accept a CDP connection after spawn.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// A fully resolved engine invocation, built without touching the filesystem.
///
/// Pure so that the argument vector — including the decision about
/// `--allow-private-network`, which is the security-relevant one — is
/// assertable in a test without starting a process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineLaunchSpec {
    program: PathBuf,
    args: Vec<String>,
    port: u16,
    allow_private_network: bool,
    profile_dir: PathBuf,
}

impl EngineLaunchSpec {
    /// Build the invocation for one profile.
    ///
    /// `decision` is the caller's [`net_policy::decide_origin`] result. It is
    /// passed through [`net_policy::engine_relaxation`], so a refused origin
    /// cannot reach this function: there is no path from a refusal to a
    /// launched engine.
    pub fn build(
        program: impl Into<PathBuf>,
        profile_dir: impl Into<PathBuf>,
        port: u16,
        decision: &PrivateNetworkDecision,
    ) -> Result<Self> {
        let profile_dir = profile_dir.into();
        let mut args = vec![
            "serve".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--host".to_string(),
            BIND_HOST.to_string(),
            // One worker. The engine's `--workers` default is already 1, and
            // a docked panel needs one page at a time, not a fleet.
            "--workers".to_string(),
            "1".to_string(),
            "--storage-dir".to_string(),
            profile_dir.display().to_string(),
        ];
        let allow_private_network = match decision {
            PrivateNetworkDecision::Public => false,
            PrivateNetworkDecision::Permitted { .. } => true,
            PrivateNetworkDecision::Refused { reason } => {
                return Err(SidebarBrowserError::NetworkRefused {
                    reason: format!("refusing to launch an engine for a refused target ({reason})"),
                });
            }
        };
        if allow_private_network {
            args.push("--allow-private-network".to_string());
        }
        Ok(Self {
            program: program.into(),
            args,
            port,
            allow_private_network,
            profile_dir,
        })
    }

    /// Build the invocation for an origin, deciding the network policy here.
    ///
    /// A refused origin is refused twice over: once by
    /// [`net_policy::engine_relaxation`], which refuses to authorise the
    /// engine's relaxation for a target Loams refused, and again by
    /// [`EngineLaunchSpec::build`]. The duplication is deliberate — one of the
    /// two is the policy and the other is the thing that would launch a process,
    /// and the invariant worth defending is "a refusal cannot become a running
    /// engine", not "there is one place that says no".
    pub fn for_origin(
        program: impl Into<PathBuf>,
        profile_dir: impl Into<PathBuf>,
        port: u16,
        origin: &EmbeddedOrigin,
    ) -> Result<Self> {
        let decision = net_policy::decide_origin(origin)?;
        net_policy::engine_relaxation(origin, &decision)?;
        Self::build(program, profile_dir, port, &decision)
    }

    /// The engine binary.
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// The argument vector, without the program.
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// The CDP port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Whether `--allow-private-network` was requested.
    pub fn allows_private_network(&self) -> bool {
        self.allow_private_network
    }

    /// The profile directory passed as `--storage-dir`.
    pub fn profile_dir(&self) -> &Path {
        &self.profile_dir
    }

    /// The DevTools URL for this invocation.
    pub fn devtools_url(&self) -> String {
        format!("ws://{BIND_HOST}:{}/devtools/browser", self.port)
    }

    /// Assert that the invocation binds loopback.
    ///
    /// Called by [`ObscuraEngine::start`] so a spec assembled by hand in a test
    /// or a future call site cannot quietly widen the bind address.
    pub fn assert_loopback(&self) -> Result<()> {
        let bound = self
            .args
            .windows(2)
            .find(|pair| pair[0] == "--host")
            .map(|pair| pair[1].as_str());
        match bound {
            Some(host) if host == BIND_HOST => Ok(()),
            Some(host) => Err(SidebarBrowserError::Rejected(format!(
                "the engine would bind {host}; its CDP endpoint has no authentication, so it must \
                 bind {BIND_HOST}"
            ))),
            None => Err(SidebarBrowserError::Rejected(format!(
                "the invocation has no --host, so it would inherit a default this crate does not \
                 control; it must state {BIND_HOST}"
            ))),
        }
    }

    /// The command line, for a log line and for a test.
    pub fn command_line(&self) -> String {
        std::iter::once(self.program.display().to_string())
            .chain(self.args.iter().cloned())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Locate the engine binary.
///
/// [`OBSCURA_BIN_ENV`] wins if it is set, so an operator can point at a
/// pinned, checksummed copy of the release asset. Otherwise `obscura` is
/// looked up on `PATH`.
pub fn resolve_program() -> PathBuf {
    match std::env::var_os(OBSCURA_BIN_ENV) {
        Some(value) if !value.is_empty() => PathBuf::from(value),
        _ => PathBuf::from("obscura"),
    }
}

/// Read the engine's `--version` output and compare it with the pin.
///
/// Obscura prints its build version for `--version` (`OBSCURA_BUILD_VERSION`).
/// Only a leading `v` is tolerated and only surrounding whitespace, because a
/// loose match would accept a different release that happens to contain the
/// pinned string.
pub fn check_version(reported: &str) -> Result<()> {
    let found = reported
        .trim()
        .trim_start_matches("obscura ")
        .trim()
        .trim_start_matches('v')
        .trim();
    if found == PINNED_ENGINE_VERSION {
        return Ok(());
    }
    Err(SidebarBrowserError::EngineVersion {
        expected: PINNED_ENGINE_VERSION.to_string(),
        found: if found.is_empty() {
            "<nothing>".to_string()
        } else {
            found.to_string()
        },
    })
}

/// A running engine process.
#[derive(Debug)]
pub struct ObscuraEngine {
    spec: EngineLaunchSpec,
    child: Option<tokio::process::Child>,
}

impl ObscuraEngine {
    /// Spawn the engine and wait for its CDP endpoint to accept a connection.
    ///
    /// Readiness is a real TCP connect to the loopback port, not a sleep: the
    /// desktop opens the sidebar immediately, and a panel that renders a
    /// connection error because the engine was 200 ms behind is a bug report
    /// nobody can act on.
    pub async fn start(spec: EngineLaunchSpec, timeout: Duration) -> Result<Self> {
        spec.assert_loopback()?;
        let mut child = tokio::process::Command::new(spec.program())
            .args(spec.args())
            // The engine's CDP transport is plaintext on loopback and its own
            // logging goes to stderr; keeping stdout quiet avoids it
            // interleaving with anything the desktop prints.
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                SidebarBrowserError::Engine(format!(
                    "spawning {}: {error}. Set {OBSCURA_BIN_ENV} to the pinned Obscura \
                     {PINNED_ENGINE_VERSION} binary; see this crate's README for the release \
                     asset to use (the unsuffixed one, not -no-render).",
                    spec.program().display()
                ))
            })?;
        let url = spec.devtools_url();
        wait_for_port(BIND_HOST, spec.port, timeout)
            .await
            .map_err(|error| {
                let _ = child.start_kill();
                SidebarBrowserError::Engine(format!(
                    "{} did not accept a CDP connection on {url} within {}s: {error}",
                    spec.program().display(),
                    timeout.as_secs()
                ))
            })?;
        Ok(Self {
            spec,
            child: Some(child),
        })
    }

    /// The DevTools URL to connect the CDP client to.
    pub fn devtools_url(&self) -> String {
        self.spec.devtools_url()
    }

    /// The spec this engine was launched from.
    pub fn spec(&self) -> &EngineLaunchSpec {
        &self.spec
    }

    /// Stop the engine: ask it to close over CDP, then kill it if it will not.
    ///
    /// The kill is unconditional on the second path because the engine's CDP
    /// endpoint has no shutdown guarantee. `kill_on_drop` is also set, so a
    /// dropped engine cannot outlive the desktop.
    pub async fn stop(mut self) -> Result<()> {
        match self.child.take() {
            Some(mut child) => {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

async fn wait_for_port(host: &str, port: u16, timeout: Duration) -> std::io::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let address = format!("{host}:{port}");
    let mut last: Option<std::io::Error> = None;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(last.unwrap_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out")
            }));
        }
        match tokio::net::TcpStream::connect(&address).await {
            Ok(_) => return Ok(()),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
