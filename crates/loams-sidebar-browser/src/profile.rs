//! The persistent per-(environment, app) profile directory.
//!
//! # Why this module exists
//!
//! SF1 Task 0's ruling E1 blocked the sidebar browser partly because the
//! per-platform webview's website-data store was ephemeral
//! (`WKWebsiteDataStore::nonPersistentDataStore` on macOS,
//! `webkit_web_context_new_ephemeral()` on the Linux helper). D620 supersedes
//! E1 by replacing the webview, and the replacement has a real on-disk store.
//! This module is the part of that we own: it decides *where* that store
//! lives, and it guarantees two properties the feature depends on.
//!
//! 1. **Isolation.** Distinct `(environment, app)` pairs get distinct
//!    directories. A developer signed into Zulip in `env_dev` must not find
//!    that session in `env_prod`, and Forgejo's cookies must not be readable
//!    as Zulip's.
//! 2. **Durability.** The directory is derived from the pair alone, with no
//!    process-lifetime state, so the same pair finds the same directory on the
//!    next launch. That is the whole of "survives a restart"; everything else
//!    is [`crate::session`].
//!
//! The layout is two levels of real path segments rather than one encoded
//! name. That is deliberate: a single joined name needs an escaping scheme, and
//! every escaping scheme has an injection or ambiguity case
//! (`("a_b", "c")` versus `("a", "b_c")` collide under any separator that can
//! appear in an identifier). Two segments cannot collide at all, because a
//! validated segment cannot contain a separator.
//!
//! ```
//! # use loams_sidebar_browser::profile::ProfileKey;
//! let key = ProfileKey::new("env_dev", "zulip").expect("valid pair");
//! assert_eq!(key.profile_dir("/data/sidebar").to_str().unwrap(),
//!            "/data/sidebar/env_dev/zulip");
//! assert!(ProfileKey::new("env_dev", "../etc").is_err());
//! ```

use std::fmt;
use std::path::{Path, PathBuf};

use crate::error::{Result, SidebarBrowserError};

/// The name of the manifest written into every profile directory.
pub const PROFILE_MANIFEST: &str = "profile.json";

/// The manifest's schema version, so a future format change is detectable
/// rather than silently misread.
pub const PROFILE_MANIFEST_VERSION: u32 = 1;

/// Longest accepted identifier component. Directory names longer than this
/// are a mistake, and on macOS a single path component is capped at 255 bytes
/// anyway.
pub const MAX_COMPONENT_LEN: usize = 64;

/// Identifies one embedded app in one environment.
///
/// This is the key of the persistent profile. It is deliberately two strings
/// rather than a combined name: see the module docs.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileKey {
    environment: String,
    app: String,
}

impl ProfileKey {
    /// Build a key, rejecting anything that is not a safe path segment.
    ///
    /// The accepted grammar is `[A-Za-z0-9._-]`, one or more characters, not
    /// `.` or `..`, not starting with `.`, and not one of Windows' reserved
    /// device names. The Windows reservation matters because the same profile
    /// directory has to work on all three desktop targets: `CON` is a perfectly
    /// reasonable environment name to a developer and an unusable directory on
    /// Windows.
    pub fn new(environment: impl Into<String>, app: impl Into<String>) -> Result<Self> {
        let environment = environment.into();
        let app = app.into();
        check_component("environment", &environment)?;
        check_component("app", &app)?;
        Ok(Self { environment, app })
    }

    /// The environment identifier, e.g. `env_dev`.
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// The app identifier, e.g. `zulip`.
    pub fn app(&self) -> &str {
        &self.app
    }

    /// The profile directory for this pair under `base`, without creating it.
    ///
    /// `base` is expected to be the app's data directory. It is used verbatim:
    /// Loams does not get to decide where an operator keeps state.
    pub fn profile_dir(&self, base: impl AsRef<Path>) -> PathBuf {
        base.as_ref().join(&self.environment).join(&self.app)
    }

    /// Create the profile directory and write its manifest.
    ///
    /// The manifest records the pair and the schema version so that a
    /// directory written by an older Loams, or by a different key, is visible
    /// rather than assumed compatible. It contains no credential: the only
    /// cookie material under this directory is
    /// [`crate::ledger::COOKIE_LEDGER`], which holds embedded-origin scoped
    /// app session cookies.
    pub fn ensure_profile_dir(&self, base: impl AsRef<Path>) -> Result<PathBuf> {
        let dir = self.profile_dir(base);
        std::fs::create_dir_all(&dir)
            .map_err(|source| SidebarBrowserError::profile_io(&dir, source))?;
        let manifest = serde_json::json!({
            "version": PROFILE_MANIFEST_VERSION,
            "environment": self.environment,
            "app": self.app,
        });
        let path = dir.join(PROFILE_MANIFEST);
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| SidebarBrowserError::Rejected(error.to_string()))?;
        std::fs::write(&path, bytes)
            .map_err(|source| SidebarBrowserError::profile_io(&path, source))?;
        Ok(dir)
    }

    /// Read back the pair a profile directory claims to be for.
    ///
    /// `Ok(None)` when the manifest is absent. This is what makes
    /// "sessions survive a process restart" checkable from the outside: a
    /// fresh key built from the same pair, pointed at the same base, must
    /// report the same directory.
    pub fn read_manifest(dir: impl AsRef<Path>) -> Result<Option<ProfileKey>> {
        let dir = dir.as_ref();
        let path = dir.join(PROFILE_MANIFEST);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(SidebarBrowserError::profile_io(&path, source)),
        };
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| SidebarBrowserError::profile_io(&path, to_io(error)))?;
        let version = value
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if version != u64::from(PROFILE_MANIFEST_VERSION) {
            return Err(SidebarBrowserError::Rejected(format!(
                "{} has profile manifest version {version}, this build writes {PROFILE_MANIFEST_VERSION}",
                path.display()
            )));
        }
        let environment = value.get("environment").and_then(serde_json::Value::as_str);
        let app = value.get("app").and_then(serde_json::Value::as_str);
        match (environment, app) {
            (Some(environment), Some(app)) => Ok(Some(ProfileKey::new(environment, app)?)),
            _ => Err(SidebarBrowserError::Rejected(format!(
                "{} is missing the environment or app component",
                path.display()
            ))),
        }
    }
}

fn to_io(error: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

impl fmt::Debug for ProfileKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProfileKey")
            .field("environment", &self.environment)
            .field("app", &self.app)
            .finish()
    }
}

impl fmt::Display for ProfileKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.environment, self.app)
    }
}

/// Windows device names, which cannot be directory names there.
///
/// Matched case-insensitively and ignoring an extension, so `con` and
/// `con.txt` are both refused. The list is the documented reserved set plus
/// the superscript digits Windows also reserves.
const WINDOWS_RESERVED: &[&str] = &[
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
    "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9", "com¹", "com²",
    "com³", "lpt¹", "lpt²", "lpt³",
];

fn check_component(what: &str, value: &str) -> Result<()> {
    let reject = |reason: &str| {
        Err(SidebarBrowserError::Rejected(format!(
            "profile {what} {value:?} is not usable as a directory name: {reason}"
        )))
    };

    if value.is_empty() {
        return reject("it is empty");
    }
    if value.len() > MAX_COMPONENT_LEN {
        return reject(&format!(
            "it is {} bytes, over the {MAX_COMPONENT_LEN}-byte limit",
            value.len()
        ));
    }
    if value == "." || value == ".." {
        return reject("it is a relative path component");
    }
    if value.starts_with('.') {
        return reject("a leading dot would make it a hidden directory");
    }
    if value.ends_with('.') || value.ends_with(' ') {
        return reject("Windows silently drops a trailing dot or space");
    }
    if let Some(bad) = value
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return reject(&format!("it contains {bad:?}, outside [A-Za-z0-9._-]"));
    }
    let stem = value
        .split('.')
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase();
    if WINDOWS_RESERVED.contains(&stem.as_str()) {
        return reject("it is a reserved Windows device name");
    }
    Ok(())
}
