//! Persistence of the scoped app session cookies.
//!
//! # Why Loams keeps its own ledger instead of trusting the engine's
//!
//! Obscura's `--storage-dir` does persist cookies (`cookies.json`,
//! `localStorage/<origin>.json`, flushed after every `Page.navigate`), so
//! waiting for the engine would have been the smaller change. It is not what
//! this crate does, for three reasons.
//!
//! 1. **The engine's file format is not a contract.** It is one crate's
//!    internal layout, described only in `docs/Persist-cookies-and-storage.md`
//!    of that project, and this crate pins a version. A Loams-owned,
//!    versioned file with its own error handling is something we can migrate.
//! 2. **The credential boundary is only auditable if Loams wrote the file.**
//!    [`crate::boundary`] scans the whole profile directory for a Loams token.
//!    That check is only meaningful because Loams knows every byte it put
//!    there; the engine's own `cookies.json` is still scanned, but the
//!    authoritative record is ours.
//! 3. **Re-injection has to happen anyway.** A cookie the app rotates during
//!    the session lives only in the engine's jar. Loams holds the credential
//!    it minted the session from, so on the next launch Loams re-injects from
//!    the credential path, not from whatever the engine happened to write.
//!
//! Concretely, "survives a process restart" is: build a [`ProfileKey`] for the
//! same `(environment, app)`, point it at the same base directory,
//! [`CookieLedger::load`] the cookies, and hand them to `Storage.setCookies`.
//! No process state is involved, which is what
//! `tests/persistence.rs::sessions_survive_a_process_restart` asserts by
//! dropping one whole set of objects and building another.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SidebarBrowserError};
use crate::profile::ProfileKey;
use crate::session::{EmbeddedOrigin, SameSite, ScopedSessionCookie};

/// The ledger's file name inside a profile directory.
pub const COOKIE_LEDGER: &str = "injected-cookies.json";

/// The ledger's schema version.
pub const COOKIE_LEDGER_VERSION: u32 = 1;

/// The persisted set of scoped app session cookies for one profile.
///
/// Ordering is by `(origin, name, path)` so a rewrite of an unchanged set is
/// byte-identical, which keeps a dirty profile directory from looking like a
/// change on every launch.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CookieLedger {
    cookies: Vec<ScopedSessionCookie>,
}

/// The on-disk shape. Private to this module so that [`ScopedSessionCookie`]
/// never gains a `Serialize` by accident: the wire format has to stay a
/// deliberate, versioned decision.
#[derive(Serialize, Deserialize)]
struct LedgerFile {
    version: u32,
    /// The one origin this profile embeds. Recorded so that a hand-edited file
    /// cannot introduce a second one: a cookie whose origin differs from this
    /// is refused on load rather than carried.
    origin: String,
    cookies: Vec<LedgerCookie>,
}

#[derive(Serialize, Deserialize)]
struct LedgerCookie {
    origin: String,
    name: String,
    value: String,
    path: String,
    secure: bool,
    http_only: bool,
    same_site: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires: Option<f64>,
}

impl CookieLedger {
    /// An empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a ledger for one origin's cookies.
    ///
    /// Refuses a mixed-origin set. One profile is one `(environment, app)` pair
    /// and therefore one embedded origin; a ledger holding two origins would be
    /// a file the engine could not be given as a unit, and would blur the
    /// isolation property [`crate::profile::ProfileKey`] exists to provide.
    pub fn for_origin(origin: &EmbeddedOrigin, cookies: Vec<ScopedSessionCookie>) -> Result<Self> {
        let mut seen = BTreeSet::new();
        for cookie in &cookies {
            if cookie.origin() != origin {
                return Err(SidebarBrowserError::Rejected(format!(
                    "cookie {:?} belongs to {}, not to {origin}; a profile holds one origin",
                    cookie.name(),
                    cookie.origin()
                )));
            }
            seen.insert((cookie.name().to_string(), cookie.path().to_string()));
        }
        if seen.len() != cookies.len() {
            return Err(SidebarBrowserError::Rejected(format!(
                "two cookies share a name and path for {origin}"
            )));
        }
        let mut ledger = Self { cookies };
        ledger.sort();
        Ok(ledger)
    }

    /// Add or replace a cookie.
    pub fn upsert(&mut self, cookie: ScopedSessionCookie) {
        self.cookies.retain(|existing| {
            existing.name() != cookie.name() || existing.path() != cookie.path()
        });
        self.cookies.push(cookie);
        self.sort();
    }

    /// Remove every cookie for an origin, returning how many went.
    pub fn clear_origin(&mut self, origin: &EmbeddedOrigin) -> usize {
        let before = self.cookies.len();
        self.cookies.retain(|cookie| cookie.origin() != origin);
        before - self.cookies.len()
    }

    /// The cookies, in `(origin, name, path)` order.
    pub fn cookies(&self) -> &[ScopedSessionCookie] {
        &self.cookies
    }

    /// The cookies for one origin, in `(name, path)` order.
    pub fn for_origin_cookies(&self, origin: &EmbeddedOrigin) -> Vec<&ScopedSessionCookie> {
        self.cookies
            .iter()
            .filter(|cookie| cookie.origin() == origin)
            .collect()
    }

    /// Whether the ledger is empty.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }

    fn sort(&mut self) {
        self.cookies.sort_by(|a, b| {
            a.origin()
                .cmp(b.origin())
                .then_with(|| a.name().cmp(b.name()))
                .then_with(|| a.path().cmp(b.path()))
        });
    }

    /// Write the ledger into a profile directory.
    ///
    /// `origin` is the profile's one embedded origin and is recorded in the
    /// file. A cookie in the ledger for any other origin is refused here as
    /// well as on load, so the two directions cannot disagree.
    ///
    /// The file is created with owner-only permissions on Unix, because it
    /// holds live app session cookies even though it holds no Loams token. On
    /// Windows the flag is a no-op and the profile directory inherits the
    /// per-user ACL of the app data directory.
    pub fn store(&self, profile_dir: impl AsRef<Path>, origin: &EmbeddedOrigin) -> Result<PathBuf> {
        let path = profile_dir.as_ref().join(COOKIE_LEDGER);
        if let Some(foreign) = self.cookies.iter().find(|cookie| cookie.origin() != origin) {
            return Err(SidebarBrowserError::Rejected(format!(
                "cookie {:?} belongs to {}, not to {origin}",
                foreign.name(),
                foreign.origin()
            )));
        }
        let file = LedgerFile {
            version: COOKIE_LEDGER_VERSION,
            origin: origin.as_display(),
            cookies: self
                .cookies
                .iter()
                .map(|cookie| LedgerCookie {
                    origin: cookie.origin().as_display(),
                    name: cookie.name().to_string(),
                    value: cookie.value().to_string(),
                    path: cookie.path().to_string(),
                    secure: cookie.secure(),
                    http_only: cookie.http_only(),
                    same_site: cookie.same_site().as_str().to_string(),
                    expires: cookie.expires(),
                })
                .collect(),
        };
        let mut bytes = serde_json::to_vec_pretty(&file)
            .map_err(|error| SidebarBrowserError::Rejected(error.to_string()))?;
        bytes.push(b'\n');
        write_private(&path, &bytes)?;
        Ok(path)
    }

    /// Read a ledger from a profile directory.
    ///
    /// A missing file is an empty ledger, not an error: the first launch of a
    /// new profile has nothing to restore. Every other failure is an error,
    /// because silently starting a profile logged out is exactly the
    /// persistence bug this module exists to prevent.
    pub fn load(profile_dir: impl AsRef<Path>) -> Result<Self> {
        let path = profile_dir.as_ref().join(COOKIE_LEDGER);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(source) => return Err(SidebarBrowserError::profile_io(&path, source)),
        };
        let file: LedgerFile = serde_json::from_slice(&bytes)
            .map_err(|error| SidebarBrowserError::profile_io(&path, to_io(error)))?;
        if file.version != COOKIE_LEDGER_VERSION {
            return Err(SidebarBrowserError::Rejected(format!(
                "{} is ledger version {}, this build reads {COOKIE_LEDGER_VERSION}",
                path.display(),
                file.version
            )));
        }
        let ledger_origin = EmbeddedOrigin::parse(&file.origin)?;
        let mut ledger = Self::new();
        for entry in file.cookies {
            let origin = EmbeddedOrigin::parse(&entry.origin)?;
            if origin != ledger_origin {
                return Err(SidebarBrowserError::Rejected(format!(
                    "{} is the ledger for {ledger_origin} but holds a cookie for {origin}",
                    path.display()
                )));
            }
            let cookie = ScopedSessionCookie::new(
                origin,
                entry.name,
                entry.value,
                crate::session::CookieAttributes {
                    path: entry.path,
                    secure: entry.secure,
                    http_only: entry.http_only,
                    same_site: SameSite::parse(&entry.same_site),
                    expires: entry.expires,
                },
            )?;
            ledger.upsert(cookie);
        }
        Ok(ledger)
    }

    /// Load a profile's ledger, verifying the directory really is that
    /// profile's before trusting anything in it.
    ///
    /// The manifest check is the cheap half of the isolation property: a
    /// directory whose `profile.json` names a different pair is not this
    /// pair's directory, and reading its cookies would be a cross-tenant leak
    /// caused by a wrong path rather than by a bug in the engine.
    pub fn load_for(profile_dir: impl AsRef<Path>, key: &ProfileKey) -> Result<Self> {
        let profile_dir = profile_dir.as_ref();
        match ProfileKey::read_manifest(profile_dir)? {
            Some(found) if &found == key => Ok(Self::load(profile_dir)?),
            Some(found) => Err(SidebarBrowserError::Rejected(format!(
                "{} is the profile for {found}, not for {key}",
                profile_dir.display()
            ))),
            None => Err(SidebarBrowserError::Rejected(format!(
                "{} has no {}; refusing to read a profile whose key is unknown",
                profile_dir.display(),
                crate::profile::PROFILE_MANIFEST
            ))),
        }
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| SidebarBrowserError::profile_io(path, source))?;
    file.write_all(bytes)
        .map_err(|source| SidebarBrowserError::profile_io(path, source))?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    // Windows has no `mode`; the profile directory lives under the per-user
    // app data directory and inherits its ACL.
    std::fs::write(path, bytes).map_err(|source| SidebarBrowserError::profile_io(path, source))
}

fn to_io(error: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}
