//! The audit behind SF1's Global Constraint, "embeds never hold a Loams token".
//!
//! # What is being claimed
//!
//! D620 ships the sidebar browser on a **persistent** engine profile. That is
//! the opposite of the situation SF1 Task 0 relied on when it said the
//! constraint was easy to keep: an ephemeral store cannot retain a token
//! because it retains nothing. So the claim has to be earned, and this module
//! is the part of the code that earns it.
//!
//! # What is checked
//!
//! [`audit_profile`] walks a profile directory — the whole directory, including
//! the engine's own `cookies.json` and `localStorage/<origin>.json`, which this
//! crate did not write — and refuses if the secret appears anywhere in it.
//! [`assert_cookies_exclude`] does the same over the cookie set the engine
//! reports back through `Storage.getCookies`, which is the other half: a token
//! that reached the jar but was never written to disk would still be in the
//! engine's memory.
//!
//! The directory scan searches for the secret in its raw bytes **and** in the
//! base64 spellings a page can produce with `btoa`. A raw-only scan would be
//! evadable in the way that matters — a page that receives a token in a URL
//! fragment and then calls `btoa()` on it leaves a credential in
//! `localStorage` that no literal search finds.
//!
//! # What this is not
//!
//! It is a regression test, not a proof. The structural half of the guarantee is
//! in [`crate::session`]: [`ScopedSessionCookie`](crate::session::ScopedSessionCookie)
//! has no constructor that accepts a
//! [`LoamsCredential`](crate::session::LoamsCredential), it has no domain
//! argument so a cookie cannot be scoped beyond the embedded origin, and
//! [`LoamsCredential`](crate::session::LoamsCredential) has no `Serialize` and
//! a redacted `Debug`. This module is what catches someone later routing around
//! those types — which is the realistic failure, because routing around a type
//! is one line and forgetting why the type existed is easy.

use std::path::{Path, PathBuf};

use base64::Engine as _;

use crate::error::{Result, SidebarBrowserError};
use crate::session::ScopedSessionCookie;

/// How deep [`audit_profile`] descends below the profile directory.
///
/// The engine writes `cookies.json` and one file per origin, both at the top
/// level, but a page can create arbitrarily nested `localStorage` keys. This is
/// a backstop against a symlink loop or a pathological tree, not a limit on
/// what is scanned in practice.
pub const MAX_SCAN_DEPTH: usize = 32;

/// The most bytes [`audit_profile`] will read from one file.
pub const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Assert that no cookie the engine reports carries the secret.
pub fn assert_cookies_exclude(cookies: &[ScopedSessionCookie], secret: &str) -> Result<()> {
    if secret.is_empty() {
        return Err(SidebarBrowserError::BoundaryViolation(
            "refusing to audit against an empty secret; an empty needle matches everything"
                .to_string(),
        ));
    }
    for cookie in cookies {
        if cookie.value() == secret {
            return Err(SidebarBrowserError::BoundaryViolation(format!(
                "the engine's cookie jar holds the Loams token as cookie {:?} for {}",
                cookie.name(),
                cookie.origin()
            )));
        }
        // Substring rather than equality: a token can arrive base64-wrapped, or
        // as the value of a cookie the page set itself from a URL fragment.
        if cookie.value().contains(secret) {
            return Err(SidebarBrowserError::BoundaryViolation(format!(
                "cookie {:?} for {} contains the Loams token as a substring",
                cookie.name(),
                cookie.origin()
            )));
        }
    }
    Ok(())
}

/// Walk `profile_dir` and refuse if `secret` appears in any file in it.
///
/// The needle is checked in more than one encoding. A raw byte search alone is
/// evadable in the way that matters: a page that receives a token in a URL
/// fragment and then calls `btoa()` on it has put a credential in
/// `localStorage` that no literal scan would find. So the raw form and the three
/// base64 spellings a page can produce are all searched for. This is a
/// regression test's worth of thoroughness, not a claim of completeness — see
/// the module docs.
///
/// Returns the paths that were scanned on success, so a caller can log the
/// scope of the check rather than a bare "ok".
pub fn audit_profile(profile_dir: impl AsRef<Path>, secret: &str) -> Result<Vec<PathBuf>> {
    let profile_dir = profile_dir.as_ref();
    if secret.is_empty() {
        return Err(SidebarBrowserError::BoundaryViolation(
            "refusing to audit against an empty secret; an empty needle matches everything"
                .to_string(),
        ));
    }
    let forms = derived_forms(secret);
    let mut scanned = Vec::new();
    walk(profile_dir, 0, &forms, &mut scanned)?;
    Ok(scanned)
}

/// The encodings of `secret` that are searched for.
///
/// Raw bytes plus the base64 spellings `btoa` and its relatives produce:
/// standard with padding, standard without, and URL-safe without. Padded and
/// unpadded differ by two characters at the end, so a page that strips the
/// padding defeats a scan that only looks at one of them.
fn derived_forms(secret: &str) -> Vec<Vec<u8>> {
    let raw = secret.as_bytes().to_vec();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let mut forms = vec![encoded.as_bytes().to_vec()];
    forms.push(encoded.trim_end_matches('=').as_bytes().to_vec());
    forms.push(
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(&raw)
            .into_bytes(),
    );
    // Raw last: it is the form a leak is overwhelmingly most likely to take,
    // and putting it first would make every base64 hit report the wrong shape.
    forms.push(raw);
    forms.retain(|form| !form.is_empty());
    forms
}

fn walk(dir: &Path, depth: usize, needles: &[Vec<u8>], scanned: &mut Vec<PathBuf>) -> Result<()> {
    if depth > MAX_SCAN_DEPTH {
        return Err(SidebarBrowserError::BoundaryViolation(format!(
            "{} is deeper than {MAX_SCAN_DEPTH} levels below the profile directory",
            dir.display()
        )));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(SidebarBrowserError::profile_io(dir, source)),
    };
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| SidebarBrowserError::profile_io(dir, source))?;
        paths.push(entry.path());
    }
    // Sorted so the audit's output is stable and a difference between two runs
    // is a real difference.
    paths.sort();
    for path in paths {
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(SidebarBrowserError::profile_io(&path, source)),
        };
        if metadata.file_type().is_symlink() {
            // A symlink in a profile directory is not something this crate
            // writes. Following it would let a page-stage walk out of the
            // directory, so it is reported rather than followed.
            return Err(SidebarBrowserError::BoundaryViolation(format!(
                "{} is a symlink; the sidebar browser never creates one in a profile directory",
                path.display()
            )));
        }
        if metadata.is_dir() {
            walk(&path, depth + 1, needles, scanned)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        if metadata.len() > MAX_FILE_BYTES {
            return Err(SidebarBrowserError::BoundaryViolation(format!(
                "{} is {} bytes, over the {MAX_FILE_BYTES}-byte audit limit",
                path.display(),
                metadata.len()
            )));
        }
        let bytes = std::fs::read(&path)
            .map_err(|source| SidebarBrowserError::profile_io(&path, source))?;
        if let Some(needle) = needles.iter().find(|needle| find(&bytes, needle).is_some()) {
            return Err(SidebarBrowserError::BoundaryViolation(format!(
                "the Loams token is present in {} inside the engine's profile directory \
                 (matched {} bytes, raw or base64-encoded)",
                path.display(),
                needle.len()
            )));
        }
        scanned.push(path);
    }
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
