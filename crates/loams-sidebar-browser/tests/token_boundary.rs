//! SF1's Global Constraint, as an executable claim: **embeds never hold a
//! Loams token**.
//!
//! # Why this test is the important one
//!
//! D620 supersedes E1 and ships the embed on a **persistent** engine profile.
//! E1 could claim the constraint was easy to keep precisely because the store
//! it was reasoning about was ephemeral, and an ephemeral store retains
//! nothing. A persistent profile is the opposite case: it retains everything,
//! including anything a page writes into it, and it survives a restart. So
//! "the embed is a browser on a persistent disk" is exactly the design in which
//! this constraint would be violated by accident, and it needs to be a test.
//!
//! # What is asserted
//!
//! 1. A Loams token, injected the way a careless implementation would inject
//!    it, **is found** by [`audit_profile`]. The check is not vacuous: it has to
//!    fail when something is actually wrong.
//! 2. The same check **passes** on a profile directory built the way this crate
//!    builds one, containing the engine's own `cookies.json` and a
//!    `localStorage/<origin>.json`, holding only the app's scoped session
//!    cookies.
//! 3. The cookie jar the engine reports back contains no Loams token, and the
//!    check catches one that does.
//! 4. The [`LoamsCredential`] type cannot reach a [`ScopedSessionCookie`]: its
//!    `Debug` is redacted, and this crate's own sources never call
//!    `LoamsCredential::expose` outside a test.
//!
//! Points 1 and 4 are the ones that keep the others honest. A boundary test
//! that cannot fail proves nothing, and a redacted `Debug` proves nothing if
//! nothing calls the accessor.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use loams_sidebar_browser::{
    CdpClient, CookieAttributes, CookieLedger, EmbeddedOrigin, LoamsCredential, ProfileKey,
    ScopedSessionCookie, SidebarBrowser, assert_cookies_exclude, audit_profile, cookies_from_cdp,
};
use tempfile::TempDir;

/// A Loams API token, long and high-entropy so the audit is not testing a
/// placeholder that could never match anything.
///
/// Deliberately not shaped like any third-party provider's live key. GitHub's
/// push protection matches on those patterns, and a fixture that trips it makes
/// the branch unpushable — which is the correct outcome for a real key and a
/// pointless obstacle for a synthetic one. This is a Loams token, and Loams
/// tokens are not anybody else's key format.
fn loams_token() -> LoamsCredential {
    LoamsCredential::new(
        "loams.tok.9f3a1c7e5b2d8046af1e37c9d0b5a6e8f2c4d1a9b7e3f5c8a2d6b0e4f7a1c3d5",
    )
}

/// The embedded app's session cookie, which is what the engine *is* given.
fn chat() -> EmbeddedOrigin {
    EmbeddedOrigin::parse("https://chat.example.com").expect("an origin")
}

fn app_session(value: &str) -> ScopedSessionCookie {
    ScopedSessionCookie::new(
        chat(),
        "__Host-sessionid",
        value,
        CookieAttributes::session(&chat()).with_expires(1_900_000_000.0),
    )
    .expect("a host-only session cookie")
}

fn profile(base: &TempDir) -> SidebarBrowser {
    SidebarBrowser::prepare(
        base.path(),
        ProfileKey::new("env_prod", "zulip").expect("a valid pair"),
        EmbeddedOrigin::parse("https://chat.example.com").expect("an origin"),
        "obscura",
        9222,
    )
    .expect("a launch plan")
}

/// Write the files the pinned engine writes into a profile directory.
///
/// `cookies.json` and `localStorage/<origin>.json` are the engine's own layout
/// (`docs/Persist-cookies-and-storage.md` of the engine's repository, verified
/// against v0.2.3). Writing them here means the audit covers bytes this crate
/// did not produce, which is the only version of this test worth having.
fn write_engine_files(dir: &Path, jar: &str, local_storage: &str) {
    std::fs::write(dir.join("cookies.json"), jar).expect("a written jar");
    let storage = dir.join("localStorage");
    std::fs::create_dir_all(&storage).expect("a storage directory");
    std::fs::write(storage.join("https_chat.example.com.json"), local_storage).expect("storage");
}

/// A profile built the way this crate builds one holds no Loams token.
///
/// The app's session cookie is there; the Loams token is not.
#[test]
fn a_built_profile_holds_no_loams_token() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let browser = profile(&base);
    let dir = browser.plan().profile_dir();
    let token = loams_token();

    // Loams's own ledger, holding only the app's scoped session cookie.
    CookieLedger::for_origin(browser.origin(), vec![app_session("zulip-session-abc")])
        .expect("a ledger")
        .store(dir, browser.origin())
        .expect("a stored ledger");

    // And the engine's own files, as it would have written them after the
    // page loaded. Neither mentions a Loams token.
    write_engine_files(
        dir,
        r#"[{"name":"__Host-sessionid","value":"zulip-session-abc","domain":"chat.example.com",
             "path":"/","secure":true,"httpOnly":true,"sameSite":"Lax"}]"#,
        r#"{"theme":"dark"}"#,
    );

    let scanned = audit_profile(dir, token.expose()).expect("the profile must be clean");
    assert!(
        scanned.len() >= 3,
        "the audit should have covered the manifest, the ledger and the engine's files, \
         covered {}",
        scanned.len()
    );
    assert!(
        scanned.iter().any(|path| path.ends_with("cookies.json")),
        "the engine's own cookie jar must be in scope"
    );
}

/// The check is not vacuous: a Loams token written into the profile the way a
/// careless implementation would write it **is caught**.
///
/// Every shape below is something that could plausibly happen: the engine
/// persisting a cookie the page set from a URL fragment, the desktop writing
/// the token into the ledger "just to reuse the cookie", or a token arriving
/// base64-encoded inside a storage value.
#[test]
fn a_leaked_token_is_caught_in_every_shape() {
    let token = loams_token();
    let needle = token.expose();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(needle.as_bytes());

    for (name, contents) in [
        ("cookies.json", needle.to_string()),
        (
            "localStorage/https_chat.example.com.json",
            needle.to_string(),
        ),
        ("nested", format!("prefix {needle} suffix")),
        ("base64 in storage", encoded),
    ] {
        let base = tempfile::tempdir().expect("a temporary directory");
        let dir = base.path().join("env_prod").join("zulip");
        std::fs::create_dir_all(&dir).expect("a directory");
        let file = dir.join(name);
        std::fs::create_dir_all(file.parent().expect("a parent directory"))
            .expect("a parent directory");
        std::fs::write(&file, &contents).expect("a written file");
        let error =
            audit_profile(&dir, needle).expect_err(&format!("a token in {name} must be caught"));
        assert!(
            error.to_string().contains("Loams token"),
            "the error for {name} should name the violation, got: {error}"
        );
        assert!(
            error.to_string().contains(&file.display().to_string()),
            "the error for {name} should name the file, got: {error}"
        );
    }
}

/// The cookie jar the engine reports is checked too, so a token that reached
/// memory but never reached disk is still caught.
#[test]
fn the_cookie_jar_is_checked() {
    let token = loams_token();

    let clean = vec![app_session("zulip-session-abc")];
    assert_cookies_exclude(&clean, token.expose()).expect("a clean jar passes");

    let leaked = vec![
        app_session("zulip-session-abc"),
        ScopedSessionCookie::new(
            chat(),
            "loams_session",
            token.expose(),
            CookieAttributes::session(&chat()),
        )
        .expect("a host-only cookie"),
    ];
    let error = assert_cookies_exclude(&leaked, token.expose())
        .expect_err("a token in the jar must be caught");
    assert!(
        error.to_string().contains("loams_session"),
        "the error should name the cookie, got: {error}"
    );
}

/// A token wrapped inside a cookie value is caught too, not only an exact
/// match.
#[test]
fn a_token_embedded_in_a_cookie_value_is_caught() {
    let token = loams_token();
    let cookie = ScopedSessionCookie::new(
        chat(),
        "redirect",
        format!("https://app.example.com/?t={}", token.expose()),
        CookieAttributes::session(&chat()).with_http_only(false),
    )
    .expect("a host-only cookie");
    let error = assert_cookies_exclude(&[cookie], token.expose())
        .expect_err("a token inside a cookie value must be caught");
    assert!(error.to_string().contains("substring"), "got: {error}");
}

/// An empty needle is refused, because it matches everything and would make
/// the audit fail for the wrong reason.
#[test]
fn an_empty_needle_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let browser = profile(&base);
    assert_cookies_exclude(&[app_session("s")], "").expect_err("an empty needle is refused");
    audit_profile(browser.plan().profile_dir(), "").expect_err("an empty needle is refused");
}

/// A symlink in the profile directory is refused rather than followed.
///
/// Nothing this crate writes creates one, so finding one means something else
/// put it there, and following it would walk the audit out of the directory it
/// is meant to bound.
#[cfg(unix)]
#[test]
fn a_symlink_in_the_profile_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let browser = profile(&base);
    let dir = browser.plan().profile_dir();
    let outside = base.path().join("outside.txt");
    std::fs::write(&outside, "a token elsewhere").expect("a written file");
    std::os::unix::fs::symlink(&outside, dir.join("link.txt")).expect("a symlink");
    let error = audit_profile(dir, "a token elsewhere").expect_err("a symlink must be refused");
    assert!(error.to_string().contains("symlink"), "got: {error}");
}

/// The engine's `Storage.getCookies` response is parsed into cookies, and a
/// foreign-origin cookie in it is dropped rather than accepted.
#[test]
fn the_engines_cookie_response_is_parsed_scoped() {
    let origin = EmbeddedOrigin::parse("https://chat.example.com").expect("an origin");
    let response = serde_json::json!({
        "cookies": [
            {
                "name": "__Host-sessionid",
                "value": "zulip-session-abc",
                "domain": "chat.example.com",
                "path": "/",
                "secure": true,
                "httpOnly": true,
                "sameSite": "Lax",
                "expires": 1_900_000_000.0
            },
            {
                "name": "session-id",
                "value": "a-sibling-subdomain-session",
                "domain": "plane.example.com",
                "path": "/",
                "secure": true,
                "httpOnly": true,
                "sameSite": "Lax"
            },
            {
                "name": "sessionid",
                "value": "a-non-secure-session",
                "domain": "chat.example.com",
                "path": "/",
                "secure": false,
                "httpOnly": true,
                "sameSite": "Lax"
            },
            { "domain": "chat.example.com" }
        ]
    });
    let cookies = cookies_from_cdp(&response, &origin).expect("a parseable response");
    assert_eq!(
        cookies.len(),
        1,
        "only the in-scope, in-policy cookie survives"
    );
    assert_eq!(cookies[0].name(), "__Host-sessionid");
    assert_eq!(cookies[0].origin(), &origin);
    assert!(cookies[0].secure());
}

/// The redacted `Debug` does not print the token.
#[test]
fn the_credentials_debug_is_redacted() {
    let token = loams_token();
    let rendered = format!("{token:?}");
    assert!(
        !rendered.contains(token.expose()),
        "Debug must not print the token, got: {rendered}"
    );
    assert_eq!(rendered, "LoamsCredential(<redacted>)");
}

/// The cookie's `Debug` does not print its value either, because an app session
/// is a live credential even though it is not a Loams one.
#[test]
fn the_cookie_debug_is_redacted() {
    let cookie = app_session("zulip-session-abc");
    let rendered = format!("{cookie:?}");
    assert!(
        !rendered.contains("zulip-session-abc"),
        "Debug must not print a live session value, got: {rendered}"
    );
    assert!(rendered.contains("__Host-sessionid"), "got: {rendered}");
    assert!(rendered.contains("chat.example.com"), "got: {rendered}");
}

/// In the library, a Loams token is read in exactly one way: to be compared
/// against something.
///
/// `LoamsCredential::expose` is the only accessor for the token, and the only
/// two things in this crate that may receive its value are `assert_cookies_exclude`
/// and `audit_profile` — the two halves of this test. Every other use would be
/// routing the token somewhere, and this assertion is what makes that a test
/// failure rather than something for a reviewer to notice.
///
/// The check is a source scan, which is blunt. It is blunt on purpose: it is
/// cheap, it needs no code generation, and it fails at test time. Its one
/// weakness is that a multi-line call could hide the read, so the assertion
/// also requires that the read be on the same line as the callee.
#[test]
fn the_library_only_compares_a_credential_and_never_sends_it() {
    let src: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources: Vec<PathBuf> = Vec::new();
    collect_rust_files(&src, &mut sources);
    assert!(
        !sources.is_empty(),
        "no sources found under {}",
        src.display()
    );

    /// The only two callees allowed to see a token's value.
    const ALLOWED: [&str; 2] = ["assert_cookies_exclude", "audit_profile"];

    let mut readers: Vec<String> = Vec::new();
    let mut offenders: Vec<String> = Vec::new();
    for file in &sources {
        let text = std::fs::read_to_string(file).expect("a readable source");
        for (number, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if !trimmed.contains(".expose()") {
                continue;
            }
            if trimmed.starts_with("//") {
                continue;
            }
            // A `pub fn expose` definition line is not a read.
            if trimmed.starts_with("pub fn expose") {
                continue;
            }
            readers.push(format!("{}:{number}", file.display()));
            if !ALLOWED.iter().any(|callee| trimmed.contains(callee)) {
                offenders.push(format!("{}:{number}: {trimmed}", file.display()));
            }
        }
    }
    assert!(
        !readers.is_empty(),
        "the library must read the token at least once, or this test proves nothing"
    );
    assert!(
        offenders.is_empty(),
        "a Loams token is read somewhere other than the boundary checks: {offenders:#?}"
    );
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// A `CdpClient` is only ever constructed against a loopback DevTools URL, and
/// the launch spec always binds loopback.
///
/// The CDP endpoint has no authentication and no encryption. Binding it to
/// anything other than loopback would hand arbitrary code execution to anyone
/// who can reach the port, so this asserts the bind address rather than
/// trusting the engine's own default.
#[tokio::test]
async fn the_devtools_url_is_loopback() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let browser = profile(&base);
    let url = browser.plan().spec().devtools_url();
    assert!(
        url.starts_with("ws://127.0.0.1:"),
        "the DevTools URL must be loopback, got: {url}"
    );
    browser
        .plan()
        .spec()
        .assert_loopback()
        .expect("a loopback bind");
    // And a URL that is not a CDP endpoint fails to connect rather than
    // silently succeeding against something else.
    let error = CdpClient::connect("ws://127.0.0.1:1/devtools/browser")
        .await
        .expect_err("nothing is listening on port 1");
    assert!(matches!(
        error,
        loams_sidebar_browser::SidebarBrowserError::Transport(_)
    ));
}
