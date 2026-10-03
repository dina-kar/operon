//! Sessions surviving a process restart.
//!
//! SF1 Task 0's E1 held that "sign in once" could not be delivered because the
//! per-platform store was ephemeral on every OS. The replacement store is
//! Obscura's `--storage-dir`, but the property is not the engine's: it is Loams
//! deriving the same directory from the same `(environment, app)` pair on the
//! next launch and re-injecting the app's session cookies from its own ledger.
//!
//! Every test here simulates the restart by dropping one complete set of
//! objects — the `SidebarBrowser`, the ledger handle, the temporary base's
//! in-memory view of what was there — and building entirely new ones from the
//! same inputs. Nothing survives because an object survived; it survives
//! because it is on disk.

use loams_sidebar_browser::{
    COOKIE_LEDGER, CookieAttributes, CookieLedger, EmbeddedOrigin, ProfileKey, SameSite,
    ScopedSessionCookie, SidebarBrowser,
};
use tempfile::TempDir;

/// The app session cookie the OIDC ceremony produced, for `chat.example.com`.
///
/// Shaped like Zulip's: `__Host-sessionid`, `Secure`, `HttpOnly`, `SameSite=Lax`,
/// `Path=/` (SF1 Task 0, Decision 2's findings table).
fn zulip_origin() -> EmbeddedOrigin {
    EmbeddedOrigin::parse("https://chat.example.com").expect("an origin")
}

fn zulip_session(value: &str) -> ScopedSessionCookie {
    ScopedSessionCookie::new(
        zulip_origin(),
        "__Host-sessionid",
        value,
        CookieAttributes::session(&zulip_origin()).with_expires(1_900_000_000.0),
    )
    .expect("a host-only session cookie")
}

fn browser(base: &TempDir, environment: &str, app: &str, origin: &str) -> SidebarBrowser {
    SidebarBrowser::prepare(
        base.path(),
        ProfileKey::new(environment, app).expect("a valid pair"),
        EmbeddedOrigin::parse(origin).expect("an origin"),
        "obscura",
        9222,
    )
    .expect("a launch plan")
}

/// The headline property: a session injected before a restart is there after
/// it.
///
/// There is no engine in this test, deliberately. The property under test is
/// Loams's — the same pair finds the same directory and the same cookies — and
/// a test that needed a 70 MB browser process to assert a file lookup would be
/// a test that only runs on a machine with the engine installed.
#[test]
fn sessions_survive_a_process_restart() {
    let base = base();
    let first = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let first_dir = first.plan().profile_dir().to_path_buf();
    let cookie = zulip_session("app-session-value-1");
    {
        let loaded = CookieLedger::load_for(first.plan().profile_dir(), first.key())
            .expect("a ledger for a fresh profile");
        assert!(
            loaded.is_empty(),
            "a profile created by `prepare` must start with no session"
        );
        let ledger = CookieLedger::for_origin(first.origin(), vec![cookie.clone()])
            .expect("a single-origin ledger");
        ledger
            .store(first.plan().profile_dir(), first.origin())
            .expect("a stored ledger");
        assert!(ledger.cookies().len() == 1);
    }

    // ---- the "restart" ----
    // Everything that could hold state in memory is dropped: the browser, its
    // plan, the ledger handle. Only the directory survives.
    drop(cookie);
    drop(first);

    let second = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    assert_eq!(
        first_dir,
        second.plan().profile_dir(),
        "the restart must resolve to the same directory"
    );
    let restored = CookieLedger::load_for(second.plan().profile_dir(), second.key())
        .expect("a ledger restored from disk");
    let cookies = restored.for_origin_cookies(second.origin());
    assert_eq!(cookies.len(), 1, "the session must have survived");
    assert_eq!(cookies[0].name(), "__Host-sessionid");
    assert_eq!(
        cookies[0].value(),
        "app-session-value-1",
        "the app's session value must round-trip byte for byte"
    );
    assert_eq!(cookies[0].origin(), second.origin());
    assert!(cookies[0].secure());
    assert!(cookies[0].http_only());
    assert_eq!(cookies[0].same_site(), SameSite::Lax);
    assert_eq!(cookies[0].path(), "/");
}

/// A rotation replaces the old value rather than accumulating entries.
#[test]
fn a_rotated_session_replaces_the_previous_value() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    let key = browser.key();

    for value in ["session-1", "session-2", "session-3"] {
        let mut ledger = CookieLedger::load_for(dir, key).expect("a ledger");
        ledger.upsert(zulip_session(value));
        ledger
            .store(dir, browser.origin())
            .expect("a stored ledger");
    }

    let restored = CookieLedger::load_for(dir, key).expect("a ledger");
    let cookies = restored.for_origin_cookies(browser.origin());
    assert_eq!(
        cookies.len(),
        1,
        "three rotations of one cookie must leave one entry, not three"
    );
    assert_eq!(cookies[0].value(), "session-3");
}

/// A profile with no ledger yet loads as empty rather than failing.
///
/// A first launch has nothing to restore, and refusing to open the sidebar on
/// a first launch would be a worse bug than an empty ledger.
#[test]
fn a_fresh_profile_loads_an_empty_ledger() {
    let base = base();
    let browser = browser(&base, "env_dev", "plane", "https://plane.example.com");
    let ledger = CookieLedger::load_for(browser.plan().profile_dir(), browser.key())
        .expect("an empty ledger, not an error");
    assert!(ledger.is_empty());
    assert!(
        !browser.plan().profile_dir().join(COOKIE_LEDGER).exists(),
        "reading an absent ledger must not create one"
    );
}

/// A ledger from another pair is refused, so a wrong path cannot be read as
/// if it were this profile's.
#[test]
fn a_ledger_from_another_profile_is_refused() {
    let base = base();
    let prod = browser(&base, "env_prod", "zulip", "https://chat.example.com");
    CookieLedger::for_origin(prod.origin(), vec![zulip_session("prod-session")])
        .expect("a ledger")
        .store(prod.plan().profile_dir(), prod.origin())
        .expect("a stored ledger");
    drop(prod);

    let dev = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    // Point the dev browser's profile directory at the prod one by asking for
    // the prod directory explicitly: this is the mistake the check exists for.
    let error = CookieLedger::load_for(base.path().join("env_prod").join("zulip"), dev.key())
        .expect_err("a mismatched profile must be refused");
    assert!(
        error.to_string().contains("env_prod/zulip"),
        "the error should name the directory's real owner, got: {error}"
    );
}

/// The ledger refuses a mixed-origin set, which is what keeps one profile equal
/// to one embedded origin.
#[test]
fn a_ledger_refuses_two_origins() {
    let zulip = EmbeddedOrigin::parse("https://chat.example.com").expect("an origin");
    let plane = EmbeddedOrigin::parse("https://plane.example.com").expect("an origin");
    let other = ScopedSessionCookie::new(
        plane.clone(),
        "session-id",
        "plane-session",
        CookieAttributes::session(&plane),
    )
    .expect("a cookie");
    let error = CookieLedger::for_origin(&zulip, vec![zulip_session("z"), other])
        .expect_err("a mixed-origin ledger must be refused");
    assert!(
        error.to_string().contains("plane.example.com"),
        "the error should name the foreign origin, got: {error}"
    );
}

/// The ledger's file is owner-only on Unix, because it holds live app session
/// cookies even though it holds no Loams token.
#[cfg(unix)]
#[test]
fn the_ledger_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;

    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let path = CookieLedger::for_origin(browser.origin(), vec![zulip_session("s")])
        .expect("a ledger")
        .store(browser.plan().profile_dir(), browser.origin())
        .expect("a stored ledger");
    let mode = std::fs::metadata(&path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o600,
        "the ledger must not be group- or world-readable"
    );
}

/// Rewriting an unchanged ledger produces identical bytes.
///
/// Without this, every launch would rewrite the file and every profile would
/// look modified, which is how a real persistence bug hides behind noise.
#[test]
fn an_unchanged_ledger_rewrites_identically() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    let ledger =
        CookieLedger::for_origin(browser.origin(), vec![zulip_session("s")]).expect("a ledger");
    let path = ledger
        .store(dir, browser.origin())
        .expect("a stored ledger");
    let first = std::fs::read(&path).expect("read one");
    ledger.store(dir, browser.origin()).expect("stored again");
    let second = std::fs::read(&path).expect("read two");
    assert_eq!(first, second);
}

/// Insertion order does not change the file, so the ordering is a property of
/// the type and not of the caller.
#[test]
fn cookie_order_does_not_change_the_file() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    let origin = browser.origin().clone();

    let second = ScopedSessionCookie::new(
        origin.clone(),
        "csrftoken",
        "csrf-value",
        CookieAttributes::session(&origin).with_http_only(false),
    )
    .expect("a cookie");

    let forward = CookieLedger::for_origin(&origin, vec![zulip_session("s"), second.clone()])
        .expect("a ledger")
        .store(dir, &origin)
        .expect("a stored ledger");
    let mut ledger = CookieLedger::new();
    ledger.upsert(second);
    ledger.upsert(zulip_session("s"));
    let backward = ledger.store(dir, &origin).expect("a stored ledger");

    assert_eq!(
        std::fs::read(forward).expect("read one"),
        std::fs::read(backward).expect("read two")
    );
}

/// A ledger written by a future Loams is refused rather than silently
/// misread.
#[test]
fn a_future_ledger_version_is_refused() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    std::fs::write(
        dir.join(COOKIE_LEDGER),
        br#"{"version":99,"origin":"https://chat.example.com","cookies":[]}"#,
    )
    .expect("a written ledger");
    let error = CookieLedger::load(dir).expect_err("a future version must be refused");
    assert!(
        error.to_string().contains("ledger version 99"),
        "the error should name the version it found, got: {error}"
    );
}

/// A corrupt ledger is an error, never an empty one.
///
/// Silently treating unparseable state as "logged out" is precisely the failure
/// E1 was worried about, in a harder-to-spot form.
#[test]
fn a_corrupt_ledger_is_an_error_not_an_empty_one() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    std::fs::write(dir.join(COOKIE_LEDGER), b"{not json").expect("a written ledger");
    CookieLedger::load(dir).expect_err("a corrupt ledger must be an error");
}

/// A ledger whose cookies would not pass the constructor is refused on load,
/// so a hand-edited file cannot widen the boundary.
#[test]
fn a_ledger_cannot_widen_the_cookie_boundary() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    // A well-formed ledger for chat.example.com, with one cookie for another
    // origin appended by hand.
    std::fs::write(
        dir.join(COOKIE_LEDGER),
        br#"{"version":1,"origin":"https://chat.example.com","cookies":[
            {"origin":"https://chat.example.com","name":"__Host-sessionid","value":"v","path":"/",
             "secure":true,"http_only":true,"same_site":"Lax"},
            {"origin":"https://evil.example.com","name":"s","value":"v","path":"/",
             "secure":true,"http_only":true,"same_site":"Lax"}
        ]}"#,
    )
    .expect("a written ledger");
    let error = CookieLedger::load(dir)
        .expect_err("a cookie for a second origin must be refused, not dropped");
    assert!(
        error.to_string().contains("evil.example.com"),
        "the error should name the foreign origin, got: {error}"
    );
}

/// A ledger for an `http` origin cannot carry a non-`Secure` cookie, which is
/// what keeps a downgraded connection from being handed a session.
#[test]
fn a_ledger_cannot_record_a_non_secure_cookie_for_https() {
    let base = base();
    let browser = browser(&base, "env_dev", "zulip", "https://chat.example.com");
    let dir = browser.plan().profile_dir();
    std::fs::write(
        dir.join(COOKIE_LEDGER),
        br#"{"version":1,"origin":"https://chat.example.com","cookies":[
            {"origin":"https://chat.example.com","name":"sessionid","value":"v","path":"/",
             "secure":false,"http_only":true,"same_site":"Lax"}
        ]}"#,
    )
    .expect("a written ledger");
    let error = CookieLedger::load(dir).expect_err("a non-Secure https cookie must be refused");
    assert!(
        error.to_string().contains("Secure"),
        "the error should say why, got: {error}"
    );
}

fn base() -> TempDir {
    tempfile::tempdir().expect("a temporary directory")
}
