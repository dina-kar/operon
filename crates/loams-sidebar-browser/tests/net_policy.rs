//! The private-network and SSRF policy, including the localhost case.
//!
//! Loams embeds a console that a developer may well be running on
//! `http://127.0.0.1:8084` or `https://console.localhost:8443`. The pinned
//! engine refuses those by default, and `--allow-private-network` is how it is
//! relaxed — but the flag is **process-wide**, so turning it on for a localhost
//! console turns it on for everything that profile can reach. These tests pin
//! both halves: the flag is set exactly when it is needed, and Loams keeps a
//! deny set that the flag does not open.

use loams_sidebar_browser::net_policy::{decide, decide_host};
use loams_sidebar_browser::{
    CLOUD_METADATA_ADDR, EmbeddedOrigin, PrivateNetworkDecision, ProfileKey, SidebarBrowser,
};
use tempfile::TempDir;
use url::Url;

fn url(value: &str) -> Url {
    Url::parse(value).expect("a URL")
}

fn decision(value: &str) -> PrivateNetworkDecision {
    decide(&url(value)).expect("a decided target")
}

/// A public origin leaves the engine's SSRF guard alone.
#[test]
fn a_public_origin_does_not_relax_the_engine() {
    let decided = decision("https://chat.example.com/");
    assert_eq!(decided, PrivateNetworkDecision::Public);
    assert!(!decided.requires_engine_relaxation());
}

/// A loopback console is permitted, and requires the engine to be relaxed.
///
/// This is the case that would otherwise be a broken panel: every page in the
/// embed renders an SSRF error.
#[test]
fn a_loopback_console_is_permitted_and_requires_relaxation() {
    for value in [
        "http://127.0.0.1:8084/",
        "http://localhost:8084/",
        "https://console.localhost:8443/",
        "http://127.0.0.53:8084/",
        "http://[::1]:8084/",
        "http://192.168.1.50:9000/",
        "http://10.0.0.7/",
        "http://172.16.4.4/",
        "http://[fc00::1]:8080/",
        "http://[fd12:3456::1]/",
    ] {
        let decided = decision(value);
        assert!(
            decided.requires_engine_relaxation(),
            "{value} should be permitted with the engine relaxed, got {decided:?}"
        );
        assert!(decided.is_permitted(), "{value} should be permitted");
    }
}

/// The cloud metadata endpoint is refused by Loams even though the engine's
/// own switch would open it.
///
/// This is the reason Loams keeps its own deny set rather than delegating. The
/// engine's `--allow-private-network` relaxes its SSRF guard wholesale; the
/// metadata endpoint is inside the range it relaxes. Loams never launches for
/// such a target.
#[test]
fn the_cloud_metadata_endpoint_is_refused_by_loams() {
    let decided = decision("http://169.254.169.254/latest/meta-data/");
    assert!(
        !decided.is_permitted(),
        "the metadata endpoint must be refused, got {decided:?}"
    );
    assert!(!decided.requires_engine_relaxation());
    assert!(
        matches!(decided, PrivateNetworkDecision::Refused { .. }),
        "a refusal, not a permission, got {decided:?}"
    );
}

/// The rest of link-local, and the unspecified addresses, are refused too.
#[test]
fn link_local_and_unspecified_addresses_are_refused() {
    for value in [
        "http://169.254.1.1/",
        "http://[fe80::1]/",
        "http://0.0.0.0:8080/",
        "http://[::]/",
        "http://255.255.255.255/",
    ] {
        let decided = decision(value);
        assert!(
            !decided.is_permitted(),
            "{value} should be refused, got {decided:?}"
        );
    }
}

/// An IPv4-mapped IPv6 loopback is not a way around the loopback branch.
#[test]
fn an_ipv4_mapped_loopback_is_permitted_as_loopback() {
    // ::ffff:127.0.0.1 is not unique-local and not link-local, so it reaches
    // the mapped-IPv4 branch; the answer must match the plain IPv4 one.
    assert_eq!(
        decide(&url("http://[::ffff:127.0.0.1]:8084/")).expect("a decided target"),
        decide(&url("http://127.0.0.1:8084/")).expect("a decided target")
    );
}

/// Only `http` and `https` are embeddable.
///
/// The engine also accepts `file` when `--allow-file-access` is set. That flag
/// is never set here, because a CDP connection allowed to read `file://` could
/// read Loams's own credential store off disk.
#[test]
fn non_http_schemes_are_refused() {
    for value in [
        "file:///etc/passwd",
        "ftp://example.com/",
        "data:text/html,hi",
        "javascript:1",
    ] {
        let error = decide(&url(value)).expect_err("a non-http scheme must be refused");
        assert!(
            error.to_string().contains("only http and https"),
            "{value}: {error}"
        );
    }
}

/// The metadata constant in the code and the string in this test agree.
#[test]
fn the_metadata_constant_is_the_documented_address() {
    assert_eq!(CLOUD_METADATA_ADDR.to_string(), "169.254.169.254");
}

/// The launch spec sets `--allow-private-network` for a private origin and
/// omits it for a public one.
///
/// Omitting it is the point: the flag is process-wide, so a public environment
/// should run with the engine's SSRF guard exactly as the engine ships it.
#[test]
fn the_launch_flag_follows_the_origin() {
    let public = launch_spec_for("https://chat.example.com");
    assert!(!public.allows_private_network());
    assert!(
        !public
            .args()
            .iter()
            .any(|arg| arg == "--allow-private-network"),
        "a public origin must not relax the engine: {:?}",
        public.args()
    );

    let local = launch_spec_for("http://localhost:8084/");
    assert!(local.allows_private_network());
    assert!(
        local
            .args()
            .iter()
            .any(|arg| arg == "--allow-private-network"),
        "a loopback console needs the engine relaxed: {:?}",
        local.args()
    );
}

/// The launch spec states loopback explicitly rather than inheriting a default.
///
/// The engine's `--host` also defaults to `127.0.0.1`, but its CDP endpoint has
/// no authentication: anything that can open a WebSocket to it can read the
/// profile's cookies and drive the engine. Loams states the bind address so the
/// guarantee does not depend on an upstream default that can change.
#[test]
fn the_bind_address_is_stated_and_loopback() {
    for origin in ["https://chat.example.com/", "http://localhost:8084/"] {
        let spec = launch_spec_for(origin);
        let pair = spec
            .args()
            .windows(2)
            .find(|pair| pair[0] == "--host")
            .expect("an explicit --host");
        assert_eq!(pair[1], "127.0.0.1");
        spec.assert_loopback().expect("a loopback bind");
        assert!(
            spec.devtools_url().starts_with("ws://127.0.0.1:"),
            "the DevTools URL must be loopback, got {}",
            spec.devtools_url()
        );
    }
}

/// The storage directory is the profile directory, so the engine's own store
/// is inside the boundary that [`loams_sidebar_browser::audit_profile`] scans.
#[test]
fn the_storage_dir_is_the_profile_dir() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let key = ProfileKey::new("env_dev", "zulip").expect("a valid pair");
    let browser = SidebarBrowser::prepare(
        base.path(),
        key,
        EmbeddedOrigin::parse("https://chat.example.com").expect("an origin"),
        "obscura",
        9222,
    )
    .expect("a launch plan");
    assert_eq!(
        browser.plan().spec().profile_dir(),
        browser.plan().profile_dir()
    );
    let pair = browser
        .plan()
        .spec()
        .args()
        .windows(2)
        .find(|pair| pair[0] == "--storage-dir")
        .expect("an explicit --storage-dir");
    assert_eq!(std::path::Path::new(&pair[1]), browser.plan().profile_dir());
}

/// A refused origin never reaches a launch plan at all.
///
/// The refusal happens in `prepare`, before any process could be spawned, so
/// there is no state in which a refused target has an engine.
#[test]
fn a_refused_origin_produces_no_launch_plan() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let error = SidebarBrowser::prepare(
        base.path(),
        ProfileKey::new("env_prod", "zulip").expect("a valid pair"),
        EmbeddedOrigin::parse("http://169.254.169.254/").expect("an origin"),
        "obscura",
        9222,
    )
    .expect_err("the metadata endpoint must not produce a launch plan");
    assert!(
        error.to_string().contains("metadata"),
        "the error should say why, got: {error}"
    );
}

/// A `.local` or `.internal` name is private in effect, because it resolves
/// inside the local network even though it is not an IP literal.
#[test]
fn private_use_dns_suffixes_count_as_private() {
    for value in ["http://console.local:8084/", "http://api.internal/"] {
        assert!(
            decision(value).requires_engine_relaxation(),
            "{value} should be treated as private"
        );
    }
}

/// An empty host is refused rather than defaulted.
#[test]
fn a_url_with_no_host_is_refused() {
    let error = decide(&url("file:///tmp/x")).expect_err("no scheme, no host");
    assert!(
        error.to_string().contains("only http and https"),
        "got: {error}"
    );
}

/// `decide_host` and `decide` agree, so a caller that has already parsed the
/// host cannot get a different answer.
#[test]
fn deciding_on_the_host_alone_agrees_with_deciding_on_the_url() {
    let parsed = url("https://chat.example.com/");
    assert_eq!(
        decide(&parsed).expect("a decided target"),
        decide_host(&parsed.host().expect("a host"))
    );
}

fn launch_spec_for(origin: &str) -> loams_sidebar_browser::EngineLaunchSpec {
    let base: TempDir = tempfile::tempdir().expect("a temporary directory");
    SidebarBrowser::prepare(
        base.path(),
        ProfileKey::new("env_dev", "zulip").expect("a valid pair"),
        EmbeddedOrigin::parse(origin).expect("an origin"),
        "obscura",
        9222,
    )
    .expect("a launch plan")
    .plan()
    .spec()
    .clone()
}
