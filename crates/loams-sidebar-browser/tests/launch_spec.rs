//! The engine invocation, and the version pin.
//!
//! E1's third blocker was that there was no Windows browser at all. The
//! mechanism that closes it here is that **there is no per-platform webview to
//! be missing**: one command line, built by one pure function, is what Linux,
//! macOS and Windows all run. These tests assert that the command line contains
//! nothing platform-specific — no `.dll`, no `.dylib`, no `.so`, no framework
//! name, no Apple-only flag — which is the version of "one path for three
//! operating systems" that a test can actually hold.

use loams_sidebar_browser::engine::{BIND_HOST, DEFAULT_CDP_PORT, PINNED_ENGINE_VERSION};
use loams_sidebar_browser::{EngineLaunchSpec, ProfileKey, SidebarBrowser, check_version};
use tempfile::TempDir;

fn plan(origin: &str) -> EngineLaunchSpec {
    let base: TempDir = tempfile::tempdir().expect("a temporary directory");
    SidebarBrowser::prepare(
        base.path(),
        ProfileKey::new("env_dev", "zulip").expect("a valid pair"),
        loams_sidebar_browser::EmbeddedOrigin::parse(origin).expect("an origin"),
        "/usr/local/bin/obscura",
        DEFAULT_CDP_PORT,
    )
    .expect("a launch plan")
    .plan()
    .spec()
    .clone()
}

/// The invocation is the same shape on every operating system.
#[test]
fn the_invocation_is_platform_neutral() {
    let command = plan("https://chat.example.com/").command_line();
    for forbidden in [
        ".dll",
        ".dylib",
        ".so",
        "webkit",
        "WebKit",
        "WKWebView",
        "WebView2",
        "wry",
        "gtk",
        "webkit2gtk",
        "cocoa",
        "AppKit",
        "objc2",
        "microsoft.web.webview2",
        "Microsoft.Web.WebView2",
    ] {
        assert!(
            !command.contains(forbidden),
            "the invocation mentions {forbidden:?}, which would make it platform-specific: {command}"
        );
    }
}

/// It starts the CDP server, on loopback, with one worker and the profile as
/// its store.
#[test]
fn the_invocation_serves_cdp_with_the_profile_as_its_store() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let key = ProfileKey::new("env_prod", "forgejo").expect("a valid pair");
    let browser = SidebarBrowser::prepare(
        base.path(),
        key,
        loams_sidebar_browser::EmbeddedOrigin::parse("https://git.example.com").expect("an origin"),
        "obscura",
        9222,
    )
    .expect("a launch plan");
    let spec = browser.plan().spec();
    assert_eq!(spec.args().first().map(String::as_str), Some("serve"));
    assert_eq!(spec.program().to_str(), Some("obscura"));
    assert_eq!(spec.port(), 9222);
    assert_eq!(spec.devtools_url(), "ws://127.0.0.1:9222/devtools/browser");
    let args = spec.args();
    let value = |flag: &str| {
        args.windows(2)
            .find(|pair| pair[0] == flag)
            .map(|pair| pair[1].clone())
            .unwrap_or_else(|| panic!("{flag} should be present in {args:?}"))
    };
    assert_eq!(value("--host"), BIND_HOST);
    assert_eq!(value("--workers"), "1");
    assert_eq!(
        std::path::Path::new(&value("--storage-dir")),
        browser.plan().profile_dir()
    );
}

/// `--allow-file-access` is never set.
///
/// The engine's CDP connection can read `file://` URLs when that flag is on,
/// which on a desktop means it can read the app's own credential store off
/// disk. It is refused here by omission, and the omission is asserted so it
/// cannot be added by a later "just for local testing" change.
#[test]
fn the_invocation_never_enables_file_access() {
    let args = plan("http://localhost:8084/").args().to_vec();
    assert!(
        !args.iter().any(|arg| arg == "--allow-file-access"),
        "a CDP connection that can read file:// can read Loams's credentials: {args:?}"
    );
    assert!(
        !args.iter().any(|arg| arg == "--stealth"),
        "stealth is for scraping a hostile web, not for embedding an app the user already \
         authenticated to: {args:?}"
    );
}

/// The version check accepts the pin, tolerates the two spellings a release
/// build prints, and refuses everything else.
#[test]
fn the_version_pin_is_checked() {
    assert!(check_version(PINNED_ENGINE_VERSION).is_ok());
    assert!(check_version(&format!("obscura {PINNED_ENGINE_VERSION}")).is_ok());
    assert!(check_version(&format!("v{PINNED_ENGINE_VERSION}\n")).is_ok());
    assert!(check_version(&format!("  {PINNED_ENGINE_VERSION}  ")).is_ok());

    for reported in ["0.2.2", "0.2.4", "0.1.0", "1.0.0", "", "obscura"] {
        let error =
            check_version(reported).expect_err(&format!("{reported:?} must not pass the pin"));
        assert!(
            error.to_string().contains(PINNED_ENGINE_VERSION),
            "the error should name the expected version, got: {error}"
        );
    }
}

/// A version that merely *contains* the pin is refused.
///
/// This is the mistake a substring check makes, and it is the one that would
/// let a different release through silently.
#[test]
fn a_version_containing_the_pin_is_refused() {
    let error = check_version("0.2.30").expect_err("0.2.30 is not the pin");
    assert!(error.to_string().contains("found 0.2.30"), "got: {error}");
    let error = check_version("0.2.3-rc1").expect_err("a pre-release is not the pin");
    assert!(
        error.to_string().contains("found 0.2.3-rc1"),
        "got: {error}"
    );
}

/// The pin is the version this crate's comments cite.
#[test]
fn the_pinned_version_is_the_one_the_docs_cite() {
    assert_eq!(PINNED_ENGINE_VERSION, "0.2.3");
}

/// The engine binary is found through the override first, then on `PATH`.
#[test]
fn the_engine_binary_is_overridable() {
    // The override exists so an operator can point at a checksummed copy of the
    // release asset rather than whatever is on `PATH`. Asserted without
    // mutating the process environment, because a test that sets an env var is
    // a test that races every other test in the binary.
    let expected = match std::env::var_os(loams_sidebar_browser::OBSCURA_BIN_ENV) {
        Some(value) if !value.is_empty() => std::path::PathBuf::from(value),
        _ => std::path::PathBuf::from("obscura"),
    };
    assert_eq!(
        loams_sidebar_browser::resolve_program(),
        expected,
        "the override wins when set, and the engine is looked up on PATH otherwise"
    );
    assert_eq!(
        loams_sidebar_browser::OBSCURA_BIN_ENV,
        "LOAMS_OBSCURA_BIN",
        "the override's name is part of the operator-facing contract"
    );
}
