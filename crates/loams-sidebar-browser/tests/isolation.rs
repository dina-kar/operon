//! Per-`(environment, app)` profile directory isolation.
//!
//! This is one of the two properties E1 said could not be delivered — a store
//! that survives the window closing, kept per app and per environment. The
//! other property (surviving a *process* restart) is `persistence.rs`. Both
//! matter: a store that is shared is as broken as no store at all, because
//! "sign in once" then means "signed in to the wrong app".

use std::collections::BTreeSet;
use std::path::PathBuf;

use loams_sidebar_browser::{PROFILE_MANIFEST, ProfileKey};
use tempfile::TempDir;

fn base() -> TempDir {
    tempfile::tempdir().expect("a temporary directory")
}

/// Every distinct pair in the matrix gets a distinct directory.
#[test]
fn distinct_pairs_get_distinct_directories() {
    let base = base();
    let pairs = [
        ("env_dev", "zulip"),
        ("env_prod", "zulip"),
        ("env_dev", "plane"),
        ("env_dev", "forgejo"),
        ("env_staging", "zulip"),
        ("env_dev", "zulip_mobile"),
    ];
    let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
    for (environment, app) in pairs {
        let key = ProfileKey::new(environment, app).expect("a valid pair");
        let dir = key
            .ensure_profile_dir(base.path())
            .expect("a profile directory");
        assert!(dir.is_dir(), "{} should have been created", dir.display());
        dirs.insert(dir);
    }
    assert_eq!(
        dirs.len(),
        pairs.len(),
        "every (environment, app) pair must map to its own directory"
    );
}

/// The same pair always resolves to the same directory.
///
/// This is the property that makes persistence possible at all: nothing about
/// the mapping depends on process state, a clock or a counter.
#[test]
fn a_pair_is_stable_across_constructions() {
    let base = base();
    let first = ProfileKey::new("env_dev", "zulip")
        .expect("a valid pair")
        .ensure_profile_dir(base.path())
        .expect("a profile directory");
    let second = ProfileKey::new("env_dev", "zulip")
        .expect("a valid pair")
        .profile_dir(base.path());
    assert_eq!(first, second);
    assert_eq!(
        first,
        base.path().join("env_dev").join("zulip"),
        "the layout should be two real path segments"
    );
}

/// The isolation property that a joined, escaped directory name would break.
///
/// `("a_b", "c")` and `("a", "b_c")` collapse to the same string under any
/// separator that can appear inside an identifier, so the two-level layout
/// exists instead. This test is the reason it does.
#[test]
fn pairs_that_would_collide_if_joined_do_not_collide() {
    let base = base();
    let left = ProfileKey::new("a_b", "c")
        .expect("a valid pair")
        .profile_dir(base.path());
    let right = ProfileKey::new("a", "b_c")
        .expect("a valid pair")
        .profile_dir(base.path());
    assert_ne!(left, right);
    left.parent().expect("a parent");
    assert_ne!(
        left.parent(),
        right.parent(),
        "even the environments differ"
    );
}

/// Identifiers that would escape the base directory, or otherwise be unusable
/// as a path segment, are refused rather than sanitised.
#[test]
fn hostile_identifiers_are_refused() {
    let refused = [
        ("../etc", "zulip"),
        ("env_dev", "../../etc"),
        ("env_dev", "a/b"),
        ("env_dev", ""),
        ("", "zulip"),
        (".", "zulip"),
        ("..", "zulip"),
        ("env_dev", "."),
        ("env_dev", ".."),
        (".hidden", "zulip"),
        ("env_dev", "trailing."),
        ("env_dev", "trailing "),
        ("env_dev", "with space"),
        ("env_dev", "with/slash"),
        ("env_dev", "with\\backslash"),
        ("env_dev", "nul\0byte"),
        ("env_dev", "café"),
        ("CON", "zulip"),
        ("env_dev", "com1"),
        ("env_dev", "NUL.txt"),
        ("env_dev", "aux"),
    ];
    for (environment, app) in refused {
        assert!(
            ProfileKey::new(environment, app).is_err(),
            "environment {environment:?} app {app:?} should be refused"
        );
    }
}

/// The longest accepted identifier is accepted, and one byte more is not.
#[test]
fn the_length_limit_is_inclusive() {
    let ok = "a".repeat(64);
    assert!(ProfileKey::new(ok.as_str(), "zulip").is_ok());
    let too_long = "a".repeat(65);
    assert!(ProfileKey::new(too_long.as_str(), "zulip").is_err());
}

/// The manifest names the pair, so a directory can be checked rather than
/// assumed.
#[test]
fn the_manifest_records_the_pair() {
    let base = base();
    let key = ProfileKey::new("env_prod", "plane").expect("a valid pair");
    let dir = key
        .ensure_profile_dir(base.path())
        .expect("a profile directory");
    assert!(dir.join(PROFILE_MANIFEST).is_file());
    let read_back = ProfileKey::read_manifest(&dir).expect("a readable manifest");
    assert_eq!(read_back, Some(key));
}

/// A manifest written for a different pair is refused, which is the cheap half
/// of isolation: a wrong path cannot be read as if it were this profile.
#[test]
fn a_manifest_for_another_pair_is_refused() {
    let base = base();
    let zulip = ProfileKey::new("env_dev", "zulip").expect("a valid pair");
    let plane = ProfileKey::new("env_dev", "plane").expect("a valid pair");
    let dir = plane
        .ensure_profile_dir(base.path())
        .expect("a profile directory");
    assert_ne!(
        ProfileKey::read_manifest(&dir).expect("a readable manifest"),
        Some(zulip)
    );
}

/// A missing manifest is `None`, not a panic and not a fabricated key.
#[test]
fn a_missing_manifest_is_none() {
    let base = base();
    let dir = base.path().join("env_dev").join("zulip");
    std::fs::create_dir_all(&dir).expect("a directory");
    assert_eq!(
        ProfileKey::read_manifest(&dir).expect("a readable manifest"),
        None
    );
}

/// A manifest from a future Loams is refused rather than misread.
#[test]
fn a_future_manifest_version_is_refused() {
    let base = base();
    let dir = base.path().join("env_dev").join("zulip");
    std::fs::create_dir_all(&dir).expect("a directory");
    std::fs::write(
        dir.join(PROFILE_MANIFEST),
        br#"{"version":99,"environment":"env_dev","app":"zulip"}"#,
    )
    .expect("a written manifest");
    let error = ProfileKey::read_manifest(&dir).expect_err("a future version must be refused");
    assert!(
        error.to_string().contains("version 99"),
        "the error should name the version it found, got: {error}"
    );
}
