//! The case list itself.

use std::collections::BTreeSet;

use operon_meta_conformance::CASES;

#[test]
fn every_case_is_listed_once() {
    let unique: BTreeSet<&str> = CASES.iter().copied().collect();
    assert_eq!(
        unique.len(),
        CASES.len(),
        "a case is listed twice: {CASES:?}"
    );
    assert!(!CASES.is_empty());
}
