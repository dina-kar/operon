//! The shared metastore types moved from `operon-meta` (M1.2a plan, Task 2).

use operon_common::meta::{
    ApplyError, Freshness, Lease, MetaError, StaleLag, collection_pk_prefix,
    collection_pointer_key, collection_prefix, implicit_name, link_pointer_key,
};
use operon_common::{CollectionId, NamespaceId};

#[test]
fn stale_lag_reports_the_proposers_lag() {
    let stale = ApplyError::StaleObject {
        object: "ns/1/streams/1/0/seg".to_string(),
        created_at_ms: 1_000,
        max_age_ms: 500,
        clock_ms: 2_000,
    };
    assert_eq!(
        stale.stale_lag(1_900),
        Some(StaleLag {
            deadline_ms: 1_500,
            clock_ms: 2_000,
            proposer_now_ms: 1_900,
            late_by_ms: 500,
            proposer_lag_ms: 100,
        })
    );
    let fenced = ApplyError::Fenced {
        lease: "task/gc".to_string(),
    };
    assert_eq!(fenced.stale_lag(1_900), None);
}

#[test]
fn implicit_names_and_keys_have_their_documented_form() {
    let ns = NamespaceId(3);
    let collection = CollectionId(7);
    assert_eq!(implicit_name("docs", collection), "_collection.docs.7");
    assert_eq!(collection_pointer_key(collection), "collection/7");
    assert_eq!(collection_prefix(ns, collection), "ns/3/collections/7/");
    assert_eq!(
        collection_pk_prefix(ns, collection),
        "ns/3/pk/collection-7/"
    );
    assert_eq!(link_pointer_key(operon_common::meta::LinkId(4)), "link/4");
}

#[test]
fn freshness_expires_strictly_after_its_deadline() {
    let fresh = Freshness {
        created_at_ms: 1_000,
        max_age_ms: 500,
    };
    // The deadline itself (1_500) is still fresh; only strictly after it is expired.
    assert!(!fresh.expired_at(1_500));
    assert!(fresh.expired_at(1_501));
}

#[test]
fn a_lease_is_held_until_its_deadline_and_not_after_release() {
    let held = Lease {
        epoch: 1,
        owner: Some("worker-a".to_string()),
        deadline_ms: 1_000,
    };
    assert!(held.is_held_at(999));
    assert!(!held.is_held_at(1_000));
    assert!(!held.is_held_at(1_001));

    let released = Lease {
        owner: None,
        ..held
    };
    assert!(!released.is_held_at(500));
}

#[test]
fn unexpected_reply_display_is_unchanged() {
    assert_eq!(
        MetaError::UnexpectedReply("SegmentSwapped".into()).to_string(),
        "unexpected reply: SegmentSwapped"
    );
}
