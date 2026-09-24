use operon_common::NamespaceId;
use operon_meta::{ApplyError, Command, Fence, MetaState, Pointer, Reply};

const NS: NamespaceId = NamespaceId(1);
const KEY: &str = "collections/7/manifest";

fn state_with_namespace() -> MetaState {
    let mut state = MetaState::default();
    state
        .apply(Command::CreateNamespace {
            name: "acme".to_string(),
        })
        .expect("create namespace");
    state
}

fn cas(
    state: &mut MetaState,
    expected: Option<u64>,
    value: &str,
    fence: Option<Fence>,
) -> Result<Reply, ApplyError> {
    state.apply(Command::CasPointer {
        namespace: NS,
        key: KEY.to_string(),
        expected,
        value: value.to_string(),
        fence,
    })
}

fn pointer(version: u64, value: &str) -> Pointer {
    Pointer {
        version,
        value: value.to_string(),
    }
}

#[test]
fn pointers_are_created_then_advanced_one_version_at_a_time() {
    let mut state = state_with_namespace();
    assert_eq!(
        cas(&mut state, None, "m/1.pb", None),
        Ok(Reply::PointerSet { version: 1 })
    );
    assert_eq!(
        cas(&mut state, Some(1), "m/2.pb", None),
        Ok(Reply::PointerSet { version: 2 })
    );
    assert_eq!(state.pointer(NS, KEY), Some(&pointer(2, "m/2.pb")));
    assert_eq!(state.pointer(NS, "other"), None);
}

#[test]
fn a_stale_expected_version_is_rejected_with_the_current_pointer() {
    let mut state = state_with_namespace();
    cas(&mut state, None, "m/1.pb", None).unwrap();
    cas(&mut state, Some(1), "m/2.pb", None).unwrap();
    let before = state.clone();

    let current = Some(pointer(2, "m/2.pb"));
    assert_eq!(
        cas(&mut state, Some(1), "m/3.pb", None),
        Err(ApplyError::VersionMismatch {
            current: current.clone()
        })
    );
    assert_eq!(
        cas(&mut state, None, "m/3.pb", None),
        Err(ApplyError::VersionMismatch { current })
    );
    assert_eq!(state, before);

    let mut empty = state_with_namespace();
    assert_eq!(
        cas(&mut empty, Some(1), "m/1.pb", None),
        Err(ApplyError::VersionMismatch { current: None })
    );
}

#[test]
fn pointers_need_an_existing_namespace_and_valid_keys() {
    let mut state = MetaState::default();
    assert_eq!(
        cas(&mut state, None, "m/1.pb", None),
        Err(ApplyError::NamespaceNotFound(NS))
    );

    let mut state = state_with_namespace();
    let err = cas(&mut state, None, "", None).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    let err = state
        .apply(Command::CasPointer {
            namespace: NS,
            key: String::new(),
            expected: None,
            value: "m/1.pb".to_string(),
            fence: None,
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
}

#[test]
fn a_fenced_write_needs_the_lease_at_the_fence_epoch() {
    let mut state = state_with_namespace();
    let lease = "link/tickets/0".to_string();
    let acquire = |owner: &str, now_ms| Command::AcquireLease {
        key: lease.clone(),
        owner: owner.to_string(),
        ttl_ms: 1_000,
        now_ms,
    };
    let fence = |epoch| {
        Some(Fence {
            lease: lease.clone(),
            epoch,
        })
    };
    let fenced = || {
        Err(ApplyError::Fenced {
            lease: lease.clone(),
        })
    };

    assert_eq!(
        cas(&mut state, None, "m/1.pb", fence(1)),
        fenced(),
        "never acquired"
    );

    state.apply(acquire("w1", 5_000)).unwrap();
    assert_eq!(
        cas(&mut state, None, "m/1.pb", fence(1)),
        Ok(Reply::PointerSet { version: 1 })
    );

    // w2 takes over after w1's lease expires; w1 is now a zombie.
    state.apply(acquire("w2", 6_000)).unwrap();
    assert_eq!(cas(&mut state, Some(1), "m/zombie.pb", fence(1)), fenced());
    assert_eq!(
        cas(&mut state, Some(1), "m/2.pb", fence(2)),
        Ok(Reply::PointerSet { version: 2 })
    );

    // After release, the released epoch no longer fences in.
    state
        .apply(Command::ReleaseLease {
            key: lease.clone(),
            owner: "w2".to_string(),
            epoch: 2,
        })
        .unwrap();
    assert_eq!(cas(&mut state, Some(2), "m/3.pb", fence(2)), fenced());
    assert_eq!(state.pointer(NS, KEY), Some(&pointer(2, "m/2.pb")));
}
