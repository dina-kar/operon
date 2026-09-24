use operon_meta::{ApplyError, Command, LeaseGrant, MAX_LEASE_TTL_MS, MetaState, Reply};

const KEY: &str = "task/segmenter/0";

fn acquire(
    state: &mut MetaState,
    owner: &str,
    ttl_ms: u64,
    now_ms: u64,
) -> Result<Reply, ApplyError> {
    state.apply(Command::AcquireLease {
        key: KEY.to_string(),
        owner: owner.to_string(),
        ttl_ms,
        now_ms,
    })
}

fn renew(state: &mut MetaState, owner: &str, epoch: u64, now_ms: u64) -> Result<Reply, ApplyError> {
    state.apply(Command::RenewLease {
        key: KEY.to_string(),
        owner: owner.to_string(),
        epoch,
        ttl_ms: 1_000,
        now_ms,
    })
}

fn release(state: &mut MetaState, owner: &str, epoch: u64) -> Result<Reply, ApplyError> {
    state.apply(Command::ReleaseLease {
        key: KEY.to_string(),
        owner: owner.to_string(),
        epoch,
    })
}

fn grant(epoch: u64, deadline_ms: u64) -> Result<Reply, ApplyError> {
    Ok(Reply::Lease(LeaseGrant { epoch, deadline_ms }))
}

fn lost() -> Result<Reply, ApplyError> {
    Err(ApplyError::LeaseLost {
        key: KEY.to_string(),
    })
}

#[test]
fn a_free_lease_is_granted_at_epoch_one() {
    let mut state = MetaState::default();
    assert_eq!(acquire(&mut state, "w1", 1_000, 5_000), grant(1, 6_000));

    let lease = state.lease(KEY).unwrap();
    assert_eq!(lease.owner.as_deref(), Some("w1"));
    assert!(lease.is_held_at(5_999));
    assert!(!lease.is_held_at(6_000));
    assert_eq!(state.clock_ms(), 5_000);
}

#[test]
fn a_held_lease_is_refused_to_others_until_it_expires() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();

    assert_eq!(
        acquire(&mut state, "w2", 1_000, 5_999),
        Err(ApplyError::LeaseHeld {
            owner: "w1".to_string(),
            deadline_ms: 6_000
        })
    );
    assert_eq!(acquire(&mut state, "w2", 1_000, 6_000), grant(2, 7_000));
    assert_eq!(state.lease(KEY).unwrap().owner.as_deref(), Some("w2"));
}

#[test]
fn reacquiring_your_own_lease_keeps_the_epoch() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();
    // A retry after a lost acknowledgement.
    assert_eq!(acquire(&mut state, "w1", 1_000, 5_500), grant(1, 6_500));
    // Once expired, even the same owner gets a new epoch.
    assert_eq!(acquire(&mut state, "w1", 1_000, 6_500), grant(2, 7_500));
}

#[test]
fn renew_extends_only_a_held_lease_at_the_right_epoch() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();

    assert_eq!(renew(&mut state, "w1", 1, 5_900), grant(1, 6_900));
    assert_eq!(renew(&mut state, "w1", 2, 6_000), lost());
    assert_eq!(renew(&mut state, "w2", 1, 6_000), lost());
    assert_eq!(renew(&mut state, "w1", 1, 6_900), lost(), "expired");

    let mut empty = MetaState::default();
    assert_eq!(renew(&mut empty, "w1", 1, 0), lost(), "never acquired");
}

#[test]
fn release_frees_the_lease_and_is_idempotent() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();

    assert_eq!(release(&mut state, "w1", 1), Ok(Reply::LeaseReleased));
    assert_eq!(release(&mut state, "w1", 1), Ok(Reply::LeaseReleased));
    assert!(!state.lease(KEY).unwrap().is_held_at(5_000));
    assert_eq!(renew(&mut state, "w1", 1, 5_100), lost());

    // The next holder gets a new epoch even though the old deadline has not passed.
    assert_eq!(acquire(&mut state, "w2", 1_000, 5_100), grant(2, 6_100));
    assert_eq!(release(&mut state, "w1", 1), lost(), "stale epoch");
    assert_eq!(release(&mut state, "w1", 2), lost(), "not the owner");
}

#[test]
fn the_clock_never_goes_backwards() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();
    acquire(&mut state, "w2", 1_000, 6_000).unwrap();

    // A command stamped by a leader whose clock is behind cannot revive w1's lease.
    assert_eq!(renew(&mut state, "w1", 1, 5_100), lost());
    assert_eq!(state.clock_ms(), 6_000);
    // Its deadline is computed from the metastore clock, not the stale stamp.
    assert_eq!(renew(&mut state, "w2", 2, 1_000), grant(2, 7_000));
}

#[test]
fn invalid_lease_arguments_are_rejected_and_leave_state_unchanged() {
    let mut state = MetaState::default();
    for ttl_ms in [0, MAX_LEASE_TTL_MS + 1] {
        let err = acquire(&mut state, "w1", ttl_ms, 5_000).unwrap_err();
        assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    }
    let err = acquire(&mut state, "", 1_000, 5_000).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    let err = state
        .apply(Command::AcquireLease {
            key: String::new(),
            owner: "w1".to_string(),
            ttl_ms: 1_000,
            now_ms: 5_000,
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    assert_eq!(state, MetaState::default());
}

#[test]
fn a_far_future_clock_saturates_instead_of_overflowing() {
    let mut state = MetaState::default();
    assert_eq!(
        acquire(&mut state, "w1", 1_000, u64::MAX - 10),
        grant(1, u64::MAX)
    );
}

fn reacquire(
    state: &mut MetaState,
    owner: &str,
    epoch: u64,
    now_ms: u64,
) -> Result<Reply, ApplyError> {
    state.apply(Command::ReacquireLease {
        key: KEY.to_string(),
        owner: owner.to_string(),
        epoch,
        ttl_ms: 1_000,
        now_ms,
    })
}

/// M0.3 re-review N3: a holder whose renewal came too late re-takes its
/// expired lease at the same epoch, as long as nobody else took it.
#[test]
fn an_expired_lease_nobody_took_is_reacquired_at_the_same_epoch() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();
    assert_eq!(renew(&mut state, "w1", 1, 6_500), lost(), "expired");
    assert_eq!(reacquire(&mut state, "w1", 1, 6_500), grant(1, 7_500));
    // A retry extends it again.
    assert_eq!(reacquire(&mut state, "w1", 1, 6_600), grant(1, 7_600));
    // It is held again, so renewals work and others are kept out.
    assert_eq!(renew(&mut state, "w1", 1, 7_000), grant(1, 8_000));
    assert!(matches!(
        acquire(&mut state, "w2", 1_000, 7_100),
        Err(ApplyError::LeaseHeld { .. })
    ));
}

#[test]
fn a_lease_taken_over_or_released_cannot_be_reacquired() {
    let mut state = MetaState::default();
    acquire(&mut state, "w1", 1_000, 5_000).unwrap();
    acquire(&mut state, "w2", 1_000, 6_500).unwrap();
    let before = state.clone();
    assert_eq!(reacquire(&mut state, "w1", 1, 6_600), lost(), "taken over");
    assert_eq!(reacquire(&mut state, "w2", 1, 6_600), lost(), "wrong epoch");
    assert_eq!(state, before, "a rejected reacquire changes nothing");

    release(&mut state, "w2", 2).unwrap();
    assert_eq!(reacquire(&mut state, "w2", 2, 6_700), lost(), "released");
    let mut fresh = MetaState::default();
    assert_eq!(reacquire(&mut fresh, "w1", 1, 1), lost(), "never taken");
    assert_eq!(fresh, MetaState::default());
}
