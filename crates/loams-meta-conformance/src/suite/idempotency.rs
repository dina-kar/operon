//! The stream ingest ledger (design §02 §7.4, D270).

use std::time::Duration;

use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, Consistency, Fence, IdempotencyClaim, IdempotencyCompletion, IdempotencyEntry,
    IdempotencyKey, IdempotencyState, MAX_IDEMPOTENCY_KEYS, MAX_IDEMPOTENCY_TTL_MS, MetaStore,
};

use super::{namespace, rejected, stream};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;
const TTL_MS: u64 = 60_000;
const WINDOW_MS: u64 = 3_600_000;

fn key(n: u8) -> IdempotencyKey {
    [n; 32]
}

fn claim(stream: StreamId, owner: &str, keys: &[u8], ttl_ms: u64) -> IdempotencyClaim {
    IdempotencyClaim {
        stream,
        owner: owner.to_string(),
        keys: keys.iter().copied().map(key).collect(),
        ttl_ms,
    }
}

fn completion(stream: StreamId, owner: &str, done: &[(u8, u32, u64)]) -> IdempotencyCompletion {
    IdempotencyCompletion {
        stream,
        owner: owner.to_string(),
        done: done.iter().map(|&(k, p, o)| (key(k), p, o)).collect(),
        window_ms: WINDOW_MS,
    }
}

/// Long enough for a 1 ms claim to have lapsed by any clock.
async fn let_it_lapse() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn one_stream(meta: &dyn MetaStore, name: &str) -> StreamId {
    let ns = namespace(meta, name).await;
    stream(meta, ns, "events", 2).await
}

pub async fn claim_complete_then_duplicate(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let s = one_stream(meta, "idem-complete").await;
    let states = meta
        .claim_idempotency_keys(claim(s, "req-1", &[1, 2], TTL_MS))
        .await
        .expect("claim");
    assert_eq!(states, vec![IdempotencyState::Claimed; 2]);
    // The same owner claiming again (a retry) claims again.
    let again = meta
        .claim_idempotency_keys(claim(s, "req-1", &[1, 2], TTL_MS))
        .await
        .expect("claim again");
    assert_eq!(again, vec![IdempotencyState::Claimed; 2]);
    meta.complete_idempotency_keys(completion(s, "req-1", &[(1, 0, 10), (2, 1, 20)]))
        .await
        .expect("complete");
    // Completing again changes nothing.
    meta.complete_idempotency_keys(completion(s, "req-1", &[(1, 0, 99)]))
        .await
        .expect("complete again");
    let states = db
        .last()
        .claim_idempotency_keys(claim(s, "req-2", &[2, 1, 3], TTL_MS))
        .await
        .expect("claim");
    assert_eq!(
        states,
        vec![
            IdempotencyState::Done {
                partition: 1,
                offset: 20
            },
            IdempotencyState::Done {
                partition: 0,
                offset: 10
            },
            IdempotencyState::Claimed,
        ]
    );
    match db.last().idempotency_key(L, s, key(1)).await.expect("read") {
        Some(IdempotencyEntry::Done {
            partition, offset, ..
        }) => assert_eq!((partition, offset), (0, 10)),
        other => panic!("expected a done entry, got {other:?}"),
    }
    assert_eq!(
        db.last().idempotency_key(L, s, key(9)).await.expect("read"),
        None
    );
}

pub async fn a_pending_claim_is_in_flight_until_it_lapses_or_is_released(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let s = one_stream(meta, "idem-inflight").await;
    meta.claim_idempotency_keys(claim(s, "req-1", &[1], TTL_MS))
        .await
        .expect("claim");
    let states = meta
        .claim_idempotency_keys(claim(s, "req-2", &[1], TTL_MS))
        .await
        .expect("claim");
    assert!(matches!(states[0], IdempotencyState::InFlight { .. }));
    // Only the owner releases.
    meta.release_idempotency_keys(s, "req-2", vec![key(1)])
        .await
        .expect("release");
    let states = meta
        .claim_idempotency_keys(claim(s, "req-3", &[1], TTL_MS))
        .await
        .expect("claim");
    assert!(matches!(states[0], IdempotencyState::InFlight { .. }));
    meta.release_idempotency_keys(s, "req-1", vec![key(1)])
        .await
        .expect("release");
    assert_eq!(
        meta.idempotency_key(L, s, key(1)).await.expect("read"),
        None
    );
    let states = meta
        .claim_idempotency_keys(claim(s, "req-3", &[1, 2], 1))
        .await
        .expect("claim");
    assert_eq!(states, vec![IdempotencyState::Claimed; 2]);
    let_it_lapse().await;
    // A lapsed claim is claimed by the next request.
    let states = meta
        .claim_idempotency_keys(claim(s, "req-4", &[1], TTL_MS))
        .await
        .expect("claim");
    assert_eq!(states, vec![IdempotencyState::Claimed]);
    // A lapsed claim still completes, if nobody claimed the key since.
    meta.complete_idempotency_keys(completion(s, "req-3", &[(2, 0, 5)]))
        .await
        .expect("complete");
    // The key another owner claimed since is left alone.
    meta.complete_idempotency_keys(completion(s, "req-3", &[(1, 0, 6)]))
        .await
        .expect("complete");
    assert!(matches!(
        meta.idempotency_key(L, s, key(1)).await.expect("read"),
        Some(IdempotencyEntry::Pending { owner, .. }) if owner == "req-4"
    ));
    assert!(matches!(
        meta.idempotency_key(L, s, key(2)).await.expect("read"),
        Some(IdempotencyEntry::Done { offset: 5, .. })
    ));
}

pub async fn a_done_key_lapses_with_its_window_and_prune_forgets_it(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let s = one_stream(meta, "idem-prune").await;
    meta.claim_idempotency_keys(claim(s, "req-1", &[1, 2, 3], TTL_MS))
        .await
        .expect("claim");
    let mut short = completion(s, "req-1", &[(1, 0, 1)]);
    short.window_ms = 1;
    meta.complete_idempotency_keys(short)
        .await
        .expect("complete");
    meta.complete_idempotency_keys(completion(s, "req-1", &[(2, 0, 2)]))
        .await
        .expect("complete");
    let_it_lapse().await;
    // The lapsed done key is claimed anew; the live one still answers.
    let states = meta
        .claim_idempotency_keys(claim(s, "req-2", &[1, 2], TTL_MS))
        .await
        .expect("claim");
    assert_eq!(
        states,
        vec![
            IdempotencyState::Claimed,
            IdempotencyState::Done {
                partition: 0,
                offset: 2
            }
        ]
    );
    // Key 3 is completed with a 1 ms window: once it lapses, a prune forgets
    // it and only it; a second prune finds nothing more.
    meta.release_idempotency_keys(s, "req-2", vec![key(1)])
        .await
        .expect("release");
    let mut brief = completion(s, "req-1", &[(3, 1, 3)]);
    brief.window_ms = 1;
    meta.complete_idempotency_keys(brief)
        .await
        .expect("complete");
    let_it_lapse().await;
    assert_eq!(meta.prune_idempotency_keys(None).await.expect("prune"), 1);
    assert_eq!(meta.prune_idempotency_keys(None).await.expect("prune"), 0);
    assert_eq!(
        meta.idempotency_key(L, s, key(3)).await.expect("read"),
        None
    );
    assert!(
        meta.idempotency_key(L, s, key(2))
            .await
            .expect("read")
            .is_some()
    );
}

pub async fn ledger_requests_are_validated(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let s = one_stream(meta, "idem-invalid").await;
    let too_many: Vec<IdempotencyKey> = (0..=MAX_IDEMPOTENCY_KEYS)
        .map(|n| {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&(n as u64).to_be_bytes());
            k
        })
        .collect();
    let cases = [
        claim(s, "req", &[], TTL_MS),
        claim(s, "req", &[1, 1], TTL_MS),
        claim(s, "req", &[1], 0),
        claim(s, "req", &[1], MAX_IDEMPOTENCY_TTL_MS + 1),
        claim(s, "", &[1], TTL_MS),
        IdempotencyClaim {
            keys: too_many,
            ..claim(s, "req", &[1], TTL_MS)
        },
    ];
    for case in cases {
        assert!(
            matches!(
                rejected(meta.claim_idempotency_keys(case).await),
                ApplyError::InvalidArgument(_)
            ),
            "an invalid claim is refused"
        );
    }
    let mut bad = completion(s, "req", &[(1, 0, 1)]);
    bad.window_ms = MAX_IDEMPOTENCY_TTL_MS + 1;
    assert!(matches!(
        rejected(meta.complete_idempotency_keys(bad).await),
        ApplyError::InvalidArgument(_)
    ));
    let missing = StreamId(s.0 + 1000);
    assert_eq!(
        rejected(
            meta.claim_idempotency_keys(claim(missing, "req", &[1], TTL_MS))
                .await
        ),
        ApplyError::StreamNotFound(missing)
    );
}

/// A ledger prune under a stale lease epoch is refused with `Fenced` and
/// removes nothing: pending claims, unexpired done entries and even lapsed
/// ones stay. A prune under the current epoch then removes only the lapsed
/// entry.
pub async fn a_fenced_ledger_prune_is_refused_and_keeps_every_entry(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let s = one_stream(meta, "idem-fenced").await;
    meta.claim_idempotency_keys(claim(s, "req-1", &[1, 2, 3], TTL_MS))
        .await
        .expect("claim");
    meta.complete_idempotency_keys(completion(s, "req-1", &[(1, 0, 1)]))
        .await
        .expect("complete");
    // Key 3 is done with a 1 ms window: it lapses before the prunes.
    let mut brief = completion(s, "req-1", &[(3, 0, 3)]);
    brief.window_ms = 1;
    meta.complete_idempotency_keys(brief)
        .await
        .expect("complete");
    let_it_lapse().await;
    let lease = "idem-fenced/lease";
    let grant = meta
        .acquire_lease(lease, "retention", Duration::from_secs(30))
        .await
        .expect("acquire");
    let fence = |epoch| {
        Some(Fence {
            lease: lease.to_string(),
            epoch,
        })
    };
    // A holder that lost the lease prunes nothing.
    assert_eq!(
        rejected(meta.prune_idempotency_keys(fence(grant.epoch + 1)).await),
        ApplyError::Fenced {
            lease: lease.to_string()
        }
    );
    // The refusal removed nothing: the lapsed entry is still readable.
    assert!(
        meta.idempotency_key(L, s, key(3))
            .await
            .expect("read")
            .is_some()
    );
    // The right holder prunes exactly the lapsed entry; live ones (pending
    // and done) stay.
    assert_eq!(
        db.last()
            .prune_idempotency_keys(fence(grant.epoch))
            .await
            .expect("prune"),
        1
    );
    assert_eq!(
        meta.idempotency_key(L, s, key(3)).await.expect("read"),
        None
    );
    for n in [1, 2] {
        assert!(
            meta.idempotency_key(L, s, key(n))
                .await
                .expect("read")
                .is_some()
        );
    }
    meta.release_lease(lease, "retention", grant.epoch)
        .await
        .expect("release");
}
