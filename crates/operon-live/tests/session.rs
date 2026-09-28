//! R1 plan Task 12: session versions, merged Transitions, blocked sessions,
//! chunks and resume (design §20 §7.1, §8.2). The version, merge, block and
//! chunk tests run without a cluster; the resume test needs TiKV and skips
//! without `OPERON_TEST_PD`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use buffa::{Message, MessageField};
use operon_live::pb::__buffa::oneof::query_update::Update;
use operon_live::session::{
    ClientState, Outbox, QueryResult, SessionConfig, Sessions, Start, Version, chunks, merge,
};
use operon_live::subs::{SubsConfig, Subscriptions};
use operon_live::system::{INSERT, QUERY};
use operon_live::{LiveConfig, LiveError, LiveValue, Runner, deploy, pb};
use operon_tikv::TimestampExt;
use operon_tikv::testing::{self, TEST_LIVE};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio_util::sync::CancellationToken;

fn v(query_set: u64, ts: u64) -> Version {
    Version {
        query_set,
        identity: 0,
        ts,
    }
}

fn value(q: u32, x: i64) -> pb::QueryUpdate {
    pb::QueryUpdate {
        query_id: q,
        update: Some(Update::Value(Box::new(LiveValue::I64(x).to_proto()))),
        ..Default::default()
    }
}

fn removed(q: u32) -> pb::QueryUpdate {
    pb::QueryUpdate {
        query_id: q,
        update: Some(Update::Removed(Box::default())),
        ..Default::default()
    }
}

fn transition(start: Version, end: Version, updates: Vec<pb::QueryUpdate>) -> pb::Transition {
    pb::Transition {
        session_id: "1-test".into(),
        start: MessageField::some(start.to_proto()),
        end: MessageField::some(end.to_proto()),
        updates,
        more: false,
        ..Default::default()
    }
}

fn int(x: i64) -> QueryResult {
    QueryResult::Value(LiveValue::I64(x))
}

/// Semantics 1 and Review Focus 5: a client applies a Transition only from
/// its current version; a gap changes nothing and asks for a resume.
#[test]
fn transition_applies_only_from_current_version() {
    let mut c = ClientState::new();
    let t1 = transition(v(0, 0), v(1, 10), vec![value(1, 5), value(2, 6)]);
    assert!(c.apply(&t1).expect("from the zero version"));
    assert_eq!(c.version, v(1, 10));
    assert_eq!(c.results, BTreeMap::from([(1, int(5)), (2, int(6))]));
    // The same Transition again, and one past a gap, do not apply.
    for stale in [
        t1.clone(),
        transition(v(1, 11), v(1, 12), vec![value(1, 0)]),
    ] {
        let err = c.apply(&stale).expect_err("a gap");
        assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
        assert_eq!(c.version, v(1, 10));
        assert_eq!(c.results[&1], int(5));
    }
    // A heartbeat (start == end) applies and changes nothing.
    assert!(
        c.apply(&transition(v(1, 10), v(1, 10), vec![]))
            .expect("ok")
    );
    // Chunks apply together, at the last one.
    let mut first = transition(v(1, 10), v(2, 20), vec![value(1, 7)]);
    first.more = true;
    assert!(!c.apply(&first).expect("a chunk"));
    assert_eq!(c.results[&1], int(5), "not applied before the last chunk");
    let mut wrong = transition(v(2, 20), v(2, 30), vec![]);
    wrong.more = true;
    assert!(
        c.apply(&wrong).is_err(),
        "another Transition inside the chunks"
    );
    assert!(
        c.apply(&transition(v(1, 10), v(2, 20), vec![removed(2)]))
            .expect("the last chunk")
    );
    assert_eq!(c.version, v(2, 20));
    assert_eq!(c.results, BTreeMap::from([(1, int(7))]));
}

/// Semantics 3 and Review Focus 5: a full queue merges what it holds; the
/// merged Transitions take a client to the same state as applying every one
/// of them.
#[test]
fn merged_transitions_stay_consistent() {
    for seed in 0..64u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let capacity = rng.random_range(1..5usize);
        let outbox = Outbox::new(capacity, Duration::from_secs(60));
        let mut direct = ClientState::new();
        let mut version = Version::default();
        for i in 0..rng.random_range(1..40u64) {
            let end = v(version.query_set + rng.random_range(0..2u64), 100 + i);
            let updates = (0..rng.random_range(0..4usize))
                .map(|_| {
                    let q = rng.random_range(0..6u32);
                    if rng.random_bool(0.2) {
                        removed(q)
                    } else {
                        value(q, rng.random_range(0..1000))
                    }
                })
                .collect();
            let t = transition(version, end, updates);
            direct.apply(&t).expect("in order");
            assert!(outbox.push(t));
            assert!(outbox.len() <= capacity, "seed {seed}");
            version = end;
        }
        let mut merged = ClientState::new();
        outbox.close(None);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        rt.block_on(async {
            while let Some(t) = outbox.pop().await {
                merged.apply(&t.expect("no error")).expect("in order");
            }
        });
        assert_eq!(merged.version, direct.version, "seed {seed}");
        assert_eq!(merged.results, direct.results, "seed {seed}");
    }
}

/// Semantics 3: a client that takes nothing for `blocked_limit` after its
/// queue filled is closed with RESOURCE_EXHAUSTED.
#[tokio::test]
async fn blocked_session_is_closed() {
    let outbox = Outbox::new(2, Duration::from_millis(50));
    let mut version = Version::default();
    for i in 1..=3 {
        let end = v(1, i);
        assert!(outbox.push(transition(version, end, vec![value(1, i as i64)])));
        version = end;
    }
    assert_eq!(outbox.len(), 1, "the full queue merged");
    assert!(!outbox.check_blocked(), "not blocked yet");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!outbox.push(transition(version, v(1, 9), vec![])), "closed");
    let err = outbox
        .pop()
        .await
        .expect("an item")
        .expect_err("the closing error");
    assert_eq!(
        err.code(),
        pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED,
        "{err}"
    );
    assert!(outbox.pop().await.is_none(), "then the end");

    // A client that keeps taking Transitions is never blocked.
    let outbox = Outbox::new(1, Duration::from_millis(30));
    for i in 1..=5u64 {
        assert!(outbox.push(transition(v(1, i - 1), v(1, i), vec![])));
        tokio::time::sleep(Duration::from_millis(20)).await;
        outbox.pop().await.expect("an item").expect("a Transition");
    }
    assert!(!outbox.check_blocked());
}

/// Semantics 1: a Transition over the message limit goes out as chunks with
/// one `end`, all but the last with `more`, which a client applies as one.
#[test]
fn a_transition_over_the_limit_is_chunked_with_more() {
    let big = |q: u32| pb::QueryUpdate {
        query_id: q,
        update: Some(Update::Value(Box::new(
            LiveValue::Str("x".repeat(3000)).to_proto(),
        ))),
        ..Default::default()
    };
    let t = transition(v(0, 0), v(1, 5), (0..10).map(big).collect());
    let parts = chunks(t.clone(), 8 * 1024);
    assert!(parts.len() >= 4, "{} chunks", parts.len());
    for (i, p) in parts.iter().enumerate() {
        assert!(p.encoded_len() as usize <= 8 * 1024, "chunk {i}");
        assert_eq!(p.more, i + 1 < parts.len());
        assert_eq!(p.end, t.end);
    }
    let mut c = ClientState::new();
    for p in &parts {
        c.apply(p).expect("in order");
    }
    assert_eq!(c.results.len(), 10);
    assert_eq!(chunks(t.clone(), usize::MAX).len(), 1);
    // Merging keeps the later update of a query.
    let a = transition(v(0, 0), v(1, 1), vec![value(1, 1), value(2, 2)]);
    let b = transition(v(1, 1), v(1, 2), vec![removed(1), value(3, 3)]);
    let m = merge(a, b);
    assert_eq!(Version::from_proto(m.start.as_option()), v(0, 0));
    assert_eq!(Version::from_proto(m.end.as_option()), v(1, 2));
    let mut c = ClientState::new();
    c.apply(&m).expect("applies");
    assert_eq!(c.results, BTreeMap::from([(2, int(2)), (3, int(3))]));
}

// ---- on TiKV ----

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn spec(query_id: u32, table: &str) -> pb::QuerySpec {
    pb::QuerySpec {
        query_id,
        function: QUERY.into(),
        args: MessageField::some(obj(&[("table", LiveValue::Str(table.into()))]).to_proto()),
        ..Default::default()
    }
}

/// Semantics 5: a resumed session starts at the client's last version, at a
/// tick at or after its timestamp, with every query's full result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_sends_full_results_at_or_after_last_ts() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_LIVE).await;
    let config = LiveConfig::with_tikv("t12", cluster.config(TEST_LIVE));
    let runner = Runner::open(tikv.clone(), &config).await.expect("a runner");
    let stop = CancellationToken::new();
    let subs = Arc::new(Subscriptions::spawn(
        runner.clone(),
        SubsConfig::default(),
        stop.clone(),
    ));
    let sessions = Sessions::new(
        subs,
        Arc::new(deploy::resolve),
        SessionConfig::default(),
        "1".into(),
        stop.clone(),
    );
    runner
        .mutate(
            deploy::resolve(INSERT).expect("insert"),
            obj(&[
                ("table", LiveValue::Str("rooms".into())),
                ("fields", obj(&[("n", LiveValue::I64(1))])),
            ]),
            None,
        )
        .await
        .expect("an insert");
    // The client last saw query set 3 at a timestamp newer than any tick
    // yet (the manager reads `tick_read_lag` back).
    let last = Version {
        query_set: 3,
        identity: 0,
        ts: tikv.now().await.expect("now").version(),
    };
    let set = pb::QuerySet {
        version: 3,
        queries: vec![spec(1, "rooms"), spec(2, "empty")],
        ..Default::default()
    };
    let session = sessions.open(Start::Resume { last, set }).expect("resumes");
    let first = tokio::time::timeout(Duration::from_secs(20), session.outbox.pop())
        .await
        .expect("a Transition within 20 s")
        .expect("an item")
        .expect("no error");
    assert_eq!(Version::from_proto(first.start.as_option()), last);
    let end = Version::from_proto(first.end.as_option());
    assert!(end.ts >= last.ts, "{end:?} is before {last:?}");
    assert_eq!(end.query_set, 3);
    assert_eq!(first.session_id, session.id);
    let mut client = ClientState::at(last);
    client
        .apply(&first)
        .expect("applies at the client's last version");
    let rooms = match &client.results[&1] {
        QueryResult::Value(LiveValue::Array(docs)) => docs.len(),
        other => panic!("rooms: {other:?}"),
    };
    assert_eq!(rooms, 1);
    assert_eq!(
        client.results[&2],
        QueryResult::Value(LiveValue::Array(vec![]))
    );
    // The client's version only moves forward from here.
    stop.cancel();
    while let Some(next) = session.outbox.pop().await {
        client.apply(&next.expect("no error")).expect("in order");
    }
}
