//! The TiKV WalStore against a live cluster. Skipped unless `OPERON_TEST_PD`
//! is set (`scripts/tikv/playground.sh start`).
#![cfg(feature = "tikv")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use operon_safekeeper::proto::{
    AcceptorMessage, AppendRequest, AppendRequestHeader, ProposerElected, ProposerGreeting,
    VoteRequest,
};
use operon_safekeeper::store::{AppendBatch, Deposed, WalStore};
use operon_safekeeper::tikv::TikvWalStore;
use operon_safekeeper::types::{Configuration, Id, Lsn, ServerInfo, TermHistory, TermLsn};
use operon_safekeeper::{Acceptor, TimelineId};
use operon_tikv::testing;

async fn store() -> Option<TikvWalStore> {
    let cluster = testing::cluster().await?;
    Some(TikvWalStore::new(cluster.connect(testing::TEST_META).await))
}

fn tl(n: u8) -> TimelineId {
    TimelineId::new(Id([n; 16]), Id([n.wrapping_add(1); 16]))
}

fn elected(term: u64, start: u64, th: &[(u64, u64)]) -> ProposerElected {
    ProposerElected {
        generation: 0,
        term,
        start_streaming_at: Lsn(start),
        term_history: TermHistory(
            th.iter()
                .map(|&(t, l)| TermLsn {
                    term: t,
                    lsn: Lsn(l),
                })
                .collect(),
        ),
    }
}

fn batch(term: u64, begin: u64, data: &[u8], commit: u64) -> AppendBatch {
    AppendBatch {
        term,
        begin_lsn: Lsn(begin),
        wal: vec![Bytes::copy_from_slice(data)],
        commit_lsn: Lsn(commit),
        truncate_lsn: Lsn::INVALID,
    }
}

async fn read_all(s: &TikvWalStore, tl: &TimelineId, from: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = Lsn(from);
    loop {
        let got = s.read(tl, at, 1 << 20).await.unwrap();
        if got.is_empty() {
            return out;
        }
        for (lsn, b) in got {
            assert_eq!(lsn, at);
            at = Lsn(at.0 + b.len() as u64);
            out.extend_from_slice(&b);
        }
    }
}

#[tokio::test]
async fn head_vote_elected_append_read() {
    let Some(s) = store().await else { return };
    let t = tl(1);
    assert_eq!(s.load(&t).await.unwrap(), None);
    let st = s
        .create(
            &t,
            ServerInfo {
                pg_version: 160_009,
                system_id: 1,
                wal_seg_size: 16 << 20,
            },
            Lsn::INVALID,
        )
        .await
        .unwrap();
    assert_eq!(
        s.create(&t, ServerInfo::default(), Lsn(5)).await.unwrap(),
        st,
        "create is get-or-create"
    );

    assert!(s.vote(&t, 1).await.unwrap().0);
    assert!(!s.vote(&t, 1).await.unwrap().0);
    s.elected(&t, &elected(1, 100, &[(1, 100)]))
        .await
        .unwrap()
        .unwrap();
    s.append(&t, &batch(1, 100, b"hello ", 0))
        .await
        .unwrap()
        .unwrap();
    let st = s
        .append(&t, &batch(1, 106, b"world", 103))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(111));
    assert_eq!(st.commit_lsn, Lsn(103));
    assert_eq!(read_all(&s, &t, 100).await, b"hello world");
    assert_eq!(read_all(&s, &t, 104).await, b"o world");

    // A retry of the last write is a no-op.
    s.append(&t, &batch(1, 106, b"world", 103))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, &t, 100).await, b"hello world");
}

#[tokio::test]
async fn fencing_and_truncation() {
    let Some(s) = store().await else { return };
    let t = tl(3);
    s.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    s.vote(&t, 1).await.unwrap();
    s.elected(&t, &elected(1, 10, &[(1, 10)]))
        .await
        .unwrap()
        .unwrap();
    s.append(&t, &batch(1, 10, b"abcdef", 12))
        .await
        .unwrap()
        .unwrap();

    s.vote(&t, 2).await.unwrap();
    assert_eq!(
        s.append(&t, &batch(1, 16, b"zz", 0)).await.unwrap(),
        Err(Deposed { current: 2 })
    );
    // Term 1 ended at 14 in the new proposer's history: the tail is cut.
    let st = s
        .elected(&t, &elected(2, 14, &[(1, 10), (2, 14)]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(14));
    assert_eq!(read_all(&s, &t, 10).await, b"abcd");
    s.append(&t, &batch(2, 14, b"XY", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, &t, 10).await, b"abcdXY");
    // Committed WAL is never truncated.
    s.vote(&t, 3).await.unwrap();
    assert!(
        s.elected(&t, &elected(3, 11, &[(1, 10), (3, 11)]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_vote_fences_an_in_flight_append() {
    let Some(s) = store().await else { return };
    let s = Arc::new(s);
    let t = tl(5);
    s.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    s.vote(&t, 1).await.unwrap();
    s.elected(&t, &elected(1, 1, &[(1, 1)]))
        .await
        .unwrap()
        .unwrap();
    // Race many term-1 appends against a term-2 vote: every append that is
    // acknowledged must be below the flush_lsn the vote saw, or be refused.
    let mut handles = Vec::new();
    for i in 0..20u64 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let data = [b'a' + (i % 26) as u8; 4];
            // Each writer appends at its own offset; only contiguous ones apply.
            s.append(&t, &batch(1, 1 + i * 4, &data, 0)).await
        }));
    }
    let (_, voted) = s.vote(&t, 2).await.unwrap();
    for h in handles {
        match h.await.unwrap() {
            Ok(Ok(st)) => assert!(st.term == 1),
            Ok(Err(Deposed { current })) => assert_eq!(current, 2),
            Err(_) => {} // a gap: an earlier writer had not landed yet
        }
    }
    let after = s.load(&t).await.unwrap().unwrap();
    assert_eq!(after.term, 2);
    assert_eq!(
        after.flush_lsn, voted.flush_lsn,
        "no term-1 WAL landed after the vote"
    );
}

#[tokio::test]
async fn trim_is_clamped_and_deletes_chunks() {
    let Some(s) = store().await else { return };
    let t = tl(7);
    s.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    s.vote(&t, 1).await.unwrap();
    s.elected(&t, &elected(1, 1000, &[(1, 1000)]))
        .await
        .unwrap()
        .unwrap();
    for i in 0..10u64 {
        s.append(&t, &batch(1, 1000 + i * 10, &[b'0' + i as u8; 10], 0))
            .await
            .unwrap()
            .unwrap();
    }
    s.record_commit_lsn(&t, Lsn(1100)).await.unwrap();
    // backup_lsn and remote_consistent_lsn still at the start: nothing goes.
    assert_eq!(s.trim(&t, Lsn(1050)).await.unwrap(), Lsn(1000));
    s.record_remote_consistent_lsn(&t, Lsn(1055)).await.unwrap();
    // backup_lsn is still 1000 (no bucket copy yet).
    assert_eq!(s.trim(&t, Lsn(1050)).await.unwrap(), Lsn(1000));
    assert_eq!(read_all(&s, &t, 1000).await.len(), 100);
}

/// The acceptor end to end on TiKV, and a latency sample of the commit path
/// (one 8 KiB append per commit), printed for the P4b notes.
#[tokio::test]
async fn acceptor_on_tikv_commit_latency_sample() {
    let Some(s) = store().await else { return };
    let s = Arc::new(s);
    let g = ProposerGreeting {
        tenant_id: Id([9; 16]),
        timeline_id: Id([10; 16]),
        mconf: Configuration::default(),
        pg_version: 160_009,
        system_id: 5,
        wal_seg_size: 16 << 20,
    };
    let (mut a, _) = Acceptor::greet(s.clone(), 1, &g, true).await.unwrap();
    a.handle_vote(&VoteRequest {
        generation: 0,
        term: 1,
    })
    .await
    .unwrap();
    a.handle_elected(&elected(1, 1 << 24, &[(1, 1 << 24)]))
        .await
        .unwrap();
    let chunk = Bytes::from(vec![7u8; 8192]);
    let mut at = 1u64 << 24;
    let mut lat = Vec::new();
    for _ in 0..200 {
        let req = AppendRequest {
            h: AppendRequestHeader {
                generation: 0,
                term: 1,
                begin_lsn: Lsn(at),
                end_lsn: Lsn(at + 8192),
                commit_lsn: Lsn(at),
                truncate_lsn: Lsn::INVALID,
            },
            wal: chunk.clone(),
        };
        let t0 = Instant::now();
        let r = a.handle_appends(&[req]).await.unwrap();
        lat.push(t0.elapsed());
        at += 8192;
        match r {
            AcceptorMessage::AppendResponse(r) => assert_eq!(r.flush_lsn, Lsn(at)),
            other => panic!("{other:?}"),
        }
    }
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    eprintln!(
        "tikv acceptor append 8 KiB: p50 {:?} p90 {:?} p99 {:?}",
        p(0.5),
        p(0.9),
        p(0.99)
    );
}
