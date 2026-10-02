//! The TiKV WalStore against a live cluster. Skipped unless `LOAMS_TEST_PD`
//! is set (`scripts/tikv/playground.sh start`).
#![cfg(feature = "tikv")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use loams_safekeeper::proto::{
    AcceptorMessage, AppendRequest, AppendRequestHeader, ProposerElected, ProposerGreeting,
    VoteRequest,
};
use loams_safekeeper::store::{AppendBatch, Deposed, WalStore};
use loams_safekeeper::tikv::TikvWalStore;
use loams_safekeeper::types::{Configuration, Id, Lsn, ServerInfo, TermHistory, TermLsn};
use loams_safekeeper::{Acceptor, TimelineId};
use loams_tikv::testing;

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
    s.record_commit_lsn(&t, 1, Lsn(1100))
        .await
        .unwrap()
        .unwrap();
    // backup_lsn and remote_consistent_lsn still at the start: nothing goes.
    assert_eq!(s.trim(&t, Lsn(1050)).await.unwrap(), Lsn(1000));
    s.record_remote_consistent_lsn(&t, Lsn(1055)).await.unwrap();
    assert_eq!(
        s.trim(&t, Lsn(1050)).await.unwrap(),
        Lsn(1000),
        "no bucket copy yet"
    );
    assert_eq!(read_all(&s, &t, 1000).await.len(), 100);

    // The bucket copy reaches 1045: trim to 1045, keeping the chunk at 1040.
    s.record_backup_lsn(&t, Lsn(1045)).await.unwrap();
    assert_eq!(s.trim(&t, Lsn(1050)).await.unwrap(), Lsn(1045));
    assert!(matches!(
        s.read(&t, Lsn(1044), 10).await,
        Err(loams_safekeeper::Error::Trimmed { .. })
    ));
    let rest = read_all(&s, &t, 1045).await;
    assert_eq!(rest.len(), 55);
    assert_eq!(&rest[..5], b"44444");
    // The chunks below the kept one are gone from TiKV.
    let tikv = s.tikv().clone();
    let left = tikv
        .run(loams_tikv::TxnOptions::new("test.scan"), |txn| {
            Box::pin(async move { txn.scan(b"W", Some(b"X"), 100).await })
        })
        .await
        .unwrap()
        .value;
    assert_eq!(left.len(), 6, "chunks from 1040 on remain");
}

/// A long uncommitted tail of a deposed term is truncated in bounded batches.
#[tokio::test]
async fn election_truncates_a_long_tail_in_batches() {
    let Some(s) = store().await else { return };
    let t = tl(11);
    s.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    s.vote(&t, 1).await.unwrap();
    s.elected(&t, &elected(1, 100, &[(1, 100)]))
        .await
        .unwrap()
        .unwrap();
    // 200 one-byte chunks, more than one truncation batch (64).
    for i in 0..200u64 {
        s.append(&t, &batch(1, 100 + i, b"x", 101))
            .await
            .unwrap()
            .unwrap();
    }
    s.vote(&t, 2).await.unwrap();
    let st = s
        .elected(&t, &elected(2, 101, &[(1, 100), (2, 101)]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(101));
    assert_eq!(read_all(&s, &t, 100).await, b"x");
    s.append(&t, &batch(2, 101, b"NEW", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, &t, 100).await, b"xNEW");
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

// The raw store (§28 §7.3) on a live cluster.

async fn raw_store() -> Option<loams_safekeeper::tikv_raw::TikvRawWalStore> {
    let cluster = testing::cluster().await?;
    let kv = loams_safekeeper::tikv_raw::TikvRawKv::connect(cluster.config(testing::TEST_META))
        .await
        .unwrap();
    Some(kv.into_store(8))
}

async fn read_all_raw(
    s: &loams_safekeeper::tikv_raw::TikvRawWalStore,
    tl: &TimelineId,
    from: u64,
) -> Vec<u8> {
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
async fn raw_store_fences_a_deposed_writer() {
    let Some(old) = raw_store().await else { return };
    let t = tl(40);
    old.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    assert!(old.vote(&t, 1).await.unwrap().0);
    old.elected(&t, &elected(1, 100, &[(1, 100)]))
        .await
        .unwrap()
        .unwrap();
    let st = old
        .append(&t, &batch(1, 100, b"abcdef", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(106));

    // A second instance on the same keys: a new proposer takes over.
    let new =
        loams_safekeeper::tikv_raw::RawWalStore::new(old.kv().clone(), old.kv().root().to_vec(), 8);
    let (given, st) = new.vote(&t, 2).await.unwrap();
    assert!(given);
    assert_eq!(st.flush_lsn, Lsn(106));
    // The old writer's put lands, its fence read refuses the ack.
    assert_eq!(
        old.append(&t, &batch(1, 106, b"ghi", 0)).await.unwrap(),
        Err(Deposed { current: 2 })
    );
    new.elected(&t, &elected(2, 104, &[(1, 100), (2, 104)]))
        .await
        .unwrap()
        .unwrap();
    new.append(&t, &batch(2, 104, b"XY", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all_raw(&new, &t, 100).await, b"abcdXY");
    let st = new.load(&t).await.unwrap().unwrap();
    assert_eq!((st.term, st.flush_lsn), (2, Lsn(106)));
    // Trim below the commit point.
    new.record_commit_lsn(&t, 2, Lsn(106))
        .await
        .unwrap()
        .unwrap();
    new.record_backup_lsn(&t, Lsn(106)).await.unwrap();
    new.record_remote_consistent_lsn(&t, Lsn(106))
        .await
        .unwrap();
    assert_eq!(new.trim(&t, Lsn(105)).await.unwrap(), Lsn(105));
    assert_eq!(read_all_raw(&new, &t, 105).await, b"Y");
}

/// Pipelined appends on a live cluster, and a latency sample of the raw
/// commit path (one 8 KiB append per commit, one in flight).
#[tokio::test]
async fn raw_store_pipelined_appends_and_latency_sample() {
    let Some(s) = raw_store().await else { return };
    let s = Arc::new(s);
    let t = tl(41);
    s.create(&t, ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    s.vote(&t, 1).await.unwrap();
    s.elected(&t, &elected(1, 1 << 24, &[(1, 1 << 24)]))
        .await
        .unwrap()
        .unwrap();
    let chunk = vec![7u8; 8192];
    let mut at = 1u64 << 24;
    let mut lat = Vec::new();
    for _ in 0..200 {
        let t0 = Instant::now();
        let st = s
            .append(&t, &batch(1, at, &chunk, at))
            .await
            .unwrap()
            .unwrap();
        lat.push(t0.elapsed());
        at += 8192;
        assert_eq!(st.flush_lsn, Lsn(at));
    }
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    eprintln!(
        "tikv raw append 8 KiB: p50 {:?} p90 {:?} p99 {:?}",
        p(0.5),
        p(0.9),
        p(0.99)
    );
    // 64 appends, 8 in flight at a time.
    let start = at;
    let mut tasks = Vec::new();
    for i in 0..64u64 {
        let s = s.clone();
        let b = batch(1, start + i * 8192, &chunk, 0);
        tasks.push(tokio::spawn(async move { s.append(&t, &b).await }));
        if tasks.len() >= 8 {
            tasks.remove(0).await.unwrap().unwrap().unwrap();
        }
    }
    for task in tasks {
        task.await.unwrap().unwrap().unwrap();
    }
    let end = start + 64 * 8192;
    assert_eq!(s.load(&t).await.unwrap().unwrap().flush_lsn, Lsn(end));
    assert_eq!(read_all_raw(&s, &t, start).await.len() as u64, end - start);
}
