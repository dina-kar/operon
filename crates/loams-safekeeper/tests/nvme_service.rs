//! The WAL service over the local-NVMe store (§28 §7.2): walproposer's bytes
//! over TCP, pipelined acknowledgements, and recovery after a restart.
#![cfg(all(feature = "server", feature = "nvme"))]
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use loams_safekeeper::journal::{JournalConfig, Tier};
use loams_safekeeper::meta::LocalMeta;
use loams_safekeeper::nvme::NvmeWalStore;
use loams_safekeeper::pgwire::client;
use loams_safekeeper::proto::{
    AcceptorMessage, AppendRequest, AppendRequestHeader, ProposerElected, ProposerGreeting,
    ProposerMessage, VoteRequest,
};
use loams_safekeeper::service::{WalService, WalServiceConfig};
use loams_safekeeper::store::WalStore;
use loams_safekeeper::types::{Configuration, Id, Lsn, TermHistory, TermLsn, TimelineId};
use tokio::net::TcpStream;

const TENANT: &str = "cf0480929707ee75372337efaa5ecf96";
const TIMELINE: &str = "112ded66422aa5e953e5440fa5427ac4";
const START: u64 = 0x0149_6F10;

fn tl() -> TimelineId {
    TimelineId::new(TENANT.parse().unwrap(), TIMELINE.parse().unwrap())
}

async fn store(dir: &Path, tier: Tier) -> Arc<NvmeWalStore> {
    let cfg = JournalConfig {
        segment_size: 1 << 20,
        unit_capacity: 256 << 10,
        ..JournalConfig::new(dir.join("journal"), tier)
    };
    let meta = Arc::new(LocalMeta::open(&dir.join("meta")).unwrap());
    Arc::new(NvmeWalStore::open(cfg, meta).await.unwrap())
}

async fn start(s: Arc<NvmeWalStore>) -> SocketAddr {
    let svc = WalService::new(
        s,
        WalServiceConfig {
            poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    );
    let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = pg.local_addr().unwrap();
    tokio::spawn(svc.serve(pg, std::future::pending()));
    addr
}

async fn connect(addr: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let options = format!("-c timeline_id={TIMELINE} tenant_id={TENANT}");
    client::startup(
        &mut s,
        &[
            ("user", "cloud_admin"),
            ("dbname", "replication"),
            ("options", &options),
        ],
    )
    .await
    .unwrap();
    s
}

async fn send(s: &mut TcpStream, m: ProposerMessage) {
    let mut buf = BytesMut::new();
    m.serialize(&mut buf);
    client::send_copy_data(s, &buf).await.unwrap();
}

async fn recv(s: &mut TcpStream) -> AcceptorMessage {
    AcceptorMessage::parse(client::recv_copy_data(s).await.unwrap().unwrap()).unwrap()
}

fn append(term: u64, begin: u64, data: &[u8], commit: u64) -> ProposerMessage {
    ProposerMessage::Append(AppendRequest {
        h: AppendRequestHeader {
            generation: 0,
            term,
            begin_lsn: Lsn(begin),
            end_lsn: Lsn(begin + data.len() as u64),
            commit_lsn: Lsn(commit),
            truncate_lsn: Lsn(0),
        },
        wal: Bytes::copy_from_slice(data),
    })
}

/// Greet, vote for `term`, and get elected with `history`.
async fn elect(pg: SocketAddr, term: u64, start: u64, history: &[(u64, u64)]) -> TcpStream {
    let mut p = connect(pg).await;
    client::query(
        &mut p,
        "START_WAL_PUSH (proto_version '3', allow_timeline_creation 'true')",
    )
    .await
    .unwrap();
    client::expect_copy_both(&mut p).await.unwrap();
    send(
        &mut p,
        ProposerMessage::Greeting(ProposerGreeting {
            tenant_id: TENANT.parse::<Id>().unwrap(),
            timeline_id: TIMELINE.parse::<Id>().unwrap(),
            mconf: Configuration::default(),
            pg_version: 160_009,
            system_id: 99,
            wal_seg_size: 16 << 20,
        }),
    )
    .await;
    assert!(matches!(recv(&mut p).await, AcceptorMessage::Greeting(_)));
    send(
        &mut p,
        ProposerMessage::VoteRequest(VoteRequest {
            generation: 0,
            term,
        }),
    )
    .await;
    match recv(&mut p).await {
        AcceptorMessage::VoteResponse(v) => assert!(v.vote_given),
        other => panic!("{other:?}"),
    }
    send(
        &mut p,
        ProposerMessage::Elected(ProposerElected {
            generation: 0,
            term,
            start_streaming_at: Lsn(start),
            term_history: TermHistory(
                history
                    .iter()
                    .map(|&(t, l)| TermLsn {
                        term: t,
                        lsn: Lsn(l),
                    })
                    .collect(),
            ),
        }),
    )
    .await;
    p
}

/// Wait for an ack of `end`; acks only ever report durable, rising LSNs.
async fn acked(p: &mut TcpStream, end: u64) {
    let mut last = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), recv(p))
            .await
            .unwrap()
        {
            AcceptorMessage::AppendResponse(r) => {
                assert!(r.flush_lsn.0 >= last, "acks never go back");
                last = r.flush_lsn.0;
                if r.flush_lsn == Lsn(end) {
                    return;
                }
                assert!(r.flush_lsn < Lsn(end));
            }
            other => panic!("{other:?}"),
        }
    }
}

async fn read_all(s: &NvmeWalStore, from: u64) -> Vec<u8> {
    s.read(&tl(), Lsn(from), usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|(_, b)| b.to_vec())
        .collect()
}

#[tokio::test]
async fn pipelined_appends_are_acknowledged_when_durable_and_survive_a_restart() {
    for tier in [Tier::Buffered, Tier::Pwritev2 { depth: 3 }] {
        let d = tempfile::tempdir().unwrap();
        let s = store(d.path(), tier).await;
        let pg = start(s.clone()).await;
        let mut p = elect(pg, 1, START, &[(1, START)]).await;
        // Stream without waiting for acks, as walproposer does.
        let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let mut at = START;
        for chunk in payload.chunks(3000) {
            send(&mut p, append(1, at, chunk, START + 1000)).await;
            at += chunk.len() as u64;
        }
        acked(&mut p, at).await;
        assert_eq!(read_all(&s, START).await, payload);
        drop(p);
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.close();
        drop(s);

        // A restart: the WAL, the term and the history come back.
        let s = store(d.path(), tier).await;
        let st = s.load(&tl()).await.unwrap().unwrap();
        assert_eq!((st.term, st.flush_lsn), (1, Lsn(at)));
        assert_eq!(read_all(&s, START).await, payload);
        let pg = start(s.clone()).await;
        // Term 2 keeps the committed prefix and continues after it.
        let keep = START + 200_000;
        let mut p = elect(pg, 2, keep, &[(1, START), (2, keep)]).await;
        send(&mut p, append(2, keep, b"term two", keep)).await;
        acked(&mut p, keep + 8).await;
        let got = read_all(&s, START).await;
        assert_eq!(&got[..200_000], &payload[..200_000]);
        assert_eq!(&got[200_000..], b"term two");
        s.close();
    }
}

/// The compio data path (§28 §7.2, D263): the tokio prelude hands the WAL
/// push to the timeline's shard, whose io_uring engine makes it durable.
#[cfg(feature = "compio")]
#[tokio::test]
async fn the_push_runs_on_a_compio_shard_with_io_uring() {
    use loams_safekeeper::shard::{ShardConfig, ShardedStore, Shards, UringSync, shard_of};
    for (sync, sqpoll) in [
        (UringSync::Dsync, None),
        (UringSync::Fsync, Some(Duration::from_millis(20))),
    ] {
        let d = tempfile::tempdir().unwrap();
        let n = 2;
        let meta = Arc::new(LocalMeta::open(&d.path().join("meta")).unwrap());
        let mut stores = Vec::new();
        for k in 0..n {
            let cfg = JournalConfig {
                segment_size: 1 << 20,
                unit_capacity: 256 << 10,
                ..JournalConfig::new(
                    d.path().join(format!("shard-{k}")),
                    Tier::Uring { depth: 4 },
                )
            };
            stores.push(Arc::new(
                NvmeWalStore::open_owning(cfg, meta.clone(), |t| shard_of(t, n) == k)
                    .await
                    .unwrap(),
            ));
        }
        let cfg = ShardConfig {
            shards: n,
            depth: 4,
            sync,
            uring: true,
            sqpoll,
            node_id: 1,
            commit_flush_interval: Duration::from_millis(100),
        };
        let (shards, starter) = Shards::start(stores.clone(), cfg).unwrap();
        let router = Arc::new(ShardedStore::new(stores.clone()));
        let svc = WalService::new(
            router.clone(),
            WalServiceConfig {
                poll_interval: Duration::from_millis(5),
                handoff: Some(shards.handoff()),
                ..Default::default()
            },
        );
        starter.run(svc.clone()).unwrap();
        let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = pg.local_addr().unwrap();
        tokio::spawn(svc.serve(pg, std::future::pending()));

        let mut p = elect(addr, 1, START, &[(1, START)]).await;
        let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let mut at = START;
        for chunk in payload.chunks(5000) {
            send(&mut p, append(1, at, chunk, START)).await;
            at += chunk.len() as u64;
        }
        acked(&mut p, at).await;
        let got: Vec<u8> = router
            .read(&tl(), Lsn(START), usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b.to_vec())
            .collect();
        assert_eq!(got, payload);
        // Non-push commands stay on tokio.
        let mut q = connect(addr).await;
        let rows = client::query_rows(&mut q, "TIMELINE_STATUS").await.unwrap();
        assert_eq!(rows[0][0], Some(Lsn(at).to_string()));
        for s in &stores {
            s.close();
        }
    }
}
