//! The WAL service over TCP, driven by a hand-rolled proposer and reader
//! speaking walproposer's bytes.
#![cfg(feature = "server")]
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use loams_safekeeper::feeder::FeederConfig;
use loams_safekeeper::pgwire::client;
use loams_safekeeper::proto::{
    AcceptorMessage, AppendRequest, AppendRequestHeader, ProposerElected, ProposerGreeting,
    ProposerMessage, VoteRequest,
};
use loams_safekeeper::service::{WalService, WalServiceConfig};
use loams_safekeeper::store::MemWalStore;
use loams_safekeeper::store::WalStore;
use loams_safekeeper::types::{Configuration, Id, Lsn, TermHistory, TermLsn};
use tokio::net::TcpStream;

const TENANT: &str = "cf0480929707ee75372337efaa5ecf96";
const TIMELINE: &str = "112ded66422aa5e953e5440fa5427ac4";

async fn start() -> (SocketAddr, SocketAddr, Arc<WalService<MemWalStore>>) {
    start_with(None).await
}

async fn start_with(
    feeder: Option<FeederConfig>,
) -> (SocketAddr, SocketAddr, Arc<WalService<MemWalStore>>) {
    start_cfg(feeder, None).await
}

async fn start_cfg(
    feeder: Option<FeederConfig>,
    auth_token: Option<String>,
) -> (SocketAddr, SocketAddr, Arc<WalService<MemWalStore>>) {
    let svc = WalService::new(
        Arc::new(MemWalStore::new()),
        WalServiceConfig {
            poll_interval: Duration::from_millis(5),
            feeder,
            auth_token,
            ..Default::default()
        },
    );
    let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg.local_addr().unwrap();
    let web = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let web_addr = web.local_addr().unwrap();
    let app = loams_safekeeper::http::router(svc.clone());
    tokio::spawn(async move { axum::serve(web, app).await });
    tokio::spawn(svc.clone().serve(pg, std::future::pending()));
    (pg_addr, web_addr, svc)
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

const START: u64 = 0x0149_6F10;

/// Greeting, vote, election and a stream of appends, as walproposer does.
async fn propose(pg: SocketAddr, payload: &[u8]) -> TcpStream {
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
    assert!(matches!(recv(&mut p).await, AcceptorMessage::Greeting(g) if g.term == 0));
    send(
        &mut p,
        ProposerMessage::VoteRequest(VoteRequest {
            generation: 0,
            term: 1,
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
            term: 1,
            start_streaming_at: Lsn(START),
            term_history: TermHistory(vec![TermLsn {
                term: 1,
                lsn: Lsn(START),
            }]),
        }),
    )
    .await;
    // Stream without waiting, as walproposer does; acks may cover several.
    let mut at = START;
    for chunk in payload.chunks(7) {
        send(&mut p, append(1, at, chunk, 0)).await;
        at += chunk.len() as u64;
    }
    let end = START + payload.len() as u64;
    loop {
        match recv(&mut p).await {
            AcceptorMessage::AppendResponse(r) if r.flush_lsn == Lsn(end) => break,
            AcceptorMessage::AppendResponse(r) => assert!(r.flush_lsn < Lsn(end)),
            other => panic!("{other:?}"),
        }
    }
    // The quorum commit arrives in a heartbeat. (A pipelined store may
    // still owe responses for writes the one above already covered.)
    send(&mut p, append(1, end, b"", end)).await;
    loop {
        match recv(&mut p).await {
            AcceptorMessage::AppendResponse(r) if r.commit_lsn == Lsn(end) => break,
            AcceptorMessage::AppendResponse(r) => assert_eq!(r.flush_lsn, Lsn(end)),
            other => panic!("{other:?}"),
        }
    }
    p
}

#[tokio::test]
async fn walproposer_flow_then_replication_and_status() {
    let (pg, web, _svc) = start().await;
    let payload: Vec<u8> = (0..500u32).map(|i| (i % 251) as u8).collect();
    let _p = propose(pg, &payload).await;
    let end = START + payload.len() as u64;

    let mut q = connect(pg).await;
    let rows = client::query_rows(&mut q, "TIMELINE_STATUS").await.unwrap();
    assert_eq!(
        rows,
        vec![vec![Some(Lsn(end).to_string()), Some(Lsn(end).to_string())]]
    );
    let rows = client::query_rows(&mut q, "IDENTIFY_SYSTEM").await.unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("99"));
    assert_eq!(rows[0][2], Some(Lsn(end).to_string()));
    assert!(client::query_rows(&mut q, "SELECT 1").await.is_err());

    // A vanilla reader gets the committed WAL, from an offset.
    let mut r = connect(pg).await;
    client::query(
        &mut r,
        &format!("START_REPLICATION PHYSICAL {}", Lsn(START + 10)),
    )
    .await
    .unwrap();
    client::expect_copy_both(&mut r).await.unwrap();
    let mut got = Vec::new();
    while got.len() < payload.len() - 10 {
        let mut m = client::recv_copy_data(&mut r).await.unwrap().unwrap();
        match m.get_u8() {
            b'w' => {
                let start = m.get_u64();
                let _end = m.get_u64();
                let _ts = m.get_i64();
                assert_eq!(start, START + 10 + got.len() as u64);
                got.extend_from_slice(&m);
            }
            b'k' => {}
            t => panic!("unexpected {}", t as char),
        }
    }
    assert_eq!(got, &payload[10..]);

    // The HTTP API shows the same head.
    let body = http_get(web, &format!("/v1/tenant/{TENANT}/timeline/{TIMELINE}")).await;
    assert!(
        body.contains(&format!("\"flush_lsn\":\"{}\"", Lsn(end))),
        "{body}"
    );
    assert!(body.contains("\"term\":1"), "{body}");
}

/// A RawKv whose writes complete out of order: each put sleeps a little,
/// longer for some than for later ones.
#[derive(Debug, Default)]
struct Shuffled {
    inner: loams_safekeeper::tikv_raw::MemRawKv,
    n: std::sync::atomic::AtomicU64,
}

#[async_trait::async_trait]
impl loams_safekeeper::tikv_raw::RawKv for Shuffled {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, loams_safekeeper::Error> {
        self.inner.get(key).await
    }
    async fn batch_put(
        &self,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), loams_safekeeper::Error> {
        let i = self.n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis((i * 7) % 5)).await;
        self.inner.batch_put(pairs).await
    }
    async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, bool), loams_safekeeper::Error> {
        self.inner.compare_and_swap(key, expected, new).await
    }
    async fn scan(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, loams_safekeeper::Error> {
        self.inner.scan(from, to, limit).await
    }
    async fn delete_range(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
    ) -> Result<(), loams_safekeeper::Error> {
        self.inner.delete_range(from, to).await
    }
}

/// The walproposer flow over the raw store with 8 appends in flight whose
/// writes land out of order: acks stay monotonic and cover only contiguous
/// WAL, and readers get exactly the stream.
#[tokio::test]
async fn pipelined_appends_over_the_raw_store() {
    let store = Arc::new(loams_safekeeper::tikv_raw::RawWalStore::new(
        Arc::new(Shuffled::default()),
        b"t/".to_vec(),
        8,
    ));
    assert_eq!(store.max_in_flight(), 8);
    let svc = WalService::new(
        store,
        WalServiceConfig {
            poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    );
    let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg.local_addr().unwrap();
    tokio::spawn(svc.clone().serve(pg, std::future::pending()));

    let payload: Vec<u8> = (0..3000u32).map(|i| (i % 253) as u8).collect();
    let _p = propose(pg_addr, &payload).await;
    let end = START + payload.len() as u64;
    let mut q = connect(pg_addr).await;
    let rows = client::query_rows(&mut q, "TIMELINE_STATUS").await.unwrap();
    assert_eq!(rows[0][0], Some(Lsn(end).to_string()));

    let mut r = connect(pg_addr).await;
    client::query(
        &mut r,
        &format!("START_REPLICATION PHYSICAL {}", Lsn(START)),
    )
    .await
    .unwrap();
    client::expect_copy_both(&mut r).await.unwrap();
    let mut got = Vec::new();
    while got.len() < payload.len() {
        let mut m = client::recv_copy_data(&mut r).await.unwrap().unwrap();
        if m.get_u8() == b'w' {
            let start = m.get_u64();
            let _end = m.get_u64();
            let _ts = m.get_i64();
            assert_eq!(start, START + got.len() as u64);
            got.extend_from_slice(&m);
        }
    }
    assert_eq!(got, payload);
}

#[tokio::test]
async fn proto_v2_and_missing_timeline_are_refused() {
    let (pg, _web, _svc) = start().await;
    let mut p = connect(pg).await;
    client::query(&mut p, "START_WAL_PUSH (proto_version '2')")
        .await
        .unwrap();
    assert!(client::expect_copy_both(&mut p).await.is_err());

    let mut r = connect(pg).await;
    client::query(&mut r, "START_REPLICATION PHYSICAL 0/0")
        .await
        .unwrap();
    let err = client::expect_copy_both(&mut r).await;
    assert!(err.is_err() || client::recv_copy_data(&mut r).await.is_err());
}

#[tokio::test]
async fn http_create_then_walproposer_without_creation() {
    let (pg, web, _svc) = start().await;
    let body = http_post(
        web,
        "/v1/tenant/timeline",
        &format!(
            r#"{{"tenant_id":"{TENANT}","timeline_id":"{TIMELINE}","pg_version":16,"start_lsn":"0/1496F10","mconf":{{"generation":1}}}}"#
        ),
    )
    .await;
    assert!(
        body.contains("\"timeline_start_lsn\":\"0/1496F10\""),
        "{body}"
    );
    assert!(body.contains("\"pg_version\":160000"), "{body}");

    let mut p = connect(pg).await;
    client::query(
        &mut p,
        "START_WAL_PUSH (proto_version '3', allow_timeline_creation 'false')",
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
            system_id: 0,
            wal_seg_size: 16 << 20,
        }),
    )
    .await;
    assert!(matches!(recv(&mut p).await, AcceptorMessage::Greeting(_)));
}

async fn http(addr: SocketAddr, req: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

async fn http_get(addr: SocketAddr, path: &str) -> String {
    http(
        addr,
        format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
    )
    .await
}

async fn http_post(addr: SocketAddr, path: &str, body: &str) -> String {
    http(
        addr,
        format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

/// The feeder copies exactly the committed WAL to a safekeeper: here a second
/// WAL service standing in for Neon's.
#[tokio::test]
async fn feeder_copies_committed_wal_to_a_safekeeper() {
    let (sk_pg, _, sk) = start().await;
    let (pg, _, _svc) = start_with(Some(FeederConfig {
        safekeeper: sk_pg.to_string(),
        retry: Duration::from_millis(50),
        poll: Duration::from_millis(5),
    }))
    .await;
    let payload: Vec<u8> = (0..3000u32).map(|i| (i % 253) as u8).collect();
    let _p = propose(pg, &payload).await;
    let end = Lsn(START + payload.len() as u64);
    let tl = loams_safekeeper::TimelineId::new(TENANT.parse().unwrap(), TIMELINE.parse().unwrap());
    let mut got = Vec::new();
    for _ in 0..400 {
        if let Some(st) = sk.store().load(&tl).await.unwrap()
            && st.flush_lsn == end
            && st.commit_lsn == end
        {
            got = sk
                .store()
                .read(&tl, Lsn(START), usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .flat_map(|(_, b)| b.to_vec())
                .collect();
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(got, payload);
}

#[tokio::test]
async fn auth_token_is_required_on_both_listeners() {
    let (pg, web, _svc) = start_cfg(None, Some("s3cret".into())).await;
    let options = format!("-c timeline_id={TIMELINE} tenant_id={TENANT}");
    let mut s = TcpStream::connect(pg).await.unwrap();
    let bad = client::startup(
        &mut s,
        &[("user", "u"), ("options", &options), ("password", "nope")],
    )
    .await;
    assert!(bad.is_err());
    let mut s = TcpStream::connect(pg).await.unwrap();
    client::startup(
        &mut s,
        &[("user", "u"), ("options", &options), ("password", "s3cret")],
    )
    .await
    .unwrap();
    assert!(
        http_get(web, "/v1/status")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let ok = http(
        web,
        "GET /v1/status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret\r\nConnection: close\r\n\r\n".into(),
    )
    .await;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
}
