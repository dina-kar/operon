//! R1 plan Task 12: the connect-rust sync service, through the generated
//! `LiveServiceClient` over HTTP/1.1 and HTTP/2 (design §20 §7). The
//! loopback refusal runs without a cluster; the rest need TiKV and skip
//! without `OPERON_TEST_PD`.

use std::net::SocketAddr;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use operon_live::pb::__buffa::oneof::query_set_change::Change;
use operon_live::pb::__buffa::oneof::query_update::Update;
use operon_live::pb::__buffa::oneof::watch_request::Start;
use operon_live::service::{ERROR_DETAIL_TYPE, connect_error, error_detail};
use operon_live::session::{ClientState, QueryResult, SESSION_HEADER, SessionConfig, Version};
use operon_live::system::{INSERT, QUERY};
use operon_live::{LiveConfig, LiveError, LiveHandle, LiveServer, LiveValue, check_listen, pb};
use operon_tikv::testing::{self, TEST_LIVE};
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Watch = connectrpc::client::ServerStream<
    <HttpClient as connectrpc::client::ClientTransport>::ResponseBody,
    pb::__buffa::view::TransitionView<'static>,
>;

// ---- without a cluster ----

/// Semantics 7 and the loopback rule (D111): only loopback addresses pass.
#[test]
fn only_loopback_addresses_pass_the_listen_check() {
    for ok in [
        "127.0.0.1:7710",
        "127.1.2.3:0",
        "[::1]:0",
        "[::ffff:127.0.0.1]:1",
    ] {
        let addr: SocketAddr = ok.parse().expect("an address");
        check_listen(addr).unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    for bad in [
        "0.0.0.0:0",
        "192.168.1.10:7710",
        "[::]:0",
        "10.0.0.1:1",
        "[fe80::1]:0",
    ] {
        let addr: SocketAddr = bad.parse().expect("an address");
        let err = check_listen(addr).expect_err(bad);
        assert!(
            matches!(err, LiveError::NotLoopback(a) if a == addr),
            "{bad}: {err}"
        );
        assert!(
            err.to_string().contains("is not a loopback address")
                && err.to_string().contains("D111"),
            "{err}"
        );
    }
}

/// Owner ruling on row T14-12: every call error carries the `LiveError` as
/// a typed Connect detail with the exact wire code, so a client never
/// parses the message (`resource_exhausted` is both a limit and
/// `FUNCTION_OUT_OF_MEMORY`).
#[test]
fn every_call_error_carries_its_live_error_detail() {
    let cases = [
        (
            LiveError::FunctionOutOfMemory("64 MiB".into()),
            ErrorCode::ResourceExhausted,
        ),
        (
            LiveError::LimitExceeded {
                limit: "max_written_docs",
                message: "too many".into(),
            },
            ErrorCode::ResourceExhausted,
        ),
        (
            LiveError::FunctionTimeout("1 s".into()),
            ErrorCode::DeadlineExceeded,
        ),
        (LiveError::FunctionError("boom".into()), ErrorCode::Unknown),
        (LiveError::Busy("deploy".into()), ErrorCode::Unavailable),
        (
            LiveError::InvalidArgument("bad".into()),
            ErrorCode::InvalidArgument,
        ),
        (LiveError::NotFound("gone".into()), ErrorCode::NotFound),
        (LiveError::Internal("bug".into()), ErrorCode::Internal),
    ];
    for (e, code) in cases {
        let c = connect_error(&e);
        assert_eq!(c.code, code, "{e}");
        assert_eq!(c.details.len(), 1, "{e}");
        assert_eq!(c.details[0].type_url, ERROR_DETAIL_TYPE);
        let detail = error_detail(&c).unwrap_or_else(|| panic!("{e}: a decodable detail"));
        assert_eq!(detail.code.as_known(), Some(e.code()), "{e}");
        assert_eq!(detail.message, e.to_string());
    }
    // An error without the detail (another server) has none.
    assert!(error_detail(&ConnectError::internal("x")).is_none());
}

/// Semantics 7: `0.0.0.0:0` and a LAN address fail startup before anything
/// connects; `127.0.0.1:0` and `[::1]:0` start (on a cluster).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_loopback_bind_is_refused() {
    for bad in ["0.0.0.0:0", "192.168.1.10:0"] {
        let mut config = LiveConfig::with_tikv(
            "t12",
            operon_tikv::TikvConfig::new(vec!["127.0.0.1:1".into()], TEST_LIVE),
        );
        config.listen = bad.parse().expect("an address");
        let err = LiveServer::start(config, CancellationToken::new())
            .await
            .expect_err(bad);
        assert!(matches!(err, LiveError::NotLoopback(_)), "{bad}: {err}");
    }
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    for ok in ["127.0.0.1:0", "[::1]:0"] {
        let mut config = LiveConfig::with_tikv("t12", cluster.config(TEST_LIVE));
        config.listen = ok.parse().expect("an address");
        let handle = LiveServer::start(config, CancellationToken::new())
            .await
            .unwrap_or_else(|e| panic!("{ok}: {e}"));
        assert!(handle.addr.ip().is_loopback());
        assert_ne!(handle.addr.port(), 0);
        handle.stop().await;
    }
}

// ---- on TiKV ----

async fn server(session: SessionConfig) -> Option<LiveHandle> {
    let cluster = testing::cluster().await?;
    let mut config = LiveConfig::with_tikv("t12", cluster.config(TEST_LIVE));
    config.listen = "127.0.0.1:0".parse().expect("an address");
    config.session = session;
    Some(
        LiveServer::start(config, CancellationToken::new())
            .await
            .expect("the server starts"),
    )
}

fn client(handle: &LiveHandle, http2: bool) -> Client {
    let transport = if http2 {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };
    let uri = format!("http://{}", handle.addr).parse().expect("a uri");
    pb::LiveServiceClient::new(transport, ClientConfig::new(uri))
}

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

fn set(version: u64, queries: Vec<pb::QuerySpec>) -> pb::QuerySet {
    pb::QuerySet {
        version,
        queries,
        ..Default::default()
    }
}

async fn watch(c: &Client, start: Start) -> Watch {
    c.watch(pb::WatchRequest {
        start: Some(start),
        ..Default::default()
    })
    .await
    .expect("Watch opens")
}

async fn next(w: &mut Watch) -> pb::Transition {
    tokio::time::timeout(Duration::from_secs(20), w.message())
        .await
        .expect("a Transition within 20 s")
        .expect("the stream")
        .expect("not the end")
        .to_owned_message()
}

/// Receives Transitions (applying each to `client`) until `done` holds.
async fn until(
    w: &mut Watch,
    client: &mut ClientState,
    mut done: impl FnMut(&ClientState, &pb::Transition) -> bool,
) -> pb::Transition {
    loop {
        let t = next(w).await;
        client
            .apply(&t)
            .expect("each Transition starts at the last one's end");
        if done(client, &t) {
            return t;
        }
    }
}

async fn insert(c: &Client, table: &str, n: i64, session: Option<&str>) -> u64 {
    let req = pb::MutateRequest {
        function: INSERT.into(),
        args: MessageField::some(
            obj(&[
                ("table", LiveValue::Str(table.into())),
                ("fields", obj(&[("n", LiveValue::I64(n))])),
            ])
            .to_proto(),
        ),
        ..Default::default()
    };
    let mut options = CallOptions::default();
    if let Some(id) = session {
        options = options.with_header(SESSION_HEADER, id);
    }
    c.mutate_with_options(req, options)
        .await
        .expect("Mutate commits")
        .into_owned()
        .commit_ts
}

fn docs(client: &ClientState, q: u32) -> usize {
    match client.results.get(&q) {
        Some(QueryResult::Value(LiveValue::Array(docs))) => docs.len(),
        other => panic!("query {q}: {other:?}"),
    }
}

fn end(t: &pb::Transition) -> Version {
    Version::from_proto(t.end.as_option())
}

/// Semantics 1 and 6, over HTTP/1.1 and HTTP/2: the first Transition starts
/// at the zero version with every result; a Mutate's write reaches the
/// session in a Transition at or after its commit timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_receives_update_after_mutate() {
    let Some(handle) = server(SessionConfig::default()).await else {
        return;
    };
    for http2 in [false, true] {
        let c = client(&handle, http2);
        let table = if http2 { "msgs2" } else { "msgs1" };
        let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(7, table)])))).await;
        let first = next(&mut w).await;
        assert_eq!(
            Version::from_proto(first.start.as_option()),
            Version::default()
        );
        assert_eq!(end(&first).query_set, 1);
        let mut state = ClientState::new();
        state.apply(&first).expect("from the zero version");
        assert_eq!(docs(&state, 7), 0);
        let commit_ts = insert(&c, table, 1, None).await;
        let t = until(&mut w, &mut state, |s, _| docs(s, 7) == 1).await;
        assert!(end(&t).ts >= commit_ts, "http2 {http2}");
        // A one-shot Query at the latest tick agrees.
        let q = c
            .query(pb::QueryRequest {
                function: QUERY.into(),
                args: spec(0, table).args,
                ..Default::default()
            })
            .await
            .expect("Query")
            .into_owned();
        let result = LiveValue::from_proto(q.result.into_option().expect("a result")).expect("ok");
        assert!(matches!(result, LiveValue::Array(d) if d.len() == 1));
    }
    handle.stop().await;
}

/// Semantics 1 and §20 §8.2 step 3: two sessions watching one query share
/// one subscription, and one write reaches both at the same tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_share_one_tick() {
    let Some(handle) = server(SessionConfig::default()).await else {
        return;
    };
    let c = client(&handle, true);
    let mut a = watch(
        &c,
        Start::Initial(Box::new(set(1, vec![spec(1, "shared")]))),
    )
    .await;
    let mut b = watch(
        &c,
        Start::Initial(Box::new(set(1, vec![spec(9, "shared")]))),
    )
    .await;
    let (mut sa, mut sb) = (ClientState::new(), ClientState::new());
    sa.apply(&next(&mut a).await).expect("a");
    sb.apply(&next(&mut b).await).expect("b");
    assert_eq!(handle.stats().subscriptions, 1, "one shared subscription");
    assert_eq!(handle.sessions(), 2);
    insert(&c, "shared", 1, None).await;
    let ta = until(&mut a, &mut sa, |s, _| docs(s, 1) == 1).await;
    let tb = until(&mut b, &mut sb, |s, _| docs(s, 9) == 1).await;
    assert_eq!(end(&ta).ts, end(&tb).ts, "one tick for both sessions");
    drop(a);
    drop(b);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while handle.sessions() > 0 || handle.stats().subscriptions > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sessions end when clients go"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await;
}

/// Semantics 2: `ModifyQuerySet` adds and removes queries from the
/// session's current query-set version (else FAILED_PRECONDITION), and the
/// next Transition reflects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn modify_query_set_adds_and_removes() {
    let Some(handle) = server(SessionConfig::default()).await else {
        return;
    };
    let c = client(&handle, false);
    let second_ts = insert(&c, "second", 1, None).await;
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(1, "first")])))).await;
    let first = next(&mut w).await;
    let mut state = ClientState::new();
    state.apply(&first).expect("first");
    let session = first.session_id.clone();
    let modify = |base: u64, new: u64, changes: Vec<Change>| pb::ModifyQuerySetRequest {
        session_id: session.clone(),
        base_version: base,
        new_version: new,
        changes: changes
            .into_iter()
            .map(|c| pb::QuerySetChange {
                change: Some(c),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    c.modify_query_set(modify(1, 2, vec![Change::Add(Box::new(spec(2, "second")))]))
        .await
        .expect("an add");
    let t = next(&mut w).await;
    state.apply(&t).expect("the add");
    assert_eq!(end(&t).query_set, 2);
    assert!(
        t.updates.iter().any(|u| u.query_id == 2),
        "the added query's result"
    );
    // Results are those of the session's tick, which reads `tick_read_lag` back: the
    // insert shows once the tick passes its commit.
    if end(&t).ts < second_ts {
        until(&mut w, &mut state, |s, _| docs(s, 2) == 1).await;
    }
    assert_eq!(docs(&state, 2), 1);
    assert_eq!(docs(&state, 1), 0);
    let stale = c
        .modify_query_set(modify(1, 3, vec![Change::Remove(1)]))
        .await
        .expect_err("a stale base version");
    assert_eq!(stale.code, ErrorCode::FailedPrecondition, "{stale:?}");
    c.modify_query_set(modify(2, 3, vec![Change::Remove(1)]))
        .await
        .expect("a remove");
    let t = next(&mut w).await;
    assert!(
        t.updates
            .iter()
            .any(|u| u.query_id == 1 && matches!(u.update, Some(Update::Removed(_))))
    );
    state.apply(&t).expect("the remove");
    assert_eq!(end(&t).query_set, 3);
    assert_eq!(state.results.keys().copied().collect::<Vec<_>>(), vec![2]);
    // The removed query no longer follows writes; the kept one does.
    insert(&c, "first", 1, None).await;
    insert(&c, "second", 2, None).await;
    until(&mut w, &mut state, |s, _| docs(s, 2) == 2).await;
    assert!(!state.results.contains_key(&1));
    let unknown = c
        .modify_query_set(pb::ModifyQuerySetRequest {
            session_id: "1-nope".into(),
            ..Default::default()
        })
        .await
        .expect_err("no such session");
    assert_eq!(unknown.code, ErrorCode::NotFound);
    handle.stop().await;
}

/// Semantics 4: empty Transitions arrive every `heartbeat`, and a session
/// with a pending mutation gets ts-only Transitions up to its commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_arrive() {
    let Some(handle) = server(SessionConfig {
        heartbeat: Duration::from_millis(300),
        ts_only_interval: Duration::from_millis(200),
        ..SessionConfig::default()
    })
    .await
    else {
        return;
    };
    let c = client(&handle, true);
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(1, "quiet")])))).await;
    let mut state = ClientState::new();
    let first = next(&mut w).await;
    state.apply(&first).expect("first");
    let started = std::time::Instant::now();
    let beat = next(&mut w).await;
    state.apply(&beat).expect("a heartbeat");
    assert!(beat.updates.is_empty());
    assert_eq!(beat.start, beat.end, "a heartbeat keeps the version");
    assert!(started.elapsed() >= Duration::from_millis(200));
    // A mutation of this session on another table: ts-only Transitions
    // carry the session's ts to its commit, with no update.
    let commit_ts = insert(&c, "elsewhere", 1, Some(&first.session_id)).await;
    let t = until(&mut w, &mut state, |_, t| end(t).ts >= commit_ts).await;
    assert!(t.updates.is_empty(), "{t:?}");
    handle.stop().await;
}

/// Semantics 5 (reconnect): a client that lost its stream resumes from its
/// last version and converges to the writes made while it was away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_resumes_after_reconnect() {
    let Some(handle) = server(SessionConfig::default()).await else {
        return;
    };
    let c = client(&handle, false);
    let queries = vec![spec(1, "resumed"), spec(2, "resumed_other")];
    let mut w = watch(&c, Start::Initial(Box::new(set(4, queries.clone())))).await;
    let mut state = ClientState::new();
    state.apply(&next(&mut w).await).expect("first");
    let last = state.version;
    drop(w);
    let commit_ts = insert(&c, "resumed", 1, None).await;
    let mut w = watch(
        &c,
        Start::Resume(Box::new(pb::Resume {
            last_version: MessageField::some(last.to_proto()),
            query_set: MessageField::some(set(4, queries)),
            ..Default::default()
        })),
    )
    .await;
    let first = next(&mut w).await;
    assert_eq!(Version::from_proto(first.start.as_option()), last);
    assert!(end(&first).ts >= last.ts);
    assert_eq!(first.updates.len(), 2, "full results");
    state.apply(&first).expect("applies at the last version");
    let t = until(&mut w, &mut state, |s, _| docs(s, 1) == 1).await;
    assert!(end(&t).ts >= commit_ts);
    assert_eq!(docs(&state, 2), 0);
    handle.stop().await;
}

/// Per-query errors stay inside the stream; call errors use Connect codes;
/// `Deploy` without a function engine is refused (Task 13).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_map_to_connect_codes() {
    let Some(handle) = server(SessionConfig::default()).await else {
        return;
    };
    let c = client(&handle, false);
    let unknown = pb::QuerySpec {
        query_id: 3,
        function: "app:missing".into(),
        ..Default::default()
    };
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![unknown])))).await;
    let mut state = ClientState::new();
    state.apply(&next(&mut w).await).expect("first");
    assert!(matches!(
        &state.results[&3],
        QueryResult::Error(e) if e.code.as_known() == Some(pb::ErrorCode::ERROR_CODE_NOT_FOUND)
    ));
    let e: ConnectError = c
        .mutate(pb::MutateRequest {
            function: QUERY.into(),
            ..Default::default()
        })
        .await
        .expect_err("a query is not a mutation");
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    // Row T14-12: the detail survives the wire.
    assert_eq!(
        error_detail(&e).map(|d| d.code.as_known()),
        Some(Some(pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT)),
        "{e:?}"
    );
    let e = c
        .deploy(pb::DeployRequest::default())
        .await
        .expect_err("no function engine");
    assert_eq!(e.code, ErrorCode::FailedPrecondition);
    let dup = c
        .watch(pb::WatchRequest {
            start: Some(Start::Initial(Box::new(set(
                1,
                vec![spec(1, "a"), spec(1, "b")],
            )))),
            ..Default::default()
        })
        .await;
    let err = match dup {
        Err(e) => e,
        Ok(mut s) => s.message().await.expect_err("a duplicate query id"),
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    handle.stop().await;
}
