//! Keyspace bootstrap, connect checks, root isolation and the TSO clock (R1
//! plan Task 1). The cluster tests skip unless `LOAMS_TEST_PD` is set; the
//! `ensure_keyspace_*_against_a_fake_pd` tests run everywhere.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use loams_tikv::testing::{self, TEST_META};
use loams_tikv::{Tikv, TikvError, TimestampExt, TxnOptions, ensure_keyspace};

// ---- against the test cluster ----

#[tokio::test]
async fn connect_to_missing_keyspace_names_it() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let name = format!("loams_test_absent_{:016x}", rand_u64());
    let mut config = cluster.config(TEST_META);
    config.keyspace = name.clone();
    let err = Tikv::connect(config).await.expect_err("no such keyspace");
    match &err {
        TikvError::KeyspaceMissing { name: missing } => assert_eq!(missing, &name),
        other => panic!("expected KeyspaceMissing, got {other:?}"),
    }
    assert!(err.to_string().contains(&name), "{err}");
}

#[tokio::test]
async fn ensure_keyspace_is_idempotent() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let first = ensure_keyspace(&cluster.pd_http, TEST_META).await.unwrap();
    let second = ensure_keyspace(&cluster.pd_http, TEST_META).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first.name, TEST_META);
    assert_eq!(first.state, "ENABLED");
    let tikv = cluster.connect(TEST_META).await;
    assert_eq!(tikv.keyspace_meta().await.unwrap(), first);
}

#[tokio::test]
async fn two_roots_never_see_each_other() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let a = cluster.connect(TEST_META).await;
    let b = cluster.connect(TEST_META).await;
    assert_ne!(a.root(), b.root());
    for (tikv, tag) in [(&a, b"a"), (&b, b"b")] {
        tikv.run(TxnOptions::new("seed"), move |txn| {
            Box::pin(async move {
                for i in 0..3u8 {
                    txn.put(&[b'k', i], tag.to_vec()).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    for (tikv, tag) in [(&a, b"a"), (&b, b"b")] {
        let mut snap = tikv.snapshot(tikv.now().await.unwrap()).await.unwrap();
        let pairs = snap.scan(b"", None, 100).await.unwrap();
        assert_eq!(pairs.len(), 3, "each root sees exactly its own three keys");
        for (i, (key, value)) in pairs.into_iter().enumerate() {
            assert_eq!(key, vec![b'k', u8::try_from(i).unwrap()]);
            assert_eq!(value, tag.to_vec());
        }
    }
}

#[tokio::test]
async fn now_is_monotonic() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let mut previous = tikv.now().await.unwrap().version();
    for _ in 0..1_000 {
        let next = tikv.now().await.unwrap().version();
        assert!(next > previous, "TSO went from {previous} to {next}");
        previous = next;
    }
    let (latest, _) = tikv.latest_timestamp().expect("a timestamp was obtained");
    assert_eq!(latest.version(), previous);
}

#[tokio::test]
async fn physical_ms_is_close_to_wall_clock() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let ts = tikv.now().await.unwrap();
    let physical = Tikv::physical_ms(&ts);
    let wall = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    assert!(
        physical.abs_diff(wall) < 1_000,
        "TSO physical {physical} ms vs wall clock {wall} ms"
    );
}

#[tokio::test]
async fn client_without_keyspace_is_refused_with_a_hint() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let mut config = cluster.config(TEST_META);
    config.keyspace = String::new();
    let err = Tikv::connect(config)
        .await
        .expect_err("API v2 needs a keyspace");
    match &err {
        TikvError::ApiVersion { hint } => {
            assert!(hint.contains("TikvConfig.keyspace"), "{hint}");
            assert!(hint.contains("api-version = 2"), "{hint}");
            assert!(
                hint.contains("InvalidKeyMode"),
                "TiKV refused the key: {hint}"
            );
        }
        other => panic!("expected ApiVersion, got {other:?}"),
    }
}

// ---- against a fake PD HTTP API (no cluster needed) ----

#[derive(Default)]
struct FakePd {
    keyspaces: Mutex<BTreeMap<String, u32>>,
    /// A POST inserts the keyspace but answers "already exists", as when a
    /// concurrent caller created it between our GET and POST.
    race_on_create: bool,
    /// Every GET fails with an unrelated 500.
    broken: bool,
    posts: Mutex<u32>,
}

fn meta(name: &str, id: u32) -> serde_json::Value {
    serde_json::json!({
        "id": id, "name": name, "state": "ENABLED",
        "created_at": 1_790_000_000, "state_changed_at": 1_790_000_000
    })
}

fn pd_500(body: &str) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("\"{body}\"")).into_response()
}

async fn fake_get(State(pd): State<Arc<FakePd>>, Path(name): Path<String>) -> Response {
    if pd.broken {
        return pd_500("etcd is unavailable");
    }
    match pd.keyspaces.lock().expect("fake PD").get(&name) {
        Some(id) => Json(meta(&name, *id)).into_response(),
        None => pd_500("keyspace does not exist"),
    }
}

async fn fake_post(State(pd): State<Arc<FakePd>>, Json(req): Json<serde_json::Value>) -> Response {
    *pd.posts.lock().expect("fake PD") += 1;
    let name = req["name"].as_str().expect("fake PD").to_string();
    let mut keyspaces = pd.keyspaces.lock().expect("fake PD");
    if keyspaces.contains_key(&name) {
        return pd_500("keyspace already exists");
    }
    let id = u32::try_from(keyspaces.len()).expect("fake PD") + 1;
    keyspaces.insert(name.clone(), id);
    if pd.race_on_create {
        return pd_500("keyspace already exists");
    }
    Json(meta(&name, id)).into_response()
}

async fn serve(pd: Arc<FakePd>) -> String {
    let app = Router::new()
        .route("/pd/api/v2/keyspaces/{name}", get(fake_get))
        .route("/pd/api/v2/keyspaces", post(fake_post))
        .with_state(pd);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake PD");
    let addr = listener.local_addr().expect("fake PD");
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

#[tokio::test]
async fn ensure_keyspace_creates_once_against_a_fake_pd() {
    let pd = Arc::new(FakePd::default());
    let url = serve(pd.clone()).await;
    let created = ensure_keyspace(&url, "loams_x").await.unwrap();
    let again = ensure_keyspace(&url, "loams_x").await.unwrap();
    assert_eq!(created, again);
    assert_eq!(created.id, 1);
    assert_eq!(*pd.posts.lock().unwrap(), 1, "the second call only reads");
}

#[tokio::test]
async fn ensure_keyspace_survives_a_concurrent_create_against_a_fake_pd() {
    let pd = Arc::new(FakePd {
        race_on_create: true,
        ..FakePd::default()
    });
    let url = serve(pd.clone()).await;
    let meta = ensure_keyspace(&url, "loams_y").await.unwrap();
    assert_eq!(meta.name, "loams_y");
    assert_eq!(*pd.posts.lock().unwrap(), 1);
}

#[tokio::test]
async fn ensure_keyspace_surfaces_other_errors_against_a_fake_pd() {
    let pd = Arc::new(FakePd {
        broken: true,
        ..FakePd::default()
    });
    let url = serve(pd).await;
    match ensure_keyspace(&url, "loams_z").await {
        Err(TikvError::Pd {
            status: 500, body, ..
        }) => assert!(body.contains("etcd")),
        other => panic!("expected a PD error, got {other:?}"),
    }
    assert!(matches!(
        ensure_keyspace(&url, "no/slashes").await,
        Err(TikvError::Config(_))
    ));
}

// ---- helpers ----

fn rand_u64() -> u64 {
    let root = testing::random_root();
    u64::from_le_bytes(root[..8].try_into().expect("8 bytes"))
}
