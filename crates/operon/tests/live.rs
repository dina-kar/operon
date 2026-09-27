//! `operon dev` with Loam Live (R1 plan Task 12): the sync API listens on
//! loopback beside the HTTP API, and its TiKV handle is swept by the
//! metastore's GC loop when both are on one cluster, else by its own loop.
//! Each test uses a random root in the test keyspaces and skips without
//! `OPERON_TEST_PD`.
#![cfg(feature = "live")]

use std::net::SocketAddr;
use std::time::Duration;

use operon::{MetaBackend, Server, ServerConfig};
use operon_tikv::testing::{self, TEST_LIVE, TEST_META, TestCluster};
use tempfile::TempDir;

fn config(dir: &TempDir, cluster: &TestCluster) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.log.flush_interval = Duration::from_millis(20);
    let mut live = operon_live::LiveConfig::with_tikv("t12", cluster.config(TEST_LIVE));
    live.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.live = Some(live);
    config
}

async fn serves(server: &Server) {
    let addr = server.live_addr().expect("Live runs");
    assert!(addr.ip().is_loopback());
    tokio::net::TcpStream::connect(addr)
        .await
        .expect("the Live listener accepts");
    assert_eq!(server.live_stats().expect("stats").missed_invalidations, 0);
}

/// On the openraft metastore, Live runs its own cluster GC loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_serves_live_beside_the_http_api() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let dir = TempDir::new().expect("a temp dir");
    let server = Server::start(config(&dir, &cluster)).await.expect("starts");
    serves(&server).await;
    assert_eq!(server.live_swept_by_metastore_gc(), Some(false));
    server.shutdown().await.expect("stops");
}

/// On a TiKV metastore of the same cluster, the metastore's GC loop sweeps
/// Live's handle (Carry T6: `GcConfig.sweep`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_on_tikv_sweeps_live_with_the_metastore_gc_loop() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let root: String = testing::random_root()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let dir = TempDir::new().expect("a temp dir");
    let mut config = config(&dir, &cluster);
    config.meta = MetaBackend::parse(&format!(
        "tikv://{}/{TEST_META}?root={root}",
        cluster.pd.join(",")
    ))
    .expect("url");
    let server = Server::start(config).await.expect("starts");
    serves(&server).await;
    assert_eq!(server.live_swept_by_metastore_gc(), Some(true));
    server.shutdown().await.expect("stops");
}
