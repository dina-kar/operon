//! D111's unauthenticated-listener warning (plan M1.6 Task 7 rule 14,
//! owner ruling O-M16-1): every listener bound to a non-loopback address is
//! flagged once, in bind order.

use std::net::SocketAddr;
use std::time::Duration;

use operon::{Server, ServerConfig};
use tempfile::TempDir;

fn config(dir: &TempDir, ip: [u8; 4]) -> ServerConfig {
    let any = SocketAddr::from((ip, 0));
    let mut config = ServerConfig::new(dir.path());
    config.listen = any;
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    config.flight_sql = Some(any);
    #[cfg(feature = "qdrant")]
    {
        config.qdrant = Some(operon_qdrant::QdrantConfig {
            rest_listen: any,
            grpc_listen: any,
            ..operon_qdrant::QdrantConfig::default()
        });
    }
    #[cfg(feature = "es")]
    {
        config.es = Some(operon_es::EsConfig {
            listen: any,
            ..operon_es::EsConfig::default()
        });
    }
    #[cfg(feature = "mcp")]
    {
        config.mcp = Some(operon::McpServerConfig {
            listen: any,
            mcp: operon_mcp::McpConfig::default(),
        });
    }
    config
}

#[tokio::test]
async fn every_exposed_listener_is_flagged() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, [0, 0, 0, 0])).await.unwrap();
    let mut expected = vec!["native"];
    if cfg!(feature = "flight") {
        expected.push("flight-sql");
    }
    if cfg!(feature = "qdrant") {
        expected.extend(["qdrant-rest", "qdrant-grpc"]);
    }
    if cfg!(feature = "es") {
        expected.push("es");
    }
    if cfg!(feature = "mcp") {
        expected.push("mcp");
    }
    assert_eq!(server.exposed_listeners(), expected);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn loopback_listeners_are_not_flagged() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, [127, 0, 0, 1])).await.unwrap();
    assert!(server.exposed_listeners().is_empty());
    server.shutdown().await.unwrap();
}
