//! Native TiKV durable execution across two Resonate servers and a restart.
//! Runs when the standard TiKV playground is named by `LOAMS_TEST_PD`.
#![cfg(feature = "tikv")]

use std::net::TcpListener;

use loams_durable::{DurableConfig, DurableServer, DurableStore};
use loams_tikv::testing::{TEST_META, cluster};
use serde_json::json;

fn config(store: &DurableStore) -> DurableConfig {
    let probe = TcpListener::bind("127.0.0.1:0").expect("free loopback port");
    let mut config = DurableConfig::new(store.clone());
    config.listen = probe.local_addr().expect("local address");
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn promise_state_survives_two_servers_and_restart() {
    let Some(cluster) = cluster().await else {
        return;
    };
    let tikv = cluster.config(TEST_META);
    let store = DurableStore::Tikv {
        pd: tikv.pd,
        keyspace: tikv.keyspace,
        root: tikv.root,
    };
    let first = DurableServer::start(config(&store), "durable-a")
        .await
        .expect("first server");
    let second = DurableServer::start(config(&store), "durable-b")
        .await
        .expect("second server");
    assert!(first.ready().await && second.ready().await);

    let created = first
        .process(json!({
            "kind": "promise.create",
            "data": {
                "id": "native-tikv-promise",
                "timeoutAt": i64::MAX / 2,
                "param": {},
                "tags": {}
            }
        }))
        .await
        .expect("promise created");
    assert_eq!(created["data"]["promise"]["state"], "pending");
    second
        .process(json!({
            "kind": "promise.settle",
            "data": {
                "id": "native-tikv-promise",
                "state": "resolved",
                "value": { "data": "YQ==" }
            }
        }))
        .await
        .expect("promise settled on another server");
    first.stop().await;
    second.stop().await;

    let restarted = DurableServer::start(config(&store), "durable-c")
        .await
        .expect("restarted server");
    let got = restarted
        .process(json!({
            "kind": "promise.get",
            "data": { "id": "native-tikv-promise" }
        }))
        .await
        .expect("read after restart");
    assert_eq!(got["data"]["promise"]["state"], "resolved");
    restarted.stop().await;
}
