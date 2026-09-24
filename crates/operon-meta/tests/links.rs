//! The link catalog, on `MetaState` directly and through a client.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use operon_common::{NamespaceId, StreamId};
use operon_meta::{
    ApplyError, Command, LinkId, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode,
    MetaState, Reply, Router, SystemClock, TargetRef, WalClass,
};
use operon_store::Store;
use tempfile::TempDir;

fn counter(name: &str) -> TargetRef {
    TargetRef {
        kind: "counter".to_string(),
        name: name.to_string(),
    }
}

fn create_link(
    state: &mut MetaState,
    namespace: u64,
    name: &str,
    source: u64,
) -> Result<Reply, ApplyError> {
    state.apply(Command::CreateLink {
        namespace: NamespaceId(namespace),
        name: name.to_string(),
        source: StreamId(source),
        target: counter(name),
        options: BTreeMap::new(),
    })
}

/// Namespaces 1 and 2, with stream 1 in namespace 1 and stream 2 in 2.
fn state() -> MetaState {
    let mut state = MetaState::default();
    for (ns, stream) in [("acme", "events"), ("globex", "orders")] {
        let Ok(Reply::NamespaceCreated(id)) = state.apply(Command::CreateNamespace {
            name: ns.to_string(),
        }) else {
            panic!("namespace");
        };
        state
            .apply(Command::CreateStream {
                namespace: id,
                name: stream.to_string(),
                partitions: 2,
                class: WalClass::Standard,
                retention: operon_meta::Retention::default(),
            })
            .expect("stream");
    }
    state
}

#[test]
fn links_get_ids_and_names_are_unique_per_namespace() {
    let mut state = state();
    assert_eq!(
        create_link(&mut state, 1, "counts", 1),
        Ok(Reply::LinkCreated(LinkId(1)))
    );
    // A retry after a lost acknowledgement recovers the id.
    let before = state.clone();
    assert_eq!(
        create_link(&mut state, 1, "counts", 1),
        Err(ApplyError::LinkExists(LinkId(1)))
    );
    assert_eq!(state, before);
    // The same name in another namespace is another link.
    assert_eq!(
        create_link(&mut state, 2, "counts", 2),
        Ok(Reply::LinkCreated(LinkId(2)))
    );
    let link = state.link(LinkId(1)).unwrap();
    assert_eq!(
        (link.namespace, link.name.as_str(), link.source),
        (NamespaceId(1), "counts", StreamId(1))
    );
    assert_eq!(link.target, counter("counts"));
    assert_eq!(
        state.link_by_name(NamespaceId(2), "counts").unwrap().id,
        LinkId(2)
    );
    let in_acme: Vec<LinkId> = state.links(NamespaceId(1)).map(|l| l.id).collect();
    assert_eq!(in_acme, [LinkId(1)]);
    assert_eq!(state.all_links().count(), 2);
}

#[test]
fn invalid_links_are_rejected_and_change_nothing() {
    let mut state = state();
    let before = state.clone();
    assert_eq!(
        create_link(&mut state, 9, "counts", 1),
        Err(ApplyError::NamespaceNotFound(NamespaceId(9)))
    );
    assert_eq!(
        create_link(&mut state, 1, "counts", 9),
        Err(ApplyError::StreamNotFound(StreamId(9)))
    );
    // The source must be in the link's namespace.
    assert!(matches!(
        create_link(&mut state, 1, "counts", 2),
        Err(ApplyError::InvalidArgument(_))
    ));
    assert!(matches!(
        create_link(&mut state, 1, "bad/name", 1),
        Err(ApplyError::InvalidArgument(_))
    ));
    let bad_target = state.apply(Command::CreateLink {
        namespace: NamespaceId(1),
        name: "counts".to_string(),
        source: StreamId(1),
        target: TargetRef {
            kind: String::new(),
            name: "counts".to_string(),
        },
        options: BTreeMap::new(),
    });
    assert!(matches!(bad_target, Err(ApplyError::InvalidArgument(_))));
    assert_eq!(state, before);
}

#[tokio::test]
async fn a_retried_create_link_through_the_client_returns_the_same_id() {
    let dir = TempDir::new().unwrap();
    let node = MetaNode::start(
        MetaConfig::new(1, dir.path(), Store::in_memory()),
        &Router::new(),
    )
    .await
    .unwrap();
    node.initialize([1]).await.unwrap();
    node.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let client = MetaClient::new(
        node.clone(),
        vec![],
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    let ns = client.create_namespace("acme").await.unwrap();
    let stream = client
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    client.inject_lost_ack();
    let err = client
        .create_link(ns, "counts", stream, counter("counts"), BTreeMap::new())
        .await
        .unwrap_err();
    // The first attempt applied; its retry reports the id it created.
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::LinkExists(LinkId(1)))),
        "{err:?}"
    );
    node.shutdown().await.unwrap();
}
