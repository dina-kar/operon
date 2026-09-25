//! The change watch, readiness, and linearizable reads across handles.

use std::time::Duration;

use operon_common::meta::{Consistency, Pointer};

use super::{cas, namespace};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

pub async fn changes_wake_a_waiter_armed_before_a_write(backend: &dyn Backend) {
    let db = backend.start().await;
    for (i, watcher) in db.clients.iter().enumerate() {
        // Armed before the write, so the write cannot slip between the
        // subscription and the wait.
        let mut changes = watcher.watch_changes();
        let writer = db.clients[0].clone();
        let name = format!("changes-{i}");
        let write = tokio::spawn(async move { writer.create_namespace(&name).await });
        let woke = tokio::time::timeout(Duration::from_secs(20), changes.changed()).await;
        assert!(
            matches!(woke, Ok(Ok(()))),
            "handle {i}: the watch did not wake: {woke:?}"
        );
        write.await.expect("join").expect("create namespace");
    }
    // A write that completed before the watch was armed may still wake it
    // (spurious wake-ups are allowed), so there is nothing to assert about
    // a watch armed after the last write.
}

pub async fn is_ready_after_start(backend: &dyn Backend) {
    let db = backend.start().await;
    assert!(!db.is_empty());
    for (i, meta) in db.clients.iter().enumerate() {
        assert!(meta.is_ready(), "handle {i} is not ready");
    }
    // Ready means it serves writes, through every handle.
    for (i, meta) in db.clients.iter().enumerate() {
        namespace(&**meta, &format!("ready-{i}")).await;
    }
    assert_eq!(
        db.first().namespaces(L).await.expect("read").len(),
        db.len()
    );
}

pub async fn a_linearizable_read_through_another_client_sees_an_acknowledged_write(
    backend: &dyn Backend,
) {
    let db = backend.start().await;
    let ns = namespace(db.first(), "linearizable-read").await;
    let key = "linearizable-read/pointer";
    let mut version = None;
    for i in 0..10 {
        let writer = &db.clients[i % db.len()];
        let reader = &db.clients[(i + 1) % db.len()];
        let value = format!("v{i}");
        let written = writer
            .cas_pointer(cas(ns, key, version, &value))
            .await
            .into_result()
            .expect("cas");
        version = Some(written);
        assert_eq!(
            reader.pointer(L, ns, key).await.expect("read"),
            Some(Pointer {
                version: written,
                value,
            }),
            "iteration {i}"
        );
        let clock = writer.clock_ms(L).await.expect("clock");
        assert!(reader.clock_ms(L).await.expect("clock") >= clock);
    }
}
