//! Catalog: namespaces, streams, links.

use std::collections::BTreeMap;

use operon_common::meta::{
    ApplyError, Consistency, Link, MAX_NAME_LEN, MAX_PARTITIONS, Namespace, PartitionBounds,
    Retention, StreamState, TargetRef, WalClass,
};
use operon_common::{NamespaceId, StreamId};

use super::{chunk, commit, namespace, rejected, schema, stream};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

fn counter(name: &str) -> TargetRef {
    TargetRef {
        kind: "counter".to_string(),
        name: name.to_string(),
    }
}

pub async fn namespace_create_then_lookup(backend: &dyn Backend) {
    let db = backend.start().await;
    let id = namespace(db.first(), "catalog-lookup").await;
    let expected = Namespace {
        id,
        name: "catalog-lookup".to_string(),
    };
    for meta in &db.clients {
        assert_eq!(
            meta.namespace_by_name(L, "catalog-lookup")
                .await
                .expect("read"),
            Some(expected.clone())
        );
        assert_eq!(
            meta.namespaces(L).await.expect("read"),
            vec![expected.clone()]
        );
        assert_eq!(
            meta.namespace_by_name(L, "catalog-missing")
                .await
                .expect("read"),
            None
        );
    }
}

pub async fn namespace_create_retry_reports_namespace_exists(backend: &dyn Backend) {
    let db = backend.start().await;
    let id = namespace(db.first(), "catalog-retry").await;
    let again = db.last().create_namespace("catalog-retry").await;
    assert_eq!(rejected(again), ApplyError::NamespaceExists(id));
    assert!(matches!(
        rejected(db.first().create_namespace("bad name").await),
        ApplyError::InvalidArgument(_)
    ));
    assert_eq!(db.first().namespaces(L).await.expect("read").len(), 1);
}

pub async fn stream_create_validates_names_partitions_and_reserved_prefix(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "catalog-streams").await;
    let create = |name: String, partitions: u32| async move {
        meta.create_stream(
            ns,
            &name,
            partitions,
            WalClass::Standard,
            Retention::default(),
        )
        .await
    };
    for (name, partitions) in [
        ("has space".to_string(), 1),
        (String::new(), 1),
        ("..".to_string(), 1),
        ("x".repeat(MAX_NAME_LEN + 1), 1),
        ("_reserved".to_string(), 1),
        ("no-partitions".to_string(), 0),
        ("too-many".to_string(), MAX_PARTITIONS + 1),
    ] {
        let err = rejected(create(name.clone(), partitions).await);
        assert!(
            matches!(err, ApplyError::InvalidArgument(_)),
            "{name:?} × {partitions}: {err:?}"
        );
    }
    let missing = NamespaceId(ns.0 + 1000);
    assert_eq!(
        rejected(
            meta.create_stream(
                missing,
                "events",
                1,
                WalClass::Standard,
                Retention::default()
            )
            .await
        ),
        ApplyError::NamespaceNotFound(missing)
    );
    let longest = "s".repeat(MAX_NAME_LEN);
    let id = create(longest.clone(), 3).await.expect("create");
    assert_eq!(
        rejected(create(longest.clone(), 3).await),
        ApplyError::StreamExists(id)
    );
    let stream = db
        .last()
        .stream_by_name(L, ns, &longest)
        .await
        .expect("read")
        .expect("the stream exists");
    assert_eq!(stream.id, id);
    assert_eq!(stream.partitions, 3);
    assert_eq!(stream.class, WalClass::Standard);
    assert_eq!(
        db.last().streams(L, Some(ns)).await.expect("read"),
        vec![stream]
    );
}

pub async fn stream_state_reports_bounds_per_partition(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "catalog-state").await;
    let s = stream(meta, ns, "events", 3).await;
    let offsets = commit(
        meta,
        "catalog-state/wal-1",
        vec![chunk(s, 0, 5, 0..50), chunk(s, 2, 2, 50..70)],
    )
    .await;
    assert_eq!(offsets, vec![0, 0]);
    let state = db
        .last()
        .stream_state(L, s)
        .await
        .expect("read")
        .expect("the stream exists");
    let bounds = |high_watermark, bytes| {
        Some(PartitionBounds {
            log_start_offset: 0,
            high_watermark,
            bytes,
        })
    };
    assert_eq!(
        state,
        StreamState {
            stream: meta.stream(L, s).await.expect("read").expect("stream"),
            partitions: vec![bounds(5, 50), bounds(0, 0), bounds(2, 20)],
        }
    );
    assert_eq!(
        meta.stream_state(L, StreamId(s.0 + 1000))
            .await
            .expect("read"),
        None
    );
}

pub async fn set_retention_round_trips(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "catalog-retention").await;
    let s = stream(meta, ns, "events", 1).await;
    let read = || async {
        db.last()
            .stream(L, s)
            .await
            .expect("read")
            .expect("stream")
            .retention
    };
    assert_eq!(read().await, Retention::default());
    let retention = Retention {
        max_age_ms: Some(60_000),
        max_bytes: Some(1 << 20),
    };
    meta.set_retention(s, retention).await.expect("set");
    assert_eq!(read().await, retention);
    // Setting it again is a no-op.
    meta.set_retention(s, retention).await.expect("set again");
    assert_eq!(read().await, retention);
    meta.set_retention(s, Retention::default())
        .await
        .expect("clear");
    assert_eq!(read().await, Retention::default());

    let missing = StreamId(s.0 + 1000);
    assert_eq!(
        rejected(meta.set_retention(missing, retention).await),
        ApplyError::StreamNotFound(missing)
    );
    let (_, implicit, _) = meta
        .create_collection(ns, "retention-docs", schema(), 1)
        .await
        .expect("create collection");
    assert!(matches!(
        rejected(meta.set_retention(implicit, retention).await),
        ApplyError::InvalidArgument(_)
    ));
}

pub async fn link_create_lookup_and_retry(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "catalog-links").await;
    let s = stream(meta, ns, "events", 2).await;
    let options = BTreeMap::from([("group".to_string(), "by-key".to_string())]);
    let id = meta
        .create_link(ns, "counts", s, counter("counts"), options.clone())
        .await
        .expect("create link");
    let link = Link {
        id,
        namespace: ns,
        name: "counts".to_string(),
        source: s,
        target: counter("counts"),
        options: options.clone(),
    };
    assert_eq!(
        db.last().link_by_name(L, ns, "counts").await.expect("read"),
        Some(link.clone())
    );
    assert_eq!(
        db.last().links(L, Some(ns)).await.expect("read"),
        vec![link.clone()]
    );
    assert_eq!(db.last().links(L, None).await.expect("read"), vec![link]);
    assert_eq!(
        rejected(
            db.last()
                .create_link(ns, "counts", s, counter("counts"), options)
                .await
        ),
        ApplyError::LinkExists(id)
    );
    let missing = StreamId(s.0 + 1000);
    assert_eq!(
        rejected(
            meta.create_link(ns, "other", missing, counter("other"), BTreeMap::new())
                .await
        ),
        ApplyError::StreamNotFound(missing)
    );
    let missing_ns = NamespaceId(ns.0 + 1000);
    assert_eq!(
        rejected(
            meta.create_link(missing_ns, "other", s, counter("other"), BTreeMap::new())
                .await
        ),
        ApplyError::NamespaceNotFound(missing_ns)
    );
    assert!(matches!(
        rejected(
            meta.create_link(ns, "_reserved", s, counter("x"), BTreeMap::new())
                .await
        ),
        ApplyError::InvalidArgument(_)
    ));
    assert_eq!(meta.link_by_name(L, ns, "other").await.expect("read"), None);
}

pub async fn lists_are_ordered_by_id(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let mut namespaces = Vec::new();
    for name in ["order-c", "order-a", "order-b"] {
        namespaces.push(namespace(meta, name).await);
    }
    let ns = namespaces[0];
    let mut streams = Vec::new();
    for name in ["zeta", "alpha", "mid"] {
        streams.push(stream(meta, ns, name, 1).await);
    }
    let other = stream(meta, namespaces[1], "other", 1).await;
    let mut links = Vec::new();
    for name in ["link-z", "link-a"] {
        links.push(
            meta.create_link(ns, name, streams[0], counter(name), BTreeMap::new())
                .await
                .expect("create link"),
        );
    }
    let mut collections = Vec::new();
    for name in ["docs-z", "docs-a"] {
        let (id, _, _) = meta
            .create_collection(ns, name, schema(), 1)
            .await
            .expect("create collection");
        collections.push(id);
    }
    let reader = db.last();

    let listed: Vec<NamespaceId> = reader
        .namespaces(L)
        .await
        .expect("read")
        .into_iter()
        .map(|n| n.id)
        .collect();
    assert_eq!(listed, namespaces);
    assert!(listed.is_sorted());

    let listed: Vec<StreamId> = reader
        .streams(L, Some(ns))
        .await
        .expect("read")
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(listed.is_sorted(), "{listed:?}");
    // The collections' implicit streams come after the user streams.
    assert_eq!(listed[..3], streams[..]);
    assert_eq!(listed.len(), 5);
    let all: Vec<StreamId> = reader
        .streams(L, None)
        .await
        .expect("read")
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(all.is_sorted(), "{all:?}");
    assert_eq!(all.len(), 6);
    assert!(all.contains(&other));

    let listed: Vec<_> = reader
        .links(L, Some(ns))
        .await
        .expect("read")
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert!(listed.is_sorted(), "{listed:?}");
    assert_eq!(listed[..2], links[..]);
    assert_eq!(listed.len(), 4);

    let listed: Vec<_> = reader
        .collections(L, Some(ns))
        .await
        .expect("read")
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(listed, collections);
    assert!(listed.is_sorted());
    let heads: Vec<_> = reader
        .collection_heads(L, None)
        .await
        .expect("read")
        .into_iter()
        .map(|h| h.collection.id)
        .collect();
    assert_eq!(heads, collections);
}
