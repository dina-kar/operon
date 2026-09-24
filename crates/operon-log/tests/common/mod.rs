//! Shared test harness: a single-node metastore, streams, and record helpers.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_common::{NamespaceId, StreamId};
use operon_log::{LogConfig, OffsetRecord, Record, batch, segment};
use operon_meta::{
    Clock, Consistency, EntryKind, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, WalClass,
};
use operon_store::{FaultyStore, Store};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(20);

/// A single-node metastore with a client.
pub struct Meta {
    pub node: MetaNode,
    pub client: MetaClient,
    _dir: TempDir,
}

impl Meta {
    pub async fn start() -> Self {
        Self::start_with(Arc::new(SystemClock), MetaClientConfig::default()).await
    }

    pub async fn start_with(clock: Arc<dyn Clock>, client_config: MetaClientConfig) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = MetaConfig::new(1, dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(WAIT).await.expect("leader");
        let client = MetaClient::new(node.clone(), vec![], clock, client_config);
        Self {
            node,
            client,
            _dir: dir,
        }
    }

    /// Creates namespace `ns` (if needed) and stream `name`.
    pub async fn stream(&self, ns: &str, name: &str, partitions: u32) -> (NamespaceId, StreamId) {
        let ns = match self.client.create_namespace(ns).await {
            Ok(id) => id,
            Err(operon_meta::MetaError::Rejected(operon_meta::ApplyError::NamespaceExists(id))) => {
                id
            }
            Err(err) => panic!("create namespace: {err}"),
        };
        let stream = self
            .client
            .create_stream(ns, name, partitions, WalClass::Standard)
            .await
            .expect("create stream");
        (ns, stream)
    }

    pub async fn high_watermark(&self, stream: StreamId, partition: u32) -> u64 {
        self.client
            .read(Consistency::Local, |s| {
                s.partition(stream, partition)
                    .map(|p| p.high_watermark())
                    .expect("partition")
            })
            .await
            .expect("read")
    }

    pub async fn shutdown(&self) {
        self.node.shutdown().await.expect("shutdown meta");
    }
}

/// A store whose faults the returned `FaultyStore` controls.
pub fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

/// A writer config that flushes quickly.
pub fn fast_config() -> LogConfig {
    LogConfig {
        flush_interval: Duration::from_millis(20),
        ..LogConfig::new(1)
    }
}

/// `n` records whose values are `"<tag>-<i>"`.
pub fn records(tag: &str, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{tag}-{i}"))),
            headers: vec![],
            timestamp_ms: 1_000 + i as i64,
        })
        .collect()
}

pub fn value(record: &Record) -> String {
    String::from_utf8(record.value.clone().expect("value").to_vec()).expect("utf-8")
}

/// Reads a partition straight from its index entries and the store, without
/// the reader: every committed record, in offset order.
pub async fn read_direct(
    meta: &MetaClient,
    store: &Store,
    stream: StreamId,
    partition: u32,
) -> Vec<OffsetRecord> {
    let entries = meta
        .read(Consistency::Local, |s| {
            s.partition(stream, partition)
                .expect("partition")
                .entries()
                .cloned()
                .collect::<Vec<_>>()
        })
        .await
        .expect("read");
    let mut out = Vec::new();
    for entry in entries {
        match entry.kind {
            EntryKind::Wal => {
                let bytes = store
                    .get_range(&entry.object, entry.byte_range.clone())
                    .await
                    .expect("get");
                out.extend(batch::decode(&bytes, entry.base_offset).expect("decode"));
            }
            EntryKind::Segment => {
                let (bytes, _) = store.get(&entry.object).await.expect("get segment");
                let footer = segment::parse(&bytes).expect("parse segment");
                let data = &bytes[footer.data.start as usize..footer.data.end as usize];
                out.extend(batch::decode(data, footer.base_offset).expect("decode"));
            }
        }
    }
    out
}

/// Polls `check` until it holds.
pub async fn eventually<F, Fut>(what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + WAIT;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
