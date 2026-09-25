//! `CollectionWriter`: validation, partition routing, one WAL object per
//! write, and consistency tokens.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use common::{Cluster, Meta, collection, faulty_store, field, log_writer, namespace, upsert};
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionSchema, CollectionWriter, ConsistencyToken, DocOp, DynamicMapping, FieldKind,
    MAX_WRITE_OPS, OpError, OpResult, PrimaryKey, WriteError, decode, partition_of,
};
use operon_common::{CollectionId, StreamId};
use operon_log::{FetchRequest, LogReader};
use operon_meta::Consistency;
use operon_store::{Op, Store};
use serde_json::json;

fn strict_schema() -> CollectionSchema {
    common::schema(
        vec![field("n", FieldKind::I64), field("tag", FieldKind::Keyword)],
        DynamicMapping::Strict,
    )
}

async fn reader(meta: &operon_meta::MetaClient, store: &Store) -> LogReader {
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: 4096,
            memory_bytes: 1 << 20,
            disk: None,
        },
    )
    .await
    .expect("cache");
    LogReader::new(meta.clone(), cache)
}

/// Reads the op at `offset` of `partition`.
async fn read_op(reader: &LogReader, stream: StreamId, partition: u32, offset: u64) -> DocOp {
    let fetched = reader
        .fetch(FetchRequest {
            stream,
            partition,
            offset,
            max_bytes: 1,
            max_wait: Duration::ZERO,
        })
        .await
        .expect("fetch");
    let record = fetched
        .records
        .iter()
        .find(|r| r.offset == offset)
        .expect("the record at the offset");
    decode(&record.record).expect("decode")
}

#[tokio::test]
async fn a_write_routes_each_op_to_its_partition_and_returns_offsets() {
    let meta = Meta::start().await;
    let ns = namespace(&meta.client, "acme").await;
    let (cid, stream) = collection(&meta.client, ns, "docs", strict_schema(), 4).await;
    let store = Store::in_memory();
    let writer = CollectionWriter::new(meta.client.clone(), log_writer(&meta.client, &store));

    let ops: Vec<DocOp> = (0..20).map(|i| upsert(i, json!({"n": i}))).collect();
    let outcome = writer.write(ns, cid, ops.clone()).await.unwrap();
    assert_eq!(outcome.schema_version, 1);
    assert_eq!(outcome.results.len(), 20);

    let reader = reader(&meta.client, &store).await;
    let mut next: BTreeMap<u32, u64> = BTreeMap::new();
    for (op, result) in ops.iter().zip(&outcome.results) {
        let OpResult::Written { partition, offset } = *result else {
            panic!("rejected: {result:?}");
        };
        assert_eq!(partition, partition_of(op.pk(), 4));
        // Input order within a partition, from offset 0.
        let expected = next.entry(partition).or_insert(0);
        assert_eq!(offset, *expected, "partition {partition}");
        *expected += 1;
        assert_eq!(read_op(&reader, stream, partition, offset).await, *op);
    }
    assert!(next.len() > 1, "20 keys spread over several partitions");
    meta.shutdown().await;
}

#[tokio::test]
async fn a_write_is_one_wal_object() {
    let meta = Meta::start().await;
    let ns = namespace(&meta.client, "acme").await;
    let (cid, _) = collection(&meta.client, ns, "docs", strict_schema(), 4).await;
    let (faulty, store) = faulty_store();
    let writer = CollectionWriter::new(meta.client.clone(), log_writer(&meta.client, &store));

    // Enough keys to reach every partition.
    let ops: Vec<DocOp> = (0..64).map(|i| upsert(i, json!({"n": i}))).collect();
    let partitions: BTreeSet<u32> = ops.iter().map(|op| partition_of(op.pk(), 4)).collect();
    assert_eq!(partitions.len(), 4);

    let puts = faulty.calls(Op::PutCreate);
    let outcome = writer.write(ns, cid, ops).await.unwrap();
    assert_eq!(faulty.calls(Op::PutCreate), puts + 1);
    let written: BTreeSet<u32> = outcome
        .results
        .iter()
        .map(|r| match r {
            OpResult::Written { partition, .. } => *partition,
            other => panic!("rejected: {other:?}"),
        })
        .collect();
    assert_eq!(written, partitions);
    meta.shutdown().await;
}

#[tokio::test]
async fn rejected_ops_are_reported_and_valid_ones_written() {
    let meta = Meta::start().await;
    let ns = namespace(&meta.client, "acme").await;
    let mapped = common::schema(vec![field("n", FieldKind::I64)], DynamicMapping::Map);
    let (cid, stream) = collection(&meta.client, ns, "docs", mapped, 2).await;
    let store = Store::in_memory();
    let writer = CollectionWriter::new(meta.client.clone(), log_writer(&meta.client, &store));

    let mut bad_keys = common::patch(PrimaryKey::U64(6), json!({}));
    if let DocOp::Patch { delete_keys, .. } = &mut bad_keys {
        delete_keys.push("a..b".to_string());
    }
    let ops = vec![
        upsert(1, json!({"n": 1})),
        upsert(2, json!({"n": "two"})),
        DocOp::Delete(PrimaryKey::Str(String::new())),
        upsert(3, json!({"n": 3, "extra": true})),
        DocOp::Delete(PrimaryKey::U64(4)),
        common::patch(PrimaryKey::U64(5), json!({"n": 5})),
        bad_keys,
    ];
    let outcome = writer.write(ns, cid, ops.clone()).await.unwrap();
    let results = &outcome.results;
    assert_eq!(results.len(), 7);
    assert!(matches!(results[0], OpResult::Written { .. }));
    assert_eq!(
        results[1],
        OpResult::Rejected(OpError::SchemaViolation {
            field: "n".to_string(),
            message: "cannot index \"two\" as i64".to_string(),
        })
    );
    assert!(
        matches!(&results[2], OpResult::Rejected(OpError::InvalidArgument(_))),
        "{:?}",
        results[2]
    );
    assert_eq!(
        results[3],
        OpResult::Rejected(OpError::DynamicMappingRequired {
            paths: vec!["extra".to_string()],
        })
    );
    assert!(matches!(results[4], OpResult::Written { .. }));
    assert!(matches!(results[5], OpResult::Written { .. }));
    assert!(
        matches!(&results[6], OpResult::Rejected(OpError::InvalidArgument(_))),
        "{:?}",
        results[6]
    );

    let reader = reader(&meta.client, &store).await;
    for i in [0, 4, 5] {
        let OpResult::Written { partition, offset } = results[i] else {
            unreachable!()
        };
        assert_eq!(read_op(&reader, stream, partition, offset).await, ops[i]);
    }
    let high: u64 = meta
        .client
        .read(Consistency::Local, move |s| {
            (0..2)
                .map(|p| s.partition(stream, p).expect("partition").high_watermark())
                .sum()
        })
        .await
        .unwrap();
    assert_eq!(high, 3, "only the valid ops were appended");

    // Nothing valid: nothing is appended, and the token is empty.
    let outcome = writer
        .write(ns, cid, vec![upsert(9, json!({"n": "x"}))])
        .await
        .unwrap();
    assert!(matches!(outcome.results[0], OpResult::Rejected(_)));
    assert_eq!(outcome.token, ConsistencyToken::default());

    // Too many ops is refused before anything else.
    let many = vec![DocOp::Delete(PrimaryKey::U64(1)); MAX_WRITE_OPS + 1];
    assert!(matches!(
        writer.write(ns, cid, many).await,
        Err(WriteError::TooManyOps(n)) if n == MAX_WRITE_OPS + 1
    ));
    meta.shutdown().await;
}

#[tokio::test]
async fn a_writer_retries_validation_on_a_linearizable_schema_read() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await.id();
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let leader_client = cluster.client(leader);
    let follower_client = cluster.client(follower);

    let ns = namespace(&leader_client, "acme").await;
    let (cid, stream) = collection(&leader_client, ns, "docs", strict_schema(), 2).await;
    common::eventually(follower_client.local(), move |s| {
        s.collection(cid).is_some()
    })
    .await;

    // The follower misses the schema update, so its local read is stale.
    cluster.router.isolate(follower);
    let mut next = strict_schema();
    next.fields.push(field("added", FieldKind::Bool));
    next.version = 2;
    leader_client
        .update_collection_schema(cid, 1, next)
        .await
        .unwrap();
    let local_version = follower_client
        .read(Consistency::Local, move |s| {
            s.collection(cid).map(|c| c.schema.version)
        })
        .await
        .unwrap();
    assert_eq!(
        local_version,
        Some(1),
        "the follower has not seen the update"
    );

    let store = Store::in_memory();
    let writer = CollectionWriter::new(
        follower_client.clone(),
        log_writer(&follower_client, &store),
    );
    let outcome = writer
        .write(ns, cid, vec![upsert(1, json!({"n": 1, "added": true}))])
        .await
        .unwrap();
    assert!(
        matches!(outcome.results[0], OpResult::Written { .. }),
        "{:?}",
        outcome.results
    );
    assert_eq!(outcome.schema_version, 2);
    let partition = partition_of(&PrimaryKey::U64(1), 2);
    assert_eq!(
        outcome.token,
        ConsistencyToken(vec![(stream, partition, 1)])
    );
    cluster.router.heal(follower);
    cluster.shutdown().await;
}

#[tokio::test]
async fn the_token_names_every_written_partition() {
    let meta = Meta::start().await;
    let ns = namespace(&meta.client, "acme").await;
    let (cid, stream) = collection(&meta.client, ns, "docs", strict_schema(), 4).await;
    let store = Store::in_memory();
    let writer = CollectionWriter::new(meta.client.clone(), log_writer(&meta.client, &store));

    // Two keys of one partition and one of another.
    let pks: Vec<u64> = (0..).take(200).collect();
    let p0 = partition_of(&PrimaryKey::U64(pks[0]), 4);
    let same: Vec<u64> = pks
        .iter()
        .copied()
        .filter(|&k| partition_of(&PrimaryKey::U64(k), 4) == p0)
        .take(2)
        .collect();
    let other = pks
        .iter()
        .copied()
        .find(|&k| partition_of(&PrimaryKey::U64(k), 4) != p0)
        .unwrap();
    let p1 = partition_of(&PrimaryKey::U64(other), 4);

    let first = writer
        .write(
            ns,
            cid,
            vec![upsert(same[0], json!({})), upsert(other, json!({}))],
        )
        .await
        .unwrap();
    assert_eq!(
        first.token,
        ConsistencyToken(vec![(stream, p0, 1), (stream, p1, 1)]).normalized()
    );
    let second = writer
        .write(
            ns,
            cid,
            vec![upsert(same[1], json!({})), upsert(same[0], json!({}))],
        )
        .await
        .unwrap();
    assert_eq!(second.token, ConsistencyToken(vec![(stream, p0, 3)]));
    assert_eq!(
        second.results,
        [
            OpResult::Written {
                partition: p0,
                offset: 1
            },
            OpResult::Written {
                partition: p0,
                offset: 2
            },
        ]
    );
    // The token parses back from its text form.
    let text = second.token.to_string();
    assert_eq!(text.parse::<ConsistencyToken>().unwrap(), second.token);
    meta.shutdown().await;
}

#[tokio::test]
async fn a_write_to_a_dropped_collection_is_not_found() {
    let meta = Meta::start().await;
    let ns = namespace(&meta.client, "acme").await;
    let (cid, _) = collection(&meta.client, ns, "docs", strict_schema(), 1).await;
    let store = Store::in_memory();
    let writer = CollectionWriter::new(meta.client.clone(), log_writer(&meta.client, &store));
    writer
        .write(ns, cid, vec![upsert(1, json!({}))])
        .await
        .unwrap();

    meta.client.drop_collection(ns, "docs").await.unwrap();
    let err = writer
        .write(ns, cid, vec![upsert(1, json!({}))])
        .await
        .unwrap_err();
    assert!(
        matches!(err, WriteError::CollectionNotFound(id) if id == cid),
        "{err:?}"
    );
    // An id that never existed, and a collection of another namespace.
    assert!(matches!(
        writer.write(ns, CollectionId(99), vec![]).await,
        Err(WriteError::CollectionNotFound(_))
    ));
    let (other, _) = collection(&meta.client, ns, "other", strict_schema(), 1).await;
    let elsewhere = namespace(&meta.client, "elsewhere").await;
    assert!(matches!(
        writer.write(elsewhere, other, vec![]).await,
        Err(WriteError::CollectionNotFound(_))
    ));
    meta.shutdown().await;
}
