use operon_common::{NamespaceId, StreamId};
use operon_meta::{ApplyError, Command, MetaState, Reply, WalChunk, WalClass};
use proptest::prelude::*;

/// A state with namespace 1 and streams 1 (`partitions_a` partitions) and 2
/// (`partitions_b` partitions).
fn state_with_streams(partitions_a: u32, partitions_b: u32) -> MetaState {
    let mut state = MetaState::default();
    state
        .apply(Command::CreateNamespace {
            name: "acme".to_string(),
        })
        .expect("create namespace");
    for (name, partitions) in [("a", partitions_a), ("b", partitions_b)] {
        state
            .apply(Command::CreateStream {
                namespace: NamespaceId(1),
                name: name.to_string(),
                partitions,
                class: WalClass::Standard,
            })
            .expect("create stream");
    }
    state
}

fn chunk(stream: u64, partition: u32, records: u32, start: u64) -> WalChunk {
    WalChunk {
        stream: StreamId(stream),
        partition,
        records,
        byte_range: start..start + 100,
        max_timestamp_ms: 1_700_000_000_000,
    }
}

fn commit(state: &mut MetaState, object: &str, chunks: Vec<WalChunk>) -> Result<Reply, ApplyError> {
    state.apply(Command::CommitWal {
        object: object.to_string(),
        chunks,
    })
}

fn offsets(reply: Result<Reply, ApplyError>) -> Vec<u64> {
    match reply {
        Ok(Reply::WalCommitted { base_offsets }) => base_offsets,
        other => panic!("expected WalCommitted, got {other:?}"),
    }
}

#[test]
fn commits_assign_dense_offsets_per_partition() {
    let mut state = state_with_streams(2, 1);

    let first = commit(
        &mut state,
        "wal/1.wal",
        vec![chunk(1, 0, 10, 0), chunk(1, 1, 5, 100), chunk(2, 0, 3, 200)],
    );
    assert_eq!(offsets(first), [0, 0, 0]);

    let second = commit(
        &mut state,
        "wal/2.wal",
        vec![chunk(1, 0, 4, 0), chunk(2, 0, 1, 100)],
    );
    assert_eq!(offsets(second), [10, 3]);

    assert_eq!(state.partition(StreamId(1), 0).unwrap().next_offset(), 14);
    assert_eq!(state.partition(StreamId(1), 1).unwrap().next_offset(), 5);
    assert_eq!(state.partition(StreamId(2), 0).unwrap().next_offset(), 4);
}

#[test]
fn two_chunks_for_one_partition_get_consecutive_offsets() {
    let mut state = state_with_streams(1, 1);
    let reply = commit(
        &mut state,
        "wal/1.wal",
        vec![chunk(1, 0, 2, 0), chunk(1, 0, 3, 100)],
    );
    assert_eq!(offsets(reply), [0, 2]);
    assert_eq!(state.partition(StreamId(1), 0).unwrap().next_offset(), 5);
}

#[test]
fn lookup_finds_the_entry_holding_an_offset() {
    let mut state = state_with_streams(1, 1);
    commit(&mut state, "wal/1.wal", vec![chunk(1, 0, 10, 0)]).unwrap();
    commit(&mut state, "wal/2.wal", vec![chunk(1, 0, 5, 300)]).unwrap();
    let partition = state.partition(StreamId(1), 0).unwrap();

    let entry = partition.lookup(0).unwrap();
    assert_eq!(
        (entry.base_offset, entry.records, entry.object.as_str()),
        (0, 10, "wal/1.wal")
    );
    assert_eq!(entry.byte_range, 0..100);
    assert_eq!(entry.end_offset(), 10);
    assert_eq!(partition.lookup(9).unwrap().object, "wal/1.wal");
    assert_eq!(partition.lookup(10).unwrap().object, "wal/2.wal");
    assert_eq!(partition.lookup(14).unwrap().byte_range, 300..400);
    assert!(partition.lookup(15).is_none());

    let from_7: Vec<u64> = partition.entries_from(7).map(|e| e.base_offset).collect();
    assert_eq!(from_7, [0, 10]);
    let from_10: Vec<u64> = partition.entries_from(10).map(|e| e.base_offset).collect();
    assert_eq!(from_10, [10]);
    assert_eq!(partition.entries_from(15).count(), 0);
}

#[test]
fn retrying_a_commit_returns_the_original_offsets_and_changes_nothing() {
    let mut state = state_with_streams(1, 1);
    let first = offsets(commit(&mut state, "wal/1.wal", vec![chunk(1, 0, 10, 0)]));
    commit(&mut state, "wal/2.wal", vec![chunk(1, 0, 5, 0)]).unwrap();
    let before = state.clone();

    let retry = offsets(commit(&mut state, "wal/1.wal", vec![chunk(1, 0, 10, 0)]));
    assert_eq!(retry, first);
    assert_eq!(state, before);
}

#[test]
fn an_invalid_chunk_rejects_the_whole_commit() {
    let mut state = state_with_streams(2, 1);
    let before = state.clone();
    let valid = chunk(1, 0, 10, 0);

    let cases = [
        (chunk(9, 0, 1, 0), ApplyError::StreamNotFound(StreamId(9))),
        (
            chunk(1, 2, 1, 0),
            ApplyError::PartitionNotFound {
                stream: StreamId(1),
                partition: 2,
            },
        ),
    ];
    for (bad, expected) in cases {
        let reply = commit(&mut state, "wal/x.wal", vec![valid.clone(), bad]);
        assert_eq!(reply, Err(expected));
    }

    let zero_records = chunk(1, 1, 0, 0);
    let mut empty_range = chunk(1, 1, 1, 0);
    empty_range.byte_range = 50..50;
    for bad in [zero_records, empty_range] {
        let err = commit(&mut state, "wal/x.wal", vec![valid.clone(), bad]).unwrap_err();
        assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    }
    for object in ["", &"o".repeat(operon_meta::MAX_KEY_LEN + 1)] {
        let err = commit(&mut state, object, vec![valid.clone()]).unwrap_err();
        assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    }
    let err = commit(&mut state, "wal/x.wal", vec![]).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");

    assert_eq!(state, before);
}

proptest! {
    /// Whatever commits (and retries) arrive, each partition's index entries tile
    /// `[0, next_offset)` with no gaps or overlaps, and `lookup` finds every offset.
    #[test]
    fn index_entries_tile_each_partition(
        commits in prop::collection::vec(
            prop::collection::vec((1u64..=2, 0u32..3, 1u32..20), 1..5),
            1..30,
        ),
        retries in prop::collection::vec(any::<prop::sample::Index>(), 0..10),
    ) {
        let mut state = state_with_streams(3, 3);
        let mut replies = Vec::new();
        for (i, chunks) in commits.iter().enumerate() {
            let chunks = chunks
                .iter()
                .map(|&(stream, partition, records)| chunk(stream, partition, records, 0))
                .collect();
            replies.push(offsets(commit(&mut state, &format!("wal/{i}.wal"), chunks)));
        }
        for index in retries {
            let i = index.index(commits.len());
            let again = offsets(commit(&mut state, &format!("wal/{i}.wal"), vec![chunk(1, 0, 1, 0)]));
            prop_assert_eq!(&again, &replies[i]);
        }

        for stream in [1, 2] {
            for p in 0..3 {
                let partition = state.partition(StreamId(stream), p).unwrap();
                let mut expected_base = 0;
                for entry in partition.entries_from(0) {
                    prop_assert_eq!(entry.base_offset, expected_base);
                    expected_base = entry.end_offset();
                }
                prop_assert_eq!(expected_base, partition.next_offset());
                for offset in 0..partition.next_offset() {
                    let entry = partition.lookup(offset).unwrap();
                    prop_assert!(entry.base_offset <= offset && offset < entry.end_offset());
                }
                prop_assert!(partition.lookup(partition.next_offset()).is_none());
            }
        }
    }
}
