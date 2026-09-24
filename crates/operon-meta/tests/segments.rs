//! Segment swaps, trimming, retention settings, WAL-commit pruning and the
//! retired-object lifecycle, on `MetaState` directly.

use operon_common::{NamespaceId, StreamId};
use operon_meta::{
    ApplyError, Command, EntryKind, Fence, MetaState, Reply, Retention, WAL_COMMIT_WINDOW_MS,
    WalChunk, WalClass,
};
use proptest::prelude::*;

const S: StreamId = StreamId(1);

/// Namespace 1 with stream 1 (`partitions` partitions).
fn state(partitions: u32) -> MetaState {
    let mut state = MetaState::default();
    state
        .apply(Command::CreateNamespace {
            name: "acme".to_string(),
        })
        .expect("create namespace");
    state
        .apply(Command::CreateStream {
            namespace: NamespaceId(1),
            name: "events".to_string(),
            partitions,
            class: WalClass::Standard,
            retention: operon_meta::Retention::default(),
        })
        .expect("create stream");
    state
}

fn chunk(partition: u32, records: u32) -> WalChunk {
    WalChunk {
        stream: S,
        partition,
        records,
        byte_range: 0..100,
        max_timestamp_ms: 1_000,
    }
}

fn commit_at(
    state: &mut MetaState,
    object: &str,
    created_at_ms: u64,
    chunks: Vec<WalChunk>,
) -> Result<Reply, ApplyError> {
    state.apply(Command::CommitWal {
        object: object.to_string(),
        created_at_ms,
        chunks,
    })
}

fn commit(state: &mut MetaState, object: &str, chunks: Vec<WalChunk>) -> Vec<u64> {
    let created_at_ms = state.clock_ms();
    match commit_at(state, object, created_at_ms, chunks) {
        Ok(Reply::WalCommitted { base_offsets }) => base_offsets,
        other => panic!("expected WalCommitted, got {other:?}"),
    }
}

fn swap(
    partition: u32,
    replaces: &[(u64, &str)],
    segment: &str,
    fence: Option<Fence>,
    now_ms: u64,
) -> Command {
    Command::SwapSegment {
        stream: S,
        partition,
        replaces: replaces.iter().map(|(b, o)| (*b, o.to_string())).collect(),
        segment: segment.to_string(),
        byte_range: 40..500,
        max_timestamp_ms: 2_000,
        fence,
        now_ms,
    }
}

fn trim(partition: u32, before_offset: u64, now_ms: u64) -> Command {
    Command::TrimPartition {
        stream: S,
        partition,
        before_offset,
        now_ms,
    }
}

/// Advances the metastore clock to `now_ms` with a command that only moves time.
fn set_clock(state: &mut MetaState, now_ms: u64) {
    state
        .apply(Command::PruneWalCommits { now_ms })
        .expect("prune");
}

/// Partition 0 with WAL entries [0, 2) in w1, [2, 5) in w2 and [5, 9) in w3.
fn three_wal_entries() -> MetaState {
    let mut state = state(1);
    commit(&mut state, "w1", vec![chunk(0, 2)]);
    commit(&mut state, "w2", vec![chunk(0, 3)]);
    commit(&mut state, "w3", vec![chunk(0, 4)]);
    state
}

fn kinds(state: &MetaState, partition: u32) -> Vec<(u64, EntryKind, String)> {
    state
        .partition(S, partition)
        .expect("partition exists")
        .entries()
        .map(|e| (e.base_offset, e.kind, e.object.clone()))
        .collect()
}

fn retired(state: &MetaState) -> Vec<(String, u64)> {
    state
        .retired()
        .map(|(path, at)| (path.to_string(), at))
        .collect()
}

#[test]
fn a_commit_records_its_time_and_live_chunks_without_moving_the_clock() {
    let mut state = state(2);
    let reply = commit_at(&mut state, "w1", 5_000, vec![chunk(0, 2), chunk(1, 3)]);
    assert_eq!(
        reply,
        Ok(Reply::WalCommitted {
            base_offsets: vec![0, 0]
        })
    );
    let record = state.wal_commit("w1").unwrap();
    assert_eq!(record.base_offsets, [0, 0]);
    assert_eq!(record.created_at_ms, 5_000);
    assert_eq!(state.wal_live_chunks("w1"), Some(2));
    assert_eq!(state.clock_ms(), 0);
    assert!(
        state
            .partition(S, 0)
            .unwrap()
            .entries()
            .all(|e| e.kind == EntryKind::Wal)
    );
}

#[test]
fn a_commit_older_than_the_window_is_rejected_as_stale() {
    let mut state = state(1);
    let now = 10 * WAL_COMMIT_WINDOW_MS;
    set_clock(&mut state, now);
    let before = state.clone();

    let err = commit_at(
        &mut state,
        "old",
        now - WAL_COMMIT_WINDOW_MS - 1,
        vec![chunk(0, 1)],
    );
    assert_eq!(
        err,
        Err(ApplyError::StaleCommit {
            object: "old".to_string()
        })
    );
    assert_eq!(state, before);

    // Exactly at the window's edge is still accepted.
    let ok = commit_at(
        &mut state,
        "edge",
        now - WAL_COMMIT_WINDOW_MS,
        vec![chunk(0, 1)],
    );
    assert!(ok.is_ok(), "{ok:?}");
}

#[test]
fn a_retried_commit_is_deduplicated_until_pruned_and_then_rejected() {
    let mut state = state(1);
    let t0 = 1_000_000;
    set_clock(&mut state, t0);
    let first = commit_at(&mut state, "w1", t0, vec![chunk(0, 5)]).unwrap();

    // Twice the window later the record is still kept, so the retry dedupes.
    set_clock(&mut state, t0 + 2 * WAL_COMMIT_WINDOW_MS);
    assert_eq!(
        state.apply(Command::PruneWalCommits {
            now_ms: t0 + 2 * WAL_COMMIT_WINDOW_MS
        }),
        Ok(Reply::Pruned { removed: 0 })
    );
    assert_eq!(
        commit_at(&mut state, "w1", t0, vec![chunk(0, 5)]),
        Ok(first)
    );

    // One millisecond more and the record is pruned; the retry is now stale,
    // never committed again as new offsets.
    assert_eq!(
        state.apply(Command::PruneWalCommits {
            now_ms: t0 + 2 * WAL_COMMIT_WINDOW_MS + 1
        }),
        Ok(Reply::Pruned { removed: 1 })
    );
    assert!(state.wal_commit("w1").is_none());
    let err = commit_at(&mut state, "w1", t0, vec![chunk(0, 5)]).unwrap_err();
    assert!(matches!(err, ApplyError::StaleCommit { .. }), "{err:?}");
    assert_eq!(state.partition(S, 0).unwrap().next_offset(), 5);
}

#[test]
fn a_swap_replaces_wal_entries_with_one_segment_entry() {
    let mut state = three_wal_entries();
    let reply = state.apply(swap(0, &[(0, "w1"), (2, "w2")], "seg-a", None, 7_000));
    assert_eq!(reply, Ok(Reply::SegmentSwapped));

    assert_eq!(
        kinds(&state, 0),
        [
            (0, EntryKind::Segment, "seg-a".to_string()),
            (5, EntryKind::Wal, "w3".to_string())
        ]
    );
    let partition = state.partition(S, 0).unwrap();
    let segment = partition.lookup(3).unwrap();
    assert_eq!(segment.records, 5);
    assert_eq!(segment.byte_range, 40..500);
    assert_eq!(segment.max_timestamp_ms, 2_000);
    assert_eq!(partition.next_offset(), 9);
    assert_eq!(partition.bytes(), 460 + 100);

    assert_eq!(state.clock_ms(), 7_000);
    assert_eq!(state.wal_live_chunks("w1"), None);
    assert_eq!(state.wal_live_chunks("w2"), None);
    assert_eq!(state.wal_live_chunks("w3"), Some(1));
    assert_eq!(
        retired(&state),
        [("w1".to_string(), 7_000), ("w2".to_string(), 7_000)]
    );
}

#[test]
fn a_retried_swap_succeeds_and_changes_nothing() {
    let mut state = three_wal_entries();
    let command = swap(0, &[(0, "w1"), (2, "w2")], "seg-a", None, 7_000);
    state.apply(command.clone()).unwrap();
    let before = state.clone();

    // Even with a later clock and a fence that no longer holds.
    let mut retry = command;
    if let Command::SwapSegment { now_ms, fence, .. } = &mut retry {
        *now_ms = 9_000;
        *fence = Some(Fence {
            lease: "gone".to_string(),
            epoch: 3,
        });
    }
    assert_eq!(state.apply(retry), Ok(Reply::SegmentSwapped));
    assert_eq!(state, before);
}

#[test]
fn a_swap_of_entries_that_moved_is_an_index_mismatch() {
    let mismatch = Err(ApplyError::IndexMismatch {
        stream: S,
        partition: 0,
    });
    let mut state = three_wal_entries();
    let before = state.clone();
    let cases: [&[(u64, &str)]; 4] = [
        // Not contiguous: skips w2.
        &[(0, "w1"), (5, "w3")],
        // Wrong object.
        &[(0, "w1"), (2, "other")],
        // No entry at that base.
        &[(1, "w1")],
        // Out of order.
        &[(2, "w2"), (0, "w1")],
    ];
    for replaces in cases {
        assert_eq!(
            state.apply(swap(0, replaces, "seg-x", None, 7_000)),
            mismatch,
            "{replaces:?}"
        );
        assert_eq!(state, before);
    }

    // Already swapped by another segment.
    state
        .apply(swap(0, &[(0, "w1"), (2, "w2")], "seg-a", None, 7_000))
        .unwrap();
    let swapped = state.clone();
    assert_eq!(
        state.apply(swap(0, &[(0, "w1"), (2, "w2")], "seg-b", None, 8_000)),
        mismatch
    );
    assert_eq!(state, swapped);

    // Already trimmed.
    let mut state = three_wal_entries();
    state.apply(trim(0, 2, 7_000)).unwrap();
    let trimmed = state.clone();
    assert_eq!(
        state.apply(swap(0, &[(0, "w1"), (2, "w2")], "seg-c", None, 9_000)),
        mismatch
    );
    assert_eq!(state, trimmed);
}

#[test]
fn invalid_swaps_are_rejected() {
    let mut state = three_wal_entries();
    let before = state.clone();
    let empty = state.apply(swap(0, &[], "seg", None, 1)).unwrap_err();
    assert!(matches!(empty, ApplyError::InvalidArgument(_)), "{empty:?}");
    let mut no_bytes = swap(0, &[(0, "w1")], "seg", None, 1);
    if let Command::SwapSegment { byte_range, .. } = &mut no_bytes {
        *byte_range = 10..10;
    }
    let err = state.apply(no_bytes).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    let err = state.apply(swap(0, &[(0, "w1")], "", None, 1)).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    assert_eq!(
        state.apply(swap(3, &[(0, "w1")], "seg", None, 1)),
        Err(ApplyError::PartitionNotFound {
            stream: S,
            partition: 3
        })
    );
    let mut other_stream = swap(0, &[(0, "w1")], "seg", None, 1);
    if let Command::SwapSegment { stream, .. } = &mut other_stream {
        *stream = StreamId(9);
    }
    assert_eq!(
        state.apply(other_stream),
        Err(ApplyError::StreamNotFound(StreamId(9)))
    );
    assert_eq!(state, before);
}

#[test]
fn a_fenced_swap_needs_the_lease_at_its_epoch() {
    let mut state = three_wal_entries();
    let acquire = |owner: &str, now_ms: u64| Command::AcquireLease {
        key: "segmenter/1/0".to_string(),
        owner: owner.to_string(),
        ttl_ms: 1_000,
        now_ms,
    };
    state.apply(acquire("a", 100)).unwrap();
    let fence = |epoch| {
        Some(Fence {
            lease: "segmenter/1/0".to_string(),
            epoch,
        })
    };
    // Someone else took the lease over after it expired: epoch 2.
    state.apply(acquire("b", 5_000)).unwrap();
    let before = state.clone();
    assert_eq!(
        state.apply(swap(0, &[(0, "w1")], "seg-zombie", fence(1), 6_000)),
        Err(ApplyError::Fenced {
            lease: "segmenter/1/0".to_string()
        })
    );
    assert_eq!(state, before);

    assert_eq!(
        state.apply(swap(0, &[(0, "w1")], "seg-b", fence(2), 6_000)),
        Ok(Reply::SegmentSwapped)
    );
}

#[test]
fn trimming_at_an_entry_boundary_drops_the_entries_below_it() {
    let mut state = three_wal_entries();
    assert_eq!(
        state.apply(trim(0, 5, 3_000)),
        Ok(Reply::Trimmed {
            log_start_offset: 5
        })
    );
    let partition = state.partition(S, 0).unwrap();
    assert_eq!(partition.log_start_offset(), 5);
    assert_eq!(partition.high_watermark(), 9);
    assert!(partition.lookup(4).is_none());
    assert_eq!(partition.lookup(5).unwrap().object, "w3");
    let bases: Vec<u64> = partition.entries_from(0).map(|e| e.base_offset).collect();
    assert_eq!(bases, [5]);
    assert_eq!(partition.bytes(), 100);
    assert_eq!(
        retired(&state),
        [("w1".to_string(), 3_000), ("w2".to_string(), 3_000)]
    );
    assert_eq!(state.clock_ms(), 3_000);
}

#[test]
fn trimming_mid_entry_keeps_the_entry_but_hides_the_trimmed_offsets() {
    let mut state = three_wal_entries();
    assert_eq!(
        state.apply(trim(0, 3, 3_000)),
        Ok(Reply::Trimmed {
            log_start_offset: 3
        })
    );
    let partition = state.partition(S, 0).unwrap();
    assert_eq!(partition.log_start_offset(), 3);
    assert!(partition.lookup(2).is_none());
    assert_eq!(partition.lookup(3).unwrap().base_offset, 2);
    let bases: Vec<u64> = partition.entries_from(0).map(|e| e.base_offset).collect();
    assert_eq!(bases, [2, 5]);
    assert_eq!(retired(&state), [("w1".to_string(), 3_000)]);
    assert_eq!(state.wal_live_chunks("w2"), Some(1));
}

#[test]
fn trimming_beyond_the_high_watermark_stops_at_it() {
    let mut state = three_wal_entries();
    assert_eq!(
        state.apply(trim(0, 100, 3_000)),
        Ok(Reply::Trimmed {
            log_start_offset: 9
        })
    );
    let partition = state.partition(S, 0).unwrap();
    assert_eq!(partition.entries().count(), 0);
    assert_eq!(partition.bytes(), 0);
    assert!(partition.lookup(8).is_none());

    // The next commit continues at 9 and is readable.
    assert_eq!(commit(&mut state, "w4", vec![chunk(0, 1)]), [9]);
    let partition = state.partition(S, 0).unwrap();
    assert_eq!(partition.log_start_offset(), 9);
    assert_eq!(partition.lookup(9).unwrap().object, "w4");
}

#[test]
fn trimming_is_idempotent_and_never_moves_the_log_start_back() {
    let mut state = three_wal_entries();
    state.apply(trim(0, 5, 3_000)).unwrap();
    let after = state.clone();
    assert_eq!(
        state.apply(trim(0, 5, 3_000)),
        Ok(Reply::Trimmed {
            log_start_offset: 5
        })
    );
    assert_eq!(state, after);
    assert_eq!(
        state.apply(trim(0, 1, 3_000)),
        Ok(Reply::Trimmed {
            log_start_offset: 5
        })
    );
    assert_eq!(state, after);
}

#[test]
fn objects_retire_when_no_entry_references_them() {
    // A WAL object with chunks in two partitions retires only when both are gone.
    let mut state = state(2);
    commit(&mut state, "w1", vec![chunk(0, 2), chunk(1, 2)]);
    commit(&mut state, "w2", vec![chunk(0, 2)]);
    state
        .apply(swap(0, &[(0, "w1"), (2, "w2")], "seg-0", None, 1_000))
        .unwrap();
    assert_eq!(state.wal_live_chunks("w1"), Some(1));
    assert_eq!(retired(&state), [("w2".to_string(), 1_000)]);

    state.apply(trim(1, 2, 2_000)).unwrap();
    assert_eq!(state.wal_live_chunks("w1"), None);
    assert_eq!(
        retired(&state),
        [("w1".to_string(), 2_000), ("w2".to_string(), 1_000)]
    );

    // A trimmed segment retires too.
    state.apply(trim(0, 4, 3_000)).unwrap();
    assert_eq!(
        retired(&state),
        [
            ("seg-0".to_string(), 3_000),
            ("w1".to_string(), 2_000),
            ("w2".to_string(), 1_000)
        ]
    );
}

#[test]
fn forgetting_removes_retired_objects_and_ignores_unknown_ones() {
    let mut state = three_wal_entries();
    state.apply(trim(0, 5, 3_000)).unwrap();
    let forget = Command::ForgetObjects {
        objects: vec!["w1".to_string(), "unknown".to_string()],
    };
    assert_eq!(
        state.apply(forget.clone()),
        Ok(Reply::Forgotten { removed: 1 })
    );
    assert_eq!(retired(&state), [("w2".to_string(), 3_000)]);
    assert_eq!(state.apply(forget), Ok(Reply::Forgotten { removed: 0 }));
}

#[test]
fn retention_is_set_per_stream() {
    let mut state = state(1);
    assert_eq!(state.stream(S).unwrap().retention, Retention::default());
    let retention = Retention {
        max_age_ms: Some(60_000),
        max_bytes: None,
    };
    let command = Command::SetRetention {
        stream: S,
        retention,
    };
    assert_eq!(state.apply(command.clone()), Ok(Reply::RetentionSet));
    let after = state.clone();
    assert_eq!(state.apply(command), Ok(Reply::RetentionSet));
    assert_eq!(state, after);
    assert_eq!(state.stream(S).unwrap().retention, retention);
    assert_eq!(
        state.apply(Command::SetRetention {
            stream: StreamId(9),
            retention
        }),
        Err(ApplyError::StreamNotFound(StreamId(9)))
    );
    assert_eq!(state, after);
}

#[test]
fn rejected_commands_leave_the_state_and_clock_unchanged() {
    let mut state = three_wal_entries();
    let rejected = [
        swap(0, &[(0, "wrong")], "seg", None, 99_000),
        swap(
            0,
            &[(0, "w1")],
            "seg",
            Some(Fence {
                lease: "none".to_string(),
                epoch: 1,
            }),
            99_000,
        ),
        Command::TrimPartition {
            stream: StreamId(9),
            partition: 0,
            before_offset: 1,
            now_ms: 99_000,
        },
        trim(7, 1, 99_000),
        // Empty `replaces`, an empty or inverted byte range, an over-long path.
        swap(0, &[], "seg", None, 99_000),
        with_byte_range(40..40),
        with_byte_range(std::ops::Range { start: 90, end: 40 }),
        swap(
            0,
            &[(0, "w1")],
            &"s".repeat(operon_meta::MAX_KEY_LEN + 1),
            None,
            99_000,
        ),
        // A first commit older than the window.
        Command::CommitWal {
            object: "late".to_string(),
            created_at_ms: 0,
            chunks: vec![chunk(0, 1)],
        },
        Command::SetRetention {
            stream: StreamId(9),
            retention: Retention::default(),
        },
    ];
    // Make the window matter for the stale commit above.
    set_clock(&mut state, WAL_COMMIT_WINDOW_MS + 1_000);
    let before = state.clone();
    for command in rejected {
        assert!(state.apply(command.clone()).is_err(), "{command:?}");
        assert_eq!(state, before, "{command:?}");
        assert_eq!(state.clock_ms(), WAL_COMMIT_WINDOW_MS + 1_000);
    }
}

/// A swap of w1 whose segment has `byte_range`.
fn with_byte_range(range: std::ops::Range<u64>) -> Command {
    let mut command = swap(0, &[(0, "w1")], "seg", None, 99_000_000);
    if let Command::SwapSegment { byte_range, .. } = &mut command {
        *byte_range = range;
    }
    command
}

#[test]
fn a_stream_is_created_with_its_retention() {
    let mut state = state(1);
    let retention = Retention {
        max_age_ms: None,
        max_bytes: Some(1 << 20),
    };
    let reply = state.apply(Command::CreateStream {
        namespace: NamespaceId(1),
        name: "kept".to_string(),
        partitions: 1,
        class: WalClass::Standard,
        retention,
    });
    let Ok(Reply::StreamCreated(id)) = reply else {
        panic!("{reply:?}");
    };
    assert_eq!(state.stream(id).unwrap().retention, retention);
}

#[derive(Clone, Debug)]
enum Op {
    Commit(Vec<(u32, u32)>),
    /// Swap `len` WAL entries starting at the `start`-th WAL entry.
    Swap {
        partition: u32,
        start: prop::sample::Index,
        len: usize,
    },
    Trim {
        partition: u32,
        offset: prop::sample::Index,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        prop::collection::vec((0u32..2, 1u32..5), 1..4).prop_map(Op::Commit),
        (0u32..2, any::<prop::sample::Index>(), 1usize..4).prop_map(|(partition, start, len)| {
            Op::Swap {
                partition,
                start,
                len,
            }
        }),
        (0u32..2, any::<prop::sample::Index>())
            .prop_map(|(partition, offset)| Op::Trim { partition, offset }),
    ]
}

proptest! {
    /// Whatever commits, swaps and trims arrive, each partition's index tiles
    /// the readable range from the log start to the high watermark, and the
    /// live chunk counts match the WAL entries.
    #[test]
    fn index_stays_contiguous_and_live_counts_match(ops in prop::collection::vec(op(), 1..40)) {
        let mut state = state(2);
        for (i, op) in ops.into_iter().enumerate() {
            let now = (i as u64 + 1) * 10;
            match op {
                Op::Commit(chunks) => {
                    let chunks = chunks.into_iter().map(|(p, r)| chunk(p, r)).collect();
                    commit(&mut state, &format!("w{i}"), chunks);
                }
                Op::Swap { partition, start, len } => {
                    let wal: Vec<(u64, String)> = state
                        .partition(S, partition)
                        .unwrap()
                        .entries()
                        .filter(|e| e.kind == EntryKind::Wal)
                        .map(|e| (e.base_offset, e.object.clone()))
                        .collect();
                    if wal.is_empty() {
                        continue;
                    }
                    let start = start.index(wal.len());
                    let end = (start + len).min(wal.len());
                    let command = Command::SwapSegment {
                        stream: S,
                        partition,
                        replaces: wal[start..end].to_vec(),
                        segment: format!("seg{i}"),
                        byte_range: 40..90,
                        max_timestamp_ms: 0,
                        fence: None,
                        now_ms: now,
                    };
                    // Runs of WAL entries split by a segment are not contiguous:
                    // those swaps must fail as mismatches and change nothing.
                    let before = state.clone();
                    match state.apply(command) {
                        Ok(_) => {}
                        Err(ApplyError::IndexMismatch { .. }) => prop_assert_eq!(&state, &before),
                        Err(err) => prop_assert!(false, "unexpected error {:?}", err),
                    }
                }
                Op::Trim { partition, offset } => {
                    let next = state.partition(S, partition).unwrap().next_offset();
                    let before_offset = offset.index(next as usize + 2) as u64;
                    state.apply(trim(partition, before_offset, now)).unwrap();
                }
            }

            let mut wal_entries = 0u64;
            for p in 0..2 {
                let partition = state.partition(S, p).unwrap();
                let entries: Vec<_> = partition.entries().collect();
                match entries.first() {
                    None => prop_assert_eq!(partition.log_start_offset(), partition.next_offset()),
                    Some(first) => {
                        prop_assert!(first.base_offset <= partition.log_start_offset());
                        prop_assert!(partition.log_start_offset() < first.end_offset());
                    }
                }
                let mut expected = entries.first().map_or(0, |e| e.base_offset);
                for entry in &entries {
                    prop_assert_eq!(entry.base_offset, expected);
                    expected = entry.end_offset();
                    if entry.kind == EntryKind::Wal {
                        wal_entries += 1;
                    }
                }
                if !entries.is_empty() {
                    prop_assert_eq!(expected, partition.next_offset());
                }
                for offset in partition.log_start_offset()..partition.next_offset() {
                    prop_assert!(partition.lookup(offset).is_some());
                }
            }
            let live: u64 = state
                .partition(S, 0)
                .into_iter()
                .chain(state.partition(S, 1))
                .flat_map(|p| p.entries())
                .filter(|e| e.kind == EntryKind::Wal)
                .map(|e| e.object.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .iter()
                .map(|o| u64::from(state.wal_live_chunks(o).unwrap_or(0)))
                .sum();
            prop_assert_eq!(live, wal_entries);
        }
    }
}
