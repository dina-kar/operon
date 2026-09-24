//! Binary encodings: postcard for Raft log entries and local records, and a
//! checksummed, versioned envelope for snapshots.

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::SnapshotMeta;
use crate::state::MetaState;

const SNAPSHOT_MAGIC: &[u8; 8] = b"OPNMETA\0";
/// Version 2 (M0.3) added the log engine's state: entry kinds, log start
/// offsets, retention, WAL commit times, live chunk counts and retired objects.
/// Version-1 snapshots are rejected (M0.3 plan, ruling 9).
const SNAPSHOT_FORMAT_VERSION: u32 = 2;
/// Magic, then the format version.
const HEADER_LEN: usize = 12;
/// The crc32c trailer.
const TRAILER_LEN: usize = 4;

pub(crate) fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    postcard::to_stdvec(value).map_err(invalid_data)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(invalid_data)?;
    if !rest.is_empty() {
        return Err(invalid_data(format!("{} trailing bytes", rest.len())));
    }
    Ok(value)
}

#[derive(Serialize)]
struct SnapshotBodyRef<'a> {
    meta: &'a SnapshotMeta,
    state: &'a MetaState,
}

#[derive(serde::Deserialize)]
struct SnapshotBody {
    meta: SnapshotMeta,
    state: MetaState,
}

/// Encodes a snapshot as `magic | format version (u32 LE) | postcard body | crc32c (u32 LE)`,
/// where the checksum covers everything before it.
pub(crate) fn encode_snapshot(meta: &SnapshotMeta, state: &MetaState) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(SNAPSHOT_MAGIC);
    out.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    postcard::to_io(&SnapshotBodyRef { meta, state }, &mut out).map_err(invalid_data)?;
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Decodes and verifies a snapshot written by [`encode_snapshot`].
pub(crate) fn decode_snapshot(bytes: &[u8]) -> io::Result<(SnapshotMeta, MetaState)> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN || &bytes[..8] != SNAPSHOT_MAGIC {
        return Err(invalid_data("not a metastore snapshot"));
    }
    let (covered, trailer) = bytes.split_at(bytes.len() - TRAILER_LEN);
    let stored = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    if crc32c::crc32c(covered) != stored {
        return Err(invalid_data("snapshot checksum mismatch"));
    }
    let version = u32::from_le_bytes([covered[8], covered[9], covered[10], covered[11]]);
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported snapshot format version {version}"),
        ));
    }
    let body: SnapshotBody = decode(&covered[HEADER_LEN..])?;
    Ok((body.meta, body.state))
}

fn invalid_data(err: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::types::{Retention, WalChunk, WalClass};
    use operon_common::{NamespaceId, StreamId};

    /// A state that uses every field the log engine added.
    fn log_state() -> MetaState {
        let mut state = MetaState::default();
        let commands = [
            Command::CreateNamespace {
                name: "acme".to_string(),
            },
            Command::CreateStream {
                namespace: NamespaceId(1),
                name: "events".to_string(),
                partitions: 1,
                class: WalClass::Standard,
                retention: Retention::default(),
            },
            Command::SetRetention {
                stream: StreamId(1),
                retention: Retention {
                    max_age_ms: Some(10),
                    max_bytes: Some(20),
                },
            },
        ];
        for command in commands {
            state.apply(command).expect("setup");
        }
        for (i, object) in ["w1", "w2", "w3"].iter().enumerate() {
            state
                .apply(Command::CommitWal {
                    object: object.to_string(),
                    created_at_ms: 100 + i as u64,
                    chunks: vec![WalChunk {
                        stream: StreamId(1),
                        partition: 0,
                        records: 2,
                        byte_range: 0..10,
                        max_timestamp_ms: 5,
                    }],
                })
                .expect("commit");
        }
        state
            .apply(Command::SwapSegment {
                stream: StreamId(1),
                partition: 0,
                replaces: vec![(2, "w2".to_string())],
                segment: "seg".to_string(),
                byte_range: 40..50,
                max_timestamp_ms: 5,
                fence: None,
                now_ms: 1_000,
            })
            .expect("swap");
        state
            .apply(Command::TrimPartition {
                stream: StreamId(1),
                partition: 0,
                before_offset: 3,
                now_ms: 2_000,
            })
            .expect("trim");
        state
    }

    #[test]
    fn snapshots_round_trip_the_log_engine_state() {
        let state = log_state();
        assert_eq!(
            state.partition(StreamId(1), 0).unwrap().log_start_offset(),
            3
        );
        assert_eq!(state.retired().count(), 2);
        assert_eq!(state.wal_live_chunks("w3"), Some(1));
        let meta = SnapshotMeta::default();
        let bytes = encode_snapshot(&meta, &state).unwrap();
        assert_eq!(&bytes[8..12], &2u32.to_le_bytes());
        let (decoded_meta, decoded) = decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded_meta, meta);
        assert_eq!(decoded, state);
    }

    #[test]
    fn version_1_snapshots_are_rejected() {
        let mut bytes = encode_snapshot(&SnapshotMeta::default(), &log_state()).unwrap();
        bytes.truncate(bytes.len() - TRAILER_LEN);
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
        let crc = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        let err = decode_snapshot(&bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
    }
}
