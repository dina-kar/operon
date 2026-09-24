//! Binary encodings: postcard for Raft log entries and local records, and a
//! checksummed, versioned envelope for snapshots.

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::SnapshotMeta;
use crate::state::MetaState;

const SNAPSHOT_MAGIC: &[u8; 8] = b"OPNMETA\0";
const SNAPSHOT_FORMAT_VERSION: u32 = 1;
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
