//! The WAL object format, version 1 (little-endian):
//!
//! ```text
//! header   36 B : magic b"OPNWAL\0\0" | version u16 = 1 | class u8 (0 standard, 1 express, 2 quorum)
//!                 | reserved u8 = 0 | node_id u64 | ulid [u8; 16]
//! chunks        : chunk payloads back to back, one per (stream, partition), sorted by
//!                 (stream, partition); a chunk's payload is its RecordBatch v2 bytes concatenated
//! index         : postcard(Vec<ChunkMeta>)          (ChunkMeta.offset is absolute within the object)
//! trailer  24 B : index_offset u64 | index_len u32 | index_crc32c u32 | magic b"OPNWALFT"
//! ```
//!
//! `index_crc32c` covers the header followed by the index, so a corrupt header
//! is detected too (the trailer has no header checksum of its own). Each chunk
//! carries its own crc32c in its `ChunkMeta`.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use operon_common::StreamId;
use operon_meta::WalClass;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{LogError, corrupt};
use crate::record::Encoding;

pub const HEADER_LEN: usize = 36;
pub const TRAILER_LEN: usize = 24;
pub const VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"OPNWAL\0\0";
const TRAILER_MAGIC: &[u8; 8] = b"OPNWALFT";

/// Where one partition's chunk sits in a WAL object, and how to check it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub stream: StreamId,
    pub partition: u32,
    pub encoding: Encoding,
    pub records: u32,
    pub max_timestamp_ms: i64,
    /// Absolute byte offset of the chunk within the object.
    pub offset: u64,
    pub len: u64,
    pub crc32c: u32,
}

impl ChunkMeta {
    /// The chunk's bytes within the object, as sent to the metastore.
    pub fn byte_range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + self.len
    }
}

/// The fixed header of a WAL object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalHeader {
    pub version: u16,
    pub class: WalClass,
    pub node_id: u64,
    pub ulid: Ulid,
}

fn class_byte(class: WalClass) -> u8 {
    match class {
        WalClass::Standard => 0,
        WalClass::Express => 1,
        WalClass::Quorum => 2,
    }
}

fn class_from_byte(byte: u8) -> Result<WalClass, LogError> {
    match byte {
        0 => Ok(WalClass::Standard),
        1 => Ok(WalClass::Express),
        2 => Ok(WalClass::Quorum),
        other => Err(corrupt(format!("unknown WAL class {other}"))),
    }
}

#[derive(Debug, Default)]
struct PendingChunk {
    payload: BytesMut,
    records: u32,
    max_timestamp_ms: i64,
}

/// Builds one WAL object from record batches of many partitions.
#[derive(Debug)]
pub struct WalObjectBuilder {
    header: WalHeader,
    chunks: BTreeMap<(StreamId, u32), PendingChunk>,
}

impl WalObjectBuilder {
    pub fn new(node_id: u64, class: WalClass, ulid: Ulid) -> Self {
        Self {
            header: WalHeader {
                version: VERSION,
                class,
                node_id,
                ulid,
            },
            chunks: BTreeMap::new(),
        }
    }

    /// Appends a `kafka`-encoded batch holding `records` records to the chunk
    /// of `(stream, partition)`. Batches of one partition stay in push order.
    pub fn push(
        &mut self,
        stream: StreamId,
        partition: u32,
        batch: Bytes,
        records: u32,
        max_timestamp_ms: i64,
    ) {
        let chunk = self
            .chunks
            .entry((stream, partition))
            .or_insert_with(|| PendingChunk {
                max_timestamp_ms: i64::MIN,
                ..PendingChunk::default()
            });
        chunk.payload.extend_from_slice(&batch);
        chunk.records = chunk.records.saturating_add(records);
        chunk.max_timestamp_ms = chunk.max_timestamp_ms.max(max_timestamp_ms);
    }

    /// Whether nothing was pushed.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Writes the object. Returns its bytes and its chunks, sorted by
    /// `(stream, partition)`.
    pub fn finish(self) -> (Bytes, Vec<ChunkMeta>) {
        let mut out = BytesMut::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.header.version.to_le_bytes());
        out.extend_from_slice(&[class_byte(self.header.class), 0]);
        out.extend_from_slice(&self.header.node_id.to_le_bytes());
        out.extend_from_slice(&self.header.ulid.to_bytes());
        let mut metas = Vec::with_capacity(self.chunks.len());
        for ((stream, partition), chunk) in self.chunks {
            let offset = out.len() as u64;
            metas.push(ChunkMeta {
                stream,
                partition,
                encoding: Encoding::Kafka,
                records: chunk.records,
                max_timestamp_ms: chunk.max_timestamp_ms,
                offset,
                len: chunk.payload.len() as u64,
                crc32c: crc32c::crc32c(&chunk.payload),
            });
            out.extend_from_slice(&chunk.payload);
        }
        let index_offset = out.len() as u64;
        let index = postcard::to_stdvec(&metas).expect("a Vec of plain structs always serializes");
        let index_crc = crc32c::crc32c_append(crc32c::crc32c(&out[..HEADER_LEN]), &index);
        out.extend_from_slice(&index);
        out.extend_from_slice(&index_offset.to_le_bytes());
        out.extend_from_slice(&(index.len() as u32).to_le_bytes());
        out.extend_from_slice(&index_crc.to_le_bytes());
        out.extend_from_slice(TRAILER_MAGIC);
        (out.freeze(), metas)
    }
}

fn le_u16(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn le_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn le_u64(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(b)
}

fn parse_header(bytes: &[u8]) -> Result<WalHeader, LogError> {
    if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
        return Err(corrupt("not a WAL object"));
    }
    let version = le_u16(bytes, 8);
    if version != VERSION {
        return Err(corrupt(format!("unsupported WAL format version {version}")));
    }
    let class = class_from_byte(bytes[10])?;
    if bytes[11] != 0 {
        return Err(corrupt("WAL header reserved byte is not zero"));
    }
    let mut ulid = [0u8; 16];
    ulid.copy_from_slice(&bytes[20..36]);
    Ok(WalHeader {
        version,
        class,
        node_id: le_u64(bytes, 12),
        ulid: Ulid::from_bytes(ulid),
    })
}

/// Parses a whole WAL object, checking its header, trailer, index and every
/// chunk's checksum and placement.
pub fn parse(bytes: &[u8]) -> Result<(WalHeader, Vec<ChunkMeta>), LogError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(corrupt(format!(
            "WAL object truncated: {} bytes",
            bytes.len()
        )));
    }
    let header = parse_header(bytes)?;
    let trailer_at = bytes.len() - TRAILER_LEN;
    let trailer = &bytes[trailer_at..];
    if &trailer[16..24] != TRAILER_MAGIC {
        return Err(corrupt("bad WAL trailer magic"));
    }
    let index_offset = le_u64(trailer, 0);
    let index_len = u64::from(le_u32(trailer, 8));
    let index_crc = le_u32(trailer, 12);
    if index_offset < HEADER_LEN as u64
        || index_offset.checked_add(index_len) != Some(trailer_at as u64)
    {
        return Err(corrupt(format!(
            "bad WAL index location {index_offset}+{index_len}"
        )));
    }
    let index = &bytes[index_offset as usize..trailer_at];
    let crc = crc32c::crc32c_append(crc32c::crc32c(&bytes[..HEADER_LEN]), index);
    if crc != index_crc {
        return Err(corrupt("WAL index checksum mismatch"));
    }
    let (metas, rest): (Vec<ChunkMeta>, _) =
        postcard::take_from_bytes(index).map_err(|e| corrupt(format!("bad WAL index: {e}")))?;
    if !rest.is_empty() {
        return Err(corrupt("trailing bytes after the WAL index"));
    }
    let mut expected_offset = HEADER_LEN as u64;
    let mut previous: Option<(StreamId, u32)> = None;
    for meta in &metas {
        let key = (meta.stream, meta.partition);
        if previous.is_some_and(|p| p >= key) {
            return Err(corrupt("WAL chunks are not sorted by (stream, partition)"));
        }
        previous = Some(key);
        if meta.offset != expected_offset || meta.len == 0 || meta.records == 0 {
            return Err(corrupt(format!(
                "bad WAL chunk placement at {} ({} bytes, {} records)",
                meta.offset, meta.len, meta.records
            )));
        }
        expected_offset = meta
            .offset
            .checked_add(meta.len)
            .filter(|end| *end <= index_offset)
            .ok_or_else(|| corrupt("WAL chunk overruns the index"))?;
        read_chunk_unchecked_encoding(bytes, meta)?;
    }
    if expected_offset != index_offset {
        return Err(corrupt("gap between the last WAL chunk and the index"));
    }
    Ok((header, metas))
}

fn read_chunk_unchecked_encoding<'a>(
    object: &'a [u8],
    meta: &ChunkMeta,
) -> Result<&'a [u8], LogError> {
    let chunk = usize::try_from(meta.offset)
        .ok()
        .zip(usize::try_from(meta.len).ok())
        .and_then(|(start, len)| object.get(start..start.checked_add(len)?))
        .ok_or_else(|| corrupt("WAL chunk out of bounds"))?;
    if crc32c::crc32c(chunk) != meta.crc32c {
        return Err(corrupt(format!(
            "WAL chunk checksum mismatch at {}",
            meta.offset
        )));
    }
    Ok(chunk)
}

/// The checksum-verified bytes of one chunk of `object`: its concatenated
/// record batches. Fails with [`LogError::UnsupportedEncoding`] for a chunk
/// this build cannot decode.
pub fn read_chunk<'a>(object: &'a [u8], meta: &ChunkMeta) -> Result<&'a [u8], LogError> {
    let chunk = read_chunk_unchecked_encoding(object, meta)?;
    meta.encoding.ensure_readable()?;
    Ok(chunk)
}
