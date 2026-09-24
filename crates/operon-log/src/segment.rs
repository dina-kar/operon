//! The segment format, version 1 (little-endian):
//!
//! ```text
//! header   40 B : magic b"OPNSEG\0\0" | version u16 = 1 | encoding u8 | reserved u8 = 0 | stream u64
//!                 | partition u32 | base_offset u64 | end_offset u64 (exclusive)
//! data          : RecordBatch v2 bytes, absolute baseOffsets, contiguous offsets
//! index         : postcard(Vec<BatchIndexEntry>)
//! trailer  40 B : index_offset u64 | index_len u32 | index_crc32c u32 | data_crc32c u32 | header_crc32c u32
//!                 | reserved u64 = 0 | magic b"OPNSEGFT"
//! ```
//!
//! The metastore's index entry for a segment has `byte_range = data`. A reader
//! loads the trailer and the index once per segment (segments are immutable)
//! and then range-reads whole batches from the data region.

use std::ops::Range;

use bytes::{Bytes, BytesMut};
use operon_common::StreamId;
use serde::{Deserialize, Serialize};

use crate::batch;
use crate::error::{LogError, corrupt};
use crate::record::Encoding;

pub const HEADER_LEN: u64 = 40;
pub const TRAILER_LEN: u64 = 40;
pub const VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"OPNSEG\0\0";
const TRAILER_MAGIC: &[u8; 8] = b"OPNSEGFT";

/// Where one record batch starts in a segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchIndexEntry {
    pub base_offset: u64,
    /// Absolute position of the batch within the object.
    pub position: u64,
    pub max_timestamp_ms: i64,
}

/// A segment's header and batch index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentFooter {
    pub stream: StreamId,
    pub partition: u32,
    pub encoding: Encoding,
    pub base_offset: u64,
    /// One past the last offset.
    pub end_offset: u64,
    /// The data region: all record batches, back to back.
    pub data: Range<u64>,
    pub batches: Vec<BatchIndexEntry>,
}

impl SegmentFooter {
    /// The index of the batch holding `offset`.
    pub fn batch_index_for(&self, offset: u64) -> Option<usize> {
        if offset < self.base_offset || offset >= self.end_offset {
            return None;
        }
        self.batches
            .partition_point(|b| b.base_offset <= offset)
            .checked_sub(1)
    }

    /// The start of the batch holding `offset`.
    pub fn position_for(&self, offset: u64) -> Option<u64> {
        self.batch_index_for(offset)
            .map(|i| self.batches[i].position)
    }

    /// The bytes of batch `i` within the object.
    pub fn batch_range(&self, i: usize) -> Range<u64> {
        let start = self.batches[i].position;
        let end = self
            .batches
            .get(i + 1)
            .map_or(self.data.end, |next| next.position);
        start..end
    }

    /// One past the last offset of batch `i`.
    pub fn batch_end_offset(&self, i: usize) -> u64 {
        self.batches
            .get(i + 1)
            .map_or(self.end_offset, |next| next.base_offset)
    }
}

/// The fixed trailer: where the index is, and the checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trailer {
    pub index_offset: u64,
    pub index_len: u32,
    pub index_crc32c: u32,
    pub data_crc32c: u32,
    pub header_crc32c: u32,
}

impl Trailer {
    /// The index's byte range, checked against the object's length: the index
    /// must sit right before the trailer, after the header.
    pub fn index_range(&self, object_len: u64) -> Result<Range<u64>, LogError> {
        let end = self.index_offset.checked_add(u64::from(self.index_len));
        if self.index_offset < HEADER_LEN
            || object_len < HEADER_LEN + TRAILER_LEN
            || end != Some(object_len - TRAILER_LEN)
        {
            return Err(corrupt(format!(
                "bad segment index location {}+{} in {object_len} bytes",
                self.index_offset, self.index_len
            )));
        }
        Ok(self.index_offset..object_len - TRAILER_LEN)
    }
}

/// Builds a segment of one partition from record batches.
#[derive(Debug)]
pub struct SegmentBuilder {
    stream: StreamId,
    partition: u32,
    encoding: Encoding,
    base_offset: u64,
    next_offset: u64,
    data: BytesMut,
    batches: Vec<BatchIndexEntry>,
}

impl SegmentBuilder {
    pub fn new(stream: StreamId, partition: u32, base_offset: u64, encoding: Encoding) -> Self {
        Self {
            stream,
            partition,
            encoding,
            base_offset,
            next_offset: base_offset,
            data: BytesMut::new(),
            batches: Vec::new(),
        }
    }

    /// The offset the next pushed batch starts at.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Bytes of batch data pushed so far.
    pub fn data_len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Appends one record batch holding `records` records. The builder
    /// patches its `baseOffset` so offsets stay contiguous. Rejects anything
    /// that is not exactly one well-formed batch of `records` records, since
    /// it would break the segment's offset contiguity.
    pub fn push_batch(
        &mut self,
        batch: Bytes,
        records: u32,
        max_timestamp_ms: i64,
    ) -> Result<(), LogError> {
        let mut parsed = batch::batches(&batch);
        let first = match (parsed.next(), parsed.next()) {
            (Some(Ok(first)), None) => first,
            (Some(Err(err)), _) => {
                return Err(LogError::InvalidArgument(format!(
                    "bad record batch: {err}"
                )));
            }
            _ => {
                return Err(LogError::InvalidArgument(
                    "push_batch takes exactly one record batch".to_string(),
                ));
            }
        };
        if first.record_count != records || records == 0 {
            return Err(LogError::InvalidArgument(format!(
                "batch holds {} records, not {records}",
                first.record_count
            )));
        }
        let position = HEADER_LEN + self.data.len() as u64;
        let start = self.data.len();
        self.data.extend_from_slice(&batch);
        batch::patch_base_offset(&mut self.data[start..], self.next_offset);
        self.batches.push(BatchIndexEntry {
            base_offset: self.next_offset,
            position,
            max_timestamp_ms,
        });
        self.next_offset += u64::from(records);
        Ok(())
    }

    /// Writes the segment. Returns its bytes and its footer.
    pub fn finish(self) -> (Bytes, SegmentFooter) {
        let mut out = BytesMut::with_capacity(self.data.len() + 128);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&[u8::from(self.encoding), 0]);
        out.extend_from_slice(&self.stream.0.to_le_bytes());
        out.extend_from_slice(&self.partition.to_le_bytes());
        out.extend_from_slice(&self.base_offset.to_le_bytes());
        out.extend_from_slice(&self.next_offset.to_le_bytes());
        let header_crc = crc32c::crc32c(&out);
        let data_crc = crc32c::crc32c(&self.data);
        out.extend_from_slice(&self.data);
        let index_offset = out.len() as u64;
        let index =
            postcard::to_stdvec(&self.batches).expect("a Vec of plain structs always serializes");
        out.extend_from_slice(&index);
        out.extend_from_slice(&index_offset.to_le_bytes());
        out.extend_from_slice(&(index.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(&index).to_le_bytes());
        out.extend_from_slice(&data_crc.to_le_bytes());
        out.extend_from_slice(&header_crc.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(TRAILER_MAGIC);
        let footer = SegmentFooter {
            stream: self.stream,
            partition: self.partition,
            encoding: self.encoding,
            base_offset: self.base_offset,
            end_offset: self.next_offset,
            data: HEADER_LEN..index_offset,
            batches: self.batches,
        };
        (out.freeze(), footer)
    }
}

fn le_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn le_u64(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(b)
}

/// Parses the last 40 bytes of a segment.
pub fn parse_trailer(tail: &[u8; 40]) -> Result<Trailer, LogError> {
    if &tail[32..40] != TRAILER_MAGIC {
        return Err(corrupt("bad segment trailer magic"));
    }
    if le_u64(tail, 24) != 0 {
        return Err(corrupt("segment trailer reserved field is not zero"));
    }
    Ok(Trailer {
        index_offset: le_u64(tail, 0),
        index_len: le_u32(tail, 8),
        index_crc32c: le_u32(tail, 12),
        data_crc32c: le_u32(tail, 16),
        header_crc32c: le_u32(tail, 20),
    })
}

/// Checks a segment's header and index against its trailer and returns the
/// footer. Does not read the data region (see [`parse`] for a full check).
pub fn parse_footer(
    header: &[u8],
    index: &[u8],
    trailer: &Trailer,
) -> Result<SegmentFooter, LogError> {
    if header.len() != HEADER_LEN as usize || &header[..8] != MAGIC {
        return Err(corrupt("not a segment"));
    }
    if crc32c::crc32c(header) != trailer.header_crc32c {
        return Err(corrupt("segment header checksum mismatch"));
    }
    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != VERSION {
        return Err(corrupt(format!(
            "unsupported segment format version {version}"
        )));
    }
    let encoding = Encoding::try_from(header[10])?;
    encoding.ensure_readable()?;
    if header[11] != 0 {
        return Err(corrupt("segment header reserved byte is not zero"));
    }
    if index.len() != trailer.index_len as usize || trailer.index_offset < HEADER_LEN {
        return Err(corrupt("segment index length mismatch"));
    }
    if crc32c::crc32c(index) != trailer.index_crc32c {
        return Err(corrupt("segment index checksum mismatch"));
    }
    let (batches, rest): (Vec<BatchIndexEntry>, _) =
        postcard::take_from_bytes(index).map_err(|e| corrupt(format!("bad segment index: {e}")))?;
    if !rest.is_empty() {
        return Err(corrupt("trailing bytes after the segment index"));
    }
    let footer = SegmentFooter {
        stream: StreamId(le_u64(header, 12)),
        partition: le_u32(header, 20),
        encoding,
        base_offset: le_u64(header, 24),
        end_offset: le_u64(header, 32),
        data: HEADER_LEN..trailer.index_offset,
        batches,
    };
    check_index(&footer)?;
    Ok(footer)
}

/// Checks that the batch index tiles the data region and the offset range.
fn check_index(footer: &SegmentFooter) -> Result<(), LogError> {
    let bad = |what: &str| Err(corrupt(format!("bad segment index: {what}")));
    if footer.end_offset < footer.base_offset {
        return bad("end offset below base offset");
    }
    let Some(first) = footer.batches.first() else {
        if footer.end_offset == footer.base_offset && footer.data.is_empty() {
            return Ok(());
        }
        return bad("no batches");
    };
    if first.base_offset != footer.base_offset || first.position != footer.data.start {
        return bad("first batch misplaced");
    }
    for pair in footer.batches.windows(2) {
        if pair[1].base_offset <= pair[0].base_offset || pair[1].position <= pair[0].position {
            return bad("batches out of order");
        }
    }
    let last = footer.batches.last().unwrap_or(first);
    if last.base_offset >= footer.end_offset || last.position >= footer.data.end {
        return bad("last batch out of range");
    }
    Ok(())
}

/// Parses a whole segment, checking every checksum and that each batch in
/// the data region matches the index.
pub fn parse(bytes: &[u8]) -> Result<SegmentFooter, LogError> {
    let len = bytes.len() as u64;
    if len < HEADER_LEN + TRAILER_LEN {
        return Err(corrupt(format!("segment truncated: {len} bytes")));
    }
    let tail: &[u8; 40] = bytes[bytes.len() - TRAILER_LEN as usize..]
        .try_into()
        .map_err(|_| corrupt("segment trailer"))?;
    let trailer = parse_trailer(tail)?;
    let index_range = trailer.index_range(len)?;
    let index = &bytes[index_range.start as usize..index_range.end as usize];
    let footer = parse_footer(&bytes[..HEADER_LEN as usize], index, &trailer)?;
    let data = &bytes[footer.data.start as usize..footer.data.end as usize];
    if crc32c::crc32c(data) != trailer.data_crc32c {
        return Err(corrupt("segment data checksum mismatch"));
    }
    let mut parsed = batch::batches(data);
    for i in 0..footer.batches.len() {
        let batch = parsed
            .next()
            .ok_or_else(|| corrupt("segment data has fewer batches than its index"))??;
        let range = footer.batch_range(i);
        let expected_records = footer.batch_end_offset(i) - footer.batches[i].base_offset;
        if batch.bytes.len() as u64 != range.end - range.start
            || u64::from(batch.record_count) != expected_records
            || batch::base_offset(batch.bytes) != Some(footer.batches[i].base_offset)
        {
            return Err(corrupt(format!(
                "segment batch {i} does not match the index"
            )));
        }
    }
    if parsed.next().is_some() {
        return Err(corrupt("segment data has more batches than its index"));
    }
    Ok(footer)
}
