//! Kafka `RecordBatch` v2 encoding (magic 2, uncompressed, `CreateTime`).
//!
//! Batches are written with `baseOffset = 0`; readers and the segmenter patch
//! bytes 0..8 with the absolute offset. `baseOffset` and
//! `partitionLeaderEpoch` sit outside the batch CRC, so patching is free
//! (M0.3 plan, ruling 2).
//!
//! Layout (big-endian): `baseOffset i64 | batchLength i32 | partitionLeaderEpoch i32
//! | magic i8 | crc u32 | attributes i16 | lastOffsetDelta i32 | baseTimestamp i64
//! | maxTimestamp i64 | producerId i64 | producerEpoch i16 | baseSequence i32
//! | recordCount i32 | records`, where the crc32c covers `attributes` to the
//! end and each record is `length varint | attributes i8 | timestampDelta varlong
//! | offsetDelta varint | key | value | headers`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{LogError, corrupt};
use crate::record::{OffsetRecord, Record};

/// Bytes before the records: the fixed batch header.
pub const HEADER_LEN: usize = 61;
/// `baseOffset` and `batchLength`, which `batchLength` does not count.
const LOG_OVERHEAD: usize = 12;
const MAGIC_OFFSET: usize = 16;
const CRC_OFFSET: usize = 17;
/// The CRC covers everything from the attributes on.
const ATTRIBUTES_OFFSET: usize = 21;
const LAST_OFFSET_DELTA_OFFSET: usize = 23;
const BASE_TIMESTAMP_OFFSET: usize = 27;
const MAX_TIMESTAMP_OFFSET: usize = 35;
const RECORD_COUNT_OFFSET: usize = 57;
const MAGIC: u8 = 2;
/// No producer id, epoch, sequence or leader epoch: a non-idempotent batch.
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;
const NO_PARTITION_LEADER_EPOCH: i32 = -1;
/// The compression bits of the attributes.
const COMPRESSION_MASK: i16 = 0x07;

fn len_i32(what: &str, len: usize) -> Result<i32, LogError> {
    i32::try_from(len).map_err(|_| LogError::InvalidArgument(format!("{what} is too large")))
}

fn put_varint(buf: &mut Vec<u8>, value: i32) {
    put_varlong(buf, i64::from(value));
}

fn put_varlong(buf: &mut Vec<u8>, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    while zigzag >= 0x80 {
        buf.push((zigzag as u8) | 0x80);
        zigzag >>= 7;
    }
    buf.push(zigzag as u8);
}

fn put_bytes(buf: &mut Vec<u8>, what: &str, bytes: Option<&[u8]>) -> Result<(), LogError> {
    match bytes {
        None => put_varint(buf, -1),
        Some(bytes) => {
            put_varint(buf, len_i32(what, bytes.len())?);
            buf.extend_from_slice(bytes);
        }
    }
    Ok(())
}

fn encode_record(
    out: &mut Vec<u8>,
    record: &Record,
    offset_delta: i32,
    base_timestamp: i64,
) -> Result<(), LogError> {
    let timestamp_delta = record
        .timestamp_ms
        .checked_sub(base_timestamp)
        .ok_or_else(|| LogError::InvalidArgument("timestamps too far apart".to_string()))?;
    let mut body = Vec::new();
    body.push(0); // record attributes, unused
    put_varlong(&mut body, timestamp_delta);
    put_varint(&mut body, offset_delta);
    put_bytes(&mut body, "record key", record.key.as_deref())?;
    put_bytes(&mut body, "record value", record.value.as_deref())?;
    put_varint(&mut body, len_i32("header count", record.headers.len())?);
    for (key, value) in &record.headers {
        put_varint(&mut body, len_i32("header key", key.len())?);
        body.extend_from_slice(key.as_bytes());
        put_bytes(&mut body, "header value", value.as_deref())?;
    }
    put_varint(out, len_i32("record", body.len())?);
    out.extend_from_slice(&body);
    Ok(())
}

/// Encodes `records` as one `RecordBatch` v2 with `baseOffset = 0`.
pub fn encode(records: &[Record]) -> Result<Bytes, LogError> {
    let (first, _) = records
        .split_first()
        .ok_or_else(|| LogError::InvalidArgument("a batch needs at least one record".into()))?;
    let count = len_i32("record count", records.len())?;
    let base_timestamp = first.timestamp_ms;
    let max_timestamp = records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(base_timestamp);
    let mut body = Vec::new();
    for (delta, record) in (0..count).zip(records) {
        encode_record(&mut body, record, delta, base_timestamp)?;
    }

    let batch_length = len_i32("batch", HEADER_LEN - LOG_OVERHEAD + body.len())?;
    let mut out = BytesMut::with_capacity(HEADER_LEN + body.len());
    out.put_i64(0);
    out.put_i32(batch_length);
    out.put_i32(NO_PARTITION_LEADER_EPOCH);
    out.put_u8(MAGIC);
    out.put_u32(0); // crc, filled in below
    out.put_i16(0); // attributes: no compression, CreateTime
    out.put_i32(count - 1);
    out.put_i64(base_timestamp);
    out.put_i64(max_timestamp);
    out.put_i64(NO_PRODUCER_ID);
    out.put_i16(NO_PRODUCER_EPOCH);
    out.put_i32(NO_SEQUENCE);
    out.put_i32(count);
    out.extend_from_slice(&body);
    let crc = crc32c::crc32c(&out[ATTRIBUTES_OFFSET..]);
    out[CRC_OFFSET..ATTRIBUTES_OFFSET].copy_from_slice(&crc.to_be_bytes());
    Ok(out.freeze())
}

/// Sets a batch's `baseOffset` field (bytes 0..8). Does nothing to a slice
/// shorter than 8 bytes.
pub fn patch_base_offset(batch: &mut [u8], base_offset: u64) {
    if let Some(field) = batch.get_mut(..8) {
        field.copy_from_slice(&base_offset.to_be_bytes());
    }
}

/// The `baseOffset` field of a batch (bytes 0..8).
pub fn base_offset(batch: &[u8]) -> Option<u64> {
    batch
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

/// One well-formed, CRC-checked batch inside a byte buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchRef<'a> {
    /// The whole batch, header included.
    pub bytes: &'a [u8],
    pub record_count: u32,
    pub max_timestamp_ms: i64,
}

fn be_i16(bytes: &[u8], at: usize) -> i16 {
    i16::from_be_bytes([bytes[at], bytes[at + 1]])
}

fn be_i32(bytes: &[u8], at: usize) -> i32 {
    i32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn be_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn be_i64(bytes: &[u8], at: usize) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    i64::from_be_bytes(b)
}

/// Checks the batch at the start of `bytes` and returns it.
fn parse_batch(bytes: &[u8]) -> Result<BatchRef<'_>, LogError> {
    if bytes.len() < HEADER_LEN {
        return Err(corrupt(format!(
            "record batch truncated: {} bytes",
            bytes.len()
        )));
    }
    let length = be_i32(bytes, 8);
    let total = usize::try_from(length)
        .ok()
        .and_then(|l| l.checked_add(LOG_OVERHEAD))
        .filter(|total| *total >= HEADER_LEN && *total <= bytes.len())
        .ok_or_else(|| corrupt(format!("bad record batch length {length}")))?;
    let bytes = &bytes[..total];
    if bytes[MAGIC_OFFSET] != MAGIC {
        return Err(corrupt(format!(
            "unsupported record batch magic {}",
            bytes[MAGIC_OFFSET]
        )));
    }
    if crc32c::crc32c(&bytes[ATTRIBUTES_OFFSET..]) != be_u32(bytes, CRC_OFFSET) {
        return Err(corrupt("record batch checksum mismatch"));
    }
    let attributes = be_i16(bytes, ATTRIBUTES_OFFSET);
    if attributes & COMPRESSION_MASK != 0 {
        return Err(corrupt(format!(
            "compressed record batches are not supported (attributes {attributes:#x})"
        )));
    }
    let count = be_i32(bytes, RECORD_COUNT_OFFSET);
    let last_delta = be_i32(bytes, LAST_OFFSET_DELTA_OFFSET);
    let record_count = u32::try_from(count)
        .ok()
        .filter(|c| *c > 0 && i64::from(*c) - 1 == i64::from(last_delta))
        .ok_or_else(|| {
            corrupt(format!(
                "bad record count {count} (last offset delta {last_delta})"
            ))
        })?;
    Ok(BatchRef {
        bytes,
        record_count,
        max_timestamp_ms: be_i64(bytes, MAX_TIMESTAMP_OFFSET),
    })
}

/// Splits concatenated batches, checking each one's framing and CRC. Stops
/// after the first error.
pub fn batches(bytes: &[u8]) -> impl Iterator<Item = Result<BatchRef<'_>, LogError>> {
    let mut rest = Some(bytes);
    std::iter::from_fn(move || {
        let current = rest.take()?;
        if current.is_empty() {
            return None;
        }
        match parse_batch(current) {
            Ok(batch) => {
                rest = Some(&current[batch.bytes.len()..]);
                Some(Ok(batch))
            }
            Err(err) => Some(Err(err)),
        }
    })
}

/// A bounds-checked reader over a record's bytes.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], LogError> {
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt("record truncated"))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn varlong(&mut self, max_bytes: u32) -> Result<i64, LogError> {
        let mut value: u64 = 0;
        for i in 0..max_bytes {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
            }
        }
        Err(corrupt("varint too long"))
    }

    fn varint(&mut self) -> Result<i32, LogError> {
        let value = self.varlong(5)?;
        i32::try_from(value).map_err(|_| corrupt("varint out of range"))
    }

    fn nullable_bytes(&mut self) -> Result<Option<&'a [u8]>, LogError> {
        match self.varint()? {
            -1 => Ok(None),
            len => {
                let len = usize::try_from(len).map_err(|_| corrupt("negative length"))?;
                self.take(len).map(Some)
            }
        }
    }

    fn is_done(&self) -> bool {
        self.at == self.bytes.len()
    }
}

fn decode_record(
    cursor: &mut Cursor<'_>,
    expected_delta: i32,
    base_timestamp: i64,
) -> Result<Record, LogError> {
    let len = usize::try_from(cursor.varint()?).map_err(|_| corrupt("negative record length"))?;
    let mut record = Cursor {
        bytes: cursor.take(len)?,
        at: 0,
    };
    record.take(1)?; // attributes
    let timestamp_delta = record.varlong(10)?;
    let offset_delta = record.varint()?;
    if offset_delta != expected_delta {
        return Err(corrupt(format!(
            "record offset delta {offset_delta}, expected {expected_delta}"
        )));
    }
    let timestamp_ms = base_timestamp
        .checked_add(timestamp_delta)
        .ok_or_else(|| corrupt("record timestamp overflows"))?;
    let key = record.nullable_bytes()?.map(Bytes::copy_from_slice);
    let value = record.nullable_bytes()?.map(Bytes::copy_from_slice);
    let header_count =
        usize::try_from(record.varint()?).map_err(|_| corrupt("negative header count"))?;
    let mut headers = Vec::with_capacity(header_count.min(64));
    for _ in 0..header_count {
        let key_len =
            usize::try_from(record.varint()?).map_err(|_| corrupt("negative header key length"))?;
        let key = std::str::from_utf8(record.take(key_len)?)
            .map_err(|_| corrupt("header key is not UTF-8"))?
            .to_string();
        let value = record.nullable_bytes()?.map(Bytes::copy_from_slice);
        headers.push((key, value));
    }
    if !record.is_done() {
        return Err(corrupt("trailing bytes in record"));
    }
    Ok(Record {
        key,
        value,
        headers,
        timestamp_ms,
    })
}

/// Decodes one checked batch; its records get offsets from `base_offset`.
pub(crate) fn decode_batch(
    batch: &BatchRef<'_>,
    base_offset: u64,
    out: &mut Vec<OffsetRecord>,
) -> Result<(), LogError> {
    let base_timestamp = be_i64(batch.bytes, BASE_TIMESTAMP_OFFSET);
    let mut cursor = Cursor {
        bytes: &batch.bytes[HEADER_LEN..],
        at: 0,
    };
    let count = i32::try_from(batch.record_count).map_err(|_| corrupt("record count"))?;
    for (delta, offset) in (0..count).zip(base_offset..) {
        let record = decode_record(&mut cursor, delta, base_timestamp)?;
        out.push(OffsetRecord { offset, record });
    }
    if !cursor.is_done() {
        return Err(corrupt("trailing bytes in record batch"));
    }
    Ok(())
}

/// Decodes concatenated batches, verifying every CRC. The records get
/// contiguous offsets starting at `base_offset`; the batches' own
/// `baseOffset` fields are ignored.
pub fn decode(bytes: &[u8], base_offset: u64) -> Result<Vec<OffsetRecord>, LogError> {
    let mut out = Vec::new();
    let mut next = base_offset;
    for batch in batches(bytes) {
        let batch = batch?;
        decode_batch(&batch, next, &mut out)?;
        next += u64::from(batch.record_count);
    }
    Ok(out)
}
