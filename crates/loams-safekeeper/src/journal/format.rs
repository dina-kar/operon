//! The journal's on-disk format (§28 §7.2, D265), little-endian:
//!
//! ```text
//! segment  = header block (4 KiB) | flush unit* | zeroes
//! header   = magic b"LOAMJNL1" | version u32 = 1 | block u32 | seq u64 | size u64
//!            | crc32c(the 32 bytes before) u32 | zeroes to 4 KiB
//! unit     = record+ | zeroes to the next 4 KiB boundary
//! record   = len u32 | crc u32 | kind u8 | 7 zero bytes | timeline [32] | term u64
//!            | lsn u64 | aux u64 | aux2 u64 | payload (len bytes) | zeroes to 8 bytes
//! ```
//!
//! A record's CRC32C covers the segment's sequence number, then header bytes
//! `8..80` and the payload, so a record left over in a recycled segment never
//! validates under the segment's new number. The kinds:
//!
//! | kind | `lsn` | `aux` | `aux2` | payload |
//! |---|---|---|---|---|
//! | 1 Append | begin LSN | proposer's `commit_lsn` | proposer's `truncate_lsn` | WAL bytes |
//! | 2 Truncate | truncation LSN | – | – | – |
//! | 3 Progress | `commit_lsn` | `backup_lsn` | `remote_consistent_lsn` | – |
//!
//! A zero `kind` is padding: at a block boundary it ends the segment's data,
//! elsewhere it pads to the next boundary.

use crate::types::{Lsn, Term, TimelineId};

/// The flush-unit alignment: what `O_DIRECT` needs on every drive we target.
pub const BLOCK: usize = 4096;
/// The segment header's size (one block).
pub const SEGMENT_HEADER: usize = BLOCK;
/// A record header's size.
pub const RECORD_HEADER: usize = 80;

const MAGIC: &[u8; 8] = b"LOAMJNL1";
const VERSION: u32 = 1;

/// A record's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Append = 1,
    Truncate = 2,
    Progress = 3,
}

impl Kind {
    fn from_u8(v: u8) -> Option<Kind> {
        match v {
            1 => Some(Kind::Append),
            2 => Some(Kind::Truncate),
            3 => Some(Kind::Progress),
            _ => None,
        }
    }
}

/// A decoded record header (the payload follows it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub kind: Kind,
    pub tl: TimelineId,
    pub term: Term,
    pub lsn: Lsn,
    pub aux: u64,
    pub aux2: u64,
    pub len: u32,
}

/// Round `n` up to a multiple of `to` (a power of two).
pub const fn align_up(n: usize, to: usize) -> usize {
    (n + to - 1) & !(to - 1)
}

/// The bytes a record with `payload` bytes takes, padding included.
pub const fn record_size(payload: usize) -> usize {
    RECORD_HEADER + align_up(payload, 8)
}

fn seed(seq: u64) -> u32 {
    crc32c::crc32c(&seq.to_le_bytes())
}

/// Append one record to `out` (which must end on an 8-byte boundary). The
/// payload is the concatenation of `payload`'s slices.
pub fn put_record(out: &mut impl super::Sink, seq: u64, h: &RecordHeader, payload: &[&[u8]]) {
    debug_assert_eq!(out.len() % 8, 0);
    let len: usize = payload.iter().map(|p| p.len()).sum();
    debug_assert_eq!(h.len as usize, len);
    let start = out.len();
    out.put(&h.len.to_le_bytes());
    out.put(&[0; 4]); // the CRC, filled in below
    out.put(&[h.kind as u8, 0, 0, 0, 0, 0, 0, 0]);
    out.put(&h.tl.to_bytes());
    out.put(&h.term.to_le_bytes());
    out.put(&h.lsn.0.to_le_bytes());
    out.put(&h.aux.to_le_bytes());
    out.put(&h.aux2.to_le_bytes());
    for p in payload {
        out.put(p);
    }
    let crc = crc32c::crc32c_append(seed(seq), out.tail_from(start + 8));
    out.patch(start + 4, &crc.to_le_bytes());
    out.zero_to(start + record_size(len));
}

/// What [`parse_record`] found at a position.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed<'a> {
    /// A valid record and its payload; the next record starts `size` bytes on.
    Record {
        header: RecordHeader,
        payload: &'a [u8],
        size: usize,
    },
    /// Padding: continue at the next block boundary.
    Pad,
    /// The end of the valid data: zeroes at a block boundary, a torn or
    /// stale record, or a truncated buffer.
    End,
}

/// Parse the record at the start of `buf`, which sits at segment offset
/// `offset` in segment `seq`.
pub fn parse_record(buf: &[u8], seq: u64, offset: usize) -> Parsed<'_> {
    if buf.len() < RECORD_HEADER {
        return Parsed::End;
    }
    let u32_at = |i: usize| u32::from_le_bytes(buf[i..i + 4].try_into().unwrap_or_default());
    let u64_at = |i: usize| u64::from_le_bytes(buf[i..i + 8].try_into().unwrap_or_default());
    // With only eight padding bytes, buf[8] belongs to the next unit.
    // Still validate a record first: a zero-length record with CRC zero
    // has the same first eight bytes and may cross a block boundary.
    let invalid = if offset % BLOCK == BLOCK - 8 && buf[..8] == [0; 8] {
        Parsed::Pad
    } else {
        Parsed::End
    };
    if buf[8] == 0 {
        return if offset.is_multiple_of(BLOCK) {
            Parsed::End
        } else {
            Parsed::Pad
        };
    }
    let Some(kind) = Kind::from_u8(buf[8]) else {
        return invalid;
    };
    let len = u32_at(0);
    let size = record_size(len as usize);
    if buf.len() < RECORD_HEADER + len as usize {
        return Parsed::End;
    }
    let crc = crc32c::crc32c_append(seed(seq), &buf[8..RECORD_HEADER + len as usize]);
    if crc != u32_at(4) {
        return invalid;
    }
    let mut tl = [0u8; 32];
    tl.copy_from_slice(&buf[16..48]);
    let mut tenant = [0u8; 16];
    let mut timeline = [0u8; 16];
    tenant.copy_from_slice(&tl[..16]);
    timeline.copy_from_slice(&tl[16..]);
    Parsed::Record {
        header: RecordHeader {
            kind,
            tl: TimelineId::new(crate::types::Id(tenant), crate::types::Id(timeline)),
            term: u64_at(48),
            lsn: Lsn(u64_at(56)),
            aux: u64_at(64),
            aux2: u64_at(72),
            len,
        },
        payload: &buf[RECORD_HEADER..RECORD_HEADER + len as usize],
        size,
    }
}

/// A segment's header block.
pub fn segment_header(seq: u64, size: u64) -> Vec<u8> {
    let mut h = Vec::with_capacity(SEGMENT_HEADER);
    h.extend_from_slice(MAGIC);
    h.extend_from_slice(&VERSION.to_le_bytes());
    h.extend_from_slice(&(BLOCK as u32).to_le_bytes());
    h.extend_from_slice(&seq.to_le_bytes());
    h.extend_from_slice(&size.to_le_bytes());
    let crc = crc32c::crc32c(&h);
    h.extend_from_slice(&crc.to_le_bytes());
    h.resize(SEGMENT_HEADER, 0);
    h
}

/// The `(seq, size)` a header block names, if it is valid.
pub fn parse_segment_header(h: &[u8]) -> Option<(u64, u64)> {
    if h.len() < 36 || &h[..8] != MAGIC {
        return None;
    }
    let crc = u32::from_le_bytes(h[32..36].try_into().ok()?);
    if crc != crc32c::crc32c(&h[..32]) {
        return None;
    }
    let version = u32::from_le_bytes(h[8..12].try_into().ok()?);
    let block = u32::from_le_bytes(h[12..16].try_into().ok()?);
    if version != VERSION || block as usize != BLOCK {
        return None;
    }
    let seq = u64::from_le_bytes(h[16..24].try_into().ok()?);
    let size = u64::from_le_bytes(h[24..32].try_into().ok()?);
    Some((seq, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Id;

    fn hdr(kind: Kind, len: usize) -> RecordHeader {
        RecordHeader {
            kind,
            tl: TimelineId::new(Id([1; 16]), Id([2; 16])),
            term: 7,
            lsn: Lsn(0x1234),
            aux: 5,
            aux2: 6,
            len: len as u32,
        }
    }

    #[test]
    fn a_record_round_trips_and_pads_to_eight() {
        let mut out: Vec<u8> = Vec::new();
        put_record(
            &mut out,
            3,
            &hdr(Kind::Append, 5),
            &[&b"hel"[..], &b"lo"[..]],
        );
        assert_eq!(out.len(), RECORD_HEADER + 8);
        match parse_record(&out, 3, SEGMENT_HEADER) {
            Parsed::Record {
                header,
                payload,
                size,
            } => {
                assert_eq!(header, hdr(Kind::Append, 5));
                assert_eq!(payload, b"hello");
                assert_eq!(size, out.len());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn another_segment_number_or_a_flipped_bit_does_not_validate() {
        let mut out: Vec<u8> = Vec::new();
        put_record(&mut out, 3, &hdr(Kind::Progress, 0), &[]);
        assert_eq!(parse_record(&out, 4, SEGMENT_HEADER), Parsed::End);
        out[60] ^= 1;
        assert_eq!(parse_record(&out, 3, SEGMENT_HEADER), Parsed::End);
    }

    #[test]
    fn zeroes_end_at_a_block_and_pad_inside_one() {
        let z = vec![0u8; 128];
        assert_eq!(parse_record(&z, 1, BLOCK), Parsed::End);
        assert_eq!(parse_record(&z, 1, BLOCK + 88), Parsed::Pad);
        assert_eq!(parse_record(&z[..10], 1, BLOCK + 88), Parsed::End);
    }

    #[test]
    fn eight_padding_bytes_before_an_append_skip_to_the_next_block() {
        let mut out = vec![0; 8];
        let h = hdr(Kind::Append, 3000);
        put_record(&mut out, 1, &h, &[&[7; 3000]]);
        assert_eq!(parse_record(&out, 1, BLOCK - 8), Parsed::Pad);
        assert!(matches!(
            parse_record(&out[8..], 1, BLOCK),
            Parsed::Record { header, .. } if header == h
        ));
    }

    #[test]
    fn a_zero_crc_record_crossing_a_block_boundary_is_not_padding() {
        let mut h = hdr(Kind::Progress, 0);
        // CRC32C of segment 1 and this header is zero, so the length and
        // CRC fields look exactly like eight bytes of padding.
        h.aux = 0x8970_dad8;
        let mut out = Vec::new();
        put_record(&mut out, 1, &h, &[]);
        assert_eq!(&out[..8], &[0; 8]);
        assert!(matches!(
            parse_record(&out, 1, BLOCK - 8),
            Parsed::Record { header, .. } if header == h
        ));
    }

    #[test]
    fn a_truncated_record_is_the_end() {
        let mut out: Vec<u8> = Vec::new();
        put_record(&mut out, 1, &hdr(Kind::Append, 100), &[&[9u8; 100][..]]);
        assert_eq!(parse_record(&out[..150], 1, BLOCK), Parsed::End);
    }

    #[test]
    fn segment_headers_round_trip() {
        let h = segment_header(42, 64 << 20);
        assert_eq!(h.len(), SEGMENT_HEADER);
        assert_eq!(parse_segment_header(&h), Some((42, 64 << 20)));
        let mut bad = h.clone();
        bad[20] ^= 1;
        assert_eq!(parse_segment_header(&bad), None);
    }
}
