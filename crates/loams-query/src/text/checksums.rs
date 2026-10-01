//! Block checksums of a pinned split file (M1.3 row F3).
//!
//! The hot tier writes `<split file>.crc` next to every split it pins: the
//! crc32c of each [`SPLIT_CHECK_BLOCK`]-byte block of the file. The query
//! engine's [`LocalSplitStorage`](super::LocalSplitStorage) loads it when it
//! opens the file and checks every block a read touches, so a local file whose
//! bytes changed after the download (disk corruption, a stray write) is refused
//! before Tantivy parses it, and the split is read remotely instead. Tantivy
//! can panic on some corrupt data it trusts (fast-field warm-up, row 12.7);
//! checking only the blocks read keeps the cost to one crc32c pass over the
//! bytes a query already reads.
//!
//! Format (little-endian): `b"OPSC"`, version `u32` (1), block size `u32`,
//! file size `u64`, block count `u32`, one crc32c `u32` per block, then the
//! crc32c of everything before it.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The bytes of each checked block.
pub const SPLIT_CHECK_BLOCK: u32 = 64 << 10;

const MAGIC: &[u8; 4] = b"OPSC";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 4 + 4 + 4 + 8 + 4;

/// The checksum file of the split file `split`: `<split>.crc`.
pub fn checksums_path(split: &Path) -> PathBuf {
    let mut name: OsString = split.as_os_str().to_owned();
    name.push(".crc");
    PathBuf::from(name)
}

/// The block checksums of one split file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitChecksums {
    block: u32,
    size: u64,
    crcs: Vec<u32>,
}

impl SplitChecksums {
    /// The checksums of `bytes`, a whole file.
    pub fn of(bytes: &[u8]) -> Self {
        let mut builder = SplitChecksumsBuilder::default();
        builder.update(bytes);
        builder.finish()
    }

    /// The file size the checksums cover.
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + 4 * self.crcs.len() + 4);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.block.to_le_bytes());
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&(self.crcs.len() as u32).to_le_bytes());
        for crc in &self.crcs {
            out.extend_from_slice(&crc.to_le_bytes());
        }
        let total = crc32c::crc32c(&out);
        out.extend_from_slice(&total.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < HEADER_LEN + 4 {
            return Err(format!("{} bytes cannot hold split checksums", bytes.len()));
        }
        let (body, total) = bytes.split_at(bytes.len() - 4);
        if crc32c::crc32c(body) != u32::from_le_bytes(total.try_into().expect("4 bytes")) {
            return Err("the split checksums fail their own checksum".to_string());
        }
        let u32_at = |at: usize| u32::from_le_bytes(body[at..at + 4].try_into().expect("4 bytes"));
        if &body[..4] != MAGIC || u32_at(4) != VERSION {
            return Err("not split checksums (magic or version)".to_string());
        }
        let block = u32_at(8);
        let size = u64::from_le_bytes(body[12..20].try_into().expect("8 bytes"));
        let count = u32_at(20) as usize;
        if block == 0
            || body.len() != HEADER_LEN + 4 * count
            || size.div_ceil(u64::from(block)) != count as u64
        {
            return Err("split checksums with an inconsistent header".to_string());
        }
        let crcs = (0..count).map(|i| u32_at(HEADER_LEN + 4 * i)).collect();
        Ok(Self { block, size, crcs })
    }

    /// Writes the checksums of the split file `split` (to [`checksums_path`]).
    pub fn write_for(&self, split: &Path) -> std::io::Result<()> {
        std::fs::write(checksums_path(split), self.encode())
    }

    /// Reads the checksums of the split file `split`.
    pub fn read_for(split: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(checksums_path(split))?;
        Self::decode(&bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
    }

    /// `range` widened to whole blocks (and cut at the file's end).
    pub fn aligned(&self, start: u64, end: u64) -> (u64, u64) {
        let block = u64::from(self.block);
        let from = start / block * block;
        let to = end.div_ceil(block).saturating_mul(block).min(self.size);
        (from, to.max(from))
    }

    /// Checks `bytes`, read at `offset` (a block boundary) and ending at a
    /// block boundary or the file's end.
    pub fn verify(&self, offset: u64, bytes: &[u8]) -> Result<(), String> {
        let block = u64::from(self.block);
        if !offset.is_multiple_of(block) {
            return Err(format!("a checked read at {offset} is not block-aligned"));
        }
        let first = (offset / block) as usize;
        for (i, chunk) in bytes.chunks(self.block as usize).enumerate() {
            let index = first + i;
            let expected = self.crcs.get(index).ok_or_else(|| {
                format!(
                    "block {index} is past the {} checked blocks",
                    self.crcs.len()
                )
            })?;
            let full = (u64::from(self.block)).min(self.size - index as u64 * block);
            if chunk.len() as u64 != full {
                return Err(format!(
                    "block {index} is {} bytes, expected {full}",
                    chunk.len()
                ));
            }
            if crc32c::crc32c(chunk) != *expected {
                return Err(format!("block {index} fails its checksum"));
            }
        }
        Ok(())
    }
}

/// Builds [`SplitChecksums`] from a file's bytes, fed in order in pieces of
/// any size (a download's pieces).
#[derive(Debug, Default)]
pub struct SplitChecksumsBuilder {
    size: u64,
    crcs: Vec<u32>,
    /// The crc32c of the current partial block, and its length.
    partial: (u32, u32),
}

impl SplitChecksumsBuilder {
    pub fn update(&mut self, mut bytes: &[u8]) {
        self.size += bytes.len() as u64;
        while !bytes.is_empty() {
            let (crc, len) = self.partial;
            let take = ((SPLIT_CHECK_BLOCK - len) as usize).min(bytes.len());
            let crc = crc32c::crc32c_append(crc, &bytes[..take]);
            let len = len + take as u32;
            bytes = &bytes[take..];
            if len == SPLIT_CHECK_BLOCK {
                self.crcs.push(crc);
                self.partial = (0, 0);
            } else {
                self.partial = (crc, len);
            }
        }
    }

    pub fn finish(mut self) -> SplitChecksums {
        if self.partial.1 > 0 {
            self.crcs.push(self.partial.0);
        }
        SplitChecksums {
            block: SPLIT_CHECK_BLOCK,
            size: self.size,
            crcs: self.crcs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + i / 300) as u8).collect()
    }

    #[test]
    fn pieces_of_any_size_give_the_same_checksums() {
        let data = bytes(3 * SPLIT_CHECK_BLOCK as usize + 1234);
        let whole = SplitChecksums::of(&data);
        let mut builder = SplitChecksumsBuilder::default();
        for piece in data.chunks(10_007) {
            builder.update(piece);
        }
        assert_eq!(builder.finish(), whole);
        assert_eq!(SplitChecksums::decode(&whole.encode()), Ok(whole.clone()));
        let mut bad = whole.encode();
        bad[HEADER_LEN] ^= 1;
        assert!(SplitChecksums::decode(&bad).is_err());
    }

    #[test]
    fn a_changed_byte_fails_the_block_that_holds_it() {
        let mut data = bytes(2 * SPLIT_CHECK_BLOCK as usize + 99);
        let sums = SplitChecksums::of(&data);
        let (from, to) = sums.aligned(10, 20);
        assert_eq!((from, to), (0, u64::from(SPLIT_CHECK_BLOCK)));
        let (from, to) = sums.aligned(u64::from(SPLIT_CHECK_BLOCK) * 2 + 5, data.len() as u64);
        assert_eq!(to, data.len() as u64);
        sums.verify(from, &data[from as usize..to as usize])
            .expect("the tail block");
        sums.verify(0, &data).expect("intact");
        data[SPLIT_CHECK_BLOCK as usize + 3] ^= 0x40;
        assert!(sums.verify(0, &data[..SPLIT_CHECK_BLOCK as usize]).is_ok());
        assert!(sums.verify(0, &data).is_err());
        assert!(sums.verify(1, &data[1..]).is_err(), "unaligned");
    }
}
