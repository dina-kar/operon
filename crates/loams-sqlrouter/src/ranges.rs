//! Keyspace-id ranges: Vitess shard names, partition checks, split and merge
//! (§31 §6.1, §12; D312).
//!
//! Each function here has a Lean counterpart in `spec/lean/LoamsRouter/KeyRange.lean`
//! with the same name, the same algorithm and the same error vocabulary:
//!
//! - [`validate_partition`] is Lean's `validate`. Lean proves `validate_iff`:
//!   it accepts exactly the lists that cover `[0, 2^64)` with adjacent,
//!   non-empty ranges in order.
//! - [`lookup`] is Lean's `lookup`. On a valid partition it always finds a
//!   range (`lookup_isSome`), the range contains the id (`lookup_spec`), and
//!   no other range does (`partition_total_unique`).
//! - [`split`] and [`merge`] are Lean's `split` and `merge`. Both keep a
//!   partition a partition (`split_partition`, `merge_partition`).
//!
//! The `lean_oracle` test runs both implementations on random inputs and
//! compares the answers.

use serde::{Deserialize, Serialize};

/// A Vitess keyspace id: 8 bytes compared big-endian, so numeric order on the
/// `u64` is the keyspace order.
pub type KeyspaceId = u64;

/// The keyspace ids `[lo, hi)`; `hi = None` runs to the end of the space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyRange {
    pub lo: KeyspaceId,
    pub hi: Option<KeyspaceId>,
}

impl KeyRange {
    /// The whole keyspace, Vitess shard `-`.
    pub const FULL: KeyRange = KeyRange { lo: 0, hi: None };

    pub fn contains(&self, id: KeyspaceId) -> bool {
        self.lo <= id && self.hi.is_none_or(|hi| id < hi)
    }
}

/// Why a list of ranges is not a partition of the keyspace. `at` is the
/// first uncovered id (`Gap`) or the start of a range that overlaps its
/// predecessor (`Overlap`). Same variants as Lean's `PartitionError`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    #[error("keyspace ids from {at:#018x} are not covered")]
    Gap { at: KeyspaceId },
    #[error("the range starting at {at:#018x} overlaps the one before it")]
    Overlap { at: KeyspaceId },
    #[error("a range ends where it starts or before")]
    NotSorted,
    #[error("no ranges")]
    Empty,
}

/// Why a split or merge was refused. Lean's `split` and `merge` return `none`
/// in the same cases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EditError {
    #[error("no range at index {0}")]
    BadIndex(usize),
    #[error("the split point {0:#018x} is not strictly inside the range")]
    NotInside(KeyspaceId),
    #[error("range {index} does not end where the next one starts")]
    NotAdjacent { index: usize },
}

/// A malformed Vitess shard name.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RangeError {
    #[error("shard name {0:?} is not <hex>-<hex>")]
    Syntax(String),
    #[error("shard name {0:?} has a bound longer than 8 bytes")]
    TooLong(String),
    #[error("shard name {0:?} is empty or reversed")]
    Empty(String),
}

/// Parse a Vitess shard name: `-80`, `80-`, `40-80`, or `-` for the whole
/// keyspace. Each bound is hex, left-aligned in 8 bytes (`80` is
/// `0x8000_0000_0000_0000`), case-insensitive, with an even number of digits.
pub fn parse_vitess_shard(name: &str) -> Result<KeyRange, RangeError> {
    let Some((lo, hi)) = name.split_once('-') else {
        return Err(RangeError::Syntax(name.to_owned()));
    };
    let bound = |s: &str| -> Result<Option<u64>, RangeError> {
        if s.is_empty() {
            return Ok(None);
        }
        if !s.len().is_multiple_of(2) || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(RangeError::Syntax(name.to_owned()));
        }
        if s.len() > 16 {
            return Err(RangeError::TooLong(name.to_owned()));
        }
        let v = u64::from_str_radix(s, 16).map_err(|_| RangeError::Syntax(name.to_owned()))?;
        // Left-align: `80` is the top byte. 2 to 16 digits, so the shift is 0..=56.
        Ok(Some(v << (64 - 4 * s.len() as u32)))
    };
    let lo = bound(lo)?.unwrap_or(0);
    let hi = bound(hi)?;
    if hi.is_some_and(|hi| hi <= lo) {
        return Err(RangeError::Empty(name.to_owned()));
    }
    Ok(KeyRange { lo, hi })
}

/// The Vitess shard name of a range: the inverse of [`parse_vitess_shard`],
/// with trailing zero bytes dropped (`[0x80…, end)` is `80-`).
pub fn vitess_shard_name(r: &KeyRange) -> String {
    let bound = |v: u64| -> String {
        if v == 0 {
            return String::new();
        }
        let bytes = v.to_be_bytes();
        let len = 8 - bytes.iter().rev().take_while(|&&b| b == 0).count();
        bytes[..len].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!("{}-{}", bound(r.lo), r.hi.map(bound).unwrap_or_default())
}

/// Check that `ranges`, in order, cover the keyspace exactly. The algorithm
/// is Lean's `validate`, so both report the same first error.
pub fn validate_partition(ranges: &[KeyRange]) -> Result<(), PartitionError> {
    let Some((last, init)) = ranges.split_last() else {
        return Err(PartitionError::Empty);
    };
    let mut start = 0u64;
    for (i, r) in init.iter().enumerate() {
        check_start(start, r)?;
        match r.hi {
            None => {
                return Err(PartitionError::Overlap {
                    at: ranges[i + 1].lo,
                });
            }
            Some(hi) if hi <= r.lo => return Err(PartitionError::NotSorted),
            Some(hi) => start = hi,
        }
    }
    check_start(start, last)?;
    match last.hi {
        None => Ok(()),
        Some(hi) if hi <= last.lo => Err(PartitionError::NotSorted),
        Some(hi) => Err(PartitionError::Gap { at: hi }),
    }
}

fn check_start(start: u64, r: &KeyRange) -> Result<(), PartitionError> {
    if start < r.lo {
        Err(PartitionError::Gap { at: start })
    } else if r.lo < start {
        Err(PartitionError::Overlap { at: r.lo })
    } else {
        Ok(())
    }
}

/// The index of the first range containing `id`. On a valid partition it is
/// always `Some`, and no other range contains `id`.
pub fn lookup(ranges: &[KeyRange], id: KeyspaceId) -> Option<usize> {
    ranges.iter().position(|r| r.contains(id))
}

/// Split range `index` at `at`, which must lie strictly inside it.
pub fn split(
    ranges: &[KeyRange],
    index: usize,
    at: KeyspaceId,
) -> Result<Vec<KeyRange>, EditError> {
    let r = *ranges.get(index).ok_or(EditError::BadIndex(index))?;
    if !(r.lo < at && r.hi.is_none_or(|hi| at < hi)) {
        return Err(EditError::NotInside(at));
    }
    let mut out = Vec::with_capacity(ranges.len() + 1);
    out.extend_from_slice(&ranges[..index]);
    out.push(KeyRange {
        lo: r.lo,
        hi: Some(at),
    });
    out.push(KeyRange { lo: at, hi: r.hi });
    out.extend_from_slice(&ranges[index + 1..]);
    Ok(out)
}

/// Merge range `index` with range `index + 1`, which must start where it ends.
pub fn merge(ranges: &[KeyRange], index: usize) -> Result<Vec<KeyRange>, EditError> {
    let (Some(a), Some(b)) = (ranges.get(index), ranges.get(index + 1)) else {
        return Err(EditError::BadIndex(index));
    };
    if a.hi != Some(b.lo) {
        return Err(EditError::NotAdjacent { index });
    }
    let mut out = Vec::with_capacity(ranges.len() - 1);
    out.extend_from_slice(&ranges[..index]);
    out.push(KeyRange { lo: a.lo, hi: b.hi });
    out.extend_from_slice(&ranges[index + 2..]);
    Ok(out)
}
