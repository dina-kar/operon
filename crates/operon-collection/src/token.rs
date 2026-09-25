//! Consistency tokens (overview §6.5, plan M1.1 Ruling 20).

use std::fmt;
use std::str::FromStr;

use operon_common::StreamId;

/// The HTTP header that carries a token on write responses and read requests.
pub const CONSISTENCY_TOKEN_HEADER: &str = "Operon-Consistency-Token";

/// What a write made durable: per `(stream, partition)`, the next offset after
/// the write. A read that waits for these offsets sees the write.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ConsistencyToken(pub Vec<(StreamId, u32, u64)>);

const PREFIX: &str = "v1:";

impl ConsistencyToken {
    /// Sorted by `(stream, partition)`, one entry per pair: duplicates keep
    /// the max offset.
    pub fn normalized(mut self) -> Self {
        self.0.sort_unstable();
        // Sorted ascending, so the last entry of each pair has the max offset.
        let mut items: Vec<(StreamId, u32, u64)> = Vec::with_capacity(self.0.len());
        for item in self.0 {
            match items.last_mut() {
                Some(last) if (last.0, last.1) == (item.0, item.1) => *last = item,
                _ => items.push(item),
            }
        }
        ConsistencyToken(items)
    }

    /// Adds `other`'s offsets; the result is normalized, so a pair both
    /// tokens name keeps the larger offset.
    pub fn merge(&mut self, other: &ConsistencyToken) {
        self.0.extend_from_slice(&other.0);
        *self = std::mem::take(self).normalized();
    }

    /// The offset for `(stream, partition)`, the max one if it is listed more
    /// than once; `None` if the token does not name it.
    pub fn offset(&self, stream: StreamId, partition: u32) -> Option<u64> {
        self.0
            .iter()
            .filter(|&&(s, p, _)| (s, p) == (stream, partition))
            .map(|&(_, _, offset)| offset)
            .max()
    }
}

/// `v1:` followed by `s<stream>/p<partition>@<offset>` items joined by `,`,
/// in the token's order.
impl fmt::Display for ConsistencyToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PREFIX)?;
        for (i, (stream, partition, offset)) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "s{stream}/p{partition}@{offset}")?;
        }
        Ok(())
    }
}

/// A token string that is not the `v1:` text form.
#[derive(Debug, thiserror::Error)]
#[error("invalid consistency token {0:?}")]
pub struct TokenParseError(pub String);

/// Parses the [`Display`](fmt::Display) form. Every number must be canonical
/// decimal (no sign, no leading zeros), so a token has exactly one spelling;
/// the items are kept in the order given.
impl FromStr for ConsistencyToken {
    type Err = TokenParseError;

    fn from_str(s: &str) -> Result<Self, TokenParseError> {
        let invalid = || TokenParseError(s.to_string());
        let body = s.strip_prefix(PREFIX).ok_or_else(invalid)?;
        if body.is_empty() {
            return Ok(ConsistencyToken::default());
        }
        body.split(',')
            .map(|item| parse_item(item).ok_or_else(invalid))
            .collect::<Result<_, _>>()
            .map(ConsistencyToken)
    }
}

/// `s<stream>/p<partition>@<offset>`.
fn parse_item(item: &str) -> Option<(StreamId, u32, u64)> {
    let (stream, rest) = item.strip_prefix('s')?.split_once("/p")?;
    let (partition, offset) = rest.split_once('@')?;
    Some((
        stream.parse().ok()?,
        u32::try_from(canonical_u64(partition)?).ok()?,
        canonical_u64(offset)?,
    ))
}

/// A decimal `u64` with no sign and no leading zeros (except `"0"`).
fn canonical_u64(s: &str) -> Option<u64> {
    let canonical =
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && (s == "0" || !s.starts_with('0'));
    canonical.then(|| s.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(items: &[(u64, u32, u64)]) -> ConsistencyToken {
        ConsistencyToken(
            items
                .iter()
                .map(|&(stream, partition, offset)| (StreamId(stream), partition, offset))
                .collect(),
        )
    }

    #[test]
    fn tokens_display_as_specified() {
        let one = ConsistencyToken(vec![(StreamId(7), 3, 918274)]);
        assert_eq!(one.to_string(), "v1:s7/p3@918274");
        assert_eq!("v1:s7/p3@918274".parse::<ConsistencyToken>().unwrap(), one);

        let two = token(&[(0, 0, 0), (7, 3, 918274)]);
        assert_eq!(two.to_string(), "v1:s0/p0@0,s7/p3@918274");
        assert_eq!(two.to_string().parse::<ConsistencyToken>().unwrap(), two);

        let max = token(&[(u64::MAX, u32::MAX, u64::MAX)]);
        assert_eq!(max.to_string().parse::<ConsistencyToken>().unwrap(), max);
    }

    #[test]
    fn an_empty_token_is_v1_colon() {
        assert_eq!(ConsistencyToken::default().to_string(), "v1:");
        assert_eq!(
            "v1:".parse::<ConsistencyToken>().unwrap(),
            ConsistencyToken::default()
        );
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        for bad in [
            "v2:s1/p0@1",
            "v1:s1/p0",
            "v1:s01/p0@1",
            "v1:s1/p0@1,",
            "",
            "v1",
            "s1/p0@1",
            "v1:,s1/p0@1",
            "v1:s1/p00@1",
            "v1:s1/p0@01",
            "v1:s1/p4294967296@1",
            "v1:s18446744073709551616/p0@1",
            "v1:s+1/p0@1",
            "v1:s1/p0@-1",
            "v1:s1p0@1",
            "v1:S1/P0@1",
            "v1: s1/p0@1",
            "v1:s1/p0@1@2",
            "v1:s/p0@1",
        ] {
            let err = bad.parse::<ConsistencyToken>().unwrap_err();
            assert_eq!(err.0, bad);
        }
    }

    #[test]
    fn merge_keeps_the_max_offset() {
        let mut merged = token(&[(7, 3, 10), (1, 0, 5)]);
        merged.merge(&token(&[(7, 3, 4), (7, 1, 2), (1, 0, 9)]));
        assert_eq!(merged, token(&[(1, 0, 9), (7, 1, 2), (7, 3, 10)]));
        assert_eq!(merged.offset(StreamId(7), 3), Some(10));
        assert_eq!(merged.offset(StreamId(1), 0), Some(9));
        assert_eq!(merged.offset(StreamId(1), 1), None);

        // normalized sorts by (stream, partition) and keeps the max of duplicates.
        assert_eq!(
            token(&[(2, 0, 1), (1, 5, 3), (2, 0, 8), (1, 5, 2)]).normalized(),
            token(&[(1, 5, 3), (2, 0, 8)])
        );
        // offset is the max even before normalizing.
        assert_eq!(
            token(&[(2, 0, 8), (2, 0, 1)]).offset(StreamId(2), 0),
            Some(8)
        );

        let mut empty = ConsistencyToken::default();
        empty.merge(&ConsistencyToken::default());
        assert_eq!(empty, ConsistencyToken::default());
    }
}
