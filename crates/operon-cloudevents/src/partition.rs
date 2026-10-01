//! Which partition an event goes to when the request names none: the Kafka
//! default partitioner (murmur2 of the key), so the M5 Kafka gateway agrees,
//! and for an event without a key its idempotency key's hash, so a retry
//! lands on the same partition.

use crate::CloudEvent;

/// Kafka's `Utils.murmur2`.
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    // Kafka hashes an `int` length; longer keys are refused before this.
    let length = data.len() as u32;
    let mut h = SEED ^ length;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let rest = chunks.remainder();
    if rest.len() >= 3 {
        h ^= u32::from(rest[2]) << 16;
    }
    if rest.len() >= 2 {
        h ^= u32::from(rest[1]) << 8;
    }
    if !rest.is_empty() {
        h ^= u32::from(rest[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// Kafka's partition for `key` among `partitions` (at least 1).
pub fn kafka_partition(key: &[u8], partitions: u32) -> u32 {
    (murmur2(key) as u32 & 0x7fff_ffff) % partitions.max(1)
}

/// The partition of `event` among `partitions`.
pub fn partition_of(event: &CloudEvent, partitions: u32) -> u32 {
    match event.key() {
        Some(key) => kafka_partition(key.as_bytes(), partitions),
        None => {
            let hash = event.dedup_key();
            u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]) % partitions.max(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kafka's `UtilsTest.testMurmur2`.
    #[test]
    fn matches_kafka() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];
        for (key, hash) in cases {
            assert_eq!(murmur2(key), hash, "{}", String::from_utf8_lossy(key));
        }
        assert!(kafka_partition(b"foobar", 6) < 6);
    }
}
