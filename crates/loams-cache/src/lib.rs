//! Read-through byte-range cache for immutable objects (hot-tier layers H0/H1).
//!
//! Objects are split into fixed-size blocks. Blocks live in a foyer hybrid cache
//! (RAM, optionally spilling to an NVMe directory), each with a crc32c checksum
//! verified on every hit. Because cached objects are immutable, entries never
//! need invalidation.

mod range_cache;

pub use range_cache::{CacheError, CacheStats, DiskConfig, RangeCache, RangeCacheConfig};
