//! The hash functions that place a key on a shard exactly as the routers do
//! (§31 §7.1): Postgres hash partitioning, which PgDog follows, and Vitess's
//! vindexes.

pub mod pg;
pub mod vitess;

pub use pg::{HASH_PARTITION_SEED, PgKey, PgKeyType, pg_partition_index};
pub use vitess::{vitess_hash, vitess_unhash, vitess_xxhash};
