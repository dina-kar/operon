//! The sans-I/O kernel of the Loams router's sharding control plane (design
//! §31 §6–§7, D303–D313).
//!
//! Loams routes Postgres through unmodified PgDog and MySQL through
//! unmodified Vitess (D300). What it builds is the control plane around
//! them: the versioned shard map ([`record`]), the key-range arithmetic that
//! keeps a keyspace partitioned ([`ranges`]), the hash functions that place a
//! key exactly where Postgres hash partitioning and Vitess vindexes place it
//! ([`hash`]), and the seams every protocol machine runs behind
//! ([`machine`], [`trace`]).
//!
//! Nothing here does I/O, reads a clock or draws unseeded randomness: drivers
//! supply time and randomness through [`machine::Ctx`] and execute the
//! commands machines return, so the deterministic simulator and production
//! run the same code (D313). The crate's `clippy.toml` and the
//! `kernel_crate_has_no_io_dependencies` test enforce it.
//!
//! The TLA+ specs in `spec/tla/router/` model the protocols, and the Lean
//! proofs in `spec/lean/` cover the key ranges. The `lean_oracle` test
//! compares [`ranges`] with the Lean reference on random inputs.

pub mod hash;
pub mod machine;
pub mod ranges;
pub mod record;
pub mod trace;

pub use machine::{Ctx, Machine, Millis};
pub use ranges::{KeyRange, KeyspaceId, PartitionError};
pub use record::{RECORD_FORMAT, RecordError, ShardMapRecord};
pub use trace::{SpecEvent, SpecValue, TraceSink};
