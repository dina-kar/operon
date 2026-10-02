//! The shard-map record: one per mapped database, in the metastore, changed
//! only by compare-and-set on `version` (§31 §6.1, D304). The TLA+ spec
//! `ShardMap` models its generations.

use serde::{Deserialize, Serialize};

use crate::hash::{PgKey, PgKeyType, pg_partition_index, vitess_hash, vitess_xxhash};
use crate::ranges::{self, KeyRange, PartitionError};

/// The leading byte of an encoded record (RT0 plan, ruling 8). A layout
/// change gets a new byte and keeps a reader for this one.
pub const RECORD_FORMAT: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMapRecord {
    /// The CAS version: every write increments it.
    pub version: u64,
    /// The routing generation: increments when routing changes, never reused.
    pub generation: u64,
    pub engine: Engine,
    pub router: RouterKind,
    pub scheme: Scheme,
    /// Ordered; the index is PgDog's `shard` number.
    pub shards: Vec<ShardEntry>,
    pub state: MapState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Engine {
    Postgres,
    MySql,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouterKind {
    PgDog { database: String },
    Vitess { keyspace: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scheme {
    /// Postgres hash partitioning over `shards` shards (PgDog `hash`).
    PgHash {
        column: String,
        data_type: PgKeyType,
        shards: u32,
    },
    /// PgDog `[[sharded_mappings]]` of kind `range`: `[lo, hi)` to a shard.
    PgRange {
        column: String,
        ranges: Vec<(Bound, Bound, u32)>,
    },
    /// PgDog `[[sharded_mappings]]` of kind `list`.
    PgList {
        column: String,
        lists: Vec<(Vec<PgKey>, u32)>,
    },
    /// A Vitess vindex over keyspace-id ranges, one per shard in order.
    Vindex {
        vindex: VindexKind,
        ranges: Vec<KeyRange>,
    },
}

/// A bound of a PgDog range mapping over an `int8` key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Bound {
    Unbounded,
    Int(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VindexKind {
    /// DES of the big-endian `u64` key.
    Hash,
    /// XXH64 of the key's raw bytes.
    XxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    pub name: String,
    pub backend: BackendRef,
    pub fence: FenceState,
}

/// Where a shard's primary is reached. RT1 fills the as-built fields (§23's
/// endpoint records); RT0 needs only enough to round-trip.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendRef {
    pub host: String,
    pub port: u16,
    pub database: String,
}

/// Whether the shard's backends accept router logins (§6.4: `ALTER ROLE … NOLOGIN`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FenceState {
    Open,
    Fenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MapState {
    Serving,
    Resharding { to: u64 },
    CuttingOver { step: CutoverStep },
    Fenced,
}

/// The saga's steps, named as the TLA+ spec `ReshardCutover` names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CutoverStep {
    Copy,
    CatchUp,
    Paused,
    Fenced,
    CutOver,
    Published,
    Resumed,
    Finalized,
    RollingBack,
    RolledBack,
}

/// A value to route. Postgres schemes take a [`PgKey`]; the `Hash` vindex
/// takes an integer, and `XxHash` takes raw bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyValue {
    Pg(PgKey),
    Uint(u64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("unknown record format byte {0}")]
    UnknownFormat(u8),
    #[error("corrupt record: {0}")]
    Corrupt(String),
    #[error("the scheme names {scheme} shards but the record lists {listed}")]
    ShardCount { scheme: usize, listed: usize },
    #[error("a mapping names shard {0}, which the record does not list")]
    NoSuchShard(u32),
    #[error("the vindex ranges are not a partition: {0}")]
    Partition(#[from] PartitionError),
    #[error("the key's type does not match the scheme")]
    UnsupportedKeyType,
    #[error("no shard owns the key")]
    Unmapped,
}

impl ShardMapRecord {
    /// `[RECORD_FORMAT] ++ postcard`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![RECORD_FORMAT];
        // Serializing these plain types into a Vec cannot fail.
        out.extend(postcard::to_stdvec(self).unwrap_or_default());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        match bytes.split_first() {
            None => Err(RecordError::Corrupt("empty".into())),
            Some((&RECORD_FORMAT, body)) => {
                postcard::from_bytes(body).map_err(|e| RecordError::Corrupt(e.to_string()))
            }
            Some((&other, _)) => Err(RecordError::UnknownFormat(other)),
        }
    }

    /// The shard count matches the scheme, mappings name listed shards, and
    /// vindex ranges partition the keyspace.
    pub fn validate(&self) -> Result<(), RecordError> {
        let listed = self.shards.len();
        let known = |s: u32| {
            if (s as usize) < listed {
                Ok(())
            } else {
                Err(RecordError::NoSuchShard(s))
            }
        };
        match &self.scheme {
            Scheme::PgHash { shards, .. } if *shards as usize != listed => {
                Err(RecordError::ShardCount {
                    scheme: *shards as usize,
                    listed,
                })
            }
            // Zero shards cannot route anything, and pg_partition_index needs a positive modulus.
            Scheme::PgHash { shards: 0, .. } => Err(RecordError::ShardCount { scheme: 0, listed }),
            Scheme::PgHash { .. } => Ok(()),
            Scheme::PgRange { ranges, .. } => ranges.iter().try_for_each(|(_, _, s)| known(*s)),
            Scheme::PgList { lists, .. } => lists.iter().try_for_each(|(_, s)| known(*s)),
            Scheme::Vindex { ranges, .. } => {
                ranges::validate_partition(ranges)?;
                if ranges.len() != listed {
                    return Err(RecordError::ShardCount {
                        scheme: ranges.len(),
                        listed,
                    });
                }
                Ok(())
            }
        }
    }

    /// The index of the shard that owns `key` in this generation.
    pub fn shard_for(&self, key: &KeyValue) -> Result<u32, RecordError> {
        match (&self.scheme, key) {
            (
                Scheme::PgHash {
                    data_type, shards, ..
                },
                KeyValue::Pg(k),
            ) if k.key_type() == *data_type => Ok(pg_partition_index(k, *shards)),
            (Scheme::PgRange { ranges, .. }, KeyValue::Pg(PgKey::Int8(v))) => ranges
                .iter()
                .find(|(lo, hi, _)| {
                    let above_lo = match lo {
                        Bound::Unbounded => true,
                        Bound::Int(l) => l <= v,
                    };
                    let below_hi = match hi {
                        Bound::Unbounded => true,
                        Bound::Int(h) => v < h,
                    };
                    above_lo && below_hi
                })
                .map(|(_, _, s)| *s)
                .ok_or(RecordError::Unmapped),
            (Scheme::PgList { lists, .. }, KeyValue::Pg(k)) => lists
                .iter()
                .find(|(values, _)| values.contains(k))
                .map(|(_, s)| *s)
                .ok_or(RecordError::Unmapped),
            (Scheme::Vindex { vindex, ranges }, key) => {
                let ksid = match (vindex, key) {
                    (VindexKind::Hash, KeyValue::Uint(v)) => vitess_hash(*v),
                    (VindexKind::Hash, KeyValue::Pg(PgKey::Int8(v))) => vitess_hash(*v as u64),
                    (VindexKind::XxHash, KeyValue::Bytes(b)) => vitess_xxhash(b),
                    _ => return Err(RecordError::UnsupportedKeyType),
                };
                ranges::lookup(ranges, ksid)
                    .map(|i| i as u32)
                    .ok_or(RecordError::Unmapped)
            }
            _ => Err(RecordError::UnsupportedKeyType),
        }
    }
}
