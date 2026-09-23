use operon_common::{NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

/// A namespace: the unit of tenancy, quotas and routing (design §01 §1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Namespace {
    pub id: NamespaceId,
    pub name: String,
}

/// WAL durability class of a stream (design §02 §2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalClass {
    Standard,
    Express,
    Quorum,
}

/// A partitioned, offset-addressed stream (design §01 §2.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub id: StreamId,
    pub namespace: NamespaceId,
    pub name: String,
    pub partitions: u32,
    pub class: WalClass,
}

/// Sequencer state of one stream partition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub(crate) next_offset: u64,
}

impl PartitionState {
    /// The offset the next committed record will get. For `standard` streams this
    /// is also the high watermark: every offset below it is committed and readable.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}
