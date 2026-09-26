//! Object paths of the log (design §01 §6, M0.3 plan ruling 4).

use operon_common::meta::WalClass;
use operon_common::{NamespaceId, StreamId};
use ulid::Ulid;

/// The path segment naming a WAL class.
pub fn class_name(class: WalClass) -> &'static str {
    match class {
        WalClass::Standard => "standard",
        WalClass::Express => "express",
        WalClass::Quorum => "quorum",
    }
}

/// `wal/<class>/<node_id>/<ulid>.wal`. WAL objects live at the cluster level:
/// one object holds chunks from many namespaces.
pub fn wal_object(class: WalClass, node_id: u64, ulid: Ulid) -> String {
    format!("wal/{}/{node_id}/{ulid}.wal", class_name(class))
}

/// `ns/<ns>/streams/<stream>/<partition>/<base_offset:020>-<ulid>.seg`.
pub fn segment(
    namespace: NamespaceId,
    stream: StreamId,
    partition: u32,
    base_offset: u64,
    ulid: Ulid,
) -> String {
    format!("ns/{namespace}/streams/{stream}/{partition}/{base_offset:020}-{ulid}.seg")
}
