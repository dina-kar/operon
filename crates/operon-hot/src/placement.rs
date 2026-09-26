//! Placement (plan M1.3 Task 10 defines `PlacementImpl`; Task 8 adds the
//! trivial single-node placement first).

use operon_common::{CollectionId, NamespaceId};
use operon_query::placement::{Owner, Placement};

/// Every collection is owned by this node: `operon dev` and `standalone`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysLocal;

impl Placement for AlwaysLocal {
    fn owner(&self, _: NamespaceId, _: CollectionId) -> Owner {
        Owner::Local
    }
}
