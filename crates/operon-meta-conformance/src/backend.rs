//! What a backend hands the suite: fresh metastores, and optional fault
//! injection.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use operon_common::meta::MetaStore;

/// One fresh, empty metastore, for one case.
pub struct Instance {
    /// One handle per node (a single node: one), all serving the same metastore.
    pub clients: Vec<Arc<dyn MetaStore>>,
    pub faults: Option<Arc<dyn Faults>>,
    /// Keeps nodes, temporary directories and runtimes alive for the case; dropped at its end.
    pub guard: Box<dyn Any + Send + Sync>,
}

impl fmt::Debug for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instance")
            .field("clients", &self.clients)
            .field("faults", &self.faults.is_some())
            .finish_non_exhaustive()
    }
}

impl Instance {
    /// The number of handles.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Whether there is no handle (never, for a working backend).
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// The first handle.
    pub fn first(&self) -> &dyn MetaStore {
        &*self.clients[0]
    }

    /// The last handle: another node than the first when there are several.
    pub fn last(&self) -> &dyn MetaStore {
        &*self.clients[self.clients.len() - 1]
    }
}

/// A metastore implementation under test.
#[async_trait]
pub trait Backend: Send + Sync {
    /// A fresh, empty metastore, ready to serve writes through every handle.
    async fn start(&self) -> Instance;
}

/// Fault injection a backend may offer.
#[async_trait]
pub trait Faults: Send + Sync {
    /// The next successful write through `clients[client]` reports an unknown outcome, and the implementation retries it.
    fn lose_next_ack(&self, client: usize);
    /// Disturbs the backend in a seeded way (openraft: isolate the current leader).
    async fn disturb(&self, seed: u64);
    async fn heal(&self);
}
