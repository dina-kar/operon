//! The link-target registry (plan M1.1 Task 10): which [`LinkTarget`] serves
//! a link, by its `TargetRef.kind`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use operon_meta::{Link, MetaClient};
use operon_store::Store;

use crate::counter::{COUNTER_KIND, CounterTable};
use crate::error::LinkError;
use crate::target::LinkTarget;

/// Makes the targets of one link kind.
pub trait LinkTargetFactory: Send + Sync + fmt::Debug {
    /// The `TargetRef.kind` this factory serves.
    fn kind(&self) -> &str;

    /// A target for `link` (cheap; may return a cached instance). Never
    /// opens writers.
    fn open(&self, meta: &MetaClient, link: &Link) -> Result<Arc<dyn LinkTarget>, LinkError>;
}

/// Link target factories by the kind they serve. Cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct TargetRegistry {
    factories: BTreeMap<String, Arc<dyn LinkTargetFactory>>,
}

impl TargetRegistry {
    /// An empty registry: no link is applied.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `factory`, replacing a factory of the same kind.
    pub fn with(mut self, factory: Arc<dyn LinkTargetFactory>) -> Self {
        self.factories.insert(factory.kind().to_string(), factory);
        self
    }

    /// The factory of `kind`, if one is registered.
    pub fn get(&self, kind: &str) -> Option<&Arc<dyn LinkTargetFactory>> {
        self.factories.get(kind)
    }

    /// The registered kinds, in order.
    pub fn kinds(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

/// Serves [`COUNTER_KIND`] links with a [`CounterTable`] each.
#[derive(Clone)]
pub struct CounterTargetFactory {
    store: Store,
    max_commit_delay: Duration,
    #[cfg(feature = "test-util")]
    hook: Option<crate::target::CommitHook>,
}

impl fmt::Debug for CounterTargetFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CounterTargetFactory")
            .field("store", &self.store)
            .field("max_commit_delay", &self.max_commit_delay)
            .finish_non_exhaustive()
    }
}

impl CounterTargetFactory {
    /// Tables on `store` whose commits must reach their CAS within
    /// `max_commit_delay` (below GC's grace).
    pub fn new(store: Store, max_commit_delay: Duration) -> Self {
        Self {
            store,
            max_commit_delay,
            #[cfg(feature = "test-util")]
            hook: None,
        }
    }

    /// Test hook: every table this factory opens awaits `hook` at each
    /// commit step. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: crate::target::CommitHook) -> Self {
        self.hook = Some(hook);
        self
    }
}

impl LinkTargetFactory for CounterTargetFactory {
    fn kind(&self) -> &str {
        COUNTER_KIND
    }

    fn open(&self, meta: &MetaClient, link: &Link) -> Result<Arc<dyn LinkTarget>, LinkError> {
        let table = CounterTable::for_link(meta.clone(), self.store.clone(), link)
            .with_max_commit_delay(self.max_commit_delay);
        #[cfg(feature = "test-util")]
        let table = table.with_hook(self.hook.clone());
        Ok(Arc::new(table))
    }
}
