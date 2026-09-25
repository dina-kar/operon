//! The names of every namespace's collections and aliases, for DataFusion's
//! synchronous `SchemaProvider::table_names` (plan M1.2 Task 9; Task 10).
//!
//! A background task reads `namespaces`, `collections` and `aliases`
//! (`Local`) once at start and again whenever `watch_changes()` completes.
//! When the metastore stops (`Err(MetaStopped)`, row 0.79), the task ends and
//! the names stay as last read.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use operon_common::meta::{Consistency, MetaResult, MetaStore};
use tokio::task::JoinHandle;

/// How long the refresh task waits before it retries a failed read when no
/// change wakes it first.
const RETRY: Duration = Duration::from_secs(1);

/// Namespace name → the sorted names of its collections and aliases.
type Names = BTreeMap<String, Vec<String>>;

/// A cached view of the catalog's names, refreshed on every metastore
/// change. Clones share one cache and one refresh task.
#[derive(Clone)]
pub struct CatalogCache {
    inner: Arc<Inner>,
}

struct Inner {
    names: RwLock<Names>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for CatalogCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self
            .inner
            .names
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("CatalogCache")
            .field("namespaces", &names.len())
            .finish()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

impl CatalogCache {
    /// Starts the refresh task; call from within a Tokio runtime.
    pub fn start(meta: Arc<dyn MetaStore>) -> Self {
        let inner = Arc::new(Inner {
            names: RwLock::new(Names::new()),
            task: Mutex::new(None),
        });
        let task = tokio::spawn(refresh_loop(meta, Arc::downgrade(&inner)));
        *inner.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
        Self { inner }
    }

    /// The sorted names of the collections and aliases of namespace `ns`, as
    /// of the last refresh; empty for an unknown namespace.
    pub fn names(&self, ns: &str) -> Vec<String> {
        self.inner
            .names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(ns)
            .cloned()
            .unwrap_or_default()
    }

    /// Stops the refresh task; the names stay as last read.
    pub fn stop(&self) {
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}

/// Reads the names, then waits for the next change; ends when the metastore
/// stops or every [`CatalogCache`] is gone.
async fn refresh_loop(meta: Arc<dyn MetaStore>, inner: Weak<Inner>) {
    loop {
        // Armed before the reads, so a change during them wakes the wait.
        let mut changes = meta.watch_changes();
        let read = read_names(&*meta).await;
        let Some(cache) = inner.upgrade() else {
            return;
        };
        let failed = match read {
            Ok(names) => {
                *cache.names.write().unwrap_or_else(PoisonError::into_inner) = names;
                false
            }
            Err(err) => {
                tracing::debug!(%err, "catalog cache: reading the catalog");
                true
            }
        };
        drop(cache);
        if failed {
            tokio::select! {
                changed = changes.changed() => if changed.is_err() { return },
                () = tokio::time::sleep(RETRY) => {}
            }
        } else if changes.changed().await.is_err() {
            return;
        }
    }
}

async fn read_names(meta: &dyn MetaStore) -> MetaResult<Names> {
    let mut out = Names::new();
    for namespace in meta.namespaces(Consistency::Local).await? {
        let mut names: BTreeSet<String> = meta
            .collections(Consistency::Local, Some(namespace.id))
            .await?
            .into_iter()
            .map(|collection| collection.name)
            .collect();
        names.extend(
            meta.aliases(Consistency::Local, namespace.id)
                .await?
                .into_iter()
                .map(|(alias, _)| alias),
        );
        out.insert(namespace.name, names.into_iter().collect());
    }
    Ok(out)
}
