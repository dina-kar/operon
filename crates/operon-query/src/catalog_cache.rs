//! The names of every namespace's collections and aliases, for DataFusion's
//! synchronous `SchemaProvider::table_names` (plan M1.2 Task 9; Task 10),
//! and the collections they name, for the synchronous planning of the SQL
//! search table functions (Task 10).
//!
//! A background task reads `namespaces`, `collections` and `aliases`
//! (`Local`) once at start and again whenever `watch_changes()` completes.
//! When the metastore stops (`Err(MetaStopped)`, row 0.79), the task ends and
//! the names stay as last read. [`CatalogCache::refresh`] re-reads one
//! namespace on demand (every SQL statement does, before it plans).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use operon_common::meta::{Collection, Consistency, MetaResult, MetaStore, Namespace};
use tokio::task::JoinHandle;

/// How long the refresh task waits before it retries a failed read when no
/// change wakes it first.
const RETRY: Duration = Duration::from_secs(1);

/// What the cache knows of one namespace.
#[derive(Clone, Debug, Default)]
struct NsEntry {
    /// The sorted names of its collections and aliases.
    names: Vec<String>,
    /// Collection name or alias → the collection.
    collections: BTreeMap<String, Collection>,
}

/// Namespace name → what the cache knows of it.
type Names = BTreeMap<String, NsEntry>;

/// A cached view of the catalog's names, refreshed on every metastore
/// change. Clones share one cache and one refresh task.
#[derive(Clone)]
pub struct CatalogCache {
    inner: Arc<Inner>,
}

struct Inner {
    meta: Arc<dyn MetaStore>,
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
            meta: meta.clone(),
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
            .map(|entry| entry.names.clone())
            .unwrap_or_default()
    }

    /// The collection named or aliased `name_or_alias` in namespace `ns`, as
    /// of the last refresh.
    pub fn collection(&self, ns: &str, name_or_alias: &str) -> Option<Collection> {
        self.inner
            .names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(ns)?
            .collections
            .get(name_or_alias)
            .cloned()
    }

    /// Re-reads namespace `ns` now (`Local`), so a collection created or
    /// changed through this node is known before the next change wakes the
    /// refresh task.
    pub async fn refresh(&self, ns: &str) -> MetaResult<()> {
        let meta = &*self.inner.meta;
        let namespace = meta.namespace_by_name(Consistency::Local, ns).await?;
        let entry = match namespace {
            Some(namespace) => Some(read_namespace(meta, &namespace).await?),
            None => None,
        };
        let mut names = self
            .inner
            .names
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        match entry {
            Some(entry) => names.insert(ns.to_string(), entry),
            None => names.remove(ns),
        };
        Ok(())
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
        let entry = read_namespace(meta, &namespace).await?;
        out.insert(namespace.name, entry);
    }
    Ok(out)
}

async fn read_namespace(meta: &dyn MetaStore, namespace: &Namespace) -> MetaResult<NsEntry> {
    let collections = meta
        .collections(Consistency::Local, Some(namespace.id))
        .await?;
    let aliases = meta.aliases(Consistency::Local, namespace.id).await?;
    let mut entry = NsEntry::default();
    let mut names: BTreeSet<String> = BTreeSet::new();
    for collection in &collections {
        names.insert(collection.name.clone());
        entry
            .collections
            .insert(collection.name.clone(), collection.clone());
    }
    for (alias, cid) in aliases {
        names.insert(alias.clone());
        if let Some(collection) = collections.iter().find(|c| c.id == cid) {
            // A collection's own name wins over an alias of the same name.
            entry
                .collections
                .entry(alias)
                .or_insert_with(|| collection.clone());
        }
    }
    entry.names = names.into_iter().collect();
    Ok(entry)
}
