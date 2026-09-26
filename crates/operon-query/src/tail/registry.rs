//! The tails of this node (plan M1.2 Task 4 rule 2): one per collection,
//! started on first use and stopped when idle, dropped or at shutdown.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use operon_collection::CollectionContext;
use operon_common::meta::Collection;
use operon_common::{CollectionId, NamespaceId};
use operon_log::LogReader;
use tokio::task::JoinHandle;

use super::{Tail, TailBudget, TailConfig, TailState};

/// How often the sweep stops idle tails.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug)]
struct Inner {
    ctx: CollectionContext,
    reader: LogReader,
    config: TailConfig,
    budget: Arc<TailBudget>,
    tails: Mutex<HashMap<CollectionId, Arc<Tail>>>,
}

/// `CollectionId → Arc<Tail>`, with a background sweep every 60 s that
/// stops tails whose last read is older than `idle_ttl`.
#[derive(Debug)]
pub struct TailRegistry {
    inner: Arc<Inner>,
    sweep: Mutex<Option<JoinHandle<()>>>,
}

impl TailRegistry {
    /// Needs a tokio runtime for the sweep; without one, idle tails still
    /// stop themselves and are replaced on their next use.
    pub fn new(
        ctx: CollectionContext,
        reader: LogReader,
        config: TailConfig,
        budget: Arc<TailBudget>,
    ) -> Self {
        let inner = Arc::new(Inner {
            ctx,
            reader,
            config,
            budget,
            tails: Mutex::new(HashMap::new()),
        });
        let sweep = tokio::runtime::Handle::try_current()
            .ok()
            .map(|handle| handle.spawn(sweep(Arc::downgrade(&inner))));
        Self {
            inner,
            sweep: Mutex::new(sweep),
        }
    }

    pub fn budget(&self) -> &Arc<TailBudget> {
        &self.inner.budget
    }

    /// The running tail of `cid`, if any.
    pub fn get(&self, cid: CollectionId) -> Option<Arc<Tail>> {
        lock(&self.inner.tails)
            .get(&cid)
            .filter(|tail| !tail.is_stopped())
            .cloned()
    }

    /// The tail of `collection`, started if it is not running (a stopped
    /// tail is replaced).
    pub fn tail(&self, ns: NamespaceId, collection: &Collection) -> Arc<Tail> {
        let mut tails = lock(&self.inner.tails);
        if let Some(tail) = tails.get(&collection.id)
            && !tail.is_stopped()
        {
            return tail.clone();
        }
        let tail = Tail::start(
            ns,
            collection.clone(),
            self.inner.ctx.clone(),
            self.inner.reader.clone(),
            self.inner.config.clone(),
            self.inner.budget.clone(),
        );
        tails.insert(collection.id, tail.clone());
        tail
    }

    pub fn notify(&self, cid: CollectionId) {
        if let Some(tail) = self.get(cid) {
            tail.notify();
        }
    }

    pub fn state(&self, cid: CollectionId) -> Option<TailState> {
        self.get(cid).map(|tail| tail.state())
    }

    /// Stops and removes the tail of `cid`.
    pub async fn stop(&self, cid: CollectionId) {
        let tail = lock(&self.inner.tails).remove(&cid);
        if let Some(tail) = tail {
            tail.stop().await;
        }
    }

    /// Stops the sweep and every tail.
    pub async fn shutdown(&self) {
        if let Some(sweep) = lock(&self.sweep).take() {
            sweep.abort();
        }
        let tails: Vec<Arc<Tail>> = lock(&self.inner.tails).drain().map(|(_, t)| t).collect();
        for tail in tails {
            tail.stop().await;
        }
    }
}

impl Drop for TailRegistry {
    fn drop(&mut self) {
        if let Some(sweep) = lock(&self.sweep).take() {
            sweep.abort();
        }
    }
}

/// Every [`SWEEP_INTERVAL`]: stops idle tails and forgets stopped ones.
async fn sweep(inner: Weak<Inner>) {
    let mut interval = tokio::time::interval(SWEEP_INTERVAL);
    interval.tick().await;
    loop {
        interval.tick().await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let idle: Vec<Arc<Tail>> = {
            let mut tails = lock(&inner.tails);
            let idle_ttl = inner.config.idle_ttl;
            let expired: Vec<CollectionId> = tails
                .iter()
                .filter(|(_, tail)| tail.is_stopped() || tail.last_read().elapsed() >= idle_ttl)
                .map(|(cid, _)| *cid)
                .collect();
            expired
                .into_iter()
                .filter_map(|cid| tails.remove(&cid))
                .collect()
        };
        for tail in idle {
            tail.stop().await;
        }
    }
}
