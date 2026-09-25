//! The follower (plan M1.2 Task 3 rules 5, 8 and 9): one tokio task per
//! tail, which follows the implicit stream from the live manifest's
//! `applied` offsets, adopts newer manifests and publishes a snapshot after
//! each change.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use operon_collection::{
    CollectionContext, CollectionError, CollectionManifest, CollectionSnapshot, live_manifest,
};
use operon_common::NamespaceId;
use operon_common::meta::{Collection, CollectionHead, Consistency, MetaChanges};
use operon_log::{FetchRequest, LogError, LogReader};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use super::index::TailIndex;
use super::snapshot::TailSnapshot;
use super::{TailBudget, TailConfig, TailError, TailState};

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What the follower and its readers share.
#[derive(Debug)]
struct Shared {
    published: RwLock<Arc<TailSnapshot>>,
    state: RwLock<TailState>,
    /// The tail stopped: dropped collection, idle, or `stop`.
    stopped: AtomicBool,
    stop: watch::Sender<bool>,
    notify: Notify,
    /// Incremented on every publish and state change.
    publishes: watch::Sender<u64>,
    last_read: Mutex<Instant>,
    /// This tail's bytes in the budget.
    accounted: AtomicUsize,
    #[cfg(feature = "test-util")]
    pause: watch::Sender<bool>,
    #[cfg(feature = "test-util")]
    held: watch::Sender<bool>,
}

impl Shared {
    fn current(&self) -> Arc<TailSnapshot> {
        self.published
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn state(&self) -> TailState {
        self.state.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn set_state(&self, state: TailState) {
        let mut current = self.state.write().unwrap_or_else(|p| p.into_inner());
        if *current != state {
            *current = state;
            drop(current);
            self.publishes.send_modify(|n| *n += 1);
        }
    }

    fn publish(&self, snapshot: Arc<TailSnapshot>) {
        *self.published.write().unwrap_or_else(|p| p.into_inner()) = snapshot;
        self.publishes.send_modify(|n| *n += 1);
    }
}

/// One collection's tail on this node.
#[derive(Debug)]
pub struct Tail {
    shared: Arc<Shared>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tail {
    /// Starts following collection `collection` of namespace `ns`. It
    /// publishes an empty snapshot at once; the follower then loads the live
    /// manifest (`Local`), sets `head = applied` and follows from there.
    pub fn start(
        ns: NamespaceId,
        collection: Collection,
        ctx: CollectionContext,
        reader: LogReader,
        config: TailConfig,
        budget: Arc<TailBudget>,
    ) -> Arc<Self> {
        let empty = TailSnapshot::empty(
            None,
            Arc::new(CollectionManifest::empty(collection.id)),
            collection.schema.version,
        );
        let shared = Arc::new(Shared {
            published: RwLock::new(empty),
            state: RwLock::new(TailState::Following),
            stopped: AtomicBool::new(false),
            stop: watch::channel(false).0,
            notify: Notify::new(),
            publishes: watch::channel(0).0,
            last_read: Mutex::new(Instant::now()),
            accounted: AtomicUsize::new(0),
            #[cfg(feature = "test-util")]
            pause: watch::channel(false).0,
            #[cfg(feature = "test-util")]
            held: watch::channel(false).0,
        });
        let follower = Follower {
            shared: shared.clone(),
            ns,
            collection,
            ctx,
            reader,
            config,
            budget,
            index: None,
            durable: None,
            adopt_failures: 0,
            fresh_reset: BTreeSet::new(),
        };
        let task = tokio::spawn(follower.run());
        Arc::new(Self {
            shared,
            task: Mutex::new(Some(task)),
        })
    }

    fn touch(&self) {
        *lock(&self.shared.last_read) = Instant::now();
    }

    /// The latest snapshot (one `Arc` clone).
    pub fn current(&self) -> Arc<TailSnapshot> {
        self.touch();
        self.shared.current()
    }

    /// A started snapshot (one published after the follower loaded the live
    /// manifest) whose head covers `targets` (partition → offset); waits for
    /// the follower until `deadline` (rule 8).
    pub async fn sync(
        &self,
        targets: &BTreeMap<u32, u64>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<TailSnapshot>, TailError> {
        let mut publishes = self.shared.publishes.subscribe();
        loop {
            publishes.borrow_and_update();
            self.touch();
            let snapshot = self.shared.current();
            let head_of = |p: &u32| snapshot.head().get(p).copied().unwrap_or(0);
            if snapshot.is_started() && targets.iter().all(|(p, target)| head_of(p) >= *target) {
                return Ok(snapshot);
            }
            if self.shared.stopped.load(Ordering::Acquire) {
                return Err(TailError::Stopped);
            }
            match self.shared.state() {
                TailState::Overflow { head }
                    if targets
                        .iter()
                        .any(|(p, target)| head.get(p).copied().unwrap_or(0) < *target) =>
                {
                    return Err(TailError::Overflow { head });
                }
                TailState::Failed(message) => {
                    return Err(TailError::Collection(CollectionError::Internal(message)));
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(TailError::Timeout {
                    target: targets.clone(),
                });
            }
            self.notify();
            tokio::select! {
                changed = publishes.changed() => {
                    if changed.is_err() {
                        return Err(TailError::Stopped);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Wakes the follower.
    pub fn notify(&self) {
        self.shared.notify.notify_one();
    }

    pub fn state(&self) -> TailState {
        self.shared.state()
    }

    /// When `current` or `sync` last ran.
    pub fn last_read(&self) -> Instant {
        *lock(&self.shared.last_read)
    }

    /// Whether the follower has stopped.
    pub fn is_stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }

    /// Stops the follower and waits for it.
    pub async fn stop(&self) {
        self.shared.stop.send_replace(true);
        self.notify();
        let task = lock(&self.task).take();
        if let Some(task) = task {
            let _ = task.await;
        }
        self.shared.stopped.store(true, Ordering::Release);
    }

    /// Holds the follower before its next iteration's metastore read (so
    /// before its next adoption and fetch) while `paused`.
    #[cfg(feature = "test-util")]
    pub fn pause_fetch(&self, paused: bool) {
        self.shared.pause.send_replace(paused);
        self.notify();
    }

    /// Waits until a paused follower is held.
    #[cfg(feature = "test-util")]
    pub async fn wait_held(&self) {
        let mut held = self.shared.held.subscribe();
        let _ = held.wait_for(|held| *held).await;
    }
}

impl Drop for Tail {
    fn drop(&mut self) {
        self.shared.stop.send_replace(true);
        self.shared.notify.notify_one();
    }
}

/// The follower task's state.
struct Follower {
    shared: Arc<Shared>,
    ns: NamespaceId,
    collection: Collection,
    ctx: CollectionContext,
    reader: LogReader,
    config: TailConfig,
    budget: Arc<TailBudget>,
    /// `None` until the first (re)start succeeds.
    index: Option<TailIndex>,
    /// M_T's snapshot, for durable lookups.
    durable: Option<CollectionSnapshot>,
    adopt_failures: u32,
    /// Partitions whose first fetch after a reset has not returned yet.
    fresh_reset: BTreeSet<u32>,
}

enum Step {
    /// Go on; `true` when some partition is still behind (skip the wait).
    Continue(bool),
    Stop,
}

impl Follower {
    async fn run(mut self) {
        let mut stop = self.shared.stop.subscribe();
        let mut changes: Option<MetaChanges> = Some(self.ctx.meta.watch_changes());
        let mut behind = true;
        loop {
            if *stop.borrow_and_update() {
                break;
            }
            if !behind {
                let wait = tokio::time::sleep(self.config.follow_interval);
                tokio::select! {
                    () = self.shared.notify.notified() => {}
                    () = wait => {}
                    _ = stop.changed() => {}
                    stopped = async {
                        match changes.as_mut() {
                            Some(changes) => changes.changed().await.is_err(),
                            None => std::future::pending().await,
                        }
                    } => {
                        if stopped {
                            // The metastore stopped: wait only on `notify`
                            // and the interval from now on (row 0.79).
                            changes = None;
                        }
                    }
                }
                if *stop.borrow_and_update() {
                    break;
                }
            }
            if lock(&self.shared.last_read).elapsed() >= self.config.idle_ttl {
                tracing::debug!(collection = %self.collection.id, "the tail is idle; stopping");
                break;
            }
            // Armed before the metastore read below, so a change after it
            // wakes the next wait.
            if changes.is_some() {
                changes = Some(self.ctx.meta.watch_changes());
            }
            #[cfg(feature = "test-util")]
            if !self.hold(&mut stop).await {
                break;
            }
            match self.iteration().await {
                Step::Continue(still_behind) => behind = still_behind,
                Step::Stop => break,
            }
        }
        self.shared.stopped.store(true, Ordering::Release);
        let accounted = self.shared.accounted.swap(0, Ordering::Relaxed);
        self.budget.account(accounted, 0);
        self.shared.publishes.send_modify(|n| *n += 1);
    }

    /// Holds while paused; `false` when stopped meanwhile.
    #[cfg(feature = "test-util")]
    async fn hold(&self, stop: &mut watch::Receiver<bool>) -> bool {
        let mut pause = self.shared.pause.subscribe();
        if !*pause.borrow_and_update() {
            return true;
        }
        self.shared.held.send_replace(true);
        let resumed = tokio::select! {
            _ = pause.wait_for(|paused| !*paused) => true,
            _ = stop.wait_for(|stop| *stop) => false,
        };
        self.shared.held.send_replace(false);
        resumed
    }

    /// Loads the live manifest and starts an empty tail over it (rule 9).
    async fn restart(&mut self) -> Result<(), TailError> {
        let live = live_manifest(
            &*self.ctx.meta,
            &self.ctx.store,
            &self.ctx.manifests,
            self.ns,
            self.collection.id,
            Consistency::Local,
        )
        .await?;
        let (path, manifest) = match live {
            Some((path, manifest)) => (Some(path), manifest),
            None => (
                None,
                Arc::new(CollectionManifest::empty(self.collection.id)),
            ),
        };
        self.index = Some(TailIndex::new(
            &self.collection.schema,
            path,
            manifest,
            self.config.writer_memory,
        )?);
        self.durable = None;
        self.adopt_failures = 0;
        Ok(())
    }

    /// M_T's snapshot, opened once per manifest.
    async fn durable(&mut self) -> Result<Option<&CollectionSnapshot>, TailError> {
        let index = self.index.as_ref().expect("the tail has started");
        if index.manifest.lance_version == 0 {
            return Ok(None);
        }
        let stale = self
            .durable
            .as_ref()
            .is_none_or(|s| s.manifest().version != index.manifest.version);
        if stale {
            let snapshot = CollectionSnapshot::at(
                &self.ctx,
                self.ns,
                self.collection.clone(),
                index.manifest_path.clone(),
                index.manifest.clone(),
            )
            .await?;
            self.durable = Some(snapshot);
        }
        Ok(self.durable.as_ref())
    }

    async fn iteration(&mut self) -> Step {
        // 2. The collection, its pointer and the high watermarks.
        let head = match self
            .ctx
            .meta
            .collection_head(Consistency::Local, self.collection.id)
            .await
        {
            Ok(Some(head)) if head.collection.namespace == self.ns => head,
            Ok(_) => {
                tracing::debug!(collection = %self.collection.id, "the collection was dropped; the tail stops");
                return Step::Stop;
            }
            Err(err) => {
                tracing::warn!(collection = %self.collection.id, %err, "tail: reading the collection head");
                return Step::Continue(false);
            }
        };
        self.collection = head.collection.clone();
        if self.index.is_none() {
            match self.restart().await {
                Ok(()) => {
                    self.shared.set_state(TailState::Following);
                    self.publish();
                }
                Err(err) => {
                    tracing::warn!(collection = %self.collection.id, %err, "tail: starting");
                    self.shared.set_state(TailState::Failed(err.to_string()));
                    return Step::Continue(false);
                }
            }
        }
        match self.follow(&head).await {
            Ok(behind) => Step::Continue(behind),
            Err(err) => {
                tracing::warn!(collection = %self.collection.id, %err, "tail: following");
                Step::Continue(false)
            }
        }
    }

    /// Steps 3–6 of one iteration; returns whether a partition is behind.
    async fn follow(&mut self, head: &CollectionHead) -> Result<bool, TailError> {
        let mut changed = false;
        // 3. A schema change rebuilds the index with the new layout (Ruling 19).
        let schema = head.collection.schema.clone();
        if self.index().schema_version != schema.version {
            self.index_mut().compact(&schema)?;
            changed = true;
        }
        // 4. Adoption.
        if let Some(pointer) = &head.pointer
            && pointer.version > self.index().manifest.version
        {
            changed = true;
            match self.adopt(pointer.version, &pointer.value).await {
                Ok(()) => self.adopt_failures = 0,
                Err(err) => {
                    self.adopt_failures += 1;
                    tracing::warn!(collection = %self.collection.id, %err, failures = self.adopt_failures, "tail: adopting a manifest");
                    if self.adopt_failures >= 2 {
                        self.reset().await?;
                    }
                }
            }
        }
        // 5. Fetch.
        let mut behind = false;
        let overflowing = matches!(self.shared.state(), TailState::Overflow { .. });
        if !overflowing {
            for partition in 0..self.collection.partitions {
                let hwm = head
                    .high_watermarks
                    .get(partition as usize)
                    .copied()
                    .unwrap_or(0);
                let from = self.index().head.get(&partition).copied().unwrap_or(0);
                if from >= hwm {
                    continue;
                }
                match self.fetch(partition, from).await {
                    Ok(()) => changed = true,
                    Err(Fetch::Reset) => {
                        changed = true;
                        break;
                    }
                    Err(Fetch::Failed(err)) => {
                        tracing::warn!(collection = %self.collection.id, partition, %err, "tail: fetching");
                        continue;
                    }
                }
                if self.index().head.get(&partition).copied().unwrap_or(0) < hwm {
                    behind = true;
                }
            }
        }
        // 7. Compaction of garbage.
        let config = &self.config;
        if self
            .index()
            .wants_compaction(config.compact_garbage_ratio, config.compact_min_entries)
        {
            self.index_mut().compact(&schema)?;
            changed = true;
        }
        if changed {
            self.publish_checked()?;
        }
        Ok(behind)
    }

    fn index(&self) -> &TailIndex {
        self.index.as_ref().expect("the tail has started")
    }

    fn index_mut(&mut self) -> &mut TailIndex {
        self.index.as_mut().expect("the tail has started")
    }

    async fn adopt(&mut self, version: u64, path: &str) -> Result<(), TailError> {
        let manifest = self.ctx.manifests.load(&self.ctx.store, path).await?;
        if manifest.version != version || manifest.collection_id != self.collection.id {
            return Err(CollectionError::Corrupt(format!(
                "{path} is version {} of collection {}, but the pointer of collection {} is at version {version}",
                manifest.version, manifest.collection_id, self.collection.id
            ))
            .into());
        }
        let ctx = self.ctx.clone();
        let gap = self
            .index_mut()
            .adopt(&ctx, path.to_string(), manifest)
            .await?;
        if gap {
            let batch = self.config.resolve_batch;
            let durable = self.durable().await?.cloned();
            self.index_mut().reresolve(durable.as_ref(), batch).await?;
        }
        // An overflowing tail compacts after every adoption, to get back
        // under its bound.
        if matches!(self.shared.state(), TailState::Overflow { .. }) && self.index().garbage() > 0 {
            let schema = self.collection.schema.clone();
            self.index_mut().compact(&schema)?;
        }
        Ok(())
    }

    /// Discards the generation and starts again from the live manifest.
    async fn reset(&mut self) -> Result<(), TailError> {
        tracing::info!(collection = %self.collection.id, "tail: resetting");
        self.restart().await?;
        self.fresh_reset = (0..self.collection.partitions).collect();
        Ok(())
    }

    async fn fetch(&mut self, partition: u32, from: u64) -> Result<(), Fetch> {
        let response = self
            .reader
            .fetch(FetchRequest {
                stream: self.collection.stream,
                partition,
                offset: from,
                max_bytes: self.config.fetch_bytes,
                max_wait: Duration::ZERO,
            })
            .await;
        let fresh = self.fresh_reset.remove(&partition);
        let response = match response {
            Ok(response) => response,
            Err(LogError::OffsetOutOfRange {
                requested,
                log_start_offset,
                ..
            }) if requested < log_start_offset => {
                if fresh {
                    // The live manifest's `applied` is below the log start:
                    // skip the gap, as the link does (row 0.55).
                    self.index_mut().head.insert(partition, log_start_offset);
                    return Ok(());
                }
                self.reset().await.map_err(Fetch::Failed)?;
                return Err(Fetch::Reset);
            }
            Err(err) => return Err(Fetch::Failed(err.into())),
        };
        let partitions = self.collection.partitions;
        let batch = self.config.resolve_batch;
        let durable = self.durable().await.map_err(Fetch::Failed)?.cloned();
        self.index_mut()
            .apply_batch(
                partitions,
                partition,
                &response.records,
                durable.as_ref(),
                batch,
            )
            .await
            .map_err(Fetch::Failed)
    }

    /// Publishes a snapshot, updates the budget and the memory state (rule
    /// 5.6).
    fn publish_checked(&mut self) -> Result<(), TailError> {
        let snapshot = self.index_mut().snapshot()?;
        let bytes = snapshot.bytes();
        let old = self.shared.accounted.swap(bytes, Ordering::Relaxed);
        self.budget.account(old, bytes);
        let head = snapshot.head().clone();
        self.shared.publish(snapshot);
        let over = bytes > self.config.max_bytes || self.budget.is_over();
        match self.shared.state() {
            TailState::Overflow { .. } => {
                if bytes < self.config.max_bytes / 2 && !self.budget.is_over() {
                    self.shared.set_state(TailState::Following);
                } else {
                    self.shared.set_state(TailState::Overflow { head });
                }
            }
            _ if over => self.shared.set_state(TailState::Overflow { head }),
            TailState::Failed(_) => self.shared.set_state(TailState::Following),
            TailState::Following => {}
        }
        Ok(())
    }

    fn publish(&mut self) {
        if let Err(err) = self.publish_checked() {
            tracing::warn!(collection = %self.collection.id, %err, "tail: publishing");
        }
    }
}

/// Why a fetch did not apply.
enum Fetch {
    /// The tail reset (the partition was trimmed).
    Reset,
    Failed(TailError),
}
