//! [`LiveTxn`], the transaction a function runs in, its [`ReadSet`], the
//! [`Function`] trait and the [`Runner`] that runs mutations and queries
//! (design §20 §5.1–§5.2, §8.1; R1 plan Task 10).
//!
//! A **mutation** runs in an optimistic TiKV transaction with a commit
//! token (so an unknown outcome is resolved, never replayed blindly). A
//! conflict reruns the whole function at a new start timestamp, up to
//! [`DEFAULT_MUTATION_ATTEMPTS`] attempts or the mutation deadline. Documents
//! read by id are promoted into the lock set (`lock_keys`), so write skew is
//! impossible on point reads; index-range reads are plain snapshot reads
//! (Q31), unless [`RunnerOptions::serializable_ranges`] is set. The journal
//! entry is appended just before the commit when the function wrote
//! something; a rerun draws its shard among the shards other than the one
//! its previous attempt drew (R1 plan row T10-2).
//!
//! With an **idempotency key**, the record `0x05 ‖ SHA-256(key)[..16]` is
//! read first: a live record returns its result without running the
//! function. Otherwise the record is written in the mutation's transaction
//! with the result. Records live 24 h and the [`Janitor`] deletes them.
//!
//! A **query** runs on a snapshot at a given timestamp and returns its
//! [`ReadSet`]. Limits (§20 §5.1) are counted per attempt.
//!
//! [`Janitor`]: crate::Janitor

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use buffa::Message;
use futures::future::BoxFuture;
use operon_tikv::{CommitMode, Snap, Tikv, Timestamp, TimestampExt, Txn, TxnError, TxnOptions};
use rand::SeedableRng;
use rand::rngs::StdRng;
use sha2::{Digest, Sha256};

use crate::catalog::{self, TableDef};
use crate::docs::{self, Doc, IndexRange, Reads, WriteRecord};
use crate::ids::{DocId, TableId};
use crate::journal::Journal;
use crate::keys::{AppKeys, IDEMPOTENCY_HASH_BYTES, KeyRange};
use crate::{Limits, LiveConfig, LiveError, LiveValue, pb};

/// The attempts a Live mutation gets by default (R1 plan row T10-3; the
/// metastore keeps the runner's 8).
pub const DEFAULT_MUTATION_ATTEMPTS: u32 = 16;

/// How long an idempotency record counts (§20 §5.1).
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// The largest encoded result an idempotent mutation may record (1 MiB).
pub const MAX_IDEMPOTENT_RESULT_BYTES: usize = 1024 * 1024;

/// The longest idempotency key, in bytes.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// The runner's name for a mutation's transactions (fault plans match it).
pub const MUTATION_OP: &str = "live.mutation";

/// Whether a function reads only (a query) or may write (a mutation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FnKind {
    Query,
    Mutation,
}

/// A server function: a built-in `_system:*` function or (Task 13) a
/// deployed JavaScript export. A mutation's `call` is rerun on conflict, so
/// it must have no effect outside `txn`.
pub trait Function: Send + Sync {
    /// The function's path (`_system:insert`, `module:export`).
    fn name(&self) -> &str;
    /// Query or mutation.
    fn kind(&self) -> FnKind;
    /// Runs the function in `txn` with `args`.
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>>;
}

/// What a function read (§20 §8.1): document keys read by id (found or
/// not) and index key ranges `[lo, hi)`, each of one index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadSet {
    pub points: BTreeSet<Vec<u8>>,
    pub ranges: Vec<KeyRange>,
}

impl ReadSet {
    /// Whether a write to `key` can change what was read.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.points.contains(key) || self.ranges.iter().any(|r| r.contains(key))
    }

    /// Whether nothing was read.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty() && self.ranges.is_empty()
    }
}

/// What one attempt of a function used, against [`Limits`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    /// Bytes written: document keys and records, index keys.
    pub written_bytes: usize,
    /// Documents written (inserted, replaced, patched or deleted).
    pub written_docs: usize,
    /// Documents read, by id or by range.
    pub scanned_docs: usize,
    /// Index ranges read.
    pub index_ranges: usize,
}

enum Access<'a> {
    Mutation(&'a mut Txn),
    Query(&'a mut Snap),
}

/// The transaction a function runs in: a TiKV transaction (mutations) or a
/// snapshot (queries), the read set, the writes with their journal records,
/// and the per-attempt limit counters.
pub struct LiveTxn<'a> {
    access: Access<'a>,
    app: &'a AppKeys,
    limits: &'a Limits,
    start_ts: Timestamp,
    tables: HashMap<TableId, Option<TableDef>>,
    names: HashMap<String, TableId>,
    read_set: ReadSet,
    writes: Vec<WriteRecord>,
    usage: Usage,
}

impl fmt::Debug for LiveTxn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveTxn")
            .field("mutation", &self.is_mutation())
            .field("start_ts", &self.start_ts)
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

impl<'a> LiveTxn<'a> {
    /// A mutation's transaction.
    pub fn for_mutation(txn: &'a mut Txn, app: &'a AppKeys, limits: &'a Limits) -> Self {
        let start_ts = txn.start_ts();
        LiveTxn::new(Access::Mutation(txn), app, limits, start_ts)
    }

    /// A query's transaction, reading `snap`.
    pub fn for_query(snap: &'a mut Snap, app: &'a AppKeys, limits: &'a Limits) -> Self {
        let start_ts = snap.ts().clone();
        LiveTxn::new(Access::Query(snap), app, limits, start_ts)
    }

    fn new(access: Access<'a>, app: &'a AppKeys, limits: &'a Limits, start_ts: Timestamp) -> Self {
        LiveTxn {
            access,
            app,
            limits,
            start_ts,
            tables: HashMap::new(),
            names: HashMap::new(),
            read_set: ReadSet::default(),
            writes: Vec::new(),
            usage: Usage::default(),
        }
    }

    /// The timestamp every read sees: the transaction's start timestamp, or
    /// the query's snapshot timestamp.
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts.clone()
    }

    /// Whether this is a mutation (it may write).
    pub fn is_mutation(&self) -> bool {
        matches!(self.access, Access::Mutation(_))
    }

    /// What was read so far.
    pub fn read_set(&self) -> &ReadSet {
        &self.read_set
    }

    /// What this attempt used so far.
    pub fn usage(&self) -> Usage {
        self.usage
    }

    /// The document `id`, or `None`. In a mutation the document key is
    /// locked (§20 §5.2): a concurrent write to it makes one side rerun.
    pub async fn get(&mut self, id: DocId) -> Result<Option<Doc>, LiveError> {
        self.count_scanned(1)?;
        let key = self.app.document(&id);
        let doc = match &mut self.access {
            Access::Mutation(txn) => {
                txn.lock_keys([key.as_slice()]).await?;
                docs::get(&mut **txn, self.app, id).await?
            }
            Access::Query(snap) => docs::get(&mut **snap, self.app, id).await?,
        };
        self.read_set.points.insert(key);
        Ok(doc)
    }

    /// The documents of an index range, in index order. The range is added
    /// to the read set (up to the last key read when a limit is filled). A
    /// range on a table that does not exist reads nothing, and still
    /// depends on the range (its id may be created later); a user index of
    /// a missing table is [`LiveError::NotFound`].
    pub async fn query(&mut self, range: IndexRange) -> Result<Vec<Doc>, LiveError> {
        self.usage.index_ranges += 1;
        if self.usage.index_ranges > self.limits.max_index_ranges {
            return Err(LiveError::limit(
                "max_index_ranges",
                format!(
                    "a function reads more than {} index ranges",
                    self.limits.max_index_ranges
                ),
            ));
        }
        let table = match self.table_by_id(range.table).await? {
            Some(table) => table,
            None => TableDef {
                id: range.table,
                name: format!("#{}", range.table.0),
                indexes: Vec::new(),
                next_index_id: crate::IndexId::FIRST_USER,
            },
        };
        let remaining = self
            .limits
            .max_scanned_docs
            .saturating_sub(self.usage.scanned_docs);
        let limits = Limits {
            max_scanned_docs: remaining,
            ..self.limits.clone()
        };
        let (found, read) = match &mut self.access {
            Access::Mutation(txn) => {
                docs::scan(&mut **txn, self.app, &table, &range, &limits).await
            }
            Access::Query(snap) => docs::scan(&mut **snap, self.app, &table, &range, &limits).await,
        }
        .map_err(|e| self.scan_limit(e))?;
        self.count_scanned(found.len())?;
        self.read_set.ranges.push(read);
        Ok(found)
    }

    /// The table named `name`, or `None`. When it does not exist, the read
    /// set gains the index entries of every table not created yet, so the
    /// insert that creates it invalidates the read.
    pub async fn table(&mut self, name: &str) -> Result<Option<TableDef>, LiveError> {
        if let Some(id) = self.names.get(name) {
            return Ok(self.tables.get(id).cloned().flatten());
        }
        let found = match &mut self.access {
            Access::Mutation(txn) => catalog::load_table(&mut **txn, self.app, name).await?,
            Access::Query(snap) => catalog::load_table(&mut **snap, self.app, name).await?,
        };
        match found {
            Some(table) => {
                self.remember(&table);
                Ok(Some(table))
            }
            None => {
                let counter = self.app.table_counter();
                let next = match &mut self.access {
                    Access::Mutation(txn) => txn.get(&counter).await?,
                    Access::Query(snap) => snap.get(&counter).await?,
                };
                let next = match next {
                    None => 1,
                    Some(bytes) => {
                        u32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                            LiveError::Corrupt("the table counter is not 4 bytes".into())
                        })?)
                    }
                };
                self.read_set
                    .ranges
                    .push(self.app.tables_from(TableId(next)));
                Ok(None)
            }
        }
    }

    /// Inserts a document into the table named `table` (created on first
    /// insert unless a schema is deployed); returns its id.
    pub async fn insert(
        &mut self,
        table: &str,
        fields: BTreeMap<String, LiveValue>,
    ) -> Result<DocId, LiveError> {
        let Access::Mutation(txn) = &mut self.access else {
            return Err(query_write("insert"));
        };
        let def = catalog::table_for_insert(txn, self.app, table, self.limits).await?;
        let (id, record, bytes) =
            docs::insert_sized(txn, self.app, &def, fields, self.limits).await?;
        self.remember(&def);
        self.wrote(record, bytes)?;
        Ok(id)
    }

    /// Sets the given fields of document `id`, keeping the others.
    pub async fn patch(
        &mut self,
        id: DocId,
        fields: BTreeMap<String, LiveValue>,
    ) -> Result<(), LiveError> {
        self.rewrite(id, fields, true).await
    }

    /// Replaces the fields of document `id`.
    pub async fn replace(
        &mut self,
        id: DocId,
        fields: BTreeMap<String, LiveValue>,
    ) -> Result<(), LiveError> {
        self.rewrite(id, fields, false).await
    }

    /// Deletes document `id`; a missing document is [`LiveError::NotFound`].
    pub async fn delete(&mut self, id: DocId) -> Result<(), LiveError> {
        if !self.is_mutation() {
            return Err(query_write("delete"));
        }
        let table = self.existing_table(id).await?;
        self.count_scanned(1)?;
        let Access::Mutation(txn) = &mut self.access else {
            unreachable!("checked above");
        };
        let record = docs::delete(txn, self.app, &table, id, self.limits)
            .await?
            .ok_or_else(|| {
                LiveError::NotFound(format!("document {id} in table '{}'", table.name))
            })?;
        let bytes = self.app.document(&id).len()
            + record
                .index_keys_removed
                .iter()
                .map(Vec::len)
                .sum::<usize>();
        self.read_set.points.insert(self.app.document(&id));
        self.wrote(record, bytes)
    }

    async fn rewrite(
        &mut self,
        id: DocId,
        fields: BTreeMap<String, LiveValue>,
        merge: bool,
    ) -> Result<(), LiveError> {
        if !self.is_mutation() {
            return Err(query_write(if merge { "patch" } else { "replace" }));
        }
        let table = self.existing_table(id).await?;
        self.count_scanned(1)?;
        let Access::Mutation(txn) = &mut self.access else {
            unreachable!("checked above");
        };
        let (record, bytes) = if merge {
            docs::patch_sized(txn, self.app, &table, id, fields, self.limits).await?
        } else {
            docs::replace_sized(txn, self.app, &table, id, fields, self.limits).await?
        };
        self.read_set.points.insert(self.app.document(&id));
        self.wrote(record, bytes)
    }

    /// The table of `id`, which must exist.
    async fn existing_table(&mut self, id: DocId) -> Result<TableDef, LiveError> {
        self.table_by_id(id.table).await?.ok_or_else(|| {
            LiveError::NotFound(format!(
                "document {id}: its table {} does not exist",
                id.table.0
            ))
        })
    }

    async fn table_by_id(&mut self, id: TableId) -> Result<Option<TableDef>, LiveError> {
        if let Some(found) = self.tables.get(&id) {
            return Ok(found.clone());
        }
        let found = match &mut self.access {
            Access::Mutation(txn) => catalog::load_table_by_id(&mut **txn, self.app, id).await?,
            Access::Query(snap) => catalog::load_table_by_id(&mut **snap, self.app, id).await?,
        };
        match &found {
            Some(table) => self.remember(table),
            None => {
                self.tables.insert(id, None);
            }
        }
        Ok(found)
    }

    fn remember(&mut self, table: &TableDef) {
        self.names.insert(table.name.clone(), table.id);
        self.tables.insert(table.id, Some(table.clone()));
    }

    fn wrote(&mut self, record: WriteRecord, bytes: usize) -> Result<(), LiveError> {
        self.usage.written_docs += 1;
        self.usage.written_bytes += bytes;
        self.writes.push(record);
        if self.usage.written_docs > self.limits.max_written_docs {
            return Err(LiveError::limit(
                "max_written_docs",
                format!(
                    "a mutation writes more than {} documents",
                    self.limits.max_written_docs
                ),
            ));
        }
        if self.usage.written_bytes > self.limits.max_written_bytes {
            return Err(LiveError::limit(
                "max_written_bytes",
                format!(
                    "a mutation writes more than {} bytes",
                    self.limits.max_written_bytes
                ),
            ));
        }
        Ok(())
    }

    fn count_scanned(&mut self, n: usize) -> Result<(), LiveError> {
        self.usage.scanned_docs += n;
        if self.usage.scanned_docs > self.limits.max_scanned_docs {
            return Err(self.scanned_over());
        }
        Ok(())
    }

    fn scanned_over(&self) -> LiveError {
        LiveError::limit(
            "max_scanned_docs",
            format!(
                "a function reads more than {} documents",
                self.limits.max_scanned_docs
            ),
        )
    }

    /// A scan's own `max_scanned_docs` refusal, restated for the whole
    /// function (the scan saw only what was left of the budget).
    fn scan_limit(&self, e: LiveError) -> LiveError {
        match e {
            LiveError::LimitExceeded {
                limit: "max_scanned_docs",
                ..
            } => self.scanned_over(),
            other => other,
        }
    }
}

/// The options of a [`Runner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerOptions {
    /// Attempts per mutation ([`DEFAULT_MUTATION_ATTEMPTS`]).
    pub max_attempts: u32,
    /// How mutations commit: two-phase commit until `tikv-client` resolves
    /// async-commit locks on the read path (R1 plan row T7-1).
    pub commit_mode: CommitMode,
    /// Opt-in: a mutation that read an index range and writes also locks
    /// every journal head, so it conflicts with every mutation that commits
    /// between its start and its commit, and range write skew cannot occur.
    /// Correct but coarse (such mutations serialize with all writers); off
    /// by default until Q31 is decided (R1 plan row T10-4).
    pub serializable_ranges: bool,
}

impl Default for RunnerOptions {
    fn default() -> Self {
        RunnerOptions {
            max_attempts: DEFAULT_MUTATION_ATTEMPTS,
            commit_mode: CommitMode::TwoPc,
            serializable_ranges: false,
        }
    }
}

/// A committed mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct Mutated {
    /// Its commit timestamp. For a replay (an idempotency hit) and for a
    /// commit resolved through its token, the timestamp of the read that
    /// found the record or token: at or after the commit.
    pub commit_ts: Timestamp,
    /// What the function returned.
    pub result: LiveValue,
    /// Attempts, from 1.
    pub attempts: u32,
    /// The journal shard and last sequence of its entry; `None` when it
    /// wrote nothing (or was replayed).
    pub journal: Option<(u16, u64)>,
    /// The result came from the idempotency record; the function did not
    /// run.
    pub replayed: bool,
    /// The committing attempt's outcome was unknown and resolved through
    /// the commit token.
    pub earlier_unknown: bool,
    /// What the committing attempt used.
    pub usage: Usage,
}

/// A query's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Queried {
    pub result: LiveValue,
    pub read_set: ReadSet,
    /// The snapshot timestamp it read at.
    pub ts: Timestamp,
    pub usage: Usage,
}

/// Runs mutations and queries of one app. Cheap to clone.
#[derive(Clone)]
pub struct Runner {
    inner: Arc<Inner>,
}

struct Inner {
    tikv: Tikv,
    app: AppKeys,
    limits: Limits,
    options: RunnerOptions,
    /// Mutations hold it shared for their whole run; [`Runner::try_quiesce`]
    /// takes it exclusively.
    gate: Arc<tokio::sync::RwLock<()>>,
}

/// Exclusive admission to an app's mutations, from [`Runner::try_quiesce`]:
/// while it lives no mutation of this runner is in flight and new ones wait.
#[derive(Debug)]
pub struct Quiesced {
    _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl fmt::Debug for Runner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runner")
            .field("tikv", &self.inner.tikv)
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

/// How one mutation attempt ended.
enum Outcome {
    Ran {
        result: LiveValue,
        journal: Option<(u16, u64)>,
        usage: Usage,
    },
    Replayed(LiveValue),
}

/// State kept across the attempts of one mutation.
#[derive(Default)]
struct Attempts {
    /// The journal shard the previous attempt drew.
    last_shard: Option<u16>,
    /// The function's own error, which ended the run (its writes are rolled
    /// back).
    failure: Option<LiveError>,
}

impl Runner {
    /// A runner for the app on `tikv` (whose root is the app's), with the
    /// config's limits and the default options. Writes the app's journal
    /// shard count (`config.journal_shards`) when the app has none yet; a
    /// stored count wins over the config (R1 plan row T10-1).
    pub async fn open(tikv: Tikv, config: &LiveConfig) -> Result<Self, LiveError> {
        Runner::open_with(tikv, config, RunnerOptions::default()).await
    }

    /// Like [`open`](Self::open), with `options`.
    pub async fn open_with(
        tikv: Tikv,
        config: &LiveConfig,
        options: RunnerOptions,
    ) -> Result<Self, LiveError> {
        let app = AppKeys::dedicated();
        let shards = config.journal_shards;
        let keys = app.clone();
        let stored = tikv
            .run(catalog_txn("live.app.open", &options), move |txn| {
                let keys = keys.clone();
                Box::pin(
                    async move { lift(catalog::ensure_journal_shards(txn, &keys, shards).await) },
                )
            })
            .await?
            .value?;
        if stored != shards {
            tracing::warn!(
                app = %config.app,
                stored,
                configured = shards,
                "the app's stored journal shard count differs from the configured one; using the stored one"
            );
        }
        Ok(Runner {
            inner: Arc::new(Inner {
                tikv,
                app,
                limits: config.limits.clone(),
                options,
                gate: Arc::default(),
            }),
        })
    }

    /// The TiKV handle.
    pub fn tikv(&self) -> &Tikv {
        &self.inner.tikv
    }

    /// The app's keys.
    pub fn app(&self) -> &AppKeys {
        &self.inner.app
    }

    /// The options.
    pub fn options(&self) -> &RunnerOptions {
        &self.inner.options
    }

    /// Exclusive admission for a change that must not race mutations
    /// (Task 13's index-changing `Deploy`, R1 plan row T9-1 and the review
    /// of #73): `None` while a mutation of this runner is in flight (the
    /// caller answers "busy" and the client retries); otherwise, until the
    /// returned guard drops, new mutations wait at admission, so none can
    /// start between the in-flight check and the change's commit. Per
    /// runner, so per node: R1 serves an app from one node.
    pub fn try_quiesce(&self) -> Option<Quiesced> {
        self.inner
            .gate
            .clone()
            .try_write_owned()
            .ok()
            .map(|guard| Quiesced { _guard: guard })
    }

    /// The app's journal, with its stored shard count (read now).
    pub async fn journal(&self) -> Result<Journal, LiveError> {
        let at = self
            .inner
            .tikv
            .now()
            .await
            .map_err(|e| LiveError::Internal(e.to_string()))?;
        let mut snap = snapshot(&self.inner.tikv, at).await?;
        let shards = catalog::load_journal_shards(&mut snap, &self.inner.app)
            .await?
            .ok_or_else(no_app_record)?;
        Journal::new(self.inner.app.clone(), shards)
    }

    /// Changes the app's journal shard count; refused unless the journal is
    /// empty (R1 plan row T10-1). Consumers built on the old count must be
    /// rebuilt.
    pub async fn set_journal_shards(&self, shards: u16) -> Result<(), LiveError> {
        let keys = self.inner.app.clone();
        self.inner
            .tikv
            .run(
                catalog_txn("live.app.journal_shards", &self.inner.options),
                move |txn| {
                    let keys = keys.clone();
                    Box::pin(
                        async move { lift(catalog::set_journal_shards(txn, &keys, shards).await) },
                    )
                },
            )
            .await?
            .value
    }

    /// Runs mutation `f` with `args` and commits it (semantics 1–3, 5).
    ///
    /// With `idempotency_key`, a live record of the key returns its result
    /// (`replayed`) without running `f`; otherwise the record is written with
    /// the result, in the same transaction. A function error rolls the
    /// attempt back and is returned; a conflict reruns `f` from scratch.
    pub async fn mutate(
        &self,
        f: Arc<dyn Function>,
        args: LiveValue,
        idempotency_key: Option<String>,
    ) -> Result<Mutated, LiveError> {
        if f.kind() != FnKind::Mutation {
            return Err(LiveError::invalid(format!(
                "{} is a query; run it with Query",
                f.name()
            )));
        }
        let hash = idempotency_key
            .as_deref()
            .map(idempotency_hash)
            .transpose()?;
        let _admitted = self.inner.gate.read().await;
        let request_id = idempotency_key.unwrap_or_default();
        let inner = self.inner.clone();
        let state = Arc::new(Mutex::new(Attempts::default()));
        let mut opts = TxnOptions::new(MUTATION_OP).with_token();
        opts.max_attempts = inner.options.max_attempts;
        opts.deadline = inner.limits.mutation_deadline;
        opts.commit_mode = Some(inner.options.commit_mode);
        let body_state = state.clone();
        let run = self
            .inner
            .tikv
            .run(opts, move |txn| {
                let inner = inner.clone();
                let f = f.clone();
                let args = args.clone();
                let request_id = request_id.clone();
                let state = body_state.clone();
                Box::pin(async move {
                    let attempt =
                        mutation_attempt(txn, &inner, &*f, args, hash, request_id, &state);
                    match attempt.await {
                        Ok(outcome) => Ok(outcome),
                        Err(e) => match e.into_txn() {
                            Err(storage) => Err(storage),
                            Ok(own) => {
                                lock(&state).failure = Some(own);
                                Err(TxnError::Fatal("the function failed".into()))
                            }
                        },
                    }
                })
            })
            .await;
        let committed = match run {
            Ok(c) => c,
            Err(e) => {
                if let Some(own) = lock(&state).failure.take() {
                    return Err(own);
                }
                return Err(LiveError::Txn(e));
            }
        };
        Ok(match committed.value {
            Outcome::Ran {
                result,
                journal,
                usage,
            } => Mutated {
                commit_ts: committed.commit_ts,
                result,
                attempts: committed.attempts,
                journal,
                replayed: false,
                earlier_unknown: committed.earlier_unknown,
                usage,
            },
            Outcome::Replayed(result) => Mutated {
                commit_ts: committed.commit_ts,
                result,
                attempts: committed.attempts,
                journal: None,
                replayed: true,
                earlier_unknown: committed.earlier_unknown,
                usage: Usage::default(),
            },
        })
    }

    /// Runs query `f` with `args` on a snapshot at `at` (semantics 4).
    pub async fn query(
        &self,
        f: &dyn Function,
        args: LiveValue,
        at: Timestamp,
    ) -> Result<Queried, LiveError> {
        if f.kind() != FnKind::Query {
            return Err(LiveError::invalid(format!(
                "{} is a mutation; run it with Mutate",
                f.name()
            )));
        }
        let mut snap = snapshot(&self.inner.tikv, at.clone()).await?;
        let mut txn = LiveTxn::for_query(&mut snap, &self.inner.app, &self.inner.limits);
        let result =
            tokio::time::timeout(self.inner.limits.mutation_deadline, f.call(&mut txn, args))
                .await
                .map_err(|_| LiveError::Txn(TxnError::Deadline))??;
        Ok(Queried {
            result,
            usage: txn.usage,
            read_set: txn.read_set,
            ts: at,
        })
    }
}

/// One attempt of a mutation, inside its transaction.
async fn mutation_attempt(
    txn: &mut Txn,
    inner: &Inner,
    f: &dyn Function,
    args: LiveValue,
    hash: Option<[u8; IDEMPOTENCY_HASH_BYTES]>,
    request_id: String,
    state: &Mutex<Attempts>,
) -> Result<Outcome, LiveError> {
    let app = &inner.app;
    let now_ms = Tikv::physical_ms(&txn.start_ts());
    let mut keys = vec![app.app_def()];
    if let Some(hash) = &hash {
        keys.push(app.idempotency(hash));
    }
    let found: HashMap<Vec<u8>, Vec<u8>> = txn.batch_get(keys).await?.into_iter().collect();
    let shards = catalog::load_journal_shards(&mut Found(&found), app)
        .await?
        .ok_or_else(no_app_record)?;
    if let Some(hash) = &hash
        && let Some(bytes) = found.get(&app.idempotency(hash))
    {
        let record = decode_idempotency(bytes)?;
        if record.expires_ms > now_ms {
            if record.function != f.name() {
                return Err(LiveError::invalid(format!(
                    "the idempotency key was used for {}, not {}",
                    record.function,
                    f.name()
                )));
            }
            let result = match record.result.as_option() {
                Some(v) => LiveValue::from_proto(v.clone())
                    .map_err(|e| LiveError::Corrupt(format!("an idempotency record: {e}")))?,
                None => LiveValue::Null,
            };
            return Ok(Outcome::Replayed(result));
        }
    }

    let mut live = LiveTxn::for_mutation(txn, app, &inner.limits);
    let result = f.call(&mut live, args).await?;
    let has_ranges = !live.read_set.ranges.is_empty();
    let usage = live.usage;
    let writes = std::mem::take(&mut live.writes);
    drop(live);
    if writes.is_empty() {
        // Read-only: no journal entry and no idempotency record; a retry of
        // the call just reads again.
        return Ok(Outcome::Ran {
            result,
            journal: None,
            usage,
        });
    }

    if let Some(hash) = &hash {
        let record = pb::IdempotencyRecord {
            format: 1,
            result: result.to_proto().into(),
            expires_ms: now_ms
                .saturating_add(u64::try_from(IDEMPOTENCY_TTL.as_millis()).unwrap_or(u64::MAX)),
            function: f.name().to_string(),
            start_ts: txn.start_ts().version(),
            ..Default::default()
        };
        let bytes = record.encode_to_vec();
        if bytes.len() > MAX_IDEMPOTENT_RESULT_BYTES + 1024 {
            return Err(LiveError::limit(
                "max_idempotent_result_bytes",
                format!(
                    "an idempotent mutation's result encodes to more than {MAX_IDEMPOTENT_RESULT_BYTES} bytes"
                ),
            ));
        }
        txn.put(&app.idempotency(hash), bytes).await?;
    }

    let journal = Journal::new(app.clone(), shards)?;
    if inner.options.serializable_ranges && has_ranges {
        txn.lock_keys((0..shards).map(|s| app.journal_head(s)))
            .await?;
    }
    let shard = {
        let mut rng = StdRng::from_rng(&mut rand::rng());
        let mut state = lock(state);
        let shard = journal.pick_other(&mut rng, state.last_shard);
        state.last_shard = Some(shard);
        shard
    };
    let entry = pb::JournalEntry {
        writes,
        function: f.name().to_string(),
        request_id,
        ..Default::default()
    };
    let position = journal.append_to(txn, shard, entry).await?;
    Ok(Outcome::Ran {
        result,
        journal: Some(position),
        usage,
    })
}

/// Reads answered from one batch get.
struct Found<'a>(&'a HashMap<Vec<u8>, Vec<u8>>);

impl Reads for Found<'_> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        Ok(self.0.get(key).cloned())
    }

    async fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> Result<Vec<operon_tikv::Pair>, TxnError> {
        let mut out: Vec<_> = keys
            .into_iter()
            .filter_map(|k| self.0.get(&k).cloned().map(|v| (k, v)))
            .collect();
        out.sort();
        Ok(out)
    }

    async fn scan(
        &mut self,
        _range: &KeyRange,
        _limit: usize,
        _reverse: bool,
    ) -> Result<Vec<operon_tikv::Pair>, TxnError> {
        Err(TxnError::Fatal("no scans over a batch get".into()))
    }
}

/// The key hash of an idempotency key: the first 16 bytes of its SHA-256.
pub fn idempotency_hash(key: &str) -> Result<[u8; IDEMPOTENCY_HASH_BYTES], LiveError> {
    if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(LiveError::invalid(format!(
            "an idempotency key has 1 to {MAX_IDEMPOTENCY_KEY_BYTES} bytes"
        )));
    }
    let digest = Sha256::digest(key.as_bytes());
    let mut hash = [0; IDEMPOTENCY_HASH_BYTES];
    hash.copy_from_slice(&digest[..IDEMPOTENCY_HASH_BYTES]);
    Ok(hash)
}

/// Decodes an idempotency record.
pub(crate) fn decode_idempotency(bytes: &[u8]) -> Result<pb::IdempotencyRecord, LiveError> {
    let record = pb::IdempotencyRecord::decode_from_slice(bytes)
        .map_err(|e| LiveError::Corrupt(format!("an idempotency record: {e}")))?;
    if record.format != 1 {
        return Err(LiveError::Corrupt(format!(
            "idempotency record format {} (expected 1)",
            record.format
        )));
    }
    Ok(record)
}

fn query_write(op: &str) -> LiveError {
    LiveError::invalid(format!(
        "a query cannot write ({op}); call it from a mutation"
    ))
}

fn no_app_record() -> LiveError {
    LiveError::FailedPrecondition(
        "the app has no settings record: open its Runner first".to_string(),
    )
}

fn catalog_txn(op: &'static str, options: &RunnerOptions) -> TxnOptions {
    let mut opts = TxnOptions::new(op);
    opts.commit_mode = Some(options.commit_mode);
    opts
}

fn lock(state: &Mutex<Attempts>) -> std::sync::MutexGuard<'_, Attempts> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A run body's result: a storage error goes back to the runner (which
/// retries conflicts); any other stays in the value.
fn lift<T>(r: Result<T, LiveError>) -> Result<Result<T, LiveError>, TxnError> {
    match r {
        Ok(v) => Ok(Ok(v)),
        Err(e) => e.into_txn().map(Err),
    }
}

async fn snapshot(tikv: &Tikv, at: Timestamp) -> Result<Snap, LiveError> {
    tikv.snapshot(at)
        .await
        .map_err(|e| LiveError::Internal(format!("a snapshot: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_hash_is_the_sha256_prefix() {
        let hash = idempotency_hash("abc").expect("a valid key");
        // SHA-256("abc") = ba7816bf 8f01cfea 414140de 5dae2223 …
        assert_eq!(
            hash,
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23
            ]
        );
        assert!(idempotency_hash("").is_err());
        assert!(idempotency_hash(&"k".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)).is_err());
    }

    #[test]
    fn read_set_covers_its_points_and_ranges() {
        let mut rs = ReadSet::default();
        assert!(rs.is_empty());
        rs.points.insert(vec![2, 1]);
        rs.ranges.push(KeyRange {
            lo: vec![3, 0],
            hi: vec![3, 5],
        });
        assert!(rs.covers(&[2, 1]));
        assert!(rs.covers(&[3, 4, 9]));
        assert!(!rs.covers(&[3, 5]));
        assert!(!rs.covers(&[2, 2]));
    }

    #[test]
    fn mutations_get_sixteen_attempts_and_two_phase_commit() {
        let o = RunnerOptions::default();
        assert_eq!(o.max_attempts, 16);
        assert_eq!(o.commit_mode, CommitMode::TwoPc);
        assert!(!o.serializable_ranges);
        assert_eq!(TxnOptions::new("meta").max_attempts, 8);
    }
}
