//! The operations API (D1 Task 7, D146, design §21 §6.4 and §6.7): a long
//! operation is a durable promise, submitted, then polled.
//!
//! - **Submit.** The root promise is created in process (`promise.create`)
//!   with the tags `loam:op`, `loam:kind`, `loam:ns` and
//!   `resonate:target = inproc://any@loam`, and a 7-day `timeoutAt`. Its
//!   `param` is what the Rust SDK writes for an `rpc` of the kind's workflow
//!   (`{func, args}`, base64 JSON), so the server's task reaches Loam's
//!   runtime (`worker_inproc`) and the SDK runs the workflow with an
//!   [`OpInput`]. An idempotency key names the operation
//!   ([`OperationId::for_key`]); `promise.create` of an existing id returns
//!   the existing promise, whose `params_hash` then decides between the same
//!   operation and `idempotency_key_reused`.
//! - **State** maps from the root promise and its task (§21 §6.4,
//!   [`map_state`]). **Progress** is the kind's progress function, or by
//!   default the steps of the operation's origin, cached for 2 s.
//! - **Cancel** settles the root `rejected_canceled`; workflows stop at the
//!   next step boundary through [`check_canceled`].
//! - **Retention** deletes finished operations older than 7 days (Ruling 8)
//!   with SQL on the store (T0-12, X9): the protocol has no delete.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use resonate_sdk::prelude::{Context, Durable, Resonate};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::config::DurableStore;
use crate::embed::DurableClient;
use crate::error::DurableError;
use crate::ids::{OPERATION_PREFIX, OperationId};
use crate::inproc::{GROUP, SCHEME};

/// The worker task that prunes finished operations (Ruling 8).
pub const RETENTION_TASK: &str = "durable-op-retention";

/// The tag every operation's root promise carries: its own id.
pub const TAG_OP: &str = "loam:op";
/// The root's kind (`collection.import`, …).
pub const TAG_KIND: &str = "loam:kind";
/// The root's namespace. Only roots carry it, so a search by it lists
/// operations.
pub const TAG_NS: &str = "loam:ns";

/// The longest idempotency key.
pub const MAX_KEY_LEN: usize = 255;

/// What an operation is doing (§21 §6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    /// Submitted; no runtime holds its task yet.
    Queued,
    /// A runtime holds its task, or it waits on a step.
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl OperationState {
    /// The state as the API spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    /// Whether the operation is over.
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

impl std::str::FromStr for OperationState {
    type Err = OpsError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(match text {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "canceled" => Self::Canceled,
            other => {
                return Err(OpsError::Invalid(format!(
                    "unknown operation state {other:?}; use queued, running, succeeded, failed \
                     or canceled"
                )));
            }
        })
    }
}

/// Why an operation failed or stopped: a machine-readable `code` and a
/// message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationError {
    pub code: String,
    pub message: String,
}

/// An operation as `GET /v1/operations/{id}` shows it. Times are Unix
/// milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Operation {
    pub id: OperationId,
    pub kind: String,
    pub namespace: String,
    /// What it works on: the `target` of its parameters (for an import,
    /// `{"collection": …}`), else null.
    pub target: Value,
    pub state: OperationState,
    pub progress: Value,
    pub result: Option<Value>,
    pub error: Option<OperationError>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The argument every operation's workflow gets, as it is stored in the root
/// promise's `param`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpInput {
    pub id: OperationId,
    pub kind: String,
    pub namespace: String,
    pub params: Value,
    /// SHA-256 of the canonical `{kind, namespace, params}`: an idempotent
    /// resubmit must match it (D146).
    pub params_hash: String,
    /// Unique per submit call: tells the submit that created the root from
    /// one that found it.
    pub submission: String,
}

/// What the operations API refuses or cannot do.
#[derive(Debug)]
pub enum OpsError {
    /// No such operation (404 `not_found`).
    NotFound(String),
    /// The idempotency key names an operation with other parameters (409
    /// `idempotency_key_reused`).
    IdempotencyKeyReused(OperationId),
    /// The operation is over and cannot be canceled (409
    /// `operation_finished`).
    Finished(OperationId),
    /// No workflow for this kind (400).
    UnknownKind(String),
    /// A bad request (400 `invalid_argument`).
    Invalid(String),
    /// The durable server cannot answer now (503 `durable_unavailable`).
    Unavailable(String),
    /// Anything else (500).
    Internal(String),
}

impl OpsError {
    /// The API error code (`{"error": code}`).
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::IdempotencyKeyReused(_) => "idempotency_key_reused",
            Self::Finished(_) => "operation_finished",
            Self::UnknownKind(_) | Self::Invalid(_) => "invalid_argument",
            Self::Unavailable(_) => "durable_unavailable",
            Self::Internal(_) => "internal",
        }
    }
}

impl fmt::Display for OpsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "no operation {id}"),
            Self::IdempotencyKeyReused(id) => write!(
                f,
                "the idempotency key already names operation {id}, with other parameters"
            ),
            Self::Finished(id) => write!(f, "operation {id} is finished"),
            Self::UnknownKind(kind) => write!(f, "no operation kind {kind:?}"),
            Self::Invalid(message) => f.write_str(message),
            Self::Unavailable(message) => {
                write!(f, "durable execution is unavailable: {message}")
            }
            Self::Internal(message) => write!(f, "operations: {message}"),
        }
    }
}

impl std::error::Error for OpsError {}

impl From<DurableError> for OpsError {
    fn from(err: DurableError) -> Self {
        match err {
            DurableError::Unavailable(message) => Self::Unavailable(message),
            other => Self::Internal(other.to_string()),
        }
    }
}

/// A kind's progress: the operation's id and its client, to a JSON value.
pub type ProgressFn = Arc<
    dyn Fn(
            DurableClient,
            OperationId,
        ) -> Pin<Box<dyn Future<Output = Result<Value, OpsError>> + Send>>
        + Send
        + Sync,
>;

/// Registers one durable function on the SDK.
type Register = Arc<dyn Fn(&Resonate) -> resonate_sdk::error::Result<()> + Send + Sync>;

/// Adds one dependency to the SDK before it starts (`Resonate::with_dependency`).
type Dependency = Arc<dyn Fn(Resonate) -> Resonate + Send + Sync>;

/// Loam's operation kinds, the durable functions they run on, and their
/// progress functions. [`register`](Self::register) is what
/// `DurableRuntime::start_with` calls (T6-4).
#[derive(Clone, Default)]
pub struct OperationKinds {
    kinds: Vec<String>,
    progress: HashMap<String, ProgressFn>,
    registers: Vec<Register>,
    dependencies: Vec<Dependency>,
    resumable: Vec<String>,
}

impl fmt::Debug for OperationKinds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationKinds")
            .field("kinds", &self.kinds)
            .finish_non_exhaustive()
    }
}

impl OperationKinds {
    /// No kinds.
    pub fn new() -> Self {
        Self::default()
    }

    /// A kind named after its workflow (`#[resonate_sdk::function(name =
    /// "collection.import")]`), which takes an [`OpInput`].
    pub fn kind<D, T>(mut self, workflow: D) -> Self
    where
        D: Durable<OpInput, T> + Copy + Send + Sync + 'static,
        T: Serialize + Send + 'static,
    {
        self.kinds.push(D::NAME.to_string());
        self.registers
            .push(Arc::new(move |sdk: &Resonate| sdk.register(workflow)));
        self
    }

    /// Another durable function the kinds' workflows call (a step, a
    /// branch).
    pub fn function<D, Args, T>(mut self, function: D) -> Self
    where
        D: Durable<Args, T> + Copy + Send + Sync + 'static,
        Args: serde::de::DeserializeOwned + Send + 'static,
        T: Serialize + Send + 'static,
    {
        self.registers
            .push(Arc::new(move |sdk: &Resonate| sdk.register(function)));
        self
    }

    /// A kind whose workflow is registered some other way: a test hook for
    /// submitting operations that no runtime runs (they stay queued).
    #[doc(hidden)]
    pub fn declare(mut self, kind: &str) -> Self {
        self.kinds.push(kind.to_string());
        self
    }

    /// `kind`'s progress, instead of the default step count.
    pub fn progress(mut self, kind: &str, progress: ProgressFn) -> Self {
        self.progress.insert(kind.to_string(), progress);
        self
    }

    /// A value the kinds' functions read with `ctx.get_dependency::<T>()`
    /// (D1 Task 8: the import's sink and sources). It is added when the
    /// runtime is built ([`crate::DurableRuntime::start_kinds`]).
    pub fn dependency<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.dependencies.push(Arc::new(move |sdk: Resonate| {
            sdk.with_dependency(value.clone())
        }));
        self
    }

    /// `kind` resumes when it failed and is resubmitted with the same
    /// idempotency key and parameters (design §21 §7.2): its failed origin is
    /// deleted and the root created again, while the work it keeps in other
    /// origins tagged `loam:op` (an import's file roots) is reused. Other
    /// kinds answer the failed operation.
    pub fn resumable(mut self, kind: &str) -> Self {
        self.resumable.push(kind.to_string());
        self
    }

    /// `sdk` with every [`dependency`](Self::dependency).
    pub fn with_dependencies(&self, sdk: Resonate) -> Resonate {
        self.dependencies
            .iter()
            .fold(sdk, |sdk, dependency| dependency(sdk))
    }

    /// Register every function on `sdk`.
    pub fn register(&self, sdk: &Resonate) -> resonate_sdk::error::Result<()> {
        self.registers.iter().try_for_each(|register| register(sdk))
    }

    /// The kinds' names.
    pub fn names(&self) -> &[String] {
        &self.kinds
    }
}

/// Loam's own operation kinds, which `operon` registers on its runtime
/// through [`crate::DurableRuntime::start_kinds`] (T6-4): `collection.import`
/// (D1 Task 8), over `import`.
pub fn loam_kinds(import: crate::import::ImportEnv) -> OperationKinds {
    crate::import::kinds(OperationKinds::new(), import)
}

/// Settings of [`Operations`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsConfig {
    /// Operations per page of `list`. Default 100.
    pub page_size: usize,
    /// The root promise's timeout: an operation still running then fails
    /// with `deadline_exceeded`. Default 7 days (§21 §6.4).
    pub operation_timeout: Duration,
    /// How long a finished operation stays listed. Default 7 days (Ruling 8).
    pub retention: Duration,
    /// How long a computed progress is reused. Default 2 s (§21 §6.4).
    pub progress_ttl: Duration,
}

impl Default for OpsConfig {
    fn default() -> Self {
        const WEEK: Duration = Duration::from_secs(7 * 24 * 3600);
        Self {
            page_size: 100,
            operation_timeout: WEEK,
            retention: WEEK,
            progress_ttl: Duration::from_secs(2),
        }
    }
}

/// A computed progress and the latest step activity it saw.
#[derive(Clone)]
struct Progress {
    at: Instant,
    value: Value,
    activity: i64,
    /// Computed once the operation was finished: it no longer changes.
    finished: bool,
}

/// The operations of one durable server (D146).
pub struct Operations {
    client: DurableClient,
    store: DurableStore,
    kinds: HashMap<String, Option<ProgressFn>>,
    resumable: Vec<String>,
    config: OpsConfig,
    progress: Mutex<HashMap<String, Progress>>,
}

impl fmt::Debug for Operations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operations")
            .field("store", &self.store)
            .field("kinds", &self.kinds.keys().collect::<Vec<_>>())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Operations {
    /// The operations of the server `client` reaches, whose store is
    /// `store`, for `kinds`.
    pub fn new(
        client: DurableClient,
        store: DurableStore,
        kinds: &OperationKinds,
        config: OpsConfig,
    ) -> Self {
        let resumable = kinds.resumable.clone();
        let kinds = kinds
            .names()
            .iter()
            .map(|kind| (kind.clone(), kinds.progress.get(kind).cloned()))
            .collect();
        Self {
            client,
            store,
            resumable,
            kinds,
            config,
            progress: Mutex::new(HashMap::new()),
        }
    }

    /// Start a `kind` operation in namespace `ns` with `params`. With an
    /// idempotency key, an operation the key already names is returned
    /// (`created = false`) when its parameters are the same, and refused
    /// with [`OpsError::IdempotencyKeyReused`] when they are not.
    pub async fn submit(
        &self,
        ns: &str,
        kind: &str,
        params: Value,
        idempotency_key: Option<&str>,
    ) -> Result<(OperationId, bool), OpsError> {
        if ns.is_empty() {
            return Err(OpsError::Invalid("the namespace is empty".into()));
        }
        if !self.kinds.contains_key(kind) {
            return Err(OpsError::UnknownKind(kind.to_string()));
        }
        if let Some(key) = idempotency_key {
            if key.is_empty() || key.len() > MAX_KEY_LEN {
                return Err(OpsError::Invalid(format!(
                    "an Idempotency-Key is 1 to {MAX_KEY_LEN} bytes; got {}",
                    key.len()
                )));
            }
            if key.chars().any(char::is_control) {
                return Err(OpsError::Invalid(
                    "an Idempotency-Key holds no control characters".into(),
                ));
            }
        }
        let params_hash = params_hash(kind, ns, &params);
        let id = match idempotency_key {
            Some(key) => OperationId::for_key(ns, key),
            None => OperationId::generate(),
        };
        let submission = ulid::Ulid::generate().to_string();
        let input = OpInput {
            id: id.clone(),
            kind: kind.to_string(),
            namespace: ns.to_string(),
            params,
            params_hash: params_hash.clone(),
            submission: submission.clone(),
        };
        let timeout = i64::try_from(self.config.operation_timeout.as_millis()).unwrap_or(i64::MAX);
        let root = id.as_str();
        let data = json!({
            "id": root,
            "timeoutAt": now_ms().saturating_add(timeout),
            // What the Rust SDK's `rpc` writes, so its runtime runs the kind.
            "param": encode(&json!({ "func": kind, "args": input })),
            "tags": {
                TAG_OP: root,
                TAG_KIND: kind,
                TAG_NS: ns,
                "resonate:target": format!("{SCHEME}://any@{GROUP}"),
                "resonate:origin": root,
                "resonate:branch": root,
                "resonate:parent": root,
                "resonate:scope": "global",
            },
        });
        let record = self.call("promise.create", data.clone()).await?;
        let record = record_of(&record, "promise")?;
        let Some(existing) = input_of(&record) else {
            // The id belongs to a promise that is not an operation.
            return Err(OpsError::IdempotencyKeyReused(id));
        };
        if existing.submission == submission {
            return Ok((id, true));
        }
        if existing.params_hash != params_hash {
            return Err(OpsError::IdempotencyKeyReused(id));
        }
        let failed = matches!(record.state.as_str(), "rejected" | "rejected_timedout");
        if !(failed && self.resumable.iter().any(|k| k == kind)) {
            return Ok((id, false));
        }
        // Resume (§21 §7.2): start the failed operation again under its id.
        // Only a failed root is reset, so of two racing resubmits one resumes
        // and the other finds the new root.
        if retention::reset(&self.store, id.as_str()).await? {
            tracing::info!(op = %id, "resuming a failed operation");
        }
        self.forget(&id);
        let record = self.call("promise.create", data).await?;
        let record = record_of(&record, "promise")?;
        match input_of(&record) {
            Some(existing) if existing.submission == submission => Ok((id, true)),
            Some(existing) if existing.params_hash == params_hash => Ok((id, false)),
            _ => Err(OpsError::IdempotencyKeyReused(id)),
        }
    }

    /// How many operations of `kind` in namespace `ns` are not finished.
    pub async fn pending_count(&self, ns: &str, kind: &str) -> Result<usize, OpsError> {
        let mut count = 0;
        let mut cursor: Option<String> = None;
        loop {
            let mut data = json!({
                "tags": { TAG_NS: ns, TAG_KIND: kind },
                "state": "pending",
                "limit": 500,
            });
            if let Some(cursor) = &cursor {
                data["cursor"] = json!(cursor);
            }
            let page = self.call("promise.search", data).await?;
            let records: Vec<Record> = serde_json::from_value(
                page.get("promises")
                    .cloned()
                    .unwrap_or(Value::Array(vec![])),
            )
            .map_err(|e| OpsError::Internal(format!("an unreadable search answer: {e}")))?;
            count += records.iter().filter(|r| is_operation(r)).count();
            match page.get("cursor").and_then(Value::as_str) {
                Some(next) if !records.is_empty() => cursor = Some(next.to_string()),
                _ => return Ok(count),
            }
        }
    }

    /// Whether operation `id` exists.
    pub async fn exists(&self, id: &OperationId) -> Result<bool, OpsError> {
        match self.root(id).await {
            Ok(_) => Ok(true),
            Err(OpsError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// The operation `id`.
    pub async fn get(&self, id: &OperationId) -> Result<Operation, OpsError> {
        let record = self.root(id).await?;
        self.operation(record).await
    }

    /// One page of namespace `ns`'s operations, optionally in `state`, from
    /// `cursor`; the second value is the next page's cursor. A page can hold
    /// fewer than `page_size` operations when a state filter applies.
    pub async fn list(
        &self,
        ns: &str,
        state: Option<OperationState>,
        cursor: Option<String>,
    ) -> Result<(Vec<Operation>, Option<String>), OpsError> {
        let promise_state = match state {
            None | Some(OperationState::Failed) => None,
            Some(OperationState::Queued | OperationState::Running) => Some("pending"),
            Some(OperationState::Succeeded) => Some("resolved"),
            Some(OperationState::Canceled) => Some("rejected_canceled"),
        };
        let mut data = json!({
            "tags": { TAG_NS: ns },
            "limit": self.config.page_size.max(1),
        });
        if let Some(promise_state) = promise_state {
            data["state"] = json!(promise_state);
        }
        if let Some(cursor) = cursor {
            data["cursor"] = json!(cursor);
        }
        let page = self.call("promise.search", data).await?;
        let next = page
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        let records: Vec<Record> = serde_json::from_value(
            page.get("promises")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        )
        .map_err(|e| OpsError::Internal(format!("an unreadable search answer: {e}")))?;
        let mut operations = Vec::with_capacity(records.len());
        for record in records {
            if !is_operation(&record) {
                continue;
            }
            let operation = self.operation(record).await?;
            if state.is_none_or(|state| operation.state == state) {
                operations.push(operation);
            }
        }
        Ok((operations, next))
    }

    /// Cancel `id`: its root is settled `rejected_canceled`, and its workflow
    /// stops at the next step boundary ([`check_canceled`]). Writes already
    /// made stay.
    pub async fn cancel(&self, id: &OperationId) -> Result<(), OpsError> {
        let record = self.root(id).await?;
        if record.state != "pending" {
            return Err(OpsError::Finished(id.clone()));
        }
        let value = json!({
            "__type": "error",
            "message": format!("application error: canceled: {CANCELED}"),
        });
        let settled = self
            .call(
                "promise.settle",
                json!({ "id": id.as_str(), "state": "rejected_canceled", "value": encode(&value) }),
            )
            .await?;
        self.forget(id);
        match record_of(&settled, "promise")?.state.as_str() {
            "rejected_canceled" => Ok(()),
            // It finished first.
            _ => Err(OpsError::Finished(id.clone())),
        }
    }

    /// Delete every operation that finished more than `retention` before
    /// `now_ms` (Ruling 8), with every promise of its origin; the store's
    /// cascades remove their callbacks and listeners (T0-12). Returns how
    /// many. `now_ms` is the clock, so tests can move it.
    pub async fn prune_finished(&self, now_ms: i64) -> Result<usize, OpsError> {
        let retention = i64::try_from(self.config.retention.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now_ms.saturating_sub(retention);
        let pruned = retention::prune(&self.store, cutoff).await?;
        for id in &pruned {
            self.progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(id);
        }
        if !pruned.is_empty() {
            tracing::info!(count = pruned.len(), "pruned finished durable operations");
        }
        Ok(pruned.len())
    }

    /// The root promise of operation `id`; a promise that is not an
    /// operation is not found.
    async fn root(&self, id: &OperationId) -> Result<Record, OpsError> {
        let answer = match self.call("promise.get", json!({ "id": id.as_str() })).await {
            Err(OpsError::NotFound(_)) => return Err(OpsError::NotFound(id.to_string())),
            other => other?,
        };
        let record = record_of(&answer, "promise")?;
        if is_operation(&record) {
            Ok(record)
        } else {
            Err(OpsError::NotFound(id.to_string()))
        }
    }

    /// The operation whose root is `record`.
    async fn operation(&self, record: Record) -> Result<Operation, OpsError> {
        let input = input_of(&record)
            .ok_or_else(|| OpsError::Internal(format!("{}: an unreadable param", record.id)))?;
        let task = match record.state.as_str() {
            "pending" => self.task_state(&record.id).await?,
            _ => None,
        };
        let (state, code) = map_state(&record.state, task.as_deref())?;
        let (result, error) = match state {
            OperationState::Succeeded => (decode(&record.value), None),
            OperationState::Failed | OperationState::Canceled => {
                (None, Some(error_of(&record.value, code)))
            }
            OperationState::Queued | OperationState::Running => (None, None),
        };
        let (progress, activity) = self
            .progress_of(&input.id, &input.kind, state.is_finished())
            .await?;
        let updated_at = record
            .settled_at
            .unwrap_or_else(|| record.created_at.max(activity));
        Ok(Operation {
            target: input.params.get("target").cloned().unwrap_or(Value::Null),
            id: input.id,
            kind: input.kind,
            namespace: input.namespace,
            state,
            progress,
            result,
            error,
            created_at: record.created_at,
            updated_at,
        })
    }

    /// The root task's state, `None` without one.
    async fn task_state(&self, id: &str) -> Result<Option<String>, OpsError> {
        match self.call("task.get", json!({ "id": id })).await {
            Ok(answer) => Ok(answer
                .get("task")
                .and_then(|task| task.get("state"))
                .and_then(Value::as_str)
                .map(str::to_string)),
            Err(OpsError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// The progress of `id` and the latest step activity (Unix ms), reused
    /// for `progress_ttl` while it runs. The first progress computed after it
    /// finished is kept.
    async fn progress_of(
        &self,
        id: &OperationId,
        kind: &str,
        finished: bool,
    ) -> Result<(Value, i64), OpsError> {
        let cached = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id.as_str())
            .filter(|p| p.finished || (!finished && p.at.elapsed() < self.config.progress_ttl))
            .cloned();
        if let Some(cached) = cached {
            return Ok((cached.value, cached.activity));
        }
        let (steps, done, activity) = self.steps(id).await?;
        let value = match self.kinds.get(kind).cloned().flatten() {
            Some(progress) => progress(self.client.clone(), id.clone()).await?,
            None => json!({ "steps": steps, "steps_done": done }),
        };
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id.to_string(),
                Progress {
                    at: Instant::now(),
                    value: value.clone(),
                    activity,
                    finished,
                },
            );
        Ok((value, activity))
    }

    /// The steps of `id`'s origin (every promise but the root): how many,
    /// how many settled, and the latest creation or settlement among them.
    async fn steps(&self, id: &OperationId) -> Result<(u64, u64, i64), OpsError> {
        let (mut steps, mut done, mut activity) = (0, 0, 0);
        let mut cursor: Option<String> = None;
        loop {
            let mut data = json!({ "tags": { "resonate:origin": id.as_str() }, "limit": 500 });
            if let Some(cursor) = &cursor {
                data["cursor"] = json!(cursor);
            }
            let page = self.call("promise.search", data).await?;
            let records: Vec<Record> = serde_json::from_value(
                page.get("promises")
                    .cloned()
                    .unwrap_or(Value::Array(vec![])),
            )
            .map_err(|e| OpsError::Internal(format!("an unreadable search answer: {e}")))?;
            for record in records.iter().filter(|r| r.id != id.as_str()) {
                steps += 1;
                if record.state != "pending" {
                    done += 1;
                }
                activity = activity
                    .max(record.created_at)
                    .max(record.settled_at.unwrap_or(0));
            }
            match page.get("cursor").and_then(Value::as_str) {
                Some(next) if !records.is_empty() => cursor = Some(next.to_string()),
                _ => return Ok((steps, done, activity)),
            }
        }
    }

    fn forget(&self, id: &OperationId) {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id.as_str());
    }

    /// One protocol call; the answer's `data`. A 404 is
    /// [`OpsError::NotFound`].
    async fn call(&self, kind: &str, data: Value) -> Result<Value, OpsError> {
        match self
            .client
            .process(json!({ "kind": kind, "data": data }))
            .await
        {
            Ok(mut answer) => Ok(answer
                .get_mut("data")
                .map(Value::take)
                .unwrap_or(Value::Null)),
            Err(DurableError::Protocol { status: 404, .. }) => {
                Err(OpsError::NotFound(kind.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// The message of a canceled operation.
const CANCELED: &str = "the operation was canceled";

/// The state of an operation whose root promise is in state `promise` and
/// whose task is in state `task` (`None`: no task), with the error code the
/// state implies (§21 §6.4): `pending` with no acquired task → queued; an
/// acquired or suspended task → running; `resolved` → succeeded; `rejected`
/// → failed; `rejected_canceled` → canceled; `rejected_timedout` → failed
/// with `deadline_exceeded`.
pub fn map_state(
    promise: &str,
    task: Option<&str>,
) -> Result<(OperationState, Option<&'static str>), OpsError> {
    let unknown = || {
        OpsError::Internal(format!(
            "an unknown state: promise {promise}, task {task:?}"
        ))
    };
    Ok(match promise {
        "pending" => match task {
            None | Some("pending" | "halted") => (OperationState::Queued, None),
            // A fulfilled task on a pending root: the settle is in flight.
            Some("acquired" | "suspended" | "fulfilled") => (OperationState::Running, None),
            Some(_) => return Err(unknown()),
        },
        "resolved" => (OperationState::Succeeded, None),
        "rejected" => (OperationState::Failed, None),
        "rejected_canceled" => (OperationState::Canceled, Some("canceled")),
        "rejected_timedout" => (OperationState::Failed, Some("deadline_exceeded")),
        _ => return Err(unknown()),
    })
}

/// Stop at this step boundary if operation `id` was canceled (§21 §6.4): an
/// `Err` that ends the workflow. Call it between steps. A failed check
/// (the server busy) lets the workflow go on; the next check sees the
/// cancel.
pub async fn check_canceled(ctx: &Context, id: &OperationId) -> resonate_sdk::error::Result<()> {
    let client = ctx.get_dependency::<DurableClient>();
    match client
        .process(json!({ "kind": "promise.get", "data": { "id": id.as_str() } }))
        .await
    {
        Ok(answer) if answer["data"]["promise"]["state"] == "rejected_canceled" => {
            Err(fail("canceled", CANCELED))
        }
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(op = %id, error = %e, "could not check whether the operation was canceled");
            Ok(())
        }
    }
}

/// A workflow failure with a machine-readable `code` (`snake_case`), which
/// the operation's `error.code` shows. Returning it rejects the operation for
/// good (T6-6): only for failures a retry cannot fix.
pub fn fail(code: &str, message: impl fmt::Display) -> resonate_sdk::error::Error {
    resonate_sdk::error::Error::Application {
        message: format!("{code}: {message}"),
    }
}

/// A promise record, as the protocol answers it.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Record {
    pub(crate) id: String,
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) param: Value,
    #[serde(default)]
    pub(crate) value: Value,
    #[serde(default)]
    pub(crate) tags: HashMap<String, String>,
    #[serde(rename = "createdAt", default)]
    pub(crate) created_at: i64,
    #[serde(rename = "settledAt", default)]
    pub(crate) settled_at: Option<i64>,
}

pub(crate) fn record_of(answer: &Value, field: &str) -> Result<Record, OpsError> {
    serde_json::from_value(answer.get(field).cloned().unwrap_or(Value::Null))
        .map_err(|e| OpsError::Internal(format!("an unreadable {field}: {e}")))
}

/// Whether `record` is an operation's root.
fn is_operation(record: &Record) -> bool {
    record.id.starts_with(OPERATION_PREFIX)
        && record.tags.get(TAG_OP).map(String::as_str) == Some(record.id.as_str())
}

/// The [`OpInput`] in a root's `param` (`{func, args}`).
fn input_of(record: &Record) -> Option<OpInput> {
    if !is_operation(record) {
        return None;
    }
    let param = decode(&record.param)?;
    serde_json::from_value(param.get("args")?.clone()).ok()
}

/// `value` in the SDK's wire form: `{"data": base64(JSON)}`.
pub(crate) fn encode(value: &Value) -> Value {
    json!({ "data": BASE64.encode(value.to_string()) })
}

/// The JSON in a promise value, `None` when empty or unreadable.
pub(crate) fn decode(value: &Value) -> Option<Value> {
    let data = value.get("data")?.as_str().filter(|d| !d.is_empty())?;
    let bytes = BASE64.decode(data).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// The error of a failed or canceled root: `code` when the state implies
/// one, else the workflow's (`fail`), else `failed`.
pub(crate) fn error_of(value: &Value, code: Option<&'static str>) -> OperationError {
    let message = decode(value)
        .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
        .map(|m| {
            m.strip_prefix("application error: ")
                .map(str::to_string)
                .unwrap_or(m)
        });
    let split = message.as_deref().and_then(|m| {
        let (code, rest) = m.split_once(": ")?;
        (!code.is_empty()
            && code
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
        .then(|| (code.to_string(), rest.to_string()))
    });
    match (code, split, message) {
        (Some(code), Some((_, rest)), _) => OperationError {
            code: code.into(),
            message: rest,
        },
        (Some(code), None, message) => OperationError {
            code: code.into(),
            message: message.unwrap_or_else(|| match code {
                "deadline_exceeded" => "the operation did not finish before its deadline".into(),
                _ => CANCELED.into(),
            }),
        },
        (None, Some((code, message)), _) => OperationError { code, message },
        (None, None, message) => OperationError {
            code: "failed".into(),
            message: message.unwrap_or_else(|| "the operation failed".into()),
        },
    }
}

/// SHA-256 (hex) of the canonical `{kind, namespace, params}`: object keys
/// sorted at every level, so key order does not matter.
fn params_hash(kind: &str, ns: &str, params: &Value) -> String {
    let canonical = canonical(&json!({ "kind": kind, "namespace": ns, "params": params }));
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

/// `value` with every object's keys in sorted order (the workspace's
/// `serde_json` keeps insertion order).
fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical(&map[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// The retention delete (Ruling 8, T0-12, X9). The protocol has no delete for
/// settled promises, so this is SQL on Resonate's own schema: both the SQLite
/// and the MySQL schema have the generated column `origin_id`, task state
/// lives on `promises`, and `callbacks` and `listeners` cascade. A general
/// retention setting is proposed upstream as PR 5 (resonatehq/resonate#1166);
/// this goes away when it lands.
mod retention {
    use super::{OPERATION_PREFIX, OpsError, TAG_OP};
    use crate::config::DurableStore;

    /// At most this many operations per sweep.
    const BATCH: i64 = 1000;

    /// Delete the finished operations settled before `cutoff`, with the
    /// origins they own outside their own (an import's file roots, tagged
    /// `loam:op`); their ids.
    pub(super) async fn prune(store: &DurableStore, cutoff: i64) -> Result<Vec<String>, OpsError> {
        match store {
            DurableStore::Sqlite { path } => {
                let path = path.clone();
                tokio::task::spawn_blocking(move || sqlite::prune(&path, cutoff))
                    .await
                    .map_err(|e| OpsError::Internal(format!("the retention sweep: {e}")))?
            }
            #[cfg(feature = "mysql")]
            DurableStore::Mysql { url, tls } => mysql::prune(url, *tls, cutoff).await,
            #[cfg(not(feature = "mysql"))]
            DurableStore::Mysql { .. } => Err(OpsError::Internal(
                "this build has no MySQL durable store".into(),
            )),
        }
    }

    /// Reset the failed operation `id` for a resume (§21 §7.2): delete its
    /// own origin and every failed origin it owns (tagged `loam:op`), keeping
    /// the resolved and pending ones. Only while the root is still failed
    /// (`rejected` or `rejected_timedout`), in one transaction, so a racing
    /// resubmit that already resumed it is left alone. Whether it reset.
    pub(super) async fn reset(store: &DurableStore, id: &str) -> Result<bool, OpsError> {
        match store {
            DurableStore::Sqlite { path } => {
                let (path, id) = (path.clone(), id.to_string());
                tokio::task::spawn_blocking(move || sqlite::reset(&path, &id))
                    .await
                    .map_err(|e| OpsError::Internal(format!("resuming: {e}")))?
            }
            #[cfg(feature = "mysql")]
            DurableStore::Mysql { url, tls } => mysql::reset(url, *tls, id).await,
            #[cfg(not(feature = "mysql"))]
            DurableStore::Mysql { .. } => Err(OpsError::Internal(
                "this build has no MySQL durable store".into(),
            )),
        }
    }

    /// The states of an owned origin that a resume deletes.
    const FAILED: &str = "'rejected', 'rejected_canceled', 'rejected_timedout'";

    mod sqlite {
        use rusqlite::{Connection, TransactionBehavior};

        use super::{BATCH, FAILED, OPERATION_PREFIX, OpsError, TAG_OP};

        fn open(path: &std::path::Path) -> Result<Connection, rusqlite::Error> {
            let conn = Connection::open(path)?;
            // The cascades need foreign keys on this connection too; Resonate
            // holds the store, so wait for its writes.
            conn.execute_batch("PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON;")?;
            Ok(conn)
        }

        /// The roots of the other origins operation `id` owns; only the
        /// failed ones with `failed_only`.
        fn owned(
            conn: &Connection,
            id: &str,
            failed_only: bool,
        ) -> Result<Vec<String>, rusqlite::Error> {
            let filter = if failed_only {
                format!(" AND state IN ({FAILED})")
            } else {
                String::new()
            };
            let mut select = conn.prepare(&format!(
                "SELECT id FROM promises WHERE id = origin_id AND id <> ?1 \
                 AND json_extract(tags, '$.\"{TAG_OP}\"') = ?1{filter}"
            ))?;
            select
                .query_map([id], |row| row.get(0))?
                .collect::<Result<_, _>>()
        }

        pub(super) fn prune(path: &std::path::Path, cutoff: i64) -> Result<Vec<String>, OpsError> {
            let failed =
                |e: rusqlite::Error| OpsError::Internal(format!("the retention sweep: {e}"));
            let mut conn = open(path).map_err(failed)?;
            let ids: Vec<String> = {
                let mut select = conn
                    .prepare(&format!(
                        "SELECT id FROM promises WHERE id = origin_id AND id LIKE '{OPERATION_PREFIX}%' \
                         AND state <> 'pending' AND settled_at < ?1 \
                         AND json_extract(tags, '$.\"{TAG_OP}\"') = id LIMIT {BATCH}"
                    ))
                    .map_err(failed)?;
                select
                    .query_map([cutoff], |row| row.get(0))
                    .map_err(failed)?
                    .collect::<Result<_, _>>()
                    .map_err(failed)?
            };
            for id in &ids {
                let tx = conn.transaction().map_err(failed)?;
                for origin in owned(&tx, id, false).map_err(failed)? {
                    tx.execute("DELETE FROM promises WHERE origin_id = ?1", [&origin])
                        .map_err(failed)?;
                }
                tx.execute("DELETE FROM promises WHERE origin_id = ?1", [id])
                    .map_err(failed)?;
                tx.commit().map_err(failed)?;
            }
            Ok(ids)
        }

        pub(super) fn reset(path: &std::path::Path, id: &str) -> Result<bool, OpsError> {
            let failed = |e: rusqlite::Error| OpsError::Internal(format!("resuming {id}: {e}"));
            let mut conn = open(path).map_err(failed)?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failed)?;
            let state: Option<String> =
                match tx.query_row("SELECT state FROM promises WHERE id = ?1", [id], |row| {
                    row.get(0)
                }) {
                    Ok(state) => Some(state),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(e) => return Err(failed(e)),
                };
            if !matches!(state.as_deref(), Some("rejected" | "rejected_timedout")) {
                return Ok(false);
            }
            for origin in owned(&tx, id, true).map_err(failed)? {
                tx.execute("DELETE FROM promises WHERE origin_id = ?1", [&origin])
                    .map_err(failed)?;
            }
            tx.execute("DELETE FROM promises WHERE origin_id = ?1", [id])
                .map_err(failed)?;
            tx.commit().map_err(failed)?;
            Ok(true)
        }
    }

    #[cfg(feature = "mysql")]
    mod mysql {
        use sqlx::{Connection, MySqlConnection};

        use super::{BATCH, FAILED, OPERATION_PREFIX, OpsError, TAG_OP};

        async fn connect(
            url: &str,
            tls: crate::config::MysqlTls,
            what: &str,
        ) -> Result<MySqlConnection, OpsError> {
            let target = crate::config::mysql_url(url, tls)
                .map_err(|e| OpsError::Internal(e.to_string()))?;
            MySqlConnection::connect(&target)
                .await
                .map_err(|e| unavailable(url, what, &e))
        }

        fn unavailable(url: &str, what: &str, e: &sqlx::Error) -> OpsError {
            OpsError::Unavailable(crate::config::scrub(&format!("{what}: {e}"), url))
        }

        /// See the SQLite `owned`.
        async fn owned(
            conn: &mut MySqlConnection,
            id: &str,
            failed_only: bool,
        ) -> Result<Vec<String>, sqlx::Error> {
            let filter = if failed_only {
                format!(" AND state IN ({FAILED})")
            } else {
                String::new()
            };
            sqlx::query_scalar(&format!(
                "SELECT id FROM promises WHERE id = origin_id AND id <> ? \
                 AND tags->>'$.\"{TAG_OP}\"' = ?{filter}"
            ))
            .bind(id)
            .bind(id)
            .fetch_all(conn)
            .await
        }

        pub(super) async fn prune(
            url: &str,
            tls: crate::config::MysqlTls,
            cutoff: i64,
        ) -> Result<Vec<String>, OpsError> {
            const WHAT: &str = "the retention sweep";
            let mut conn = connect(url, tls, WHAT).await?;
            let failed = |e: sqlx::Error| unavailable(url, WHAT, &e);
            let ids: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT id FROM promises WHERE id = origin_id AND id LIKE '{OPERATION_PREFIX}%' \
                 AND state <> 'pending' AND settled_at < ? \
                 AND tags->>'$.\"{TAG_OP}\"' = id LIMIT {BATCH}"
            ))
            .bind(cutoff)
            .fetch_all(&mut conn)
            .await
            .map_err(failed)?;
            for id in &ids {
                for origin in owned(&mut conn, id, false).await.map_err(failed)? {
                    sqlx::query("DELETE FROM promises WHERE origin_id = ?")
                        .bind(&origin)
                        .execute(&mut conn)
                        .await
                        .map_err(failed)?;
                }
                sqlx::query("DELETE FROM promises WHERE origin_id = ?")
                    .bind(id)
                    .execute(&mut conn)
                    .await
                    .map_err(failed)?;
            }
            let _ = conn.close().await;
            Ok(ids)
        }

        pub(super) async fn reset(
            url: &str,
            tls: crate::config::MysqlTls,
            id: &str,
        ) -> Result<bool, OpsError> {
            const WHAT: &str = "resuming an operation";
            let mut conn = connect(url, tls, WHAT).await?;
            let failed = |e: sqlx::Error| unavailable(url, WHAT, &e);
            // TiDB locks `FOR UPDATE` rows only in a pessimistic transaction;
            // MySQL has no such variable and always locks, so a failure here
            // is ignored.
            let _ = sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
                .execute(&mut conn)
                .await;
            let mut tx = conn.begin().await.map_err(failed)?;
            let state: Option<String> =
                sqlx::query_scalar("SELECT state FROM promises WHERE id = ? FOR UPDATE")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(failed)?;
            if !matches!(state.as_deref(), Some("rejected" | "rejected_timedout")) {
                return Ok(false);
            }
            for origin in owned(&mut tx, id, true).await.map_err(failed)? {
                sqlx::query("DELETE FROM promises WHERE origin_id = ?")
                    .bind(&origin)
                    .execute(&mut *tx)
                    .await
                    .map_err(failed)?;
            }
            sqlx::query("DELETE FROM promises WHERE origin_id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(failed)?;
            tx.commit().await.map_err(failed)?;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_hash_ignores_key_order() {
        let a = params_hash(
            "k",
            "ns",
            &json!({ "a": 1, "b": { "x": [1, { "p": 1, "q": 2 }], "y": 2 } }),
        );
        let b = params_hash(
            "k",
            "ns",
            &json!({ "b": { "y": 2, "x": [1, { "q": 2, "p": 1 }] }, "a": 1 }),
        );
        assert_eq!(a, b);
        assert_ne!(a, params_hash("k", "other", &json!({ "a": 1 })));
        assert_ne!(
            a,
            params_hash(
                "j",
                "ns",
                &json!({ "a": 1, "b": { "x": [1, { "p": 1, "q": 2 }], "y": 2 } })
            )
        );
    }

    #[test]
    fn errors_carry_the_workflow_code() {
        let wire = |message: &str| encode(&json!({ "__type": "error", "message": message }));
        assert_eq!(
            error_of(&wire("application error: bad_input: no good"), None),
            OperationError {
                code: "bad_input".into(),
                message: "no good".into()
            }
        );
        assert_eq!(
            error_of(&wire("server error (code=500): boom"), None).code,
            "failed"
        );
        assert_eq!(
            error_of(&json!({}), Some("deadline_exceeded")).code,
            "deadline_exceeded"
        );
        assert_eq!(
            error_of(
                &wire("application error: canceled: the operation was canceled"),
                Some("canceled")
            ),
            OperationError {
                code: "canceled".into(),
                message: CANCELED.into()
            }
        );
    }
}
