//! Bulk import from object storage (D1 Task 8, D145, design §21 §7.2): a
//! durable operation of kind `collection.import`, with one branch per file
//! and one step per slice.
//!
//! - **Plan** (`ctx.run`, memoized): list the source with `operon-store`
//!   (credentials from the process environment, as for `--bucket`), filter
//!   by `pattern`, sort by key, and fix `(key, size, etag)`.
//! - **File branches**: one **root** promise per file,
//!   `opf-<hex26(SHA-256(op ‖ 0 ‖ key ‖ 0 ‖ etag))>`, tagged `loam:op`,
//!   `loam:kind = file` and `loam:file = <index>`, running
//!   `collection.import.file`, at most `max_parallel_files` at a time. Each
//!   file is its own origin, so a resubmit of a failed operation reuses the
//!   files it finished (§21 §7.2, T8-2), and a scheduled import (Task 9)
//!   starts the same function. The operation's workflow waits for each file
//!   root in a local step.
//! - **Slices**: a Parquet row group, or 64 MiB of NDJSON split at a newline;
//!   each is `ctx.run` → `{rows, bytes, token}`. Every read is conditional on
//!   the planned etag (`If-Match`), so a file that changed fails its branch
//!   with `file_changed`.
//! - **Mapping and writing** ([`slice`]): the renames of `mapping`, the
//!   `_id` rules (O3, O5, T1-8, X4), then the [`ImportSink`] (in `operon`,
//!   M1.2's `CollectionBatchMapper` and `CollectionService::write`), in
//!   chunks of `put_chunk_rows`. Backpressure, `Unavailable` and timeouts are
//!   retried inside the step for up to 5 minutes (X7); a function that
//!   returns `Err` rejects its promise for good (T6-6).
//! - **Fan-in**: `{files_total, files_done, files_failed, rows_written,
//!   bytes_read, token}`, with every slice's consistency token merged (D76).
//! - **Scheduled import** ([`schedule`], D1 Task 9): a Resonate schedule
//!   whose runs start the same file function as roots of their own,
//!   `impf-…`, one per `(schedule, key, etag)`.

mod ndjson;
mod parquet;
mod plan;
pub mod schedule;
mod slice;

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use operon_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::embed::DurableClient;
use crate::ids::OperationId;
use crate::inproc::{GROUP, SCHEME};
use crate::ops::{
    OpInput, OperationError, OperationKinds, Operations, OpsError, ProgressFn, TAG_KIND, TAG_OP,
    check_canceled, decode, encode, error_of, fail, record_of,
};

pub use plan::{PlannedFile, glob_matches};
pub use slice::{IdSeed, MappedSlice, derived_id, map_slice, scheduled_id};

/// The operation kind (the workflow's registered name).
pub const IMPORT_KIND: &str = "collection.import";
/// The file branch's function.
pub const FILE_FUNCTION: &str = "collection.import.file";
/// The `loam:kind` of a file root.
pub const FILE_KIND: &str = "file";
/// A file root's index in its operation's plan.
pub const TAG_FILE: &str = "loam:file";
/// Every file root id starts with this.
pub const FILE_PREFIX: &str = "opf-";

/// The longest a step waits between two write or read attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(10);
/// A step's timeout: the operation's (a child's default is 24 h).
pub(crate) const STEP_TIMEOUT: Duration = Duration::from_secs(7 * 24 * 3600);

/// The file format of an import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Parquet,
    Ndjson,
}

/// What a failed file does to the operation (§21 §7.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// The operation fails with the file's error. The default.
    #[default]
    Fail,
    /// The file is counted in `files_failed` and the operation goes on.
    SkipFile,
}

/// How a key is read (M1.2's `IdType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdType {
    Str,
    U64,
    Uuid,
}

impl IdType {
    /// As `operon-id-type` spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Str => "str",
            Self::U64 => "u64",
            Self::Uuid => "uuid",
        }
    }
}

/// The column mapping (O3, X5): `columns` renames source columns to
/// collection columns; `id_column` names the source column that is the
/// `_id`, typed by `id_type`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    #[serde(default)]
    pub columns: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_type: Option<IdType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_column: Option<String>,
}

/// `POST /v1/namespaces/{ns}/collections/{c}/import` (§21 §7.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportRequest {
    /// The prefix, such as `s3://bucket/prefix/`.
    pub source: String,
    pub format: Format,
    /// A glob on the key after the prefix: `*` matches any run of
    /// characters (`/` included), `?` one character.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapping: Option<Mapping>,
    #[serde(default)]
    pub on_error: OnError,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_files: Option<usize>,
}

/// The largest `max_parallel_files` a request may ask for.
pub const MAX_PARALLEL_FILES: usize = 64;

impl ImportRequest {
    /// The request as a body, refused with the reason (400).
    pub fn parse(body: Value) -> Result<Self, ImportError> {
        serde_json::from_value(body)
            .map_err(|e| ImportError::Invalid(format!("an import request: {e}")))
    }

    /// Refuse what is wrong before submit (400): a source outside
    /// `config.schemes`, a bad pattern or parallelism, and a mapping whose
    /// renames cannot be unique (O3, T1-8).
    pub fn validate(&self, config: &ImportConfig) -> Result<(), ImportError> {
        let invalid = |m: String| Err(ImportError::Invalid(m));
        let scheme = match url::Url::parse(&self.source) {
            Ok(url) => url.scheme().to_string(),
            Err(e) => return invalid(format!("source {:?} is not a URL: {e}", self.source)),
        };
        if !config.schemes.contains(&scheme) {
            return invalid(format!(
                "source scheme {scheme:?} is not allowed here; use one of {}",
                config.schemes.join(", ")
            ));
        }
        if self.pattern.as_deref() == Some("") {
            return invalid("pattern is empty".into());
        }
        if let Some(n) = self.max_parallel_files
            && !(1..=MAX_PARALLEL_FILES).contains(&n)
        {
            return invalid(format!(
                "max_parallel_files must be 1..={MAX_PARALLEL_FILES}, got {n}"
            ));
        }
        let Some(mapping) = &self.mapping else {
            return Ok(());
        };
        let mut targets: BTreeMap<&str, &str> = BTreeMap::new();
        for (source, target) in &mapping.columns {
            if source.is_empty() || target.is_empty() {
                return invalid("mapping.columns: a column name is empty".into());
            }
            if let Some(other) = targets.insert(target, source) {
                return invalid(format!(
                    "mapping.columns: {other:?} and {source:?} both map to {target:?}"
                ));
            }
        }
        if let Some(id_column) = &mapping.id_column {
            if id_column.is_empty() {
                return invalid("mapping.id_column is empty".into());
            }
            if let Some(source) = targets.get(slice::ID_COLUMN) {
                return invalid(format!(
                    "mapping: id_column {id_column:?} and columns entry {source:?} both map to _id"
                ));
            }
            if mapping.columns.contains_key(id_column) {
                return invalid(format!(
                    "mapping: id_column {id_column:?} is also renamed by mapping.columns"
                ));
            }
        }
        Ok(())
    }
}

/// What an import route refuses before submit, or the submit's error.
#[derive(Debug)]
pub enum ImportError {
    /// 400 `invalid_argument`.
    Invalid(String),
    /// The collection (or namespace) does not exist: 404.
    NotFound(String),
    /// The namespace runs `max_concurrent_operations` imports: 429
    /// `too_many_operations` (Ruling 9).
    TooManyOperations {
        limit: usize,
    },
    /// The source cannot be listed now: 503.
    Unavailable(String),
    /// The name is taken (D1 Task 9: an import schedule): 409
    /// `already_exists`.
    Conflict(String),
    Ops(OpsError),
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(m) | Self::NotFound(m) | Self::Unavailable(m) | Self::Conflict(m) => {
                f.write_str(m)
            }
            Self::TooManyOperations { limit } => write!(
                f,
                "this namespace already runs {limit} imports; wait for one to finish"
            ),
            Self::Ops(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<OpsError> for ImportError {
    fn from(e: OpsError) -> Self {
        Self::Ops(e)
    }
}

/// What a sink wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkWrite {
    pub rows: u64,
    /// The write's consistency token (D76), as text.
    pub token: String,
}

/// Why a sink did not write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    /// Transient: backpressure (429, waiting at least `after_ms`),
    /// unavailable, a timeout. The step retries it (X7).
    Retry { message: String, after_ms: u64 },
    /// The collection is gone.
    NotFound(String),
    /// Anything else, such as a row error (`row_error`): the file fails and
    /// `on_error` decides (T0-1).
    Failed { code: String, message: String },
}

/// Where an import writes: a collection's write path (`operon` implements it
/// over `CollectionBatchMapper` and `CollectionService::write`).
#[async_trait]
pub trait ImportSink: Send + Sync + 'static {
    /// Whether collection `collection` of namespace `ns` exists (else
    /// [`SinkError::NotFound`]).
    async fn check(&self, ns: &str, collection: &str) -> Result<(), SinkError>;

    /// Write `batch` (at most `put_chunk_rows` rows, with an `_id` column) as
    /// upserts. `id_type` types a text `_id` when its field carries no
    /// `operon-id-type`.
    async fn write(
        &self,
        ns: &str,
        collection: &str,
        batch: RecordBatch,
        id_type: Option<IdType>,
    ) -> Result<SinkWrite, SinkError>;

    /// One token covering every token in `tokens`.
    fn merge_tokens(&self, tokens: &[String]) -> String;
}

/// Opens a source URL as a store rooted at its prefix.
pub trait SourceOpener: Send + Sync + 'static {
    fn open(&self, source: &str) -> Result<Store, String>;
}

/// [`Store::from_url`] with no options: credentials come from the process
/// environment, as for `--bucket` (T0-5; per-namespace credentials are M2).
#[derive(Debug, Clone, Copy, Default)]
pub struct UrlSources;

impl SourceOpener for UrlSources {
    fn open(&self, source: &str) -> Result<Store, String> {
        Store::from_url(source, Vec::<(String, String)>::new()).map_err(|e| e.to_string())
    }
}

/// Import settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportConfig {
    /// The source schemes accepted: `s3`, `gs`, `az`, and `file` on `dev`.
    pub schemes: Vec<String>,
    /// Rows per sink write (`config.flight.put_chunk_rows`, T0-3).
    pub chunk_rows: usize,
    /// The NDJSON slice size. Default 64 MiB (Ruling 6).
    pub ndjson_slice_bytes: u64,
    /// The largest uncompressed Parquet row group. Default 512 MiB.
    pub max_row_group_bytes: u64,
    /// Files per operation. Default 100,000.
    pub max_files: usize,
    /// Files in flight when the request does not say. Default 4 (Ruling 9).
    pub max_parallel_files: usize,
    /// Unfinished imports per namespace. Default 2 (Ruling 9).
    pub max_concurrent_operations: usize,
    /// How long a step retries transient failures. Default 5 minutes (X7).
    pub retry_for: Duration,
    /// The first backoff of a retry. Default 100 ms.
    pub backoff: Duration,
    /// How often the workflow looks at a running file root. Default 200 ms.
    pub poll: Duration,
}

impl ImportConfig {
    /// The defaults; `file://` sources only when `dev`.
    pub fn new(dev: bool) -> Self {
        let mut schemes = vec!["s3".to_string(), "gs".to_string(), "az".to_string()];
        if dev {
            schemes.push("file".to_string());
        }
        Self {
            schemes,
            chunk_rows: 10_000,
            ndjson_slice_bytes: 64 << 20,
            max_row_group_bytes: 512 << 20,
            max_files: 100_000,
            max_parallel_files: 4,
            max_concurrent_operations: 2,
            retry_for: Duration::from_secs(300),
            backoff: Duration::from_millis(100),
            poll: Duration::from_millis(200),
        }
    }
}

/// A step of an import, as a [`StepHook`] sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Plan,
    Layout { file: usize },
    Slice { file: usize, slice: u64 },
}

/// A test hook called when a step has done its work and before it returns:
/// `true` stops it there (it never settles), as a crash between the effect
/// and the settle would.
pub type StepHook = Arc<dyn Fn(&Step) -> bool + Send + Sync>;

/// What the import's durable functions run with: an SDK dependency.
#[derive(Clone)]
pub struct ImportEnv {
    pub sink: Arc<dyn ImportSink>,
    pub sources: Arc<dyn SourceOpener>,
    pub config: ImportConfig,
    #[doc(hidden)]
    pub hook: Option<StepHook>,
}

impl fmt::Debug for ImportEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportEnv")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ImportEnv {
    /// An environment over `sink`, with sources opened by URL.
    pub fn new(sink: Arc<dyn ImportSink>, config: ImportConfig) -> Self {
        Self {
            sink,
            sources: Arc::new(UrlSources),
            config,
            hook: None,
        }
    }

    /// Whether the hook stops `step`; the caller then never returns.
    async fn after(&self, step: Step) {
        if self.hook.as_ref().is_some_and(|hook| hook(&step)) {
            std::future::pending::<()>().await;
        }
    }
}

/// `kinds` with `collection.import`, its functions, its progress, and `env`
/// as their dependency.
pub fn kinds(kinds: OperationKinds, env: ImportEnv) -> OperationKinds {
    kinds
        .kind(import)
        .function(import_plan)
        .function(import_run_file)
        .function(import_file)
        .function(import_layout)
        .function(import_slice)
        .function(schedule::import_run)
        .progress(IMPORT_KIND, progress_fn())
        .resumable(IMPORT_KIND)
        .dependency(env)
}

/// The parameters of an import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportParams {
    /// `{"collection": …}` (Task 7's `target`).
    pub target: Value,
    pub collection: String,
    pub request: ImportRequest,
}

/// Submit an import of `body` into collection `collection` of `ns`: the
/// checks before submit (400, 404, 429), then [`Operations::submit`].
pub async fn submit(
    ops: &Operations,
    env: &ImportEnv,
    ns: &str,
    collection: &str,
    body: Value,
    idempotency_key: Option<&str>,
) -> Result<(OperationId, bool), ImportError> {
    let request = ImportRequest::parse(body)?;
    request.validate(&env.config)?;
    match env.sink.check(ns, collection).await {
        Ok(()) => {}
        Err(SinkError::NotFound(m)) => return Err(ImportError::NotFound(m)),
        Err(SinkError::Retry { message, .. }) => return Err(ImportError::Unavailable(message)),
        Err(SinkError::Failed { message, .. }) => return Err(ImportError::Invalid(message)),
    }
    let files = plan::list(env, &request).await?;
    if files.len() > env.config.max_files {
        return Err(ImportError::Invalid(format!(
            "the source holds {} files; an import takes at most {}. Import several prefixes \
             (or narrow the pattern) instead",
            files.len(),
            env.config.max_files
        )));
    }
    // An idempotent repeat is not a new operation.
    let repeat = match idempotency_key {
        Some(key) if !key.is_empty() => ops.exists(&OperationId::for_key(ns, key)).await?,
        _ => false,
    };
    if !repeat {
        let limit = env.config.max_concurrent_operations;
        if ops.pending_count(ns, IMPORT_KIND).await? >= limit {
            return Err(ImportError::TooManyOperations { limit });
        }
    }
    let params = ImportParams {
        target: json!({ "collection": collection }),
        collection: collection.to_string(),
        request,
    };
    let params = serde_json::to_value(params).map_err(|e| ImportError::Invalid(e.to_string()))?;
    Ok(ops.submit(ns, IMPORT_KIND, params, idempotency_key).await?)
}

/// The plan step's value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub files: Vec<PlannedFile>,
    pub bytes_total: u64,
}

/// The argument of every file-level function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileArgs {
    /// The operation the file belongs to: its cancel is checked between
    /// slices, and its id seeds the file root's id, its tags and the
    /// generated row ids. `None` for a file a schedule started (D1 Task 9),
    /// which no cancel stops and whose ids come from `schedule`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<OperationId>,
    /// The schedule of a scheduled file (`isched-…`, D1 Task 9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    pub namespace: String,
    pub collection: String,
    pub source: String,
    pub format: Format,
    pub mapping: Option<Mapping>,
    pub index: usize,
    pub file: PlannedFile,
}

impl FileArgs {
    /// Whose rows the generated ids belong to: the operation's file, or the
    /// schedule's key.
    pub fn seed(&self) -> IdSeed<'_> {
        match (&self.op, &self.schedule) {
            (Some(op), _) => IdSeed::File {
                op,
                file: self.index,
            },
            (None, schedule) => IdSeed::Scheduled {
                schedule: schedule.as_deref().unwrap_or(""),
                key: &self.file.key,
            },
        }
    }
}

/// A slice step's value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceValue {
    pub rows: u64,
    pub bytes: u64,
    pub token: String,
}

/// A file root's value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileResult {
    pub key: String,
    pub slices: u64,
    pub rows: u64,
    pub bytes: u64,
    pub token: String,
}

/// How a file branch ended, as the operation's workflow sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileOutcome {
    pub index: usize,
    pub key: String,
    pub result: Option<FileResult>,
    pub error: Option<OperationError>,
}

/// A file layout step's value: its slices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Layout {
    pub slices: u64,
}

/// The id of the file root of `file` in operation `op`:
/// `opf-<hex26(SHA-256(op ‖ 0 ‖ key ‖ 0 ‖ etag))>`, so a changed file (a new
/// etag) is a new branch.
pub fn file_id(op: &OperationId, file: &PlannedFile) -> String {
    let mut hash = Sha256::new();
    hash.update(op.as_str().as_bytes());
    hash.update([0u8]);
    hash.update(file.key.as_bytes());
    hash.update([0u8]);
    hash.update(file.etag.as_deref().unwrap_or("").as_bytes());
    let hex = hex::encode(hash.finalize());
    format!("{FILE_PREFIX}{}", &hex[..26])
}

/// A `fail` of `code` with `message`, as an SDK error.
pub(crate) fn failed(code: &str, message: impl fmt::Display) -> resonate_sdk::error::Error {
    fail(code, message)
}

// ─── the durable functions ──────────────────────────────────────────────────

/// The operation's workflow: plan, fan out over the files, fan in.
#[resonate_sdk::function(name = "collection.import")]
async fn import(ctx: &Context, input: OpInput) -> Result<Value> {
    let env = ctx.get_dependency::<ImportEnv>();
    let params: ImportParams = serde_json::from_value(input.params.clone())
        .map_err(|e| failed("invalid_argument", format!("the import parameters: {e}")))?;
    let plan: Plan = ctx
        .run(import_plan, params.request.clone())
        .timeout(STEP_TIMEOUT)
        .await?;
    check_canceled(ctx, &input.id).await?;
    let parallel = params
        .request
        .max_parallel_files
        .unwrap_or(env.config.max_parallel_files)
        .clamp(1, MAX_PARALLEL_FILES);
    let mut outcomes: Vec<FileOutcome> = Vec::with_capacity(plan.files.len());
    let mut in_flight = VecDeque::new();
    for (index, file) in plan.files.iter().enumerate() {
        if in_flight.len() >= parallel {
            let handle: resonate_sdk::prelude::DurableFuture<FileOutcome> =
                in_flight.pop_front().expect("in flight");
            let outcome = handle.await?;
            give_up_on(&params.request, &outcome)?;
            outcomes.push(outcome);
        }
        let args = FileArgs {
            op: Some(input.id.clone()),
            schedule: None,
            namespace: input.namespace.clone(),
            collection: params.collection.clone(),
            source: params.request.source.clone(),
            format: params.request.format,
            mapping: params.request.mapping.clone(),
            index,
            file: file.clone(),
        };
        in_flight.push_back(
            ctx.run(import_run_file, args)
                .timeout(STEP_TIMEOUT)
                .spawn()?,
        );
    }
    while let Some(handle) = in_flight.pop_front() {
        let outcome = handle.await?;
        give_up_on(&params.request, &outcome)?;
        outcomes.push(outcome);
    }
    let tokens: Vec<String> = outcomes
        .iter()
        .filter_map(|o| o.result.as_ref())
        .map(|r| r.token.clone())
        .filter(|t| !t.is_empty())
        .collect();
    let failures: Vec<Value> = outcomes
        .iter()
        .filter_map(|o| {
            o.error
                .as_ref()
                .map(|e| json!({ "key": o.key, "code": e.code, "message": e.message }))
        })
        .collect();
    Ok(json!({
        "files_total": plan.files.len(),
        "files_done": outcomes.iter().filter(|o| o.result.is_some()).count(),
        "files_failed": failures.len(),
        "rows_written": outcomes.iter().filter_map(|o| o.result.as_ref()).map(|r| r.rows).sum::<u64>(),
        "bytes_read": outcomes.iter().filter_map(|o| o.result.as_ref()).map(|r| r.bytes).sum::<u64>(),
        "token": env.sink.merge_tokens(&tokens),
        "failures": failures,
    }))
}

/// With `on_error: fail`, a failed file fails the operation with its code.
fn give_up_on(
    request: &ImportRequest,
    outcome: &FileOutcome,
) -> std::result::Result<(), resonate_sdk::error::Error> {
    match (&outcome.error, request.on_error) {
        (Some(error), OnError::Fail) => Err(failed(
            &error.code,
            format!("file {}: {}", outcome.key, error.message),
        )),
        _ => Ok(()),
    }
}

/// The plan step: the source's files, sorted by key.
#[resonate_sdk::function]
async fn import_plan(info: &Info, request: ImportRequest) -> Result<Plan> {
    let env = info.get_dependency::<ImportEnv>();
    let files = match plan::list(&env, &request).await {
        Ok(files) => files,
        Err(e) => return Err(failed("source_unavailable", e)),
    };
    if files.len() > env.config.max_files {
        return Err(failed(
            "too_many_files",
            format!(
                "the source holds {} files; an import takes at most {}",
                files.len(),
                env.config.max_files
            ),
        ));
    }
    let bytes_total = files.iter().map(|f| f.size).sum();
    env.after(Step::Plan).await;
    Ok(Plan { files, bytes_total })
}

/// One file, as a step of the operation's workflow: create its root (a
/// repeat finds it) and wait for it to settle. It never fails for the file's
/// sake: the outcome carries the file's error.
#[resonate_sdk::function]
async fn import_run_file(info: &Info, args: FileArgs) -> Result<FileOutcome> {
    let env = info.get_dependency::<ImportEnv>();
    let client = info.get_dependency::<DurableClient>();
    let op = args
        .op
        .clone()
        .ok_or_else(|| failed("internal", "an operation's file without its operation"))?;
    let id = file_id(&op, &args.file);
    let create = json!({
        "kind": "promise.create",
        "data": {
            "id": id,
            "timeoutAt": info.timeout_at(),
            "param": encode(&json!({ "func": FILE_FUNCTION, "args": args })),
            "tags": {
                TAG_OP: op.as_str(),
                TAG_KIND: FILE_KIND,
                TAG_FILE: args.index.to_string(),
                "resonate:target": format!("{SCHEME}://any@{GROUP}"),
                "resonate:origin": id,
                "resonate:branch": id,
                "resonate:parent": id,
                "resonate:scope": "global",
            },
        },
    });
    let mut answer = retry_protocol(&env, &client, create).await?;
    loop {
        let record = record_of(&answer["data"], "promise")
            .map_err(|e| failed("internal", format!("file {id}: {e}")))?;
        let outcome = |result, error| FileOutcome {
            index: args.index,
            key: args.file.key.clone(),
            result,
            error,
        };
        match record.state.as_str() {
            "pending" => {}
            "resolved" => {
                let result = decode(&record.value)
                    .and_then(|v| serde_json::from_value::<FileResult>(v).ok())
                    .ok_or_else(|| failed("internal", format!("file {id}: an unreadable value")))?;
                return Ok(outcome(Some(result), None));
            }
            _ => {
                let code = (record.state == "rejected_timedout").then_some("deadline_exceeded");
                return Ok(outcome(None, Some(error_of(&record.value, code))));
            }
        }
        // The root may be canceled while this file runs.
        let root = client
            .process(json!({ "kind": "promise.get", "data": { "id": op.as_str() } }))
            .await;
        if let Ok(root) = root
            && root["data"]["promise"]["state"] == "rejected_canceled"
        {
            return Err(failed("canceled", "the operation was canceled"));
        }
        tokio::time::sleep(env.config.poll).await;
        answer = retry_protocol(
            &env,
            &client,
            json!({ "kind": "promise.get", "data": { "id": id } }),
        )
        .await?;
    }
}

/// One in-process protocol call, retried while the server is unavailable
/// for up to `retry_for`.
pub(crate) async fn retry_protocol(
    env: &ImportEnv,
    client: &DurableClient,
    request: Value,
) -> std::result::Result<Value, resonate_sdk::error::Error> {
    let deadline = tokio::time::Instant::now() + env.config.retry_for;
    let mut backoff = env.config.backoff;
    loop {
        match client.process(request.clone()).await {
            Ok(answer) => return Ok(answer),
            Err(crate::DurableError::Unavailable(m)) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(error = %m, "the durable server is busy; retrying");
            }
            Err(e) => return Err(failed("durable_unavailable", e)),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// A file branch: the root of its own origin. Its layout, then each slice,
/// with a cancel check between slices (T7-7) when it belongs to an
/// operation. A schedule's run starts the same function (D1 Task 9).
#[resonate_sdk::function(name = "collection.import.file")]
async fn import_file(ctx: &Context, args: FileArgs) -> Result<FileResult> {
    let env = ctx.get_dependency::<ImportEnv>();
    if let Some(op) = &args.op {
        check_canceled(ctx, op).await?;
    }
    let layout: Layout = ctx
        .run(import_layout, args.clone())
        .timeout(STEP_TIMEOUT)
        .await?;
    let mut result = FileResult {
        key: args.file.key.clone(),
        slices: layout.slices,
        ..FileResult::default()
    };
    let mut tokens = Vec::new();
    for n in 0..layout.slices {
        if let Some(op) = &args.op {
            check_canceled(ctx, op).await?;
        }
        let value: SliceValue = ctx
            .run(
                import_slice,
                slice::SliceArgs {
                    file: args.clone(),
                    slice: n,
                },
            )
            .timeout(STEP_TIMEOUT)
            .await?;
        result.rows += value.rows;
        result.bytes += value.bytes;
        if !value.token.is_empty() {
            tokens.push(value.token);
        }
    }
    result.token = env.sink.merge_tokens(&tokens);
    Ok(result)
}

/// A file's slices: its row groups, or its 64 MiB NDJSON ranges.
#[resonate_sdk::function]
async fn import_layout(info: &Info, args: FileArgs) -> Result<Layout> {
    let env = info.get_dependency::<ImportEnv>();
    let store = env
        .sources
        .open(&args.source)
        .map_err(|e| failed("source_unavailable", e))?;
    let slices = match args.format {
        Format::Parquet => parquet::slices(&env, &store, &args.file)
            .await
            .map_err(|(code, message)| failed(&code, message))?,
        Format::Ndjson => ndjson::slices(&env, &args.file),
    };
    env.after(Step::Layout { file: args.index }).await;
    Ok(Layout { slices })
}

/// One slice: read, map, write (with the transient failures retried).
#[resonate_sdk::function]
async fn import_slice(info: &Info, args: slice::SliceArgs) -> Result<SliceValue> {
    let env = info.get_dependency::<ImportEnv>();
    let value = slice::run(&env, &args)
        .await
        .map_err(|(code, message)| failed(&code, message))?;
    env.after(Step::Slice {
        file: args.file.index,
        slice: args.slice,
    })
    .await;
    Ok(value)
}

// ─── progress ───────────────────────────────────────────────────────────────

fn progress_fn() -> ProgressFn {
    Arc::new(|client, id| Box::pin(async move { progress(&client, &id).await }))
}

/// `{files_total, files_done, files_failed, rows_written, bytes_read}` of
/// operation `id` (Task 8 semantics 6): the file roots by tag, the slices of
/// running files by origin, the total from the plan step.
async fn progress(client: &DurableClient, id: &OperationId) -> Result<Value, OpsError> {
    let plan = match call(client, "promise.get", json!({ "id": format!("{id}:0") })).await {
        Ok(answer) => record_of(&answer, "promise")
            .ok()
            .and_then(|r| decode(&r.value))
            .and_then(|v| serde_json::from_value::<Plan>(v).ok()),
        Err(OpsError::NotFound(_)) => None,
        Err(e) => return Err(e),
    };
    let files = search(client, json!({ TAG_OP: id.as_str(), TAG_KIND: FILE_KIND })).await?;
    let (mut done, mut failed_files, mut rows, mut bytes) = (0u64, 0u64, 0u64, 0u64);
    for file in &files {
        match file.state.as_str() {
            "resolved" => {
                done += 1;
                if let Some(result) =
                    decode(&file.value).and_then(|v| serde_json::from_value::<FileResult>(v).ok())
                {
                    rows += result.rows;
                    bytes += result.bytes;
                }
            }
            "pending" => {
                for step in search(client, json!({ "resonate:origin": file.id })).await? {
                    if step.state != "resolved" {
                        continue;
                    }
                    if let Some(value) = decode(&step.value)
                        .filter(|v| v.get("token").is_some() && v.get("rows").is_some())
                        .and_then(|v| serde_json::from_value::<SliceValue>(v).ok())
                    {
                        rows += value.rows;
                        bytes += value.bytes;
                    }
                }
            }
            _ => failed_files += 1,
        }
    }
    Ok(json!({
        "files_total": plan.as_ref().map(|p| p.files.len()),
        "files_done": done,
        "files_failed": failed_files,
        "rows_written": rows,
        "bytes_read": bytes,
    }))
}

pub(crate) async fn call(
    client: &DurableClient,
    kind: &str,
    data: Value,
) -> Result<Value, OpsError> {
    match client.process(json!({ "kind": kind, "data": data })).await {
        Ok(mut answer) => Ok(answer
            .get_mut("data")
            .map(Value::take)
            .unwrap_or(Value::Null)),
        Err(crate::DurableError::Protocol { status: 404, .. }) => {
            Err(OpsError::NotFound(kind.to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

/// Every promise matching `tags`.
async fn search(client: &DurableClient, tags: Value) -> Result<Vec<crate::ops::Record>, OpsError> {
    search_in(client, tags, None).await
}

/// Every promise matching `tags`, only in `state` when given.
pub(crate) async fn search_in(
    client: &DurableClient,
    tags: Value,
    state: Option<&str>,
) -> Result<Vec<crate::ops::Record>, OpsError> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut data = json!({ "tags": tags, "limit": 500 });
        if let Some(state) = state {
            data["state"] = json!(state);
        }
        if let Some(cursor) = &cursor {
            data["cursor"] = json!(cursor);
        }
        let page = call(client, "promise.search", data).await?;
        let records: Vec<crate::ops::Record> = serde_json::from_value(
            page.get("promises")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        )
        .map_err(|e| OpsError::Internal(format!("an unreadable search answer: {e}")))?;
        let empty = records.is_empty();
        out.extend(records);
        match page.get("cursor").and_then(Value::as_str) {
            Some(next) if !empty => cursor = Some(next.to_string()),
            _ => return Ok(out),
        }
    }
}

// ─── retries ────────────────────────────────────────────────────────────────

/// One attempt's failure.
#[derive(Debug, Clone)]
pub(crate) enum Attempt {
    /// Transient; wait at least `after_ms`.
    Retry { message: String, after_ms: u64 },
    /// Permanent: the file fails with `code`.
    Fail { code: String, message: String },
}

/// `attempt` until it succeeds, fails for good, or `retry_for` runs out,
/// with jittered exponential backoff (X7).
pub(crate) async fn retrying<T, F, Fut>(
    env: &ImportEnv,
    what: &str,
    mut attempt: F,
) -> std::result::Result<T, (String, String)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, Attempt>>,
{
    let deadline = tokio::time::Instant::now() + env.config.retry_for;
    let mut backoff = env.config.backoff;
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(Attempt::Fail { code, message }) => return Err((code, message)),
            Err(Attempt::Retry { message, after_ms }) => {
                let wait = jitter(backoff).max(Duration::from_millis(after_ms));
                if tokio::time::Instant::now() + wait > deadline {
                    return Err((
                        "unavailable".into(),
                        format!(
                            "{what}: still failing after {:?}: {message}",
                            env.config.retry_for
                        ),
                    ));
                }
                tracing::debug!(what, error = %message, ?wait, "retrying an import step");
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// `d` scaled by a factor in [0.5, 1.5).
fn jitter(d: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.subsec_nanos())
        .unwrap_or(0);
    let factor = 0.5 + f64::from(nanos % 1000) / 1000.0;
    d.mul_f64(factor)
}
