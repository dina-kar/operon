//! The functions an app serves, and `Deploy` (design §20 §6; R1 plan
//! Tasks 12–13).
//!
//! An app serves the built-in system functions ([`system`]) and the
//! functions of its current deployment: a bundle loaded by an [`Engine`]
//! (`operon-live-js`'s QuickJS engine in `operon`). [`Deployments`] holds
//! the current deployment and resolves names; [`Deployments::deploy`]
//! validates a bundle, stores it with `operon-store` at
//! `live/<app>/deployments/<id>.js` (R1 plan Ruling 10), applies the schema
//! (tables and indexes, Ruling 5) and swaps the catalog's deployment record
//! in one transaction.
//!
//! **The deploy gate** (rows T9-1, T10-12): a deploy that changes an
//! existing table's indexes takes [`Runner::try_quiesce`] before it applies
//! the schema and holds it through the commit. While a mutation is in
//! flight that is refused with [`LiveError::Busy`] (`UNAVAILABLE`; the
//! client retries), and while the gate is held new mutations wait at
//! admission, so none can race the index change. Inserts do not lock the
//! table record.
//!
//! A resolved deployed function is a proxy that looks the name up in the
//! current deployment on every call, so a subscription moves to a new
//! deployment at its next rerun and keeps its old results until then.

use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use buffa::Message;
use operon_store::{Store, StoreError};
use operon_tikv::{Tikv, TxnError, TxnOptions};
use rand::TryRngCore;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::catalog::{self, IndexSpec, TableDef};
use crate::session::Resolve;
use crate::txn::{FnKind, Function, LiveTxn, Runner};
use crate::{LiveError, LiveValue, pb, system};

/// The largest bundle `Deploy` takes (16 MiB).
pub const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;

/// The runner's name for a deploy's catalog transaction.
pub const DEPLOY_OP: &str = "live.deploy";

/// A function engine: validates and loads bundles.
pub trait Engine: Send + Sync + fmt::Debug {
    /// Loads the bundle `source` (one ES module), refusing an invalid one
    /// with [`LiveError::InvalidArgument`].
    fn load(&self, source: &str) -> Result<Arc<dyn Deployment>, LiveError>;
}

/// A loaded bundle: its functions.
pub trait Deployment: Send + Sync {
    /// Every function, as `("module:export", kind)`, sorted by path.
    fn functions(&self) -> Vec<(String, FnKind)>;
    /// The function at `path`.
    fn function(&self, path: &str) -> Option<Arc<dyn Function>>;
}

/// The function `name` among the system functions only, else
/// [`LiveError::NotFound`].
pub fn resolve(name: &str) -> Result<Arc<dyn Function>, LiveError> {
    system::lookup(name).ok_or_else(|| not_deployed(name))
}

fn not_deployed(name: &str) -> LiveError {
    LiveError::NotFound(format!("function {name:?} is not deployed"))
}

/// The object path of deployment `id` of `app`.
pub fn bundle_path(app: &str, id: &str) -> String {
    format!("live/{app}/deployments/{id}.js")
}

/// The app's deployments on this node: the current one, name resolution
/// and `Deploy`. Cheap to clone.
#[derive(Clone)]
pub struct Deployments {
    inner: Arc<Inner>,
}

struct Inner {
    app: String,
    runner: Runner,
    store: Store,
    engine: Option<Arc<dyn Engine>>,
    current: RwLock<Option<Current>>,
    /// Serializes deploys on this node.
    deploying: tokio::sync::Mutex<()>,
    pause: Mutex<Option<Pause>>,
}

#[derive(Clone)]
struct Current {
    id: String,
    deployment: Arc<dyn Deployment>,
}

/// A test hook: the deploy stops right after it takes the gate.
struct Pause {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

/// The two ends of [`Deployments::pause_after_gate`].
#[doc(hidden)]
#[derive(Debug)]
pub struct DeployPause {
    /// Fires when the next deploy holds the gate.
    pub reached: oneshot::Receiver<()>,
    /// Send (or drop) to let it go on.
    pub release: oneshot::Sender<()>,
}

impl fmt::Debug for Deployments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deployments")
            .field("app", &self.inner.app)
            .field("current", &self.current_id())
            .field("engine", &self.inner.engine)
            .finish_non_exhaustive()
    }
}

impl Deployments {
    /// The deployments of `app`, run by `runner`, with bundles in `store`
    /// loaded by `engine`. Nothing is deployed until [`load_current`] or
    /// [`deploy`].
    ///
    /// [`load_current`]: Self::load_current
    /// [`deploy`]: Self::deploy
    pub fn new(app: &str, runner: Runner, store: Store, engine: Option<Arc<dyn Engine>>) -> Self {
        Deployments {
            inner: Arc::new(Inner {
                app: app.to_string(),
                runner,
                store,
                engine,
                current: RwLock::new(None),
                deploying: tokio::sync::Mutex::new(()),
                pause: Mutex::new(None),
            }),
        }
    }

    /// The current deployment's id.
    pub fn current_id(&self) -> Option<String> {
        self.current().map(|c| c.id)
    }

    fn current(&self) -> Option<Current> {
        self.inner
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Loads the deployment the catalog names (at startup): reads its
    /// record, fetches and checks the bundle, and loads it. `Ok(None)` when
    /// nothing is deployed.
    pub async fn load_current(&self) -> Result<Option<String>, LiveError> {
        let tikv = self.inner.runner.tikv();
        let at = tikv
            .now()
            .await
            .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?;
        let mut snap = tikv
            .snapshot(at)
            .await
            .map_err(|e| LiveError::Internal(format!("a snapshot: {e}")))?;
        let Some(bytes) = snap.get(&self.inner.runner.app().deployment()).await? else {
            return Ok(None);
        };
        let record = pb::DeploymentRecord::decode_from_slice(&bytes)
            .map_err(|e| LiveError::Corrupt(format!("the deployment record: {e}")))?;
        if record.format != 1 {
            return Err(LiveError::Corrupt(format!(
                "deployment record format {} (expected 1)",
                record.format
            )));
        }
        let engine = self.engine()?;
        let (bundle, _) = self
            .inner
            .store
            .get(&record.object)
            .await
            .map_err(|e| store_error("reading the deployed bundle", &e))?;
        if Sha256::digest(&bundle).as_slice() != record.sha256.as_slice() {
            return Err(LiveError::Corrupt(format!(
                "the bundle at {} does not match its deployment record",
                record.object
            )));
        }
        let source = std::str::from_utf8(&bundle)
            .map_err(|_| {
                LiveError::Corrupt(format!("the bundle at {} is not UTF-8", record.object))
            })?
            .to_string();
        let deployment = load_blocking(engine, source).await?;
        self.swap(record.id.clone(), deployment);
        Ok(Some(record.id))
    }

    fn engine(&self) -> Result<Arc<dyn Engine>, LiveError> {
        self.inner.engine.clone().ok_or_else(|| {
            LiveError::FailedPrecondition(
                "this server has no function engine, so it cannot deploy or load bundles".into(),
            )
        })
    }

    fn swap(&self, id: String, deployment: Arc<dyn Deployment>) {
        *self
            .inner
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Current { id, deployment });
    }

    /// The function `name`: a system function, else a function of the
    /// current deployment (as a proxy that follows later deployments), else
    /// [`LiveError::NotFound`].
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn Function>, LiveError> {
        if let Some(f) = system::lookup(name) {
            return Ok(f);
        }
        let current = self.current().ok_or_else(|| not_deployed(name))?;
        let f = current
            .deployment
            .function(name)
            .ok_or_else(|| not_deployed(name))?;
        Ok(Arc::new(Deployed {
            name: name.to_string(),
            kind: f.kind(),
            deployments: self.clone(),
        }))
    }

    /// [`resolve`](Self::resolve) as the sessions' resolver.
    pub fn resolver(&self) -> Resolve {
        let this = self.clone();
        Arc::new(move |name: &str| this.resolve(name))
    }

    /// Test hook: the next deploy that takes the gate waits for `release`
    /// after firing `reached`.
    #[doc(hidden)]
    pub fn pause_after_gate(&self) -> DeployPause {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self
            .inner
            .pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Pause {
            reached: reached_tx,
            release: release_rx,
        });
        DeployPause {
            reached: reached_rx,
            release: release_tx,
        }
    }

    /// Deploys `bundle` with `schema` (semantics 4): validates and loads
    /// the bundle, stores it, and in one transaction applies the schema and
    /// swaps the deployment record; then new calls resolve to it. Without a
    /// schema, the schema record is removed (inserts create tables again);
    /// tables are never dropped. Returns the deployment id.
    pub async fn deploy(
        &self,
        bundle: &[u8],
        schema: Option<pb::Schema>,
    ) -> Result<String, LiveError> {
        let engine = self.engine()?;
        if bundle.len() > MAX_BUNDLE_BYTES {
            return Err(LiveError::limit(
                "max_bundle_bytes",
                format!(
                    "the bundle has {} bytes, more than {MAX_BUNDLE_BYTES}",
                    bundle.len()
                ),
            ));
        }
        let source = std::str::from_utf8(bundle)
            .map_err(|_| LiveError::invalid("the bundle is not UTF-8"))?
            .to_string();
        let tables = schema_tables(schema.as_ref())?;
        let deployment = load_blocking(engine, source).await?;
        let _one = self.inner.deploying.lock().await;

        let id = new_id()?;
        let object = bundle_path(&self.inner.app, &id);
        self.inner
            .store
            .put(&object, bytes::Bytes::copy_from_slice(bundle))
            .await
            .map_err(|e| store_error("storing the bundle", &e))?;

        let changes = self.index_changes(&tables).await?;
        let _gate = if changes {
            let gate = self.inner.runner.try_quiesce().ok_or_else(|| {
                LiveError::Busy(
                    "the deploy changes an index and the app has mutations in flight; retry".into(),
                )
            })?;
            let pause = self
                .inner
                .pause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(pause) = pause {
                let _ = pause.reached.send(());
                let _ = pause.release.await;
            }
            Some(gate)
        } else {
            None
        };

        let functions = deployment
            .functions()
            .into_iter()
            .map(|(path, kind)| pb::DeployedFunction {
                path,
                mutation: kind == FnKind::Mutation,
                ..Default::default()
            })
            .collect();
        let record = pb::DeploymentRecord {
            format: 1,
            id: id.clone(),
            object,
            sha256: Sha256::digest(bundle).to_vec(),
            functions,
            ..Default::default()
        };
        let schema_record = schema.map(|schema| pb::SchemaRecord {
            format: 1,
            deployment_id: id.clone(),
            schema: schema.into(),
            ..Default::default()
        });
        self.commit(record, schema_record, tables, changes).await?;
        self.swap(id.clone(), deployment);
        tracing::info!(app = %self.inner.app, deployment = %id, "deployed a function bundle");
        Ok(id)
    }

    /// Whether applying `tables` would change the indexes of a table that
    /// exists now.
    async fn index_changes(&self, tables: &[(String, Vec<IndexSpec>)]) -> Result<bool, LiveError> {
        if tables.is_empty() {
            return Ok(false);
        }
        let tikv = self.inner.runner.tikv();
        let at = tikv
            .now()
            .await
            .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?;
        let mut snap = tikv
            .snapshot(at)
            .await
            .map_err(|e| LiveError::Internal(format!("a snapshot: {e}")))?;
        let app = self.inner.runner.app();
        for (name, indexes) in tables {
            if let Some(table) = catalog::load_table(&mut snap, app, name).await?
                && !same_indexes(&table, indexes)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The deploy's transaction: tables, the schema record, the deployment
    /// record. Without the gate (`gated` false), a table whose indexes
    /// would change is refused as busy: it changed after the check.
    async fn commit(
        &self,
        record: pb::DeploymentRecord,
        schema: Option<pb::SchemaRecord>,
        tables: Vec<(String, Vec<IndexSpec>)>,
        gated: bool,
    ) -> Result<(), LiveError> {
        let runner = &self.inner.runner;
        let app = runner.app().clone();
        let limits = runner.limits().clone();
        let mut opts = TxnOptions::new(DEPLOY_OP);
        opts.commit_mode = Some(runner.options().commit_mode);
        let record = Arc::new(record);
        let schema = Arc::new(schema);
        let tables = Arc::new(tables);
        runner
            .tikv()
            .run(opts, move |txn| {
                let app = app.clone();
                let limits = limits.clone();
                let record = record.clone();
                let schema = schema.clone();
                let tables = tables.clone();
                Box::pin(async move {
                    let applied = async {
                        for (name, indexes) in tables.iter() {
                            if !gated
                                && let Some(table) = catalog::load_table(txn, &app, name).await?
                                && !same_indexes(&table, indexes)
                            {
                                return Err(LiveError::Busy(format!(
                                    "table '{name}' changed during the deploy; retry"
                                )));
                            }
                            catalog::define_table(txn, &app, name, indexes, &limits).await?;
                        }
                        let mut record = (*record).clone();
                        record.deployed_ms = Tikv::physical_ms(&txn.start_ts());
                        txn.put(&app.deployment(), record.encode_to_vec()).await?;
                        match schema.as_ref() {
                            Some(schema) => {
                                txn.put(&app.schema(), schema.encode_to_vec()).await?;
                            }
                            None => txn.delete(&app.schema()).await?,
                        }
                        Ok(())
                    };
                    match applied.await {
                        Ok(()) => Ok(Ok(())),
                        Err(e) => e.into_txn().map(Err),
                    }
                })
            })
            .await
            .map_err(LiveError::Txn)?
            .value
    }
}

/// A deployed function as resolved: it looks its name up in the current
/// deployment on every call.
struct Deployed {
    name: String,
    kind: FnKind,
    deployments: Deployments,
}

impl Function for Deployed {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FnKind {
        self.kind
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> futures::future::BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let current = self
                .deployments
                .current()
                .ok_or_else(|| not_deployed(&self.name))?;
            let f = current
                .deployment
                .function(&self.name)
                .ok_or_else(|| not_deployed(&self.name))?;
            if f.kind() != self.kind {
                return Err(LiveError::FailedPrecondition(format!(
                    "{} is a {:?} in deployment {}, no longer a {:?}; call it again",
                    self.name,
                    f.kind(),
                    current.id,
                    self.kind
                )));
            }
            f.call(txn, args).await
        })
    }
}

fn same_indexes(table: &TableDef, want: &[IndexSpec]) -> bool {
    table.indexes.len() == want.len()
        && table
            .indexes
            .iter()
            .zip(want)
            .all(|(have, want)| have.name == want.name && have.fields == want.fields)
}

/// The schema's tables as `define_table` takes them, refusing duplicates.
fn schema_tables(schema: Option<&pb::Schema>) -> Result<Vec<(String, Vec<IndexSpec>)>, LiveError> {
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };
    let mut out: Vec<(String, Vec<IndexSpec>)> = Vec::with_capacity(schema.tables.len());
    for table in &schema.tables {
        catalog::check_name("table", &table.name)?;
        if out.iter().any(|(name, _)| *name == table.name) {
            return Err(LiveError::invalid(format!(
                "the schema defines table '{}' twice",
                table.name
            )));
        }
        let indexes = table
            .indexes
            .iter()
            .map(|i| IndexSpec {
                name: i.name.clone(),
                fields: i.fields.clone(),
            })
            .collect();
        out.push((table.name.clone(), indexes));
    }
    Ok(out)
}

/// Loads a bundle off the async threads (evaluating it runs JavaScript).
async fn load_blocking(
    engine: Arc<dyn Engine>,
    source: String,
) -> Result<Arc<dyn Deployment>, LiveError> {
    tokio::task::spawn_blocking(move || engine.load(&source))
        .await
        .map_err(|e| LiveError::Internal(format!("loading the bundle: {e}")))?
}

/// A new deployment id: 16 bytes from the OS random source, in hex.
fn new_id() -> Result<String, LiveError> {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| LiveError::Internal(format!("the OS random source: {e}")))?;
    Ok(hex::encode(bytes))
}

fn store_error(what: &str, e: &StoreError) -> LiveError {
    if e.is_retryable() {
        LiveError::Txn(TxnError::NotApplied(format!("{what}: {e}")))
    } else {
        LiveError::Internal(format!("{what}: {e}"))
    }
}
