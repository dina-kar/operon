//! Lance integration (plan M1.1 Task 7, Rulings 1, 2 and 5; overview R7,
//! R18, R19).
//!
//! Every dataset is opened through a [`LanceEnv`]: Lance's I/O goes through
//! the Operon [`Store`] (so a `FaultyStore` sees all of it), with an explicit
//! commit handler, file format 2.1, stable row ids and no auto-cleanup.
//!
//! A dataset has exactly one mainline version, the empty version 1
//! ([`LanceEnv::ensure_created`]).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, WriteMode, WriteParams};
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use lance_io::object_store::providers::ObjectStoreProvider;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_table::io::commit::{CommitHandler, ConditionalPutCommitHandler};
use operon_common::{CollectionId, NamespaceId};
use operon_store::Store;
use url::Url;

use crate::arrow_schema::base_arrow_schema;
use crate::config::LanceConfig;
use crate::error::CollectionError;

/// The URL scheme of Operon's Lance object-store provider.
pub const LANCE_SCHEME: &str = "operon";

/// Retries Lance makes of one failed download.
const DOWNLOAD_RETRIES: usize = 3;

/// Opens collection datasets over one [`Store`]. Cheap to clone.
#[derive(Clone)]
pub struct LanceEnv {
    store: Store,
    /// Unique per environment, so two stores in one process never share
    /// Lance's caches.
    authority: String,
    session: Arc<Session>,
    handler: Arc<dyn CommitHandler>,
    config: LanceConfig,
}

impl fmt::Debug for LanceEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceEnv")
            .field("store", &self.store)
            .field("authority", &self.authority)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Hands Lance the Operon store for `operon://<authority>/…` URLs; the URL
/// path is the path within the store.
#[derive(Debug)]
struct OperonStoreProvider {
    store: Store,
    authority: String,
    io_parallelism: usize,
}

#[async_trait::async_trait]
impl ObjectStoreProvider for OperonStoreProvider {
    async fn new_store(
        &self,
        base: Url,
        _params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        if base.host_str() != Some(self.authority.as_str()) {
            return Err(lance::Error::invalid_input(format!(
                "{base} is not in the Operon store {LANCE_SCHEME}://{}/",
                self.authority
            )));
        }
        let location = Url::parse(&format!("{LANCE_SCHEME}://{}/", self.authority))
            .map_err(|err| lance::Error::invalid_input(err.to_string()))?;
        Ok(ObjectStore::new(
            self.store.inner().clone(),
            location,
            None,
            None,
            false,
            true,
            self.io_parallelism,
            DOWNLOAD_RETRIES,
            None,
        ))
    }

    fn calculate_object_store_prefix(
        &self,
        _url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(format!("{LANCE_SCHEME}${}", self.authority))
    }
}

impl LanceEnv {
    /// An environment with a fresh authority (a lowercase ULID) and its own
    /// Lance session.
    pub fn new(store: Store, config: LanceConfig) -> Self {
        let authority = ulid::Ulid::generate().to_string().to_lowercase();
        let registry = Arc::new(ObjectStoreRegistry::empty());
        registry.insert(
            LANCE_SCHEME,
            Arc::new(OperonStoreProvider {
                store: store.clone(),
                authority: authority.clone(),
                io_parallelism: config.io_parallelism,
            }),
        );
        let session = Arc::new(Session::new(
            config.index_cache_bytes,
            config.metadata_cache_bytes,
            registry,
        ));
        Self {
            store,
            authority,
            session,
            handler: Arc::new(ConditionalPutCommitHandler),
            config,
        }
    }

    /// `operon://<authority>/ns/<ns>/collections/<cid>/lance`.
    pub fn uri(&self, namespace: NamespaceId, collection: CollectionId) -> String {
        format!(
            "{LANCE_SCHEME}://{}/ns/{namespace}/collections/{collection}/lance",
            self.authority
        )
    }

    /// The parameters of every Lance data write (R8, R18, R19).
    pub fn write_params(&self) -> WriteParams {
        WriteParams {
            max_rows_per_file: self.config.max_rows_per_file,
            max_rows_per_group: self.config.max_rows_per_group,
            // Data is only ever staged onto an existing dataset.
            mode: WriteMode::Append,
            commit_handler: Some(self.handler.clone()),
            data_storage_version: Some(LanceFileVersion::V2_1),
            enable_stable_row_ids: true,
            enable_v2_manifest_paths: true,
            session: Some(self.session.clone()),
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..WriteParams::default()
        }
    }

    /// Mainline version 1 (empty, base schema), creating it if absent (Ruling 5).
    pub async fn ensure_created(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let uri = self.uri(namespace, collection);
        match self.load(&uri, 1).await {
            Ok(dataset) => return check_version_one(dataset),
            Err(err) if is_not_found(&err) => {}
            Err(err) => return Err(err.into()),
        }
        let operation = Operation::Overwrite {
            fragments: vec![],
            schema: lance::datatypes::Schema::try_from(&base_arrow_schema())?,
            config_upsert_values: None,
            initial_bases: None,
        };
        let created = CommitBuilder::new(uri.as_str())
            .with_session(self.session.clone())
            .with_commit_handler(self.handler.clone())
            .use_stable_row_ids(true)
            .with_storage_format(LanceFileVersion::V2_1)
            .enable_v2_manifest_paths(true)
            .with_skip_auto_cleanup(true)
            // A strict overwrite: if another creator's version 1 appears
            // after the load above, Lance fails (it cannot check out our read
            // version 0) instead of committing version 2 on top of it.
            .with_max_retries(0)
            .execute(Transaction::new(0, operation, None))
            .await;
        match created {
            Ok(dataset) => check_version_one(dataset),
            // Another creator won, or our write landed ambiguously: either way
            // version 1 is the same empty dataset. If it is not there, the
            // original (retryable) error stands.
            Err(err) => match self.load(&uri, 1).await {
                Ok(dataset) => check_version_one(dataset),
                Err(load) if is_not_found(&load) => Err(err.into()),
                Err(load) => Err(load.into()),
            },
        }
    }

    /// The dataset at `version` (1 or a detached id). A missing version is
    /// `NotFound`.
    pub async fn open(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
        version: u64,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let uri = self.uri(namespace, collection);
        match self.load(&uri, version).await {
            Ok(dataset) => Ok(Arc::new(dataset)),
            Err(err) if is_not_found(&err) => Err(CollectionError::NotFound(format!(
                "lance version {version} of {uri}: {err}"
            ))),
            Err(err) => Err(err.into()),
        }
    }

    async fn load(&self, uri: &str, version: u64) -> lance::Result<Dataset> {
        DatasetBuilder::from_uri(uri)
            .with_version(version)
            .with_session(self.session.clone())
            .with_commit_handler(self.handler.clone())
            .load()
            .await
    }
}

fn is_not_found(err: &lance::Error) -> bool {
    matches!(
        err,
        lance::Error::NotFound { .. }
            | lance::Error::DatasetNotFound { .. }
            | lance::Error::VersionNotFound { .. }
    )
}

/// `dataset` if it is a mainline version 1 whose schema starts with the
/// system columns.
fn check_version_one(dataset: Dataset) -> Result<Arc<Dataset>, CollectionError> {
    if dataset.manifest.version != 1 {
        return Err(CollectionError::Internal(format!(
            "expected lance version 1, got {}",
            dataset.manifest.version
        )));
    }
    let schema = ArrowSchema::from(dataset.schema());
    let base = base_arrow_schema();
    let starts_with_base = schema.fields().len() >= base.fields().len()
        && base
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(want, got)| want == got);
    if !starts_with_base {
        return Err(CollectionError::Corrupt(format!(
            "lance version 1 at {} does not start with the system columns: {schema:?}",
            dataset.uri()
        )));
    }
    Ok(Arc::new(dataset))
}
