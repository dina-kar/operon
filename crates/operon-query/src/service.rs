//! `CollectionService`, the one facade every gateway uses (overview §6.7;
//! plan M1.2 Task 9): collections, schemas, aliases, versions and pins
//! through the metastore, writes through M1.1's `CollectionWriter`
//! ([`crate::write`]), and reads through the read views and the planner,
//! forwarded to the owner of the collection when the placement says so.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use operon_collection::{
    CollectionContext, CollectionSchema, CollectionWriter, FieldSpec, PrimaryKey, VectorSpec,
    live_manifest, retained_chain,
};
use operon_common::meta::{
    AliasAction, ApplyError, Collection, CollectionHead, Consistency, MetaError,
};
use operon_common::{CollectionId, NamespaceId};
use operon_log::LogReader;

use crate::catalog_cache::CatalogCache;
use crate::error::ServiceError;
use crate::exec::planner::{SearchConfig, SearchPlanner};
use crate::hot::{self, HotTier, HotUsed, NoHotTier, RequestHot};
use crate::ir::{Query, ReadConsistency, SearchRequest, SearchResponse};
use crate::placement::{LocalOnly, NoRemoteReads, Owner, Placement, RemoteReads};
use crate::read::{ReadConfig, Reads};
use crate::tail::TailConfig;
use crate::types::{CollectionInfo, ManifestInfo, PinnedRead, Projection, StoredDoc};
use crate::validate::validate_request;
use crate::vector::AnnConfig;

/// The schema annotation `create_collection` writes: the proposer's clock
/// (`MetaStore::now_ms`) at creation, in ms since the epoch (Ruling 13).
pub const CREATED_AT_ANNOTATION: &str = "operon.created_at_ms";

/// How SQL statements are bounded (Task 10).
#[derive(Clone, Debug, PartialEq)]
pub struct SqlConfig {
    /// 10 000 rows per statement.
    pub max_rows: usize,
    /// 30 s per statement.
    pub timeout: Duration,
    /// 8 192 rows per record batch.
    pub batch_rows: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            timeout: Duration::from_secs(30),
            batch_rows: 8_192,
        }
    }
}

/// How the service creates collections and bounds requests.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceConfig {
    /// 4 (Ruling 20).
    pub default_partitions: u32,
    /// true: whether a request without a hot scope reads the hot tier (the
    /// `operon` flag `--hot=on|off`).
    pub hot_default: bool,
    pub tail: TailConfig,
    pub read: ReadConfig,
    pub search: SearchConfig,
    pub ann: AnnConfig,
    pub sql: SqlConfig,
    /// 10 000 keys per get.
    pub max_get_keys: usize,
    /// 10 000 documents per scroll page.
    pub max_scroll_limit: usize,
    /// 5 compare-and-set attempts per schema update (Ruling 18).
    pub schema_retries: u32,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            default_partitions: 4,
            hot_default: true,
            tail: TailConfig::default(),
            read: ReadConfig::default(),
            search: SearchConfig::default(),
            ann: AnnConfig::default(),
            sql: SqlConfig::default(),
            max_get_keys: 10_000,
            max_scroll_limit: 10_000,
            schema_retries: 5,
        }
    }
}

/// The one facade every gateway uses (overview §6.7). It is the only code in
/// `operon-query` that calls metastore write operations.
pub struct CollectionService {
    pub(crate) ctx: CollectionContext,
    pub(crate) writer: CollectionWriter,
    pub(crate) reads: Reads,
    pub(crate) planner: SearchPlanner,
    pub(crate) hot: RwLock<Arc<dyn HotTier>>,
    pub(crate) placement: RwLock<(Arc<dyn Placement>, Arc<dyn RemoteReads>)>,
    pub(crate) catalog: CatalogCache,
    pub(crate) config: ServiceConfig,
    /// The service itself, for the SQL catalog of Task 10.
    #[allow(dead_code)]
    pub(crate) this: Weak<CollectionService>,
}

impl fmt::Debug for CollectionService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionService")
            .field("reads", &self.reads)
            .field("planner", &self.planner)
            .field("catalog", &self.catalog)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A read the owner could not serve now falls back to the local path: its
/// result, or `None` to read locally.
async fn forwarded<T>(
    hot: &RequestHot,
    what: &str,
    call: impl Future<Output = Result<T, ServiceError>>,
) -> Option<Result<T, ServiceError>> {
    // The transport forwards `Operon-Hot` from, and records the owner's
    // `Operon-Hot-Used` into, the request's scope (the `RemoteReads` contract).
    match hot::scope(hot.clone(), call).await {
        Err(err @ (ServiceError::Unavailable(_) | ServiceError::Timeout)) => {
            tracing::debug!(%err, what, "the owner cannot serve the read; reading locally");
            None
        }
        result => Some(result),
    }
}

fn collection_not_found(name: &str) -> ServiceError {
    ServiceError::NotFound {
        kind: "collection",
        name: name.to_string(),
    }
}

/// `schema` without its version and creation annotation, for the
/// retry-safety comparison of `create_collection` (Ruling 13).
fn requested_shape(schema: &CollectionSchema) -> CollectionSchema {
    let mut shape = schema.clone();
    shape.version = 1;
    shape.annotations.remove(CREATED_AT_ANNOTATION);
    shape
}

/// Σ over the collection's partitions of (high watermark − applied).
fn link_lag(head: &CollectionHead, applied: &BTreeMap<u32, u64>) -> u64 {
    (0..head.collection.partitions)
        .map(|p| {
            let hwm = head.high_watermarks.get(p as usize).copied().unwrap_or(0);
            hwm.saturating_sub(applied.get(&p).copied().unwrap_or(0))
        })
        .sum()
}

impl CollectionService {
    /// A service over `ctx`, writing through `writer` and following the
    /// implicit streams through `reader`; starts the catalog cache (call
    /// from within a Tokio runtime). The hot tier starts as [`NoHotTier`] and
    /// the placement as [`LocalOnly`] with [`NoRemoteReads`].
    pub fn new(
        ctx: CollectionContext,
        writer: CollectionWriter,
        reader: LogReader,
        config: ServiceConfig,
    ) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            reads: Reads::new(
                ctx.clone(),
                reader,
                config.tail.clone(),
                config.read.clone(),
            ),
            planner: SearchPlanner::new(config.search.clone(), config.ann.clone()),
            catalog: CatalogCache::start(ctx.meta.clone()),
            hot: RwLock::new(Arc::new(NoHotTier)),
            placement: RwLock::new((Arc::new(LocalOnly), Arc::new(NoRemoteReads))),
            ctx,
            writer,
            config,
            this: this.clone(),
        })
    }

    pub fn set_hot_tier(&self, hot: Arc<dyn HotTier>) {
        *self.hot.write().unwrap_or_else(PoisonError::into_inner) = hot;
    }

    pub fn set_placement(&self, placement: Arc<dyn Placement>, remote: Arc<dyn RemoteReads>) {
        *self
            .placement
            .write()
            .unwrap_or_else(PoisonError::into_inner) = (placement, remote);
    }

    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    /// The names of the collections and aliases of every namespace (Task 10).
    pub fn catalog(&self) -> &CatalogCache {
        &self.catalog
    }

    /// The read views (tests hold and inspect tails through them).
    #[cfg(feature = "test-util")]
    pub fn reads(&self) -> &Reads {
        &self.reads
    }

    // ----- Resolution, the hot switch and placement -----

    /// The id of namespace `ns`, if it exists.
    pub(crate) async fn namespace_id(&self, ns: &str) -> Result<Option<NamespaceId>, ServiceError> {
        Ok(self
            .ctx
            .meta
            .namespace_by_name(Consistency::Local, ns)
            .await?
            .map(|namespace| namespace.id))
    }

    /// The collection named (or aliased) `name_or_alias` in namespace `ns`;
    /// an absent namespace behaves like an empty one (S7).
    pub(crate) async fn resolve(
        &self,
        ns: &str,
        name_or_alias: &str,
    ) -> Result<(NamespaceId, Collection), ServiceError> {
        let Some(ns_id) = self.namespace_id(ns).await? else {
            return Err(collection_not_found(name_or_alias));
        };
        let collection = self
            .ctx
            .meta
            .resolve_collection(Consistency::Local, ns_id, name_or_alias)
            .await?
            .filter(|collection| collection.namespace == ns_id)
            .ok_or_else(|| collection_not_found(name_or_alias))?;
        Ok((ns_id, collection))
    }

    /// The request's hot scope, or the server default outside one (Ruling
    /// 11); read once at method entry, before anything is spawned.
    pub(crate) fn request_hot(&self) -> RequestHot {
        hot::current().unwrap_or_else(|| RequestHot {
            enabled: self.config.hot_default,
            used: HotUsed::default(),
        })
    }

    /// The hot tier a read uses: none when the request switched it off.
    fn hot_tier(&self, hot: &RequestHot) -> Arc<dyn HotTier> {
        if hot.enabled {
            self.hot
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        } else {
            Arc::new(NoHotTier)
        }
    }

    /// The owner and the transport when another node owns the reads of
    /// `cid`.
    fn remote_owner(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
    ) -> Option<(Owner, Arc<dyn RemoteReads>)> {
        let (placement, remote) = self
            .placement
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match placement.owner(ns, cid) {
            Owner::Local => None,
            owner @ Owner::Remote { .. } => Some((owner, remote)),
        }
    }

    fn check_get(&self, pks: &[PrimaryKey]) -> Result<(), ServiceError> {
        if pks.len() > self.config.max_get_keys {
            return Err(ServiceError::InvalidArgument(format!(
                "a get takes at most {} keys, got {}",
                self.config.max_get_keys,
                pks.len()
            )));
        }
        Ok(())
    }

    fn check_scroll(&self, limit: usize) -> Result<(), ServiceError> {
        if limit > self.config.max_scroll_limit {
            return Err(ServiceError::InvalidArgument(format!(
                "a scroll page holds at most {} documents, got {limit}",
                self.config.max_scroll_limit
            )));
        }
        Ok(())
    }

    // ----- Namespaces and collections -----

    /// Creates namespace `ns` unless it exists; the metastore checks the name.
    pub async fn ensure_namespace(&self, ns: &str) -> Result<(), ServiceError> {
        match self.ctx.meta.create_namespace(ns).await {
            Ok(_) | Err(MetaError::Rejected(ApplyError::NamespaceExists(_))) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Creates collection `name` (and a missing namespace) with schema
    /// version 1. A retry with the same schema (ignoring the creation
    /// annotation) and no or the same partition count returns the existing
    /// collection.
    pub async fn create_collection(
        &self,
        ns: &str,
        name: &str,
        schema: CollectionSchema,
        partitions: Option<u32>,
    ) -> Result<CollectionInfo, ServiceError> {
        self.ensure_namespace(ns).await?;
        let ns_id = self.namespace_id(ns).await?.ok_or_else(|| {
            ServiceError::Unavailable(format!("namespace {ns} is not visible yet"))
        })?;
        let requested = requested_shape(&schema);
        let mut schema = schema;
        schema.version = 1;
        schema.annotations.insert(
            CREATED_AT_ANNOTATION.to_string(),
            self.ctx.meta.now_ms().to_string(),
        );
        let count = partitions.unwrap_or(self.config.default_partitions);
        let cid = match self
            .ctx
            .meta
            .create_collection(ns_id, name, schema, count)
            .await
        {
            Ok((cid, _, _)) | Err(MetaError::Rejected(ApplyError::CollectionExists(cid))) => cid,
            Err(MetaError::Rejected(ApplyError::NameTaken(_))) => {
                let existing = self
                    .ctx
                    .meta
                    .resolve_collection(Consistency::Local, ns_id, name)
                    .await?
                    .filter(|c| c.namespace == ns_id && c.name == name);
                match existing {
                    Some(existing)
                        if requested_shape(&existing.schema).same_ignoring_version(&requested)
                            && partitions.is_none_or(|p| p == existing.partitions) =>
                    {
                        existing.id
                    }
                    _ => return Err(ServiceError::AlreadyExists(name.to_string())),
                }
            }
            Err(MetaError::Rejected(ApplyError::InvalidArgument(message))) => {
                return Err(ServiceError::InvalidArgument(message));
            }
            Err(err) => return Err(err.into()),
        };
        self.info(ns, ns_id, cid, name).await
    }

    /// Drops collection `name` (not an alias) and stops its tail; `false`
    /// when there is no such collection.
    pub async fn drop_collection(&self, ns: &str, name: &str) -> Result<bool, ServiceError> {
        let Some(ns_id) = self.namespace_id(ns).await? else {
            return Ok(false);
        };
        let target = self
            .ctx
            .meta
            .resolve_collection(Consistency::Local, ns_id, name)
            .await?
            .filter(|c| c.namespace == ns_id && c.name == name);
        if let Some(collection) = target {
            self.reads.stop_collection(collection.id).await;
        }
        match self.ctx.meta.drop_collection(ns_id, name).await {
            Ok(Some(cid)) => {
                // A read racing the drop may have started a new tail.
                self.reads.stop_collection(cid).await;
                Ok(true)
            }
            Ok(None) | Err(MetaError::Rejected(ApplyError::NamespaceNotFound(_))) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_collection(
        &self,
        ns: &str,
        name_or_alias: &str,
    ) -> Result<CollectionInfo, ServiceError> {
        let (ns_id, collection) = self.resolve(ns, name_or_alias).await?;
        self.info(ns, ns_id, collection.id, name_or_alias).await
    }

    /// The collections of `ns` by name; `[]` for an absent namespace.
    pub async fn list_collections(&self, ns: &str) -> Result<Vec<CollectionInfo>, ServiceError> {
        let Some(ns_id) = self.namespace_id(ns).await? else {
            return Ok(Vec::new());
        };
        let heads = self
            .ctx
            .meta
            .collection_heads(Consistency::Local, Some(ns_id))
            .await?;
        let aliases = self.ctx.meta.aliases(Consistency::Local, ns_id).await?;
        let mut infos = Vec::with_capacity(heads.len());
        for head in heads
            .into_iter()
            .filter(|head| head.collection.namespace == ns_id)
        {
            infos.push(self.info_of(ns, head, &aliases).await?);
        }
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(infos)
    }

    /// The info of collection `cid` of namespace `ns_id` (named `ns`);
    /// `name` names it in a `NotFound`.
    async fn info(
        &self,
        ns: &str,
        ns_id: NamespaceId,
        cid: CollectionId,
        name: &str,
    ) -> Result<CollectionInfo, ServiceError> {
        let head = self
            .ctx
            .meta
            .collection_head(Consistency::Local, cid)
            .await?
            .filter(|head| head.collection.namespace == ns_id)
            .ok_or_else(|| collection_not_found(name))?;
        let aliases = self.ctx.meta.aliases(Consistency::Local, ns_id).await?;
        self.info_of(ns, head, &aliases).await
    }

    /// Rule 5: the catalog record and stream bounds of `head`, the live
    /// manifest through the manifest cache and its Lance version through
    /// the snapshot cache.
    async fn info_of(
        &self,
        ns: &str,
        head: CollectionHead,
        aliases: &[(String, CollectionId)],
    ) -> Result<CollectionInfo, ServiceError> {
        let collection = &head.collection;
        let mut manifest_version = 0;
        let mut live_doc_count = 0;
        let mut size_bytes = 0;
        let mut applied = BTreeMap::new();
        if let Some(pointer) = &head.pointer {
            let manifest = self
                .ctx
                .manifests
                .load(&self.ctx.store, &pointer.value)
                .await?;
            manifest_version = pointer.version;
            live_doc_count = manifest.live_doc_count;
            applied = manifest.applied.clone();
            size_bytes = manifest.splits.iter().map(|split| split.size_bytes).sum();
            let snapshot = self
                .reads
                .snapshot(
                    collection.namespace,
                    collection,
                    Some(pointer.value.clone()),
                    manifest,
                )
                .await?;
            if let Some(dataset) = snapshot.dataset() {
                // Sizes Lance did not record count 0.
                size_bytes += dataset
                    .fragments()
                    .iter()
                    .flat_map(|fragment| &fragment.files)
                    .map(|file| file.file_size_bytes.get().map_or(0, u64::from))
                    .sum::<u64>();
            }
        }
        let mut alias_names: Vec<String> = aliases
            .iter()
            .filter(|(_, target)| *target == collection.id)
            .map(|(alias, _)| alias.clone())
            .collect();
        alias_names.sort();
        let created_at_ms = collection
            .schema
            .annotations
            .get(CREATED_AT_ANNOTATION)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let hot = self
            .hot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .status(collection.namespace, collection.id);
        Ok(CollectionInfo {
            id: collection.id,
            name: collection.name.clone(),
            namespace: ns.to_string(),
            schema: collection.schema.clone(),
            partitions: collection.partitions,
            aliases: alias_names,
            stream: collection.stream,
            manifest_version,
            live_doc_count,
            size_bytes,
            created_at_ms,
            link_lag_records: link_lag(&head, &applied),
            hot,
        })
    }

    /// Appends `fields` and dense `vectors` to the schema and merges
    /// `annotations` into it, by compare-and-set (at most `schema_retries`
    /// attempts). A field or vector that exists with the same spec is
    /// skipped, so a retry is idempotent. Existing documents are not
    /// re-indexed (A4).
    pub async fn add_fields(
        &self,
        ns: &str,
        name: &str,
        fields: Vec<FieldSpec>,
        vectors: Vec<VectorSpec>,
        annotations: BTreeMap<String, String>,
    ) -> Result<CollectionSchema, ServiceError> {
        let (ns_id, collection) = self.resolve(ns, name).await?;
        for _ in 0..self.config.schema_retries {
            // 1.
            let current = self
                .ctx
                .meta
                .collection(Consistency::Linearizable, collection.id)
                .await?
                .filter(|c| c.namespace == ns_id)
                .ok_or_else(|| collection_not_found(name))?
                .schema;
            // 2.
            let mut next = current.clone();
            for field in &fields {
                match current.fields.iter().find(|f| f.name == field.name) {
                    Some(existing) if existing == field => {}
                    Some(_) => {
                        return Err(ServiceError::AlreadyExists(format!("field {}", field.name)));
                    }
                    None => next.fields.push(field.clone()),
                }
            }
            for vector in &vectors {
                match current.vectors.iter().find(|v| v.name == vector.name) {
                    Some(existing) if existing == vector => {}
                    Some(_) => {
                        return Err(ServiceError::AlreadyExists(format!(
                            "field {}",
                            vector.name
                        )));
                    }
                    None => next.vectors.push(vector.clone()),
                }
            }
            next.annotations
                .extend(annotations.iter().map(|(k, v)| (k.clone(), v.clone())));
            if next == current {
                return Ok(current);
            }
            // 3.
            current
                .check_additive(&next)
                .map_err(|err| ServiceError::InvalidArgument(err.to_string()))?;
            // 4.
            match self
                .ctx
                .meta
                .update_collection_schema(collection.id, current.version, next.clone())
                .await
            {
                Ok(version) => {
                    self.reads.notify(collection.id);
                    return Ok(CollectionSchema { version, ..next });
                }
                Err(MetaError::Rejected(ApplyError::SchemaVersionMismatch { .. })) => {}
                Err(MetaError::Rejected(ApplyError::CollectionNotFound(_))) => {
                    return Err(collection_not_found(name));
                }
                Err(err) => return Err(err.into()),
            }
        }
        Err(ServiceError::Unavailable(format!(
            "schema update raced {} times",
            self.config.schema_retries
        )))
    }

    /// Applies alias actions atomically (rule 7).
    pub async fn update_aliases(
        &self,
        ns: &str,
        actions: Vec<AliasAction>,
    ) -> Result<(), ServiceError> {
        // An absent namespace holds no collection an alias could name.
        let absent =
            |actions: &[AliasAction]| match actions.iter().find_map(|action| match action {
                AliasAction::Create { collection, .. } => Some(collection),
                AliasAction::Delete { .. } => None,
            }) {
                Some(collection) => Err(collection_not_found(collection)),
                None => Ok(()),
            };
        let Some(ns_id) = self.namespace_id(ns).await? else {
            return absent(&actions);
        };
        match self.ctx.meta.update_aliases(ns_id, actions.clone()).await {
            Ok(()) => Ok(()),
            Err(MetaError::Rejected(ApplyError::NameTaken(name))) => {
                Err(ServiceError::AlreadyExists(name))
            }
            Err(MetaError::Rejected(ApplyError::UnknownCollection(name))) => {
                Err(collection_not_found(&name))
            }
            Err(MetaError::Rejected(ApplyError::NamespaceNotFound(_))) => absent(&actions),
            Err(err) => Err(err.into()),
        }
    }

    // ----- Reads -----

    /// Rule 1: resolve, forward to a remote owner (falling back to the local
    /// path when it is unavailable), or plan locally.
    pub async fn search(
        &self,
        ns: &str,
        request: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        let hot = self.request_hot();
        validate_request(&request, &self.config.search.limits)?;
        let (ns_id, collection) = self.resolve(ns, &request.collection).await?;
        if let Some((owner, remote)) = self.remote_owner(ns_id, collection.id) {
            let mut forward = request.clone();
            forward.collection = collection.name.clone();
            if let Some(result) =
                forwarded(&hot, "search", remote.search(&owner, ns, forward)).await
            {
                return result;
            }
        }
        self.search_in(ns_id, &collection, request, &hot).await
    }

    /// [`CollectionService::search`] on this node, never forwarded.
    pub async fn search_local(
        &self,
        ns: &str,
        request: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        let hot = self.request_hot();
        validate_request(&request, &self.config.search.limits)?;
        let (ns_id, collection) = self.resolve(ns, &request.collection).await?;
        self.search_in(ns_id, &collection, request, &hot).await
    }

    async fn search_in(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        request: SearchRequest,
        hot: &RequestHot,
    ) -> Result<SearchResponse, ServiceError> {
        let view = self
            .reads
            .view(
                ns_id,
                collection,
                &request.consistency,
                hot,
                self.hot_tier(hot),
            )
            .await?;
        self.planner.search(Arc::new(view), request).await
    }

    /// The documents of `pks` in request order, `None` for a missing key.
    pub async fn get(
        &self,
        ns: &str,
        name: &str,
        pks: &[PrimaryKey],
        select: &Projection,
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        let hot = self.request_hot();
        self.check_get(pks)?;
        let (ns_id, collection) = self.resolve(ns, name).await?;
        if let Some((owner, remote)) = self.remote_owner(ns_id, collection.id) {
            let call = remote.get(
                &owner,
                ns,
                &collection.name,
                pks.to_vec(),
                select.clone(),
                consistency.clone(),
            );
            if let Some(result) = forwarded(&hot, "get", call).await {
                return result;
            }
        }
        self.get_in(ns_id, &collection, pks, select, &consistency, &hot)
            .await
    }

    /// [`CollectionService::get`] on this node, never forwarded.
    pub async fn get_local(
        &self,
        ns: &str,
        name: &str,
        pks: &[PrimaryKey],
        select: &Projection,
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        let hot = self.request_hot();
        self.check_get(pks)?;
        let (ns_id, collection) = self.resolve(ns, name).await?;
        self.get_in(ns_id, &collection, pks, select, &consistency, &hot)
            .await
    }

    pub(crate) async fn get_in(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        pks: &[PrimaryKey],
        select: &Projection,
        consistency: &ReadConsistency,
        hot: &RequestHot,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        let view = self
            .reads
            .view(ns_id, collection, consistency, hot, self.hot_tier(hot))
            .await?;
        self.planner.get(&view, pks, select).await
    }

    /// The live documents matching `filter` (every live document without one).
    pub async fn count(
        &self,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        consistency: ReadConsistency,
    ) -> Result<u64, ServiceError> {
        let hot = self.request_hot();
        let (ns_id, collection) = self.resolve(ns, name).await?;
        if let Some((owner, remote)) = self.remote_owner(ns_id, collection.id) {
            let call = remote.count(
                &owner,
                ns,
                &collection.name,
                filter.clone(),
                consistency.clone(),
            );
            if let Some(result) = forwarded(&hot, "count", call).await {
                return result;
            }
        }
        self.count_in(ns_id, &collection, filter, &consistency, &hot)
            .await
    }

    /// [`CollectionService::count`] on this node, never forwarded.
    pub async fn count_local(
        &self,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        consistency: ReadConsistency,
    ) -> Result<u64, ServiceError> {
        let hot = self.request_hot();
        let (ns_id, collection) = self.resolve(ns, name).await?;
        self.count_in(ns_id, &collection, filter, &consistency, &hot)
            .await
    }

    async fn count_in(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        filter: Option<Query>,
        consistency: &ReadConsistency,
        hot: &RequestHot,
    ) -> Result<u64, ServiceError> {
        let view = self
            .reads
            .view(ns_id, collection, consistency, hot, self.hot_tier(hot))
            .await?;
        self.planner.count(Arc::new(view), filter).await
    }

    /// The next `limit` documents after `after` in primary-key order that
    /// match `filter`, and the key to continue after when more remain.
    #[allow(clippy::too_many_arguments)]
    pub async fn scroll(
        &self,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        after: Option<PrimaryKey>,
        limit: usize,
        select: &Projection,
        consistency: ReadConsistency,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        let hot = self.request_hot();
        self.check_scroll(limit)?;
        let (ns_id, collection) = self.resolve(ns, name).await?;
        if let Some((owner, remote)) = self.remote_owner(ns_id, collection.id) {
            let call = remote.scroll(
                &owner,
                ns,
                &collection.name,
                filter.clone(),
                after.clone(),
                limit,
                select.clone(),
                consistency.clone(),
            );
            if let Some(result) = forwarded(&hot, "scroll", call).await {
                return result;
            }
        }
        let scroll = Scroll {
            filter,
            after,
            limit,
            select,
            consistency: &consistency,
        };
        self.scroll_in(ns_id, &collection, scroll, &hot).await
    }

    /// [`CollectionService::scroll`] on this node, never forwarded.
    #[allow(clippy::too_many_arguments)]
    pub async fn scroll_local(
        &self,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        after: Option<PrimaryKey>,
        limit: usize,
        select: &Projection,
        consistency: ReadConsistency,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        let hot = self.request_hot();
        self.check_scroll(limit)?;
        let (ns_id, collection) = self.resolve(ns, name).await?;
        let scroll = Scroll {
            filter,
            after,
            limit,
            select,
            consistency: &consistency,
        };
        self.scroll_in(ns_id, &collection, scroll, &hot).await
    }

    async fn scroll_in(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        scroll: Scroll<'_>,
        hot: &RequestHot,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        let view = self
            .reads
            .view(
                ns_id,
                collection,
                scroll.consistency,
                hot,
                self.hot_tier(hot),
            )
            .await?;
        self.planner
            .scroll(
                Arc::new(view),
                scroll.filter,
                scroll.after,
                scroll.limit,
                scroll.select,
            )
            .await
    }

    // ----- Versions, pins, SQL, shutdown -----

    /// The retained manifests, oldest first; `[]` before the first commit.
    pub async fn versions(&self, ns: &str, name: &str) -> Result<Vec<ManifestInfo>, ServiceError> {
        let (ns_id, collection) = self.resolve(ns, name).await?;
        let live = live_manifest(
            &*self.ctx.meta,
            &self.ctx.store,
            &self.ctx.manifests,
            ns_id,
            collection.id,
            Consistency::Linearizable,
        )
        .await?;
        let Some(live) = live else {
            return Ok(Vec::new());
        };
        let clock = self.ctx.meta.clock_ms(Consistency::Linearizable).await?;
        let chain = retained_chain(
            &self.ctx.store,
            &self.ctx.manifests,
            live,
            self.ctx.config.keep_manifests,
            self.ctx.config.time_travel_retention,
            clock,
        )
        .await?;
        Ok(chain
            .into_iter()
            .rev()
            .map(|(_, manifest)| ManifestInfo {
                version: manifest.version,
                created_at_ms: manifest.created_at_ms,
                size_bytes: manifest.splits.iter().map(|split| split.size_bytes).sum(),
                live_doc_count: manifest.live_doc_count,
                lance_version: manifest.lance_version,
            })
            .collect())
    }

    /// The current manifest version and high watermarks of the collection,
    /// from one linearizable read (Ruling 14): read them later with
    /// [`PinnedRead::consistency`].
    pub async fn pin(&self, ns: &str, name_or_alias: &str) -> Result<PinnedRead, ServiceError> {
        let (_, collection) = self.resolve(ns, name_or_alias).await?;
        let (manifest_version, token) = self.reads.pin(&collection).await?;
        Ok(PinnedRead {
            collection: collection.id,
            name: collection.name,
            manifest_version,
            token,
        })
    }

    /// A read-only SQL context over namespace `ns` at `Strong` consistency.
    pub fn sql_context(&self, ns: &str) -> datafusion::prelude::SessionContext {
        self.sql_context_with(ns, ReadConsistency::Strong)
    }

    /// A read-only SQL context over namespace `ns` at `consistency`. Task 10
    /// registers the namespace's catalog; until then the context is empty.
    pub fn sql_context_with(
        &self,
        _ns: &str,
        _consistency: ReadConsistency,
    ) -> datafusion::prelude::SessionContext {
        datafusion::prelude::SessionContext::new()
    }

    /// Stops every tail and the catalog cache's refresh task.
    pub async fn shutdown(&self) {
        self.reads.shutdown().await;
        self.catalog.stop();
    }
}

/// The arguments of one scroll.
struct Scroll<'a> {
    filter: Option<Query>,
    after: Option<PrimaryKey>,
    limit: usize,
    select: &'a Projection,
    consistency: &'a ReadConsistency,
}
