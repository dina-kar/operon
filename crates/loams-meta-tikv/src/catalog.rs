//! The catalog: namespaces, streams, links, collections with their aliases
//! and hot configuration (design §20 §11.2–§11.3). Validation and rejections
//! follow the openraft state machine (`loams-meta/src/state/`) exactly, so
//! both backends answer the conformance suite alike.
//!
//! Reads that a write's outcome depends on, but that the write does not
//! itself overwrite, are locked (`lock_keys`): snapshot isolation otherwise
//! lets two transactions that read each other's keys both commit (a
//! collection created under a name an alias takes at the same time).

use std::collections::BTreeMap;

use loams_common::meta::{
    AliasAction, AliasTargetAction, AliasTargets, ApplyError, COLLECTION_KIND, Collection,
    CollectionHead, HotConfig, IndexEntry, Link, LinkHead, LinkId, MAX_ALIAS_TARGETS,
    MAX_COLLECTION_NAME_LEN, MAX_KEY_LEN, MAX_NAME_LEN, MAX_PARTITIONS, MetaError, MetaResult,
    NameTarget, Namespace, Pointer, Retention, Stream, TargetRef, WalClass, collection_pk_prefix,
    collection_pointer_key, collection_prefix, implicit_name, link_pointer_key,
};
use loams_common::schema::{CollectionSchema, SchemaError};
use loams_common::{CollectionId, NamespaceId, StreamId};
use loams_tikv::{Tikv, Txn, TxnError};

use crate::keys::{self, AliasMap, Head, IdKind};
use crate::log::release_entries;
use crate::{Reader, TikvMeta, decode_all, fatal, load, load_id, rejected};

/// Most options one link may carry.
const MAX_LINK_OPTIONS: usize = 64;
/// Most actions one alias update may carry.
const MAX_ALIAS_ACTIONS: usize = 100;

// ---- Validation (as `loams-meta/src/state/mod.rs`) ----

/// Names are 1..=255 bytes of ASCII letters, digits, `-`, `_` and `.`, and are
/// not `.` or `..`.
pub(crate) fn validate_name(kind: &str, name: &str) -> Result<(), ApplyError> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(ApplyError::InvalidArgument(format!(
            "invalid {kind} name {name:?}"
        )))
    }
}

/// Names starting with `_` belong to implicit objects.
fn refuse_reserved(name: &str) -> Result<(), ApplyError> {
    if name.starts_with('_') {
        return Err(ApplyError::InvalidArgument(
            "names starting with '_' are reserved for implicit objects".to_string(),
        ));
    }
    Ok(())
}

/// Keys (object paths, lease keys, pointer keys) are 1..=1024 bytes.
pub(crate) fn validate_key(kind: &str, key: &str) -> Result<(), ApplyError> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(ApplyError::InvalidArgument(format!(
            "{kind} must be 1..={MAX_KEY_LEN} bytes, got {}",
            key.len()
        )));
    }
    Ok(())
}

/// A stream has 1..=[`MAX_PARTITIONS`] partitions.
fn check_partitions(partitions: u32) -> Result<(), ApplyError> {
    if !(1..=MAX_PARTITIONS).contains(&partitions) {
        return Err(ApplyError::InvalidArgument(format!(
            "partitions must be 1..={MAX_PARTITIONS}, got {partitions}"
        )));
    }
    Ok(())
}

fn schema_message(err: SchemaError) -> String {
    match err {
        SchemaError::Invalid(message) | SchemaError::Incompatible(message) => message,
    }
}

// ---- Lookups shared by reads and writes ----

pub(crate) async fn namespace_exists(
    r: &mut dyn Reader,
    ns: NamespaceId,
) -> Result<bool, TxnError> {
    Ok(r.get(&keys::namespace(ns)).await?.is_some())
}

pub(crate) async fn load_stream(
    r: &mut dyn Reader,
    id: StreamId,
) -> Result<Option<Stream>, TxnError> {
    load(r, "stream", &keys::stream(id)).await
}

async fn load_link(r: &mut dyn Reader, id: LinkId) -> Result<Option<Link>, TxnError> {
    load(r, "link", &keys::link(id)).await
}

pub(crate) async fn load_collection(
    r: &mut dyn Reader,
    id: CollectionId,
) -> Result<Option<Collection>, TxnError> {
    load(r, "collection", &keys::collection(id)).await
}

/// The collection named `name` (not an alias) in `ns`.
async fn collection_by_name(
    r: &mut dyn Reader,
    ns: NamespaceId,
    name: &str,
) -> Result<Option<Collection>, TxnError> {
    match load_id(r, "collection name", &keys::collection_name(ns, name)).await? {
        Some(id) => load_collection(r, CollectionId(id)).await,
        None => Ok(None),
    }
}

async fn load_aliases(r: &mut dyn Reader, ns: NamespaceId) -> Result<AliasMap, TxnError> {
    Ok(load(r, "aliases", &keys::aliases(ns))
        .await?
        .unwrap_or_default())
}

/// The records named by the ids stored under `names` (a name prefix), in id
/// order.
async fn records_by_names<T: serde::de::DeserializeOwned>(
    r: &mut dyn Reader,
    what: &str,
    names: &[u8],
    record_key: impl Fn(u64) -> Vec<u8>,
) -> Result<Vec<T>, TxnError> {
    let pairs = r.scan_prefix(names).await?;
    let mut record_keys = Vec::with_capacity(pairs.len());
    for (_, v) in &pairs {
        record_keys.push(record_key(keys::decode_u64(what, v).map_err(fatal)?));
    }
    // Sorted by key, and record keys sort by id.
    let records = r.batch_get(record_keys).await?;
    decode_all(what, &records)
}

pub(crate) async fn list_streams(
    r: &mut dyn Reader,
    ns: Option<NamespaceId>,
) -> Result<Vec<Stream>, TxnError> {
    match ns {
        Some(ns) => {
            records_by_names(r, "stream", &keys::stream_names(ns), |id| {
                keys::stream(StreamId(id))
            })
            .await
        }
        None => decode_all("stream", &r.scan_prefix(keys::STREAMS).await?),
    }
}

async fn list_links(r: &mut dyn Reader, ns: Option<NamespaceId>) -> Result<Vec<Link>, TxnError> {
    match ns {
        Some(ns) => {
            records_by_names(r, "link", &keys::link_names(ns), |id| {
                keys::link(LinkId(id))
            })
            .await
        }
        None => decode_all("link", &r.scan_prefix(keys::LINKS).await?),
    }
}

pub(crate) async fn list_collections(
    r: &mut dyn Reader,
    ns: Option<NamespaceId>,
) -> Result<Vec<Collection>, TxnError> {
    match ns {
        Some(ns) => {
            records_by_names(r, "collection", &keys::collection_names(ns), |id| {
                keys::collection(CollectionId(id))
            })
            .await
        }
        None => decode_all("collection", &r.scan_prefix(keys::COLLECTIONS).await?),
    }
}

/// The heads of `stream`'s partitions `0..partitions`; an absent head is an
/// empty partition.
pub(crate) async fn load_heads(
    r: &mut dyn Reader,
    stream: StreamId,
    partitions: u32,
) -> Result<Vec<Head>, TxnError> {
    let mut heads = vec![Head::default(); partitions as usize];
    for (key, value) in r.scan_prefix(&keys::heads(stream)).await? {
        let Some(p) = keys::head_partition(&key, stream) else {
            continue;
        };
        if let Some(slot) = heads.get_mut(p as usize) {
            *slot = keys::decode("head", &value).map_err(fatal)?;
        }
    }
    Ok(heads)
}

async fn collection_head(
    r: &mut dyn Reader,
    collection: Collection,
    clock_ms: u64,
) -> Result<CollectionHead, TxnError> {
    let pointer = load(
        r,
        "pointer",
        &keys::pointer(collection.namespace, &collection_pointer_key(collection.id)),
    )
    .await?;
    let heads = load_heads(r, collection.stream, collection.partitions).await?;
    Ok(CollectionHead {
        pointer,
        log_start_offsets: heads.iter().map(|h| h.log_start).collect(),
        high_watermarks: heads.iter().map(|h| h.next).collect(),
        clock_ms,
        collection,
    })
}

impl TikvMeta {
    // ---- Namespaces ----

    pub(crate) async fn create_namespace_impl(&self, name: &str) -> MetaResult<NamespaceId> {
        validate_name("namespace", name)?;
        let id = NamespaceId(self.next_id(IdKind::Namespace).await?);
        let name = name.to_string();
        self.write_plain("meta.create_namespace", move |txn| {
            let name = name.clone();
            Box::pin(async move {
                let name_key = keys::namespace_name(&name);
                if let Some(existing) = load_id(txn, "namespace name", &name_key).await? {
                    return Ok(Err(ApplyError::NamespaceExists(NamespaceId(existing))));
                }
                txn.put(&name_key, keys::encode_u64(id.0)).await?;
                txn.put(&keys::namespace(id), keys::encode(&Namespace { id, name }))
                    .await?;
                Ok(Ok(id))
            })
        })
        .await
    }

    pub(crate) async fn namespace_by_name_impl(&self, name: &str) -> MetaResult<Option<Namespace>> {
        let name = name.to_string();
        self.read(move |snap| {
            let name = name.clone();
            Box::pin(async move {
                Ok(Ok(
                    match load_id(snap, "namespace name", &keys::namespace_name(&name)).await? {
                        Some(id) => {
                            load(snap, "namespace", &keys::namespace(NamespaceId(id))).await?
                        }
                        None => None,
                    },
                ))
            })
        })
        .await
    }

    pub(crate) async fn namespaces_impl(&self) -> MetaResult<Vec<Namespace>> {
        self.read(|snap| {
            Box::pin(async move {
                let pairs = snap.scan_prefix(keys::NAMESPACES).await?;
                Ok(Ok(decode_all("namespace", &pairs)?))
            })
        })
        .await
    }

    // ---- Streams ----

    pub(crate) async fn create_stream_impl(
        &self,
        ns: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> MetaResult<StreamId> {
        validate_name("stream", name)?;
        refuse_reserved(name)?;
        check_partitions(partitions)?;
        let id = StreamId(self.next_id(IdKind::Stream).await?);
        let name = name.to_string();
        self.write_plain("meta.create_stream", move |txn| {
            let name = name.clone();
            Box::pin(async move {
                if !namespace_exists(txn, ns).await? {
                    return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                }
                let name_key = keys::stream_name(ns, &name);
                if let Some(existing) = load_id(txn, "stream name", &name_key).await? {
                    return Ok(Err(ApplyError::StreamExists(StreamId(existing))));
                }
                put_stream(
                    txn,
                    &Stream {
                        id,
                        namespace: ns,
                        name,
                        partitions,
                        class,
                        retention,
                    },
                )
                .await?;
                Ok(Ok(id))
            })
        })
        .await
    }

    pub(crate) async fn set_retention_impl(
        &self,
        stream: StreamId,
        retention: Retention,
    ) -> MetaResult<()> {
        self.write_plain("meta.set_retention", move |txn| {
            Box::pin(async move {
                let Some(mut record) = load_stream(txn, stream).await? else {
                    return Ok(Err(ApplyError::StreamNotFound(stream)));
                };
                // A collection's implicit stream is trimmed only by its
                // collection (M1.1 Ruling 12).
                if record.name.starts_with('_') {
                    return Ok(Err(ApplyError::InvalidArgument(format!(
                        "stream {stream} belongs to a collection; its retention cannot be set"
                    ))));
                }
                if record.retention != retention {
                    record.retention = retention;
                    txn.put(&keys::stream(stream), keys::encode(&record))
                        .await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(crate) async fn stream_impl(&self, id: StreamId) -> MetaResult<Option<Stream>> {
        self.read(move |snap| Box::pin(async move { Ok(Ok(load_stream(snap, id).await?)) }))
            .await
    }

    pub(crate) async fn stream_by_name_impl(
        &self,
        ns: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Stream>> {
        let name = name.to_string();
        self.read(move |snap| {
            let name = name.clone();
            Box::pin(async move {
                Ok(Ok(
                    match load_id(snap, "stream name", &keys::stream_name(ns, &name)).await? {
                        Some(id) => load_stream(snap, StreamId(id)).await?,
                        None => None,
                    },
                ))
            })
        })
        .await
    }

    pub(crate) async fn streams_impl(&self, ns: Option<NamespaceId>) -> MetaResult<Vec<Stream>> {
        self.read(move |snap| Box::pin(async move { Ok(Ok(list_streams(snap, ns).await?)) }))
            .await
    }

    // ---- Links ----

    pub(crate) async fn create_link_impl(
        &self,
        ns: NamespaceId,
        name: &str,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> MetaResult<LinkId> {
        validate_name("link", name)?;
        refuse_reserved(name)?;
        let id = LinkId(self.next_id(IdKind::Link).await?);
        let name = name.to_string();
        self.write_plain("meta.create_link", move |txn| {
            let (name, target, options) = (name.clone(), target.clone(), options.clone());
            Box::pin(async move {
                if !namespace_exists(txn, ns).await? {
                    return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                }
                let name_key = keys::link_name(ns, &name);
                if let Some(existing) = load_id(txn, "link name", &name_key).await? {
                    return Ok(Err(ApplyError::LinkExists(LinkId(existing))));
                }
                match load_stream(txn, source).await? {
                    None => return Ok(Err(ApplyError::StreamNotFound(source))),
                    Some(stream) if stream.namespace != ns => {
                        return Ok(Err(ApplyError::InvalidArgument(format!(
                            "stream {source} is not in namespace {ns}"
                        ))));
                    }
                    Some(stream) if stream.name.starts_with('_') => {
                        return Ok(Err(ApplyError::InvalidArgument(format!(
                            "stream {source} belongs to a collection and cannot be a link source"
                        ))));
                    }
                    Some(_) => {}
                }
                if let Err(e) = validate_link_target(&target, &options) {
                    return Ok(Err(e));
                }
                put_link(
                    txn,
                    &Link {
                        id,
                        namespace: ns,
                        name,
                        source,
                        target,
                        options,
                    },
                )
                .await?;
                Ok(Ok(id))
            })
        })
        .await
    }

    pub(crate) async fn link_by_name_impl(
        &self,
        ns: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<Link>> {
        let name = name.to_string();
        self.read(move |snap| {
            let name = name.clone();
            Box::pin(async move {
                Ok(Ok(
                    match load_id(snap, "link name", &keys::link_name(ns, &name)).await? {
                        Some(id) => load_link(snap, LinkId(id)).await?,
                        None => None,
                    },
                ))
            })
        })
        .await
    }

    pub(crate) async fn links_impl(&self, ns: Option<NamespaceId>) -> MetaResult<Vec<Link>> {
        self.read(move |snap| Box::pin(async move { Ok(Ok(list_links(snap, ns).await?)) }))
            .await
    }

    pub(crate) async fn links_with_pointers_impl(
        &self,
        ns: NamespaceId,
    ) -> MetaResult<Vec<LinkHead>> {
        self.read(move |snap| {
            Box::pin(async move {
                let links = list_links(snap, Some(ns)).await?;
                let mut heads = Vec::with_capacity(links.len());
                for link in links {
                    let pointer: Option<Pointer> = load(
                        snap,
                        "pointer",
                        &keys::pointer(ns, &link_pointer_key(link.id)),
                    )
                    .await?;
                    heads.push(LinkHead { link, pointer });
                }
                Ok(Ok(heads))
            })
        })
        .await
    }

    // ---- Collections ----

    pub(crate) async fn create_collection_impl(
        &self,
        ns: NamespaceId,
        name: &str,
        schema: CollectionSchema,
        partitions: u32,
    ) -> MetaResult<(CollectionId, StreamId, LinkId)> {
        validate_name("collection", name)?;
        if name.len() > MAX_COLLECTION_NAME_LEN {
            return rejected(ApplyError::InvalidArgument(format!(
                "a collection name is at most {MAX_COLLECTION_NAME_LEN} bytes, got {}",
                name.len()
            )));
        }
        refuse_reserved(name)?;
        check_partitions(partitions)?;
        schema
            .validate()
            .map_err(|e| ApplyError::InvalidArgument(e.to_string()))?;
        if schema.version != 1 {
            return rejected(ApplyError::InvalidArgument(format!(
                "a new collection's schema is at version 1, got {}",
                schema.version
            )));
        }
        let id = CollectionId(self.next_id(IdKind::Collection).await?);
        let stream = StreamId(self.next_id(IdKind::Stream).await?);
        let link = LinkId(self.next_id(IdKind::Link).await?);
        let name = name.to_string();
        let (result, unknown) = self
            .write("meta.create_collection", move |txn| {
                let (name, schema) = (name.clone(), schema.clone());
                Box::pin(async move {
                    if !namespace_exists(txn, ns).await? {
                        return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                    }
                    if let Some(existing) = collection_by_name(txn, ns, &name).await? {
                        return Ok(Err(
                            if existing.schema == schema && existing.partitions == partitions {
                                ApplyError::CollectionExists(existing.id)
                            } else {
                                ApplyError::NameTaken(name)
                            },
                        ));
                    }
                    let aliases_key = keys::aliases(ns);
                    if load_aliases(txn, ns).await?.contains(&name) {
                        return Ok(Err(ApplyError::NameTaken(name)));
                    }
                    txn.lock_keys([aliases_key]).await?;
                    let implicit = implicit_name(&name, id);
                    put_stream(
                        txn,
                        &Stream {
                            id: stream,
                            namespace: ns,
                            name: implicit.clone(),
                            partitions,
                            class: WalClass::Standard,
                            retention: Retention::default(),
                        },
                    )
                    .await?;
                    put_link(
                        txn,
                        &Link {
                            id: link,
                            namespace: ns,
                            name: implicit,
                            source: stream,
                            target: TargetRef {
                                kind: COLLECTION_KIND.to_string(),
                                name: name.clone(),
                            },
                            options: BTreeMap::new(),
                        },
                    )
                    .await?;
                    txn.put(&keys::collection_name(ns, &name), keys::encode_u64(id.0))
                        .await?;
                    let record = Collection {
                        id,
                        namespace: ns,
                        name,
                        schema,
                        partitions,
                        stream,
                        link,
                    };
                    txn.put(&keys::collection(id), keys::encode(&record))
                        .await?;
                    Ok(Ok((id, stream, link)))
                })
            })
            .await;
        match result {
            // A retry after an attempt of unknown outcome that finds the
            // collection returns the first attempt's ids (the trait's rule).
            Err(MetaError::Rejected(ApplyError::CollectionExists(existing))) if unknown => {
                let found = self.collection_impl(existing).await?;
                found
                    .map(|c| (c.id, c.stream, c.link))
                    .ok_or(MetaError::Rejected(ApplyError::CollectionExists(existing)))
            }
            other => other,
        }
    }

    pub(crate) async fn drop_collection_impl(
        &self,
        ns: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<CollectionId>> {
        self.reach_now().await;
        let name = name.to_string();
        let dropped = self
            .write_plain("meta.drop_collection", move |txn| {
                let name = name.clone();
                Box::pin(async move {
                    let now_ms = Tikv::physical_ms(&txn.start_ts());
                    let name_key = keys::collection_name(ns, &name);
                    let Some(id) = load_id(txn, "collection name", &name_key).await? else {
                        return Ok(Ok(None));
                    };
                    let id = CollectionId(id);
                    let Some(collection) = load_collection(txn, id).await? else {
                        return Ok(Ok(None));
                    };
                    // Rule 3 of M1.5 Task 0a: the collection leaves every alias,
                    // and each changed alias goes to its canonical map.
                    let mut aliases = load_aliases(txn, ns).await?;
                    let before = aliases.clone();
                    aliases.aliases.retain(|_, target| *target != id);
                    let changed: Vec<String> = aliases
                        .targets
                        .iter()
                        .filter(|(_, t)| t.members.contains_key(&id))
                        .map(|(alias, _)| alias.clone())
                        .collect();
                    for alias in changed {
                        let mut members = aliases.members(&alias);
                        members.remove(&id);
                        aliases.put(alias, members);
                    }
                    if aliases != before {
                        aliases.version += 1;
                        txn.put(&keys::aliases(ns), keys::encode(&aliases)).await?;
                    }
                    txn.delete(&keys::hot(id)).await?;
                    // The implicit stream, its heads and index entries. Every
                    // partition's head is deleted, present or not, so a
                    // concurrent first commit into a partition (which creates its
                    // head under a pessimistic lock) conflicts with the drop
                    // instead of leaving entries behind it (row T5-3).
                    let implicit = implicit_name(&collection.name, id);
                    txn.delete(&keys::stream(collection.stream)).await?;
                    txn.delete(&keys::stream_name(ns, &implicit)).await?;
                    for p in 0..collection.partitions {
                        txn.delete(&keys::head(collection.stream, p)).await?;
                    }
                    let entries = txn
                        .scan_prefix(&keys::index_entries(collection.stream))
                        .await?;
                    let mut released = Vec::with_capacity(entries.len());
                    for (key, value) in entries {
                        let entry: IndexEntry =
                            keys::decode("index entry", &value).map_err(fatal)?;
                        txn.delete(&key).await?;
                        released.push(entry);
                    }
                    release_entries(txn, &released, now_ms).await?;
                    // The implicit link and its pointer, the manifest pointer,
                    // the collection.
                    txn.delete(&keys::link(collection.link)).await?;
                    txn.delete(&keys::link_name(ns, &implicit)).await?;
                    txn.delete(&keys::pointer(ns, &link_pointer_key(collection.link)))
                        .await?;
                    txn.delete(&keys::pointer(ns, &collection_pointer_key(id)))
                        .await?;
                    txn.delete(&keys::collection(id)).await?;
                    txn.delete(&name_key).await?;
                    for prefix in [collection_prefix(ns, id), collection_pk_prefix(ns, id)] {
                        txn.put(&keys::retired(&prefix), keys::encode_u64(now_ms))
                            .await?;
                    }
                    Ok(Ok(Some((id, collection.stream))))
                })
            })
            .await?;
        let Some((id, stream)) = dropped else {
            return Ok(None);
        };
        // The stream's ledger entries go in pages after the drop commits: a
        // busy stream holds too many for the drop's own transaction. A
        // concurrent claim conflicts with the drop through the stream
        // record and then finds no stream; entries this misses (an error here is
        // ignored) lapse and are pruned.
        let _ = self.purge_idempotency_stream(stream).await;
        Ok(Some(id))
    }

    pub(crate) async fn update_collection_schema_impl(
        &self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> MetaResult<u64> {
        self.write_plain("meta.update_collection_schema", move |txn| {
            let schema = schema.clone();
            Box::pin(async move {
                let Some(mut record) = load_collection(txn, collection).await? else {
                    return Ok(Err(ApplyError::CollectionNotFound(collection)));
                };
                if let Err(e) = schema.validate() {
                    return Ok(Err(ApplyError::InvalidArgument(e.to_string())));
                }
                let current = &record.schema;
                // A retry of an update that was applied.
                if expected_version.checked_add(1) == Some(current.version)
                    && current.same_ignoring_version(&schema)
                {
                    return Ok(Ok(current.version));
                }
                if current.version != expected_version {
                    return Ok(Err(ApplyError::SchemaVersionMismatch {
                        collection,
                        current: current.version,
                    }));
                }
                if let Err(e) = current.check_additive(&schema) {
                    return Ok(Err(ApplyError::IncompatibleSchema(schema_message(e))));
                }
                let Some(version) = expected_version.checked_add(1) else {
                    return Ok(Err(ApplyError::InvalidArgument(
                        "the schema version cannot grow past u64::MAX".to_string(),
                    )));
                };
                record.schema = CollectionSchema { version, ..schema };
                txn.put(&keys::collection(collection), keys::encode(&record))
                    .await?;
                Ok(Ok(version))
            })
        })
        .await
    }

    pub(crate) async fn update_aliases_impl(
        &self,
        ns: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> MetaResult<()> {
        self.write_plain("meta.update_aliases", move |txn| {
            let actions = actions.clone();
            Box::pin(async move {
                if !namespace_exists(txn, ns).await? {
                    return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                }
                if !(1..=MAX_ALIAS_ACTIONS).contains(&actions.len()) {
                    return Ok(Err(ApplyError::InvalidArgument(format!(
                        "an alias update takes 1..={MAX_ALIAS_ACTIONS} actions, got {}",
                        actions.len()
                    ))));
                }
                // The actions' effect, applied once every action succeeded.
                let mut changes: BTreeMap<String, Option<CollectionId>> = BTreeMap::new();
                let mut read = Vec::new();
                for action in actions {
                    match action {
                        AliasAction::Create { alias, collection } => {
                            if let Err(e) = validate_name("alias", &alias)
                                .and_then(|()| refuse_reserved(&alias))
                            {
                                return Ok(Err(e));
                            }
                            let alias_key = keys::collection_name(ns, &alias);
                            if txn.get(&alias_key).await?.is_some() {
                                return Ok(Err(ApplyError::NameTaken(alias)));
                            }
                            let target_key = keys::collection_name(ns, &collection);
                            let Some(id) = load_id(txn, "collection name", &target_key).await?
                            else {
                                return Ok(Err(ApplyError::UnknownCollection(collection)));
                            };
                            read.push(alias_key);
                            read.push(target_key);
                            changes.insert(alias, Some(CollectionId(id)));
                        }
                        AliasAction::Delete { alias } => {
                            changes.insert(alias, None);
                        }
                    }
                }
                let mut map = load_aliases(txn, ns).await?;
                let before = map.clone();
                for (alias, target) in changes {
                    // Either map may hold the alias (M1.5 Task 0a rule 3):
                    // `Create` re-points it to exactly one collection,
                    // `Delete` removes it.
                    map.put(alias, target.map(|id| (id, None)).into_iter().collect());
                }
                // A collection created or dropped under a name this update
                // read must conflict with it.
                txn.lock_keys(read).await?;
                if map != before {
                    map.version += 1;
                    txn.put(&keys::aliases(ns), keys::encode(&map)).await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    /// M1.5 Task 0a rule 2: alias-target actions over a working copy of
    /// each touched alias, checked, then applied at once (as
    /// `loams-meta/src/state/collections.rs`).
    pub(crate) async fn update_alias_targets_impl(
        &self,
        ns: NamespaceId,
        actions: Vec<AliasTargetAction>,
    ) -> MetaResult<()> {
        self.write_plain("meta.update_alias_targets", move |txn| {
            let actions = actions.clone();
            Box::pin(async move {
                if !namespace_exists(txn, ns).await? {
                    return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                }
                if !(1..=MAX_ALIAS_ACTIONS).contains(&actions.len()) {
                    return Ok(Err(ApplyError::InvalidArgument(format!(
                        "an alias update takes 1..={MAX_ALIAS_ACTIONS} actions, got {}",
                        actions.len()
                    ))));
                }
                let mut map = load_aliases(txn, ns).await?;
                let mut working: BTreeMap<String, BTreeMap<CollectionId, Option<bool>>> =
                    BTreeMap::new();
                let mut read = Vec::new();
                for action in actions {
                    match action {
                        AliasTargetAction::Add {
                            alias,
                            collection,
                            is_write_index,
                        } => {
                            if let Err(e) = validate_name("alias", &alias)
                                .and_then(|()| refuse_reserved(&alias))
                            {
                                return Ok(Err(e));
                            }
                            let alias_key = keys::collection_name(ns, &alias);
                            if txn.get(&alias_key).await?.is_some() {
                                return Ok(Err(ApplyError::NameTaken(alias)));
                            }
                            let target_key = keys::collection_name(ns, &collection);
                            let Some(id) = load_id(txn, "collection name", &target_key).await?
                            else {
                                return Ok(Err(ApplyError::UnknownCollection(collection)));
                            };
                            read.push(alias_key);
                            read.push(target_key);
                            working
                                .entry(alias)
                                .or_insert_with_key(|a| map.members(a))
                                .insert(CollectionId(id), is_write_index);
                        }
                        AliasTargetAction::Remove { alias, collection } => {
                            let target_key = keys::collection_name(ns, &collection);
                            let id = load_id(txn, "collection name", &target_key).await?;
                            read.push(target_key);
                            let members = working
                                .entry(alias)
                                .or_insert_with_key(|a| map.members(a));
                            if let Some(id) = id {
                                members.remove(&CollectionId(id));
                            }
                        }
                        AliasTargetAction::RemoveAlias { alias } => {
                            working
                                .entry(alias)
                                .or_insert_with_key(|a| map.members(a))
                                .clear();
                        }
                    }
                }
                for (alias, members) in &working {
                    if members.len() > MAX_ALIAS_TARGETS {
                        return Ok(Err(ApplyError::InvalidArgument(format!(
                            "alias [{alias}] would name {} collections; the limit is {MAX_ALIAS_TARGETS}",
                            members.len()
                        ))));
                    }
                    let mut writers = Vec::new();
                    for (&id, _) in members.iter().filter(|(_, w)| **w == Some(true)) {
                        if let Some(c) = load_collection(txn, id).await? {
                            writers.push(c.name);
                        }
                    }
                    if writers.len() > 1 {
                        writers.sort_unstable();
                        return Ok(Err(ApplyError::InvalidArgument(format!(
                            "alias [{alias}] has more than one write index [{}]",
                            writers.join(",")
                        ))));
                    }
                }
                let before = map.clone();
                for (alias, members) in working {
                    map.put(alias, members);
                }
                // A collection created or dropped under a name this update
                // read must conflict with it.
                txn.lock_keys(read).await?;
                if map != before {
                    map.version += 1;
                    txn.put(&keys::aliases(ns), keys::encode(&map)).await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(crate) async fn alias_targets_impl(
        &self,
        ns: NamespaceId,
    ) -> MetaResult<Vec<(String, AliasTargets)>> {
        self.read(move |snap| {
            Box::pin(async move { Ok(Ok(load_aliases(snap, ns).await?.all_targets())) })
        })
        .await
    }

    /// What `name` names in `ns`: a collection, or an alias with its member
    /// records, from one snapshot; `None` if neither.
    pub(crate) async fn resolve_name_impl(
        &self,
        ns: NamespaceId,
        name: &str,
    ) -> MetaResult<Option<NameTarget>> {
        let name = name.to_string();
        self.read(move |snap| {
            let name = name.clone();
            Box::pin(async move {
                if let Some(found) = collection_by_name(snap, ns, &name).await? {
                    return Ok(Ok(Some(NameTarget::Collection(found))));
                }
                let members = load_aliases(snap, ns).await?.members(&name);
                if members.is_empty() {
                    return Ok(Ok(None));
                }
                let targets = AliasTargets { members };
                let mut records = Vec::with_capacity(targets.members.len());
                for (&id, &w) in &targets.members {
                    if let Some(c) = load_collection(snap, id).await? {
                        records.push((c, w));
                    }
                }
                Ok(Ok(Some(NameTarget::Alias {
                    write_target: targets.write_target(),
                    members: records,
                })))
            })
        })
        .await
    }

    pub(crate) async fn collection_impl(&self, id: CollectionId) -> MetaResult<Option<Collection>> {
        self.read(move |snap| Box::pin(async move { Ok(Ok(load_collection(snap, id).await?)) }))
            .await
    }

    pub(crate) async fn resolve_collection_impl(
        &self,
        ns: NamespaceId,
        name_or_alias: &str,
    ) -> MetaResult<Option<Collection>> {
        let name = name_or_alias.to_string();
        self.read(move |snap| {
            let name = name.clone();
            Box::pin(async move {
                if let Some(found) = collection_by_name(snap, ns, &name).await? {
                    return Ok(Ok(Some(found)));
                }
                // Through an alias with exactly one member; `None` for an
                // alias with several (M1.5 Task 0a).
                let members = load_aliases(snap, ns).await?.members(&name);
                let mut ids = members.keys();
                Ok(Ok(match (ids.next(), ids.next()) {
                    (Some(&id), None) => load_collection(snap, id).await?,
                    _ => None,
                }))
            })
        })
        .await
    }

    pub(crate) async fn collection_for_link_impl(
        &self,
        link: LinkId,
    ) -> MetaResult<Option<Collection>> {
        self.read(move |snap| {
            Box::pin(async move {
                let Some(link) = load_link(snap, link).await? else {
                    return Ok(Ok(None));
                };
                if link.target.kind != COLLECTION_KIND {
                    return Ok(Ok(None));
                }
                Ok(Ok(collection_by_name(
                    snap,
                    link.namespace,
                    &link.target.name,
                )
                .await?
                .filter(|c| c.link == link.id)))
            })
        })
        .await
    }

    pub(crate) async fn collections_impl(
        &self,
        ns: Option<NamespaceId>,
    ) -> MetaResult<Vec<Collection>> {
        self.read(move |snap| Box::pin(async move { Ok(Ok(list_collections(snap, ns).await?)) }))
            .await
    }

    pub(crate) async fn aliases_impl(
        &self,
        ns: NamespaceId,
    ) -> MetaResult<Vec<(String, CollectionId)>> {
        self.read(move |snap| {
            Box::pin(async move {
                // One pair per member, by alias name and then collection id.
                let mut pairs: Vec<(String, CollectionId)> = load_aliases(snap, ns)
                    .await?
                    .all_targets()
                    .into_iter()
                    .flat_map(|(alias, t)| t.members.into_keys().map(move |id| (alias.clone(), id)))
                    .collect();
                pairs.sort_unstable();
                Ok(Ok(pairs))
            })
        })
        .await
    }

    pub(crate) async fn collection_head_impl(
        &self,
        id: CollectionId,
    ) -> MetaResult<Option<CollectionHead>> {
        self.read(move |snap| {
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                Ok(Ok(match load_collection(snap, id).await? {
                    Some(c) => Some(collection_head(snap, c, clock_ms).await?),
                    None => None,
                }))
            })
        })
        .await
    }

    pub(crate) async fn collection_heads_impl(
        &self,
        ns: Option<NamespaceId>,
    ) -> MetaResult<Vec<CollectionHead>> {
        self.read(move |snap| {
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                let collections = list_collections(snap, ns).await?;
                let mut heads = Vec::with_capacity(collections.len());
                for c in collections {
                    heads.push(collection_head(snap, c, clock_ms).await?);
                }
                Ok(Ok(heads))
            })
        })
        .await
    }

    pub(crate) async fn set_collection_hot_impl(
        &self,
        ns: NamespaceId,
        collection: CollectionId,
        hot: HotConfig,
    ) -> MetaResult<()> {
        self.write_plain("meta.set_collection_hot", move |txn| {
            Box::pin(async move {
                let record_key = keys::collection(collection);
                match load_collection(txn, collection).await? {
                    Some(c) if c.namespace == ns => {}
                    _ => return Ok(Err(ApplyError::CollectionNotFound(collection))),
                }
                // A concurrent drop must conflict, or it could leave H/ behind.
                txn.lock_keys([record_key]).await?;
                let hot_key = keys::hot(collection);
                if hot.any() {
                    txn.put(&hot_key, keys::encode(&hot)).await?;
                } else {
                    txn.delete(&hot_key).await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(crate) async fn collection_hot_impl(
        &self,
        ns: NamespaceId,
        collection: CollectionId,
    ) -> MetaResult<HotConfig> {
        self.read(move |snap| {
            Box::pin(async move {
                match load_collection(snap, collection).await? {
                    Some(c) if c.namespace == ns => {}
                    _ => return Ok(Err(ApplyError::CollectionNotFound(collection))),
                }
                Ok(Ok(load(snap, "hot configuration", &keys::hot(collection))
                    .await?
                    .unwrap_or_default()))
            })
        })
        .await
    }
}

/// Writes a stream's name and record.
async fn put_stream(txn: &mut Txn, stream: &Stream) -> Result<(), TxnError> {
    txn.put(
        &keys::stream_name(stream.namespace, &stream.name),
        keys::encode_u64(stream.id.0),
    )
    .await?;
    txn.put(&keys::stream(stream.id), keys::encode(stream))
        .await
}

/// Writes a link's name and record.
async fn put_link(txn: &mut Txn, link: &Link) -> Result<(), TxnError> {
    txn.put(
        &keys::link_name(link.namespace, &link.name),
        keys::encode_u64(link.id.0),
    )
    .await?;
    txn.put(&keys::link(link.id), keys::encode(link)).await
}

fn validate_link_target(
    target: &TargetRef,
    options: &BTreeMap<String, String>,
) -> Result<(), ApplyError> {
    validate_name("link target kind", &target.kind)?;
    validate_name("link target name", &target.name)?;
    if target.kind == COLLECTION_KIND {
        return Err(ApplyError::InvalidArgument(
            "a collection's link is created with the collection".to_string(),
        ));
    }
    if options.len() > MAX_LINK_OPTIONS
        || options
            .iter()
            .any(|(k, v)| k.is_empty() || k.len() > MAX_KEY_LEN || v.len() > MAX_KEY_LEN)
    {
        return Err(ApplyError::InvalidArgument(format!(
            "a link takes at most {MAX_LINK_OPTIONS} options, with keys of 1..={MAX_KEY_LEN} \
             bytes and values of at most {MAX_KEY_LEN} bytes"
        )));
    }
    Ok(())
}
