//! Folding records into the tail (plan M1.2 Task 3 rules 3, 4, 6 and 10).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use operon_collection::{
    CollectionContext, CollectionError, CollectionManifest, CollectionSnapshot, DocOp, Document,
    PrimaryKey, check_document, decode, decode_pk_delta, fold, partition_of,
};
use operon_common::NamespaceId;
use operon_common::meta::Collection;
use operon_log::{FetchRequest, LogError, LogReader, OffsetRecord};
use operon_store::StoreError;

use super::index::{TailIndex, next_row_id};
use super::snapshot::TailSnapshot;
use super::{TailConfig, TailDoc, TailError};

/// The durable state of keys in M_T: row id and document, `None` when
/// absent.
pub(crate) type Durable = HashMap<PrimaryKey, Option<(u64, Document)>>;

/// One fetched record: its offset and its op, `None` when it does not decode.
pub(crate) struct Decoded {
    pub offset: u64,
    pub op: Option<DocOp>,
}

pub(crate) fn decode_records(records: &[OffsetRecord]) -> Vec<Decoded> {
    records
        .iter()
        .map(|record| Decoded {
            offset: record.offset,
            op: decode(&record.record).ok(),
        })
        .collect()
}

fn not_found(err: &CollectionError) -> bool {
    matches!(err, CollectionError::Store(StoreError::NotFound { .. }))
}

/// Looks up `keys` in `durable` (rule 4), in chunks of `batch`; every key is
/// absent without a snapshot or before the first Lance version.
pub(crate) async fn resolve(
    durable: Option<&CollectionSnapshot>,
    keys: Vec<PrimaryKey>,
    batch: usize,
) -> Result<Durable, TailError> {
    let mut out: Durable = HashMap::with_capacity(keys.len());
    let Some(snapshot) = durable.filter(|s| s.manifest().lance_version != 0) else {
        out.extend(keys.into_iter().map(|key| (key, None)));
        return Ok(out);
    };
    for chunk in keys.chunks(batch.max(1)) {
        let found = snapshot.get_by_pk(chunk).await?;
        for (key, stored) in chunk.iter().zip(found) {
            let value = stored.map(|stored| {
                let doc = Document {
                    pk: stored.pk,
                    source: stored.source,
                    vectors: stored.vectors,
                    sparse_vectors: stored.sparse_vectors,
                };
                (stored.row_id, doc)
            });
            out.insert(key.clone(), value);
        }
    }
    Ok(out)
}

impl TailIndex {
    /// The keys of `records` (on their home partition `partition`) that
    /// have no overlay entry, each once.
    pub fn unresolved(
        &self,
        partitions: u32,
        partition: u32,
        records: &[Decoded],
    ) -> Vec<PrimaryKey> {
        let mut seen = HashSet::new();
        records
            .iter()
            .filter_map(|record| record.op.as_ref())
            .map(DocOp::pk)
            .filter(|pk| partition_of(pk, partitions) == partition)
            .filter(|pk| !self.shadow_of.contains_key(*pk))
            .filter(|pk| seen.insert((*pk).clone()))
            .cloned()
            .collect()
    }

    /// Applies one record of `partition` (rule 3); `durable` holds every key
    /// of the batch that had no overlay entry.
    pub fn apply(
        &mut self,
        partitions: u32,
        partition: u32,
        record: Decoded,
        durable: &Durable,
    ) -> Result<(), TailError> {
        let offset = record.offset;
        self.head.insert(partition, offset + 1);
        // Undecodable, or on another partition than its key's: the link
        // dead-letters it too.
        let Some(op) = record.op else {
            return Ok(());
        };
        if partition_of(op.pk(), partitions) != partition {
            return Ok(());
        }
        let pk = op.pk().clone();
        let (before, durable_row) = match self.overlay_state(&pk) {
            Some(state) => (state, None),
            None => match durable.get(&pk) {
                Some(Some((row_id, doc))) => (Some(doc.clone()), Some(*row_id)),
                _ => (None, None),
            },
        };
        let in_overlay = self.shadow_of.contains_key(&pk);
        let after = fold(before.clone(), [&op]);
        let extracted = match &after {
            // A result that breaks the schema is dead-lettered by the link.
            Some(doc) => match check_document(&self.generation.check, doc) {
                Ok(extracted) => Some(extracted),
                Err(_) => return Ok(()),
            },
            None => None,
        };
        // Not a write: the link writes no row for it either (P32).
        if !matches!(op, DocOp::Upsert(_)) && after == before {
            return Ok(());
        }
        if in_overlay
            && let Some(previous) = self.generation.latest_entry(&pk)
            && previous.doc.is_some()
        {
            self.live_now.remove(previous.row_id);
        }
        let row_id = next_row_id();
        if after.is_some() {
            self.live_now.insert(row_id);
        }
        self.generation.push(
            TailDoc {
                row_id,
                pk: pk.clone(),
                partition,
                offset,
                doc: after.map(Arc::new),
            },
            extracted.as_ref(),
        )?;
        if !in_overlay {
            if let Some(row) = durable_row {
                self.shadow_now.insert(row);
            }
            self.shadow_of.insert(pk, durable_row);
        }
        Ok(())
    }

    /// Resolves the batch's new keys against `durable` and applies every
    /// record of `partition` at or after its head.
    pub async fn apply_batch(
        &mut self,
        partitions: u32,
        partition: u32,
        records: &[OffsetRecord],
        durable: Option<&CollectionSnapshot>,
        resolve_batch: usize,
    ) -> Result<(), TailError> {
        let from = self.head.get(&partition).copied().unwrap_or(0);
        let records: Vec<OffsetRecord> = records
            .iter()
            .filter(|record| record.offset >= from)
            .cloned()
            .collect();
        let decoded = decode_records(&records);
        let keys = self.unresolved(partitions, partition, &decoded);
        let resolved = resolve(durable, keys, resolve_batch).await?;
        for record in decoded {
            self.apply(partitions, partition, record, &resolved)?;
        }
        Ok(())
    }

    /// Adopts manifest `manifest` at `path`, newer than M_T (rule 6).
    ///
    /// Returns whether the shadows need a fresh resolution against the new
    /// manifest: the chain back to M_T has a gap, or one of its PK deltas is
    /// gone. An error leaves the tail as it was.
    pub async fn adopt(
        &mut self,
        ctx: &CollectionContext,
        path: String,
        manifest: Arc<CollectionManifest>,
    ) -> Result<bool, TailError> {
        let current = self.manifest.version;
        // 1. The chain from `manifest` back to M_T (exclusive), newest first.
        let mut chain = vec![manifest.clone()];
        let mut gap;
        loop {
            let child = chain
                .last()
                .expect("the chain starts with the new manifest")
                .clone();
            if child.parent_version <= current {
                gap = child.parent_version < current;
                break;
            }
            let Some(parent_path) = child.parent_manifest.clone() else {
                gap = true;
                break;
            };
            match ctx.manifests.load(&ctx.store, &parent_path).await {
                Ok(parent) => {
                    if parent.version != child.parent_version
                        || parent.collection_id != child.collection_id
                    {
                        return Err(CollectionError::Corrupt(format!(
                            "{parent_path} is version {} of collection {}, not the parent version {}",
                            parent.version, parent.collection_id, child.parent_version
                        ))
                        .into());
                    }
                    chain.push(parent);
                }
                Err(err) if not_found(&err) => {
                    gap = true;
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }
        // 3. The PK deltas, oldest first, read before anything changes.
        let mut deltas = Vec::new();
        if !gap {
            for link in chain.iter().rev() {
                let Some(delta_path) = &link.pk_delta else {
                    continue;
                };
                match ctx.store.get(delta_path).await {
                    Ok((bytes, _)) => {
                        // Keys are decoded here too, so a malformed one
                        // fails before anything changes.
                        let parsed = decode_pk_delta(&bytes)?
                            .into_iter()
                            .map(|(key, row)| {
                                PrimaryKey::from_canonical(&key)
                                    .map(|pk| (pk, row))
                                    .map_err(CollectionError::from)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        deltas.push(parsed);
                    }
                    Err(StoreError::NotFound { .. }) => {
                        gap = true;
                        break;
                    }
                    Err(err) => return Err(CollectionError::from(err).into()),
                }
            }
        }
        // 2. Drop the entries the new manifest covers.
        self.drop_covered(&manifest.applied);
        if !gap {
            for delta in deltas {
                for (pk, row) in delta {
                    let Some(slot) = self.shadow_of.get_mut(&pk) else {
                        continue;
                    };
                    if let Some(old) = *slot {
                        self.shadow_now.remove(old);
                    }
                    *slot = row;
                    if let Some(row) = row {
                        self.shadow_now.insert(row);
                    }
                }
            }
        }
        // 5. M_T := the new manifest; records below its `applied` are covered.
        for (partition, applied) in &manifest.applied {
            let head = self.head.entry(*partition).or_insert(0);
            *head = (*head).max(*applied);
        }
        self.manifest_path = Some(path);
        self.manifest = manifest;
        Ok(gap)
    }

    /// Re-resolves every overlay key's shadow row against `durable`, M_T's
    /// snapshot (rule 6.4).
    pub async fn reresolve(
        &mut self,
        durable: Option<&CollectionSnapshot>,
        resolve_batch: usize,
    ) -> Result<(), TailError> {
        let keys: Vec<PrimaryKey> = self.shadow_of.keys().cloned().collect();
        let resolved = resolve(durable, keys, resolve_batch).await?;
        self.shadow_now.clear();
        for (pk, found) in resolved {
            let row = found.map(|(row_id, _)| row_id);
            if let Some(row) = row {
                self.shadow_now.insert(row);
            }
            self.shadow_of.insert(pk, row);
        }
        Ok(())
    }
}

/// Builds a tail over exactly `(durable manifest's applied, upper]`
/// (Ruling 1), without a follower: the records of each partition from its
/// `applied` offset up to, not including, `upper[p]` (rule 10).
pub async fn build_range_tail(
    _ns: NamespaceId,
    collection: &Collection,
    _ctx: &CollectionContext,
    reader: &LogReader,
    durable: &CollectionSnapshot,
    upper: &BTreeMap<u32, u64>,
    max_bytes: usize,
) -> Result<Arc<TailSnapshot>, TailError> {
    let config = TailConfig::default();
    let manifest = Arc::new(durable.manifest().clone());
    let mut index = TailIndex::new(
        &collection.schema,
        durable.manifest_path().map(str::to_string),
        manifest,
        config.writer_memory,
    )?;
    for (&partition, &upper) in upper {
        loop {
            let from = index.head.get(&partition).copied().unwrap_or(0);
            if from >= upper {
                break;
            }
            let response = reader
                .fetch(FetchRequest {
                    stream: collection.stream,
                    partition,
                    offset: from,
                    max_bytes: config.fetch_bytes,
                    max_wait: Duration::ZERO,
                })
                .await
                .map_err(|err| match err {
                    LogError::OffsetOutOfRange {
                        requested,
                        log_start_offset,
                        ..
                    } if requested < log_start_offset => TailError::Trimmed {
                        partition,
                        offset: requested,
                        log_start: log_start_offset,
                    },
                    err => TailError::Log(err),
                })?;
            let records: Vec<OffsetRecord> = response
                .records
                .into_iter()
                .filter(|record| record.offset < upper)
                .collect();
            if records.is_empty() {
                // The stream ends below `upper`.
                break;
            }
            index
                .apply_batch(
                    collection.partitions,
                    partition,
                    &records,
                    Some(durable),
                    config.resolve_batch,
                )
                .await?;
            if index.generation.entry_bytes() > max_bytes {
                return Err(TailError::Overflow { head: index.head });
            }
        }
    }
    let snapshot = index.snapshot()?;
    if snapshot.bytes() > max_bytes {
        return Err(TailError::Overflow {
            head: snapshot.head().clone(),
        });
    }
    Ok(snapshot)
}
