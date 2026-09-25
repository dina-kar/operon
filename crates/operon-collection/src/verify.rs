//! The gates' model of a collection (plan M1.1 Task 10 rule 8): what the
//! implicit stream should fold to, and every way the committed collection
//! differs from it. Used by the target tests, the crash gate and the
//! simulation.
//!
//! [`fold_stream`] is an independent model: it has its own latest-wins loop
//! and its own per-kind validation table, and shares only the record codec
//! and `apply_patch` (each tested on its own) with the target, so a bug in
//! the target's fold or validation cannot hide in both.

use std::collections::{BTreeMap, BTreeSet};

use operon_common::{CollectionId, NamespaceId};
use operon_log::OffsetRecord;
use operon_meta::{Consistency, collection_pk_prefix};
use operon_pk::PkReader;
use serde_json::{Map, Value};

use crate::codec::decode;
use crate::doc::{DocOp, Document, apply_patch};
use crate::error::CollectionError;
use crate::pk::{PrimaryKey, partition_of};
use crate::pkindex::{PkWatermark, parse_pk_value};
use crate::schema::{CollectionSchema, FieldKind};
use crate::snapshot::{CollectionContext, CollectionSnapshot, StoredDoc};
use crate::tantivy_schema::{PK_FIELD, ROWID_FIELD};
use crate::target::PK_WATERMARK_KEY;
use crate::values::parse_date;

/// A key's expected document and the record that last wrote it.
#[derive(Clone, Debug, PartialEq)]
pub struct Expected {
    pub doc: Document,
    pub partition: u32,
    pub offset: u64,
}

/// Folds a collection's stream records into the state its link must commit,
/// in partition order (offset order within a partition), with the
/// dead-letter rules: a record that does not decode, sits on a partition
/// other than `partition_of(pk, partitions)`, or whose upsert or patch
/// result breaks `schema` (with dynamic mapping ignored) changes nothing. A
/// patch whose result equals the state before it is not a write, so the key
/// keeps its earlier record.
///
/// (Deviation from the plan's signature: `partitions` is needed for the
/// wrong-partition rule.)
pub fn fold_stream(
    schema: &CollectionSchema,
    partitions: u32,
    records: &[(u32, OffsetRecord)],
) -> BTreeMap<PrimaryKey, Expected> {
    let mut ordered: Vec<&(u32, OffsetRecord)> = records.iter().collect();
    ordered.sort_by_key(|(partition, record)| (*partition, record.offset));
    let mut state: BTreeMap<PrimaryKey, Expected> = BTreeMap::new();
    for (partition, record) in ordered {
        let Ok(op) = decode(&record.record) else {
            continue;
        };
        if partition_of(op.pk(), partitions) != *partition {
            continue;
        }
        let pk = op.pk().clone();
        let after = match &op {
            DocOp::Delete(_) => {
                state.remove(&pk);
                continue;
            }
            DocOp::Upsert(doc) => doc.clone(),
            DocOp::Patch { .. } => {
                let before = state.get(&pk).map(|e| &e.doc);
                match apply_patch(before, &op) {
                    None => continue,
                    Some(after) if Some(&after) == before => continue,
                    Some(after) => after,
                }
            }
        };
        if model_accepts(schema, &after) {
            let expected = Expected {
                doc: after,
                partition: *partition,
                offset: record.offset,
            };
            state.insert(pk, expected);
        }
    }
    state
}

/// The model's validation: every typed field's values fit its kind (or the
/// field ignores malformed values), and every vector is declared, dense ones
/// with their dimension and finite values. Json fields and unmapped paths
/// are never checked.
fn model_accepts(schema: &CollectionSchema, doc: &Document) -> bool {
    let fields_fit = schema.fields.iter().all(|field| {
        field.kind == FieldKind::Json
            || field.ignore_malformed
            || values_at(&doc.source, &field.source_path)
                .iter()
                .all(|value| fits(&field.kind, value))
    });
    let vectors_fit = doc.vectors.iter().all(|(name, vector)| {
        schema.vectors.iter().any(|spec| {
            spec.name == *name
                && vector.len() == spec.dim as usize
                && vector.iter().all(|x| x.is_finite())
        })
    });
    let sparse_fit = doc
        .sparse_vectors
        .keys()
        .all(|name| schema.sparse_vectors.iter().any(|spec| spec.name == *name));
    fields_fit && vectors_fit && sparse_fit
}

/// The values at the dot path `path`, through nested objects, with arrays
/// flattened at every step (the model does not follow keys that contain
/// dots).
fn values_at<'a>(source: &'a Map<String, Value>, path: &str) -> Vec<&'a Value> {
    let mut objects: Vec<&'a Map<String, Value>> = vec![source];
    let mut found: Vec<&'a Value> = Vec::new();
    let segments: Vec<&str> = path.split('.').collect();
    for (i, segment) in segments.iter().enumerate() {
        let last = i + 1 == segments.len();
        let mut next = Vec::new();
        for object in objects {
            if let Some(value) = object.get(*segment) {
                flatten(value, &mut next);
            }
        }
        if last {
            found = next;
            break;
        }
        objects = next.iter().filter_map(|v| v.as_object()).collect();
    }
    found
}

fn flatten<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => items.iter().for_each(|item| flatten(item, out)),
        other => out.push(other),
    }
}

/// The model's per-kind table of acceptable JSON values (ES coercion).
fn fits(kind: &FieldKind, value: &Value) -> bool {
    // i64's range as f64: [-2^63, 2^63).
    const LOW: f64 = -9_223_372_036_854_775_808.0;
    let in_i64 = |f: f64| f.trunc() >= LOW && f.trunc() < -LOW;
    match (kind, value) {
        (_, Value::Null) => true,
        (_, Value::Object(_) | Value::Array(_)) => false,
        (FieldKind::Json, _) => true,
        (FieldKind::Text { .. } | FieldKind::Keyword, _) => true,
        (FieldKind::I64, Value::Number(n)) => {
            n.as_i64().is_some() || n.as_f64().is_some_and(in_i64)
        }
        (FieldKind::I64, Value::String(s)) => {
            s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok_and(in_i64)
        }
        (FieldKind::F64, Value::Number(n)) => n.as_f64().is_some_and(f64::is_finite),
        (FieldKind::F64, Value::String(s)) => s.parse::<f64>().is_ok_and(f64::is_finite),
        (FieldKind::Bool, Value::Bool(_)) => true,
        (FieldKind::Bool, Value::String(s)) => matches!(s.as_str(), "true" | "false" | ""),
        (FieldKind::Date, _) => parse_date(value).is_ok(),
        (FieldKind::Uuid, Value::String(s)) => uuid::Uuid::parse_str(s).is_ok(),
        _ => false,
    }
}

/// Every violation of rule 8 of Task 10 in the live version of collection
/// `cid` against `expected` (empty = ok):
/// 1. the live Lance keys are `expected`'s keys;
/// 2. each key's source, vectors, sparse vectors, partition and `seq_no`
///    are the expected ones;
/// 3. each key has exactly one live Lance row;
/// 4. the splits' live docs, `live_doc_count` and `expected.len()` agree;
/// 5. every live split doc is a live Lance row with the same key, and every
///    live Lance row is exactly one live split doc;
/// 6. if the PK index's watermark is the manifest's `applied`, the PK index
///    maps each key to its live row id and has no other keys.
pub async fn verify_collection(
    ctx: &CollectionContext,
    ns: NamespaceId,
    cid: CollectionId,
    expected: &BTreeMap<PrimaryKey, Expected>,
) -> Result<Vec<String>, CollectionError> {
    let snapshot = CollectionSnapshot::open(ctx, ns, cid, Consistency::Linearizable).await?;
    let manifest = snapshot.manifest().clone();
    let docs = snapshot.scan_all().await?;
    let mut problems = Vec::new();

    // 1–3. Lance against the model.
    let mut rows: BTreeMap<&PrimaryKey, Vec<&StoredDoc>> = BTreeMap::new();
    for doc in &docs {
        rows.entry(&doc.pk).or_default().push(doc);
    }
    for pk in expected.keys().filter(|pk| !rows.contains_key(pk)) {
        problems.push(format!("{pk:?} is expected but has no lance row"));
    }
    for (pk, found) in &rows {
        let Some(want) = expected.get(*pk) else {
            problems.push(format!("{pk:?} has a lance row but is not expected"));
            continue;
        };
        if found.len() != 1 {
            problems.push(format!("{pk:?} has {} live lance rows", found.len()));
        }
        let doc = found[0];
        if doc.source != want.doc.source {
            problems.push(format!(
                "{pk:?}: source {:?}, expected {:?}",
                doc.source, want.doc.source
            ));
        }
        if doc.vectors != want.doc.vectors {
            problems.push(format!(
                "{pk:?}: vectors {:?}, expected {:?}",
                doc.vectors, want.doc.vectors
            ));
        }
        if doc.sparse_vectors != want.doc.sparse_vectors {
            problems.push(format!(
                "{pk:?}: sparse vectors {:?}, expected {:?}",
                doc.sparse_vectors, want.doc.sparse_vectors
            ));
        }
        if (doc.partition, doc.seq_no) != (want.partition, want.offset) {
            problems.push(format!(
                "{pk:?}: written at {}/{}, expected {}/{}",
                doc.partition, doc.seq_no, want.partition, want.offset
            ));
        }
    }

    // 4. The counts.
    let split_live: u64 = manifest
        .splits
        .iter()
        .map(|s| s.doc_count.saturating_sub(s.deleted_count))
        .sum();
    if split_live != manifest.live_doc_count || split_live != expected.len() as u64 {
        problems.push(format!(
            "live split docs {split_live}, live_doc_count {}, expected {}",
            manifest.live_doc_count,
            expected.len()
        ));
    }

    // 5. Exactly once across Lance and the splits.
    let lance_rows: BTreeMap<u64, &PrimaryKey> = docs.iter().map(|d| (d.row_id, &d.pk)).collect();
    let mut seen: BTreeMap<u64, usize> = BTreeMap::new();
    for (index, split) in manifest.splits.iter().enumerate() {
        let tantivy = snapshot.open_split(split).await?;
        let searcher = operon_text::warm_up_all(&tantivy).await?;
        let deleted = snapshot.deleted_docs(split).await?;
        if deleted.len() != split.deleted_count {
            problems.push(format!(
                "split {}: {} deleted docs, the manifest says {}",
                split.ulid,
                deleted.len(),
                split.deleted_count
            ));
        }
        let segment = searcher.segment_reader(0);
        let fast = segment.fast_fields();
        let row_ids = fast.u64(ROWID_FIELD).map_err(tantivy_error)?;
        let pks = fast
            .bytes(PK_FIELD)
            .map_err(tantivy_error)?
            .ok_or_else(|| CollectionError::Corrupt(format!("split {} has no _pk", split.ulid)))?;
        let doc_count = u32::try_from(split.doc_count).unwrap_or(u32::MAX);
        for doc_id in (0..doc_count).filter(|d| !deleted.contains(*d)) {
            let Some(row_id) = row_ids.first(doc_id) else {
                problems.push(format!("split {} doc {doc_id} has no _rowid", split.ulid));
                continue;
            };
            *seen.entry(row_id).or_default() += 1;
            if snapshot.locate_row(row_id) != Some((index, doc_id)) {
                problems.push(format!(
                    "split {} doc {doc_id}: row {row_id} is located at {:?}",
                    split.ulid,
                    snapshot.locate_row(row_id)
                ));
            }
            let mut pk = Vec::new();
            if let Some(ord) = pks.term_ords(doc_id).next() {
                pks.ord_to_bytes(ord, &mut pk)
                    .map_err(|err| CollectionError::Corrupt(err.to_string()))?;
            }
            match lance_rows.get(&row_id) {
                None => problems.push(format!(
                    "split {} doc {doc_id}: row {row_id} is not a live lance row",
                    split.ulid
                )),
                Some(lance_pk) if lance_pk.canonical() != pk => problems.push(format!(
                    "split {} doc {doc_id}: _pk {pk:?}, lance row {row_id} has {lance_pk:?}",
                    split.ulid
                )),
                Some(_) => {}
            }
        }
    }
    for row_id in lance_rows.keys() {
        match seen.get(row_id).copied().unwrap_or(0) {
            1 => {}
            n => problems.push(format!("lance row {row_id} is {n} live split docs")),
        }
    }

    // 6. The PK index, when it has caught up.
    if manifest.version > 0 {
        let reader = PkReader::open(&ctx.store, &collection_pk_prefix(ns, cid)).await?;
        let watermark = match reader.get(PK_WATERMARK_KEY).await? {
            Some(bytes) => PkWatermark::decode(&bytes)?,
            None => PkWatermark::default(),
        };
        if watermark.applied == manifest.applied {
            let want: BTreeMap<Vec<u8>, u64> =
                docs.iter().map(|d| (d.pk.canonical(), d.row_id)).collect();
            let mut have: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
            for tag in [0x01u8, 0x02, 0x03] {
                for (key, value) in reader.scan_prefix(&[tag], usize::MAX).await? {
                    have.insert(key.to_vec(), parse_pk_value(&value)?);
                }
            }
            let keys: BTreeSet<&Vec<u8>> = want.keys().chain(have.keys()).collect();
            for key in keys {
                if want.get(key) != have.get(key) {
                    problems.push(format!(
                        "pk index maps {key:?} to {:?}, lance has {:?}",
                        have.get(key),
                        want.get(key)
                    ));
                }
            }
        }
        reader.close().await?;
    }
    Ok(problems)
}

fn tantivy_error(err: impl std::fmt::Display) -> CollectionError {
    CollectionError::Corrupt(format!("split fast field: {err}"))
}
