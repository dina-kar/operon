//! The record format of the implicit stream (overview §6.2): one record per
//! [`DocOp`], keyed by the canonical PK bytes, valued `0x01 ‖ postcard(WireOp)`.

use std::borrow::Cow;
use std::collections::BTreeMap;

use bytes::Bytes;
use operon_log::Record;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::doc::{DocOp, Document, PatchMode, SparseVector};
use crate::error::CodecError;
use crate::pk::PrimaryKey;

/// The first byte of every record value.
pub const CODEC_VERSION: u8 = 0x01;

/// The largest record value, version byte included.
pub const MAX_RECORD_VALUE_BYTES: usize = 16 * 1024 * 1024;

type Vectors = BTreeMap<String, Vec<f32>>;
type SparseVectors = BTreeMap<String, SparseVector>;
type VectorChanges = BTreeMap<String, Option<Vec<f32>>>;
type SparseVectorChanges = BTreeMap<String, Option<SparseVector>>;

/// The postcard body. postcard cannot carry `serde_json::Value`, so sources
/// travel as UTF-8 JSON bytes. Variant order and field order are the format.
/// `Cow` lets `encode` borrow the op; `decode` always owns.
#[derive(Serialize, Deserialize)]
enum WireOp<'a> {
    Upsert(WireDoc<'a>),
    Delete(Cow<'a, PrimaryKey>),
    Patch {
        pk: Cow<'a, PrimaryKey>,
        mode: PatchMode,
        source: Vec<u8>,
        delete_keys: Cow<'a, [String]>,
        vectors: Cow<'a, VectorChanges>,
        sparse_vectors: Cow<'a, SparseVectorChanges>,
        upsert: Option<WireDoc<'a>>,
    },
}

#[derive(Serialize, Deserialize)]
struct WireDoc<'a> {
    pk: Cow<'a, PrimaryKey>,
    /// A UTF-8 JSON object (`serde_json::to_vec`).
    source: Vec<u8>,
    vectors: Cow<'a, Vectors>,
    sparse_vectors: Cow<'a, SparseVectors>,
}

/// Encodes `op` as its record: key = canonical pk, value = `0x01 ‖ postcard`,
/// no headers, and timestamp `-1` so the log writer stamps its own clock.
///
/// Refuses what [`decode`] would refuse: an invalid key, a non-finite vector
/// value, or a value over [`MAX_RECORD_VALUE_BYTES`].
pub fn encode(op: &DocOp) -> Result<Record, CodecError> {
    op.pk().validate()?;
    let wire = match op {
        DocOp::Upsert(doc) => WireOp::Upsert(wire_doc(doc)?),
        DocOp::Delete(pk) => WireOp::Delete(Cow::Borrowed(pk)),
        DocOp::Patch {
            pk,
            mode,
            source,
            delete_keys,
            vectors,
            sparse_vectors,
            upsert,
        } => {
            check_finite_changes(vectors)?;
            WireOp::Patch {
                pk: Cow::Borrowed(pk),
                mode: *mode,
                source: source_bytes(source)?,
                delete_keys: Cow::Borrowed(delete_keys),
                vectors: Cow::Borrowed(vectors),
                sparse_vectors: Cow::Borrowed(sparse_vectors),
                upsert: upsert.as_ref().map(wire_doc).transpose()?,
            }
        }
    };
    let value = postcard::to_extend(&wire, vec![CODEC_VERSION])
        .map_err(|e| CodecError::Malformed(e.to_string()))?;
    if value.len() > MAX_RECORD_VALUE_BYTES {
        return Err(CodecError::TooLarge(value.len()));
    }
    Ok(Record {
        key: Some(Bytes::from(op.pk().canonical())),
        value: Some(Bytes::from(value)),
        headers: Vec::new(),
        timestamp_ms: -1,
    })
}

/// Decodes a record of the implicit stream. Every error means the record is
/// dead-lettered at apply time (plan M1.1 Ruling 11).
pub fn decode(record: &Record) -> Result<DocOp, CodecError> {
    let key = record.key.as_ref().ok_or(CodecError::MissingKey)?;
    let value = record.value.as_ref().ok_or(CodecError::MissingValue)?;
    let (&version, body) = value.split_first().ok_or(CodecError::MissingValue)?;
    if value.len() > MAX_RECORD_VALUE_BYTES {
        return Err(CodecError::TooLarge(value.len()));
    }
    if version != CODEC_VERSION {
        return Err(CodecError::UnknownVersion(version));
    }
    let pk = PrimaryKey::from_canonical(key)?;
    let (wire, rest) = postcard::take_from_bytes::<WireOp>(body)
        .map_err(|e| CodecError::Malformed(e.to_string()))?;
    if !rest.is_empty() {
        return Err(CodecError::Malformed(format!(
            "{} trailing bytes after the body",
            rest.len()
        )));
    }
    let op = match wire {
        WireOp::Upsert(doc) => DocOp::Upsert(document(doc)?),
        WireOp::Delete(pk) => DocOp::Delete(pk.into_owned()),
        WireOp::Patch {
            pk,
            mode,
            source,
            delete_keys,
            vectors,
            sparse_vectors,
            upsert,
        } => {
            check_finite_changes(&vectors)?;
            DocOp::Patch {
                pk: pk.into_owned(),
                mode,
                source: parse_source(&source)?,
                delete_keys: delete_keys.into_owned(),
                vectors: vectors.into_owned(),
                sparse_vectors: sparse_vectors.into_owned(),
                upsert: upsert.map(document).transpose()?,
            }
        }
    };
    if *op.pk() != pk {
        return Err(CodecError::KeyMismatch);
    }
    Ok(op)
}

fn wire_doc(doc: &Document) -> Result<WireDoc<'_>, CodecError> {
    doc.pk.validate()?;
    check_finite(&doc.vectors)?;
    Ok(WireDoc {
        pk: Cow::Borrowed(&doc.pk),
        source: source_bytes(&doc.source)?,
        vectors: Cow::Borrowed(&doc.vectors),
        sparse_vectors: Cow::Borrowed(&doc.sparse_vectors),
    })
}

fn document(wire: WireDoc<'_>) -> Result<Document, CodecError> {
    wire.pk.validate()?;
    check_finite(wire.vectors.iter())?;
    Ok(Document {
        pk: wire.pk.into_owned(),
        source: parse_source(&wire.source)?,
        vectors: wire.vectors.into_owned(),
        sparse_vectors: wire.sparse_vectors.into_owned(),
    })
}

fn source_bytes(source: &Map<String, Value>) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(source).map_err(|e| CodecError::Malformed(format!("source: {e}")))
}

fn parse_source(bytes: &[u8]) -> Result<Map<String, Value>, CodecError> {
    serde_json::from_slice(bytes)
        .map_err(|e| CodecError::Malformed(format!("source is not a JSON object: {e}")))
}

fn check_finite_changes(changes: &VectorChanges) -> Result<(), CodecError> {
    check_finite(
        changes
            .iter()
            .filter_map(|(name, v)| Some((name, v.as_ref()?))),
    )
}

/// Dense vector values must be finite (sparse vectors are checked by
/// [`SparseVector::new`] as they are decoded).
fn check_finite<'v>(
    vectors: impl IntoIterator<Item = (&'v String, &'v Vec<f32>)>,
) -> Result<(), CodecError> {
    match vectors
        .into_iter()
        .find(|(_, vector)| vector.iter().any(|x| !x.is_finite()))
    {
        Some((name, _)) => Err(CodecError::Malformed(format!(
            "vector {name:?} has a non-finite value"
        ))),
        None => Ok(()),
    }
}
