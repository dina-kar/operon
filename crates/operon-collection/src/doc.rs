use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::SparseVectorError;
use crate::pk::PrimaryKey;

/// A sparse vector in canonical form (overview A27): indices strictly
/// ascending, one finite value per index (zeros allowed).
///
/// The fields are private, and every constructor goes through
/// [`SparseVector::new`], deserialization included. The JSON form is
/// `{"indices": [u32], "values": [f32]}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawSparseVector", into = "RawSparseVector")]
pub struct SparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
}

/// The serialized form of a [`SparseVector`]: two parallel sequences.
#[derive(Serialize, Deserialize)]
struct RawSparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl TryFrom<RawSparseVector> for SparseVector {
    type Error = SparseVectorError;

    fn try_from(raw: RawSparseVector) -> Result<Self, SparseVectorError> {
        SparseVector::new(raw.indices, raw.values)
    }
}

impl From<SparseVector> for RawSparseVector {
    fn from(vector: SparseVector) -> Self {
        RawSparseVector {
            indices: vector.indices,
            values: vector.values,
        }
    }
}

impl SparseVector {
    /// Sorts the `(index, value)` pairs by index. Unequal lengths, an index
    /// that appears twice or a non-finite value is an error.
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Result<Self, SparseVectorError> {
        if indices.len() != values.len() {
            return Err(SparseVectorError::LengthMismatch {
                indices: indices.len(),
                values: values.len(),
            });
        }
        if let Some((&index, _)) = indices.iter().zip(&values).find(|(_, v)| !v.is_finite()) {
            return Err(SparseVectorError::NonFinite(index));
        }
        let mut pairs: Vec<(u32, f32)> = indices.into_iter().zip(values).collect();
        pairs.sort_by_key(|&(index, _)| index);
        if let Some(pair) = pairs.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(SparseVectorError::DuplicateIndex(pair[0].0));
        }
        let (indices, values) = pairs.into_iter().unzip();
        Ok(SparseVector { indices, values })
    }

    /// The indices, strictly ascending.
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    /// The values, one per index, in index order.
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// The number of stored entries (zero weights included).
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// A whole document (overview §6.2).
#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub pk: PrimaryKey,
    pub source: Map<String, Value>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    pub sparse_vectors: BTreeMap<String, SparseVector>,
}

/// How a patch's source combines with the current source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatchMode {
    /// Objects merge recursively; anything else replaces (ES partial doc).
    MergeDeep,
    /// Each top-level key replaces (Qdrant `set_payload`).
    MergeTop,
    /// The patch source becomes the source (Qdrant `overwrite_payload`).
    Replace,
}

/// One write to one key: the value of one record on the implicit stream.
#[derive(Clone, Debug, PartialEq)]
pub enum DocOp {
    Upsert(Document),
    Delete(PrimaryKey),
    Patch {
        pk: PrimaryKey,
        mode: PatchMode,
        source: Map<String, Value>,
        /// Dot-separated paths removed after the source is combined.
        delete_keys: Vec<String>,
        /// `None` deletes the vector.
        vectors: BTreeMap<String, Option<Vec<f32>>>,
        /// `None` deletes the sparse vector.
        sparse_vectors: BTreeMap<String, Option<SparseVector>>,
        /// Inserted as it is when the key does not exist. Without it, a patch
        /// of a missing key changes nothing.
        upsert: Option<Document>,
    },
}

impl DocOp {
    /// The key this op writes, which is also its record key.
    pub fn pk(&self) -> &PrimaryKey {
        match self {
            DocOp::Upsert(doc) => &doc.pk,
            DocOp::Delete(pk) | DocOp::Patch { pk, .. } => pk,
        }
    }
}

/// The result of applying `patch` (a [`DocOp::Patch`]) to `current`; `None`
/// means the key stays absent.
///
/// With `current = None`, the result is the patch's `upsert` document when
/// its pk is the patch's pk, and `None` otherwise. With a current document,
/// the source is combined per [`PatchMode`], then `delete_keys` are removed,
/// then the vectors and sparse vectors are set or removed; the pk is kept.
///
/// Any other op is applied as it is (an upsert gives its document, a delete
/// gives `None`), so the function is total.
pub fn apply_patch(current: Option<&Document>, patch: &DocOp) -> Option<Document> {
    let DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys,
        vectors,
        sparse_vectors,
        upsert,
    } = patch
    else {
        return match patch {
            DocOp::Upsert(doc) => Some(doc.clone()),
            _ => None,
        };
    };
    let Some(current) = current else {
        return upsert.as_ref().filter(|doc| doc.pk == *pk).cloned();
    };
    let mut doc = current.clone();
    match mode {
        PatchMode::MergeDeep => merge_deep(&mut doc.source, source),
        PatchMode::MergeTop => doc
            .source
            .extend(source.iter().map(|(k, v)| (k.clone(), v.clone()))),
        PatchMode::Replace => doc.source = source.clone(),
    }
    for path in delete_keys {
        delete_path(&mut doc.source, path);
    }
    set_or_remove(&mut doc.vectors, vectors);
    set_or_remove(&mut doc.sparse_vectors, sparse_vectors);
    Some(doc)
}

/// Merges `patch` into `target`: where both values are objects they merge
/// recursively; otherwise the patch value replaces (arrays and `null` too).
fn merge_deep(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, value) in patch {
        match (target.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(nested)) => merge_deep(existing, nested),
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Removes the dot-separated `path`, walking objects only; a missing path, or
/// one that runs into a non-object, is a no-op.
fn delete_path(source: &mut Map<String, Value>, path: &str) {
    let mut segments = path.split('.');
    let Some(last) = segments.next_back() else {
        return;
    };
    let mut object = source;
    for segment in segments {
        match object.get_mut(segment) {
            Some(Value::Object(nested)) => object = nested,
            _ => return,
        }
    }
    object.remove(last);
}

/// `Some` sets the entry, `None` removes it.
fn set_or_remove<T: Clone>(
    target: &mut BTreeMap<String, T>,
    changes: &BTreeMap<String, Option<T>>,
) {
    for (name, change) in changes {
        match change {
            Some(value) => {
                target.insert(name.clone(), value.clone());
            }
            None => {
                target.remove(name);
            }
        }
    }
}
