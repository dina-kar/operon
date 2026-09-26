//! Point reads by key (plan M1.2 Task 7 rule 7; S8): the tail first, then
//! one durable lookup of the keys it does not hold.

use operon_collection::PrimaryKey;

use crate::error::ServiceError;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow, fetch_rows};
use crate::exec::project::{fetch_columns, stored_doc};
use crate::read::{ReadView, collection_error};
use crate::tail::TailLookup;
use crate::types::{Projection, StoredDoc};

/// A durable document projected to `columns`.
fn durable_row(
    view: &ReadView,
    doc: operon_collection::StoredDoc,
    columns: &FetchColumns,
) -> FetchedRow {
    let schema = &view.collection.schema;
    let mut vectors = doc.vectors;
    let mut sparse = doc.sparse_vectors;
    FetchedRow {
        row_id: doc.row_id,
        pk: doc.pk,
        source: columns.source.then_some(doc.source),
        vectors: columns
            .vectors
            .iter()
            .filter_map(|i| {
                let name = &schema.vectors.get(*i)?.name;
                Some((name.clone(), vectors.remove(name)?))
            })
            .collect(),
        sparse_vectors: columns
            .sparse
            .iter()
            .filter_map(|i| {
                let name = &schema.sparse_vectors.get(*i)?.name;
                Some((name.clone(), sparse.remove(name)?))
            })
            .collect(),
        seq_no: doc.seq_no,
        partition: doc.partition,
    }
}

/// One entry per key of `pks`, in request order, `None` for a miss: a tail
/// doc or delete wins; the keys the tail does not hold go to one
/// `get_by_pk` (they cannot be shadowed, since a shadowed key has a tail
/// entry).
pub(crate) async fn get(
    view: &ReadView,
    pks: &[PrimaryKey],
    select: &Projection,
) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
    let schema = &view.collection.schema;
    let columns = fetch_columns(schema, select, false)?;
    enum Slot {
        Tail(u64),
        Miss,
        Durable(usize),
    }
    let mut slots = Vec::with_capacity(pks.len());
    let mut durable: Vec<PrimaryKey> = Vec::new();
    for pk in pks {
        slots.push(match view.tail.get(pk) {
            TailLookup::Present(doc) => Slot::Tail(doc.row_id),
            TailLookup::Deleted(_) => Slot::Miss,
            TailLookup::Absent => {
                durable.push(pk.clone());
                Slot::Durable(durable.len() - 1)
            }
        });
    }
    let tail_ids: Vec<u64> = slots
        .iter()
        .filter_map(|slot| match slot {
            Slot::Tail(row) => Some(*row),
            _ => None,
        })
        .collect();
    let mut tail_rows = fetch_rows(view, &tail_ids, &columns).await?.into_iter();
    let mut found = if durable.is_empty() {
        Vec::new()
    } else {
        view.snapshot
            .get_by_pk(&durable)
            .await
            .map_err(collection_error)?
    };
    slots
        .into_iter()
        .map(|slot| {
            let row = match slot {
                Slot::Tail(_) => Some(tail_rows.next().ok_or_else(|| {
                    ServiceError::Internal("a tail row was not fetched".to_string())
                })?),
                Slot::Miss => None,
                Slot::Durable(i) => found
                    .get_mut(i)
                    .and_then(Option::take)
                    .map(|doc| durable_row(view, doc, &columns)),
            };
            Ok(row.map(|row| stored_doc(schema, row, select)))
        })
        .collect()
}
