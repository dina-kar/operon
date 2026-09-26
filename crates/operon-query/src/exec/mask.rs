//! Row sets and per-split masks (plan M1.2 Task 5): deleted split docs and
//! shadowed rows are masked inside every operator, before any top-k cut.

use roaring::{RoaringBitmap, RoaringTreemap};

/// A set of rows of a view, in the one row-id space (Ruling 7).
#[derive(Clone, Debug, PartialEq)]
pub enum RowSet {
    /// Every row of the view.
    All,
    Rows(RoaringTreemap),
}

impl RowSet {
    /// Whether `row_id` is in the set.
    pub fn contains(&self, row_id: u64) -> bool {
        match self {
            RowSet::All => true,
            RowSet::Rows(rows) => rows.contains(row_id),
        }
    }
}

/// The masked doc ids of one split: deleted by its delete bitmap, or
/// shadowed by the tail.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SplitMask {
    pub deleted: RoaringBitmap,
    pub shadowed: RoaringBitmap,
}

impl SplitMask {
    pub fn masked(&self, doc: u32) -> bool {
        self.deleted.contains(doc) || self.shadowed.contains(doc)
    }
}
