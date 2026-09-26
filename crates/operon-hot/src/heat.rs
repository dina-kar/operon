//! The heat of collections (plan M1.3 Task 7 rule 4): a count-min sketch
//! with periodic halving, TinyLFU's frequency sketch. Reads record into it;
//! every `heat_window` it is halved, so an estimate is roughly the hits of
//! the last window plus half of the one before, and so on.

use std::sync::{Mutex, PoisonError};

use operon_common::{CollectionId, NamespaceId};
use xxhash_rust::xxh3::xxh3_64_with_seed;

const ROWS: usize = 4;
const WIDTH: usize = 4_096;

/// A count-min sketch of `(namespace, collection)` hits: 4 rows of 4 096
/// saturating `u8` counters, each row hashed with xxh3 under its own seed
/// (0..4). An estimate never undercounts a key's hits since the last decay,
/// up to the counters' saturation at 255.
pub struct HeatSketch {
    rows: Mutex<Box<[[u8; WIDTH]; ROWS]>>,
}

impl std::fmt::Debug for HeatSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeatSketch").finish_non_exhaustive()
    }
}

impl Default for HeatSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// The counter of `(ns, cid)` in each row.
fn slots(ns: NamespaceId, cid: CollectionId) -> [usize; ROWS] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&ns.0.to_be_bytes());
    key[8..].copy_from_slice(&cid.0.to_be_bytes());
    std::array::from_fn(|row| (xxh3_64_with_seed(&key, row as u64) % WIDTH as u64) as usize)
}

impl HeatSketch {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Box::new([[0; WIDTH]; ROWS])),
        }
    }

    /// One hit: a saturating +1 in each row.
    pub fn record(&self, ns: NamespaceId, cid: CollectionId) {
        let slots = slots(ns, cid);
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        for (row, slot) in slots.into_iter().enumerate() {
            rows[row][slot] = rows[row][slot].saturating_add(1);
        }
    }

    /// The minimum over the rows.
    pub fn estimate(&self, ns: NamespaceId, cid: CollectionId) -> u32 {
        let slots = slots(ns, cid);
        let rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        slots
            .into_iter()
            .enumerate()
            .map(|(row, slot)| u32::from(rows[row][slot]))
            .min()
            .unwrap_or(0)
    }

    /// Halves every counter.
    pub fn decay(&self) {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        for row in rows.iter_mut() {
            for counter in row.iter_mut() {
                *counter >>= 1;
            }
        }
    }

    /// Records hits until `(ns, cid)` is estimated at `hits` or more (at most
    /// 255, the counters' saturation).
    pub(crate) fn raise_to(&self, ns: NamespaceId, cid: CollectionId, hits: u32) {
        let target = hits.min(u32::from(u8::MAX));
        while self.estimate(ns, cid) < target {
            self.record(ns, cid);
        }
    }
}
