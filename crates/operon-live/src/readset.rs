//! [`ReadSetIndex`]: the app's index of subscription read sets (design §20
//! §8.2 step 2; R1 plan Task 11).
//!
//! Point keys (documents read by id) go in a hash map; index ranges `[lo,
//! hi)` go in a hand-written augmented interval tree (R1 plan rows R14 and
//! X8: `rust-lapper` needs integer coordinates and cannot remove). The tree
//! is an AVL tree ordered by `(lo, hi, subscription)`, and every node keeps
//! the largest `hi` of its subtree, so a stab (every range holding one key)
//! costs `O(log n + matches)` and insert and remove `O(log n)`.
//!
//! One tree holds the ranges of every table and index (R1 plan row T11-2):
//! a read of a missing table depends on [`AppKeys::tables_from`], which
//! spans the index entries of every table not created yet, and a single
//! tree has the same bounds as one per index.
//!
//! [`AppKeys::tables_from`]: crate::AppKeys::tables_from

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::ids::{DOC_ID_BYTES, DocId, TableId};
use crate::keys::{AppKeys, KeyRange};
use crate::{ReadSet, WriteRecord};

/// A subscription's id, unique per [`Subscriptions`](crate::Subscriptions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubId(pub u64);

impl fmt::Display for SubId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sub-{}", self.0)
    }
}

/// The upper end of a range: a key (exclusive) or the end of the root
/// ([`KeyRange`]'s empty `hi`). Declared in this order so that every key
/// sorts below `Unbounded`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Upper {
    Key(Vec<u8>),
    Unbounded,
}

impl Upper {
    fn of(range: &KeyRange) -> Self {
        if range.hi.is_empty() {
            Upper::Unbounded
        } else {
            Upper::Key(range.hi.clone())
        }
    }

    /// Whether `key` is below this upper end.
    fn above(&self, key: &[u8]) -> bool {
        match self {
            Upper::Key(hi) => key < hi.as_slice(),
            Upper::Unbounded => true,
        }
    }
}

type Link = Option<Box<Node>>;

#[derive(Debug)]
struct Node {
    lo: Vec<u8>,
    hi: Upper,
    id: SubId,
    /// The largest `hi` in this subtree.
    max: Upper,
    height: u32,
    left: Link,
    right: Link,
}

impl Node {
    fn cmp_key(&self, lo: &[u8], hi: &Upper, id: SubId) -> Ordering {
        (self.lo.as_slice(), &self.hi, self.id).cmp(&(lo, hi, id))
    }
}

fn height(link: &Link) -> u32 {
    link.as_ref().map_or(0, |n| n.height)
}

fn update(node: &mut Node) {
    node.height = 1 + height(&node.left).max(height(&node.right));
    let mut max = node.hi.clone();
    for child in [&node.left, &node.right].into_iter().flatten() {
        if child.max > max {
            max = child.max.clone();
        }
    }
    node.max = max;
}

fn rotate_right(mut node: Box<Node>) -> Box<Node> {
    let Some(mut left) = node.left.take() else {
        return node;
    };
    node.left = left.right.take();
    update(&mut node);
    left.right = Some(node);
    update(&mut left);
    left
}

fn rotate_left(mut node: Box<Node>) -> Box<Node> {
    let Some(mut right) = node.right.take() else {
        return node;
    };
    node.right = right.left.take();
    update(&mut node);
    right.left = Some(node);
    update(&mut right);
    right
}

/// Restores the AVL balance of `node`, whose children are balanced and
/// differ in height by at most 2.
fn balance(mut node: Box<Node>) -> Box<Node> {
    update(&mut node);
    let (l, r) = (height(&node.left), height(&node.right));
    if l > r + 1 {
        if let Some(left) = node.left.take() {
            node.left = Some(if height(&left.right) > height(&left.left) {
                rotate_left(left)
            } else {
                left
            });
        }
        return rotate_right(node);
    }
    if r > l + 1 {
        if let Some(right) = node.right.take() {
            node.right = Some(if height(&right.left) > height(&right.right) {
                rotate_right(right)
            } else {
                right
            });
        }
        return rotate_left(node);
    }
    node
}

fn insert(link: Link, new: Box<Node>) -> Box<Node> {
    let Some(mut node) = link else {
        return new;
    };
    match node.cmp_key(&new.lo, &new.hi, new.id) {
        Ordering::Greater => node.left = Some(insert(node.left.take(), new)),
        Ordering::Less => node.right = Some(insert(node.right.take(), new)),
        // Already present: the index never inserts one range of one
        // subscription twice (it dedups first).
        Ordering::Equal => return node,
    }
    balance(node)
}

/// Removes and returns the smallest node of `node`'s subtree, and the rest.
fn take_min(mut node: Box<Node>) -> (Box<Node>, Link) {
    match node.left.take() {
        None => {
            let rest = node.right.take();
            (node, rest)
        }
        Some(left) => {
            let (min, rest) = take_min(left);
            node.left = rest;
            (min, Some(balance(node)))
        }
    }
}

fn remove(link: Link, lo: &[u8], hi: &Upper, id: SubId) -> (Link, bool) {
    let Some(mut node) = link else {
        return (None, false);
    };
    let found = match node.cmp_key(lo, hi, id) {
        Ordering::Greater => {
            let (left, found) = remove(node.left.take(), lo, hi, id);
            node.left = left;
            found
        }
        Ordering::Less => {
            let (right, found) = remove(node.right.take(), lo, hi, id);
            node.right = right;
            found
        }
        Ordering::Equal => {
            let rest = match (node.left.take(), node.right.take()) {
                (None, None) => None,
                (Some(child), None) | (None, Some(child)) => Some(child),
                (Some(left), Some(right)) => {
                    let (mut min, right) = take_min(right);
                    min.left = Some(left);
                    min.right = right;
                    Some(balance(min))
                }
            };
            return (rest, true);
        }
    };
    (Some(balance(node)), found)
}

/// Adds the ids of every range in `link`'s subtree that holds `key`.
fn stab(link: &Link, key: &[u8], out: &mut HashSet<SubId>) {
    let Some(node) = link else { return };
    if !node.max.above(key) {
        // Every range here ends at or before `key`.
        return;
    }
    stab(&node.left, key, out);
    if node.lo.as_slice() > key {
        // This range and every range to its right start after `key`.
        return;
    }
    if node.hi.above(key) {
        out.insert(node.id);
    }
    stab(&node.right, key, out);
}

/// What one subscription put in the index, so it can be taken out again.
#[derive(Debug, Default)]
struct Entry {
    points: Vec<Vec<u8>>,
    ranges: Vec<(Vec<u8>, Upper)>,
}

/// The read sets of an app's subscriptions: a hash map of point keys and an
/// augmented interval tree of ranges, both mapping to [`SubId`]s. Holds at
/// most one read set per subscription; inserting again replaces it.
pub struct ReadSetIndex {
    app: AppKeys,
    points: HashMap<Vec<u8>, HashSet<SubId>>,
    tree: Link,
    ranges: usize,
    entries: HashMap<SubId, Entry>,
}

impl fmt::Debug for ReadSetIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadSetIndex")
            .field("subscriptions", &self.entries.len())
            .field("points", &self.points.len())
            .field("ranges", &self.ranges)
            .finish_non_exhaustive()
    }
}

impl ReadSetIndex {
    /// An empty index for the app whose keys are `app`.
    pub fn new(app: AppKeys) -> Self {
        ReadSetIndex {
            app,
            points: HashMap::new(),
            tree: None,
            ranges: 0,
            entries: HashMap::new(),
        }
    }

    /// Indexes `rs` as subscription `id`'s read set, replacing any earlier
    /// one. Empty ranges hold no key and are not stored.
    pub fn insert(&mut self, id: SubId, rs: &ReadSet) {
        self.remove(id);
        let mut entry = Entry::default();
        for key in &rs.points {
            self.points.entry(key.clone()).or_default().insert(id);
            entry.points.push(key.clone());
        }
        let mut ranges: Vec<(Vec<u8>, Upper)> = rs
            .ranges
            .iter()
            .filter(|r| !r.is_empty())
            .map(|r| (r.lo.clone(), Upper::of(r)))
            .collect();
        ranges.sort();
        ranges.dedup();
        for (lo, hi) in &ranges {
            let node = Box::new(Node {
                lo: lo.clone(),
                hi: hi.clone(),
                id,
                max: hi.clone(),
                height: 1,
                left: None,
                right: None,
            });
            self.tree = Some(insert(self.tree.take(), node));
            self.ranges += 1;
        }
        entry.ranges = ranges;
        self.entries.insert(id, entry);
    }

    /// Removes subscription `id`'s read set (nothing if it has none).
    pub fn remove(&mut self, id: SubId) {
        let Some(entry) = self.entries.remove(&id) else {
            return;
        };
        for key in entry.points {
            if let Some(ids) = self.points.get_mut(&key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.points.remove(&key);
                }
            }
        }
        for (lo, hi) in entry.ranges {
            let (tree, found) = remove(self.tree.take(), &lo, &hi, id);
            self.tree = tree;
            if found {
                self.ranges -= 1;
            }
        }
    }

    /// Whether subscription `id` has a read set here.
    pub fn contains(&self, id: SubId) -> bool {
        self.entries.contains_key(&id)
    }

    /// The subscriptions indexed.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no subscription is indexed.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds every subscription whose read set a write can change: those
    /// holding the written document's key, or a removed or added index key
    /// of the write (§20 §8.2 step 2).
    pub fn stab(&self, w: &WriteRecord, out: &mut HashSet<SubId>) {
        if let Ok(bytes) = <[u8; DOC_ID_BYTES]>::try_from(w.doc_id.as_slice()) {
            let key = self.app.document(&DocId {
                table: TableId(w.table_id),
                bytes,
            });
            self.stab_key(&key, out);
        }
        for key in w.index_keys_removed.iter().chain(&w.index_keys_added) {
            self.stab_key(key, out);
        }
    }

    /// Adds every subscription whose read set covers `key`
    /// ([`ReadSet::covers`]).
    pub fn stab_key(&self, key: &[u8], out: &mut HashSet<SubId>) {
        if let Some(ids) = self.points.get(key) {
            out.extend(ids.iter().copied());
        }
        stab(&self.tree, key, out);
    }

    /// Checks the tree's invariants (order, balance, subtree maxima, the
    /// range count); for tests.
    #[doc(hidden)]
    pub fn check_invariants(&self) -> Result<(), String> {
        fn walk<'a>(
            link: &'a Link,
            prev: &mut Option<(&'a [u8], &'a Upper, SubId)>,
            count: &mut usize,
        ) -> Result<(u32, Option<Upper>), String> {
            let Some(node) = link else {
                return Ok((0, None));
            };
            let (lh, lmax) = walk(&node.left, prev, count)?;
            let key = (node.lo.as_slice(), &node.hi, node.id);
            if let Some(p) = prev
                && *p >= key
            {
                return Err("the tree is out of order".into());
            }
            *prev = Some(key);
            *count += 1;
            let (rh, rmax) = walk(&node.right, prev, count)?;
            if lh.abs_diff(rh) > 1 {
                return Err(format!("unbalanced: heights {lh} and {rh}"));
            }
            if node.height != 1 + lh.max(rh) {
                return Err("a stale height".into());
            }
            let max = [Some(node.hi.clone()), lmax, rmax]
                .into_iter()
                .flatten()
                .max();
            if max.as_ref() != Some(&node.max) {
                return Err("a stale subtree maximum".into());
            }
            Ok((node.height, max))
        }
        let mut count = 0;
        walk(&self.tree, &mut None, &mut count)?;
        if count != self.ranges {
            return Err(format!("{count} nodes, {} counted", self.ranges));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn range(lo: &[u8], hi: &[u8]) -> KeyRange {
        KeyRange {
            lo: lo.to_vec(),
            hi: hi.to_vec(),
        }
    }

    #[test]
    fn stab_finds_points_bounded_and_unbounded_ranges() {
        let mut index = ReadSetIndex::new(AppKeys::dedicated());
        let mut a = ReadSet::default();
        a.points.insert(vec![5]);
        a.ranges.push(range(&[1], &[3]));
        let mut b = ReadSet::default();
        b.ranges.push(range(&[2], &[]));
        b.ranges.push(range(&[9], &[4])); // empty
        index.insert(SubId(1), &a);
        index.insert(SubId(2), &b);
        let hits = |index: &ReadSetIndex, key: &[u8]| {
            let mut out = HashSet::new();
            index.stab_key(key, &mut out);
            let mut v: Vec<u64> = out.into_iter().map(|s| s.0).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(hits(&index, &[0]), Vec::<u64>::new());
        assert_eq!(hits(&index, &[1]), vec![1]);
        assert_eq!(hits(&index, &[2, 7]), vec![1, 2]);
        assert_eq!(hits(&index, &[3]), vec![2]);
        assert_eq!(hits(&index, &[5]), vec![1, 2]);
        assert_eq!(hits(&index, &[255, 255]), vec![2]);
        index.check_invariants().expect("invariants");
        index.remove(SubId(2));
        assert_eq!(hits(&index, &[3]), Vec::<u64>::new());
        index.check_invariants().expect("invariants");
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u64, Vec<(Vec<u8>, Vec<u8>)>),
        Remove(u64),
    }

    fn key() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(0u8..4, 0..3)
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0u64..24, prop::collection::vec((key(), key()), 0..4)).prop_map(|(id, r)| Op::Insert(id, r)),
            1 => (0u64..24).prop_map(Op::Remove),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The tree stays ordered, balanced and correctly augmented under
        /// any sequence of inserts and removes.
        #[test]
        fn tree_invariants_hold(ops in prop::collection::vec(op(), 0..120)) {
            let mut index = ReadSetIndex::new(AppKeys::dedicated());
            for op in ops {
                match op {
                    Op::Insert(id, ranges) => {
                        let rs = ReadSet {
                            points: Default::default(),
                            ranges: ranges.into_iter().map(|(lo, hi)| KeyRange { lo, hi }).collect(),
                        };
                        index.insert(SubId(id), &rs);
                    }
                    Op::Remove(id) => index.remove(SubId(id)),
                }
                prop_assert_eq!(index.check_invariants(), Ok(()));
            }
        }
    }
}
