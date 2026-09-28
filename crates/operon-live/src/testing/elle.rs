//! An Elle-style checker of list-append histories for snapshot isolation
//! (R1 plan Task 16 semantics 2; Kingsbury and Alvaro, "Elle: Inferring
//! Isolation Anomalies from Experimental Observations", VLDB 2020).
//!
//! A history is a list of transactions over keys that hold lists. Every
//! append writes an element no other append writes, so a read of a list
//! names the transaction behind each element, and the longest read of a key
//! gives the key's version order (every other read must be a prefix of it).
//! From the order and the reads the checker infers the dependency edges
//! between transactions:
//! - `ww`: `a` appended the element right before `b`'s;
//! - `wr`: `b` read a list whose last element `a` appended;
//! - `rw`: `a` read a list that `b`'s append extended next.
//!
//! and reports:
//!
//! | Anomaly | Meaning |
//! |---|---|
//! | `G0` | a cycle of `ww` edges (dirty write) |
//! | `G1a` | a committed read saw an element of a failed transaction (aborted read) |
//! | `G1b` | a committed read saw an element that was not its writer's last append to the key (intermediate read) |
//! | `G1c` | a cycle of `ww` and `wr` edges (circular information flow) |
//! | `LostUpdate` | two committed transactions read the same version of a key and both appended to it |
//! | `GSingle` | a cycle with exactly one `rw` edge (read skew) |
//! | `G2` | a cycle with two or more `rw` edges (write skew) |
//! | `LostAppend` | a read made after every other transaction completed misses a committed append |
//! | `Duplicate` | a read holds one element twice (an append applied twice) |
//! | `IncompatibleOrder` | two reads of a key are not prefixes of one another |
//! | `Internal` | a transaction's read misses its own earlier appends |
//! | `Garbage` | a read holds an element no transaction appended |
//!
//! Snapshot isolation forbids all of them but `G2`. With point reads only
//! (Live's `get` locks the documents it reads, D118) `G2` cannot occur
//! either, so the transaction checker allows nothing; a workload with range
//! reads may allow `G2` ([`Report::violations`]).
//!
//! Transactions whose outcome is unknown (`Info`) take part through the
//! elements of theirs that reads observed; their own reads are not trusted.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;

/// A key of the list-append workload.
pub type Key = u64;
/// An element appended to a list; unique across the history.
pub type Elem = u64;

/// One operation of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Appends `elem` to the list at `key`.
    Append { key: Key, elem: Elem },
    /// Reads the list at `key`; `None` when the result is unknown (the
    /// transaction failed or its outcome is unknown).
    Read { key: Key, list: Option<Vec<Elem>> },
}

/// How a transaction ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Committed.
    Ok,
    /// Certainly did not commit.
    Fail,
    /// May or may not have committed.
    Info,
}

/// One transaction of a history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Txn {
    pub ops: Vec<Op>,
    pub outcome: Outcome,
    /// Invoked after every other transaction of the history completed: its
    /// reads must hold every committed append to their keys.
    pub final_read: bool,
}

impl Txn {
    /// A transaction with `outcome` that is not a final read.
    pub fn new(ops: Vec<Op>, outcome: Outcome) -> Self {
        Txn {
            ops,
            outcome,
            final_read: false,
        }
    }
}

/// A kind of anomaly (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    G0,
    G1a,
    G1b,
    G1c,
    LostUpdate,
    GSingle,
    G2,
    LostAppend,
    Duplicate,
    IncompatibleOrder,
    Internal,
    Garbage,
}

/// One anomaly: its kind, the transactions (indices into the history) and a
/// description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anomaly {
    pub kind: Kind,
    pub txns: Vec<usize>,
    pub detail: String,
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} (txns {:?}): {}", self.kind, self.txns, self.detail)
    }
}

/// What [`check`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub anomalies: Vec<Anomaly>,
    /// Transactions in the history, and how many committed.
    pub txns: usize,
    pub committed: usize,
    /// Inferred edges: `ww`, `wr`, `rw`.
    pub edges: [usize; 3],
}

impl Report {
    /// The kinds found.
    pub fn kinds(&self) -> BTreeSet<Kind> {
        self.anomalies.iter().map(|a| a.kind).collect()
    }

    /// The anomalies whose kind is not in `allowed`.
    pub fn violations(&self, allowed: &[Kind]) -> Vec<&Anomaly> {
        self.anomalies
            .iter()
            .filter(|a| !allowed.contains(&a.kind))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Edge {
    Ww,
    Wr,
    Rw,
}

impl Edge {
    fn name(self) -> &'static str {
        match self {
            Edge::Ww => "ww",
            Edge::Wr => "wr",
            Edge::Rw => "rw",
        }
    }
}

/// A read a transaction made of a key before appending to it.
struct ExternalRead<'a> {
    txn: usize,
    key: Key,
    list: &'a [Elem],
}

/// Checks `history` (see the module docs). At most a few anomalies of each
/// cycle kind are reported per strongly connected component.
pub fn check(history: &[Txn]) -> Report {
    let mut anomalies = Vec::new();
    let mut push = |kind: Kind, txns: Vec<usize>, detail: String| {
        anomalies.push(Anomaly { kind, txns, detail });
    };

    // Who appended each element, and each transaction's appends per key.
    let mut writer: HashMap<(Key, Elem), usize> = HashMap::new();
    let mut appends: HashMap<(usize, Key), Vec<Elem>> = HashMap::new();
    for (i, t) in history.iter().enumerate() {
        for op in &t.ops {
            if let Op::Append { key, elem } = op {
                if let Some(first) = writer.insert((*key, *elem), i) {
                    push(
                        Kind::Garbage,
                        vec![first, i],
                        format!("element {elem} of key {key} appended twice by the workload"),
                    );
                }
                appends.entry((i, *key)).or_default().push(*elem);
            }
        }
    }

    // Committed reads: each list is checked on its own, then kept for the
    // version order; external reads also feed the edges.
    let mut reads: HashMap<Key, Vec<(usize, &[Elem])>> = HashMap::new();
    let mut external: Vec<ExternalRead<'_>> = Vec::new();
    for (i, t) in history.iter().enumerate() {
        if t.outcome != Outcome::Ok {
            continue;
        }
        let mut own: HashMap<Key, Vec<Elem>> = HashMap::new();
        for op in &t.ops {
            match op {
                Op::Append { key, elem } => own.entry(*key).or_default().push(*elem),
                Op::Read { list: None, .. } => {}
                Op::Read {
                    key,
                    list: Some(list),
                } => {
                    let mut seen = HashSet::new();
                    for e in list {
                        if !seen.insert(*e) {
                            push(
                                Kind::Duplicate,
                                vec![i],
                                format!("key {key} holds element {e} twice: {list:?}"),
                            );
                        }
                        match writer.get(&(*key, *e)) {
                            None => push(
                                Kind::Garbage,
                                vec![i],
                                format!("key {key} holds element {e}, which nothing appended"),
                            ),
                            Some(&w) if history[w].outcome == Outcome::Fail => push(
                                Kind::G1a,
                                vec![w, i],
                                format!(
                                    "txn {i} read element {e} of key {key}, appended by failed txn {w}"
                                ),
                            ),
                            Some(_) => {}
                        }
                    }
                    match own.get(key) {
                        Some(mine) => {
                            if !list.ends_with(mine) {
                                push(
                                    Kind::Internal,
                                    vec![i],
                                    format!(
                                        "txn {i} appended {mine:?} to key {key} and then read {list:?}"
                                    ),
                                );
                            }
                        }
                        None => external.push(ExternalRead {
                            txn: i,
                            key: *key,
                            list,
                        }),
                    }
                    reads.entry(*key).or_default().push((i, list));
                }
            }
        }
    }

    // Version orders: the longest committed read of each key; every other
    // read must be its prefix.
    let mut order: HashMap<Key, Vec<Elem>> = HashMap::new();
    let mut incompatible: HashSet<(usize, Key)> = HashSet::new();
    let mut keys: Vec<Key> = reads.keys().copied().collect();
    keys.sort_unstable();
    for key in &keys {
        let rs = &reads[key];
        let (_, longest) = rs
            .iter()
            .max_by_key(|(_, l)| l.len())
            .expect("a key with reads");
        for (t, l) in rs {
            if !longest.starts_with(l) {
                incompatible.insert((*t, *key));
                let other = rs
                    .iter()
                    .find(|(_, x)| x == longest)
                    .map_or(*t, |(o, _)| *o);
                push(
                    Kind::IncompatibleOrder,
                    vec![other, *t],
                    format!("key {key}: {l:?} is not a prefix of {longest:?}"),
                );
            }
        }
        order.insert(*key, longest.to_vec());
    }

    // G1b: an external read ending in an element its writer later
    // overwrote with another append to the same key.
    for r in &external {
        let Some(last) = r.list.last() else { continue };
        let Some(&w) = writer.get(&(r.key, *last)) else {
            continue;
        };
        if w == r.txn {
            continue;
        }
        if let Some(mine) = appends.get(&(w, r.key))
            && mine.last() != Some(last)
        {
            push(
                Kind::G1b,
                vec![w, r.txn],
                format!(
                    "txn {} read key {} ending in {last}, an intermediate append of txn {w} ({mine:?})",
                    r.txn, r.key
                ),
            );
        }
    }

    // Lost appends: final reads hold every committed append to their keys.
    for r in &external {
        if !history[r.txn].final_read {
            continue;
        }
        let held: HashSet<Elem> = r.list.iter().copied().collect();
        let mut missing: Vec<(usize, Elem)> = appends
            .iter()
            .filter(|((t, k), _)| *k == r.key && history[*t].outcome == Outcome::Ok)
            .flat_map(|((t, _), es)| es.iter().map(move |e| (*t, *e)))
            .filter(|(_, e)| !held.contains(e))
            .collect();
        missing.sort_unstable();
        if !missing.is_empty() {
            let mut txns: Vec<usize> = missing.iter().map(|(t, _)| *t).collect();
            txns.dedup();
            txns.push(r.txn);
            push(
                Kind::LostAppend,
                txns,
                format!(
                    "the final read of key {} misses committed appends {:?}",
                    r.key,
                    missing.iter().map(|(_, e)| *e).collect::<Vec<_>>()
                ),
            );
        }
    }

    // Lost updates: two committed read-then-append transactions that read
    // the same version of a key.
    let mut by_version: HashMap<(Key, usize), Vec<usize>> = HashMap::new();
    for r in &external {
        if incompatible.contains(&(r.txn, r.key)) || !appends.contains_key(&(r.txn, r.key)) {
            continue;
        }
        by_version
            .entry((r.key, r.list.len()))
            .or_default()
            .push(r.txn);
    }
    let mut lost: Vec<_> = by_version
        .into_iter()
        .filter(|(_, t)| t.len() > 1)
        .collect();
    lost.sort_unstable();
    for ((key, len), mut txns) in lost {
        txns.sort_unstable();
        txns.dedup();
        if txns.len() > 1 {
            push(
                Kind::LostUpdate,
                txns,
                format!("key {key}: these txns all read its first {len} element(s) and appended"),
            );
        }
    }

    // The dependency graph.
    let mut graph = Graph::default();
    for key in &keys {
        let v = &order[key];
        let w = |i: usize| writer.get(&(*key, v[i])).copied();
        for i in 1..v.len() {
            if let (Some(a), Some(b)) = (w(i - 1), w(i))
                && a != b
            {
                graph.add(a, b, Edge::Ww);
            }
        }
    }
    for r in &external {
        if incompatible.contains(&(r.txn, r.key)) {
            continue;
        }
        let v = &order[&r.key];
        let m = r.list.len();
        if m > 0
            && let Some(&w) = writer.get(&(r.key, v[m - 1]))
            && w != r.txn
        {
            graph.add(w, r.txn, Edge::Wr);
        }
        if m < v.len()
            && let Some(&w) = writer.get(&(r.key, v[m]))
            && w != r.txn
        {
            graph.add(r.txn, w, Edge::Rw);
        }
    }
    let edges = graph.counts();

    for scc in graph.sccs(|_| true) {
        let members: HashSet<usize> = scc.iter().copied().collect();
        let inside =
            |e: Edge, allowed: &[Edge], b: usize| allowed.contains(&e) && members.contains(&b);
        let mut found = false;
        // G0 and G1c: cycles without rw edges.
        for (kind, allowed) in [
            (Kind::G0, &[Edge::Ww][..]),
            (Kind::G1c, &[Edge::Ww, Edge::Wr][..]),
        ] {
            if found {
                break;
            }
            for sub in graph.sccs(|e| allowed.contains(&e)) {
                if !sub.iter().all(|n| members.contains(n)) {
                    continue;
                }
                let start = sub[0];
                if let Some(path) = graph.path(start, start, |e, b| inside(e, allowed, b)) {
                    push(kind, cycle_txns(&path), describe(&path));
                    found = true;
                    break;
                }
            }
        }
        if found {
            continue;
        }
        // G-single: one rw edge a → b closed by ww/wr edges from b to a.
        let mut singles = 0;
        for &a in &scc {
            for &(b, e) in graph.out(a) {
                if e != Edge::Rw || !members.contains(&b) {
                    continue;
                }
                if let Some(back) = graph.path(b, a, |e, n| inside(e, &[Edge::Ww, Edge::Wr], n)) {
                    let mut path = vec![(a, Edge::Rw, b)];
                    path.extend(back);
                    push(Kind::GSingle, cycle_txns(&path), describe(&path));
                    singles += 1;
                    if singles >= 3 {
                        break;
                    }
                }
            }
            if singles >= 3 {
                break;
            }
        }
        if singles > 0 {
            continue;
        }
        // Otherwise every cycle here has two or more rw edges.
        let start = scc[0];
        let path = graph
            .path(start, start, |_, b| members.contains(&b))
            .expect("a strongly connected component has a cycle");
        push(Kind::G2, cycle_txns(&path), describe(&path));
    }
    Report {
        anomalies,
        txns: history.len(),
        committed: history.iter().filter(|t| t.outcome == Outcome::Ok).count(),
        edges,
    }
}

fn cycle_txns(path: &[(usize, Edge, usize)]) -> Vec<usize> {
    path.iter().map(|(a, _, _)| *a).collect()
}

fn describe(path: &[(usize, Edge, usize)]) -> String {
    let mut s = String::from("cycle");
    for (a, e, b) in path {
        s.push_str(&format!(" {a} -{}-> {b}", e.name()));
    }
    s
}

/// The dependency graph: typed edges between transactions.
#[derive(Default)]
struct Graph {
    out: HashMap<usize, Vec<(usize, Edge)>>,
    seen: HashSet<(usize, usize, Edge)>,
}

impl Graph {
    fn add(&mut self, a: usize, b: usize, e: Edge) {
        if self.seen.insert((a, b, e)) {
            self.out.entry(a).or_default().push((b, e));
            self.out.entry(b).or_default();
        }
    }

    fn out(&self, a: usize) -> &[(usize, Edge)] {
        self.out.get(&a).map_or(&[], Vec::as_slice)
    }

    fn counts(&self) -> [usize; 3] {
        let mut c = [0; 3];
        for (_, _, e) in &self.seen {
            c[match e {
                Edge::Ww => 0,
                Edge::Wr => 1,
                Edge::Rw => 2,
            }] += 1;
        }
        c
    }

    /// The strongly connected components with more than one node, over the
    /// edges `keep` accepts (Kosaraju, iterative).
    fn sccs(&self, keep: impl Fn(Edge) -> bool) -> Vec<Vec<usize>> {
        let mut nodes: Vec<usize> = self.out.keys().copied().collect();
        nodes.sort_unstable();
        let mut reverse: HashMap<usize, Vec<usize>> = HashMap::new();
        for (&a, es) in &self.out {
            for &(b, e) in es {
                if keep(e) {
                    reverse.entry(b).or_default().push(a);
                }
            }
        }
        // First pass: finish order.
        let mut visited = HashSet::new();
        let mut finished = Vec::with_capacity(nodes.len());
        for &n in &nodes {
            if !visited.insert(n) {
                continue;
            }
            let mut stack = vec![(n, 0usize)];
            while let Some((u, i)) = stack.pop() {
                let es = self.out(u);
                if let Some(&(v, e)) = es.get(i) {
                    stack.push((u, i + 1));
                    if keep(e) && visited.insert(v) {
                        stack.push((v, 0));
                    }
                } else {
                    finished.push(u);
                }
            }
        }
        // Second pass on the reverse graph, in reverse finish order.
        let mut assigned = HashSet::new();
        let mut out = Vec::new();
        for &n in finished.iter().rev() {
            if !assigned.insert(n) {
                continue;
            }
            let mut comp = vec![n];
            let mut stack = vec![n];
            while let Some(u) = stack.pop() {
                for &v in reverse.get(&u).map_or(&[][..], Vec::as_slice) {
                    if assigned.insert(v) {
                        comp.push(v);
                        stack.push(v);
                    }
                }
            }
            if comp.len() > 1 {
                comp.sort_unstable();
                out.push(comp);
            }
        }
        out
    }

    /// A shortest path of at least one edge from `from` to `to` over the
    /// edges `ok(edge, target)` accepts, as (from, edge, to) steps.
    fn path(
        &self,
        from: usize,
        to: usize,
        ok: impl Fn(Edge, usize) -> bool,
    ) -> Option<Vec<(usize, Edge, usize)>> {
        let mut prev: HashMap<usize, (usize, Edge)> = HashMap::new();
        let mut queue = VecDeque::new();
        for &(v, e) in self.out(from) {
            if ok(e, v) && !prev.contains_key(&v) {
                prev.insert(v, (from, e));
                queue.push_back(v);
            }
        }
        while let Some(u) = queue.pop_front() {
            if u == to {
                let mut path = Vec::new();
                let mut cur = to;
                loop {
                    let (p, e) = prev[&cur];
                    path.push((p, e, cur));
                    cur = p;
                    if cur == from {
                        break;
                    }
                }
                path.reverse();
                return Some(path);
            }
            for &(v, e) in self.out(u) {
                if ok(e, v) && !prev.contains_key(&v) {
                    prev.insert(v, (u, e));
                    queue.push_back(v);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(key: Key, elem: Elem) -> Op {
        Op::Append { key, elem }
    }

    fn r(key: Key, list: &[Elem]) -> Op {
        Op::Read {
            key,
            list: Some(list.to_vec()),
        }
    }

    fn ok(ops: Vec<Op>) -> Txn {
        Txn::new(ops, Outcome::Ok)
    }

    fn last(ops: Vec<Op>) -> Txn {
        Txn {
            final_read: true,
            ..ok(ops)
        }
    }

    fn kinds(h: &[Txn]) -> BTreeSet<Kind> {
        check(h).kinds()
    }

    #[test]
    fn a_serial_history_is_clean() {
        let h = vec![
            ok(vec![r(1, &[]), a(1, 1)]),
            ok(vec![r(1, &[1]), a(1, 2), a(2, 3)]),
            ok(vec![r(2, &[3]), r(1, &[1, 2])]),
            Txn::new(vec![a(1, 9), r(1, &[1, 2, 9])], Outcome::Fail),
            Txn::new(vec![a(2, 10)], Outcome::Info),
            last(vec![r(1, &[1, 2]), r(2, &[3])]),
        ];
        let report = check(&h);
        assert!(report.anomalies.is_empty(), "{:?}", report.anomalies);
        assert_eq!(report.committed, 4);
        assert!(report.edges[0] >= 1 && report.edges[1] >= 2);
    }

    #[test]
    fn an_unknown_outcome_that_was_observed_joins_the_order() {
        let h = vec![
            Txn::new(vec![a(1, 1)], Outcome::Info),
            ok(vec![r(1, &[1]), a(1, 2)]),
            last(vec![r(1, &[1, 2])]),
        ];
        assert!(check(&h).anomalies.is_empty());
    }

    #[test]
    fn aborted_read_is_g1a() {
        let h = vec![Txn::new(vec![a(1, 1)], Outcome::Fail), ok(vec![r(1, &[1])])];
        assert!(kinds(&h).contains(&Kind::G1a));
    }

    #[test]
    fn intermediate_read_is_g1b() {
        let h = vec![
            ok(vec![a(1, 1), a(1, 2)]),
            ok(vec![r(1, &[1])]),
            last(vec![r(1, &[1, 2])]),
        ];
        assert!(kinds(&h).contains(&Kind::G1b));
    }

    #[test]
    fn dirty_write_cycle_is_g0() {
        // x: 1 (t0) then 2 (t1); y: 3 (t1) then 4 (t0).
        let h = vec![
            ok(vec![a(1, 1), a(2, 4)]),
            ok(vec![a(1, 2), a(2, 3)]),
            last(vec![r(1, &[1, 2]), r(2, &[3, 4])]),
        ];
        assert_eq!(kinds(&h), BTreeSet::from([Kind::G0]));
    }

    #[test]
    fn circular_information_flow_is_g1c() {
        let h = vec![ok(vec![a(1, 1), r(2, &[2])]), ok(vec![r(1, &[1]), a(2, 2)])];
        assert_eq!(kinds(&h), BTreeSet::from([Kind::G1c]));
    }

    #[test]
    fn lost_update_is_found_as_lost_update_and_g_single() {
        let h = vec![
            ok(vec![a(1, 1)]),
            ok(vec![r(1, &[1]), a(1, 2)]),
            ok(vec![r(1, &[1]), a(1, 3)]),
            last(vec![r(1, &[1, 2, 3])]),
        ];
        let k = kinds(&h);
        assert!(k.contains(&Kind::LostUpdate), "{k:?}");
        assert!(k.contains(&Kind::GSingle), "{k:?}");
    }

    #[test]
    fn read_skew_is_g_single() {
        // t2 reads x before t1's append and y after it.
        let h = vec![
            ok(vec![a(1, 1), a(2, 2)]),
            ok(vec![r(1, &[]), r(2, &[2])]),
            last(vec![r(1, &[1]), r(2, &[2])]),
        ];
        assert_eq!(kinds(&h), BTreeSet::from([Kind::GSingle]));
    }

    #[test]
    fn write_skew_is_g2_and_only_g2() {
        let h = vec![
            ok(vec![r(1, &[]), r(2, &[]), a(1, 1)]),
            ok(vec![r(1, &[]), r(2, &[]), a(2, 2)]),
            last(vec![r(1, &[1]), r(2, &[2])]),
        ];
        let report = check(&h);
        assert_eq!(report.kinds(), BTreeSet::from([Kind::G2]));
        assert!(report.violations(&[Kind::G2]).is_empty());
        assert_eq!(report.violations(&[]).len(), 1);
    }

    #[test]
    fn a_final_read_missing_a_committed_append_is_a_lost_append() {
        let h = vec![ok(vec![a(1, 1)]), ok(vec![a(1, 2)]), last(vec![r(1, &[1])])];
        assert!(kinds(&h).contains(&Kind::LostAppend));
    }

    #[test]
    fn duplicates_incompatible_orders_internal_and_garbage_reads() {
        assert!(kinds(&[ok(vec![a(1, 1)]), ok(vec![r(1, &[1, 1])])]).contains(&Kind::Duplicate));
        assert!(
            kinds(&[
                ok(vec![a(1, 1)]),
                ok(vec![a(1, 2)]),
                ok(vec![a(1, 3)]),
                ok(vec![r(1, &[1, 2])]),
                ok(vec![r(1, &[1, 3])]),
            ])
            .contains(&Kind::IncompatibleOrder)
        );
        assert!(
            kinds(&[ok(vec![a(1, 5), r(1, &[1])]), ok(vec![a(1, 1)])]).contains(&Kind::Internal)
        );
        assert!(kinds(&[ok(vec![r(1, &[7])])]).contains(&Kind::Garbage));
    }

    #[test]
    fn a_long_clean_history_checks_quickly() {
        // 2 000 serial transactions over 4 keys.
        let mut h = Vec::new();
        let mut lists: Vec<Vec<Elem>> = vec![Vec::new(); 4];
        for i in 0..2_000u64 {
            let k = i % 4;
            let seen = lists[k as usize].clone();
            lists[k as usize].push(i);
            h.push(ok(vec![r(k, &seen), a(k, i)]));
        }
        h.push(last((0..4).map(|k| r(k, &lists[k as usize])).collect()));
        let started = std::time::Instant::now();
        let report = check(&h);
        assert!(report.anomalies.is_empty(), "{:?}", &report.anomalies[..1]);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
