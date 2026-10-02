//! A Wing–Gong–Lowe linearizability checker for recorded histories (M0.4
//! plan ruling 6).
//!
//! A history is a set of operations, each with the client that issued it,
//! its invoke and complete times (from one clock that orders events, such as
//! a logical counter), its input, and its outcome. An operation whose
//! outcome is unknown (a timeout, a lost acknowledgement) is
//! [`Outcome::Indeterminate`]: it may have taken effect at any point after
//! its invocation, or never. The checker gives it a completion time of
//! `u64::MAX` and accepts any output from it, so it may be linearized
//! anywhere after its invocation, including after everything else, which is
//! the same as not at all (review focus 5).
//!
//! The search is Wing and Gong's, with Lowe's memoisation of `(linearized
//! set, model state)` pairs. It is exponential in the worst case; keep
//! histories to a few hundred operations per object.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

/// How an operation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<R> {
    Ok(R),
    /// Timed out or otherwise unknown: it may or may not have taken effect.
    Indeterminate,
}

/// One recorded operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op<I, R> {
    pub client: u32,
    pub invoke: u64,
    /// `u64::MAX` if indeterminate.
    pub complete: u64,
    pub input: I,
    pub outcome: Outcome<R>,
}

/// A sequential specification. `step` must be total: every input has an
/// output in every state.
pub trait Model: Clone + Eq + Hash {
    type Input: Debug;
    type Output: Debug + PartialEq;
    fn step(&self, input: &Self::Input) -> (Self, Self::Output);
}

/// A history that no sequential order explains.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// The index of an operation the search could not place.
    pub op: usize,
    /// The most operations any explored linearization placed.
    pub longest_prefix: usize,
    pub message: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Bits(Vec<u64>);

impl Bits {
    fn new(n: usize) -> Self {
        Self(vec![0; n.div_ceil(64)])
    }
    fn set(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }
    fn clear(&mut self, i: usize) {
        self.0[i / 64] &= !(1 << (i % 64));
    }
}

const NIL: usize = usize::MAX;

/// Checks that `history` is linearizable with respect to `initial`.
pub fn check<M: Model>(initial: M, history: &[Op<M::Input, M::Output>]) -> Result<(), Violation> {
    let n = history.len();
    if n == 0 {
        return Ok(());
    }
    // Entries: 2i is op i's call, 2i + 1 its return. Sort by time, calls
    // before returns at equal times (ties are treated as concurrent).
    let mut order: Vec<usize> = (0..2 * n).collect();
    let time = |e: usize| {
        let op = &history[e / 2];
        if e.is_multiple_of(2) {
            (op.invoke, 0u8)
        } else {
            let complete = match op.outcome {
                Outcome::Indeterminate => u64::MAX,
                Outcome::Ok(_) => op.complete,
            };
            (complete, 1u8)
        }
    };
    order.sort_by_key(|&e| (time(e), e));
    // A doubly linked list over `order`, with a head sentinel at index 2n.
    let head = 2 * n;
    let mut next = vec![NIL; 2 * n + 1];
    let mut prev = vec![NIL; 2 * n + 1];
    let mut last = head;
    for &e in &order {
        next[last] = e;
        prev[e] = last;
        last = e;
    }
    let lift = |next: &mut Vec<usize>, prev: &mut Vec<usize>, op: usize| {
        for e in [2 * op, 2 * op + 1] {
            let (p, q) = (prev[e], next[e]);
            next[p] = q;
            if q != NIL {
                prev[q] = p;
            }
        }
    };
    let unlift = |next: &mut Vec<usize>, prev: &mut Vec<usize>, op: usize| {
        for e in [2 * op + 1, 2 * op] {
            let (p, q) = (prev[e], next[e]);
            next[p] = e;
            if q != NIL {
                prev[q] = e;
            }
        }
    };

    let mut state = initial;
    let mut linearized = Bits::new(n);
    let mut cache: HashSet<(Bits, M)> = HashSet::new();
    let mut stack: Vec<(usize, M)> = Vec::new();
    let mut longest = 0usize;
    let mut entry = next[head];
    while next[head] != NIL {
        if entry == NIL {
            // Unreachable: the list ends with return entries, which stop the
            // walk above. Treat it as a dead end.
            return Err(Violation {
                op: 0,
                longest_prefix: longest,
                message: "the search ran off the end of the history".to_string(),
            });
        }
        let op = entry / 2;
        if entry.is_multiple_of(2) {
            let record = &history[op];
            let (after, output) = state.step(&record.input);
            let matches = match &record.outcome {
                Outcome::Ok(expected) => *expected == output,
                Outcome::Indeterminate => true,
            };
            if matches {
                linearized.set(op);
                if cache.insert((linearized.clone(), after.clone())) {
                    let before = std::mem::replace(&mut state, after);
                    stack.push((op, before));
                    longest = longest.max(stack.len());
                    lift(&mut next, &mut prev, op);
                    entry = next[head];
                    continue;
                }
                linearized.clear(op);
            }
            entry = next[entry];
        } else {
            // A return with its operation not yet placed: undo the last choice.
            let Some((undone, before)) = stack.pop() else {
                return Err(Violation {
                    op,
                    longest_prefix: longest,
                    message: format!(
                        "not linearizable: no order places op {op} ({:?} -> {:?}, client {}, \
                         invoked {}, completed {}); the longest linearizable prefix has \
                         {longest} of {n} ops",
                        history[op].input,
                        history[op].outcome,
                        history[op].client,
                        history[op].invoke,
                        history[op].complete
                    ),
                });
            };
            linearized.clear(undone);
            state = before;
            unlift(&mut next, &mut prev, undone);
            entry = next[2 * undone];
        }
    }
    Ok(())
}

/// Input of the per-partition sequencer model.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SequencerInput {
    /// Commit a WAL chunk of `records` records under `object`.
    Commit { object: String, records: u32 },
    /// Read the high watermark.
    ReadHwm,
}

/// Output of the per-partition sequencer model.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SequencerOutput {
    BaseOffset(u64),
    Hwm(u64),
}

/// One partition's sequencer: commits get dense base offsets, a repeated
/// commit of the same object gets its first base offset back and changes
/// nothing, and reads return the high watermark.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct SequencerModel {
    next: u64,
    committed: std::collections::BTreeMap<String, u64>,
}

impl Model for SequencerModel {
    type Input = SequencerInput;
    type Output = SequencerOutput;

    fn step(&self, input: &SequencerInput) -> (Self, SequencerOutput) {
        match input {
            SequencerInput::ReadHwm => (self.clone(), SequencerOutput::Hwm(self.next)),
            SequencerInput::Commit { object, records } => {
                if let Some(base) = self.committed.get(object) {
                    return (self.clone(), SequencerOutput::BaseOffset(*base));
                }
                let mut after = self.clone();
                after.committed.insert(object.clone(), self.next);
                after.next += u64::from(*records);
                (after, SequencerOutput::BaseOffset(self.next))
            }
        }
    }
}

/// Input of the versioned CAS register model.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CasInput {
    /// Set the value if the current version is `expected` (`None`: unset).
    Cas {
        expected: Option<u64>,
        value: String,
    },
    Read,
}

/// Output of the versioned CAS register model.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CasOutput {
    /// The new version.
    Ok(u64),
    /// The current `(version, value)`.
    Mismatch(Option<(u64, String)>),
    Read(Option<(u64, String)>),
}

/// A manifest pointer: a versioned register with compare-and-swap.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CasRegisterModel {
    current: Option<(u64, String)>,
}

impl Model for CasRegisterModel {
    type Input = CasInput;
    type Output = CasOutput;

    fn step(&self, input: &CasInput) -> (Self, CasOutput) {
        match input {
            CasInput::Read => (self.clone(), CasOutput::Read(self.current.clone())),
            CasInput::Cas { expected, value } => {
                let version = self.current.as_ref().map(|(v, _)| *v);
                if version == *expected {
                    let next = version.map_or(1, |v| v + 1);
                    (
                        Self {
                            current: Some((next, value.clone())),
                        },
                        CasOutput::Ok(next),
                    )
                } else {
                    (self.clone(), CasOutput::Mismatch(self.current.clone()))
                }
            }
        }
    }
}
