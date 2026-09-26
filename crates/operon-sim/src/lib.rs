//! Operon's seeded cluster simulation (M0.4 plan Task 6, ruling 1) and a
//! linearizability checker for the histories it records (ruling 6).
//!
//! [`run`] builds an in-process cluster (meta nodes over a
//! [`Router`](operon_meta::Router), log writers, a reader, a collection, a
//! worker running the segmenter, retention, link apply (counter and
//! collection targets) and GC, all over a
//! [`FaultyStore::random`](operon_store::FaultyStore::random) store), drives
//! a seeded workload with meta node isolation, worker restarts and store
//! fault bursts on a single-threaded runtime, records every client
//! operation, and checks:
//! 1. linearizability of every partition's sequencer and every CAS register;
//! 2. every acknowledged append is readable at its offset, exactly once;
//! 3. the link's `CounterTable` equals the model;
//! 4. the metastore invariants on every node, which all hold the same
//!    state, and no node stopped on a fatal Raft error;
//! 5. no index entry or pointer names a missing object;
//! 6. the collection equals the fold of its implicit stream, every
//!    acknowledged document write is in the stream at its offsets, every
//!    upsert and patch is there at most once (and never one of a failed
//!    write), and the live collection manifest names no missing object
//!    (plan M1.1 Task 13).
//!
//! The simulation is seeded, not deterministic: openraft, redb and the
//! object store do real I/O and use real time, so a failing seed may not
//! replay exactly. The event schedule, though, is a function of the seed
//! alone. A failure report carries the seed and the full schedule.

mod sim;

/// The checker, which lives in `operon-meta-conformance` so a metastore
/// backend can use it without this crate (M1.2a Ruling 14).
pub use operon_meta_conformance::linearizability;

pub use sim::{Event, Histories, SimConfig, SimReport, SimStats, run};
