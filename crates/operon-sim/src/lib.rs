//! Operon's seeded cluster simulation (M0.4 plan Task 6, ruling 1) and a
//! linearizability checker for the histories it records (ruling 6).
//!
//! [`run`] builds an in-process cluster (meta nodes over a
//! [`Router`](operon_meta::Router), log writers, a reader, a worker running
//! the segmenter, retention, link apply and GC, all over a
//! [`FaultyStore::random`](operon_store::FaultyStore::random) store), drives
//! a seeded workload with meta node isolation, worker restarts and store
//! fault bursts on a single-threaded runtime, records every client
//! operation, and checks:
//! 1. linearizability of every partition's sequencer and every CAS register;
//! 2. every acknowledged append is readable at its offset, exactly once;
//! 3. the link's `CounterTable` equals the model;
//! 4. the metastore invariants on every node, which all hold the same
//!    state, and no node stopped on a fatal Raft error.
//!
//! The simulation is seeded, not deterministic: openraft, redb and the
//! object store do real I/O and use real time, so a failing seed may not
//! replay exactly. A failure report carries the seed and the full schedule.

pub mod linearizability;
mod sim;

pub use sim::{Event, Histories, SimConfig, SimReport, SimStats, run};
