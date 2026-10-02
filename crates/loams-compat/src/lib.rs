//! `compat-replay`: the replay and classification half of the router compatibility inventory
//! (design §31 §15, RT0 plan Tasks 5 and 6).
//!
//! A capture (JSON lines of statements a router sent to its backend) is replayed against a
//! reference engine and a target engine; each statement gets one TSV row with a class. Engine
//! access sits behind [`replay::Connect`], so the classifier and the TSV code are tested with
//! in-process mocks.

pub mod classify;
pub mod mysql;
pub mod pg;
pub mod replay;
pub mod tsv;
