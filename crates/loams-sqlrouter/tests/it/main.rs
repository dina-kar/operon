//! loams-sqlrouter's integration tests (RT0 Task 7, Task 8's oracle test).

#![allow(clippy::unwrap_used)]

mod deps;
mod hash;
mod lean_oracle;
mod ranges;
mod record;
mod trace;

/// Random valid partitions of the keyspace, shared by the range tests.
pub fn partition_from_cuts(mut cuts: Vec<u64>) -> Vec<loams_sqlrouter::KeyRange> {
    cuts.retain(|&c| c != 0);
    cuts.sort_unstable();
    cuts.dedup();
    let mut out = Vec::with_capacity(cuts.len() + 1);
    let mut lo = 0;
    for c in cuts {
        out.push(loams_sqlrouter::KeyRange { lo, hi: Some(c) });
        lo = c;
    }
    out.push(loams_sqlrouter::KeyRange { lo, hi: None });
    out
}
