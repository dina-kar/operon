//! Differential test against the Lean reference (RT0 Task 8, D312): random
//! partitions, broken lists, lookups, splits and merges go to
//! `loams-router-oracle` (built by `lake build` in `spec/lean/`), and Rust and
//! Lean must agree on every result or error kind. Skipped when the binary is
//! not on `PATH` or in `spec/lean/.lake/build/bin/`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use loams_sqlrouter::KeyRange;
use loams_sqlrouter::ranges::{PartitionError, lookup, merge, split, validate_partition};
use rand::{RngExt, SeedableRng};
use serde_json::{Value, json};

fn oracle_path() -> Option<std::path::PathBuf> {
    let local = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/lean/.lake/build/bin/loams-router-oracle");
    if local.exists() {
        return Some(local);
    }
    std::env::var_os("PATH")?
        .to_str()?
        .split(':')
        .map(|d| std::path::Path::new(d).join("loams-router-oracle"))
        .find(|p| p.exists())
}

fn ranges_json(rs: &[KeyRange]) -> Value {
    Value::Array(rs.iter().map(|r| json!([r.lo, r.hi])).collect())
}

fn rust_answer(req: &Value, rs: &[KeyRange]) -> Value {
    match req["op"].as_str().unwrap() {
        "partition_check" => match validate_partition(rs) {
            Ok(()) => json!({"ok": true}),
            Err(PartitionError::Gap { at }) => json!({"ok": false, "error": "Gap", "at": at}),
            Err(PartitionError::Overlap { at }) => {
                json!({"ok": false, "error": "Overlap", "at": at})
            }
            Err(PartitionError::NotSorted) => json!({"ok": false, "error": "NotSorted"}),
            Err(PartitionError::Empty) => json!({"ok": false, "error": "Empty"}),
        },
        "lookup" => json!({"ok": true, "result": lookup(rs, req["id"].as_u64().unwrap())}),
        "split" => match split(
            rs,
            req["index"].as_u64().unwrap() as usize,
            req["at"].as_u64().unwrap(),
        ) {
            Ok(out) => json!({"ok": true, "result": ranges_json(&out)}),
            Err(_) => json!({"ok": false, "error": "Invalid"}),
        },
        "merge" => match merge(rs, req["index"].as_u64().unwrap() as usize) {
            Ok(out) => json!({"ok": true, "result": ranges_json(&out)}),
            Err(_) => json!({"ok": false, "error": "Invalid"}),
        },
        other => panic!("unknown op {other}"),
    }
}

/// A random list of ranges: usually a partition, sometimes broken on purpose.
/// Half the lists use cut points below 16, so ids, splits and edits collide
/// with the bounds often enough to reach every boundary case.
fn random_ranges(rng: &mut impl RngExt) -> Vec<KeyRange> {
    let max = if rng.random() { 16 } else { u64::MAX };
    let cuts: Vec<u64> = (0..rng.random_range(0..8))
        .map(|_| rng.random_range(1..max))
        .collect();
    let mut rs = crate::partition_from_cuts(cuts);
    match rng.random_range(0..8) {
        // An empty range in the middle: [lo, lo), then the next starts at lo.
        4 if rs.len() > 1 => {
            let i = rng.random_range(0..rs.len() - 1);
            rs[i].hi = Some(rs[i].lo);
            rs[i + 1].lo = rs[i].lo;
        }
        // An empty last range.
        5 if !rs.is_empty() => {
            let last = rs.len() - 1;
            rs[last].hi = Some(rs[last].lo);
        }
        0 if !rs.is_empty() => {
            let i = rng.random_range(0..rs.len());
            rs[i].lo = rs[i].lo.wrapping_add(rng.random_range(1..1000));
        }
        1 => rs.truncate(rng.random_range(0..=rs.len())),
        2 if rs.len() > 1 => {
            let i = rng.random_range(0..rs.len() - 1);
            rs.swap(i, i + 1);
        }
        3 if !rs.is_empty() => {
            let i = rng.random_range(0..rs.len());
            rs[i].hi = if rng.random() {
                None
            } else {
                Some(rng.random())
            };
        }
        _ => {}
    }
    rs
}

#[test]
fn partition_matches_lean_oracle() {
    let Some(path) = oracle_path() else {
        eprintln!("skipped: needs loams-router-oracle (cd spec/lean && lake build)");
        return;
    };
    // The test is a driver, not kernel code: it may read its configuration.
    #[allow(clippy::disallowed_methods)]
    let cases: usize = std::env::var("LOAMS_ORACLE_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let mut child = Command::new(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut rng = rand::rngs::ChaCha8Rng::seed_from_u64(20261002);
    for n in 0..cases {
        let rs = random_ranges(&mut rng);
        let len = rs.len().max(1) as u64;
        let req = match n % 4 {
            0 => json!({"op": "partition_check", "ranges": ranges_json(&rs)}),
            1 => {
                let id = if rng.random() {
                    rng.random_range(0..20)
                } else {
                    rng.random::<u64>()
                };
                json!({"op": "lookup", "ranges": ranges_json(&rs), "id": id})
            }
            2 => {
                let i = rng.random_range(0..len + 1);
                let r = rs.get(i as usize).copied().unwrap_or(KeyRange::FULL);
                let hi = r.hi.unwrap_or(u64::MAX);
                // Mostly inside the range, sometimes on or outside its bounds.
                let at = if rng.random_range(0..4) == 0 || hi <= r.lo {
                    rng.random()
                } else {
                    rng.random_range(r.lo..=hi)
                };
                json!({"op": "split", "ranges": ranges_json(&rs), "index": i, "at": at})
            }
            _ => {
                json!({"op": "merge", "ranges": ranges_json(&rs), "index": rng.random_range(0..len + 1)})
            }
        };
        writeln!(stdin, "{req}").unwrap();
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        let lean: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(rust_answer(&req, &rs), lean, "case {n}: {req}");
    }
    drop(stdin);
    assert!(child.wait().unwrap().success());
}
