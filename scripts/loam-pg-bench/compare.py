#!/usr/bin/env python3
"""The P4b merge gate (design §28 §7): Loam WAL vs stock safekeepers.

    compare.py --baseline sk-a.json sk-b.json sk-c.json --candidate loam.json [...]

For every workload the gate holds when the candidate's mean is inside the
baseline's run-to-run noise band:
  mean p99(candidate) <= max p99 over the baseline repeats, and
  mean tps(candidate) >= min tps over the baseline repeats.
The noise column shows the baseline spread ((max - min) / mean). `bulk`
compares WAL MB/s as throughput. Prints a Markdown table; exits 1 if the gate
fails.
"""
import argparse
import json
import statistics
import sys


def load(paths):
    runs = [json.load(open(p)) for p in paths]
    by = {}
    for r in runs:
        for w in r["workloads"]:
            by.setdefault(w["workload"], []).append(w)
    return runs, by


def spread(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return 0.0
    m = statistics.mean(xs)
    return (max(xs) - min(xs)) / m if m else 0.0


def mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", nargs="+", required=True)
    ap.add_argument("--candidate", nargs="+", required=True)
    a = ap.parse_args()
    _, base = load(a.baseline)
    _, cand = load(a.candidate)
    ok = True
    print("| workload | safekeepers p99 (ms) | Loam p99 (ms) | noise | safekeepers TPS | Loam TPS | gate |")
    print("|---|---|---|---|---|---|---|")
    for name in base:
        if name not in cand:
            print(f"| {name} | – | missing | – | – | missing | FAIL |")
            ok = False
            continue
        b, c = base[name], cand[name]
        keys = ["wal_mb_per_s"] if name == "bulk" else ["p99_ms", "tps"]
        if any(w.get(k) is None for w in b + c for k in keys):
            print(f"| {name} | – | incomplete | – | – | incomplete | FAIL |")
            ok = False
            continue
        if name == "bulk":
            bt, ct = mean([w["wal_mb_per_s"] for w in b]), mean([w["wal_mb_per_s"] for w in c])
            nt = spread([w["wal_mb_per_s"] for w in b])
            passed = ct >= min(w["wal_mb_per_s"] for w in b)
            print(f"| bulk (WAL MB/s) | – | – | {nt:.0%} | {bt:.1f} | {ct:.1f} | {'pass' if passed else 'FAIL'} |")
        else:
            bp, cp = mean([w["p99_ms"] for w in b]), mean([w["p99_ms"] for w in c])
            bt, ct = mean([w["tps"] for w in b]), mean([w["tps"] for w in c])
            np_, nt = spread([w["p99_ms"] for w in b]), spread([w["tps"] for w in b])
            passed = cp <= max(w["p99_ms"] for w in b) and ct >= min(w["tps"] for w in b)
            print(f"| {name} | {bp:.2f} | {cp:.2f} | p99 {np_:.0%}, TPS {nt:.0%} | {bt:.0f} | {ct:.0f} | {'pass' if passed else 'FAIL'} |")
        ok &= passed
    print()
    print("**Gate: " + ("PASS" if ok else "FAIL") + "**")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
