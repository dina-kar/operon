#!/usr/bin/env python3
"""The P4b merge gate (design §28 §7): Loams WAL vs stock safekeepers.

    compare.py [--name NAME] --baseline sk-a.json sk-b.json sk-c.json --candidate loams.json [...]

For every workload the gate holds when the candidate's mean is inside the
baseline's run-to-run noise band:
  mean p99(candidate) <= max p99 over the baseline repeats, and
  mean tps(candidate) >= min tps over the baseline repeats.
The noise column shows the baseline spread ((max - min) / mean). `bulk`
(a sustained 1 GB write) compares WAL MB/s as throughput and gates.
`bulk-burst` (250 MB, which the drive cache absorbs) is reported but does not
count toward pass or fail. The CPU columns are the WAL tier's CPU time per
transaction (per MB for bulk), from run.sh; they are reported, not gated.
Prints a Markdown table; exits 1 if the gate fails.
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
    ap.add_argument("--name", default="Loams")
    ap.add_argument("--baseline", nargs="+", required=True)
    ap.add_argument("--candidate", nargs="+", required=True)
    a = ap.parse_args()
    _, base = load(a.baseline)
    _, cand = load(a.candidate)
    n = a.name

    def cpu(ws, key):
        v = mean([w.get(key) for w in ws])
        return "–" if v is None else f"{v:.0f}" if v >= 10 else f"{v:.1f}"

    ok = True
    print(f"### {n} vs safekeepers ({len(a.candidate)} runs vs {len(a.baseline)})")
    print()
    print(f"| workload | safekeepers p50 / p99 (ms) | {n} p50 / p99 (ms) | noise | safekeepers TPS | {n} TPS "
          f"| safekeepers CPU µs/tx | {n} CPU µs/tx | gate |")
    print("|---|---|---|---|---|---|---|---|---|")
    for name in base:
        # Reported only: the drive cache absorbs a 250 MB burst, so a missing
        # or incomplete bulk-burst never fails the gate.
        reported_only = name == "bulk-burst"
        miss = "reported" if reported_only else "FAIL"
        if name not in cand:
            print(f"| {name} | – | missing | – | – | missing | – | – | {miss} |")
            ok &= reported_only
            continue
        b, c = base[name], cand[name]
        bulk = name in ("bulk", "bulk-burst")
        keys = ["wal_mb_per_s"] if bulk else ["p99_ms", "tps"]
        if any(w.get(k) is None for w in b + c for k in keys):
            print(f"| {name} | – | incomplete | – | – | incomplete | – | – | {miss} |")
            ok &= reported_only
            continue
        if bulk:
            bt, ct = mean([w["wal_mb_per_s"] for w in b]), mean([w["wal_mb_per_s"] for w in c])
            nt = spread([w["wal_mb_per_s"] for w in b])
            passed = ct >= min(w["wal_mb_per_s"] for w in b)
            if reported_only:
                label = "bulk-burst (WAL MB/s, not gated; CPU ms/MB)"
                verdict = "reported"
                passed = True
            else:
                label = "bulk (WAL MB/s, sustained 1 GB; CPU ms/MB)"
                verdict = "pass" if passed else "FAIL"
            print(f"| {label} | – | – | {nt:.0%} | {bt:.1f} | {ct:.1f} "
                  f"| {cpu(b, 'wal_cpu_ms_per_mb')} | {cpu(c, 'wal_cpu_ms_per_mb')} | {verdict} |")
        else:
            b50, c50 = mean([w["p50_ms"] for w in b]), mean([w["p50_ms"] for w in c])
            bp, cp = mean([w["p99_ms"] for w in b]), mean([w["p99_ms"] for w in c])
            bt, ct = mean([w["tps"] for w in b]), mean([w["tps"] for w in c])
            np_, nt = spread([w["p99_ms"] for w in b]), spread([w["tps"] for w in b])
            passed = cp <= max(w["p99_ms"] for w in b) and ct >= min(w["tps"] for w in b)
            print(f"| {name} | {b50:.2f} / {bp:.2f} | {c50:.2f} / {cp:.2f} | p99 {np_:.0%}, TPS {nt:.0%} "
                  f"| {bt:.0f} | {ct:.0f} | {cpu(b, 'wal_cpu_us_per_tx')} | {cpu(c, 'wal_cpu_us_per_tx')} "
                  f"| {'pass' if passed else 'FAIL'} |")
        ok &= passed
    print()
    print(f"**Gate ({n}): " + ("PASS" if ok else "FAIL") + "**")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
