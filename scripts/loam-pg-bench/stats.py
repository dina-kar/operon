#!/usr/bin/env python3
"""Percentiles and TPS from pgbench per-transaction logs (design §28 §7).

    stats.py <dir-with-tx.*-logs-and-summary.txt> <workload-name>

Prints one JSON object. Latencies come from the logs (column 3, microseconds),
not from pgbench's averages.
"""
import glob
import json
import os
import re
import sys


def pct(sorted_us, q):
    if not sorted_us:
        return None
    i = min(len(sorted_us) - 1, int(len(sorted_us) * q))
    return round(sorted_us[i] / 1000.0, 3)


def main():
    d, name = sys.argv[1], sys.argv[2]
    summary = open(os.path.join(d, "summary.txt")).read()
    out = {"workload": name}
    if name in ("bulk", "bulk-burst"):
        m = re.search(r"wal_bytes=(\d+) nanos=(\d+)", summary)
        b, s = int(m.group(1)), int(m.group(2)) / 1e9
        out.update(wal_bytes=b, seconds=round(s, 3), wal_mb_per_s=round(b / s / 1e6, 2))
    else:
        lat = []
        for f in glob.glob(os.path.join(d, "tx*")):
            with open(f) as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) >= 3:
                        lat.append(int(parts[2]))
        lat.sort()
        m = re.search(r"tps = ([\d.]+)", summary)
        out.update(
            n=len(lat),
            tps=round(float(m.group(1)), 1) if m else None,
            p50_ms=pct(lat, 0.50),
            p90_ms=pct(lat, 0.90),
            p99_ms=pct(lat, 0.99),
            p999_ms=pct(lat, 0.999),
            max_ms=round(lat[-1] / 1000.0, 3) if lat else None,
        )
    print(json.dumps(out))


if __name__ == "__main__":
    main()
