#!/usr/bin/env bash
# Rewrite a conctrace history's 503 answers as "not applied" (D1 plan Task 5,
# the research rule for F3), for Resonate's porcupine checker.
#
#   scripts/durable/porc-503.sh <in.history> <out.history>
#
# Resonate answers 503 ("serialization failure, please retry") only when the
# request's transaction did not commit, so the operation never took effect.
# The checker (`spec/valid/porc`) cannot say that: it reads 500 as "maybe
# applied" and matches every other status literally, and its model never
# answers 503, so one 503 refutes a history the server kept linearizable.
# An operation that did not take effect is the same, to linearizability, as
# one never issued, so each 503 row is dropped; every other row is kept
# byte for byte, in order. Until upstream PR 0b teaches the checker 503, the
# TiDB leg of conformance.sh checks the rewritten history.
#
# Prints one line, `porc-503: <n> of <total> responses were 503 (dropped as
# not applied)`, which conformance.sh copies into the job summary.
set -euo pipefail

if [ $# -ne 2 ]; then
  sed -n '5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
fi
in=$1
out=$2
[ -f "$in" ] || { echo "porc-503: $in is not a file" >&2; exit 2; }

python3 - "$in" "$out" <<'PY'
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
total = dropped = 0
with open(src, encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
    for n, line in enumerate(fin, 1):
        if not line.strip():
            continue
        total += 1
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            sys.exit(f"porc-503: {src}:{n}: not JSON ({e})")
        head = (row.get("res") or {}).get("head") or {}
        if head.get("status") == 503:
            dropped += 1
            continue
        fout.write(line if line.endswith("\n") else line + "\n")
print(f"porc-503: {dropped} of {total} responses were 503 (dropped as not applied)")
PY
