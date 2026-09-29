#!/usr/bin/env bash
# The P4b gate run (docs/design/28-loam-postgres.md §7): baseline and
# candidate interleaved (A B A B A B) so that drift on the host hits both,
# then compare.py over the results.
#
#   scripts/loam-pg-bench/gate.sh [--replicas 1|3] [--repeats N] [--out DIR] [run.sh options]
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
replicas=1 repeats=3 out=$ROOT/bench/results extra=()
while [ $# -gt 0 ]; do
  case $1 in
    --replicas) replicas=$2; shift 2 ;;
    --repeats) repeats=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    *) extra+=("$1"); shift ;;
  esac
done
base=() cand=()
for i in $(seq 1 "$repeats"); do
  base+=("$("$ROOT/scripts/loam-pg-bench/run.sh" --variant safekeepers --replicas "$replicas" \
    --out "$out" --label "r$i" "${extra[@]}")")
  cand+=("$("$ROOT/scripts/loam-pg-bench/run.sh" --variant loam --replicas "$replicas" \
    --out "$out" --label "r$i" "${extra[@]}")")
done
python3 "$ROOT/scripts/loam-pg-bench/compare.py" --baseline "${base[@]}" --candidate "${cand[@]}" |
  tee "$out/gate-rf$replicas-$(date -u +%Y%m%dT%H%M%SZ).md"
