#!/usr/bin/env bash
# The P4b gate run (docs/design/28-loams-postgres.md §7): the baseline and
# each candidate interleaved (A B C A B C ...) so that drift on the host hits
# all of them, then compare.py of each candidate against the baseline.
#
#   scripts/loams-pg-bench/gate.sh [--replicas 1|3] [--repeats N] [--out DIR]
#       [--baseline VARIANT] [--candidates "VARIANT ..."] [run.sh options]
#
# The defaults compare safekeepers with loams (P4a). Arm A (§7.2):
#   --candidates "nvme-pwritev2 nvme-uring nvme-sqpoll"
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
replicas=1 repeats=3 out=$ROOT/bench/results baseline=safekeepers candidates=loams extra=()
while [ $# -gt 0 ]; do
  case $1 in
    --replicas) replicas=$2; shift 2 ;;
    --repeats) repeats=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --baseline) baseline=$2; shift 2 ;;
    --candidates) candidates=$2; shift 2 ;;
    *) extra+=("$1"); shift ;;
  esac
done
# An empty list would run the baseline alone and report a gate that compared nothing.
read -r -a cands <<<"$candidates"
[ "${#cands[@]}" -gt 0 ] || { echo "gate: --candidates names no variant" >&2; exit 2; }
declare -A files
for i in $(seq 1 "$repeats"); do
  for v in $baseline $candidates; do
    f=$("$ROOT/scripts/loams-pg-bench/run.sh" --variant "$v" --replicas "$replicas" \
      --out "$out" --label "r$i" "${extra[@]}")
    files[$v]="${files[$v]:-} $f"
  done
done
report=$out/gate-rf$replicas-$(date -u +%Y%m%dT%H%M%SZ).md
: >"$report"
failed=0
for c in $candidates; do
  # shellcheck disable=SC2086
  python3 "$ROOT/scripts/loams-pg-bench/compare.py" --name "$c" \
    --baseline ${files[$baseline]} --candidate ${files[$c]} | tee -a "$report" || failed=1
  echo | tee -a "$report"
done
echo "gate: wrote $report" >&2
exit "$failed"
