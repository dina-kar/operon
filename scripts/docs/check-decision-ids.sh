#!/usr/bin/env bash
# Validate declarations in the central decision log; references are not rows.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
if [[ "${1:-}" == --self-test ]]; then
  "$0" "$root/scripts/docs/testdata/unique-decision-ids.md"
  if output=$("$0" "$root/scripts/docs/testdata/duplicate-decision-ids.md" 2>&1); then
    echo 'decision IDs: planted duplicates were accepted' >&2
    exit 1
  fi
  [[ "$output" == *'duplicate D1'* && "$output" == *'duplicate Q2'* && "$output" == *'duplicate Q-RT-1'* && "$output" == *'duplicate D-SC-1'* ]] || {
    echo "$output" >&2
    exit 1
  }
  echo 'decision IDs: self-test passed (D and Q duplicates rejected)'
  exit 0
fi
awk -F '|' '
  /^[[:space:]]*(\|[[:space:]]*)?[DQ]([0-9]+|-[A-Za-z0-9]+-[0-9]+)([[:space:]]+\([^|]*\))?[[:space:]]*\|/ {
    id = ($1 ~ /^[[:space:]]*$/) ? $2 : $1
    sub(/[[:space:]]+\(.*/, "", id)
    gsub(/[[:space:]]/, "", id)
    if (id in seen) {
      printf "%s:%d: duplicate %s (first declaration at line %d)\n", FILENAME, FNR, id, seen[id] > "/dev/stderr"
      failed = 1
    } else {
      seen[id] = FNR
    }
  }
  END { exit failed }
' "${1:-$root/docs/design/13-decision-log.md}"
