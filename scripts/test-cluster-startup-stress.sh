#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Exercise every startup path, including learner restarts, on fresh ports each pass.
for ((pass = 1; pass <= 8; pass++)); do
  echo "cluster startup stress pass ${pass}/8"
  cargo test -p loams --features cluster-tests --test cluster --locked -- --test-threads=1
done
