#!/usr/bin/env bash
# Merge the two Vitess captures and replay them: the whole MySQL half after vitess-capture.sh.
#
#   vitess-replay.sh <dir>        <dir> holds ref/ and wesql/ (the outputs of vitess-capture.sh ref|wesql)
#
# 1. builds compat-replay (cargo, -p loams-compat only: run with no inventory container up),
# 2. vitess-static.sh and vt-merge.py: capture.jsonl (statements the reference run sent, plus Vitess's static
#    statements) and the digests each step of each run touched,
# 3. starts one MySQL 8.0.46 (the reference, 127.0.0.1:13306) and one WeSQL (the target, 127.0.0.1:13316),
#    restores the captured schemas on both,
# 4. replays: each example on both, classified; WeSQL refusals listed in vitess-unsupported-rules.tsv are `unsupported`,
# 5. writes <dir>/statements.tsv and suites.tsv (the WeSQL run's steps, the reference's result in each test name).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
dir="${1:?capture dir with ref/ and wesql/}"
VITESS_SRC="${VITESS_SRC:-$HOME/.cache/loam/vitess-v24.0.4}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/user/$(id -u)/podman/podman.sock}"
dc() { if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then docker compose -f "$here/compose.vitess.yml" --profile ref --profile wesql "$@"; else docker-compose -f "$here/compose.vitess.yml" --profile ref --profile wesql "$@"; fi; }
MY="$(command -v mariadb || command -v mysql)"
(cd "$root" && cargo build -p loams-compat --bin compat-replay)
bin="$(cd "$root" && cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/debug/compat-replay"
"$here/vitess-static.sh" "$VITESS_SRC" > "$dir/static.tsv"
python3 "$here/vt-merge.py" "$dir/ref" "$dir/static.tsv"
python3 "$here/vt-merge.py" "$dir/wesql" "$dir/static.tsv" 2>/dev/null   # step-digests of the WeSQL run (its capture.jsonl is not used)
trap 'dc down -v >/dev/null 2>&1 || true' EXIT
dc up -d my-a wesql-a >/dev/null
for p in 13306 13316; do for _ in $(seq 120); do "$MY" -h127.0.0.1 -P$p -uroot -ploam-dev -e 'select 1' >/dev/null 2>&1 && break; sleep 3; done; done
for p in 13306 13316; do
  for _ in $(seq 20); do "$MY" -h127.0.0.1 -P$p -uroot -ploam-dev -e 'create database if not exists compat_empty' >/dev/null 2>&1 && break; sleep 3; done
  for n in a b c; do
    "$MY" -h127.0.0.1 -P$p -uroot -ploam-dev --force < "$dir/ref/schema/$n.sql" > "$dir/restore-$p-$n.log" 2>&1 || true
  done
done
"$bin" --engine mysql --reference mysql://root:loam-dev@127.0.0.1:13306 --target mysql://root:loam-dev@127.0.0.1:13316 \
  --empty-db compat_empty --unsupported-rules "$here/vitess-unsupported-rules.tsv" --shape-only "$here/vitess-shape-only.txt" \
  --input "$dir/ref/capture.jsonl" --out "$dir/statements.tsv"
python3 "$here/suites.py" "$dir/wesql/steps.tsv" "$dir/statements.tsv" "$dir/suites.tsv" \
  --ref-steps "$dir/ref/steps.tsv" --step-digests "$dir/wesql/step-digests.tsv"
