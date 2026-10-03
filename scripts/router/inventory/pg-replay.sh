#!/usr/bin/env bash
# Merge a capture and replay it: the whole Postgres half after pg-capture.sh.
#
#   pg-replay.sh <capture-dir> [target-url]
#
# 1. builds compat-replay (cargo, -p loams-compat only: run with no inventory container up),
# 2. pg-static.sh and pg-merge.py -> <capture-dir>/capture.jsonl,
# 3. starts the reference (`ref` of compose.pg.yml, postgres:17.11), restores the captured schemas onto it,
# 4. replays: with no target-url every row is `pending-target` (Ruling 7: no Loams Postgres compute yet),
#    with one (postgres://user:pw@host:port/db) the same statements run there too,
# 5. writes <capture-dir>/statements.tsv and suites.tsv; stops the reference.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
dir="${1:?capture dir}"; target="${2:-}"
PGDOG_SRC="${PGDOG_SRC:-$HOME/.cache/loam/pgdog-v0.1.60}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/user/$(id -u)/podman/podman.sock}"
compose() { if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then docker compose "$@"; else docker-compose "$@"; fi; }
(cd "$root" && cargo build -p loams-compat --bin compat-replay)
bin="$(cd "$root" && cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/debug/compat-replay"
"$here/pg-static.sh" "$PGDOG_SRC" > "$dir/static.tsv"
python3 "$here/pg-merge.py" "$dir" "$dir/static.tsv" "$here/pg-static-kinds.tsv"
trap 'compose -f "$here/compose.pg.yml" down -v >/dev/null 2>&1 || true' EXIT
compose -f "$here/compose.pg.yml" up -d ref >/dev/null
until PGPASSWORD=pgdog pg_isready -q -h 127.0.0.1 -p 15440 -U pgdog; do sleep 1; done
sleep 4
export PGPASSWORD=pgdog
"$here/pg-prepare-ref.sh" "$dir" postgres://pgdog:pgdog@127.0.0.1:15440 > "$dir/prepare.log" 2>&1
psql -X -q -h 127.0.0.1 -p 15440 -U pgdog -d postgres -c 'drop database if exists compat_empty with (force)' -c 'create database compat_empty'
args=(--engine postgres --reference postgres://pgdog:pgdog@127.0.0.1:15440/postgres --empty-db compat_empty --input "$dir/capture.jsonl" --out "$dir/statements.tsv")
[[ -n "$target" ]] && args+=(--target "$target")
[[ -f "$here/pg-unsupported.tsv" ]] && args+=(--unsupported "$here/pg-unsupported.tsv")
"$bin" "${args[@]}"
python3 "$here/suites.py" "$dir/steps.tsv" "$dir/statements.tsv" "$dir/suites.tsv" --target-pending   # the suites run through PgDog on the reference only; a target run needs P2b
