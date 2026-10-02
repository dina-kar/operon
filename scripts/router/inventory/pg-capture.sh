#!/usr/bin/env bash
# The Postgres half's dynamic capture (RT0 plan Task 5, §31 §15 steps 2 and 5).
#
#   pg-capture.sh [out-dir]       (default $HOME/.cache/loam/inventory/pg/<date>)
#
# Runs PgDog v0.1.60's own integration scenarios, from the pinned checkout $PGDOG_SRC (default
# $HOME/.cache/loam/pgdog-v0.1.60), against Postgres 17.11 with pg_stat_statements and
# log_replication_commands on. The scenarios are RUN, never copied (D236, D318). PgDog itself is the
# unmodified binary from the pinned image (shims/pgdog). After each scenario the statistics and the
# schemas are dumped (pg-dump-stats.sh) and pg_stat_statements is reset, so every statement is
# attributed to the scenario that sent it.
#
# Scenarios (suite names of the suites table; SCENARIOS="pgbench two_pc" runs a subset, the rows of the others are kept):
#   baseline     pg-baseline.sql                    through PgDog's container with the two-shard config of this directory
#   resharding   integration/resharding/dev.sh      its own 4-server compose (override: pgdog-resharding.override.yaml)
#   pgbench      integration/pgbench/run.sh         root layout on shard0 (127.0.0.1:5432), pgbench from the postgres image
#   schema_sync  integration/schema_sync/dev.sh     pgdog schema-sync, pg_dump, publications
#   data_sync    integration/copy_data/data_sync/run.sh   0 -> 2 and 2 -> 2 resharding under write load
#   two_pc       integration/two_pc/crash_recovery.py     2PC with PgDog killed mid-commit
# Scenarios that cannot run here (rewrite, logical, failover: they need Ruby, a dataset download or a
# human) are recorded as `skip` with the reason. Output files:
#   steps.tsv                 suite, test, result, detail          -> pgdog-loampg-suites.tsv (after merge)
#   <label>-<port>.pgss.jsonl, <label>-<port>.replcmds.log, schema/*.sql, <scenario>.log
# Stop all containers before any cargo build; never run next to compose.vitess.yml.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PGDOG_SRC="${PGDOG_SRC:-$HOME/.cache/loam/pgdog-v0.1.60}"
OUT="${1:-$HOME/.cache/loam/inventory/pg/$(date +%F)}"
mkdir -p "$OUT"
avail_gb=$(df --output=avail -BG "$HOME" | tail -1 | tr -dc 0-9)
[[ "$avail_gb" -ge 8 ]] || { echo "only ${avail_gb} GB free under $HOME; stop (plan: 8 GB)"; exit 2; }
[[ -d "$PGDOG_SRC/integration" ]] || { echo "PGDOG_SRC=$PGDOG_SRC is not a PgDog checkout"; exit 2; }
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/user/$(id -u)/podman/podman.sock}"
export PGDOG_SRC LOAMS_INV_DUMP_DIR="$OUT"
# The unmodified binary, taken out of the pinned image: the 2PC scenario kills PgDog, which a
# `podman run` wrapper cannot do. The container wrapper (shims/pgdog) is the fallback.
digest="${PGDOG_DIGEST:-sha256:25d1908886f595a2e3b00ec59783326712e3c0f75f91d29f032ff712b30f2266}"
bin="$HOME/.cache/loam/inventory/bin/pgdog"
engine="$(command -v podman || command -v docker)"
if [[ ! -x "$bin" ]]; then
  mkdir -p "$(dirname "$bin")"
  c="$("$engine" create "ghcr.io/pgdogdev/pgdog@$digest")" && "$engine" cp "$c:/usr/local/bin/pgdog" "$bin" && "$engine" rm "$c" >/dev/null
fi
if "$bin" --version >/dev/null 2>&1; then export PGDOG_BIN="$bin"; else export PGDOG_BIN="$here/shims/pgdog"; fi
echo "PgDog binary: $PGDOG_BIN ($("$PGDOG_BIN" --version 2>&1 | head -1))"
want() { [[ " ${SCENARIOS:-resharding baseline pgbench schema_sync data_sync two_pc} " == *" $1 "* ]]; }
export PATH="$here/shims:$PATH"
compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then docker compose "$@"; else docker-compose "$@"; fi
}
[[ -n "${SCENARIOS:-}" ]] || : > "$OUT/steps.tsv"
record() { # replaces the suite's earlier row, so a re-run of one scenario updates it
  touch "$OUT/steps.tsv"; grep -v -P "^$1\t" "$OUT/steps.tsv" > "$OUT/steps.tsv.new" || true; mv "$OUT/steps.tsv.new" "$OUT/steps.tsv"
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$OUT/steps.tsv"; echo "== $1: $3 ($4)"; }
reset_stats() { PGPASSWORD=pgdog psql -X -q -h 127.0.0.1 -p "$1" -U pgdog -d postgres -c 'select pg_stat_statements_reset()' >/dev/null 2>&1 || true; }

# --- resharding: its own stack -------------------------------------------------------------------------
if want resharding; then
  R="$PGDOG_SRC/integration/resharding"
  sed "s#\${INV_DIR}#$here#" "$here/pgdog-resharding.override.yaml" > "$R/docker-compose.override.yaml"
  LOAMS_INV_LABEL=resharding timeout 20m bash "$R/dev.sh" > "$OUT/resharding.log" 2>&1; rc=$?
  case $rc in 0) r=pass; d="COPY_DATA 2 -> 2 shards under pgbench write load, then logical replication catch-up" ;;
    124) r=fail; d="timed out at 20 min (scenario limit 16): catch-up did not finish under the pgbench load" ;;
    *) r=fail; d="exit code $rc" ;; esac
  record resharding "integration/resharding/dev.sh" "$r" "$d"
  rm -f "$R/docker-compose.override.yaml"
  LOAMS_INV_DUMP_DIR= compose -f "$R/docker-compose.yaml" down >/dev/null 2>&1 || true
fi

# --- the two-shard baseline, then the root layout: shard0 on 127.0.0.1:5432 --------------------------------
compose -f "$here/compose.pg.yml" --profile baseline up -d shard0 shard1 ref pgdog >/dev/null 2>&1 || { record setup "compose.pg.yml" fail "could not start"; exit 1; }
for p in 5432 15433 15440; do until PGPASSWORD=pgdog pg_isready -q -h 127.0.0.1 -p "$p" -U pgdog; do sleep 1; done; done
sleep 3
if want baseline; then
  for p in 5432 15433; do reset_stats "$p"; done
  until PGPASSWORD=pgdog pg_isready -q -h 127.0.0.1 -p 6432 -U pgdog -d inv; do sleep 1; done
  if PGPASSWORD=pgdog timeout 5m psql -X -h 127.0.0.1 -p 6432 -U pgdog -d inv -f "$here/pg-baseline.sql" > "$OUT/baseline.log" 2>&1; then r=pass; else r=fail; fi
  bash "$here/pg-dump-stats.sh" "$OUT" baseline 5432 15433 >/dev/null 2>&1
  record baseline "scripts/router/inventory/pg-baseline.sql" "$r" "two-shard PgDog container: DDL, cross-shard DML, 2PC, aggregates, COPY"
fi
# PgDog's own scenarios run the binary on 6432: free the port.
compose -f "$here/compose.pg.yml" --profile baseline stop pgdog >/dev/null 2>&1 || true
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=pgdog PGPASSWORD=pgdog PGDATABASE=postgres
( cd "$PGDOG_SRC/integration" && bash setup.sh ) > "$OUT/setup.log" 2>&1 || echo "setup.sh exited non-zero (its toxiproxy download is the usual cause); continuing" >> "$OUT/setup.log"
export PGDATABASE=pgdog
# pg_dump after each scenario: only shard0 hosts the root layout.
scenario() { # scenario <suite> <test> <detail> <timeout> <command...>
  local suite="$1" test="$2" detail="$3" to="$4"; shift 4
  want "$suite" || return 0
  reset_stats 5432
  if (cd "$PGDOG_SRC/integration" && LOAMS_INV_LABEL="$suite" timeout "$to" "$@") > "$OUT/$suite.log" 2>&1; then r=pass; else r=fail; fi
  bash "$here/pg-dump-stats.sh" "$OUT" "$suite" 5432 >/dev/null 2>&1
  record "$suite" "$test" "$r" "$detail"
}
scenario pgbench "integration/pgbench/run.sh" "pgbench simple, extended and prepared through PgDog; COPY" 10m bash pgbench/run.sh
scenario schema_sync "integration/schema_sync/dev.sh" "pgdog schema-sync (pre-data, post-data, cutover) with a publication" 10m bash schema_sync/dev.sh
scenario data_sync "integration/copy_data/data_sync/run.sh" "0 -> 2 and 2 -> 2 resharding with live write traffic" 20m bash copy_data/data_sync/run.sh
if ! want two_pc; then :
elif command -v python3 >/dev/null && python3 -c 'import asyncpg' 2>/dev/null; then
  scenario two_pc "integration/two_pc/crash_recovery.py" "PgDog killed during two-phase commit, WAL recovery" 15m python3 two_pc/crash_recovery.py
else
  # asyncpg is not installed here; a venv under the cache directory provides it.
  [[ -d "$HOME/.cache/loam/inventory/venv" ]] || { python3 -m venv "$HOME/.cache/loam/inventory/venv" && "$HOME/.cache/loam/inventory/venv/bin/pip" -q install asyncpg==0.30.0; } >/dev/null 2>&1
  scenario two_pc "integration/two_pc/crash_recovery.py" "PgDog killed during two-phase commit, WAL recovery" 15m "$HOME/.cache/loam/inventory/venv/bin/python" two_pc/crash_recovery.py
fi
record rewrite "integration/rewrite" skip "config only in this checkout; the rewrite specs are Ruby and Rust suites"
record logical "integration/logical" skip "needs a downloaded dataset (gutenberg) and a manual walk-through"
record failover "integration/failover" skip "interactive dev-server scripts"
LOAMS_INV_DUMP_DIR= compose -f "$here/compose.pg.yml" down -v >/dev/null 2>&1 || true
echo "capture written to $OUT"
