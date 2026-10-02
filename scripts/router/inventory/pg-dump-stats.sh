#!/usr/bin/env bash
# Dumps what the capture needs from running Postgres servers on 127.0.0.1:<port>...:
#   <out>/<label>-<port>.pgss.jsonl   pg_stat_statements rows (db, query, calls, rows)
#   <out>/schema/<label>-<port>-<db>.sql   pg_dump --schema-only of every user database
# Usage: pg-dump-stats.sh <out-dir> <label> <port>...   Servers that do not answer are skipped.
set -uo pipefail
out="$1" label="$2"; shift 2
mkdir -p "$out/schema"
export PGPASSWORD="${PGPASSWORD:-pgdog}" PGUSER="${PGUSER:-pgdog}" PGHOST=127.0.0.1
for port in "$@"; do
  pg_isready -q -h 127.0.0.1 -p "$port" -t 2 || continue
  psql -X -At -p "$port" -d postgres -c "
    select json_build_object('db', d.datname, 'query', s.query, 'calls', s.calls, 'rows', s.rows, 'queryid', s.queryid::text)
    from pg_stat_statements s join pg_database d on d.oid = s.dbid
    where s.query not ilike '%pg_stat_statements%'" > "$out/$label-$port.pgss.jsonl" 2>>"$out/dump-errors.log" || true
  for db in $(psql -X -At -p "$port" -d postgres -c "select datname from pg_database where not datistemplate"); do
    pg_dump --schema-only --no-owner --no-privileges -p "$port" -d "$db" -f "$out/schema/$label-$port-$db.sql" 2>>"$out/dump-errors.log" || true
  done
  # Replication commands never reach pg_stat_statements; log_replication_commands = on writes them to the log.
  name="$(podman ps --format '{{.Names}} {{.Ports}}' | awk -v p=":$port->" 'index($0, p) {print $1; exit}')"
  [[ -n "$name" ]] && podman logs "$name" 2>&1 | grep -E 'received replication command' > "$out/$label-$port.replcmds.log" || true
done
exit 0
