#!/usr/bin/env bash
# Restores the schemas dumped by pg-dump-stats.sh onto a Postgres server, one database per
# <label>_<db> (the names pg-merge.py gives the capture entries), so that replayed statements
# find their tables. Usage: pg-prepare-ref.sh <capture-dir> <url-without-database>
#   e.g. pg-prepare-ref.sh $OUT postgres://pgdog:pgdog@127.0.0.1:15440
# Schemas only: replay compares how both engines answer, on empty tables.
set -uo pipefail
dir="${1:?capture dir}"; url="${2:?server url}"
declare -A done_for
for f in $(ls "$dir"/schema/*.sql 2>/dev/null | sort); do
  base="$(basename "$f" .sql)"            # <label>-<port>-<db>
  label="${base%%-*}"; rest="${base#*-}"; db="${rest#*-}"
  key="${label}_${db}"
  [[ -n "${done_for[$key]:-}" ]] && continue
  done_for[$key]=1
  psql -X -q "$url/postgres" -c "drop database if exists \"$key\" with (force)" -c "create database \"$key\"" >/dev/null 2>&1
  psql -X -q "$url/$key" -v ON_ERROR_STOP=0 -f "$f" >/dev/null 2>"$dir/prepare-$key.err" || true
  echo "prepared $key from $(basename "$f") ($(grep -c . "$dir/prepare-$key.err") error lines)"
done
