#!/usr/bin/env bash
# Runs inside the benchmark compute (deploy/loam-pg-bench): one workload, with
# a warm-up, and pgbench per-transaction logs under /tmp/bench/<name>/. It
# prints MEASURE_START and MEASURE_END around the measured phase, where
# run.sh samples the WAL tier's CPU time.
#
#   workload.sh <name> <duration-s> <warmup-s> <scale>
#
# Workloads (docs/design/28-loam-postgres.md §7):
#   commit-1   one single-row INSERT per transaction, 1 client (the commit RTT)
#   commit-16  the same, 16 clients (group commit)
#   tpcb-16    built-in TPC-B, 16 clients
#   tpcb-64    built-in TPC-B, 64 clients (saturation)
#   bulk       one transaction inserting ~1 GB (sustained WAL throughput; gated)
#   bulk-burst one transaction inserting ~250 MB (a burst the drive cache
#              absorbs; reported, not gated)
set -euo pipefail
name=$1 duration=$2 warmup=$3 scale=$4
export PGPASSWORD=${PGPASSWORD:-cloud_admin}
PG=(-h 127.0.0.1 -p 55433 -U cloud_admin)
out=/tmp/bench/$name
rm -rf "$out"; mkdir -p "$out"; cd "$out"

sql() { psql "${PG[@]}" -X -qAt postgres -c "$1"; }

case $name in
  commit-*)
    clients=${name#commit-}
    sql "CREATE TABLE IF NOT EXISTS t(id bigserial PRIMARY KEY, v text)"
    echo 'INSERT INTO t(v) VALUES (repeat($$x$$,100));' > commit.sql
    pgbench "${PG[@]}" -n -f commit.sql -c "$clients" -j "$clients" -T "$warmup" postgres >/dev/null 2>&1
    echo MEASURE_START
    pgbench "${PG[@]}" -n -f commit.sql -c "$clients" -j "$clients" -T "$duration" \
      -l --log-prefix=tx postgres > summary.txt 2>&1
    echo MEASURE_END
    ;;
  tpcb-*)
    clients=${name#tpcb-}
    if [ "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'pgbench_accounts'")" = 0 ]; then
      pgbench "${PG[@]}" -i -s "$scale" -q postgres >/dev/null 2>&1
    fi
    pgbench "${PG[@]}" -n -c "$clients" -j "$clients" -T "$warmup" postgres >/dev/null 2>&1
    echo MEASURE_START
    pgbench "${PG[@]}" -n -c "$clients" -j "$clients" -T "$duration" \
      -l --log-prefix=tx postgres > summary.txt 2>&1
    echo MEASURE_END
    ;;
  bulk | bulk-burst)
    rows=1000000
    [ "$name" = bulk-burst ] && rows=250000
    sql "DROP TABLE IF EXISTS bulk; CREATE TABLE bulk(v text)"
    start=$(sql "SELECT pg_current_wal_lsn()")
    echo MEASURE_START
    t0=$(date +%s%N)
    sql "INSERT INTO bulk SELECT repeat('x', 1000) FROM generate_series(1, $rows)"
    t1=$(date +%s%N)
    echo MEASURE_END
    end=$(sql "SELECT pg_current_wal_lsn()")
    bytes=$(sql "SELECT pg_wal_lsn_diff('$end', '$start')")
    echo "bulk wal_bytes=$bytes nanos=$((t1 - t0))" > summary.txt
    ;;
  *) echo "unknown workload $name" >&2; exit 2 ;;
esac
cat summary.txt
