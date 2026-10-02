#!/usr/bin/env bash
# Waits until a Loams TiKV playground is ready (R1 plan Task 1): PD lists every
# keyspace pre-allocated in deploy/tikv/pd.toml, and with --with-tidb the TiDB
# answers `select 1` over MySQL.
#
#   scripts/tikv/wait-ready.sh [--pd HOST:PORT] [--with-tidb] [--timeout S]
#
# Defaults: PD 127.0.0.1:19379 (port offset 17000), 60 s. TiDB is expected on
# the PD port offset too (127.0.0.1:21000).
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
pd=127.0.0.1:19379
tidb_host=127.0.0.1
tidb_port=21000
with_tidb=0
timeout=60
while [ $# -gt 0 ]; do
  case $1 in
    --pd) pd=${2:?--pd needs a value}; shift 2 ;;
    --with-tidb) with_tidb=1; shift ;;
    --timeout) timeout=${2:?--timeout needs a value}; shift 2 ;;
    *) sed -n '6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
  esac
done

# The pre-allocated keyspaces, from the one place that lists them.
keyspaces=$(sed -n 's/^pre-alloc *= *\[\(.*\)\]/\1/p' "$ROOT/deploy/tikv/pd.toml" | tr -d '" ' | tr ',' ' ')
[ -n "$keyspaces" ] || { echo "wait-ready: no pre-alloc list in deploy/tikv/pd.toml" >&2; exit 1; }

deadline=$((SECONDS + timeout))
missing=$keyspaces
while [ $SECONDS -lt $deadline ]; do
  listed=$(curl -s -m 3 "http://$pd/pd/api/v2/keyspaces" 2>/dev/null || true)
  missing=
  for ks in $keyspaces; do
    case $listed in
      *"\"name\":\"$ks\""* | *"\"name\": \"$ks\""*) ;;
      *) missing="$missing $ks" ;;
    esac
  done
  [ -z "$missing" ] && break
  sleep 1
done
if [ -n "$missing" ]; then
  echo "wait-ready: PD at $pd does not list the keyspaces:$missing (after ${timeout} s)" >&2
  exit 1
fi
echo "wait-ready: PD at $pd lists $keyspaces"

if [ "$with_tidb" = 1 ]; then
  command -v mysql >/dev/null || { echo "wait-ready: no mysql client (install the MariaDB client package)" >&2; exit 1; }
  until mysql -h"$tidb_host" -P"$tidb_port" -uroot -e 'select 1' >/dev/null 2>&1; do
    if [ $SECONDS -ge $deadline ]; then
      echo "wait-ready: TiDB at $tidb_host:$tidb_port does not answer (after ${timeout} s)" >&2
      exit 1
    fi
    sleep 1
  done
  echo "wait-ready: TiDB at $tidb_host:$tidb_port answers"
fi
