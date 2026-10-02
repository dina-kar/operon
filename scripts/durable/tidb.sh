#!/usr/bin/env bash
# A TiDB for the durable store's tests (D1 plan Task 4): a plain TiDB v8.5.8
# from `tiup playground`, with the database loams_durable_default.
#
#   scripts/durable/tidb.sh up     [--tag T] [--port-offset N] [--timeout S] [--force]
#   scripts/durable/tidb.sh down   [--tag T]
#   scripts/durable/tidb.sh status [--tag T] [--port-offset N]
#
# `up` prints the LOAMS_TEST_TIDB line for `cargo test -p loams-durable
# --features mysql --test tidb` and the --durable-store URL for loams.
#
# Defaults: tag loams-durable, port offset 17000 (TiDB on 127.0.0.1:21000),
# which is R1's playground offset: run one of them at a time, or pass another
# offset (D1 uses 27000 beside R1). This is not scripts/tikv/playground.sh:
# that one runs a keyspace-mode TiDB for Loams's TiKV, while Resonate's MySQL
# plugin is verified on a plain TiDB (resonatehq/resonate#1161). Data lives in
# ~/.tiup/data/<tag> and is deleted by `down`.
set -euo pipefail

VERSION=v8.5.8
DATABASE=loams_durable_default

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_DIR=$ROOT/target/durable-tidb
export PATH="$HOME/.tiup/bin:$PATH"

usage() {
  sed -n '5,7p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 2
}

die() {
  echo "tidb: $*" >&2
  exit 1
}

cmd=${1:-}
[ -n "$cmd" ] || usage
shift

tag=loams-durable
offset=17000
timeout=120
force=0
while [ $# -gt 0 ]; do
  case $1 in
    --tag) tag=${2:?--tag needs a value}; shift 2 ;;
    --port-offset) offset=${2:?--port-offset needs a value}; shift 2 ;;
    --timeout) timeout=${2:?--timeout needs a value}; shift 2 ;;
    --force) force=1; shift ;;
    *) usage ;;
  esac
done
case $tag in
  loams-*) ;;
  *) die "the tag must start with loams- (got '$tag')" ;;
esac
case $offset in
  '' | *[!0-9]*) die "--port-offset takes a number (got '$offset')" ;;
esac

port=$((4000 + offset))
pid_file=$RUN_DIR/$tag.pid
log_file=$RUN_DIR/$tag.log
data_dir=$HOME/.tiup/data/$tag

# Pids of this playground: tiup and tiup-playground (their command lines hold
# "playground <version> --tag <tag>") and the servers, whose data lives under
# ~/.tiup/data/<tag>/.
tag_pids() {
  pgrep -f -- "playground $VERSION --tag $tag( |\$)|/\.tiup/components/.* .*/\.tiup/data/$tag/" || true
}

# The MariaDB client's own name first: its `mysql` alias warns on every call.
client=$(command -v mariadb || command -v mysql || true)
sql() {
  "$client" -h127.0.0.1 -P"$port" -uroot "$@" 2> >(grep -v -e 'Deprecated program name' -e 'ssl-verify-server-cert' >&2)
}

up() {
  command -v tiup >/dev/null ||
    die "tiup is not installed: curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh"
  [ -n "$client" ] || die "no mysql client (install the MariaDB client package)"
  # The build-machine rule: never run the playground during a build.
  if [ "$force" = 0 ] && { pgrep -x cargo || pgrep -x rustc; } >/dev/null; then
    die "cargo or rustc is running; start TiDB after the build (or pass --force)"
  fi
  [ -z "$(tag_pids)" ] || die "playground '$tag' is already running (scripts/durable/tidb.sh down --tag $tag)"
  if sql -e 'select 1' >/dev/null 2>&1; then
    die "something already answers MySQL on 127.0.0.1:$port (pass another --port-offset)"
  fi
  [ ! -e "$data_dir" ] || die "$data_dir exists from an earlier run; delete it first"

  mkdir -p "$RUN_DIR"
  echo "tidb: starting '$tag' (TiDB 127.0.0.1:$port, port offset $offset); log $log_file"
  # setsid detaches the playground from this shell, so it outlives the script
  # (and a CI step); down sends SIGINT to the pid recorded here.
  setsid nohup tiup playground "$VERSION" --tag "$tag" --port-offset "$offset" \
    --pd 1 --kv 1 --db 1 --tiflash 0 --without-monitor \
    >"$log_file" 2>&1 </dev/null &
  echo $! >"$pid_file"

  local deadline=$((SECONDS + timeout))
  until sql -e 'select 1' >/dev/null 2>&1; do
    if [ $SECONDS -ge $deadline ]; then
      echo "tidb: TiDB on 127.0.0.1:$port does not answer after ${timeout} s; last lines of $log_file:" >&2
      tail -n 20 "$log_file" >&2 || true
      down
      exit 1
    fi
    sleep 1
  done
  sql -e "CREATE DATABASE IF NOT EXISTS $DATABASE"
  echo "tidb: '$tag' ready ($(sql -N -e 'select version()'))"
  echo "export LOAMS_TEST_TIDB=mysql://root@127.0.0.1:$port"
  echo "loams: --durable-store mysql://root@127.0.0.1:$port/$DATABASE"
}

down() {
  local pids
  if [ -f "$pid_file" ]; then
    kill -INT "$(cat "$pid_file")" 2>/dev/null || true
  fi
  # tiup forwards SIGINT to tiup-playground; send it there too in case tiup
  # has already gone.
  pgrep -f -- "tiup-playground $VERSION --tag $tag( |\$)" | xargs -r kill -INT 2>/dev/null || true
  for _ in $(seq 1 60); do
    pids=$(tag_pids)
    [ -z "$pids" ] && break
    sleep 1
  done
  pids=$(tag_pids)
  if [ -n "$pids" ]; then
    echo "tidb: '$tag' still running after 60 s; sending SIGKILL" >&2
    echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    sleep 1
  fi
  rm -f "$pid_file"
  # `tiup clean <tag>` fails after an INT shutdown ("missing meta file").
  rm -rf "$data_dir"
  echo "tidb: '$tag' stopped; $data_dir deleted"
}

status() {
  if [ -n "$(tag_pids)" ]; then
    echo "tidb: '$tag' running (TiDB 127.0.0.1:$port)"
  else
    echo "tidb: '$tag' not running"
    return 1
  fi
}

case $cmd in
  up) up ;;
  down) down ;;
  status) status ;;
  *) usage ;;
esac
