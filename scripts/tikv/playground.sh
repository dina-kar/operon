#!/usr/bin/env bash
# Loams's TiKV dev playground (R1 plan Task 1): PD, TiKV on API v2 with Loams's
# keyspaces, and optionally a keyspace-mode TiDB, from `tiup playground v8.5.8`.
#
#   scripts/tikv/playground.sh start [--with-tidb] [--stores N] [--tag T]
#                                    [--tidb-config FILE] [--kv-config FILE]
#                                    [--timeout S] [--force]
#   scripts/tikv/playground.sh stop  [--tag T]
#   scripts/tikv/playground.sh status [--tag T]
#
# Ports: every Loams playground uses --port-offset 17000 (PD 127.0.0.1:19379,
# TiDB 127.0.0.1:21000), so only one runs at a time on a host. The tag
# defaults to loams-dev; data lives in ~/.tiup/data/<tag> and is deleted on stop.
#
# Install tiup once (user-local, under ~/.tiup):
#   curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
#   export PATH="$HOME/.tiup/bin:$PATH"      # fish: fish_add_path ~/.tiup/bin
# The first start downloads about 500 MB.
set -euo pipefail

VERSION=v8.5.8
PORT_OFFSET=17000
PD_ADDR=127.0.0.1:$((2379 + PORT_OFFSET))

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_DIR=$ROOT/target/tikv-playground
export PATH="$HOME/.tiup/bin:$PATH"

usage() {
  sed -n '5,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 2
}

die() {
  echo "playground: $*" >&2
  exit 1
}

cmd=${1:-}
[ -n "$cmd" ] || usage
shift

tag=loams-dev
with_tidb=0
stores=1
tidb_config=$ROOT/deploy/tikv/tidb.toml
kv_config=$ROOT/deploy/tikv/tikv.toml
timeout=60
force=0
while [ $# -gt 0 ]; do
  case $1 in
    --tag) tag=${2:?--tag needs a value}; shift 2 ;;
    --with-tidb) with_tidb=1; shift ;;
    --stores) stores=${2:?--stores needs a value}; shift 2 ;;
    --tidb-config) tidb_config=${2:?--tidb-config needs a value}; shift 2 ;;
    --kv-config) kv_config=${2:?--kv-config needs a value}; shift 2 ;;
    --timeout) timeout=${2:?--timeout needs a value}; shift 2 ;;
    --force) force=1; shift ;;
    *) usage ;;
  esac
done
case $tag in
  loams-*) ;;
  *) die "the tag must start with loams- (got '$tag')" ;;
esac
case $stores in
  '' | *[!0-9]*) die "--stores takes a positive number (got '$stores')" ;;
esac
[ "$stores" -ge 1 ] || die "--stores takes a positive number (got '$stores')"

pid_file=$RUN_DIR/$tag.pid
log_file=$RUN_DIR/$tag.log
data_dir=$HOME/.tiup/data/$tag

# Pids of every process of this playground: tiup and tiup-playground (their
# command lines hold "playground <version> --tag <tag>") and the servers (tiup
# component binaries whose data lives under ~/.tiup/data/<tag>/).
tag_pids() {
  pgrep -f -- "playground $VERSION --tag $tag( |\$)|/\.tiup/components/.* .*/\.tiup/data/$tag/" || true
}

start() {
  command -v tiup >/dev/null ||
    die "tiup is not installed: curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh"
  # The build-machine rule: never run the playground during a build.
  if [ "$force" = 0 ] && { pgrep -x cargo || pgrep -x rustc; } >/dev/null; then
    die "cargo or rustc is running; start the playground after the build (or pass --force)"
  fi
  if [ -n "$(tag_pids)" ]; then
    die "playground '$tag' is already running (scripts/tikv/playground.sh stop --tag $tag)"
  fi
  if curl -s -m 2 -o /dev/null "http://$PD_ADDR/pd/api/v1/version"; then
    die "something already answers on $PD_ADDR (another Loams playground?)"
  fi
  [ ! -e "$data_dir" ] || die "$data_dir exists from an earlier run; delete it first"

  local db=0 db_args=()
  if [ "$with_tidb" = 1 ]; then
    [ -f "$tidb_config" ] || die "no TiDB config at $tidb_config"
    db=1
    db_args=(--db.config "$tidb_config")
  fi

  mkdir -p "$RUN_DIR"
  echo "playground: starting '$tag' (PD $PD_ADDR, $stores store(s), TiDB: $db); log $log_file"
  # setsid detaches the playground from this shell, so it outlives the script
  # (and a CI step); stop sends SIGINT to the pid recorded here.
  setsid nohup tiup playground "$VERSION" --tag "$tag" --port-offset "$PORT_OFFSET" \
    --pd 1 --kv "$stores" --db "$db" --tiflash 0 --without-monitor \
    --kv.config "$kv_config" --pd.config "$ROOT/deploy/tikv/pd.toml" \
    "${db_args[@]}" >"$log_file" 2>&1 </dev/null &
  echo $! >"$pid_file"

  local ready_args=(--pd "$PD_ADDR" --timeout "$timeout")
  [ "$with_tidb" = 1 ] && ready_args+=(--with-tidb)
  if ! "$ROOT/scripts/tikv/wait-ready.sh" "${ready_args[@]}"; then
    echo "playground: not ready; last lines of $log_file:" >&2
    tail -n 20 "$log_file" >&2 || true
    stop
    exit 1
  fi
  echo "playground: '$tag' ready. export LOAMS_TEST_PD=$PD_ADDR"
}

stop() {
  local pids
  if [ -f "$pid_file" ]; then
    kill -INT "$(cat "$pid_file")" 2>/dev/null || true
  fi
  # tiup forwards SIGINT to tiup-playground, which stops the servers; send it
  # to tiup-playground directly too in case tiup has already gone.
  pgrep -f -- "tiup-playground $VERSION --tag $tag( |\$)" | xargs -r kill -INT 2>/dev/null || true
  for _ in $(seq 1 60); do
    pids=$(tag_pids)
    [ -z "$pids" ] && break
    sleep 1
  done
  pids=$(tag_pids)
  if [ -n "$pids" ]; then
    echo "playground: '$tag' still running after 60 s; sending SIGKILL" >&2
    echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    sleep 1
  fi
  rm -f "$pid_file"
  # `tiup clean <tag>` fails after an INT shutdown ("missing meta file").
  rm -rf "$data_dir"
  echo "playground: '$tag' stopped; $data_dir deleted"
}

status() {
  if [ -n "$(tag_pids)" ]; then
    echo "playground: '$tag' running (PD $PD_ADDR)"
  else
    echo "playground: '$tag' not running"
    return 1
  fi
}

case $cmd in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) usage ;;
esac
