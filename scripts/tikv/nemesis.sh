#!/usr/bin/env bash
# The R1 nemesis (R1 plan Task 16 semantics 3): runs a command (the reactive
# and transaction checkers) against a Loam playground while it injects
# faults into the playground and the command, one every --interval seconds,
# in round-robin order:
#
#   tikv-kill   SIGKILL a TiKV store, then start it again with the same
#               command line and data (tiup playground does not restart it)
#   pd-kill     SIGKILL the PD leader, then start it again the same way
#   pd-stall    SIGSTOP the PD leader for --pd-stall seconds (past the
#               client's 5 s request timeout), then SIGCONT (owner ruling
#               T2-17: the real death of tikv-client's TSO stream)
#   live-pause  SIGSTOP the command's processes (the Live server runs inside
#               the checkers' test binary) for --pause seconds, then SIGCONT
#
#   scripts/tikv/nemesis.sh [--tag T] [--faults LIST] [--interval S]
#                           [--pd-stall S] [--pause S] [--log FILE] -- COMMAND...
#
# LIST is comma-separated (default: all four). The command gets
# OPERON_CHECKER_NEMESIS=1, and OPERON_NEMESIS_EXPECT_REBUILD=1 when the list
# holds pd-stall, so the checkers tolerate unavailability, and the reactive
# checker asserts that the Live server's TSO supervisor rebuilt its client.
# The exit status is the command's; the fault log goes to stdout and --log.
# Every stopped process is continued on exit.
set -euo pipefail

VERSION=v8.5.8
PORT_OFFSET=17000
PD_ADDR=127.0.0.1:$((2379 + PORT_OFFSET))

usage() {
  sed -n '2,26p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 2
}

tag=loam-dev
faults=tikv-kill,pd-kill,pd-stall,live-pause
interval=20
pd_stall=15
pause=5
log=
while [ $# -gt 0 ]; do
  case $1 in
    --tag) tag=${2:?}; shift 2 ;;
    --faults) faults=${2:?}; shift 2 ;;
    --interval) interval=${2:?}; shift 2 ;;
    --pd-stall) pd_stall=${2:?}; shift 2 ;;
    --pause) pause=${2:?}; shift 2 ;;
    --log) log=${2:?}; shift 2 ;;
    --) shift; break ;;
    *) usage ;;
  esac
done
[ $# -gt 0 ] || usage
case $tag in
  loam-*) ;;
  *) echo "nemesis: the tag must start with loam- (got '$tag')" >&2; exit 2 ;;
esac
IFS=, read -r -a fault_list <<<"$faults"
for f in "${fault_list[@]}"; do
  case $f in
    tikv-kill | pd-kill | pd-stall | live-pause) ;;
    *) echo "nemesis: unknown fault '$f'" >&2; exit 2 ;;
  esac
done

note() {
  local line
  line="nemesis $(date -u +%H:%M:%S) $*"
  echo "$line"
  [ -z "$log" ] || echo "$line" >>"$log"
}

# The pids of the playground's servers of one component (tikv-server or
# pd-server), found by their data under ~/.tiup/data/<tag>/.
servers() {
  pgrep -f -- "/\.tiup/components/[a-z]+/[^ ]*/$1 .*/\.tiup/data/$tag/" || true
}

# Every descendant of a pid (the command's processes).
descendants() {
  local p
  for p in $(pgrep -P "$1" || true); do
    echo "$p"
    descendants "$p"
  done
}

stopped=()
cont_all() {
  local p
  for p in "${stopped[@]}"; do kill -CONT "$p" 2>/dev/null || true; done
  stopped=()
}
trap cont_all EXIT

# The PD leader's pid: the pd-server whose command line names the leader.
pd_leader() {
  local name pid
  name=$(curl -s -m 3 "http://$PD_ADDR/pd/api/v1/leader" | sed -n 's/.*"name": *"\([^"]*\)".*/\1/p' | head -n 1)
  for pid in $(servers pd-server); do
    if [ -n "$name" ] && tr '\0' ' ' <"/proc/$pid/cmdline" | grep -q -- "--name=$name\b"; then
      echo "$pid"
      return
    fi
  done
  # One PD (or the leader is unknown while PD is down): the first one.
  servers pd-server | head -n 1
}

# SIGKILLs `pid` and starts the same command line again from the same
# directory, unless something (tiup) already restarted it.
kill_and_restart() {
  local pid=$1 component=$2 cwd args=() before
  cwd=$(readlink "/proc/$pid/cwd")
  mapfile -d '' -t args <"/proc/$pid/cmdline"
  before=$(servers "$component" | { grep -vx "$pid" || true; } | sort | tr "\n" " ")
  kill -KILL "$pid"
  while kill -0 "$pid" 2>/dev/null; do sleep 0.2; done
  note "killed $component $pid"
  sleep 3
  if [ "$(servers "$component" | sort | tr '\n' ' ')" != "$before" ]; then
    note "$component was restarted by tiup"
    return
  fi
  (cd "$cwd" && setsid nohup "${args[@]}" >>"$cwd/nemesis-restart.log" 2>&1 </dev/null &)
  sleep 1
  note "restarted $component: $(servers "$component" | tr '\n' ' ')"
}

inject() {
  local pid p
  case $1 in
    tikv-kill)
      pid=$(servers tikv-server | shuf -n 1)
      [ -n "$pid" ] || { note "no tikv-server to kill"; return; }
      kill_and_restart "$pid" tikv-server
      ;;
    pd-kill)
      pid=$(pd_leader)
      [ -n "$pid" ] || { note "no pd-server to kill"; return; }
      kill_and_restart "$pid" pd-server
      ;;
    pd-stall)
      pid=$(pd_leader)
      [ -n "$pid" ] || { note "no pd-server to stall"; return; }
      kill -STOP "$pid"
      stopped+=("$pid")
      note "stalled pd-server $pid for ${pd_stall}s"
      sleep "$pd_stall"
      cont_all
      note "continued pd-server $pid"
      ;;
    live-pause)
      local procs=("$cmd_pid")
      mapfile -t -O 1 procs < <(descendants "$cmd_pid")
      for p in "${procs[@]}"; do
        kill -STOP "$p" 2>/dev/null && stopped+=("$p")
      done
      note "paused the command (${#stopped[@]} processes) for ${pause}s"
      sleep "$pause"
      cont_all
      note "continued the command"
      ;;
  esac
}

expect_rebuild=0
case ",$faults," in *,pd-stall,*) expect_rebuild=1 ;; esac
OPERON_CHECKER_NEMESIS=1 OPERON_NEMESIS_EXPECT_REBUILD=$expect_rebuild "$@" &
cmd_pid=$!
note "started the command ($cmd_pid): $*"
i=0
while kill -0 "$cmd_pid" 2>/dev/null; do
  for _ in $(seq 1 "$interval"); do
    kill -0 "$cmd_pid" 2>/dev/null || break 2
    sleep 1
  done
  inject "${fault_list[$((i % ${#fault_list[@]}))]}"
  i=$((i + 1))
done
status=0
wait "$cmd_pid" || status=$?
note "the command exited with $status after $i faults"
exit "$status"
