#!/usr/bin/env bash
# Runs a language's conformance suite against a real `loams dev` (SDK1 Task 4).
#
#   sdks/conformance/run.sh                      # every SDK's suite, no server
#   sdks/conformance/run.sh typescript           # one
#   sdks/conformance/run.sh typescript --keep    # leave the server running
#
# Without a server the suite replays the committed corpus
# (`sdks/fixtures/recorded`, design §44 §10.4), which is what CI does on every
# change: booting a Rust server for each of the thirteen SDKs on every PR is not
# affordable. With one, the same suite runs against it, which is what catches a
# corpus that has drifted from the API.
#
# Rust builds are capped at -j 4 on a 15 GB machine.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

langs=("typescript")
keep=0
if [ "${1:-}" != "" ] && [ "${1:-}" != "--keep" ]; then
  langs=("$1")
  shift
fi
if [ "${1:-}" = "--keep" ]; then
  keep=1
fi

# Where a Rust build output lands, without hard-coding a target directory: the
# repository's cargo config sends it outside the tree.
# The server's pid, written to a file rather than a shell variable.
#
# `endpoint="$(start_server)"` runs the function in a subshell, so a `pid=$!`
# inside it never reaches the parent and cleanup has nothing to kill: the
# server survives the run and the next one collides with it. Writing the pid to
# a file is what makes the trap work.
PID_FILE=""

loams_bin() {
  cargo build -p loams --message-format=json 2>/dev/null \
    | grep -o '"executable":"[^"]*/loams"' \
    | head -1 \
    | cut -d '"' -f 4
}

start_server() {
  local bin
  bin="$(loams_bin)"
  if [ -z "$bin" ]; then
    echo "could not build loams; running against the recorded corpus" >&2
    return 1
  fi
  local log datadir port endpoint
  log="$(mktemp)"
  # A conformance run wants the API and nothing else, so the optional
  # listeners are switched off: a dev stack that also binds Flight SQL,
  # Qdrant and Elasticsearch fails on a developer machine that already has
  # something on 8082.
  datadir="$(mktemp -d)"
  # `loams dev` does not take port 0, so a free port is chosen here. Two
  # concurrent runs must not collide, which is why this is a probe and not a
  # constant.
  for _ in $(seq 1 10); do
    port=$((20000 + RANDOM % 20000))
    "$bin" dev --listen "127.0.0.1:$port" --data-dir "$datadir" \
      --no-flight-sql --no-qdrant --no-es > "$log" 2>&1 &
    echo "$!" > "$PID_FILE"
    pid="$!"
    endpoint=""
    for _ in $(seq 1 100); do
      endpoint="$(grep -o "http://127\.0\.0\.1:$port" "$log" | head -1 || true)"
      if [ -n "$endpoint" ]; then
        break
      fi
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if [ -n "$endpoint" ]; then
      # The catalogue is the SDK's first call, so wait for the port to answer
      # it rather than for the log line alone.
      for _ in $(seq 1 100); do
        if curl -sf -X POST "$endpoint/loams.instance.v1.InstanceService/GetInstance" \
          -H 'content-type: application/json' -d '{}' > /dev/null; then
          break
        fi
        sleep 0.2
      done
      echo "$endpoint"
      return 0
    fi
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    : > "$PID_FILE"
  done
  echo "loams dev did not come up:" >&2
  cat "$log" >&2
  return 1
}

server=""
started=0
PID_FILE="$(mktemp)"

cleanup() {
  local pid=""
  if [ -s "$PID_FILE" ]; then
    pid="$(cat "$PID_FILE")"
  fi
  if [ -n "$pid" ]; then
    if [ "$keep" -eq 1 ] && [ "$started" -eq 1 ]; then
      echo "leaving $server running (pid $pid)" >&2
      rm -f "$PID_FILE"
      return
    fi
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
}
trap cleanup EXIT

endpoint="$(start_server)" && started=1 || true
if [ "$started" -eq 1 ]; then
  server="$endpoint"
  echo "loams dev on $server"
  export LOAMS_TEST_ENDPOINT="$server"
else
  unset LOAMS_TEST_ENDPOINT || true
  echo "no live server: replaying sdks/fixtures/recorded"
fi

cd web
for lang in "${langs[@]}"; do
  case "$lang" in
    typescript)
      pnpm --filter @loams/client test
      pnpm --filter @loams/live typecheck
      ;;
    *)
      echo "no suite for $lang yet" >&2
      exit 2
      ;;
  esac
done

if [ "$started" -eq 1 ] && [ "$keep" -eq 0 ]; then
  echo "the corpus should match what the server just answered:"
  echo "  LOAMS_TEST_ENDPOINT=$server node sdks/conformance/record-fixtures.mjs"
  echo "  git diff --exit-code -- sdks/fixtures"
fi
