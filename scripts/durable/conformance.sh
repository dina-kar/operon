#!/usr/bin/env bash
# Resonate's linearizability check against the durable server embedded in
# `loams dev` (D1 plan Task 5, D144).
#
#   scripts/durable/conformance.sh --store sqlite|tidb [--clients N] [--ops M] [--seed S]
#                                  [--tidb-url URL] [--loams-bin PATH] [--fork DIR] [--out DIR]
#   scripts/durable/conformance.sh --self-test [--fork DIR]
#
# A run:
#   1. builds `loams` (release, --features durable-mysql) unless --loams-bin
#      names one, and, from the pinned fork (the rev in Cargo.toml),
#      `conctrace` (`cargo build --release --example conctrace` in
#      impl/server/core, target dir $DURABLE_FORK_TARGET_DIR, default
#      ~/.cache/cargo-target/durable-fork);
#   2. starts `loams dev --durable-debug --durable-listen 127.0.0.1:<free>`
#      on a fresh store: a new data directory, and for tidb a new database on
#      the TiDB at --tidb-url (default $LOAMS_TEST_TIDB, an admin URL without
#      a database), migrated with `loams durable migrate` first;
#   3. records a concurrent history with `conctrace --clients N --ops M
#      --seed S` (defaults 8, 600, 1);
#   4. prints the status tally (2xx, 4xx, 5xx);
#   5. on tidb, drops the 503 answers as not applied (porc-503.sh), until
#      upstream PR 0b teaches the checker 503;
#   6. runs `go run ./cmd/conccheck -partition=false` in the fork's
#      spec/valid/porc, and exits 0 only if both read disciplines answer
#      LINEARIZABLE (a TIMEOUT or INCONCLUSIVE verdict fails the run too).
#
# --self-test needs no server: the checker must accept a small recorded
# history (scripts/durable/testdata/embedded-sqlite.history), must refute
# the same history doctored so that one settle is answered twice with
# different values, and porc-503.sh must drop exactly an injected 503.
#
# The fork is --fork DIR (or $DURABLE_FORK_DIR), which must be checked out at
# the pinned rev; without either, the rev is fetched into
# target/durable-fork/resonate. Go (1.24) and python3 must be on PATH; tidb
# also needs a mysql or mariadb client. Output goes to --out (default
# target/durable-conformance/<store>-c<N>-o<M>-s<S>); $GITHUB_STEP_SUMMARY,
# when set, gets the result line and the tally.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
HERE=$ROOT/scripts/durable
FORK_URL=https://github.com/ostrium-labs/resonate
FIXTURE=$HERE/testdata/embedded-sqlite.history

usage() {
  sed -n '5,7p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

die() {
  echo "conformance: $*" >&2
  exit 1
}

store=
clients=8
ops=600
seed=1
tidb_url=${LOAMS_TEST_TIDB:-}
loams_bin=
fork=${DURABLE_FORK_DIR:-}
out=
self_test=0
# Globals, for the EXIT traps: the loams pid, the TiDB database to drop,
# and the self-test's scratch directory.
dir=
pid=
database=
mysql_client=
admin_user=
admin_host=
admin_port=
while [ $# -gt 0 ]; do
  case $1 in
    --store) store=${2:?--store needs a value}; shift 2 ;;
    --clients) clients=${2:?--clients needs a value}; shift 2 ;;
    --ops) ops=${2:?--ops needs a value}; shift 2 ;;
    --seed) seed=${2:?--seed needs a value}; shift 2 ;;
    --tidb-url) tidb_url=${2:?--tidb-url needs a value}; shift 2 ;;
    --loams-bin) loams_bin=${2:?--loams-bin needs a value}; shift 2 ;;
    --fork) fork=${2:?--fork needs a value}; shift 2 ;;
    --out) out=${2:?--out needs a value}; shift 2 ;;
    --self-test) self_test=1; shift ;;
    -h | --help) usage ;;
    *) echo "conformance: unknown argument '$1'" >&2; usage ;;
  esac
done
for n in "$clients" "$ops" "$seed"; do
  case $n in
    '' | *[!0-9]*) die "--clients, --ops and --seed take numbers (got '$n')" ;;
  esac
done

command -v go >/dev/null || die "go is not on PATH (the checker is Go; 1.24 or later)"
command -v python3 >/dev/null || die "python3 is not on PATH"

# The fork rev every Resonate crate is pinned to (D140, X1).
rev=$(sed -n 's/^resonate-base = .*rev = "\([0-9a-f]\{40\}\)".*/\1/p' "$ROOT/Cargo.toml")
[ -n "$rev" ] || die "no resonate-base rev in Cargo.toml"

fork_checkout() {
  if [ -n "$fork" ]; then
    git -C "$fork" rev-parse --git-dir >/dev/null 2>&1 || die "--fork $fork is not a git checkout"
    local head
    head=$(git -C "$fork" rev-parse HEAD)
    [ "$head" = "$rev" ] || die "--fork $fork is at $head, not the pinned rev $rev"
    return
  fi
  fork=$ROOT/target/durable-fork/resonate
  if [ "$(git -C "$fork" rev-parse HEAD 2>/dev/null || true)" != "$rev" ]; then
    echo "conformance: fetching $FORK_URL at $rev into $fork"
    rm -rf "$fork"
    mkdir -p "$fork"
    git -C "$fork" init -q
    git -C "$fork" fetch -q --depth 1 "$FORK_URL" "$rev"
    git -C "$fork" checkout -q FETCH_HEAD
  fi
}

# conccheck on <history>, the verdict in <log>. 0 only if both read
# disciplines linearize: conccheck itself exits 0 on TIMEOUT and INCONCLUSIVE.
check() {
  local history=$1 log=$2 status=0
  (cd "$fork/spec/valid/porc" && go run ./cmd/conccheck -partition=false <"$history") >"$log" 2>&1 ||
    status=$?
  cat "$log"
  [ "$status" = 0 ] || return 1
  [ "$(grep -c ' LINEARIZABLE  (some order works)' "$log")" = 2 ] || return 1
  ! grep -qE 'NOT LINEARIZABLE|TIMEOUT|INCONCLUSIVE' "$log"
}

summary() {
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '%s\n' "$@" >>"$GITHUB_STEP_SUMMARY"
  fi
}

self_test() {
  fork_checkout
  [ -f "$FIXTURE" ] || die "the fixture $FIXTURE is missing"
  dir=$(mktemp -d "${TMPDIR:-/tmp}/loams-conformance-self-test.XXXXXX")
  trap 'rm -rf "$dir"' EXIT

  echo "self-test 1/3: the recorded history linearizes"
  check "$FIXTURE" "$dir/clean.txt" || die "self-test: the checker refuses the clean fixture"

  echo "self-test 2/3: a settle answered twice with different values is refuted"
  python3 - "$FIXTURE" "$dir/doctored.history" <<'PY'
import copy
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
settled = [r for r in rows
           if r["kind"] == "promise.settle" and r["res"]["head"]["status"] == 200]
if not settled:
    sys.exit("self-test: the fixture has no settle answered 200")
twice = copy.deepcopy(settled[0])
# After everything else, so no order can put it first: the promise is already
# settled, and the doctored answer carries another value.
twice["now"] = max(r["now"] for r in rows) + 1
twice["call"] = max(r["return"] for r in rows) + 1
twice["return"] = twice["call"] + 1
twice["req"]["value"] = {"data": "doctored"}
twice["res"]["data"]["promise"]["value"] = {"data": "doctored"}
with open(sys.argv[2], "w", encoding="utf-8") as out:
    for r in rows + [twice]:
        out.write(json.dumps(r, separators=(",", ":")) + "\n")
PY
  local status=0
  (cd "$fork/spec/valid/porc" && go run ./cmd/conccheck -partition=false <"$dir/doctored.history") \
    >"$dir/doctored.txt" 2>&1 || status=$?
  cat "$dir/doctored.txt"
  [ "$status" = 1 ] && grep -q 'NOT LINEARIZABLE' "$dir/doctored.txt" ||
    die "self-test: the doctored history was not refuted (conccheck exit $status)"
  # The script's own verdict must fail on it too.
  if check "$dir/doctored.history" "$dir/doctored-again.txt" >/dev/null; then
    die "self-test: check() accepted the doctored history"
  fi

  echo "self-test 3/3: porc-503.sh drops exactly the 503 answers"
  python3 - "$FIXTURE" "$dir/with-503.history" <<'PY'
import copy
import json
import sys

lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = copy.deepcopy(json.loads(lines[len(lines) // 2]))
row["res"] = {"kind": row["kind"], "data": "Serialization failure, please retry",
              "head": {"corrId": "conctrace", "status": 503, "version": "2026-04-01"}}
lines.insert(len(lines) // 2, json.dumps(row, separators=(",", ":")) + "\n")
open(sys.argv[2], "w", encoding="utf-8").writelines(lines)
PY
  local line
  line=$("$HERE/porc-503.sh" "$dir/with-503.history" "$dir/rewritten.history")
  echo "$line"
  case $line in
    "porc-503: 1 of "*) ;;
    *) die "self-test: porc-503.sh did not count one 503" ;;
  esac
  cmp -s <(grep -v '^$' "$FIXTURE") "$dir/rewritten.history" ||
    die "self-test: porc-503.sh changed rows other than the 503"
  echo "conformance: self-test passed"
}

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

run() {
  case $store in
    sqlite | tidb) ;;
    '') die "--store sqlite|tidb is required" ;;
    *) die "--store is sqlite or tidb (got '$store')" ;;
  esac
  if [ "$store" = tidb ]; then
    [ -n "$tidb_url" ] ||
      die "--store tidb needs --tidb-url or LOAMS_TEST_TIDB (scripts/durable/tidb.sh up prints it)"
    mysql_client=$(command -v mariadb || command -v mysql || true)
    [ -n "$mysql_client" ] || die "--store tidb needs a mysql or mariadb client"
  fi

  fork_checkout
  local fork_target=${DURABLE_FORK_TARGET_DIR:-$HOME/.cache/cargo-target/durable-fork}
  echo "conformance: building conctrace from the fork at $rev"
  (cd "$fork/impl/server/core" &&
    CARGO_TARGET_DIR=$fork_target cargo build --release --locked --example conctrace)
  local conctrace=$fork_target/release/examples/conctrace

  if [ -z "$loams_bin" ]; then
    echo "conformance: building loams (release, durable-mysql)"
    (cd "$ROOT" && cargo build --release --locked -p loams --features durable-mysql)
    loams_bin=${CARGO_TARGET_DIR:-$ROOT/target}/release/loams
  fi
  [ -x "$loams_bin" ] || die "$loams_bin is not an executable"
  loams_bin=$(realpath "$loams_bin")

  [ -n "$out" ] || out=$ROOT/target/durable-conformance/$store-c$clients-o$ops-s$seed
  # Absolute: the checker runs from the fork's directory.
  out=$(realpath -m "$out")
  rm -rf "$out"
  mkdir -p "$out"
  local data=$out/data log=$out/loams.log port
  port=$(free_port)
  local args=(dev --durable-debug --durable-listen "127.0.0.1:$port" --data-dir "$data"
    --listen 127.0.0.1:0 --no-flight-sql --no-qdrant)

  cleanup() {
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
      for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      kill -KILL "$pid" 2>/dev/null || true
    fi
    if [ -n "$database" ]; then
      "$mysql_client" -h"$admin_host" -P"$admin_port" -u"$admin_user" \
        -e "DROP DATABASE IF EXISTS \`$database\`" 2>/dev/null || true
    fi
  }
  trap cleanup EXIT

  if [ "$store" = tidb ]; then
    # mysql://user@host:port with no password (the playground's root): the
    # client gets the same parts.
    read -r admin_user admin_host admin_port < <(python3 - "$tidb_url" <<'PY'
import sys
from urllib.parse import urlsplit
u = urlsplit(sys.argv[1])
if u.scheme != "mysql" or not u.hostname:
    sys.exit("conformance: --tidb-url must be mysql://user@host:port")
if u.password:
    sys.exit("conformance: --tidb-url with a password is not supported (use a throwaway TiDB)")
print(u.username or "root", u.hostname, u.port or 4000)
PY
)
    database=loams_conf_$$_$(date +%s)
    "$mysql_client" -h"$admin_host" -P"$admin_port" -u"$admin_user" -e "CREATE DATABASE \`$database\`"
    local url=mysql://$admin_user@$admin_host:$admin_port/$database
    echo "conformance: migrating the fresh TiDB database $database"
    "$loams_bin" durable migrate --durable-store "$url"
    args+=(--durable-store "$url")
  fi

  echo "conformance: loams ${args[*]}"
  "$loams_bin" "${args[@]}" >"$log" 2>&1 &
  pid=$!
  local ready=0
  for _ in $(seq 1 120); do
    if curl -sf "http://127.0.0.1:$port/ready" >/dev/null 2>&1; then
      ready=1
      break
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if [ "$ready" != 1 ]; then
    tail -n 30 "$log" >&2 || true
    die "loams dev did not serve the durable API on 127.0.0.1:$port"
  fi

  echo "conformance: conctrace --clients $clients --ops $ops --seed $seed"
  "$conctrace" --url "http://127.0.0.1:$port/" --out "$out/trace" \
    --clients "$clients" --ops "$ops" --seed "$seed"
  cleanup
  pid=
  database=

  local tally
  tally=$(python3 - "$out/trace.history" <<'PY'
import collections
import json
import sys

classes = collections.Counter()
codes = collections.Counter()
for line in open(sys.argv[1], encoding="utf-8"):
    if not line.strip():
        continue
    status = json.loads(line)["res"].get("head", {}).get("status")
    codes[status] += 1
    classes[f"{status // 100}xx" if isinstance(status, int) else "none"] += 1
print("statuses: " + ", ".join(f"{k}={classes[k]}" for k in ("2xx", "3xx", "4xx", "5xx")),
      "(" + ", ".join(f"{k}: {v}" for k, v in sorted(codes.items(), key=str)) + ")")
PY
)
  echo "$tally"

  local history=$out/trace.history rewrite=
  if [ "$store" = tidb ]; then
    rewrite=$("$HERE/porc-503.sh" "$history" "$out/trace.checked.history")
    echo "$rewrite"
    history=$out/trace.checked.history
  fi

  local what="$store, $clients clients x $ops ops, seed $seed"
  if check "$history" "$out/conccheck.txt"; then
    echo "conformance: LINEARIZABLE ($what)"
    summary "### Durable linearizability: LINEARIZABLE ($what)" "" "- $tally" ${rewrite:+"- $rewrite"} ""
  else
    summary "### Durable linearizability: FAILED ($what)" "" "- $tally" ${rewrite:+"- $rewrite"} "" \
      '```' "$(cat "$out/conccheck.txt")" '```' ""
    die "not linearizable, or no verdict ($what); see $out"
  fi
}

if [ "$self_test" = 1 ]; then
  self_test
else
  run
fi
