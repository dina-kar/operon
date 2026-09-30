#!/usr/bin/env bash
# The P4b benchmark (docs/design/28-loam-postgres.md §7): pgbench through a
# Neon compute whose WAL goes to stock safekeepers (the baseline) or to Loam's
# WAL service on TiKV (the candidate), on the same host and topology.
#
#   scripts/loam-pg-bench/run.sh --variant safekeepers|loam [--replicas 1|3]
#       [--duration S] [--warmup S] [--scale N] [--workloads "commit-1 ..."]
#       [--label L] [--out DIR] [--keep] [--force]
#       [--store tikv-raw|tikv] [--depth N] [--kv-config FILE] [--no-place]
#
# Each run uses a fresh tenant and timeline and a fresh compute, and writes
# <out>/<date>-<git-sha>-<variant>-rf<replicas>[-<label>].json. Compare runs
# with scripts/loam-pg-bench/compare.py.
#
# Candidate topology: compute -> loam-wal (host process, --store tikv-raw by
# default: blind pipelined appends, --depth in flight per timeline; or the
# P4a transactional store with --store tikv) -> TiKV playground (<replicas>
# stores, --kv-config, default deploy/loam-pg-bench/tikv.toml). With 3
# stores, place-leaders.sh labels them z1..z3 and pins the loam_pgwal leaders
# to z1, the compute's zone (--no-place skips it). The
# pageserver still ingests through one stock safekeeper (--no-sync) that
# loam-wal feeds with committed WAL off the commit path (crates/
# operon-safekeeper feeder.rs), until the WAL service serves the interpreted
# protocol itself (§28 Q112).
#
# Needs: podman (or docker) with docker-compose, tiup (scripts/tikv), and the
# loam-wal binary: cargo build --release -p operon-safekeeper \
#   --features server,tikv --bin loam-wal
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DEPLOY=$ROOT/deploy/loam-pg-bench

variant= replicas=1 duration=60 warmup=10 scale=10 label= keep=0 force=0
store=tikv-raw depth=8 kv_config= place=1
workloads="commit-1 commit-16 tpcb-16 bulk"
out=$ROOT/bench/results
while [ $# -gt 0 ]; do
  case $1 in
    --variant) variant=$2; shift 2 ;;
    --replicas) replicas=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --warmup) warmup=$2; shift 2 ;;
    --scale) scale=$2; shift 2 ;;
    --workloads) workloads=$2; shift 2 ;;
    --label) label=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --keep) keep=1; shift ;;
    --force) force=1; shift ;;
    --store) store=$2; shift 2 ;;
    --depth) depth=$2; shift 2 ;;
    --kv-config) kv_config=$(realpath "$2"); shift 2 ;;
    --no-place) place=0; shift ;;
    *) sed -n '5,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
case $variant in safekeepers | loam) ;; *) echo "run: --variant safekeepers|loam" >&2; exit 2 ;; esac
case $replicas in 1 | 3) ;; *) echo "run: --replicas 1|3" >&2; exit 2 ;; esac
case $store in tikv | tikv-raw) ;; *) echo "run: --store tikv|tikv-raw" >&2; exit 2 ;; esac
kv_config=${kv_config:-$ROOT/deploy/loam-pg-bench/tikv.toml}
out=$(realpath -m "$out")
cd "$DEPLOY"

# p99s on a shared machine: wait until no build runs (--force skips this).
if [ "$force" = 0 ]; then
  while pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; do
    echo "run: waiting for cargo/rustc to finish (--force to skip)" >&2
    sleep 30
  done
fi

if [ -z "${DOCKER_HOST:-}" ] && [ -S "/run/user/$(id -u)/podman/podman.sock" ]; then
  export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
fi
COMPOSE=(docker-compose)
command -v docker-compose >/dev/null || COMPOSE=(docker compose)
# The container engine for exec/cp/inspect: podman when its socket is in use.
ENGINE=docker
case ${DOCKER_HOST:-} in *podman*) ENGINE=podman ;; esac
command -v "$ENGINE" >/dev/null || ENGINE=podman
LOAM_WAL=${LOAM_WAL:-$(cargo metadata --format-version 1 --no-deps 2>/dev/null |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/loam-wal}
PD=127.0.0.1:19379
TAG=loam-bench
RUN_DIR=$ROOT/target/loam-pg-bench
mkdir -p "$RUN_DIR" "$out"
log() { echo "run: $*" >&2; }

cleanup() {
  [ "$keep" = 1 ] && return
  "${COMPOSE[@]}" rm -sf compute >/dev/null 2>&1 || true
  if [ -f "$RUN_DIR/loam-wal.pid" ]; then
    kill "$(cat "$RUN_DIR/loam-wal.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/loam-wal.pid"
  fi
}
trap cleanup EXIT

# 1. Storage: RustFS, broker, pageserver; the baseline's safekeepers, or the
#    candidate's feeder safekeeper. Only one variant's WAL tier runs at a time.
ALL=(--profile sk --profile sk3 --profile loam)
if [ "$variant" = safekeepers ]; then
  "${COMPOSE[@]}" "${ALL[@]}" rm -sf feeder-safekeeper >/dev/null 2>&1 || true
  [ "$replicas" = 1 ] && { "${COMPOSE[@]}" "${ALL[@]}" rm -sf safekeeper2 safekeeper3 >/dev/null 2>&1 || true; }
  wal_services="safekeeper1"
  [ "$replicas" = 3 ] && wal_services="safekeeper1 safekeeper2 safekeeper3"
else
  "${COMPOSE[@]}" "${ALL[@]}" rm -sf safekeeper1 safekeeper2 safekeeper3 >/dev/null 2>&1 || true
  wal_services="feeder-safekeeper"
fi
# shellcheck disable=SC2086
"${COMPOSE[@]}" "${ALL[@]}" up -d rustfs create-bucket storage_broker pageserver $wal_services >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf localhost:9898/v1/status >/dev/null && break; sleep 1; done

# 2. The WAL tier.
if [ "$variant" = safekeepers ]; then
  if [ "$replicas" = 3 ]; then SAFEKEEPERS=127.0.0.1:5454,127.0.0.1:5455,127.0.0.1:5456
  else SAFEKEEPERS=127.0.0.1:5454; fi
else
  [ -x "$LOAM_WAL" ] || { echo "run: no loam-wal at $LOAM_WAL (see the header)" >&2; exit 1; }
  stores=$(curl -sf "http://$PD/pd/api/v1/stores" 2>/dev/null |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null || echo 0)
  # A playground started with another store count or TiKV config is restarted.
  cfg_sum=$(sha256sum "$kv_config" | cut -c1-16)
  if "$ROOT/scripts/tikv/playground.sh" status --tag "$TAG" >/dev/null 2>&1 &&
    { [ "$stores" != "$replicas" ] || [ "$(cat "$RUN_DIR/kv-config.sum" 2>/dev/null)" != "$cfg_sum" ]; }; then
    log "restarting the playground with $replicas store(s) and $(basename "$kv_config")"
    "$ROOT/scripts/tikv/playground.sh" stop --tag "$TAG" >&2
  fi
  if ! "$ROOT/scripts/tikv/playground.sh" status --tag "$TAG" >/dev/null 2>&1; then
    "$ROOT/scripts/tikv/playground.sh" start --tag "$TAG" --stores "$replicas" \
      --kv-config "$kv_config" --timeout 180 --force >&2
    echo "$cfg_sum" >"$RUN_DIR/kv-config.sum"
  fi
  if [ "$place" = 1 ] && [ "$replicas" = 3 ]; then
    "$ROOT/scripts/loam-pg-bench/place-leaders.sh" --pd "$PD" --zone z1 >&2
  fi
  RUST_LOG=${RUST_LOG:-info} setsid nohup "$LOAM_WAL" --listen-pg 127.0.0.1:5460 \
    --listen-http 127.0.0.1:7690 --store "$store" --pipeline-depth "$depth" \
    --pd "$PD" --keyspace loam_pgwal \
    --feed-safekeeper 127.0.0.1:5457 >"$RUN_DIR/loam-wal.log" 2>&1 </dev/null &
  echo $! >"$RUN_DIR/loam-wal.pid"
  for _ in $(seq 1 30); do curl -sf localhost:7690/v1/status >/dev/null && break; sleep 1; done
  SAFEKEEPERS=127.0.0.1:5460
fi

# 3. A fresh tenant, timeline and compute.
export TENANT_ID=$(openssl rand -hex 16) TIMELINE_ID=$(openssl rand -hex 16) SAFEKEEPERS
curl -sf -X PUT -H 'Content-Type: application/json' \
  -d '{"mode":"AttachedSingle","generation":1,"tenant_conf":{}}' \
  "localhost:9898/v1/tenant/$TENANT_ID/location_config" >/dev/null
curl -sf -X POST -H 'Content-Type: application/json' \
  -d "{\"new_timeline_id\":\"$TIMELINE_ID\",\"pg_version\":${PG_VERSION:-16}}" \
  "localhost:9898/v1/tenant/$TENANT_ID/timeline/" >/dev/null
"${COMPOSE[@]}" rm -sf compute >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d compute >/dev/null 2>&1
container=$("${COMPOSE[@]}" ps -q compute)
for _ in $(seq 1 90); do
  "$ENGINE" exec "$container" pg_isready -q -h 127.0.0.1 -p 55433 2>/dev/null && break
  sleep 1
done
"$ENGINE" exec "$container" pg_isready -q -h 127.0.0.1 -p 55433 ||
  { echo "run: the compute did not become ready" >&2; exit 1; }
log "variant=$variant replicas=$replicas tenant=$TENANT_ID timeline=$TIMELINE_ID wal=$SAFEKEEPERS"

# 4. The workloads, in order, on the same compute.
results=()
for w in $workloads; do
  log "workload $w (${duration}s after ${warmup}s warm-up)"
  "$ENGINE" exec "$container" bash /bench/workload.sh "$w" "$duration" "$warmup" "$scale" >&2
  rm -rf "$RUN_DIR/$w"
  "$ENGINE" cp "$container:/tmp/bench/$w" "$RUN_DIR/$w"
  results+=("$(python3 "$ROOT/scripts/loam-pg-bench/stats.py" "$RUN_DIR/$w" "$w")")
  log "${results[-1]}"
done

# 5. The result file.
sha=$(git -C "$ROOT" rev-parse --short HEAD)
file=$out/$(date -u +%Y%m%dT%H%M%SZ)-$sha-$variant-rf$replicas${label:+-$label}.json
neon_image=$("$ENGINE" image inspect --format '{{.Digest}}' "${NEON_REPOSITORY:-ghcr.io/neondatabase}/neon:${NEON_TAG:-latest}" 2>/dev/null || echo unknown)
disk=$(lsblk -dno MODEL "$(df --output=source "$HOME" | tail -1 | sed 's/p\?[0-9]*$//')" 2>/dev/null | head -1 || echo unknown)
printf '%s\n' "${results[@]}" | V="$variant" R="$replicas" L="$label" SHA="$sha" \
  DATE="$(date -u +%FT%TZ)" DISK="$disk" IMG="$neon_image" DUR="$duration" WARM="$warmup" \
  SCALE="$scale" SB="${SHARED_BUFFERS:-2GB}" CF="${COMPUTE_FSYNC:-off}" SK="$SAFEKEEPERS" \
  STORE="$([ "$variant" = loam ] && echo "$store" || echo -)" DEPTH="$depth" \
  KVCFG="$([ "$variant" = loam ] && basename "$kv_config" || echo -)" \
  python3 -c '
import json, sys, platform, os
workloads = [json.loads(l) for l in sys.stdin if l.strip()]
print(json.dumps({
  "variant": os.environ["V"], "replicas": int(os.environ["R"]), "label": os.environ["L"],
  "git_sha": os.environ["SHA"], "date": os.environ["DATE"],
  "host": {"kernel": platform.release(), "cpus": os.cpu_count(), "disk": os.environ["DISK"]},
  "versions": {"neon_image": os.environ["IMG"], "tikv": "v8.5.8"},
  "settings": {"duration_s": int(os.environ["DUR"]), "warmup_s": int(os.environ["WARM"]),
               "scale": int(os.environ["SCALE"]), "shared_buffers": os.environ["SB"],
               "compute_fsync": os.environ["CF"], "wal": os.environ["SK"],
               "store": os.environ["STORE"], "pipeline_depth": int(os.environ["DEPTH"]),
               "tikv_config": os.environ["KVCFG"]},
  "workloads": workloads}, indent=2))' >"$file"
log "wrote $file"
echo "$file"
