#!/usr/bin/env bash
# The P4b benchmark (docs/design/28-loam-postgres.md §7): pgbench through a
# Neon compute whose WAL goes to stock safekeepers (the baseline) or to Loam's
# WAL service, on the same host and topology.
#
#   scripts/loam-pg-bench/run.sh --variant VARIANT [--replicas 1|3]
#       [--duration S] [--warmup S] [--scale N] [--workloads "commit-1 ..."]
#       [--label L] [--out DIR] [--disk-root DIR] [--feeder-root DIR]
#       [--io-depth N] [--keep] [--force]
#       [--store tikv-raw|tikv] [--depth N] [--kv-config FILE] [--no-place]
#
# Variants:
#   safekeepers    stock Neon safekeepers (the baseline)
#   loam           loam-wal --store tikv on a TiKV playground (P4a, §7.1)
#   nvme-buffered  Arm A (§7.2): <replicas> loam-wal acceptors, local journal,
#                  pwrite + fdatasync, tokio front end
#   nvme-pwritev2  Arm A, O_DIRECT + pwritev2(RWF_DSYNC), tokio front end
#   nvme-uring     Arm A, compio shards with io_uring
#   nvme-sqpoll    Arm A, compio shards with io_uring and SQPOLL
#
# Each run uses a fresh tenant and timeline and a fresh compute, and writes
# <out>/<date>-<git-sha>-<variant>-rf<replicas>[-<label>].json with TPS,
# latency percentiles and the WAL tier's CPU time per commit. Compare runs
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
# --disk-root puts every WAL tier's data (the safekeepers' volumes, the
# acceptors' journals) under one directory, to compare filesystems (for
# example an ext4 partition against btrfs). Without it, safekeepers use
# podman volumes and the acceptors' journals go to target/loam-pg-bench/nvme,
# on the same filesystem as those volumes.
#
# The Arm A variants feed the pageserver through one stock --no-sync
# safekeeper that acceptor 1 streams committed WAL to (feeder.rs, D271); its
# data goes to --feeder-root, on another filesystem than the journals.
#
# Needs: podman (or docker) with docker-compose, and the loam-wal binary:
#   cargo build --release -p operon-safekeeper --features server,tikv,nvme \
#     --bin loam-wal         (add compio for nvme-uring and nvme-sqpoll)
# The loam variant also needs tiup (scripts/tikv).
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DEPLOY=$ROOT/deploy/loam-pg-bench

variant= replicas=1 duration=60 warmup=10 scale=10 label= keep=0 force=0
store=tikv-raw depth=8 kv_config= place=1
workloads="commit-1 commit-16 tpcb-16 bulk bulk-burst"
out=$ROOT/bench/results disk_root= feeder_root=
io_depth=4
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
    --disk-root) disk_root=$2; shift 2 ;;
    --feeder-root) feeder_root=$2; shift 2 ;;
    --io-depth) io_depth=$2; shift 2 ;;
    --keep) keep=1; shift ;;
    --force) force=1; shift ;;
    --store | --depth | --kv-config)
      case ${2-} in "" | --*) echo "run: $1 needs a value" >&2; exit 2 ;; esac
      case $1 in
        --store) store=$2 ;;
        --depth) depth=$2 ;;
        --kv-config) kv_config=$(realpath "$2") ;;
      esac
      shift 2 ;;
    --no-place) place=0; shift ;;
    *) sed -n '5,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
case $variant in
  safekeepers | loam) ;;
  nvme-buffered) nvme_args=(--io buffered) ;;
  nvme-pwritev2) nvme_args=(--io pwritev2 --io-depth "$io_depth") ;;
  nvme-uring) nvme_args=(--runtime compio --io uring --io-depth "$io_depth") ;;
  nvme-sqpoll) nvme_args=(--runtime compio --io uring --io-depth "$io_depth" --uring-sqpoll) ;;
  *) echo "run: unknown --variant '$variant' (see the header)" >&2; exit 2 ;;
esac
case $replicas in 1 | 3) ;; *) echo "run: --replicas 1|3" >&2; exit 2 ;; esac
case $depth in "" | *[!0-9]* | 0) echo "run: --depth N (a positive integer)" >&2; exit 2 ;; esac
case $store in tikv | tikv-raw) ;; *) echo "run: --store tikv|tikv-raw" >&2; exit 2 ;; esac
kv_config=${kv_config:-$ROOT/deploy/loam-pg-bench/tikv.toml}
out=$(realpath -m "$out")
cd "$DEPLOY"

# One benchmark at a time per host: the compose project, the ports and the
# p99s are shared (a second run.sh waits here).
LOCK=${LOAM_BENCH_LOCK:-$HOME/.cache/loam-pg-bench.lock}
mkdir -p "$(dirname "$LOCK")"
exec 9>"$LOCK"
flock 9

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
# The feeder's data, by default inside the repository's target directory.
feeder_root=${feeder_root:-$RUN_DIR/feeder}
mkdir -p "$RUN_DIR" "$out"
log() { echo "run: $*" >&2; }

stop_loam_wal() {
  if [ -f "$RUN_DIR/loam-wal.pids" ]; then
    # shellcheck disable=SC2046
    kill $(cat "$RUN_DIR/loam-wal.pids") 2>/dev/null || true
    for _ in $(seq 1 50); do
      # shellcheck disable=SC2046
      kill -0 $(cat "$RUN_DIR/loam-wal.pids") 2>/dev/null || break
      sleep 0.1
    done
    rm -f "$RUN_DIR/loam-wal.pids"
  fi
}
cleanup() {
  [ "$keep" = 1 ] && return
  "${COMPOSE[@]}" rm -sf compute >/dev/null 2>&1 || true
  stop_loam_wal
}
trap cleanup EXIT
stop_loam_wal

# A fresh, world-writable directory (the containers run as user neon; under
# rootless podman their files belong to a subuid, so remove them from inside
# the user namespace).
fresh_dir() {
  rm -rf "$1" 2>/dev/null || { [ "$ENGINE" = podman ] && podman unshare rm -rf "$1"; }
  mkdir -p "$1"
  chmod 777 "$1"
}

# 1. Storage: RustFS, broker, pageserver; the baseline's safekeepers, or the
#    candidate's feeder safekeeper. Only one variant's WAL tier runs at a time.
ALL=(--profile sk --profile sk3 --profile loam)
"${COMPOSE[@]}" "${ALL[@]}" rm -sf safekeeper1 safekeeper2 safekeeper3 feeder-safekeeper >/dev/null 2>&1 || true
if [ -n "$disk_root" ]; then
  for n in 1 2 3; do fresh_dir "$disk_root/sk$n"; done
  export SK1_DATA=$disk_root/sk1 SK2_DATA=$disk_root/sk2 SK3_DATA=$disk_root/sk3
fi
if [ "$variant" = safekeepers ]; then
  wal_services="safekeeper1"
  [ "$replicas" = 3 ] && wal_services="safekeeper1 safekeeper2 safekeeper3"
else
  wal_services="feeder-safekeeper"
  if [ "$variant" != loam ]; then
    fresh_dir "$feeder_root"
    export SKF_DATA=$feeder_root
  fi
fi
# shellcheck disable=SC2086
"${COMPOSE[@]}" "${ALL[@]}" up -d rustfs create-bucket storage_broker pageserver $wal_services >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf localhost:9898/v1/status >/dev/null && break; sleep 1; done

# 2. The WAL tier.
case $variant in
  safekeepers)
    if [ "$replicas" = 3 ]; then SAFEKEEPERS=127.0.0.1:5454,127.0.0.1:5455,127.0.0.1:5456
    else SAFEKEEPERS=127.0.0.1:5454; fi
    ;;
  loam)
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
        --kv-config "$kv_config" --timeout 180 --force >&2 9>&-
      echo "$cfg_sum" >"$RUN_DIR/kv-config.sum"
    fi
    if [ "$place" = 1 ] && [ "$replicas" = 3 ]; then
      "$ROOT/scripts/loam-pg-bench/place-leaders.sh" --pd "$PD" --zone z1 \
        --mode "$([ "$store" = tikv ] && echo txn || echo raw)" >&2
    fi
    RUST_LOG=${RUST_LOG:-info} setsid nohup "$LOAM_WAL" --listen-pg 127.0.0.1:5460 \
      --listen-http 127.0.0.1:7690 --store "$store" --pipeline-depth "$depth" \
      --pd "$PD" --keyspace loam_pgwal \
      --feed-safekeeper 127.0.0.1:5457 >"$RUN_DIR/loam-wal.log" 2>&1 </dev/null 9>&- &
    echo $! >"$RUN_DIR/loam-wal.pids"
    for _ in $(seq 1 30); do curl -sf localhost:7690/v1/status >/dev/null && break; sleep 1; done
    curl -sf localhost:7690/v1/status >/dev/null ||
      { echo "run: loam-wal did not become ready (see $RUN_DIR/loam-wal.log)" >&2; exit 1; }
    SAFEKEEPERS=127.0.0.1:5460
    ;;
  nvme-*)
    [ -x "$LOAM_WAL" ] || { echo "run: no loam-wal at $LOAM_WAL (see the header)" >&2; exit 1; }
    wal_root=${disk_root:-$RUN_DIR/nvme}
    SAFEKEEPERS=
    : >"$RUN_DIR/loam-wal.pids"
    for i in $(seq 1 "$replicas"); do
      fresh_dir "$wal_root/wal$i"
      feed=()
      [ "$i" = 1 ] && feed=(--feed-safekeeper 127.0.0.1:5457)
      RUST_LOG=${RUST_LOG:-info} setsid nohup "$LOAM_WAL" --id "$i" \
        --listen-pg "127.0.0.1:$((5459 + i))" --listen-http "127.0.0.1:$((7689 + i))" \
        --store nvme --data-dir "$wal_root/wal$i" "${nvme_args[@]}" "${feed[@]}" \
        >"$RUN_DIR/loam-wal-$i.log" 2>&1 </dev/null 9>&- &
      echo $! >>"$RUN_DIR/loam-wal.pids"
      SAFEKEEPERS=${SAFEKEEPERS:+$SAFEKEEPERS,}127.0.0.1:$((5459 + i))
    done
    for i in $(seq 1 "$replicas"); do
      for _ in $(seq 1 30); do curl -sf "localhost:$((7689 + i))/v1/status" >/dev/null && break; sleep 1; done
    done
    grep -h 'I/O tier\|journal ready' "$RUN_DIR"/loam-wal-1.log | sed 's/\x1b\[[0-9;]*m//g' >&2 || true
    ;;
esac

# The WAL tier's processes, for CPU time: the acceptors (and, for loam, TiKV)
# and, separately, the feeder safekeeper that stands in for the decoder.
wal_pids() {
  case $variant in
    safekeepers) pgrep -f 'safekeeper --listen-pg=127.0.0.1:545[456]' || true ;;
    loam) cat "$RUN_DIR/loam-wal.pids"; pgrep -x tikv-server || true ;;
    nvme-*) cat "$RUN_DIR/loam-wal.pids" ;;
  esac
}
feeder_pids() { pgrep -f 'safekeeper --listen-pg=127.0.0.1:5457' || true; }
# utime + stime of the given pids, in clock ticks.
ticks() {
  local sum=0 p f
  for p in "$@"; do
    f=$(cat "/proc/$p/stat" 2>/dev/null) || continue
    f=${f##*) }
    # shellcheck disable=SC2086
    set -- $f
    sum=$((sum + ${12} + ${13}))
  done
  echo "$sum"
}

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

# 4. The workloads, in order, on the same compute. workload.sh prints
#    MEASURE_START / MEASURE_END around the measured phase; CPU is sampled there.
HZ=$(getconf CLK_TCK)
results=()
for w in $workloads; do
  log "workload $w (${duration}s after ${warmup}s warm-up)"
  mapfile -t wp < <(wal_pids)
  mapfile -t fp < <(feeder_pids)
  w0=0 w1=0 f0=0 f1=0 seen=0
  while IFS= read -r line; do
    case $line in
      MEASURE_START) w0=$(ticks "${wp[@]}"); f0=$(ticks "${fp[@]}"); seen=$((seen | 1)) ;;
      MEASURE_END) w1=$(ticks "${wp[@]}"); f1=$(ticks "${fp[@]}"); seen=$((seen | 2)) ;;
      *) echo "$line" >&2 ;;
    esac
  done < <(timeout "${WORKLOAD_TIMEOUT:-$((duration + warmup + 900))}" "$ENGINE" exec "$container" bash /bench/workload.sh "$w" "$duration" "$warmup" "$scale")
  # A workload that failed or never reached its measured phase has no valid CPU figures.
  wait $! || { echo "run: workload $w failed" >&2; exit 1; }
  [ "$seen" = 3 ] || { echo "run: workload $w did not report both MEASURE_START and MEASURE_END" >&2; exit 1; }
  rm -rf "$RUN_DIR/$w"
  "$ENGINE" cp "$container:/tmp/bench/$w" "$RUN_DIR/$w"
  r=$(python3 "$ROOT/scripts/loam-pg-bench/stats.py" "$RUN_DIR/$w" "$w")
  r=$(W="$((w1 - w0))" F="$((f1 - f0))" HZ="$HZ" python3 -c '
import json, os, sys
r = json.loads(sys.argv[1])
hz = int(os.environ["HZ"])
wal, feed = int(os.environ["W"]) / hz, int(os.environ["F"]) / hz
r["wal_cpu_s"], r["feeder_cpu_s"] = round(wal, 2), round(feed, 2)
if r.get("n"):
    r["wal_cpu_us_per_tx"] = round(wal * 1e6 / r["n"], 1)
if r.get("wal_bytes"):
    r["wal_cpu_ms_per_mb"] = round(wal * 1e3 / (r["wal_bytes"] / 1e6), 2)
print(json.dumps(r))' "$r")
  results+=("$r")
  log "${results[-1]}"
done

# 5. The result file.
sha=$(git -C "$ROOT" rev-parse --short HEAD)
file=$out/$(date -u +%Y%m%dT%H%M%SZ)-$sha-$variant-rf$replicas${label:+-$label}.json
neon_image=$("$ENGINE" image inspect --format '{{.Digest}}' "${NEON_REPOSITORY:-ghcr.io/neondatabase}/neon:${NEON_TAG:-latest}" 2>/dev/null || echo unknown)
disk=$(lsblk -dno MODEL "$(df --output=source "$HOME" | tail -1 | sed 's/p\?[0-9]*$//')" 2>/dev/null | head -1 || echo unknown)
wal_fs=$(stat -f -c %T "${disk_root:-$HOME/.local/share/containers}" 2>/dev/null || echo unknown)
printf '%s\n' "${results[@]}" | V="$variant" R="$replicas" L="$label" SHA="$sha" \
  DATE="$(date -u +%FT%TZ)" DISK="$disk" IMG="$neon_image" DUR="$duration" WARM="$warmup" \
  SCALE="$scale" SB="${SHARED_BUFFERS:-2GB}" CF="${COMPUTE_FSYNC:-off}" SK="$SAFEKEEPERS" \
  STORE="$([ "$variant" = loam ] && echo "$store" || echo -)" DEPTH="$depth" IODEPTH="$io_depth" FS="$wal_fs" \
  KVCFG="$([ "$variant" = loam ] && basename "$kv_config" || echo -)" \
  python3 -c '
import json, sys, platform, os
workloads = [json.loads(l) for l in sys.stdin if l.strip()]
print(json.dumps({
  "variant": os.environ["V"], "replicas": int(os.environ["R"]), "label": os.environ["L"],
  "git_sha": os.environ["SHA"], "date": os.environ["DATE"],
  "host": {"kernel": platform.release(), "cpus": os.cpu_count(), "disk": os.environ["DISK"],
           "wal_fs": os.environ["FS"]},
  "versions": {"neon_image": os.environ["IMG"], "tikv": "v8.5.8"},
  "settings": {"duration_s": int(os.environ["DUR"]), "warmup_s": int(os.environ["WARM"]),
               "scale": int(os.environ["SCALE"]), "shared_buffers": os.environ["SB"],
               "compute_fsync": os.environ["CF"], "wal": os.environ["SK"],
               "store": os.environ["STORE"], "pipeline_depth": int(os.environ["DEPTH"]),
               "io_depth": int(os.environ["IODEPTH"]), "tikv_config": os.environ["KVCFG"]},
  "workloads": workloads}, indent=2))' >"$file"
log "wrote $file"
echo "$file"
