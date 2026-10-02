#!/usr/bin/env bash
# Leader placement for the Loams WAL benchmark (docs/design/28-loams-postgres.md
# §6.6, §7.3): label the TiKV stores zone=z1..zN and pin the Raft leaders of
# the loams_pgwal keyspace to one zone, the compute's, with the followers in
# the others. Loams's control plane writes the same rule per timeline range
# when it places a compute; the benchmark uses one rule for the keyspace.
#
#   scripts/loams-pg-bench/place-leaders.sh [--pd HOST:PORT] [--zone z1]
#       [--keyspace loams_pgwal] [--mode raw|txn] [--timeout S]
#
# On one host every store shares the disk and there is no network distance,
# so this only matters once cross-AZ delays are modelled; it keeps the
# candidate's topology the one §6.6 specifies.
set -euo pipefail
pd=127.0.0.1:19379 zone=z1 keyspace=loams_pgwal timeout=60 mode=raw
while [ $# -gt 0 ]; do
  case $1 in
    --pd) pd=$2; shift 2 ;;
    --zone) zone=$2; shift 2 ;;
    --keyspace) keyspace=$2; shift 2 ;;
    --mode) mode=$2; shift 2 ;;
    --timeout) timeout=$2; shift 2 ;;
    *) sed -n '8,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
case $mode in raw) prefix=r ;; txn) prefix=x ;; *) echo "place-leaders: --mode raw|txn" >&2; exit 2 ;; esac
api=http://$pd/pd/api

# 1. One zone per store, in store id order.
ids=$(curl -sf -m 10 "$api/v1/stores" | python3 -c '
import json, sys
print(" ".join(str(s["store"]["id"]) for s in sorted(json.load(sys.stdin)["stores"], key=lambda s: s["store"]["id"])))')
n=0
for id in $ids; do
  n=$((n + 1))
  curl -sf -m 10 -X POST -H 'Content-Type: application/json' -d "{\"zone\": \"z$n\"}" \
    "$api/v1/store/$id/label" >/dev/null
done
[ "$n" -ge 2 ] || { echo "place-leaders: $n store(s): nothing to place" >&2; exit 0; }

# 2. The keyspace's key range, as PD sees it (memcomparable 'r' (raw) or 'x'
#    (txn) + id).
ks_id=$(curl -sf -m 10 "$api/v2/keyspaces/$keyspace" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
read -r start end < <(python3 - "$ks_id" "$prefix" <<'PY'
import sys
def enc(b):
    # TiKV's memcomparable encode_bytes: 8-byte groups + marker 0xFF - pad.
    out, i = bytearray(), 0
    while True:
        g = b[i:i + 8]
        pad = 8 - len(g)
        out += g + b"\0" * pad + bytes([0xFF - pad])
        if pad:
            return out.hex()
        i += 8
k, pre = int(sys.argv[1]), sys.argv[2].encode()
print(enc(pre + k.to_bytes(3, "big")), enc(pre + (k + 1).to_bytes(3, "big")))
PY
)

# 3. A rule group (one per store mode, so that both ranges keep their own
#    leader and follower rules) that overrides the default rule on the range.
#    (A playground from before the per-mode groups has a stale `loams_pgwal`
#    group: stop the playground to clear it.)
curl -sf -m 10 -X POST -H 'Content-Type: application/json' \
  -d "{\"id\": \"loams_pgwal_$mode\", \"index\": 10, \"override\": true}" "$api/v1/config/rule_group" >/dev/null
rule() {
  curl -sf -m 10 -X POST -H 'Content-Type: application/json' -d "$1" "$api/v1/config/rule" >/dev/null
}
rule "{\"group_id\": \"loams_pgwal_$mode\", \"id\": \"leader\", \"start_key\": \"$start\", \"end_key\": \"$end\",
  \"role\": \"leader\", \"count\": 1,
  \"label_constraints\": [{\"key\": \"zone\", \"op\": \"in\", \"values\": [\"$zone\"]}]}"
rule "{\"group_id\": \"loams_pgwal_$mode\", \"id\": \"followers\", \"start_key\": \"$start\", \"end_key\": \"$end\",
  \"role\": \"follower\", \"count\": $((n - 1)),
  \"label_constraints\": [{\"key\": \"zone\", \"op\": \"notIn\", \"values\": [\"$zone\"]}]}"

# 4. Wait until PD reports every region of the range led from the zone.
leader_store=$(curl -sf -m 10 "$api/v1/stores" | python3 -c "
import json, sys
for s in json.load(sys.stdin)['stores']:
    if any(l['key'] == 'zone' and l['value'] == '$zone' for l in s['store'].get('labels', [])):
        print(s['store']['id'])")
off=unknown
for _ in $(seq 1 "$timeout"); do
  off=$(curl -sf -m 10 "$api/v1/regions" | python3 -c "
import json, sys
start, end, want = bytes.fromhex('$start'), bytes.fromhex('$end'), $leader_store
bad = 0
for r in json.load(sys.stdin).get('regions', []):
    s = bytes.fromhex(r.get('start_key', '')); e = bytes.fromhex(r.get('end_key', ''))
    if (not e or e > start) and s < end and r.get('leader', {}).get('store_id') != want:
        bad += 1
print(bad)")
  [ "$off" = 0 ] && { echo "place-leaders: loams_pgwal leaders on store $leader_store ($zone)" >&2; exit 0; }
  sleep 1
done
echo "place-leaders: $off region(s) of loams_pgwal still led outside $zone after ${timeout}s" >&2
exit 1
