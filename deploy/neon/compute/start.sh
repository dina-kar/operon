#!/usr/bin/env bash
# Minimal compute entrypoint, adapted from neondatabase/neon
# docker-compose/compute_wrapper/shell/compute.sh (Apache-2.0). The tenant and timeline are created by the caller
# (Loams, or the spike's curl calls) through the pageserver API, so this script
# needs no curl/jq: it fills the compute spec and starts compute_ctl.
set -euo pipefail
: "${TENANT_ID:?set TENANT_ID}" "${TIMELINE_ID:?set TIMELINE_ID}"
until (exec 3<>/dev/tcp/pageserver/6400) 2>/dev/null; do echo "waiting for pageserver"; sleep 1; done
sed -e "s|TENANT_ID|${TENANT_ID}|" -e "s|TIMELINE_ID|${TIMELINE_ID}|" \
    /var/db/postgres/configs/config.json > /tmp/config.json
exec /usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute \
     -C "postgresql://cloud_admin@localhost:55433/postgres" \
     -b /usr/local/bin/postgres \
     --compute-id "compute-${HOSTNAME}" \
     --config /tmp/config.json
