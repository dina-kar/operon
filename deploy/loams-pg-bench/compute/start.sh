#!/usr/bin/env bash
# The benchmark compute's entrypoint: deploy/neon/compute/start.sh on host
# networking, with the safekeeper list, shared_buffers and fsync filled in.
set -euo pipefail
: "${TENANT_ID:?set TENANT_ID}" "${TIMELINE_ID:?set TIMELINE_ID}" "${SAFEKEEPERS:?set SAFEKEEPERS}"
until (exec 3<>/dev/tcp/127.0.0.1/6400) 2>/dev/null; do echo "waiting for pageserver"; sleep 1; done
sed -e "s|TENANT_ID|${TENANT_ID}|" -e "s|TIMELINE_ID|${TIMELINE_ID}|" \
    -e "s|SAFEKEEPERS|${SAFEKEEPERS}|" -e "s|SHARED_BUFFERS|${SHARED_BUFFERS:-2GB}|" \
    -e "s|COMPUTE_FSYNC|${COMPUTE_FSYNC:-off}|" \
    /var/db/postgres/configs/config.json > /tmp/config.json
exec /usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute \
     -C "postgresql://cloud_admin@127.0.0.1:55433/postgres" \
     -b /usr/local/bin/postgres \
     --compute-id "compute-bench" \
     --config /tmp/config.json
