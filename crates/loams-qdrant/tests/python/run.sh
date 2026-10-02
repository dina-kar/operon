#!/usr/bin/env bash
# Runs the Python qdrant-client smoke suite against a running `loams dev`
# (plan M1.4 Task 10).
#
#   run.sh <client-version> <legacy|modern>
#
# The REST URL is http://127.0.0.1:6333 unless LOAMS_QDRANT_URL says
# otherwise; gRPC is on the next port. Exits non-zero on any failure.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <client-version> <legacy|modern>" >&2
    exit 2
fi
version="$1"
mode="$2"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec uv run --no-project --python 3.12 --with "qdrant-client==${version}" \
    python "${here}/smoke.py" --mode "${mode}" --url "${LOAMS_QDRANT_URL:-http://127.0.0.1:6333}"
