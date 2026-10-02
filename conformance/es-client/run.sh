#!/usr/bin/env bash
# Runs the elasticsearch-py 8.19 client suite against `loams dev` (plan
# M1.5 Task 11), with the pinned requirements, through uv.
#
#   run.sh [pytest args...]
#
# LOAMS_BIN names the binary (default target/debug/loams); the suite
# starts it on ephemeral ports. LOAMS_ES_URL runs the suite against a
# running server instead. ES_ORACLE_URL, when set, also runs the oracle
# checks (test_oracle.py) against that Elasticsearch 8.19.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec uv run --no-project --python 3.12 --with-requirements "${here}/requirements.txt" \
    pytest -q -p no:cacheprovider "${here}" "$@"
