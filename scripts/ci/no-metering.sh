#!/usr/bin/env bash
# Run from the repository to audit all tracked files, including binary fixtures.
set -euo pipefail
exec python3 "$(dirname "$0")/no-metering.py"
