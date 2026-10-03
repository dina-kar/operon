#!/usr/bin/env bash
# See check.py for usage. Needs Java 21+ and Python 3.11+.
exec python3 "$(dirname "$0")/check.py" "$@"
