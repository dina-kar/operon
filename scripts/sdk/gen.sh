#!/usr/bin/env bash
# Generate one language's SDK (design §44 §10.1, SDK1 Task 1's
# `scripts/sdk/gen.sh <lang>`).
#
#   scripts/sdk/gen.sh typescript   # @loams/client's facade
#   scripts/sdk/gen.sh typescript-live  # @loams/live's stubs
#
# The facade generator is a Rust binary (SDK1 Task 3), so the script builds it
# first and points buf at wherever cargo put it: the repository's cargo config
# sends build output outside the tree, so the path cannot be committed in the
# template, and buf does not expand environment variables in a `local:` plugin
# path. So the script substitutes it into a copy of the template.
#
# CI runs this and fails on a diff (SDK1's global constraint: generated code is
# never hand-edited).
set -euo pipefail

lang="${1:-}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# Rust builds are capped at -j 4 on a 15 GB machine; the SDK pipeline's jobs are
# the light ones and must not oversubscribe it.
jobs="${LOAMS_BUILD_JOBS:-4}"

# buf is pinned as a devDependency of @loams/client (the package that owns
# generation) rather than taken from the machine: a generation difference must
# be a dependency bump in a PR, not whatever the runner had installed.
BUF="sdks/typescript/packages/client/node_modules/.bin/buf"
if [ ! -x "$BUF" ]; then
  echo "$BUF is missing: run 'pnpm install' in web/ first" >&2
  exit 1
fi

plugin_path() {
  cargo build -p loams-facade-gen -j "$jobs" --message-format=json 2>/dev/null \
    | grep -o '"executable":"[^"]*protoc-gen-loams-facade"' \
    | head -1 \
    | cut -d '"' -f 4
}

case "$lang" in
  typescript)
    plugin="$(plugin_path)"
    if [ -z "$plugin" ]; then
      echo "could not find protoc-gen-loams-facade after cargo build" >&2
      exit 1
    fi
    # buf only reads a template whose file name is buf.gen.yaml (or
    # buf.<name>.yaml), so the substituted copy goes in its own directory.
    # Every path inside the template is relative to $root, not to the
    # template, so a temporary directory changes nothing else.
    workdir="$(mktemp -d)"
    trap 'rm -rf "$workdir"' EXIT
    sed "s|\${LOAMS_FACADE_PLUGIN}|$plugin|" sdks/typescript/buf.gen.yaml > "$workdir/buf.gen.yaml"
    "$BUF" generate --template "$workdir/buf.gen.yaml"
    echo "generated sdks/typescript/packages/client/src/gen/facade.ts"
    ;;
  typescript-live)
    # protoc-gen-es is a Node plugin pinned by @loams/client; buf.gen.live.yaml
    # points at its binary.
    "$BUF" generate --template sdks/typescript/buf.gen.live.yaml
    echo "generated sdks/typescript/packages/live/src/gen"
    ;;
  *)
    echo "usage: scripts/sdk/gen.sh <typescript|typescript-live>" >&2
    exit 2
    ;;
esac
