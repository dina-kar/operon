#!/usr/bin/env bash
# Apply .github/labels.yml to a GitHub repository with the gh CLI.
#
#   scripts/sync-labels.sh [owner/repo]
#
# Creates missing labels and updates the colour and description of existing
# ones (`gh label create --force`). It never deletes a label. With no
# argument it targets the repository of the current directory.
#
# Needs gh (authenticated, with write access to the repository) and python3
# with PyYAML, or yq.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
file="$root/.github/labels.yml"
repo_args=()
if [ $# -ge 1 ]; then
  repo_args=(--repo "$1")
fi

# Emit one tab-separated line per label: name, colour, description.
labels() {
  if command -v yq >/dev/null 2>&1; then
    yq -r '.[] | [.name, .color, .description] | @tsv' "$file"
  else
    python3 - "$file" <<'PY'
import sys, yaml
for label in yaml.safe_load(open(sys.argv[1])):
    print("\t".join([label["name"], str(label["color"]), label.get("description", "")]))
PY
  fi
}

if ! records=$(labels); then
  echo "could not read $file (needs yq, or python3 with PyYAML)" >&2
  exit 1
fi

count=0
while IFS=$'\t' read -r name color description; do
  [ -n "$name" ] || continue
  if [ "${#description}" -gt 100 ]; then echo "label $name: description over 100 characters" >&2; exit 1; fi
  gh label create "$name" --color "$color" --description "$description" --force ${repo_args[@]+"${repo_args[@]}"}
  count=$((count + 1))
done <<< "$records"
echo "Synced $count labels from .github/labels.yml."
