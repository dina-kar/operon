#!/usr/bin/env bash
set -euo pipefail
status=0
# --no-merges skips merge commits.
for sha in $(git rev-list --no-merges "$BASE..$HEAD"); do
  name=$(git log -1 --format='%an' "$sha")
  email=$(git log -1 --format='%ae' "$sha")
  # GitHub's Dependabot commits use its support address in DCO.
  if [[ "$name" == 'dependabot[bot]' && "$email" == '49699333+dependabot[bot]@users.noreply.github.com' ]] &&
      git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$sha" |
      grep -Fxq 'dependabot[bot] <support@github.com>'; then
    continue
  fi
  if ! git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$sha" \
      | grep -qiF "<$email>"; then
    echo "::error::commit $sha is missing 'Signed-off-by: ... <$email>' (use git commit -s)"
    status=1
  fi
done
exit $status
