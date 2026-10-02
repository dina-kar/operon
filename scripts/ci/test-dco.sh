#!/usr/bin/env bash
set -euo pipefail
checker="$(cd "$(dirname "$0")" && pwd)/check-dco.sh"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/loams-agents"
mkdir -p "$cache"
fixture=$(mktemp -d "$cache/dco-fixture.XXXXXX")
trap 'python3 -c "import shutil, sys; shutil.rmtree(sys.argv[1])" "$fixture"' EXIT
git init -q "$fixture"
cd "$fixture"
git config user.name 'DCO fixture'
git config user.email fixture@example.com
git -c commit.gpgsign=false commit -q --allow-empty -m base
base=$(git rev-parse HEAD)
check() {
  local name=$1 email=$2 signoff=$3 expected=$4
  git -c commit.gpgsign=false commit -q --allow-empty --author "$name <$email>" -m fixture ${signoff:+-m "$signoff"}
  local head result=0
  head=$(git rev-parse HEAD)
  BASE=$base HEAD=$head "$checker" > "$fixture/result" 2>&1 || result=$?
  if [[ $expected == pass && $result != 0 || $expected == fail && $result == 0 ]]; then
    cat "$fixture/result"
    echo "unexpected DCO result for $name <$email>: $expected" >&2
    exit 1
  fi
  base=$head
}
check 'Human' human@example.com 'Signed-off-by: Human <human@example.com>' pass
check 'Human' human@example.com '' fail
check 'dependabot[bot]' '49699333+dependabot[bot]@users.noreply.github.com' 'Signed-off-by: dependabot[bot] <support@github.com>' pass
check 'dependabot[bot]' '49699333+dependabot[bot]@users.noreply.github.com' '' fail
check 'Human' human@example.com 'Signed-off-by: dependabot[bot] <support@github.com>' fail
check 'dependabot[bot]' impostor@example.com 'Signed-off-by: dependabot[bot] <support@github.com>' fail
echo 'DCO: signed humans/bot accepted; unsigned bot and mismatched identities rejected'
