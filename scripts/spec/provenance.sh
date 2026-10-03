#!/usr/bin/env bash
# Fails if PgDog (AGPL-3.0) code or text appears in Loams' router specs, inventory or kernel crate
# (RT0 plan, Global Constraints; D236, D318). PgDog is read as a reference and run unmodified; never copied.
# The identifiers are distinctive names from PgDog's 2PC and resharding modules (pinned v0.1.60),
# chosen so that an honest independent implementation does not produce them.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
paths=()
for p in spec conformance crates/loams-sqlrouter crates/operon-sqlrouter; do
  [[ -e "$root/$p" ]] && paths+=("$root/$p")
done
[[ ${#paths[@]} -eq 0 ]] && { echo "provenance: nothing to check"; exit 0; }
patterns=(
  'GNU AFFERO' 'pgdog::' 'use pgdog'
  'TwoPcGuard' 'TwoPcPhase' 'TwoPcServerTransaction' 'TwoPcTransaction' 'TwoPcStats'
  'ReshardingStateInner' 'ReshardTask' 'resolve_resharding_replicas' 'take_owned_slots'
  'drop_slots_if_owned' 'create_reverse_slots' 'inner_with_slots' 'phase_control_statements'
  'set_transaction_phase' 'set_transaction_identity' 'cleanup_phase' 'detach_slots'
  'InnerNotify' 'recovered_total'
)
fail=0
for pat in "${patterns[@]}"; do
  # This script lists the patterns, so exclude it.
  if hits=$(grep -rnF --exclude=provenance.sh -- "$pat" "${paths[@]}" 2>/dev/null); then
    echo "provenance: PgDog identifier '$pat' found:"; echo "$hits"; fail=1
  fi
done
[[ $fail -eq 0 ]] && echo "provenance: clean (${#patterns[@]} patterns over ${paths[*]#$root/})"
exit $fail
