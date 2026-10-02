#!/usr/bin/env bash
# Static half of the Postgres inventory (RT0 plan Task 5, §31 §15 step 1).
#
# Usage: pg-static.sh "$PGDOG_SRC"   (a PgDog v0.1.60 checkout, e.g. $HOME/.cache/loam/pgdog-v0.1.60)
#
# Prints `source_path:line<TAB>statement-kind` for every line in PgDog's backend and 2PC code
# that holds a SQL string literal or a replication command. PgDog is AGPL-3.0 (D236, D318):
# this script NEVER prints the line's text, only the path, the line number and a kind drawn from
# the fixed keyword lists below. A human classifies each kind into an inventory row
# (pg-static-kinds.tsv maps a kind to a canonical example written for Loams, not taken from PgDog).
set -euo pipefail
src="${1:-${PGDOG_SRC:-}}"
[[ -n "$src" && -d "$src/pgdog/src" ]] || { echo "usage: $0 <pgdog checkout>" >&2; exit 2; }
cd "$src"
paths=(pgdog/src/backend/replication pgdog/src/backend/schema pgdog/src/backend/pool
       pgdog/src/frontend/client/query_engine/two_pc pgdog/src/healthcheck.rs)
find "${paths[@]}" -type f \( -name '*.rs' -o -name '*.sql' \) ! -path '*/test/*' ! -name 'tests.rs' ! -name 'test_*' | LC_ALL=C sort |
perl -e '
  my @verbs = qw(SELECT INSERT UPDATE DELETE CREATE ALTER DROP SET SHOW BEGIN COMMIT ROLLBACK COPY LISTEN NOTIFY
                 PREPARE DEALLOCATE EXECUTE DISCARD RESET WITH EXPLAIN LOCK TRUNCATE GRANT);
  my $verb = join "|", @verbs;
  my @keywords = ("START_REPLICATION", "CREATE_REPLICATION_SLOT", "DROP_REPLICATION_SLOT", "IDENTIFY_SYSTEM",
                  "PREPARE TRANSACTION", "COMMIT PREPARED", "ROLLBACK PREPARED", "pg_prepared_xacts", "pg_is_in_recovery",
                  "pg_current_wal_lsn", "pg_last_wal_replay_lsn", "pg_replication_slots", "pg_stat_replication",
                  "FORMAT BINARY", "pg_dump", "pg_restore", "pg_stat_activity", "pg_publication", "pg_subscription",
                  "pg_advisory", "information_schema", "pg_catalog", "pg_class", "pg_attribute", "pg_index",
                  "pg_constraint", "pg_sequence", "pg_namespace", "pg_terminate_backend", "pg_cancel_backend",
                  "pg_export_snapshot", "pg_current_snapshot", "pg_create_logical_replication_slot", "pg_logical_slot");
  while (my $f = <STDIN>) {
    chomp $f;
    open(my $h, "<", $f) or next;
    my $n = 0;
    my $sql = $f =~ /\.sql$/;
    while (my $l = <$h>) {
      $n++;
      my %kinds;
      if ($sql) { $kinds{uc $1} = 1 if $l =~ /^\s*($verb)\b/i; }
      else      { $kinds{uc $1} = 1 while $l =~ /"\s*($verb)\b/ig; }
      for my $k (@keywords) { $kinds{$k} = 1 if index(lc $l, lc $k) >= 0 && ($sql || $l =~ /"/ || $k =~ /^[A-Z_ ]+$/); }
      print "$f:$n\t$_\n" for sort keys %kinds;
    }
  }'
