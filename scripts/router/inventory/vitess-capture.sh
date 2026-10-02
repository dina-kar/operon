#!/usr/bin/env bash
# The MySQL half's dynamic capture (RT0 plan Task 6, §31 §15 steps 2 and 5).
#
#   vitess-capture.sh ref|wesql [out-dir]
#
# Brings up compose.vitess.yml with the chosen backend (mysql:8.0.46 or WeSQL), runs the scenario list
# through unmanaged vttablets and vtgate, snapshots performance_schema.events_statements_summary_by_digest
# around every step, and writes under <out-dir> (default $HOME/.cache/loam/inventory/mysql/<date>/<backend>):
#   steps.tsv              step, suite, test, result (pass|fail|skip)       -> the suites table
#   snap/<step>.<node>.{before,after}   digest counts around each step
#   digests.<node>.jsonl   the final digest dump (DIGEST, DIGEST_TEXT, QUERY_SAMPLE_TEXT, COUNT_STAR, SCHEMA_NAME)
#   schema/<node>.sql      mysqldump --no-data of the vt_* and sidecar databases (to prepare replay targets)
#   observations.tsv       the C-1..C-7 observations of design §31 §9.2
#   <step>.log             each step's output
# Stops the stack when done (unless KEEP_UP=1). Never run next to compose.pg.yml: 15 GB of RAM.
set -uo pipefail
backend="${1:?usage: vitess-capture.sh ref|wesql [out-dir]}"
here="$(cd "$(dirname "$0")" && pwd)"
VITESS_SRC="${VITESS_SRC:-$HOME/.cache/loam/vitess-v24.0.4}"
OUT="${2:-$HOME/.cache/loam/inventory/mysql/$(date +%F)/$backend}"
mkdir -p "$OUT/snap" "$OUT/schema"
: > "$OUT/steps.tsv"; : > "$OUT/observations.tsv"
avail_gb=$(df --output=avail -BG "$HOME" | tail -1 | tr -dc 0-9)
[[ "$avail_gb" -ge 8 ]] || { echo "only ${avail_gb} GB free under $HOME; stop (plan: 8 GB)"; exit 2; }
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/user/$(id -u)/podman/podman.sock}"
dc() {
  if command -v docker >/dev/null 2>&1; then docker compose -f "$here/compose.vitess.yml" --profile "$backend" "$@"
  else docker-compose -f "$here/compose.vitess.yml" --profile "$backend" "$@"; fi
}
case "$backend" in
  ref)   nodes=(my-a my-b my-c) ;;
  wesql) nodes=(wesql-a wesql-b wesql-c); export SOCK_PREFIX=wsock ;;   # the tablets mount the WeSQL socket volumes
  *) echo "backend must be ref or wesql"; exit 2 ;;
esac
MY="$(command -v mariadb || command -v mysql)"   # the host's client; recent mysql wrappers print a deprecation line
case "$backend" in ref) declare -A port=([a]=13306 [b]=13307 [c]=13308) ;; wesql) declare -A port=([a]=13316 [b]=13317 [c]=13318) ;; esac
log() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$OUT/capture.log"; }
sql() { local n="$1"; shift; "$MY" -h127.0.0.1 -P"${port[$n]}" -uroot -ploam-dev -N -B -r -e "$*" 2>&1; }
vt() { dc exec -T vtctld vtctldclient --server localhost:15999 "$@"; }
vsql() { "$MY" -h127.0.0.1 -P15306 -uroot -N -B -r "$@"; }
snap() { # snap <step> before|after
  for n in a b c; do
    sql "$n" "select digest, count_star from performance_schema.events_statements_summary_by_digest where digest is not null" > "$OUT/snap/$1.$n.$2" 2>/dev/null || true
  done
}
STEP=0
step() { # step <suite> <test> <command...>
  STEP=$((STEP + 1)); local id; id=$(printf s%02d "$STEP")
  log "== $id $1 / $2"
  snap "$id" before
  if "${@:3}" > "$OUT/$id.log" 2>&1; then r=pass; else r=fail; fi
  snap "$id" after
  printf '%s\t%s\t%s\t%s\n' "$id" "$1" "$2" "$r" >> "$OUT/steps.tsv"
  log "   -> $r"
}
skipstep() { # skipstep <suite> <test> <reason>
  STEP=$((STEP + 1)); local id; id=$(printf s%02d "$STEP")
  printf '%s\t%s\t%s\tskip\n' "$id" "$1" "$2 ($3)" >> "$OUT/steps.tsv"; log "== $id $1 / $2 -> skip ($3)"
}
obs() { printf '%s\t%s\n' "$1" "$2" >> "$OUT/observations.tsv"; }   # obs C-n <text>
retry() { local n="$1"; shift; for _ in $(seq "$n"); do "$@" && return 0; sleep 3; done; return 1; }
wait_workflow() { for _ in $(seq 90); do vt "$@" status 2>&1 | grep -qiE 'running|"state": "Running"|Copying completed|streams.*Running' && return 0; sleep 2; done; return 1; }
export PORT_A="${port[a]}"
export backend here DOCKER_HOST MY
export -f dc vt vsql retry wait_workflow sql

teardown() { [[ "${KEEP_UP:-0}" = 1 ]] || dc down -v >/dev/null 2>&1; }
trap teardown EXIT

# --- bring-up ------------------------------------------------------------------------------------------
log "backend=$backend nodes=${nodes[*]} out=$OUT"
dc down -v >/dev/null 2>&1
dc pull -q 2>&1 | tail -2
dc up -d etcd "${nodes[@]}" || { log "compose up failed"; exit 1; }
for n in a b c; do retry 120 sql "$n" "select 1" >/dev/null || { log "backend $n never became ready"; exit 1; }; done
for n in a b c; do
  retry 20 sql "$n" "create database if not exists vt_customer" >/dev/null || { log "backend $n refuses writes"; exit 1; }
done
retry 20 sql a "create database if not exists vt_commerce" >/dev/null
{ for n in a b c; do echo "--- $n"; sql "$n" "select version(), @@gtid_mode, @@enforce_gtid_consistency, @@binlog_format, @@binlog_row_image, @@default_storage_engine, @@global.transaction_isolation"; done; } > "$OUT/backend-settings.txt"
for n in a b c; do sql "$n" "truncate table performance_schema.events_statements_summary_by_digest" >/dev/null; done

step bringup "start vtctld, register the cell, create the keyspaces" bash -c '
  set -e
  dc up -d vtctld
  retry 60 vt GetKeyspaces >/dev/null
  vt AddCellInfo --root /vitess/zone1 --server-address etcd:2379 zone1
  vt CreateKeyspace --durability-policy=semi_sync commerce
  vt CreateKeyspace --sidecar-db-name=_vt_customer --durability-policy=semi_sync customer'
step bringup "start two unmanaged vttablets (commerce/0, customer/0)" bash -c '
  set -e
  dc up -d vttablet-commerce vttablet-customer
  retry 90 bash -c "[ \$(vt GetTablets 2>/dev/null | grep -c zone1-) -ge 2 ]"'
step bringup "TabletExternallyReparented for the shard primaries (durability policy semi_sync)" bash -c '
  set -e
  for uid in 0000000100 0000000200; do retry 3 vt TabletExternallyReparented zone1-$uid; done
  vt GetTablets'
if [[ "$(tail -1 "$OUT/steps.tsv" | cut -f4)" = fail ]]; then
  # WeSQL has no semi-sync plugins (it replicates through its consensus log): vttablet refuses to make a
  # primary under a semi_sync policy. Without semi-sync, Vitess's two-phase commit is not allowed either (C-2).
  obs C-2 "TabletExternallyReparented under durability policy semi_sync failed: $(grep -o 'VT[0-9]*: [^"\\]*' "$OUT/$(printf s%02d "$STEP").log" | head -1)"
  step bringup "SetKeyspaceDurabilityPolicy none, then TabletExternallyReparented" bash -c '
    set -e
    vt SetKeyspaceDurabilityPolicy --durability-policy=none commerce
    vt SetKeyspaceDurabilityPolicy --durability-policy=none customer
    for uid in 0000000100 0000000200; do retry 20 vt TabletExternallyReparented zone1-$uid; done
    vt GetTablets'
fi
step bringup "start vtgate and wait for the commerce primary" bash -c '
  set -e
  dc up -d vtgate
  retry 90 vsql -e "show vitess_tablets" | tee /dev/stderr | grep -q commerce'
vsql -e "show vitess_tablets" > "$OUT/vitess_tablets.txt" 2>&1 || true
engines() { for n in a b c; do echo "--- $n"; sql "$n" "select table_schema, table_name, engine from information_schema.tables where table_schema like '\\_vt%' order by 1,2"; done; }
engines > "$OUT/c1-engines-after-first-start.txt"

# --- keyspace and VSchema --------------------------------------------------------------------------------
sqlfile="$OUT/commerce-schema.sql"
cat "$VITESS_SRC"/go/vt/vtgate/planbuilder/testdata/schemas/main.sql "$VITESS_SRC"/go/vt/vtgate/planbuilder/testdata/schemas/user.sql > "$sqlfile"
cat >> "$sqlfile" <<'SQL'
create table if not exists product(sku varchar(128), description varchar(128), price bigint, primary key(sku)) ENGINE=InnoDB;
create table if not exists shopper(shopper_id bigint not null auto_increment, email varchar(128), primary key(shopper_id)) ENGINE=InnoDB;
create table if not exists corder(order_id bigint not null auto_increment, shopper_id bigint, sku varchar(128), price bigint, primary key(order_id)) ENGINE=InnoDB;
SQL
step keyspace-vschema "ApplySchema commerce (planbuilder test schema plus product, shopper, corder)" bash -c '
  dc cp "'"$sqlfile"'" vtctld:/tmp/commerce-schema.sql && vt ApplySchema --sql-file /tmp/commerce-schema.sql commerce'
step keyspace-vschema "ApplyVSchema commerce" bash -c '
  vt ApplyVSchema --vschema "{}" commerce && vt GetVSchema commerce'
step keyspace-vschema "ReloadSchemaKeyspace and GetSchema" bash -c '
  vt ReloadSchemaKeyspace commerce && vt GetSchema zone1-0000000100'

# --- vtgate DML and SELECT -------------------------------------------------------------------------------
python3 "$here/vt-corpus.py" "$VITESS_SRC" "${CORPUS_LIMIT:-1500}" > "$OUT/corpus.sql" 2> "$OUT/corpus.stats"
step vtgate "DML and SELECT corpus from the planbuilder test data" bash -c \
  '"$MY" -h127.0.0.1 -P15306 -uroot --force --database=commerce < "'"$OUT"'/corpus.sql" > "'"$OUT"'/corpus.out" 2> "'"$OUT"'/corpus.err"; true'
step vtgate "inserts, updates, deletes, joins and aggregates on product, shopper, corder" bash -c '
  "$MY" -h127.0.0.1 -P15306 -uroot --database=commerce <<SQL
insert into product(sku, description, price) values ("SKU-1001","Monitor",100),("SKU-1002","Keyboard",30),("SKU-1003","Mouse",15);
insert into shopper(email) values ("alice@domain.com"),("bob@domain.com"),("charlie@domain.com"),("dan@domain.com"),("eve@domain.com");
insert into corder(shopper_id, sku, price) values (1,"SKU-1001",100),(2,"SKU-1002",30),(3,"SKU-1002",30),(4,"SKU-1003",15),(5,"SKU-1003",15);
update product set price = price + 1 where sku = "SKU-1001";
select count(*) from shopper;
select sum(price) from corder;
select c.email, o.sku, p.description from shopper c join corder o on o.shopper_id = c.shopper_id join product p on p.sku = o.sku order by c.shopper_id;
select shopper_id, count(*) from corder group by shopper_id having count(*) >= 1 order by 1 limit 3;
begin; insert into corder(shopper_id, sku, price) values (1,"SKU-1002",30); rollback;
begin; delete from corder where shopper_id = 5; commit;
show tables; show create table product; select @@version;
SQL'

# --- MoveTables commerce -> customer ---------------------------------------------------------------------
step movetables "MoveTables create commerce2customer (shopper, corder)" bash -c '
  vt MoveTables --workflow commerce2customer --target-keyspace customer create --source-keyspace commerce --tables "shopper,corder" &&
  wait_workflow MoveTables --workflow commerce2customer --target-keyspace customer'
step movetables "VDiff create and show" bash -c '
  vt VDiff --workflow commerce2customer --target-keyspace customer create &&
  sleep 15 && vt VDiff --workflow commerce2customer --target-keyspace customer show last'
step movetables "MoveTables SwitchTraffic" bash -c '
  vt MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic'
step movetables "MoveTables ReverseTraffic" bash -c '
  vt MoveTables --workflow commerce2customer --target-keyspace customer reversetraffic'
step movetables "MoveTables SwitchTraffic again, then Complete" bash -c '
  vt MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic &&
  vt MoveTables --workflow commerce2customer --target-keyspace customer complete'

# --- Reshard customer 0 -> -80,80- ------------------------------------------------------------------------
step reshard "ApplySchema and sharded VSchema on customer" bash -c '
  vt ApplySchema --sql "alter table shopper change shopper_id shopper_id bigint not null; alter table corder change order_id order_id bigint not null" customer &&
  vt ApplyVSchema --vschema "{\"sharded\":true,\"vindexes\":{\"xxhash\":{\"type\":\"xxhash\"}},\"tables\":{\"shopper\":{\"column_vindexes\":[{\"column\":\"shopper_id\",\"name\":\"xxhash\"}]},\"corder\":{\"column_vindexes\":[{\"column\":\"shopper_id\",\"name\":\"xxhash\"}]}}}" customer'
step reshard "start the unmanaged vttablets of customer/-80 and customer/80- and reparent them" bash -c '
  set -e
  dc up -d vttablet-customer-lo vttablet-customer-hi
  retry 90 bash -c "[ \$(vt GetTablets 2>/dev/null | grep -c zone1-) -ge 4 ]"
  for uid in 0000000300 0000000400; do retry 20 vt TabletExternallyReparented zone1-$uid; done
  vt GetTablets'
step reshard "Reshard create cust2cust 0 -> -80,80-" bash -c '
  vt Reshard --workflow cust2cust --target-keyspace customer create --source-shards 0 --target-shards "-80,80-" &&
  wait_workflow Reshard --workflow cust2cust --target-keyspace customer'
step reshard "VDiff on the reshard" bash -c '
  vt VDiff --workflow cust2cust --target-keyspace customer create && sleep 15 && vt VDiff --workflow cust2cust --target-keyspace customer show last'
step reshard "Reshard SwitchTraffic" bash -c '
  vt Reshard --workflow cust2cust --target-keyspace customer switchtraffic'
step reshard "Reshard Complete" bash -c '
  vt Reshard --workflow cust2cust --target-keyspace customer complete'

# --- Online DDL, reparent --------------------------------------------------------------------------------
step onlineddl "ALTER with ddl_strategy=vitess" bash -c '
  vt ApplySchema --ddl-strategy vitess --sql "alter table product add column loams_inv int" commerce | tee /tmp/uuid.$$ &&
  sleep 30 && vt OnlineDDL commerce show all'
step reparent "TabletExternallyReparented for commerce/0 again" bash -c '
  vt TabletExternallyReparented zone1-0000000100'
skipstep reparent "PlannedReparentShard" "unmanaged tablets refuse InitShardPrimary, PlannedReparentShard, EmergencyReparentShard and ReparentTablet"

# --- C-1: sidecar engine across a restart ----------------------------------------------------------------
engines > "$OUT/c1-engines-before-restart.txt"
step sidecar "restart vttablet commerce/0 and watch for ALTER TABLE ... ENGINE" bash -c '
  dc restart vttablet-commerce && sleep 25'
engines > "$OUT/c1-engines-after-restart.txt"
last="$(printf s%02d "$STEP")"
# Digests that grew during the restart and look like engine changes or sidecar DDL.
for n in a b c; do
  sql "$n" "select digest_text from performance_schema.events_statements_summary_by_digest where digest_text like 'ALTER TABLE%' and (digest_text like '%_vt%') order by last_seen desc limit 40" > "$OUT/c1-alter-digests.$n.txt" 2>/dev/null
  sql "$n" "show variables like 'serverless_honor_innodb_engine'" > "$OUT/c1-honor-var.$n.txt" 2>&1
done
obs C-1 "engines of the _vt* tables after first start: see c1-engines-after-first-start.txt; after a vttablet restart: c1-engines-after-restart.txt; ALTER digests touching _vt*: c1-alter-digests.<node>.txt; serverless_honor_innodb_engine: c1-honor-var.<node>.txt"

# --- C-4, C-5: isolation and temporary tables through vtgate ---------------------------------------------
step isolation "SET SESSION TRANSACTION ISOLATION LEVEL via vtgate (RC, RR, SERIALIZABLE) then a read" bash -c '
  for lvl in "READ COMMITTED" "REPEATABLE READ" "SERIALIZABLE"; do
    echo "== $lvl"; "$MY" -h127.0.0.1 -P15306 -uroot --database=commerce -e "set session transaction isolation level $lvl; begin; select count(*) from product; select @@transaction_isolation; commit;" 2>&1
  done'
step temporary-tables "CREATE TEMPORARY TABLE through vtgate and on the backend" bash -c '
  "$MY" -h127.0.0.1 -P15306 -uroot --database=commerce -e "create temporary table loams_tmp(i int); insert into loams_tmp values (1); select * from loams_tmp;" 2>&1
  "$MY" -h127.0.0.1 -P"$PORT_A" -uroot -ploam-dev vt_commerce -e "create temporary table loams_tmp2(i int); insert into loams_tmp2 values (1); select * from loams_tmp2;" 2>&1'

# --- two-phase commit, last: when it fails it can leave a prepared transaction that blocks DDL on the table ----
step 2pc "transaction_mode=twopc across commerce and customer" bash -c '
  "$MY" -h127.0.0.1 -P15306 -uroot -e "set transaction_mode=twopc; begin; insert into commerce.product(sku, description, price) values (\"SKU-2PC\",\"twopc\",1); insert into customer.shopper(shopper_id, email) values (900,\"twopc@domain.com\"); commit; select count(*) from commerce.product; select count(*) from customer.shopper;" 2>&1'
for n in a b c; do obs C-2 "semi-sync on backend $n: $(sql "$n" "show status like 'Rpl_semi_sync_source_status'" | tr '\t\n' '= ')"; done
obs C-2 "two-phase commit needs a Unix socket to MySQL: vttablet refuses Prepare over TCP (dt_executor.go: 'We can only prepare on a Unix socket connection'). A first run on 2026-10-02 with tablets on --db-host failed this step with 'VT10002: atomic distributed transaction not allowed: cannot prepare the transaction on a network connection'; this script starts the tablets on --db-socket (a volume shared with the MySQL container)"
obs C-2 "2pc step result: $(tail -c 300 "$OUT/$(printf s%02d "$STEP").log" | tr '\n\t' '  ')"

# --- final dumps -----------------------------------------------------------------------------------------
for n in a b c; do
  sql "$n" "select json_object('digest', digest, 'text', digest_text, 'sample', query_sample_text, 'count', count_star, 'schema', schema_name) from performance_schema.events_statements_summary_by_digest where digest is not null" > "$OUT/digests.$n.jsonl"
  dc exec -T "$([[ $backend = ref ]] && echo my-$n || echo wesql-$n)" sh -c 'mysqldump -uroot -ploam-dev --no-data --skip-comments --skip-lock-tables --set-gtid-purged=OFF --databases $(mysql -uroot -ploam-dev -N -B -e "select schema_name from information_schema.schemata where schema_name like \"vt\\_%\" or schema_name like \"\\_vt%\"")' > "$OUT/schema/$n.sql" 2>>"$OUT/capture.log" || true
done
vt GetTablets > "$OUT/tablets.txt" 2>&1 || true
for c in vttablet-commerce vtgate; do dc logs --no-color "$c" 2>&1 | tail -200 > "$OUT/log.$c.txt"; done
log "done: $(awk -F'\t' '{c[$4]++} END {for (k in c) printf "%s=%d ", k, c[k]}' "$OUT/steps.tsv")"
