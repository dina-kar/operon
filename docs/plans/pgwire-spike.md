# Spike: Postgres wire read access through `datafusion-postgres`

> **Date:** 2026-09-27. **Owner approval:** yes (feasibility spike). **Decision:** D-PG-1, a placeholder numbered at merge. **Plan:** [PG1](2026-09-28-pg1-postgres-wire.md).
> **Code:** not committed. The spike code (about 300 lines behind the `pgwire` feature, plus 4 tests) is kept as a patch outside the repository (§7). This PR holds only the docs.

## 1. Question and answer

**Can Operon serve its read-only SQL surface (collections as tables, the search table functions) over the Postgres wire protocol, using `datafusion-postgres` 0.18 on Operon's DataFusion 54, well enough for `psql`, `psycopg` 3 and the `pg` npm package?**

**Yes.** With one `SessionContext` per namespace (the one Flight SQL and REST already plan in), `datafusion-pg-catalog`'s `pg_catalog`, and a read-only hook in front of the library's handler, these all work:

- `psql`'s `\dt`, `\d kb`, `\l`, `\dn`, `\dv`, `\di` and `\du`;
- filters, aggregates, `ORDER BY … NULLS LAST LIMIT/OFFSET`, `ANY(ARRAY[…])`, `ILIKE` and `EXPLAIN [ANALYZE]`;
- `vector_search`, `text_search` and `rrf(vector_search(…), text_search(…))`;
- the extended protocol with parameters (`psql \bind`, psycopg, node-postgres), psycopg's binary cursors and pipeline mode;
- SQLAlchemy's `has_table` query.

Every write is refused with SQLSTATE **25006** (`read_only_sql_transaction`), which psycopg raises as `ReadOnlySqlTransaction` and node-postgres reports as `code: '25006'`.

The blockers are all fixable:

1. `arrow-pg` 0.15 **panics** when it encodes a `FixedSizeList` column. That is every vector column. The panic kills the client's connection, though not the server.
2. The crates **turn on DataFusion's default features**. That compiles `parquet` and the compression codecs into the binary and makes the first build with the feature rebuild about 260 crates.
3. The session is **one namespace per listener**, with no per-connection consistency or hot scope.
4. A few catalog functions are missing (`pg_function_is_visible`, `pg_type_is_visible`, `set_config`, `array` in `\d+`), and `information_schema.columns` reports Arrow type names.

**Recommendation:** adopt it as the PG1 milestone after M1 (§6). The read-only surface comes first, then writes behind `--pg-allow-writes`.

## 2. What the spike built

The patch touches 8 files: 1,102 lines added, of which about 600 are `Cargo.lock`.

| Piece | Where | What it does |
|---|---|---|
| Feature | `crates/operon/Cargo.toml` | `pgwire = ["dep:datafusion", "dep:datafusion-postgres"]`, **off by default**. `datafusion-postgres = "0.18"` pulls `arrow-pg` 0.15.0, `datafusion-pg-catalog` 0.18.3 and `pgwire` 0.40.7 |
| Flags | `crates/operon/src/main.rs` | `--pg-listen <addr>` (off unless given) and `--pg-namespace <ns>` (default `default`), on `dev`, `standalone` and `cluster`. Without the feature, the flag logs `this build has no Postgres wire listener (the pgwire feature is off)` |
| Listener | `crates/operon/src/pg.rs` | `bind` **refuses any non-loopback address**: `postgres listen on 0.0.0.0:5432: only loopback addresses are served until the unified auth plan (D111)`. This is the stricter rule that D121 and D138 use, not D111's warning, because the listener has no auth and reads every collection. The listener has its own accept loop with a `CancellationToken` and a `TaskTracker`, since the library's `serve` binds by itself and has no shutdown. `pgwire::tokio::process_socket` runs per connection with the no-auth startup handler |
| Session | `pg::session` | `CollectionService::sql_context(ns)`, the same context as Flight SQL (strong consistency, the hot scope captured at start), plus `setup_pg_catalog(ctx, ns, EmptyContextProvider)`. Hooks, in order: the library's `CursorStatementHook`, `SetShowHook` and `TransactionStatementHook`, then **`ReadOnlyHook`** |
| Read-only guard | `pg::ReadOnlyHook` | `Statement::Query` and `Statement::Explain` are planned with **`operon_query::sql::plan_read_only`**. That call refreshes the namespace catalog, so a new collection is visible, and it runs `read_only_options().verify_plan`, so `SELECT … INTO` is refused. Every other statement the earlier hooks did not take is refused with 25006. In the extended protocol, the plan is checked at Parse and the library executes it |
| Catalog | `operon-query/src/sql/catalog.rs` | `NamespaceCatalog::register_schema` holds extra schemas beside `collections`, which cannot be replaced. `setup_pg_catalog` needs it to register `pg_catalog` |
| Tests | `crates/operon/tests/it/pgwire.rs` | Run through the library's `MockClient`: `selects_filters_and_aggregates_read_collections`, `pg_catalog_lists_the_collections`, `writes_are_refused_with_25006` (INSERT, CREATE, DROP, DELETE, UPDATE, COPY, SELECT INTO) and `the_listener_refuses_non_loopback_addresses` |

The spike does **not** use `datafusion-postgres`'s `serve()`, `HandlerFactory` (which is private), `AuthManager` or `PermissionsHook`.

## 3. Commands

```bash
# Worktree
git -C /home/dinakaran/Documents/Operon-wt/m1.2a fetch origin
git -C /home/dinakaran/Documents/Operon-wt/m1.2a worktree add -b spike-pgwire /home/dinakaran/Documents/Operon-wt/pgwire origin/main

# Builds (shared target, measured one after the other)
export CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=$HOME/.cache/cargo-target/operon
cargo build -p operon --bin operon                              # baseline
cargo build -p operon --bin operon --features pgwire --timings  # with the feature
cargo test  -p operon --features pgwire --test it pgwire
cargo deny check                                                # deny.toml has [graph] all-features = true
cargo tree -p operon --features pgwire -e normal -i <crate>

# Server
operon dev --data-dir ./data --listen 127.0.0.1:18480 --flight-sql-listen 127.0.0.1:18482 \
  --no-qdrant --no-es --pg-listen 127.0.0.1:15432
# → operon postgres listening on postgres://127.0.0.1:15432/default
# The collection `kb`: the M1.6 fixture schema (body text, tenant keyword, n i64,
# embedding dim 3 cosine), 2 partitions, 4 documents (ids 1, 2, 3, "k-str").

# Clients
psql -h 127.0.0.1 -p 15432 -U postgres -d default -X -c '\dt'   # psql 18.6
uv venv ~/.cache/operon-pgspike/venv && uv pip install 'psycopg[binary]>=3.2'  # psycopg 3.3.6
npm install pg@8                                                   # pg 8.23.0 on Node
```

## 4. Results

### 4.1 psql 18.6

| Command or query | Result |
|---|---|
| `\dt`, `\dt collections.*` | ✅ `collections \| kb \| table \| postgres` |
| `\d kb`, `\d collections.kb` | ✅ columns and types: `_id text not null`, `_source text not null`, `_seq_no numeric`, `_partition bigint`, `body text`, `tenant text`, `n bigint`, `embedding text` (see gap G5) |
| `\l` | ✅ lists `default`, and also a phantom `postgres` database |
| `\dn`, `\dv`, `\di`, `\du` | ✅ (`\du` is empty) |
| `\d+ kb` | ❌ `Invalid function 'array'` |
| `\df` | ❌ `Invalid function 'pg_function_is_visible'` |
| `\dT` | ❌ `Invalid function 'pg_type_is_visible'` |
| `SELECT _id, tenant, n FROM kb WHERE tenant = 'a' ORDER BY n` | ✅ 2 rows |
| `SELECT tenant, count(*), avg(n) FROM kb GROUP BY tenant` | ✅ 3 rows |
| `vector_search('kb', [1.0,0.0,0.0], 'embedding', 2)`, also with `ARRAY[…]` | ✅ `1 → 1.0`, `2 → 0.99388` |
| `text_search('kb', 'refund', 'body', 10)` | ✅ 2 rows |
| `rrf(vector_search(…), text_search(…))` | ✅ 3 fused rows |
| `hybrid_search('kb', …)` with 3 arguments | ❌ as designed: `hybrid_search takes 6 to 9 arguments` (Operon's own signature) |
| `EXPLAIN`, `EXPLAIN ANALYZE` | ✅ with `CollectionScanExec` |
| `BEGIN; SELECT count(*) FROM kb; COMMIT;` | ✅ |
| `SET statement_timeout = 1000; SHOW statement_timeout` | ✅ `1000ms` |
| `SELECT n, n::text FROM kb` | ❌ duplicate expression name (DataFusion). ✅ once aliased |
| `SELECT embedding FROM kb` (simple or extended) | ❌ **the connection is dropped**: `arrow-pg-0.15.0/src/encoder.rs:488` panics on `downcast_ref::<ListArray>().unwrap()` for a `FixedSizeList` (gap G1) |
| `SELECT _id, n FROM kb WHERE n > $1 \bind 1 \g` | ✅ 3 rows (extended protocol) |
| `SELECT version()` | `Apache DataFusion 54.1.0, x86_64 on linux`. The server reports `server_version` `16.6-pgwire-0.40.7` |

### 4.2 Writes (psql, psycopg, pg)

| Statement | Result |
|---|---|
| `INSERT …` (simple, and extended `\bind`) | `ERROR: 25006: cannot execute INSERT in a read-only transaction` |
| `CREATE TABLE`, `CREATE VIEW` | 25006 `cannot execute CREATE …` |
| `DROP TABLE`, `DELETE`, `UPDATE`, `TRUNCATE` | 25006 |
| `COPY kb TO STDOUT`, `COPY kb FROM STDIN` | 25006. Neither is served yet |
| `SELECT * INTO t2 FROM kb` | 25006 `only read-only queries are allowed: … DDL not supported: CreateMemoryTable` (from `plan_read_only`) |
| `BEGIN; INSERT …; COMMIT;` | 25006 on the INSERT |
| `count(*)` afterwards | 4. Nothing was written or created |
| psycopg 3.3.6 | `psycopg.errors.ReadOnlySqlTransaction` for INSERT, CREATE, DELETE and COPY |
| pg 8.23.0 | `code 25006` |

### 4.3 psycopg 3.3.6 (uv venv under `~/.cache`)

| Probe | Result |
|---|---|
| Connect (`autocommit=False`, so psycopg sends `BEGIN`) | ✅ `server_version` 160006 |
| `SELECT … WHERE tenant = %s` and `n > %s` (server-side binding) | ✅ |
| Aggregate | ✅ |
| `vector_search` | ✅ |
| `text_search('kb', %s, 'body', 10)` | ❌ `argument 2 of text_search must be a constant`. The search table functions plan before parameters are bound (gap G7) |
| `cursor(binary=True)` | ✅ `numeric` comes back as `Decimal` |
| `pg_class` joined with `pg_namespace` | ✅ |
| SQLAlchemy `has_table` (`relkind = ANY (ARRAY[%s…])`, `pg_table_is_visible`) | ✅ |
| `pg_attribute` with `format_type` and `'collections.kb'::regclass` | ✅ |
| `pg_type` OIDs | ✅ |
| Pipeline mode | ✅ |
| After an error in a transaction | ✅ `current transaction is aborted…` until `ROLLBACK`, as in Postgres (psycopg maps it to `NoActiveSqlTransaction`) |
| `SELECT embedding` | ❌ connection lost (G1) |

### 4.4 node-postgres (`pg` 8.23.0)

Simple queries, `$1` parameters, `count(*)::int` aggregates and `vector_search` all work ✅. INSERT fails with `25006` ✅. `bigint` comes back as a string, which is node-postgres's default.

### 4.5 Catalog queries from DBeaver, Grafana and Metabase (run through psql)

`datafusion-pg-catalog` covers most of these:

| Query | Result |
|---|---|
| DBeaver: namespaces with `pg_description` and `'pg_namespace'::regclass` | ✅ |
| DBeaver: `pg_database WHERE datname = 'default'` | ✅ |
| DBeaver: `pg_settings WHERE name = 'standard_conforming_strings'` | ✅ `on` |
| DBeaver: tables (`pg_class`, `reltoastrelid`, `pg_description`) | ✅ |
| DBeaver: columns (`pg_attribute`, `pg_attrdef`, `pg_get_expr`, `pg_depend`) | ✅ |
| DBeaver: types (`pg_type`, `format_type(nullif(…))`) | ✅ |
| DBeaver: indexes (`pg_index`) | ✅ empty |
| DBeaver: `pg_total_relation_size`, `pg_relation_size` | ✅ but always 0 |
| DBeaver: `pg_constraint`, `pg_get_constraintdef` | ✅ empty |
| DBeaver: `pg_get_userbyid` | ✅ |
| DBeaver: `pg_stat_activity` | ❌ table not found |
| Grafana: `quote_ident(table_name) FROM information_schema.tables` | ✅ |
| Metabase: `has_schema_privilege(nspname, 'USAGE')` | ✅ |
| `information_schema.columns.data_type` | ⚠️ Arrow names (`Utf8`, `UInt64`), not Postgres names: DataFusion's own `information_schema` wins over the pg one |
| `'kb'::regclass` without a schema | ⚠️ empty, because `search_path` is `public` and the tables are in `collections` (G4) |
| `set_config('search_path', …)` | ❌ missing |
| `to_char(now(), 'YYYY')` | ⚠️ wrong result (`YYYY`) |
| `json_extract_path_text`, `::json->>` | ❌ missing, or `Unsupported SQL type JSON` |

### 4.6 Tests

`cargo test -p operon --features pgwire --test it pgwire`: **3 of 4 passed** in 0.64 s. `selects_filters_and_aggregates_read_collections` failed because its `SELECT * FROM vector_search(…)` returns `embedding` and hit G1. The patch changes that query to `SELECT _id, _score`. The test was **not re-run**: free space on `/home` had fallen to 6.1 GB, below the 8 GB floor for builds on this machine. Clippy was not run with the feature for the same reason. The feature-on binary built with no warnings.

## 5. Dependencies, policy and cost

### 5.1 The dependency graph (Cargo.lock at `origin/main` 2d83ad5)

- **45 packages are added and none removed.** They include `datafusion-postgres` 0.18.0, `datafusion-pg-catalog` 0.18.3, `arrow-pg` 0.15.0, `pgwire` 0.40.7, `postgres-types` 0.2.14, `postgres-protocol` 0.6.12, `pg_interval`, `rust_decimal` features, `x509-certificate`/`bcder`, `lazy-regex`, `smol_str`, `derive-new` 0.7, `getset` and `md5`. DataFusion's default features add `parquet` 58.4, `datafusion-datasource-parquet`, `thrift`, `brotli`, `bzip2`/`libbz2-rs-sys`, `liblzma`, `zstd` 0.14, `snap`, and `recursive`/`stacker`/`psm`.
- **arrow 58.4 and DataFusion 54.1 unify.** No second arrow, DataFusion or sqlparser version appears.
- **Seven new duplicate versions**, all small: `zstd` 0.13 and 0.14 (0.14 through `async-compression`, from DataFusion's `compression` feature), `zstd-safe` 7 and 8, `hmac` 0.12 and 0.13 (`postgres-protocol`), `derive-new` 0.5 and 0.7 (`pgwire`), `fallible-iterator` 0.2 and 0.3 (`postgres-protocol`), `integer-encoding` 3 and 4 (`thrift`, from `parquet`), and `object` 0.37 and 0.39 (a build-time dependency of `psm`).
- **The main cost is DataFusion's default features.** `datafusion-postgres`, `arrow-pg`, `datafusion-pg-catalog` and `datafusion-pg-functions` all depend on `datafusion = "^54"` with default features. Operon's workspace deliberately builds DataFusion with Lance's feature set, without parquet or compression (root `Cargo.toml`). Cargo unifies features, so turning `pgwire` on recompiles DataFusion, Lance, the arrow crates, qdrant-edge and everything above them. The fix is upstream: `default-features = false` plus the `sql` feature in the three crates (PG1 Task 1).
- **pgwire's features.** `datafusion-postgres` asks for `server-api-ring` and `arrow-pg` for `server-api` and `pg-ext-types`. `ring` and both of rustls's providers were already in Operon's graph, so there is no new crypto library and no new provider ambiguity. `rsa`, `jsonwebtoken`, `aws-lc-rs` from pgwire and `reqwest` 0.13 are not enabled.
- `datafusion-pg-functions` 0.1.0 was **evaluated and not added**. It is 688 lines, only its `math` category has functions, and the other categories register nothing yet. It does not supply `set_config`, `to_char`, JSON or `pg_*_is_visible`. Revisit it when it grows.

### 5.2 `cargo deny check` (Operon's `deny.toml`, `all-features = true`, so `pgwire` is included)

`advisories ok, bans ok, licenses ok, sources ok`, exit 0. The new crates are Apache-2.0, MIT or Apache-2.0, and BSD. There is no AGPL, BSL, SSPL or ELv2 code, no new git source and no new advisory ignore.

### 5.3 Build time and binary size (debug, `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0`, shared target)

| Build | Wall time | Crates compiled | Binary (`debug/operon`, line tables) |
|---|---|---|---|
| Baseline `-p operon --bin operon`, dependencies warm (workspace crates rebuilt) | 157 s | about 20 workspace crates | 1,080,889,968 B |
| `--features pgwire`, first build | **501 s (+344 s)** | **261** (datafusion, lance, arrow, qdrant-edge and their dependents, because of feature unification, plus the new crates) | 1,155,116,560 B (**+74.2 MB, +6.9 %**) |
| `cargo test … --test it pgwire` after that | 167 s | test binary and dev dependencies | not measured |

- The new crates themselves take about 88 s of compile time. `datafusion-postgres` alone takes 34 s, and the rest is the feature-unification rebuild.
- Both variants stay in the shared target, so switching back and forth costs disk rather than time. The two builds together took several GB of `/home`; that was not isolated from other agents' builds.
- A stripped release delta was **not measured**: disk was too low, and the rules allow only one release build. PG1 Task 0 measures it after Task 1 has removed the default-features problem, when the number means something.

## 6. Gaps

| # | Gap | Severity | Fix | Plan task |
|---|---|---|---|---|
| G1 | `arrow-pg` 0.15 panics encoding `FixedSizeList` (`encoder.rs:488`); `LargeList` is probably also wrong. Every vector column is affected, and the connection drops | **Blocker** for any `SELECT *` | Upstream PR to arrow-pg handling `FixedSizeList` and `LargeList`. Until it merges, Operon's hook casts `FixedSizeList` to `List` before encoding, in both the simple and extended paths | PG1 T1, T2 |
| G2 | DataFusion default features through all three crates: parquet, compression, and the 261-crate rebuild | High (build cost, binary size) | Upstream PRs (`default-features = false`, `features = ["sql"]`, and `nested_expressions` where needed). Pin the fixed versions | PG1 T1 |
| G3 | One namespace per listener, fixed at start. Strong consistency only, the hot scope captured once, no `operon.consistency_token` | Medium | A per-connection session keyed by the startup `database` (= namespace), with `SET operon.consistency_token` and `SET operon.hot`. Needs handlers that choose the context per client; `DfSessionService` holds one context | PG1 T3 |
| G4 | `search_path`/`current_schema()` is `public`, but the tables are in `collections`. Unqualified `'kb'::regclass` returns nothing | Medium (tools) | Report `search_path = collections` in ParameterStatus and `current_schema()`, or register `public` as an alias of `collections` | PG1 T3 |
| G5 | Type mapping: vectors show as `text` in `\d`, `_seq_no` (u64) as `numeric`, `information_schema.columns` shows Arrow names | Medium | Vectors as `float4[]` (after G1), `_seq_no` as `int8` with a range check or documented as `numeric`, and serve `information_schema` from the pg catalog | PG1 T2 |
| G6 | Missing catalog pieces: `pg_function_is_visible`, `pg_type_is_visible`, `set_config`, `array()` (`\d+`), `pg_stat_activity`, `to_char`, JSON functions; relation sizes are 0; `\l` shows a phantom `postgres` database | Low/medium | Upstream to `datafusion-pg-catalog`, or local UDFs; sizes from the manifest's `size_bytes` | PG1 T4 |
| G7 | The search table functions need literal arguments, so `text_search('kb', %s, …)` fails with server-side binding | Medium (driver ergonomics) | Plan `$n` in table-function arguments by substituting the parameters before planning, or document `%s` → literal (psycopg `ClientCursor`) | PG1 T4 |
| G8 | No statement timeout or row cap by default (Flight SQL uses `FlightConfig.max_duration`) | Medium | Apply `SqlConfig.timeout` unless the session sets `statement_timeout`. Rows stream, and the cap is optional | PG1 T3 |
| G9 | No auth or TLS (D111). The listener refuses non-loopback addresses | By design in M1 | The unified auth plan (Q30): password/SCRAM against API keys, TLS through `ServerOptions`' rustls path | After PG1 (auth plan) |
| G10 | Graceful shutdown drops open connections at once, with no drain | Low | Drain for `HTTP_GRACE` like Flight SQL, and wire the listener into `Server` rather than `main` | PG1 T2 |
| G11 | `COPY … TO STDOUT` is refused | Low | Serve it read-only through the CopyHandler (text and CSV) | PG1 T5 |

## 7. Recommendation

**Adopt it.** Keep the feature opt-in until G1 and G2 are fixed upstream, then consider turning it on by default. `datafusion-postgres` saves writing a protocol layer, a type encoder and a pg catalog. What Operon adds is small, about 300 lines: the listener, the session and the read-only hook over `plan_read_only`. This follows the owner's preference to buy rather than build. Ownership: datafusion-contrib, Apache-2.0, releases every 6–8 weeks, pgwire by the same maintainer (sunng87).

- **Read-only first** (PG1 Tasks 1–5): upstream fixes, the listener inside `Server`, per-connection sessions, catalog polish, `COPY TO`, and client tests in CI.
- **Writes after that** (PG1 Tasks 6–10, behind `--pg-allow-writes`, off by default). They map onto Operon's collection write paths (D-PG-1): `INSERT` is create-if-absent (`ON CONFLICT (_id) DO UPDATE` is upsert, `DO NOTHING` skips existing keys); `UPDATE … WHERE` is `patch_by_filter`; `DELETE … WHERE` is `delete_by_filter`; `COPY … FROM STDIN` is the Flight `DoPut` bulk path. Every statement is autocommit, and a transaction block may hold at most one write. `CREATE TABLE` is not mapped in PG1.
- **Boundary:** the Postgres wire is for analytics and ingest over collections. It is not an OLTP database. Clients that need multi-statement ACID transactions go to Loam Live and TiDB (D123), which stays the MySQL-protocol OLTP store.
- **Placement:** PG1 is the first milestone after M1 exits (after M1.7), before M2. Its read-only half (Tasks 0–5) has no dependency on M1.5–M1.7 and could start earlier between builds if the owner wants. The write half needs M1.5 Task 9a (`patch_by_filter` and `delete_by_filter`) merged.

### Artifacts (outside the repository)

Scratchpad `…/scratchpad/pgwire/`:

- `pgwire-spike.patch`: the code, which applies cleanly to `origin/main` 2d83ad5;
- `psql-reads.txt`, `psql-reads2.txt`, `psql-writes.txt`, `psycopg.txt`, `node-pg.txt`, `dbeaver.txt`: raw client output;
- `tree.txt`: `cargo tree` inversions;
- `deny.txt`;
- `build-base.log`, `build-pg.log` and `test.log`;
- the probe scripts `psycopg_probe.py`, `probe.mjs` and `catalog_queries.sql`.

The scratchpad is temporary. PG1 Task 2 re-creates the code from the patch and this document.
