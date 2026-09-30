# PG1 — Postgres Wire Access over Collections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, SQLSTATEs), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-09-28). **Slot: the first milestone after M1 exits (after M1.7), before M2** (proposed; D-PG-1). Branches `pg1-t<N>`, stacked; PRs target `main`. The read-only half (Tasks 0–5) depends only on M1.2 as merged, so it may be pulled forward between M1 builds if the owner wants. The write half (Tasks 6–10) needs M1.5 Task 9a (`patch_by_filter`, `delete_by_filter`) merged. PG1 adds a feature, a module and a listener; it changes no M1 code path beyond `NamespaceCatalog::register_schema` and the server's listener wiring.

> **As built (2026-09-29):** the spike's read-only listener is on `main` behind `pgwire` (off by default): `crates/operon/src/pg/`, `--pg-listen` (loopback only) and `--pg-namespace` (default `default`), `ReadOnlyHook` over `plan_read_only` with SQLSTATE 25006 for anything but a query or a session statement, 0A000 for list columns the encoder cannot yet return, and `NamespaceCatalog::register_schema` for `pg_catalog`. Task 1's upstream fixes, the socket-level tests of Task 2, the exact startup line and the feature-off warning are still to do.

**Goal:** Serve Operon's collections over the Postgres wire protocol through `datafusion-postgres` (datafusion-contrib, Apache-2.0), in two halves:
- **Read-only (Tasks 0–5).** The same SQL surface as Flight SQL: collections as tables, `vector_search`, `text_search`, `hybrid_search` and `rrf`. `psql`, `psycopg` 3, node-postgres and DBeaver-style catalog queries work, per namespace, with consistency tokens.
- **Writes (Tasks 6–10), behind `--pg-allow-writes` (off by default).** `INSERT` (with `ON CONFLICT`), `UPDATE … WHERE`, `DELETE … WHERE` and `COPY … FROM STDIN` map onto `CollectionService`'s write paths. Every statement is autocommit, and a transaction block may hold at most one write.

The Postgres wire is analytical and ingest access to collections. **It is not an OLTP database:** clients that need multi-statement transactions go to Loam Live and TiDB, which stays the MySQL-protocol OLTP store (D123).

**Architecture:**
- **Feature `pgwire`** on `operon`, off by default until Task 1's upstream fixes land; the owner then decides the default. Dependencies: `datafusion-postgres` 0.18+, which brings `arrow-pg`, `datafusion-pg-catalog` and `pgwire` 0.40. `datafusion-pg-functions` is not used (spike §5.1).
- **`crates/operon/src/pg/`**: the listener (loopback only), the per-connection session (database = namespace), `ReadOnlyHook` over `operon_query::sql::plan_read_only`, and in Tasks 6–10 `WriteHook` and the COPY handler. It holds an `Arc<CollectionService>` and never touches storage or meta (overview §8).
- **Wire library use.** `pgwire::tokio::process_socket` runs with Operon's own `PgWireServerHandlers`. The library's `DfSessionService` executes plans; its cursor, `SET`/`SHOW` and transaction hooks come first; Operon's hooks come last. Operon never uses `datafusion_postgres::serve`, `AuthManager` or `PermissionsHook`.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. `datafusion-postgres` 0.18 (or the release carrying Task 1's fixes). Dev-dependency `tokio-postgres` 0.7 (MIT OR Apache-2.0) for socket-level tests; Task 0 checks it against `deny.toml`. psql ≥ 16, Python 3.13 with `uv` and `psycopg[binary]` 3.3, Node ≥ 22 with `pg` 8, used in the CI job of Task 5.

**Spec:**
- [`docs/plans/pgwire-spike.md`](pgwire-spike.md): all of it; §6 lists the gaps this plan closes.
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D-PG-1, D111 (loopback; no auth or TLS until the unified auth plan, Q30), D56 (search function names), D86 (backpressure), D87 (filter writes), D123 (TiDB SQL).
- As built: `operon_query::sql` (`plan_read_only`, `read_only_options`, `NamespaceCatalog`, `expr_to_query`), `CollectionService::{sql_context_with, write, pin, get}`, `flight_ingest` (`CollectionBatchMapper`, `put_chunk_rows`), and M1.5 Task 9a's `filter_write.rs` once it has merged.

## Global Constraints

Same as the M1 overview §8, plus:
- **Loopback only (D111, D-PG-1).** `--pg-listen` accepts only loopback addresses. Any other address fails startup with `postgres listen on <addr>: only loopback addresses are served until the unified auth plan (D111)`. The listener is off unless `--pg-listen` is given; there is no default port. When the flag is given, the listener's own address prints `operon postgres listening on postgres://<addr>/<ns>`.
- **Read-only unless `--pg-allow-writes`.** Without the flag, every statement other than a query, `EXPLAIN`, `SET`/`SHOW`, a transaction statement or a cursor statement is refused with SQLSTATE `25006` (`cannot execute <VERB> in a read-only transaction`). With it, only the statements of Tasks 7–9 are served. Everything else stays 25006 or `0A000`.
- **Never copy** vul-os/basin (`crates/basin-router`, pgwire 0.28: COPY, connection limits, TLS). It may be read as a reference for COPY streaming only.
- **Upstream first.** Fixes to `arrow-pg`, `datafusion-pg-catalog` or `datafusion-postgres` go upstream as small PRs. Operon carries a local workaround only until the fixed release is pinned, and each workaround names its upstream PR in a comment.
- **The build machine.** One cargo build at a time, the shared target, `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0` for measurements. Stop and report if `/home` has under 8 GB free.
- **Commit areas:** `pg`, `query`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The Postgres database is the Operon namespace.** The startup parameter `database` selects it; with no parameter (or `postgres`), `--pg-namespace` is used (default `default`). The schema is `collections`; `search_path` and `current_schema()` report `collections` | One connection string per tenant, the way Postgres tools expect; the spike's `public` search path broke unqualified `regclass` (G4) | A tool that hard-codes `public` misses tables; Task 3 registers `public` as an alias schema if Task 5's DBeaver replay needs it |
| 2 | **Each connection has its own session:** consistency `Strong` unless `SET operon.consistency_token = '<token>'` (then `AtLeast`), `SET operon.hot = on\|off`, and read-your-writes: after a write, the session's reads use `AtLeast(last token)` | Mirrors Flight SQL's metadata (`operon-consistency-token`, `operon-hot`) per connection | Per-connection contexts cost a `SessionContext` per connection; they are cheap (catalog providers resolve lazily) |
| 3 | **Every statement is autocommit.** In a `BEGIN … COMMIT` block, reads are unlimited and **at most one write** is allowed. It is applied when it runs. A second write answers `0A000` (`Operon runs one write per transaction; use separate transactions, or Loam Live for multi-statement transactions`) and aborts the block. `ROLLBACK` after an applied write answers `0A000` (`cannot roll back: the write was applied at <token>`) and ends the block. `SAVEPOINT` answers `0A000` | Operon has no multi-statement ACID over collections. Deferring the write to `COMMIT` would break ORMs that check `rowcount` at `UPDATE`. An error is the honest answer to a rollback that cannot undo | A client that relies on `ROLLBACK` to undo a successful write gets an error instead of silent success. Documented with the OLTP boundary |
| 4 | **Plain `INSERT` is create-if-absent with Postgres's duplicate error.** A strong `get` of the statement's keys runs first; if any exists, `23505` (`duplicate key value violates unique constraint "<collection>_pkey"`, detail `Key (_id)=(…) already exists.`) and nothing is written. Otherwise each row is a `DocOp::Patch { upsert: Some(doc) }` with an empty patch, in one atomic write. A key created between the check and the write stays unchanged and is reported as 23505 afterwards, with the inserted count in the detail | Matches Postgres in the common case without an atomic conditional-write primitive | A race answers 23505 after the other rows are in; documented and tested |
| 5 | **`ON CONFLICT (_id) DO UPDATE SET <every column> = EXCLUDED.<column>`** is `DocOp::Upsert`. **`DO UPDATE SET <some columns> = EXCLUDED.…`** is `DocOp::Patch { mode: Merge, upsert: Some(doc) }`. **`DO NOTHING`** is create-if-absent, skipping existing keys silently (tag `INSERT 0 <inserted>`). Any other conflict target or expression answers `0A000` | The owner's mapping (upsert, create-if-absent); a partial `SET` is a merge patch | None beyond the documented refusals |
| 6 | **`UPDATE … SET col = <constant> [, …] WHERE <filter>`** is `patch_by_filter` (merge mode). **`DELETE … WHERE <filter>`** is `delete_by_filter`. `DELETE` without `WHERE` deletes every document, within `MAX_DELETE_BY_FILTER_ROWS`. The filter goes through `expr_to_query`; a predicate it cannot express answers `0A000` naming the predicate. Assigning `_id` or an expression that reads columns answers `0A000`. Hitting a row limit answers `54000` (`program_limit_exceeded`) with the affected count; `allow_partial` stays false | Reuses D87's operations and their limits unchanged | Users hitting 50 000 rows on `UPDATE` must batch by key range; the hint says so |
| 7 | **`COPY … FROM STDIN`** in `text` and `csv` (with `HEADER`, `DELIMITER`, `NULL`, a column list) is Flight `DoPut`'s bulk path: rows become Arrow batches through `CollectionBatchMapper`, written as upserts in chunks of `FlightConfig.put_chunk_rows`. `binary` is deferred. A failed chunk ends the COPY with an error whose detail names the rows already committed; Postgres would roll back the whole COPY | One ingest semantics across Flight and Postgres; chunked writes bound memory | A partially applied COPY; documented, and the error says how far it got |
| 8 | **`CREATE TABLE` is not mapped in PG1** (`0A000`, hint: create collections through the REST API or SDK). `DROP`/`ALTER`/`TRUNCATE` stay refused | A collection schema holds analyzers, keyword vs text, vector distance, HNSW settings and partitions, which plain SQL types cannot express without a custom `WITH (…)` grammar. Worth doing only with a pgvector-style `vector(n)` mapping, which could come after PG1 | Tools that create tables before loading fail; the hint points to the REST call |
| 9 | **Consistency tokens come back after every write** as the ParameterStatus `operon_consistency_token` (psycopg: `conn.info.parameter_status(…)`) and as a NOTICE `operon consistency token: <token>` | ParameterStatus is machine-readable across drivers; the NOTICE shows in psql | None |
| 10 | **Backpressure** (`ServiceError::ResourceExhausted`, D86) answers `53000` (`insufficient_resources`) with detail `retry after <ms> ms`; `Timeout` answers `57014`; `NotFound` `42P01`; `InvalidArgument` and `SchemaViolation` `42000` or `22000`; `Unavailable` `08006` only while the service is stopping, else `53000` | Standard classes drivers already map to retryable and non-retryable errors | None |

## Carried in

From the spike (§6): G1–G11. The patch `pgwire-spike.patch` (listener, session, `ReadOnlyHook`, `NamespaceCatalog::register_schema`, 4 tests) is the starting point of Task 2 if it is still available; otherwise the spike doc describes it completely.

## Review Focus

1. **Read-only is airtight without `--pg-allow-writes`.** DDL, DML, `COPY`, `SELECT INTO`, `EXPLAIN ANALYZE <dml>`, and multi-statement strings that hide a write. Tests: Task 2 (`writes_are_refused_with_25006`, `explain_analyze_of_dml_is_refused`, `multi_statement_hides_no_write`).
2. **No panic reaches a connection.** Every Arrow type Operon produces encodes. Tests: Task 2 (`every_collection_column_type_encodes`, which includes vectors, sparse vectors and `_seq_no`).
3. **Loopback refusal.** Tests: Task 2 (`non_loopback_is_refused`).
4. **Write mapping matches the rulings exactly.** Tests: Tasks 7–9, plus Task 10's differential against the REST writes.
5. **Transactions never pretend.** Tests: Task 6 (`second_write_in_block_is_0a000`, `rollback_after_write_is_0a000`).

## File structure

```
Cargo.toml / Cargo.lock                     # datafusion-postgres (pinned per Task 1); dev: tokio-postgres
crates/operon-query/src/sql/catalog.rs      # NamespaceCatalog::register_schema (from the spike)
crates/operon/Cargo.toml                    # feature pgwire
crates/operon/src/pg/{mod.rs,listen.rs,session.rs,read.rs,encode.rs,write.rs,copy.rs,txn.rs,errors.rs}
crates/operon/src/{server.rs,main.rs,lib.rs} # PgConfig in ServerConfig; --pg-listen, --pg-namespace, --pg-allow-writes
crates/operon/tests/pg/{main.rs,read.rs,catalog.rs,session.rs,write.rs,copy.rs,txn.rs}
scripts/pg/{clients.sh,psycopg_probe.py,pg_probe.mjs,catalog_replay.sql}
.github/workflows/ci.yml                    # job pgwire (path-filtered)
docs/design/05-query-engine.md  docs/design/13-decision-log.md  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile and measure

**Files:** read `crates/operon/src/{server.rs,main.rs}`, `crates/operon-query/src/{sql/,flight.rs,flight_ingest.rs,service.rs}` and, if merged, `filter_write.rs`, all as on `main`. Fill this plan's "Rulings made during execution" table.

**Checks** (record each result, with the command, in the spike doc as §8):
- The latest `datafusion-postgres`, `arrow-pg`, `datafusion-pg-catalog` and `pgwire` versions, and whether they still match Operon's DataFusion and arrow. If Operon has moved past DataFusion 54, the versions to use.
- Whether G1 and G2 are fixed upstream (Task 1 may already be done).
- `cargo deny check` with the feature, and the new duplicates (`cargo tree -d`) against the spike's list.
- **A stripped release size delta and cold release build delta, one measured build each way**, deleted afterwards. Do this after Task 1 if Task 1 lands first.
- Whether M1.5 Task 9a has merged (it gates Tasks 8), and the as-built `FilterWriteOptions`/`FilterWriteResult`.
- The final decision number for D-PG-1.

**Commit:** `docs: reconcile PG1 with main`.

### Task 1: Upstream fixes and pins

**Files:** PRs to `datafusion-contrib/datafusion-postgres` (the workspace holds `arrow-pg`, `datafusion-pg-catalog` and `datafusion-postgres`); `Cargo.toml`.

**Semantics:**
- `arrow-pg`: encode `FixedSizeList` and `LargeList` (and `FixedSizeList<Float32>` as `float4[]`) without `unwrap`. The unsupported-type path returns an error, never a panic (G1).
- `arrow-pg`, `datafusion-pg-catalog`, `datafusion-postgres`: `datafusion = { version = "54", default-features = false, features = ["sql", …] }`, with only the features each crate uses (G2).
- Until they are released: Task 2's `encode.rs` casts `FixedSizeList` to `List` before encoding, and the feature stays off by default. Each workaround names its PR.

**Tests:** upstream's own tests, plus a local `cargo tree -p operon --features pgwire -e features -i datafusion` showing no `parquet` or `compression` feature once pinned.

**Commit:** `pg: pin datafusion-postgres with upstream fixes`.

### Task 2: The listener in `Server`, read-only, with encoding safety

**Files:** `crates/operon/src/pg/{mod.rs,listen.rs,read.rs,encode.rs,errors.rs}`, `crates/operon/src/{server.rs,main.rs,lib.rs}`, `crates/operon-query/src/sql/catalog.rs`, `crates/operon/tests/pg/{main.rs,read.rs}`.

**Produces:**

```rust
// crates/operon/src/pg/mod.rs
pub const READ_ONLY_SQLSTATE: &str = "25006";
#[derive(Clone, Debug)]
pub struct PgConfig { pub listen: SocketAddr, pub namespace: String, pub allow_writes: bool, pub max_connections: usize /* 256 */ }
pub struct PgHandle { pub addr: SocketAddr, /* task, stop, tracker */ }
impl PgHandle { pub async fn stop_within(self, grace: Duration); }
pub async fn bind(addr: SocketAddr) -> Result<TcpListener, PgError>;          // refuses non-loopback
pub fn serve(listener: TcpListener, service: Arc<CollectionService>, config: PgConfig) -> PgHandle;
// ServerConfig gains `#[cfg(feature = "pgwire")] pub pg: Option<PgConfig>`; Server gains `pg_addr()`.
```

**Semantics:**
- Wire `pg` beside the Elasticsearch gateway in `Server::assemble` (gateway role only), not in `main`: bind before the HTTP line, stop first on shutdown, and drain connections for `HTTP_GRACE` (G10). Connections over `max_connections` get `53300` (`too_many_connections`).
- `ReadOnlyHook` as in the spike: queries and `EXPLAIN` through `plan_read_only`; everything else not taken by the library's hooks answers 25006. It also inspects `EXPLAIN ANALYZE`'s inner statement.
- `encode.rs`: before `encode_dataframe`, cast every column whose type the pinned `arrow-pg` cannot encode (Task 1's list) to one it can, in both the simple and extended paths. Vectors are `float4[]`. `_seq_no` stays `numeric` (u64), documented.
- Errors map to SQLSTATEs per Ruling 10.

**Tests** (`tests/pg/read.rs`, over a real socket with `tokio-postgres`, ports from `127.0.0.1:0`): `select_filter_aggregate`; `search_table_functions` (vector, text, `rrf`); `extended_protocol_with_parameters`; `every_collection_column_type_encodes`; `writes_are_refused_with_25006` (INSERT, UPDATE, DELETE, CREATE, DROP, TRUNCATE, COPY, SELECT INTO); `explain_analyze_of_dml_is_refused`; `multi_statement_hides_no_write`; `non_loopback_is_refused`; `shutdown_drains_then_closes`; flag parsing in `main.rs` (`--pg-listen`, `--pg-namespace` requires `--pg-listen`, and the feature-off warning).

**Commit:** `pg: serve read-only SQL over the Postgres wire protocol`.

### Task 3: Per-connection sessions

**Files:** `crates/operon/src/pg/session.rs`, `crates/operon/tests/pg/session.rs`.

**Semantics:** Rulings 1 and 2. A `PgWireServerHandlers` whose query handlers pick, per client, a session keyed by the connection (created at startup from `database`) and cached in the client's `SessionExtensions`. `SET operon.consistency_token` / `operon.hot` rebuild the session's context. `statement_timeout` defaults to `SqlConfig.timeout` (G8). An unknown database answers `3D000` (`database "<ns>" does not exist`) at startup. `pg_database` lists the namespaces, with no phantom `postgres`. `search_path` is `collections`.

**Tests:** `database_selects_namespace`; `unknown_database_is_3d000`; `consistency_token_setting_reads_at_least`; `hot_setting_is_per_connection`; `default_statement_timeout_applies`; `unqualified_regclass_resolves`.

**Commit:** `pg: give each connection its namespace and consistency`.

### Task 4: Catalog and function polish

**Files:** upstream PRs to `datafusion-pg-catalog` where general, otherwise `crates/operon/src/pg/catalog.rs`; `crates/operon/tests/pg/catalog.rs`; `scripts/pg/catalog_replay.sql`.

**Semantics:** close G6 and G7:
- `pg_function_is_visible`, `pg_type_is_visible`, `set_config`, `array(…)` for `\d+`, `pg_stat_activity` (one row per connection);
- relation sizes from the manifest's `size_bytes`;
- `information_schema.columns` with Postgres type names;
- `$n` parameters in search table-function arguments, substituted before planning.

JSON (`->>`, `json_extract_path_text` over `_source`) comes in as far as DataFusion's functions allow. What is still missing is listed in the docs.

**Tests:** `catalog_replay_passes` (psql `\d+`, `\df`, `\dT`, plus the DBeaver, Grafana and Metabase queries of the spike §4.5 as a golden file); `search_function_accepts_parameters`.

**Commit:** `pg: complete the catalog queries psql and DBeaver send`.

### Task 5: `COPY TO`, the client CI job and docs

**Files:** `crates/operon/src/pg/copy.rs` (read side), `scripts/pg/{clients.sh,psycopg_probe.py,pg_probe.mjs}`, `.github/workflows/ci.yml` (job `pgwire`, path-filtered on `crates/operon/src/pg/**`), `docs/design/05-query-engine.md` (a Postgres wire section), `CHANGELOG.md`.

**Semantics:** `COPY (<query>) TO STDOUT` and `COPY <collection> TO STDOUT`, in `text` and `csv` (G11), through the read-only guard. `clients.sh` starts `operon dev --pg-listen 127.0.0.1:<port>`, loads the M1.6 fixture over REST, and runs psql, psycopg (uv venv) and node-postgres probes that assert results; the probes are the spike's scripts, made asserting.

**Tests:** `copy_to_stdout_text_and_csv`; the CI job itself.

**Commit:** `pg: add COPY TO and the Postgres client job`.

### Task 6: Write plumbing: flag, transactions, tokens

**Files:** `crates/operon/src/pg/{txn.rs,write.rs,errors.rs}`, `crates/operon/src/main.rs` (`--pg-allow-writes`), `crates/operon/tests/pg/txn.rs`.

**Semantics:** Rulings 3, 9 and 10. `WriteHook` sits before `ReadOnlyHook` when `allow_writes` is set and takes only `INSERT`, `UPDATE`, `DELETE` and `COPY FROM`; Tasks 7–9 fill in what each does. It records the connection's transaction state: `Idle`, `InBlock { wrote: Option<ConsistencyToken> }` and `Aborted`, alongside pgwire's transaction status. `--pg-allow-writes` without `--pg-listen` is a CLI error. Without the flag nothing changes: writes stay 25006.

**Tests:** `writes_need_the_flag`; `autocommit_write_returns_token_parameter_status`; `one_write_in_block_commits`; `second_write_in_block_is_0a000`; `rollback_after_write_is_0a000`; `savepoint_is_0a000`; `read_your_writes_in_session`.

**Commit:** `pg: add the write gate, one-write transactions and consistency tokens`.

### Task 7: `INSERT`

**Files:** `crates/operon/src/pg/write.rs`, `crates/operon/tests/pg/write.rs`.

**Semantics:** Rulings 4 and 5. `INSERT INTO <collection> (<columns>) VALUES (…), …` with literals or `$n`, at most `MAX_WRITE_OPS` rows. `_id` is required: integer, text or uuid map to `PrimaryKey`. Other columns map to schema fields by name (`source_path`) and must be in the schema: an unknown column answers `42703`. Alternatively, a whole `_source` column carries JSON text; mixing the two answers `42601`. Vector columns take `float4[]`, `ARRAY[…]` or `'[…]'` text. `INSERT … SELECT` and `RETURNING` answer `0A000` in PG1. The tag is `INSERT 0 <n>`.

**Tests:** `insert_values_creates`; `insert_duplicate_is_23505_and_writes_nothing`; `insert_race_reports_23505_after`; `on_conflict_do_update_all_is_upsert`; `on_conflict_do_update_some_is_merge_patch`; `on_conflict_do_nothing_skips`; `insert_vector_forms`; `insert_unknown_column_is_42703`; `insert_select_is_0a000`; `backpressure_is_53000`.

**Commit:** `pg: map INSERT onto collection writes`.

### Task 8: `UPDATE` and `DELETE` through filter writes

**Files:** `crates/operon/src/pg/write.rs`, `crates/operon/tests/pg/write.rs`. Needs M1.5 Task 9a.

**Semantics:** Ruling 6. The `WHERE` expression is planned against the collection's arrow schema and translated with `expr_to_query`. The pin is the session's consistency (`Strong` or `AtLeast`). The tags are `UPDATE <affected>` and `DELETE <affected>`. `UPDATE … WHERE _id = <k>` with one key may use a single `DocOp::Patch` instead of a filter write; the result is the same.

**Tests:** `update_where_patches_matches_only`; `update_vector_column`; `update_id_is_0a000`; `update_expression_is_0a000`; `update_over_limit_is_54000`; `delete_where_deletes_matches_only`; `delete_all_within_limit`; `untranslatable_filter_is_0a000`.

**Commit:** `pg: map UPDATE and DELETE onto filter writes`.

### Task 9: `COPY … FROM STDIN`

**Files:** `crates/operon/src/pg/copy.rs`, `crates/operon/tests/pg/copy.rs`.

**Semantics:** Ruling 7. This is pgwire's `CopyHandler`, never basin's code. Rows are parsed incrementally, `put_chunk_rows` at a time; memory stays bounded whatever the COPY's size. The tag is `COPY <n>`.

**Tests:** `copy_text_ingests`; `copy_csv_header_and_columns`; `copy_large_is_bounded_memory` (1 M rows, RSS bound); `copy_bad_row_reports_committed_rows`; `copy_binary_is_0a000`; `copy_equals_doput` (the same rows through Flight `DoPut` and COPY give identical collections).

**Commit:** `pg: ingest COPY FROM STDIN through the bulk path`.

### Task 10: Write gates and docs

**Files:** `scripts/pg/{clients.sh,psycopg_probe.py,pg_probe.mjs}` (write probes, including SQLAlchemy Core `insert`/`update`/`delete` and psycopg `copy`), `crates/operon/tests/pg/write.rs` (a differential test), `docs/design/05-query-engine.md`, `docs/design/13-decision-log.md`, `CHANGELOG.md`, `docs/plans/README.md`.

**Semantics:** The differential: a seeded random sequence of writes applied once through Postgres and once through REST must give identical `scroll` results. The docs page states the transaction rules, the SQLSTATEs, the limits and **the OLTP boundary (use Loam Live / TiDB for multi-statement transactions)**.

**Tests:** `pg_writes_equal_rest_writes` (proptest, 64 cases); the CI job's write probes.

**Commit:** `docs: document Postgres wire writes and close PG1`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
