# operon-live

Loam Live: a reactive document database on TiKV. It serves one app in one TiKV keyspace, with:
- documents, tables and indexes;
- a sharded commit journal;
- mutations as TiKV transactions (`LiveTxn`);
- built-in `_system:*` and QuickJS query and mutation functions;
- reactive subscriptions;
- the `loam.live.v1` sync API over connect-rust (`Watch`, `ModifyQuerySet`, `Query`, `Mutate`, `Deploy`).

Design: [§20](../../docs/design/20-reactive-database-on-tikv.md), with its as-built notes in §20 §20. The R1 plan is [here](../../docs/plans/2026-09-27-r1-reactive-core.md), and the gate results are in the [R1 exit report](../../docs/plans/r1-exit-report.md).

R1 is a development release. **The Live API has no authentication** (D111): the listener binds 127.0.0.1:7710, and `operon` refuses a non-loopback `--live-listen`. The crate is `publish = false` while `tikv-client` is a git pin.

## Dev setup

### 1. A TiKV playground

Live needs PD and TiKV v8.5.8 on API v2. The playground script starts them with `tiup` on the Loam port offset, which puts PD on `127.0.0.1:19379`. It also pre-allocates the keyspaces `loam_meta`, `loam_live_dev` and the test keyspaces (`deploy/tikv/pd.toml`).

```sh
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh   # once
scripts/tikv/playground.sh start          # tag loam-dev; the first start downloads about 500 MB
scripts/tikv/playground.sh status
scripts/tikv/playground.sh stop           # also deletes ~/.tiup/data/loam-dev
```

- **Memory.** The playground peaks at about 3.2 GB of RAM, most of it TiKV at startup. The script refuses to start while `cargo` or `rustc` runs; pass `--force` to override.
- **More stores.** `--stores N` starts N TiKV stores.
- **TiDB.** `--with-tidb` adds a keyspace-mode TiDB. R1's TiDB task (Task 15) is parked, so nothing in Live uses it yet.

### 2. `operon dev` with Live

Live is behind the `live` feature, which is off by default and enables `tikv`:

```sh
cargo run -p operon --features live -- dev
# …
# operon live listening on http://127.0.0.1:7710
# operon listening on http://127.0.0.1:8080
```

In a `live` build, `operon dev` serves Live by default against the playground's PD. The flags are on `dev` and `standalone`:

| Flag | Default | Meaning |
|---|---|---|
| `--live-listen` | `127.0.0.1:7710` | The sync API; loopback addresses only |
| `--live-pd` | `127.0.0.1:19379` on `dev`; required on `standalone` | PD endpoints, comma-separated |
| `--live-app` | `dev` | The app name |
| `--live-keyspace` | `loam_live_<app>` | The app's keyspace; it must exist in PD |
| `--live-root` | none | A hex key prefix inside the keyspace (tests isolate by it) |
| `--live-tick-read-lag-ms` | `200` | How far behind a fresh TSO timestamp subscription ticks read |
| `--live-js-contexts` | `4` | QuickJS contexts per deployment, each with its own runtime and a 64 MiB limit |
| `--no-live` | off | Run without Live |

Add `--meta tikv://127.0.0.1:19379/loam_meta` to put the metastore on the same cluster. Its GC loop then also sweeps Live's commit tokens. Without it, Live runs its own GC loop.

### 3. Calling the API

Every call is a Connect unary or server-streaming RPC under `/loam.live.v1.LiveService/`. The built-in functions work before any deploy: `_system:insert`, `_system:get`, `_system:query`, `_system:patch`, `_system:replace` and `_system:delete`. The first insert into a table creates it.

```sh
curl -s http://127.0.0.1:7710/loam.live.v1.LiveService/Mutate \
  -H 'content-type: application/json' \
  -d '{"function": "_system:insert", "args": {"objectValue": {"fields": {
        "table": {"stringValue": "messages"},
        "fields": {"objectValue": {"fields": {"body": {"stringValue": "hello"}}}}}}}}'
# {"commitTs":"…","result":{"stringValue":"<document id>"}}
```

Values use the protobuf JSON mapping of `loam.live.v1.Value`. An `int64` is a JSON string. An error carries a `loam.live.v1.LiveError` Connect error detail with the exact code.

## TypeScript: `@operon/live`

The client is in [`sdks/live-typescript`](../../sdks/live-typescript). It is private until the Loam rename, so build it from the repository:

```sh
cd sdks/live-typescript && pnpm install && pnpm run build
```

A deployed bundle is one ES module that imports `query` and `mutation` from `loam:server`. Functions are addressed as `module:export`. The CLI that bundles TypeScript with esbuild is not built yet, so this example deploys the module as a string:

```ts
import { LiveClient, type LocalStore } from "@operon/live";

const client = new LiveClient({ baseUrl: "http://127.0.0.1:7710" }); // transport: "connect" (default) or "grpc-web"

await client.deploy(
  `
  import { query, mutation } from "loam:server";
  export const messages = {
    send: mutation(async (ctx, { channel, body }) => ctx.db.insert("messages", { channel, body })),
    list: query(async (ctx, { channel }) =>
      ctx.db.query("messages").withIndex("by_channel", (q) => q.eq("channel", channel)).collect()),
  };
  `,
  { tables: [{ name: "messages", indexes: [{ name: "by_channel", fields: ["channel"] }] }] },
);

interface Message { _id: string; _creationTime: bigint; channel: string; body: string }

// A live query: onUpdate runs with each new result.
const list = client.watch<Message[]>("messages:list", { channel: "general" });
list.onUpdate((docs) => console.log(docs.map((d) => d.body)));
list.onError((err) => console.error(err.code, err.message));

// A mutation with an optimistic update. The update shows at once. The client
// drops it when the session's results reach the commit timestamp.
const { result: id, commitTs } = await client.mutate<string>(
  "messages:send",
  { channel: "general", body: "hello" },
  {
    idempotencyKey: crypto.randomUUID(), // a retry with the same key applies once
    optimistic: (local: LocalStore) => {
      const docs = local.getQuery<Message[]>("messages:list", { channel: "general" }) ?? [];
      local.setQuery("messages:list", { channel: "general" }, [
        ...docs,
        { _id: "pending", _creationTime: 0n, channel: "general", body: "hello" },
      ]);
    },
  },
);

// A one-shot query at the commit timestamp sees the write.
const now = await client.query<Message[]>("messages:list", { channel: "general" }, { ts: commitTs });
console.log(id, now.length);

client.close();
```

- **Values.** An `int64` is a `bigint`, a `float64` a `number`, and bytes a `Uint8Array`.
- **One session per client.** It resumes after a reconnect or a version gap.
- **Busy deploys.** `deploy` retries a deploy answered `UNAVAILABLE`. That is the answer to an index change while mutations are in flight.

## Tests

Tests that need TiKV skip unless `OPERON_TEST_PD` is set:

```sh
scripts/tikv/playground.sh start --tag loam-test
OPERON_TEST_PD=127.0.0.1:19379 cargo test -p operon-live
OPERON_TEST_PD=127.0.0.1:19379 OPERON_CHECKER_SECS=60 cargo test -p operon-live --test reactive_checker --test txn_checker
cargo build -p operon --features live   # test:live spawns it; OPERON_BIN overrides target/debug/operon
cd sdks/live-typescript && pnpm run build && pnpm test && OPERON_TEST_PD=127.0.0.1:19379 pnpm run test:live
```

The nemesis (`scripts/tikv/nemesis.sh`) runs the checkers while it kills and restarts TiKV and PD, stalls PD, and pauses the test process. The CI job `tikv-nemesis` runs it nightly.
