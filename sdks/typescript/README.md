# The TypeScript SDK (`@loams/client`)

Design §44 §7 and §10.2: the SDK is a `Loams` object with namespaced modules,
generated from the `loams.options.v1` annotations on the protos, over the
unified Connect API on one port. This directory is SDK2 Task 0's deliverable,
the reference the other twelve SDKs follow.

```
sdks/typescript/
  buf.gen.yaml            # the SDK facade: protoc-gen-loams-facade -> client/src/gen
  buf.gen.live.yaml       # @loams/live's stubs: protoc-gen-es
  packages/
    client/               # @loams/client: the generated facade + the runtime
    live/                 # @loams/live: the generated loams.live.v1 stubs
```

`@loams/client` and `@loams/live` are workspace packages of `web/` (see
`web/pnpm-workspace.yaml`), so they resolve `@loams/proto` — the console's
generated package — as a workspace dependency and share one lockfile. The
generator inputs stay outside the workspace, next to `proto/`.

## Using it

```ts
import { Loams, apiKey, FeatureNotInVariantError } from '@loams/client';

const loams = new Loams({
  endpoint: 'https://acme.loams.dev',
  auth: apiKey(process.env.LOAMS_API_KEY!),   // or envToken(), oidcExchange({...})
});

const info = await loams.instance.getInstance({});   // no auth needed
await loams.system.guard('live');                    // costs no RPC once cached

try {
  await loams.tables.mutate(request);
} catch (error) {
  if (error instanceof FeatureNotInVariantError) {
    // This build variant does not carry loams.live.v1.
    console.warn(error.variant);                    // 'standard'
  }
}
```

One port serves the Connect protocol, gRPC and gRPC-Web (D600), so the same
object works in a browser, in Node, in Deno and in Bun. The default transport is
Connect over `fetch`. For HTTP/2 gRPC on a server:

```ts
import { Loams, createNodeTransport } from '@loams/client/node';
const loams = new Loams({ endpoint, transport: createNodeTransport() });
```

The `/node` subpath exists so a bundler never follows `@connectrpc/connect-node`
into a browser build it cannot satisfy; the conformance suite checks that
statically.

## What is generated and what is hand-written

Design §44 §7.3 draws the line, and this SDK follows it exactly.

**Generated**, by `buf generate` and never hand-edited:

- the messages and service descriptors (`protoc-gen-es`), in `@loams/proto`
  (the app packages) and `@loams/live` (`loams.live.v1`);
- the **facade** — `src/gen/facade.ts`, written by `protoc-gen-loams-facade`
  (`crates/loams-facade-gen`): one typed interface per service annotated with
  `loams.options.v1.module`, one method per `loams.options.v1.facade` call, the
  binding table the runtime dispatches through, and the `Reason` union read
  from `docs/api/reasons.md`.

**Hand-written**, once, in `src/runtime/`: the transport, the token sources, the
retry loop, the error mapping, the token store, the pagination iterator and the
streams.

`src/loams.ts` is the join: it reads the generated table, builds one object per
module out of it, and hands every call to the same invoker. It contains no
method names and no RPC paths, which is why annotating a proto is enough to add
an SDK method in thirteen languages.

## Adding a method to the SDK

1. Annotate the service with a module, and the method with a facade call, in the
   proto:

   ```proto
   service ThingService {
     option (loams.options.v1.module) = { name: "things" summary: "Things." };

     rpc Get(GetRequest) returns (GetResponse) {
       option idempotency_level = NO_SIDE_EFFECTS;
       option (loams.options.v1.facade) = { name: "get" };
     }
   }
   ```

   A service with no `module` option is not in the SDK, and a method with no
   `facade` option is not exposed — which is how an admin-only RPC such as
   `LiveService/Deploy` stays out of every SDK by default.

2. `pnpm --filter @loams/client generate` (or `scripts/sdk/gen.sh typescript`).
3. `pnpm --filter @loams/client typecheck && pnpm --filter @loams/client test`.

CI regenerates and fails on a diff, and `crates/loams-facade-gen`'s
`golden_typescript` compares the committed facade against the generator's
output over the real `proto/` tree.

## Tests

```
pnpm --filter @loams/client test                              # the recorded corpus
LOAMS_TEST_ENDPOINT=http://127.0.0.1:8080 pnpm --filter @loams/client test   # a live server
sdks/conformance/run.sh typescript                            # starts a server and does both
```

Without `LOAMS_TEST_ENDPOINT` the suite replays `sdks/fixtures/recorded`, which
was captured from a real `loams dev` by `sdks/conformance/record-fixtures.mjs`.
That is deliberate: CI cannot boot a Rust server for each of the thirteen SDKs
on every change. `run.sh` runs the same suite against a live server, which is
what catches a corpus that has drifted from the API.

The suite covers the three things the design asks of a conforming SDK: a
successful call in every encoding a client might pick, a structured-reason
error, and the unavailable-service path in all three of its shapes (the guard,
the unary refusal, and the refusal on a stream). See
`docs/sdk/runtime-contract.md` for the clauses and `src/runtime/` for the code.
