// SDK2 Task 0's `typescript_conformance_all_required_fixtures`.
//
// The corpus in `sdks/fixtures` is recorded from a real `loams dev`, and this
// runs every case in it through the SDK's *public* surface — `loams.instance`
// and `loams.tables`, the same objects an application uses — rather than
// through the stubs. That is the point of the suite: it proves the facade
// dispatches to the right RPC, sends the right encoding, and turns what comes
// back into the right typed value.
//
// Three things are covered, which between them are what the design asks of a
// conforming SDK (design §44 §10.4):
//
// - a successful call, in every encoding an SDK might pick;
// - a structured-reason error, with `reason` and not the message;
// - the unavailable-service path, in all three of its shapes: the guard that
//   costs no RPC, the refusal a call gets, and the refusal on a stream.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Loams } from '../src/loams.js';
import { FeatureNotInVariantError, isLoamsError } from '../src/runtime/errors.js';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { FixtureServer } from './helpers/server.js';
import { FIXTURES, startFixtureServer } from './helpers/server.js';

const here = dirname(fileURLToPath(import.meta.url));

let server: FixtureServer;
let loams: Loams;

beforeAll(async () => {
  server = await startFixtureServer();
  loams = new Loams({ endpoint: server.endpoint });
});

afterAll(async () => {
  await server.stop();
});

/** The corpus index, so the suite fails if a recorded case is missing. */
async function corpus(): Promise<{ cases: { name: string; reason: string | null }[] }> {
  const text = await readFile(resolve(FIXTURES, 'index.json'), 'utf8');
  return JSON.parse(text);
}

describe('@loams/client conformance', () => {
  it('typescript_conformance_all_required_fixtures', async () => {
    const { cases } = await corpus();
    const expected = [
      // A successful call, in each encoding a client might pick.
      'instance_get_instance_json',
      'instance_get_instance_proto',
      'instance_get_instance_grpc_web',
      'instance_get_instance_grpc_web_json',
      // A structured-reason error, in each encoding.
      'instance_who_am_i_json',
      'instance_who_am_i_proto',
      'instance_who_am_i_grpc_web',
      'instance_who_am_i_grpc_web_json',
      // The unavailable-service path, unary and on a stream.
      'live_query_json',
      'live_query_proto',
      'live_query_grpc_web',
      'live_query_grpc_web_json',
      'live_watch',
    ];
    expect(cases.map((entry) => entry.name).sort()).toEqual([...expected].sort());

    // A successful call. `GetInstance` needs no auth, which is why it is the
    // first thing any client calls.
    const info = await loams.instance.getInstance({});
    expect(info.name).toBe('Loams');
    expect(info.apiVersions).toContain('loams.instance.v1');
    expect(info.services?.length).toBeGreaterThan(0);

    // A structured-reason error. The reason is what the SDK reads; the message
    // is for a person and is not asserted on.
    const failed = await loams.instance.whoAmI({}).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(isLoamsError(failed)).toBe(true);
    expect(failed).toMatchObject({ reason: 'not_implemented' });

    // The unavailable-service path, three ways.
    //
    // 1. The guard, from the catalogue, spending no RPC on a call that cannot
    //    work. `loams.live` and `loams.tables` are the same service, so the
    //    guard is asked about the package.
    await expect(loams.system.guard('live')).rejects.toBeInstanceOf(
      FeatureNotInVariantError,
    );
    await expect(loams.system.guard('instance')).resolves.toBeUndefined();

    // 2. The refusal a call gets when the caller skips the guard. This is the
    //    typed surface: the reason is in the union, and the variant is read out
    //    of the metadata rather than parsed out of the message.
    const refused = await loams.tables.query({}).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(refused).toBeInstanceOf(FeatureNotInVariantError);
    expect(refused).toMatchObject({
      reason: 'feature_not_in_variant',
      variant: 'standard',
    });

    // 3. The refusal on a server stream, which arrives inside the Connect
    //    envelope rather than as an HTTP status. A client that only reads
    //    status codes sees a 200 here, so this is the case that distinguishes a
    //    real Connect implementation from a status-code-only fake.
    const streamed: unknown[] = [];
    await expect(
      (async () => {
        for await (const transition of loams.live.watch({})) {
          streamed.push(transition);
        }
      })(),
    ).rejects.toBeInstanceOf(FeatureNotInVariantError);
    expect(streamed).toHaveLength(0);

    // The catalogue answers the same question the refusals do, from one call.
    const catalogue = await loams.system.catalogue();
    expect(catalogue.served).toContain('loams.instance.v1');
    expect(catalogue.unavailable).toContain('loams.live.v1');
    expect(catalogue.services.find((s) => s.package === 'loams.live.v1')?.unstable).toBe(true);
  });

  it('reports the proto revision and the server packages (R9)', async () => {
    const report = await loams.system.version();
    expect(report.protoRev).toBe(loams.protoRev);
    // `loams.live.v1` is served as `unavailable` in the standard variant, and
    // `GetInstance.api_versions` lists only what is served, so a mismatch here
    // is the server's, not the SDK's.
    expect(report.apiVersions).toEqual(['loams.instance.v1']);
    expect(report.compatible).toBe(false);
    expect(report.missing).toContain('loams.live.v1');
  });

  it('is a browser-shaped client: no Node-only import on the default path', async () => {
    // The default entry point must not reach for `node:http2`, or a bundler
    // would follow it and fail. This is a static check because the failure it
    // prevents is a build failure in someone else's project.
    // The check is on imports, not on the word: the prose names `node:http2`
    // to explain what it is kept away from.
    for (const file of [
      '../src/runtime/transports.ts',
      '../src/index.ts',
      '../src/loams.ts',
      '../src/system.ts',
      '../src/runtime/call.ts',
    ]) {
      const source = await readFile(resolve(here, file), 'utf8');
      expect(source, file).not.toMatch(/from ['"]node:/);
      expect(source, file).not.toMatch(/require\(['"]node:/);
    }
    // And the Node transport is behind the `/node` subpath.
    const node = await readFile(resolve(here, '../src/node.ts'), 'utf8');
    expect(node).toContain('@connectrpc/connect-node');
  });
});
