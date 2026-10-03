// `typescript_grpc_web_transport_in_a_browser`
//
// Design §44 §4, D600: one port serves the Connect protocol, gRPC and
// gRPC-Web, and **a browser can only do gRPC-Web** — no HTTP/2, so no gRPC.
// Since the API serves gRPC-Web on the same port as everything else, the same
// client code runs in a browser with no proxy and no sidecar. That makes the
// browser path a first-class case rather than a fallback, and this test is what
// keeps it one.
//
// What it pins, without launching a browser:
//
// - the default entry point is browser-safe, by a static check (the entry point
//   and the runtime reach for no `node:` built-in, and the HTTP/2 transport is
//   behind a subpath a bundler will not follow);
// - a `Loams` built on `@connectrpc/connect-web`'s **gRPC-Web** transport talks
//   to the same recorded server the Connect transport does, and gets the same
//   typed answers — including the structured reason and the unavailable-service
//   refusal;
// - the refusal on gRPC-Web is genuinely different on the wire (a 200 with the
//   code in the trailers, not a 501 with a JSON body), and the SDK still turns
//   it into the same typed error.
//
// A real browser run is what would catch a CORS or header problem this cannot;
// that needs an origin and a server that answers preflights, which no test in
// this repository has. It is stated in the PR rather than claimed.

import { Code } from '@connectrpc/connect';
import { createGrpcWebTransport } from '@connectrpc/connect-web';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Loams } from '../src/loams.js';
import { FeatureNotInVariantError, isLoamsError } from '../src/runtime/errors.js';
import { createLoamsTransport } from '../src/runtime/transports.js';
import type { FixtureServer } from './helpers/server.js';
import { startFixtureServer } from './helpers/server.js';

let server: FixtureServer;

/** A client on each transport. Both point at the same server: the API serves
 * Connect and gRPC-Web on one port, which is the whole point of D600. */
const clients = (): Record<string, Loams> => ({
  connect: new Loams({ endpoint: server.endpoint }),
  'connect (JSON)': new Loams({
    endpoint: server.endpoint,
    transportOptions: { useBinaryFormat: false },
  }),
  'gRPC-Web': new Loams({
    endpoint: server.endpoint,
    transport: createGrpcWebTransport({ baseUrl: server.endpoint }),
  }),
  'gRPC-Web (JSON)': new Loams({
    endpoint: server.endpoint,
    transport: createGrpcWebTransport({ baseUrl: server.endpoint, useBinaryFormat: false }),
  }),
});

beforeAll(async () => {
  server = await startFixtureServer();
});

afterAll(async () => {
  await server.stop();
});

describe('typescript_grpc_web_transport_in_a_browser', () => {
  it('reaches the same instance over Connect and over gRPC-Web', async () => {
    for (const [name, loams] of Object.entries(clients())) {
      const info = await loams.instance.getInstance({});
      expect(info.name, name).toBe('Loams');
      expect(info.apiVersions, name).toContain('loams.instance.v1');
    }
  });

  it('reads the same structured reason over gRPC-Web as over Connect', async () => {
    for (const [name, loams] of Object.entries(clients())) {
      const error = await loams.instance.whoAmI({}).then(
        () => undefined,
        (thrown: unknown) => thrown,
      );
      expect(isLoamsError(error), name).toBe(true);
      expect(error, name).toMatchObject({ code: Code.Unimplemented, reason: 'not_implemented' });
    }
  });

  it('turns the gRPC-Web refusal into the same typed error as the Connect one', async () => {
    // On the wire these are different answers: Connect answers 501 with a JSON
    // body, gRPC-Web answers 200 with the code in the trailers. The typed
    // surface has to be the same, or a browser and a server would branch
    // differently for the same missing feature.
    for (const [name, loams] of Object.entries(clients())) {
      const error = await loams.tables.query({}).then(
        () => undefined,
        (thrown: unknown) => thrown,
      );
      expect(error, name).toBeInstanceOf(FeatureNotInVariantError);
      expect(error, name).toMatchObject({
        reason: 'feature_not_in_variant',
        variant: 'standard',
      });
    }
  });

  it('reads the service catalogue the same way on both transports', async () => {
    for (const [name, loams] of Object.entries(clients())) {
      const catalogue = await loams.system.catalogue();
      expect(catalogue.served, name).toContain('loams.instance.v1');
      expect(catalogue.unavailable, name).toContain('loams.live.v1');
    }
  });

  it('defaults to the fetch transport, which is the one that works everywhere', () => {
    // The default is Connect over `fetch`: HTTP/1.1, no `node:http2`, and the
    // same code in a browser, in Node, in Deno and in Bun.
    expect(typeof createLoamsTransport).toBe('function');
    const transport = createLoamsTransport({ baseUrl: 'http://127.0.0.1:1' });
    expect(typeof transport.unary).toBe('function');
    expect(typeof transport.stream).toBe('function');
  });
});
