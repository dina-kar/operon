// `@loams/client/node`: the Node, Deno and Bun entry point.
//
//   import { Loams, createNodeTransport } from '@loams/client/node';
//
//   const loams = new Loams({ endpoint, transport: createNodeTransport() });
//
// The difference from the default entry point is the transport, and only the
// transport: it lives here rather than in `@loams/client` because
// `@connectrpc/connect-node` reaches for `node:http2`, which a browser bundler
// would follow into a build it cannot satisfy. Keeping it in a subpath means
// the main entry stays browser-safe by construction rather than by a
// conditional import that a bundler has to resolve correctly.
//
// Which to use. One port serves the Connect protocol, gRPC and gRPC-Web
// (design §44 §4, D600), so both entries reach the same server with the same
// code. The `fetch` transport is HTTP/1.1 Connect or gRPC-Web and works
// everywhere, including through a proxy that only speaks HTTP/1.1; the Node
// transport is HTTP/2 gRPC, which is fewer round trips and multiplexes
// connections, at the cost of requiring an HTTP/2-capable path all the way
// down. Prefer this one on a loopback stack or inside the cluster; prefer the
// default one in a browser, an edge function, or behind an HTTP/1.1 proxy.

import { createGrpcTransport } from '@connectrpc/connect-node';
import type { Transport } from '@connectrpc/connect';
import type { TransportOptions } from './runtime/transports.js';

export * from './index.js';

/**
 * HTTP/2 gRPC, over `node:http2`.
 *
 * The server's loopback listener is plaintext, so there is no ALPN to
 * negotiate and the client uses HTTP/2 with prior knowledge. Against a TLS
 * endpoint this is ordinary gRPC: HTTP/2, the same credentials, the same
 * retries.
 */
export function createNodeTransport(options: TransportOptions = {}): Transport {
  return createGrpcTransport({
    baseUrl: options.baseUrl ?? '',
    useBinaryFormat: options.useBinaryFormat ?? true,
    ...(options.headers === undefined ? {} : { headers: options.headers }),
    ...(options.defaultTimeoutMs === undefined
      ? {}
      : { defaultTimeoutMs: options.defaultTimeoutMs }),
    ...(options.interceptors === undefined ? {} : { interceptors: options.interceptors }),
  });
}
