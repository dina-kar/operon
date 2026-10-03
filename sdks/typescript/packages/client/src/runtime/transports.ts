// Transports (design §44 §4, D612).
//
// One port serves the Connect protocol, gRPC and gRPC-Web (D600), so the only
// question a client has is which of those it can speak:
//
// - **Browsers** cannot do HTTP/2 gRPC. They speak gRPC-Web and Connect, both
//   over `fetch`, which is what `@connectrpc/connect-web` implements. Because
//   the API serves gRPC-Web on the same port as everything else, the same
//   client code runs in a browser with no proxy and no sidecar.
// - **Node, Deno and Bun** can do HTTP/2 gRPC. `@connectrpc/connect-node`
//   implements it over `node:http2`, and the server's plaintext listener means
//   no ALPN to negotiate — the client asks for HTTP/2 with prior knowledge.
//
// The default is the `fetch` transport, deliberately: it is the one case where
// the *same* code path works everywhere, so it is what an application gets
// unless it asks for the Node one through `@loams/client/node`. Choosing the
// HTTP/2 transport in a browser bundle would be a build error, which is the
// right failure.

import { createConnectTransport } from '@connectrpc/connect-web';
import type { Transport } from '@connectrpc/connect';

/** What a caller may configure on the transport. */
export interface TransportOptions {
  /**
   * The instance's base URL. `Loams` sets this per call from its own
   * `endpoint`, so a caller building a transport by hand rarely needs it.
   */
  baseUrl?: string;
  /** Extra headers on every request, for a gateway or a proxy. */
  headers?: Record<string, string>;
  /**
   * `useBinaryFormat` picks the encoding: `true` is protobuf, `false` is the
   * proto3 JSON mapping. Default `true`. JSON is what `curl` sends
   * (design §44 §4) and what a proxy that logs bodies can read; binary is
   * smaller and does not need a JSON codec on the wire.
   */
  useBinaryFormat?: boolean;
  /**
   * The deadline for a call that does not set its own, passed to the
   * transport as `defaultTimeoutMs`. A per-call `timeoutMs` still wins.
   */
  defaultTimeoutMs?: number;
  /**
   * CORS preflights for the browser: custom headers and the credential mode.
   * A browser sending `Authorization` needs the server's CORS policy to allow
   * it; this only tells the transport what the request will look like.
   */
  interceptors?: Parameters<typeof createConnectTransport>[0]['interceptors'];
}

/**
 * The default transport: Connect over `fetch`.
 *
 * Works in a browser, in Node 18+ (which has `fetch`), in Deno and in Bun, and
 * speaks both the Connect unary protocol and gRPC-Web, which is what the server
 * serves on the same port (D600). This is the browser path as a first-class
 * case, not a fallback: it is the default in every runtime.
 */
export function createLoamsTransport(options: TransportOptions = {}): Transport {
  return createConnectTransport({
    baseUrl: options.baseUrl ?? '',
    useBinaryFormat: options.useBinaryFormat ?? true,
    ...(options.headers === undefined ? {} : { headers: options.headers }),
    ...(options.defaultTimeoutMs === undefined
      ? {}
      : { defaultTimeoutMs: options.defaultTimeoutMs }),
    ...(options.interceptors === undefined ? {} : { interceptors: options.interceptors }),
  });
}
