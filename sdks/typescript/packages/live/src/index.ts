// @loams/live: the generated `loams.live.v1` messages and the `LiveService`
// descriptor that `@connectrpc/connect` turns into a client (R1 plan Task 7).
//
// Everything under ./gen is written by `buf generate` from
// `sdks/typescript/buf.gen.live.yaml` and committed; CI regenerates and fails
// on a diff. The package is folded into `sdks/typescript/` (design §44 §10.2)
// and `@loams/client` imports its types through the `@loams/live/live` path,
// which is why the service's package appears in the SDK's facade bindings.
//
// The live package is `unstable` (design §44 §10.3): its wire contract may
// still change, `buf breaking` skips it, and an SDK marks the modules built on
// it experimental.

export * from './gen/loams/live/v1/live_pb.js';
export * from './gen/loams/live/v1/value_pb.js';

/** The proto package this module generates, as `GetInstance.api_versions` names it. */
export const LIVE_API_PACKAGE = 'loams.live.v1';
