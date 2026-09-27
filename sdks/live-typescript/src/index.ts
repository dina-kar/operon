// @operon/live: the Loam Live TypeScript client (R1 plan Task 14).
export {
  DEPLOY_ATTEMPTS,
  IDLE_TIMEOUT_MS,
  LiveClient,
  type LiveClientOptions,
  type Mutated,
  type MutateOptions,
  RECONNECT_MAX_MS,
  RECONNECT_MIN_MS,
  SESSION_HEADER,
  type Subscription,
  type TableInput,
} from "./client.js";
export { LiveError, type LiveErrorCode, toLiveError } from "./errors.js";
export * as pb from "./gen/loam/live/v1/live_pb.js";
export { LiveService } from "./gen/loam/live/v1/live_pb.js";
export type { LocalStore, OptimisticUpdate } from "./optimistic.js";
export { OptimisticLayer } from "./optimistic.js";
export {
  type Applied,
  type QueryDef,
  type QueryResult,
  QuerySetState,
  SessionState,
  type Version,
  VersionGap,
  versionOf,
  ZERO,
} from "./session.js";
export { canonical, fromValue, type LiveValue, MAX_DEPTH, toValue } from "./values.js";
