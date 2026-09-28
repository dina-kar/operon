/** `@operon/client`: a zero-dependency client for the Operon native REST API. */
export { Namespace, OperonClient } from "./client.js";
export {
  AlreadyExistsError,
  ConflictError,
  InternalError,
  InvalidArgumentError,
  NotFoundError,
  OffsetOutOfRangeError,
  OperonError,
  OperonTimeoutError,
  ResourceExhaustedError,
  SchemaViolationError,
  TransportError,
  UnavailableError,
} from "./errors.js";
export { ConsistencyToken, type TokenItem } from "./token.js";
export type {
  ClientOptions,
  Consistency,
  FetchedRecord,
  FetchResult,
  Id,
  PartitionInfo,
  ProduceRecord,
  ProduceResult,
  RequestOptions,
  StreamInfo,
} from "./types.js";

export const VERSION = "0.0.1";
