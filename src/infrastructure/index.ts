// Types
export type {
  EventStatus,
  EventTypes,
  BackoffMode,
  IRetryConfig,
  IWorkerConfig,
  IRateLimitConfig,
  IPollResult,
  IPersistedEvent,
  IEventStore,
  IEventScheduler,
  IScheduleOptions,
} from "./types.js";

// Event Store
export { PersistentEventStore } from "./PersistentEventStore.js";
export { createEventStore } from "./createEventStore.js";

// Event Scheduler
export { PersistentEventScheduler } from "./PersistentEventScheduler.js";
export { createEventScheduler } from "./createEventScheduler.js";

// Polling Worker
export { PollingWorker, createPollingWorker } from "./PollingWorker.js";
export type { EventProcessor, CompletionCallback } from "./PollingWorker.js";

// Utilities
export {
  calculateBackoffDelay,
  calculateNextRetryAt,
  shouldAbort,
  mergeRetryConfig,
  DEFAULT_RETRY_CONFIG,
} from "./utils/backoff.js";

export {
  createRateLimiter,
  mergeRateLimitConfig,
  RateLimitExceededError,
  DEFAULT_RATE_LIMIT_CONFIG,
} from "./utils/rateLimiter.js";
export type { IRateLimiter } from "./utils/rateLimiter.js";

export {
  StreamController,
  createStreamController,
} from "./utils/StreamController.js";
export type { IStreamEvent } from "./utils/StreamController.js";
