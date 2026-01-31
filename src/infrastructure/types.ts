import type { AnyEither, ExecuteOpts, ICommand, IMeta, ISerializedEvent, ITransactionalScope, StringEither } from "../types.js";

export interface IScheduleOptions extends ExecuteOpts {
  executeSync?: boolean;
}

export interface IEventScheduler {
  scheduleCommand<TPayload extends Record<string, any>, TRes extends AnyEither>(
    command: ICommand<TPayload, TRes>,
    executeAt: Date,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ): Promise<string>;

  updateScheduledEventStatus(command: ICommand, status: "CREATED" | "FAILED" | "PROCESSED"): Promise<void>;

  reset(): Promise<number>;
}

export type EventTypes = "COMMAND" | "QUERY" | "EVENT";

export type EventStatus = "CREATED" | "PROCESSING" | "FAILED" | "PROCESSED" | "ABORTED";

/** Backoff strategy for retry delays */
export type BackoffMode = "IMMEDIATE" | "UNIFORM" | "EXPONENTIAL";

/** Configuration for retry behavior */
export interface IRetryConfig {
  /** Maximum number of retries before marking as ABORTED (default: 5) */
  maxRetries: number;
  /** Backoff strategy for calculating retry delays */
  backoffMode: BackoffMode;
  /** Base delay in milliseconds for backoff calculations (default: 1000) */
  baseDelayMs: number;
  /** Maximum delay cap in milliseconds (default: 300000 = 5 minutes) */
  maxDelayMs: number;
  /** Multiplier for exponential backoff (default: 2, only used with EXPONENTIAL mode) */
  multiplier: number;
  /** Jitter factor (0-1) to add randomness to delays (default: 0.1) */
  jitterFactor: number;
}

/** Configuration for the polling worker */
export interface IWorkerConfig {
  /** Unique identifier for this worker instance */
  workerId: string;
  /** Polling interval in milliseconds (default: 1000) */
  pollIntervalMs: number;
  /** Batch size for polling (default: 10) */
  batchSize: number;
  /** Lock timeout in milliseconds before stale locks are released (default: 30000) */
  lockTimeoutMs: number;
  /** Retry configuration */
  retry: IRetryConfig;
}

/** Configuration for rate limiting */
export interface IRateLimitConfig {
  /** Maximum operations per second (primary rate limit) */
  maxPerSecond?: number;
  /** Maximum concurrent operations (optional, secondary limit) */
  maxConcurrent?: number;
  /** Key prefix for PostgreSQL rate limiter storage */
  keyPrefix?: string;

  type: 'pg' | 'in-memory'
}

export interface IPersistedEvent<TEventPayload = any, TMeta extends IMeta = IMeta> {
  eventId: string;
  eventName: string;
  streamId: string;
  event: ISerializedEvent<TEventPayload>;
  timestamp: Date;
  status: EventStatus;
  type: EventTypes;
  meta?: TMeta;
  /** Number of retry attempts (default: 0) */
  retryCount?: number;
  /** Timestamp when the event was locked for processing */
  lockedAt?: Date | null;
  /** Worker ID that holds the lock */
  lockedBy?: string | null;
  /** Timestamp when the event is eligible for next retry */
  nextRetryAt?: Date;
}

/** Result of a poll operation */
export interface IPollResult {
  /** Events claimed by this poll */
  claimed: IPersistedEvent[];
  /** Whether there may be more events to poll */
  hasMore: boolean;
}

export interface IEventStore {
  insert(
    event: IPersistedEvent,
    opts?: { allowUpsert?: boolean; scope?: ITransactionalScope },
  ): Promise<void>;
  updateByEventId(
    eventId: string,
    event: Partial<IPersistedEvent>,
    opts?: { scope?: ITransactionalScope },
  ): Promise<void>;
  findUnprocessedCommands<TKeys extends keyof IPersistedEvent>(
    ignoredEventIds?: string[],
    fields?: TKeys[],
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
  findByEventIds<TKeys extends keyof IPersistedEvent>(
    eventIds: string[],
    fields?: TKeys[],
    type?: EventTypes,
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
  find(query: Partial<IPersistedEvent>): Promise<IPersistedEvent[]>;
  findByStreamIds<TKeys extends keyof IPersistedEvent>(
    streamIds: string[],
    fields?: TKeys[],
    type?: EventTypes,
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
}
