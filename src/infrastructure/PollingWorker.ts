import type { Pool, PoolClient } from "pg";
import type { ITransactionalScope, Logger } from "../types.js";
import { v4 as uuid } from "uuid";

import type {
  EventStatus,
  EventTypes,
  IPersistedEvent,
  IPollResult,
  IWorkerConfig,
  IRateLimitConfig,
} from "./types.js";
import {
  calculateNextRetryAt,
  DEFAULT_RETRY_CONFIG,
  shouldAbort,
} from "./utils/backoff.js";
import {
  createRateLimiter,
  type IRateLimiter,
  RateLimitExceededError,
} from "./utils/rateLimiter.js";

/** Default worker configuration */
export const DEFAULT_WORKER_CONFIG: IWorkerConfig = {
  workerId: "",
  pollIntervalMs: 1000,
  batchSize: 10,
  lockTimeoutMs: 30000,
  retry: DEFAULT_RETRY_CONFIG,
};

/** Callback for processing a claimed event */
export type EventProcessor = (
  event: IPersistedEvent,
  client: PoolClient,
) => Promise<void>;

/** Callback for handling event completion (success or failure) */
export type CompletionCallback = (
  event: IPersistedEvent,
  status: EventStatus,
  error?: Error,
) => void;

/**
 * PollingWorker handles the core polling mechanism for processing events
 * from the outbox table using FOR UPDATE SKIP LOCKED pattern.
 *
 * Features:
 * - Distributed job claiming with FOR UPDATE SKIP LOCKED
 * - PostgreSQL-backed rate limiting via rate-limiter-flexible
 * - Exponential backoff for retries
 * - Stale lock cleanup
 * - LISTEN/NOTIFY support for executeSync
 */

export class PollingWorker {
  private workerId: string;
  private config: IWorkerConfig;
  private rateLimiter?: IRateLimiter;
  private pollingTimer?: ReturnType<typeof setInterval>;
  private staleLockTimer?: ReturnType<typeof setInterval>;
  private isRunning = false;
  private notifyClient?: PoolClient;
  private notifyListeners = new Map<string, (status: EventStatus) => void>();

  constructor(
    private pool: Pool,
    private logger: Logger,
    private eventType: EventTypes,
    config?: Partial<IWorkerConfig>,
    private rateLimitConfig?: IRateLimitConfig,
  ) {

    this.workerId = config?.workerId || `worker-${uuid()}`;
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...config,
      workerId: this.workerId,
      retry: {
        ...DEFAULT_RETRY_CONFIG,
        ...config?.retry,
      },
    };
  }

  /**
   * Initialize the worker (rate limiter, LISTEN/NOTIFY).
   */
  async initialize(): Promise<void> {
    // Initialize rate limiter if configured
    if (this.rateLimitConfig?.maxPerSecond) {
      this.rateLimiter = await createRateLimiter(this.pool, this.rateLimitConfig);
    }

    // Setup LISTEN/NOTIFY for executeSync
    await this.setupNotifyListener();
  }

  /**
   * Start the polling worker.
   *
   * @param processor - Callback to process each claimed event
   * @param onCompletion - Optional callback when an event completes
   */
  async start(processor: EventProcessor, onCompletion?: CompletionCallback): Promise<void> {
    if (this.isRunning) {
      return;
    }
    this.isRunning = true;

    this.logger.info(
      { workerId: this.workerId, eventType: this.eventType },
      "Starting polling worker",
    );

    // Start polling loop
    this.pollingTimer = setInterval(async () => {
      if (!this.isRunning) return;
      await this.pollAndProcess(processor, onCompletion);
    }, this.config.pollIntervalMs);

    // Start stale lock cleanup (every 30 seconds)
    this.staleLockTimer = setInterval(async () => {
      if (!this.isRunning) return;
      await this.cleanupStaleLocks();
    }, 30000);

    // Initial poll
    await this.pollAndProcess(processor, onCompletion);
  }

  /**
   * Stop the polling worker gracefully.
   */
  async stop(): Promise<void> {
    this.isRunning = false;

    if (this.pollingTimer) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = undefined;
    }

    if (this.staleLockTimer) {
      clearInterval(this.staleLockTimer);
      this.staleLockTimer = undefined;
    }

    // Close NOTIFY listener
    if (this.notifyClient) {
      try {
        await this.notifyClient.query("UNLISTEN cqrs_event_status");
      } catch {
        // Ignore errors during cleanup
      }
      this.notifyClient = undefined;
    }

    this.logger.info({ workerId: this.workerId }, "Polling worker stopped");
  }

  /**
   * Wait for a specific event to complete (for executeSync).
   *
   * @param eventId - The event ID to wait for
   * @param timeoutMs - Maximum time to wait (0 = no timeout)
   * @returns The final status of the event
   */
  async waitForCompletion(eventId: string, timeoutMs: number = 0): Promise<EventStatus> {
    return new Promise((resolve, reject) => {
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined;

      const cleanup = () => {
        this.notifyListeners.delete(eventId);
        if (timeoutHandle) {
          clearTimeout(timeoutHandle);
        }
      };

      // Register listener
      this.notifyListeners.set(eventId, (status) => {
        cleanup();
        resolve(status);
      });

      // Setup timeout
      if (timeoutMs > 0) {
        timeoutHandle = setTimeout(() => {
          cleanup();
          reject(new Error(`Timeout waiting for event ${eventId}`));
        }, timeoutMs);
      }

      // Check if already completed (race condition protection)
      this.checkEventStatus(eventId).then((status) => {
        if (status && ["PROCESSED", "FAILED", "ABORTED"].includes(status)) {
          cleanup();
          resolve(status);
        }
      });
    });
  }

  /**
   * Wait for multiple events to complete.
   *
   * @param eventIds - Array of event IDs to wait for
   * @param timeoutMs - Maximum time to wait (0 = no timeout)
   * @returns Map of event IDs to their final statuses
   */
  async waitForCompletionBatch(
    eventIds: string[],
    timeoutMs: number = 0,
  ): Promise<Map<string, EventStatus>> {
    const results = new Map<string, EventStatus>();
    const promises = eventIds.map(async (eventId) => {
      const status = await this.waitForCompletion(eventId, timeoutMs);
      results.set(eventId, status);
    });

    await Promise.all(promises);
    return results;
  }

  /**
   * Poll for pending events and process them.
   */
  private async pollAndProcess(
    processor: EventProcessor,
    onCompletion?: CompletionCallback,
  ): Promise<void> {
    this.logger.debug({ workerId: this.workerId, eventType: this.eventType }, "Poll cycle starting");
    try {
      // Poll for FAILED events first (higher priority)
      const failedResult = await this.poll("FAILED");
      this.logger.debug({ workerId: this.workerId, claimedFailed: failedResult.claimed.length }, "Polled FAILED events");
      await this.processBatch(failedResult.claimed, processor, onCompletion);

      // Then poll for CREATED events
      const createdResult = await this.poll("CREATED");
      this.logger.debug({ workerId: this.workerId, claimedCreated: createdResult.claimed.length }, "Polled CREATED events");
      await this.processBatch(createdResult.claimed, processor, onCompletion);
    } catch (error) {
      this.logger.error({ error, workerId: this.workerId }, "Error during polling");
    }
    this.logger.debug({ workerId: this.workerId, eventType: this.eventType }, "Poll cycle complete");
  }

  /**
   * Poll for events with a specific status.
   *
   * @param status - The status to poll for (CREATED or FAILED)
   * @returns Poll result with claimed events
   */
  private async poll(status: "CREATED" | "FAILED"): Promise<IPollResult> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      // Use FOR UPDATE SKIP LOCKED to claim events without blocking
      const result = await client.query(
        `
        UPDATE events
        SET
          status = 'PROCESSING',
          locked_at = NOW(),
          locked_by = $1
        WHERE event_id IN (
          SELECT event_id FROM events
          WHERE status = $2
            AND type = $3
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
          ORDER BY
            CASE WHEN status = 'FAILED' THEN 0 ELSE 1 END,
            next_retry_at ASC,
            timestamp ASC
          LIMIT $4
          FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        `,
        [this.workerId, status, this.eventType, this.config.batchSize],
      );

      await client.query("COMMIT");

      const claimed = result.rows.map(this.mapRowToEvent);
      return {
        claimed,
        hasMore: claimed.length === this.config.batchSize,
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Process a batch of claimed events.
   */
  private async processBatch(
    events: IPersistedEvent[],
    processor: EventProcessor,
    onCompletion?: CompletionCallback,
  ): Promise<void> {
    for (const event of events) {
      await this.processEvent(event, processor, onCompletion);
    }
  }

  /**
   * Process a single event with rate limiting and error handling.
   */
  private async processEvent(
    event: IPersistedEvent,
    processor: EventProcessor,
    onCompletion?: CompletionCallback,
  ): Promise<void> {
    this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - starting");
    const rateLimitKey = `${this.eventType}:${event.eventName}`;

    // Apply rate limiting
    if (this.rateLimiter) {
      try {
        await this.rateLimiter.acquire(rateLimitKey);
      } catch (error) {
        if (error instanceof RateLimitExceededError) {
          // Release the lock and let another worker pick it up later
          await this.releaseLock(event.eventId);
          return;
        }
        throw error;
      }
    }

    this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - starting transaction");
    const client  = await this.pool.connect()
    try {
      await client.query('BEGIN')
      // Use adapter.transaction to avoid conflicts with UowDecorator's transaction management
      // This wraps the entire processing in a single transaction
        this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - in transaction, calling processor");

        // Process the event (handler may update event meta within this transaction)
        await processor(event, client);
        this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - processor done, marking processed");

        // Mark as processed within the same transaction
        await this.markProcessed(event.eventId, client);
        this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - marked processed");
      this.logger.debug({ workerId: this.workerId, eventId: event.eventId }, "Processing event - transaction committed");
      onCompletion?.(event, "PROCESSED");
      await client.query("COMMIT");
    } catch (error) {
      this.logger.error({ workerId: this.workerId, eventId: event.eventId, error }, "Processing event - error");
      await client.query("ROLLBACK");
      await this.handleProcessingError(event, error as Error, onCompletion);
    }  finally {
      // Release rate limiter slot
      if (this.rateLimiter) {
        await this.rateLimiter.release(rateLimitKey);
      }
      client.release()
    }
  }

  /**
   * Handle a processing error (retry or abort).
   */
  private async handleProcessingError(
    event: IPersistedEvent,
    error: Error,
    onCompletion?: CompletionCallback,
  ): Promise<void> {
    const retryCount = (event.retryCount ?? 0) + 1;

    if (shouldAbort(retryCount, this.config.retry.maxRetries)) {
      // Exceeded max retries - mark as aborted
      await this.markAborted(event.eventId, retryCount, error);
      this.logger.error(
        { eventId: event.eventId, retryCount, error },
        "Event aborted after max retries",
      );
      onCompletion?.(event, "ABORTED", error);
    } else {
      // Schedule retry with backoff
      const nextRetryAt = calculateNextRetryAt(retryCount, this.config.retry);
      await this.markFailed(event.eventId, retryCount, nextRetryAt, error);
      this.logger.warn(
        { eventId: event.eventId, retryCount, nextRetryAt, error },
        "Event failed, scheduled for retry",
      );
      onCompletion?.(event, "FAILED", error);
    }
  }

  /**
   * Mark an event as processed.
   */
  private async markProcessed(eventId: string, client?: ITransactionalScope): Promise<void> {
    const db = client || this.pool;
    await db.query(
      `
      UPDATE events
      SET status = 'PROCESSED', locked_at = NULL, locked_by = NULL
      WHERE event_id = $1
      `,
      [eventId],
    );
  }

  /**
   * Mark an event as failed with retry scheduling.
   */
  private async markFailed(
    eventId: string,
    retryCount: number,
    nextRetryAt: Date,
    error: Error,
  ): Promise<void> {
    await this.pool.query(
      `
      UPDATE events
      SET
        status = 'FAILED',
        retry_count = $2,
        next_retry_at = $3,
        locked_at = NULL,
        locked_by = NULL,
        meta = jsonb_set(COALESCE(meta, '{}'), '{error}', $4::jsonb)
      WHERE event_id = $1
      `,
      [
        eventId,
        retryCount,
        nextRetryAt,
        JSON.stringify({ message: error.message, stack: error.stack }),
      ],
    );
  }

  /**
   * Mark an event as aborted (max retries exceeded).
   */
  private async markAborted(eventId: string, retryCount: number, error: Error): Promise<void> {
    await this.pool.query(
      `
      UPDATE events
      SET
        status = 'ABORTED',
        retry_count = $2,
        locked_at = NULL,
        locked_by = NULL,
        meta = jsonb_set(COALESCE(meta, '{}'), '{error}', $3::jsonb)
      WHERE event_id = $1
      `,
      [
        eventId,
        retryCount,
        JSON.stringify({ message: error.message, stack: error.stack }),
      ],
    );
  }

  /**
   * Release a lock (e.g., when rate limited).
   */
  private async releaseLock(eventId: string): Promise<void> {
    await this.pool.query(
      `
      UPDATE events
      SET status = 'CREATED', locked_at = NULL, locked_by = NULL
      WHERE event_id = $1 AND locked_by = $2
      `,
      [eventId, this.workerId],
    );
  }

  /**
   * Cleanup stale locks from dead workers.
   */
  private async cleanupStaleLocks(): Promise<void> {
    const result = await this.pool.query(
      `
      UPDATE events
      SET
        status = 'FAILED',
        locked_at = NULL,
        locked_by = NULL,
        retry_count = COALESCE(retry_count, 0) + 1,
        next_retry_at = NOW()
      WHERE locked_at IS NOT NULL
        AND locked_at < NOW() - INTERVAL '${this.config.lockTimeoutMs} milliseconds'
        AND status = 'PROCESSING'
      RETURNING event_id
      `,
    );

    if (result.rowCount && result.rowCount > 0) {
      this.logger.warn(
        { count: result.rowCount, eventIds: result.rows.map((r) => r.event_id) },
        "Cleaned up stale locks",
      );
    }
  }

  /**
   * Setup LISTEN/NOTIFY for executeSync.
   */
  private async setupNotifyListener(): Promise<void> {
    this.notifyClient = await this.pool.connect();

    this.notifyClient.on("notification", (msg) => {
      if (msg.channel === "cqrs_event_status" && msg.payload) {
        try {
          const data = JSON.parse(msg.payload);
          const listener = this.notifyListeners.get(data.event_id);
          if (listener) {
            listener(data.status as EventStatus);
          }
        } catch {
          // Ignore parse errors
        }
      }
    });

    await this.notifyClient.query("LISTEN cqrs_event_status");
  }

  /**
   * Check the current status of an event.
   */
  private async checkEventStatus(eventId: string): Promise<EventStatus | null> {
    const result = await this.pool.query(
      "SELECT status FROM events WHERE event_id = $1",
      [eventId],
    );
    return result.rows[0]?.status || null;
  }

  /**
   * Map a database row to IPersistedEvent.
   */
  private mapRowToEvent(row: Record<string, any>): IPersistedEvent {
    return {
      eventId: row.event_id,
      eventName: row.event_name,
      streamId: row.stream_id,
      event: row.event,
      timestamp: row.timestamp,
      status: row.status,
      type: row.type,
      meta: row.meta,
      retryCount: row.retry_count,
      lockedAt: row.locked_at,
      lockedBy: row.locked_by,
      nextRetryAt: row.next_retry_at,
    };
  }
}

/**
 * Create a new PollingWorker instance.
 */
export function createPollingWorker(
  pool: Pool,
  logger: Logger,
  eventType: EventTypes,
  config?: Partial<IWorkerConfig>,
  rateLimitConfig?: IRateLimitConfig,
): PollingWorker {
  return new PollingWorker(pool, logger, eventType, config, rateLimitConfig);
}
