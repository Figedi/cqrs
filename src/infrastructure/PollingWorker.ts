import { sql, type Transaction } from "kysely"
import type { Pool, PoolClient } from "pg"
import { v4 as uuid } from "uuid"
import type { Logger } from "../types.js"
import type { Database, KyselyDb } from "./db/index.js"
import type { EventStatus, EventTypes, IPersistedEvent, IPollResult, IRateLimitConfig, IWorkerConfig } from "./types.js"
import { calculateNextRetryAt, DEFAULT_RETRY_CONFIG, shouldAbort } from "./utils/backoff.js"
import { createRateLimiter, type IRateLimiter, RateLimitExceededError } from "./utils/rateLimiter.js"

/** Default worker configuration */
export const DEFAULT_WORKER_CONFIG: IWorkerConfig = {
  workerId: "",
  pollIntervalMs: 1000,
  batchSize: 10,
  lockTimeoutMs: 30000,
  retry: DEFAULT_RETRY_CONFIG,
}

/** Callback for processing a claimed event */
export type EventProcessor = (event: IPersistedEvent, trx: Transaction<Database>) => Promise<void>

/** Callback for handling event completion (success or failure) */
export type CompletionCallback = (event: IPersistedEvent, status: EventStatus, error?: Error) => void

export interface IRegisteredProcessor {
  processor: EventProcessor
  onCompletion?: CompletionCallback
}

/**
 * PollingWorker handles the core polling mechanism for processing events
 * from the outbox table using FOR UPDATE SKIP LOCKED pattern.
 *
 * Supports multiple event types (COMMAND, EVENT) - register processors via registerProcessor().
 *
 * Features:
 * - Distributed job claiming with FOR UPDATE SKIP LOCKED
 * - PostgreSQL-backed rate limiting via rate-limiter-flexible
 * - Exponential backoff for retries
 * - Stale lock cleanup
 * - LISTEN/NOTIFY support for executeSync
 */
export class PollingWorker {
  private workerId: string
  private config: IWorkerConfig
  private rateLimiter?: IRateLimiter
  private pollingTimer?: ReturnType<typeof setInterval>
  private staleLockTimer?: ReturnType<typeof setInterval>
  private isRunning = false
  private isPolling = false // Guard to prevent overlapping poll cycles
  private notifyClient?: PoolClient
  private notifyListeners = new Map<string, (status: EventStatus) => void>()
  private processors = new Map<EventTypes, IRegisteredProcessor>()
  private isInitialized = false

  constructor(
    private db: KyselyDb,
    private pool: Pool, // Keep for LISTEN/NOTIFY
    private logger: Logger,
    config?: Partial<IWorkerConfig>,
    private rateLimitConfig?: IRateLimitConfig,
  ) {
    this.workerId = config?.workerId || `worker-${uuid()}`
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...config,
      workerId: this.workerId,
      retry: {
        ...DEFAULT_RETRY_CONFIG,
        ...config?.retry,
      },
    }
  }

  /**
   * Register a processor for an event type (COMMAND or EVENT).
   * Must be called before start().
   */
  registerProcessor(
    eventType: EventTypes,
    processor: EventProcessor,
    onCompletion?: CompletionCallback,
  ): void {
    if (this.isRunning) {
      throw new Error("Cannot register processor after worker has started")
    }
    this.processors.set(eventType, { processor, onCompletion })
  }

  /**
   * Initialize the worker (rate limiter, LISTEN/NOTIFY).
   * Idempotent - safe to call multiple times.
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      return
    }
    this.isInitialized = true

    // Initialize rate limiter if configured
    if (this.rateLimitConfig?.maxPerSecond) {
      this.rateLimiter = await createRateLimiter(this.pool, this.rateLimitConfig)
    }

    // Setup LISTEN/NOTIFY for executeSync
    await this.setupNotifyListener()
  }

  /**
   * Start the polling worker.
   * Uses processors registered via registerProcessor().
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      return
    }
    if (this.processors.size === 0) {
      throw new Error("No processors registered. Call registerProcessor() before start().")
    }
    this.isRunning = true

    const eventTypes = Array.from(this.processors.keys())
    this.logger.info({ workerId: this.workerId, eventTypes }, "Starting polling worker")

    // Start polling loop
    this.pollingTimer = setInterval(async () => {
      if (!this.isRunning) return
      await this.pollAndProcess()
    }, this.config.pollIntervalMs)

    // Start stale lock cleanup (every 30 seconds)
    this.staleLockTimer = setInterval(async () => {
      if (!this.isRunning) return
      await this.cleanupStaleLocks()
    }, 30000)

    // Initial poll
    await this.pollAndProcess()
  }

  /**
   * Stop the polling worker gracefully.
   */
  async stop(): Promise<void> {
    this.isRunning = false

    if (this.pollingTimer) {
      clearInterval(this.pollingTimer)
      this.pollingTimer = undefined
    }

    if (this.staleLockTimer) {
      clearInterval(this.staleLockTimer)
      this.staleLockTimer = undefined
    }

    // Close NOTIFY listener
    if (this.notifyClient) {
      try {
        await this.notifyClient.query("UNLISTEN cqrs_event_status")
      } catch {
        // Ignore errors during cleanup
      }
      this.notifyClient.release()
      this.notifyClient = undefined
    }

    this.logger.info({ workerId: this.workerId }, "Polling worker stopped")
  }

  /**
   * Wait for a specific event to complete (for executeSync).
   */
  async waitForCompletion(eventId: string, timeoutMs: number = 0): Promise<EventStatus> {
    return new Promise((resolve, reject) => {
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined

      const cleanup = () => {
        this.notifyListeners.delete(eventId)
        if (timeoutHandle) {
          clearTimeout(timeoutHandle)
        }
      }

      // Register listener
      this.notifyListeners.set(eventId, status => {
        cleanup()
        resolve(status)
      })

      // Setup timeout
      if (timeoutMs > 0) {
        timeoutHandle = setTimeout(() => {
          cleanup()
          reject(new Error(`Timeout waiting for event ${eventId}`))
        }, timeoutMs)
      }

      // Check if already completed (race condition protection)
      this.checkEventStatus(eventId).then(status => {
        if (status && ["PROCESSED", "FAILED", "ABORTED"].includes(status)) {
          cleanup()
          resolve(status)
        }
      })
    })
  }

  /**
   * Wait for multiple events to complete.
   */
  async waitForCompletionBatch(eventIds: string[], timeoutMs: number = 0): Promise<Map<string, EventStatus>> {
    const results = new Map<string, EventStatus>()
    const promises = eventIds.map(async eventId => {
      const status = await this.waitForCompletion(eventId, timeoutMs)
      results.set(eventId, status)
    })

    await Promise.all(promises)
    return results
  }

  /**
   * Poll for pending events and process them.
   */
  private async pollAndProcess(): Promise<void> {
    // Prevent overlapping poll cycles (important for single-connection databases like PGlite)
    if (this.isPolling) {
      return
    }
    this.isPolling = true

    try {
      // Poll for FAILED events first (higher priority)
      const failedResult = await this.poll("FAILED")
      await this.processBatch(failedResult.claimed)

      // Then poll for CREATED events
      const createdResult = await this.poll("CREATED")
      await this.processBatch(createdResult.claimed)
    } catch (error) {
      this.logger.error({ error, workerId: this.workerId }, "Error during polling")
    } finally {
      this.isPolling = false
    }
  }

  /**
   * Poll for events with a specific status using FOR UPDATE SKIP LOCKED pattern.
   * This ensures distributed workers can safely claim events without conflicts.
   */
  private async poll(status: "CREATED" | "FAILED"): Promise<IPollResult> {
    const eventTypes = Array.from(this.processors.keys())
    if (eventTypes.length === 0) {
      return { claimed: [], hasMore: false }
    }

    return await this.db.transaction().execute(async trx => {
      // SELECT with FOR UPDATE SKIP LOCKED to atomically claim rows
      const selected = await trx
        .selectFrom("events")
        .select("eventId")
        .where("status", "=", status)
        .where("type", "in", eventTypes)
        .limit(this.config.batchSize)
        .forUpdate()
        .skipLocked()
        .execute()

      if (selected.length === 0) {
        return { claimed: [], hasMore: false }
      }

      const eventIds = selected.map(r => r.eventId)

      // Update events one by one to avoid PGlite issues with IN clause + RETURNING
      const claimed: IPersistedEvent[] = []
      for (const eventId of eventIds) {
        const result = await trx
          .updateTable("events")
          .set({
            status: "PROCESSING",
            lockedAt: sql`NOW()`,
            lockedBy: this.workerId,
          })
          .where("eventId", "=", eventId)
          .returningAll()
          .executeTakeFirst()

        if (result) {
          claimed.push(result as unknown as IPersistedEvent)
        }
      }

      return {
        claimed: claimed as unknown as IPersistedEvent[],
        hasMore: claimed.length === this.config.batchSize,
      }
    })
  }

  /**
   * Process a batch of claimed events.
   */
  private async processBatch(events: IPersistedEvent[]): Promise<void> {
    for (const event of events) {
      await this.processEvent(event)
    }
  }

  /**
   * Process a single event with rate limiting and error handling.
   */
  private async processEvent(event: IPersistedEvent): Promise<void> {
    const registered = this.processors.get(event.type as EventTypes)
    if (!registered) {
      this.logger.warn({ eventId: event.eventId, type: event.type }, "No processor for event type, skipping")
      return
    }

    const { processor, onCompletion } = registered
    const rateLimitKey = `${event.type}:${event.eventName}`

    // Apply rate limiting
    if (this.rateLimiter) {
      try {
        await this.rateLimiter.acquire(rateLimitKey)
      } catch (error) {
        if (error instanceof RateLimitExceededError) {
          // Release the lock and let another worker pick it up later
          await this.releaseLock(event.eventId)
          return
        }
        throw error
      }
    }

    try {
      await this.db.transaction().execute(async trx => {
        // Process the event (handler may update event meta within this transaction)
        await processor(event, trx)

        // Mark as processed within the same transaction
        await this.markProcessed(event.eventId, trx)
      })

      onCompletion?.(event, "PROCESSED")
    } catch (error) {
      this.logger.error({ eventId: event.eventId, error }, "Event processing failed")
      await this.handleProcessingError(event, error as Error, onCompletion)
    } finally {
      // Release rate limiter slot
      if (this.rateLimiter) {
        await this.rateLimiter.release(rateLimitKey)
      }
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
    const retryCount = (event.retryCount ?? 0) + 1

    if (shouldAbort(retryCount, this.config.retry.maxRetries)) {
      // Exceeded max retries - mark as aborted
      await this.markAborted(event.eventId, retryCount, error)
      this.logger.error({ eventId: event.eventId, retryCount, error }, "Event aborted after max retries")
      onCompletion?.(event, "ABORTED", error)
    } else {
      // Schedule retry with backoff
      const nextRetryAt = calculateNextRetryAt(retryCount, this.config.retry)
      await this.markFailed(event.eventId, retryCount, nextRetryAt, error)
      this.logger.warn({ eventId: event.eventId, retryCount, nextRetryAt, error }, "Event failed, scheduled for retry")
      onCompletion?.(event, "FAILED", error)
    }
  }

  /**
   * Mark an event as processed.
   */
  private async markProcessed(eventId: string, trx?: Transaction<Database>): Promise<void> {
    const db = trx ?? this.db
    await db
      .updateTable("events")
      .set({
        status: "PROCESSED",
        lockedAt: null,
        lockedBy: null,
      })
      .where("eventId", "=", eventId)
      .execute()
  }

  /**
   * Mark an event as failed with retry scheduling.
   */
  private async markFailed(eventId: string, retryCount: number, nextRetryAt: Date, error: Error): Promise<void> {
    await this.db
      .updateTable("events")
      .set({
        status: "FAILED",
        retryCount,
        nextRetryAt,
        lockedAt: null,
        lockedBy: null,
        meta: sql`jsonb_set(COALESCE(meta, '{}'), '{error}', ${JSON.stringify({
          message: error.message,
          stack: error.stack,
        })}::jsonb)`,
      })
      .where("eventId", "=", eventId)
      .execute()
  }

  /**
   * Mark an event as aborted (max retries exceeded).
   */
  private async markAborted(eventId: string, retryCount: number, error: Error): Promise<void> {
    await this.db
      .updateTable("events")
      .set({
        status: "ABORTED",
        retryCount,
        lockedAt: null,
        lockedBy: null,
        meta: sql`jsonb_set(COALESCE(meta, '{}'), '{error}', ${JSON.stringify({
          message: error.message,
          stack: error.stack,
        })}::jsonb)`,
      })
      .where("eventId", "=", eventId)
      .execute()
  }

  /**
   * Release a lock (e.g., when rate limited).
   */
  private async releaseLock(eventId: string): Promise<void> {
    await this.db
      .updateTable("events")
      .set({
        status: "CREATED",
        lockedAt: null,
        lockedBy: null,
      })
      .where("eventId", "=", eventId)
      .where("lockedBy", "=", this.workerId)
      .execute()
  }

  /**
   * Cleanup stale locks from dead workers.
   */
  private async cleanupStaleLocks(): Promise<void> {
    const lockTimeoutInterval = `${this.config.lockTimeoutMs} milliseconds`

    const result = await this.db
      .updateTable("events")
      .set({
        status: "FAILED",
        lockedAt: null,
        lockedBy: null,
        retryCount: sql`COALESCE(retry_count, 0) + 1`,
        nextRetryAt: sql`NOW()`,
      })
      .where("lockedAt", "is not", null)
      .where("lockedAt", "<", sql<Date>`NOW() - INTERVAL ${sql.lit(lockTimeoutInterval)}`)
      .where("status", "=", "PROCESSING")
      .returning("eventId")
      .execute()

    if (result.length > 0) {
      this.logger.warn({ count: result.length, eventIds: result.map(r => r.eventId) }, "Cleaned up stale locks")
    }
  }

  /**
   * Setup LISTEN/NOTIFY for executeSync.
   */
  private async setupNotifyListener(): Promise<void> {
    this.notifyClient = await this.pool.connect()

    this.notifyClient.on("notification", msg => {
      if (msg.channel === "cqrs_event_status" && msg.payload) {
        try {
          const data = JSON.parse(msg.payload)
          const listener = this.notifyListeners.get(data.event_id)
          if (listener) {
            listener(data.status as EventStatus)
          }
        } catch {
          // Ignore parse errors
        }
      }
    })

    await this.notifyClient.query("LISTEN cqrs_event_status")
  }

  /**
   * Check the current status of an event.
   */
  private async checkEventStatus(eventId: string): Promise<EventStatus | null> {
    const result = await this.db.selectFrom("events").select("status").where("eventId", "=", eventId).executeTakeFirst()

    return result?.status ?? null
  }
}

/**
 * Create a new shared PollingWorker instance.
 * Register processors for COMMAND and/or EVENT via registerProcessor() before calling start().
 */
export function createPollingWorker(
  db: KyselyDb,
  pool: Pool,
  logger: Logger,
  config?: Partial<IWorkerConfig>,
  rateLimitConfig?: IRateLimitConfig,
): PollingWorker {
  return new PollingWorker(db, pool, logger, config, rateLimitConfig)
}
