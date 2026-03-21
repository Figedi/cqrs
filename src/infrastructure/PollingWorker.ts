import { sql, type Transaction } from "kysely"
import { v4 as uuid } from "uuid"
import type { IDbAdapter } from "./db/index.js"
import type { Database } from "./db/index.js"
import type { Logger } from "../types.js"
import type { EventStatus, EventTypes, IPersistedEvent, IPollResult, IRateLimitConfig, IWorkerConfig } from "./types.js"
import { calculateNextRetryAt, DEFAULT_RETRY_CONFIG, shouldAbort } from "./utils/backoff.js"
import { type IRateLimiter, RateLimitExceededError } from "./utils/rateLimiter.js"

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
  private notifyListeners = new Map<string, (status: EventStatus) => void>()
  private processors = new Map<EventTypes, IRegisteredProcessor>()
  private isInitialized = false
  private adapter?: IDbAdapter

  constructor(
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

  public setAdapter(adapter: IDbAdapter): void {
    this.adapter = adapter
  }

  /**
   * Register a processor for an event type (COMMAND or EVENT).
   * Can be called before or after start() - supports frameworks with arbitrary preflight order.
   */
  public registerProcessor(
    eventType: EventTypes,
    processor: EventProcessor,
    onCompletion?: CompletionCallback,
  ): void {
    this.processors.set(eventType, { processor, onCompletion })
  }

  /**
   * Lifecycle: initialize (rate limiter, LISTEN/NOTIFY) and start polling.
   * Call from CQRSModule.preflight() after buses have registered their processors.
   */
  public async preflight(): Promise<void> {
    if (!this.adapter) {
      throw new Error("Adapter not set. Call setAdapter() before preflight().")
    }
    await this.initialize()
    await this.start()
  }

  /**
   * Lifecycle: stop polling and release NOTIFY connection.
   * Call from CQRSModule.shutdown().
   */
  public async shutdown(): Promise<void> {
    this.isRunning = false

    if (this.pollingTimer) {
      clearInterval(this.pollingTimer)
      this.pollingTimer = undefined
    }

    if (this.staleLockTimer) {
      clearInterval(this.staleLockTimer)
      this.staleLockTimer = undefined
    }

    try {
      await this.adapter!.unlisten("cqrs_event_status")
    } catch {
      // Ignore errors during cleanup
    }
    await this.adapter!.releaseNotify()

    this.logger.info({ workerId: this.workerId }, "Polling worker stopped")
  }

  private async initialize(): Promise<void> {
    if (this.isInitialized) {
      return
    }
    this.isInitialized = true

    if (this.rateLimitConfig?.maxPerSecond) {
      this.rateLimiter = await this.adapter!.getRateLimiter(this.rateLimitConfig)
    }

    await this.setupNotifyListener()
  }

  private async start(): Promise<void> {
    if (this.isRunning) {
      return
    }
    if (this.processors.size === 0) {
      throw new Error("No processors registered. Call registerProcessor() before preflight().")
    }
    this.isRunning = true

    const eventTypes = Array.from(this.processors.keys())
    this.logger.info({ workerId: this.workerId, eventTypes }, "Starting polling worker")

    this.pollingTimer = setInterval(async () => {
      if (!this.isRunning) return
      await this.pollAndProcess()
    }, this.config.pollIntervalMs)

    this.staleLockTimer = setInterval(async () => {
      if (!this.isRunning) return
      await this.cleanupStaleLocks()
    }, 30000)

    await this.pollAndProcess()
  }

  /**
   * Wait for a specific event to complete (for executeSync).
   * Uses LISTEN/NOTIFY when available, with polling fallback for environments that lack it (e.g. PGlite).
   */
  public async waitForCompletion(eventId: string, timeoutMs: number = 0): Promise<EventStatus> {
    return new Promise((resolve, reject) => {
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined
      let pollInterval: ReturnType<typeof setInterval> | undefined

      const cleanup = () => {
        this.notifyListeners.delete(eventId)
        if (timeoutHandle) {
          clearTimeout(timeoutHandle)
          timeoutHandle = undefined
        }
        if (pollInterval) {
          clearInterval(pollInterval)
          pollInterval = undefined
        }
      }

      const resolveWithCleanup = (status: EventStatus) => {
        cleanup()
        resolve(status)
      }

      // Register NOTIFY listener
      this.notifyListeners.set(eventId, resolveWithCleanup)

      // Setup timeout
      if (timeoutMs > 0) {
        timeoutHandle = setTimeout(() => {
          cleanup()
          reject(new Error(`Timeout waiting for event ${eventId}`))
        }, timeoutMs)
      }

      // Initial check (race condition protection)
      this.checkEventStatus(eventId).then(status => {
        if (status && ["PROCESSED", "FAILED", "ABORTED"].includes(status)) {
          resolveWithCleanup(status)
          return
        }
        // Polling fallback for environments without LISTEN/NOTIFY (e.g. PGlite)
        pollInterval = setInterval(async () => {
          const s = await this.checkEventStatus(eventId)
          if (s && ["PROCESSED", "FAILED", "ABORTED"].includes(s)) {
            resolveWithCleanup(s)
          }
        }, 50)
      })
    })
  }

  /**
   * Wait for multiple events to complete.
   */
  public async waitForCompletionBatch(eventIds: string[], timeoutMs: number = 0): Promise<Map<string, EventStatus>> {
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

    return await this.adapter!.db.transaction().execute(async trx => {
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
      await this.adapter!.db.transaction().execute(async trx => {
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
    const db = trx ?? this.adapter!.db
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
    await this.adapter!.db
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
    await this.adapter!.db
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
    await this.adapter!.db
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

    const result = await this.adapter!.db
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
    this.adapter!.onNotification((channel, payload) => {
      if (channel === "cqrs_event_status" && payload) {
        try {
          const data = JSON.parse(payload)
          const listener = this.notifyListeners.get(data.event_id)
          if (listener) {
            listener(data.status as EventStatus)
          }
        } catch {
          // Ignore parse errors
        }
      }
    })

    await this.adapter!.listen("cqrs_event_status")
  }

  /**
   * Check the current status of an event.
   */
  private async checkEventStatus(eventId: string): Promise<EventStatus | null> {
    const result = await this.adapter!.db.selectFrom("events").select("status").where("eventId", "=", eventId).executeTakeFirst()

    return result?.status ?? null
  }
}
