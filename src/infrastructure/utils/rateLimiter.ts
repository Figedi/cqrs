import type { Pool } from "pg"
import { RateLimiterMemory, RateLimiterPostgres, RateLimiterRes } from "rate-limiter-flexible"

import type { IRateLimitConfig } from "../types.js"

/** Default rate limit configuration */
export const DEFAULT_RATE_LIMIT_CONFIG: Required<IRateLimitConfig> = {
  maxPerSecond: 10,
  maxConcurrent: 5,
  keyPrefix: "cqrs_rate_limit",
  type: "in-memory",
}

export interface IRateLimiter {
  /** Acquire a rate limit slot. Throws if rate limit exceeded. */
  acquire(key: string): Promise<void>
  /** Release a rate limit slot (for concurrency limiting). */
  release(key: string): Promise<void>
  /** Check if rate limit is exceeded without consuming a point. */
  isLimited(key: string): Promise<boolean>
  /** Get remaining points for a key. */
  getRemaining(key: string): Promise<number>
}

/**
 * Error thrown when rate limit is exceeded.
 */
export class RateLimitExceededError extends Error {
  constructor(
    message: string,
    public readonly retryAfterMs: number,
  ) {
    super(message)
    this.name = "RateLimitExceededError"
  }
}

/**
 * Create a RateLimiterPostgres instance with callback-based initialization.
 */
function createPostgresLimiter(opts: {
  storeClient: Pool
  tableName: string
  points: number
  duration: number
  keyPrefix: string
  tableCreated?: boolean
}): Promise<RateLimiterPostgres> {
  return new Promise((resolve, reject) => {
    const limiter = new RateLimiterPostgres(opts, (err?: Error) => {
      if (err) {
        reject(err)
      } else {
        resolve(limiter)
      }
    })
  })
}

/**
 * Create an in-memory rate limiter (for testing or non-pg backends).
 */
function createMemoryLimiter(opts: { points: number; duration: number; keyPrefix: string }): RateLimiterMemory {
  return new RateLimiterMemory({
    points: opts.points,
    duration: opts.duration,
    keyPrefix: opts.keyPrefix,
  })
}

/**
 * Creates a rate limiter for distributed rate limiting across workers.
 *
 * Uses rate-limiter-flexible with PostgreSQL (storeClient) or in-memory storage.
 * When storeClient is a PGlite-backed mock pool (query() runs on PGlite), config.type === "pg" will store rate limit state in PGlite.
 * Supports both maxPerSecond (primary) and maxConcurrent (optional) limits.
 *
 * @param pool - PostgreSQL connection pool (or PGlite-compatible mock with query())
 * @param config - Rate limit configuration
 * @returns Rate limiter instance
 */
export async function createRateLimiter(pool: Pool, config?: Partial<IRateLimitConfig>): Promise<IRateLimiter> {
  const mergedConfig = {
    ...DEFAULT_RATE_LIMIT_CONFIG,
    ...config,
  }

  // Primary rate limiter: maxPerSecond
  const rateLimiter =
    config?.type === "pg"
      ? await createPostgresLimiter({
          storeClient: pool,
          tableName: `${mergedConfig.keyPrefix}_rate`,
          points: mergedConfig.maxPerSecond,
          duration: 1, // 1 second window
          keyPrefix: `${mergedConfig.keyPrefix}:rate`,
        })
      : createMemoryLimiter({
          points: mergedConfig.maxPerSecond,
          duration: 1,
          keyPrefix: `${mergedConfig.keyPrefix}:rate`,
        })

  // Concurrency limiter (optional): tracks active operations
  let concurrencyLimiter: RateLimiterPostgres | RateLimiterMemory | null = null
  if (mergedConfig.maxConcurrent && mergedConfig.maxConcurrent > 0) {
    concurrencyLimiter =
      config?.type === "pg"
        ? await createPostgresLimiter({
            storeClient: pool,
            tableName: `${mergedConfig.keyPrefix}_concurrent`,
            points: mergedConfig.maxConcurrent,
            duration: 86400, // Long duration - we manually release
            keyPrefix: `${mergedConfig.keyPrefix}:concurrent`,
          })
        : createMemoryLimiter({
            points: mergedConfig.maxConcurrent,
            duration: 86400,
            keyPrefix: `${mergedConfig.keyPrefix}:concurrent`,
          })
  }

  // Track active operations for release
  const activeOperations = new Map<string, number>()

  return {
    async acquire(key: string): Promise<void> {
      // Check rate limit first
      try {
        await rateLimiter.consume(key, 1)
      } catch (error) {
        if (error instanceof RateLimiterRes) {
          const retryAfterMs = error.msBeforeNext || 1000
          throw new RateLimitExceededError(
            `Rate limit exceeded for ${key}. Retry after ${retryAfterMs}ms`,
            retryAfterMs,
          )
        }
        throw error
      }

      // Check concurrency limit if configured
      if (concurrencyLimiter) {
        try {
          await concurrencyLimiter.consume(key, 1)
          const count = activeOperations.get(key) || 0
          activeOperations.set(key, count + 1)
        } catch (error) {
          if (error instanceof RateLimiterRes) {
            throw new RateLimitExceededError(
              `Concurrency limit exceeded for ${key}. Active: ${mergedConfig.maxConcurrent}`,
              0,
            )
          }
          throw error
        }
      }
    },

    async release(key: string): Promise<void> {
      if (!concurrencyLimiter) {
        return
      }

      const count = activeOperations.get(key) || 0
      if (count > 0) {
        activeOperations.set(key, count - 1)
        try {
          // Reward a point back to allow another concurrent operation
          await concurrencyLimiter.reward(key, 1)
        } catch {
          // Ignore errors on release
        }
      }
    },

    async isLimited(key: string): Promise<boolean> {
      try {
        const res = await rateLimiter.get(key)
        if (!res) {
          return false
        }
        return res.remainingPoints <= 0
      } catch {
        return false
      }
    },

    async getRemaining(key: string): Promise<number> {
      try {
        const res = await rateLimiter.get(key)
        if (!res) {
          return mergedConfig.maxPerSecond
        }
        return res.remainingPoints
      } catch {
        return 0
      }
    },
  }
}

/**
 * Creates an in-memory-only rate limiter (no PostgreSQL).
 * Use for PGlite or when no shared store is needed.
 */
export async function createRateLimiterInMemory(
  config?: Partial<IRateLimitConfig>,
): Promise<IRateLimiter> {
  const mergedConfig = {
    ...DEFAULT_RATE_LIMIT_CONFIG,
    ...config,
    type: "in-memory" as const,
  }
  return createRateLimiter(null as unknown as Pool, mergedConfig)
}

/**
 * Merge partial rate limit config with defaults.
 *
 * @param partial - Partial rate limit configuration
 * @returns Complete rate limit configuration
 */
export function mergeRateLimitConfig(partial?: Partial<IRateLimitConfig>): Required<IRateLimitConfig> {
  return {
    ...DEFAULT_RATE_LIMIT_CONFIG,
    ...partial,
  }
}
