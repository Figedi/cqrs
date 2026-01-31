import type { IRetryConfig } from "../types.js"

/** Default retry configuration */
export const DEFAULT_RETRY_CONFIG: IRetryConfig = {
  maxRetries: 5,
  backoffMode: "EXPONENTIAL",
  baseDelayMs: 1000,
  maxDelayMs: 300000, // 5 minutes
  multiplier: 2,
  jitterFactor: 0.1,
}

/**
 * Calculate the next retry delay based on the backoff configuration.
 *
 * @param retryCount - Current retry count (0-indexed)
 * @param config - Retry configuration
 * @returns Delay in milliseconds before next retry
 */
export function calculateBackoffDelay(retryCount: number, config: IRetryConfig): number {
  const { backoffMode, baseDelayMs, maxDelayMs, jitterFactor, multiplier = 2 } = config

  let delay: number

  switch (backoffMode) {
    case "IMMEDIATE":
      delay = 0
      break
    case "UNIFORM":
      delay = baseDelayMs
      break
    case "EXPONENTIAL":
      // Exponential backoff: baseDelay * multiplier^retryCount
      delay = baseDelayMs * Math.pow(multiplier, retryCount)
      break
    default:
      delay = baseDelayMs
  }

  // Apply maximum cap
  delay = Math.min(delay, maxDelayMs)

  // Apply jitter to prevent thundering herd
  if (jitterFactor > 0 && delay > 0) {
    const jitter = delay * jitterFactor * (Math.random() * 2 - 1) // +/- jitterFactor%
    delay = Math.max(0, delay + jitter)
  }

  return Math.floor(delay)
}

/**
 * Calculate the next retry timestamp based on current time and backoff delay.
 *
 * @param retryCount - Current retry count (0-indexed)
 * @param config - Retry configuration
 * @param now - Current timestamp (defaults to Date.now())
 * @returns Date when next retry should be attempted
 */
export function calculateNextRetryAt(retryCount: number, config: IRetryConfig, now: number = Date.now()): Date {
  const delayMs = calculateBackoffDelay(retryCount, config)
  return new Date(now + delayMs)
}

/**
 * Check if the event should be aborted (exceeded max retries).
 *
 * @param retryCount - Current retry count
 * @param maxRetries - Maximum allowed retries
 * @returns true if event should be aborted
 */
export function shouldAbort(retryCount: number, maxRetries: number): boolean {
  return retryCount >= maxRetries
}

/**
 * Merge partial retry config with defaults.
 *
 * @param partial - Partial retry configuration
 * @returns Complete retry configuration
 */
export function mergeRetryConfig(partial?: Partial<IRetryConfig>): IRetryConfig {
  return {
    ...DEFAULT_RETRY_CONFIG,
    ...partial,
  }
}
