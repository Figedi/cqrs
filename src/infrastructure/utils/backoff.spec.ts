import { describe, expect, it } from "vitest"
import type { IRetryConfig } from "../types.js"
import {
  calculateBackoffDelay,
  calculateNextRetryAt,
  DEFAULT_RETRY_CONFIG,
  mergeRetryConfig,
  shouldAbort,
} from "./backoff.js"

describe("backoff utilities", () => {
  describe("DEFAULT_RETRY_CONFIG", () => {
    it("should have sensible defaults", () => {
      expect(DEFAULT_RETRY_CONFIG.backoffMode).to.equal("EXPONENTIAL")
      expect(DEFAULT_RETRY_CONFIG.maxRetries).to.equal(5)
      expect(DEFAULT_RETRY_CONFIG.baseDelayMs).to.equal(1000)
      expect(DEFAULT_RETRY_CONFIG.maxDelayMs).to.equal(300000)
      expect(DEFAULT_RETRY_CONFIG.multiplier).to.equal(2)
      expect(DEFAULT_RETRY_CONFIG.jitterFactor).to.equal(0.1)
    })
  })

  describe("mergeRetryConfig", () => {
    it("should use defaults when no config provided", () => {
      const result = mergeRetryConfig()
      expect(result).to.deep.equal(DEFAULT_RETRY_CONFIG)
    })

    it("should merge partial config with defaults", () => {
      const result = mergeRetryConfig({ maxRetries: 10 })
      expect(result.maxRetries).to.equal(10)
      expect(result.backoffMode).to.equal("EXPONENTIAL")
      expect(result.baseDelayMs).to.equal(1000)
    })

    it("should override all defaults when full config provided", () => {
      const custom: IRetryConfig = {
        backoffMode: "UNIFORM",
        maxRetries: 3,
        baseDelayMs: 500,
        maxDelayMs: 30000,
        multiplier: 3,
        jitterFactor: 0.2,
      }
      const result = mergeRetryConfig(custom)
      expect(result).to.deep.equal(custom)
    })
  })

  describe("calculateBackoffDelay", () => {
    describe("IMMEDIATE mode", () => {
      const config: IRetryConfig = {
        backoffMode: "IMMEDIATE",
        maxRetries: 5,
        baseDelayMs: 1000,
        maxDelayMs: 60000,
        multiplier: 2,
        jitterFactor: 0,
      }

      it("should return 0 for all retry counts", () => {
        expect(calculateBackoffDelay(0, config)).to.equal(0)
        expect(calculateBackoffDelay(1, config)).to.equal(0)
        expect(calculateBackoffDelay(5, config)).to.equal(0)
      })
    })

    describe("UNIFORM mode", () => {
      const config: IRetryConfig = {
        backoffMode: "UNIFORM",
        maxRetries: 5,
        baseDelayMs: 1000,
        maxDelayMs: 60000,
        multiplier: 2,
        jitterFactor: 0,
      }

      it("should return baseDelayMs for all retry counts", () => {
        expect(calculateBackoffDelay(0, config)).to.equal(1000)
        expect(calculateBackoffDelay(1, config)).to.equal(1000)
        expect(calculateBackoffDelay(5, config)).to.equal(1000)
      })

      it("should apply jitter when configured", () => {
        const configWithJitter = { ...config, jitterFactor: 0.5 }
        const delays = Array.from({ length: 100 }, () => calculateBackoffDelay(1, configWithJitter))

        // All delays should be within 50% range of 1000 (500-1500)
        delays.forEach(delay => {
          expect(delay).to.be.at.least(500)
          expect(delay).to.be.at.most(1500)
        })

        // Should have some variation (not all the same)
        const uniqueDelays = new Set(delays)
        expect(uniqueDelays.size).to.be.greaterThan(1)
      })
    })

    describe("EXPONENTIAL mode", () => {
      const config: IRetryConfig = {
        backoffMode: "EXPONENTIAL",
        maxRetries: 5,
        baseDelayMs: 1000,
        maxDelayMs: 60000,
        multiplier: 2,
        jitterFactor: 0,
      }

      it("should increase delay exponentially", () => {
        const delay0 = calculateBackoffDelay(0, config)
        const delay1 = calculateBackoffDelay(1, config)
        const delay2 = calculateBackoffDelay(2, config)
        const delay3 = calculateBackoffDelay(3, config)

        expect(delay0).to.equal(1000) // 1000 * 2^0
        expect(delay1).to.equal(2000) // 1000 * 2^1
        expect(delay2).to.equal(4000) // 1000 * 2^2
        expect(delay3).to.equal(8000) // 1000 * 2^3
      })

      it("should cap at maxDelayMs", () => {
        const smallMax: IRetryConfig = { ...config, maxDelayMs: 5000 }

        const delay3 = calculateBackoffDelay(3, smallMax)
        expect(delay3).to.equal(5000) // Would be 8000 but capped at 5000
      })

      it("should apply jitter when configured", () => {
        const configWithJitter = { ...config, jitterFactor: 0.2 }
        const delays = Array.from({ length: 100 }, () => calculateBackoffDelay(2, configWithJitter))

        // Base delay at retry 2 is 4000, with 20% jitter: 3200-4800
        delays.forEach(delay => {
          expect(delay).to.be.at.least(3200)
          expect(delay).to.be.at.most(4800)
        })
      })

      it("should respect custom multiplier", () => {
        const configWith3x: IRetryConfig = { ...config, multiplier: 3 }

        const delay0 = calculateBackoffDelay(0, configWith3x)
        const delay1 = calculateBackoffDelay(1, configWith3x)
        const delay2 = calculateBackoffDelay(2, configWith3x)

        expect(delay0).to.equal(1000) // 1000 * 3^0
        expect(delay1).to.equal(3000) // 1000 * 3^1
        expect(delay2).to.equal(9000) // 1000 * 3^2
      })
    })
  })

  describe("calculateNextRetryAt", () => {
    const config: IRetryConfig = {
      backoffMode: "EXPONENTIAL",
      maxRetries: 5,
      baseDelayMs: 1000,
      maxDelayMs: 60000,
      multiplier: 2,
      jitterFactor: 0,
    }

    it("should return a future date", () => {
      const now = new Date()
      const nextRetry = calculateNextRetryAt(0, config)

      expect(nextRetry.getTime()).to.be.greaterThan(now.getTime())
    })

    it("should calculate correct future time", () => {
      const before = Date.now()
      const nextRetry = calculateNextRetryAt(1, config)
      const after = Date.now()

      // Delay at retry 1 is 2000ms
      expect(nextRetry.getTime()).to.be.at.least(before + 2000)
      expect(nextRetry.getTime()).to.be.at.most(after + 2000 + 100) // 100ms tolerance
    })
  })

  describe("shouldAbort", () => {
    it("should return false when retryCount is below maxRetries", () => {
      expect(shouldAbort(0, 3)).to.equal(false)
      expect(shouldAbort(1, 3)).to.equal(false)
      expect(shouldAbort(2, 3)).to.equal(false)
    })

    it("should return true when retryCount reaches maxRetries", () => {
      expect(shouldAbort(3, 3)).to.equal(true)
    })

    it("should return true when retryCount exceeds maxRetries", () => {
      expect(shouldAbort(4, 3)).to.equal(true)
      expect(shouldAbort(10, 3)).to.equal(true)
    })
  })
})
