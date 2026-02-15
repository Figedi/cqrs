import pg from "pg"
import type { Pool, PoolClient } from "pg"
import type { IRateLimitConfig } from "../types.js"
import { createRateLimiter } from "../utils/rateLimiter.js"
import type { IRateLimiter } from "../utils/rateLimiter.js"
import type { KyselyDb } from "./kysely.js"
import { createKyselyFromPool } from "./kysely.js"

/**
 * Isolation levels for database transactions.
 * Values match Kysely's expected format for setIsolationLevel().
 */
export type IsolationLevel =
  | "read uncommitted"
  | "read committed"
  | "repeatable read"
  | "serializable"
  | "snapshot"

/**
 * Common isolation level constants for convenience.
 */
export const IsolationLevels = {
  ReadUncommitted: "read uncommitted",
  ReadCommitted: "read committed",
  RepeatableRead: "repeatable read",
  Serializable: "serializable",
  Snapshot: "snapshot",
} as const satisfies Record<string, IsolationLevel>

/**
 * Database adapter abstraction: all DB access (Kysely, LISTEN/NOTIFY, rate limiting) goes through the adapter.
 * Pool is not exposed; Kysely is owned and exposed by the adapter.
 */
export interface IDbAdapter {
  /** Kysely instance for queries; created/owned by the adapter when using a connection string. */
  readonly db: KyselyDb
  /** Listen for NOTIFY on a channel. Call onNotification() before or after to receive messages. */
  listen(channel: string): Promise<void>
  /** Stop listening on a channel. */
  unlisten(channel: string): Promise<void>
  /** Register callback for NOTIFY messages. Safe to call before or after listen(). */
  onNotification(callback: (channel: string, payload: string) => void): void
  /** Release the NOTIFY connection (e.g. when worker stops). Idempotent. */
  releaseNotify(): Promise<void>
  /**
   * Rate limiter using adapter storage.
   * With real pg: uses PostgreSQL tables when config.type === "pg".
   * With PGlite (mock pool): config.type === "pg" uses the mock pool's query() so state is stored in PGlite; otherwise in-memory.
   */
  getRateLimiter(config?: Partial<IRateLimitConfig>): Promise<IRateLimiter>
}

/**
 * PG-backed adapter using a real Pool and Kysely.
 */
export class PgDbAdapter implements IDbAdapter {
  private notifyClient: PoolClient | null = null
  private notifyCallbacks: Array<(channel: string, payload: string) => void> = []

  constructor(
    public readonly db: KyselyDb,
    private readonly pool: Pool,
  ) {}

  onNotification(callback: (channel: string, payload: string) => void): void {
    this.notifyCallbacks.push(callback)
    if (this.notifyClient) {
      this.notifyClient.on("notification", (msg: { channel: string; payload?: string }) => {
        if (msg.payload) callback(msg.channel, msg.payload)
      })
    }
  }

  async listen(channel: string): Promise<void> {
    if (!this.notifyClient) {
      this.notifyClient = await this.pool.connect()
      for (const cb of this.notifyCallbacks) {
        this.notifyClient.on("notification", (msg: { channel: string; payload?: string }) => {
          if (msg.payload) cb(msg.channel, msg.payload)
        })
      }
    }
    await this.notifyClient.query(`LISTEN ${channel}`)
  }

  async unlisten(channel: string): Promise<void> {
    if (this.notifyClient) {
      await this.notifyClient.query(`UNLISTEN ${channel}`)
    }
  }

  async releaseNotify(): Promise<void> {
    if (this.notifyClient) {
      this.notifyClient.release()
      this.notifyClient = null
    }
  }

  async getRateLimiter(config?: Partial<IRateLimitConfig>): Promise<IRateLimiter> {
    return createRateLimiter(this.pool, config)
  }
}

export type DriverConfig = string | IDbAdapter

/**
 * Type guard: true when driver is an IDbAdapter instance.
 */
export function isDbAdapter(driver: DriverConfig): driver is IDbAdapter {
  return (
    typeof driver === "object" &&
    driver !== null &&
    "listen" in driver &&
    "db" in driver
  )
}

/**
 * Create a DbAdapter from pg driver config.
 * - connection string -> new pg.Pool + Kysely
 * - IDbAdapter -> returned as-is (no-op)
 */
export function createDbAdapter(
  driver: DriverConfig,
  options?: Record<string, unknown>,
): IDbAdapter {
  if (isDbAdapter(driver)) {
    return driver
  }
  const pool = new pg.Pool({ connectionString: driver, ...options })
  const db = createKyselyFromPool(pool)
  return new PgDbAdapter(db, pool)
}
