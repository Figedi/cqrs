import { PGlite } from "@electric-sql/pglite"
import { CamelCasePlugin, Kysely } from "kysely"
import { PGliteDialect } from "kysely-pglite-dialect"
import type { Pool, PoolClient, QueryResult, QueryResultRow } from "pg"
import type { Database, KyselyDb } from "../infrastructure/db/index.js"

/**
 * Simple adapter interface for backward compatibility with tests that use raw SQL.
 */
export interface IDbAdapter {
  query<R extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<R>>
  connect(): Promise<IDbClient>
}

export interface IDbClient {
  query<R extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<R>>
  release(): void
}

/**
 * Convert PGlite result to pg.QueryResult format.
 */
function toPgResult<R extends QueryResultRow>(result: {
  rows: R[]
  affectedRows?: number
  fields?: Array<{ name: string; dataTypeID?: number | string }>
}): QueryResult<R> {
  return {
    rows: result.rows,
    rowCount: result.affectedRows ?? result.rows.length,
    command: "",
    oid: 0,
    fields: (result.fields || []).map(f => ({
      name: f.name,
      tableID: 0,
      columnID: 0,
      dataTypeID: parseInt(f.dataTypeID?.toString() || "0", 10),
      dataTypeSize: 0,
      dataTypeModifier: 0,
      format: "text" as const,
    })),
  }
}

/**
 * Check if a SQL string contains multiple statements.
 */
function hasMultipleStatements(sql: string): boolean {
  const cleaned = sql
    .replace(/'[^']*'/g, "''")
    .replace(/--[^\n]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")

  const statements = cleaned
    .split(";")
    .map(s => s.trim())
    .filter(s => s.length > 0)

  return statements.length > 1
}

/**
 * PGlite-based adapter that provides both Kysely and raw query interfaces for testing.
 */
export class PGliteTestAdapter {
  private pglite: PGlite | null = null
  private kyselyDb: KyselyDb | null = null
  private initPromise: Promise<void> | null = null
  private notificationListeners: Map<string, Set<(payload: string) => void>> = new Map()
  private activeListens: Map<string, () => Promise<void>> = new Map()

  constructor() {
    this.initPromise = this.init()
  }

  private async init(): Promise<void> {
    this.pglite = await PGlite.create()

    // Create Kysely instance with PGlite dialect and CamelCasePlugin
    this.kyselyDb = new Kysely<Database>({
      dialect: new PGliteDialect(this.pglite),
      plugins: [new CamelCasePlugin()],
    })
  }

  private async ensureReady(): Promise<{ pglite: PGlite; db: KyselyDb }> {
    await this.initPromise
    if (!this.pglite || !this.kyselyDb) {
      throw new Error("PGlite database not initialized")
    }
    return { pglite: this.pglite, db: this.kyselyDb }
  }

  /**
   * Get the Kysely database instance.
   */
  async getDb(): Promise<KyselyDb> {
    const { db } = await this.ensureReady()
    return db
  }

  /**
   * Get a mock Pool that wraps PGlite for LISTEN/NOTIFY compatibility.
   * Note: This is a simplified mock that supports LISTEN/NOTIFY through PGlite.
   */
  async getPool(): Promise<Pool> {
    const { pglite } = await this.ensureReady()

    // Create a mock pool that wraps PGlite
    const mockPool = {
      connect: async (): Promise<PoolClient> => {
        return this.createMockPoolClient(pglite)
      },
      query: async <R extends QueryResultRow = QueryResultRow>(
        text: string,
        values?: unknown[],
      ): Promise<QueryResult<R>> => {
        return this.executeQuery<R>(pglite, text, values)
      },
      end: async (): Promise<void> => {
        // Cleanup handled by close()
      },
      on: () => mockPool,
      off: () => mockPool,
    } as unknown as Pool

    return mockPool
  }

  /**
   * Create a mock PoolClient for LISTEN/NOTIFY support.
   */
  private createMockPoolClient(pglite: PGlite): PoolClient {
    const notificationCallbacks: Array<(msg: { channel: string; payload?: string }) => void> = []

    const client = {
      query: async <R extends QueryResultRow = QueryResultRow>(
        text: string,
        values?: unknown[],
      ): Promise<QueryResult<R>> => {
        const trimmedText = text.trim().toUpperCase()

        // Handle LISTEN command
        const listenMatch = text.match(/^LISTEN\s+(\w+)/i)
        if (listenMatch) {
          const channel = listenMatch[1].toLowerCase()
          await this.setupListen(pglite, channel, payload => {
            notificationCallbacks.forEach(cb => cb({ channel, payload }))
          })
          return toPgResult({ rows: [] as R[], affectedRows: 0 })
        }

        // Handle UNLISTEN command
        const unlistenMatch = text.match(/^UNLISTEN\s+(\w+)/i)
        if (unlistenMatch) {
          const channel = unlistenMatch[1].toLowerCase()
          await this.teardownListen(channel)
          return toPgResult({ rows: [] as R[], affectedRows: 0 })
        }

        return this.executeQuery<R>(pglite, text, values)
      },
      release: (): void => {
        // Cleanup listeners
        this.activeListens.forEach(async unsub => {
          try {
            await unsub()
          } catch {
            // Ignore errors during cleanup
          }
        })
        this.activeListens.clear()
      },
      on: (event: string, callback: (msg: any) => void): PoolClient => {
        if (event === "notification") {
          notificationCallbacks.push(callback)
        }
        return client as PoolClient
      },
      off: (): PoolClient => client as PoolClient,
    } as unknown as PoolClient

    return client
  }

  private async setupListen(pglite: PGlite, channel: string, callback: (payload: string) => void): Promise<void> {
    if (this.activeListens.has(channel)) {
      // Add to existing listeners
      const listeners = this.notificationListeners.get(channel) || new Set()
      listeners.add(callback)
      this.notificationListeners.set(channel, listeners)
      return
    }

    const listeners = new Set<(payload: string) => void>()
    listeners.add(callback)
    this.notificationListeners.set(channel, listeners)

    const unsub = await pglite.listen(channel, payload => {
      const channelListeners = this.notificationListeners.get(channel)
      if (channelListeners) {
        channelListeners.forEach(listener => listener(payload ?? ""))
      }
    })

    this.activeListens.set(channel, unsub)
  }

  private async teardownListen(channel: string): Promise<void> {
    const unsub = this.activeListens.get(channel)
    if (unsub) {
      await unsub()
      this.activeListens.delete(channel)
      this.notificationListeners.delete(channel)
    }
  }

  private async executeQuery<R extends QueryResultRow>(
    pglite: PGlite,
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<R>> {
    // If no parameters and multiple statements, use exec()
    if ((!values || values.length === 0) && hasMultipleStatements(text)) {
      const results = await pglite.exec(text)
      const lastResult = results[results.length - 1] || { rows: [], affectedRows: 0 }
      return toPgResult(lastResult as unknown as { rows: R[]; affectedRows?: number })
    }

    const result = await pglite.query<R>(text, values as any[])
    return toPgResult(result)
  }

  /**
   * Execute a raw SQL query (for test backward compatibility).
   */
  async query<R extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<R>> {
    const { pglite } = await this.ensureReady()
    return this.executeQuery<R>(pglite, text, values)
  }

  /**
   * Get a client connection (for test backward compatibility).
   */
  async connect(): Promise<IDbClient> {
    const { pglite } = await this.ensureReady()
    return {
      query: async <R extends QueryResultRow = QueryResultRow>(
        text: string,
        values?: unknown[],
      ): Promise<QueryResult<R>> => {
        return this.executeQuery<R>(pglite, text, values)
      },
      release: () => {
        // No-op for single-connection PGlite
      },
    }
  }

  /**
   * Close the PGlite instance.
   */
  async close(): Promise<void> {
    if (this.kyselyDb) {
      await this.kyselyDb.destroy()
      this.kyselyDb = null
    }
    if (this.pglite) {
      await this.pglite.close()
      this.pglite = null
    }
    this.activeListens.clear()
    this.notificationListeners.clear()
  }
}

/**
 * Create a PGlite-backed adapter for testing.
 * Returns an adapter that provides both Kysely and raw query interfaces.
 */
export function createPGliteAdapter(): PGliteTestAdapter {
  return new PGliteTestAdapter()
}
