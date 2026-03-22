import { PGlite } from "@electric-sql/pglite"
import { CamelCasePlugin, Kysely } from "kysely"
import { PGliteDialect } from "./pgliteDialect.js"
import type { QueryResult, QueryResultRow } from "pg"
import type { IRateLimitConfig } from "../src/infrastructure/types.js"
import { createRateLimiterInMemory } from "../src/infrastructure/utils/rateLimiter.js"
import type { IRateLimiter } from "../src/infrastructure/utils/rateLimiter.js"
import type { IDbAdapter } from "../src/infrastructure/db/pgAdapter.js"
import type { KyselyDb } from "../src/infrastructure/db/kysely.js"
import { Database } from "../src/infrastructure/db/schema.js"

/**
 * Convert PGlite result to pg.QueryResult format (for raw SQL).
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
 * PGlite-backed IDbAdapter. Uses PGlite natively for LISTEN/NOTIFY and in-memory rate limiting.
 */
export class PGliteDbAdapter implements IDbAdapter {
  private notificationCallbacks: Array<(channel: string, payload: string) => void> = []
  private activeListens = new Map<string, () => Promise<void>>()

  constructor(
    public readonly db: KyselyDb,
    private readonly pglite: PGlite,
  ) {}

  onNotification(callback: (channel: string, payload: string) => void): void {
    this.notificationCallbacks.push(callback)
  }

  async listen(channel: string): Promise<void> {
    const unsub = await this.pglite.listen(channel, payload => {
      this.notificationCallbacks.forEach(cb => cb(channel, payload ?? ""))
    })
    this.activeListens.set(channel, unsub)
  }

  async unlisten(channel: string): Promise<void> {
    const unsub = this.activeListens.get(channel)
    if (unsub) {
      await unsub()
      this.activeListens.delete(channel)
    }
  }

  async releaseNotify(): Promise<void> {
    for (const unsub of this.activeListens.values()) {
      await unsub()
    }
    this.activeListens.clear()
  }

  async getRateLimiter(config?: Partial<IRateLimitConfig>): Promise<IRateLimiter> {
    return createRateLimiterInMemory(config)
  }

  /** Raw SQL query (e.g. for tests). Prefer this.db (Kysely) when possible. */
  async query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<R>> {
    if ((!values || values.length === 0) && hasMultipleStatements(text)) {
      const results = await this.pglite.exec(text)
      const lastResult = results[results.length - 1] || { rows: [], affectedRows: 0 }
      return toPgResult(lastResult as unknown as { rows: R[]; affectedRows?: number })
    }
    const result = await this.pglite.query<R>(text, values as any[])
    return toPgResult(result)
  }

  /** Client-like handle (single connection; release is no-op). */
  async connect(): Promise<{ query: PGliteDbAdapter["query"]; release: () => void }> {
    return {
      query: this.query.bind(this),
      release: () => {},
    }
  }

  async close(): Promise<void> {
    await this.releaseNotify()
    await this.db.destroy()
    await this.pglite.close()
  }
}

/**
 * Create a PGlite-backed adapter. Use with persistence.type === "pglite".
 */
export async function createPGliteAdapter(): Promise<PGliteDbAdapter> {
  const pglite = await PGlite.create()
  const db = new Kysely<Database>({
    dialect: new PGliteDialect(pglite),
    plugins: [new CamelCasePlugin()],
  })
  return new PGliteDbAdapter(db, pglite)
}
