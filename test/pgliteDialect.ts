import type { PGlite } from "@electric-sql/pglite"
import {
  CompiledQuery,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  type DatabaseConnection,
  type DatabaseIntrospector,
  type Dialect,
  type Driver,
  type Kysely,
  type QueryCompiler,
  type QueryResult,
  type TransactionSettings,
} from "kysely"

class PGliteConnection implements DatabaseConnection {
  constructor(private client: PGlite) {}

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const result = await this.client.query<R>(compiledQuery.sql, [...compiledQuery.parameters])
    if (result.affectedRows) {
      return {
        numAffectedRows: BigInt(result.affectedRows),
        rows: result.rows,
      }
    }
    return { rows: result.rows }
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery, chunkSize: number): AsyncIterableIterator<QueryResult<R>> {
    const result = await this.client.query<R>(compiledQuery.sql, [...compiledQuery.parameters])
    for (let i = 0; i < result.rows.length; i += chunkSize) {
      yield { rows: result.rows.slice(i, i + chunkSize) }
    }
  }
}

class PGliteDriver implements Driver {
  private client: PGlite | undefined
  private connection: PGliteConnection | undefined
  private queue: Array<(conn: PGliteConnection) => void> = []

  constructor(pgLite: PGlite) {
    this.client = pgLite
  }

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    if (!this.client) {
      throw new Error("PGlite not initialized")
    }
    if (this.connection) {
      return new Promise<PGliteConnection>((resolve) => {
        this.queue.push(resolve)
      })
    }
    this.connection = new PGliteConnection(this.client)
    return this.connection
  }

  async releaseConnection(connection: DatabaseConnection): Promise<void> {
    if (connection !== this.connection) {
      throw new Error("Invalid connection")
    }
    const next = this.queue.shift()
    if (!next) {
      this.connection = undefined
      return
    }
    next(this.connection)
  }

  async beginTransaction(conn: DatabaseConnection, settings: TransactionSettings): Promise<void> {
    if (settings.isolationLevel) {
      await (conn as PGliteConnection).executeQuery(
        CompiledQuery.raw(`start transaction isolation level ${settings.isolationLevel}`),
      )
    } else {
      await (conn as PGliteConnection).executeQuery(CompiledQuery.raw("begin"))
    }
  }

  async commitTransaction(conn: DatabaseConnection): Promise<void> {
    await (conn as PGliteConnection).executeQuery(CompiledQuery.raw("commit"))
  }

  async rollbackTransaction(conn: DatabaseConnection): Promise<void> {
    await (conn as PGliteConnection).executeQuery(CompiledQuery.raw("rollback"))
  }

  async destroy(): Promise<void> {
    this.client = undefined
  }
}

export class PGliteDialect implements Dialect {
  constructor(private readonly pgLite: PGlite) {}

  createAdapter(): PostgresAdapter {
    return new PostgresAdapter()
  }

  createDriver(): Driver {
    return new PGliteDriver(this.pgLite)
  }

  createQueryCompiler(): QueryCompiler {
    return new PostgresQueryCompiler()
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new PostgresIntrospector(db)
  }
}
