import { CamelCasePlugin, Kysely, PostgresDialect, type Transaction } from "kysely"
import type { Pool } from "pg"
import type { Database } from "./schema.js"

export type KyselyDb = Kysely<Database>

export function createKyselyFromPool(pool: Pool): KyselyDb {
  return new Kysely<Database>({
    dialect: new PostgresDialect({ pool }),
    plugins: [new CamelCasePlugin()],
  })
}

export function isTransaction(db: KyselyDb | Transaction<Database>): db is Transaction<Database> {
  return (db as Transaction<Database>).isTransaction === true
}
