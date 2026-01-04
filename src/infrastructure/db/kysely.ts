import { Kysely, PostgresDialect, CamelCasePlugin, sql, type Transaction } from 'kysely';
import type { Pool } from 'pg';
import type { Database } from './schema.js';

/**
 * Type alias for a Kysely instance configured with our database schema.
 */
export type KyselyDb = Kysely<Database>;

/**
 * Create a Kysely instance from a pg Pool for production use.
 *
 * @param pool - PostgreSQL connection pool
 * @returns Configured Kysely instance with CamelCasePlugin
 */
export function createKyselyFromPool(pool: Pool): KyselyDb {
  return new Kysely<Database>({
    dialect: new PostgresDialect({ pool }),
    plugins: [new CamelCasePlugin()],
  });
}

/**
 * Check if a KyselyDb instance is actually a Transaction.
 * We check by looking at the constructor name since Kysely's Transaction class
 * extends Kysely but can be identified by its constructor.
 */
export function isTransaction(db: KyselyDb | Transaction<Database>): db is Transaction<Database> {
  // Check constructor name - Transaction class has name 'Transaction'
  if (db?.constructor?.name === 'Transaction') {
    return true;
  }
  // Also check for isTransaction method as a fallback
  if ('isTransaction' in db && typeof (db as any).isTransaction === 'function') {
    try {
      return (db as any).isTransaction();
    } catch {
      return false;
    }
  }
  return false;
}

// Re-export sql template tag for raw SQL queries
export { sql };

// Re-export types for convenience
export type { Database } from './schema.js';
