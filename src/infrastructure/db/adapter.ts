/**
 * Isolation levels for database transactions.
 * Values match Kysely's expected format for setIsolationLevel().
 */
export type IsolationLevel = "read uncommitted" | "read committed" | "repeatable read" | "serializable" | "snapshot"

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
