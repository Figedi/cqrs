import type { Generated, ColumnType } from 'kysely';

/**
 * Status of an event in the outbox.
 */
export type EventStatus = 'CREATED' | 'PROCESSING' | 'FAILED' | 'PROCESSED' | 'ABORTED';

/**
 * Type of event (command, query, or domain event).
 */
export type EventType = 'COMMAND' | 'QUERY' | 'EVENT';

/**
 * Status of a scheduled event.
 */
export type ScheduledEventStatus = 'CREATED' | 'FAILED' | 'PROCESSED' | 'ABORTED';

/**
 * Events table - the core outbox table for commands, queries, and events.
 * Note: Property names are camelCase here; CamelCasePlugin converts to snake_case in SQL.
 */
export interface EventsTable {
  eventId: string;
  eventName: string;
  streamId: string;
  event: ColumnType<unknown, string, string>;
  timestamp: ColumnType<Date, Date | string, Date | string>;
  status: EventStatus;
  type: EventType;
  meta: ColumnType<unknown | null, string | null, string | null>;
  retryCount: Generated<number>;
  lockedAt: ColumnType<Date | null, Date | string | null, Date | string | null>;
  lockedBy: string | null;
  nextRetryAt: ColumnType<Date | null, Date | string | null, Date | string | null>;
}

/**
 * Scheduled events table - for time-based command scheduling.
 * Note: Property names are camelCase here; CamelCasePlugin converts to snake_case in SQL.
 */
export interface ScheduledEventsTable {
  scheduledEventId: string;
  executeAt: ColumnType<Date, Date | string, Date | string>;
  event: ColumnType<unknown, string, string>;
  status: ScheduledEventStatus;
}

/**
 * Database interface representing all tables.
 * Used with Kysely for type-safe queries.
 * Note: Table names are camelCase here; CamelCasePlugin converts to snake_case in SQL.
 */
export interface Database {
  events: EventsTable;
  scheduledEvents: ScheduledEventsTable;
}
