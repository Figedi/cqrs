import type { ColumnType, Generated } from "kysely"

export type EventStatus = "CREATED" | "PROCESSING" | "FAILED" | "PROCESSED" | "ABORTED"
export type EventType = "COMMAND" | "QUERY" | "EVENT"
export type ScheduledEventStatus = "CREATED" | "FAILED" | "PROCESSED" | "ABORTED"

export interface EventsTable {
  eventId: string
  eventName: string
  streamId: string
  event: ColumnType<unknown, string, string>
  timestamp: ColumnType<Date, Date | string, Date | string>
  status: EventStatus
  type: EventType
  meta: ColumnType<unknown | null, string | null, string | null>
  retryCount: Generated<number>
  lockedAt: ColumnType<Date | null, Date | string | null, Date | string | null>
  lockedBy: string | null
  nextRetryAt: ColumnType<Date | null, Date | string | null, Date | string | null>
}

export interface ScheduledEventsTable {
  scheduledEventId: string
  executeAt: ColumnType<Date, Date | string, Date | string>
  event: ColumnType<unknown, string, string>
  status: ScheduledEventStatus
}

export interface Database {
  events: EventsTable
  scheduledEvents: ScheduledEventsTable
}
