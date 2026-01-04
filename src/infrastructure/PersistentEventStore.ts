import type { EventTypes, IEventStore, IPersistedEvent } from "./types.js";
import { camelCase, isNil, omitBy, snakeCase } from "lodash-es";

import type { IPostgresSettings, ITransactionalScope } from "../types.js";
import { PoolClient } from "pg";

export class PersistentEventStore implements IEventStore {
  constructor(private opts: IPostgresSettings) {}

  public async preflight() {
    if (!this.opts.runMigrations) {
      return;
    }
    const client = await this.opts.pool.connect()
    try {
  // Create base table
      await client.query(`CREATE TABLE IF NOT EXISTS "events" (
        event_id TEXT PRIMARY KEY,
        event_name TEXT NOT NULL,
        stream_id TEXT NOT NULL,
        event JSONB NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        type TEXT NOT NULL,
        meta JSONB
      )`);

      // Add columns for distributed processing (idempotent)
      await client.query(`
        ALTER TABLE events ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
        ALTER TABLE events ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
        ALTER TABLE events ADD COLUMN IF NOT EXISTS locked_by TEXT;
        ALTER TABLE events ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ DEFAULT NOW();
      `);

      // Create polling index (drop and recreate to ensure correct definition)
      await client.query(`
        DROP INDEX IF EXISTS idx_events_pending_poll;
        CREATE INDEX idx_events_pending_poll
        ON events (status, type, next_retry_at, timestamp)
        WHERE status IN ('CREATED', 'FAILED');
      `);

      // Create stale lock cleanup index
      await client.query(`
        DROP INDEX IF EXISTS idx_events_stale_locks;
        CREATE INDEX idx_events_stale_locks
        ON events (locked_at)
        WHERE locked_at IS NOT NULL AND status = 'PROCESSING';
      `);

      // Create LISTEN/NOTIFY function for executeSync
      await client.query(`
        CREATE OR REPLACE FUNCTION notify_event_status_change()
        RETURNS TRIGGER AS $$
        BEGIN
          IF NEW.status IN ('PROCESSED', 'FAILED', 'ABORTED') THEN
            PERFORM pg_notify(
              'cqrs_event_status',
              json_build_object(
                'event_id', NEW.event_id,
                'status', NEW.status
              )::text
            );
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS event_status_notify ON events;
        CREATE TRIGGER event_status_notify
        AFTER UPDATE OF status ON events
        FOR EACH ROW
        EXECUTE FUNCTION notify_event_status_change();
      `);
    } finally {
client.release()
    }
    
  }

  public async insert(
    event: IPersistedEvent,
    { allowUpsert = false, scope }: { allowUpsert?: boolean; scope?: ITransactionalScope } = {},
  ): Promise<void> {
    const client = scope ?? (await this.opts.pool.connect());

    const columns: string[] = [];
    const values: unknown[] = [];
    const placeholders: string[] = [];

    const data = omitBy(
      {
        event_id: event.eventId,
        event_name: event.eventName,
        stream_id: event.streamId,
        event: event.event ? JSON.stringify(event.event) : undefined,
        timestamp: event.timestamp,
        status: event.status,
        type: event.type,
        meta: event.meta ? JSON.stringify(event.meta) : undefined,
        retry_count: event.retryCount ?? 0,
        locked_at: event.lockedAt,
        locked_by: event.lockedBy,
        next_retry_at: event.nextRetryAt ?? event.timestamp,
      },
      isNil,
    );

    let idx = 1;
    for (const [key, value] of Object.entries(data)) {
      columns.push(key);
      values.push(value);
      placeholders.push(`$${idx++}`);
    }

    try {
 if (allowUpsert) {
      // Build update clause for upsert (exclude event_id from updates)
      const updateClauses = columns
        .filter((col) => col !== "event_id")
        .map((col) => `${col} = EXCLUDED.${col}`)
        .join(", ");

      await client!.query(
        `INSERT INTO events (${columns.join(", ")}) VALUES (${placeholders.join(", ")})
         ON CONFLICT (event_id) DO UPDATE SET ${updateClauses}`,
        values,
      );
    } else {
      await client!.query(
        `INSERT INTO events (${columns.join(", ")}) VALUES (${placeholders.join(", ")})`,
        values,
      );
    }
    } finally {
      if (!scope) {
        (client as PoolClient).release()
      }
    }

   
  }

  public async find(query: Partial<IPersistedEvent>): Promise<IPersistedEvent[]> {
    if (!Object.keys(query).length) {
      return [];
    }

    const conditions: string[] = [];
    const values: unknown[] = [];
    let idx = 1;

    const searchQuery = omitBy(
      {
        event_id: query.eventId,
        event_name: query.eventName,
        stream_id: query.streamId,
        timestamp: query.timestamp,
        status: query.status,
        type: query.type,
      },
      isNil,
    );

    for (const [key, value] of Object.entries(searchQuery)) {
      conditions.push(`${key} = $${idx++}`);
      values.push(value);
    }

    const { rows } = await client.query(
      `SELECT * FROM events WHERE ${conditions.join(" AND ")}`,
      values,
    );

    return rows.map(this.mapRowToEvent);
  }

  public async updateByEventId(
    eventId: string,
    event: Partial<IPersistedEvent>,
    { scope }: { scope?: ITransactionalScope } = {},
  ): Promise<void> {
    const client = scope ?? (await this.opts.pool.connect())

    const updates: string[] = [];
    const values: unknown[] = [];
    let idx = 1;

    const data = omitBy(
      {
        event_id: event.eventId,
        event_name: event.eventName,
        stream_id: event.streamId,
        event: event.event ? JSON.stringify(event.event) : undefined,
        timestamp: event.timestamp,
        status: event.status,
        type: event.type,
        meta: event.meta ? JSON.stringify(event.meta) : undefined,
        retry_count: event.retryCount,
        locked_at: event.lockedAt,
        locked_by: event.lockedBy,
        next_retry_at: event.nextRetryAt,
      },
      isNil,
    );

    for (const [key, value] of Object.entries(data)) {
      updates.push(`${key} = $${idx++}`);
      values.push(value);
    }

    values.push(eventId);
    await client!.query(
      `UPDATE events SET ${updates.join(", ")} WHERE event_id = $${idx}`,
      values,
    );
  }

  public async findByEventIds(
    eventIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    const mappedFields = fields?.length ? this.mapFieldsToCols(fields).join(", ") : "*";

    const placeholders = eventIds.map((_, i) => `$${i + 1}`).join(", ");
    const values: unknown[] = [...eventIds];

    let sql = `SELECT ${mappedFields} FROM events WHERE event_id IN (${placeholders})`;
    if (type) {
      values.push(type);
      sql += ` AND type = $${values.length}`;
    }

    const { rows } = await client.query(sql, values);
    return rows.map(this.mapRowToEvent);
  }

  public async findUnprocessedCommands(
    ignoredEventIds?: string[],
    fields?: (keyof IPersistedEvent)[],
  ): Promise<IPersistedEvent[]> {
    const mappedFields = fields?.length ? this.mapFieldsToCols(fields).join(", ") : "*";

    if (ignoredEventIds?.length) {
      const placeholders = ignoredEventIds.map((_, i) => `$${i + 1}`).join(", ");
      const { rows } = await client.query(
        `SELECT ${mappedFields} FROM events WHERE status = 'CREATED' AND type = 'COMMAND' AND event_id NOT IN (${placeholders})`,
        ignoredEventIds,
      );
      return rows.map(this.mapRowToEvent);
    }

    const { rows } = await client.query(
      `SELECT ${mappedFields} FROM events WHERE status = 'CREATED' AND type = 'COMMAND'`,
    );
    return rows.map(this.mapRowToEvent);
  }

  public async findByStreamIds(
    streamIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    const mappedFields = fields?.length ? this.mapFieldsToCols(fields).join(", ") : "*";

    const placeholders = streamIds.map((_, i) => `$${i + 1}`).join(", ");
    const values: unknown[] = [...streamIds];

    let sql = `SELECT ${mappedFields} FROM events WHERE stream_id IN (${placeholders})`;
    if (type) {
      values.push(type);
      sql += ` AND type = $${values.length}`;
    }

    const { rows } = await client.query(sql, values);
    return rows.map(this.mapRowToEvent);
  }

  private mapFieldsToCols(fields: (keyof IPersistedEvent)[]): string[] {
    return fields.map(snakeCase);
  }

  private mapRowToEvent(row: Record<string, any>): IPersistedEvent {
    return Object.entries(row).reduce((acc, [k, v]) => ({ ...acc, [camelCase(k)]: v }), {}) as IPersistedEvent;
  }
}
