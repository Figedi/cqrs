import * as db from "zapatos/db";

import type { EventTypes, IEventStore, IPersistedEvent } from "./types.js";
import type { Pool, PoolClient } from "pg";
import { camelCase, isNil, omitBy, snakeCase } from "lodash-es";

import type { IPersistentSettingsWithClient } from "../types.js";

export class PersistentEventStore implements IEventStore {
  private _pool?: Pool | PoolClient;
  constructor(private opts: IPersistentSettingsWithClient) {}

  public async preflight() {
    if (this.opts.runMigrations) {
      await this.pool.query(`CREATE TABLE IF NOT EXISTS "events" (
        event_id TEXT PRIMARY KEY,
        event_name TEXT NOT NULL,
        stream_id TEXT NOT NULL,
        event JSONB NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        type TEXT NOT NULL,
        meta JSONB
      )`);
    }
  }

  public async insert(event: IPersistedEvent): Promise<void> {
    const insertable = omitBy(
      {
        event_id: event.eventId,
        event_name: event.eventName,
        stream_id: event.streamId,
        event: event.event ? db.param(event.event as {}, true) : undefined,
        timestamp: event.timestamp,
        status: event.status,
        type: event.type,
        meta: event.meta ? db.param(event.meta as {}, true) : undefined,
      },
      isNil,
    ) as any;
    await db.insert("events", insertable).run(this.pool);
  }

  public async updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void> {
    await db
      .update(
        "events",
        omitBy(
          {
            event_id: event.eventId,
            event_name: event.eventName,
            stream_id: event.streamId,
            event: event.event ? db.param(event.event, true) : undefined,
            timestamp: event.timestamp,
            status: event.status,
            type: event.type,
            meta: event.meta ? db.param(event.meta, true) : undefined,
          },
          isNil,
        ),
        { event_id: eventId },
      )
      .run(this.pool);
  }

  public async findByEventIds(
    eventIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    const rows = await db
      .select(
        "events",
        omitBy(
          {
            event_id: db.sql`${"event_id"} IN (${db.vals(eventIds)})`,
            type,
          },
          isNil,
        ),
        fields?.length ? { columns: this.mapFieldsToCols(fields) } : undefined,
      )
      .run(this.pool);
    return rows.map(this.mapRowToEvent);
  }

  public async findUnprocessedCommands(
    ignoredEventIds?: string[],
    fields?: (keyof IPersistedEvent)[],
  ): Promise<IPersistedEvent[]> {
    const extra = ignoredEventIds?.length
      ? ` AND event_id NOT IN (${ignoredEventIds.map(e => `'${e}'`).join(",")})`
      : "";
    const mappedFields = fields?.length ? `${this.mapFieldsToCols(fields).join(",")}` : "*";
    const { rows } = await this.pool.query(
      `SELECT ${mappedFields} from events e WHERE e.status = 'CREATED' AND type = 'COMMAND'${extra}`,
    );

    return rows.map(this.mapRowToEvent);
  }

  public async findByStreamIds(
    streamIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    const rows = await db
      .select(
        "events",
        omitBy(
          {
            streamId: db.sql`${"stream_id"} IN (${db.vals(streamIds)})`,
            type,
          },
          isNil,
        ),
        fields?.length ? { columns: this.mapFieldsToCols(fields) } : undefined,
      )
      .run(this.pool);
    return rows.map(this.mapRowToEvent);
  }

  private mapFieldsToCols(fields: (keyof IPersistedEvent)[]): any[] {
    return fields.map(snakeCase) as any[];
  }

  private mapRowToEvent(row: Record<string, any>): IPersistedEvent {
    return Object.entries(row).reduce((acc, [k, v]) => ({ ...acc, [camelCase(k)]: v }), {}) as IPersistedEvent;
  }

  private get pool() {
    if (!this._pool) {
      this._pool = this.opts.client;
    }
    return this._pool;
  }
}
