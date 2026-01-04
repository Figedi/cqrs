import type { Transaction } from 'kysely';
import type { EventTypes, IEventStore, IPersistedEvent } from "./types.js";
import type { IPostgresSettings } from "../types.js";
import type { Database, KyselyDb } from "./db/index.js";
import { runEventsMigration } from "./db/index.js";

export class PersistentEventStore implements IEventStore {
  constructor(private opts: IPostgresSettings) {}

  private get db(): KyselyDb {
    return this.opts.db;
  }

  public async preflight(): Promise<void> {
    if (!this.opts.runMigrations) {
      return;
    }
    await runEventsMigration(this.db);
  }

  public async insert(
    event: IPersistedEvent,
    { allowUpsert = false, scope }: { allowUpsert?: boolean; scope?: Transaction<Database> } = {},
  ): Promise<void> {
    const db = scope ?? this.db;

    const data = {
      eventId: event.eventId,
      eventName: event.eventName,
      streamId: event.streamId,
      event: JSON.stringify(event.event),
      timestamp: event.timestamp,
      status: event.status,
      type: event.type,
      meta: event.meta ? JSON.stringify(event.meta) : null,
      retryCount: event.retryCount ?? 0,
      lockedAt: event.lockedAt ?? null,
      lockedBy: event.lockedBy ?? null,
      nextRetryAt: event.nextRetryAt ?? event.timestamp,
    };

    if (allowUpsert) {
      await db
        .insertInto('events')
        .values(data)
        .onConflict((oc) =>
          oc.column('eventId').doUpdateSet({
            eventName: (eb) => eb.ref('excluded.eventName'),
            streamId: (eb) => eb.ref('excluded.streamId'),
            event: (eb) => eb.ref('excluded.event'),
            timestamp: (eb) => eb.ref('excluded.timestamp'),
            status: (eb) => eb.ref('excluded.status'),
            type: (eb) => eb.ref('excluded.type'),
            meta: (eb) => eb.ref('excluded.meta'),
            retryCount: (eb) => eb.ref('excluded.retryCount'),
            lockedAt: (eb) => eb.ref('excluded.lockedAt'),
            lockedBy: (eb) => eb.ref('excluded.lockedBy'),
            nextRetryAt: (eb) => eb.ref('excluded.nextRetryAt'),
          })
        )
        .execute();
    } else {
      await db.insertInto('events').values(data).execute();
    }
  }

  public async find(query: Partial<IPersistedEvent>): Promise<IPersistedEvent[]> {
    if (!Object.keys(query).length) {
      return [];
    }

    let qb = this.db.selectFrom('events').selectAll();

    if (query.eventId) qb = qb.where('eventId', '=', query.eventId);
    if (query.eventName) qb = qb.where('eventName', '=', query.eventName);
    if (query.streamId) qb = qb.where('streamId', '=', query.streamId);
    if (query.status) qb = qb.where('status', '=', query.status);
    if (query.type) qb = qb.where('type', '=', query.type);

    const rows = await qb.execute();
    return rows as unknown as IPersistedEvent[];
  }

  public async updateByEventId(
    eventId: string,
    event: Partial<IPersistedEvent>,
    { scope }: { scope?: Transaction<Database> } = {},
  ): Promise<void> {
    const db = scope ?? this.db;

    const updateData: Record<string, unknown> = {};

    if (event.eventName !== undefined) updateData.eventName = event.eventName;
    if (event.streamId !== undefined) updateData.streamId = event.streamId;
    if (event.event !== undefined) updateData.event = JSON.stringify(event.event);
    if (event.timestamp !== undefined) updateData.timestamp = event.timestamp;
    if (event.status !== undefined) updateData.status = event.status;
    if (event.type !== undefined) updateData.type = event.type;
    if (event.meta !== undefined) updateData.meta = JSON.stringify(event.meta);
    if (event.retryCount !== undefined) updateData.retryCount = event.retryCount;
    if (event.lockedAt !== undefined) updateData.lockedAt = event.lockedAt;
    if (event.lockedBy !== undefined) updateData.lockedBy = event.lockedBy;
    if (event.nextRetryAt !== undefined) updateData.nextRetryAt = event.nextRetryAt;

    if (Object.keys(updateData).length === 0) {
      return;
    }

    await db
      .updateTable('events')
      .set(updateData)
      .where('eventId', '=', eventId)
      .execute();
  }

  public async findByEventIds(
    eventIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    if (!eventIds.length) {
      return [];
    }

    let qb = this.db.selectFrom('events');

    // Select specific fields or all
    if (fields?.length) {
      qb = qb.select(fields as (keyof IPersistedEvent & string)[]);
    } else {
      qb = qb.selectAll();
    }

    qb = qb.where('eventId', 'in', eventIds);

    if (type) {
      qb = qb.where('type', '=', type);
    }

    const rows = await qb.execute();
    return rows as unknown as IPersistedEvent[];
  }

  public async findUnprocessedCommands(
    ignoredEventIds?: string[],
    fields?: (keyof IPersistedEvent)[],
  ): Promise<IPersistedEvent[]> {
    let qb = this.db.selectFrom('events');

    // Select specific fields or all
    if (fields?.length) {
      qb = qb.select(fields as (keyof IPersistedEvent & string)[]);
    } else {
      qb = qb.selectAll();
    }

    qb = qb
      .where('status', '=', 'CREATED')
      .where('type', '=', 'COMMAND');

    if (ignoredEventIds?.length) {
      qb = qb.where('eventId', 'not in', ignoredEventIds);
    }

    const rows = await qb.execute();
    return rows as unknown as IPersistedEvent[];
  }

  public async findByStreamIds(
    streamIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    if (!streamIds.length) {
      return [];
    }

    let qb = this.db.selectFrom('events');

    // Select specific fields or all
    if (fields?.length) {
      qb = qb.select(fields as (keyof IPersistedEvent & string)[]);
    } else {
      qb = qb.selectAll();
    }

    qb = qb.where('streamId', 'in', streamIds);

    if (type) {
      qb = qb.where('type', '=', type);
    }

    const rows = await qb.execute();
    return rows as unknown as IPersistedEvent[];
  }
}
