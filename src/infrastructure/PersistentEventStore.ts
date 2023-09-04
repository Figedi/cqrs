import { In } from "typeorm";
import { omitBy, isNil } from "lodash-es";
import { EventEntity } from "./EventEntity.js";
import type { EventTypes, IEventStore, IPersistedEvent, IScopeProvider } from "./types.js";
import type { QueryDeepPartialEntity } from "typeorm/query-builder/QueryPartialEntity.js";

export class PersistentEventStore implements IEventStore {
  constructor(private scopeProvider: IScopeProvider) {}

  public withTransactionalScope(scopeProvider: IScopeProvider): PersistentEventStore {
    return new PersistentEventStore(scopeProvider);
  }

  public async insert(event: IPersistedEvent): Promise<void> {
    await this.em.getRepository(EventEntity).insert(event as QueryDeepPartialEntity<EventEntity>);
  }

  public async updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void> {
    await this.em.getRepository(EventEntity).update({ eventId }, event as QueryDeepPartialEntity<EventEntity>);
  }

  public async findByEventIds(
    eventIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    return this.em
      .getRepository(EventEntity)
      .find({ where: omitBy({ eventId: In(eventIds), type }, isNil), ...(fields ? { select: fields } : {}) });
  }

  public async findUnprocessedCommands(fields?: (keyof IPersistedEvent)[]): Promise<IPersistedEvent[]> {
    return (await this.em.getRepository(EventEntity).find({
      where: { type: "COMMAND", status: "CREATED" },
      ...(fields ? { select: fields } : {}),
    })) as IPersistedEvent[];
  }

  public async findByStreamIds(
    streamIds: string[],
    fields?: (keyof IPersistedEvent)[],
    type?: EventTypes,
  ): Promise<IPersistedEvent[]> {
    return (await this.em.getRepository(EventEntity).find({
      where: omitBy({ streamId: In(streamIds), type }, isNil),
      ...(fields ? { select: fields } : {}),
    })) as IPersistedEvent[];
  }

  private get em() {
    return this.scopeProvider();
  }
}
