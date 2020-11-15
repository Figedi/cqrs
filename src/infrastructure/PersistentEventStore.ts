import { getManager, In } from "typeorm";
import { TransactionalScope } from "../types";
import { EventEntity } from "./EventEntity";
import { IEventStore, IPersistedEvent } from "./types";

export class PersistentEventStore implements IEventStore {
  constructor(private em = getManager()) {}

  public withTransactionalScope(em: TransactionalScope): PersistentEventStore {
    return new PersistentEventStore(em);
  }

  public async insert(event: IPersistedEvent): Promise<void> {
    await this.em.getRepository(EventEntity).insert(event);
  }

  public async updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void> {
    await this.em.getRepository(EventEntity).update({ eventId }, event);
  }

  public async findByEventId(eventId: string): Promise<IPersistedEvent> {
    return (await this.em.getRepository(EventEntity).findOne({ eventId })) as IPersistedEvent;
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
    type?: IPersistedEvent["type"],
  ): Promise<IPersistedEvent[]> {
    return (await this.em
      .getRepository(EventEntity)
      .find({ where: { streamId: In(streamIds), type }, select: fields })) as IPersistedEvent[];
  }
}
