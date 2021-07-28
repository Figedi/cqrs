import { In } from "typeorm";
import { EventEntity } from "./EventEntity";
import { IEventStore, IPersistedEvent, IScopeProvider } from "./types";

export class PersistentEventStore implements IEventStore {
  constructor(private scopeProvider: IScopeProvider) {}

  public withTransactionalScope(scopeProvider: IScopeProvider): PersistentEventStore {
    return new PersistentEventStore(scopeProvider);
  }

  public async insert(event: IPersistedEvent): Promise<void> {
    await this.em.getRepository(EventEntity).insert(event);
  }

  public async updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void> {
    await this.em.getRepository(EventEntity).update({ eventId }, event);
  }

  public async findByEventId(eventId: string): Promise<IPersistedEvent | undefined> {
    return this.em.getRepository(EventEntity).findOne({ eventId });
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

  private get em() {
    return this.scopeProvider();
  }
}
