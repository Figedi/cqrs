import { IMeta, ISerializedEvent, TransactionalScope } from "../types";

export interface IPersistedEvent<TEventPayload = any, TMeta extends IMeta = IMeta> {
  eventId: string;
  eventName: string;
  streamId: string;
  event: ISerializedEvent<TEventPayload>;
  timestamp: Date;
  status: "CREATED" | "PROCESSING" | "FAILED" | "PROCESSED";
  type: "COMMAND" | "QUERY" | "EVENT";
  meta?: TMeta;
}

export interface IEventStore {
  withTransactionalScope(scope: TransactionalScope): IEventStore;
  insert(event: IPersistedEvent): Promise<void>;
  updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void>;
  findByEventId(eventId: string): Promise<IPersistedEvent>;
  findUnprocessedCommands<TKeys extends keyof IPersistedEvent>(
    fields?: TKeys[],
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
  findByStreamIds<TKeys extends keyof IPersistedEvent>(
    streamIds: string[],
    fields?: TKeys[],
    type?: IPersistedEvent["type"],
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
}

export type IScopeProvider = () => TransactionalScope;
