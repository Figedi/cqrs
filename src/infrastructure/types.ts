import type { AnyEither, ExecuteOpts, ICommand, IMeta, ISerializedEvent, StringEither } from "../types.js";

export interface IScheduleOptions extends ExecuteOpts {
  executeSync?: boolean;
}

export interface IEventScheduler {
  scheduleCommand<TPayload extends Record<string, any>, TRes extends AnyEither>(
    command: ICommand<TPayload, TRes>,
    executeAt: Date,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ): Promise<string>;

  updateScheduledEventStatus(command: ICommand, status: "CREATED" | "FAILED" | "PROCESSED"): Promise<void>;

  reset(): Promise<number>;
}

export type EventTypes = "COMMAND" | "QUERY" | "EVENT";

export interface IPersistedEvent<TEventPayload = any, TMeta extends IMeta = IMeta> {
  eventId: string;
  eventName: string;
  streamId: string;
  event: ISerializedEvent<TEventPayload>;
  timestamp: Date;
  status: "CREATED" | "PROCESSING" | "FAILED" | "PROCESSED";
  type: EventTypes;
  meta?: TMeta;
}

export interface IEventStore {
  insert(event: IPersistedEvent): Promise<void>;
  updateByEventId(eventId: string, event: Partial<IPersistedEvent>): Promise<void>;
  findUnprocessedCommands<TKeys extends keyof IPersistedEvent>(
    ignoredEventIds?: string[],
    fields?: TKeys[],
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
  findByEventIds<TKeys extends keyof IPersistedEvent>(
    eventIds: string[],
    fields?: TKeys[],
    type?: EventTypes,
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
  findByStreamIds<TKeys extends keyof IPersistedEvent>(
    streamIds: string[],
    fields?: TKeys[],
    type?: EventTypes,
  ): Promise<Pick<IPersistedEvent, TKeys>[]>;
}
