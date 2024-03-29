import type { Logger, ServiceWithLifecycleHandlers } from "@figedi/svc";
import type { Pool, PoolClient } from "pg";

import type { Either } from "fp-ts/lib/Either.js";
import type { ErrorObject } from "serialize-error";
import type { IPersistedEvent } from "./infrastructure/types.js";
import type { Observable } from "rxjs";
import type { Option } from "fp-ts/lib/Option.js";
import type { RetryBackoffConfig } from "backoff-rxjs";
import type { UowTxSettings } from "./decorators/UowDecorator.js";

export type ScheduledEventStatus = "CREATED" | "FAILED" | "ABORTED" | "PROCESSED";
export interface Constructor<T> extends Function {
  new (...args: any[]): T;
}
export interface IMeta {
  lastCalled?: Date;
  error?: Error | ErrorObject;
}

export enum CQRSEventType {
  QUERY = "QUERY",
  COMMAND = "COMMAND",
  EVENT = "EVENT",
}

export interface IEventMeta {
  className: string;
  classType: CQRSEventType;
  streamId?: string;
  eventId?: string;
  transient?: boolean;
}

export interface IInmemorySettings {
  type: "inmem";
}
export interface IPersistentSettings {
  type: "pg";
  client?: Pool;
  runMigrations?: boolean;
  options?: Record<string, any>;
}
export interface IPersistentSettingsWithClient extends IPersistentSettings {
  client: Pool;
}
export type IPersistenceSettings = IInmemorySettings | IPersistentSettings;
export type IPersistenceSettingsWithClient = IInmemorySettings | IPersistentSettingsWithClient;

export interface ICQRSSettings {
  persistence: IPersistenceSettings;
  transaction: UowTxSettings;
}

export type ITransactionalScope = PoolClient | Pool;

export type VoidEither<TError = any> = Either<TError, Option<never>>;
export type StringEither<TError = any> = Either<TError, string>;
export type AnyEither = Either<any, any>;

export interface ICommand<TPayload = any, TRes extends AnyEither = AnyEither> {
  meta: IEventMeta;
  payload: TPayload;
  publish(): Promise<TRes>;
}

export interface IQuery<TPayload = any, TRes extends AnyEither = AnyEither> {
  meta: IEventMeta;
  payload: TPayload;
  publish(): Promise<TRes>;
}

export interface IEvent<TPayload = any, TRes extends StringEither = StringEither> {
  meta: IEventMeta;
  payload: TPayload;
  publish(delayUntilNextTick?: boolean): Promise<TRes>;
}

export interface IScheduledEvent {
  scheduledEventId: string;
  executeAt: Date;
  eventId: string;
  event?: ICommand;
  status: ScheduledEventStatus;
}

export interface ISaga<TPayload = any> {
  process(events$: Observable<IEvent<TPayload, any>>): Observable<ICommand>;
}

export interface HandlerContext {
  logger: Logger;
  scope: ITransactionalScope;
}

export interface IUniformRetryOpts {
  maxRetries: number;
}

export interface IHandlerConfig<THandler = ICommand | IQuery | IEvent> {
  handles?: Constructor<THandler>;
  topic: string; // this is the className of the Query / Command
  maxPerSecond?: number;
  concurrency?: number;
  retries?:
    | number
    | {
        mode: "UNIFORM" | "EXPONENTIAL";
        opts?: RetryBackoffConfig | IUniformRetryOpts;
      };
  classType: CQRSEventType;
}

export interface IProcessResult<TRes> {
  meta: {
    timeTakenMs: number;
  };
  eventId: string;
  payload: TRes;
}
export interface ExecuteOpts {
  transient?: boolean;
  timeout?: number;
  delayUntilNextTick?: boolean;
  scope?: ITransactionalScope;
  streamId?: string;
  eventId?: string;
}

export interface ISerializedEvent<TPayload> {
  meta: IEventMeta;
  payload: TPayload;
}

export interface IQueryHandler<Query extends IQuery, TRes extends AnyEither> {
  config: IHandlerConfig;
  handle(query: Query): Promise<TRes>;
}

export interface ICommandHandler<Command extends ICommand, TRes extends AnyEither> {
  config: IHandlerConfig;
  setPublishableEvents: (events: IEvent[]) => void;
  publishableEvents: IEvent[];
  handle(command: Command, ctx?: HandlerContext): Promise<TRes>;
}

export interface IDecorator {
  decorate<T extends ICommand | IQuery, TRes extends AnyEither>(
    handler: ICommandHandler<T, TRes> | IQueryHandler<T, TRes>,
  ): ICommandHandler<T, TRes> | IQueryHandler<T, TRes>;
}

export interface IEventBus extends ServiceWithLifecycleHandlers {
  execute<TPayload, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<TPayload, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes>;
  deserializeEvent(event: IPersistedEvent): IEvent;
  replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]>;
  register(...events: Constructor<IEvent>[]): void;
  registerSagas(...saga: ISaga[]): void;
  stream(): Observable<IEvent>;
}

export interface IQueryBus extends ServiceWithLifecycleHandlers {
  registerDecorator(decorator: IDecorator): void;
  deserializeQuery(query: IPersistedEvent): IQuery;
  execute<TPayload, TRes extends AnyEither>(query: IQuery<TPayload, TRes>, opts?: ExecuteOpts): Promise<TRes>;
  register(...handlers: IQueryHandler<any, any>[]): void;
  stream(): Observable<IQuery>;
}

export interface ICommandBus extends ServiceWithLifecycleHandlers {
  registeredCommands: Constructor<ICommand>[];

  registerDecorator(decorator: IDecorator): void;
  drain(ignoredEventIds?: string[]): Promise<void>;
  deserializeCommand(commands: IPersistedEvent): ICommand;
  executeSync<TPayload, TCommandRes extends AnyEither>(
    command: ICommand<TPayload, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TCommandRes>;
  execute<TPayload, TRes extends StringEither, TCommandRes extends AnyEither>(
    command: ICommand<TPayload, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes>;
  replayAllFailed(opts?: ExecuteOpts): Promise<void>;
  replay<TPayload, TRes extends StringEither, TCommandRes extends AnyEither>(
    commandOrEventId: string | ICommand<TPayload, TCommandRes>,
  ): Promise<TRes>;
  register(...handlers: ICommandHandler<any, any>[]): void;
  stream(topic?: string): Observable<ICommand>;
}

export interface IClassContext {
  eventBus: IEventBus;
  commandBus: ICommandBus;
  queryBus: IQueryBus;
}

export type ClassContextProvider = () => IClassContext;
