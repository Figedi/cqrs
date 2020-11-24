import type { ErrorObject } from "serialize-error";
import type { Logger, ServiceWithLifecycleHandlers } from "@figedi/svc";
import { Either } from "fp-ts/lib/Either";
import { Option } from "fp-ts/lib/Option";
import { Observable } from "rxjs";
import { EntityManager } from "typeorm";

export interface IDecorator {
  decorate<T extends ICommand | IQuery, TRes extends AnyEither>(
    handler: ICommandHandler<T, TRes> | IQueryHandler<T, TRes>,
  ): ICommandHandler<T, TRes> | IQueryHandler<T, TRes>;
}

export interface ISaga<TPayload = any> {
  process(events$: Observable<IEvent<TPayload, any>>): Observable<ICommand>;
}

export interface IMeta {
  lastCalled?: Date;
  error?: Error | ErrorObject;
}
export type VoidEither<TError = any> = Either<TError, Option<never>>;
export type StringEither<TError = any> = Either<TError, string>;
export type AnyEither = Either<any, any>;
export type TransactionalScope = EntityManager;

export interface HandlerContext {
  logger: Logger;
  scope: TransactionalScope;
}

export interface IHandlerConfig {
  topic: string; // this is the className of the Query / Command
  maxPerSecond?: number;
  concurrency?: number;
  maxRetries?: number;
  handles?: ICommand | IQuery | IEvent;
  classType: CQRSEventType;
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

export interface IProcessResult<TRes> {
  meta: {
    timeTakenMs: number;
  };
  eventId: string;
  payload: TRes;
}

export interface IQueryBus extends ServiceWithLifecycleHandlers {
  registerDecorator(decorator: IDecorator): void;
  execute<TPayload, TRes extends AnyEither>(query: IQuery<TPayload, TRes>, opts?: ExecuteOpts): Promise<TRes>;
  register(...handlers: IQueryHandler<any, any>[]): void;
  stream(): Observable<IQuery>;
}

export interface ICommandBus extends ServiceWithLifecycleHandlers {
  registerDecorator(decorator: IDecorator): void;
  drain(): Promise<void>;
  executeSync<TPayload, TCommandRes extends AnyEither>(
    command: ICommand<TPayload, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TCommandRes>;
  execute<TPayload, TRes extends StringEither, TCommandRes extends AnyEither>(
    command: ICommand<TPayload, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes>;
  replay<TPayload, TRes extends StringEither, TCommandRes extends AnyEither>(
    command: ICommand<TPayload, TCommandRes>,
  ): Promise<TRes>;
  register(...handlers: ICommandHandler<any, any>[]): void;
  stream(topic?: string): Observable<ICommand>;
}

export interface ExecuteOpts {
  transient?: boolean;
  timeout?: number;
  delayUntilNextTick?: boolean;
  scope?: TransactionalScope;
}

export interface IEventBus extends ServiceWithLifecycleHandlers {
  execute<TPayload, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<TPayload, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes>;
  replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]>;
  register(...events: Constructor<IEvent>[]): void;
  registerSagas(...saga: ISaga[]): void;
  stream(): Observable<IEvent>;
}

export interface ISerializedEvent<TPayload> {
  meta: {
    className: string;
    classType: CQRSEventType;
    streamId?: string;
    eventId?: string;
  };
  payload: TPayload;
}

export interface ICommand<TPayload = any, TRes extends AnyEither = AnyEither> {
  meta: {
    className: string;
    classType: CQRSEventType;
    streamId?: string;
    eventId?: string;
  };
  payload: TPayload;
  publish(): Promise<TRes>;
}

export interface IQuery<TPayload = any, TRes extends AnyEither = AnyEither> {
  meta: {
    className: string;
    classType: CQRSEventType;
    streamId?: string;
    eventId?: string;
  };
  payload: TPayload;
  publish(): Promise<TRes>;
}

export interface IEvent<TPayload = any, TRes extends StringEither = StringEither> {
  meta: {
    className: string;
    classType: CQRSEventType;
    streamId?: string;
    eventId?: string;
  };
  payload: TPayload;
  publish(delayUntilNextTick?: boolean): Promise<TRes>;
}

export interface Constructor<T> extends Function {
  new (...args: any[]): T;
}

export interface IClassContext {
  eventBus: IEventBus;
  commandBus: ICommandBus;
  queryBus: IQueryBus;
}

export type ClassContextProvider = () => IClassContext;

export enum CQRSEventType {
  QUERY = "QUERY",
  COMMAND = "COMMAND",
  EVENT = "EVENT",
}
