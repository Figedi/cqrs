import { left } from "fp-ts/lib/Either";
import { Observable } from "rxjs";
import { filter } from "rxjs/operators";
import { getManager } from "typeorm";
import { v4 as uuid } from "uuid";

import { ApplicationError } from "./errors";
import { IScopeProvider } from "./infrastructure/types";
import {
  AnyEither,
  CQRSEventType,
  ClassContextProvider,
  Constructor,
  HandlerContext,
  ICommand,
  ICommandHandler,
  IEvent,
  IHandlerConfig,
  IQuery,
  IQueryHandler,
  ISerializedEvent,
  StringEither,
} from "./types";

export const createScopeProvider = (persistence: "inmem" | "pg"): IScopeProvider => () => {
  if (persistence === "pg") {
    return getManager();
  }
  return {} as any;
};

export const ofType = <TInput extends IEvent, TOutput extends IEvent>(...events: Constructor<TOutput>[]) => {
  const isInstanceOf = (event: IEvent): event is TOutput =>
    !!events.find(classType => event.constructor.name === classType.name || event instanceof classType);
  return (source: Observable<TInput>): Observable<TOutput> => source.pipe(filter(isInstanceOf));
};

export const serializeEvent = <T extends ICommand<TPayload> | IEvent<TPayload> | IQuery<TPayload>, TPayload>(
  ev: T,
): ISerializedEvent<TPayload> => ({
  meta: ev.meta,
  payload: ev.payload,
});

export const deserializeEvent = <TPayload>(
  { meta, payload }: ISerializedEvent<TPayload>,
  klass: ICommand | IQuery | IEvent,
): ICommand<TPayload> | IQuery<TPayload> | IEvent<TPayload> => {
  const instance = new ((klass as any) as Constructor<ICommand | IQuery | IEvent>)();
  instance.meta = meta;
  instance.payload = payload;
  return instance;
};

// todo: the tx-scope must be in here
export const mergeObjectContext = <T extends IQuery | ICommand | IEvent>(
  ctxProvider: ClassContextProvider,
  klass: T,
  handlerCtx: HandlerContext,
) => {
  // eslint-disable-next-line no-param-reassign
  klass.publish = async (delayUntilNextTick?: boolean): Promise<StringEither | AnyEither> => {
    const classType = klass.meta?.classType;
    const ctx = ctxProvider();
    if (classType === CQRSEventType.EVENT) {
      return ctx.eventBus.execute(klass as IEvent, {
        scope: handlerCtx.scope,
        delayUntilNextTick,
      });
    }
    if (classType === CQRSEventType.COMMAND) {
      return ctx.commandBus.execute(klass as ICommand, {
        scope: handlerCtx.scope,
        delayUntilNextTick,
      });
    }
    if (classType === CQRSEventType.QUERY) {
      return ctx.queryBus.execute(klass as IQuery, {
        scope: handlerCtx.scope,
        delayUntilNextTick,
      });
    }
    throw new ApplicationError(`Unknown classType: ${String(classType)} for class: ${klass.constructor.name}`);
  };
  return klass;
};

export const createQuery = <TPayload, TRes extends AnyEither = AnyEither>(streamId?: string, name?: string) => {
  const C = class implements IQuery<TPayload, TRes> {
    public meta = {
      classType: CQRSEventType.QUERY,
      className: this.className,
      streamId,
      eventId: uuid(),
    };

    constructor(public payload: TPayload) {}

    public get className() {
      return name || this.constructor.name;
    }

    public async publish() {
      return left(new ApplicationError("Querybus not found, please call mergeObjectContext() first")) as TRes;
    }
  };
  if (name) {
    Object.defineProperty(C, "name", { value: name });
  }
  return C;
};

export const createEvent = <TPayload, TRes extends StringEither = StringEither>(streamId?: string, name?: string) => {
  const C = class implements IEvent<TPayload, TRes> {
    public meta = {
      classType: CQRSEventType.EVENT,
      className: this.className,
      streamId,
      eventId: uuid(),
    };

    constructor(public payload: TPayload) {}

    public get className() {
      return name || this.constructor.name;
    }

    public async publish() {
      return left(new ApplicationError("EventBus not found, please call mergeObjectContext() first")) as TRes;
    }
  };

  if (name) {
    Object.defineProperty(C, "name", { value: name });
  }
  return C;
};

export const mergeWithParentEvent = (command: ICommand, parent: IEvent): ICommand => {
  const streamId = parent.meta?.streamId || parent.meta?.eventId;
  // eslint-disable-next-line no-param-reassign
  command.meta = { ...(command.meta || {}), streamId };
  return command;
};

export const mergeWithParentCommand = (event: IEvent, parent: ICommand): IEvent => {
  const streamId = parent.meta?.streamId || parent.meta?.eventId;
  // eslint-disable-next-line no-param-reassign
  event.meta = { ...(event.meta || {}), streamId };
  return event;
};

export const createCommand = <TPayload, TRes extends AnyEither = AnyEither>(streamId?: string, name?: string) => {
  const C = class implements ICommand<TPayload, TRes> {
    public meta = {
      classType: CQRSEventType.COMMAND,
      className: this.className,
      streamId,
      eventId: uuid(),
    };

    constructor(public payload: TPayload) {}

    public get className() {
      return name || this.constructor.name;
    }

    public async publish() {
      return left(new ApplicationError("CommandBus not found, please call mergeObjectContext() first")) as TRes;
    }
  };

  if (name) {
    Object.defineProperty(C, "name", { value: name });
  }
  return C;
};

export const createCommandHandler = <TRes extends AnyEither, Command extends ICommand = any>(
  command: Command,
  name?: string,
) => {
  const C = class BaseCommandHandler implements ICommandHandler<Command, TRes> {
    public config: IHandlerConfig;

    public publishableEvents: IEvent[] = [];

    constructor(baseConfig?: Omit<IHandlerConfig, "topic" | "classType">) {
      this.config = {
        ...(baseConfig || {}),
        classType: CQRSEventType.COMMAND,
        topic: ((command as any) as Constructor<any>).name,
        handles: command,
      };
    }

    public apply(event: IEvent) {
      this.publishableEvents.push(event);
    }

    public setPublishableEvents(events: IEvent[]): void {
      this.publishableEvents = events;
    }

    public async handle(_c: Command, _ctx?: HandlerContext) {
      return left(new ApplicationError("BaseCommand-handler not usable by itself, please extend this class")) as TRes;
    }
  };

  if (name) {
    Object.defineProperty(C, "name", { value: name });
  }
  return C;
};

export const createQueryHandler = <TRes extends AnyEither, Query extends IQuery = any>(query: Query, name?: string) => {
  const C = class BaseQueryHandler implements IQueryHandler<Query, TRes> {
    public config: IHandlerConfig;

    constructor(baseConfig?: Omit<IHandlerConfig, "topic" | "classType">) {
      this.config = {
        ...(baseConfig || {}),
        classType: CQRSEventType.QUERY,
        topic: ((query as any) as Constructor<any>).name,
        handles: query,
      };
    }

    public async handle(_q: Query) {
      return left(new ApplicationError("BaseQuery-handler not usable by itself, please extend this class")) as TRes;
    }
  };

  if (name) {
    Object.defineProperty(C, "name", { value: name });
  }
  return C;
};

export const isCommandHandler = (
  handler: ICommandHandler<any, any> | IQueryHandler<any, any>,
): handler is ICommandHandler<any, any> => {
  return handler.config.classType === CQRSEventType.COMMAND;
};
