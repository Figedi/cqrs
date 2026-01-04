import { isLeft, left, right } from "fp-ts/lib/Either.js";
import { v4 as uuid } from "uuid";
import { Subject, type Observable, firstValueFrom, filter, map, timeout } from "rxjs";
import { share } from "rxjs/operators";

import type {
  AnyEither,
  Constructor,
  ExecuteOpts,
  ICommand,
  ICommandBus,
  ICommandHandler,
  IDecorator,
  Logger,
  StringEither,
  VoidEither,
} from "../types.js";
import { ApplicationError, NoHandlerFoundError, TimeoutExceededError } from "../errors.js";
import type { IPersistedEvent } from "../infrastructure/types.js";

interface ICommandEnvelope {
  command: ICommand;
  streamId: string;
}

interface ICommandResult {
  streamId: string;
  className: string;
  result: AnyEither;
}

/**
 * InMemoryCommandBus processes commands synchronously in-memory.
 * Suitable for testing and single-process deployments.
 */
export class InMemoryCommandBus implements ICommandBus {
  public registeredCommands: Constructor<ICommand>[] = [];

  private handlers = new Map<string, ICommandHandler<any, any>>();
  private decorators: IDecorator[] = [];
  private in$ = new Subject<ICommandEnvelope>();
  private results$ = new Subject<ICommandResult>();
  private out$ = this.in$.pipe(share());

  constructor(private logger: Logger) {
    // Process commands as they arrive
    this.out$.subscribe(async (envelope) => {
      await this.processCommand(envelope);
    });
  }

  /**
   * Register a decorator for command handlers.
   */
  registerDecorator(decorator: IDecorator): void {
    this.decorators.push(decorator);
  }

  /**
   * Register command handlers.
   */
  register(...handlers: ICommandHandler<any, any>[]): void {
    for (const handler of handlers) {
      const topic = handler.config.topic;
      const decoratedHandler = this.decorateHandler(handler);

      this.handlers.set(topic, decoratedHandler);

      if (handler.config.handles) {
        this.registeredCommands.push(handler.config.handles);
      }
    }
  }

  /**
   * Execute a command asynchronously.
   */
  async execute<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = command.meta?.eventId || opts?.eventId || uuid();
    const streamId = command.meta?.streamId || opts?.streamId || eventId;
    command.meta = { ...command.meta, eventId };

    this.in$.next({ command, streamId });

    return right(streamId) as TRes;
  }

  /**
   * Execute a command and wait for the result.
   */
  async executeSync<T, TRes extends AnyEither, TCommandRes extends AnyEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const executeResult = await this.execute(command, opts);
    if (isLeft(executeResult)) {
      return executeResult as TRes;
    }

    const streamId = executeResult.right;
    const className = command.meta.className;
    const timeoutMs = opts?.timeout || 30000;

    try {
      const result = await firstValueFrom(
        this.results$.pipe(
          filter((r) => r.streamId === streamId && r.className === className),
          map((r) => r.result),
          timeout(timeoutMs),
        ),
      );
      return result as TRes;
    } catch (e) {
      if (e instanceof Error && e.name === "TimeoutError") {
        throw new TimeoutExceededError(`Timeout waiting for command ${className}`);
      }
      throw e;
    }
  }

  /**
   * Deserialize a persisted event back to a command instance.
   * Not supported for in-memory bus as there's no persistent storage.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  deserializeCommand(_event: IPersistedEvent): ICommand {
    throw new ApplicationError("deserializeCommand not supported for InMemoryCommandBus");
  }

  /**
   * Drain is a no-op for in-memory bus.
   */
  async drain(): Promise<void> {
    // No-op for in-memory
  }

  /**
   * Replay all failed commands (not supported for in-memory).
   */
  async replayAllFailed(): Promise<void> {
    throw new Error("replayAllFailed not supported for InMemoryCommandBus");
  }

  /**
   * Replay a command.
   */
  async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    return this.execute(command, opts);
  }

  /**
   * Get the command stream as an Observable.
   */
  stream(): Observable<ICommand> {
    return this.out$.pipe(map((envelope) => envelope.command));
  }

  /**
   * Process a command through its handler.
   */
  private async processCommand(envelope: ICommandEnvelope): Promise<void> {
    const { command, streamId } = envelope;
    const className = command.meta.className;
    const handler = this.handlers.get(className);

    if (!handler) {
      const error = new NoHandlerFoundError(`No handler registered for ${className}`);
      this.results$.next({
        streamId,
        className,
        result: left(error),
      });
      return;
    }

    try {
      const ctx = {
        logger: this.logger,
        scope: {} as any, // In-memory has no transactional scope
      };

      const result = await handler.handle(command, ctx);
      this.results$.next({
        streamId,
        className,
        result,
      });
    } catch (e) {
      this.results$.next({
        streamId,
        className,
        result: left(e as Error),
      });
    }
  }

  /**
   * Apply decorators to a handler.
   */
  private decorateHandler<TPayload extends ICommand, TRes extends AnyEither>(
    handler: ICommandHandler<TPayload, TRes>,
  ): ICommandHandler<TPayload, TRes> {
    return this.decorators.reduce(
      (acc, decorator) => decorator.decorate(acc) as ICommandHandler<TPayload, TRes>,
      handler,
    );
  }
}
