import { serializeError } from "serialize-error";
import type { Logger, ServiceWithLifecycleHandlers } from "@figedi/svc";
import { Left, isLeft, left, right } from "fp-ts/lib/Either";
import { Subscription } from "rxjs";
import { v4 as uuid } from "uuid";

import { deserializeEvent, serializeEvent } from "../common";
import { EventIdMissingError } from "../errors";
import { IEventStore, IScopeProvider } from "../infrastructure/types";
import { AnyEither, ExecuteOpts, ICommand, ICommandBus, ICommandHandler, StringEither, VoidEither } from "../types";
import { BaseCommandBus, IMeteredCommandHandlerResult } from "./BaseCommandBus";

export class PersistentCommandBus extends BaseCommandBus implements ICommandBus, ServiceWithLifecycleHandlers {
  private pollingSubscription: Subscription;

  constructor(logger: Logger, private eventStore: IEventStore, private scopeProvider: IScopeProvider) {
    super(logger);
  }

  public async drain(): Promise<void> {
    const unprocessedEvents = await this.eventStore.findUnprocessedCommands();

    for (const event of unprocessedEvents) {
      const klass = this.registeredCommands.find(command => {
        return (command as any).name === event.eventName;
      });
      if (klass) {
        const deserialized = deserializeEvent(event.event, klass);
        this.logger.info(`Draining previous event: ${event.eventId} (${event.eventName})`);
        await this.replay(deserialized);
      }
    }
  }

  public register(...handlers: ICommandHandler<any, any>[]) {
    super.register(...handlers);
    this.topics$ = Object.entries(this.topics$).reduce((acc, [topicName, handlerConfig]) => {
      if (handlerConfig.subscription) {
        return acc;
      }
      return {
        ...acc,
        [topicName]: {
          ...handlerConfig,
          subscription: handlerConfig.out$.subscribe({
            next: this.processComandResult,
          }),
        },
      };
    }, this.topics$);
  }

  private processComandResult = async (commandResult: IMeteredCommandHandlerResult<AnyEither>) => {
    await this.eventStore.updateByEventId(commandResult.eventId, {
      status: isLeft(commandResult.payload) ? "FAILED" : "PROCESSED",
    });
  };

  public async shutdown() {
    if (this.pollingSubscription) {
      this.pollingSubscription.unsubscribe();
    }
  }

  public async execute<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const delayUntilNextTick = !!opts && opts?.delayUntilNextTick;

    const eventId = command.meta?.eventId || opts?.eventId || uuid();
    const streamId = command.meta?.streamId || opts?.streamId || eventId;
    const transient = command.meta?.transient || opts?.transient;
    const now = new Date();
    // eslint-disable-next-line no-param-reassign
    command.meta = { ...command.meta, eventId };

    if (transient) {
      this.in$.next({ command, scope: opts?.scope || this.scopeProvider() });
      return right(streamId) as TRes;
    }

    try {
      await this.eventStore.insert({
        eventId,
        eventName: command.meta.className,
        streamId,
        event: serializeEvent(command),
        status: "CREATED",
        timestamp: now,
        type: "COMMAND",
      });
      if (!delayUntilNextTick) {
        this.in$.next({ command, scope: opts?.scope || this.scopeProvider() });
      }

      return right(streamId) as TRes;
    } catch (e) {
      const result = left(e) as TRes;
      await this.onLeftResult(eventId, result as Left<Error>);
      return result;
    }
  }

  protected async onLeftResult(eventId: string, leftResult: Left<Error>): Promise<void> {
    await this.eventStore.updateByEventId(eventId, {
      status: "FAILED",
      meta: {
        error: serializeError(leftResult.left),
      },
    });
  }

  public async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
  ): Promise<TRes> {
    const eventId = command.meta?.eventId;
    const streamId = command.meta?.streamId || command.meta?.eventId;
    if (!eventId) {
      throw new EventIdMissingError("Need at least an eventId, was this command properly deserialized?");
    }
    const now = new Date();

    try {
      this.in$.next({ command, scope: this.scopeProvider() });

      return right(streamId) as TRes;
    } catch (e) {
      await this.eventStore.updateByEventId(eventId, {
        status: "FAILED",
        meta: {
          lastCalled: now,
          error: serializeError(e),
        },
      });
      return left(e) as TRes;
    }
  }

  public async executeSync<T, TRes extends AnyEither, TCommandRes extends AnyEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const executeResult = await this.execute(command, opts);
    if (isLeft(executeResult)) {
      return executeResult as TRes;
    }
    return this.waitForCommandResult<TRes>(command.meta.className, executeResult.right, opts?.timeout || 0);
  }
}
