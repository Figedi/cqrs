import type {
  AnyEither,
  ExecuteOpts,
  ICommand,
  ICommandBus,
  ICommandHandler,
  IPersistentSettingsWithClient,
  StringEither,
  VoidEither,
} from "../types.js";
import type { Logger, ServiceWithLifecycleHandlers } from "@figedi/svc";
import { deserializeEvent, serializeEvent } from "../common.js";
import { isLeft, left, right } from "fp-ts/lib/Either.js";

import { BaseCommandBus } from "./BaseCommandBus.js";
import { EventByIdNotFoundError, EventIdMissingError, NoHandlerFoundError } from "../errors.js";
import type { IEventStore } from "../infrastructure/types.js";
import type { IMeteredCommandHandlerResult } from "./BaseCommandBus.js";
import type { Left } from "fp-ts/lib/Either.js";
import type { Subscription } from "rxjs";
import { serializeError } from "serialize-error";
import { v4 as uuid } from "uuid";

export class PersistentCommandBus extends BaseCommandBus implements ICommandBus, ServiceWithLifecycleHandlers {
  private pollingSubscription?: Subscription;

  constructor(
    logger: Logger,
    private eventStore: IEventStore,
    private opts: IPersistentSettingsWithClient,
  ) {
    super(logger);
  }

  public async drain(ignoredEventIds?: string[]): Promise<void> {
    const unprocessedEvents = await this.eventStore.findUnprocessedCommands(ignoredEventIds);
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
    const status = isLeft(commandResult.payload) ? "FAILED" : "PROCESSED";
    await this.eventStore.updateByEventId(commandResult.eventId, {
      status,
      meta: isLeft(commandResult.payload)
        ? { error: serializeError(commandResult.payload.left), lastCalled: new Date() }
        : {},
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
      this.in$.next({ command, scope: opts?.scope || this.opts.client });
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
        this.in$.next({ command, scope: opts?.scope || this.opts.client });
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
        lastCalled: new Date(),
        error: serializeError(leftResult.left),
      },
    });
  }

  public async replayAllFailed(opts?: ExecuteOpts): Promise<void> {
    const events = await this.eventStore.find({ status: "FAILED", type: "COMMAND" });
    for (const event of events) {
      this.logger.info({ eventId: event.eventId, eventName: event.eventName }, "Replaying failed command");
      await this.replay(event.eventId, opts);
    }
  }

  public async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    commandOrEventId: string | ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    let command: ICommand<T, TCommandRes>;
    if (typeof commandOrEventId === "string") {
      const [event] = await this.eventStore.findByEventIds([commandOrEventId], undefined, "COMMAND");
      if (!event) {
        throw new EventByIdNotFoundError(`Did not find command by id ${commandOrEventId}`);
      }
      const klass = this.registeredCommands.find(cmd => {
        return cmd.name === event.eventName;
      });
      if (!klass) {
        throw new NoHandlerFoundError(`No command was registered for command ${event.eventName}`);
      }
      const deserializedEvent = deserializeEvent(event.event, klass);

      command = deserializedEvent as ICommand<T, TCommandRes>;
    } else {
      command = commandOrEventId;
    }
    const eventId = command.meta?.eventId;
    if (!eventId) {
      throw new EventIdMissingError("Need at least an eventId, was this command properly deserialized?");
    }
    await this.eventStore.updateByEventId(eventId, {
      status: "CREATED",
      meta: {},
    });
    return this.execute(command, { ...opts, transient: true });
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
