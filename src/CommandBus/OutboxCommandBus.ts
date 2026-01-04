import { isLeft, left, right } from "fp-ts/lib/Either.js";
import type { Left } from "fp-ts/lib/Either.js";
import { v4 as uuid } from "uuid";
import { serializeError } from "serialize-error";
import { Observable } from "rxjs";

import type { Pool } from "pg";
import type { Transaction } from "kysely";

import type {
  AnyEither,
  Constructor,
  ExecuteOpts,
  ICommand,
  ICommandBus,
  ICommandHandler,
  Logger,
  ServiceWithLifecycleHandlers,
  IDecorator,
  IHandlerConfig,
  IPostgresSettings,
  ITransactionalScope,
  StringEither,
  VoidEither,
} from "../types.js";
import { deserializeEvent, serializeEvent } from "../common.js";
import { ApplicationError, EventByIdNotFoundError, EventIdMissingError, NoHandlerFoundError, TimeoutExceededError } from "../errors.js";
import type { EventStatus, IEventStore, IPersistedEvent, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js";
import type { Database, KyselyDb } from "../infrastructure/db/index.js";
import { createPollingWorker, type PollingWorker } from "../infrastructure/PollingWorker.js";
import { createStreamController, type StreamController, type IStreamEvent } from "../infrastructure/utils/StreamController.js";

interface IHandlerMeta {
  handles?: Constructor<ICommand>;
  topic: string;
  maxPerSecond?: number;
  concurrency?: number;
  retries?: IHandlerConfig["retries"];
}

interface IRegisteredHandler {
  handler: ICommandHandler<any, any>;
  meta: IHandlerMeta;
}

/**
 * OutboxCommandBus implements the Transactional Outbox pattern for reliable
 * command processing with PostgreSQL-based job queuing.
 *
 * Features:
 * - Commands are persisted to the events table before processing
 * - Processing happens asynchronously via PollingWorker
 * - Supports rate limiting via rate-limiter-flexible
 * - Supports retries with exponential backoff
 * - executeSync uses LISTEN/NOTIFY for efficient waiting
 * - AsyncIterator-based streaming with RxJS shorthand
 */
export class OutboxCommandBus implements ICommandBus, ServiceWithLifecycleHandlers {
  public registeredCommands: Constructor<ICommand>[] = [];

  private handlers: Map<string, IRegisteredHandler> = new Map();
  private decorators: IDecorator[] = [];
  private pollingWorker?: PollingWorker;
  private streamController?: StreamController;

  constructor(
    private logger: Logger,
    private eventStore: IEventStore,
    private db: KyselyDb,
    private pool: Pool,
    private workerConfig?: Partial<IWorkerConfig>,
    private rateLimitConfig?: IRateLimitConfig,
  ) {}

  /**
   * Initialize and start the command bus.
   */
  async startup(): Promise<void> {
    // Create polling worker
    this.pollingWorker = createPollingWorker(
      this.db,
      this.pool,
      this.logger,
      "COMMAND",
      this.workerConfig,
      this.rateLimitConfig,
    );

    await this.pollingWorker.initialize();

    // Create stream controller for streaming API
    this.streamController = createStreamController();

    // Start polling
    await this.pollingWorker.start(
      async (event, client) => this.processEvent(event, client),
      (event, status, error) => this.onEventCompletion(event, status, error),
    );

    this.logger.info("OutboxCommandBus started");
  }

  /**
   * Shutdown the command bus gracefully.
   */
  async shutdown(): Promise<void> {
    if (this.pollingWorker) {
      await this.pollingWorker.stop();
    }
    if (this.streamController) {
      this.streamController.close();
    }
    this.logger.info("OutboxCommandBus stopped");
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

      this.handlers.set(topic, {
        handler: decoratedHandler,
        meta: {
          handles: handler.config.handles,
          topic,
          maxPerSecond: handler.config.maxPerSecond,
          concurrency: handler.config.concurrency,
          retries: handler.config.retries,
        },
      });

      if (handler.config.handles) {
        this.registeredCommands.push(handler.config.handles);
      }
    }
  }

  /**
   * Deserialize a persisted event back to a command instance.
   */
  deserializeCommand(event: IPersistedEvent): ICommand {
    const registered = this.handlers.get(event.eventName);
    if (!registered?.meta.handles) {
      throw new ApplicationError(`Did not find registered command for event ${event.eventName}`);
    }
    return deserializeEvent(event.event, registered.meta.handles) as ICommand;
  }

  /**
   * Execute a command asynchronously.
   *
   * The command is persisted to the outbox and returns immediately.
   * Processing happens asynchronously via the polling worker.
   *
   * @param command - The command to execute
   * @param opts - Execution options
   * @returns Either with the streamId on success, or error on failure
   */
  async execute<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = command.meta?.eventId || opts?.eventId || uuid();
    const streamId = command.meta?.streamId || opts?.streamId || eventId;
    const transient = command.meta?.transient || opts?.transient;
    const now = new Date();

    // Update command meta with eventId
    command.meta = { ...command.meta, eventId };

    // Transient commands bypass persistence
    if (transient) {
      await this.processCommandDirectly(command, opts?.scope || this.db);
      return right(streamId) as TRes;
    }

    try {
      // Insert command into outbox (using scope if provided for transactionality)
      await this.eventStore.insert(
        {
          eventId,
          eventName: command.meta.className,
          streamId,
          event: serializeEvent(command),
          status: "CREATED",
          timestamp: now,
          type: "COMMAND",
        },
        { scope: opts?.scope },
      );

      return right(streamId) as TRes;
    } catch (e) {
      const result = left(e) as TRes;
      await this.onLeftResult(eventId, result as Left<Error>);
      return result;
    }
  }

  /**
   * Execute a command and wait for the result synchronously.
   *
   * Uses LISTEN/NOTIFY for efficient waiting with polling fallback.
   *
   * @param command - The command to execute
   * @param opts - Execution options (timeout is respected)
   * @returns The command handler result
   */
  async executeSync<T, TRes extends AnyEither, TCommandRes extends AnyEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const executeResult = await this.execute(command, opts);
    if (isLeft(executeResult)) {
      return executeResult as TRes;
    }

    const eventId = command.meta.eventId!;
    const timeout = opts?.timeout || 0;

    if (!this.pollingWorker) {
      throw new ApplicationError("PollingWorker not initialized");
    }

    try {
      const status = await this.pollingWorker.waitForCompletion(eventId, timeout);

      if (status === "PROCESSED") {
        // Fetch the result from the event store
        const [event] = await this.eventStore.findByEventIds([eventId]);
        if (event?.meta && "result" in event.meta) {
          return event.meta.result as TRes;
        }
        return right(undefined) as TRes;
      } else {
        // FAILED or ABORTED
        const [event] = await this.eventStore.findByEventIds([eventId]);
        const error = event?.meta?.error || new Error(`Command ${status}`);
        return left(error) as TRes;
      }
    } catch (e) {
      if (e instanceof Error && e.message.includes("Timeout")) {
        throw new TimeoutExceededError(`Timeout while waiting for command ${eventId}`);
      }
      throw e;
    }
  }

  /**
   * Drain all unprocessed commands (for startup recovery).
   *
   * Commands in ignoredEventIds will be marked as ABORTED so they won't be processed.
   * All other unprocessed commands will be reset to CREATED for processing.
   *
   * @param ignoredEventIds - Event IDs to skip (will be marked ABORTED)
   */
  async drain(ignoredEventIds?: string[]): Promise<void> {
    // First, abort any ignored commands so polling worker won't pick them up
    if (ignoredEventIds?.length) {
      for (const eventId of ignoredEventIds) {
        this.logger.info(`Aborting ignored event during drain: ${eventId}`);
        await this.eventStore.updateByEventId(eventId, { status: "ABORTED" });
      }
    }

    // Then find remaining unprocessed commands and ensure they're ready for processing
    const unprocessedEvents = await this.eventStore.findUnprocessedCommands();

    for (const event of unprocessedEvents) {
      const registered = this.handlers.get(event.eventName);
      if (registered?.meta.handles) {
        this.logger.info(`Draining previous event: ${event.eventId} (${event.eventName})`);
        // Reset to CREATED so polling worker picks it up
        await this.eventStore.updateByEventId(event.eventId, { status: "CREATED" });
      }
    }
  }

  /**
   * Replay all failed commands.
   */
  async replayAllFailed(opts?: ExecuteOpts): Promise<void> {
    const events = await this.eventStore.find({ status: "FAILED", type: "COMMAND" });

    for (const event of events) {
      this.logger.info({ eventId: event.eventId, eventName: event.eventName }, "Replaying failed command");
      await this.replay(event.eventId, opts);
    }
  }

  /**
   * Replay a specific command by ID or instance.
   *
   * This resets the command status to CREATED so the polling worker will pick it up
   * and process it again. Returns immediately after resetting - use waitUntilSettled
   * to wait for completion.
   */
  async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    commandOrEventId: string | ICommand<T, TCommandRes>,
    _opts?: ExecuteOpts,
  ): Promise<TRes> {
    let eventId: string;
    let streamId: string;

    if (typeof commandOrEventId === "string") {
      const [event] = await this.eventStore.findByEventIds([commandOrEventId], undefined, "COMMAND");
      if (!event) {
        throw new EventByIdNotFoundError(`Did not find command by id ${commandOrEventId}`);
      }
      eventId = event.eventId;
      streamId = event.streamId;
    } else {
      eventId = commandOrEventId.meta?.eventId!;
      streamId = commandOrEventId.meta?.streamId ?? eventId;
      if (!eventId) {
        throw new EventIdMissingError("Need at least an eventId, was this command properly deserialized?");
      }
    }

    // Reset to CREATED for replay - the polling worker will pick it up and process it
    await this.eventStore.updateByEventId(eventId, {
      status: "CREATED",
      meta: {},
      retryCount: 0,
      nextRetryAt: new Date(),
    });

    return right(streamId) as TRes;
  }

  /**
   * Get an AsyncIterator-based stream of completed commands.
   *
   * @param topic - Optional topic to filter by
   * @returns AsyncIterableIterator of stream events
   */
  stream(topic?: string): Observable<ICommand> {
    if (!this.streamController) {
      throw new ApplicationError("StreamController not initialized");
    }

    const observable = this.streamController.stream$();

    if (topic) {
      return new Observable((subscriber) => {
        const subscription = observable.subscribe({
          next: (streamEvent) => {
            if (streamEvent.event.eventName === topic) {
              const command = this.deserializeCommand(streamEvent.event);
              subscriber.next(command);
            }
          },
          error: (err) => subscriber.error(err),
          complete: () => subscriber.complete(),
        });
        return () => subscription.unsubscribe();
      });
    }

    return new Observable((subscriber) => {
      const subscription = observable.subscribe({
        next: (streamEvent) => {
          const command = this.deserializeCommand(streamEvent.event);
          subscriber.next(command);
        },
        error: (err) => subscriber.error(err),
        complete: () => subscriber.complete(),
      });
      return () => subscription.unsubscribe();
    });
  }

  /**
   * Get the AsyncIterator for streaming (primary API).
   */
  getStreamIterator(): AsyncIterableIterator<IStreamEvent> {
    if (!this.streamController) {
      throw new ApplicationError("StreamController not initialized");
    }
    return this.streamController.getIterator();
  }

  /**
   * Get the RxJS Observable stream (convenience shorthand).
   */
  stream$(): Observable<IStreamEvent> {
    if (!this.streamController) {
      throw new ApplicationError("StreamController not initialized");
    }
    return this.streamController.stream$();
  }

  /**
   * Process a command event from the polling worker.
   */
  private async processEvent(event: IPersistedEvent, trx: Transaction<Database>): Promise<void> {
    const registered = this.handlers.get(event.eventName);
    if (!registered) {
      throw new NoHandlerFoundError(`No handler registered for ${event.eventName}`);
    }

    const command = this.deserializeCommand(event);
    const ctx = {
      logger: this.logger,
      scope: trx,
    };

    const result = await registered.handler.handle(command, ctx);

    // Store result in meta for executeSync retrieval
    if (!isLeft(result)) {
      await this.eventStore.updateByEventId(event.eventId, {
        meta: { ...event.meta, result },
      }, { scope: trx });
    }

    if (isLeft(result)) {
      throw result.left;
    }
  }

  /**
   * Process a transient command directly (bypasses outbox).
   */
  private async processCommandDirectly(
    command: ICommand,
    scope: ITransactionalScope,
  ): Promise<void> {
    const registered = this.handlers.get(command.meta.className);
    if (!registered) {
      throw new NoHandlerFoundError(`No handler registered for ${command.meta.className}`);
    }

    const ctx = {
      logger: this.logger,
      scope,
    };

    await registered.handler.handle(command, ctx);
  }

  /**
   * Handle event completion from the polling worker.
   */
  private onEventCompletion(event: IPersistedEvent, status: EventStatus, error?: Error): void {
    // Push to stream controller
    if (this.streamController && (status === "PROCESSED" || status === "ABORTED")) {
      this.streamController.push({
        event,
        status: status as "PROCESSED" | "FAILED" | "ABORTED",
        error,
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

  /**
   * Handle a left (error) result during execute.
   */
  private async onLeftResult(eventId: string, leftResult: Left<Error>): Promise<void> {
    await this.eventStore.updateByEventId(eventId, {
      status: "FAILED",
      meta: {
        lastCalled: new Date(),
        error: serializeError(leftResult.left),
      },
    });
  }
}

/**
 * Create an OutboxCommandBus instance.
 */
export function createOutboxCommandBus(
  logger: Logger,
  eventStore: IEventStore,
  db: KyselyDb,
  pool: Pool,
  workerConfig?: Partial<IWorkerConfig>,
  rateLimitConfig?: IRateLimitConfig,
): OutboxCommandBus {
  return new OutboxCommandBus(logger, eventStore, db, pool, workerConfig, rateLimitConfig);
}
