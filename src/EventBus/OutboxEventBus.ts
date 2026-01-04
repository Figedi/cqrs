import type { Pool } from "pg";
import { left, right } from "fp-ts/lib/Either.js";
import { v4 as uuid } from "uuid";
import { serializeError } from "serialize-error";
import { type Observable, Subject } from "rxjs";
import { share } from "rxjs/operators";
import type { Subscription } from "rxjs";

import type {
  ClassContextProvider,
  Constructor,
  ExecuteOpts,
  IEvent,
  IEventBus,
  ISaga,
  Logger,
  ServiceWithLifecycleHandlers,
  StringEither,
} from "../types.js";
import { deserializeEvent, serializeEvent } from "../common.js";
import { ApplicationError } from "../errors.js";
import type { EventStatus, IEventStore, IPersistedEvent, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js";
import { createPollingWorker, type PollingWorker } from "../infrastructure/PollingWorker.js";
import { createStreamController, type StreamController, type IStreamEvent } from "../infrastructure/utils/StreamController.js";
import { SagaTriggeredCommandEvent } from "../utils/internalEvents.js";

/**
 * OutboxEventBus implements the Transactional Outbox pattern for reliable
 * event processing with PostgreSQL-based job queuing.
 *
 * Key difference from CommandBus:
 * - Events are typically inserted within a command's transaction
 * - Processing happens asynchronously via PollingWorker in separate transactions
 * - Supports sagas for event-driven command triggering
 */
export class OutboxEventBus implements IEventBus, ServiceWithLifecycleHandlers {
  protected registeredEvents: Constructor<IEvent>[] = [];

  private sagaSubscriptions: Record<string, Subscription> = {};
  private pollingWorker?: PollingWorker;
  private streamController?: StreamController;

  // Internal RxJS subjects for saga compatibility
  private in$ = new Subject<IEvent>();
  private out$ = this.in$.pipe(share());

  constructor(
    private readonly logger: Logger,
    private readonly eventStore: IEventStore,
    private readonly pool: Pool,
    private readonly ctxProvider: ClassContextProvider,
    private workerConfig?: Partial<IWorkerConfig>,
    private rateLimitConfig?: IRateLimitConfig,
  ) {}

  /**
   * Initialize and start the event bus.
   */
  async startup(): Promise<void> {
    // Create polling worker for EVENT type
    this.pollingWorker = createPollingWorker(
      this.pool,
      this.logger,
      "EVENT",
      this.workerConfig,
      this.rateLimitConfig,
    );

    await this.pollingWorker.initialize();

    // Create stream controller
    this.streamController = createStreamController();

    // Start polling
    await this.pollingWorker.start(
      async (event) => this.processEvent(event),
      (event, status, error) => this.onEventCompletion(event, status, error),
    );

    this.logger.info("OutboxEventBus started");
  }

  /**
   * Shutdown the event bus gracefully.
   */
  async shutdown(): Promise<void> {
    // Unsubscribe from sagas
    Object.values(this.sagaSubscriptions).forEach((subscription) => subscription.unsubscribe());
    this.sagaSubscriptions = {};

    if (this.pollingWorker) {
      await this.pollingWorker.stop();
    }
    if (this.streamController) {
      this.streamController.close();
    }
    this.logger.info("OutboxEventBus stopped");
  }

  /**
   * Deserialize a persisted event back to an event instance.
   */
  deserializeEvent(event: IPersistedEvent): IEvent {
    const klass = this.registeredEvents.find(
      (registeredEv) => registeredEv.name === event.eventName,
    );
    if (!klass) {
      throw new ApplicationError(`Did not find registered event for event ${event.eventName}`);
    }
    return deserializeEvent(event.event, klass) as IEvent;
  }

  /**
   * Register event classes.
   */
  register(...events: Constructor<IEvent>[]): void {
    this.registeredEvents.push(...events);
  }

  /**
   * Register sagas for event-driven command triggering.
   */
  registerSagas(...sagas: ISaga[]): void {
    for (const saga of sagas) {
      const sagaName = saga.constructor.name;
      if (!this.shouldIgnoreSaga(sagaName)) {
        const stream$ = saga.process(this.out$);
        const { commandBus } = this.ctxProvider();
        const subscription = stream$.subscribe((command) => {
          this.execute(
            new SagaTriggeredCommandEvent({
              sagaName: saga.constructor.name,
              outgoingEventId: command.meta.eventId,
              outgoingEventName: command.constructor.name,
            }),
          );
          return commandBus.execute(command);
        });

        this.sagaSubscriptions[sagaName] = subscription;
      }
    }
  }

  /**
   * Execute (publish) an event.
   *
   * If called with opts.scope, the event is inserted within that transaction
   * (e.g., from UowDecorator within a command handler).
   *
   * @param event - The event to publish
   * @param opts - Execution options
   * @returns Either with the streamId on success
   */
  async execute<T, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<T, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = event.meta?.eventId || opts?.eventId || uuid();
    const streamId = event.meta?.streamId || opts?.streamId || eventId;
    const transient = event.meta?.transient || opts?.transient;
    const delayUntilNextTick = opts?.delayUntilNextTick ?? false;

    const now = new Date();
    event.meta = { ...event.meta, eventId, streamId };

    // Transient events bypass persistence
    if (transient) {
      this.in$.next(event);
      return right(streamId) as TRes;
    }

    try {
      // Insert event into outbox
      // If opts.scope is provided (from UowDecorator), it will be part of that transaction
      await this.eventStore.insert(
        {
          eventId,
          eventName: event.meta.className,
          streamId,
          event: serializeEvent(event),
          status: "CREATED",
          timestamp: now,
          type: "EVENT",
        },
        { scope: opts?.scope },
      );

      // If not delayed, also emit to the internal stream for immediate saga processing
      if (!delayUntilNextTick) {
        this.in$.next(event);
      }

      return right(streamId) as TRes;
    } catch (e: any) {
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

  /**
   * Replay events by stream IDs.
   */
  async replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]> {
    if (!streamIds.length) {
      return [];
    }

    this.logger.debug(`Replaying events by streamIds = ${streamIds}`);
    const events = await this.eventStore.findByStreamIds(streamIds, undefined, "EVENT");

    const deserializedEvents = events
      .map((ev) => {
        try {
          return this.deserializeEvent(ev);
        } catch (_error) {
          this.logger.warn(
            `Did not find a klass for event ${ev.eventName}. Available are: ${this.registeredEvents.map((e) => e.name)}`,
          );
          return null;
        }
      })
      .filter((ev): ev is IEvent => ev !== null);

    // Reset events to CREATED for reprocessing
    for (const event of events) {
      await this.eventStore.updateByEventId(event.eventId, {
        status: "CREATED",
        retryCount: 0,
        nextRetryAt: new Date(),
      });
    }

    // Emit to internal stream for saga processing
    deserializedEvents.forEach((ev) => this.in$.next(ev));

    return deserializedEvents.map((ev) => right(ev.meta.streamId!) as TRes);
  }

  /**
   * Get the RxJS Observable stream (for saga compatibility).
   */
  stream(): Observable<IEvent> {
    return this.out$;
  }

  /**
   * Get the AsyncIterator for streaming completed events.
   */
  getStreamIterator(): AsyncIterableIterator<IStreamEvent> {
    if (!this.streamController) {
      throw new ApplicationError("StreamController not initialized");
    }
    return this.streamController.getIterator();
  }

  /**
   * Get the RxJS Observable for completed events (convenience shorthand).
   */
  stream$(): Observable<IStreamEvent> {
    if (!this.streamController) {
      throw new ApplicationError("StreamController not initialized");
    }
    return this.streamController.stream$();
  }

  /**
   * Process an event from the polling worker.
   */
  private async processEvent(event: IPersistedEvent): Promise<void> {
    // Deserialize and emit to internal stream for saga processing
    try {
      const deserializedEvent = this.deserializeEvent(event);
      this.in$.next(deserializedEvent);
    } catch (error) {
      this.logger.warn(
        { eventId: event.eventId, eventName: event.eventName, error },
        "Failed to deserialize event, marking as processed to prevent infinite retry",
      );
    }

    // Events don't have explicit handlers like commands
    // They are processed by sagas which subscribe to the stream
    // Mark as processed after emitting
  }

  /**
   * Handle event completion from the polling worker.
   */
  private onEventCompletion(event: IPersistedEvent, status: EventStatus, error?: Error): void {
    if (this.streamController && (status === "PROCESSED" || status === "ABORTED")) {
      this.streamController.push({
        event,
        status: status as "PROCESSED" | "FAILED" | "ABORTED",
        error,
      });
    }
  }

  /**
   * Check if a saga should be ignored (via environment variable).
   */
  private shouldIgnoreSaga(sagaName: string): boolean {
    if (process.env.IGNORE_SAGAS) {
      const ignorableSagas = process.env.IGNORE_SAGAS.split(",").map((s) => s.trim());
      return ignorableSagas.includes(sagaName);
    }
    return false;
  }
}

/**
 * Create an OutboxEventBus instance.
 */
export function createOutboxEventBus(
  logger: Logger,
  eventStore: IEventStore,
  pool: Pool,
  ctxProvider: ClassContextProvider,
  workerConfig?: Partial<IWorkerConfig>,
  rateLimitConfig?: IRateLimitConfig,
): OutboxEventBus {
  return new OutboxEventBus(logger, eventStore, pool, ctxProvider, workerConfig, rateLimitConfig);
}
