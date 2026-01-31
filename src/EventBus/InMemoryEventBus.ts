import { right } from "fp-ts/lib/Either.js"
import { type Observable, Subject, type Subscription } from "rxjs"
import { share } from "rxjs/operators"
import { v4 as uuid } from "uuid"
import { ApplicationError } from "../errors.js"
import type { IPersistedEvent } from "../infrastructure/types.js"
import type {
  ClassContextProvider,
  Constructor,
  ExecuteOpts,
  IEvent,
  IEventBus,
  ISaga,
  Logger,
  StringEither,
} from "../types.js"

/**
 * InMemoryEventBus processes events synchronously in-memory.
 * Suitable for testing and single-process deployments.
 */
export class InMemoryEventBus implements IEventBus {
  protected registeredEvents: Constructor<IEvent>[] = []

  private sagaSubscriptions: Record<string, Subscription> = {}
  private in$ = new Subject<IEvent>()
  private out$ = this.in$.pipe(share())

  constructor(
    private readonly logger: Logger,
    private readonly ctxProvider: ClassContextProvider,
  ) {}

  /**
   * Register event classes.
   */
  register(...events: Constructor<IEvent>[]): void {
    this.registeredEvents.push(...events)
  }

  /**
   * Deserialize a persisted event back to an event instance.
   * Not supported for in-memory bus as there's no persistent storage.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  deserializeEvent(_event: IPersistedEvent): IEvent {
    throw new ApplicationError("deserializeEvent not supported for InMemoryEventBus")
  }

  /**
   * Register sagas for event-driven command triggering.
   */
  registerSagas(...sagas: ISaga[]): void {
    for (const saga of sagas) {
      const sagaName = saga.constructor.name
      if (!this.shouldIgnoreSaga(sagaName)) {
        const stream$ = saga.process(this.out$)
        const { commandBus } = this.ctxProvider()
        const subscription = stream$.subscribe(command => {
          return commandBus.execute(command)
        })

        this.sagaSubscriptions[sagaName] = subscription
      }
    }
  }

  /**
   * Execute (publish) an event.
   */
  async execute<T, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<T, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = event.meta?.eventId || opts?.eventId || uuid()
    const streamId = event.meta?.streamId || opts?.streamId || eventId
    event.meta = { ...event.meta, eventId, streamId }

    this.in$.next(event)

    return right(streamId) as TRes
  }

  /**
   * Replay events by stream IDs (not supported for in-memory).
   */
  async replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]> {
    this.logger.warn(`Ignoring replayByStreamIds(${streamIds}) in InMemoryEventBus`)
    return []
  }

  /**
   * Get the event stream as an Observable.
   */
  stream(): Observable<IEvent> {
    return this.out$
  }

  /**
   * Check if a saga should be ignored (via environment variable).
   */
  private shouldIgnoreSaga(sagaName: string): boolean {
    if (process.env.IGNORE_SAGAS) {
      const ignorableSagas = process.env.IGNORE_SAGAS.split(",").map(s => s.trim())
      return ignorableSagas.includes(sagaName)
    }
    return false
  }
}
