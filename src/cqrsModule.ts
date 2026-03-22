import { LoggingDecorator } from "./decorators/LoggingDecorator.js"
import { UowDecorator } from "./decorators/UowDecorator.js"
import { ApplicationError } from "./errors.js"
import { createDbAdapter } from "./infrastructure/db/index.js"
import type { IDbAdapter } from "./infrastructure/db/index.js"
import { PollingWorker } from "./infrastructure/PollingWorker.js"
import type {
  EventTypes,
  IEventScheduler,
  IEventStore,
  IPersistedEvent
} from "./infrastructure/types.js"
import type {
  ICommandBus,
  ICQRSSettings,
  IEventBus,
  IPostgresSettings,
  IQueryBus,
  Logger,
} from "./types.js"
import { createPGliteAdapter } from "./infrastructure/db/index.js"
import { TimeBasedEventScheduler } from "./utils/TimeBasedEventScheduler.js"
import { createWaitUntilIdle } from "./utils/waitUntilIdle.js"
import { createWaitUntilSettled } from "./utils/waitUntilSettled.js"
import { PersistentEventStore } from "./infrastructure/PersistentEventStore.js"
import { OutboxCommandBus } from "./CommandBus/OutboxCommandBus.js"
import { PersistentQueryBus } from "./QueryBus/PersistentQueryBus.js"
import { PersistentEventScheduler } from "./infrastructure/PersistentEventScheduler.js"
import { OutboxEventBus } from "./EventBus/OutboxEventBus.js"

export class CQRSModule {
  public timeBasedEventScheduler!: TimeBasedEventScheduler

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>

  public commandBus!: ICommandBus

  public eventBus!: IEventBus

  public queryBus!: IQueryBus

  public eventScheduler?: IEventScheduler

  private eventStore?: IEventStore

  private adapter?: IDbAdapter

  private pollingWorker?: PollingWorker

  constructor(
    private settings: ICQRSSettings,
    private logger: Logger,
  ) {
    this.init()
  }


  private init(): void {
    const ctxProvider = () => ({
      queryBus: this.queryBus!,
      eventBus: this.eventBus!,
      commandBus: this.commandBus!,
    });

    this.pollingWorker = new PollingWorker(
      this.logger,
      this.settings.outbox?.worker,
      this.settings.outbox?.rateLimit,
    )

    this.eventStore = new PersistentEventStore(this.settings.persistence)
    this.waitUntilIdle = createWaitUntilIdle(this.eventStore)
    this.waitUntilSettled = createWaitUntilSettled(this.eventStore)
    this.commandBus = new OutboxCommandBus(this.logger, this.eventStore, this.pollingWorker)
    this.eventBus = new OutboxEventBus(this.logger, this.eventStore, ctxProvider, this.pollingWorker, this.settings.outbox?.ignoredSagas)
    this.queryBus = new PersistentQueryBus(this.logger, this.eventStore)
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger))
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider))
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger))
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger)
    this.eventScheduler = new PersistentEventScheduler(this.settings.persistence, this.commandBus, this.logger)
  }

  private wirePersistence(adapter: IDbAdapter): void {
    this.pollingWorker?.setAdapter(adapter);
    this.commandBus?.setAdapter(adapter);
    this.eventStore?.setAdapter(adapter);
    this.eventScheduler?.setAdapter(adapter);
  }

  /**
   * Returns the db adapter when persistence is pg or pglite and preflight() has run; undefined otherwise.
   */
  public getDbAdapter(): IDbAdapter | undefined {
    return this.adapter
  }

  public async status(params: {
    eventIds?: string[]
    streamIds?: string[]
    type?: EventTypes
  }): Promise<IPersistedEvent[]> {
    if (!this.eventStore) {
      throw new Error("Call preflight() first")
    }
    if (!params.eventIds?.length && !params.streamIds?.length) {
      throw new ApplicationError("Need to provide at least one eventId or streamId to retrieve status")
    }
    if (params.eventIds?.length) {
      return this.eventStore.findByEventIds(params.eventIds, undefined, params.type)
    }
    return this.eventStore.findByStreamIds(params.streamIds!, undefined, params.type)
  }

  /**
   * Initialize the CQRS module: wire adapter (pg/pglite) if not yet done, then run preflight on stores/buses.
   * For pg and pglite, adapter is created and wired here; call preflight() before using the module.
   */
  public async preflight(): Promise<void> {
    const hasDb = this.settings.persistence.type === "pg" || this.settings.persistence.type === "pglite"
    if (hasDb && !this.adapter) {
      if (this.settings.persistence.type === "pg") {
        const pgSettings = this.settings.persistence as IPostgresSettings
        this.adapter = createDbAdapter(pgSettings.connectionString, pgSettings.options)
      } else {
        this.adapter = await createPGliteAdapter()
      }
      this.wirePersistence(this.adapter)
    }

    if (this.eventStore && "preflight" in this.eventStore) {
      await (this.eventStore as any).preflight()
    }
    if (this.eventScheduler && "preflight" in this.eventScheduler) {
      await (this.eventScheduler as any).preflight()
    }
    if (this.timeBasedEventScheduler && "preflight" in this.timeBasedEventScheduler) {
      await (this.timeBasedEventScheduler as any).preflight()
    }
    if (this.commandBus && "preflight" in this.commandBus) {
      await (this.commandBus as any).preflight()
    }
    if (this.eventBus && "preflight" in this.eventBus) {
      await (this.eventBus as any).preflight()
    }
    if (this.pollingWorker) {
      await this.pollingWorker.preflight()
    }
  }

  /**
   * Shutdown the CQRS module workers gracefully.
   * For outbox-enabled buses, this stops the polling workers and waits for in-flight operations.
   */
  public async shutdown(): Promise<void> {
    if (this.commandBus && "shutdown" in this.commandBus) {
      await (this.commandBus as any).shutdown()
    }
    if (this.eventBus && "shutdown" in this.eventBus) {
      await (this.eventBus as any).shutdown()
    }
    if (this.pollingWorker) {
      await this.pollingWorker.shutdown()
    }
    if (this.timeBasedEventScheduler && "shutdown" in this.timeBasedEventScheduler) {
      await (this.timeBasedEventScheduler as any).shutdown()
    }
    if (this.adapter?.db) {
      await this.adapter.db.destroy()
    }
  }
}
