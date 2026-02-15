import pg from "pg"
import { createCommandBus } from "./CommandBus/index.js"
import { LoggingDecorator } from "./decorators/LoggingDecorator.js"
import { UowDecorator } from "./decorators/UowDecorator.js"
import { createEventBus, IOutboxEventBusOptions } from "./EventBus/index.js"
import { ApplicationError } from "./errors.js"
import { createEventScheduler } from "./infrastructure/createEventScheduler.js"
import { createEventStore } from "./infrastructure/createEventStore.js"
import type { KyselyDb } from "./infrastructure/db/index.js"
import { createKyselyFromPool } from "./infrastructure/db/index.js"
import type {
  EventTypes,
  IEventScheduler,
  IEventStore,
  IPersistedEvent
} from "./infrastructure/types.js"
import { createQuerybus } from "./QueryBus/index.js"
import type { ICommandBus, ICQRSSettings, IEventBus, IInitializedPostgresSettings, IInmemorySettings, IPostgresSettings, IQueryBus, Logger } from "./types.js"
import { TimeBasedEventScheduler } from "./utils/TimeBasedEventScheduler.js"
import { createWaitUntilIdle } from "./utils/waitUntilIdle.js"
import { createWaitUntilSettled } from "./utils/waitUntilSettled.js"

export class CQRSModule {
  public timeBasedEventScheduler!: TimeBasedEventScheduler

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>

  public commandBus!: ICommandBus

  public eventBus!: IEventBus

  public queryBus!: IQueryBus

  public eventScheduler!: IEventScheduler

  private eventStore!: IEventStore

  private pool!: pg.Pool

  private db!: KyselyDb

  constructor(
    private settings: ICQRSSettings,
    private logger: Logger,
  ) {
    this.init()
  }

  private init() {
    const ctxProvider = () => ({
      queryBus: this.queryBus,
      eventBus: this.eventBus,
      commandBus: this.commandBus,
    })

    if (this.settings.persistence.type === "pg") {
      // Use provided db/pool or create new ones
      const pgSettings = this.settings.persistence as IPostgresSettings
      if (typeof pgSettings.driver === 'string') {
        this.pool = new pg.Pool({
          connectionString: pgSettings.driver,
          ...pgSettings.options,
        })
        this.db = createKyselyFromPool(this.pool)
      } else {
        this.db = pgSettings.driver.db
        this.pool = pgSettings.driver.pool
      }
    }

    // Build opts with db and pool for pg settings
    const opts: IInitializedPostgresSettings | IInmemorySettings =
      this.settings.persistence.type === "pg"
        ? { ...this.settings.persistence, db: this.db, pool: this.pool }
        : this.settings.persistence

    // Prepare outbox options if enabled
    const outboxOpts: IOutboxEventBusOptions | undefined = this.settings.outbox?.enabled
      ? {
          workerConfig: this.settings.outbox.worker,
          rateLimitConfig: this.settings.outbox.rateLimit,
          ignoredSagas: this.settings.outbox.ignoredSagas,
        }
      : undefined

    this.eventStore = createEventStore(opts)
    this.waitUntilIdle = createWaitUntilIdle(this.eventStore)
    this.waitUntilSettled = createWaitUntilSettled(this.eventStore)
    this.commandBus = createCommandBus(opts, this.eventStore, this.logger, outboxOpts)
    this.eventBus = createEventBus(opts, this.eventStore, ctxProvider, this.logger, outboxOpts)
    this.queryBus = createQuerybus(opts, this.eventStore, this.logger)
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger))
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider, this.db))
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger))
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger)
    this.eventScheduler = createEventScheduler(opts, this.commandBus, this.logger)
  }

  public async status(params: {
    eventIds?: string[]
    streamIds?: string[]
    type?: EventTypes
  }): Promise<IPersistedEvent[]> {
    if (!params.eventIds?.length && !params.streamIds?.length) {
      throw new ApplicationError("Need to provide at least one eventId or streamId to retrieve status")
    }
    if (params.eventIds?.length) {
      return this.eventStore.findByEventIds(params.eventIds, undefined, params.type)
    }
    return this.eventStore.findByStreamIds(params.streamIds!, undefined, params.type)
  }

  /**
   * Initialize the CQRS module.
   * Creates tables, runs migrations, starts outbox polling workers, etc.
   * Must be called after registering all handlers.
   */
  public async preflight(): Promise<void> {
    if ("preflight" in this.eventStore) {
      await (this.eventStore as any).preflight()
    }
    if ("preflight" in this.eventScheduler) {
      await (this.eventScheduler as any).preflight()
    }
    if ("preflight" in this.timeBasedEventScheduler) {
      await (this.timeBasedEventScheduler as any).preflight()
    }
    if ("preflight" in this.commandBus) {
      await (this.commandBus as any).preflight()
    }
    if ("preflight" in this.eventBus) {
      await (this.eventBus as any).preflight()
    }
  }

  /**
   * Shutdown the CQRS module workers gracefully.
   * For outbox-enabled buses, this stops the polling workers and waits for in-flight operations.
   */
  public async shutdown(): Promise<void> {
    if ("shutdown" in this.commandBus) {
      await (this.commandBus as any).shutdown()
    }
    if ("shutdown" in this.eventBus) {
      await (this.eventBus as any).shutdown()
    }
    if ("shutdown" in this.timeBasedEventScheduler) {
      await (this.timeBasedEventScheduler as any).shutdown()
    }
  }
}
