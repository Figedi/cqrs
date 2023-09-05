import type { EventTypes, IEventScheduler, IEventStore, IPersistedEvent } from "./infrastructure/types.js";
import type {
  ICQRSSettings,
  ICommandBus,
  IEventBus,
  IPersistenceSettingsWithClient,
  IPersistentSettings,
  IQueryBus,
} from "./types.js";

import { ApplicationError } from "./errors.js";
import type { Logger } from "@figedi/svc";
import { LoggingDecorator } from "./decorators/LoggingDecorator.js";
import { TimeBasedEventScheduler } from "./utils/TimeBasedEventScheduler.js";
import { UowDecorator } from "./decorators/UowDecorator.js";
import { createCommandBus } from "./CommandBus/index.js";
import { createEventBus } from "./EventBus/index.js";
import { createEventScheduler } from "./infrastructure/createEventScheduler.js";
import { createEventStore } from "./infrastructure/createEventStore.js";
import { createQuerybus } from "./QueryBus/index.js";
import { createWaitUntilIdle } from "./utils/waitUntilIdle.js";
import { createWaitUntilSettled } from "./utils/waitUntilSettled.js";
import pg from "pg";

export class CQRSModule {
  public timeBasedEventScheduler!: TimeBasedEventScheduler;

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>;

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>;

  public commandBus!: ICommandBus;

  public eventBus!: IEventBus;

  public queryBus!: IQueryBus;

  public eventScheduler!: IEventScheduler;

  private eventStore!: IEventStore;

  private pool!: pg.Pool;

  constructor(
    private settings: ICQRSSettings,
    private logger: Logger,
  ) {
    this.init();
  }

  private init() {
    const ctxProvider = () => ({
      queryBus: this.queryBus,
      eventBus: this.eventBus,
      commandBus: this.commandBus,
    });
    this.pool =
      (this.settings.persistence as IPersistentSettings).client ??
      new pg.Pool({
        connectionString: process.env.DATABASE_URL,
        ...(this.settings.persistence as IPersistentSettings).options,
      });

    const opts = { ...this.settings.persistence, client: this.pool } as IPersistenceSettingsWithClient;

    this.eventStore = createEventStore(opts);
    this.waitUntilIdle = createWaitUntilIdle(this.eventStore);
    this.waitUntilSettled = createWaitUntilSettled(this.eventStore);
    this.commandBus = createCommandBus(opts, this.eventStore, this.logger);
    this.eventBus = createEventBus(opts, this.eventStore, ctxProvider, this.logger);
    this.queryBus = createQuerybus(opts, this.eventStore, this.logger);
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger));
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider));
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger));
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger);
    this.eventScheduler = createEventScheduler(opts, this.commandBus, this.logger);
  }

  public async status(params: {
    eventIds?: string[];
    streamIds?: string[];
    type?: EventTypes;
  }): Promise<IPersistedEvent[]> {
    if (!params.eventIds?.length && !params.streamIds?.length) {
      throw new ApplicationError("Need to provide at least one eventId or streamId to retrieve status");
    }
    if (params.eventIds?.length) {
      return this.eventStore.findByEventIds(params.eventIds, undefined, params.type);
    }
    return this.eventStore.findByStreamIds(params.streamIds!, undefined, params.type);
  }

  public async preflight(): Promise<void> {
    if ("preflight" in this.eventStore) {
      await (this.eventStore as any).preflight();
    }
    if ("preflight" in this.eventScheduler) {
      await (this.eventScheduler as any).preflight();
    }
    if ("preflight" in this.timeBasedEventScheduler) {
      await (this.timeBasedEventScheduler as any).preflight();
    }
  }
}
