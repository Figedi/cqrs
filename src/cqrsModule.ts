import type { Logger } from "@figedi/svc";
import type { DataSourceOptions } from "typeorm";
import { DataSource } from "typeorm";

import { createCommandBus } from "./CommandBus/index.js";
import { LoggingDecorator } from "./decorators/LoggingDecorator.js";
import type { UowTxSettings } from "./decorators/UowDecorator.js";
import { UowDecorator } from "./decorators/UowDecorator.js";
import { createEventBus } from "./EventBus/index.js";
import { createEventStore } from "./infrastructure/createEventStore.js";
import { createQuerybus } from "./QueryBus/index.js";
import { TimeBasedEventScheduler } from "./utils/TimeBasedEventScheduler.js";
import type { ICommandBus, IEventBus, IQueryBus } from "./types.js";
import { createWaitUntilIdle } from "./utils/waitUntilIdle.js";
import { createWaitUntilSettled } from "./utils/waitUntilSettled.js";
import { createScopeProvider } from "./common.js";
import { ApplicationError } from "./errors.js";
import { createEventScheduler } from "./infrastructure/createEventScheduler.js";
import type { EventTypes, IEventScheduler, IEventStore, IPersistedEvent } from "./infrastructure/types.js";

export interface IConnectionProvider {
  get: () => DataSource;
}

export interface ICQRSSettings {
  persistence: {
    type: "inmem" | "pg";
    runMigrations?: boolean;
    connectionProvider?: IConnectionProvider;
    options?: Record<string, any>;
  };
  transaction: UowTxSettings;
}

export class CQRSModule {
  private connections: Record<string, DataSource> = {};

  public timeBasedEventScheduler!: TimeBasedEventScheduler;

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>;

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>;

  public commandBus!: ICommandBus;

  public eventBus!: IEventBus;

  public queryBus!: IQueryBus;

  public eventScheduler!: IEventScheduler;

  private eventStore!: IEventStore;

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

    const { type } = this.settings.persistence;

    const internalScopeProvider = createScopeProvider({
      type,
      name: "__internal__",
      connectionProvider: this.getConnection,
    });
    const scopeProvider = createScopeProvider({ type, connectionProvider: this.getConnection });
    this.eventStore = createEventStore(type, internalScopeProvider);
    this.waitUntilIdle = createWaitUntilIdle(this.eventStore);
    this.waitUntilSettled = createWaitUntilSettled(this.eventStore);

    this.commandBus = createCommandBus(type, this.eventStore, this.logger, scopeProvider);
    this.eventBus = createEventBus(type, this.eventStore, ctxProvider, this.logger);
    this.queryBus = createQuerybus(type, this.eventStore, this.logger);
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger));
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider));
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger));
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger);
    this.eventScheduler = createEventScheduler(type, scopeProvider, this.commandBus, this.logger);
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

  public async preflight() {
    if (Object.keys(this.connections).length === 0 && this.settings.persistence.type === "pg") {
      const connectionOptions = this.settings.persistence.options as DataSourceOptions;
      const mainConnection =
        this.settings.persistence.connectionProvider?.get() || (await new DataSource(connectionOptions).initialize());

      const internalConnection = await new DataSource({
        ...connectionOptions,
        extra: { poolSize: 5 },
      }).initialize();

      this.connections = {
        __internal__: internalConnection,
        default: mainConnection,
      };
    }

    if (
      Object.keys(this.connections).length &&
      !this.connections.default.entityMetadatas.find(entity => entity.name === "EventEntity")
    ) {
      throw new ApplicationError(
        "Did not find Entity in the provided connection, did you call injectEntitiesIntoOrmConfig()?",
      );
    }
    if (this.settings.persistence.runMigrations && this.connections.default) {
      await this.connections.default.runMigrations();
    }
  }

  private getConnection = (name?: string): DataSource => {
    if (this.settings.persistence.type !== "pg") {
      throw new ApplicationError("Cannot get Connection when persistence is not 'pg'");
    }
    if (!this.connections[name || "default"]) {
      throw new ApplicationError("No connection found, did you call preflight() already?");
    }
    return this.connections[name || "default"];
  };
}
