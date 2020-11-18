import { Logger } from "@figedi/svc";
import { Connection, ConnectionOptions, createConnection } from "typeorm";

import { createCommandBus } from "./CommandBus";
import { LoggingDecorator } from "./decorators/LoggingDecorator";
import { UowDecorator, UowTxSettings } from "./decorators/UowDecorator";
import { createEventBus } from "./EventBus";
import { createEventStore } from "./infrastructure/createEventStore";
import { createQuerybus } from "./QueryBus";
import { TimeBasedEventScheduler } from "./utils/TimeBasedEventScheduler";
import { ICommandBus, IEventBus, IQueryBus } from "./types";
import { createWaitUntilIdle } from "./utils/waitUntilIdle";
import { createWaitUntilSettled } from "./utils/waitUntilSettled";
import { createScopeProvider } from "./common";
import { ApplicationError } from "./errors";

export interface IConnectionProvider {
  get: () => Connection;
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
  private connection?: Connection;

  public timeBasedEventScheduler!: TimeBasedEventScheduler;

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>;

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>;

  public commandBus!: ICommandBus;

  public eventBus!: IEventBus;

  public queryBus!: IQueryBus;

  constructor(private settings: ICQRSSettings, private logger: Logger) {
    this.init();
  }

  private init() {
    const ctxProvider = () => ({
      queryBus: this.queryBus,
      eventBus: this.eventBus,
      commandBus: this.commandBus,
    });

    const { type } = this.settings.persistence;

    const scopeProvider = createScopeProvider({ type, connectionProvider: this.getConnection });
    const eventStore = createEventStore(type, scopeProvider);
    this.waitUntilIdle = createWaitUntilIdle(eventStore);
    this.waitUntilSettled = createWaitUntilSettled(eventStore);

    this.commandBus = createCommandBus(type, eventStore, this.logger, scopeProvider);
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger));
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider));
    this.eventBus = createEventBus(type, eventStore, ctxProvider, this.logger);
    this.queryBus = createQuerybus(type, eventStore, this.logger);
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger));
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger);
  }

  public async preflight() {
    if (!this.connection && this.settings.persistence.type === "pg") {
      this.connection =
        this.settings.persistence.connectionProvider?.get() ||
        (await createConnection(this.settings.persistence.options as ConnectionOptions));
    }

    if (this.connection && !this.connection.entityMetadatas.find(entity => entity.name === "EventEntity")) {
      throw new ApplicationError(
        "Did not find Entity in the provided connection, did you call injectEntitiesIntoOrmConfig()?",
      );
    }

    if (this.settings.persistence.runMigrations && this.connection) {
      await this.connection.runMigrations();
    }
  }

  private getConnection = (): Connection => {
    if (this.settings.persistence.type !== "pg") {
      throw new ApplicationError("Cannot get Connection when persistence is not 'pg'");
    }
    if (!this.connection) {
      throw new ApplicationError("No connection found, did you call preflight() already?");
    }
    return this.connection;
  };
}
