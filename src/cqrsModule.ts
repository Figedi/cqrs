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
import { createEventScheduler } from "./infrastructure/createEventScheduler";
import { IEventScheduler } from "./infrastructure/types";

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
  private connections: Record<string, Connection> = {};

  public timeBasedEventScheduler!: TimeBasedEventScheduler;

  public waitUntilIdle!: ReturnType<typeof createWaitUntilIdle>;

  public waitUntilSettled!: ReturnType<typeof createWaitUntilSettled>;

  public commandBus!: ICommandBus;

  public eventBus!: IEventBus;

  public queryBus!: IQueryBus;

  public eventScheduler!: IEventScheduler;

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

    const internalScopeProvider = createScopeProvider({
      type,
      name: "__internal__",
      connectionProvider: this.getConnection,
    });
    const scopeProvider = createScopeProvider({ type, connectionProvider: this.getConnection });
    const eventStore = createEventStore(type, internalScopeProvider);
    this.waitUntilIdle = createWaitUntilIdle(eventStore);
    this.waitUntilSettled = createWaitUntilSettled(eventStore);

    this.commandBus = createCommandBus(type, eventStore, this.logger, scopeProvider);
    this.eventBus = createEventBus(type, eventStore, ctxProvider, this.logger);
    this.queryBus = createQuerybus(type, eventStore, this.logger);
    this.commandBus.registerDecorator(new LoggingDecorator(this.logger));
    this.commandBus.registerDecorator(new UowDecorator(this.settings.transaction, ctxProvider));
    this.queryBus.registerDecorator(new LoggingDecorator(this.logger));
    this.timeBasedEventScheduler = new TimeBasedEventScheduler(this.eventBus, this.logger);
    this.eventScheduler = createEventScheduler(type, scopeProvider, this.commandBus, this.logger);
  }

  public async preflight() {
    if (Object.keys(this.connections).length === 0 && this.settings.persistence.type === "pg") {
      const connectionOptions = this.settings.persistence.options as ConnectionOptions;
      const mainConnection =
        this.settings.persistence.connectionProvider?.get() || (await createConnection(connectionOptions));

      const internalConnection = await createConnection({
        ...connectionOptions,
        name: "__internal__",
        extra: { poolSize: 5 },
      });

      this.connections = {
        [internalConnection.name]: internalConnection,
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

  private getConnection = (name?: string): Connection => {
    if (this.settings.persistence.type !== "pg") {
      throw new ApplicationError("Cannot get Connection when persistence is not 'pg'");
    }
    if (!this.connections[name || "default"]) {
      throw new ApplicationError("No connection found, did you call preflight() already?");
    }
    return this.connections[name || "default"];
  };
}
