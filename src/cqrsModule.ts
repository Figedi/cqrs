import { Logger } from "@figedi/svc";
import { ConnectionOptions, createConnection } from "typeorm";

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

export interface ICQRSSettings {
  persistence: {
    type: "inmem" | "pg";
    autoCreateConnection?: boolean;
    runMigrations?: boolean;
    options?: Record<string, any>;
  };
  transaction: UowTxSettings;
}

export const createCQRSModule = ({ transaction, persistence }: ICQRSSettings, logger: Logger) => {
  let commandBus: ICommandBus;
  let eventBus: IEventBus;
  let queryBus: IQueryBus;
  const persistenceType = persistence.type;

  const ctxProvider = () => ({
    queryBus,
    eventBus,
    commandBus,
  });

  const scopeProvider = createScopeProvider(persistenceType);
  const eventStore = createEventStore(persistenceType, scopeProvider);
  const waitUntilIdle = createWaitUntilIdle(eventStore);
  const waitUntilSettled = createWaitUntilSettled(eventStore);

  commandBus = createCommandBus(persistenceType, eventStore, logger, scopeProvider);
  commandBus.registerDecorator(new LoggingDecorator(logger));
  commandBus.registerDecorator(new UowDecorator(transaction, ctxProvider));
  eventBus = createEventBus(persistenceType, eventStore, ctxProvider, logger);
  queryBus = createQuerybus(persistenceType, eventStore, logger);
  queryBus.registerDecorator(new LoggingDecorator(logger));
  const timeBasedEventScheduler = new TimeBasedEventScheduler(eventBus, logger);

  return {
    timeBasedEventScheduler,
    waitUntilIdle,
    waitUntilSettled,
    commandBus,
    eventBus,
    queryBus,

    preflight: async () => {
      if (persistenceType === "pg") {
        if (persistence.autoCreateConnection) {
          const connection = await createConnection(persistence.options as ConnectionOptions);
          if (persistence.runMigrations) {
            await connection.runMigrations();
          }
        }
      }
    },
  };
};

export type CQRSModule = ReturnType<typeof createCQRSModule>;
