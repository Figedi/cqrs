import type { IPersistenceSettings, IQueryBus, Logger } from "../types.js";

import type { IEventStore } from "../infrastructure/types.js";
import { InMemoryQueryBus } from "./InMemoryQueryBus.js";
import { PersistentQueryBus } from "./PersistentQueryBus.js";

export const createQuerybus = (
  opts: IPersistenceSettings,
  eventStore: IEventStore,
  logger: Logger,
): IQueryBus => {
  if (opts.type === "inmem") {
    return new InMemoryQueryBus(logger);
  }
  return new PersistentQueryBus(logger, eventStore);
};
