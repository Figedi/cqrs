import type { IPersistenceSettingsWithClient, IQueryBus } from "../types.js";

import type { IEventStore } from "../infrastructure/types.js";
import { InMemoryQueryBus } from "./InMemoryQueryBus.js";
import type { Logger } from "@figedi/svc";
import { PersistentQueryBus } from "./PersistentQueryBus.js";

export const createQuerybus = (
  opts: IPersistenceSettingsWithClient,
  eventStore: IEventStore,
  logger: Logger,
): IQueryBus => {
  if (opts.type === "inmem") {
    return new InMemoryQueryBus(logger);
  }
  return new PersistentQueryBus(logger, eventStore);
};
