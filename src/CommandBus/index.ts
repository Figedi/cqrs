import type { ICommandBus, IPersistenceSettingsWithClient } from "../types.js";

import type { IEventStore } from "../infrastructure/types.js";
import { InMemoryCommandBus } from "./InMemoryCommandBus.js";
import type { Logger } from "@figedi/svc";
import { PersistentCommandBus } from "./PersistentCommandBus.js";

export const createCommandBus = (
  opts: IPersistenceSettingsWithClient,
  eventStore: IEventStore,
  logger: Logger,
): ICommandBus => {
  if (opts.type === "inmem") {
    return new InMemoryCommandBus(logger);
  }
  return new PersistentCommandBus(logger, eventStore, opts);
};
