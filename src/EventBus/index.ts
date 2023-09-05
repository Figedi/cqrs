import type { ClassContextProvider, IEventBus, IPersistenceSettingsWithClient } from "../types.js";

import type { IEventStore } from "../infrastructure/types.js";
import { InMemoryEventBus } from "./InMemoryEventBus.js";
import type { Logger } from "@figedi/svc";
import { PersistentEventBus } from "./PersistentEventBus.js";

export const createEventBus = (
  opts: IPersistenceSettingsWithClient,
  eventStore: IEventStore,
  ctxProvider: ClassContextProvider,
  logger: Logger,
): IEventBus => {
  if (opts.type === "inmem") {
    return new InMemoryEventBus(logger, ctxProvider);
  }
  return new PersistentEventBus(logger, eventStore, ctxProvider);
};
