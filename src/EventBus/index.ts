import type { Logger } from "@figedi/svc";
import type { IEventStore } from "../infrastructure/types.js";

import type { ClassContextProvider, IEventBus } from "../types.js";
import { InMemoryEventBus } from "./InMemoryEventBus.js";
import { PersistentEventBus } from "./PersistentEventBus.js";

export const createEventBus = (
  persistence: "inmem" | "pg",
  eventStore: IEventStore,
  ctxProvider: ClassContextProvider,
  logger: Logger,
): IEventBus => {
  if (persistence === "inmem") {
    return new InMemoryEventBus(logger, ctxProvider);
  }
  return new PersistentEventBus(logger, eventStore, ctxProvider);
};
