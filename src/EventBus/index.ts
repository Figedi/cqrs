import type { Logger } from "@figedi/svc";
import { IEventStore } from "../infrastructure/types";

import { ClassContextProvider, IEventBus } from "../types";
import { InMemoryEventBus } from "./InMemoryEventBus";
import { PersistentEventBus } from "./PersistentEventBus";

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
