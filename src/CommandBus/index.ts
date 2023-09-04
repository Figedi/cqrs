import type { Logger } from "@figedi/svc";

import type { IEventStore, IScopeProvider } from "../infrastructure/types.js";
import type { ICommandBus } from "../types.js";
import { InMemoryCommandBus } from "./InMemoryCommandBus.js";
import { PersistentCommandBus } from "./PersistentCommandBus.js";

export const createCommandBus = (
  persistence: "inmem" | "pg",
  eventStore: IEventStore,
  logger: Logger,
  scopeProvider: IScopeProvider,
): ICommandBus => {
  if (persistence === "inmem") {
    return new InMemoryCommandBus(logger);
  }
  return new PersistentCommandBus(logger, eventStore, scopeProvider);
};
