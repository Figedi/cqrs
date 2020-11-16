import type { Logger } from "@figedi/svc";

import { IEventStore, IScopeProvider } from "../infrastructure/types";

import { ICommandBus } from "../types";
import { InMemoryCommandBus } from "./InMemoryCommandBus";
import { PersistentCommandBus } from "./PersistentCommandBus";

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
