import type { Logger } from "@figedi/svc";
import { createScopeProvider } from "../infrastructure/createEventStore";
import { IEventStore } from "../infrastructure/types";

import { ICommandBus } from "../types";
import { InMemoryCommandBus } from "./InMemoryCommandBus";
import { PersistentCommandBus } from "./PersistentCommandBus";

export const createCommandBus = (persistence: "inmem" | "pg", eventStore: IEventStore, logger: Logger): ICommandBus => {
  const scopeProvider = createScopeProvider(persistence);
  if (persistence === "inmem") {
    return new InMemoryCommandBus(logger);
  }
  return new PersistentCommandBus(logger, eventStore, scopeProvider);
};
