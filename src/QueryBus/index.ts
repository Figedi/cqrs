import type { Logger } from "@figedi/svc";
import type { IEventStore } from "../infrastructure/types.js";

import type { IQueryBus } from "../types.js";
import { InMemoryQueryBus } from "./InMemoryQueryBus.js";
import { PersistentQueryBus } from "./PersistentQueryBus.js";

export const createQuerybus = (persistence: "inmem" | "pg", eventStore: IEventStore, logger: Logger): IQueryBus => {
  if (persistence === "inmem") {
    return new InMemoryQueryBus(logger);
  }
  return new PersistentQueryBus(logger, eventStore);
};
