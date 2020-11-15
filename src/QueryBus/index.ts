import type { Logger } from "@figedi/svc";
import { IEventStore } from "../infrastructure/types";

import { IQueryBus } from "../types";
import { InMemoryQueryBus } from "./InMemoryQueryBus";
import { PersistentQueryBus } from "./PersistentQueryBus";

export const createQuerybus = (persistence: "inmem" | "pg", eventStore: IEventStore, logger: Logger): IQueryBus => {
  if (persistence === "inmem") {
    return new InMemoryQueryBus(logger);
  }
  return new PersistentQueryBus(logger, eventStore);
};
