import type { Logger } from "@figedi/svc";
import type { ICommandBus } from "../types.js";
import { InMemoryEventScheduler, PersistentEventScheduler } from "./PersistentEventScheduler.js";
import type { IEventScheduler, IScopeProvider } from "./types.js";

export const createEventScheduler = (
  persistence: "inmem" | "pg",
  scopeProvider: IScopeProvider,
  commandBus: ICommandBus,
  logger: Logger,
): IEventScheduler => {
  if (persistence === "pg") {
    return new PersistentEventScheduler(scopeProvider, commandBus, logger);
  }
  return new InMemoryEventScheduler(scopeProvider, commandBus, logger);
};
