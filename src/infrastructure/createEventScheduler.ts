import { Logger } from "@figedi/svc";
import { ICommandBus } from "../types";
import { InMemoryEventScheduler, PersistentEventScheduler } from "./PersistentEventScheduler";
import { IEventScheduler, IScopeProvider } from "./types";

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
