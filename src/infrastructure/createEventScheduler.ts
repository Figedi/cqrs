import type { ICommandBus, IPersistenceSettingsWithClient } from "../types.js";
import { InMemoryEventScheduler, PersistentEventScheduler } from "./PersistentEventScheduler.js";

import type { IEventScheduler } from "./types.js";
import type { Logger } from "@figedi/svc";

export const createEventScheduler = (
  opts: IPersistenceSettingsWithClient,
  commandBus: ICommandBus,
  logger: Logger,
): IEventScheduler => {
  if (opts.type === "pg") {
    return new PersistentEventScheduler(opts, commandBus, logger);
  }
  return new InMemoryEventScheduler(opts, commandBus, logger);
};
