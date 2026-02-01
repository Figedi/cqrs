import type { ICommandBus, IInitializedPostgresSettings, IPersistenceSettings, Logger } from "../types.js"
import { InMemoryEventScheduler, PersistentEventScheduler } from "./PersistentEventScheduler.js"

import type { IEventScheduler } from "./types.js"

export const createEventScheduler = (
  opts: IPersistenceSettings,
  commandBus: ICommandBus,
  logger: Logger,
): IEventScheduler => {
  if (opts.type === "pg") {
    return new PersistentEventScheduler(opts as IInitializedPostgresSettings, commandBus, logger)
  }
  return new InMemoryEventScheduler(opts, commandBus, logger)
}
