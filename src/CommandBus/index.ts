import type { PollingWorker } from "../infrastructure/PollingWorker.js"
import type { IEventStore, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js"
import type { ICommandBus, IInitializedPostgresSettings, IPersistenceSettings, Logger } from "../types.js"
import { InMemoryCommandBus } from "./InMemoryCommandBus.js"
import { createOutboxCommandBus, OutboxCommandBus } from "./OutboxCommandBus.js"

export { OutboxCommandBus, createOutboxCommandBus }
export { InMemoryCommandBus }

export interface IOutboxCommandBusOptions {
  workerConfig?: Partial<IWorkerConfig>
  rateLimitConfig?: IRateLimitConfig
  /** Shared polling worker (required for outbox) */
  sharedWorker: PollingWorker
}

/**
 * Create a CommandBus instance.
 *
 * @param opts - Persistence settings
 * @param eventStore - Event store instance
 * @param logger - Logger instance
 * @param outboxOpts - Optional outbox configuration (enables OutboxCommandBus when provided)
 * @returns CommandBus instance
 */
export const createCommandBus = (
  opts: IPersistenceSettings,
  eventStore: IEventStore,
  logger: Logger,
  outboxOpts?: IOutboxCommandBusOptions,
): ICommandBus => {
  if (opts.type === "inmem") {
    return new InMemoryCommandBus(logger)
  }

  // Use OutboxCommandBus for persistent storage
  if (!outboxOpts?.sharedWorker) {
    throw new Error("OutboxCommandBus requires sharedWorker in outbox options")
  }
  return createOutboxCommandBus(
    logger,
    eventStore,
    (opts as IInitializedPostgresSettings).db,
    (opts as IInitializedPostgresSettings).pool,
    outboxOpts.sharedWorker,
  )
}
