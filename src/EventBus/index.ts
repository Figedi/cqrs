import type { IEventStore, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js"
import type { ClassContextProvider, IEventBus, IPersistenceSettings, IPostgresSettings, Logger } from "../types.js"
import { InMemoryEventBus } from "./InMemoryEventBus.js"
import { createOutboxEventBus, OutboxEventBus } from "./OutboxEventBus.js"

export { OutboxEventBus, createOutboxEventBus }
export { InMemoryEventBus }

export interface IOutboxEventBusOptions {
  workerConfig?: Partial<IWorkerConfig>
  rateLimitConfig?: IRateLimitConfig
}

/**
 * Create an EventBus instance.
 *
 * @param opts - Persistence settings
 * @param eventStore - Event store instance
 * @param ctxProvider - Class context provider
 * @param logger - Logger instance
 * @param outboxOpts - Optional outbox configuration (enables OutboxEventBus when provided)
 * @returns EventBus instance
 */
export const createEventBus = (
  opts: IPersistenceSettings,
  eventStore: IEventStore,
  ctxProvider: ClassContextProvider,
  logger: Logger,
  outboxOpts?: IOutboxEventBusOptions,
): IEventBus => {
  if (opts.type === "inmem") {
    return new InMemoryEventBus(logger, ctxProvider)
  }

  const pgOpts = opts as IPostgresSettings
  // Use OutboxEventBus for persistent storage
  return createOutboxEventBus(
    logger,
    eventStore,
    pgOpts.db,
    pgOpts.pool,
    ctxProvider,
    outboxOpts?.workerConfig,
    outboxOpts?.rateLimitConfig,
  )
}
