import type { PollingWorker } from "../infrastructure/PollingWorker.js"
import type { IEventStore, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js"
import type { ClassContextProvider, IEventBus, IInitializedPostgresSettings, IPersistenceSettings, Logger } from "../types.js"
import { InMemoryEventBus } from "./InMemoryEventBus.js"
import { createOutboxEventBus, OutboxEventBus } from "./OutboxEventBus.js"

export { OutboxEventBus, createOutboxEventBus }
export { InMemoryEventBus }

export interface IOutboxEventBusOptions {
  ignoredSagas?: string[]
  workerConfig?: Partial<IWorkerConfig>
  rateLimitConfig?: IRateLimitConfig
  /** Shared polling worker (required for outbox) */
  sharedWorker: PollingWorker
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
    return new InMemoryEventBus(logger, ctxProvider, outboxOpts?.ignoredSagas)
  }

  // Use OutboxEventBus for persistent storage
  if (!outboxOpts?.sharedWorker) {
    throw new Error("OutboxEventBus requires sharedWorker in outbox options")
  }
  return createOutboxEventBus(
    logger,
    eventStore,
    (opts as IInitializedPostgresSettings).db,
    (opts as IInitializedPostgresSettings).pool,
    ctxProvider,
    outboxOpts.sharedWorker,
    outboxOpts?.ignoredSagas,
  )
}
