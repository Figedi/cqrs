import type { ICommandBus, IPersistenceSettings, Logger } from "../types.js";

import type { IEventStore, IRateLimitConfig, IWorkerConfig } from "../infrastructure/types.js";
import { InMemoryCommandBus } from "./InMemoryCommandBus.js";
import { OutboxCommandBus, createOutboxCommandBus } from "./OutboxCommandBus.js";

export { OutboxCommandBus, createOutboxCommandBus };
export { InMemoryCommandBus };

export interface IOutboxCommandBusOptions {
  workerConfig?: Partial<IWorkerConfig>;
  rateLimitConfig?: IRateLimitConfig;
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
    return new InMemoryCommandBus(logger);
  }

  // Use OutboxCommandBus for persistent storage
  return createOutboxCommandBus(
    logger,
    eventStore,
    opts,
    outboxOpts?.workerConfig,
    outboxOpts?.rateLimitConfig,
  );
};
