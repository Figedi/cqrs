import type { DataSourceOptions } from "typeorm";
import { EventEntity } from "../infrastructure/EventEntity.js";
import { ScheduledEventEntity } from "../infrastructure/ScheduledEventEntity.js";

export const injectEntitiesIntoOrmConfig = (config: DataSourceOptions): DataSourceOptions => {
  if (!config.entities) {
    return {
      ...config,
      entities: [EventEntity, ScheduledEventEntity],
    };
  }
  if (Array.isArray(config.entities)) {
    return {
      ...config,
      entities: [...config.entities, EventEntity, ScheduledEventEntity],
    };
  }
  return {
    ...config,
    entities: { ...config.entities, EventEntity, ScheduledEventEntity },
  };
};
