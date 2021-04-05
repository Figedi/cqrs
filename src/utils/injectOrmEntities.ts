import { ConnectionOptions } from "typeorm";
import { EventEntity } from "../infrastructure/EventEntity";
import { ScheduledEventEntity } from "../infrastructure/ScheduledEventEntity";

export const injectEntitiesIntoOrmConfig = (config: ConnectionOptions): ConnectionOptions => ({
  ...config,
  entities: [...(config.entities || []), EventEntity, ScheduledEventEntity],
});
