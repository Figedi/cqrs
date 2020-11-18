import { ConnectionOptions } from "typeorm";
import { EventEntity } from "../infrastructure/EventEntity";

export const injectEntitiesIntoOrmConfig = (config: ConnectionOptions): ConnectionOptions => ({
  ...config,
  entities: [...(config.entities || []), EventEntity],
});
