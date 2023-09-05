import type { IEventStore } from "./types.js";
import type { IPersistenceSettingsWithClient } from "../types.js";
import { PersistentEventStore } from "./PersistentEventStore.js";

export const createEventStore = (opts: IPersistenceSettingsWithClient): IEventStore => {
  if (opts.type === "pg") {
    return new PersistentEventStore(opts);
  }
  throw new Error("inmem persistence not yet supported for eventStore");
};
