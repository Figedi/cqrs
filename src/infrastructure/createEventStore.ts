import type { IPersistenceSettings } from "../types.js"
import { PersistentEventStore } from "./PersistentEventStore.js"
import type { IEventStore } from "./types.js"

export const createEventStore = (opts: IPersistenceSettings): IEventStore => {
  if (opts.type === "pg") {
    return new PersistentEventStore(opts)
  }
  throw new Error("inmem persistence not yet supported for eventStore")
}
