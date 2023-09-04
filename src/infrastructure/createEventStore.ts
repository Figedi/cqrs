import { PersistentEventStore } from "./PersistentEventStore.js";
import type { IEventStore, IScopeProvider } from "./types.js";

export const createEventStore = (persistence: "inmem" | "pg", scopeProvider: IScopeProvider): IEventStore => {
  if (persistence === "pg") {
    return new PersistentEventStore(scopeProvider);
  }
  throw new Error("inmem persistence not yet supported for eventStore");
};
