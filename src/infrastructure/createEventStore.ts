import { PersistentEventStore } from "./PersistentEventStore";
import { IEventStore, IScopeProvider } from "./types";

export const createEventStore = (persistence: "inmem" | "pg", scopeProvider: IScopeProvider): IEventStore => {
  if (persistence === "pg") {
    return new PersistentEventStore(scopeProvider);
  }
  throw new Error("inmem persistence not yet supported for eventStore");
};
