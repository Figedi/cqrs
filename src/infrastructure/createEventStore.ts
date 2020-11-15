import { getManager } from "typeorm";
import { TransactionalScope } from "../types";
import { PersistentEventStore } from "./PersistentEventStore";
import { IEventStore } from "./types";

export const createScopeProvider = (persistence: "inmem" | "pg") => (): TransactionalScope => {
  if (persistence === "pg") {
    return getManager();
  }
  return {} as any;
};

export const createEventStore = (persistence: "inmem" | "pg"): IEventStore => {
  if (persistence === "pg") {
    return new PersistentEventStore();
  }
  throw new Error("inmem persistence not yet supported for eventStore");
};
