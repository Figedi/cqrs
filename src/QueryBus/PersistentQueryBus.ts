import { serializeError } from "serialize-error";
import type { Logger } from "@figedi/svc";
import { isLeft, left } from "fp-ts/lib/Either";
import { v4 as uuid } from "uuid";

import { serializeEvent } from "../common";
import { NoHandlerFoundError } from "../errors";
import { IEventStore } from "../infrastructure/types";
import { AnyEither, ExecuteOpts, IQuery, IQueryBus } from "../types";
import { BaseQueryBus } from "./BaseQueryBus";

export class PersistentQueryBus extends BaseQueryBus implements IQueryBus {
  constructor(private logger: Logger, private eventStore: IEventStore) {
    super();
  }

  public async execute<T, TRes extends AnyEither>(query: IQuery<T, TRes>, opts?: ExecuteOpts): Promise<TRes> {
    const topic = query.meta.className;
    const topicConfig = this.topics$[topic];
    if (!topicConfig) {
      const error = new NoHandlerFoundError(
        `No handler found for topic: ${topic}, available handlers are: ${Object.keys(this.topics$)}`,
      );
      this.logger.error({ error }, error.message);
      throw error;
    }
    const eventId = query.meta?.eventId || opts?.eventId || uuid();
    const streamId = query.meta?.streamId || opts?.streamId || eventId;
    const now = new Date();
    // eslint-disable-next-line no-param-reassign
    query.meta = { ...query.meta, eventId };

    const store = opts?.scope ? this.eventStore.withTransactionalScope(() => opts!.scope!) : this.eventStore;

    try {
      this.in$.next(query); // this makes it available for stream()
      const result = await this.handleQuery(topicConfig.handler, query); // directly handle it afterwards
      // notes down the outcome of the query for auditing
      await store.insert({
        eventId,
        eventName: query.meta.className,
        streamId,
        event: serializeEvent(query),
        status: isLeft(result.payload) ? "FAILED" : "PROCESSED",
        timestamp: now,
        type: "QUERY",
      });
      return result.payload;
    } catch (e) {
      await store.updateByEventId(eventId, {
        status: "FAILED",
        meta: {
          lastCalled: now,
          error: serializeError(e),
        },
      });

      return left(e) as TRes;
    }
  }
}
