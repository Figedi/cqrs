import { isLeft, left } from "fp-ts/lib/Either.js"
import { serializeError } from "serialize-error"
import { v4 as uuid } from "uuid"
import { serializeEvent } from "../common.js"
import { NoHandlerFoundError } from "../errors.js"
import type { IEventStore } from "../infrastructure/types.js"
import type { AnyEither, ExecuteOpts, IQuery, IQueryBus, Logger } from "../types.js"
import { BaseQueryBus } from "./BaseQueryBus.js"

export class PersistentQueryBus extends BaseQueryBus implements IQueryBus {
  constructor(
    private logger: Logger,
    private eventStore: IEventStore,
  ) {
    super()
  }

  public async execute<T, TRes extends AnyEither>(query: IQuery<T, TRes>, opts?: ExecuteOpts): Promise<TRes> {
    const topic = query.meta.className
    const topicConfig = this.topics$[topic]
    if (!topicConfig) {
      const error = new NoHandlerFoundError(
        `No handler found for topic: ${topic}, available handlers are: ${Object.keys(this.topics$)}`,
      )
      this.logger.error({ error }, error.message)
      throw error
    }
    const eventId = query.meta?.eventId || opts?.eventId || uuid()
    const streamId = query.meta?.streamId || opts?.streamId || eventId
    const transient = query.meta?.transient || opts?.transient
    const now = new Date()
    query.meta = { ...query.meta, eventId }

    if (transient) {
      const result = await this.handleQuery(topicConfig.handler, query) // directly handle it afterwards
      return result.payload
    }

    try {
      const result = await this.handleQuery(topicConfig.handler, query) // directly handle it afterwards
      // notes down the outcome of the query for auditing
      await this.eventStore.insert({
        eventId,
        eventName: query.meta.className,
        streamId,
        event: serializeEvent(query),
        status: isLeft(result.payload) ? "FAILED" : "PROCESSED",
        timestamp: now,
        type: "QUERY",
      })
      return result.payload
    } catch (e: any) {
      await this.eventStore.updateByEventId(eventId, {
        status: "FAILED",
        meta: {
          lastCalled: now,
          error: serializeError(e),
        },
      })

      return left(e) as TRes
    }
  }
}
