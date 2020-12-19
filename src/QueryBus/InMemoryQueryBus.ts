import type { Logger } from "@figedi/svc";
import { left } from "fp-ts/lib/Either";
import { v4 as uuid } from "uuid";

import { NoHandlerFoundError } from "../errors";
import { AnyEither, ExecuteOpts, IQuery, IQueryBus } from "../types";
import { BaseQueryBus } from "./BaseQueryBus";

export class InMemoryQueryBus extends BaseQueryBus implements IQueryBus {
  constructor(private logger: Logger) {
    super();
  }

  public async execute<T, TRes extends AnyEither>(query: IQuery<T, TRes>, opts?: ExecuteOpts): Promise<TRes> {
    const topic = query.meta.className;
    const { handler } = this.topics$[topic];
    if (!handler) {
      const error = new NoHandlerFoundError(`No handler found for topic: ${topic}`);
      this.logger.errror({ error }, error.message);
      throw error;
    }
    const eventId = query.meta?.eventId || opts?.eventId || uuid();
    // eslint-disable-next-line no-param-reassign
    query.meta = { ...query.meta, eventId };

    try {
      this.in$.next(query);
      const result = await this.handleQuery(handler, query);

      return result.payload;
    } catch (e) {
      return left(e) as TRes;
    }
  }
}
