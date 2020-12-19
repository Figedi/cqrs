import type { Logger } from "@figedi/svc";
import { right } from "fp-ts/lib/Either";
import { v4 as uuid } from "uuid";

import { ClassContextProvider, ExecuteOpts, IEvent, IEventBus, StringEither } from "../types";
import { BaseEventBus } from "./BaseEventBus";

export class InMemoryEventBus extends BaseEventBus implements IEventBus {
  constructor(private readonly logger: Logger, readonly ctxProvider: ClassContextProvider) {
    super(ctxProvider);
  }

  public async replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]> {
    this.logger.warn(`Ignoring replayByStreamIds(${streamIds}) in InMemoryEventbus`);
    return [];
  }

  public async execute<T, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<T, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = event.meta?.eventId || opts?.eventId || uuid();
    const streamId = event.meta?.streamId || opts?.streamId || eventId;
    // eslint-disable-next-line no-param-reassign
    event.meta = { ...event.meta, eventId };

    this.in$.next(event);

    return right(streamId) as TRes;
  }
}
