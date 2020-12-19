import { serializeError } from "serialize-error";
import type { Logger } from "@figedi/svc";
import { left, right } from "fp-ts/lib/Either";
import { v4 as uuid } from "uuid";

import { serializeEvent } from "../common";
import { IEventStore } from "../infrastructure/types";
import { ClassContextProvider, ExecuteOpts, IEvent, IEventBus, StringEither } from "../types";
import { BaseEventBus } from "./BaseEventBus";

export class PersistentEventBus extends BaseEventBus implements IEventBus {
  constructor(
    private readonly logger: Logger,
    private readonly eventStore: IEventStore,
    readonly ctxProvider: ClassContextProvider,
  ) {
    super(ctxProvider);
  }

  public async replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]> {
    if (!streamIds.length) {
      return [];
    }
    this.logger.debug(`Replaying events by streamIds = ${streamIds}`);
    const events = await this.eventStore.findByStreamIds(streamIds, undefined, "EVENT");
    const deserializedEvents = events
      .map(ev => {
        try {
          return this.deserializeEvent(ev);
        } catch (_error) {
          this.logger.warn(
            `Did not find a klass for event ${ev.eventName}. Available are: ${this.registeredEvents.map(e => e.name)}`,
          );
          return null;
        }
      })
      .filter(ev => !!ev) as IEvent[];
    deserializedEvents.forEach(ev => this.in$.next(ev));

    return deserializedEvents.map(ev => right(ev.meta.streamId!) as TRes);
  }

  public async execute<T, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<T, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = event.meta?.eventId || opts?.eventId || uuid();
    const streamId = event.meta?.streamId || opts?.streamId || eventId;
    const now = new Date();
    // eslint-disable-next-line no-param-reassign
    event.meta = { ...event.meta, eventId };
    const shouldPersist = !opts || !opts.transient;
    if (!shouldPersist) {
      this.in$.next(event);
      return right(streamId) as TRes;
    }

    const store = opts?.scope ? this.eventStore.withTransactionalScope(() => opts!.scope!) : this.eventStore;

    try {
      await store.insert({
        eventId,
        eventName: event.meta.className,
        streamId,
        event: serializeEvent(event),
        status: "CREATED",
        timestamp: now,
        type: "EVENT",
      });

      this.in$.next(event);

      return right(streamId) as TRes;
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
