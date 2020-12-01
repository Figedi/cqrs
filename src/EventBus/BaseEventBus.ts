import type { ServiceWithLifecycleHandlers } from "@figedi/svc";
import { Subject, Subscription } from "rxjs";
import { share } from "rxjs/operators";
import { deserializeEvent } from "../common";
import { ApplicationError } from "../errors";
import { IPersistedEvent } from "../infrastructure/types";

import { ClassContextProvider, Constructor, ExecuteOpts, IEvent, IEventBus, ISaga, StringEither } from "../types";

export abstract class BaseEventBus implements IEventBus, ServiceWithLifecycleHandlers {
  private readonly sagaSubscriptions: Record<string, Subscription> = {};

  protected registeredEvents: Constructor<IEvent>[] = [];

  protected in$ = new Subject<IEvent>();

  protected out$ = this.in$.pipe(share());

  constructor(protected readonly ctxProvider: ClassContextProvider) {}

  private shouldIgnoreSaga(sagaName: string): boolean {
    if (process.env.IGNORE_SAGAS) {
      const ignorableSagas = process.env.IGNORE_SAGAS.split(",").map(s => s.trim());
      return ignorableSagas.includes(sagaName);
    }
    return false;
  }

  public deserializeEvent(event: IPersistedEvent): IEvent {
    const klass = this.registeredEvents.find(registeredEv => registeredEv.name === event.eventName);
    if (!klass) {
      throw new ApplicationError(`Did not find registered event for event ${event.eventName}`);
    }
    return deserializeEvent(event.event, klass) as IEvent;
  }

  public async shutdown() {
    Object.values(this.sagaSubscriptions).forEach(subscription => subscription.unsubscribe());
  }

  public abstract execute<T, TRes extends StringEither, IEventRes extends StringEither>(
    event: IEvent<T, IEventRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes>;

  public abstract replayByStreamIds<TRes extends StringEither>(streamIds: string[]): Promise<TRes[]>;

  public register(...events: Constructor<IEvent>[]): void {
    this.registeredEvents.push(...events);
  }

  public registerSagas(...sagas: ISaga[]): void {
    for (const saga of sagas) {
      const sagaName = saga.constructor.name;
      if (!this.shouldIgnoreSaga(sagaName)) {
        const stream$ = saga.process(this.out$);
        const { commandBus } = this.ctxProvider();
        const subscription = stream$.subscribe(command => {
          return commandBus.execute(command);
        });

        this.sagaSubscriptions[sagaName] = subscription;
      }
    }
  }

  public stream() {
    return this.out$;
  }
}
