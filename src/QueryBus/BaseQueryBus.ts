import type { Observable } from "rxjs"
import { merge, Subject } from "rxjs"
import { filter } from "rxjs/operators"
import { deserializeEvent } from "../common.js"
import { ApplicationError } from "../errors.js"
import type { IPersistedEvent } from "../infrastructure/types.js"
import type { AnyEither, Constructor, IDecorator, IQuery, IQueryHandler } from "../types.js"

export class BaseQueryBus {
  protected topics$: Record<
    string,
    { meta: { handles?: Constructor<IQuery> }; handler: IQueryHandler<any, any>; out$: Observable<any> }
  > = {}

  protected decorators: IDecorator[] = []

  public registerDecorator = (decorator: IDecorator): void => {
    this.decorators.push(decorator)
  }

  protected in$ = new Subject<IQuery>()

  protected handleQuery = async <TPayload extends IQuery<any, any>, TRes extends AnyEither>(
    handler: IQueryHandler<TPayload, TRes>,
    query: TPayload,
  ) => {
    this.in$.next(query) // makes it available to stream()
    // eslint-disable-next-line prefer-spread
    const result = await handler.handle.apply(handler, [query])

    return {
      eventId: query.meta.eventId,
      payload: result,
    }
  }

  private decorateHandler = <TPayload extends IQuery, TRes extends AnyEither>(handler: IQueryHandler<TPayload, TRes>) =>
    this.decorators.reduce((acc, decorator) => decorator.decorate(acc), handler) as IQueryHandler<TPayload, TRes>

  public deserializeQuery(query: IPersistedEvent): IQuery {
    const registeredTopics = Object.values(this.topics$)
    const klass = registeredTopics.find(({ meta: { handles } }) => handles?.name === query.eventName)?.meta.handles
    if (!klass) {
      throw new ApplicationError(`Did not find registered command for event ${query.eventName}`)
    }
    return deserializeEvent(query.event, klass) as IQuery
  }

  public register(...handlers: IQueryHandler<any, any>[]) {
    handlers.forEach(handler => {
      const { topic } = handler.config
      const decoratedHandler = this.decorateHandler(handler)

      this.topics$[topic] = {
        meta: { handles: handler.config.handles },
        handler: decoratedHandler,
        out$: this.in$.pipe(filter(({ meta: { className } }) => className === topic)),
      }
    })
  }

  public stream(): Observable<IQuery> {
    return merge(...Object.values(this.topics$).map(topicConfig => topicConfig.out$))
  }
}
