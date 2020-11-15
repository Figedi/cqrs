import { Observable, Subject, merge } from "rxjs";
import { filter } from "rxjs/operators";

import { AnyEither, IDecorator, IQuery, IQueryHandler } from "../types";

export class BaseQueryBus {
  protected topics$: Record<string, { handler: IQueryHandler<any, any>; out$: Observable<any> }> = {};

  protected decorators: IDecorator[] = [];

  public registerDecorator = (decorator: IDecorator): void => {
    this.decorators.push(decorator);
  };

  protected in$ = new Subject<IQuery>();

  protected handleQuery = async <TPayload extends IQuery<any, any>, TRes extends AnyEither>(
    handler: IQueryHandler<TPayload, TRes>,
    query: TPayload,
  ) => {
    // eslint-disable-next-line prefer-spread
    const result = await handler.handle.apply(handler, [query]);

    return {
      eventId: query.meta.eventId,
      payload: result,
    };
  };

  private decorateHandler = <TPayload extends IQuery, TRes extends AnyEither>(handler: IQueryHandler<TPayload, TRes>) =>
    this.decorators.reduce((acc, decorator) => decorator.decorate(acc), handler) as IQueryHandler<TPayload, TRes>;

  public register(...handlers: IQueryHandler<any, any>[]) {
    handlers.forEach(handler => {
      const { topic } = handler.config;
      const decoratedHandler = this.decorateHandler(handler);

      this.topics$[topic] = {
        handler: decoratedHandler,
        out$: this.in$.pipe(filter(({ meta: { className } }) => className === topic)),
      };
    });
  }

  public stream(): Observable<IQuery> {
    return merge(...Object.values(this.topics$).map(topicConfig => topicConfig.out$));
  }
}
