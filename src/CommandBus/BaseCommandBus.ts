import { Logger, sleep } from "@figedi/svc";
import { Left, isLeft } from "fp-ts/lib/Either";
import { isNil } from "lodash";
import { Observable, Subject, Subscription, defer, merge } from "rxjs";
import RateLimiter from "rxjs-ratelimiter";
import { filter, map, mergeMap, retry, share, tap } from "rxjs/operators";

import { ConfigError, StreamEndedError, TimeoutExceededError } from "../errors";
import { AnyEither, ICommand, ICommandHandler, IDecorator, IProcessResult, TransactionalScope } from "../types";

interface IInitializedTopicConsumer<O> {
  meta: {
    topic: string;
    maxPerSecond?: number;
    concurrency?: number;
    maxRetries?: number;
  };
  out$: Observable<O>;
  subscription?: Subscription;
}

export interface IMeteredCommandHandlerResult<TRes extends AnyEither> {
  scope: TransactionalScope;
  eventId: string;
  payload: TRes;
}

const verifyExclusive = (a: any, b: any, help: string) => {
  if (!isNil(a) && !isNil(b)) {
    throw new ConfigError(help);
  }
};

export class BaseCommandBus {
  protected topics$: Record<string, IInitializedTopicConsumer<any>> = {};

  protected decorators: IDecorator[] = [];

  protected in$ = new Subject<{ scope: TransactionalScope; command: ICommand }>();

  constructor(protected logger: Logger) {}

  public registerDecorator = (decorator: IDecorator): void => {
    this.decorators.push(decorator);
  };

  private handleCommand = async <TPayload extends ICommand, TRes extends AnyEither>(
    handler: ICommandHandler<TPayload, TRes>,
    command: TPayload,
    scope: TransactionalScope,
  ): Promise<IMeteredCommandHandlerResult<TRes>> => {
    const ctx = {
      logger: this.logger,
      scope,
    };
    const result = await handler.handle(command, ctx);
    return {
      scope: ctx.scope,
      eventId: command.meta.eventId!,
      payload: result,
    };
  };

  private decorateHandler = <TPayload extends ICommand, TRes extends AnyEither>(
    handler: ICommandHandler<TPayload, TRes>,
  ) => this.decorators.reduce((acc, decorator) => decorator.decorate(acc), handler) as ICommandHandler<TPayload, TRes>;

  public register(...handlers: ICommandHandler<any, any>[]) {
    this.topics$ = handlers.reduce((acc, h) => {
      const handlerTopic = h.config.topic;
      const decoratedHandler = this.decorateHandler(h);

      const { concurrency, maxPerSecond, maxRetries } = decoratedHandler.config || {};

      verifyExclusive(concurrency, maxPerSecond, "Cannot have concurrency and maxPerSecond at the same time");
      const rateLimiter = maxPerSecond ? new RateLimiter(maxPerSecond, 1000) : undefined;

      const out$ = this.in$.pipe(
        filter(
          ({
            command: {
              meta: { className },
            },
          }) => className === handlerTopic,
        ),
        map(event => ({ ...event, handler: decoratedHandler })),
        filter(({ handler }) => !!handler),
        rateLimiter
          ? mergeMap(({ command, scope, handler }) =>
              rateLimiter.limit(defer(() => this.handleCommand(handler, command, scope))),
            )
          : mergeMap(({ command, scope, handler }) => this.handleCommand(handler, command, scope), concurrency),
        maxRetries !== undefined ? retry(maxRetries) : ev => ev,
        tap((result: IMeteredCommandHandlerResult<AnyEither>) => {
          if (!isLeft(result.payload)) {
            return;
          }
          this.onLeftResult(result.eventId, result.payload, result.scope).catch(e => {
            this.logger.error({ error: e }, `Unknown error happened while processing left-result: ${e.message}`);
          });
        }),
        share(),
      );

      const handlerConfig = {
        meta: {
          topic: handlerTopic,
          maxPerSecond,
          concurrency,
          maxRetries,
        },
        out$,
      };
      return {
        ...acc,
        [handlerTopic]: handlerConfig,
      };
    }, this.topics$);
  }

  protected async waitForCommandResult<TRes extends AnyEither>(
    topic: string,
    eventId: string,
    timeout: number,
  ): Promise<TRes> {
    let resolved = false;

    const commandResultPromise = new Promise<TRes>((resolve, reject) => {
      const subscription = this.topics$[topic].out$.subscribe({
        next: (event: IProcessResult<TRes>) => {
          if (eventId === event.eventId) {
            subscription.unsubscribe();
            resolved = true;
            resolve(event.payload);
          }
        },
        error: reject,
        complete: () => {
          if (!resolved) {
            subscription.unsubscribe();
            reject(
              new StreamEndedError(
                `Received complete event before receiving a value for eventId ${eventId} on topic ${topic}`,
              ),
            );
          }
        },
      });
    });
    if (timeout === 0) {
      return commandResultPromise;
    }

    return Promise.race([
      sleep(timeout, true).then(() => {
        throw new TimeoutExceededError(`Timeout while waiting for event with eventId: ${eventId} on topic ${topic}`);
      }),
      commandResultPromise,
    ]);
  }

  protected async onLeftResult(eventId: string, left: Left<Error>, _scope?: TransactionalScope): Promise<void> {
    this.logger.error(
      { error: left.left },
      `Unexpected left-result in handler for event ${eventId}: ${left.left.message}`,
    );
  }

  public stream(topic?: string): Observable<ICommand> {
    if (topic) {
      return this.topics$[topic].out$;
    }
    return merge(...Object.values(this.topics$).map(topicConfig => topicConfig.out$));
  }
}
