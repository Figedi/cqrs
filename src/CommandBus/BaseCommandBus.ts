import type {
  AnyEither,
  Constructor,
  ICommand,
  ICommandHandler,
  IDecorator,
  IHandlerConfig,
  IProcessResult,
  ITransactionalScope,
  IUniformRetryOpts,
} from "../types.js";
import { ApplicationError, ConfigError, StreamEndedError, TimeoutExceededError } from "../errors.js";
import type { Observable, Subscription } from "rxjs";
import { Subject, defer, merge } from "rxjs";
import { filter, map, mergeMap, retry, share, tap } from "rxjs/operators";
import { identity, isNil } from "lodash-es";

import type { IPersistedEvent } from "../infrastructure/types.js";
import type { Left } from "fp-ts/lib/Either.js";
import type { Logger } from "@figedi/svc";
import RateLimiter from "../utils/rxjsRateLimiter.js";
import type { RetryBackoffConfig } from "backoff-rxjs";
import { deserializeEvent } from "../common.js";
import { isLeft } from "fp-ts/lib/Either.js";
import { retryBackoff } from "backoff-rxjs";
import { sleep } from "../utils/sleep.js";

interface IInitializedTopicConsumer<O> {
  meta: Omit<IHandlerConfig<ICommand>, "classType">;
  out$: Observable<O>;
  subscription?: Subscription;
}

export interface IMeteredCommandHandlerResult<TRes extends AnyEither> {
  scope: ITransactionalScope;
  eventId: string;
  payload: TRes;
}

const verifyExclusive = (a: any, b: any, help: string) => {
  if (!isNil(a) && !isNil(b)) {
    throw new ConfigError(help);
  }
};

export class BaseCommandBus {
  public registeredCommands: Constructor<ICommand>[] = [];

  protected topics$: Record<string, IInitializedTopicConsumer<any>> = {};

  protected decorators: IDecorator[] = [];

  protected in$ = new Subject<{ scope: ITransactionalScope; command: ICommand }>();

  constructor(protected logger: Logger) {}

  public deserializeCommand(command: IPersistedEvent): ICommand {
    const registeredTopics = Object.values(this.topics$);
    const klass = registeredTopics.find(({ meta: { handles } }) => handles?.name === command.eventName)?.meta.handles;
    if (!klass) {
      throw new ApplicationError(`Did not find registered command for event ${command.eventName}`);
    }
    return deserializeEvent(command.event, klass) as ICommand;
  }

  public registerDecorator = (decorator: IDecorator): void => {
    this.decorators.push(decorator);
  };

  private handleCommand = async <TPayload extends ICommand, TRes extends AnyEither>(
    handler: ICommandHandler<TPayload, TRes>,
    command: TPayload,
    scope: ITransactionalScope,
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
  ) =>
    this.decorators.reduce((acc, decorator) => decorator.decorate(acc), handler as any) as unknown as ICommandHandler<
      TPayload,
      TRes
    >;

  private getRetryMethod = (retriesConfig?: IHandlerConfig["retries"]) => {
    if (!retriesConfig) {
      return identity;
    }
    if (typeof retriesConfig === "number") {
      return retry<any>(retriesConfig);
    }

    if (retriesConfig.mode === "UNIFORM") {
      if ((retriesConfig.opts as IUniformRetryOpts).maxRetries === undefined) {
        throw new ApplicationError("Need to provide maxRetries when selection retryConfig UNIFORM");
      }
      return retry<any>(retriesConfig.opts!.maxRetries);
    }
    return retryBackoff(retriesConfig.opts as RetryBackoffConfig);
  };

  public register(...handlers: ICommandHandler<any, any>[]) {
    this.topics$ = handlers.reduce((acc, h) => {
      const handlerTopic = h.config.topic;

      const decoratedHandler = this.decorateHandler(h);

      const { concurrency, maxPerSecond, retries } = decoratedHandler.config || {};

      verifyExclusive(concurrency, maxPerSecond, "Cannot have concurrency and maxPerSecond at the same time");
      const rateLimiter = maxPerSecond ? new RateLimiter(maxPerSecond, 1000) : undefined;

      this.registeredCommands.push(h.config.handles!);

      const out$ = this.in$.pipe(
        filter(({ command }) => command.meta.className === handlerTopic),
        map(event => ({ ...event, handler: decoratedHandler })),
        filter(({ handler }) => !!handler),
        rateLimiter
          ? mergeMap(({ command, scope, handler }) =>
              rateLimiter.limit(defer(() => this.handleCommand(handler, command, scope))),
            )
          : mergeMap(({ command, scope, handler }) => this.handleCommand(handler, command, scope), concurrency),
        this.getRetryMethod(retries),
        tap((result: IMeteredCommandHandlerResult<AnyEither>) => {
          if (!isLeft(result.payload)) {
            return;
          }
          this.onLeftResult(result.eventId, result.payload, result.scope).catch((e: any) => {
            this.logger.error({ error: e }, `Unknown error happened while processing left-result: ${e.message}`);
          });
        }),
        share(),
      );

      const handlerConfig = {
        meta: {
          handles: h.config.handles,
          topic: handlerTopic,
          maxPerSecond,
          concurrency,
          retries,
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected async onLeftResult(eventId: string, left: Left<Error>, _scope: ITransactionalScope): Promise<void> {
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
