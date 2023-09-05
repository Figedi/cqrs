import * as db from "zapatos/db";

import type {
  AnyEither,
  ClassContextProvider,
  HandlerContext,
  ICommand,
  ICommandHandler,
  IDecorator,
  IQuery,
  IQueryHandler,
  ITransactionalScope,
  StringEither,
} from "../types.js";
import { isCommandHandler, mergeObjectContext, mergeWithParentCommand } from "../common.js";
import { isLeft, left } from "fp-ts/lib/Either.js";

import type { Logger } from "@figedi/svc";
import { TxTimeoutError } from "../errors.js";
import { random } from "lodash-es";
import { sleep } from "../utils/sleep.js";

enum ErrorCodes {
  TIMEOUT = "TIMEOUT",
}

export interface UowTxSettings {
  enabled: boolean;
  isolationLevel?: db.IsolationLevel;
  timeoutMs: number | undefined;
  maxRetries: number;
  sleepRange: {
    min: number;
    max: number;
  };
}

export class UowDecorator implements IDecorator {
  constructor(
    private txSettings: UowTxSettings,
    private ctxProvider: ClassContextProvider,
  ) {}

  private async maybePublishCommandEvents<TPayload extends ICommand, TRes extends AnyEither>(
    parentCommand: TPayload,
    result: TRes,
    handler: ICommandHandler<TPayload, TRes>,
    ctx: HandlerContext,
    delayUntilNextTick: boolean,
  ): Promise<StringEither[]> {
    if (isLeft(result) || !handler.publishableEvents?.length) {
      return [];
    }
    const publishableEventIds = handler.publishableEvents
      .map(event => event.meta?.eventId)
      .filter(eventId => !!eventId);

    const publishableEvents = handler.publishableEvents
      .filter(event => publishableEventIds.includes(event.meta?.eventId))
      .map(event => mergeWithParentCommand(event, parentCommand));

    // eslint-disable-next-line no-param-reassign
    handler.publishableEvents = handler.publishableEvents.filter(
      event => !publishableEventIds.includes(event.meta?.eventId),
    );
    return Promise.all(
      publishableEvents.map(event => mergeObjectContext(this.ctxProvider, event, ctx).publish(delayUntilNextTick)),
    );
  }

  private maybeLogTxErrorResult<T extends ICommand | IQuery>(
    e: any,
    commandOrQuery: T,
    sleepTimeMs: number,
    tries: number,
    logger: Logger,
  ) {
    const isConcurrentUpdateError = e.code === "40001";
    const isDeadlockError = e.code === "40P01";
    const isTimeoutError = e.code === ErrorCodes.TIMEOUT;
    const { className, eventId } = commandOrQuery.meta;

    if (!isConcurrentUpdateError && !isDeadlockError && !isTimeoutError) {
      logger.error({ error: e }, `Error in uow-decorator for handler: ${className} (${eventId})`);
    }

    if (isConcurrentUpdateError) {
      logger.error(
        `Concurrent-update error for handler: ${className} (${eventId}). ` +
          `Trying one more time in ${sleepTimeMs}ms (try = ${tries + 1})`,
      );
    }
    if (isDeadlockError) {
      logger.error(
        `Deadlock-error for handler: ${className} (${eventId}). Trying one more time in ${sleepTimeMs}ms (try = ${
          tries + 1
        })`,
      );
    }
    if (isTimeoutError) {
      logger.error(
        `Timeout-error for handler: ${className} (${eventId}). Trying one more time in ${sleepTimeMs}ms (try = ${
          tries + 1
        })`,
      );
    }
  }

  decorate<T extends ICommand | IQuery, TRes extends AnyEither>(
    handler: ICommandHandler<T, TRes> | IQueryHandler<T, TRes>,
  ) {
    if (!isCommandHandler(handler)) {
      return handler;
    }
    const originalHandle = handler.handle.bind(handler);

    // eslint-disable-next-line no-param-reassign
    handler.handle = async (commandOrQuery: T, ctx: HandlerContext) => {
      const process = async (scope: ITransactionalScope) => {
        const scopedCtx = { ...ctx, scope };
        const result = (await originalHandle(commandOrQuery, scopedCtx)) as TRes;
        await this.maybePublishCommandEvents(commandOrQuery, result, handler, scopedCtx, false);
        return result;
      };

      const processWithoutTx = async (): Promise<TRes> => {
        try {
          return await process(ctx.scope);
        } catch (e: any) {
          ctx.logger.error(
            { error: e },
            `Unknown error in uow-decorator for handler: ${commandOrQuery.meta.className} ` +
              `(${commandOrQuery.meta.eventId})`,
          );
          return left(e) as TRes;
        }
      };

      const processInTx = async (tries = 0): Promise<TRes> => {
        try {
          return await db.transaction(
            ctx.scope,
            this.txSettings.isolationLevel ?? db.IsolationLevel.Serializable,
            async txScope => {
              const result = this.txSettings.timeoutMs
                ? await Promise.race([
                    process(txScope),
                    sleep(this.txSettings.timeoutMs).then(() => {
                      throw new TxTimeoutError(
                        `Timeout for ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
                        ErrorCodes.TIMEOUT,
                      );
                    }),
                  ])
                : await process(txScope);

              /**
               * in case of a left result, the tx must be rolled back,
               * thus we throw to catch it in the following rollback block
               */
              if (isLeft(result)) {
                throw result.left;
              }
              return result;
            },
          );
        } catch (e: any) {
          const isConcurrentUpdateError = e.code === "40001";
          const isDeadlockError = e.code === "40P01";
          const isTimeoutError = e.code === ErrorCodes.TIMEOUT;
          if (!isConcurrentUpdateError && !isDeadlockError && !isTimeoutError) {
            ctx.logger.error(
              { error: e, message: e.message },
              `Error in uow-decorator for handler: ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
            );
          }

          /**
           * Resolve deadlocks, concurrent-updates or timeouts (which could result from a deadlock),
           * by simply retrying later
           */
          if (isConcurrentUpdateError || isDeadlockError || isTimeoutError) {
            if (tries >= this.txSettings.maxRetries) {
              ctx.logger.error(
                `Used up all retries for handler: ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId}).`,
              );
              return left(e) as TRes;
            }
            const sleepTimeMs = random(this.txSettings.sleepRange.min, this.txSettings.sleepRange.max);
            this.maybeLogTxErrorResult(e, commandOrQuery, sleepTimeMs, tries, ctx.logger);

            await sleep(sleepTimeMs);
            // execute the finally block before the return
            return await processInTx(tries + 1);
          }
          return left(e) as TRes;
        }
      };

      return this.txSettings.enabled ? processInTx() : processWithoutTx();
    };
    return handler;
  }
}
