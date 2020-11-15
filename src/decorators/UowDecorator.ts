import { Logger, sleep } from "@figedi/svc";
import { isLeft, left } from "fp-ts/lib/Either";
import { random } from "lodash";
import { EntityManager } from "typeorm";
import { IsolationLevel } from "typeorm/driver/types/IsolationLevel";
import VError from "verror";

import { isCommandHandler, mergeObjectContext, mergeWithParentCommand } from "../common";
import {
  AnyEither,
  ClassContextProvider,
  HandlerContext,
  ICommand,
  ICommandHandler,
  IDecorator,
  IQuery,
  IQueryHandler,
  StringEither,
} from "../types";

export class TxTimeoutError extends VError {
  constructor(public message: string, public code: string) {
    super(message);
  }
}

enum ErrorCodes {
  TIMEOUT = "TIMEOUT",
}

export interface UowTxSettings {
  enabled: boolean;
  isolation?: IsolationLevel;
  timeoutMs: number | undefined;
  maxRetries: number;
  sleepRange: {
    min: number;
    max: number;
  };
}

export class UowDecorator implements IDecorator {
  constructor(private txSettings: UowTxSettings, private ctxProvider: ClassContextProvider) {}

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
    if (isCommandHandler(handler)) {
      const originalHandle = handler.handle.bind(handler);

      // eslint-disable-next-line no-param-reassign
      handler.handle = async (commandOrQuery: T, ctx: HandlerContext) => {
        const process = async (em: EntityManager) => {
          const scopedCtx = { ...ctx, scope: em };
          const result = (await originalHandle(commandOrQuery, scopedCtx)) as TRes;
          await this.maybePublishCommandEvents(commandOrQuery, result, handler, scopedCtx, false);
          return result;
        };

        const processWithoutTx = async (): Promise<TRes> => {
          try {
            return await process(ctx.scope);
          } catch (e) {
            ctx.logger.error(
              { error: e },
              `Unknown error in uow-decorator for handler: ${commandOrQuery.meta.className} ` +
                `(${commandOrQuery.meta.eventId})`,
            );
            return left(e) as TRes;
          }
        };

        const processInTx = async (tries = 0): Promise<TRes> => {
          const queryRunner =
            (ctx.scope.queryRunner && !ctx.scope.queryRunner.isReleased && ctx.scope.queryRunner) ||
            ctx.scope.connection.createQueryRunner();

          try {
            await queryRunner.connect();
            await queryRunner.startTransaction(this.txSettings.isolation || "REPEATABLE READ");

            const result = this.txSettings.timeoutMs
              ? await Promise.race([
                  process(queryRunner.manager),
                  sleep(this.txSettings.timeoutMs).then(() => {
                    throw new TxTimeoutError(
                      `Timeout for ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
                      ErrorCodes.TIMEOUT,
                    );
                  }),
                ])
              : await process(queryRunner.manager);

            /**
             * in case of a left result, the tx must be rolled back,
             * thus we throw to catch it in the following rollback block
             */
            if (isLeft(result)) {
              throw result.left;
            }
            await queryRunner.commitTransaction();
            return result;
          } catch (e) {
            const isConcurrentUpdateError = e.code === "40001";
            const isDeadlockError = e.code === "40P01";
            const isTimeoutError = e.code === ErrorCodes.TIMEOUT;
            if (!isConcurrentUpdateError && !isDeadlockError && !isTimeoutError) {
              ctx.logger.error(
                { error: e, message: e.message },
                `Error in uow-decorator for handler: ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
              );
            }

            await queryRunner.rollbackTransaction();
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
              await queryRunner.manager.release();
              await queryRunner.release();
              return processInTx(tries + 1);
            }
            return left(e) as TRes;
          } finally {
            await queryRunner.manager.release();
            await queryRunner.release();
          }
        };

        return this.txSettings.enabled ? processInTx() : processWithoutTx();
      };
      return handler;
    }
    return handler;
  }
}
