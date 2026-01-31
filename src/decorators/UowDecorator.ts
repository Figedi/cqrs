import type {
  AnyEither,
  ClassContextProvider,
  HandlerContext,
  ICommand,
  ICommandHandler,
  IDecorator,
  IQuery,
  IQueryHandler,
  Logger,
  StringEither,
} from "../types.js";
import type { KyselyDb } from "../infrastructure/db/index.js";
import type { IsolationLevel } from "../infrastructure/db/index.js";
import { IsolationLevels, isTransaction } from "../infrastructure/db/index.js";
import { isCommandHandler, mergeObjectContext, mergeWithParentCommand } from "../common.js";
import { isLeft, left } from "fp-ts/lib/Either.js";
import { TxTimeoutError } from "../errors.js";
import { random } from "lodash-es";
import { sleep } from "../utils/sleep.js";

enum ErrorCodes {
  TIMEOUT = "TIMEOUT",
}

export interface UowTxSettings {
  enabled: boolean;
  isolationLevel?: IsolationLevel;
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
    private db: KyselyDb,
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
      .map((event) => event.meta?.eventId)
      .filter((eventId) => !!eventId);

    const publishableEvents = handler.publishableEvents
      .filter((event) => publishableEventIds.includes(event.meta?.eventId))
      .map((event) => mergeWithParentCommand(event, parentCommand));

    // eslint-disable-next-line no-param-reassign
    handler.publishableEvents = handler.publishableEvents.filter(
      (event) => !publishableEventIds.includes(event.meta?.eventId),
    );
    return Promise.all(
      publishableEvents.map((event) => mergeObjectContext(this.ctxProvider, event, ctx).publish(delayUntilNextTick)),
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

  private isRetryableError(e: any): boolean {
    const isConcurrentUpdateError = e.code === "40001";
    const isDeadlockError = e.code === "40P01";
    const isTimeoutError = e.code === ErrorCodes.TIMEOUT;
    return isConcurrentUpdateError || isDeadlockError || isTimeoutError;
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

      const processWithoutTx = async (): Promise<TRes> => {
        try {
          // Run without transaction - just call the handler directly
          const result = (await originalHandle(commandOrQuery, ctx)) as TRes;
          await this.maybePublishCommandEvents(commandOrQuery, result, handler, ctx, true);
          return result;
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
        const isolationLevel = this.txSettings.isolationLevel ?? IsolationLevels.Serializable;

        // Check if we're already inside a transaction (e.g., from PollingWorker)
        // If so, reuse that transaction instead of starting a new one
        const existingTrx = isTransaction(ctx.scope) ? ctx.scope : null;

        const executeInTransaction = async (trx: typeof ctx.scope): Promise<TRes> => {
          const scopedCtx = { ...ctx, scope: trx };

          const result = this.txSettings.timeoutMs
            ? await Promise.race([
                (async () => {
                  const res = (await originalHandle(commandOrQuery, scopedCtx)) as TRes;
                  await this.maybePublishCommandEvents(commandOrQuery, res, handler, scopedCtx, false);
                  return res;
                })(),
                sleep(this.txSettings.timeoutMs).then(() => {
                  throw new TxTimeoutError(
                    `Timeout for ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
                    ErrorCodes.TIMEOUT,
                  );
                }),
              ])
            : await (async () => {
                const res = (await originalHandle(commandOrQuery, scopedCtx)) as TRes;
                await this.maybePublishCommandEvents(commandOrQuery, res, handler, scopedCtx, false);
                return res;
              })();

          if (isLeft(result)) {
            throw result.left;
          }
          return result as TRes;
        };

        try {
          // If already in a transaction, reuse it
          if (existingTrx) {
            return await executeInTransaction(existingTrx);
          }

          // Otherwise, start a new transaction
          return await this.db
            .transaction()
            .setIsolationLevel(isolationLevel)
            .execute(async (trx) => executeInTransaction(trx));
        } catch (e: any) {
          // Retry logic for deadlocks, concurrent updates, or timeouts
          if (this.isRetryableError(e)) {
            if (tries >= this.txSettings.maxRetries) {
              ctx.logger.error(
                `Used up all retries for handler: ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId}).`,
              );
              return left(e) as TRes;
            }
            const sleepTimeMs = random(this.txSettings.sleepRange.min, this.txSettings.sleepRange.max);
            this.maybeLogTxErrorResult(e, commandOrQuery, sleepTimeMs, tries, ctx.logger);

            await sleep(sleepTimeMs);
            return await processInTx(tries + 1);
          }

          ctx.logger.error(
            { error: e, message: e.message },
            `Error in uow-decorator for handler: ${commandOrQuery.meta.className} (${commandOrQuery.meta.eventId})`,
          );
          return left(e) as TRes;
        }
      };

      return this.txSettings.enabled ? processInTx() : processWithoutTx();
    };
    return handler;
  }
}
