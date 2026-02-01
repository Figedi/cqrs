import { isLeft } from "fp-ts/lib/Either.js"
import type {
  AnyEither,
  HandlerContext,
  ICommand,
  ICommandHandler,
  IDecorator,
  IQuery,
  IQueryHandler,
  Logger,
} from "../types.js"
import { CQRSEventType } from "../types.js"

export const convertTimingsToMs = ([seconds, nanoseconds]: number[]): number =>
  ((seconds * 1e9 + nanoseconds) / 1e6) | 0

export class LoggingDecorator implements IDecorator {
  constructor(private rootLogger: Logger) {}

  decorate<T extends ICommand | IQuery, TRes extends AnyEither>(
    handler: ICommandHandler<T, TRes> | IQueryHandler<T, TRes>,
  ) {
    const originalHandle = handler.handle.bind(handler)
    handler.handle = async (commandOrQuery: T, ctx: HandlerContext) => {
      const eventType = commandOrQuery.meta.classType?.toLowerCase() || CQRSEventType.QUERY
      const logger = this.rootLogger.child({
        eventId: commandOrQuery.meta.eventId,
        streamId: commandOrQuery.meta.streamId,
        [eventType]: commandOrQuery.meta.className,
      })
      logger.debug(`Started processing ${eventType} (${commandOrQuery.meta.className})`)
      const t1 = process.hrtime()
      try {
        const result = (await originalHandle(commandOrQuery, {
          ...ctx,
          logger,
        })) as TRes
        const timeTaken = convertTimingsToMs(process.hrtime(t1))
        if (isLeft(result)) {
          logger.error(
            { processingTimeMS: timeTaken, error: result.left },
            `Error while processing ${eventType} (${commandOrQuery.meta.className}): ${result.left.message}`,
          )
        } else {
          logger.debug({ processingTimeMS: timeTaken }, `Processed ${eventType} (${commandOrQuery.meta.className})`)
        }
        return result
      } catch (e: any) {
        logger.error(
          { error: e },
          `Unknown error while processing ${eventType} (${commandOrQuery.meta.className}): ${e.message}`,
        )
        throw e
      }
    }
    return handler
  }
}
