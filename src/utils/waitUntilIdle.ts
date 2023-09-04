import type { Logger } from "@figedi/svc";
import { TimeoutExceededError } from "../errors.js";
import type { IEventStore } from "../infrastructure/types.js";
import { sleep } from "./sleep.js";

export const createWaitUntilIdle =
  (eventStore: IEventStore) =>
  async (logger: Logger, maxTimeoutMs = 60000, idleTimeoutMs = 500, stepMs = 100) => {
    const findUnprocessedCommands = () => eventStore.findUnprocessedCommands(["eventId"]);

    const process = async () => {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const events = await findUnprocessedCommands();
        if (!events.length) {
          logger.info(`Did not find any commands anymore, will sleep ${idleTimeoutMs}ms and then return on inactivity`);
          await sleep(idleTimeoutMs, true);
          const nextEvents = await findUnprocessedCommands();
          if (!nextEvents.length) {
            logger.info(`Did not find any commands anymore after ${idleTimeoutMs}ms, we are done here`);
            return;
          }
          logger.info("Found unprocessed commands again, re-starting checking");
        }

        await sleep(stepMs, true);
      }
    };
    if (maxTimeoutMs > 0) {
      return Promise.race([
        process(),
        sleep(maxTimeoutMs, true).then(() => {
          throw new TimeoutExceededError("Timeout met while waiting for events to be processed");
        }),
      ]);
    }
    return process();
  };
