import { TimeoutExceededError, UnknownStreamIdError } from "../errors.js";
import type { IEventStore } from "../infrastructure/types.js";
import { sleep } from "./sleep.js";

export const createWaitUntilSettled =
  (eventStore: IEventStore) =>
  async (streamIds: string[], timeoutMs = 5000, stepMs = 100) => {
    if (!streamIds.length) {
      return;
    }
    const process = async () => {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const events = await eventStore.findByStreamIds(streamIds, ["status"]);
        if (!events.length) {
          throw new UnknownStreamIdError(`Event with streamIds ${streamIds} not found`);
        }
        if (events.every(event => event.status === "PROCESSED" || event.status === "FAILED")) {
          return;
        }

        await sleep(stepMs, true);
      }
    };
    if (timeoutMs > 0) {
      return Promise.race([
        process(),
        sleep(timeoutMs, true).then(() => {
          throw new TimeoutExceededError("Timeout met while waiting for events to be processed");
        }),
      ]);
    }
    return process();
  };
