import type { Logger } from "@figedi/svc";
import { Job, scheduleJob } from "node-schedule";

import { createEvent } from "../common";
import { Constructor, IEvent, IEventBus, StringEither } from "../types";

export interface ITimePassedPayload {
  now: Date;
}

export class MinutePassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class HourPassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class DayPassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class WeekPassed extends createEvent<ITimePassedPayload, StringEither>() {}

export class TimeBasedEventScheduler {
  private jobs: Job[] = [];

  constructor(private eventBus: IEventBus, private logger: Logger) {}

  private dispatchEvent = (EventCtor: Constructor<IEvent>) => (): void => {
    this.eventBus.execute(new EventCtor({ now: new Date() }), { transient: true }).catch(e => {
      this.logger.error({ error: e }, `Unknown error while trying to dispatch time-passed event: ${e.message}`);
    });
  };

  public preflight() {
    this.jobs = [
      scheduleJob("* * * * *", this.dispatchEvent(MinutePassed)),
      scheduleJob("0 * * * *", this.dispatchEvent(HourPassed)),
      scheduleJob("0 0 * * *", this.dispatchEvent(DayPassed)),
      scheduleJob("0 0 * * 1", this.dispatchEvent(WeekPassed)),
    ];
  }

  public shutdown() {
    this.jobs.forEach(job => job.cancel(false));
  }
}
