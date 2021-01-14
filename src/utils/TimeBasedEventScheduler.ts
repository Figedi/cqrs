import type { Logger } from "@figedi/svc";
import { Job, scheduleJob } from "node-schedule";

import { Constructor, IEvent, IEventBus } from "../types";
import { DayPassed, HourPassed, MinutePassed, WeekPassed } from "./internalEvents";

export class TimeBasedEventScheduler {
  private jobs: Job[] = [];

  private registeredJobs: [string, Constructor<IEvent>][] = [];

  constructor(private eventBus: IEventBus, private logger: Logger) {}

  private dispatchEvent = (EventCtor: Constructor<IEvent>) => (): void => {
    this.eventBus.execute(new EventCtor({ now: new Date() }), { transient: true }).catch(e => {
      this.logger.error({ error: e }, `Unknown error while trying to dispatch time-passed event: ${e.message}`);
    });
  };

  public registerJob(cronTab: string, EventCtor: Constructor<IEvent>) {
    this.registeredJobs.push([cronTab, EventCtor]);
  }

  public preflight() {
    this.jobs = [
      ...this.registeredJobs.map(([crontab, event]) => scheduleJob(crontab, this.dispatchEvent(event))),
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
