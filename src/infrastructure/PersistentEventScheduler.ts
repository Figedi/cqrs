import { Logger, ServiceWithLifecycleHandlers } from "@figedi/svc";
import { isLeft } from "fp-ts/lib/Either";
import { deserializeEvent, serializeEvent } from "../common";
import { AnyEither, ICommand, ICommandBus, ScheduledEventStatus, StringEither } from "../types";
import { ScheduledEventEntity } from "./ScheduledEventEntity";
import { IEventScheduler, IScheduleOptions, IScopeProvider } from "./types";

export class PersistentEventScheduler implements IEventScheduler, ServiceWithLifecycleHandlers {
  private schedules: Record<string, NodeJS.Timeout> = {};

  constructor(private scopeProvider: IScopeProvider, private commandBus: ICommandBus, private logger: Logger) {}

  private get eventRepo() {
    return this.scopeProvider().getRepository(ScheduledEventEntity);
  }

  private onCommandExecute = <TRes extends AnyEither>(
    command: ICommand<any, TRes>,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ) => async (): Promise<void> => {
    const eventId = command.meta.eventId!;
    let result: TRes | StringEither;
    try {
      if (executeOpts?.executeSync) {
        result = await this.commandBus.executeSync(command, executeOpts);
      } else {
        result = await this.commandBus.execute(command, executeOpts);
      }
      onExecute?.(result);
      await this.updateScheduledEventStatus(command, isLeft(result) ? "FAILED" : "PROCESSED");
    } catch (e) {
      await this.updateScheduledEventStatus(command, "FAILED");
    } finally {
      clearTimeout(this.schedules[eventId]);
    }
  };

  public async scheduleCommand<TPayload extends Record<string, any>, TRes extends AnyEither>(
    command: ICommand<TPayload, TRes>,
    executeAt: Date,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ): Promise<string> {
    const now = Date.now();
    const scheduleTime = executeAt.getTime() - now;

    if (scheduleTime < 0) {
      throw new Error(`Cannot schedule events in the past, you passed executeAt = ${executeAt.toISOString()}`);
    }
    const eventId = command.meta.eventId;

    if (!eventId) {
      throw new Error("Passed command does not have an eventId, refusing to schedule it");
    }

    if (this.schedules[eventId]) {
      throw new Error(
        `ScheduledEvent for passed command w/ eventId ${eventId} already exists, refusing to schedule it`,
      );
    }
    const result = await this.eventRepo
      .createQueryBuilder()
      .insert()
      .values({
        scheduledEventId: eventId,
        executeAt,
        event: serializeEvent(command),
        status: "CREATED",
      })
      .returning("scheduled_event_id")
      .execute();
    const timer = setTimeout(this.onCommandExecute(command, onExecute, executeOpts), scheduleTime);
    timer.unref();
    this.schedules[eventId] = timer;

    this.logger.debug(
      { command },
      `Scheduled event w/ eventId ${eventId}, execution time will be at ${executeAt.toISOString()}`,
    );

    return result.identifiers[0].scheduledEventId;
  }

  public async updateScheduledEventStatus(command: ICommand, status: ScheduledEventStatus): Promise<void> {
    const eventId = command.meta.eventId;

    if (!eventId) {
      throw new Error("Passed command does not have an eventId, refusing to schedule it");
    }
    const updateResult = await this.eventRepo.update({ scheduledEventId: eventId }, { status });
    const timer = this.schedules[eventId];
    if (status !== "CREATED" && timer) {
      clearTimeout(timer);
      delete this.schedules[eventId];
    }
    if (!updateResult.affected) {
      throw new Error(`ScheduledEvent by eventId: ${eventId} does not exist, cannot update`);
    }
  }

  public async reset(): Promise<number> {
    const updateResult = await this.eventRepo.update({ status: "CREATED" }, { status: "ABORTED" });
    Object.values(this.schedules).forEach(clearTimeout);
    this.schedules = {};
    return updateResult.affected || 0;
  }

  public async preflight() {
    const eventSchedules = await this.eventRepo.find({ where: { status: "CREATED" }, relations: ["event"] });
    if (!eventSchedules.length) {
      return;
    }
    const now = Date.now();
    await Promise.all(
      eventSchedules.map(async eventSchedule => {
        if (!eventSchedule.event) {
          throw new Error(`Did not find a command for eventSchedule: ${eventSchedule.scheduledEventId}`);
        }
        const klass = this.commandBus.registeredCommands.find(command => {
          return (command as any).name === eventSchedule.event!.meta.className;
        });
        if (!klass) {
          throw new Error(
            `Did not find a registered command-type for eventSchedule: ${eventSchedule.scheduledEventId}`,
          );
        }
        const command = deserializeEvent(eventSchedule.event!.payload, klass);
        const scheduleTime = eventSchedule.executeAt.getTime() - now;
        if (scheduleTime < 0) {
          this.logger.warn(`Recovered event w/ eventId ${eventSchedule.scheduledEventId} expired, will not re-arm it`);
          return this.updateScheduledEventStatus(command, "ABORTED");
        }
        const eventId = eventSchedule.event.meta.eventId!;
        this.logger.warn(
          `Re-armed previously persisted event-schedule for eventId ${eventId}, ` +
            `will trigger it at ${eventSchedule.executeAt.toISOString()}`,
        );

        const timer = setTimeout(this.onCommandExecute(command), scheduleTime);
        timer.unref();
        this.schedules[eventId] = timer;
      }),
    );
  }
}
export class InMemoryEventScheduler implements IEventScheduler, ServiceWithLifecycleHandlers {
  private schedules: Record<string, NodeJS.Timeout> = {};

  constructor(_: IScopeProvider, private commandBus: ICommandBus, private logger: Logger) {}

  private onCommandExecute = <TRes extends AnyEither>(
    command: ICommand<any, TRes>,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ) => async (): Promise<void> => {
    const eventId = command.meta.eventId!;
    let result: TRes | StringEither;
    try {
      if (executeOpts?.executeSync) {
        result = await this.commandBus.executeSync(command, executeOpts);
      } else {
        result = await this.commandBus.execute(command, executeOpts);
      }
      onExecute?.(result);
    } finally {
      clearTimeout(this.schedules[eventId]);
    }
  };

  public async scheduleCommand<TPayload extends Record<string, any>, TRes extends AnyEither>(
    command: ICommand<TPayload, TRes>,
    executeAt: Date,
    onExecute?: (result: TRes | StringEither) => Promise<void> | void,
    executeOpts?: IScheduleOptions,
  ): Promise<string> {
    const now = Date.now();
    const scheduleTime = executeAt.getTime() - now;

    if (scheduleTime < 0) {
      throw new Error(`Cannot schedule events in the past, you passed executeAt = ${executeAt.toISOString()}`);
    }
    const eventId = command.meta.eventId;

    if (!eventId) {
      throw new Error("Passed command does not have an eventId, refusing to schedule it");
    }

    if (this.schedules[eventId]) {
      throw new Error(
        `ScheduledEvent for passed command w/ eventId ${eventId} already exists, refusing to schedule it`,
      );
    }

    const timer = setTimeout(this.onCommandExecute(command, onExecute, executeOpts), scheduleTime);
    timer.unref();
    this.schedules[eventId] = timer;

    this.logger.debug(
      { command },
      `Scheduled event w/ eventId ${eventId}, execution time will be at ${executeAt.toISOString()}`,
    );

    return eventId;
  }

  public async updateScheduledEventStatus(command: ICommand, status: ScheduledEventStatus): Promise<void> {
    const eventId = command.meta.eventId;

    if (!eventId) {
      throw new Error("Passed command does not have an eventId, refusing to schedule it");
    }
    const timer = this.schedules[eventId];
    if (!timer) {
      throw new Error(`ScheduledEvent by eventId: ${eventId} does not exist, cannot update`);
    }
    if (status !== "CREATED") {
      clearTimeout(timer);
      delete this.schedules[eventId];
    }
  }

  public async reset(): Promise<number> {
    Object.values(this.schedules).forEach(clearTimeout);
    const scheduleLength = Object.keys(this.schedules).length;
    this.schedules = {};
    return scheduleLength;
  }

  public async preflight() {
    // nothing to do here in the inmem version
  }
}
