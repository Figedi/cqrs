import type {
  AnyEither,
  ICommand,
  ICommandBus,
  IInmemorySettings, IPostgresSettings,
  ISerializedEvent,
  Logger,
  ScheduledEventStatus,
  ServiceWithLifecycleHandlers,
  StringEither
} from "../types.js";
import type { IEventScheduler, IScheduleOptions } from "./types.js";
import { deserializeEvent, serializeEvent } from "../common.js";

import { isLeft } from "fp-ts/lib/Either.js";

export class PersistentEventScheduler implements IEventScheduler, ServiceWithLifecycleHandlers {
  private schedules: Record<string, NodeJS.Timeout> = {};

  constructor(
    private opts: IPostgresSettings,
    private commandBus: ICommandBus,
    private logger: Logger,
  ) {}

  private onCommandExecute =
    <TRes extends AnyEither>(
      command: ICommand<any, TRes>,
      onExecute?: (result: TRes | StringEither) => Promise<void> | void,
      executeOpts?: IScheduleOptions,
    ) =>
    async (): Promise<void> => {
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

    const { rows } = await this.client!.query<{ scheduled_event_id: string }>(
      `INSERT INTO scheduled_events (scheduled_event_id, execute_at, event, status)
       VALUES ($1, $2, $3, $4)
       RETURNING scheduled_event_id`,
      [eventId, executeAt, JSON.stringify(serializeEvent(command)), "CREATED"],
    );

    const timer = setTimeout(this.onCommandExecute(command, onExecute, executeOpts), scheduleTime);
    timer.unref();
    this.schedules[eventId] = timer;

    this.logger.debug(
      { command },
      `Scheduled event w/ eventId ${eventId}, execution time will be at ${executeAt.toISOString()}`,
    );

    return rows[0].scheduled_event_id;
  }

  public async updateScheduledEventStatus(command: ICommand, status: ScheduledEventStatus): Promise<void> {
    const eventId = command.meta.eventId;

    if (!eventId) {
      throw new Error("Passed command does not have an eventId, refusing to schedule it");
    }
    await this.client!.query(
      `UPDATE scheduled_events SET status = $1 WHERE scheduled_event_id = $2`,
      [status, eventId],
    );
    const timer = this.schedules[eventId];
    if (status !== "CREATED" && timer) {
      clearTimeout(timer);
      delete this.schedules[eventId];
    }
  }

  public async reset(): Promise<number> {
    const { rows } = await this.client!.query<{ scheduled_event_id: string }>(
      `UPDATE scheduled_events SET status = 'CREATED' WHERE status = 'ABORTED' RETURNING scheduled_event_id`,
    );
    Object.values(this.schedules).forEach(clearTimeout);
    this.schedules = {};
    return rows.length || 0;
  }

  public async preflight() {
    if ((this.opts as IPostgresSettings).runMigrations) {
      await this.client!.query(`
      CREATE TABLE IF NOT EXISTS "scheduled_events" (
        "scheduled_event_id" text PRIMARY KEY,
        "execute_at" timestamptz NOT NULL,
        "event" jsonb NOT NULL,
        "status" text NOT NULL)`);
    }

    const { rows: eventSchedules } = await this.client!.query<{
      scheduled_event_id: string;
      execute_at: string;
      event: any;
      status: string;
    }>(`SELECT * FROM scheduled_events WHERE status = 'CREATED'`);

    if (!eventSchedules.length) {
      return;
    }
    const now = Date.now();
    await Promise.all(
      eventSchedules.map(async eventSchedule => {
        if (!eventSchedule.event) {
          throw new Error(`Did not find a command for eventSchedule: ${eventSchedule.scheduled_event_id}`);
        }
        const event = eventSchedule.event as ISerializedEvent<any>;
        const klass = this.commandBus.registeredCommands.find(command => {
          return (command as any).name === event.meta?.className;
        });
        if (!klass) {
          throw new Error(
            `Did not find a registered command-type for eventSchedule: ${eventSchedule.scheduled_event_id}`,
          );
        }
        const command = deserializeEvent(event!.payload, klass);
        const scheduleTime = new Date(eventSchedule.execute_at).getTime() - now;
        if (scheduleTime < 0) {
          this.logger.warn(
            `Recovered event w/ eventId ${eventSchedule.scheduled_event_id} expired, will not re-arm it`,
          );
          return this.updateScheduledEventStatus(command, "ABORTED");
        }
        const eventId = event.meta.eventId!;
        this.logger.warn(
          `Re-armed previously persisted event-schedule for eventId ${eventId}, ` +
            `will trigger it at ${new Date(eventSchedule.execute_at).toISOString()}`,
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

  constructor(
    _opts: IInmemorySettings,
    private commandBus: ICommandBus,
    private logger: Logger,
  ) {}

  private onCommandExecute =
    <TRes extends AnyEither>(
      command: ICommand<any, TRes>,
      onExecute?: (result: TRes | StringEither) => Promise<void> | void,
      executeOpts?: IScheduleOptions,
    ) =>
    async (): Promise<void> => {
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
