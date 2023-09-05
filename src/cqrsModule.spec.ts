import * as db from "zapatos/db";

import { ConfigError, RetriesExceededError } from "./errors.js";
import type {
  Constructor,
  HandlerContext,
  ICQRSSettings,
  ICommand,
  ICommandBus,
  IEvent,
  IQuery,
  ISaga,
  ITransactionalScope,
  VoidEither,
} from "./types.js";
import { EMPTY, of } from "rxjs";
import type { Either, Right } from "fp-ts/lib/Either.js";
import { assert, stub, useFakeTimers } from "sinon";
import {
  createCommand,
  createCommandHandler,
  createEvent,
  createQuery,
  createQueryHandler,
  serializeEvent,
} from "./common.js";
import { isLeft, isRight, left, right } from "fp-ts/lib/Either.js";
import pg, { type PoolClient } from "pg";

import { CQRSModule } from "./cqrsModule.js";
import type { Observable } from "rxjs";
import type { Option } from "fp-ts/lib/Option.js";
import type Sinon from "sinon";
import { expect } from "chai";
import { mergeMap } from "rxjs/operators";
import { none } from "fp-ts/lib/Option.js";
import { sleep } from "./utils/sleep.js";
import { times } from "lodash-es";
import { v4 as uuid } from "uuid";

// const strFromTxnId = (txnId: number | undefined) => (txnId === undefined ? "-" : String(txnId));
// db.setConfig({
//   queryListener: (query, txnId) => console.log(`[db:query] (${strFromTxnId(txnId)}) ${query.text}\n${query.values}`),
// });

type ExampleQueryResult = Either<Error, { id: string; result: number }>;

class OtherCommand extends createCommand<{ id: string; type: string }>() {}
class ExampleCommand extends createCommand<{ id: string; type: string }>() {}
class ExampleEvent extends createEvent<{ id: string; type: string }>() {}
class ExampleQuery extends createQuery<{ id: string; type: string }, ExampleQueryResult>() {}

type ExampleCommandResult = Either<Error, Option<never>>;

const createExampleCommandHandler = <
  TPayload extends { id: string; type: string },
  TCommand extends ICommand<TPayload>,
>(
  cb: (payload: TPayload, scope: ITransactionalScope) => void | Promise<void>,
  Command: Constructor<TCommand> = ExampleCommand as unknown as Constructor<TCommand>,
  shouldTriggerEvent = true,
) =>
  class ExampleCommandHandler extends createCommandHandler<any>(Command) {
    constructor() {
      super({
        retries: 3,
        concurrency: 20,
      });
    }

    public async handle({ payload }: TCommand, { scope }: HandlerContext): Promise<ExampleCommandResult> {
      if (payload.id !== "42") {
        return left(new Error(`42 is the truth to everything. Your id is ${payload.id}`));
      }

      if (shouldTriggerEvent) {
        this.apply(new ExampleEvent(payload));
      }
      await cb(payload, scope);
      return right(none);
    }
  };

const createExampleQueryHandler = <TPayload extends { id: string; type: string }, TQuery extends IQuery<TPayload>>(
  cb: (payload: TPayload) => void,
  Query: Constructor<TQuery> = ExampleQuery as unknown as Constructor<TQuery>,
) =>
  class ExampleQueryHandler extends createQueryHandler<any>(Query) {
    public async handle({ payload }: ExampleCommand): Promise<ExampleQueryResult> {
      if (payload.id !== "42") {
        return left(new Error(`42 is the truth to everything. Your id is ${payload.id}`));
      }
      cb(payload as TPayload);
      return right({ id: payload.id, result: +payload.id * 2 });
    }
  };

const assertWithRetries = async (fn: () => Promise<void>, retries = 3, sleepBetweenFailures = 100) => {
  let tries = 0;
  do {
    try {
      return await fn();
    } catch (e) {
      await sleep(sleepBetweenFailures, true);
      tries += 1;
    }
  } while (tries < retries);
  throw new RetriesExceededError("Retries exceeded");
};

const executeAndWaitForPersistentCommand = async <T, TRes extends VoidEither>(
  client: PoolClient,
  commandBus: ICommandBus,
  command: ICommand<T, TRes>,
) => {
  let respId: string;
  const event$ = new Promise<TRes>((resolve, reject) =>
    commandBus.stream(command.constructor.name).subscribe({
      next: (event: any) => {
        if (event.eventId === respId) {
          resolve(event.payload as TRes);
        }
      },
      error: reject,
    }),
  );
  const commandResponse = await commandBus.execute(command);
  if (!isRight(commandResponse)) {
    throw new Error("Command failed");
  }
  respId = commandResponse.right;
  const response = await event$;
  expect(isRight(response)).to.equal(true);
  await assertWithRetries(async () => {
    const [event] = await db.select("events", { event_id: respId }).run(client);
    expect(event!.status).to.equal("PROCESSED");
  });
  return response;
};

const executeAndWaitForInmemoryCommand = async <T, TRes extends VoidEither>(
  commandBus: ICommandBus,
  command: ICommand<T, TRes>,
) => {
  let respId: string;
  const event$ = new Promise<TRes>((resolve, reject) =>
    commandBus.stream(command.constructor.name).subscribe({
      next: (event: any) => {
        if (event.eventId === respId) {
          resolve(event.payload as TRes);
        }
      },
      error: reject,
    }),
  );
  const commandResponse = await commandBus.execute(command);
  if (!isRight(commandResponse)) {
    throw new Error("Command failed");
  }
  respId = commandResponse.right;
  const response = await event$;
  expect(isRight(response)).to.equal(true);
  return response;
};

const txSettings = {
  enabled: true,
  timeoutMs: 0,
  maxRetries: 3,
  sleepRange: {
    min: 100,
    max: 5000,
  },
};

describe("cqrsModule", () => {
  const logger: any = {
    warn: () => {},
    error: () => {},
    debug: () => {},
    info: () => {},
  };
  logger.child = () => logger;

  const ID = "42";

  describe.skip("persistence mode = inmem", () => {
    describe("core behaviour", () => {
      let cqrsModule: CQRSModule;

      beforeEach(async () => {
        cqrsModule = new CQRSModule({ transaction: txSettings, persistence: { type: "inmem" } }, logger);
      });

      it("should accept commands", async () => {
        const cmdCb = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
        cqrsModule.commandBus.register(new ExampleCommandHandler());
        const commandPayload = { id: ID, type: "command" };
        await executeAndWaitForInmemoryCommand(cqrsModule.commandBus, new ExampleCommand(commandPayload));
        assert.calledOnce(cmdCb);
        assert.calledWith(cmdCb, commandPayload);
      });

      it("should accept queries", async () => {
        const cmdCb = stub();
        const ExampleQueryHandler = createExampleQueryHandler(cmdCb);
        cqrsModule.queryBus.register(new ExampleQueryHandler());
        const queryPayload = { id: ID, type: "command" };
        const result = await cqrsModule.queryBus.execute(new ExampleQuery(queryPayload));

        expect(isRight(result)).to.equal(true);
        assert.calledOnce(cmdCb);
        assert.calledWith(cmdCb, queryPayload);
        expect((result as Right<{ id: string; result: number }>).right.result).to.equal(+ID * 2);
      });

      it("should accept events", async () => {
        const result = await cqrsModule.eventBus.execute(new ExampleEvent({ id: ID, type: "event" }));

        expect(isRight(result)).to.equal(true);
      });
    });
  });

  describe("persistence mode = pg", () => {
    // eslint-disable-next-line max-len
    const connectionUrl = process.env.DATABASE_URL ?? "";
    if (!connectionUrl.length) {
      throw new ConfigError("Please define DATABASE_URL to run integration tests for exampleCommand");
    }

    const pool = new pg.Pool({ max: 50, connectionString: process.env.DATABASE_URL });
    let client: PoolClient;

    let cqrsModule: CQRSModule;

    before(async () => {
      const CQRS_OPTIONS: ICQRSSettings = {
        transaction: txSettings,
        persistence: {
          client: pool,
          type: "pg",
          runMigrations: true,
          options: {
            max: 50,
          },
        },
      };
      cqrsModule = new CQRSModule(CQRS_OPTIONS as ICQRSSettings, logger);
    });

    beforeEach(async () => {
      client = await pool.connect();
      await cqrsModule.preflight();
      await client.query("TRUNCATE TABLE events RESTART IDENTITY CASCADE");
    });

    afterEach(async () => {
      client?.release();
    });

    describe("EventScheduler", () => {
      let timer: Sinon.SinonFakeTimers;
      const now = Date.now();
      beforeEach(async () => {
        timer = useFakeTimers({ now, shouldClearNativeTimers: true });
        await client.query("TRUNCATE TABLE scheduled_events RESTART IDENTITY CASCADE");
      });

      afterEach(() => {
        timer.restore();
      });

      it("schedules events, persist them and trigger a command at a specific time (executeSync)", async () => {
        const cmdCb = stub();
        const resultCb = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
        const cmdPayload = { id: ID, type: "command" };

        cqrsModule.commandBus.register(new ExampleCommandHandler());
        await cqrsModule.eventScheduler.scheduleCommand(new ExampleCommand(cmdPayload), new Date(now + 100), resultCb, {
          executeSync: true,
        });
        const { rows: scheduledEvents } = await client.query<any>(
          "SELECT event->'meta'->>'eventId' as event_id FROM scheduled_events",
        );
        expect(scheduledEvents).to.have.lengthOf(1);
        timer.tick(100);
        timer.restore();
        await sleep(200);
        const { rows: executedCommands } = await client.query<any>(
          "SELECT event_id, status FROM events WHERE type = 'COMMAND'",
        );
        assert.calledOnce(cmdCb);
        assert.calledOnce(resultCb);
        assert.calledWith(cmdCb, cmdPayload);
        assert.calledWith(resultCb, right(none));
        expect(executedCommands[0].event_id).to.eq(scheduledEvents[0].event_id);
        expect(executedCommands[0].status).to.eq("PROCESSED");

        const { rows } = await client.query<any>(
          "SELECT status FROM scheduled_events WHERE event->'meta'->>'eventId' = $1",
          [scheduledEvents[0].event_id],
        );
        expect(rows[0].status).to.eq("PROCESSED");
      });

      it("schedules events, persist them and trigger a command at a specific time (execute)", async () => {
        const cmdCb = stub();
        const resultCb = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
        const cmdPayload = { id: ID, type: "command" };

        cqrsModule.commandBus.register(new ExampleCommandHandler());
        const command = new ExampleCommand(cmdPayload);
        await cqrsModule.eventScheduler.scheduleCommand(command, new Date(now + 100), resultCb, {
          executeSync: false,
        });
        const { rows: scheduledEvents } = await client.query<any>(
          "SELECT event->'meta'->>'eventId' as event_id FROM scheduled_events",
        );
        expect(scheduledEvents).to.have.lengthOf(1);
        timer.tick(100);
        timer.restore();
        await sleep(200);
        const { rows: executedCommands } = await client.query<any>(
          "SELECT event_id, status FROM events WHERE type = 'COMMAND'",
        );

        assert.calledOnce(cmdCb);
        assert.calledOnce(resultCb);
        assert.calledWith(cmdCb, cmdPayload);
        assert.calledWith(resultCb, right(command.meta.eventId));
        expect(executedCommands[0].event_id).to.eq(scheduledEvents[0].event_id);
        expect(executedCommands[0].status).to.eq("PROCESSED");

        const { rows } = await client.query<any>(
          "SELECT status FROM scheduled_events WHERE event->'meta'->>'eventId' = $1",
          [scheduledEvents[0].event_id],
        );
        expect(rows[0].status).to.eq("PROCESSED");
      });
    });

    describe("UowDecorator", () => {
      beforeEach(async () => {
        await client.query("CREATE TABLE IF NOT EXISTS test (id int4 UNIQUE, content varchar)");
        await client.query("TRUNCATE TABLE test");
      });

      it("resolves with inserts done concurrently on the same entity", async () => {
        const cmdCb = async ({ id, type }: { id: string; type: string }, scope: ITransactionalScope) => {
          await scope.query(
            "INSERT into test (id, content) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET content = excluded.content",
            [+id, type],
          );
        };
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb, ExampleCommand, false);

        cqrsModule.commandBus.register(new ExampleCommandHandler());

        const results = await Promise.all([
          ...times(100, i => cqrsModule.commandBus.execute(new ExampleCommand({ id: "42", type: `test${i}` }))),
        ]);

        if (results.some(isLeft)) {
          throw new Error("Something went wrong");
        }
        const streamIds = (results as Right<string>[]).map(r => r.right);

        await cqrsModule.waitUntilIdle(logger, 20000);
        const { rows } = await client.query<any>(
          "SELECT COUNT(event_id) as all_ids FROM events WHERE type = 'COMMAND'" +
            ` AND status = 'PROCESSED' AND stream_id IN (${streamIds.map(id => `'${id}'`).join(",")})`,
        );
        expect(+rows[0].all_ids).to.equal(streamIds.length);
      }).timeout(30000);
    });

    describe("core behaviour", () => {
      it("accepts commands", async () => {
        const cmdCb = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
        cqrsModule.commandBus.register(new ExampleCommandHandler());
        const cmdPayload = { id: ID, type: "command" };
        await executeAndWaitForPersistentCommand(client, cqrsModule.commandBus, new ExampleCommand(cmdPayload));
        assert.calledOnce(cmdCb);
        assert.calledWith(cmdCb, cmdPayload);
        const [event] = await db.select("events", { type: "EVENT" }).run(client);
        expect(event!.status).to.equal("CREATED");
        expect((event! as any).event.payload?.id).to.equal(ID);
      });

      it("accepts queries", async () => {
        const cmdCb = stub();
        const ExampleQueryHandler = createExampleQueryHandler(cmdCb);
        cqrsModule.queryBus.register(new ExampleQueryHandler());
        const cmdPayload = { id: ID, type: "command" };

        const result = await cqrsModule.queryBus.execute(new ExampleQuery(cmdPayload));

        expect(isRight(result)).to.equal(true);
        assert.calledOnce(cmdCb);
        assert.calledWith(cmdCb, cmdPayload);
        expect((result as Right<{ id: string; result: number }>).right.result).to.equal(+ID * 2);
      });

      it("accepts events", async () => {
        const result = await cqrsModule.eventBus.execute(new ExampleEvent({ id: ID, type: "event" }));
        expect(isRight(result)).to.equal(true);
        const eventId = (result as Right<string>).right;
        const [event] = await db.select("events", { event_id: eventId }).run(client);
        expect(event?.status).to.equal("CREATED");
        expect(event?.type).to.equal("EVENT");
      });

      it("drains events", async () => {
        const cmdCb1 = stub();
        const cmdCb2 = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb1, ExampleCommand);
        const OtherCommandHandler = createExampleCommandHandler(cmdCb2, OtherCommand);
        cqrsModule.commandBus.register(new ExampleCommandHandler());
        cqrsModule.commandBus.register(new OtherCommandHandler());
        const cmd1 = new ExampleCommand({ id: ID, type: "command1" });
        const cmd2 = new OtherCommand({ id: ID, type: "command2" });
        const events = [
          {
            event_id: cmd1.meta.eventId,
            stream_id: cmd1.meta.streamId ?? uuid(),
            event_name: cmd1.meta.className,
            event: db.param(serializeEvent(cmd1) as {}, true),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
          },
          {
            event_id: cmd2.meta.eventId,
            stream_id: cmd2.meta.streamId ?? uuid(),
            event_name: cmd2.meta.className,
            event: db.param(serializeEvent(cmd2) as {}, true),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
          },
        ];
        await db.insert("events", events as any).run(pool);

        await cqrsModule.commandBus.drain([cmd1.meta.eventId]);
        await cqrsModule.waitUntilSettled([events[1].stream_id]);
        assert.notCalled(cmdCb1);
        assert.calledOnce(cmdCb2);
      });

      it("works with custom sagas", async () => {
        /**
         * scenario:
         * 1. trigger-event is triggered
         * 2. saga is reacting on trigger-event -> successful command ("42")
         * 3. command ("42") is handled and succeeds
         */
        const cmdCb = stub();
        class TriggerEvent extends createEvent<{ id: string; type: string }>() {}

        class ExampleSaga implements ISaga<{ id: string }> {
          public process(events$: Observable<IEvent<{ id: string }, any>>) {
            return events$.pipe(
              mergeMap(ev => {
                if (ev instanceof TriggerEvent) {
                  return of(new ExampleCommand({ id: ev.payload.id, type: "command" }));
                }
                return EMPTY;
              }),
            );
          }
        }
        const CommandHandler = createExampleCommandHandler(cmdCb);
        cqrsModule.commandBus.register(new CommandHandler());
        cqrsModule.eventBus.registerSagas(new ExampleSaga());
        /**
         * note that here we are triggering NOT exampleEvent as it is being triggered upon each command-handler
         * and would create a recursion
         */
        const result = await cqrsModule.eventBus.execute(new TriggerEvent({ id: ID, type: "event" }));

        expect(isRight(result)).to.equal(true);
        const eventId = (result as Right<string>).right;
        const [event] = await db.select("events", { event_id: eventId }).run(client);
        expect(event!.status).to.equal("CREATED");
        expect(event!.type).to.equal("EVENT");
        await assertWithRetries(
          async () => {
            const commands = await db.select("events", { type: "COMMAND" }).run(client);
            expect(commands.length).to.equal(1);
            expect(commands[0].status).to.equal("PROCESSED");
          },
          5,
          100,
        );
      });
    });
  });
});
