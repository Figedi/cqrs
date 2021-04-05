import { createLogger, sleep } from "@figedi/svc";
import { expect } from "chai";
import { Either, Right, isRight, left, right } from "fp-ts/lib/Either";
import { Option, none } from "fp-ts/lib/Option";
import { isLeft } from "fp-ts/lib/These";
import { times } from "lodash";
import { EMPTY, Observable, of } from "rxjs";
import { mergeMap } from "rxjs/operators";
import Sinon, { assert, stub, useFakeTimers } from "sinon";
import { getConnection, getManager } from "typeorm";

import { createCommand, createCommandHandler, createEvent, createQuery, createQueryHandler } from "./common";
import { ICQRSSettings, CQRSModule } from "./cqrsModule";
import { RetriesExceededError, ConfigError } from "./errors";
import { EventEntity } from "./infrastructure/EventEntity";
import {
  Constructor,
  HandlerContext,
  ICommand,
  ICommandBus,
  IEvent,
  IQuery,
  ISaga,
  TransactionalScope,
  VoidEither,
} from "./types";

const baseOrmConfig = require("../ormconfig.js");

class ExampleCommand extends createCommand<{ id: string; type: string }>() {}
class ExampleEvent extends createEvent<{ id: string; type: string }>() {}
class ExampleQuery extends createQuery<{ id: string; type: string }, ExampleQueryResult>() {}

type ExampleCommandResult = Either<Error, Option<never>>;

const createExampleCommandHandler = <
  TCommand extends ICommand<TPayload>,
  TPayload extends { id: string; type: string }
>(
  cb: (payload: TPayload, scope: TransactionalScope) => void | Promise<void>,
  Command: Constructor<TCommand> = (ExampleCommand as unknown) as Constructor<TCommand>,
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
type ExampleQueryResult = Either<Error, { id: string; result: number }>;

const createExampleQueryHandler = <TQuery extends IQuery<TPayload>, TPayload extends { id: string; type: string }>(
  cb: (payload: TPayload) => void,
  Query: Constructor<TQuery> = (ExampleQuery as unknown) as Constructor<TQuery>,
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
    const event = await getManager().getRepository(EventEntity).findOne({ eventId: respId });
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
  const logger = createLogger({ level: "debug", base: { service: "test", env: "test" }, prettyPrint: false });
  // const logger = createStubbedLogger();
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
    const connectionUrl = `postgres://${process.env.PG_USER_NAME}:${process.env.PG_PASSWORD}@localhost/${process.env.PG_DB}`;
    if (!connectionUrl.includes("localhost")) {
      throw new ConfigError(
        "Please define DB_CONNECTION_URL with LOCALHOST to run integration tests for exampleCommand",
      );
    }
    const CQRS_OPTIONS: ICQRSSettings = {
      transaction: txSettings,

      persistence: {
        type: "pg",
        runMigrations: true,
        options: {
          ...baseOrmConfig,
          url: connectionUrl,
        },
      },
    };
    let cqrsModule: CQRSModule;

    before(async () => {
      cqrsModule = new CQRSModule(CQRS_OPTIONS as ICQRSSettings, logger);
      await cqrsModule.preflight();
    });

    beforeEach(async () => {
      await getConnection().query("TRUNCATE TABLE events RESTART IDENTITY CASCADE");
    });

    describe("EventScheduler", () => {
      let timer: Sinon.SinonFakeTimers;
      const now = Date.now();
      beforeEach(async () => {
        timer = useFakeTimers(now);

        await getConnection().query("TRUNCATE TABLE scheduled_events RESTART IDENTITY CASCADE");
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
        const scheduledEvents = await getConnection().query(
          "SELECT event->'meta'->>'eventId' as event_id FROM scheduled_events",
        );
        expect(scheduledEvents).to.have.lengthOf(1);
        timer.tick(100);
        timer.restore();
        await sleep(200);
        const executedCommands = await getConnection().query(
          "SELECT event_id, status FROM events WHERE type = 'COMMAND'",
        );
        assert.calledOnce(cmdCb);
        assert.calledOnce(resultCb);
        assert.calledWith(cmdCb, cmdPayload);
        assert.calledWith(resultCb, right(none));
        expect(executedCommands[0].event_id).to.eq(scheduledEvents[0].event_id);
        expect(executedCommands[0].status).to.eq("PROCESSED");

        const result = await getConnection().query(
          "SELECT status FROM scheduled_events WHERE event->'meta'->>'eventId' = $1",
          [scheduledEvents[0].event_id],
        );
        expect(result[0].status).to.eq("PROCESSED");
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
        const scheduledEvents = await getConnection().query(
          "SELECT event->'meta'->>'eventId' as event_id FROM scheduled_events",
        );
        expect(scheduledEvents).to.have.lengthOf(1);
        timer.tick(100);
        timer.restore();
        await sleep(200);
        const executedCommands = await getConnection().query(
          "SELECT event_id, status FROM events WHERE type = 'COMMAND'",
        );

        assert.calledOnce(cmdCb);
        assert.calledOnce(resultCb);
        assert.calledWith(cmdCb, cmdPayload);
        assert.calledWith(resultCb, right(command.meta.eventId));
        expect(executedCommands[0].event_id).to.eq(scheduledEvents[0].event_id);
        expect(executedCommands[0].status).to.eq("PROCESSED");

        const result = await getConnection().query(
          "SELECT status FROM scheduled_events WHERE event->'meta'->>'eventId' = $1",
          [scheduledEvents[0].event_id],
        );
        expect(result[0].status).to.eq("PROCESSED");
      });
    });

    describe("UowDecorator", () => {
      before(async () => {
        await getConnection().query("CREATE TABLE IF NOT EXISTS test (id int4 UNIQUE, content varchar)");
      });

      beforeEach(async () => {
        await getConnection().query("TRUNCATE TABLE test");
      });

      it("resolves with inserts done concurrently on the same entity", async () => {
        const cmdCb = async ({ id, type }: { id: string; type: string }, scope: TransactionalScope) => {
          return scope.query(
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

        const [{ all_ids: allIds }] = await getConnection().query(
          "SELECT COUNT(event_id) as all_ids FROM events WHERE type = 'COMMAND'" +
            ` AND status = 'PROCESSED' AND stream_id IN (${streamIds.map(id => `'${id}'`).join(",")})`,
        );
        expect(+allIds).to.equal(streamIds.length);
      }).timeout(30000);
    });

    describe("core behaviour", () => {
      it("should accept commands", async () => {
        const cmdCb = stub();
        const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
        cqrsModule.commandBus.register(new ExampleCommandHandler());
        const cmdPayload = { id: ID, type: "command" };
        await executeAndWaitForPersistentCommand(cqrsModule.commandBus, new ExampleCommand(cmdPayload));
        assert.calledOnce(cmdCb);
        assert.calledWith(cmdCb, cmdPayload);
        const event = await getManager().getRepository(EventEntity).findOne({ type: "EVENT" });
        expect(event!.status).to.equal("CREATED");
        expect(event!.event.payload.id).to.equal(ID);
      });

      it("should accept queries", async () => {
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

      it("should accept events", async () => {
        const result = await cqrsModule.eventBus.execute(new ExampleEvent({ id: ID, type: "event" }));

        expect(isRight(result)).to.equal(true);
        const eventId = (result as Right<string>).right;
        const event = await getManager().getRepository(EventEntity).findOne({ eventId });
        expect(event!.status).to.equal("CREATED");
        expect(event!.type).to.equal("EVENT");
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
        const event = await getManager().getRepository(EventEntity).findOne({ eventId });
        expect(event!.status).to.equal("CREATED");
        expect(event!.type).to.equal("EVENT");
        await assertWithRetries(
          async () => {
            const commands = await getManager().getRepository(EventEntity).find({ type: "COMMAND" });
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
