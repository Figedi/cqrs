import { createLogger, sleep } from "@figedi/svc";
import { expect } from "chai";
import { Either, Right, isRight, left, right } from "fp-ts/lib/Either";
import { Option, none } from "fp-ts/lib/Option";
import { isLeft } from "fp-ts/lib/These";
import { times } from "lodash";
import { Observable } from "rxjs";
import { map } from "rxjs/operators";
import { assert, stub } from "sinon";
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
  ISaga,
  TransactionalScope,
  VoidEither,
} from "./types";

const baseOrmConfig = require("../ormconfig.js");

class ExampleCommand extends createCommand<{ id: string; type: string }>() {}
class AnotherCommand extends createCommand<{ id: string; type: string }>() {}
class ExampleEvent extends createEvent<{ id: string; type: string }>() {}
class ExampleQuery extends createQuery<{ id: string; type: string }, ExampleQueryResult>() {}

type ExampleCommandResult = Either<Error, Option<never>>;

const createExampleSaga = (
  returnCommand: ICommand = new ExampleCommand({
    id: "not-42",
    type: "command",
  }),
) =>
  class ExampleSaga implements ISaga<{ id: string }> {
    public process(events$: Observable<IEvent<{ id: string }, any>>) {
      return events$.pipe(map(_ => returnCommand));
    }
  };

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

const createExampleQueryHandler = (cb: (id: string) => void, Query = ExampleQuery) =>
  class ExampleQueryHandler extends createQueryHandler<any>(Query) {
    public async handle({ payload: { id } }: ExampleCommand): Promise<ExampleQueryResult> {
      if (id !== "42") {
        return left(new Error(`42 is the truth to everything. Your id is ${id}`));
      }
      cb(id);
      return right({ id, result: +id * 2 });
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
    let cqrsModule: CQRSModule;

    beforeEach(async () => {
      cqrsModule = new CQRSModule({ transaction: txSettings, persistence: { type: "inmem" } }, logger);
    });

    it("should accept commands", async () => {
      const cmdCb = stub();
      const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
      cqrsModule.commandBus.register(new ExampleCommandHandler());
      await executeAndWaitForInmemoryCommand(cqrsModule.commandBus, new ExampleCommand({ id: ID, type: "command" }));
      assert.calledOnce(cmdCb);
      assert.calledWithExactly(cmdCb, ID);
    });

    it("should accept queries", async () => {
      const cmdCb = stub();
      const ExampleQueryHandler = createExampleQueryHandler(cmdCb);
      cqrsModule.queryBus.register(new ExampleQueryHandler());

      const result = await cqrsModule.queryBus.execute(new ExampleQuery({ id: ID, type: "command" }));

      expect(isRight(result)).to.equal(true);
      assert.calledOnce(cmdCb);
      assert.calledWithExactly(cmdCb, ID);
      expect((result as Right<{ id: string; result: number }>).right.result).to.equal(+ID * 2);
    });

    it("should accept events", async () => {
      const result = await cqrsModule.eventBus.execute(new ExampleEvent({ id: ID, type: "event" }));

      expect(isRight(result)).to.equal(true);
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
    const cqrsModule = new CQRSModule(CQRS_OPTIONS as ICQRSSettings, logger);

    before(async () => {
      await cqrsModule.preflight();
    });

    beforeEach(async () => {
      await getConnection().query("TRUNCATE TABLE events");
    });

    describe.only("UowDecorator", () => {
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
        console.log("streamIds", streamIds);
        await sleep(2000000);
      }).timeout(5000000);
    });

    it("should accept commands", async () => {
      const cmdCb = stub();
      const ExampleCommandHandler = createExampleCommandHandler(cmdCb);
      cqrsModule.commandBus.register(new ExampleCommandHandler());
      await executeAndWaitForPersistentCommand(cqrsModule.commandBus, new ExampleCommand({ id: ID, type: "command" }));
      assert.calledOnce(cmdCb);
      assert.calledWithExactly(cmdCb, ID);
      const event = await getManager().getRepository(EventEntity).findOne({ type: "EVENT" });
      expect(event!.status).to.equal("CREATED");
      expect(event!.event.payload.id).to.equal(ID);
    });

    it("should accept queries", async () => {
      const cmdCb = stub();
      const ExampleQueryHandler = createExampleQueryHandler(cmdCb);
      cqrsModule.queryBus.register(new ExampleQueryHandler());

      const result = await cqrsModule.queryBus.execute(new ExampleQuery({ id: ID, type: "command" }));

      expect(isRight(result)).to.equal(true);
      assert.calledOnce(cmdCb);
      assert.calledWithExactly(cmdCb, ID);
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
       * 1. example-event is triggered
       * 2. saga 1 is reacting on example-event -> failing command ("not-42")
       * 3. saga 2 is reacting on example-event -> successful command ("42")
       * 4. command ("not-42") is handled and fails
       * 5. command ("42") is handled and succeeds
       */
      const FailingSaga = createExampleSaga();
      const AnotherSaga = createExampleSaga(new AnotherCommand({ id: "42", type: "another-command" }));
      const CommandHandler = createExampleCommandHandler(() => {}, ExampleCommand, false);
      const AnotherCommandHandler = createExampleCommandHandler(() => {}, AnotherCommand, false);
      cqrsModule.commandBus.register(new CommandHandler(), new AnotherCommandHandler());
      cqrsModule.eventBus.registerSagas(new FailingSaga(), new AnotherSaga());

      const result = await cqrsModule.eventBus.execute(new ExampleEvent({ id: ID, type: "event" }));

      expect(isRight(result)).to.equal(true);
      const eventId = (result as Right<string>).right;
      const event = await getManager().getRepository(EventEntity).findOne({ eventId });
      expect(event!.status).to.equal("CREATED");
      expect(event!.type).to.equal("EVENT");
      await assertWithRetries(
        async () => {
          const commands = await getManager().getRepository(EventEntity).find({ type: "COMMAND" });
          expect(commands.length).to.equal(2);
          expect(commands.map(command => command.status).sort()).to.deep.equal(["FAILED", "PROCESSED"]);
        },
        5,
        100,
      );
    });
  });
});
