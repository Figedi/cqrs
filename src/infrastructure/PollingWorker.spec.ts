import { v4 as uuid } from "uuid"
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { sleep } from "../utils/sleep.js"
import { createPGliteAdapter } from "./db/index.js"
import { runEventsMigration } from "./db/index.js"
import type { IDbAdapter } from "./db/index.js"
import type { KyselyDb } from "./db/index.js"
import { PollingWorker } from "./PollingWorker.js"

describe("PollingWorker", () => {
  let db: KyselyDb
  let dbAdapter: IDbAdapter
  const logger: any = {
    warn: () => {},
    error: () => {},
    debug: () => {},
    info: () => {},
  }
  logger.child = () => logger

  beforeAll(async () => {
    const adapter = await createPGliteAdapter()
    db = adapter.db
    dbAdapter = adapter
    await runEventsMigration(db)
  })

  beforeEach(async () => {
    await db.deleteFrom("events").execute()
  })

  afterEach(async () => {
    // Adapter cleanup if needed
  })

  describe("processor registration and dispatch", () => {
    it("dispatches COMMAND events to COMMAND processor", async () => {
      const commandProcessor = vi.fn().mockResolvedValue(undefined)
      const eventProcessor = vi.fn().mockResolvedValue(undefined)

      const worker = new PollingWorker(logger, {
        pollIntervalMs: 50,
        batchSize: 10,
      })
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", commandProcessor)
      worker.registerProcessor("EVENT", eventProcessor)
      await worker.preflight()

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "TestCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "TestCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "CREATED",
          type: "COMMAND",
          retryCount: 0,
          nextRetryAt: new Date(),
        })
        .execute()

      await sleep(150)

      expect(commandProcessor).toHaveBeenCalledTimes(1)
      expect(commandProcessor).toHaveBeenCalledWith(
        expect.objectContaining({ eventId, type: "COMMAND" }),
        expect.anything(),
      )
      expect(eventProcessor).not.toHaveBeenCalled()

      await worker.shutdown()
    })

    it("dispatches EVENT events to EVENT processor", async () => {
      const commandProcessor = vi.fn().mockResolvedValue(undefined)
      const eventProcessor = vi.fn().mockResolvedValue(undefined)

      const worker = new PollingWorker(logger, {
        pollIntervalMs: 50,
        batchSize: 10,
      })
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", commandProcessor)
      worker.registerProcessor("EVENT", eventProcessor)
      await worker.preflight()

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "TestEvent",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "TestEvent" }, payload: {} }),
          timestamp: new Date(),
          status: "CREATED",
          type: "EVENT",
          retryCount: 0,
          nextRetryAt: new Date(),
        })
        .execute()

      await sleep(150)

      expect(eventProcessor).toHaveBeenCalledTimes(1)
      expect(eventProcessor).toHaveBeenCalledWith(
        expect.objectContaining({ eventId, type: "EVENT" }),
        expect.anything(),
      )
      expect(commandProcessor).not.toHaveBeenCalled()

      await worker.shutdown()
    })

    it("processes both COMMAND and EVENT types in same poll cycle", async () => {
      const commandProcessor = vi.fn().mockResolvedValue(undefined)
      const eventProcessor = vi.fn().mockResolvedValue(undefined)

      const worker = new PollingWorker(logger, {
        pollIntervalMs: 50,
        batchSize: 10,
      });
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", commandProcessor)
      worker.registerProcessor("EVENT", eventProcessor)
      await worker.preflight()

      const cmdId = uuid()
      const evtId = uuid()
      await db
        .insertInto("events")
        .values([
          {
            eventId: cmdId,
            eventName: "TestCommand",
            streamId: `stream-${cmdId}`,
            event: JSON.stringify({ meta: { className: "TestCommand" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
          {
            eventId: evtId,
            eventName: "TestEvent",
            streamId: `stream-${evtId}`,
            event: JSON.stringify({ meta: { className: "TestEvent" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "EVENT",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
        ])
        .execute()

      await sleep(150)

      expect(commandProcessor).toHaveBeenCalledTimes(1)
      expect(commandProcessor).toHaveBeenCalledWith(
        expect.objectContaining({ eventId: cmdId, type: "COMMAND" }),
        expect.anything(),
      )
      expect(eventProcessor).toHaveBeenCalledTimes(1)
      expect(eventProcessor).toHaveBeenCalledWith(
        expect.objectContaining({ eventId: evtId, type: "EVENT" }),
        expect.anything(),
      )

      await worker.shutdown()
    })

    it("allows registerProcessor after worker has started", async () => {
      const commandProcessor = vi.fn().mockResolvedValue(undefined)
      const eventProcessor = vi.fn().mockResolvedValue(undefined)

      const worker = new PollingWorker(logger, {
        pollIntervalMs: 50,
        batchSize: 10,
      });
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", commandProcessor)
      await worker.preflight()

      // Register EVENT processor after start (simulates framework preflight order)
      worker.registerProcessor("EVENT", eventProcessor)

      const cmdId = uuid()
      const evtId = uuid()
      await db
        .insertInto("events")
        .values([
          {
            eventId: cmdId,
            eventName: "TestCommand",
            streamId: `stream-${cmdId}`,
            event: JSON.stringify({ meta: { className: "TestCommand" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
          {
            eventId: evtId,
            eventName: "TestEvent",
            streamId: `stream-${evtId}`,
            event: JSON.stringify({ meta: { className: "TestEvent" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "EVENT",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
        ])
        .execute()

      await sleep(150)

      expect(commandProcessor).toHaveBeenCalledTimes(1)
      expect(eventProcessor).toHaveBeenCalledTimes(1)

      await worker.shutdown()
    })

    it("invokes onCompletion callback when event is processed", async () => {
      const onCompletion = vi.fn()
      const processor = vi.fn().mockResolvedValue(undefined)

      const worker = new PollingWorker(logger, {
        pollIntervalMs: 50,
        batchSize: 10,
      });
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", processor, onCompletion)
      await worker.preflight()

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "TestCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "TestCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "CREATED",
          type: "COMMAND",
          retryCount: 0,
          nextRetryAt: new Date(),
        })
        .execute()

      await sleep(150)

      expect(onCompletion).toHaveBeenCalledTimes(1)
      expect(onCompletion).toHaveBeenCalledWith(
        expect.objectContaining({ eventId }),
        "PROCESSED",
      )

      await worker.shutdown()
    })
  })
})
