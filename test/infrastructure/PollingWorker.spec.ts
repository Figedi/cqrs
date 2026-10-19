import { v4 as uuid } from "uuid"
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { sleep } from "../../src/utils/sleep.js"
import { createPGliteAdapter } from "../pgliteAdapter.js"
import { runEventsMigration } from "../../src/infrastructure/db/index.js"
import type { IDbAdapter } from "../../src/infrastructure/db/index.js"
import type { KyselyDb } from "../../src/infrastructure/db/index.js"
import { PollingWorker } from "../../src/infrastructure/PollingWorker.js"

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

  describe("lock ownership", () => {
    it("does not let another worker release a lock that is still being refreshed", async () => {
      let release: () => void = () => {}
      const ownerProcessor = vi.fn(
        () =>
          new Promise<void>(resolve => {
            release = resolve
          }),
      )
      const otherProcessor = vi.fn().mockResolvedValue(undefined)

      const owner = new PollingWorker(logger, {
        workerId: "owner",
        pollIntervalMs: 30,
        lockTimeoutMs: 120,
        staleLockIntervalMs: 10_000,
        batchSize: 10,
      })
      owner.setAdapter(dbAdapter)
      owner.registerProcessor("COMMAND", ownerProcessor)
      await owner.preflight()

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "SlowCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "SlowCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "CREATED",
          type: "COMMAND",
          retryCount: 0,
          nextRetryAt: new Date(),
        })
        .execute()

      await vi.waitFor(() => expect(ownerProcessor).toHaveBeenCalledTimes(1))

      const other = new PollingWorker(logger, {
        workerId: "other",
        pollIntervalMs: 30,
        lockTimeoutMs: 120,
        staleLockIntervalMs: 40,
        batchSize: 10,
      })
      other.setAdapter(dbAdapter)
      other.registerProcessor("COMMAND", otherProcessor)
      await other.preflight()

      await sleep(300)

      const during = await db.selectFrom("events").selectAll().where("eventId", "=", eventId).executeTakeFirstOrThrow()
      expect(during.status).toBe("PROCESSING")
      expect(during.lockedBy).toBe("owner")
      expect(otherProcessor).not.toHaveBeenCalled()

      release()
      await vi.waitFor(async () => {
        const row = await db.selectFrom("events").select("status").where("eventId", "=", eventId).executeTakeFirst()
        expect(row?.status).toBe("PROCESSED")
      })
      expect(otherProcessor).not.toHaveBeenCalled()

      await owner.shutdown()
      await other.shutdown()
    })

    it("releases a lock whose owner stopped refreshing it", async () => {
      const processor = vi.fn().mockResolvedValue(undefined)
      const worker = new PollingWorker(logger, {
        workerId: "recovery",
        pollIntervalMs: 30,
        lockTimeoutMs: 80,
        staleLockIntervalMs: 30,
        batchSize: 10,
      })
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", processor)

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "AbandonedCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "AbandonedCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "PROCESSING",
          type: "COMMAND",
          retryCount: 0,
          nextRetryAt: new Date(),
          lockedAt: new Date(Date.now() - 60_000),
          lockedBy: "dead-worker",
        })
        .execute()

      await worker.preflight()
      await vi.waitFor(() => expect(processor).toHaveBeenCalledTimes(1))

      const row = await db.selectFrom("events").select(["status", "lockedBy"]).where("eventId", "=", eventId).executeTakeFirst()
      expect(row?.status).toBe("PROCESSED")
      expect(row?.lockedBy).toBeNull()

      await worker.shutdown()
    })

    it("keeps a later claimed row while the first handler is still running", async () => {
      let releaseFirst: () => void = () => {}
      let calls = 0
      const ownerProcessor = vi.fn(
        () =>
          new Promise<void>(resolve => {
            calls += 1
            if (calls === 1) {
              releaseFirst = resolve
              return
            }
            resolve()
          }),
      )
      const otherProcessor = vi.fn().mockResolvedValue(undefined)

      const owner = new PollingWorker(logger, {
        workerId: "owner",
        pollIntervalMs: 30,
        lockTimeoutMs: 120,
        staleLockIntervalMs: 10_000,
        batchSize: 10,
      })
      owner.setAdapter(dbAdapter)
      owner.registerProcessor("COMMAND", ownerProcessor)
      await owner.preflight()

      const firstId = uuid()
      const secondId = uuid()
      await db
        .insertInto("events")
        .values([
          {
            eventId: firstId,
            eventName: "FirstCommand",
            streamId: `stream-${firstId}`,
            event: JSON.stringify({ meta: { className: "FirstCommand" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
          {
            eventId: secondId,
            eventName: "SecondCommand",
            streamId: `stream-${secondId}`,
            event: JSON.stringify({ meta: { className: "SecondCommand" }, payload: {} }),
            timestamp: new Date(),
            status: "CREATED",
            type: "COMMAND",
            retryCount: 0,
            nextRetryAt: new Date(),
          },
        ])
        .execute()

      await vi.waitFor(() => expect(ownerProcessor).toHaveBeenCalledTimes(1))

      const other = new PollingWorker(logger, {
        workerId: "other",
        pollIntervalMs: 30,
        lockTimeoutMs: 120,
        staleLockIntervalMs: 40,
        batchSize: 10,
      })
      other.setAdapter(dbAdapter)
      other.registerProcessor("COMMAND", otherProcessor)
      await other.preflight()

      await sleep(300)

      const queued = await db.selectFrom("events").selectAll().where("eventId", "=", secondId).executeTakeFirstOrThrow()
      expect(queued.status).toBe("PROCESSING")
      expect(queued.lockedBy).toBe("owner")
      expect(otherProcessor).not.toHaveBeenCalled()
      expect(ownerProcessor).toHaveBeenCalledTimes(1)

      releaseFirst()
      await vi.waitFor(() => expect(ownerProcessor).toHaveBeenCalledTimes(2))
      await vi.waitFor(async () => {
        const row = await db.selectFrom("events").select("status").where("eventId", "=", secondId).executeTakeFirst()
        expect(row?.status).toBe("PROCESSED")
      })
      expect(otherProcessor).not.toHaveBeenCalled()

      await owner.shutdown()
      await other.shutdown()
    })

    it("does not record failure after the lock was taken by another worker", async () => {
      let rejectHandler: (error: Error) => void = () => {}
      const onCompletion = vi.fn()
      const processor = vi.fn(
        () =>
          new Promise<void>((_resolve, reject) => {
            rejectHandler = reject
          }),
      )

      const worker = new PollingWorker(logger, {
        workerId: "owner",
        pollIntervalMs: 30,
        lockTimeoutMs: 10_000,
        staleLockIntervalMs: 10_000,
        batchSize: 10,
      })
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", processor, onCompletion)
      await worker.preflight()

      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "LostLockCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "LostLockCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "CREATED",
          type: "COMMAND",
          retryCount: 0,
          nextRetryAt: new Date(),
        })
        .execute()

      await vi.waitFor(() => expect(processor).toHaveBeenCalledTimes(1))
      await db
        .updateTable("events")
        .set({ lockedBy: "thief", status: "PROCESSING" })
        .where("eventId", "=", eventId)
        .execute()

      rejectHandler(new Error("handler failed after losing the lock"))
      await sleep(100)

      const row = await db.selectFrom("events").select(["status", "lockedBy"]).where("eventId", "=", eventId).executeTakeFirst()
      expect(row?.status).toBe("PROCESSING")
      expect(row?.lockedBy).toBe("thief")
      expect(onCompletion).not.toHaveBeenCalled()

      await worker.shutdown()
    })

    it("leaves a failed event alone until nextRetryAt", async () => {
      const processor = vi.fn().mockResolvedValue(undefined)
      const eventId = uuid()
      await db
        .insertInto("events")
        .values({
          eventId,
          eventName: "LaterCommand",
          streamId: `stream-${eventId}`,
          event: JSON.stringify({ meta: { className: "LaterCommand" }, payload: {} }),
          timestamp: new Date(),
          status: "FAILED",
          type: "COMMAND",
          retryCount: 1,
          nextRetryAt: new Date(Date.now() + 60_000),
        })
        .execute()

      const worker = new PollingWorker(logger, {
        workerId: "retry-wait",
        pollIntervalMs: 30,
        lockTimeoutMs: 10_000,
        staleLockIntervalMs: 10_000,
        batchSize: 10,
      })
      worker.setAdapter(dbAdapter)
      worker.registerProcessor("COMMAND", processor)
      await worker.preflight()
      await sleep(150)

      expect(processor).not.toHaveBeenCalled()
      const row = await db.selectFrom("events").select("status").where("eventId", "=", eventId).executeTakeFirst()
      expect(row?.status).toBe("FAILED")

      await worker.shutdown()
    })
  })
})
