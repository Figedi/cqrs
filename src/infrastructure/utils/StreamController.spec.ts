import { expect, describe, it } from "vitest";
import { createStreamController, StreamController, type IStreamEvent } from "./StreamController.js";
import { firstValueFrom, toArray, take, timeout } from "rxjs";
import { CQRSEventType } from "../../types.js";

const createTestEvent = (eventId: string, status: "PROCESSED" | "FAILED" | "ABORTED" = "PROCESSED"): IStreamEvent => ({
  event: {
    eventId,
    eventName: "TestCommand",
    streamId: "stream-123",
    event: { meta: { className: "TestCommand", classType: CQRSEventType.COMMAND }, payload: {} },
    timestamp: new Date(),
    status: "PROCESSED",
    type: "COMMAND",
  },
  status,
});

describe("StreamController", () => {
  describe("createStreamController", () => {
    it("should create a stream controller", () => {
      const controller = createStreamController();

      expect(controller).to.be.instanceOf(StreamController);
      expect(controller).to.have.property("push");
      expect(controller).to.have.property("close");
      expect(controller).to.have.property("getIterator");
      expect(controller).to.have.property("stream$");
    });
  });

  describe("push and getIterator", () => {
    it("should receive pushed items via AsyncIterator", async () => {
      const controller = createStreamController();
      const received: IStreamEvent[] = [];

      // Start consuming in background
      const consumerPromise = (async () => {
        for await (const item of controller.getIterator()) {
          received.push(item);
          if (received.length >= 3) break;
        }
      })();

      // Push some items with small delay to ensure consumer is ready
      await new Promise(resolve => setTimeout(resolve, 10));
      controller.push(createTestEvent("evt-1"));
      controller.push(createTestEvent("evt-2"));
      controller.push(createTestEvent("evt-3"));

      await consumerPromise;

      expect(received).to.have.length(3);
      expect(received[0].event.eventId).to.equal("evt-1");
      expect(received[1].event.eventId).to.equal("evt-2");
      expect(received[2].event.eventId).to.equal("evt-3");
    });

    it("should receive pushed items via RxJS Observable", async () => {
      const controller = createStreamController();

      // Start consuming
      const items$ = controller.stream$().pipe(
        take(3),
        toArray(),
        timeout(1000)
      );
      const itemsPromise = firstValueFrom(items$);

      // Push items
      await new Promise(resolve => setTimeout(resolve, 10));
      controller.push(createTestEvent("evt-1"));
      controller.push(createTestEvent("evt-2"));
      controller.push(createTestEvent("evt-3"));

      const received = await itemsPromise;
      expect(received).to.have.length(3);
      expect(received[0].event.eventId).to.equal("evt-1");
    });
  });

  describe("close", () => {
    it("should stop the AsyncIterator on close", async () => {
      const controller = createStreamController();
      const received: IStreamEvent[] = [];

      const consumerPromise = (async () => {
        for await (const item of controller.getIterator()) {
          received.push(item);
        }
      })();

      await new Promise(resolve => setTimeout(resolve, 10));
      controller.push(createTestEvent("evt-1"));
      controller.push(createTestEvent("evt-2"));
      controller.close();

      await consumerPromise;

      expect(received).to.have.length(2);
    });

    it("should complete the RxJS Observable", async () => {
      const controller = createStreamController();

      const items$ = controller.stream$().pipe(
        toArray(),
        timeout(1000)
      );
      const itemsPromise = firstValueFrom(items$);

      await new Promise(resolve => setTimeout(resolve, 10));
      controller.push(createTestEvent("evt-1"));
      controller.push(createTestEvent("evt-2"));
      controller.close();

      const received = await itemsPromise;
      expect(received).to.have.length(2);
    });

    it("should report isClosed correctly", () => {
      const controller = createStreamController();

      expect(controller.isClosed()).to.equal(false);
      controller.close();
      expect(controller.isClosed()).to.equal(true);
    });
  });

  describe("abort", () => {
    it("should stop accepting new items after abort", () => {
      const controller = createStreamController();

      controller.abort(new Error("Test error"));

      // Should not throw, but also should not add to queue
      controller.push(createTestEvent("evt-1"));
      expect(controller.isClosed()).to.equal(true);
    });
  });

  describe("multiple consumers", () => {
    it("should distribute items across multiple AsyncIterator consumers (work-queue pattern)", async () => {
      const controller = createStreamController();
      const received1: IStreamEvent[] = [];
      const received2: IStreamEvent[] = [];

      // Start two consumers - each event goes to exactly ONE consumer
      const consumer1 = (async () => {
        for await (const item of controller.getIterator()) {
          received1.push(item);
          if (received1.length >= 2) break;
        }
      })();

      const consumer2 = (async () => {
        for await (const item of controller.getIterator()) {
          received2.push(item);
          if (received2.length >= 2) break;
        }
      })();

      await new Promise(resolve => setTimeout(resolve, 10));
      // Push 4 events - each consumer should receive 2 (work-queue distribution)
      controller.push(createTestEvent("evt-1"));
      controller.push(createTestEvent("evt-2"));
      controller.push(createTestEvent("evt-3"));
      controller.push(createTestEvent("evt-4"));

      await Promise.all([consumer1, consumer2]);

      // Each consumer receives 2 events, totaling 4 events distributed across both
      expect(received1).to.have.length(2);
      expect(received2).to.have.length(2);

      // All 4 events should be distributed (no duplicates, no missing)
      const allReceived = [...received1, ...received2];
      const allEventIds = allReceived.map(e => e.event.eventId).sort();
      expect(allEventIds).to.deep.equal(["evt-1", "evt-2", "evt-3", "evt-4"]);
    });
  });

  describe("IStreamEvent structure", () => {
    it("should correctly carry event status", async () => {
      const controller = createStreamController();

      const consumerPromise = (async () => {
        const items: IStreamEvent[] = [];
        for await (const item of controller.getIterator()) {
          items.push(item);
          if (items.length >= 3) break;
        }
        return items;
      })();

      await new Promise(resolve => setTimeout(resolve, 10));
      controller.push(createTestEvent("evt-1", "PROCESSED"));
      controller.push(createTestEvent("evt-2", "FAILED"));
      controller.push(createTestEvent("evt-3", "ABORTED"));

      const received = await consumerPromise;

      expect(received[0].status).to.equal("PROCESSED");
      expect(received[1].status).to.equal("FAILED");
      expect(received[2].status).to.equal("ABORTED");
    });

    it("should include error information for failed events", async () => {
      const controller = createStreamController();

      const consumerPromise = (async () => {
        for await (const item of controller.getIterator()) {
          return item;
        }
      })();

      await new Promise(resolve => setTimeout(resolve, 10));
      const failedEvent: IStreamEvent = {
        ...createTestEvent("evt-1", "FAILED"),
        error: new Error("Processing failed"),
      };
      controller.push(failedEvent);

      const received = await consumerPromise;

      expect(received!.status).to.equal("FAILED");
      expect(received!.error).to.be.instanceOf(Error);
      expect(received!.error!.message).to.equal("Processing failed");
    });
  });
});
