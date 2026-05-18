import { describe, expect, it } from "bun:test";
import {
  evolveState,
  historyDocKey,
  stateDocKey,
  toHistoryDoc,
} from "../../src/couchbase/projection.ts";
import { normalizeElasticAutoOps } from "../../src/normalize.ts";
import type { ElasticAutoOpsWebhook } from "../../src/types.ts";

const ctx = {
  tenant: "elastic-cloud",
  environment: "prod",
  now: () => new Date("2026-05-18T19:28:00.000Z"),
};

function makeEvent(overrides: Partial<ElasticAutoOpsWebhook> = {}) {
  const raw: ElasticAutoOpsWebhook = {
    resourceId: "r-123",
    resourceName: "search-prod-eu",
    title: "JVM memory pressure high",
    severity: "High",
    status: "open",
    startTime: "2026-05-18T19:27:40Z",
    endTime: null,
    ...overrides,
  };
  return normalizeElasticAutoOps(raw, ctx);
}

describe("historyDocKey / stateDocKey", () => {
  it("formats keys as expected", () => {
    const event = makeEvent();
    expect(historyDocKey(event)).toBe(
      `autoops::event::r-123::2026-05-18T19:27:40Z::${event.idempotencyKey}`,
    );
    expect(stateDocKey(event)).toBe(
      "autoops::state::r-123::r-123-jvm-memory-pressure-high",
    );
  });

  it("uses the same stateDocKey across open and close", () => {
    const opened = makeEvent({ status: "open" });
    const closed = makeEvent({ status: "close", endTime: "2026-05-18T20:00:00Z" });
    expect(stateDocKey(opened)).toBe(stateDocKey(closed));
  });
});

describe("toHistoryDoc", () => {
  it("copies normalized fields onto the history shape", () => {
    const event = makeEvent();
    const doc = toHistoryDoc(event);
    expect(doc.docType).toBe("autoopsEvent");
    expect(doc.resourceId).toBe("r-123");
    expect(doc.severity).toBe("high");
    expect(doc.eventType).toBe("opened");
    expect(doc.alertSignature).toBe("r-123-jvm-memory-pressure-high");
    expect(doc.payload).toEqual(event.alert);
    expect(doc.raw).toEqual(event.raw);
  });
});

describe("evolveState", () => {
  it("creates a fresh state from an open event", () => {
    const event = makeEvent({ status: "open" });
    const state = evolveState(null, event);
    expect(state).toMatchObject({
      docType: "autoopsState",
      resourceId: "r-123",
      alertSignature: "r-123-jvm-memory-pressure-high",
      currentStatus: "opened",
      currentSeverity: "high",
      currentSeverityRank: 3,
      firstSeenAt: "2026-05-18T19:27:40Z",
      lastSeenAt: "2026-05-18T19:27:40Z",
      openCount: 1,
      closeCount: 0,
      isOpen: true,
    });
    expect(state.lastEventId).toBe(event.idempotencyKey);
  });

  it("creates a fresh state from a close-only event with closeCount=1", () => {
    const event = makeEvent({ status: "close", endTime: "2026-05-18T20:00:00Z" });
    const state = evolveState(null, event);
    expect(state.openCount).toBe(0);
    expect(state.closeCount).toBe(1);
    expect(state.isOpen).toBe(false);
    expect(state.currentStatus).toBe("closed");
  });

  it("increments openCount on a second open without changing firstSeenAt", () => {
    const first = makeEvent({ status: "open" });
    const second = makeEvent({
      status: "open",
      startTime: "2026-05-18T19:45:00Z",
    });
    const after = evolveState(evolveState(null, first), second);
    expect(after.firstSeenAt).toBe(first.occurredAt);
    expect(after.lastSeenAt).toBe(second.occurredAt);
    expect(after.openCount).toBe(2);
    expect(after.closeCount).toBe(0);
    expect(after.isOpen).toBe(true);
    expect(after.lastEventId).toBe(second.idempotencyKey);
  });

  it("flips isOpen=false and increments closeCount on a close after an open", () => {
    const opened = makeEvent({ status: "open" });
    const closed = makeEvent({
      status: "close",
      endTime: "2026-05-18T20:00:00Z",
    });
    const after = evolveState(evolveState(null, opened), closed);
    expect(after.openCount).toBe(1);
    expect(after.closeCount).toBe(1);
    expect(after.isOpen).toBe(false);
    expect(after.currentStatus).toBe("closed");
    expect(after.firstSeenAt).toBe(opened.occurredAt);
  });

  it("updates resourceName if it changes", () => {
    const opened = makeEvent({ status: "open" });
    const renamed = makeEvent({
      status: "close",
      endTime: "2026-05-18T20:00:00Z",
      resourceName: "search-prod-eu-renamed",
    });
    const after = evolveState(evolveState(null, opened), renamed);
    expect(after.resourceName).toBe("search-prod-eu-renamed");
  });
});
