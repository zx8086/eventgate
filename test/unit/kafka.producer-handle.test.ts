// test/unit/kafka.producer-handle.test.ts
import { describe, expect, it } from "bun:test";
import { createProducerHandleFromInner } from "../../src/kafka/producerHandle.ts";
import { CircuitBreakerOpenError } from "../../src/resilience/errors.ts";

function makeInner(behaviour: "ok" | "always-fail") {
  const sent: Array<{ topic: string; key: string; value: string }> = [];
  let connected = true;
  return {
    sent,
    inner: {
      isConnected: () => connected,
      disconnect: async () => {
        connected = false;
      },
      async sendByTopic(topic: string, key: string, value: string) {
        if (behaviour === "always-fail") {
          throw new Error("metadata failed 4 times.");
        }
        sent.push({ topic, key, value });
      },
    },
  };
}

const cfg = { failureThreshold: 3, successThreshold: 2, recoveryTimeoutMs: 200 };

describe("ProducerHandle", () => {
  it("delegates sendByTopic to the inner producer while breaker is closed", async () => {
    const { sent, inner } = makeInner("ok");
    const handle = createProducerHandleFromInner(inner, cfg);
    await handle.sendByTopic("T", "k", "v", null);
    await handle.sendByTopic("T", "k2", "v2", null);
    expect(sent.map((s) => s.key)).toEqual(["k", "k2"]);
    expect(handle.getBreakerSnapshot().state).toBe("closed");
  });

  it("opens the breaker after failureThreshold consecutive failures", async () => {
    const { inner } = makeInner("always-fail");
    const handle = createProducerHandleFromInner(inner, cfg);
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    expect(handle.getBreakerSnapshot().state).toBe("open");
  });

  it("fails fast with CircuitBreakerOpenError when the breaker is open", async () => {
    const { inner } = makeInner("always-fail");
    const handle = createProducerHandleFromInner(inner, cfg);
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    await expect(handle.sendByTopic("T", "x", "v", null)).rejects.toBeInstanceOf(CircuitBreakerOpenError);
  });

  it("does NOT call the inner producer once the breaker is open", async () => {
    let innerCalls = 0;
    const inner = {
      isConnected: () => true,
      disconnect: async () => {},
      sendByTopic: async () => {
        innerCalls += 1;
        throw new Error("metadata failed 4 times.");
      },
    };
    const handle = createProducerHandleFromInner(inner, cfg);
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    const callsAfterOpen = innerCalls;
    await handle.sendByTopic("T", "x", "v", null).catch(() => undefined);
    expect(innerCalls).toBe(callsAfterOpen);
  });

  it("isConnected delegates to inner", async () => {
    const { inner } = makeInner("ok");
    const handle = createProducerHandleFromInner(inner, cfg);
    expect(handle.isConnected()).toBe(true);
    await handle.disconnect();
    expect(handle.isConnected()).toBe(false);
  });

  it("disconnect delegates to inner.disconnect", async () => {
    const { inner } = makeInner("ok");
    const handle = createProducerHandleFromInner(inner, cfg);
    await handle.disconnect();
    expect(inner.isConnected()).toBe(false);
  });

  it("getBreakerSnapshot returns the live FSM state", async () => {
    const { inner } = makeInner("always-fail");
    const handle = createProducerHandleFromInner(inner, cfg);
    expect(handle.getBreakerSnapshot()).toEqual({
      state: "closed",
      failures: 0,
      nextAttemptAt: null,
    });
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    const snap = handle.getBreakerSnapshot();
    expect(snap.state).toBe("open");
    expect(snap.failures).toBe(3);
    expect(snap.nextAttemptAt).not.toBeNull();
  });

  it("invokes the onBreakerOpen callback exactly once per transition", async () => {
    const { inner } = makeInner("always-fail");
    let opens = 0;
    const handle = createProducerHandleFromInner(inner, cfg, () => {
      opens += 1;
    });
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    expect(opens).toBe(1);
  });

  it("emits an info log on the closed -> open transition", async () => {
    const { inner } = makeInner("always-fail");
    const logs: Array<{ obj: object; msg: string }> = [];
    const fakeLogger = {
      info: (obj: unknown, msg?: string) => logs.push({ obj: obj as object, msg: (msg ?? "") as string }),
      warn: () => {},
      error: () => {},
      debug: () => {},
      trace: () => {},
      fatal: () => {},
      child: () => fakeLogger,
      flush: () => {},
    };
    const handle = createProducerHandleFromInner(inner, cfg, undefined, fakeLogger);
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    const opened = logs.filter((l) => l.msg === "circuit breaker opened");
    expect(opened.length).toBe(1);
    expect((opened[0]!.obj as { event_name: string }).event_name).toBe("circuit_breaker_opened");
    expect((opened[0]!.obj as { breaker: string }).breaker).toBe("kafka-producer");
  });
});
