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
    const openedObj = opened[0]!.obj as {
      event_name: string;
      breaker: string;
      from: string;
      failures: number;
      next_attempt_at?: string;
      last_error?: string;
    };
    expect(openedObj.event_name).toBe("circuit_breaker_opened");
    expect(openedObj.breaker).toBe("kafka-producer");
    expect(openedObj.from).toBe("closed");
    expect(openedObj.failures).toBe(3);
    expect(typeof openedObj.next_attempt_at).toBe("string");
    expect(openedObj.last_error).toBe("metadata failed 4 times.");
  });

  it("logs half-open and closed transitions through the full recovery cycle", async () => {
    // Inner producer that fails 3 times then succeeds forever — drives the
    // full closed -> open -> half-open -> closed cycle.
    let calls = 0;
    const inner = {
      isConnected: () => true,
      disconnect: async () => {},
      sendByTopic: async () => {
        calls += 1;
        if (calls <= 3) throw new Error("metadata failed 4 times.");
      },
    };
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

    // Trip the breaker.
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    expect(handle.getBreakerSnapshot().state).toBe("open");

    // Wait past recoveryTimeoutMs (200ms) so the next call probes.
    await Bun.sleep(250);

    // First success after timeout: opens the half-open probe and succeeds —
    // state becomes "half-open" (successThreshold is 2, so not yet closed).
    await handle.sendByTopic("T", "probe-1", "v", null);
    expect(handle.getBreakerSnapshot().state).toBe("half-open");

    // Second success: reaches successThreshold and closes.
    await handle.sendByTopic("T", "probe-2", "v", null);
    expect(handle.getBreakerSnapshot().state).toBe("closed");

    const halfOpenLogs = logs.filter((l) => l.msg === "circuit breaker half-open");
    expect(halfOpenLogs.length).toBe(1);
    expect((halfOpenLogs[0]!.obj as { event_name: string; from: string }).event_name).toBe("circuit_breaker_half_open");
    expect((halfOpenLogs[0]!.obj as { from: string }).from).toBe("open");

    const closedLogs = logs.filter((l) => l.msg === "circuit breaker closed");
    expect(closedLogs.length).toBe(1);
    expect((closedLogs[0]!.obj as { event_name: string; from: string }).event_name).toBe("circuit_breaker_closed");
    expect((closedLogs[0]!.obj as { from: string }).from).toBe("half-open");
  });

  it("logs the second open transition with from='half-open' when the probe fails", async () => {
    // Inner producer that always fails — drives closed -> open, then on the
    // half-open probe attempt fails again and goes back to open.
    const inner = {
      isConnected: () => true,
      disconnect: async () => {},
      sendByTopic: async () => {
        throw new Error("metadata failed 4 times.");
      },
    };
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

    // First trip: closed -> open.
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    await Bun.sleep(250);
    // Probe attempt: transitions to half-open inside execute, fails, returns to open.
    await handle.sendByTopic("T", "probe", "v", null).catch(() => undefined);

    const openedLogs = logs.filter((l) => l.msg === "circuit breaker opened");
    expect(openedLogs.length).toBe(2);
    // First opened: from="closed".
    expect((openedLogs[0]!.obj as { from: string }).from).toBe("closed");
    // Second opened: from="half-open" — the bug fixed by the FSM onHalfOpen callback.
    expect((openedLogs[1]!.obj as { from: string }).from).toBe("half-open");
  });
});
