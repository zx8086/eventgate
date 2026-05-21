// test/unit/resilience.circuit-breaker.test.ts
import { beforeEach, describe, expect, it, test } from "bun:test";
import {
  CircuitBreaker,
  type CircuitBreakerConfig,
} from "../../src/resilience/circuit-breaker.ts";
import { CircuitBreakerOpenError } from "../../src/resilience/errors.ts";

const cfg: CircuitBreakerConfig = {
  failureThreshold: 3,
  successThreshold: 2,
  recoveryTimeoutMs: 200,
};

const fail = async (): Promise<void> => {
  throw new Error("upstream down");
};
const succeed = async (): Promise<string> => "ok";

describe("CircuitBreaker", () => {
  let breaker: CircuitBreaker;

  beforeEach(() => {
    breaker = new CircuitBreaker(cfg);
  });

  it("starts closed", () => {
    expect(breaker.getState()).toBe("closed");
  });

  it("opens after consecutive failures exceed threshold", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    expect(breaker.getState()).toBe("open");
  });

  it("rejects immediately when open", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    await expect(breaker.execute(succeed)).rejects.toBeInstanceOf(CircuitBreakerOpenError);
  });

  test("transitions to half-open after recovery timeout", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    await Bun.sleep(250);
    // The next execute triggers the half-open transition and lets the call through.
    await breaker.execute(succeed);
    expect(["half-open", "closed"]).toContain(breaker.getState());
  });

  test("closes after successThreshold successful probes", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    await Bun.sleep(250);
    await breaker.execute(succeed);
    await breaker.execute(succeed);
    expect(breaker.getState()).toBe("closed");
  });

  test("returns to open if probe fails in half-open", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    await Bun.sleep(250);
    await breaker.execute(fail).catch(() => undefined);
    expect(breaker.getState()).toBe("open");
  });

  it("does not trip on errors the predicate classifies as application-level", async () => {
    const appOnly = new CircuitBreaker(cfg, {
      isTransportError: (err) => !(err instanceof RangeError),
    });
    for (let i = 0; i < 5; i++) {
      await appOnly
        .execute(async () => {
          throw new RangeError("not a transport problem");
        })
        .catch(() => undefined);
    }
    expect(appOnly.getState()).toBe("closed");
  });

  it("getSnapshot returns the documented shape", async () => {
    expect(breaker.getSnapshot()).toEqual({
      state: "closed",
      failures: 0,
      nextAttemptAt: null,
    });
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    const snap = breaker.getSnapshot();
    expect(snap.state).toBe("open");
    expect(snap.failures).toBe(3);
    expect(snap.nextAttemptAt).not.toBeNull();
    expect(typeof snap.nextAttemptAt).toBe("number");
  });

  it("forceOpen() puts the breaker into open immediately", () => {
    breaker.forceOpen();
    expect(breaker.getState()).toBe("open");
  });

  it("reset() returns the breaker to closed", async () => {
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => undefined);
    }
    expect(breaker.getState()).toBe("open");
    breaker.reset();
    expect(breaker.getState()).toBe("closed");
    expect(breaker.getSnapshot().failures).toBe(0);
  });

  it("invokes the onOpen callback when transitioning to open", async () => {
    let opens = 0;
    const b = new CircuitBreaker(cfg, {
      onOpen: () => {
        opens += 1;
      },
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(fail).catch(() => undefined);
    }
    expect(opens).toBe(1);
  });

  it("invokes onOpen each time half-open returns to open", async () => {
    let opens = 0;
    const b = new CircuitBreaker(cfg, {
      onOpen: () => {
        opens += 1;
      },
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(fail).catch(() => undefined);
    }
    expect(opens).toBe(1);
    await Bun.sleep(250);
    await b.execute(fail).catch(() => undefined);
    expect(opens).toBe(2);
  });

  it("passes the last transport error message into onOpen", async () => {
    const opens: Array<string | undefined> = [];
    const b = new CircuitBreaker(cfg, {
      onOpen: (lastError) => opens.push(lastError),
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(async () => {
        throw new Error("metadata failed 4 times.");
      }).catch(() => undefined);
    }
    expect(opens).toHaveLength(1);
    expect(opens[0]).toBe("metadata failed 4 times.");
  });

  it("passes undefined to onOpen when triggered by forceOpen()", () => {
    const opens: Array<string | undefined> = [];
    const b = new CircuitBreaker(cfg, {
      onOpen: (lastError) => opens.push(lastError),
    });
    b.forceOpen();
    expect(opens).toEqual([undefined]);
  });

  it("invokes onHalfOpen when the breaker transitions from open to half-open", async () => {
    const events: string[] = [];
    const b = new CircuitBreaker(cfg, {
      onOpen: () => events.push("open"),
      onHalfOpen: () => events.push("half-open"),
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(fail).catch(() => undefined);
    }
    expect(events).toEqual(["open"]);
    await Bun.sleep(250);
    // First execute after timeout triggers half-open before running the op.
    await b.execute(succeed);
    expect(events).toEqual(["open", "half-open"]);
  });
});
