// test/unit/logging.heartbeat.test.ts
import { describe, expect, it } from "bun:test";
import type { ILogger, LogBindings } from "../../src/logging/index.ts";
import { startHeartbeat } from "../../src/logging/heartbeat.ts";

function fakeLogger(): { calls: Array<{ obj: object; msg: string }>; logger: ILogger } {
  const calls: Array<{ obj: object; msg: string }> = [];
  const logger: ILogger = {
    info: (obj: LogBindings | string, msg?: string) => {
      const bindings = typeof obj === "string" ? { message: obj } : obj;
      calls.push({ obj: bindings, msg: msg ?? "" });
    },
    warn: () => {},
    error: () => {},
    debug: () => {},
    trace: () => {},
    fatal: () => {},
    child: () => logger,
    flush: () => {},
  };
  return { calls, logger };
}

describe("startHeartbeat", () => {
  it("emits info logs on the requested cadence", async () => {
    const { calls, logger } = fakeLogger();
    let n = 0;
    const handle = startHeartbeat({
      intervalMs: 5,
      snapshot: () => ({ tick: ++n }),
      logger,
    });
    await new Promise((r) => setTimeout(r, 30));
    handle.stop();
    expect(calls.length).toBeGreaterThanOrEqual(2);
    expect(calls[0]?.msg).toBe("heartbeat");
    expect((calls[0]?.obj as { tick: number }).tick).toBeGreaterThan(0);
  });

  it("does nothing when intervalMs <= 0", async () => {
    const { calls, logger } = fakeLogger();
    const handle = startHeartbeat({
      intervalMs: 0,
      snapshot: () => ({}),
      logger,
    });
    await new Promise((r) => setTimeout(r, 20));
    handle.stop();
    expect(calls.length).toBe(0);
  });

  it("swallows snapshot errors so the interval keeps running", async () => {
    const { calls, logger } = fakeLogger();
    let throwNext = true;
    const handle = startHeartbeat({
      intervalMs: 5,
      snapshot: () => {
        if (throwNext) {
          throwNext = false;
          throw new Error("snapshot boom");
        }
        return { ok: true };
      },
      logger,
    });
    await new Promise((r) => setTimeout(r, 30));
    handle.stop();
    expect(calls.some((c) => (c.obj as { ok?: boolean }).ok === true)).toBe(true);
  });
});
