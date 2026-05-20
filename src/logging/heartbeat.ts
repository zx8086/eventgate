// src/logging/heartbeat.ts
import { getLogger, type ILogger } from "./index.ts";

export type HeartbeatOptions = {
  intervalMs: number;
  snapshot: () => Record<string, unknown>;
  logger?: ILogger;
};

export type HeartbeatHandle = {
  stop(): void;
};

export function startHeartbeat(opts: HeartbeatOptions): HeartbeatHandle {
  const log = opts.logger ?? getLogger("gateway.heartbeat");
  if (opts.intervalMs <= 0) {
    return { stop: () => {} };
  }
  const timer = setInterval(() => {
    try {
      const fields = opts.snapshot();
      log.info(fields, "heartbeat");
    } catch (err) {
      log.warn({ err: err instanceof Error ? err.message : String(err) }, "heartbeat snapshot failed");
    }
  }, opts.intervalMs);
  if (typeof (timer as { unref?: () => void }).unref === "function") {
    (timer as { unref: () => void }).unref();
  }
  return {
    stop: () => clearInterval(timer),
  };
}
