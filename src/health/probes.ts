// src/health/probes.ts
import type { OutboxDatabase } from "../outbox/db.ts";
import type { DependencyStatus } from "./types.ts";

export type AdminLike = {
  listTopics(): Promise<string[]>;
};

export function probeOutboxDb(db: OutboxDatabase): DependencyStatus {
  const lastCheckedAt = Date.now();
  try {
    db.query("SELECT 1").get();
    return { ok: true, lastCheckedAt };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, lastCheckedAt, lastError: message };
  }
}

export type KafkaAdminProbeResult = {
  broker: DependencyStatus;
  topics: DependencyStatus;
};

export async function probeKafkaAdmin(opts: {
  admin: AdminLike;
  expectedTopics: string[];
  timeoutMs: number;
}): Promise<KafkaAdminProbeResult> {
  const { admin, expectedTopics, timeoutMs } = opts;
  const startedAt = Date.now();

  const timeout = new Promise<never>((_resolve, reject) => {
    const handle = setTimeout(
      () => reject(new Error(`probe timeout after ${timeoutMs}ms`)),
      timeoutMs,
    );
    // Unref so a stuck probe never keeps the process alive.
    if (typeof (handle as { unref?: () => void }).unref === "function") {
      (handle as { unref: () => void }).unref();
    }
  });

  try {
    const topics = await Promise.race([admin.listTopics(), timeout]);
    const elapsed = Date.now() - startedAt;
    const present = new Set(topics);
    const missing = expectedTopics.filter((t) => !present.has(t));
    return {
      broker: { ok: true, lastCheckedAt: Date.now(), brokerProbeMs: elapsed },
      topics: {
        ok: missing.length === 0,
        lastCheckedAt: Date.now(),
        missing,
      },
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const at = Date.now();
    return {
      broker: { ok: false, lastCheckedAt: at, lastError: message },
      topics: {
        ok: false,
        lastCheckedAt: at,
        lastError: "broker probe failed; topic check skipped",
        missing: expectedTopics,
      },
    };
  }
}
