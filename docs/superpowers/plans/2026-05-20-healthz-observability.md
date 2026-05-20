# Healthz Dependency Probes + Outbox Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/healthz` answer "is every dependency I rely on healthy?" in one cached call, surface drain progress + per-topic backlog + last-error in the response, and add the lifecycle logs (state transitions, drainer errors verbatim, 60 s heartbeat, startup summary) needed to debug the gateway from logs alone.

**Architecture:** A `HealthMonitor` runs a 30 s probe timer against the producer, the outbox DB (`SELECT 1`), and a single long-lived `@platformatic/kafka` `Admin` client (`listTopics()` answers broker-reachability + topic-existence in one round-trip). `/healthz` reads the cached snapshot synchronously. The drainer is wired to an in-memory `DrainMetrics` (sliding 60 s throughput ring buffer + `lastPublishedAt` + `lastError`). A 60 s heartbeat interval logs combined snapshot + metrics + backlog under component `gateway.heartbeat`. New config block `config.health.{probeIntervalMs, probeTimeoutMs, heartbeatMs}` plumbed through all four pillars.

**Tech Stack:** Bun 1.x, TypeScript strict, Zod v4, Pino 10 + `@elastic/ecs-pino-format`, `bun:sqlite`, `@platformatic/kafka` (`Producer` + `Admin`), `bun:test`.

**Spec:** `docs/superpowers/specs/2026-05-20-healthz-observability-design.md`
**Linear:** SIO-815

---

## Pre-flight

### Task 0: Create a feature branch

**Files:** none (git only)

- [ ] **Step 1: Confirm clean tree**

Run: `git status`
Expected: `nothing to commit, working tree clean` on `master`.

- [ ] **Step 2: Create the branch**

Run: `git checkout -b sio-815-healthz-observability`
Expected: `Switched to a new branch 'sio-815-healthz-observability'`.

- [ ] **Step 3: Verify the baseline is green**

Run: `bun run typecheck && bun test`
Expected: typecheck clean; all tests pass.

---

## Section A — Config plumbing (`health` block)

### Task A1: Add defaults for the `health` block

**Files:**
- Modify: `src/config/defaults.ts`

- [ ] **Step 1: Add the `health` field after `outbox`**

Open `src/config/defaults.ts`. Locate the `outbox` block ending at the closing brace before `routes: [`. Insert the following AFTER the `outbox` closing brace and the comma:

```ts
  health: {
    probeIntervalMs: 30_000,
    probeTimeoutMs: 5_000,
    heartbeatMs: 60_000,
  },
```

- [ ] **Step 2: Run typecheck**

Run: `bun run typecheck`
Expected: still passes (no consumer yet — defaults can carry an unknown field via `as const` widening).

Note: typecheck WILL fail at this step because `defaults` is typed as `Defaults = typeof defaults` and Zod-validated through `configSchema`. That's fine — we'll resolve it when we add the schema in Task A3.

- [ ] **Step 3: Commit**

```bash
git add src/config/defaults.ts
git commit -m "SIO-815: add health block defaults (probeIntervalMs, probeTimeoutMs, heartbeatMs)"
```

### Task A2: Add env mapping for `health`

**Files:**
- Modify: `src/config/envMapping.ts`

- [ ] **Step 1: Extend `EnvOverrides` type**

In `src/config/envMapping.ts`, find the `EnvOverrides` type (around line 4). Add a new field after `outbox`:

```ts
  health?: {
    probeIntervalMs?: number;
    probeTimeoutMs?: number;
    heartbeatMs?: number;
  };
```

- [ ] **Step 2: Map the env vars**

In `mapEnv`, after the `outbox: filterUndefined({ … })` block (around line 121), add:

```ts
    health: filterUndefined({
      probeIntervalMs: num(env.HEALTH_PROBE_INTERVAL_MS),
      probeTimeoutMs: num(env.HEALTH_PROBE_TIMEOUT_MS),
      heartbeatMs: num(env.STATS_HEARTBEAT_MS),
    }),
```

- [ ] **Step 3: Run typecheck**

Run: `bun run typecheck`
Expected: still passes (will fail on the schema side until Task A3 — that's expected).

- [ ] **Step 4: Commit**

```bash
git add src/config/envMapping.ts
git commit -m "SIO-815: map HEALTH_PROBE_INTERVAL_MS, HEALTH_PROBE_TIMEOUT_MS, STATS_HEARTBEAT_MS"
```

### Task A3: Write the failing schema test for `health`

**Files:**
- Create: `test/unit/config.health.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/config.health.test.ts`:

```ts
// test/unit/config.health.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
  resetConfigCache();
});

afterEach(() => {
  process.env = snapshot;
  resetConfigCache();
});

describe("config.health", () => {
  it("exposes defaults when no env vars are set", async () => {
    const { config } = await import("../../src/config/index.ts");
    expect(config.health.probeIntervalMs).toBe(30_000);
    expect(config.health.probeTimeoutMs).toBe(5_000);
    expect(config.health.heartbeatMs).toBe(60_000);
  });

  it("applies env overrides", async () => {
    process.env = {
      ...process.env,
      HEALTH_PROBE_INTERVAL_MS: "15000",
      HEALTH_PROBE_TIMEOUT_MS: "2500",
      STATS_HEARTBEAT_MS: "0",
    };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(config.health.probeIntervalMs).toBe(15_000);
    expect(config.health.probeTimeoutMs).toBe(2_500);
    expect(config.health.heartbeatMs).toBe(0);
  });

  it("rejects negative probeIntervalMs", async () => {
    process.env = { ...process.env, HEALTH_PROBE_INTERVAL_MS: "-1" };
    resetConfigCache();
    expect(async () => {
      const { config } = await import("../../src/config/index.ts");
      void config.health;
    }).toThrow();
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/config.health.test.ts`
Expected: FAIL — `config.health` is undefined or schema rejects unknown field.

### Task A4: Add the `healthSchema` to make the test pass

**Files:**
- Modify: `src/config/schemas.ts`

- [ ] **Step 1: Add the schema**

In `src/config/schemas.ts`, after the `confluentSchema` definition (around line 22), add:

```ts
const healthSchema = z.strictObject({
  probeIntervalMs: z
    .number()
    .int()
    .positive()
    .describe("Background health-probe cadence in ms. /healthz reads the cached result instantly."),
  probeTimeoutMs: z
    .number()
    .int()
    .positive()
    .describe("Per-probe timeout in ms. A hung dependency cannot stall the monitor."),
  heartbeatMs: z
    .number()
    .int()
    .nonnegative()
    .describe("Periodic stats-heartbeat interval in ms. 0 disables the heartbeat."),
});
```

- [ ] **Step 2: Wire it into `configSchema`**

In the same file, inside `configSchema = z.strictObject({ … })`, after the `outbox: z.strictObject({ … })` block (ends around line 203), add:

```ts
    health: healthSchema,
```

- [ ] **Step 3: Run typecheck**

Run: `bun run typecheck`
Expected: PASS — defaults match schema, env override map matches schema.

- [ ] **Step 4: Run the failing test**

Run: `bun test test/unit/config.health.test.ts`
Expected: PASS (all three cases).

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS — nothing else regresses.

- [ ] **Step 6: Commit**

```bash
git add src/config/schemas.ts test/unit/config.health.test.ts
git commit -m "SIO-815: add healthSchema (probeIntervalMs, probeTimeoutMs, heartbeatMs)"
```

---

## Section B — Outbox: pendingByTopic + DrainMetrics

### Task B1: Extend `BacklogStats` with `pendingByTopic`

**Files:**
- Modify: `src/outbox/writer.ts`
- Modify: `test/unit/outbox.writer.test.ts`

- [ ] **Step 1: Write the failing test**

Open `test/unit/outbox.writer.test.ts`. Inside `describe("createWriter.backlogStats", ...)`, add a new `it` block AFTER the existing tests:

```ts
  it("breaks pending counts down by topic", () => {
    const writer = createWriter(db);
    writer.enqueue({ topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", messageKey: "a", payload: "{}", headers: null });
    writer.enqueue({ topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", messageKey: "b", payload: "{}", headers: null });
    writer.enqueue({ topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS", messageKey: "c", payload: "{}", headers: null });
    db.run("UPDATE outbox SET status='dispatched' WHERE message_key='a'");
    const stats = writer.backlogStats();
    expect(stats.pendingByTopic).toEqual({
      T_PRIVATE_SOURCE_ELASTIC_AUTOOPS: 1,
      T_PRIVATE_SOURCE_DATADOG_ALERTS: 1,
    });
  });

  it("returns an empty pendingByTopic object when no pending rows", () => {
    const writer = createWriter(db);
    expect(writer.backlogStats().pendingByTopic).toEqual({});
  });
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/outbox.writer.test.ts -t "pendingByTopic"`
Expected: FAIL — property does not exist.

- [ ] **Step 3: Add `pendingByTopic` to `BacklogStats`**

Open `src/outbox/writer.ts`. Replace the existing `BacklogStats` type (lines 13–17) with:

```ts
export type BacklogStats = {
  pending: number;
  failed: number;
  oldestPendingAgeMs: number;
  pendingByTopic: Record<string, number>;
};
```

- [ ] **Step 4: Add the prepared statement**

In the same file, AFTER the `oldestPendingStmt` declaration (around line 53), add:

```ts
  const pendingByTopicStmt = db.query(
    "SELECT topic, COUNT(*) AS c FROM outbox WHERE status = 'pending' GROUP BY topic",
  );
```

- [ ] **Step 5: Compute it in `backlogStats`**

Replace the body of `backlogStats()` (lines 59–65) with:

```ts
    backlogStats(): BacklogStats {
      const pending = (pendingCountStmt.get() as { c: number }).c;
      const failed = (failedCountStmt.get() as { c: number }).c;
      const oldest = (oldestPendingStmt.get() as { m: number | null }).m;
      const oldestPendingAgeMs = oldest === null ? 0 : Date.now() - oldest;
      const rows = pendingByTopicStmt.all() as Array<{ topic: string; c: number }>;
      const pendingByTopic: Record<string, number> = {};
      for (const r of rows) pendingByTopic[r.topic] = r.c;
      return { pending, failed, oldestPendingAgeMs, pendingByTopic };
    },
```

- [ ] **Step 6: Run the new tests**

Run: `bun test test/unit/outbox.writer.test.ts`
Expected: PASS (all writer tests including the two new ones).

- [ ] **Step 7: Run the full suite to catch any consumer that destructured the old shape**

Run: `bun test`
Expected: PASS. If `gateway.routes.dispatch.test.ts` fails, update its `fakeOutbox` helper to return `pendingByTopic: {}` from `backlogStats`. Make the change, re-run.

If `fakeOutbox` needs updating, replace its `backlogStats` line with:

```ts
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0, pendingByTopic: {} }),
```

- [ ] **Step 8: Commit**

```bash
git add src/outbox/writer.ts test/unit/outbox.writer.test.ts test/unit/gateway.routes.dispatch.test.ts
git commit -m "SIO-815: backlogStats exposes pendingByTopic"
```

### Task B2: Write the failing test for `DrainMetrics`

**Files:**
- Create: `test/unit/outbox.metrics.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/outbox.metrics.test.ts`:

```ts
// test/unit/outbox.metrics.test.ts
import { describe, expect, it } from "bun:test";
import { createDrainMetrics } from "../../src/outbox/metrics.ts";

describe("DrainMetrics", () => {
  it("starts empty", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    expect(m.snapshot()).toEqual({
      publishedLast60s: 0,
      lastPublishedAt: null,
      lastError: null,
    });
  });

  it("counts publishes within the window", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordPublished("T1");
    t = 2_000; m.recordPublished("T1");
    t = 3_000; m.recordPublished("T2");
    t = 4_000;
    const snap = m.snapshot();
    expect(snap.publishedLast60s).toBe(3);
    expect(snap.lastPublishedAt).toBe(3_000);
  });

  it("evicts publishes older than the window on read", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordPublished("T1");
    t = 2_000; m.recordPublished("T1");
    t = 70_000;
    expect(m.snapshot().publishedLast60s).toBe(0);
    expect(m.snapshot().lastPublishedAt).toBe(2_000);
  });

  it("captures the last error", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 5_000;
    m.recordError("T_FOO", "UNKNOWN_TOPIC_OR_PARTITION");
    expect(m.snapshot().lastError).toEqual({
      topic: "T_FOO",
      message: "UNKNOWN_TOPIC_OR_PARTITION",
      at: 5_000,
    });
  });

  it("does not clear lastError after a successful publish — operators may still want the last failure visible", () => {
    let t = 0;
    const m = createDrainMetrics({ windowMs: 60_000, now: () => t });
    t = 1_000; m.recordError("T", "boom");
    t = 2_000; m.recordPublished("T");
    expect(m.snapshot().lastError?.message).toBe("boom");
    expect(m.snapshot().publishedLast60s).toBe(1);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/outbox.metrics.test.ts`
Expected: FAIL — module `../../src/outbox/metrics.ts` not found.

### Task B3: Implement `DrainMetrics`

**Files:**
- Create: `src/outbox/metrics.ts`

- [ ] **Step 1: Write the implementation**

Create `src/outbox/metrics.ts`:

```ts
// src/outbox/metrics.ts

export type DrainMetricsSnapshot = {
  publishedLast60s: number;
  lastPublishedAt: number | null;
  lastError: { topic: string; message: string; at: number } | null;
};

export type DrainMetrics = {
  recordPublished(topic: string): void;
  recordError(topic: string, message: string): void;
  snapshot(): DrainMetricsSnapshot;
};

export type DrainMetricsOptions = {
  windowMs?: number;
  now?: () => number;
};

export function createDrainMetrics(opts: DrainMetricsOptions = {}): DrainMetrics {
  const windowMs = opts.windowMs ?? 60_000;
  const now = opts.now ?? Date.now;

  // Timestamped ring buffer of publish times. Trimmed on read.
  const publishedAt: number[] = [];
  let lastPublishedAt: number | null = null;
  let lastError: DrainMetricsSnapshot["lastError"] = null;

  const trim = (cutoff: number): void => {
    // publishedAt is push-only with monotonic timestamps, so a leading slice
    // is enough — no need for binary search.
    let drop = 0;
    while (drop < publishedAt.length && publishedAt[drop]! < cutoff) drop += 1;
    if (drop > 0) publishedAt.splice(0, drop);
  };

  return {
    recordPublished(_topic) {
      const t = now();
      publishedAt.push(t);
      lastPublishedAt = t;
      // Bound the buffer's worst-case memory at sustained 10k/sec for 60s.
      if (publishedAt.length > 1_000_000) publishedAt.splice(0, publishedAt.length - 600_000);
    },
    recordError(topic, message) {
      lastError = { topic, message, at: now() };
    },
    snapshot(): DrainMetricsSnapshot {
      const cutoff = now() - windowMs;
      trim(cutoff);
      return {
        publishedLast60s: publishedAt.length,
        lastPublishedAt,
        lastError,
      };
    },
  };
}
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/outbox.metrics.test.ts`
Expected: PASS (5 cases).

- [ ] **Step 3: Commit**

```bash
git add src/outbox/metrics.ts test/unit/outbox.metrics.test.ts
git commit -m "SIO-815: add DrainMetrics with sliding-window throughput"
```

### Task B4: Wire `DrainMetrics` into the drainer (record + log on error)

**Files:**
- Modify: `src/outbox/drainer.ts`
- Modify: `test/unit/outbox.drainer.test.ts`

- [ ] **Step 1: Write the failing test for the new wiring**

Open `test/unit/outbox.drainer.test.ts`. Add a new `describe` block at the bottom (after the closing `});` of the last describe):

```ts
import { createDrainMetrics } from "../../src/outbox/metrics.ts";

describe("runOutboxIteration metrics + error logging", () => {
  test("records published rows in DrainMetrics", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    const producer = makeFakeProducer();
    const metrics = createDrainMetrics({ windowMs: 60_000 });

    await runOutboxIteration({ db, producer, config: cfg, metrics });

    const snap = metrics.snapshot();
    expect(snap.publishedLast60s).toBe(2);
    expect(snap.lastPublishedAt).not.toBeNull();
    expect(snap.lastError).toBeNull();
  });

  test("records the broker error text on failed publish", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    const producer = makeFakeProducer({ "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS": 2 });
    const metrics = createDrainMetrics({ windowMs: 60_000 });

    await runOutboxIteration({ db, producer, config: cfg, metrics });

    const snap = metrics.snapshot();
    expect(snap.publishedLast60s).toBe(0);
    expect(snap.lastError?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(snap.lastError?.message).toMatch(/forced failure/);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/outbox.drainer.test.ts -t "metrics"`
Expected: FAIL — `metrics` is not an accepted argument.

- [ ] **Step 3: Extend the drainer's argument types**

Open `src/outbox/drainer.ts`. Add this import near the existing imports (line 2):

```ts
import type { DrainMetrics } from "./metrics.ts";
```

- [ ] **Step 4: Pass `metrics` through `runOutboxIteration`**

Replace the existing `runOutboxIteration` signature and call into the catch block. Specifically:

a) Change the `opts` type in the `export async function runOutboxIteration(opts: {…})` signature to:

```ts
export async function runOutboxIteration(opts: {
  db: OutboxDatabase;
  producer: DrainerProducer;
  config: DrainerConfig;
  metrics?: DrainMetrics;
}): Promise<IterationResult> {
  const { db, producer, config, metrics } = opts;
```

b) In the success branch, AFTER `published += 1;` (around line 97), add:

```ts
      metrics?.recordPublished(row.topic);
```

c) In the catch branch, AFTER `const message = err instanceof Error ? err.message : String(err);` (around line 101), add:

```ts
      metrics?.recordError(row.topic, message);
      log.warn(
        { topic: row.topic, attempts, ageMs, err: message, id: row.id },
        "outbox publish failed",
      );
```

(The existing max-age `log.warn` below it stays as-is.)

- [ ] **Step 5: Pass `metrics` through `startDrainer`**

In the same file, extend the `startDrainer` opts type to accept `metrics`. Replace the existing signature:

```ts
export function startDrainer(opts: {
  db: OutboxDatabase;
  producer: DrainerProducer;
  config: DrainerStartConfig;
  metrics?: DrainMetrics;
}): DrainerHandle {
  const { db, producer, config, metrics } = opts;
```

And change the `runOutboxIteration` call inside `tick` (around line 143) from:

```ts
        const result = await runOutboxIteration({ db, producer, config });
```

to:

```ts
        const result = await runOutboxIteration({ db, producer, config, metrics });
```

- [ ] **Step 6: Run the new tests**

Run: `bun test test/unit/outbox.drainer.test.ts`
Expected: PASS — all drainer tests including the two new ones.

- [ ] **Step 7: Run the full suite**

Run: `bun test`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/outbox/drainer.ts test/unit/outbox.drainer.test.ts
git commit -m "SIO-815: drainer records DrainMetrics and warn-logs broker errors"
```

---

## Section C — Health module

### Task C1: Define health types

**Files:**
- Create: `src/health/types.ts`

- [ ] **Step 1: Write the file**

Create `src/health/types.ts`:

```ts
// src/health/types.ts

export type DependencyName = "kafkaProducer" | "outboxDb" | "kafkaBroker" | "topics";

export type DependencyStatus = {
  ok: boolean;
  lastCheckedAt: number;
  lastError?: string;
  // Per-dependency extras (kept loose because each probe carries different metadata):
  connected?: boolean;       // kafkaProducer
  brokerProbeMs?: number;    // kafkaBroker
  missing?: string[];        // topics
};

export type HealthSnapshot = {
  status: "healthy" | "degraded" | "unhealthy";
  ok: boolean;
  checkedAt: number;
  dependencies: Partial<Record<DependencyName, DependencyStatus>>;
};

export type HealthRequiredness = {
  // Dependencies that demote 200 → 503 on failure. Topics are NOT in here —
  // missing topics produce status="degraded" but still return 200, because the
  // outbox is designed to ride out downstream gaps.
  required: ReadonlyArray<DependencyName>;
};

export const DEFAULT_REQUIREDNESS: HealthRequiredness = {
  required: ["kafkaProducer", "outboxDb", "kafkaBroker"],
};
```

- [ ] **Step 2: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/health/types.ts
git commit -m "SIO-815: add health types (DependencyStatus, HealthSnapshot)"
```

### Task C2: Write the failing test for probes

**Files:**
- Create: `test/unit/health.probes.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/health.probes.test.ts`:

```ts
// test/unit/health.probes.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { probeOutboxDb, probeKafkaAdmin } from "../../src/health/probes.ts";

let db: OutboxDatabase;
beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

describe("probeOutboxDb", () => {
  it("returns ok=true on a healthy DB", () => {
    const status = probeOutboxDb(db);
    expect(status.ok).toBe(true);
    expect(status.lastError).toBeUndefined();
    expect(status.lastCheckedAt).toBeGreaterThan(0);
  });

  it("returns ok=false when SELECT throws", () => {
    closeOutbox(db);
    const status = probeOutboxDb(db);
    expect(status.ok).toBe(false);
    expect(status.lastError).toMatch(/./);
  });
});

describe("probeKafkaAdmin", () => {
  it("returns ok=true with broker probe ms and empty missing when all topics exist", async () => {
    const admin = { listTopics: async () => ["A", "B", "C"] };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A", "B"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(true);
    expect(result.broker.brokerProbeMs).toBeGreaterThanOrEqual(0);
    expect(result.topics.ok).toBe(true);
    expect(result.topics.missing).toEqual([]);
  });

  it("reports missing topics", async () => {
    const admin = { listTopics: async () => ["A"] };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A", "B"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(true);
    expect(result.topics.ok).toBe(false);
    expect(result.topics.missing).toEqual(["B"]);
  });

  it("returns broker.ok=false on probe error", async () => {
    const admin = {
      listTopics: async () => {
        throw new Error("connection refused");
      },
    };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A"],
      timeoutMs: 1_000,
    });
    expect(result.broker.ok).toBe(false);
    expect(result.broker.lastError).toMatch(/connection refused/);
    expect(result.topics.ok).toBe(false);
    expect(result.topics.lastError).toMatch(/broker probe failed/);
  });

  it("times out a hanging probe", async () => {
    const admin = {
      listTopics: () => new Promise<string[]>(() => {}),
    };
    const result = await probeKafkaAdmin({
      admin,
      expectedTopics: ["A"],
      timeoutMs: 20,
    });
    expect(result.broker.ok).toBe(false);
    expect(result.broker.lastError).toMatch(/timeout/i);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/health.probes.test.ts`
Expected: FAIL — module `../../src/health/probes.ts` not found.

### Task C3: Implement probes

**Files:**
- Create: `src/health/probes.ts`

- [ ] **Step 1: Write the file**

Create `src/health/probes.ts`:

```ts
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
    const handle = setTimeout(() => reject(new Error(`probe timeout after ${timeoutMs}ms`)), timeoutMs);
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
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/health.probes.test.ts`
Expected: PASS (5 cases).

- [ ] **Step 3: Commit**

```bash
git add src/health/probes.ts test/unit/health.probes.test.ts
git commit -m "SIO-815: add probeOutboxDb and probeKafkaAdmin with timeout"
```

### Task C4: Write the failing test for `HealthMonitor`

**Files:**
- Create: `test/unit/health.monitor.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/health.monitor.test.ts`:

```ts
// test/unit/health.monitor.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createHealthMonitor } from "../../src/health/monitor.ts";

let db: OutboxDatabase;
beforeEach(() => {
  db = openOutbox(":memory:");
});
afterEach(() => closeOutbox(db));

function fakeAdmin(listTopics: () => Promise<string[]>) {
  return { listTopics };
}

function fakeProducer(connected: boolean) {
  return { isConnected: () => connected };
}

describe("HealthMonitor", () => {
  it("returns a snapshot synchronously after start() resolves", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => ["T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"]),
      expectedTopics: ["T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("healthy");
    expect(snap.ok).toBe(true);
    expect(snap.dependencies.kafkaProducer?.ok).toBe(true);
    expect(snap.dependencies.outboxDb?.ok).toBe(true);
    expect(snap.dependencies.kafkaBroker?.ok).toBe(true);
    expect(snap.dependencies.topics?.ok).toBe(true);
    await monitor.stop();
  });

  it("reports degraded (200-class) when only topics are missing", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => []),
      expectedTopics: ["T_MISSING"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("degraded");
    expect(snap.ok).toBe(true);
    expect(snap.dependencies.topics?.missing).toEqual(["T_MISSING"]);
    await monitor.stop();
  });

  it("reports unhealthy when producer is disconnected", async () => {
    const monitor = createHealthMonitor({
      producer: fakeProducer(false),
      outboxDb: db,
      admin: fakeAdmin(async () => ["T_A"]),
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    const snap = monitor.snapshot();
    expect(snap.status).toBe("unhealthy");
    expect(snap.ok).toBe(false);
    expect(snap.dependencies.kafkaProducer?.ok).toBe(false);
    await monitor.stop();
  });

  it("logs a state transition only when a dependency flips", async () => {
    let topics = ["T_A"];
    const logs: Array<{ level: string; obj: object; msg: string }> = [];
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: fakeAdmin(async () => topics),
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
      logger: {
        info: (obj, msg) => logs.push({ level: "info", obj: obj as object, msg: msg as string }),
        warn: () => {},
        error: () => {},
        debug: () => {},
        trace: () => {},
        fatal: () => {},
        child: () => ({} as never),
        flush: () => {},
      },
    });
    await monitor.start();
    logs.length = 0;
    topics = []; // simulate the topic disappearing
    await monitor.probeOnce();
    const flipLogs = logs.filter((l) => l.msg === "dependency state changed");
    expect(flipLogs.length).toBe(1);
    expect((flipLogs[0]!.obj as { dependency: string }).dependency).toBe("topics");
    topics = ["T_A"]; // restore
    await monitor.probeOnce();
    const restoreLogs = logs.filter((l) => l.msg === "dependency state changed");
    expect(restoreLogs.length).toBe(2);
    await monitor.stop();
  });

  it("isolates probe failures — a thrown probe must not stop the monitor", async () => {
    let throwOnce = true;
    const monitor = createHealthMonitor({
      producer: fakeProducer(true),
      outboxDb: db,
      admin: {
        listTopics: async () => {
          if (throwOnce) {
            throwOnce = false;
            throw new Error("broker boom");
          }
          return ["T_A"];
        },
      },
      expectedTopics: ["T_A"],
      probeIntervalMs: 10_000,
      probeTimeoutMs: 1_000,
    });
    await monitor.start();
    expect(monitor.snapshot().dependencies.kafkaBroker?.ok).toBe(false);
    await monitor.probeOnce();
    expect(monitor.snapshot().dependencies.kafkaBroker?.ok).toBe(true);
    await monitor.stop();
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/health.monitor.test.ts`
Expected: FAIL — `../../src/health/monitor.ts` not found.

### Task C5: Implement `HealthMonitor`

**Files:**
- Create: `src/health/monitor.ts`

- [ ] **Step 1: Write the file**

Create `src/health/monitor.ts`:

```ts
// src/health/monitor.ts
import { getLogger, type ILogger } from "../logging/index.ts";
import type { OutboxDatabase } from "../outbox/db.ts";
import { probeOutboxDb, probeKafkaAdmin, type AdminLike } from "./probes.ts";
import {
  DEFAULT_REQUIREDNESS,
  type DependencyName,
  type DependencyStatus,
  type HealthSnapshot,
} from "./types.ts";

export type HealthMonitor = {
  start(): Promise<void>;
  stop(): Promise<void>;
  snapshot(): HealthSnapshot;
  probeOnce(): Promise<void>;
};

type ProducerLike = { isConnected(): boolean };

export type HealthMonitorOptions = {
  producer: ProducerLike;
  outboxDb?: OutboxDatabase;
  admin: AdminLike;
  expectedTopics: string[];
  probeIntervalMs: number;
  probeTimeoutMs: number;
  logger?: ILogger;
};

function emptySnapshot(): HealthSnapshot {
  return {
    status: "healthy",
    ok: true,
    checkedAt: 0,
    dependencies: {},
  };
}

export function createHealthMonitor(opts: HealthMonitorOptions): HealthMonitor {
  const log = opts.logger ?? getLogger("gateway.health");
  let current: HealthSnapshot = emptySnapshot();
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> = Promise.resolve();

  const runProbeCycle = async (): Promise<HealthSnapshot> => {
    const checkedAt = Date.now();
    const producerOk = opts.producer.isConnected();
    const deps: HealthSnapshot["dependencies"] = {
      kafkaProducer: { ok: producerOk, lastCheckedAt: checkedAt, connected: producerOk },
    };

    if (opts.outboxDb) {
      deps.outboxDb = probeOutboxDb(opts.outboxDb);
    }

    try {
      const kafkaResult = await probeKafkaAdmin({
        admin: opts.admin,
        expectedTopics: opts.expectedTopics,
        timeoutMs: opts.probeTimeoutMs,
      });
      deps.kafkaBroker = kafkaResult.broker;
      deps.topics = kafkaResult.topics;
    } catch (err) {
      // probeKafkaAdmin already catches internally; this is a defensive guard.
      const message = err instanceof Error ? err.message : String(err);
      const at = Date.now();
      deps.kafkaBroker = { ok: false, lastCheckedAt: at, lastError: message };
      deps.topics = { ok: false, lastCheckedAt: at, lastError: "broker probe failed; topic check skipped", missing: opts.expectedTopics };
    }

    const required = DEFAULT_REQUIREDNESS.required;
    const requiredOk = required.every((name) => deps[name]?.ok ?? false);
    const allOk = requiredOk && (deps.topics?.ok ?? true);
    const status: HealthSnapshot["status"] = !requiredOk ? "unhealthy" : !allOk ? "degraded" : "healthy";

    return { status, ok: requiredOk, checkedAt, dependencies: deps };
  };

  const diffAndLog = (prev: HealthSnapshot, next: HealthSnapshot): void => {
    const names: DependencyName[] = ["kafkaProducer", "outboxDb", "kafkaBroker", "topics"];
    for (const name of names) {
      const before = prev.dependencies[name];
      const after = next.dependencies[name];
      if (after === undefined) continue;
      const beforeOk = before?.ok;
      if (beforeOk === undefined) continue; // first cycle — don't log every dep as "transitioned"
      if (beforeOk === after.ok) continue;
      log.info(
        {
          dependency: name,
          ok: after.ok,
          ...(after.lastError ? { lastError: after.lastError } : {}),
          ...(after.missing && after.missing.length > 0 ? { missing: after.missing } : {}),
        },
        "dependency state changed",
      );
    }
  };

  const cycle = async (): Promise<void> => {
    if (stopped) return;
    try {
      const next = await runProbeCycle();
      diffAndLog(current, next);
      current = next;
    } catch (err) {
      // Telemetry must never crash the monitor loop.
      log.warn({ err: err instanceof Error ? err.message : String(err) }, "health monitor cycle failed");
    }
    if (!stopped) {
      timer = setTimeout(() => {
        inFlight = cycle();
      }, opts.probeIntervalMs);
      if (typeof (timer as { unref?: () => void }).unref === "function") {
        (timer as { unref: () => void }).unref();
      }
    }
  };

  return {
    async start() {
      // First cycle awaited so /healthz never serves an empty snapshot.
      current = await runProbeCycle();
      if (!stopped) {
        timer = setTimeout(() => {
          inFlight = cycle();
        }, opts.probeIntervalMs);
        if (typeof (timer as { unref?: () => void }).unref === "function") {
          (timer as { unref: () => void }).unref();
        }
      }
    },
    async stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
      await inFlight;
    },
    snapshot() {
      return current;
    },
    async probeOnce() {
      const next = await runProbeCycle();
      diffAndLog(current, next);
      current = next;
    },
  };
}
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/health.monitor.test.ts`
Expected: PASS (5 cases).

- [ ] **Step 3: Run the full suite**

Run: `bun test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/health/monitor.ts test/unit/health.monitor.test.ts
git commit -m "SIO-815: add HealthMonitor with cached snapshot and state-transition logging"
```

### Task C6: Add the `createAdminFromProvider` factory

**Files:**
- Create: `src/health/admin.ts`

The `HealthMonitor` consumes an `AdminLike` interface so it's mockable. In production, we need a tiny factory that constructs the real `@platformatic/kafka` `Admin` from the same connection config the producer uses, and exposes `close()` for shutdown.

- [ ] **Step 1: Write the file**

Create `src/health/admin.ts`:

```ts
// src/health/admin.ts
import { Admin } from "@platformatic/kafka";
import type { KafkaProvider } from "../kafka/providers/index.ts";
import type { AdminLike } from "./probes.ts";

export type HealthAdmin = AdminLike & {
  close(): Promise<void>;
};

export async function createHealthAdmin(provider: KafkaProvider): Promise<HealthAdmin> {
  const conn = await provider.getConnectionConfig();
  const admin = new Admin({
    clientId: `${conn.clientId}-health`,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
  });
  return {
    listTopics: async () => admin.listTopics(),
    close: async () => admin.close(),
  };
}
```

- [ ] **Step 2: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/health/admin.ts
git commit -m "SIO-815: add createHealthAdmin factory wrapping @platformatic/kafka Admin"
```

---

## Section D — Heartbeat

### Task D1: Write the failing test for the heartbeat

**Files:**
- Create: `test/unit/logging.heartbeat.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/logging.heartbeat.test.ts`:

```ts
// test/unit/logging.heartbeat.test.ts
import { describe, expect, it } from "bun:test";
import { startHeartbeat } from "../../src/logging/heartbeat.ts";

function fakeLogger() {
  const calls: Array<{ obj: object; msg: string }> = [];
  return {
    calls,
    logger: {
      info: (obj: object, msg?: string) => calls.push({ obj, msg: msg ?? "" }),
      warn: () => {},
      error: () => {},
      debug: () => {},
      trace: () => {},
      fatal: () => {},
      child: () => ({} as never),
      flush: () => {},
    },
  };
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
    // At least one successful heartbeat must follow the throw.
    expect(calls.some((c) => (c.obj as { ok?: boolean }).ok === true)).toBe(true);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/logging.heartbeat.test.ts`
Expected: FAIL — module not found.

### Task D2: Implement the heartbeat

**Files:**
- Create: `src/logging/heartbeat.ts`

- [ ] **Step 1: Write the file**

Create `src/logging/heartbeat.ts`:

```ts
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
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/logging.heartbeat.test.ts`
Expected: PASS (3 cases).

- [ ] **Step 3: Commit**

```bash
git add src/logging/heartbeat.ts test/unit/logging.heartbeat.test.ts
git commit -m "SIO-815: add startHeartbeat() interval logger"
```

---

## Section E — /healthz response shape

### Task E1: Extend `RouteDeps` and rewrite the `/healthz` handler

**Files:**
- Modify: `src/gateway/routes.ts`
- Modify: `test/unit/gateway.routes.dispatch.test.ts`

- [ ] **Step 1: Write the failing tests**

Open `test/unit/gateway.routes.dispatch.test.ts`. At the top of the file, ADD this import:

```ts
import type { HealthSnapshot } from "../../src/health/types.ts";
import type { DrainMetricsSnapshot } from "../../src/outbox/metrics.ts";
```

Then add helpers AFTER the existing `fakeOutbox` function:

```ts
function fakeMonitor(snapshot: HealthSnapshot) {
  return {
    start: async () => {},
    stop: async () => {},
    snapshot: () => snapshot,
    probeOnce: async () => {},
  };
}

function fakeMetrics(snap: DrainMetricsSnapshot) {
  return {
    recordPublished: () => {},
    recordError: () => {},
    snapshot: () => snap,
  };
}

const healthySnap: HealthSnapshot = {
  status: "healthy",
  ok: true,
  checkedAt: 1_000,
  dependencies: {
    kafkaProducer: { ok: true, lastCheckedAt: 1_000, connected: true },
    outboxDb: { ok: true, lastCheckedAt: 1_000 },
    kafkaBroker: { ok: true, lastCheckedAt: 1_000, brokerProbeMs: 5 },
    topics: { ok: true, lastCheckedAt: 1_000, missing: [] },
  },
};

const emptyMetricsSnap: DrainMetricsSnapshot = {
  publishedLast60s: 0,
  lastPublishedAt: null,
  lastError: null,
};
```

REPLACE the existing `it("healthz reports producer + outbox status", …)` and `it("healthz includes the registered routes", …)` blocks with:

```ts
  it("healthz returns 200 + healthy when all dependencies are ok", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string; dependencies: Record<string, { ok: boolean }> };
    expect(body.status).toBe("healthy");
    expect(body.dependencies.kafkaProducer?.ok).toBe(true);
  });

  it("healthz returns 200 + degraded when only topics are missing", async () => {
    const outbox = fakeOutbox();
    const degraded: HealthSnapshot = {
      ...healthySnap,
      status: "degraded",
      dependencies: {
        ...healthySnap.dependencies,
        topics: { ok: false, lastCheckedAt: 1_000, missing: ["T_MISSING"] },
      },
    };
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(degraded),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string; dependencies: { topics: { missing: string[] } } };
    expect(body.status).toBe("degraded");
    expect(body.dependencies.topics.missing).toEqual(["T_MISSING"]);
  });

  it("healthz returns 503 + unhealthy when a required dependency fails", async () => {
    const outbox = fakeOutbox();
    const unhealthy: HealthSnapshot = {
      ...healthySnap,
      status: "unhealthy",
      ok: false,
      dependencies: {
        ...healthySnap.dependencies,
        kafkaBroker: { ok: false, lastCheckedAt: 1_000, lastError: "connection refused" },
      },
    };
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(unhealthy),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(503);
    const body = await res.json() as { status: string };
    expect(body.status).toBe("unhealthy");
  });

  it("healthz includes outbox stats and drain metrics", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics({
        publishedLast60s: 42,
        lastPublishedAt: 999,
        lastError: { topic: "T", message: "x", at: 500 },
      }),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    const body = await res.json() as {
      outbox: { publishedLast60s: number; lastPublishedAt: number | null; lastError: unknown; pendingByTopic: Record<string, number> };
    };
    expect(body.outbox.publishedLast60s).toBe(42);
    expect(body.outbox.lastPublishedAt).toBe(999);
    expect(body.outbox.lastError).toEqual({ topic: "T", message: "x", at: 500 });
    expect(body.outbox.pendingByTopic).toEqual({});
  });

  it("healthz still includes the registered routes", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    const body = await res.json() as { routes: Array<{ name: string }> };
    expect(body.routes.map((r) => r.name)).toEqual(["elastic-autoops", "datadog-alerts"]);
  });
```

Also update the two existing `buildRoutes({ producer: noopProducer, outbox })` calls in the first two tests of the file (`"registers a POST handler per configured route"` and `"dispatches each route to its own topic"`) to pass the new deps:

```ts
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(healthySnap),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts`
Expected: FAIL — `monitor` is not in `RouteDeps`; `/healthz` returns the old shape.

- [ ] **Step 3: Extend `RouteDeps` and replace the `/healthz` handler**

Open `src/gateway/routes.ts`. ADD these imports:

```ts
import type { DrainMetrics } from "../outbox/metrics.ts";
import type { HealthMonitor } from "../health/monitor.ts";
```

REPLACE the existing `RouteDeps` type (lines 17–21) with:

```ts
export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
  monitor: HealthMonitor;
  metrics: DrainMetrics;
  adminContext?: AdminContext;
};
```

REPLACE the whole `/healthz` handler (lines 35–59) with:

```ts
    "/healthz": () => {
      const snap = deps.monitor.snapshot();
      const stats = outbox?.backlogStats();
      const drain = deps.metrics.snapshot();
      const body = {
        ok: snap.ok,
        status: snap.status,
        checkedAt: snap.checkedAt,
        dependencies: snap.dependencies,
        outbox: stats
          ? {
              enabled: true,
              pending: stats.pending,
              failed: stats.failed,
              oldestPendingAgeMs: stats.oldestPendingAgeMs,
              pendingByTopic: stats.pendingByTopic,
              publishedLast60s: drain.publishedLast60s,
              lastPublishedAt: drain.lastPublishedAt,
              lastError: drain.lastError,
            }
          : { enabled: false },
        routes: config.routes.map((r) => ({
          name: r.name,
          path: r.path,
          topic: r.topic,
          dlqTopic: r.dlqTopic,
        })),
      };
      return Response.json(body, { status: snap.ok ? 200 : 503 });
    },
```

- [ ] **Step 4: Run the test**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts`
Expected: PASS (all old + new cases).

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS. If `test/unit/gateway.routes.test.ts` or `test/unit/gateway.index.adminEnabled.test.ts` references `buildRoutes`, update their `buildRoutes(…)` calls similarly. Open each, search for `buildRoutes(`, and if found, pass `monitor: fakeMonitor(healthySnap)` and `metrics: fakeMetrics(emptyMetricsSnap)` — copying the fake helpers from `gateway.routes.dispatch.test.ts` into the same file (or extracting to a shared `test/helpers/routesDeps.ts` if you prefer; tests are co-located today so duplication is fine for one round).

- [ ] **Step 6: Commit**

```bash
git add src/gateway/routes.ts test/unit/gateway.routes.dispatch.test.ts test/unit/gateway.routes.test.ts test/unit/gateway.index.adminEnabled.test.ts
git commit -m "SIO-815: /healthz reads HealthMonitor snapshot + DrainMetrics"
```

(If a listed test file was not actually changed, drop it from the `git add` line.)

---

## Section F — Wire everything into the gateway

### Task F1: Construct monitor + metrics + heartbeat in `src/gateway/index.ts`

**Files:**
- Modify: `src/gateway/index.ts`

- [ ] **Step 1: Add imports**

Open `src/gateway/index.ts`. ADD these imports (preserving alphabetical-ish order with the existing block):

```ts
import { createHealthAdmin, type HealthAdmin } from "../health/admin.ts";
import { createHealthMonitor, type HealthMonitor } from "../health/monitor.ts";
import { startHeartbeat, type HeartbeatHandle } from "../logging/heartbeat.ts";
import { createDrainMetrics } from "../outbox/metrics.ts";
```

- [ ] **Step 2: Construct metrics and pass into the drainer**

Locate the `drainer = startDrainer({ … })` call (around line 32). Just BEFORE it, add:

```ts
const drainMetrics = createDrainMetrics({ windowMs: 60_000 });
```

Then, inside the `startDrainer({ … })` argument object, after `config: { … }`, add a line:

```ts
    metrics: drainMetrics,
```

Also, when the outbox is DISABLED (the `else` branch around line 48), still keep `drainMetrics` so `/healthz` can serve a meaningful (zero) snapshot. The declaration sits OUTSIDE the `if (config.outbox.enabled)` block — move it above the `if` if needed. Final shape:

```ts
const drainMetrics = createDrainMetrics({ windowMs: 60_000 });

let outboxDb: OutboxDatabase | undefined;
let outboxWriter: OutboxWriter | undefined;
let drainer: DrainerHandle | undefined;

if (config.outbox.enabled) {
  // …existing code…
  drainer = startDrainer({
    db: outboxDb,
    producer,
    config: { /* unchanged */ },
    metrics: drainMetrics,
  });
  // …existing log…
} else {
  log.warn("outbox disabled; inline publish (escape hatch)");
}
```

- [ ] **Step 3: Construct Admin + HealthMonitor**

AFTER the `} else { log.warn("outbox disabled…") }` block and BEFORE the `let server` declaration (around line 52), add:

```ts
const healthAdmin: HealthAdmin = await createHealthAdmin(provider);
const monitor: HealthMonitor = createHealthMonitor({
  producer,
  outboxDb: outboxDb,
  admin: healthAdmin,
  expectedTopics: [
    ...config.routes.map((r) => r.topic),
    ...config.routes.map((r) => r.dlqTopic),
  ],
  probeIntervalMs: config.health.probeIntervalMs,
  probeTimeoutMs: config.health.probeTimeoutMs,
});
await monitor.start();
```

- [ ] **Step 4: Pass `monitor` + `metrics` to `buildRoutes` via the existing `rebuildRoutes` helper**

Inside `rebuildRoutes` (around line 54), update the final `buildRoutes({…})` call to:

```ts
  return buildRoutes({
    producer,
    outbox: outboxWriter,
    monitor,
    metrics: drainMetrics,
    adminContext,
  });
```

- [ ] **Step 5: Replace the "gateway listening" log with the richer startup summary**

Locate the `log.info({ host, port, routes }, "gateway listening")` block (lines 103–110). REPLACE it with:

```ts
log.info(
  {
    host: server.hostname,
    port: server.port,
    provider: provider.name,
    providerType: provider.type,
    outbox: { enabled: config.outbox.enabled, dbPath: config.outbox.enabled ? config.outbox.dbPath : null },
    routes: config.routes.map((r) => ({ name: r.name, path: r.path, topic: r.topic })),
    health: {
      probeIntervalMs: config.health.probeIntervalMs,
      probeTimeoutMs: config.health.probeTimeoutMs,
      dependencies: ["kafkaProducer", config.outbox.enabled ? "outboxDb" : null, "kafkaBroker", "topics"].filter(Boolean),
    },
    heartbeatMs: config.health.heartbeatMs,
  },
  "gateway listening",
);
```

- [ ] **Step 6: Start the heartbeat**

IMMEDIATELY AFTER the "gateway listening" log block, add:

```ts
const heartbeat: HeartbeatHandle = startHeartbeat({
  intervalMs: config.health.heartbeatMs,
  snapshot: () => {
    const snap = monitor.snapshot();
    const drain = drainMetrics.snapshot();
    const stats = outboxWriter?.backlogStats();
    return {
      producerConnected: snap.dependencies.kafkaProducer?.ok ?? false,
      brokerOk: snap.dependencies.kafkaBroker?.ok ?? false,
      topicsOk: snap.dependencies.topics?.ok ?? true,
      status: snap.status,
      ...(stats
        ? {
            pending: stats.pending,
            failed: stats.failed,
            oldestPendingAgeMs: stats.oldestPendingAgeMs,
            pendingByTopic: stats.pendingByTopic,
          }
        : {}),
      publishedLast60s: drain.publishedLast60s,
      lastPublishedAt: drain.lastPublishedAt,
    };
  },
});
```

- [ ] **Step 7: Extend shutdown**

REPLACE the existing `shutdown` function (lines 112–120) with:

```ts
async function shutdown(signal: string) {
  log.info({ signal }, "shutting down gateway");
  server?.stop();
  heartbeat.stop();
  await monitor.stop();
  await healthAdmin.close();
  if (drainer) await drainer.stop();
  await producer.disconnect();
  await provider.close();
  if (outboxDb) closeOutbox(outboxDb);
  process.exit(0);
}
```

- [ ] **Step 8: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 9: Run the full test suite**

Run: `bun test`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/gateway/index.ts
git commit -m "SIO-815: wire HealthMonitor, DrainMetrics, heartbeat into gateway lifecycle"
```

---

## Section G — Docs

### Task G1: Update `.env.example`

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Append the new env vars**

Open `.env.example`. After the last existing block (the outbox block ending with `#OUTBOX_BACKLOG_WARN=50000`), and BEFORE `ENVIRONMENT=dev`, add:

```bash

# --- Health probes + heartbeat ---
# Background dependency monitor that drives /healthz. /healthz reads the
# cached snapshot; probes never run inline on the request path.
#   HEALTH_PROBE_INTERVAL_MS   How often the monitor refreshes (default 30000)
#   HEALTH_PROBE_TIMEOUT_MS    Per-probe timeout (default 5000)
#   STATS_HEARTBEAT_MS         Periodic stats log cadence; 0 disables (default 60000)
#HEALTH_PROBE_INTERVAL_MS=30000
#HEALTH_PROBE_TIMEOUT_MS=5000
#STATS_HEARTBEAT_MS=60000
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "SIO-815: document HEALTH_* and STATS_HEARTBEAT_MS in .env.example"
```

### Task G2: Update `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add `health/` to the architecture tree**

Open `CLAUDE.md`. Locate the architecture tree under `## Architecture` (the `src/` block). Add `health/` block between `gateway/` and `kafka/`, formatted to match the existing style:

```
  health/                 background dependency monitor for /healthz
    types.ts              HealthSnapshot, DependencyStatus, DependencyName
    probes.ts             probeOutboxDb, probeKafkaAdmin (with timeout)
    monitor.ts            createHealthMonitor — cached snapshot + state-transition logging
    admin.ts              createHealthAdmin — long-lived @platformatic/kafka Admin client
```

And add to the `logging/` block (right after the existing `index.ts` line):

```
    heartbeat.ts          startHeartbeat — periodic stats log
```

And add to the `outbox/` block (after `drainer.ts`):

```
    metrics.ts            DrainMetrics — in-memory throughput + lastError
```

- [ ] **Step 2: Document the config block**

Under `## Config shape (4-pillar)`, add this line after the `config.outbox.{…}` line:

```
config.health.{probeIntervalMs, probeTimeoutMs, heartbeatMs}
```

- [ ] **Step 3: Update the `/healthz` description**

Locate the row in the "Single-process model" table that documents `POST /webhooks/elastic/autoops`. Update the same row's description of `/healthz` to:

> `/healthz` reads a cached `HealthMonitor` snapshot. Returns 200 when the producer, outbox DB, and Kafka broker are all healthy; 200 + `status: "degraded"` when only configured topics are missing (gateway keeps buffering); 503 when any required dependency fails. Response includes `dependencies.{kafkaProducer, outboxDb, kafkaBroker, topics}`, `outbox.{pendingByTopic, publishedLast60s, lastPublishedAt, lastError}` plus existing fields.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "SIO-815: document health module, config.health.*, and /healthz contract"
```

---

## Section H — Verification

### Task H1: End-to-end manual verification

**Files:** none (manual)

- [ ] **Step 1: Typecheck and test**

Run: `bun run typecheck && bun test`
Expected: clean. Note the test count is higher than baseline.

- [ ] **Step 2: Start Redpanda**

Run: `docker compose up -d`
Expected: redpanda + console come up cleanly.

- [ ] **Step 3: Start the gateway**

Run: `bun run dev:gateway` (in a separate terminal so logs are visible)
Expected: "gateway listening" log carries the new fields (`provider`, `health.probeIntervalMs`, `dependencies`, `heartbeatMs`).

- [ ] **Step 4: Healthy baseline**

Run: `curl -sS http://localhost:3000/healthz | jq`
Expected: HTTP 200, `status: "healthy"`, all four dependencies `ok: true`, `outbox.pendingByTopic` is `{}`, `publishedLast60s: 0`, `lastPublishedAt: null`.

- [ ] **Step 5: Topic-missing → degraded (still 200)**

Run:
```bash
docker compose exec redpanda rpk topic delete T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
sleep 35
curl -sS -o /tmp/h.json -w "%{http_code}\n" http://localhost:3000/healthz
jq '.status, .dependencies.topics' /tmp/h.json
```
Expected: `200`, `"degraded"`, `dependencies.topics.missing` lists the topic. Gateway logs include one `"dependency state changed"` info line with `dependency: "topics"`.

- [ ] **Step 6: Recover**

Run:
```bash
docker compose exec redpanda rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
sleep 35
curl -sS http://localhost:3000/healthz | jq '.status'
```
Expected: `"healthy"`. Logs include a second `"dependency state changed"` line.

- [ ] **Step 7: Broker outage → 503**

Run:
```bash
docker compose stop redpanda
sleep 35
curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:3000/healthz
```
Expected: `503`.

- [ ] **Step 8: Drainer error log surfaces broker reject text**

Run:
```bash
docker compose start redpanda
docker compose exec redpanda rpk topic delete T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
curl -X POST -H 'content-type: application/json' -d '{"resourceId":"x"}' http://localhost:3000/webhooks/elastic/autoops
sleep 5
```
Expected: gateway logs include a warn line under `component: "outbox.drainer"` with the broker error text verbatim (e.g., `UNKNOWN_TOPIC_OR_PARTITION` or similar). Eventually the row gets retried by the drainer until either the topic is recreated or maxAge fires.

- [ ] **Step 9: Heartbeat**

Run: `tail -f` on the gateway logs for ~70 s.
Expected: one info line per minute with `component: "gateway.heartbeat"`, `message: "heartbeat"`, carrying `pending`, `failed`, `pendingByTopic`, `publishedLast60s`, `lastPublishedAt`, `producerConnected`, `brokerOk`, `status`.

- [ ] **Step 10: Tear down**

Run:
- Stop the gateway (Ctrl+C in its terminal).
- `docker compose exec redpanda rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` (so the cluster is in a clean state for the next run).
- `docker compose down`.

### Task H2: Push the branch and open the PR

**Files:** none (git only)

- [ ] **Step 1: Confirm clean tree**

Run: `git status`
Expected: clean.

- [ ] **Step 2: Push the branch**

Run: `git push -u origin sio-815-healthz-observability`
Expected: branch created on origin.

- [ ] **Step 3: Open the PR**

Run:
```bash
gh pr create --title "SIO-815: healthz dependency probes + outbox observability" --body "$(cat <<'EOF'
## Summary
- `/healthz` answers "is every dependency healthy?" in one cached call (producer, outbox DB, Kafka broker, configured topics)
- New drain metrics: throughput (last 60 s), `lastPublishedAt`, `lastError`, `pendingByTopic`
- Lifecycle logs: state transitions only, drainer publish errors verbatim, 60 s heartbeat, richer startup summary

Spec: `docs/superpowers/specs/2026-05-20-healthz-observability-design.md`
Plan: `docs/superpowers/plans/2026-05-20-healthz-observability.md`
Linear: SIO-815

## Test plan
- [x] `bun run typecheck`
- [x] `bun test`
- [x] Manual: healthy → topic-missing (degraded, 200) → recover → broker outage (503) → drainer error log → heartbeat (per H1 in the plan)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
Expected: PR URL returned.

- [ ] **Step 4: Transition the Linear issue to "In Review"**

Update SIO-815 to `In Review` via the Linear MCP. Per `feedback_linear_auto_done.md`: attaching a PR URL via `save_issue links` can flip the issue to Done — so update the status FIRST (or set status explicitly in the same call), and warn the user if Linear auto-transitions to Done. Issue stays assigned to Simon Owusu.

- [ ] **Step 5: Hand off to user for review**

Return the PR URL.

---

## Notes for the implementer

- All TS imports include the `.ts` extension (project convention; `tsconfig.allowImportingTsExtensions` is on).
- All new components use `getLogger("…")` — never `console.*`. Errors as `{ err }` (Pino convention).
- `bun:test` is the test runner. Tests live under `test/unit/`. `LOG_LEVEL=silent` is preloaded.
- The `Admin` client from `@platformatic/kafka` is real and confirmed at `node_modules/@platformatic/kafka/dist/clients/admin/admin.d.ts:9–13`. If the `Admin` constructor signature in your installed version differs, mirror the `Producer` construction pattern in `src/kafka/producer.ts:21-31` — the field set is the same.
- DRY: don't extract test helpers across files yet — duplicate the `fakeMonitor` / `fakeMetrics` builders into any test file that needs them. We can de-duplicate in a future tidy-up once the shape settles.
- YAGNI: no Prometheus exporter, no `/metrics` endpoint, no per-route latency. The spec excludes them.
- TDD: every behavior change has a failing test BEFORE the implementation. Steps that lack a test (e.g., docs) skip the test step.
