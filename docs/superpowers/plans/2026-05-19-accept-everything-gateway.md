# Accept-Everything Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the gateway a dumb HTTP-to-Kafka bridge: any valid JSON POST is enqueued to `raw.v1` with 202; no Zod validation, no normalization, no synthetic-Validate fast-path, no `events.v1`/`dlq.v1` publish.

**Architecture:** Strip the gateway down to one path: parse JSON → opportunistically compute an AutoOps-aware idempotency-key hash → enqueue a single row to the outbox keyed to `raw.v1` → 202. Delete `src/normalize.ts` and `src/gateway/schema.ts`. Narrow `OutboxTopic` to `"raw"`. Delete `publishNormalized` and `publishDlq` from the producer. Update docs to match.

**Tech Stack:** Bun 1.3+, TypeScript strict, Zod v4 (only outbox schemas remain), `@platformatic/kafka`, `bun:sqlite`, `bun:test`. Linear: SIO-801.

---

## File structure after this change

```
src/
  gateway/
    index.ts                  modified — drop dead imports
    routes.ts                 rewritten (~30 lines)
    idempotencyKey.ts         NEW — opportunistic sha256 helper
  outbox/
    writer.ts                 modified — enqueue(row) replaces enqueuePair
    schemas.ts                modified — OutboxTopic narrowed to "raw"
    drainer.ts                modified — topicToKafka simplified
    db.ts                     unchanged
    backoff.ts                unchanged
  kafka/
    producer.ts               modified — drop publishNormalized + publishDlq
    providers/                unchanged
  config/                     unchanged
  logging/                    unchanged
  types.ts                    modified — delete NormalizedEvent + friends
  normalize.ts                DELETED
  gateway/schema.ts           DELETED
test/unit/
  idempotencyKey.test.ts      NEW
  gateway.routes.test.ts      NEW (replaces deleted normalize/schema tests)
  outbox.writer.test.ts       modified — pair → single
  outbox.drainer.test.ts      modified — topicToKafka mapping
  normalize.test.ts           DELETED
docs/
  architecture/overview.md    modified — restate failure handling, normalization removed
  architecture/outbox.md      modified — single-row enqueue
README.md                     modified
CLAUDE.md                     modified
```

---

### Task 1: Add the idempotency-key helper with tests (TDD)

**Files:**
- Create: `src/gateway/idempotencyKey.ts`
- Create: `test/unit/idempotencyKey.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `test/unit/idempotencyKey.test.ts`:

```ts
import { describe, expect, it } from "bun:test";
import { autoOpsIdempotencyKey } from "../../src/gateway/idempotencyKey.ts";

describe("autoOpsIdempotencyKey", () => {
  it("returns a stable sha256 hex string for a complete AutoOps body (camelCase)", () => {
    const body = {
      resourceId: "deploy-1",
      title: "JVM high",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
      endTime: null,
    };
    const k1 = autoOpsIdempotencyKey(body);
    const k2 = autoOpsIdempotencyKey({ ...body });
    expect(k1).toMatch(/^[0-9a-f]{64}$/);
    expect(k1).toBe(k2);
  });

  it("treats hyphenated keys (deployment-id, start-time, end-time) as equivalent", () => {
    const camel = autoOpsIdempotencyKey({
      resourceId: "deploy-1",
      title: "JVM high",
      status: "RESOLVED",
      startTime: "2026-05-19T00:00:00Z",
      endTime: "2026-05-19T00:05:00Z",
    });
    const hyphen = autoOpsIdempotencyKey({
      "deployment-id": "deploy-1",
      title: "JVM high",
      status: "RESOLVED",
      "start-time": "2026-05-19T00:00:00Z",
      "end-time": "2026-05-19T00:05:00Z",
    });
    expect(hyphen).toBe(camel);
  });

  it("returns undefined when resourceId, title, or status is missing", () => {
    expect(autoOpsIdempotencyKey({ title: "x", status: "open" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({ resourceId: "x", status: "open" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({ resourceId: "x", title: "x" })).toBeUndefined();
    expect(autoOpsIdempotencyKey({})).toBeUndefined();
    expect(autoOpsIdempotencyKey(null)).toBeUndefined();
    expect(autoOpsIdempotencyKey("not an object")).toBeUndefined();
    expect(autoOpsIdempotencyKey([1, 2, 3])).toBeUndefined();
  });

  it("returns different hashes for different bodies", () => {
    const a = autoOpsIdempotencyKey({ resourceId: "1", title: "t", status: "open" });
    const b = autoOpsIdempotencyKey({ resourceId: "2", title: "t", status: "open" });
    const c = autoOpsIdempotencyKey({ resourceId: "1", title: "t", status: "close" });
    expect(a).not.toBe(b);
    expect(a).not.toBe(c);
  });
});
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `bun test test/unit/idempotencyKey.test.ts`
Expected: All four tests fail because `src/gateway/idempotencyKey.ts` does not exist.

- [ ] **Step 3: Implement the helper**

Create `src/gateway/idempotencyKey.ts`:

```ts
// src/gateway/idempotencyKey.ts
import { createHash } from "node:crypto";

function pickString(b: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return undefined;
}

// Opportunistic content hash. The gateway is "accept everything" — this helper
// stamps a stable header on bodies that look like AutoOps events, so operators
// can spot repeated content via the Kafka header. Returns undefined for
// payloads we can't recognize; the message still ships with no header attached.
export function autoOpsIdempotencyKey(body: unknown): string | undefined {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return undefined;
  const b = body as Record<string, unknown>;
  const resourceId = pickString(b, "resourceId", "deployment-id");
  const title = pickString(b, "title");
  const status = pickString(b, "status");
  const startTime = pickString(b, "startTime", "start-time");
  const endTime = pickString(b, "endTime", "end-time");
  if (!resourceId || !title || !status) return undefined;
  return createHash("sha256")
    .update([resourceId, title, status, startTime ?? "", endTime ?? ""].join("::"))
    .digest("hex");
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `bun test test/unit/idempotencyKey.test.ts`
Expected: 4 pass / 0 fail.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: no output, exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/gateway/idempotencyKey.ts test/unit/idempotencyKey.test.ts
git commit -m "SIO-801: add autoOpsIdempotencyKey opportunistic hash helper"
```

---

### Task 2: Narrow `OutboxTopic` to `"raw"` and switch outbox writer to `enqueue(row)`

**Files:**
- Modify: `src/outbox/schemas.ts:5`
- Modify: `src/outbox/writer.ts` (full file rewrite)
- Modify: `src/outbox/drainer.ts:topicToKafka`
- Modify: `test/unit/outbox.writer.test.ts` (full file rewrite)
- Modify: `test/unit/outbox.drainer.test.ts` (touch only the topicToKafka coverage)

- [ ] **Step 1: Replace `src/outbox/schemas.ts` enum**

Open `src/outbox/schemas.ts` and change line 5 from:

```ts
  .enum(["raw", "events", "dlq"])
```

to:

```ts
  .enum(["raw"])
```

Leave the surrounding `.describe()` and exports unchanged.

- [ ] **Step 2: Simplify `topicToKafka` in the drainer**

Open `src/outbox/drainer.ts` and replace the `topicToKafka` function (currently a 3-case switch) with:

```ts
function topicToKafka(topic: string, topics: DrainerConfig["topics"]): string {
  // Outbox only supports "raw" today (SIO-801). Other topic families are
  // reserved for future consumers and are written by them, not by this drainer.
  void (topic as OutboxTopic);
  return topics.raw;
}
```

The `void (topic as OutboxTopic)` keeps the parameter referenced so TypeScript doesn't complain about unused args, and forces the function to update if `OutboxTopic` ever widens again.

- [ ] **Step 3: Rewrite the outbox writer to expose `enqueue(row)`**

Replace `src/outbox/writer.ts` with:

```ts
// src/outbox/writer.ts
import { randomUUID } from "node:crypto";
import type { OutboxDatabase } from "./db.ts";
import { outboxTopicSchema, type OutboxTopic } from "./schemas.ts";

export type EnqueueInput = {
  topic: OutboxTopic;
  messageKey: string;
  payload: string;
  headers: Record<string, string> | null;
};

export type BacklogStats = {
  pending: number;
  failed: number;
  oldestPendingAgeMs: number;
};

export type OutboxWriter = {
  enqueue(row: EnqueueInput): void;
  backlogStats(): BacklogStats;
};

export function createWriter(db: OutboxDatabase): OutboxWriter {
  const insertRow = db.query(
    `INSERT INTO outbox
       (id, topic, message_key, payload, headers, status, attempts, next_attempt_at, created_at)
     VALUES
       ($id, $topic, $message_key, $payload, $headers, 'pending', 0, $now, $now)`,
  );

  const enqueueTx = db.transaction((row: EnqueueInput) => {
    const now = Date.now();
    const topic = outboxTopicSchema.parse(row.topic);
    insertRow.run({
      id: randomUUID(),
      topic,
      message_key: row.messageKey,
      payload: row.payload,
      headers: row.headers === null ? null : JSON.stringify(row.headers),
      now,
    });
  });

  const pendingCountStmt = db.query(
    "SELECT COUNT(*) AS c FROM outbox WHERE status = 'pending'",
  );
  const failedCountStmt = db.query(
    "SELECT COUNT(*) AS c FROM outbox WHERE status = 'failed'",
  );
  const oldestPendingStmt = db.query(
    "SELECT MIN(created_at) AS m FROM outbox WHERE status = 'pending'",
  );

  return {
    enqueue(row) {
      enqueueTx(row);
    },
    backlogStats(): BacklogStats {
      const pending = (pendingCountStmt.get() as { c: number }).c;
      const failed = (failedCountStmt.get() as { c: number }).c;
      const oldest = (oldestPendingStmt.get() as { m: number | null }).m;
      const oldestPendingAgeMs = oldest === null ? 0 : Date.now() - oldest;
      return { pending, failed, oldestPendingAgeMs };
    },
  };
}
```

- [ ] **Step 4: Rewrite the outbox writer tests**

Replace `test/unit/outbox.writer.test.ts` with:

```ts
import { beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";
import { createWriter } from "../../src/outbox/writer.ts";

let db: OutboxDatabase;

beforeEach(() => {
  db = openOutbox(":memory:");
});

describe("createWriter.enqueue", () => {
  it("inserts a single pending row with the given fields", () => {
    const writer = createWriter(db);
    writer.enqueue({
      topic: "raw",
      messageKey: "deploy-1",
      payload: JSON.stringify({ a: 1 }),
      headers: { source: "elastic-autoops" },
    });
    const row = db.query("SELECT * FROM outbox").get() as Record<string, unknown>;
    expect(row.topic).toBe("raw");
    expect(row.message_key).toBe("deploy-1");
    expect(row.status).toBe("pending");
    expect(row.attempts).toBe(0);
    expect(row.payload).toBe(JSON.stringify({ a: 1 }));
    expect(row.headers).toBe(JSON.stringify({ source: "elastic-autoops" }));
    closeOutbox(db);
  });

  it("stores headers as null when none provided", () => {
    const writer = createWriter(db);
    writer.enqueue({
      topic: "raw",
      messageKey: "k",
      payload: "{}",
      headers: null,
    });
    const row = db.query("SELECT headers FROM outbox").get() as { headers: string | null };
    expect(row.headers).toBeNull();
    closeOutbox(db);
  });

  it("rejects unsupported topic values at the writer boundary", () => {
    const writer = createWriter(db);
    // @ts-expect-error — runtime guard, type forbids this
    expect(() => writer.enqueue({ topic: "events", messageKey: "k", payload: "{}", headers: null })).toThrow();
    closeOutbox(db);
  });
});

describe("createWriter.backlogStats", () => {
  it("counts pending and failed rows", () => {
    const writer = createWriter(db);
    writer.enqueue({ topic: "raw", messageKey: "a", payload: "{}", headers: null });
    writer.enqueue({ topic: "raw", messageKey: "b", payload: "{}", headers: null });
    db.run("UPDATE outbox SET status='failed' WHERE message_key='b'");
    const stats = writer.backlogStats();
    expect(stats.pending).toBe(1);
    expect(stats.failed).toBe(1);
    expect(stats.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);
    closeOutbox(db);
  });

  it("returns oldestPendingAgeMs=0 when nothing pending", () => {
    const writer = createWriter(db);
    expect(writer.backlogStats()).toEqual({ pending: 0, failed: 0, oldestPendingAgeMs: 0 });
    closeOutbox(db);
  });
});
```

- [ ] **Step 5: Update drainer test fixtures that referenced `"events"` or `"dlq"` topics**

Open `test/unit/outbox.drainer.test.ts`. Find any test fixture that calls `writer.enqueuePair` or `db.query("INSERT ...")` with `topic = "events"` or `topic = "dlq"`. Change them all to `topic = "raw"`. The drainer test should be exercising "row goes to Kafka" — the topic-family logic is now trivial.

Run: `grep -n 'topic.*events\|topic.*dlq\|enqueuePair' test/unit/outbox.drainer.test.ts`

For every occurrence, rewrite that test row's topic to `"raw"`. If any test was specifically about the routing-by-topic logic, simplify it to a one-line check that `topicToKafka` returns `topics.raw` regardless of input.

- [ ] **Step 6: Run all outbox tests and confirm they pass**

Run: `bun test test/unit/outbox.`
Expected: All outbox tests pass.

- [ ] **Step 7: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add src/outbox/schemas.ts src/outbox/writer.ts src/outbox/drainer.ts test/unit/outbox.writer.test.ts test/unit/outbox.drainer.test.ts
git commit -m "SIO-801: narrow OutboxTopic to raw and switch writer to enqueue(row)"
```

---

### Task 3: Add the new gateway-routes test (TDD against rewritten handler)

**Files:**
- Create: `test/unit/gateway.routes.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/gateway.routes.test.ts`:

```ts
import { describe, expect, it } from "bun:test";
import { buildRoutes } from "../../src/gateway/routes.ts";
import type { EventProducer } from "../../src/kafka/producer.ts";
import type { OutboxWriter, EnqueueInput, BacklogStats } from "../../src/outbox/writer.ts";

function fakeProducer(): EventProducer {
  return {
    publishRaw: async () => {},
    sendByTopic: async () => {},
    isConnected: () => true,
    disconnect: async () => {},
  };
}

function fakeOutbox(): { writer: OutboxWriter; rows: EnqueueInput[] } {
  const rows: EnqueueInput[] = [];
  const writer: OutboxWriter = {
    enqueue(row: EnqueueInput) {
      rows.push(row);
    },
    backlogStats(): BacklogStats {
      return { pending: rows.length, failed: 0, oldestPendingAgeMs: 0 };
    },
  };
  return { writer, rows };
}

async function postJson(routes: ReturnType<typeof buildRoutes>, body: string, contentType = "application/json") {
  const route = routes["/webhooks/elastic/autoops"] as { POST: (req: Request) => Promise<Response> };
  return route.POST(
    new Request("http://test/webhooks/elastic/autoops", {
      method: "POST",
      headers: { "content-type": contentType },
      body,
    }),
  );
}

describe("POST /webhooks/elastic/autoops", () => {
  it("returns 400 on non-JSON body", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    const res = await postJson(routes, "not json");
    expect(res.status).toBe(400);
    const json = (await res.json()) as { accepted: boolean; error: string };
    expect(json.accepted).toBe(false);
    expect(json.error).toBe("invalid JSON body");
    expect(rows).toHaveLength(0);
  });

  it("returns 202 and enqueues one row with source header only for non-AutoOps JSON", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    const res = await postJson(routes, JSON.stringify({ hello: "world" }));
    expect(res.status).toBe(202);
    expect(await res.json()).toEqual({ accepted: true });
    expect(rows).toHaveLength(1);
    expect(rows[0].topic).toBe("raw");
    expect(rows[0].messageKey).toBe("unkeyed");
    expect(rows[0].headers).toEqual({ source: "elastic-autoops" });
    const payload = JSON.parse(rows[0].payload) as { receivedAt: string; raw: unknown };
    expect(payload.raw).toEqual({ hello: "world" });
    expect(payload.receivedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("returns 202 and includes the idempotencyKey header for an AutoOps body", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    const body = {
      resourceId: "deploy-1",
      title: "JVM pressure",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
    };
    const res = await postJson(routes, JSON.stringify(body));
    expect(res.status).toBe(202);
    expect(rows).toHaveLength(1);
    expect(rows[0].messageKey).toBe("deploy-1");
    expect(rows[0].headers).toHaveProperty("source", "elastic-autoops");
    expect(rows[0].headers).toHaveProperty("idempotencyKey");
    expect((rows[0].headers as Record<string, string>).idempotencyKey).toMatch(/^[0-9a-f]{64}$/);
  });

  it("uses deployment-id (hyphenated) as message key when resourceId is absent", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    const body = {
      "deployment-id": "deploy-2",
      title: "x",
      status: "open",
    };
    const res = await postJson(routes, JSON.stringify(body));
    expect(res.status).toBe(202);
    expect(rows[0].messageKey).toBe("deploy-2");
  });

  it("falls back to 'unkeyed' when neither resourceId nor deployment-id is present", async () => {
    const { writer, rows } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    await postJson(routes, JSON.stringify({ anything: 1 }));
    expect(rows[0].messageKey).toBe("unkeyed");
  });
});

describe("GET /healthz", () => {
  it("reports producer + outbox stats", () => {
    const { writer } = fakeOutbox();
    const routes = buildRoutes({ producer: fakeProducer(), outbox: writer });
    const res = (routes["/healthz"] as () => Response)();
    expect(res.status).toBe(200);
  });
});
```

- [ ] **Step 2: Run the new test and confirm it fails for the right reason**

Run: `bun test test/unit/gateway.routes.test.ts`
Expected: tests fail (the current `buildRoutes` returns `{ accepted: true, resourceId, idempotencyKey }`, not `{ accepted: true }`, and still applies Zod validation). This is the proof we need that the rewrite is necessary. Do not modify the test to fit the current code.

- [ ] **Step 3: Commit the failing test**

```bash
git add test/unit/gateway.routes.test.ts
git commit -m "SIO-801: add failing tests for accept-everything route handler"
```

(Committing a red test pins the behaviour we're about to implement.)

---

### Task 4: Rewrite `src/gateway/routes.ts` to the accept-everything contract

**Files:**
- Modify: `src/gateway/routes.ts` (full file replacement)

- [ ] **Step 1: Replace the routes module**

Replace `src/gateway/routes.ts` with:

```ts
// src/gateway/routes.ts
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

function pickKey(body: unknown): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of ["resourceId", "deployment-id"]) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}

export function buildRoutes(deps: RouteDeps) {
  const { producer, outbox } = deps;

  return {
    "/healthz": () => {
      const stats = outbox?.backlogStats();
      const producerOk = producer.isConnected();
      return Response.json(
        {
          ok: producerOk,
          producer: { connected: producerOk },
          outbox: stats
            ? {
                enabled: true,
                pending: stats.pending,
                failed: stats.failed,
                oldestPendingAgeMs: stats.oldestPendingAgeMs,
              }
            : { enabled: false },
        },
        { status: producerOk ? 200 : 503 },
      );
    },

    "/webhooks/elastic/autoops": {
      POST: async (req: Request) => {
        let body: unknown;
        try {
          body = await req.json();
        } catch {
          return Response.json(
            { accepted: false, error: "invalid JSON body" },
            { status: 400 },
          );
        }

        const messageKey = pickKey(body);
        const idempotencyKey = autoOpsIdempotencyKey(body);
        const headers: Record<string, string> = { source: "elastic-autoops" };
        if (idempotencyKey) headers.idempotencyKey = idempotencyKey;

        const payload = JSON.stringify({
          receivedAt: new Date().toISOString(),
          raw: body,
        });

        if (outbox) {
          try {
            outbox.enqueue({
              topic: "raw",
              messageKey,
              payload,
              headers,
            });
          } catch (err) {
            log.error({ err, messageKey }, "outbox enqueue failed");
            return Response.json(
              { accepted: false, error: "outbox enqueue failed" },
              { status: 500 },
            );
          }
        } else {
          try {
            await producer.publishRaw(messageKey, body);
          } catch (err) {
            log.warn({ err, messageKey }, "kafka publish failed; will not retry without outbox");
          }
        }

        return Response.json({ accepted: true }, { status: 202 });
      },
    },
  };
}
```

- [ ] **Step 2: Run the gateway-routes test**

Run: `bun test test/unit/gateway.routes.test.ts`
Expected: 6 pass / 0 fail.

- [ ] **Step 3: Typecheck**

Run: `bun run typecheck`
Expected: clean (note: `src/normalize.ts` and `src/gateway/schema.ts` still exist; later tasks delete them. The producer's `publishNormalized`/`publishDlq` are also still there — Task 6 removes them).

- [ ] **Step 4: Commit**

```bash
git add src/gateway/routes.ts
git commit -m "SIO-801: rewrite gateway routes to accept-everything contract"
```

---

### Task 5: Delete `src/normalize.ts`, `src/gateway/schema.ts`, normalize tests

**Files:**
- Delete: `src/normalize.ts`
- Delete: `src/gateway/schema.ts`
- Delete: `test/unit/normalize.test.ts`

- [ ] **Step 1: Delete the files**

Run:

```bash
git rm src/normalize.ts src/gateway/schema.ts test/unit/normalize.test.ts
```

- [ ] **Step 2: Search for stale imports**

Run: `grep -rn "from.*['\"]\(.\\./\)\\?normalize\\|from.*gateway/schema" src/ test/`
Expected: no matches. The only callers were `routes.ts` (now rewritten) and `normalize.test.ts` (now deleted).

If anything matches, remove the import line and any dead reference to the deleted exports.

- [ ] **Step 3: Run all tests + typecheck**

Run: `bun run typecheck && bun test`
Expected: typecheck clean. Tests pass — the count will be lower than before because `normalize.test.ts` is gone, but no test should fail.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "SIO-801: delete src/normalize.ts and src/gateway/schema.ts"
```

---

### Task 6: Delete `publishNormalized` and `publishDlq` from `EventProducer`

**Files:**
- Modify: `src/kafka/producer.ts`

- [ ] **Step 1: Strip the unused methods**

Open `src/kafka/producer.ts`. Remove the `publishNormalized` and `publishDlq` properties from the `EventProducer` type and from the object returned by `createProducer`. Keep `publishRaw`, `sendByTopic` (used by the drainer), `isConnected`, and `disconnect`.

Also remove the `import type { NormalizedEvent } from "../types.ts";` line at the top — it's only used by `publishNormalized`.

After the edit the type should read:

```ts
export type EventProducer = {
  publishRaw(resourceId: string, raw: unknown): Promise<void>;
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: Record<string, string> | null,
  ): Promise<void>;
  isConnected(): boolean;
  disconnect(): Promise<void>;
};
```

And the returned object should only have those four methods.

- [ ] **Step 2: Search for stale callers**

Run: `grep -rn "publishNormalized\|publishDlq" src/ test/`
Expected: no matches in `src/`. If anything in `test/` still references these, it's a stale test — delete it.

- [ ] **Step 3: Run all tests + typecheck**

Run: `bun run typecheck && bun test`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/kafka/producer.ts
git commit -m "SIO-801: drop publishNormalized and publishDlq from producer"
```

---

### Task 7: Delete normalize-related types from `src/types.ts`

**Files:**
- Modify: `src/types.ts`

- [ ] **Step 1: Strip orphan types**

Open `src/types.ts`. Delete these top-level exports:

- `Severity`
- `SeverityRank`
- `EventType`
- `ElasticAutoOpsWebhook`
- `NormalizedAlert`
- `NormalizedEvent`

If `src/types.ts` is empty after the edit, delete the file entirely (`git rm src/types.ts`).

- [ ] **Step 2: Search for stale imports**

Run: `grep -rn "from.*['\"]\(.\\./\)*types\\.ts['\"]" src/ test/`
Expected: only matches inside `src/types.ts` itself (if it still exists) or none.

If you find references in code that's still alive, that's a bug — investigate before deleting.

- [ ] **Step 3: Run all tests + typecheck**

Run: `bun run typecheck && bun test`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "SIO-801: remove orphan NormalizedEvent and friends from types.ts"
```

---

### Task 8: Update documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/outbox.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update `docs/architecture/overview.md`**

Find the "Failure Handling" table. Replace its contents with exactly:

```markdown
| Failure | Where | Action |
|---------|-------|--------|
| Invalid JSON body | gateway (`src/gateway/routes.ts`) | Return `400`, do not enqueue |
| Outbox enqueue fails (SQLite) | gateway | Return `500`; the row is not durable, so the caller learns the truth |
| Kafka publish failure (drainer) | outbox drainer | Bump attempts, exponential backoff, eventually `status='failed'` after `OUTBOX_MAX_AGE_HOURS` |
| Producer disconnected | gateway `/healthz` | Report `producer.connected: false`, return `503` |
```

Find the "Normalization Contract" section heading and delete the entire section (heading + table + the "Hyphenated keys and synthetic test bodies" subsection that follows). The gateway no longer normalizes.

Find the data-flow diagram. Replace the line that mentions `raw.v1` + `events.v1` + `dlq.v1` as gateway outputs with something that makes it clear the gateway writes only `raw.v1`. A minimal replacement:

```
+------------------+      POST        +------------------+   enqueue     +------------------+    publish    +-------------+
|  Elastic AutoOps | ---------------> |  gateway         | -----------> |  outbox (SQLite) | ----------> | Kafka raw.v1|
|  (webhook)       | <---  202 ack -- |  Bun.serve :3000 |              |  ./data/outbox.db|             +-------------+
+------------------+                  +------------------+              +------------------+
```

Add a new "Reserved topics" subsection below the diagram:

```markdown
### Reserved topics

`ops.elastic.autoops.events.v1` and `ops.elastic.autoops.dlq.v1` stay provisioned but are not written by the gateway. They are reserved for future consumer services that may decide to publish normalized events or quarantine bad messages. The gateway has no opinion about validity — that is a consumer concern.
```

- [ ] **Step 2: Update `docs/architecture/outbox.md`**

Find every reference to `enqueuePair` or "raw + normalized rows" or "two events" and rewrite to reflect single-row enqueue.

Most importantly, in the "Flow" section, replace `outbox.enqueuePair(rawRow, normalizedRow)   ── single SQLite transaction` with:

```
outbox.enqueue(rawRow)                          ── single SQLite transaction
```

Find the file-layout block and update the `writer.ts` line to:

```
  writer.ts      createWriter(db) → { enqueue, backlogStats }
```

- [ ] **Step 3: Update `README.md`**

Find the high-level description of what eventgate does. It currently says something like "receives webhooks, normalizes them, publishes to Kafka." Rewrite the relevant paragraph to:

```
eventgate is a single-process Bun service that ingests Elastic AutoOps webhook
notifications and durably persists them to Kafka. The gateway accepts any valid
JSON POST and writes it verbatim to `raw.v1` via a local SQLite outbox.
Validation, normalization, alerting, and projection are concerns for downstream
consumers in other services.
```

If there's a "Topics" or "Architecture" block in the README, make sure it says only `raw.v1` is produced by this service.

- [ ] **Step 4: Update `CLAUDE.md`**

Find any reference to `normalize.ts`, `src/gateway/schema.ts`, `NormalizedEvent`, Zod schemas for AutoOps bodies, or the synthetic-Validate detector. Delete those references.

Add (or update) the "Contract" section to read:

```markdown
## Contract

Gateway accepts any valid JSON POST to `/webhooks/elastic/autoops` and writes
it to `ops.elastic.autoops.raw.v1` via the SQLite outbox. Non-JSON bodies get
400; everything else gets 202. The gateway does not validate AutoOps schema
shape, does not normalize, does not write `events.v1` or `dlq.v1`. Downstream
consumers in other services own those concerns.
```

- [ ] **Step 5: Commit**

```bash
git add README.md docs/architecture/overview.md docs/architecture/outbox.md CLAUDE.md
git commit -m "SIO-801: docs reflect accept-everything contract"
```

---

### Task 9: Final verification (typecheck + test + boot)

**Files:** none.

- [ ] **Step 1: Typecheck**

Run: `bun run typecheck`
Expected: no output, exit 0.

- [ ] **Step 2: Run the full test suite**

Run: `bun test`
Expected: All tests pass. Verify by reading the summary line — it should report at least the following test files:

- `config.kafka-provider.test.ts`
- `config.outbox.test.ts`
- `gateway.routes.test.ts` (new)
- `idempotencyKey.test.ts` (new)
- `kafka.providers.factory.test.ts`
- `kafka.providers.msk.test.ts`
- `outbox.backoff.test.ts`
- `outbox.drainer.test.ts`
- `outbox.writer.test.ts`

`normalize.test.ts` should be gone.

- [ ] **Step 3: Smoke-test the gateway locally**

Start docker-compose Redpanda + the gateway in two terminals, then send the three smoke-test bodies from the spec.

Terminal A:

```bash
docker compose up -d redpanda
bun run start:gateway
```

Terminal B:

```bash
# valid AutoOps body — expect 202 with idempotencyKey header on raw.v1
curl -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{"resourceId":"x","title":"t","status":"open"}' -w "\nHTTP %{http_code}\n"

# non-AutoOps JSON — expect 202, header has only source
curl -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d '{"hello":"world"}' -w "\nHTTP %{http_code}\n"

# non-JSON — expect 400
curl -X POST http://localhost:3000/webhooks/elastic/autoops \
  -H 'Content-Type: application/json' \
  -d 'not json' -w "\nHTTP %{http_code}\n"

# inspect Redpanda
docker compose exec redpanda rpk topic consume ops.elastic.autoops.raw.v1 --num 3 --offset end-3
```

Expected:
- First POST: 202, message lands with `source` and `idempotencyKey` (sha256 hex).
- Second POST: 202, message lands with only `source`.
- Third POST: 400, no message on `raw.v1`.

If the smoke test passes, the spec acceptance criteria 1–4 are satisfied. Criteria 5 (Validate body) and 6 (real AutoOps event) are exercised post-deploy.

- [ ] **Step 4: Commit (no-op if nothing changed)**

Should be no uncommitted changes at this point. If there are stragglers from doc edits or tests, commit them now:

```bash
git status
# if anything pending:
git add -A
git commit -m "SIO-801: final cleanup"
```

---

### Task 10: Push branch, open PR

**Files:** none.

- [ ] **Step 1: Push the branch**

Run: `git push -u origin SIO-801-gateway-accept-everything`

- [ ] **Step 2: Open the PR**

Use `gh pr create` (the user has authorized this in earlier sessions). PR title: `SIO-801: gateway accept-everything (delete validation, single-row outbox)`. PR body should:

- Summarize the change in 3-5 bullets.
- Link to [SIO-801](https://linear.app/siobytes/issue/SIO-801/gateway-accept-everything-delete-validation-single-row-outbox-to-rawv1).
- Link to the design doc at `docs/superpowers/specs/2026-05-19-accept-everything-gateway-design.md`.
- Include a Test plan with the acceptance criteria from the design doc.
- Use a HEREDOC for the body to preserve newlines.

- [ ] **Step 3: Update Linear to "In Review"**

Use the Linear MCP `save_issue` tool: `{ id: "SIO-801", state: "In Review" }`.

---

## Spec → task coverage

| Spec section | Implementing task |
|---|---|
| Final gateway contract (POST behaviour) | Task 4 |
| Idempotency key helper | Task 1 |
| Kafka key + headers on raw.v1 | Task 4 |
| Delete `src/normalize.ts`, `src/gateway/schema.ts`, tests | Task 5 |
| Delete normalize types from `src/types.ts` | Task 7 |
| `publishNormalized` + `publishDlq` removed | Task 6 |
| `enqueuePair` → `enqueue` | Task 2 |
| `OutboxTopic` narrowed to `"raw"` | Task 2 |
| `topicToKafka` simplified | Task 2 |
| `config.kafka.topics` unchanged | (no task needed) |
| Documentation updates | Task 8 |
| Verification (criteria 1-4) | Task 9 |
| Verification criteria 5 (Validate) + 6 (real AutoOps) | Post-deploy, separate from this PR |

## Out of scope (deliberate)

- Building a normalizer service
- Schema registry
- Topic retention automation
- New Linear ticket spawning

If verification criterion 5 (synthetic Validate body) or 6 (real AutoOps event) reveal a regression post-deploy, open a follow-up ticket — do not extend this plan.
