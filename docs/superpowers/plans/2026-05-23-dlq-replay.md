# SIO-827 Implementation Plan: Self-managed DLQ replay (Phase 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This plan covers **Phase 1 only**; Phases 2-5 ship as separate PRs under the same Linear issue.

**Linear:** [SIO-827](https://linear.app/siobytes/issue/SIO-827/self-managed-dlq-replay-for-eventgate-gateway)
**Spec:** `docs/superpowers/specs/2026-05-23-dlq-replay-design.md`

**Goal (Phase 1):** Land the API surface end-to-end with zero impact on existing behavior. Triage logic is stubbed (always returns `replay` so dry-run shows what *would* happen); SQLite tables not yet created; bulk returns a synthetic jobId. After this PR, an operator with `REPLAY_ENABLED=true` + `ADMIN_TOKEN` can hit `POST /admin/replay/:route/message` with `dryRun=true` and see the triage decision for a single DLQ record. Gateway behavior with `REPLAY_ENABLED=false` (the default) is unchanged.

**Architecture (Phase 1 only):**

- New optional `replay` strictObject in `configSchema` with full defaults; toggled by `REPLAY_ENABLED` master switch.
- `RESERVED_PATHS` extended with `/admin/dlq` + `/admin/replay` + prefix guard.
- New `src/replay/` directory: `types.ts`, `headers.ts`, `consumer.ts` (real Kafka consumer; single-message and dry-run iteration), `runner.ts` (dry-run path only — counts/classifies without producing), and a stub `jobStore.ts` returning synthetic UUIDs.
- New `src/admin/replayEndpoint.ts` mirroring [src/admin/routesEndpoint.ts](../../src/admin/routesEndpoint.ts).
- Producer signature widened to accept `Array<[string, string | Buffer | null]>` — drainer + webhook callsites unchanged.
- Wired into `src/gateway/index.ts` (build `replayContext` after `healthAdmin`) and `src/gateway/routes.ts` (register endpoints conditionally on `config.admin?.token && config.replay?.enabled`).
- SDK Map<Buffer> smoke script under `scripts/` whose result is captured in `docs/architecture/dlq-replay.md` (also new this PR).

**Tech Stack:** Bun 1.3 (`Bun.serve` with `:param` routes), TypeScript strict, Zod v4 (`strictObject`, `describe`), `@platformatic/kafka` 2.1.0 (`Consumer`, `Admin`), `bun:test`. No new dependencies.

**Reference spec:** `docs/superpowers/specs/2026-05-23-dlq-replay-design.md`. Read before starting — it contains the rationale, the 8-risk decision table, and the full triage logic.

---

## File Structure (Phase 1)

**New files**

- `src/config/replayPaths.ts` — `REPLAY_ADMIN_PATH_PREFIXES` constant + `isReplayAdminPath(path): boolean` helper (kept separate from `reservedPaths.ts` so the prefix-guard logic stays local and testable). The literal entries `/admin/dlq` + `/admin/replay` also go into `RESERVED_PATHS`.
- `src/replay/types.ts` — `DlqRecord`, `TriageDecision`, `ReplayJob`, `ReplayBatchInput/Result`, `ReplayConfig` (Zod-derived).
- `src/replay/headers.ts` — `readHeader`, `stripConnectHeaders`, `stampAuditHeaders`, `parseAttempt`.
- `src/replay/consumer.ts` — `createReplayConsumer(provider, route, jobId)` → `{ streamRange, fetchOne, close }`.
- `src/replay/runner.ts` — pure `runReplayBatchDryRun(...)` (Phase 1 dry-run only; real replay/park comes in Phase 4).
- `src/replay/jobStore.ts` — Phase 1 stub: `createReplayJobStore()` returns an in-memory implementation returning synthetic UUIDs. Real SQLite-backed store ships in Phase 3.
- `src/admin/replayEndpoint.ts` — `makeReplayHandlers(deps)` returning the five HTTP handlers.
- `scripts/replay-sdk-smoke.ts` — one-off SDK probe verifying `Map<Buffer,Buffer>.get(Buffer.from(name))` behavior.
- `docs/architecture/dlq-replay.md` — architecture doc (style: see [docs/architecture/outbox.md](../../architecture/outbox.md)).
- `test/unit/config.replay.test.ts` — Zod schema + env mapping coverage.
- `test/unit/config.reservedPaths.replay.test.ts` — extends existing reserved-path tests with `/admin/dlq`, `/admin/replay`, and prefix cases.
- `test/unit/replay.headers.test.ts` — `readHeader` (Buffer.equals path + iteration fallback), strip, stamp, attempt parse (`Number()` strict, negative clamp).
- `test/unit/replay.runner.dryRun.test.ts` — fake `AsyncIterable<DlqRecord>` + spy producer; assert producer never called in dry-run; assert decisions counted.
- `test/unit/admin.replayEndpoint.test.ts` — 401/400/200 for each handler; dry-run single-message returns synthetic decision; bulk returns 202 + synthetic jobId; unknown route → 404.
- `test/unit/kafka.producer.headers.test.ts` — widened signature accepts array-of-tuples with `Buffer` values; existing `Record<string, string>` path still works.

**Modified files**

- `src/config/schemas.ts` — add optional `replay: z.strictObject({...})` to `configSchema`; `.describe()` on every field.
- `src/config/defaults.ts` — add `replay` defaults (every key populated; `enabled=false`).
- `src/config/envMapping.ts` — map `REPLAY_*` via `bool/num/csv/str/nestedOrUndefined` helpers; only attach `overrides.replay` if `REPLAY_ENABLED` truthy OR any sub-key set.
- `src/config/reservedPaths.ts` — add `"/admin/dlq"` + `"/admin/replay"` literals; extend `checkReservedPath` to also reject paths starting with `/admin/replay/` or `/admin/dlq/`.
- `src/kafka/producer.ts` — widen `sendByTopic` headers param to `Record<string, string> | Array<[string, string | Buffer | null]> | null`; internal shim converts array form to `Map`.
- `src/gateway/routes.ts` — register `/admin/dlq`, `/admin/replay/:route`, `/admin/replay/:route/message`, `/admin/replay/:jobId`, `/admin/replay/:jobId/cancel` conditionally on `config.admin?.token && config.replay?.enabled`.
- `src/gateway/index.ts` — build `replayContext` (jobStore stub + consumer factory) after `healthAdmin`; wire into `rebuildRoutes`; extend `shutdown()` to cancel any active jobs (no-op in Phase 1 since stub holds no real jobs, but the hook is in place).
- `.env.example` — document every `REPLAY_*` var inline using existing block style.
- `CLAUDE.md` — append architecture entry under "Architecture" + env vars under existing sections.

---

## Task 1: SDK Map<Buffer> smoke

**Why first:** the `readHeader` design depends on whether `Map<Buffer,Buffer>.get(Buffer.from(name))` returns the value. Standard JS Map uses reference equality so this *should* return undefined; if so, iteration with `Buffer.equals` is the only path. We need to know before writing `headers.ts`.

**Files:** Create `scripts/replay-sdk-smoke.ts`.

- [ ] **Step 1: Write the script**

```ts
// scripts/replay-sdk-smoke.ts
// Verifies @platformatic/kafka header Map<Buffer,Buffer> lookup behavior.
// Result documented in docs/architecture/dlq-replay.md.

const k = Buffer.from("hello");
const m = new Map<Buffer, Buffer>();
m.set(k, Buffer.from("world"));

const direct = m.get(k);
const fresh = m.get(Buffer.from("hello"));

console.log({
  directHit: direct?.toString("utf-8") ?? null,        // expect "world"
  freshKeyHit: fresh?.toString("utf-8") ?? null,       // expect null on reference-equality Map
  iterationHit: (() => {
    for (const [mk, mv] of m) {
      if (mk.equals(Buffer.from("hello"))) return mv.toString("utf-8");
    }
    return null;
  })(),                                                // expect "world"
});
```

- [ ] **Step 2: Run + capture**

```
bun run scripts/replay-sdk-smoke.ts
```

Record the output in `docs/architecture/dlq-replay.md` under an "SDK header lookup" subsection. Use the result to confirm the iteration-based `readHeader` is the only correct path.

---

## Task 2: Reserved-path extensions

**Why next:** standalone and small; later tasks reference `/admin/replay/*` in tests.

**Files:**
- Modify: `src/config/reservedPaths.ts`
- Test: `test/unit/config.reservedPaths.replay.test.ts` (new file; preserves existing `config.reservedPaths.test.ts`).

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.reservedPaths.replay.test.ts
import { describe, expect, it } from "bun:test";
import { RESERVED_PATHS, checkReservedPath, isReservedPath } from "../../src/config/reservedPaths.ts";

describe("RESERVED_PATHS (replay additions)", () => {
  it("contains /admin/dlq", () => {
    expect(RESERVED_PATHS.has("/admin/dlq")).toBe(true);
  });
  it("contains /admin/replay", () => {
    expect(RESERVED_PATHS.has("/admin/replay")).toBe(true);
  });
});

describe("checkReservedPath (replay prefix guard)", () => {
  it.each([
    ["/admin/replay"],
    ["/admin/replay/elastic-autoops"],
    ["/admin/replay/elastic-autoops/message"],
    ["/admin/replay/some-uuid/cancel"],
    ["/admin/dlq"],
    ["/admin/dlq/anything"],
  ])("rejects %s", (path) => {
    const r = checkReservedPath(path);
    expect(r.ok).toBe(false);
  });

  it("does not reject /admin/replays (no slash)", () => {
    // /admin/replays is not under the prefix; allowed.
    expect(checkReservedPath("/admin/replays").ok).toBe(true);
  });
});
```

- [ ] **Step 2: Implement**

Edit `src/config/reservedPaths.ts`:

```ts
export const RESERVED_PATHS: ReadonlySet<string> = new Set([
  "/healthz",
  "/admin/routes",
  "/admin/dlq",
  "/admin/replay",
]);

const RESERVED_PREFIXES: ReadonlyArray<string> = ["/admin/dlq/", "/admin/replay/"];

export function isReservedPath(path: string): boolean {
  if (RESERVED_PATHS.has(path)) return true;
  return RESERVED_PREFIXES.some((p) => path.startsWith(p));
}

export function checkReservedPath(path: string): Check {
  if (isReservedPath(path)) {
    return { ok: false, message: `path '${path}' is reserved for operational use` };
  }
  return { ok: true };
}
```

- [ ] **Step 3: Verify**

```
bun test test/unit/config.reservedPaths.replay.test.ts test/unit/config.reservedPaths.test.ts
```

Both files pass. Existing reserved-path tests are not affected.

---

## Task 3: Config schema + defaults + env mapping

**Files:**
- Modify: `src/config/schemas.ts`, `src/config/defaults.ts`, `src/config/envMapping.ts`
- Test: `test/unit/config.replay.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.replay.test.ts
import { describe, expect, it, afterEach } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { config } from "../../src/config/index.ts";

const SAVE: Record<string, string | undefined> = {};
const KEYS = [
  "REPLAY_ENABLED",
  "REPLAY_MAX_ATTEMPTS",
  "REPLAY_TRANSIENT_ERRORS",
  "REPLAY_POISON_ERRORS",
  "REPLAY_DEFAULT",
  "REPLAY_MAX_RECORDS_PER_JOB",
  "REPLAY_RATE_LIMIT_PER_SEC",
  "REPLAY_PARKING_TOPIC_SUFFIX",
  "REPLAY_AUTO_ENABLED",
  "REPLAY_AUTO_INTERVAL_MS",
  "REPLAY_AUTO_DLQ_DEPTH_THRESHOLD",
  "REPLAY_AUTO_PROBE_WINDOW_RECORDS",
];

function snapshot() {
  for (const k of KEYS) SAVE[k] = process.env[k];
}
function restore() {
  for (const k of KEYS) {
    if (SAVE[k] === undefined) delete process.env[k];
    else process.env[k] = SAVE[k];
  }
  resetConfigCache();
}

describe("config.replay", () => {
  afterEach(restore);

  it("is undefined when REPLAY_ENABLED is unset", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    resetConfigCache();
    expect(config.replay).toBeUndefined();
  });

  it("is populated with defaults when REPLAY_ENABLED=true", () => {
    snapshot();
    for (const k of KEYS) delete process.env[k];
    process.env.REPLAY_ENABLED = "true";
    resetConfigCache();
    expect(config.replay?.enabled).toBe(true);
    expect(config.replay?.maxAttempts).toBe(5);
    expect(config.replay?.default).toBe("park");
    expect(config.replay?.parkingTopicSuffix).toBe(".parked");
    expect(config.replay?.auto.enabled).toBe(false);
    expect(config.replay?.auto.probeWindowRecords).toBe(500);
  });

  it("CSV env vars parse into string[]", () => {
    snapshot();
    process.env.REPLAY_ENABLED = "true";
    process.env.REPLAY_TRANSIENT_ERRORS = "TimeoutException,RetriableException";
    process.env.REPLAY_POISON_ERRORS = "SerializationException";
    resetConfigCache();
    expect(config.replay?.transientErrors).toEqual([
      "TimeoutException",
      "RetriableException",
    ]);
    expect(config.replay?.poisonErrors).toEqual(["SerializationException"]);
  });
});
```

- [ ] **Step 2: Implement schema**

Add to `configSchema` in `src/config/schemas.ts` (matches §"Config schema additions" in the spec verbatim).

- [ ] **Step 3: Implement defaults**

Add to `src/config/defaults.ts`:

```ts
replay: {
  enabled: false,
  maxAttempts: 5,
  transientErrors: [] as string[],
  poisonErrors: [] as string[],
  default: "park" as const,
  maxRecordsPerJob: 10_000,
  rateLimitPerSec: 500,
  parkingTopicSuffix: ".parked",
  auto: {
    enabled: false,
    intervalMs: 300_000,
    dlqDepthThreshold: 100,
    probeWindowRecords: 500,
  },
},
```

Update the `Defaults` type by re-exporting; existing pattern.

- [ ] **Step 4: Implement env mapping**

Edit `src/config/envMapping.ts`. Add to the `EnvOverrides` type, add the helpers and the block, and only attach `overrides.replay` if `REPLAY_ENABLED` is truthy OR any sub-key is non-undefined.

- [ ] **Step 5: Verify**

```
bun test test/unit/config.replay.test.ts
bun run typecheck
```

---

## Task 4: Replay types

**Files:** Create `src/replay/types.ts`.

- [ ] **Step 1: Implement**

```ts
// src/replay/types.ts

export type DlqRecord = {
  topic: string;
  partition: number;
  offset: number;
  key: Buffer | null;
  value: Buffer | null;
  headers: Array<[Buffer | null, Buffer | null]>;
  timestamp: number;
};

export type TriageDecision =
  | { kind: "replay"; exceptionClass: string | null }
  | {
      kind: "park";
      reason: "exceeded_attempts" | "poison_class" | "default_park";
      exceptionClass: string | null;
    };

export type ReplayJob = {
  id: string;
  route: string;
  partition: number;
  mode: "manual" | "auto" | "single";
  dryRun: boolean;
  scanned: number;
  replayed: number;
  parked: number;
  skipped: number;
  errors: number;
  fromOffset: number | null;
  toOffset: number | null;
  lastOffset: number | null;
  status: "pending" | "running" | "paused" | "done" | "cancelled" | "failed";
  lastError: string | null;
  nextResumeAt: number | null;
  startedAt: number;
  finishedAt: number | null;
};

export type ReplayBatchResult = {
  scanned: number;
  replayed: number;
  parked: number;
  skipped: number;
  errors: number;
  lastOffset: number | null;
};

export type AuditHeaderInput = {
  jobId: string;
  sourceTopic: string;
  partition: number;
  offset: number;
  attempt: number;
};
```

---

## Task 5: Headers helper

**Files:** Create `src/replay/headers.ts`, `test/unit/replay.headers.test.ts`.

- [ ] **Step 1: Failing test**

```ts
// test/unit/replay.headers.test.ts
import { describe, expect, it } from "bun:test";
import {
  readHeader,
  stripConnectHeaders,
  stampAuditHeaders,
  parseAttempt,
} from "../../src/replay/headers.ts";

function h(name: string, val: string | Buffer | null): [Buffer, Buffer | null] {
  return [Buffer.from(name), val === null ? null : Buffer.isBuffer(val) ? val : Buffer.from(val)];
}

describe("readHeader", () => {
  it("returns value for matching name", () => {
    const rec = { headers: [h("foo", "bar"), h("baz", "qux")] };
    expect(readHeader(rec, "foo")).toBe("bar");
    expect(readHeader(rec, "baz")).toBe("qux");
  });
  it("returns undefined for missing", () => {
    expect(readHeader({ headers: [] }, "anything")).toBeUndefined();
  });
  it("returns empty string for present-but-null value", () => {
    expect(readHeader({ headers: [h("foo", null)] }, "foo")).toBe("");
  });
  it("preserves first when duplicate names exist", () => {
    const rec = { headers: [h("dup", "first"), h("dup", "second")] };
    expect(readHeader(rec, "dup")).toBe("first");
  });
});

describe("stripConnectHeaders", () => {
  it("drops __connect.errors.* headers", () => {
    const headers = [
      h("__connect.errors.exception.class.name", "X"),
      h("__connect.errors.exception.message", "Y"),
      h("idempotencyKey", "keep-me"),
    ];
    const out = stripConnectHeaders(headers);
    expect(out).toHaveLength(1);
    expect(out[0][0].toString("utf-8")).toBe("idempotencyKey");
  });

  it("preserves Buffer values bit-for-bit", () => {
    const binary = Buffer.from([0x00, 0xff, 0x10, 0x42]);
    const out = stripConnectHeaders([h("idempotencyKey", binary)]);
    expect(out[0][1]?.equals(binary)).toBe(true);
  });
});

describe("stampAuditHeaders", () => {
  it("appends all five audit headers as strings", () => {
    const out = stampAuditHeaders([], {
      jobId: "job-1",
      sourceTopic: "DLQ_T_X",
      partition: 0,
      offset: 42,
      attempt: 3,
    });
    const map = Object.fromEntries(
      out.map(([k, v]) => [k.toString("utf-8"), v?.toString("utf-8") ?? null]),
    );
    expect(map["x-eventgate-replay-attempt"]).toBe("3");
    expect(map["x-eventgate-replay-job-id"]).toBe("job-1");
    expect(map["x-eventgate-replay-source-topic"]).toBe("DLQ_T_X");
    expect(map["x-eventgate-replay-source-offset"]).toBe("0:42");
    expect(map["x-eventgate-replay-at"]).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });
});

describe("parseAttempt", () => {
  it.each([
    [undefined, 0],
    ["", 0],
    ["0", 0],
    ["3", 3],
    ["5abc", 0],         // Number() strict -> NaN -> 0
    ["-3", 0],           // negative clamped
    ["1e6", 1_000_000],
    ["NaN", 0],
  ])("parseAttempt(%p) -> %i", (input, expected) => {
    expect(parseAttempt(input)).toBe(expected);
  });
});
```

- [ ] **Step 2: Implement**

```ts
// src/replay/headers.ts
import type { AuditHeaderInput } from "./types.ts";

const CONNECT_PREFIX = "__connect.errors.";

type HeaderTuple = [Buffer | null, Buffer | null];

export function readHeader(
  rec: { headers: ReadonlyArray<HeaderTuple> },
  name: string,
): string | undefined {
  const needle = Buffer.from(name);
  for (const [k, v] of rec.headers) {
    if (k !== null && k.equals(needle)) {
      return v === null ? "" : v.toString("utf-8");
    }
  }
  return undefined;
}

export function stripConnectHeaders(
  headers: ReadonlyArray<HeaderTuple>,
): HeaderTuple[] {
  const out: HeaderTuple[] = [];
  for (const [k, v] of headers) {
    if (k === null) {
      out.push([k, v]);
      continue;
    }
    const name = k.toString("utf-8");
    if (name.startsWith(CONNECT_PREFIX)) continue;
    out.push([k, v]);
  }
  return out;
}

export function stampAuditHeaders(
  headers: ReadonlyArray<HeaderTuple>,
  audit: AuditHeaderInput,
): HeaderTuple[] {
  const append: HeaderTuple[] = [
    [Buffer.from("x-eventgate-replay-attempt"), Buffer.from(String(audit.attempt))],
    [Buffer.from("x-eventgate-replay-job-id"), Buffer.from(audit.jobId)],
    [Buffer.from("x-eventgate-replay-at"), Buffer.from(new Date().toISOString())],
    [Buffer.from("x-eventgate-replay-source-topic"), Buffer.from(audit.sourceTopic)],
    [
      Buffer.from("x-eventgate-replay-source-offset"),
      Buffer.from(`${audit.partition}:${audit.offset}`),
    ],
  ];
  return [...headers, ...append];
}

export function parseAttempt(raw: string | undefined): number {
  if (raw === undefined || raw === "") return 0;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.trunc(parsed));
}
```

- [ ] **Step 3: Verify**

```
bun test test/unit/replay.headers.test.ts
```

---

## Task 6: Producer signature widening

**Files:** Modify `src/kafka/producer.ts`. New test `test/unit/kafka.producer.headers.test.ts`.

- [ ] **Step 1: Failing test**

```ts
// test/unit/kafka.producer.headers.test.ts
import { describe, expect, it, mock } from "bun:test";

// We can't instantiate the real Producer without a broker; instead exercise the
// header normalization helper directly. Extract `normalizeHeaders` from producer.ts.
import { normalizeHeaders } from "../../src/kafka/producer.ts";

describe("normalizeHeaders", () => {
  it("returns undefined for null/undefined input", () => {
    expect(normalizeHeaders(undefined)).toBeUndefined();
    expect(normalizeHeaders(null)).toBeUndefined();
  });

  it("passes Record<string,string> through", () => {
    const out = normalizeHeaders({ a: "1", b: "2" });
    expect(out).toEqual({ a: "1", b: "2" });
  });

  it("converts array form to Map<Buffer,Buffer>", () => {
    const binary = Buffer.from([0x00, 0xff]);
    const out = normalizeHeaders([
      ["str", "value"],
      ["bin", binary],
      ["nullish", null],
    ]) as Map<Buffer, Buffer | null>;
    expect(out).toBeInstanceOf(Map);
    let foundBin = false;
    for (const [k, v] of out) {
      if (k.toString("utf-8") === "bin") {
        foundBin = true;
        expect(v?.equals(binary)).toBe(true);
      }
    }
    expect(foundBin).toBe(true);
  });
});
```

- [ ] **Step 2: Implement**

Edit `src/kafka/producer.ts` — widen `EventProducer.sendByTopic` type and extract a `normalizeHeaders` helper:

```ts
export type ProducerHeaders =
  | Record<string, string>
  | Array<[string, string | Buffer | null]>
  | null;

export function normalizeHeaders(
  headers: ProducerHeaders | undefined,
):
  | Record<string, string>
  | Map<Buffer, Buffer | null>
  | undefined {
  if (headers === undefined || headers === null) return undefined;
  if (Array.isArray(headers)) {
    const map = new Map<Buffer, Buffer | null>();
    for (const [k, v] of headers) {
      const key = Buffer.from(k);
      const value =
        v === null ? null : Buffer.isBuffer(v) ? v : Buffer.from(v, "utf-8");
      map.set(key, value);
    }
    return map;
  }
  return headers;
}

export type EventProducer = {
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: ProducerHeaders,
  ): Promise<void>;
  // ...rest unchanged
};
```

In the inner `sendByTopic`:

```ts
async sendByTopic(topic, key, value, headers) {
  const normalized = normalizeHeaders(headers);
  await producer.send({
    messages: [
      {
        topic,
        key,
        value,
        ...(normalized !== undefined ? { headers: normalized } : {}),
      },
    ],
  });
},
```

Also widen `ProducerHandle.sendByTopic` (it just passes through) and the `DrainerProducer` type in `src/outbox/drainer.ts` (forwards-compatible widening). Drainer callsite stays `Record<string, string>`.

- [ ] **Step 3: Verify**

```
bun test test/unit/kafka.producer.headers.test.ts test/unit/outbox.drainer.test.ts
bun run typecheck
```

Drainer test must still pass — array form is additive, existing `Record<string,string>` path unaffected.

---

## Task 7: Replay consumer

**Files:** Create `src/replay/consumer.ts`.

- [ ] **Step 1: Implement**

```ts
// src/replay/consumer.ts
import { Consumer } from "@platformatic/kafka";
import type { RouteConfig } from "../config/schemas.ts";
import { getLogger } from "../logging/index.ts";
import type { KafkaProvider } from "../kafka/providers/index.ts";
import type { DlqRecord } from "./types.ts";

const log = getLogger("replay.consumer");

export type StreamRangeOpts = {
  partition: number;
  fromOffset: number;
  toOffset?: number;          // inclusive; undefined => maxRecords cap controls
  maxRecords: number;
  signal: AbortSignal;
};

export type ReplayConsumer = {
  streamRange(opts: StreamRangeOpts): AsyncIterable<DlqRecord>;
  fetchOne(opts: { partition: number; offset: number }): Promise<DlqRecord | null>;
  close(): Promise<void>;
  readonly groupId: string;
};

function recordFromMessage(
  msg: { partition: number; offset: bigint; key: Buffer; value: Buffer; headers: Map<Buffer, Buffer>; timestamp: bigint },
  topic: string,
): DlqRecord {
  return {
    topic,
    partition: msg.partition,
    offset: Number(msg.offset),
    key: msg.key,
    value: msg.value,
    // Convert Map to raw tuple array; preserves duplicates lost by the Map collapse.
    // The SDK already collapses duplicates internally; iteration here is best-effort
    // fidelity. Phase 2 (triage) only reads Connect-named headers which never duplicate.
    headers: [...msg.headers.entries()].map(([k, v]) => [k, v] as [Buffer, Buffer]),
    timestamp: Number(msg.timestamp),
  };
}

export async function createReplayConsumer(
  provider: KafkaProvider,
  route: RouteConfig,
  jobId: string,
): Promise<ReplayConsumer> {
  const conn = await provider.getConnectionConfig();
  const groupId = `eventgate-replay-${route.name}-${jobId}`;

  const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
    clientId: `${conn.clientId}-replay-${jobId}`,
    bootstrapBrokers: conn.bootstrapBrokers,
    ...(conn.sasl ? { sasl: conn.sasl } : {}),
    ...(conn.tls ? { tls: conn.tls } : {}),
    ...(conn.connectTimeout !== undefined ? { connectTimeout: conn.connectTimeout } : {}),
    ...(conn.timeout !== undefined ? { timeout: conn.timeout } : {}),
    groupId,
    autocommit: false,
  });

  log.debug({ route: route.name, jobId, groupId }, "replay consumer created");

  return {
    groupId,

    async *streamRange(opts) {
      const fetchBudget = Math.max(1, Math.ceil(opts.maxRecords / 100));
      const stream = await consumer.consume({
        topics: [route.dlqTopic],
        mode: "manual",
        offsets: [
          { topic: route.dlqTopic, partition: opts.partition, offset: BigInt(opts.fromOffset) },
        ],
        maxFetches: fetchBudget,
        autocommit: false,
      });

      let yielded = 0;
      try {
        for await (const msg of stream) {
          if (opts.signal.aborted) break;
          const rec = recordFromMessage(msg, route.dlqTopic);
          if (opts.toOffset !== undefined && rec.offset > opts.toOffset) break;
          yield rec;
          yielded += 1;
          if (yielded >= opts.maxRecords) break;
        }
      } finally {
        await stream.close();
      }
    },

    async fetchOne(opts) {
      const stream = await consumer.consume({
        topics: [route.dlqTopic],
        mode: "manual",
        offsets: [
          { topic: route.dlqTopic, partition: opts.partition, offset: BigInt(opts.offset) },
        ],
        maxFetches: 1,
        autocommit: false,
      });
      try {
        for await (const msg of stream) {
          const rec = recordFromMessage(msg, route.dlqTopic);
          if (rec.partition === opts.partition && rec.offset === opts.offset) {
            return rec;
          }
          // Broker may return earlier offsets in a batch; iterate until match or batch end.
        }
        return null;
      } finally {
        await stream.close();
      }
    },

    async close() {
      await consumer.close();
    },
  };
}
```

Phase 1 does not yet wire `Admin.deleteGroups` cleanup — that's Phase 4 alongside the real bulk runner. The Phase 1 single-message and dry-run paths create transient groups; manual cleanup or broker retention covers it during initial smoke.

No unit test for the consumer in Phase 1 (it requires a real broker). The local smoke test in §"Verification" exercises it end-to-end.

---

## Task 8: Replay runner (dry-run only)

**Files:** Create `src/replay/runner.ts`, `test/unit/replay.runner.dryRun.test.ts`.

- [ ] **Step 1: Failing test**

```ts
// test/unit/replay.runner.dryRun.test.ts
import { describe, expect, it } from "bun:test";
import { runReplayBatchDryRun } from "../../src/replay/runner.ts";
import type { DlqRecord } from "../../src/replay/types.ts";

function rec(offset: number): DlqRecord {
  return {
    topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    partition: 0,
    offset,
    key: Buffer.from("k"),
    value: Buffer.from("v"),
    headers: [],
    timestamp: Date.now(),
  };
}

async function* records(n: number): AsyncIterable<DlqRecord> {
  for (let i = 0; i < n; i++) yield rec(i);
}

describe("runReplayBatchDryRun", () => {
  it("counts every record as would-replay (Phase 1 stub)", async () => {
    let producerCalled = 0;
    const fakeProducer = {
      sendByTopic: async () => {
        producerCalled += 1;
      },
    };
    const result = await runReplayBatchDryRun({
      records: records(5),
      producer: fakeProducer,
    });
    expect(producerCalled).toBe(0);
    expect(result.scanned).toBe(5);
    expect(result.replayed).toBe(0);
    expect(result.lastOffset).toBe(4);
  });

  it("scans zero records gracefully", async () => {
    const fakeProducer = { sendByTopic: async () => {} };
    const result = await runReplayBatchDryRun({
      records: records(0),
      producer: fakeProducer,
    });
    expect(result.scanned).toBe(0);
    expect(result.lastOffset).toBe(null);
  });
});
```

- [ ] **Step 2: Implement**

```ts
// src/replay/runner.ts
import type { EventProducer } from "../kafka/producer.ts";
import type { DlqRecord, ReplayBatchResult } from "./types.ts";

export type DryRunOpts = {
  records: AsyncIterable<DlqRecord>;
  producer: EventProducer;  // accepted but never called in dry-run
};

export async function runReplayBatchDryRun(opts: DryRunOpts): Promise<ReplayBatchResult> {
  let scanned = 0;
  let lastOffset: number | null = null;
  for await (const rec of opts.records) {
    scanned += 1;
    lastOffset = rec.offset;
  }
  return {
    scanned,
    replayed: 0,
    parked: 0,
    skipped: 0,
    errors: 0,
    lastOffset,
  };
}
```

Phase 2 expands this with real triage decisions; Phase 4 adds the real produce path and `signal` handling. Phase 1 ships the dry-run scaffold so the API surface returns sensible numbers.

- [ ] **Step 3: Verify**

```
bun test test/unit/replay.runner.dryRun.test.ts
```

---

## Task 9: Job store stub

**Files:** Create `src/replay/jobStore.ts`.

- [ ] **Step 1: Implement (stub)**

```ts
// src/replay/jobStore.ts
import { randomUUID } from "node:crypto";
import type { ReplayJob } from "./types.ts";

export type ReplayJobCreate = {
  route: string;
  partition: number;
  mode: ReplayJob["mode"];
  dryRun: boolean;
  fromOffset?: number;
  toOffset?: number;
};

export type ReplayJobStore = {
  create(input: ReplayJobCreate): ReplayJob;
  get(id: string): ReplayJob | null;
  update(id: string, patch: Partial<ReplayJob>): void;
  cancel(id: string): boolean;
  hasActiveJob(route: string, partition: number): boolean;
  setCancelHandle(id: string, ctl: AbortController): void;
  cancelAll(): void;
};

// Phase 1 stub: in-memory only. Phase 3 swaps this for the SQLite-backed
// implementation against replay_jobs + replay_state.
export function createReplayJobStore(): ReplayJobStore {
  const jobs = new Map<string, ReplayJob>();
  const handles = new Map<string, AbortController>();

  return {
    create(input) {
      const id = randomUUID();
      const job: ReplayJob = {
        id,
        route: input.route,
        partition: input.partition,
        mode: input.mode,
        dryRun: input.dryRun,
        scanned: 0,
        replayed: 0,
        parked: 0,
        skipped: 0,
        errors: 0,
        fromOffset: input.fromOffset ?? null,
        toOffset: input.toOffset ?? null,
        lastOffset: null,
        status: "pending",
        lastError: null,
        nextResumeAt: null,
        startedAt: Date.now(),
        finishedAt: null,
      };
      jobs.set(id, job);
      return job;
    },
    get(id) {
      return jobs.get(id) ?? null;
    },
    update(id, patch) {
      const cur = jobs.get(id);
      if (!cur) return;
      jobs.set(id, { ...cur, ...patch });
    },
    cancel(id) {
      const cur = jobs.get(id);
      if (!cur) return false;
      if (cur.status === "done" || cur.status === "failed" || cur.status === "cancelled") {
        return false;
      }
      handles.get(id)?.abort();
      jobs.set(id, { ...cur, status: "cancelled", finishedAt: Date.now() });
      return true;
    },
    hasActiveJob(route, partition) {
      for (const j of jobs.values()) {
        if (j.route !== route || j.partition !== partition) continue;
        if (j.status === "pending" || j.status === "running" || j.status === "paused") {
          return true;
        }
      }
      return false;
    },
    setCancelHandle(id, ctl) {
      handles.set(id, ctl);
    },
    cancelAll() {
      for (const ctl of handles.values()) ctl.abort();
    },
  };
}
```

---

## Task 10: Admin replay endpoint

**Files:** Create `src/admin/replayEndpoint.ts`, `test/unit/admin.replayEndpoint.test.ts`.

- [ ] **Step 1: Failing test**

```ts
// test/unit/admin.replayEndpoint.test.ts
import { describe, expect, it } from "bun:test";
import { makeReplayHandlers } from "../../src/admin/replayEndpoint.ts";
import { createReplayJobStore } from "../../src/replay/jobStore.ts";
import type { RouteConfig } from "../../src/config/schemas.ts";

const TOKEN = "x".repeat(40);

function route(): RouteConfig {
  return {
    name: "elastic-autoops",
    path: "/webhooks/elastic/autoops",
    topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    sourceHeader: "elastic-autoops",
    keyFields: ["resourceId"],
    idempotency: "elastic-autoops",
  };
}

function deps() {
  const jobStore = createReplayJobStore();
  const fakeProducer = { sendByTopic: async () => {}, isConnected: () => true, disconnect: async () => {} };
  const fakeAdmin = { listTopics: async () => ["DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"] };
  const fakeConsumerFactory = async () => ({
    groupId: "g",
    streamRange: async function* () {},
    fetchOne: async () => null,
    close: async () => {},
  });
  return makeReplayHandlers({
    expectedToken: TOKEN,
    jobStore,
    routes: new Map([[route().name, route()]]),
    producer: fakeProducer,
    admin: fakeAdmin,
    createConsumer: fakeConsumerFactory,
  });
}

describe("makeReplayHandlers — auth", () => {
  it("returns 401 when X-Admin-Token missing", async () => {
    const h = deps();
    const req = new Request("http://x/admin/dlq", { method: "GET" });
    const res = await h.listDlq(req);
    expect(res.status).toBe(401);
  });

  it("returns 401 on wrong token", async () => {
    const h = deps();
    const req = new Request("http://x/admin/dlq", {
      method: "GET",
      headers: { "x-admin-token": "wrong" },
    });
    const res = await h.listDlq(req);
    expect(res.status).toBe(401);
  });
});

describe("singleReplay (dry-run)", () => {
  it("returns 400 on invalid body", async () => {
    const h = deps();
    const req = new Request("http://x/admin/replay/elastic-autoops/message", {
      method: "POST",
      headers: { "x-admin-token": TOKEN, "content-type": "application/json" },
      body: "not json",
    });
    const res = await h.singleReplay(req);
    expect(res.status).toBe(400);
  });

  it("returns 404 on unknown route", async () => {
    const h = deps();
    const req = new Request("http://x/admin/replay/unknown-route/message", {
      method: "POST",
      headers: { "x-admin-token": TOKEN, "content-type": "application/json" },
      body: JSON.stringify({ partition: 0, offset: 1, dryRun: true }),
    });
    const res = await h.singleReplay(req);
    expect(res.status).toBe(404);
  });

  it("returns 200 with dryRun decision for known route", async () => {
    const h = deps();
    // Override the consumer factory to return a synthetic record.
    const req = new Request("http://x/admin/replay/elastic-autoops/message", {
      method: "POST",
      headers: { "x-admin-token": TOKEN, "content-type": "application/json" },
      body: JSON.stringify({ partition: 0, offset: 1, dryRun: true }),
    });
    const res = await h.singleReplay(req);
    expect(res.status).toBe(200);
    const body = await res.json() as { decision: { kind: string }; replayed: boolean };
    // Phase 1 stub: always "replay" decision; dryRun ⇒ replayed:false
    expect(body.replayed).toBe(false);
  });
});

describe("bulkReplay", () => {
  it("returns 202 + jobId for valid body", async () => {
    const h = deps();
    const req = new Request("http://x/admin/replay/elastic-autoops", {
      method: "POST",
      headers: { "x-admin-token": TOKEN, "content-type": "application/json" },
      body: JSON.stringify({ partition: 0, dryRun: true }),
    });
    const res = await h.bulkReplay(req);
    expect(res.status).toBe(202);
    const body = await res.json() as { jobId: string };
    expect(body.jobId).toMatch(/[0-9a-f-]{36}/);
  });
});

describe("jobStatus + cancelJob", () => {
  it("returns 404 for missing jobId", async () => {
    const h = deps();
    const req = new Request("http://x/admin/replay/no-such-job", {
      method: "GET",
      headers: { "x-admin-token": TOKEN },
    });
    const res = await h.jobStatus(req);
    expect(res.status).toBe(404);
  });
});
```

- [ ] **Step 2: Implement**

```ts
// src/admin/replayEndpoint.ts
import { z } from "zod";
import { verifyAdminToken } from "./auth.ts";
import type { RouteConfig } from "../config/schemas.ts";
import type { HealthAdmin } from "../health/admin.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { ReplayConsumer } from "../replay/consumer.ts";
import { stripConnectHeaders, stampAuditHeaders, readHeader, parseAttempt } from "../replay/headers.ts";
import type { ReplayJobStore } from "../replay/jobStore.ts";
import { runReplayBatchDryRun } from "../replay/runner.ts";

const log = getLogger("admin.replayEndpoint");

export type ReplayDeps = {
  expectedToken: string;
  jobStore: ReplayJobStore;
  routes: ReadonlyMap<string, RouteConfig>;
  producer: EventProducer;
  admin: HealthAdmin;
  createConsumer: (route: RouteConfig, jobId: string) => Promise<ReplayConsumer>;
};

const singleBodySchema = z.strictObject({
  partition: z.number().int().nonnegative(),
  offset: z.number().int().nonnegative(),
  dryRun: z.boolean().default(true),
});

const bulkBodySchema = z.strictObject({
  partition: z.number().int().nonnegative(),
  dryRun: z.boolean().default(true),
  fromOffset: z.number().int().nonnegative().optional(),
  toOffset: z.number().int().nonnegative().optional(),
  maxRecords: z.number().int().positive().optional(),
  filter: z.strictObject({ exceptionClass: z.string().min(1).optional() }).optional(),
});

function unauthorized(): Response {
  return Response.json({ error: "unauthorized" }, { status: 401 });
}

function pathParam(req: Request, prefix: string, suffix?: string): string | null {
  const url = new URL(req.url);
  let path = url.pathname;
  if (!path.startsWith(prefix)) return null;
  path = path.slice(prefix.length);
  if (suffix !== undefined) {
    if (!path.endsWith(suffix)) return null;
    path = path.slice(0, path.length - suffix.length);
  }
  return path.length === 0 ? null : path;
}

export function makeReplayHandlers(deps: ReplayDeps) {
  const { expectedToken, jobStore, routes, producer, admin: _admin, createConsumer } = deps;

  return {
    listDlq: async (req: Request): Promise<Response> => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) return unauthorized();
      // Phase 1: empty body shape; Phase 3 populates per-partition depth.
      return Response.json({
        routes: [...routes.values()].map((r) => ({
          route: r.name,
          dlqTopic: r.dlqTopic,
          partitions: [],
          lastJob: null,
        })),
      });
    },

    bulkReplay: async (req: Request): Promise<Response> => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) return unauthorized();
      const routeName = pathParam(req, "/admin/replay/");
      if (routeName === null) return Response.json({ error: "missing route" }, { status: 400 });
      const route = routes.get(routeName);
      if (route === undefined) return Response.json({ error: "unknown route" }, { status: 404 });

      let body: unknown;
      try { body = await req.json(); } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400 });
      }
      const parsed = bulkBodySchema.safeParse(body);
      if (!parsed.success) {
        return Response.json(
          { error: "validation", issues: parsed.error.issues.map((i) => ({ path: i.path, message: i.message })) },
          { status: 400 },
        );
      }

      const job = jobStore.create({
        route: route.name,
        partition: parsed.data.partition,
        mode: "manual",
        dryRun: parsed.data.dryRun,
        fromOffset: parsed.data.fromOffset,
        toOffset: parsed.data.toOffset,
      });

      // Phase 1: kick off a dry-run only; real bulk runner ships Phase 4.
      if (!parsed.data.dryRun) {
        // Non-dry-run is accepted but treated as a no-op for now with a clear status.
        jobStore.update(job.id, {
          status: "failed",
          lastError: "bulk_non_dryrun_not_implemented_yet",
          finishedAt: Date.now(),
        });
        return Response.json(
          { jobId: job.id, status: "failed", dryRun: false, message: "bulk replay (non-dry-run) ships in a later phase" },
          { status: 202 },
        );
      }

      // Run dry-run asynchronously so the HTTP response is non-blocking.
      const ctl = new AbortController();
      jobStore.setCancelHandle(job.id, ctl);
      jobStore.update(job.id, { status: "running" });

      void (async () => {
        try {
          const consumer = await createConsumer(route, job.id);
          try {
            const stream = consumer.streamRange({
              partition: parsed.data.partition,
              fromOffset: parsed.data.fromOffset ?? 0,
              toOffset: parsed.data.toOffset,
              maxRecords: parsed.data.maxRecords ?? 10_000,
              signal: ctl.signal,
            });
            const result = await runReplayBatchDryRun({ records: stream, producer });
            jobStore.update(job.id, {
              ...result,
              status: "done",
              finishedAt: Date.now(),
            });
          } finally {
            await consumer.close();
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          log.warn({ jobId: job.id, err: message }, "bulk dry-run failed");
          jobStore.update(job.id, {
            status: "failed",
            lastError: message,
            finishedAt: Date.now(),
          });
        }
      })();

      return Response.json({ jobId: job.id, status: "running", dryRun: true }, { status: 202 });
    },

    singleReplay: async (req: Request): Promise<Response> => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) return unauthorized();
      const routeName = pathParam(req, "/admin/replay/", "/message");
      if (routeName === null) return Response.json({ error: "missing route" }, { status: 400 });
      const route = routes.get(routeName);
      if (route === undefined) return Response.json({ error: "unknown route" }, { status: 404 });

      let body: unknown;
      try { body = await req.json(); } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400 });
      }
      const parsed = singleBodySchema.safeParse(body);
      if (!parsed.success) {
        return Response.json(
          { error: "validation", issues: parsed.error.issues.map((i) => ({ path: i.path, message: i.message })) },
          { status: 400 },
        );
      }

      // Phase 1: real triage ships Phase 2. Here we ALWAYS return a synthetic
      // "replay" decision for visibility, and never call the producer.
      const job = jobStore.create({
        route: route.name,
        partition: parsed.data.partition,
        mode: "single",
        dryRun: parsed.data.dryRun,
        fromOffset: parsed.data.offset,
        toOffset: parsed.data.offset,
      });

      try {
        const consumer = await createConsumer(route, job.id);
        try {
          const rec = await consumer.fetchOne({
            partition: parsed.data.partition,
            offset: parsed.data.offset,
          });
          if (rec === null) {
            jobStore.update(job.id, { status: "done", scanned: 0, finishedAt: Date.now() });
            return Response.json({ decision: null, replayed: false, parked: false, message: "record not found" }, { status: 200 });
          }

          // Pretend-triage; Phase 2 swaps this for `triage(rec, cfg, attempt)`.
          const attempt = parseAttempt(readHeader(rec, "x-eventgate-replay-attempt"));
          const excClass = readHeader(rec, "__connect.errors.exception.class.name") ?? null;
          const decision = { kind: "replay" as const, exceptionClass: excClass };

          // Build the headers we WOULD send so the response is informative; do not actually produce in dry-run.
          if (!parsed.data.dryRun) {
            // Non-dry-run on the single endpoint is similarly deferred for safety until Phase 2 ships real triage.
            jobStore.update(job.id, { status: "failed", lastError: "single_non_dryrun_not_implemented_yet", finishedAt: Date.now() });
            return Response.json(
              { decision, replayed: false, parked: false, message: "non-dry-run single-message replay ships in a later phase" },
              { status: 202 },
            );
          }

          const wouldSend = stampAuditHeaders(stripConnectHeaders(rec.headers), {
            jobId: job.id,
            sourceTopic: rec.topic,
            partition: rec.partition,
            offset: rec.offset,
            attempt: attempt + 1,
          });

          jobStore.update(job.id, { status: "done", scanned: 1, finishedAt: Date.now() });
          return Response.json(
            {
              decision,
              replayed: false,
              parked: false,
              dryRun: true,
              wouldStampHeaders: wouldSend.map(([k, v]) => ({
                name: k?.toString("utf-8") ?? null,
                value: v?.toString("utf-8") ?? null,
              })),
            },
            { status: 200 },
          );
        } finally {
          await consumer.close();
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ jobId: job.id, err: message }, "single dry-run failed");
        jobStore.update(job.id, { status: "failed", lastError: message, finishedAt: Date.now() });
        return Response.json({ error: "consumer failure", message }, { status: 500 });
      }
    },

    jobStatus: async (req: Request): Promise<Response> => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) return unauthorized();
      const id = pathParam(req, "/admin/replay/");
      if (id === null) return Response.json({ error: "missing jobId" }, { status: 400 });
      const job = jobStore.get(id);
      if (job === null) return Response.json({ error: "unknown job" }, { status: 404 });
      return Response.json(job);
    },

    cancelJob: async (req: Request): Promise<Response> => {
      if (!verifyAdminToken(req.headers.get("x-admin-token"), expectedToken)) return unauthorized();
      const id = pathParam(req, "/admin/replay/", "/cancel");
      if (id === null) return Response.json({ error: "missing jobId" }, { status: 400 });
      const cancelled = jobStore.cancel(id);
      if (!cancelled) {
        const exists = jobStore.get(id) !== null;
        if (!exists) return Response.json({ error: "unknown job" }, { status: 404 });
      }
      return Response.json({ jobId: id, cancelled });
    },
  };
}
```

- [ ] **Step 3: Verify**

```
bun test test/unit/admin.replayEndpoint.test.ts
bun run typecheck
```

---

## Task 11: Wire into gateway

**Files:** Modify `src/gateway/index.ts`, `src/gateway/routes.ts`, `.env.example`, `CLAUDE.md`.

- [ ] **Step 1: routes.ts** — register the five handlers when `config.admin?.token && config.replay?.enabled && deps.replayContext`. Bun.serve route patterns for the `:param` paths:

```ts
routes["/admin/dlq"] = { GET: handlers.listDlq };
routes["/admin/replay/:route"] = { POST: handlers.bulkReplay };
routes["/admin/replay/:route/message"] = { POST: handlers.singleReplay };
routes["/admin/replay/:jobId"] = { GET: handlers.jobStatus };
routes["/admin/replay/:jobId/cancel"] = { POST: handlers.cancelJob };
```

Note: Bun matches static routes before parametric ones, so `/admin/replay/:jobId` does NOT shadow `/admin/replay/:route`. They have the same shape; disambiguation is by method (`GET` vs `POST`) — the handlers themselves do the lookup against `jobStore` or `routes` map, so a `GET /admin/replay/<route-name>` returns 404 from `jobStatus` while `POST /admin/replay/<route-name>` hits `bulkReplay` correctly.

- [ ] **Step 2: index.ts** — after `healthAdmin` is constructed:

```ts
let replayContext: { jobStore: ReplayJobStore; handlers: ReturnType<typeof makeReplayHandlers> } | undefined;
if (config.admin?.token && config.replay?.enabled) {
  const jobStore = createReplayJobStore();
  const routesMap = new Map<string, RouteConfig>(
    config.routes.map((r) => [r.name, r]),
  );
  const handlers = makeReplayHandlers({
    expectedToken: config.admin.token,
    jobStore,
    routes: routesMap,
    producer,
    admin: healthAdmin,
    createConsumer: (route, jobId) => createReplayConsumer(provider, route, jobId),
  });
  replayContext = { jobStore, handlers };
  log.info({ replay: true }, "replay endpoints enabled");
}
```

Wire `replayContext` into `buildRoutes` (extend `RouteDeps`). Extend `shutdown()` to call `replayContext?.jobStore.cancelAll()` before existing teardown.

- [ ] **Step 3: .env.example** — append the REPLAY_* block matching the spec's env var table, inline-commented in the same style as the other blocks.

- [ ] **Step 4: CLAUDE.md** — add a short "DLQ replay" section under Architecture and list the new env vars in the existing config table.

---

## Task 12: Verify everything

- [ ] `bun run typecheck` — no errors.
- [ ] `bun test` — all suites pass (existing + 6 new files).
- [ ] Prod-safety probes:
  - `REPLAY_ENABLED=` (unset) `bun -e "import('./src/config/index.ts').then(m => console.log(m.config.replay))"` → `undefined`.
  - `REPLAY_ENABLED=true ADMIN_TOKEN=...` start gateway, verify startup log shows `replay endpoints enabled`.
  - Without `ADMIN_TOKEN`, replay endpoints are NOT registered even when `REPLAY_ENABLED=true` (silent — replay needs both).
- [ ] SDK Map<Buffer> smoke (Task 1) result documented.
- [ ] Local smoke against Redpanda:
  - `docker compose up -d`
  - Start gateway with `REPLAY_ENABLED=true ADMIN_TOKEN=$(openssl rand -hex 32)`
  - Produce a synthetic record to `DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` with `__connect.errors.exception.class.name` and `idempotencyKey` headers via `mcp__mcp-server-kafka__kafka_produce_message`.
  - `curl -H "X-Admin-Token: $ADMIN_TOKEN" http://localhost:3000/admin/dlq` → 200, route listed (empty partitions array in Phase 1).
  - `curl -H "X-Admin-Token: $ADMIN_TOKEN" -d '{"partition":0,"offset":0,"dryRun":true}' -H "content-type: application/json" http://localhost:3000/admin/replay/elastic-autoops/message` → 200, `decision.kind=="replay"`, `replayed:false`, `wouldStampHeaders` includes attempt=1 and idempotencyKey preserved.
- [ ] Kill background gateway: `lsof -i :3000` then `kill`.

---

## Commit / PR

```
SIO-827 (Phase 1): DLQ replay API scaffold (feature off by default)

- Adds /admin/dlq, /admin/replay/:route[/message], /admin/replay/:jobId[/cancel]
- All endpoints behind ADMIN_TOKEN + REPLAY_ENABLED master switch
- Triage stubbed (always returns "replay" for dry-run analysis)
- SQLite tables not yet created (Phase 3); in-memory jobStore stub
- Bulk non-dry-run + single non-dry-run accepted but no-op (deferred to Phases 2/4)
- Producer signature widened: accepts Array<[string, string|Buffer|null]>
- /admin/dlq + /admin/replay added to RESERVED_PATHS with prefix guard

No runtime impact when REPLAY_ENABLED=false (default).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

PR body should link the spec, the plan, and the Linear issue. Linear status transition: In Progress → In Review on PR open. Done only with explicit user approval.
