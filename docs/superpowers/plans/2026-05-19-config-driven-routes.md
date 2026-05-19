# Config-Driven Webhook Routes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the gateway's set of webhook routes data-driven via `config.routes[]` (with `ROUTES_JSON` env override), enforce the `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` topic-naming policy at config-validation time, rename `ops.elastic.autoops.raw.v1` → `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`, and adapt the outbox to store and dispatch full topic strings.

**Architecture:** Routes become an array in the existing 4-pillar config (`defaults.ts` seed + `ROUTES_JSON` replace-override). A new Zod `routesSchema` enforces uniqueness, the naming-policy regex, the forbidden-prefix matrix, and the `dlqTopic === "DLQ_T_" + topic` invariant. The gateway iterates `config.routes` at startup and builds one closure-per-route via a generic `makeWebhookHandler(route, deps)`. The outbox stores the literal topic string per row (schema becomes `z.string()`), and the drainer publishes to `row.topic` directly — no enum mapping. Idempotency stays as a named-function registry so adding a new strategy is one function + one config reference.

**Tech Stack:** Bun 1.3+ (`Bun.serve` routes object, `bun:sqlite`), TypeScript strict mode, Zod v4 (`strictObject`, `superRefine`), Pino 10 + ECS logging, `bun:test`. No new dependencies.

**Reference spec:** `docs/superpowers/specs/2026-05-19-config-driven-routes-design.md`. Read this before starting — it contains the rationale for every decision below.

---

## File Structure

**New files**
- `src/gateway/idempotencyStrategies.ts` — named registry mapping strategy name → `(body: unknown) => string | undefined`.
- `src/gateway/handler.ts` — `makeWebhookHandler(route, deps)` factory; the per-request logic, parameterised on a `Route`.
- `test/unit/config.routes.test.ts` — schema, env override, uniqueness, basic field rules.
- `test/unit/config.routes.naming.test.ts` — topic naming policy, forbidden prefixes, `dlqTopic` derivation rule.
- `test/unit/gateway.routes.dispatch.test.ts` — verifies one handler per config route, correct topic dispatch.
- `test/unit/gateway.idempotencyStrategies.test.ts` — registry resolution, missing-strategy fallback.

**Modified files**
- `src/config/defaults.ts` — add `routes: [...]` with the renamed Elastic AutoOps seed entry.
- `src/config/envMapping.ts` — add `ROUTES_JSON` parsing (full replacement, not merge).
- `src/config/schemas.ts` — add `routesSchema` + cross-field `superRefine`; include in `configSchema`.
- `src/config/loader.ts` — special-case `routes` so the env override replaces the default array wholesale.
- `src/outbox/schemas.ts` — `outboxTopicSchema` becomes `z.string().min(1)`.
- `src/outbox/writer.ts` — accepts arbitrary topic string; remove enum-only assumption.
- `src/outbox/drainer.ts` — remove `topicToKafka` mapping; publish to `row.topic` directly; `DrainerConfig.topics` removed.
- `src/outbox/db.ts` — migration step that rewrites legacy `topic = "raw"` rows to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`.
- `src/gateway/routes.ts` — iterate `config.routes`; delegate to `makeWebhookHandler` from `handler.ts`.
- `src/gateway/index.ts` — drop the explicit `topics: { raw }` argument when starting the drainer; add a startup log line listing all registered routes.
- `test/unit/outbox.writer.test.ts` — adapt fixture topic values to the new naming policy.
- `test/unit/outbox.drainer.test.ts` — adapt the drainer config and topic dispatch.
- `test/unit/config.kafka-provider.test.ts` / `config.outbox.test.ts` — only minor adjustments if their fixtures touch `routes` indirectly.
- `docs/development/getting-started.md` — extend the smoke section with a two-route example.

---

## Task 1: Topic naming policy as a standalone helper

**Why first:** The naming policy is the single load-bearing rule in this plan; isolating it in a pure helper makes it trivially testable and lets every downstream schema/test reference one source of truth.

**Files:**
- Create: `src/config/topicPolicy.ts`
- Test: `test/unit/config.topicPolicy.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/config.topicPolicy.test.ts
import { describe, expect, it } from "bun:test";
import { checkGatewayTopic, expectedDlqTopic, isGatewayTopic } from "../../src/config/topicPolicy.ts";

describe("isGatewayTopic", () => {
  it("accepts T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>", () => {
    expect(isGatewayTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(true);
    expect(isGatewayTopic("T_PRIVATE_SOURCE_DATADOG_ALERTS")).toBe(true);
    expect(isGatewayTopic("T_PRIVATE_SOURCE_GITHUB_PR_EVENTS")).toBe(true);
  });

  it("rejects lowercase", () => {
    expect(isGatewayTopic("t_private_source_elastic_autoops")).toBe(false);
  });

  it("rejects legacy ops.*.raw.v1", () => {
    expect(isGatewayTopic("ops.elastic.autoops.raw.v1")).toBe(false);
  });

  it("rejects T_PUBLIC_*", () => {
    expect(isGatewayTopic("T_PUBLIC_SOURCE_PIM_ARTICLES_MDM")).toBe(false);
  });

  it("rejects T_PRIVATE_SINK_*", () => {
    expect(isGatewayTopic("T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS")).toBe(false);
  });

  it("rejects T_PRIVATE_*_RICH_NOTIFICATIONS and _EVENTS", () => {
    expect(isGatewayTopic("T_PRIVATE_PRODUCT_RICH_NOTIFICATIONS")).toBe(false);
    expect(isGatewayTopic("T_PRIVATE_CORRECTED_DELIVERY_DATES_CHANGED_EVENTS")).toBe(false);
  });

  it("rejects DLQ_T_*", () => {
    expect(isGatewayTopic("DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(false);
  });

  it("rejects system topics", () => {
    expect(isGatewayTopic("__consumer_offsets")).toBe(false);
    expect(isGatewayTopic("_schemas")).toBe(false);
    expect(isGatewayTopic("_confluent-monitoring")).toBe(false);
  });
});

describe("checkGatewayTopic", () => {
  it("returns ok for a valid topic", () => {
    expect(checkGatewayTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toEqual({ ok: true });
  });

  it("returns a distinct message for T_PUBLIC_*", () => {
    const r = checkGatewayTopic("T_PUBLIC_SOURCE_PIM_ARTICLES_MDM");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/MDM publisher/i);
  });

  it("returns a distinct message for T_PRIVATE_SINK_*", () => {
    const r = checkGatewayTopic("T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/sink connector/i);
  });

  it("returns a distinct message for DLQ_T_*", () => {
    const r = checkGatewayTopic("DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/dlqTopic/);
  });

  it("returns a distinct message for system topics", () => {
    const r = checkGatewayTopic("__consumer_offsets");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/system topic/i);
  });

  it("rejects topics longer than 249 characters", () => {
    const long = "T_PRIVATE_SOURCE_X_" + "A".repeat(240);
    const r = checkGatewayTopic(long);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.message).toMatch(/length/i);
  });
});

describe("expectedDlqTopic", () => {
  it("prefixes with DLQ_T_", () => {
    expect(expectedDlqTopic("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS")).toBe(
      "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
    );
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/config.topicPolicy.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```ts
// src/config/topicPolicy.ts
// Org-wide topic naming policy enforced for gateway-owned topics.
// See: docs/superpowers/specs/2026-05-19-config-driven-routes-design.md

const KAFKA_MAX_TOPIC_LENGTH = 249;

const GATEWAY_TOPIC_REGEX = /^T_PRIVATE_SOURCE_[A-Z][A-Z0-9]*_[A-Z][A-Z0-9]*(_[A-Z][A-Z0-9]*)*$/;

type Check = { ok: true } | { ok: false; message: string };

export function isGatewayTopic(topic: string): boolean {
  if (topic.length === 0 || topic.length > KAFKA_MAX_TOPIC_LENGTH) return false;
  return GATEWAY_TOPIC_REGEX.test(topic);
}

export function checkGatewayTopic(topic: string): Check {
  if (topic.length === 0) {
    return { ok: false, message: "topic must be non-empty" };
  }
  if (topic.length > KAFKA_MAX_TOPIC_LENGTH) {
    return {
      ok: false,
      message: `topic length ${topic.length} exceeds Kafka limit of ${KAFKA_MAX_TOPIC_LENGTH}`,
    };
  }
  if (topic.startsWith("T_PUBLIC_")) {
    return {
      ok: false,
      message: "gateway is not an MDM publisher; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (topic.startsWith("T_PRIVATE_SINK_")) {
    return {
      ok: false,
      message: "gateway is not a sink connector; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (topic.startsWith("DLQ_T_")) {
    return {
      ok: false,
      message: "DLQ topics are declared via dlqTopic, not topic",
    };
  }
  if (topic.startsWith("__") || topic === "_schemas" || topic.startsWith("_confluent-")) {
    return { ok: false, message: "system topic prefix; not gateway-writable" };
  }
  if (
    topic.startsWith("T_PRIVATE_") &&
    (topic.endsWith("_RICH_NOTIFICATIONS") || topic.endsWith("_EVENTS"))
  ) {
    return {
      ok: false,
      message:
        "internal event/notification streams are not gateway-owned; topic must start with T_PRIVATE_SOURCE_",
    };
  }
  if (!GATEWAY_TOPIC_REGEX.test(topic)) {
    return {
      ok: false,
      message: "topic must match T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY> (uppercase, underscores)",
    };
  }
  return { ok: true };
}

export function expectedDlqTopic(topic: string): string {
  return `DLQ_T_${topic}`;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/config.topicPolicy.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/config/topicPolicy.ts test/unit/config.topicPolicy.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: topic naming policy helper for gateway-owned topics

T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY> regex plus distinct-message
guardrails against T_PUBLIC_*, T_PRIVATE_SINK_*, DLQ_T_*, system
prefixes, and the *_RICH_NOTIFICATIONS / *_EVENTS internal streams.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Idempotency strategies registry

**Why next:** `routesSchema` will need to reference this registry's keys for `idempotency` validation. Land the registry first.

**Files:**
- Create: `src/gateway/idempotencyStrategies.ts`
- Test: `test/unit/gateway.idempotencyStrategies.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/gateway.idempotencyStrategies.test.ts
import { describe, expect, it } from "bun:test";
import {
  idempotencyStrategies,
  knownIdempotencyStrategy,
  resolveIdempotencyStrategy,
} from "../../src/gateway/idempotencyStrategies.ts";

describe("idempotencyStrategies", () => {
  it("includes elastic-autoops by default", () => {
    expect(typeof idempotencyStrategies["elastic-autoops"]).toBe("function");
  });

  it("elastic-autoops derives a hash for an AutoOps-shaped body", () => {
    const fn = idempotencyStrategies["elastic-autoops"];
    const key = fn?.({
      resourceId: "dep-1",
      title: "Cluster red",
      status: "open",
      startTime: "2026-05-19T00:00:00Z",
    });
    expect(typeof key).toBe("string");
    expect((key ?? "").length).toBeGreaterThan(10);
  });

  it("elastic-autoops returns undefined for a non-AutoOps body", () => {
    const fn = idempotencyStrategies["elastic-autoops"];
    expect(fn?.({ foo: "bar" })).toBeUndefined();
  });
});

describe("knownIdempotencyStrategy", () => {
  it("returns true for registered names", () => {
    expect(knownIdempotencyStrategy("elastic-autoops")).toBe(true);
  });
  it("returns false for unknown names", () => {
    expect(knownIdempotencyStrategy("does-not-exist")).toBe(false);
  });
});

describe("resolveIdempotencyStrategy", () => {
  it("returns undefined when name is undefined", () => {
    expect(resolveIdempotencyStrategy(undefined)).toBeUndefined();
  });
  it("returns the function for a known name", () => {
    expect(typeof resolveIdempotencyStrategy("elastic-autoops")).toBe("function");
  });
  it("returns undefined for unknown name (defensive at runtime)", () => {
    expect(resolveIdempotencyStrategy("nope")).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/gateway.idempotencyStrategies.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```ts
// src/gateway/idempotencyStrategies.ts
import { autoOpsIdempotencyKey } from "./idempotencyKey.ts";

export type IdempotencyStrategy = (body: unknown) => string | undefined;

export const idempotencyStrategies: Record<string, IdempotencyStrategy> = {
  "elastic-autoops": autoOpsIdempotencyKey,
};

export function knownIdempotencyStrategy(name: string): boolean {
  return Object.prototype.hasOwnProperty.call(idempotencyStrategies, name);
}

export function resolveIdempotencyStrategy(
  name: string | undefined,
): IdempotencyStrategy | undefined {
  if (name === undefined) return undefined;
  return idempotencyStrategies[name];
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/gateway.idempotencyStrategies.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/gateway/idempotencyStrategies.ts test/unit/gateway.idempotencyStrategies.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: idempotency strategies registry

Named-function registry keyed by strategy name. Routes reference
strategies by name; unknown name resolves to undefined (no header
attached). Seeded with elastic-autoops.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Route schema with cross-field validation

**Files:**
- Modify: `src/config/schemas.ts`
- Test: `test/unit/config.routes.test.ts`, `test/unit/config.routes.naming.test.ts`

- [ ] **Step 1: Write the failing test (basic schema + uniqueness)**

```ts
// test/unit/config.routes.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const validRoute: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  keyFields: ["resourceId", "deployment-id"],
  idempotency: "elastic-autoops",
};

describe("routesSchema basics", () => {
  it("accepts a minimal valid route", () => {
    const r = routesSchema.safeParse([validRoute]);
    expect(r.success).toBe(true);
  });

  it("rejects an empty routes array", () => {
    const r = routesSchema.safeParse([]);
    expect(r.success).toBe(false);
  });

  it("rejects a path that doesn't start with /", () => {
    const r = routesSchema.safeParse([{ ...validRoute, path: "webhooks/x" }]);
    expect(r.success).toBe(false);
  });

  it("rejects empty keyFields", () => {
    const r = routesSchema.safeParse([{ ...validRoute, keyFields: [] }]);
    expect(r.success).toBe(false);
  });

  it("rejects an unknown idempotency strategy", () => {
    const r = routesSchema.safeParse([{ ...validRoute, idempotency: "nope" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /idempotency/.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate paths across routes", () => {
    const r = routesSchema.safeParse([
      validRoute,
      { ...validRoute, name: "second", topic: "T_PRIVATE_SOURCE_OTHER_THING" },
    ]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /path/i.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate topics across routes", () => {
    const r = routesSchema.safeParse([
      validRoute,
      { ...validRoute, name: "second", path: "/webhooks/other" },
    ]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /topic/i.test(i.message))).toBe(true);
    }
  });

  it("accepts an absent idempotency field", () => {
    const { idempotency: _omit, ...withoutIdem } = validRoute;
    const r = routesSchema.safeParse([withoutIdem]);
    expect(r.success).toBe(true);
  });

  it("accepts an absent dlqTopic", () => {
    const r = routesSchema.safeParse([validRoute]);
    expect(r.success).toBe(true);
  });
});
```

- [ ] **Step 2: Write the failing test (naming policy + DLQ rule)**

```ts
// test/unit/config.routes.naming.test.ts
import { describe, expect, it } from "bun:test";
import { routesSchema, type RouteConfig } from "../../src/config/schemas.ts";

const base: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  keyFields: ["resourceId"],
};

describe("topic naming policy enforced by routesSchema", () => {
  it("accepts T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>", () => {
    expect(routesSchema.safeParse([base]).success).toBe(true);
  });

  it("rejects lowercase", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "t_private_source_elastic_autoops" }]);
    expect(r.success).toBe(false);
  });

  it("rejects legacy ops.*.raw.v1", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "ops.elastic.autoops.raw.v1" }]);
    expect(r.success).toBe(false);
  });

  it("rejects T_PUBLIC_* with MDM message", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "T_PUBLIC_SOURCE_PIM_ARTICLES_MDM" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /MDM publisher/i.test(i.message))).toBe(true);
    }
  });

  it("rejects T_PRIVATE_SINK_* with sink message", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_SINK_COUCHBASE_PRICE_DOCUMENTS" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /sink connector/i.test(i.message))).toBe(true);
    }
  });

  it("rejects DLQ_T_* used as topic", () => {
    const r = routesSchema.safeParse([{ ...base, topic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /dlqTopic/.test(i.message))).toBe(true);
    }
  });

  it("rejects internal event streams (_RICH_NOTIFICATIONS, _EVENTS)", () => {
    expect(
      routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_PRODUCT_RICH_NOTIFICATIONS" }]).success,
    ).toBe(false);
    expect(
      routesSchema.safeParse([{ ...base, topic: "T_PRIVATE_CORRECTED_DELIVERY_DATES_CHANGED_EVENTS" }])
        .success,
    ).toBe(false);
  });

  it("rejects system prefixes (__, _schemas, _confluent-)", () => {
    expect(routesSchema.safeParse([{ ...base, topic: "__consumer_offsets" }]).success).toBe(false);
    expect(routesSchema.safeParse([{ ...base, topic: "_schemas" }]).success).toBe(false);
    expect(routesSchema.safeParse([{ ...base, topic: "_confluent-monitoring" }]).success).toBe(false);
  });

  it("rejects topic length > 249", () => {
    const long = "T_PRIVATE_SOURCE_X_" + "A".repeat(240);
    expect(routesSchema.safeParse([{ ...base, topic: long }]).success).toBe(false);
  });
});

describe("dlqTopic rule", () => {
  it("accepts dlqTopic equal to DLQ_T_<topic>", () => {
    const r = routesSchema.safeParse([
      { ...base, dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" },
    ]);
    expect(r.success).toBe(true);
  });

  it("rejects mismatched dlqTopic", () => {
    const r = routesSchema.safeParse([{ ...base, dlqTopic: "DLQ_T_WRONG_NAME" }]);
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(r.error.issues.some((i) => /dlqTopic must be/.test(i.message))).toBe(true);
    }
  });

  it("rejects duplicate dlqTopic across routes (defensive)", () => {
    const r = routesSchema.safeParse([
      { ...base, dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" },
      {
        name: "second",
        path: "/webhooks/elastic/autoops-2",
        topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS_TWO",
        keyFields: ["x"],
        // intentionally wrong, mirrors the first route's dlq — uniqueness check applies
        dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      },
    ]);
    // Both checks fire (mismatch on second + duplicate). We only need the failure.
    expect(r.success).toBe(false);
  });
});
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `bun test test/unit/config.routes.test.ts test/unit/config.routes.naming.test.ts`
Expected: FAIL (`routesSchema` is not exported from `schemas.ts`).

- [ ] **Step 4: Add `routesSchema` to `src/config/schemas.ts`**

Insert at the top of `src/config/schemas.ts` after the existing imports:

```ts
import { checkGatewayTopic, expectedDlqTopic } from "./topicPolicy.ts";
import { knownIdempotencyStrategy } from "../gateway/idempotencyStrategies.ts";
```

Add this block above `export const configSchema`:

```ts
const routeSchema = z.strictObject({
  name: z.string().min(1).describe("Human-readable route id; used for logs and as default sourceHeader."),
  path: z
    .string()
    .min(2)
    .startsWith("/")
    .describe("Literal HTTP path the gateway listens on, e.g. /webhooks/elastic/autoops."),
  topic: z
    .string()
    .min(1)
    .describe("Full Kafka topic name; must match T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>."),
  dlqTopic: z
    .string()
    .min(1)
    .optional()
    .describe("Optional companion DLQ name. If set, must equal DLQ_T_<topic>. Gateway never writes here."),
  sourceHeader: z
    .string()
    .min(1)
    .optional()
    .describe("Override for the 'source' Kafka header. Defaults to name."),
  keyFields: z
    .array(z.string().min(1))
    .min(1)
    .describe("Body fields to consult in order for the partition key. First non-empty string wins."),
  idempotency: z
    .string()
    .min(1)
    .optional()
    .describe("Named strategy from idempotencyStrategies registry. Optional."),
});

export type RouteConfig = z.infer<typeof routeSchema>;

export const routesSchema = z
  .array(routeSchema)
  .min(1, "at least one route is required")
  .superRefine((routes, ctx) => {
    const pathSeen = new Map<string, number>();
    const topicSeen = new Map<string, number>();
    const dlqSeen = new Map<string, number>();

    routes.forEach((r, i) => {
      const topicCheck = checkGatewayTopic(r.topic);
      if (!topicCheck.ok) {
        ctx.addIssue({
          code: "custom",
          path: [i, "topic"],
          message: topicCheck.message,
        });
      }

      if (r.dlqTopic !== undefined) {
        const expected = expectedDlqTopic(r.topic);
        if (r.dlqTopic !== expected) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `dlqTopic must be '${expected}'; got '${r.dlqTopic}'`,
          });
        }
        if (r.dlqTopic.length > 249) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `dlqTopic length ${r.dlqTopic.length} exceeds Kafka limit of 249`,
          });
        }
      }

      if (r.idempotency !== undefined && !knownIdempotencyStrategy(r.idempotency)) {
        ctx.addIssue({
          code: "custom",
          path: [i, "idempotency"],
          message: `unknown idempotency strategy '${r.idempotency}'`,
        });
      }

      const prevPath = pathSeen.get(r.path);
      if (prevPath !== undefined) {
        ctx.addIssue({
          code: "custom",
          path: [i, "path"],
          message: `duplicate path '${r.path}' (also at routes[${prevPath}])`,
        });
      } else {
        pathSeen.set(r.path, i);
      }

      const prevTopic = topicSeen.get(r.topic);
      if (prevTopic !== undefined) {
        ctx.addIssue({
          code: "custom",
          path: [i, "topic"],
          message: `duplicate topic '${r.topic}' (also at routes[${prevTopic}])`,
        });
      } else {
        topicSeen.set(r.topic, i);
      }

      if (r.dlqTopic !== undefined) {
        const prevDlq = dlqSeen.get(r.dlqTopic);
        if (prevDlq !== undefined) {
          ctx.addIssue({
            code: "custom",
            path: [i, "dlqTopic"],
            message: `duplicate dlqTopic '${r.dlqTopic}' (also at routes[${prevDlq}])`,
          });
        } else {
          dlqSeen.set(r.dlqTopic, i);
        }
      }
    });
  });
```

Then add `routes: routesSchema` as the last field inside the existing top-level `strictObject` in `configSchema`:

```ts
// inside the existing configSchema strictObject({...}):
    outbox: z.strictObject({ /* ...unchanged... */ }),
    routes: routesSchema,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `bun test test/unit/config.routes.test.ts test/unit/config.routes.naming.test.ts`
Expected: PASS for all cases.

- [ ] **Step 6: Typecheck**

Run: `bun run typecheck`
Expected: type error in `loader.ts` and elsewhere — `defaults` doesn't yet contain `routes`. This is expected and is fixed in Task 4. **Do not commit yet.**

- [ ] **Step 7: Continue to Task 4 before committing.**

(Reason: the schema requires `routes` but defaults don't supply it; committing now leaves `bun run typecheck` and `bun test` broken for the full suite.)

---

## Task 4: Seed `routes` in defaults + env override

**Files:**
- Modify: `src/config/defaults.ts`, `src/config/envMapping.ts`, `src/config/loader.ts`

- [ ] **Step 1: Add the seed routes entry to `src/config/defaults.ts`**

Insert at the bottom of the `defaults` object (after `outbox`):

```ts
  routes: [
    {
      name: "elastic-autoops",
      path: "/webhooks/elastic/autoops",
      topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      dlqTopic: "DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
      keyFields: ["resourceId", "deployment-id"],
      idempotency: "elastic-autoops",
    },
  ],
```

The final `defaults` object now ends with `routes: [...]`.

- [ ] **Step 2: Add `ROUTES_JSON` parsing to `src/config/envMapping.ts`**

Add the `routes` field to `EnvOverrides`:

```ts
export type EnvOverrides = {
  // ...existing fields...
  routes?: unknown[]; // raw, validated by routesSchema downstream
};
```

Add a helper near the other type coercion helpers:

```ts
function jsonArray(v: string | undefined): unknown[] | undefined {
  const s = str(v);
  if (s === undefined) return undefined;
  try {
    const parsed = JSON.parse(s);
    if (!Array.isArray(parsed)) return undefined;
    return parsed;
  } catch {
    return undefined;
  }
}
```

Inside `mapEnv`, after the `outbox` block:

```ts
  const routes = jsonArray(env.ROUTES_JSON);
  if (routes !== undefined) {
    overrides.routes = routes;
  }
```

(Adjacent to the existing pruning loop; the loop already strips empty sections so no further change needed there.)

- [ ] **Step 3: Special-case `routes` in `src/config/loader.ts`**

The existing `mergeDeep` would not recurse into a top-level array correctly — we want the env-provided routes to *replace* the default array wholesale. Change `buildConfig` to handle this:

Replace the body of `buildConfig`:

```ts
export function buildConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const overrides: EnvOverrides = mapEnv(env);

  // `routes` is replace-not-merge. Pull it out, merge the rest deeply,
  // then attach `routes` separately (env wins; otherwise defaults stand).
  const { routes: routesOverride, ...rest } = overrides;
  const merged = mergeDeep(defaults as unknown as AppConfig, rest);
  const withRoutes: AppConfig = {
    ...merged,
    routes: (routesOverride ?? (defaults as unknown as AppConfig).routes) as AppConfig["routes"],
  };

  const result = configSchema.safeParse(withRoutes);
  if (!result.success) {
    const summary = result.error.issues
      .map((i) => `  - ${i.path.join(".") || "<root>"}: ${i.message}`)
      .join("\n");
    throw new Error(`Invalid configuration:\n${summary}`);
  }
  return result.data;
}
```

- [ ] **Step 4: Run typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 5: Add an env-override test**

Create `test/unit/config.routes.env.test.ts`:

```ts
// test/unit/config.routes.env.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { buildConfig } from "../../src/config/loader.ts";

const baseEnv = {
  ENVIRONMENT: "dev",
  KAFKA_PROVIDER: "local",
};

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
});
afterEach(() => {
  process.env = snapshot;
});

describe("ROUTES_JSON override", () => {
  it("falls back to defaults when ROUTES_JSON is absent", () => {
    const cfg = buildConfig({ ...baseEnv });
    expect(cfg.routes).toHaveLength(1);
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
    expect(cfg.routes[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("replaces the default array wholesale when ROUTES_JSON is set", () => {
    const ROUTES_JSON = JSON.stringify([
      {
        name: "datadog-alerts",
        path: "/webhooks/datadog/alerts",
        topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
        keyFields: ["alert_id", "id"],
      },
    ]);
    const cfg = buildConfig({ ...baseEnv, ROUTES_JSON });
    expect(cfg.routes).toHaveLength(1);
    expect(cfg.routes[0]?.name).toBe("datadog-alerts");
    expect(cfg.routes[0]?.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
  });

  it("rejects malformed ROUTES_JSON via Zod (entry without keyFields)", () => {
    const ROUTES_JSON = JSON.stringify([
      {
        name: "x",
        path: "/x",
        topic: "T_PRIVATE_SOURCE_X_Y",
      },
    ]);
    expect(() => buildConfig({ ...baseEnv, ROUTES_JSON })).toThrow(/Invalid configuration/);
  });

  it("ignores ROUTES_JSON that is not a JSON array (defaults remain)", () => {
    const cfg = buildConfig({ ...baseEnv, ROUTES_JSON: "not json" });
    expect(cfg.routes[0]?.name).toBe("elastic-autoops");
  });
});
```

- [ ] **Step 6: Run test**

Run: `bun test test/unit/config.routes.env.test.ts`
Expected: PASS.

- [ ] **Step 7: Run the full test suite**

Run: `bun test`
Expected: all previously passing tests still pass; new tests pass.

- [ ] **Step 8: Commit Tasks 3 + 4 together**

```bash
git add src/config/schemas.ts src/config/defaults.ts src/config/envMapping.ts src/config/loader.ts \
        test/unit/config.routes.test.ts test/unit/config.routes.naming.test.ts test/unit/config.routes.env.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: routesSchema with topic-naming policy + ROUTES_JSON override

routes is now part of AppConfig. Seed entry renames the Elastic AutoOps
topic to T_PRIVATE_SOURCE_ELASTIC_AUTOOPS. ROUTES_JSON env var fully
replaces the defaults array (no per-route merge). Cross-field rules
enforce the naming policy, dlqTopic = DLQ_T_<topic>, and uniqueness on
path/topic/dlqTopic.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Outbox topic schema becomes a free string

**Files:**
- Modify: `src/outbox/schemas.ts`, `src/outbox/writer.ts`
- Test: `test/unit/outbox.writer.test.ts`

- [ ] **Step 1: Update the writer test fixture to the new topic shape**

Edit `test/unit/outbox.writer.test.ts`. Replace the three `topic: "raw"` occurrences with `topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`. Replace the "rejects unsupported topic" test with one that ensures the writer accepts arbitrary non-empty strings (the topic-name policy is enforced upstream at config validation; the writer is a generic buffer):

Replace the third `it(...)` block (the `@ts-expect-error` one) with:

```ts
  it("accepts arbitrary non-empty topic strings (policy enforced upstream)", () => {
    const writer = createWriter(db);
    writer.enqueue({
      topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
      messageKey: "k",
      payload: "{}",
      headers: null,
    });
    const row = db.query("SELECT topic FROM outbox").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
  });

  it("rejects empty topic strings at the writer boundary", () => {
    const writer = createWriter(db);
    expect(() =>
      writer.enqueue({ topic: "", messageKey: "k", payload: "{}", headers: null }),
    ).toThrow();
  });
```

Update the other tests in this file to use `"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"` wherever `"raw"` appeared.

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/outbox.writer.test.ts`
Expected: FAIL — the writer still rejects non-`"raw"` topics via the old enum.

- [ ] **Step 3: Loosen the outbox schema**

Replace `src/outbox/schemas.ts` body with:

```ts
// src/outbox/schemas.ts
import { z } from "zod";

export const outboxTopicSchema = z
  .string()
  .min(1)
  .describe("Full Kafka topic name to publish to. Policy is enforced at config-validation time.");

export type OutboxTopic = z.infer<typeof outboxTopicSchema>;

export const outboxStatusSchema = z
  .enum(["pending", "dispatched", "failed"])
  .describe("Lifecycle state of an outbox row.");

export type OutboxStatus = z.infer<typeof outboxStatusSchema>;

export const outboxRowSchema = z.strictObject({
  id: z.string().min(1).describe("Row id (uuid v4)."),
  topic: outboxTopicSchema,
  message_key: z.string().describe("Kafka partition key."),
  payload: z.string().describe("Already JSON-stringified Kafka message value."),
  headers: z
    .string()
    .nullable()
    .describe("JSON-stringified record-headers object, or null when none."),
  status: outboxStatusSchema,
  attempts: z.number().int().nonnegative(),
  next_attempt_at: z.number().int().describe("Epoch ms; eligible when <= now()."),
  created_at: z.number().int(),
  dispatched_at: z.number().int().nullable(),
  last_error: z.string().nullable(),
});

export type OutboxRow = z.infer<typeof outboxRowSchema>;
```

`OutboxTopic` is now a `string` alias — that ripples through `writer.ts` and `drainer.ts` and is exactly what we want.

- [ ] **Step 4: Run writer tests**

Run: `bun test test/unit/outbox.writer.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: `drainer.ts` may show type errors on `topicToKafka`. Those are fixed in Task 6 — do not commit yet.

- [ ] **Step 6: Continue to Task 6 before committing.**

---

## Task 6: Drainer publishes to `row.topic` directly

**Files:**
- Modify: `src/outbox/drainer.ts`, `src/gateway/index.ts`
- Test: `test/unit/outbox.drainer.test.ts`

- [ ] **Step 1: Inspect the existing drainer test and identify required shape changes**

Run: `bun run --bun cat test/unit/outbox.drainer.test.ts | head -80`
(Read the test file to understand its current fixture shape.)

- [ ] **Step 2: Update `src/outbox/drainer.ts`**

Replace the top of the file through `topicToKafka` with:

```ts
// src/outbox/drainer.ts
import { getLogger } from "../logging/index.ts";
import { nextDelayMs } from "./backoff.ts";
import type { OutboxDatabase } from "./db.ts";

const log = getLogger("outbox.drainer");

export type DrainerProducer = {
  sendByTopic(
    topic: string,
    key: string,
    value: string,
    headers?: Record<string, string> | null,
  ): Promise<void>;
};

export type DrainerConfig = {
  batchSize: number;
  backoffMaxMs: number;
  maxAgeMs: number;
};

export type DrainerStartConfig = DrainerConfig & {
  idlePollMs: number;
  busyPollMs: number;
  backlogWarnThreshold: number;
};
```

Delete the `topicToKafka` helper entirely.

Inside `runOutboxIteration`, replace the line `const kafkaTopic = topicToKafka(row.topic as OutboxTopic, config.topics);` with:

```ts
    const kafkaTopic = row.topic;
```

Leave the rest of the function as is.

- [ ] **Step 3: Update `src/gateway/index.ts` to drop the `topics` arg and use a route-derived topic for the drainer**

In `src/gateway/index.ts`, change the `startDrainer` call so it no longer passes `topics`. Replace the `drainer = startDrainer({...})` block with:

```ts
  drainer = startDrainer({
    db: outboxDb,
    producer,
    config: {
      batchSize: config.outbox.batchSize,
      backoffMaxMs: config.outbox.backoffMaxMs,
      maxAgeMs: config.outbox.maxAgeHours * 60 * 60 * 1_000,
      idlePollMs: config.outbox.idlePollMs,
      busyPollMs: config.outbox.busyPollMs,
      backlogWarnThreshold: config.outbox.backlogWarnThreshold,
    },
  });
```

- [ ] **Step 4: Update `test/unit/outbox.drainer.test.ts`**

Find every usage of `topic: "raw"` and replace with `topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`. Find every drainer `config: { topics: { raw: ... }, ...}` and remove the `topics` field. The mock producer's `sendByTopic` first argument now is the full topic string — adjust any assertion that expected `"raw"` or a mapped Kafka name to expect `"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"` instead.

(If the test file is open, do a search-and-replace: `topic: "raw"` → `topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`, and `topics: { raw: "ops.elastic.autoops.raw.v1" },` → delete. Any string comparison against `"ops.elastic.autoops.raw.v1"` becomes `"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"`.)

- [ ] **Step 5: Run the drainer + writer tests**

Run: `bun test test/unit/outbox.drainer.test.ts test/unit/outbox.writer.test.ts`
Expected: PASS.

- [ ] **Step 6: Run the full test suite**

Run: `bun test`
Expected: PASS (route dispatch tests don't exist yet; we'll add them in Task 8).

- [ ] **Step 7: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 8: Commit Tasks 5 + 6 together**

```bash
git add src/outbox/schemas.ts src/outbox/writer.ts src/outbox/drainer.ts src/gateway/index.ts \
        test/unit/outbox.writer.test.ts test/unit/outbox.drainer.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: outbox stores full topic string; drainer publishes to row.topic

outboxTopicSchema becomes z.string(). Drainer drops the topicToKafka
enum mapping and publishes directly to the row's topic. DrainerConfig
no longer carries a topics map — the route layer owns topic identity.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Legacy outbox row backfill (idempotent migration)

**Why:** Existing deployments have rows with `topic = "raw"`. After the schema change, those rows must dispatch to `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`.

**Files:**
- Modify: `src/outbox/db.ts`
- Test: new test inside `test/unit/outbox.writer.test.ts` or a new file

- [ ] **Step 1: Write the failing migration test**

Create `test/unit/outbox.migration.test.ts`:

```ts
// test/unit/outbox.migration.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { closeOutbox, openOutbox, type OutboxDatabase } from "../../src/outbox/db.ts";

let db: OutboxDatabase;

afterEach(() => {
  if (db) closeOutbox(db);
});

describe("legacy topic backfill", () => {
  it("rewrites topic='raw' rows to T_PRIVATE_SOURCE_ELASTIC_AUTOOPS on open", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-1', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    closeOutbox(db);

    db = openOutbox(":memory:"); // re-open path runs migrations again
    // Re-insert because :memory: doesn't persist; instead, simulate a stale row
    // existing pre-migration in the same DB lifetime:
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-2', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );

    // The migration runs at openOutbox() — to test it deterministically we
    // call it explicitly. Expose a runMigrations function from db.ts.
    // Per the implementation step below.
    expect(true).toBe(true); // placeholder; replaced after Step 2
  });
});
```

Actually, since `:memory:` does not persist across `closeOutbox`/`openOutbox`, restructure the test to assert the migration runs against rows already present in the open DB. Replace the file with:

```ts
// test/unit/outbox.migration.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { closeOutbox, openOutbox, runOutboxMigrations, type OutboxDatabase } from "../../src/outbox/db.ts";

let db: OutboxDatabase;

afterEach(() => {
  if (db) closeOutbox(db);
});

describe("runOutboxMigrations", () => {
  it("rewrites topic='raw' rows to T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-1', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='legacy-1'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("is idempotent (running twice leaves rows unchanged)", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('legacy-2', 'raw', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='legacy-2'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
  });

  it("leaves non-legacy topic values untouched", () => {
    db = openOutbox(":memory:");
    db.run(
      `INSERT INTO outbox (id, topic, message_key, payload, status, attempts, next_attempt_at, created_at)
       VALUES ('new-1', 'T_PRIVATE_SOURCE_DATADOG_ALERTS', 'k', '{}', 'pending', 0, 0, 0)`,
    );
    runOutboxMigrations(db);
    const row = db.query("SELECT topic FROM outbox WHERE id='new-1'").get() as { topic: string };
    expect(row.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/outbox.migration.test.ts`
Expected: FAIL (`runOutboxMigrations` not exported).

- [ ] **Step 3: Add the migration to `src/outbox/db.ts`**

Replace the body of `src/outbox/db.ts` with:

```ts
// src/outbox/db.ts
import { Database, constants } from "bun:sqlite";

export type OutboxDatabase = Database;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS outbox (
  id              TEXT PRIMARY KEY,
  topic           TEXT NOT NULL,
  message_key     TEXT NOT NULL,
  payload         TEXT NOT NULL,
  headers         TEXT,
  status          TEXT NOT NULL DEFAULT 'pending',
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL,
  created_at      INTEGER NOT NULL,
  dispatched_at   INTEGER,
  last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_drain ON outbox (status, next_attempt_at);
`;

// Rewrites legacy rows written before the topic-naming policy was introduced.
// Pre-change deployments only had one route ("raw"), so the only possible
// legacy value is "raw" → T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.
export function runOutboxMigrations(db: OutboxDatabase): void {
  db.run(
    "UPDATE outbox SET topic = 'T_PRIVATE_SOURCE_ELASTIC_AUTOOPS' WHERE topic = 'raw'",
  );
}

export function openOutbox(dbPath: string): OutboxDatabase {
  const db = new Database(dbPath, { create: true, strict: true });
  if (dbPath !== ":memory:") {
    db.run("PRAGMA journal_mode = WAL;");
    db.run("PRAGMA synchronous = NORMAL;");
  }
  db.run("PRAGMA foreign_keys = ON;");
  db.exec(SCHEMA);
  runOutboxMigrations(db);
  return db;
}

export function closeOutbox(db: OutboxDatabase): void {
  try {
    db.fileControl(constants.SQLITE_FCNTL_PERSIST_WAL, 0);
    db.run("PRAGMA wal_checkpoint(TRUNCATE);");
  } catch {
    // file-mode-only operations; on :memory: these may throw, that's fine.
  }
  db.close(false);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/outbox.migration.test.ts`
Expected: PASS.

- [ ] **Step 5: Full test + typecheck**

Run: `bun test && bun run typecheck`
Expected: all PASS, typecheck clean.

- [ ] **Step 6: Commit**

```bash
git add src/outbox/db.ts test/unit/outbox.migration.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: backfill legacy outbox rows to renamed Elastic AutoOps topic

runOutboxMigrations rewrites the only possible legacy topic value
('raw') to T_PRIVATE_SOURCE_ELASTIC_AUTOOPS. Idempotent and runs at
every openOutbox(). New topic values are untouched.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Generic webhook handler factory

**Files:**
- Create: `src/gateway/handler.ts`
- Test: `test/unit/gateway.handler.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/gateway.handler.test.ts
import { describe, expect, it } from "bun:test";
import type { RouteConfig } from "../../src/config/schemas.ts";
import { makeWebhookHandler } from "../../src/gateway/handler.ts";

function fakeOutbox() {
  const calls: Array<{
    topic: string;
    messageKey: string;
    payload: string;
    headers: Record<string, string> | null;
  }> = [];
  return {
    enqueue: (row: { topic: string; messageKey: string; payload: string; headers: Record<string, string> | null }) => {
      calls.push(row);
    },
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
    calls,
  };
}

const baseRoute: RouteConfig = {
  name: "elastic-autoops",
  path: "/webhooks/elastic/autoops",
  topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
  keyFields: ["resourceId", "deployment-id"],
  idempotency: "elastic-autoops",
};

const noopProducer = {
  publishRaw: async () => {},
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};

function postJson(body: unknown): Request {
  return new Request("http://localhost/webhooks/elastic/autoops", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("makeWebhookHandler", () => {
  it("enqueues a row with the route's topic and the configured sourceHeader", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    const res = await handler(
      postJson({ resourceId: "dep-1", title: "t", status: "open" }),
    );
    expect(res.status).toBe(202);
    expect(outbox.calls).toHaveLength(1);
    expect(outbox.calls[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(outbox.calls[0]?.headers?.source).toBe("elastic-autoops");
    expect(typeof outbox.calls[0]?.headers?.idempotencyKey).toBe("string");
  });

  it("picks the partition key from configured keyFields in order", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    await handler(postJson({ "deployment-id": "fallback-key" }));
    expect(outbox.calls[0]?.messageKey).toBe("fallback-key");
  });

  it("returns 400 on non-JSON body", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(baseRoute, { producer: noopProducer, outbox });
    const res = await handler(
      new Request("http://localhost/x", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "not json",
      }),
    );
    expect(res.status).toBe(400);
    expect(outbox.calls).toHaveLength(0);
  });

  it("uses sourceHeader override when present", async () => {
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(
      { ...baseRoute, sourceHeader: "custom-source" },
      { producer: noopProducer, outbox },
    );
    await handler(postJson({ resourceId: "dep-1" }));
    expect(outbox.calls[0]?.headers?.source).toBe("custom-source");
  });

  it("omits idempotencyKey header when no strategy is configured", async () => {
    const { idempotency: _omit, ...routeWithoutIdem } = baseRoute;
    const outbox = fakeOutbox();
    const handler = makeWebhookHandler(routeWithoutIdem, { producer: noopProducer, outbox });
    await handler(postJson({ resourceId: "dep-1", title: "t", status: "open" }));
    expect(outbox.calls[0]?.headers?.idempotencyKey).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/gateway.handler.test.ts`
Expected: FAIL (`handler.ts` missing).

- [ ] **Step 3: Implement `src/gateway/handler.ts`**

```ts
// src/gateway/handler.ts
import type { RouteConfig } from "../config/schemas.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { resolveIdempotencyStrategy } from "./idempotencyStrategies.ts";

const log = getLogger("gateway.handler");

export type HandlerDeps = {
  producer: Pick<EventProducer, "publishRaw">;
  outbox?: Pick<OutboxWriter, "enqueue">;
};

function pickKey(body: unknown, fields: readonly string[]): string {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "unkeyed";
  const b = body as Record<string, unknown>;
  for (const k of fields) {
    const v = b[k];
    if (typeof v === "string" && v.length > 0) return v;
  }
  return "unkeyed";
}

export function makeWebhookHandler(route: RouteConfig, deps: HandlerDeps) {
  const { producer, outbox } = deps;
  const sourceHeader = route.sourceHeader ?? route.name;
  const strategy = resolveIdempotencyStrategy(route.idempotency);

  return async function handler(req: Request): Promise<Response> {
    let body: unknown;
    try {
      body = await req.json();
    } catch {
      return Response.json(
        { accepted: false, error: "invalid JSON body" },
        { status: 400 },
      );
    }

    const messageKey = pickKey(body, route.keyFields);
    const idempotencyKey = strategy ? strategy(body) : undefined;
    const headers: Record<string, string> = { source: sourceHeader };
    if (idempotencyKey) headers.idempotencyKey = idempotencyKey;

    const payload = JSON.stringify({
      receivedAt: new Date().toISOString(),
      raw: body,
    });

    if (outbox) {
      try {
        outbox.enqueue({
          topic: route.topic,
          messageKey,
          payload,
          headers,
        });
      } catch (err) {
        log.error(
          { err, route: route.name, topic: route.topic, messageKey },
          "outbox enqueue failed",
        );
        return Response.json(
          { accepted: false, error: "outbox enqueue failed" },
          { status: 500 },
        );
      }
    } else {
      try {
        await producer.publishRaw(messageKey, body);
      } catch (err) {
        log.warn(
          { err, route: route.name, topic: route.topic, messageKey },
          "kafka publish failed; will not retry without outbox",
        );
      }
    }

    return Response.json({ accepted: true }, { status: 202 });
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/gateway.handler.test.ts`
Expected: PASS.

- [ ] **Step 5: Typecheck**

Run: `bun run typecheck`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/gateway/handler.ts test/unit/gateway.handler.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: generic webhook handler factory parameterised on RouteConfig

makeWebhookHandler(route, deps) carries the existing per-request
behaviour: parse JSON, derive idempotency via the named strategy,
pick partition key, enqueue to outbox with route.topic. Same shape
as the current handler in routes.ts; route-specific fields injected.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: `routes.ts` iterates `config.routes`

**Files:**
- Modify: `src/gateway/routes.ts`
- Test: `test/unit/gateway.routes.dispatch.test.ts`

- [ ] **Step 1: Write the failing dispatch test**

```ts
// test/unit/gateway.routes.dispatch.test.ts
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetConfigCache } from "../../src/config/loader.ts";
import { buildRoutes } from "../../src/gateway/routes.ts";

const noopProducer = {
  publishRaw: async () => {},
  isConnected: () => true,
  disconnect: async () => {},
  sendByTopic: async () => {},
};

function fakeOutbox() {
  const calls: Array<{ topic: string; messageKey: string }> = [];
  return {
    enqueue: (row: { topic: string; messageKey: string }) => calls.push(row),
    backlogStats: () => ({ pending: 0, failed: 0, oldestPendingAgeMs: 0 }),
    calls,
  };
}

let snapshot: NodeJS.ProcessEnv;

beforeEach(() => {
  snapshot = { ...process.env };
  process.env = {
    ...process.env,
    ENVIRONMENT: "dev",
    KAFKA_PROVIDER: "local",
    ROUTES_JSON: JSON.stringify([
      {
        name: "elastic-autoops",
        path: "/webhooks/elastic/autoops",
        topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS",
        keyFields: ["resourceId"],
        idempotency: "elastic-autoops",
      },
      {
        name: "datadog-alerts",
        path: "/webhooks/datadog/alerts",
        topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS",
        keyFields: ["alert_id"],
      },
    ]),
  };
  resetConfigCache();
});

afterEach(() => {
  process.env = snapshot;
  resetConfigCache();
});

describe("buildRoutes with multiple routes", () => {
  it("registers a POST handler per configured route", () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    expect(routes["/webhooks/elastic/autoops"]).toBeDefined();
    expect(routes["/webhooks/datadog/alerts"]).toBeDefined();
    expect(routes["/healthz"]).toBeDefined();
  });

  it("dispatches each route to its own topic", async () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });

    const r1 = routes["/webhooks/elastic/autoops"] as { POST: (req: Request) => Promise<Response> };
    await r1.POST(
      new Request("http://localhost/webhooks/elastic/autoops", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ resourceId: "dep-1" }),
      }),
    );

    const r2 = routes["/webhooks/datadog/alerts"] as { POST: (req: Request) => Promise<Response> };
    await r2.POST(
      new Request("http://localhost/webhooks/datadog/alerts", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ alert_id: "a-1" }),
      }),
    );

    expect(outbox.calls).toHaveLength(2);
    expect(outbox.calls[0]?.topic).toBe("T_PRIVATE_SOURCE_ELASTIC_AUTOOPS");
    expect(outbox.calls[0]?.messageKey).toBe("dep-1");
    expect(outbox.calls[1]?.topic).toBe("T_PRIVATE_SOURCE_DATADOG_ALERTS");
    expect(outbox.calls[1]?.messageKey).toBe("a-1");
  });

  it("healthz reports producer + outbox status", () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts`
Expected: FAIL — routes.ts still has the hardcoded `/webhooks/elastic/autoops` entry.

- [ ] **Step 3: Replace `src/gateway/routes.ts`**

```ts
// src/gateway/routes.ts
import { config } from "../config/index.ts";
import type { EventProducer } from "../kafka/producer.ts";
import { getLogger } from "../logging/index.ts";
import type { OutboxWriter } from "../outbox/writer.ts";
import { makeWebhookHandler } from "./handler.ts";

const log = getLogger("gateway.routes");

export type RouteDeps = {
  producer: EventProducer;
  outbox?: OutboxWriter;
};

type RoutesMap = Record<string, unknown>;

export function buildRoutes(deps: RouteDeps): RoutesMap {
  const { producer, outbox } = deps;

  const routes: RoutesMap = {
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
  };

  for (const route of config.routes) {
    routes[route.path] = { POST: makeWebhookHandler(route, { producer, outbox }) };
    log.info(
      {
        route: route.name,
        path: route.path,
        topic: route.topic,
        dlqTopic: route.dlqTopic,
        idempotency: route.idempotency,
      },
      "route registered",
    );
  }

  return routes;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `bun test && bun run typecheck`
Expected: PASS, typecheck clean.

- [ ] **Step 6: Commit**

```bash
git add src/gateway/routes.ts test/unit/gateway.routes.dispatch.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: gateway routes iterate config.routes at startup

Each configured route registers a POST handler built via
makeWebhookHandler. Startup logs the full (name, path, topic,
dlqTopic) tuple per route so operators see the wiring.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Delete `src/gateway/idempotencyKey.ts` re-imports from `routes.ts`

**Why:** After Task 9, `routes.ts` no longer imports from `idempotencyKey.ts`. Confirm and clean up.

**Files:**
- Modify (verify only): `src/gateway/routes.ts`, `src/gateway/idempotencyKey.ts`

- [ ] **Step 1: Verify there are no leftover imports of `idempotencyKey` from `routes.ts`**

Run: `grep -n idempotencyKey src/gateway/routes.ts || echo "clean"`
Expected: `clean`.

- [ ] **Step 2: Verify `idempotencyKey.ts` is still imported only from `idempotencyStrategies.ts`**

Run: `grep -rn "from.*idempotencyKey" src test`
Expected: a single line in `src/gateway/idempotencyStrategies.ts`.

- [ ] **Step 3: No commit needed** — Task 9 already covered this cleanup. If there is leftover dead code, remove it now and commit:

```bash
# only if there are leftovers; otherwise skip
git commit --allow-empty -m "SIO-XXX: noop verification — idempotencyKey isolated to strategies registry"
```

(Skip the commit if the verification passes — there is nothing to commit.)

---

## Task 11: Health check surfaces all registered routes

**Files:**
- Modify: `src/gateway/routes.ts`
- Test: extend `test/unit/gateway.routes.dispatch.test.ts`

- [ ] **Step 1: Extend the dispatch test**

Add this `it` block inside `describe("buildRoutes with multiple routes", ...)`:

```ts
  it("healthz includes the registered routes", () => {
    const outbox = fakeOutbox();
    const routes = buildRoutes({ producer: noopProducer, outbox });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    // Response.json() returns a body we need to await:
    return res.json().then((payload: { routes?: Array<{ name: string; topic: string }> }) => {
      expect(payload.routes).toEqual([
        { name: "elastic-autoops", path: "/webhooks/elastic/autoops", topic: "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", dlqTopic: undefined },
        { name: "datadog-alerts", path: "/webhooks/datadog/alerts", topic: "T_PRIVATE_SOURCE_DATADOG_ALERTS", dlqTopic: undefined },
      ]);
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts -t "healthz includes"`
Expected: FAIL (no `routes` field in healthz response).

- [ ] **Step 3: Update `/healthz` in `src/gateway/routes.ts`**

Inside `buildRoutes`, change the `/healthz` handler to include a route summary. Replace the existing handler:

```ts
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
          routes: config.routes.map((r) => ({
            name: r.name,
            path: r.path,
            topic: r.topic,
            dlqTopic: r.dlqTopic,
          })),
        },
        { status: producerOk ? 200 : 503 },
      );
    },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts -t "healthz includes"`
Expected: PASS.

- [ ] **Step 5: Full suite + typecheck**

Run: `bun test && bun run typecheck`
Expected: PASS, clean.

- [ ] **Step 6: Commit**

```bash
git add src/gateway/routes.ts test/unit/gateway.routes.dispatch.test.ts
git commit -m "$(cat <<'EOF'
SIO-XXX: /healthz reports registered routes

Surfaces the (name, path, topic, dlqTopic) tuple for every route so
operators can confirm wiring without grepping logs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Startup banner enumerates routes (already done in Task 9, verify)

- [ ] **Step 1: Confirm Task 9 left `route registered` log per route**

Run: `grep -n 'route registered' src/gateway/routes.ts`
Expected: a single match inside `buildRoutes`.

If missing, re-add it per Task 9 Step 3. No commit needed otherwise.

---

## Task 13: Local smoke test against Redpanda

**Files:**
- Modify: `docs/development/getting-started.md`

- [ ] **Step 1: Provision both topics in Redpanda**

Run:
```bash
docker compose up -d
docker compose exec redpanda rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
docker compose exec redpanda rpk topic create T_PRIVATE_SOURCE_EXAMPLE_EVENTS
docker compose exec redpanda rpk topic list
```
Expected: both topics listed.

- [ ] **Step 2: Start the gateway with a two-route override**

Run (in one terminal):
```bash
ROUTES_JSON='[
  {"name":"elastic-autoops","path":"/webhooks/elastic/autoops","topic":"T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","dlqTopic":"DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS","keyFields":["resourceId","deployment-id"],"idempotency":"elastic-autoops"},
  {"name":"example-events","path":"/webhooks/example/events","topic":"T_PRIVATE_SOURCE_EXAMPLE_EVENTS","keyFields":["id"]}
]' bun run dev:gateway
```
Expected: two `route registered` log lines.

- [ ] **Step 3: Send a message to each route**

In another terminal:
```bash
curl -s -X POST -H 'content-type: application/json' \
  -d '{"resourceId":"dep-1","title":"t","status":"open"}' \
  http://localhost:3000/webhooks/elastic/autoops

curl -s -X POST -H 'content-type: application/json' \
  -d '{"id":"abc-1","whatever":"value"}' \
  http://localhost:3000/webhooks/example/events
```
Expected: both return `{"accepted":true}`.

- [ ] **Step 4: Verify both topics received messages**

Run:
```bash
docker compose exec redpanda rpk topic consume T_PRIVATE_SOURCE_ELASTIC_AUTOOPS -n 1
docker compose exec redpanda rpk topic consume T_PRIVATE_SOURCE_EXAMPLE_EVENTS -n 1
```
Expected: one record on each topic.

- [ ] **Step 5: Verify `/healthz` reflects both routes**

Run:
```bash
curl -s http://localhost:3000/healthz | jq .routes
```
Expected: two-element array with both routes.

- [ ] **Step 6: Negative-path smoke — invalid topic at startup**

Stop the gateway (Ctrl-C). Start with a forbidden topic:
```bash
ROUTES_JSON='[{"name":"bad","path":"/x","topic":"T_PRIVATE_SINK_DATADOG_ALERTS","keyFields":["id"]}]' bun run dev:gateway
```
Expected: process exits with a Zod error message mentioning "sink connector". Exit code non-zero.

- [ ] **Step 7: Update `docs/development/getting-started.md`**

Append (or extend the existing smoke section with) a two-route example documenting the steps above. Locate the existing smoke-test section and add a new subsection titled `### Two-route smoke (config-driven)` containing the commands from Steps 1-6. Keep the existing single-route smoke section intact; this is an addition, not a replacement.

- [ ] **Step 8: Stop services**

Run:
```bash
docker compose down
```

- [ ] **Step 9: Commit doc updates**

```bash
git add docs/development/getting-started.md
git commit -m "$(cat <<'EOF'
SIO-XXX: document two-route smoke flow

Adds a working two-route example to the getting-started guide so the
config-driven path is exercised manually before any consumer migration.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Update CLAUDE.md to reflect the new contract

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the "Contract" section**

Replace the current "Contract" paragraph with:

```markdown
## Contract

Gateway accepts any valid JSON POST to each configured route path and writes
it to that route's Kafka topic via the SQLite outbox. The set of routes lives
in `config.routes[]` (defaults in `src/config/defaults.ts`, overridable via
`ROUTES_JSON`). Every route topic must follow the
`T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>` naming policy; companion DLQs follow
`DLQ_T_<topic>` but are never written by the gateway. Non-JSON bodies get 400;
everything else gets 202. The gateway does not validate any webhook schema,
does not normalize, does not write `events.v1` or `dlq.v1`. Downstream
consumers in other services own those concerns.
```

- [ ] **Step 2: Update the "Kafka topics" subsection**

Replace it with:

```markdown
### Kafka topics

Topics are declared per route in `config.routes[]` and validated at startup
against the org-wide naming policy.

- **Gateway-owned** (only this prefix is legal): `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>`. The Elastic AutoOps seed route uses `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`.
- **Optional companion DLQ**: `DLQ_T_<topic>`. Declared on the route via `dlqTopic` so downstream consumers can introspect it; the gateway itself never publishes here.
- **Forbidden**: `T_PUBLIC_*`, `T_PRIVATE_SINK_*`, `T_PRIVATE_*_RICH_NOTIFICATIONS`, `T_PRIVATE_*_EVENTS`, `DLQ_T_*` (as the primary topic), and Kafka/Confluent system prefixes. Each is rejected at config-validation time with a distinct error message.

Adding a route: edit `defaults.ts` (PR) or set `ROUTES_JSON` (env). No
TypeScript change unless the new source needs a custom idempotency strategy
(`src/gateway/idempotencyStrategies.ts`).
```

- [ ] **Step 3: Update the "Architecture" file tree**

In the tree, replace the `gateway/` block with:

```
  gateway/
    index.ts              entry point — wires KafkaProvider + EventProducer + outbox
    routes.ts             iterates config.routes; one POST handler per route
    handler.ts            makeWebhookHandler(route, deps) — per-request logic
    idempotencyKey.ts     opportunistic sha256 header for AutoOps-shaped bodies
    idempotencyStrategies.ts  named registry of (body) => key functions
```

And under `config/`, add the line:

```
    topicPolicy.ts          gateway topic naming policy (T_PRIVATE_SOURCE_*)
```

- [ ] **Step 4: Update "Config shape (4-pillar)"**

Append to the existing config shape block:

```
config.routes[].{name, path, topic, dlqTopic?, sourceHeader?, keyFields, idempotency?}
```

- [ ] **Step 5: Update "Out of scope"**

Add to the existing Out-of-scope list:

```
parametric paths (/webhooks/:vendor/:product), topic-name templates, hot
reload of routes (server.reload()), per-route auth/validation/normalization/
response shaping, a mounted ROUTES_FILE source
```

- [ ] **Step 6: Run typecheck + tests (sanity)**

Run: `bun test && bun run typecheck`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
SIO-XXX: update CLAUDE.md for config-driven routes contract

New contract section, kafka topics policy, routes config shape, and
out-of-scope additions reflecting the implementation.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: Pre-PR checks

- [ ] **Step 1: Full clean run**

Run:
```bash
bun test
bun run typecheck
```
Expected: both clean.

- [ ] **Step 2: Prod-safety probes (from CLAUDE.md)**

Run each and confirm Zod failures:

```bash
ENVIRONMENT=prod KAFKA_PROVIDER=local bun run start:gateway
# Expect: error about provider=local not allowed in prod

ENVIRONMENT=dev KAFKA_PROVIDER=msk bun run start:gateway
# Expect: error about msk.region required

ENVIRONMENT=dev KAFKA_PROVIDER=confluent bun run start:gateway
# Expect: error about confluent.bootstrapServers / apiKey / apiSecret required
```

Add a new check specific to this PR:

```bash
ROUTES_JSON='[{"name":"bad","path":"/x","topic":"T_PRIVATE_SINK_X_Y","keyFields":["id"]}]' \
  ENVIRONMENT=dev KAFKA_PROVIDER=local bun run start:gateway
# Expect: error mentioning "sink connector"

ROUTES_JSON='[{"name":"bad","path":"/x","topic":"T_PRIVATE_SOURCE_X_Y","dlqTopic":"DLQ_T_WRONG","keyFields":["id"]}]' \
  ENVIRONMENT=dev KAFKA_PROVIDER=local bun run start:gateway
# Expect: error mentioning "dlqTopic must be"
```

All five probes must fail at startup with the expected Zod messages.

- [ ] **Step 3: Inspect the route-registration log**

Run with the default config and capture the first 30 lines:
```bash
ENVIRONMENT=dev KAFKA_PROVIDER=local bun run start:gateway 2>&1 | head -30
```
Expected: one `route registered` line for `elastic-autoops` with topic `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` and dlqTopic `DLQ_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`. Stop the gateway.

- [ ] **Step 4: Create the Linear issue (per CLAUDE.md)**

Create an issue in the **Event Gate** Linear project titled `Config-driven webhook routes + topic naming policy` with the spec link in the description and the acceptance criteria from the spec copied verbatim. Assign to Simon Owusu. Status: In Progress. Capture the SIO-XXX number and update the placeholder commit messages above to use the real ticket number for any commits not yet pushed.

(If commits are already made with `SIO-XXX`, rewrite the local branch only — never amend already-pushed commits.)

- [ ] **Step 5: Open the PR**

Coordinate with downstream consumers of `ops.elastic.autoops.raw.v1` before opening (per the spec's Rollout step 2). Then push and open the PR. Body:

```
## Summary
- Routes are now data-driven via config.routes[] (with ROUTES_JSON env override)
- Topic naming policy (T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>) enforced at startup
- Elastic AutoOps topic renamed: ops.elastic.autoops.raw.v1 -> T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
- DLQ companion (DLQ_T_<topic>) declared per route but never written by the gateway
- Outbox stores full topic string per row; drainer publishes to row.topic directly

## Test plan
- [ ] `bun test` clean
- [ ] `bun run typecheck` clean
- [ ] Two-route smoke against local Redpanda (see docs/development/getting-started.md)
- [ ] Negative startup tests: forbidden prefixes (T_PUBLIC_*, T_PRIVATE_SINK_*, DLQ_T_*) reject
- [ ] Negative startup tests: dlqTopic mismatch rejects
- [ ] Downstream consumers confirmed reading from new topic before merge
```

---

## Self-Review (against the spec)

**Spec coverage** — checked against `docs/superpowers/specs/2026-05-19-config-driven-routes-design.md`:

| Spec section | Covered by |
|---|---|
| Goal: per-route data-driven config | Tasks 3, 4, 9 |
| Naming policy regex | Tasks 1, 3 |
| Forbidden prefixes with distinct messages | Tasks 1, 3 |
| DLQ optional + must equal `DLQ_T_<topic>` | Tasks 3 |
| `routes` non-empty, unique path/topic/dlqTopic | Task 3 |
| Idempotency strategies registry | Task 2 |
| `ROUTES_JSON` env override (replace semantics) | Task 4 |
| Outbox topic becomes `z.string()` | Task 5 |
| Drainer publishes to `row.topic` directly | Task 6 |
| Legacy `topic = "raw"` backfill | Task 7 |
| Generic `makeWebhookHandler` | Task 8 |
| `routes.ts` iterates `config.routes` | Task 9 |
| Startup log enumerates routes | Tasks 9, 12 |
| `/healthz` surfaces (topic, dlqTopic) | Task 11 |
| Two-route smoke documented | Task 13 |
| CLAUDE.md updated | Task 14 |
| Acceptance criteria probes | Task 15 |

**Placeholder scan:** Searched for "TBD", "TODO", "implement later", "fill in details", "add appropriate error handling", "similar to Task N", "write tests for the above". None present. Every code step contains the actual code.

**Type consistency:**
- `RouteConfig` is exported from `src/config/schemas.ts` (Task 3) and consumed in `handler.ts` (Task 8), `routes.ts` (Task 9), tests (Tasks 8, 9, 11). Same name everywhere.
- `IdempotencyStrategy = (body: unknown) => string | undefined` defined in Task 2 matches usage in Task 8 via `resolveIdempotencyStrategy`.
- `OutboxTopic` becomes a `string` alias in Task 5 — no callers reference it as an enum afterwards (Tasks 5, 6 explicitly remove enum mappings).
- `DrainerConfig` shape change: `topics: { raw }` removed in Task 6, both in the type and at the call site in `src/gateway/index.ts` (Task 6 Step 3) and in the test in Task 6 Step 4.
- `makeWebhookHandler(route, deps)` signature is identical in Task 8 (creation) and Task 9 (consumption).
- `runOutboxMigrations(db)` exported from `src/outbox/db.ts` in Task 7 — sole call site is `openOutbox` after Task 7 Step 3.

Plan complete and saved to [docs/superpowers/plans/2026-05-19-config-driven-routes.md](docs/superpowers/plans/2026-05-19-config-driven-routes.md).
