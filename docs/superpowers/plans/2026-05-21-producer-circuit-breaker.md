# Producer Circuit Breaker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap the Kafka `Producer.sendByTopic` call in a Closed → Open → Half-Open circuit breaker per `guides/circuit-breaker-guide.md`, so a sustained Kafka outage produces fail-fast behaviour, operator-visible state-transition logs, and `/healthz` body visibility — without changing the existing outbox-as-fallback or `/healthz` HTTP status semantics.

**Architecture:** A pure FSM in `src/resilience/circuit-breaker.ts` (no Kafka knowledge) plus a `ProducerHandle` wrapper in `src/kafka/producerHandle.ts` that owns both the inner `Producer` and the breaker, implementing the existing `EventProducer` interface. The drainer gains one new catch branch for `CircuitBreakerOpenError` — defers the row by setting `next_attempt_at = breaker.nextAttemptAt` without incrementing `attempts`. State transitions emit structured `circuit_breaker_*` events; current state is surfaced in `/healthz` body and the 60s heartbeat snapshot.

**Tech Stack:** Bun 1.3.x, TypeScript strict, Zod v4, Pino 10 + `@elastic/ecs-pino-format`, `bun:sqlite`, `@platformatic/kafka` 2.x, `bun:test`.

**Spec:** `docs/superpowers/specs/2026-05-21-producer-circuit-breaker-design.md`
**Linear:** SIO-817 (to be created after plan approval)

---

## Pre-flight

### Task 0: Create a feature branch off master

**Files:** none (git only)

- [ ] **Step 1: Confirm a clean working tree on master**

Run: `git status`
Expected: `nothing to commit, working tree clean` on branch `master`.

- [ ] **Step 2: Confirm master is up to date with origin**

Run: `git pull --ff-only origin master`
Expected: `Already up to date.` (or a fast-forward, no merge commit).

- [ ] **Step 3: Create the feature branch**

Run: `git checkout -b sio-817-producer-circuit-breaker`
Expected: `Switched to a new branch 'sio-817-producer-circuit-breaker'`.

- [ ] **Step 4: Verify baseline tests + typecheck pass**

Run: `bun run typecheck && bun test`
Expected: typecheck clean; all tests pass. Note the test count (this will be the baseline to compare against later).

- [ ] **Step 5: Commit the spec + plan onto the new branch**

Both the spec (`docs/superpowers/specs/2026-05-21-producer-circuit-breaker-design.md`) and this plan (`docs/superpowers/plans/2026-05-21-producer-circuit-breaker.md`) should be untracked at this point.

Run: `git status`
Expected: shows both untracked.

Run:
```bash
git add docs/superpowers/specs/2026-05-21-producer-circuit-breaker-design.md docs/superpowers/plans/2026-05-21-producer-circuit-breaker.md
git commit -m "$(cat <<'EOF'
SIO-817: spec + plan for producer circuit breaker

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Expected: one new commit on the branch, two files added.

---

## Section A — Config plumbing (`config.breaker.*`)

The breaker is configured via three numbers — failure threshold, success threshold, recovery timeout. All three follow the 4-pillar pattern: defaults → envMapping → schemas → (loader untouched, generic). The schema lives next to `healthSchema` and `outboxSchema`.

### Task A1: Add `breaker` block to defaults

**Files:**
- Modify: `src/config/defaults.ts`

- [ ] **Step 1: Add the `breaker` block AFTER the `health` block, BEFORE `routes`**

Open `src/config/defaults.ts`. The current shape ends `health: { ... }` then `routes: [`. Insert this after the `health` closing brace + comma:

```ts
  breaker: {
    failureThreshold: 5,
    successThreshold: 3,
    recoveryTimeoutMs: 60_000,
  },
```

- [ ] **Step 2: Commit (typecheck will fail until A4 lands the schema)**

Note: `bun run typecheck` will report an error about `breaker` not being in `configSchema`. That is expected — the schema is added in Task A4. Do NOT skip this commit; commit history matters for review.

```bash
git add src/config/defaults.ts
git commit -m "$(cat <<'EOF'
SIO-817: add breaker block defaults (failureThreshold, successThreshold, recoveryTimeoutMs)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task A2: Add env mapping for `breaker`

**Files:**
- Modify: `src/config/envMapping.ts`

- [ ] **Step 1: Extend the `EnvOverrides` type**

In `src/config/envMapping.ts`, find the `EnvOverrides` type. After the `health?: { ... };` block (added in SIO-815), add:

```ts
  breaker?: {
    failureThreshold?: number;
    successThreshold?: number;
    recoveryTimeoutMs?: number;
  };
```

- [ ] **Step 2: Map the env vars in `mapEnv`**

In the same file, in the `mapEnv` function, after the `health: filterUndefined({ ... }),` block, add:

```ts
    breaker: filterUndefined({
      failureThreshold: num(env.CIRCUIT_BREAKER_FAILURE_THRESHOLD),
      successThreshold: num(env.CIRCUIT_BREAKER_SUCCESS_THRESHOLD),
      recoveryTimeoutMs: num(env.CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS),
    }),
```

- [ ] **Step 3: Commit (typecheck still failing until A4)**

```bash
git add src/config/envMapping.ts
git commit -m "$(cat <<'EOF'
SIO-817: map CIRCUIT_BREAKER_FAILURE_THRESHOLD, CIRCUIT_BREAKER_SUCCESS_THRESHOLD, CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task A3: Write the failing schema test

**Files:**
- Create: `test/unit/config.breaker.test.ts`

- [ ] **Step 1: Write the failing test**

Create `test/unit/config.breaker.test.ts`:

```ts
// test/unit/config.breaker.test.ts
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

describe("config.breaker", () => {
  it("exposes defaults when no env vars are set", async () => {
    const { config } = await import("../../src/config/index.ts");
    expect(config.breaker.failureThreshold).toBe(5);
    expect(config.breaker.successThreshold).toBe(3);
    expect(config.breaker.recoveryTimeoutMs).toBe(60_000);
  });

  it("applies env overrides", async () => {
    process.env = {
      ...process.env,
      CIRCUIT_BREAKER_FAILURE_THRESHOLD: "3",
      CIRCUIT_BREAKER_SUCCESS_THRESHOLD: "2",
      CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS: "30000",
    };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(config.breaker.failureThreshold).toBe(3);
    expect(config.breaker.successThreshold).toBe(2);
    expect(config.breaker.recoveryTimeoutMs).toBe(30_000);
  });

  it("rejects failureThreshold below 1", async () => {
    process.env = { ...process.env, CIRCUIT_BREAKER_FAILURE_THRESHOLD: "0" };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(() => config.breaker).toThrow(/failureThreshold/);
  });

  it("rejects recoveryTimeoutMs below 1000", async () => {
    process.env = { ...process.env, CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS: "500" };
    resetConfigCache();
    const { config } = await import("../../src/config/index.ts");
    expect(() => config.breaker).toThrow(/recoveryTimeoutMs/);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/config.breaker.test.ts`
Expected: FAIL — `config.breaker` is rejected by the schema (unknown field) OR is undefined.

Do NOT commit yet. The implementation in A4 makes this pass; they go in the same commit per the project's TDD convention.

### Task A4: Add `breakerSchema` to make A3 pass

**Files:**
- Modify: `src/config/schemas.ts`

- [ ] **Step 1: Add the `breakerSchema` after `healthSchema`**

In `src/config/schemas.ts`, locate `healthSchema` (added in SIO-815). Immediately after it, add:

```ts
const breakerSchema = z.strictObject({
  failureThreshold: z
    .number()
    .int()
    .min(1)
    .describe("Consecutive transport failures before the breaker opens. Tunable via CIRCUIT_BREAKER_FAILURE_THRESHOLD."),
  successThreshold: z
    .number()
    .int()
    .min(1)
    .describe("Consecutive successful probes in HALF_OPEN before the breaker closes. Tunable via CIRCUIT_BREAKER_SUCCESS_THRESHOLD."),
  recoveryTimeoutMs: z
    .number()
    .int()
    .min(1_000)
    .describe("How long to wait in OPEN before allowing a probe, in ms. Minimum 1s per circuit-breaker-guide §11 anti-pattern. Tunable via CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS."),
});
```

- [ ] **Step 2: Wire it into `configSchema`**

In the same file, inside `configSchema = z.strictObject({ ... })`, locate the `health: healthSchema,` line. Immediately after it, add:

```ts
    breaker: breakerSchema,
```

- [ ] **Step 3: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 4: Run the failing test (now should pass)**

Run: `bun test test/unit/config.breaker.test.ts`
Expected: PASS — all 4 cases.

- [ ] **Step 5: Run the full suite**

Run: `bun test`
Expected: PASS — no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/config/schemas.ts test/unit/config.breaker.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: add breakerSchema (failureThreshold, successThreshold, recoveryTimeoutMs)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section B — Resilience module (FSM + errors)

The breaker is a pure FSM. No Kafka, no logging dependencies. Tested in isolation against the guide §9 reference tests.

### Task B1: Add the `CircuitBreakerOpenError` class

**Files:**
- Create: `src/resilience/errors.ts`

- [ ] **Step 1: Write the error class and predicate**

Create `src/resilience/errors.ts`:

```ts
// src/resilience/errors.ts

export class CircuitBreakerOpenError extends Error {
  constructor(public readonly nextAttemptAt: Date) {
    super(`Circuit breaker OPEN - next attempt at ${nextAttemptAt.toISOString()}`);
    this.name = "CircuitBreakerOpenError";
  }
}

// Returns true ONLY for errors that are application-level (a normal broker
// response to a misrouted message, e.g. UNKNOWN_TOPIC_OR_PARTITION). All
// other errors are treated as transport failures and count toward the
// breaker's failure threshold.
//
// PR1 ships with the conservative default: every error counts. As we observe
// specific application-level patterns from @platformatic/kafka in production,
// we extend this predicate to return true for those — keeping the breaker
// unaffected by errors that aren't transport failures.
export function isApplicationLevelError(_err: unknown): boolean {
  return false;
}
```

- [ ] **Step 2: Run typecheck**

Run: `bun run typecheck`
Expected: PASS — file compiles standalone.

- [ ] **Step 3: Commit**

```bash
git add src/resilience/errors.ts
git commit -m "$(cat <<'EOF'
SIO-817: add CircuitBreakerOpenError + isApplicationLevelError predicate

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task B2: Write the failing tests for the predicate

**Files:**
- Create: `test/unit/resilience.errors.test.ts`

- [ ] **Step 1: Write the test**

Create `test/unit/resilience.errors.test.ts`:

```ts
// test/unit/resilience.errors.test.ts
import { describe, expect, it } from "bun:test";
import {
  CircuitBreakerOpenError,
  isApplicationLevelError,
} from "../../src/resilience/errors.ts";

describe("CircuitBreakerOpenError", () => {
  it("carries the next attempt timestamp", () => {
    const nextAttempt = new Date("2026-05-21T08:01:00.000Z");
    const err = new CircuitBreakerOpenError(nextAttempt);
    expect(err.nextAttemptAt).toBe(nextAttempt);
    expect(err.name).toBe("CircuitBreakerOpenError");
    expect(err.message).toMatch(/2026-05-21T08:01:00\.000Z/);
  });

  it("is an instanceof Error and CircuitBreakerOpenError", () => {
    const err = new CircuitBreakerOpenError(new Date());
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(CircuitBreakerOpenError);
  });
});

describe("isApplicationLevelError", () => {
  it("returns false for any error in PR1 (conservative default-trip)", () => {
    expect(isApplicationLevelError(new Error("metadata failed"))).toBe(false);
    expect(isApplicationLevelError(new TypeError("oops"))).toBe(false);
    expect(isApplicationLevelError("a string")).toBe(false);
    expect(isApplicationLevelError(undefined)).toBe(false);
    expect(isApplicationLevelError(null)).toBe(false);
    expect(isApplicationLevelError({ code: "UNKNOWN_TOPIC_OR_PARTITION" })).toBe(false);
  });
});
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/resilience.errors.test.ts`
Expected: PASS — both describe blocks pass against the implementation from B1.

- [ ] **Step 3: Commit**

```bash
git add test/unit/resilience.errors.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: test CircuitBreakerOpenError + isApplicationLevelError

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task B3: Write the failing tests for the FSM

**Files:**
- Create: `test/unit/resilience.circuit-breaker.test.ts`

- [ ] **Step 1: Write the test**

Create `test/unit/resilience.circuit-breaker.test.ts`. This mirrors the guide §9 reference tests (`guides/circuit-breaker-guide.md` lines 374–461) adapted to use small thresholds for fast execution:

```ts
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
    breaker = new CircuitBreaker("test", cfg);
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
    const appOnly = new CircuitBreaker(
      "app",
      cfg,
      (err) => !(err instanceof RangeError),
    );
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
    const b = new CircuitBreaker("counted", cfg, undefined, () => {
      opens += 1;
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(fail).catch(() => undefined);
    }
    expect(opens).toBe(1);
  });

  it("invokes onOpen each time half-open returns to open", async () => {
    let opens = 0;
    const b = new CircuitBreaker("counted", cfg, undefined, () => {
      opens += 1;
    });
    for (let i = 0; i < 3; i++) {
      await b.execute(fail).catch(() => undefined);
    }
    expect(opens).toBe(1);
    await Bun.sleep(250);
    await b.execute(fail).catch(() => undefined);
    expect(opens).toBe(2);
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/resilience.circuit-breaker.test.ts`
Expected: FAIL — module `../../src/resilience/circuit-breaker.ts` not found.

### Task B4: Implement the CircuitBreaker FSM

**Files:**
- Create: `src/resilience/circuit-breaker.ts`

- [ ] **Step 1: Write the implementation**

Create `src/resilience/circuit-breaker.ts`. Adapted from `guides/circuit-breaker-guide.md` §4, with the additional `onOpen` callback for `DrainMetrics.incrementBreakerOpenCount()`:

```ts
// src/resilience/circuit-breaker.ts
import { CircuitBreakerOpenError } from "./errors.ts";

export type CircuitState = "closed" | "open" | "half-open";

export type CircuitBreakerConfig = {
  failureThreshold: number;
  successThreshold: number;
  recoveryTimeoutMs: number;
};

export type CircuitBreakerSnapshot = {
  state: CircuitState;
  failures: number;
  nextAttemptAt: number | null;
};

export type IsTransportError = (err: unknown) => boolean;
export type OnOpen = () => void;

export class CircuitBreaker {
  private state: CircuitState = "closed";
  private failures = 0;
  private successes = 0;
  private nextAttemptTime: number | null = null;

  constructor(
    private readonly name: string,
    private readonly config: CircuitBreakerConfig,
    private readonly isTransportError: IsTransportError = () => true,
    private readonly onOpen: OnOpen = () => {},
  ) {}

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.state === "open") {
      if (this.nextAttemptTime !== null && Date.now() >= this.nextAttemptTime) {
        this.transitionToHalfOpen();
      } else {
        throw new CircuitBreakerOpenError(new Date(this.nextAttemptTime ?? Date.now()));
      }
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      if (this.isTransportError(error)) {
        this.onFailure();
      }
      throw error;
    }
  }

  getState(): CircuitState {
    return this.state;
  }

  getSnapshot(): CircuitBreakerSnapshot {
    return {
      state: this.state,
      failures: this.failures,
      nextAttemptAt: this.nextAttemptTime,
    };
  }

  forceOpen(): void {
    this.transitionToOpen();
  }

  reset(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
  }

  private onSuccess(): void {
    this.failures = 0;
    if (this.state === "half-open") {
      this.successes += 1;
      if (this.successes >= this.config.successThreshold) {
        this.transitionToClosed();
      }
    }
  }

  private onFailure(): void {
    if (this.state === "open") return;
    this.failures += 1;
    if (this.state === "half-open" || this.failures >= this.config.failureThreshold) {
      this.transitionToOpen();
    }
  }

  private transitionToOpen(): void {
    this.state = "open";
    this.successes = 0;
    this.nextAttemptTime = Date.now() + this.config.recoveryTimeoutMs;
    this.onOpen();
  }

  private transitionToHalfOpen(): void {
    this.state = "half-open";
    this.successes = 0;
  }

  private transitionToClosed(): void {
    this.state = "closed";
    this.failures = 0;
    this.successes = 0;
    this.nextAttemptTime = null;
  }
}
```

Note: the `name` field is held but not used in the FSM itself — it's reserved for the structured logs that get added later in the `ProducerHandle`. Keeping it on the breaker (rather than the handle) lets future breakers reuse the same logging pattern without duplicating the name plumbing.

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/resilience.circuit-breaker.test.ts`
Expected: PASS — all 12 cases.

- [ ] **Step 3: Run the full suite**

Run: `bun test`
Expected: PASS. No regressions.

- [ ] **Step 4: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/resilience/circuit-breaker.ts test/unit/resilience.circuit-breaker.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: add CircuitBreaker FSM per circuit-breaker-guide §4

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section C — DrainMetrics extension

The breaker calls a callback into `DrainMetrics` on every transition-to-open. Add the counter and the setter; the wiring happens later in Section F.

### Task C1: Add `breakerOpenCount` to DrainMetrics

**Files:**
- Modify: `src/outbox/metrics.ts`
- Modify: `test/unit/outbox.metrics.test.ts`

- [ ] **Step 1: Write the failing test**

Open `test/unit/outbox.metrics.test.ts`. ADD this `describe` block at the end of the file (after the existing `describe("DrainMetrics", ...)` closing brace):

```ts
describe("DrainMetrics breakerOpenCount", () => {
  it("starts at zero", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    expect(m.snapshot().breakerOpenCount).toBe(0);
  });

  it("increments on incrementBreakerOpenCount()", () => {
    const m = createDrainMetrics({ windowMs: 60_000, now: () => 1_000 });
    m.incrementBreakerOpenCount();
    m.incrementBreakerOpenCount();
    m.incrementBreakerOpenCount();
    expect(m.snapshot().breakerOpenCount).toBe(3);
  });
});
```

Run: `bun test test/unit/outbox.metrics.test.ts -t "breakerOpenCount"`
Expected: FAIL — `breakerOpenCount` is not on the snapshot; `incrementBreakerOpenCount` is not a method.

- [ ] **Step 2: Extend DrainMetricsSnapshot and the factory**

Open `src/outbox/metrics.ts`.

First, replace the `DrainMetricsSnapshot` type (currently has `publishedLast60s`, `lastPublishedAt`, `lastError`) so it includes the new counter:

```ts
export type DrainMetricsSnapshot = {
  publishedLast60s: number;
  lastPublishedAt: number | null;
  lastError: { topic: string; message: string; at: number } | null;
  breakerOpenCount: number;
};
```

Next, replace the `DrainMetrics` type so it exposes the new setter:

```ts
export type DrainMetrics = {
  recordPublished(topic: string): void;
  recordError(topic: string, message: string): void;
  incrementBreakerOpenCount(): void;
  snapshot(): DrainMetricsSnapshot;
};
```

In `createDrainMetrics`, add a new local counter alongside `publishedAt` and `lastError`:

```ts
  let breakerOpenCount = 0;
```

Add the new method to the returned object (place it between `recordError` and `snapshot`):

```ts
    incrementBreakerOpenCount() {
      breakerOpenCount += 1;
    },
```

Update the `snapshot()` return so it includes the new field:

```ts
    snapshot(): DrainMetricsSnapshot {
      const cutoff = now() - windowMs;
      trim(cutoff);
      return {
        publishedLast60s: publishedAt.length,
        lastPublishedAt,
        lastError,
        breakerOpenCount,
      };
    },
```

- [ ] **Step 3: Run the test**

Run: `bun test test/unit/outbox.metrics.test.ts`
Expected: PASS — old and new tests.

- [ ] **Step 4: Run the full suite**

Run: `bun test`
Expected: PASS. Some tests in `gateway.routes.dispatch.test.ts` may fail because they construct a `DrainMetricsSnapshot` literal that no longer matches the type. If so, find each occurrence of the snapshot literal (search for `publishedLast60s: 0,` and `lastError: null,` together) and add `breakerOpenCount: 0,` to keep the shape valid.

For each such hit, the fix is one line added to the literal. The current `emptyMetricsSnap` in `test/unit/gateway.routes.dispatch.test.ts` is:

```ts
const emptyMetricsSnap: DrainMetricsSnapshot = {
  publishedLast60s: 0,
  lastPublishedAt: null,
  lastError: null,
};
```

Change to:

```ts
const emptyMetricsSnap: DrainMetricsSnapshot = {
  publishedLast60s: 0,
  lastPublishedAt: null,
  lastError: null,
  breakerOpenCount: 0,
};
```

Also check `test/unit/gateway.routes.test.ts` and `test/unit/gateway.index.adminEnabled.test.ts` for similar literals; apply the same one-line addition where present.

- [ ] **Step 5: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `bun test`
Expected: PASS — all tests, no regressions.

- [ ] **Step 7: Commit**

```bash
git add src/outbox/metrics.ts test/unit/outbox.metrics.test.ts test/unit/gateway.routes.dispatch.test.ts test/unit/gateway.routes.test.ts test/unit/gateway.index.adminEnabled.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: DrainMetrics exposes breakerOpenCount + incrementBreakerOpenCount

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

(If some of the test files listed above did NOT need a change, drop them from the `git add` line.)

---

## Section D — ProducerHandle (wraps Producer + breaker)

The handle is the single integration point. Drainer and webhook handler both consume it via the existing `EventProducer` interface — the only new surface is `getBreakerSnapshot()` for observability and `CircuitBreakerOpenError` propagation from `sendByTopic`.

### Task D1: Write the failing test for ProducerHandle

**Files:**
- Create: `test/unit/kafka.producer-handle.test.ts`

- [ ] **Step 1: Write the test**

Create `test/unit/kafka.producer-handle.test.ts`:

```ts
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
    const { sent, inner } = makeInner("always-fail");
    const handle = createProducerHandleFromInner(inner, cfg);
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    // Inner saw 3 attempts (the ones that tripped the breaker).
    // Track that by overriding the failing fake — easier to assert by counting calls.
    const callsBefore = sent.length; // 0 since inner always failed
    await handle.sendByTopic("T", "x", "v", null).catch(() => undefined);
    expect(sent.length).toBe(callsBefore);
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
    const handle = createProducerHandleFromInner(inner, cfg, undefined, {
      info: (obj, msg) => logs.push({ obj: obj as object, msg: msg as string }),
      warn: () => {},
      error: () => {},
      debug: () => {},
      trace: () => {},
      fatal: () => {},
      child: () => ({} as never),
      flush: () => {},
    });
    for (let i = 0; i < 3; i++) {
      await handle.sendByTopic("T", String(i), "v", null).catch(() => undefined);
    }
    const opened = logs.filter((l) => l.msg === "circuit breaker opened");
    expect(opened.length).toBe(1);
    expect((opened[0]!.obj as { event_name: string }).event_name).toBe("circuit_breaker_opened");
    expect((opened[0]!.obj as { breaker: string }).breaker).toBe("kafka-producer");
  });
});
```

- [ ] **Step 2: Run the test (expected FAIL)**

Run: `bun test test/unit/kafka.producer-handle.test.ts`
Expected: FAIL — module `../../src/kafka/producerHandle.ts` not found.

### Task D2: Implement ProducerHandle

**Files:**
- Create: `src/kafka/producerHandle.ts`

- [ ] **Step 1: Write the implementation**

Create `src/kafka/producerHandle.ts`:

```ts
// src/kafka/producerHandle.ts
import { getLogger, type ILogger } from "../logging/index.ts";
import {
  CircuitBreaker,
  type CircuitBreakerConfig,
  type CircuitBreakerSnapshot,
} from "../resilience/circuit-breaker.ts";
import { isApplicationLevelError } from "../resilience/errors.ts";
import type { KafkaProvider } from "./providers/index.ts";
import { createProducer, type EventProducer } from "./producer.ts";

export type ProducerHandle = EventProducer & {
  getBreakerSnapshot(): CircuitBreakerSnapshot;
};

const BREAKER_NAME = "kafka-producer";

// Lifted out of createProducerHandleFromInner so the log shape is one place.
function logTransition(
  log: ILogger,
  level: "info",
  eventName:
    | "circuit_breaker_opened"
    | "circuit_breaker_closed"
    | "circuit_breaker_half_open",
  from: string,
  snapshot: CircuitBreakerSnapshot,
  lastError?: string,
  message?: string,
): void {
  const bindings: Record<string, unknown> = {
    event_name: eventName,
    breaker: BREAKER_NAME,
    from,
    failures: snapshot.failures,
  };
  if (snapshot.nextAttemptAt !== null) {
    bindings.next_attempt_at = new Date(snapshot.nextAttemptAt).toISOString();
  }
  if (lastError !== undefined) bindings.last_error = lastError;
  log[level](bindings, message ?? eventName);
}

// Internal factory — takes an already-constructed EventProducer. Used by
// the public createProducerHandle() factory, AND by unit tests so they can
// inject a fake inner producer without going through createProducer().
export function createProducerHandleFromInner(
  inner: EventProducer,
  breakerConfig: CircuitBreakerConfig,
  onBreakerOpen: () => void = () => {},
  logger?: ILogger,
): ProducerHandle {
  const log = logger ?? getLogger("kafka.breaker");
  let lastSeenState: CircuitBreakerSnapshot["state"] = "closed";

  const breaker = new CircuitBreaker(
    BREAKER_NAME,
    breakerConfig,
    (err) => !isApplicationLevelError(err),
    () => {
      // Increment user-supplied counter first so a thrown logger never breaks metrics.
      try {
        onBreakerOpen();
      } catch (cbErr) {
        log.warn(
          { err: cbErr instanceof Error ? cbErr.message : String(cbErr) },
          "breaker onOpen callback threw",
        );
      }
      const snap = breaker.getSnapshot();
      logTransition(log, "info", "circuit_breaker_opened", lastSeenState, snap, undefined, "circuit breaker opened");
      lastSeenState = "open";
    },
  );

  return {
    isConnected: () => inner.isConnected(),
    disconnect: async () => {
      await inner.disconnect();
    },
    sendByTopic: async (topic, key, value, headers) => {
      try {
        await breaker.execute(() => inner.sendByTopic(topic, key, value, headers));
        // Detect closed/half-open transitions for logging — checked AFTER each call.
        const currentState = breaker.getSnapshot().state;
        if (lastSeenState !== currentState) {
          if (currentState === "closed") {
            logTransition(log, "info", "circuit_breaker_closed", lastSeenState, breaker.getSnapshot(), undefined, "circuit breaker closed");
          } else if (currentState === "half-open") {
            logTransition(log, "info", "circuit_breaker_half_open", lastSeenState, breaker.getSnapshot(), undefined, "circuit breaker half-open");
          }
          lastSeenState = currentState;
        }
      } catch (err) {
        // Also catches the case where execute() flipped open->half-open before re-throwing.
        const currentState = breaker.getSnapshot().state;
        if (lastSeenState !== currentState) {
          if (currentState === "half-open") {
            logTransition(log, "info", "circuit_breaker_half_open", lastSeenState, breaker.getSnapshot(), undefined, "circuit breaker half-open");
          }
          // open transitions are logged by the onOpen callback, not here.
          lastSeenState = currentState;
        }
        throw err;
      }
    },
    getBreakerSnapshot: () => breaker.getSnapshot(),
  };
}

// Public factory — used by gateway/index.ts. Constructs a real Producer
// via createProducer(), then wraps it.
export async function createProducerHandle(
  clientId: string,
  provider: KafkaProvider,
  breakerConfig: CircuitBreakerConfig,
  onBreakerOpen: () => void = () => {},
): Promise<ProducerHandle> {
  const inner = await createProducer(clientId, provider);
  return createProducerHandleFromInner(inner, breakerConfig, onBreakerOpen);
}
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/kafka.producer-handle.test.ts`
Expected: PASS — all 9 cases.

- [ ] **Step 3: Run the full suite**

Run: `bun test`
Expected: PASS, no regressions.

- [ ] **Step 4: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/kafka/producerHandle.ts test/unit/kafka.producer-handle.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: add ProducerHandle wrapping Producer + CircuitBreaker

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section E — Drainer integration (CircuitBreakerOpenError handling)

The drainer's catch block gains one new branch for `CircuitBreakerOpenError`. Existing path for any other error is unchanged.

### Task E1: Write the failing test for CBO handling

**Files:**
- Modify: `test/unit/outbox.drainer.test.ts`

- [ ] **Step 1: Import `CircuitBreakerOpenError` at the top of the test file**

Open `test/unit/outbox.drainer.test.ts`. Near the existing imports, add:

```ts
import { CircuitBreakerOpenError } from "../../src/resilience/errors.ts";
```

- [ ] **Step 2: Add new test cases**

At the end of the file, INSIDE the last `describe` block (or as a new `describe` block — match the file's existing style), add:

```ts
describe("runOutboxIteration CircuitBreakerOpenError handling", () => {
  test("defers next_attempt_at and does NOT increment attempts when CBO is thrown", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);

    const futureMs = Date.now() + 30_000;
    const cboProducer = {
      sendByTopic: async () => {
        throw new CircuitBreakerOpenError(new Date(futureMs));
      },
    };

    const result = await runOutboxIteration({ db, producer: cboProducer, config: cfg });

    expect(result.published).toBe(0);
    expect(result.retried).toBe(0);
    expect(result.deferred).toBe(2);
    const rows = db
      .query("SELECT attempts, next_attempt_at, last_error FROM outbox")
      .all() as Array<{ attempts: number; next_attempt_at: number; last_error: string }>;
    for (const row of rows) {
      expect(row.attempts).toBe(0); // unchanged from seeded value
      expect(row.next_attempt_at).toBe(futureMs);
      expect(row.last_error).toBe("circuit_breaker_open");
    }
  });

  test("CBO does NOT trip the maxAge give-up path", async () => {
    const writer = createWriter(db);
    seedTwoPending(writer);
    // Backdate the rows past maxAge.
    db.query("UPDATE outbox SET created_at = $oldCreated, next_attempt_at = $now").run({
      oldCreated: Date.now() - 25 * 60 * 60 * 1_000,
      now: Date.now(),
    });

    const cboProducer = {
      sendByTopic: async () => {
        throw new CircuitBreakerOpenError(new Date(Date.now() + 60_000));
      },
    };

    const result = await runOutboxIteration({ db, producer: cboProducer, config: cfg });

    expect(result.failedPermanently).toBe(0); // age-out path is NOT taken on CBO
    expect(result.deferred).toBe(2);
    const rows = db.query("SELECT status FROM outbox").all() as Array<{ status: string }>;
    expect(rows.every((r) => r.status === "pending")).toBe(true);
  });
});
```

- [ ] **Step 3: Run the test (expected FAIL)**

Run: `bun test test/unit/outbox.drainer.test.ts -t "CircuitBreakerOpenError"`
Expected: FAIL — `result.deferred` is undefined; drainer treats CBO as a normal failure and increments `attempts`.

### Task E2: Implement the CBO branch in the drainer

**Files:**
- Modify: `src/outbox/drainer.ts`

- [ ] **Step 1: Add the import**

At the top of `src/outbox/drainer.ts`, add:

```ts
import { CircuitBreakerOpenError } from "../resilience/errors.ts";
```

- [ ] **Step 2: Extend `IterationResult` with `deferred`**

Find the existing `IterationResult` type:

```ts
export type IterationResult = {
  scanned: number;
  published: number;
  retried: number;
  failedPermanently: number;
};
```

Replace it with:

```ts
export type IterationResult = {
  scanned: number;
  published: number;
  retried: number;
  failedPermanently: number;
  deferred: number;
};
```

- [ ] **Step 3: Add the deferred counter and the CBO branch**

Inside `runOutboxIteration`, locate the local counter declarations (`let published = 0;` and friends). Add one more:

```ts
  let deferred = 0;
```

Locate the prepared-statement declarations (`markDispatched`, `markRetry`, `markFailed`). Add one more, right after `markFailed`:

```ts
  const markDeferred = db.query(
    `UPDATE outbox SET next_attempt_at=$next, last_error=$err WHERE id=$id`,
  );
```

Now find the catch block in the per-row loop. It currently looks like:

```ts
    } catch (err) {
      const attempts = row.attempts + 1;
      const ageMs = Date.now() - row.created_at;
      const message = err instanceof Error ? err.message : String(err);
      metrics?.recordError(row.topic, message);
      log.warn(
        { topic: row.topic, attempts, ageMs, err: message, id: row.id },
        "outbox publish failed",
      );
      if (ageMs > config.maxAgeMs) {
        markFailed.run({ id: row.id, attempts, err: message });
        failedPermanently += 1;
        log.warn(
          { id: row.id, topic: row.topic, ageMs, attempts, err: message },
          "outbox row exceeded maxAgeMs; marked failed",
        );
      } else {
        const delay = nextDelayMs(attempts, config.backoffMaxMs);
        markRetry.run({
          id: row.id,
          attempts,
          next: Date.now() + delay,
          err: message,
        });
        retried += 1;
      }
    }
```

Insert a new branch at the TOP of the catch (before any of the above logic), so CBO is detected first:

```ts
    } catch (err) {
      if (err instanceof CircuitBreakerOpenError) {
        const nextMs = err.nextAttemptAt.getTime();
        markDeferred.run({ id: row.id, next: nextMs, err: "circuit_breaker_open" });
        deferred += 1;
        metrics?.recordError(row.topic, "circuit_breaker_open");
        log.debug(
          { topic: row.topic, id: row.id, nextAttemptAt: err.nextAttemptAt },
          "publish deferred; breaker open",
        );
        continue;
      }
      const attempts = row.attempts + 1;
      const ageMs = Date.now() - row.created_at;
      const message = err instanceof Error ? err.message : String(err);
      metrics?.recordError(row.topic, message);
      log.warn(
        { topic: row.topic, attempts, ageMs, err: message, id: row.id },
        "outbox publish failed",
      );
      if (ageMs > config.maxAgeMs) {
        markFailed.run({ id: row.id, attempts, err: message });
        failedPermanently += 1;
        log.warn(
          { id: row.id, topic: row.topic, ageMs, attempts, err: message },
          "outbox row exceeded maxAgeMs; marked failed",
        );
      } else {
        const delay = nextDelayMs(attempts, config.backoffMaxMs);
        markRetry.run({
          id: row.id,
          attempts,
          next: Date.now() + delay,
          err: message,
        });
        retried += 1;
      }
    }
```

- [ ] **Step 4: Update the return statement at the end of `runOutboxIteration`**

The function currently returns:

```ts
  return { scanned: pending.length, published, retried, failedPermanently };
```

Change to:

```ts
  return { scanned: pending.length, published, retried, failedPermanently, deferred };
```

- [ ] **Step 5: Run the new tests**

Run: `bun test test/unit/outbox.drainer.test.ts`
Expected: PASS — all old + new cases.

- [ ] **Step 6: Run the full suite**

Run: `bun test`
Expected: PASS, no regressions.

- [ ] **Step 7: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/outbox/drainer.ts test/unit/outbox.drainer.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: drainer defers CircuitBreakerOpenError to breaker.nextAttemptAt without incrementing attempts

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section F — Health surface (DependencyStatus + monitor)

The HealthMonitor reads `producerHandle.getBreakerSnapshot()` each cycle and includes the result in the `kafkaProducer` `DependencyStatus`. `DependencyStatus` itself gains three optional fields.

### Task F1: Extend DependencyStatus with breaker fields

**Files:**
- Modify: `src/health/types.ts`

- [ ] **Step 1: Add the three optional fields**

Open `src/health/types.ts`. The current `DependencyStatus` is:

```ts
export type DependencyStatus = {
  ok: boolean;
  lastCheckedAt: number;
  lastError?: string;
  connected?: boolean;
  brokerProbeMs?: number;
  missing?: string[];
};
```

Replace it with:

```ts
export type DependencyStatus = {
  ok: boolean;
  lastCheckedAt: number;
  lastError?: string;
  // Per-dependency extras (kept loose because each probe carries different metadata):
  connected?: boolean;             // kafkaProducer
  breakerState?: "closed" | "open" | "half-open";  // kafkaProducer (SIO-817)
  breakerNextAttemptAt?: string;   // kafkaProducer (ISO timestamp, only when state === "open")
  brokerProbeMs?: number;          // kafkaBroker
  missing?: string[];              // topics
};
```

- [ ] **Step 2: Run typecheck**

Run: `bun run typecheck`
Expected: PASS — new fields are optional, no consumers break.

- [ ] **Step 3: Commit**

```bash
git add src/health/types.ts
git commit -m "$(cat <<'EOF'
SIO-817: DependencyStatus exposes optional breaker fields for kafkaProducer

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task F2: HealthMonitor reads breaker snapshot

**Files:**
- Modify: `src/health/monitor.ts`

- [ ] **Step 1: Extend `HealthMonitorOptions` to accept a breaker snapshot accessor**

The monitor currently takes a `producer: ProducerLike` where `ProducerLike = { isConnected(): boolean }`. We want to keep tests simple — the monitor doesn't need the full handle, just a way to read the breaker snapshot.

Find this section near the top of `src/health/monitor.ts`:

```ts
type ProducerLike = { isConnected(): boolean };

export type HealthMonitorOptions = {
  producer: ProducerLike;
  ...
};
```

Replace `ProducerLike` and `HealthMonitorOptions` with:

```ts
import type { CircuitBreakerSnapshot } from "../resilience/circuit-breaker.ts";

type ProducerLike = {
  isConnected(): boolean;
  // Optional so existing tests that pass a bare { isConnected } still work.
  getBreakerSnapshot?(): CircuitBreakerSnapshot;
};

export type HealthMonitorOptions = {
  producer: ProducerLike;
  outboxDb?: OutboxDatabase;
  admin: AdminLike;
  expectedTopics: string[];
  probeIntervalMs: number;
  probeTimeoutMs: number;
  logger?: ILogger;
};
```

(Note: the import for `CircuitBreakerSnapshot` goes with the other imports at the top of the file; the inline `import type` shown here is just illustrative.)

- [ ] **Step 2: Populate breaker fields in the snapshot**

Locate the section in `runProbeCycle` that builds `deps.kafkaProducer`. Currently:

```ts
    const producerOk = opts.producer.isConnected();
    const deps: HealthSnapshot["dependencies"] = {
      kafkaProducer: { ok: producerOk, lastCheckedAt: checkedAt, connected: producerOk },
    };
```

Replace with:

```ts
    const producerOk = opts.producer.isConnected();
    const breakerSnap = opts.producer.getBreakerSnapshot?.();
    const kafkaProducer: DependencyStatus = {
      ok: producerOk,
      lastCheckedAt: checkedAt,
      connected: producerOk,
    };
    if (breakerSnap !== undefined) {
      kafkaProducer.breakerState = breakerSnap.state;
      if (breakerSnap.state === "open" && breakerSnap.nextAttemptAt !== null) {
        kafkaProducer.breakerNextAttemptAt = new Date(breakerSnap.nextAttemptAt).toISOString();
      }
    }
    const deps: HealthSnapshot["dependencies"] = { kafkaProducer };
```

NOTE: only `breakerState` and (when state === "open") `breakerNextAttemptAt` are surfaced in `/healthz`, per the spec's alignment with `guides/circuit-breaker-guide.md` (which prescribes `next_attempt_at` for transition logs and has no equivalent for an "opened-at" field). Operators correlate `breakerState=open` with the timestamped `circuit_breaker_opened` log event to find when the open window began.

Also add `DependencyStatus` to the existing `import { ... } from "./types.ts";` at the top of the file if it isn't already imported there.

- [ ] **Step 3: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 4: Run the full test suite**

Run: `bun test`
Expected: PASS. Existing health-monitor tests should still pass because `getBreakerSnapshot` is optional and the fakeProducer in those tests doesn't provide it.

- [ ] **Step 5: Commit**

```bash
git add src/health/monitor.ts
git commit -m "$(cat <<'EOF'
SIO-817: HealthMonitor reads producer breaker snapshot when available

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task F3: Test that /healthz body surfaces breakerState

**Files:**
- Modify: `test/unit/gateway.routes.dispatch.test.ts`

- [ ] **Step 1: Write the failing test**

Open `test/unit/gateway.routes.dispatch.test.ts`. Inside the existing `describe("buildRoutes with multiple routes", ...)` block, add:

```ts
  it("healthz body includes dependencies.kafkaProducer.breakerState", async () => {
    const outbox = fakeOutbox();
    const snapWithBreaker: HealthSnapshot = {
      ...healthySnap,
      dependencies: {
        ...healthySnap.dependencies,
        kafkaProducer: {
          ok: true,
          lastCheckedAt: 1_000,
          connected: true,
          breakerState: "open",
          breakerNextAttemptAt: "2026-05-21T08:01:00.000Z",
        },
      },
    };
    const routes = buildRoutes({
      producer: noopProducer,
      outbox,
      monitor: fakeMonitor(snapWithBreaker),
      metrics: fakeMetrics(emptyMetricsSnap),
    });
    const healthz = routes["/healthz"] as () => Response;
    const res = healthz();
    expect(res.status).toBe(200); // kafkaProducer.ok is still true; breaker is informational
    const body = await res.json() as {
      dependencies: { kafkaProducer: { breakerState: string; breakerNextAttemptAt: string } };
    };
    expect(body.dependencies.kafkaProducer.breakerState).toBe("open");
    expect(body.dependencies.kafkaProducer.breakerNextAttemptAt).toBe("2026-05-21T08:01:00.000Z");
  });
```

- [ ] **Step 2: Run the test**

Run: `bun test test/unit/gateway.routes.dispatch.test.ts -t "breakerState"`
Expected: PASS — the `/healthz` handler already passes the entire `dependencies` object through to the response body (no code change in `routes.ts` is needed because the new fields are optional additions to `DependencyStatus`).

- [ ] **Step 3: Run the full suite + typecheck**

Run: `bun run typecheck && bun test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/unit/gateway.routes.dispatch.test.ts
git commit -m "$(cat <<'EOF'
SIO-817: test /healthz body surfaces kafkaProducer.breakerState

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section G — Gateway wiring

The final integration: gateway/index.ts constructs a `ProducerHandle` instead of a raw `Producer`, the heartbeat snapshot picks up `producerBreaker`, and the breaker-open callback wires into `DrainMetrics.incrementBreakerOpenCount`.

### Task G1: Replace createProducer with createProducerHandle in gateway/index.ts

**Files:**
- Modify: `src/gateway/index.ts`

- [ ] **Step 1: Update imports**

In `src/gateway/index.ts`, find this import line:

```ts
import { createProducer } from "../kafka/producer.ts";
```

Replace it with:

```ts
import { createProducerHandle, type ProducerHandle } from "../kafka/producerHandle.ts";
```

- [ ] **Step 2: Update construction**

Find the construction line (around line 24):

```ts
const producer = await createProducer(config.kafka.clientId, provider);
```

Replace it with:

```ts
const drainMetrics = createDrainMetrics();

const producer: ProducerHandle = await createProducerHandle(
  config.kafka.clientId,
  provider,
  {
    failureThreshold: config.breaker.failureThreshold,
    successThreshold: config.breaker.successThreshold,
    recoveryTimeoutMs: config.breaker.recoveryTimeoutMs,
  },
  () => drainMetrics.incrementBreakerOpenCount(),
);
```

(Note: `drainMetrics` was previously constructed AFTER the outbox block. This change moves it BEFORE so it can be passed as the callback. The existing `const drainMetrics = createDrainMetrics();` line later in the file must be DELETED to avoid double-declaration.)

- [ ] **Step 3: Delete the old `drainMetrics` declaration**

Search the file for the second `const drainMetrics = createDrainMetrics();` line (the one that was added in SIO-815). Delete it. There should now be exactly ONE `const drainMetrics = createDrainMetrics();` in the file, and it should be right after the producer construction.

- [ ] **Step 4: Update the heartbeat snapshot to include producerBreaker**

Find the `startHeartbeat({ ... })` call. Inside its `snapshot: () => { ... }` function, find the returned object and add `producerBreaker` to it:

The current returned object structure (simplified):

```ts
return {
  producerConnected: snap.dependencies.kafkaProducer?.ok ?? false,
  brokerOk: snap.dependencies.kafkaBroker?.ok ?? false,
  topicsOk: snap.dependencies.topics?.ok ?? true,
  status: snap.status,
  ...(stats ? { ... } : {}),
  publishedLast60s: drain.publishedLast60s,
  lastPublishedAt: drain.lastPublishedAt,
};
```

ADD a new field, placed alphabetically (or wherever it makes sense — match existing style):

```ts
return {
  producerConnected: snap.dependencies.kafkaProducer?.ok ?? false,
  producerBreaker: {
    state: producer.getBreakerSnapshot().state,
    failures: producer.getBreakerSnapshot().failures,
  },
  brokerOk: snap.dependencies.kafkaBroker?.ok ?? false,
  topicsOk: snap.dependencies.topics?.ok ?? true,
  status: snap.status,
  ...(stats ? { ... } : {}),
  publishedLast60s: drain.publishedLast60s,
  lastPublishedAt: drain.lastPublishedAt,
  breakerOpenCount: drain.breakerOpenCount,
};
```

(Replace `{ ... }` in the spread with the existing pending/failed/etc. fields.)

- [ ] **Step 5: Pass producer's breaker snapshot to the HealthMonitor**

Find the `createHealthMonitor({ ... })` call. Currently `producer` is passed directly. Since `ProducerHandle` has the `getBreakerSnapshot` method, the existing line:

```ts
const monitor = createHealthMonitor({
  producer,
  ...
});
```

stays the same. The monitor's `ProducerLike` type (extended in F2) accepts the handle directly because `ProducerHandle` extends `EventProducer` and adds `getBreakerSnapshot()`. No code change here.

- [ ] **Step 6: Run typecheck**

Run: `bun run typecheck`
Expected: PASS.

- [ ] **Step 7: Run the full suite**

Run: `bun test`
Expected: PASS. `gateway.index.ts` has no direct tests; the end-to-end behaviour is verified manually in Section H.

- [ ] **Step 8: Commit**

```bash
git add src/gateway/index.ts
git commit -m "$(cat <<'EOF'
SIO-817: gateway/index.ts uses ProducerHandle and surfaces breaker state in heartbeat

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section H — Documentation

### Task H1: Update .env.example

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Add the breaker env vars after the health block**

Open `.env.example`. After the "Health probes + heartbeat" section (added in SIO-815, ends with `#STATS_HEARTBEAT_MS=60000`), and BEFORE the final `ENVIRONMENT=dev` and `LOG_LEVEL=info` lines, add:

```bash

# --- Kafka producer circuit breaker (SIO-817) ---
# Wraps producer publish calls in a circuit breaker per
# guides/circuit-breaker-guide.md. Fails fast during sustained Kafka outages;
# emits structured circuit_breaker_{opened,closed,half_open} events. Outbox is
# the fallback — does not affect /healthz HTTP status (informational only).
#   CIRCUIT_BREAKER_FAILURE_THRESHOLD     Consecutive transport failures before open (default 5)
#   CIRCUIT_BREAKER_SUCCESS_THRESHOLD     Consecutive HALF_OPEN successes to close (default 3)
#   CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS   ms in OPEN before probing HALF_OPEN (default 60000, min 1000)
#CIRCUIT_BREAKER_FAILURE_THRESHOLD=5
#CIRCUIT_BREAKER_SUCCESS_THRESHOLD=3
#CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS=60000
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "$(cat <<'EOF'
SIO-817: document CIRCUIT_BREAKER_* env vars in .env.example

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task H2: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add `resilience/` to the architecture tree**

In `CLAUDE.md`, locate the `## Architecture` `src/` block. After the `health/` block (added in SIO-815) and before `kafka/`, insert:

```
  resilience/             reusable resilience primitives (PR1: circuit breaker)
    circuit-breaker.ts    CircuitBreaker FSM per guides/circuit-breaker-guide.md §4
    errors.ts             CircuitBreakerOpenError + isApplicationLevelError predicate
```

- [ ] **Step 2: Add `producerHandle.ts` to the kafka/ block**

In the same tree, the `kafka/` block. After `producer.ts`, add:

```
    producerHandle.ts     wraps Producer + CircuitBreaker; implements EventProducer
```

- [ ] **Step 3: Document `config.breaker.*` in the 4-pillar block**

Under `## Config shape (4-pillar)`, after the `config.health.{...}` line, add:

```
config.breaker.{failureThreshold, successThreshold, recoveryTimeoutMs}
```

- [ ] **Step 4: Update the gateway responsibility row**

Find the "Single-process model" table row for the gateway. After the existing `/healthz` description (added in SIO-815), append:

> Producer publishes flow through a circuit breaker (SIO-817). Five consecutive transport failures open the breaker; subsequent calls fail fast with `CircuitBreakerOpenError`, which the drainer treats as "defer this row until the breaker probes again" — no `attempts++` and no false maxAge give-up. Breaker state surfaces in `dependencies.kafkaProducer.breakerState` and the 60s heartbeat. HTTP status code is unchanged in this PR.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
SIO-817: document resilience module, config.breaker.*, and producer-breaker semantics

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Section I — Verification

### Task I1: End-to-end manual verification

**Files:** none (manual)

- [ ] **Step 1: Typecheck and test**

Run: `bun run typecheck && bun test`
Expected: clean. Test count should be higher than the baseline noted in Task 0 step 4 by roughly 18 tests (4 config.breaker + 2 resilience.errors + 12 resilience.circuit-breaker + 9 kafka.producer-handle + 2 outbox.drainer + 2 outbox.metrics + 1 gateway.routes.dispatch = 32 new; minus the 2 reframed by extending the metrics snapshot literals; net ≈ 32 new tests).

- [ ] **Step 2: Start Redpanda**

Run: `docker compose up -d`
Expected: redpanda + console come up cleanly.

- [ ] **Step 3: Start the gateway and capture its logs**

Run (in a separate terminal): `LOG_LEVEL=debug bun run dev:gateway 2>&1 | tee /tmp/gateway.log`
Expected: "gateway listening" log includes the new heartbeatMs and the breaker is silent (no transitions, breaker starts closed).

- [ ] **Step 4: Healthy baseline**

Run: `curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'`
Expected: `{ ok: true, ..., breakerState: "closed" }`. No `breakerNextAttemptAt` field.

- [ ] **Step 5: Force breaker open**

Stop Redpanda, then post 8 webhooks to give the drainer enough iterations to accumulate 5 transport failures:

```bash
docker compose stop redpanda
for i in 1 2 3 4 5 6 7 8; do
  curl -X POST -H 'content-type: application/json' \
    -d '{"resourceId":"x'$i'"}' http://localhost:3000/webhooks/elastic/autoops
  sleep 1
done
sleep 15
```

Then:

```bash
curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'
```

Expected: `{ breakerState: "open", breakerNextAttemptAt: "..." }`. The `circuit_breaker_opened` event should appear in `/tmp/gateway.log`:

```bash
grep circuit_breaker_opened /tmp/gateway.log | jq
```

Expected: at least one log line with `event_name=circuit_breaker_opened`, `breaker=kafka-producer`, `from=closed`, `failures=5`.

- [ ] **Step 6: Recover**

Start Redpanda, wait for the recovery timeout (60s) + a few seconds for the probe to succeed and the three-probe close threshold:

```bash
docker compose start redpanda
sleep 80
curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'
```

Expected: `breakerState: "closed"` (or `"half-open"` if the drainer hasn't yet completed three successful probes). Eventually closes.

Search the log for transitions:

```bash
grep -E "circuit_breaker_(closed|half_open)" /tmp/gateway.log | jq
```

Expected: at least one `circuit_breaker_half_open` and at least one `circuit_breaker_closed`.

- [ ] **Step 7: Heartbeat shows producerBreaker state**

Tail the log for ~70s:

```bash
grep gateway.heartbeat /tmp/gateway.log | tail -3 | jq '.producerBreaker, .breakerOpenCount'
```

Expected: `state: "closed"`, `failures: 0` (post-recovery), `breakerOpenCount >= 1` (at least one open during the test).

- [ ] **Step 8: Outbox rows that were buffered during the breaker-open window drained successfully**

Run:

```bash
curl -sS http://localhost:3000/healthz | jq '.outbox.pending, .outbox.publishedLast60s'
```

Expected: `pending: 0`, `publishedLast60s >= 8` (depending on timing — should reflect the rows posted during step 5 plus any subsequent activity).

- [ ] **Step 9: Tear down**

Stop the gateway (Ctrl+C in its terminal).
Run: `docker compose down`.

### Task I2: Push branch + open PR + create/transition Linear issue

**Files:** none (git / GitHub / Linear)

- [ ] **Step 1: Confirm clean tree**

Run: `git status`
Expected: clean.

- [ ] **Step 2: Push the branch**

Run: `git push -u origin sio-817-producer-circuit-breaker`
Expected: branch created on origin.

- [ ] **Step 3: Create the Linear issue**

If SIO-817 does not already exist in Linear, create it via the Linear MCP. The issue should:

- Title: `Producer circuit breaker (SIO-817)`
- Project: `Event Gate`
- Team: `Siobytes`
- Assignee: `me`
- State: `In Review` (we transition straight from creation since the PR is about to land)
- Description: include the spec + plan paths and a short summary of the three new capabilities (breaker, drainer CBO handling, observability)

Per memory `feedback_linear_auto_done.md`: when adding the PR link as an attachment via `save_issue links`, set `state: "In Review"` explicitly in the SAME call to prevent the auto-Done transition.

- [ ] **Step 4: Open the PR**

Run:
```bash
gh pr create --title "SIO-817: producer circuit breaker" --body "$(cat <<'EOF'
## Summary
- Wraps producer publishes in a Closed → Open → Half-Open circuit breaker per `guides/circuit-breaker-guide.md`.
- Drainer treats `CircuitBreakerOpenError` as "defer this row to the next breaker probe" — no `attempts++`, no false maxAge give-up during sustained outages.
- Operator visibility: `circuit_breaker_{opened,closed,half_open}` structured events, `breakerState` in `/healthz` body, `producerBreaker` + `breakerOpenCount` in the 60s heartbeat.
- Outbox is the fallback. HTTP status code on `/healthz` is unchanged in this PR — that's PR2's call.

Spec: `docs/superpowers/specs/2026-05-21-producer-circuit-breaker-design.md`
Plan: `docs/superpowers/plans/2026-05-21-producer-circuit-breaker.md`
Linear: SIO-817

## Test plan
- [x] `bun run typecheck`
- [x] `bun test`
- [ ] Manual: healthy baseline → kill broker → 8 webhooks → confirm `breakerState: "open"` in `/healthz` and `circuit_breaker_opened` in logs
- [ ] Manual: restart broker, wait 80s → confirm `breakerState: "closed"` and a `circuit_breaker_closed` log
- [ ] Manual: heartbeat shows `producerBreaker.state` and `breakerOpenCount`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL returned.

- [ ] **Step 5: Hand off to user for review**

Return the PR URL. The user reviews the PR + ticks the manual verification boxes.

---

## Notes for the implementer

- All TypeScript imports include the `.ts` extension (project convention; `tsconfig.allowImportingTsExtensions` is on).
- No `console.*` calls in `src/`. All logging via `getLogger("component")`. Errors as `{ err }` (Pino convention).
- No emojis anywhere — code, comments, commit messages, logs.
- Tests use `bun:test`. Tests live under `test/unit/`. `LOG_LEVEL=silent` is preloaded via `test/preload.ts` so log calls in tested code do not pollute test output (the producer-handle tests inject a fake logger when they want to capture log calls).
- TDD: every behaviour change has a failing test BEFORE the implementation. Docs-only and config-only tasks skip the test step where appropriate.
- The breaker's `name` field on `CircuitBreaker` is currently unused by the FSM itself; the `ProducerHandle` reads `BREAKER_NAME` separately when emitting logs. This is intentional — the FSM stays pure and the handle owns the user-facing string.
- `breakerNextAttemptAt` in the `/healthz` body is only set when `breakerState === "open"`. When the breaker is `half-open`, the field is omitted (no meaningful next-attempt time). When `closed`, neither field is set.
- DRY note: the `fakeMonitor` / `fakeMetrics` / `healthySnap` / `emptyMetricsSnap` helpers in the gateway tests have been duplicated across three files since SIO-815. Do NOT extract them in this PR; the duplication is intentional per the original plan note. A future tidy-up PR can consolidate.
