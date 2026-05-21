# Producer Circuit Breaker — Design

**Date:** 2026-05-21
**Status:** Approved (pending user review of spec)
**Author:** Simon Owusu
**Linear:** SIO-817 (to be created on plan completion)
**Related:** SIO-815 (PR #28, merged), SIO-816 (PR #29, merged)

## Context

The gateway publishes to Kafka through `@platformatic/kafka`'s `Producer`, which is an out-of-process dependency that can become slow or unavailable independently of the gateway. The project's [`guides/circuit-breaker-guide.md`](../../guides/circuit-breaker-guide.md) §1 lists message brokers in the "use it for" category and explicitly says any such dependency should sit behind a Closed → Open → Half-Open FSM. The current implementation has no breaker: every drainer iteration calls `producer.sendByTopic` directly, and a sustained broker outage produces N retry attempts per row per iteration with no fail-fast behaviour or operator-visible state-transition events.

The 2026-05-20 production incident that prompted this work was ultimately diagnosed (2026-05-21) as a misnamed environment variable — `KAFKA_LOCAL_BOOTSTRAP_SERVERS` in the task definition instead of the `KAFKA_BROKERS` the code reads from. The gateway silently fell back to `localhost:9092`, every metadata call ECONNREFUSED, and the outbox correctly buffered the un-drainable rows. That specific failure mode is fixed by setting the right env var; the broader observation that we have no breaker around Kafka calls remains valid.

This spec wraps Kafka producer calls in a circuit breaker as defensive infrastructure per the guide. The benefits are independent of any specific incident:

- **Fail-fast during real outages.** When the broker is genuinely unreachable (network partition, broker restart, ACL change), the drainer stops pounding a known-bad dependency every iteration. Pending rows stay pending; we don't waste CPU and connection attempts.
- **Operator visibility.** The breaker emits `circuit_breaker_opened` / `circuit_breaker_closed` / `circuit_breaker_half_open` structured events, giving clear "we lost Kafka at T" and "we got it back at T+60s" markers in the log stream. Today these states are invisible.
- **A single point for application-vs-transport error classification.** As we learn `@platformatic/kafka`'s specific error shapes in production, the predicate is the one place to encode them.

This spec is **PR1 of two**:
- **PR1 (this spec, SIO-817):** Wrap the `Producer` in a circuit breaker. Five consecutive transport failures open the breaker; subsequent calls fail fast with `CircuitBreakerOpenError` until 60s elapse. Three consecutive successful probes in HALF_OPEN close the breaker. The outbox is the fallback — pending rows accumulate during the open window, and the gateway accepts new webhooks normally.
- **PR2 (future, SIO-819):** Realign the `/healthz` endpoints to `guides/health-check-guide.md`'s three-probe pattern (`/health/live`, `/health/ready`, `/health`) and replace ad-hoc requiredness with the guide's `critical` + `hasFallback` aggregator. PR2 is where breaker state finally affects HTTP status.

The previously-proposed `ProducerHandle.recreate()` work (originally SIO-818) is dropped from the roadmap. It was justified by the wedged-singleton theory that the 2026-05-21 diagnosis invalidated. If a future failure genuinely needs producer recreation we can revisit.

## Goals

- Calls to `Producer.sendByTopic` route through a circuit breaker (per `guides/circuit-breaker-guide.md` §3 FSM).
- Five consecutive failures open the breaker; subsequent calls fail fast with `CircuitBreakerOpenError` until 60 seconds elapse. Three consecutive successful probes in Half-Open close the breaker.
- The drainer treats `CircuitBreakerOpenError` as a non-attempt: row stays pending, `attempts` does not increment, `next_attempt_at` is set to the breaker's `nextAttemptAt`.
- Breaker state transitions emit structured info logs under component `kafka.breaker` with `event_name=circuit_breaker_{opened,closed,half_open}`.
- `/healthz` body surfaces the current `breakerState` under `dependencies.kafkaProducer`. The HTTP status code is unchanged in this PR.
- The 60s heartbeat log includes `producerBreaker.{state, failures}`.
- The breaker is configurable via env vars (`CIRCUIT_BREAKER_FAILURE_THRESHOLD`, `CIRCUIT_BREAKER_SUCCESS_THRESHOLD`, `CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS`) per the guide's pattern, with the guide's defaults (5 / 3 / 60_000).

## Non-goals

- Recreating the inner `Producer` instance (dropped from the roadmap — see Context).
- Changing `/healthz` HTTP status semantics (PR2 / SIO-819).
- Splitting `/healthz` into three endpoints per the health-check guide (PR2 / SIO-819).
- OpenTelemetry / Prometheus metric exports — observability remains structured-log-only per CLAUDE.md.
- Wrapping consumer paths or admin client calls — only the Producer is in scope.
- Per-route or per-topic breakers — one breaker per gateway process, one Kafka dependency.

## Architecture

Three new units plus targeted modifications.

```
src/
  resilience/                      NEW
    circuit-breaker.ts             FSM per circuit-breaker-guide §4 reference
    errors.ts                      CircuitBreakerOpenError + isApplicationLevelError
  kafka/
    producer.ts                    UNCHANGED — raw client stays uncoupled
    producerHandle.ts              NEW — owns Producer + breaker; implements EventProducer
  outbox/
    drainer.ts                     MODIFY — catch CircuitBreakerOpenError specifically
  gateway/
    routes.ts                      MODIFY — surface breakerState in /healthz body
    index.ts                       MODIFY — construct ProducerHandle; heartbeat snapshot
  config/
    defaults.ts                    ADD breaker block
    envMapping.ts                  ADD env mapping
    schemas.ts                     ADD breakerSchema
```

### Boundaries

- **`CircuitBreaker`** (`src/resilience/circuit-breaker.ts`) is the pure FSM from the guide §4. Knows nothing about Kafka. Accepts an `isTransportError: (err) => boolean` predicate so the same breaker class can be reused for any dependency.
- **`ProducerHandle`** (`src/kafka/producerHandle.ts`) implements `EventProducer` exactly. Drainer and webhook handler receive a handle — they do not know a breaker exists, except they must handle `CircuitBreakerOpenError` thrown from `sendByTopic`.
- **`outbox/drainer.ts`** gains exactly one new catch branch: `if (err instanceof CircuitBreakerOpenError) { ... }`. Everything else in the drainer is unchanged.
- **`gateway/index.ts`** replaces `createProducer(...)` with `createProducerHandle(...)`. Producer construction lives behind the handle's factory. Shutdown order is unchanged.

### `CircuitBreaker` interface

Mirrors the guide §4 reference, including `forceOpen()` and `reset()` for operator use. Adds one observability accessor:

```ts
class CircuitBreaker {
  constructor(name: string, config: CircuitBreakerConfig, isTransportError: (err) => boolean);
  execute<T>(operation: () => Promise<T>): Promise<T>;
  getState(): "closed" | "open" | "half-open";
  getSnapshot(): { state, failures, nextAttemptAt: number | null };
  forceOpen(): void;
  reset(): void;
}
```

The `getSnapshot()` accessor returns a serializable view for `/healthz` and heartbeat. `nextAttemptAt` is the epoch-ms timestamp when the breaker will next probe (only meaningful when state === "open").

### `ProducerHandle` interface

```ts
export type ProducerHandle = EventProducer & {
  getBreakerSnapshot(): { state, failures, nextAttemptAt: number | null };
};

export async function createProducerHandle(
  clientId: string,
  provider: KafkaProvider,
  breakerConfig: CircuitBreakerConfig,
): Promise<ProducerHandle>;
```

The handle owns a single inner `Producer` and a single `CircuitBreaker` instance. `sendByTopic` is `(...args) => breaker.execute(() => inner.sendByTopic(...args))`. `isConnected` and `disconnect` delegate directly to the inner producer (the breaker has no shutdown work).

### Error classification

`src/resilience/errors.ts` exports `isApplicationLevelError(err: unknown): boolean`. Initial implementation is the conservative default-trip:

```ts
export function isApplicationLevelError(_err: unknown): boolean {
  // PR1: all sendByTopic errors trip the breaker. As we observe specific
  // application-level patterns from @platformatic/kafka in production
  // (e.g. UNKNOWN_TOPIC_OR_PARTITION on a deploy-config bug), we extend
  // this predicate to return true for those — keeping the breaker
  // unaffected by errors that aren't transport failures.
  return false;
}
```

The breaker is constructed with `(err) => !isApplicationLevelError(err)` as its `isTransportError` predicate. Per circuit-breaker-guide §7, this is the conservative default for any new integration.

## Data flow

### Steady-state publish (breaker CLOSED)

1. `drainer.runOutboxIteration` reads a batch of pending rows.
2. For each row: `producerHandle.sendByTopic(topic, key, payload, headers)`.
3. Handle delegates to `breaker.execute(() => inner.sendByTopic(...))`.
4. Breaker state is `closed` → calls the operation.
5. Inner producer publishes successfully → `breaker.onSuccess()` resets `failures` to 0.
6. Drainer marks row dispatched; `DrainMetrics.recordPublished(topic)`.

### Wedge detection (CLOSED → OPEN)

Iterations 1–4: `sendByTopic` throws (e.g. "metadata failed 4 times."). The breaker's `isTransportError` predicate returns true (default-trip), so `breaker.onFailure()` increments `failures`. The original error propagates to the drainer's existing catch (existing `attempts++` and exponential-backoff path).

Iteration 5: `failures` reaches `failureThreshold=5` → `transitionToOpen()` fires. The breaker emits:

```json
{ "event_name": "circuit_breaker_opened", "breaker": "kafka-producer",
  "from": "closed", "failures": 5,
  "next_attempt_at": "2026-05-21T08:01:00.000Z",
  "last_error": "metadata failed 4 times.",
  "message": "circuit breaker opened" }
```

The original error still propagates from this 5th call — the drainer treats it as a normal failure for that one row. The breaker's state has already transitioned to `open` synchronously before the error propagates, so the next drainer iteration observes the new state and starts fail-fast behavior immediately.

### Fail-fast iterations (OPEN)

Subsequent drainer iterations call `sendByTopic`. The breaker is OPEN and `Date.now() < nextAttemptTime`, so it throws `CircuitBreakerOpenError(nextAttemptAt)` without calling the inner producer.

The drainer's catch detects the new error type:

```ts
} catch (err) {
  metrics?.recordError(row.topic, message);

  if (err instanceof CircuitBreakerOpenError) {
    db.query("UPDATE outbox SET next_attempt_at = $next, last_error = $err WHERE id = $id").run({
      id: row.id,
      next: err.nextAttemptAt.getTime(),
      err: "circuit_breaker_open",
    });
    deferred += 1;
    log.debug({ topic: row.topic, id: row.id, nextAttemptAt: err.nextAttemptAt }, "publish deferred; breaker open");
    continue;
  }

  // existing path: attempts++, exponential backoff, eventual maxAge-failure
  ...
}
```

The row's `attempts` count is unchanged; only `next_attempt_at` is updated. The row's age clock keeps ticking (`created_at` is unchanged), so a real outage longer than `OUTBOX_MAX_AGE_HOURS` still ages rows out — but each row is attempted at most once per breaker recovery window during the outage, rather than every drainer iteration.

`IterationResult` gains a `deferred: number` counter (rows skipped due to breaker-open) so the drainer's existing observability surface includes it.

### Recovery (OPEN → HALF_OPEN → CLOSED or back to OPEN)

After `recoveryTimeoutMs=60_000` elapses, the next drainer iteration's first `sendByTopic` triggers the breaker's `transitionToHalfOpen()`. The breaker calls the inner producer.

- **Success**: `onSuccess()` increments `successes` (now 1, threshold 3). Stays HALF_OPEN. Next two successful iterations close the breaker. `transitionToClosed()` emits `circuit_breaker_closed`.
- **Failure**: `onFailure()` from HALF_OPEN unconditionally returns to OPEN. `transitionToOpen()` emits another `circuit_breaker_opened` log with `from: "half-open"`. `nextAttemptTime` resets to `now + 60_000`.

A dependency that stays unreachable longer than the recovery window cycles OPEN ↔ HALF_OPEN ↔ OPEN indefinitely. The drainer's outbox keeps buffering during this time. Steady-state remediation is whatever fixes the underlying outage (broker restart, env-var correction, network repair); no breaker-side automation tries to do anything fancier than the half-open probe.

### Concurrent sendByTopic calls

The breaker FSM mutates state synchronously inside `execute()`. Two concurrent `sendByTopic` calls on the same handle both reach the breaker; the second call's state read happens after the first's state mutation completes its synchronous portion. JavaScript's single-threaded model guarantees no torn reads on `state` / `failures` / `nextAttemptTime`. The single-probe-in-HALF_OPEN guarantee is preserved because once the first probe call enters `execute`, state transitions to HALF_OPEN; a concurrent second call observes `state === "half-open"` and proceeds (no extra rejection logic needed — the guide §4 reference implementation accepts this).

## Configuration

New 4-pillar block under `config.breaker.*`.

```ts
config.breaker.{
  failureThreshold: 5,        // CIRCUIT_BREAKER_FAILURE_THRESHOLD
  successThreshold: 3,        // CIRCUIT_BREAKER_SUCCESS_THRESHOLD
  recoveryTimeoutMs: 60_000,  // CIRCUIT_BREAKER_RECOVERY_TIMEOUT_MS
}
```

`schemas.ts` enforces `failureThreshold >= 1`, `successThreshold >= 1`, `recoveryTimeoutMs >= 1_000` (per guide §11 anti-pattern: too-short recovery causes probe overlap).

## Error handling

Three error categories at the boundary:

| Category | Example | Breaker counts? | Drainer behavior |
|----------|---------|------------------|-------------------|
| Transport / unknown | "metadata failed", `ECONNREFUSED`, generic Error | Yes (toward `failureThreshold`) | Existing: `attempts++`, exponential backoff, eventual maxAge-fail |
| Application-level | (future) `UNKNOWN_TOPIC_OR_PARTITION` with a known class/code | No | Same as transport — drainer doesn't distinguish |
| `CircuitBreakerOpenError` | Breaker-generated when state === "open" | N/A (self-generated) | New branch: `next_attempt_at = breaker.nextAttemptAt`, no `attempts++` |

The `isApplicationLevelError` predicate is the single point of change as we learn `@platformatic/kafka`'s specific error shapes. PR1 ships with the conservative default-trip implementation; an extension is cheap and safe — adding a case makes that error not count toward the breaker without affecting any other path.

Failures during shutdown: `handle.disconnect()` calls `inner.disconnect()` once; the breaker has no shutdown work. If a publish is in-flight when shutdown begins, the existing handler returns 500 to the caller and the row stays pending — unchanged behavior.

## /healthz body

The existing `dependencies.kafkaProducer` field gains `breakerState` and (when state === "open") `breakerNextAttemptAt`. `breakerOpenedAt` is intentionally NOT surfaced — operators can derive "when did this open" from the `circuit_breaker_opened` event in the log stream (the guide §10 prescribes `next_attempt_at` for the transition log, which we follow; an opened-at field has no precedent in either guide). Steady-state body excerpt:

```json
"dependencies": {
  "kafkaProducer": {
    "ok": true,
    "lastCheckedAt": 1737360000000,
    "connected": true,
    "breakerState": "closed"
  },
  ...
}
```

Breaker-open body excerpt:

```json
"dependencies": {
  "kafkaProducer": {
    "ok": true,
    "lastCheckedAt": 1737360000000,
    "connected": true,
    "breakerState": "open",
    "breakerNextAttemptAt": "2026-05-21T08:01:00.000Z"
  },
  ...
}
```

**HTTP status code is unchanged in this PR.** Producer's `isConnected()` still drives 200 vs 503. The breaker is purely informational here. PR2 makes the breaker state participate in the readiness aggregation per `guides/health-check-guide.md`.

`HealthMonitor` reads `producerHandle.getBreakerSnapshot()` during each probe cycle and includes the result in the `kafkaProducer` `DependencyStatus`. This means the existing 30s probe cadence determines how fresh the `breakerState` field in `/healthz` is — typical staleness ≤ 30s. State-transition logs are immediate (fired by the breaker itself, not gated on the monitor).

## Heartbeat

The existing 60s heartbeat snapshot (`src/gateway/index.ts`) gains a `producerBreaker` field:

```ts
producerBreaker: {
  state: handle.getBreakerSnapshot().state,
  failures: handle.getBreakerSnapshot().failures,
}
```

Operators reading the heartbeat log stream over time see breaker state at 60s granularity in addition to the immediate `circuit_breaker_*` transition events.

## DrainMetrics

`DrainMetrics` gains one cumulative counter:

```ts
breakerOpenCount: number;  // total transitions from non-open to open since process start
```

The counter is incremented via `DrainMetrics.incrementBreakerOpenCount()` (the public setter from the file change list above), which the breaker calls from `transitionToOpen()` through a callback passed at breaker construction time. This keeps the breaker generic (no `DrainMetrics` dependency) and lets `DrainMetrics` own the counter's lifecycle. Visible in `/healthz` body under `outbox` and in the heartbeat snapshot.

Use case: a counter that climbs while `state === "closed"` indicates flapping — the breaker is opening and recovering but each window matters operationally.

## Files modified / added

| File | Change |
|------|--------|
| `src/resilience/circuit-breaker.ts` | New — pure FSM per guide §4 |
| `src/resilience/errors.ts` | New — `CircuitBreakerOpenError`, `isApplicationLevelError` |
| `src/kafka/producerHandle.ts` | New — wraps `Producer` + breaker; implements `EventProducer` |
| `src/kafka/producer.ts` | Unchanged |
| `src/outbox/drainer.ts` | Catch `CircuitBreakerOpenError`; add `deferred` to `IterationResult` |
| `src/outbox/metrics.ts` | Add `breakerOpenCount` counter + setter |
| `src/gateway/routes.ts` | Surface `breakerState` in `/healthz` body |
| `src/gateway/index.ts` | Construct `ProducerHandle` in place of `Producer`; heartbeat snapshot gains `producerBreaker`; pass breaker-open callback to `DrainMetrics` |
| `src/health/monitor.ts` | Read `producerHandle.getBreakerSnapshot()` per cycle; include in `kafkaProducer` `DependencyStatus` |
| `src/health/types.ts` | Extend `DependencyStatus` with optional `breakerState`, `breakerNextAttemptAt` |
| `src/config/defaults.ts` | Add `breaker` block |
| `src/config/envMapping.ts` | Add `CIRCUIT_BREAKER_*` env mappings |
| `src/config/schemas.ts` | Add `breakerSchema` |
| `test/unit/resilience.circuit-breaker.test.ts` | New — FSM tests per guide §9 |
| `test/unit/kafka.producer-handle.test.ts` | New — handle delegation + breaker integration |
| `test/unit/outbox.drainer.test.ts` | Extend — `CircuitBreakerOpenError` deferral path |
| `test/unit/gateway.routes.dispatch.test.ts` | Extend — `breakerState` field in `/healthz` body |
| `CLAUDE.md` | Document `config.breaker.*` and the breaker module |
| `.env.example` | Document `CIRCUIT_BREAKER_*` env vars |

## Testing

### `test/unit/resilience.circuit-breaker.test.ts`

Mirrors circuit-breaker-guide §9 tests against the local FSM:

- Opens after `failureThreshold` consecutive failures
- Rejects with `CircuitBreakerOpenError` when open
- Transitions to half-open after `recoveryTimeoutMs` (`Bun.sleep` with small recovery for fast tests)
- Closes after `successThreshold` successful probes in half-open
- Returns to open on first failure in half-open
- Does NOT trip on errors the predicate classifies as application-level
- `forceOpen()` and `reset()` work
- `getSnapshot()` returns the documented shape

### `test/unit/kafka.producer-handle.test.ts`

- Delegates `sendByTopic` to inner producer when breaker is closed
- Records breaker failure on inner producer throw (existing inner-error propagates)
- After 5 throws, subsequent `sendByTopic` throws `CircuitBreakerOpenError` without calling inner
- `disconnect()` calls inner.disconnect()
- `getBreakerSnapshot()` returns the live breaker state
- Breaker does NOT trip on errors the predicate excludes

### `test/unit/outbox.drainer.test.ts` (extend)

- "defers `next_attempt_at` when sendByTopic throws CircuitBreakerOpenError" — fake handle that throws CBO with `nextAttemptAt = now + 30000`; assert row's `next_attempt_at` is set to that value AND `attempts` did not increment
- "increments `deferred` in IterationResult on CBO" — asserts the new counter

### `test/unit/gateway.routes.dispatch.test.ts` (extend)

- "`/healthz` body includes `dependencies.kafkaProducer.breakerState`" — fake monitor snapshot includes `breakerState: "open"`; assert response body surfaces it

### Manual verification

Documented in the plan file. Boot the gateway against a running Redpanda; kill Redpanda; post 6 webhooks; assert `/healthz` shows `breakerState: "open"`; restart Redpanda; wait > 60s; assert state returns to `"closed"`.

## Observability

State-transition logs follow `bun-logging-guide.md` ECS structure plus the `event_name` convention from circuit-breaker-guide §10:

```json
{
  "@timestamp": "...",
  "log.level": "info",
  "component": "kafka.breaker",
  "event_name": "circuit_breaker_opened" | "circuit_breaker_closed" | "circuit_breaker_half_open",
  "breaker": "kafka-producer",
  "from": "<previous state>",
  "failures": <number>,
  "next_attempt_at": "<ISO timestamp, only on opened>",
  "last_error": "<string, only on opened>",
  "message": "circuit breaker <opened|closed|half-open>"
}
```

Alerting runbook (documented in `docs/operations/`, separate change after PR merges):

- **Page** when `circuit_breaker_opened` is observed and no `circuit_breaker_closed` follows within 2 × `recoveryTimeoutMs` (= 2 minutes default).
- **Warn** when `breakerOpenCount` rises by ≥ 3 within one hour (flapping pattern).

## Risks and edge cases

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| `@platformatic/kafka` `Producer.send` internally retries 4 times before throwing. We may see only 1 user-visible error per actual outage minute, so reaching `failureThreshold=5` takes ~5 minutes during a real outage. | High | Acceptable — the breaker is intentionally slow to open during transient blips. Opening after ~5 minutes still significantly limits the wasted work versus letting every drainer iteration call a broker we know is unreachable. The threshold is env-tunable per the guide §5. |
| The default-trip predicate (`isApplicationLevelError → false`) means a real broker error like `UNKNOWN_TOPIC_OR_PARTITION` would count toward the breaker. | Medium | Acceptable for PR1 — a topic missing on the broker is a config error the operator wants visible. The breaker opening on it is a louder signal than silent retries. PR1 documents the predicate extension path. |
| Concurrent `sendByTopic` calls in HALF_OPEN could each call the inner producer. | Low | JS single-thread keeps state mutations atomic; the second call observes the post-transition state. Guide §4 reference accepts this; no extra serialization needed. |
| `next_attempt_at = breaker.nextAttemptAt` could cluster all deferred rows for the same wake-up moment — thundering herd when breaker transitions to HALF_OPEN. | Low | Drainer's `batchSize` limits how many rows it actually attempts per iteration. The breaker only allows one through before HALF_OPEN → CLOSED or back to OPEN. Worst case: drainer pulls a batch, first row triggers HALF_OPEN probe, remaining rows in the batch each call `sendByTopic` and either all succeed (CLOSED, normal drain) or hit the now-OPEN-again breaker and re-defer. No herd amplifies a real outage. |
| `breakerOpenCount` lives in `DrainMetrics` which is in-memory only. Lost on restart. | Low | Acceptable — restart history is captured in CloudWatch logs via the transition events. The counter is for operator-visible "is the gateway flapping right now" not historical analysis. |

## Out of scope

- Producer recreation (originally proposed as PR2; dropped from the roadmap — see Context).
- `/healthz` HTTP status driven by breaker state (PR2 / SIO-819).
- `/health/live` and `/health/ready` endpoints (PR2 / SIO-819).
- Prometheus or OpenTelemetry exports.
- Wrapping the Admin probe in a breaker.
- Per-route or per-topic breakers.
- Disconnect-and-reconnect logic at the breaker layer (the inner Producer lifecycle stays untouched; producer recreation was previously planned and is now dropped — see Context).
- Persisting breaker state across process restarts.

## Verification

```bash
# Unit tests + typecheck
bun run typecheck
bun test

# Manual verification against Redpanda
docker compose up -d
bun run dev:gateway &

# 1. Healthy baseline
curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'
# expect: { ok: true, breakerState: "closed", ... }

# 2. Kill broker, force consecutive failures
docker compose stop redpanda
for _ in 1 2 3 4 5 6; do
  curl -X POST -H 'content-type: application/json' \
    -d '{"resourceId":"x"}' http://localhost:3000/webhooks/elastic/autoops
done

# Wait for drainer to attempt enough times for breaker to open
sleep 10
curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'
# expect: { breakerState: "open", breakerNextAttemptAt: "...", ... }

# Inspect logs for the transition
docker logs <gateway-container> 2>&1 | grep circuit_breaker_opened

# 3. Restart broker
docker compose start redpanda

# Wait through recovery + half-open probe + success-threshold
sleep 65
curl -sS http://localhost:3000/healthz | jq '.dependencies.kafkaProducer'
# expect: breakerState transitions through "half-open" to "closed"

# 4. Heartbeat shows producerBreaker state
docker logs <gateway-container> 2>&1 | grep gateway.heartbeat | tail -1 | jq '.producerBreaker'
```
