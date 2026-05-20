# Healthz dependency probes and outbox observability — Design

**Date:** 2026-05-20
**Status:** Approved (pending user review of spec)
**Author:** Simon Owusu

## Context

The current `/healthz` endpoint reports two things: a flag the producer sets to `true` on construction (`isConnected`) and three SQLite counts (`pending`, `failed`, `oldestPendingAgeMs`). That isn't enough to answer the questions an operator asks during an incident:

- Is the Kafka broker actually reachable, or is the producer flag stale?
- Do the topics we're configured to publish to actually exist on the cluster?
- Is the outbox file open and writable?
- Is the drainer making progress, or is the backlog growing because publishes are failing silently?
- Which topic is the bottleneck when multiple routes share an instance?

Today, the gateway only logs at lifecycle events (boot, route registration, shutdown) and on outbox-row max-age give-up. A drainer that has been failing every publish for ten minutes leaves no trace in the log stream — the only visible signal is `pending` slowly climbing.

This design adds (1) a background dependency-health monitor with a cached snapshot served by `/healthz`, (2) richer drain metrics (throughput, last-published-at, per-topic backlog, last-error), and (3) lifecycle logs that surface state transitions, drainer errors, and a periodic stats heartbeat. Nothing in the gateway's accept-write-202 hot path changes.

## Goals

- `/healthz` answers, in one call, "is every dependency I rely on healthy?" — producer, outbox DB, Kafka broker, configured topics.
- Operators can see in the response body whether the drainer is making progress (`publishedLast60s`, `lastPublishedAt`) and which topic is backed up (`pendingByTopic`).
- "Topic not available" is visible — both in the healthz response (`dependencies.topics.missing`) and in drainer error logs that quote the broker's error text.
- Health checks don't hammer Kafka — expensive probes run on a 30 s background timer and `/healthz` returns instantly from cache.
- Log volume stays bounded: state-transition logs fire once per flip; a single 60 s heartbeat line carries the running stats.

## Non-goals

- OpenTelemetry / Prometheus metrics export. Stays text/JSON for now; if downstream wants Prom, that's a separate spec layered on top of the same metrics surface.
- Per-route latency histograms.
- Alarming / paging integration — operators consume the JSON and log stream; alerting lives in the platform.
- Replacing the existing `outbox.drainer` warn logs around `maxAgeMs` give-up.

## Architecture

Three new units plus targeted extensions to existing code.

```
src/
  health/                    NEW
    types.ts                 HealthSnapshot, DependencyStatus, DependencyName
    probes.ts                Pure async fns: probeOutboxDb, probeKafkaAdmin
    monitor.ts               HealthMonitor — owns the probe timer + cached snapshot
  outbox/
    metrics.ts               NEW — DrainMetrics: sliding throughput window,
                                   lastPublishedAt, lastError, recordPublished/recordError
    writer.ts                Extend BacklogStats with pendingByTopic
    drainer.ts               Accept a DrainMetrics; call recordPublished/recordError
  kafka/
    producer.ts              Add metadata() pass-through used by the broker probe
  logging/
    heartbeat.ts             NEW — startHeartbeat(): 60 s interval, single info log
  gateway/
    routes.ts                /healthz reads monitor.snapshot() + outbox stats
    index.ts                 Wires monitor + heartbeat into the boot/shutdown sequence
```

### HealthMonitor

Owns a single `setTimeout` loop. On each tick (default 30 s), it runs all probes concurrently with a per-probe timeout (default 5 s), composes a `HealthSnapshot`, diffs against the previous snapshot, and emits an info log for any dependency whose `ok` value flipped.

Probes:

| Name              | Implementation                                                                  | Failure means          |
|-------------------|---------------------------------------------------------------------------------|------------------------|
| `kafkaProducer`   | Synchronous read of `producer.isConnected()` (no network).                       | Producer torn down.     |
| `outboxDb`        | `SELECT 1` against `OutboxDatabase`. Skipped when outbox disabled.               | DB file unreachable.    |
| `kafkaBroker`     | `Admin.listTopics()` via a single long-lived `Admin` client constructed at gateway boot from the producer's `KafkaConnectionConfig`. | Broker unreachable.     |
| `topics`          | Derived from the broker probe — checks each `route.topic` + `route.dlqTopic` against the returned list. | Topic missing on cluster. |

The broker probe and the topic probe share one `listTopics()` round-trip — there's no second network call.

`HealthMonitor` exposes:

```ts
type HealthMonitor = {
  start(): void;
  stop(): Promise<void>;
  snapshot(): HealthSnapshot; // returns cached, never blocks
};
```

The cached snapshot is initialised before `start()` returns, by running the first probe cycle synchronously and awaiting it during gateway boot. That way `/healthz` never returns an empty snapshot.

### DrainMetrics

In-memory counters living inside the drainer process. The drainer's `catch` block calls `metrics.recordError(topic, errText)`; the success path calls `metrics.recordPublished(topic)`.

```ts
type DrainMetrics = {
  recordPublished(topic: string): void;
  recordError(topic: string, message: string): void;
  snapshot(): {
    publishedLast60s: number;
    lastPublishedAt: number | null;
    lastError: { topic: string; message: string; at: number } | null;
  };
};
```

Throughput uses a timestamped ring buffer (push on publish; trim entries older than 60 s on read). No external dependency, no allocations in the hot path beyond a single `number` push.

`pendingByTopic` lives in `outbox/writer.ts` as an extension of `backlogStats()` — `SELECT topic, COUNT(*) FROM outbox WHERE status='pending' GROUP BY topic`.

### /healthz response shape

Existing fields preserved; new fields added under `dependencies` and additional keys inside `outbox`:

```json
{
  "ok": true,
  "status": "healthy",
  "checkedAt": 1737360000000,
  "dependencies": {
    "kafkaProducer": { "ok": true, "connected": true, "lastCheckedAt": 1737359980000 },
    "outboxDb":      { "ok": true, "lastCheckedAt": 1737359980000 },
    "kafkaBroker":   { "ok": true, "lastCheckedAt": 1737359980000, "brokerProbeMs": 42 },
    "topics":        { "ok": true, "lastCheckedAt": 1737359980000, "missing": [] }
  },
  "outbox": {
    "enabled": true,
    "pending": 4,
    "failed": 0,
    "oldestPendingAgeMs": 1240,
    "pendingByTopic": { "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS": 4 },
    "publishedLast60s": 87,
    "lastPublishedAt": 1737359998000,
    "lastError": null
  },
  "routes": [
    { "name": "elastic-autoops", "path": "/webhooks/elastic/autoops", "topic": "T_PRIVATE_SOURCE_ELASTIC_AUTOOPS", "dlqTopic": "DLQ_T_T_PRIVATE_SOURCE_ELASTIC_AUTOOPS" }
  ]
}
```

**Status code rules:**

- `200` when `kafkaProducer.ok && outboxDb.ok && kafkaBroker.ok`. Topic probe failures do **not** demote to 503 — the gateway can still buffer to the outbox while a topic is being created. They surface as `dependencies.topics.ok=false` + a warn log on transition.
- `503` when any of the three required dependencies fail.
- `status` field: `"healthy" | "degraded" | "unhealthy"`. `degraded` means 200 status with at least one non-required dependency (topics) failing.

When `outbox.enabled === false`, `outboxDb` is omitted from `dependencies` and `outbox` reports `{ "enabled": false }` (same as today).

### Lifecycle logging

1. **Startup summary** — replace the existing `"gateway listening"` info log at `src/gateway/index.ts:103` with a richer single-line summary including provider, outbox status (`enabled`, `dbPath`), routes count, dependency probe list, and probe interval. One line, structured.

2. **State transitions** — `HealthMonitor` emits `log.info({ dependency, ok, lastError }, "dependency state changed")` only when the previous snapshot's `ok` value differs from the new one. No log on steady state.

3. **Drainer error log** — inside the existing `catch` block in `src/outbox/drainer.ts:98`, add a warn log carrying `topic`, `attempts`, `ageMs`, and the broker error text verbatim. Today the message is only written to the row's `last_error` column. The existing max-age give-up log stays.

4. **Periodic stats heartbeat** — `startHeartbeat({ intervalMs: 60_000 })` emits one info-level line every 60 s with: `pending`, `failed`, `oldestPendingAgeMs`, `pendingByTopic`, `publishedLast60s`, `lastPublishedAt`, `producerConnected`, `brokerOk`. Single component name `gateway.heartbeat`. When `outbox.enabled === false`, the outbox fields are omitted. `STATS_HEARTBEAT_MS=0` disables.

## Config additions

New 4-pillar block. Defaults in `src/config/defaults.ts`, env mapping in `src/config/envMapping.ts`, Zod schema in `src/config/schemas.ts`.

```ts
config.health.{
  probeIntervalMs: 30_000,   // HEALTH_PROBE_INTERVAL_MS
  probeTimeoutMs:   5_000,   // HEALTH_PROBE_TIMEOUT_MS
  heartbeatMs:     60_000,   // STATS_HEARTBEAT_MS (0 disables)
}
```

All optional with safe defaults — no breaking change to existing deployments.

## Data flow

```
Bun boot
  └─ createKafkaProvider → createProducer → openOutbox → createWriter → startDrainer
                                                                              │
                                                                              ▼
                                                                    DrainMetrics (in-mem)
                                                                              │
  └─ HealthMonitor.start() ◀── reads producer, outboxDb, provider connection ┤
       │   first probe cycle awaited synchronously                            │
       ▼                                                                       │
     cached HealthSnapshot                                                     │
                                                                              │
  └─ startHeartbeat() ──── 60 s tick ─── reads monitor.snapshot() + writer.backlogStats() + metrics.snapshot()
                                                                              │
  └─ Bun.serve(routes)                                                        │
       /healthz handler ──── reads monitor.snapshot() + writer.backlogStats() + metrics.snapshot()
                                                                              │
shutdown signal
  └─ server.stop → drainer.stop → monitor.stop → heartbeat.stop → producer.disconnect → provider.close → closeOutbox
```

## Error handling

- Probe failures are caught inside `HealthMonitor` and converted to `DependencyStatus { ok: false, lastError: message }`. A probe never crashes the monitor loop or the gateway.
- Probe timeouts are enforced with `AbortController` / `Promise.race`. A hung broker doesn't block the next cycle.
- `DrainMetrics.recordError` swallows any internal state mutation errors — telemetry must never break the drain loop.
- `/healthz` itself never awaits a probe — it reads cached state and returns synchronously. The endpoint cannot hang on Kafka.
- Heartbeat failures (e.g., monitor snapshot throws) are caught and logged once at warn level. The interval continues.

## Testing

New unit tests under `test/unit/`:

- `health.probes.test.ts` — each probe in isolation: stub `OutboxDatabase` for `outboxDb`, stub `Admin.listTopics` for broker/topics; covers success, failure, and timeout.
- `health.monitor.test.ts` — caching (snapshot synchronous), interval cadence, state-transition log emitted only on flip (spy on logger), graceful stop.
- `outbox.metrics.test.ts` — throughput window math (fake timers), `lastPublishedAt` updates, `lastError` set/clear, `pendingByTopic` SQL via in-memory DB.
- `logging.heartbeat.test.ts` — fires on interval with expected fields; `STATS_HEARTBEAT_MS=0` disables.
- `gateway.routes.dispatch.test.ts` — extend with new JSON shape, 200/503 cases, topic-missing degraded case (200 + `status: "degraded"`).

Manual probe: bring Redpanda up, delete the `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS` topic, hit `/healthz`, confirm `status: "degraded"` and `dependencies.topics.missing` lists the topic; recreate the topic and confirm the next probe cycle flips `topics.ok` back to true with a state-change log.

## Files modified / added

| File                                              | Change |
|---------------------------------------------------|--------|
| `src/health/types.ts`                             | new |
| `src/health/probes.ts`                            | new |
| `src/health/monitor.ts`                           | new |
| `src/outbox/metrics.ts`                           | new |
| `src/outbox/writer.ts`                            | add `pendingByTopic` to `BacklogStats` |
| `src/outbox/drainer.ts`                           | accept `DrainMetrics`, call record*, log on error |
| `src/kafka/producer.ts`                           | expose admin handle / `metadata()` for probe |
| `src/logging/heartbeat.ts`                        | new |
| `src/gateway/routes.ts`                           | new `/healthz` shape; reads monitor + metrics |
| `src/gateway/index.ts`                            | wire monitor + heartbeat; replace listening log |
| `src/config/defaults.ts`                          | add `health` block |
| `src/config/envMapping.ts`                        | add `HEALTH_*` env mappings |
| `src/config/schemas.ts`                           | add `healthSchema` |
| `test/unit/health.probes.test.ts`                 | new |
| `test/unit/health.monitor.test.ts`                | new |
| `test/unit/outbox.metrics.test.ts`                | new |
| `test/unit/logging.heartbeat.test.ts`             | new |
| `test/unit/gateway.routes.dispatch.test.ts`       | extend |
| `CLAUDE.md`                                       | add `health/` to architecture tree; document config block |
| `.env.example`                                    | document `HEALTH_PROBE_INTERVAL_MS`, `HEALTH_PROBE_TIMEOUT_MS`, `STATS_HEARTBEAT_MS` |

## Risks and edge cases

| Risk                                                                       | Mitigation |
|----------------------------------------------------------------------------|-----------|
| `Admin.listTopics()` adds a steady metadata load on the cluster.           | 30 s default cadence; configurable; one round-trip per cycle. |
| First probe cycle awaited synchronously during boot delays `listen()`.     | Bounded by `probeTimeoutMs` (default 5 s). Could fail open (start with all `ok: true`) if we want zero boot delay — defer unless boot time is a problem. |
| Ring buffer for `publishedLast60s` grows during a burst.                   | Trim on every read and on push when length exceeds an upper bound (e.g., 100k entries). |
| State-transition log spam if a dependency flaps.                           | Acceptable — we want every flap visible. Downstream alerting can de-bounce. |
| `Admin` client lifecycle — opening one per probe is wasteful.              | Construct the `Admin` once at gateway boot from the same `KafkaConnectionConfig`; close on shutdown. |

## Out of scope

- Prometheus/OTel exporters.
- Per-request latency in `/healthz`.
- Alarming on outbox backlog (downstream concern).
- A `/metrics` Prometheus endpoint — the JSON in `/healthz` carries everything; if a Prom exporter is wanted later it can read the same monitor + metrics surface.
- Auth on `/healthz` — stays public (read-only, no secrets in response).
- Multi-process metric aggregation — single-process service.

## Verification

```bash
bun run typecheck
bun test
docker compose up -d
bun run dev:gateway &

# Healthy baseline
curl -sS http://localhost:3000/healthz | jq

# Force topic-missing degraded
docker compose exec redpanda rpk topic delete T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
sleep 35   # wait for next probe cycle
curl -sS http://localhost:3000/healthz | jq '.status, .dependencies.topics'
# expect: "degraded", { ok: false, missing: ["T_PRIVATE_SOURCE_ELASTIC_AUTOOPS"], ... }

# Restore
docker compose exec redpanda rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS
sleep 35
curl -sS http://localhost:3000/healthz | jq '.status'
# expect: "healthy"

# Force broker outage
docker compose stop redpanda
sleep 35
curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:3000/healthz
# expect: 503
```
