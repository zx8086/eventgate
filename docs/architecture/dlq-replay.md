# DLQ Replay (eventgate)

> In-process replay subsystem for records that downstream Kafka Connect sinks have dropped into a route's companion `DLQ_T_*` topic. Single-process, behind a master `REPLAY_ENABLED` switch + the existing `ADMIN_TOKEN` gate. **All 5 phases shipped:** triage, attempt cap, SQLite-backed jobs + per-partition watermarks, `/admin/dlq` depth probe, real bulk replay with rate limit + parking + breaker-pause, optional auto-replay scheduler.

## Problem

EventGate publishes raw webhook payloads to per-route source topics (`T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>`). Downstream Connect sinks consume those topics and write to Couchbase / Elastic / etc. When a sink hits a transient failure (broker hiccup, sink offline for maintenance, network blip), Connect drops the affected records into the companion DLQ topic (`DLQ_T_<topic>`) and stamps them with `__connect.errors.*` headers describing the failure.

Before this subsystem, recovery meant running an external Python script with three failure modes:

1. **No triage** — every DLQ record gets replayed, including poison-pill records that fail again the same way.
2. **No loop protection** — non-transient errors bounce forever between source and DLQ.
3. **No state** — every run re-scans the DLQ from offset 0.

## Solution

A new in-gateway subsystem that:

- Reads each DLQ record via a short-lived per-job `Consumer` (single partition per job, manual offsets, no commit).
- Classifies via Connect headers (`__connect.errors.exception.class.name`) against operator-configured allow/deny lists.
- Caps retries via a self-stamped `x-eventgate-replay-attempt` header.
- Re-produces transient records through the existing `ProducerHandle` (circuit breaker covers it; sink dedupes on the preserved `idempotencyKey`).
- Sends poison records to a parking topic (`<topic>.parked` by default) for human review.
- Persists job state in the same `outbox.db` SQLite file used by the outbox drainer.

API surface:

```
GET  /admin/dlq                              per-route + per-partition depth + last-job summary
POST /admin/replay/:route                    bulk: { partition, dryRun=true, fromOffset?, toOffset?, maxRecords?, filter? } -> 202 { jobId }
POST /admin/replay/:route/message            single: { partition, offset, dryRun=true } -> 200 { decision, replayed, parked }
GET  /admin/replay/:jobId                    job status
POST /admin/replay/:jobId/cancel             cancel
```

All endpoints behind `X-Admin-Token` (timing-safe compare). Endpoints register only when `REPLAY_ENABLED=true` AND `ADMIN_TOKEN` set.

## Flow

```
operator (or scheduler in Phase 5)
   │
   ▼ POST /admin/replay/:route { partition, dryRun=true }
   │
   ▼ verifyAdminToken → Zod-validate body → jobStore.create(jobId)
   │
   ▼ createReplayConsumer(provider, route, jobId)        (per-job groupId)
   │
   ▼ consumer.streamRange({ partition, fromOffset, toOffset, maxRecords, signal })
   │
   ▼ for each DlqRecord:
       │
       ▼ triage(rec, cfg, currentAttempt)                  (Phase 2)
       │
       ├── replay → strip __connect.errors.*, preserve other headers as raw Buffer,
       │             stamp audit headers, ++attempt,
       │             producer.sendByTopic(route.topic, key, value, headers)
       │
       ├── park   → stamp audit headers (no attempt bump),
       │             producer.sendByTopic(route.topic + cfg.parkingTopicSuffix, ...)
       │
       └── dryRun → count + classify only; never call producer

   │
   ▼ jobStore.update(jobId, {scanned, replayed, parked, lastOffset, status})
   │
   ▼ consumer.close()  →  Admin.deleteGroups({ groups: [groupId] })  (fire-and-forget hygiene)
```

## File layout

```
src/replay/
  types.ts          DlqRecord, TriageDecision, ReplayJob, ReplayBatchResult
  headers.ts        readHeader (iteration via Buffer.equals), strip, stamp, parseAttempt
  triage.ts         pure triage(rec, cfg) — transient/poison classification + loop guard
  consumer.ts       createReplayConsumer(provider, route, jobId, groupCleanup?)
                    → { streamRange, fetchOne, close }; close fires groupCleanup fire-and-forget
  runner.ts         runReplayBatchDryRun + runReplayBatch (token-bucket rate limit, breaker-aware)
  jobStore.ts       createReplayJobStore (in-memory) + createSqliteReplayJobStore (persistent)
                    terminal-state sticky via WHERE NOT IN ('done','failed','cancelled')
  dlqInspector.ts   Admin.metadata + Admin.listOffsets per-partition depth; in-memory cache
                    with depthError + ageMs surfacing; Redpanda-safe (depth=null degrade)
  scheduler.ts      auto-replay loop; tickInFlight skip + hasActiveJob gate

src/admin/
  replayEndpoint.ts makeReplayHandlers(deps) → { listDlq, bulkReplay, singleReplay, jobStatus, cancelJob }
```

Wiring lives in `src/gateway/index.ts` (constructs `replayContext` after `healthAdmin`) and `src/gateway/routes.ts` (registers the five HTTP handlers conditionally).

## Schema (Phase 3)

```sql
CREATE TABLE IF NOT EXISTS replay_jobs (
  id            TEXT PRIMARY KEY,
  route         TEXT NOT NULL,
  partition     INTEGER NOT NULL,
  mode          TEXT NOT NULL,                  -- 'manual' | 'auto' | 'single'
  dry_run       INTEGER NOT NULL,
  scanned       INTEGER NOT NULL DEFAULT 0,
  replayed      INTEGER NOT NULL DEFAULT 0,
  parked        INTEGER NOT NULL DEFAULT 0,
  skipped       INTEGER NOT NULL DEFAULT 0,
  errors        INTEGER NOT NULL DEFAULT 0,
  from_offset   INTEGER,                        -- plain INTEGER; Kafka offsets fit in 2^53
  to_offset     INTEGER,
  last_offset   INTEGER,                        -- progress checkpoint
  status        TEXT NOT NULL DEFAULT 'pending', -- pending|running|paused|done|cancelled|failed
  last_error    TEXT,
  next_resume_at INTEGER,
  started_at    INTEGER NOT NULL,
  finished_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_route_started ON replay_jobs (route, started_at DESC);

CREATE TABLE IF NOT EXISTS replay_state (
  route                 TEXT NOT NULL,
  partition             INTEGER NOT NULL,
  last_replayed_offset  INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY (route, partition)
);
```

Tables are created only when `config.replay?.enabled === true` (via `runReplayMigrations(db)`); sites that never use replay keep a clean SQLite file.

## Config

| Env var | Default | Purpose |
|---|---|---|
| `REPLAY_ENABLED` | `false` | Master switch; endpoints + tables off when false |
| `REPLAY_MAX_ATTEMPTS` | `5` | Loop-guard cap via `x-eventgate-replay-attempt` |
| `REPLAY_TRANSIENT_ERRORS` | `[]` | CSV of exception class names → replay |
| `REPLAY_POISON_ERRORS` | `[]` | CSV of exception class names → park |
| `REPLAY_DEFAULT` | `park` | Fallback decision when class matches neither list |
| `REPLAY_MAX_RECORDS_PER_JOB` | `10000` | Per-job ceiling |
| `REPLAY_RATE_LIMIT_PER_SEC` | `500` | Token-bucket throttle (per-job) |
| `REPLAY_PARKING_TOPIC_SUFFIX` | `.parked` | Empty string disables parking-topic write |
| `REPLAY_AUTO_ENABLED` | `false` | Scheduler on/off |
| `REPLAY_AUTO_INTERVAL_MS` | `300000` | Scheduler tick |
| `REPLAY_AUTO_DLQ_DEPTH_THRESHOLD` | `100` | Auto-replay when depth ≥ this |
| `REPLAY_AUTO_PROBE_WINDOW_RECORDS` | `500` | Bounded probe job when listOffsets fails |

## Operational notes

### SDK header lookup

`@platformatic/kafka` 2.1.0 exposes consumed record headers as `Map<Buffer, Buffer>`. Standard JavaScript `Map` uses reference equality for keys, so `headers.get(Buffer.from(name))` returns `undefined` even for a key that's bytewise identical to one already in the Map. Verified by `scripts/replay-sdk-smoke.ts`:

```
{ directHit: "world", freshKeyHit: null, iterationHit: "world" }
```

Therefore `src/replay/headers.ts` `readHeader()` MUST iterate the headers via `Buffer.equals` and never use `Map.get` on a freshly-constructed buffer key. The `DlqRecord` type holds headers as a raw `Array<[Buffer|null, Buffer|null]>` to preserve duplicate-name fidelity that the SDK's Map collapses.

### Redpanda `Admin.listOffsets`

Per `project_redpanda_admin_probe_incompat.md`, some Admin operations fail against Redpanda in production. The dlqInspector is **degrade-tolerant by design**: when `Admin.metadata` or `Admin.listOffsets` throws, the inspector preserves the last good per-partition depth in the cache and stamps every entry with `depthError` (error class only, 200-char cap). `/admin/dlq` therefore always responds 200, and operators distinguish "depth unknown" from "depth = 0" via the explicit `depth: null + depthError` fields. The scheduler skips partitions whose depth is `null` so it never fires a job blind.

**Smoke result (Redpanda v25.3.14, local compose, 2026-05-23):** the happy path is live. Both `Admin.metadata({topics:[dlqTopic]})` and `Admin.listOffsets({topics:[{name, partitions:[{partitionIndex, timestamp: -1|-2}]}]})` respond normally; per-partition depth is computed correctly:

```json
"partitions": [
  { "partition": 0, "depth": 2, "observedAt": 1779571845063, "ageMs": 10303 },
  { "partition": 1, "depth": 0, "observedAt": 1779571845063, "ageMs": 10303 },
  { "partition": 2, "depth": 0, "observedAt": 1779571845063, "ageMs": 10303 }
]
```

The MSK / Confluent Cloud behaviour against the same `@platformatic/kafka` 2.1.0 client has not been re-verified in this commit; the degrade path ships regardless, so a regression on those brokers would surface as `depthError` rather than 5xx — operators will see it in `/admin/dlq` and the scheduler will skip the affected route until depth is known.

### Replay vs the outbox

Replay re-produces **inline** through `ProducerHandle.sendByTopic` — it does **not** write to the outbox. Rationale: the outbox is for new webhook writes that must survive a Kafka outage from the moment of accept; replays are operator-initiated, the operator can re-issue if the breaker opens mid-job. Treating replays as outbox rows would double-buffer them, complicate cancel/resume, and tangle two retry regimes.

When the breaker opens mid-job, the runner persists `status='paused'` + `nextResumeAt`, aborts the iterator, and exits. The operator or scheduler re-issues; the per-partition `replay_state` watermark ensures no duplication of completed work (sink-side dedupe via `idempotencyKey` is the second line of defence).

### Cancel semantics

`AbortController.abort()` halts the stream iterator but does NOT cancel an in-flight `producer.sendByTopic()`. Documented behavior: at most one record after the cancel timestamp may be replayed; it is logged but not counted in `replayed`. At-least-once + sink dedupe makes this overrun harmless.

Every runner status update goes through `updateJobStatusIfMutable(id, patch)` which appends `WHERE status NOT IN ('cancelled','done','failed')` — terminal states are sticky and the race between cancel and the runner's terminal write resolves to whichever lands first.

### Startup ghost-job sweep

Gateway boot runs:

```sql
UPDATE replay_jobs
   SET status='failed', last_error='gateway_restart_orphan', finished_at=$now
 WHERE status IN ('pending','running','paused');
```

Same shape as the drainer's age-out behavior. Operators see crashed jobs as `failed`, not as forever-`running`.

### Operational prerequisite: pre-create the parking topic

The parking topic (`<routeTopic><cfg.parkingTopicSuffix>`, default `<topic>.parked`) must exist on the broker before a non-dry-run replay can park a record. The gateway does **not** auto-create it — same convention as the DLQ topic, which is also externally provisioned. When the parking topic is missing, a park-targeting produce fails with `Unknown topic <…>.parked` and the runner counts an `errors++` for that record, never pauses, never crashes — but the operator sees a stuck `parked: 0, errors: N` in `/admin/replay/:jobId`.

Verified during the 2026-05-23 smoke: before pre-creating the parking topic, a bulk run with one transient + one poison record produced `replayed: 1, parked: 0, errors: 1, lastError: "Unknown topic T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.parked."`. After `rpk topic create T_PRIVATE_SOURCE_ELASTIC_AUTOOPS.parked -p 3`, the next single-message park ran cleanly with `parked: true` and the record landed on the parking topic with audit headers intact and `attempt: "0"` (spec §"On park" — no attempt bump).

**Ops checklist:** when adding a new route, provision three topics: `<sourceTopic>`, `DLQ_T_<sourceTopic>` (Connect writes here), and `<sourceTopic><parkingSuffix>` (eventgate writes parked records here). Or set `REPLAY_PARKING_TOPIC_SUFFIX=""` to disable the parking-topic write (park decisions become count-only).

### End-to-end smoke (2026-05-23)

Exercised against the project's local Redpanda compose with two synthetic DLQ records on partition 0 (offset 0: transient `RetriableException`; offset 1: poison `DataException`). All five endpoints responded as specified:

| Endpoint | Result |
|---|---|
| `GET /admin/dlq` | per-partition depths populated, `depthError` absent |
| `POST /admin/replay/:route/message` (dry-run, transient) | `decision.kind="replay"`, `targetTopic=T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`, `wouldStampHeaders` shows stripped Connect headers + preserved `idempotencyKey` + audit headers with `attempt: "1"` |
| `POST /admin/replay/:route/message` (dry-run, poison) | `decision={kind:"park", reason:"poison_class"}`, `targetTopic=…parked`, `attempt: "0"` |
| `POST /admin/replay/:route/message` (non-dry-run, transient) | `replayed: true`; verified via `rpk consume`: record on source topic with `idempotencyKey: smoke-idem-abc-123` preserved bit-for-bit |
| `POST /admin/replay/:route` (bulk, async) | `202 { jobId, status: "running" }`; subsequent `GET /admin/replay/:jobId` showed terminal `status: "done"` with correct counters |
| `POST /admin/replay/:route/message` (non-dry-run, poison, after parking topic created) | `parked: true`; record on `…parked` topic with audit headers intact |

The Phase 5 scheduler was not exercised in this run (`REPLAY_AUTO_ENABLED=false`); its unit tests (`test/unit/replay.scheduler.test.ts`) cover the threshold gating, `tickInFlight` overlap protection, and resume-from-watermark behaviour.

### Per-job consumer-group cleanup

Every replay job creates a unique consumer group (`eventgate-replay-<route>-<jobId>`, jobId is UUIDv4). Without cleanup these accumulate on the broker (default `offsets.retention.minutes=10080` ≈ 7 days). The runner's `finally` block calls `Admin.deleteGroups({ groups: [groupId] })` fire-and-forget — failure logs `warn` but does not fail the job. Group cleanup is hygiene, not correctness; the existing long-lived `healthAdmin` client handles the delete to avoid spinning up a fresh Admin per job.
