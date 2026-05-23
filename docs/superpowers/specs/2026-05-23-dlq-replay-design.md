# Design: self-managed DLQ replay for eventgate-gateway

**Date:** 2026-05-23
**Status:** Approved by user, plan exists at `docs/superpowers/plans/2026-05-23-dlq-replay.md`
**Linear:** [SIO-827](https://linear.app/siobytes/issue/SIO-827/self-managed-dlq-replay-for-eventgate-gateway)
**Related:** Builds on `2026-05-20-admin-routes-endpoint-design.md` (reuses the `ADMIN_TOKEN` gate + reserved-path pattern) and `2026-05-21-producer-circuit-breaker-design.md` (replay re-produce flows through the breaker).

## Goal

Move DLQ replay from an external one-off script into the gateway itself, exposed over an authenticated API, configurable, with triage and loop-protection built in. The gateway already owns the producer, the route → DLQ topic mapping, an admin surface, and a SQLite datastore; adding replay is a natural extension, not a separate service.

## Why

When a downstream Kafka Connect sink (Couchbase, Elastic, ...) goes through a transient failure window — broker hiccup, sink offline for maintenance, network blip — Connect drops the affected records into the route's companion `DLQ_T_*` topic. Today, recovery looks like: a human runs an external Python script, hand-rolled per incident, with no triage and no loop protection. The script's main failure modes:

1. **No triage** — every DLQ record gets replayed, including poison-pill records that will fail again the same way. Burns broker bandwidth and grows the DLQ on the next pass.
2. **No loop protection** — if a record's underlying error is not actually transient (e.g. schema/validation failure), it bounces forever between source and DLQ.
3. **No state** — each invocation re-scans the entire DLQ from offset 0. Slow, and cumulatively duplicative against any sink that doesn't dedupe.
4. **External tool drift** — the script lives in someone's `~/scripts/`; broker auth changes, env-var conventions, route additions all silently rot the tool.

Embedding the workflow in the gateway fixes all four: triage uses the Connect dead-letter headers the broker already stamps; loop protection uses an `x-eventgate-replay-attempt` header the gateway writes on every replay; state lives in the same SQLite file as the outbox; and the producer/provider/auth are the same code paths the gateway already exercises in production.

This work also takes one previously-deferred item off the table: webhook auth for `/admin/replay/*` reuses the existing `ADMIN_TOKEN` gate, no new auth surface.

## Non-goals

- **Webhook auth for public webhook paths.** The v2 webhook-auth decision stands; only `/admin/*` is protected.
- **Multi-process job coordination.** Single gateway process per outbox file. Two gateways sharing an outbox file are a misconfiguration (same constraint as the drainer).
- **Event-driven replay-on-every-DLQ-write.** That is the canonical way to build an infinite loop. Batched + triaged + attempt-capped is the safe shape.
- **Per-user audit log.** The token holder is the only identity recorded; operator identity is the deployment's concern.
- **Application-layer TLS / mTLS.** Network-layer protection (ALB, mesh) is assumed.
- **A `/admin/replay/:jobId/resume` endpoint.** Paused jobs become inputs to a fresh job; resume would re-introduce all the cancel/breaker race conditions.
- **UI / dashboard.** `/admin/dlq` returns JSON; operators script against it.
- **Changes to existing outbox or producer behavior beyond reuse.** Replay re-produces through `ProducerHandle.sendByTopic` unchanged; the circuit breaker covers it automatically.
- **A CLI for replaying `failed` outbox rows.** Different problem (outbox, not Kafka DLQ); defer until called for.

## Final contract

### Endpoints (all behind `verifyAdminToken` via `X-Admin-Token`)

Registered only when `REPLAY_ENABLED=true` AND `ADMIN_TOKEN` set. Otherwise: no routes registered, no SQLite tables created, no consumer code loaded.

| Method | Path | Body / params | Response |
|---|---|---|---|
| `GET` | `/admin/dlq` | — | `200 { routes: [{ route, dlqTopic, partitions: [{ partition, depth, observedAt, ageMs, depthError? }], lastJob? }] }` |
| `POST` | `/admin/replay/:route` | `{ partition: number, dryRun?: boolean=true, fromOffset?: number, toOffset?: number, maxRecords?: number, filter?: { exceptionClass?: string } }` | `202 { jobId, status, dryRun }` |
| `POST` | `/admin/replay/:route/message` | `{ partition: number, offset: number, dryRun?: boolean=true }` | `200 { decision: TriageDecision, replayed: boolean, parked: boolean }` |
| `GET` | `/admin/replay/:jobId` | — | `200 ReplayJob` or `404` |
| `POST` | `/admin/replay/:jobId/cancel` | — | `200 { jobId, cancelled: boolean }` |

Status codes mirror [src/admin/routesEndpoint.ts](../../src/admin/routesEndpoint.ts):

| Code | Meaning |
|---|---|
| 401 | `X-Admin-Token` missing or wrong |
| 400 | invalid JSON body or Zod failure (`{error, issues}`) |
| 404 | unknown `:route` or `:jobId` |
| 500 | persist or producer failure |

### Triage decision tree

`DlqRecord.headers` carries the **raw array** `Array<[Buffer|null, Buffer|null]>` taken directly from `KafkaRecord.headers` — not the SDK's collapsed `Map<Buffer,Buffer>`. Reason: Kafka allows duplicate header names; collapsing would silently drop a second `idempotencyKey` or audit header. `readHeader(record, name)` iterates and returns the first match via `Buffer.equals`; returns `""` for present-but-null values.

```
raw = readHeader(rec, "x-eventgate-replay-attempt")
parsed = raw === undefined ? 0 : Number(raw)             // strict parse: "5abc" -> NaN
attempt = Math.max(0, Number.isFinite(parsed) ? parsed : 0)
if attempt >= cfg.maxAttempts -> park (exceeded_attempts)

excClass = readHeader(rec, "__connect.errors.exception.class.name") ?? null
if excClass && cfg.poisonErrors.includes(excClass)    -> park (poison_class)
if excClass && cfg.transientErrors.includes(excClass) -> replay
fallback: cfg.default === "replay" ? replay : park (default_park)
```

### Re-produce semantics

1. Iterate the raw header array; drop entries whose utf-8 key starts with `__connect.errors.`.
2. Preserve every other header **as raw `Buffer`** — the original `idempotencyKey` (and any future binary header) survives bit-for-bit. Downstream sinks dedupe on `idempotencyKey`.
3. Append `x-eventgate-replay-attempt: String(attempt + 1)` as a UTF-8 string.
4. Append audit headers (UTF-8): `x-eventgate-replay-job-id`, `x-eventgate-replay-at` (ISO), `x-eventgate-replay-source-topic`, `x-eventgate-replay-source-offset` (`partition:offset`).
5. `producer.sendByTopic(route.topic, key.toString("utf-8"), value.toString("utf-8"), headersOut)`. The producer signature is widened (see "Producer signature change" below) so preserved `Buffer` values pass through unmodified.

On **park**: same audit stamping, no attempt bump; produce to `route.topic + cfg.parkingTopicSuffix` (default `.parked`). Empty suffix → count-only, no produce.

**Replay does not flow through the outbox.** Re-produces go inline through `ProducerHandle.sendByTopic`. If Kafka is unreachable mid-replay, the circuit breaker opens → `CircuitBreakerOpenError` → job pauses. Operator retries by re-issuing the replay; the per-partition `replay_state` watermark ensures we don't re-do completed work.

### Producer signature change

[src/kafka/producer.ts](../../src/kafka/producer.ts) `EventProducer.sendByTopic` currently accepts `headers?: Record<string, string> | null`. Widen to `headers?: Record<string, string> | Array<[string, string | Buffer | null]> | null`. Internal `Producer.send` from `@platformatic/kafka` already accepts `Map<HeaderKey, HeaderValue> | Record<string, HeaderValue>`; the adapter is a thin shim that detects the array form and converts to a Map of `Buffer` keys/values. Drainer and webhook handler callsites are unchanged (they still pass `Record<string, string>`).

### Consumer lifecycle

- Per job via `createReplayConsumer(provider, route, jobId)`. Reuses `provider.getConnectionConfig()` exactly like [src/health/admin.ts:10](../../src/health/admin.ts:10).
- `groupId = \`eventgate-replay-${route.name}-${jobId}\`` (jobId is UUIDv4; never reused).
- `consume({ topics: [route.dlqTopic], mode: "manual", offsets: [{ topic, partition, offset: fromOffset }], maxFetches: ceil(maxRecords/100), autocommit: false })`. Single-partition per job — `offsets` always carries exactly one entry. Never commits.
- `streamRange` iterates with `for await (const msg of stream)`, breaks when `msg.offset > toOffset || count >= maxRecords || signal.aborted`. `finally` closes stream, then consumer, then fires `Admin.deleteGroups({ groups: [groupId] })` as fire-and-forget cleanup (warn-log on failure; group cleanup is hygiene, not correctness).

### Cancel semantics

`AbortController.abort()` halts the iterator but does not abort an in-flight `producer.sendByTopic(...)`. At most one record after the cancel timestamp may be replayed; logged but not counted in `replayed`. At-least-once + sink dedupe makes overrun harmless. Every runner status update goes through `updateJobStatusIfMutable(id, patch)`, which appends `WHERE status NOT IN ('cancelled','done','failed')` — terminal states are sticky.

### Startup ghost-job sweep

After `runReplayMigrations`, gateway runs:

```sql
UPDATE replay_jobs
   SET status='failed', last_error='gateway_restart_orphan', finished_at=$now
 WHERE status IN ('pending','running','paused');
```

Info-logged with count. Same shape as drainer's age-out path.

### DLQ depth probe (Risk: Redpanda)

`/admin/dlq` reads from an in-memory cache in `dlqInspector` keyed by `(route, partition)`, refreshed at `cfg.health.probeIntervalMs` cadence via `Admin.listOffsets`. If the probe fails (e.g. Redpanda's Admin path doesn't implement it):

- response carries `depth: null + depthError` (error class only, capped 200 chars)
- last good depth + `observedAt` + `ageMs` still returned so operators can reason about staleness
- scheduler falls back to a bounded **probe job** that scans up to `cfg.auto.probeWindowRecords` from the last watermark — the count of records scanned is used as a depth proxy

A Phase 3 one-off smoke test against the project's Redpanda compose decides whether the degrade path is the only path or just the fallback. Result lands in `docs/architecture/dlq-replay.md`.

## Config schema additions

```ts
replay: z.strictObject({
  enabled: z.boolean().describe("Master switch. When false, replay endpoints and tables are not created."),
  maxAttempts: z.number().int().min(1).describe("Replay loop-guard cap; parked when exceeded."),
  transientErrors: z.array(z.string().min(1)).describe("Exception class names classified as Transient (replay)."),
  poisonErrors: z.array(z.string().min(1)).describe("Exception class names classified as Poison (park)."),
  default: z.enum(["park","replay"]).describe("Fallback decision when class matches neither list."),
  maxRecordsPerJob: z.number().int().positive(),
  rateLimitPerSec: z.number().int().positive(),
  parkingTopicSuffix: z.string().describe("Empty string disables parking-topic write (count-only)."),
  auto: z.strictObject({
    enabled: z.boolean(),
    intervalMs: z.number().int().min(60_000),
    dlqDepthThreshold: z.number().int().positive(),
    probeWindowRecords: z.number().int().positive().describe("Fallback when listOffsets fails (Redpanda): scan up to N records from last watermark as a depth proxy."),
  }),
}).optional().describe("DLQ replay subsystem; absent when REPLAY_ENABLED=false."),
```

Defaults: `enabled=false, maxAttempts=5, transientErrors=[], poisonErrors=[], default="park", maxRecordsPerJob=10_000, rateLimitPerSec=500, parkingTopicSuffix=".parked", auto={enabled:false, intervalMs:300_000, dlqDepthThreshold:100, probeWindowRecords:500}`.

| Env var | Purpose |
|---|---|
| `REPLAY_ENABLED` | Master switch (default false) |
| `REPLAY_MAX_ATTEMPTS` | Loop-guard cap (default 5) |
| `REPLAY_TRANSIENT_ERRORS` | CSV; classify as Transient |
| `REPLAY_POISON_ERRORS` | CSV; classify as Poison |
| `REPLAY_DEFAULT` | `park` \| `replay` (default park) |
| `REPLAY_MAX_RECORDS_PER_JOB` | default 10000 |
| `REPLAY_RATE_LIMIT_PER_SEC` | default 500 |
| `REPLAY_PARKING_TOPIC_SUFFIX` | default `.parked`; empty disables |
| `REPLAY_AUTO_ENABLED` | default false |
| `REPLAY_AUTO_INTERVAL_MS` | default 300000 |
| `REPLAY_AUTO_DLQ_DEPTH_THRESHOLD` | default 100 |
| `REPLAY_AUTO_PROBE_WINDOW_RECORDS` | default 500 |

## Architecture impact

| Component | Change |
|---|---|
| `src/config/schemas.ts` | Add `replay: z.strictObject({...}).optional()` to `configSchema`. |
| `src/config/defaults.ts` | Add `replay` defaults (every key populated; `enabled=false`). |
| `src/config/envMapping.ts` | Map `REPLAY_*` via existing `bool/num/csv/str/nestedOrUndefined` helpers. |
| `src/config/reservedPaths.ts` | Add `"/admin/dlq"` + `"/admin/replay"` literals; extend `checkReservedPath` with a `startsWith("/admin/replay/")` / `startsWith("/admin/dlq/")` prefix guard. |
| `src/kafka/producer.ts` | Widen `sendByTopic` headers param to accept array-of-tuples form with `Buffer` values. |
| `src/outbox/db.ts` | Additive `runReplayMigrations(db)` creating `replay_jobs` + `replay_state` tables. Called only when `config.replay?.enabled`. |
| `src/admin/replayEndpoint.ts` (NEW) | `makeReplayHandlers(deps)` → `{ listDlq, bulkReplay, singleReplay, jobStatus, cancelJob }`. |
| `src/replay/{types,triage,headers,consumer,runner,jobStore,dlqInspector,scheduler}.ts` (NEW) | Core subsystem (see "Architecture overview" in plan). |
| `src/gateway/routes.ts` | Register replay endpoints conditionally on `config.admin?.token && config.replay?.enabled`. |
| `src/gateway/index.ts` | Build `replayContext` after `healthAdmin`; extend `shutdown()` to cancel jobs + stop scheduler. |
| `.env.example` | Document every `REPLAY_*` var inline. |

## Phased build order

1. **Phase 1 (this PR):** endpoint scaffold + single-message replay + dry-run bulk. Triage logic is stubbed (always returns `replay` for dry-run analysis); SQLite tables not yet created; bulk returns a synthetic jobId. Gives the API surface end-to-end with zero risk to existing behavior.
2. Triage + attempt cap.
3. SQLite tables + `GET /admin/dlq` + `GET /admin/replay/:jobId`.
4. Real bulk replay with rate limiting + parking.
5. (Optional) scheduler for auto-replay.

Each phase is independently mergeable; gateway behavior is unchanged when `REPLAY_ENABLED=false` (default).

## Risks (resolved during planning, see plan for full mitigations)

1. **Idempotency loss on re-produce** — strip only `__connect.errors.*`; preserve everything else as raw Buffer.
2. **Map<Buffer> lookup + duplicate-header fidelity** — use the raw header array.
3. **Redpanda `Admin.listOffsets` incompat** — Phase 3 smoke first; degrade path returns `depth: null + depthError`.
4. **Offset round-trip** — plain `INTEGER` columns; Kafka offsets fit in 2^53.
5. **Cancel/breaker-pause race** — sticky terminal-state guard + startup ghost-job sweep.
6. **Consumer-group hygiene** — UUIDv4 jobId; fire-and-forget `Admin.deleteGroups` in `finally`.
7. **Scheduler thundering herd** — `tickInFlight` flag + route-level `hasActiveJob`.
8. **Malformed/negative attempt header** — `Number()` strict parse + `Math.max(0, parsed)` clamp.

## Acceptance criteria (Phase 1)

- `bun run typecheck` green.
- `bun test` green; new suites cover header read/strip/stamp, dry-run never calls producer, single-message happy path, admin endpoint 401/400/200.
- `REPLAY_ENABLED=false` (default): `config.replay === undefined`; no `/admin/replay*` or `/admin/dlq` keys in the Bun routes map; no `replay_jobs` table in SQLite (`SELECT name FROM sqlite_master`).
- `REPLAY_ENABLED=true` + `ADMIN_TOKEN` set: endpoints register and respond 401 without token; dry-run single-message replay returns correct triage decision against a synthetic DLQ record (smoke-tested against local Redpanda).
