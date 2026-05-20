# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Project-specific documentation lives under `docs/` (index at `docs/README.md`), organised per `guides/documentation-guide.md`. Portable, project-agnostic patterns live under `guides/`. When adding a doc, place it in the matching `docs/` subdirectory and update `docs/README.md`.

## Project Overview

eventgate is a single-process Bun ingestion service for Elastic AutoOps webhook notifications. Flow: Elastic AutoOps → HTTP gateway (Bun.serve) → local SQLite outbox → Kafka (Redpanda locally / AWS MSK / Confluent Cloud). The gateway accepts any valid JSON POST and writes it verbatim to `raw.v1`; the outbox makes accepted webhooks durable against Kafka outages, and a background drainer in the same Bun process publishes pending rows with exponential backoff. Validation, normalization, alerting, and projection are concerns for downstream consumers in other services.

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

## Current State

Single-package Bun project (no workspaces). Conforms to the team `guides/` for: Zod v4 schemas with `.describe()` and `.safeParse()` at config boundaries, the 4-pillar configuration pattern (`src/config/{defaults,envMapping,schemas,loader,index}.ts`), Pino 10 + ECS NDJSON logging via `@elastic/ecs-pino-format` with a synchronous Bun-compatible destination, and `bunfig.toml` test wiring with a silent-log preload. Tests live under `test/unit/`. Kafka backend selection is abstracted behind a `KafkaProvider` factory (`src/kafka/providers/`) — local Redpanda, AWS MSK (IAM/TLS/none), or Confluent Cloud (SASL/PLAIN + TLS). Webhook durability is provided by an in-process SQLite outbox (`src/outbox/`, `bun:sqlite`) drained asynchronously to Kafka with age-based give-up. The gateway does not validate the AutoOps body shape — it accepts any valid JSON and forwards it verbatim. Phases explicitly deferred: OpenTelemetry instrumentation, webhook auth.

## Architecture

```
src/
  config/                 4-pillar config (defaults, envMapping, schemas, loader, index)
    topicPolicy.ts        gateway topic naming policy (T_PRIVATE_SOURCE_*)
    reservedPaths.ts      reserved operational paths (/healthz, /admin/routes)
  gateway/                Bun.serve HTTP receiver
    index.ts              entry point — wires KafkaProvider + EventProducer + outbox
    routes.ts             iterates config.routes; one POST handler per route
    handler.ts            makeWebhookHandler(route, deps) — per-request logic
    idempotencyKey.ts     opportunistic sha256 header for AutoOps-shaped bodies
    idempotencyStrategies.ts  named registry of (body) => key functions
  health/                 background dependency monitor for /healthz
    types.ts              HealthSnapshot, DependencyStatus, DependencyName
    probes.ts             probeOutboxDb, probeKafkaAdmin (with timeout)
    monitor.ts            createHealthMonitor — cached snapshot + state-transition logging
    admin.ts              createHealthAdmin — long-lived @platformatic/kafka Admin client
  admin/                  /admin/routes endpoint (token-auth, hot reload)
    auth.ts               timing-safe X-Admin-Token verification
    routesFile.ts         atomic-write helpers for ROUTES_FILE
    routesEndpoint.ts     makeAdminRoutesHandler factory
  kafka/
    producer.ts           EventProducer wrapping @platformatic/kafka Producer (sendByTopic)
    providers/
      types.ts            KafkaProvider, KafkaConnectionConfig, MskAuthMode
      errors.ts           KafkaProviderError + ProviderErrorCode
      local.ts            PLAINTEXT, no SASL/TLS
      msk.ts              SASL/OAUTHBEARER + TLS (IAM); broker discovery; 60s token cache
      confluent.ts        SASL/PLAIN + TLS
      index.ts            createKafkaProvider(config) factory
  logging/
    index.ts              ILogger + Pino+ECS factory (sync destination)
    heartbeat.ts          startHeartbeat — periodic stats log
  outbox/
    db.ts                 bun:sqlite Database, migrations, WAL pragmas, close handling
    schemas.ts            Zod OutboxRow + topic ("raw" only) / status enums
    writer.ts             enqueue(row) — single-row SQLite insert
    drainer.ts            polling loop, exponential backoff, age-based give-up
    metrics.ts            DrainMetrics — in-memory throughput + lastError
    backoff.ts            pure: nextDelayMs(attempts, capMs)
test/
  preload.ts              sets LOG_LEVEL=silent for bun test
  unit/
    config.kafka-provider.test.ts
    config.outbox.test.ts
    kafka.providers.factory.test.ts
    kafka.providers.msk.test.ts
    outbox.backoff.test.ts
    outbox.writer.test.ts
    outbox.drainer.test.ts
```

### Single-process model

| Process | Entry | Responsibility |
|---|---|---|
| **gateway** | `src/gateway/index.ts` | `POST /webhooks/elastic/autoops` — parse JSON, `outbox.enqueue(row)` (single SQLite insert), return 202. Non-JSON bodies get 400; outbox enqueue failures return 500. The outbox drainer (`src/outbox/drainer.ts`) publishes to Kafka in the background with exponential backoff. `/healthz` reads a cached `HealthMonitor` snapshot. Returns 200 when the producer, outbox DB, and Kafka broker are all healthy; 200 + `status: "degraded"` when only configured topics are missing (gateway keeps buffering); 503 when any required dependency fails. Response includes `dependencies.{kafkaProducer, outboxDb, kafkaBroker, topics}`, `outbox.{pendingByTopic, publishedLast60s, lastPublishedAt, lastError}` plus existing fields. `OUTBOX_ENABLED=false` falls back to inline publish (escape hatch). |

Add new downstream behavior (Slack, PagerDuty, aggregates, sinks into a database) as a **separate consumer service** on `ops.elastic.autoops.raw.v1` (or a future `events.v1` published by a downstream normalizer) — never bolt it into the gateway.

### Kafka topics

Topics are declared per route in `config.routes[]` and validated at startup
against the org-wide naming policy.

- **Gateway-owned** (only this prefix is legal): `T_PRIVATE_SOURCE_<SYSTEM>_<ENTITY>`. The Elastic AutoOps seed route uses `T_PRIVATE_SOURCE_ELASTIC_AUTOOPS`.
- **Companion DLQ** (mandatory): `DLQ_T_<topic>`. Declared on the route via `dlqTopic` so downstream consumers can introspect it; the gateway itself never publishes here.
- **Forbidden**: `T_PUBLIC_*`, `T_PRIVATE_SINK_*`, `T_PRIVATE_*_RICH_NOTIFICATIONS`, `T_PRIVATE_*_EVENTS`, `DLQ_T_*` (as the primary topic), and Kafka/Confluent system prefixes. Each is rejected at config-validation time with a distinct error message.

Adding a route: edit `defaults.ts` (PR) or set `ROUTES_JSON` (env). No
TypeScript change unless the new source needs a custom idempotency strategy
(`src/gateway/idempotencyStrategies.ts`).

### Admin endpoint (optional)

When `ADMIN_TOKEN` (min 32 chars) and `ROUTES_FILE` (path to a writable JSON file) are both set, the gateway registers `PUT /admin/routes` protected by an `X-Admin-Token` header. Body is the full routes array; on success it is validated by the same `routesSchema` used at startup, atomically written to `ROUTES_FILE`, and `server.reload({ routes })` swaps the live route map. Startup precedence for routes is `ROUTES_FILE > ROUTES_JSON > defaults`.

Without `ADMIN_TOKEN`, the endpoint is not registered (returns 404). Without `ROUTES_FILE`, it is also not registered — the gateway logs a warning and refuses to enable an in-memory-only admin surface that would be lost on restart.

`/healthz` and `/admin/routes` are reserved paths; any config route attempting to declare them fails startup with a Zod error.

| Env var | Purpose |
|---|---|
| `ADMIN_TOKEN` | Shared secret for `/admin/routes`. Min 32 chars. Endpoint disabled when unset. |
| `ROUTES_FILE` | Path to mounted JSON file holding the routes array. Required alongside `ADMIN_TOKEN` for the admin endpoint to register. Read on startup; takes precedence over `ROUTES_JSON`. |

### Kafka provider factory

Selected by `KAFKA_PROVIDER`. The factory (`src/kafka/providers/index.ts:createKafkaProvider`) dispatches on the value and returns a `KafkaProvider` that exposes `getConnectionConfig()` and `close()`. The producer (`src/kafka/producer.ts`) is provider-agnostic.

All providers share `KAFKA_PROVIDER`, `KAFKA_CLIENT_ID`, and `KAFKA_BROKERS` (CSV → `string[]`). Per-provider extras:

- **local**: nothing else needed. `KAFKA_BROKERS` defaults to `localhost:9092`.
- **msk**: `MSK_REGION` is required; `MSK_AUTH_MODE` defaults to iam. `KAFKA_BROKERS` is optional when `MSK_CLUSTER_ARN` is set (brokers discovered via `GetBootstrapBrokersCommand`). AWS SDK + signer are lazy-imported so non-MSK runs don't load them.
- **confluent**: `KAFKA_BROKERS` + `CONFLUENT_API_KEY` + `CONFLUENT_API_SECRET`.

Portable pattern lives at `guides/kafka-provider-factory.md`. Project-specific application at `docs/architecture/kafka-provider-factory.md`.

### SQLite outbox (`src/outbox/`)

In-process durable buffer between HTTP accept and Kafka publish, using `bun:sqlite` (synchronous, native to Bun, no external service).

- **Write path** (`writer.ts`): `enqueue(row)` inserts a single `raw` row.
- **Drain path** (`drainer.ts`): polls `WHERE status='pending' AND next_attempt_at<=now`, publishes via `producer.sendByTopic(...)` to `raw.v1`. Busy cadence (`busyPollMs`, default 250ms) when the previous batch was full; idle (`idlePollMs`, default 5s) otherwise.
- **Retry semantics**: exponential backoff `min(2^(attempts-1) * 1s, backoffMaxMs)` (cap 10min by default). At-least-once; downstream consumers may dedupe on the opportunistic `idempotencyKey` Kafka header.
- **Give-up rule**: age-based. When `now - created_at > maxAgeHours * 1h`, row is marked `status='failed'`, surfaced via `/healthz` and a warn log. No attempt-count cap.
- **Durability**: `journal_mode=WAL` and `synchronous=NORMAL` on file-backed DBs. On close, `SQLITE_FCNTL_PERSIST_WAL=0` + `wal_checkpoint(TRUNCATE)` so `-wal`/`-shm` sidecars don't linger on macOS (Apple's system SQLite enables persistent WAL by default).
- **Escape hatch**: `OUTBOX_ENABLED=false` makes the gateway publish inline (legacy behavior); no SQLite is opened. `/healthz` reports `outbox: { enabled: false }`.

Project-specific application at `docs/architecture/outbox.md`.

### Opportunistic idempotency key (`src/gateway/idempotencyKey.ts`)

When the JSON body looks AutoOps-shaped, the helper computes `sha256(resourceId :: title :: status :: startTime :: endTime)` and the gateway attaches it as a Kafka header on the `raw.v1` message. Downstream consumers may dedupe on it. For bodies that do not match the shape the helper returns `undefined` and no header is set — the message is still forwarded.

## Config shape (4-pillar)

```
config.app.{name, version, environment}
config.server.{port}
config.kafka.{provider, clientId, brokers, msk:{region, clusterArn, authMode}, confluent:{apiKey, apiSecret}}
config.observability.{logLevel}
config.outbox.{enabled, dbPath, batchSize, backoffMaxMs, maxAgeHours, idlePollMs, busyPollMs, backlogWarnThreshold}
config.health.{probeIntervalMs, probeTimeoutMs, heartbeatMs}
config.routes[].{name, path, topic, dlqTopic, sourceHeader, keyFields, idempotency}
```

- `src/config/defaults.ts` — every key has a default; `version` comes from `package.json`. Default provider is `local`.
- `src/config/envMapping.ts` — explicit env-var → field mapping; returns deep-partial overrides.
- `src/config/schemas.ts` — Zod v4 `strictObject` + `.superRefine()` cross-field rules:
  - `provider=msk` requires `msk.region` AND (`kafka.brokers` non-empty OR `msk.clusterArn`).
  - `provider=confluent` requires `kafka.brokers` non-empty + `apiKey` + `apiSecret`.
  - `provider=local` requires `kafka.brokers` non-empty (default `["localhost:9092"]`).
  - `app.environment=prod` forbids `provider=local`.
- `src/config/loader.ts` — merge defaults + env, validate via `.safeParse()`, lazy Proxy singleton with `resetConfigCache()` for tests.

`config` is a Proxy — validation runs the first time any field is read, not at module import.

## Commands

```bash
bun install
docker compose up -d                                # Redpanda; topics auto-created
bun run dev:gateway                                 # watch mode, port 3000
bun run start:gateway                               # no watch
bun test                                            # all tests, silent (LOG_LEVEL=silent via preload)
bun test test/unit/outbox.writer.test.ts            # single file
bun test -t "deriveIdempotencyKey"                  # by test name
bun run typecheck                                   # tsc --noEmit
docker compose down                                 # stop services
```

## Linear Project

- Team: **Siobytes** (key `SIO`) — commit format: `SIO-XX: message`
- Project: [Event Gate](https://linear.app/siobytes/project/event-gate-9bf5601b0c39/overview) (status: Backlog, lead: Simon Owusu)
- All issues are assigned to Simon Owusu (`me` via the Linear MCP `assignee` arg).
- **Every approved plan MUST have a Linear issue in this project before implementation begins.** When exiting plan mode or completing a planning session, create the issue with the full plan (goals, steps, acceptance criteria, verification) and add it to the Event Gate project.
- **Every Linear issue MUST include the spec and plan paths in its description** when a spec/plan exists. Format as a section near the top:
  ```
  **Spec:** `docs/superpowers/specs/<YYYY-MM-DD>-<topic>-design.md`
  **Plan:** `docs/superpowers/plans/<YYYY-MM-DD>-<topic>.md`
  ```
  This applies to new issues AND to follow-up / cleanup tickets that derive from the same spec — they reference the parent spec/plan paths so a reader can recover the full context without grepping git history.
- Never set issues to "Done" without explicit user approval.
- Always preserve existing content when updating Linear issues (append, don't replace).

## Critical Rules

### Workflow

- **NEVER commit** without explicit user authorization (slash commands count as authorization).
- **NEVER push to `main`** directly for code changes — all code goes through PR review.
- Token usage and budget are NOT your concern — execute instructions as given.
- Always check ports before starting servers: `lsof -i :3000` (gateway), `lsof -i :9092` (Kafka).
- Kill background processes when finished.

### Code

- **No emojis** in code, logs, comments, commit messages, or any output. Use plain words.
- **TypeScript strict mode, never use `any`.** Forbidden: `: any`, `as any`, `Function`, `Record<string, any>`.
  - Tool/handler args → `z.infer<typeof schema>` or `unknown` with `typeof` guards.
  - Opaque payload fields (e.g. the raw webhook body before any inspection) → `unknown`, narrow at the use site.
  - Generic helpers → `<T>(x: T): T` to preserve caller types end-to-end.
- **Zod v4 for runtime validation** at config boundaries (env vars). The HTTP body itself is intentionally not validated — see the Contract section. Where Zod is used, prefer `.safeParse()` over `try { .parse() }`, `.strictObject()` for closed shapes, and `.describe()` so the schema doubles as docs.
- **TS module specifiers include the `.ts` extension** (`import { config } from "../config/index.ts"`) — required by `tsconfig.allowImportingTsExtensions` + Bun. Keep this in new imports.
- Named exports preferred.

### Logging

- Use `getLogger(component)` from `src/logging/index.ts`. Never `console.log` / `console.error` in `src/`.
- Errors as `{ err }` (Pino convention); first arg is the bindings object, second is the message: `log.error({ err, resourceId }, "kafka publish failed")`.
- Output is ECS NDJSON (`@timestamp`, `log.level`, `service.name`, `service.version`, `service.environment`, `ecs.version`, `component`, `message`).
- **Never** use `pino.transport()` — it relies on worker threads that don't work in Bun. The logger factory uses a synchronous destination.
- `LOG_LEVEL` env var controls the level (`trace|debug|info|warn|error|fatal|silent`). Tests run silent via `test/preload.ts`.

### Comments

File headers: single-line relative path only: `// src/services/pricing.ts`.

ALWAYS REMOVE: multi-line file header JSDoc, JSDoc restating function/parameter names, obvious `@returns`, section separators.

ALWAYS KEEP: Zod `.describe()` calls, business logic "why" comments (non-obvious algorithm explanations, alert signature / idempotency key reasoning), references to incident tickets if added later.

### Testing

- Outbox (writer, drainer, backoff), opportunistic idempotency-key derivation, and config/factory dispatch are the unit-tested layers (`test/unit/`). Kafka I/O is exercised manually via the local smoke test in `docs/development/getting-started.md`.
- Tests use `bun:test` with `bunfig.toml` `[test]` `root = "./test"` + `preload = ["./test/preload.ts"]` setting `LOG_LEVEL=silent`.
- Run `bun run typecheck` and `bun test` after every change.
- If you touch validation or config shape, also run the prod-safety probes: `ENVIRONMENT=prod KAFKA_PROVIDER=local bun run start:gateway` (expect Zod error: provider=local not allowed in prod), and equivalents for `msk` without brokers/arn and `confluent` without credentials.

### Servers / ports

| Port | Service |
|---|---|
| 3000 | gateway (Bun.serve) |
| 9092 | Redpanda (Kafka API) |
| 8082 | Redpanda (Pandaproxy) |
| 9644 | Redpanda (admin API) |

## Out of scope (do not add without discussion)

Webhook auth (AutoOps connectors don't support native HMAC; defer until v2 adds shared-token header validation), Flink rolling aggregates, OpenTelemetry instrumentation, Bun workspace catalogs (single-package repo), runtime provider switching (KAFKA_PROVIDER is read once at startup), connection pooling at the provider layer, CDC-style outbox draining (Debezium etc. — we are the publisher), multi-process outbox drainers (single-writer per file), a CLI for replaying `failed` outbox rows (defer until the first incident calls for it), Bun Worker threads for the drainer, exactly-once Kafka delivery (downstream consumers may dedupe on the opportunistic `idempotencyKey` header), AutoOps-body validation/normalization (downstream consumers own that), parametric paths (/webhooks/:vendor/:product), topic-name templates, hot reload of routes (server.reload()), per-route auth/validation/normalization/response shaping, a mounted ROUTES_FILE source.

The previous deferrals of hot route reload (`server.reload()`), a mounted routes file, and admin-endpoint authentication are now addressed by SIO-803 (`docs/superpowers/specs/2026-05-20-admin-routes-endpoint-design.md`). Webhook authentication for public webhook paths remains deferred.

> Note: the outbox is **not** a "database integration in this repo" in the originally-deferred sense — that exclusion is about *downstream* domain storage. The outbox is a transport-layer durability buffer for the gateway itself.
