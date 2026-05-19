# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Project-specific documentation lives under `docs/` (index at `docs/README.md`), organised per `guides/documentation-guide.md`. Portable, project-agnostic patterns live under `guides/`. When adding a doc, place it in the matching `docs/` subdirectory and update `docs/README.md`.

## Project Overview

eventgate is a Bun ingestion service for Elastic AutoOps webhook notifications. Two independent processes share configuration and types but only ever communicate through Kafka. Flow: Elastic AutoOps → HTTP gateway (Bun.serve) → Kafka (Redpanda locally) → Couchbase writer → `autoops_events` (history) + `autoops_state` (rolling state).

## Current State

Single-package Bun project (no workspaces). Conforms to the team `guides/` for: Zod v4 schemas with `.describe()` and `.safeParse()` at boundaries, the 4-pillar configuration pattern (`src/config/{defaults,envMapping,schemas,loader,index}.ts`), Pino 10 + ECS NDJSON logging via `@elastic/ecs-pino-format` with a synchronous Bun-compatible destination, and `bunfig.toml` test wiring with a silent-log preload. Tests live under `test/unit/`. Phases that are explicitly deferred: OpenTelemetry instrumentation (Phase 3), Couchbase Capella hardening — TLS-only config, timeout profiles, error classification, circuit breaker (Phase 4), and webhook auth.

## Architecture

```
src/
  config/                 4-pillar config (defaults, envMapping, schemas, loader, index)
  gateway/                Bun.serve HTTP receiver
    index.ts              entry point
    routes.ts             object-style routes (Bun 1.2+)
    schema.ts             Zod v4 webhook schema with .describe()
  writer/
    index.ts              kafkajs consumer + Couchbase projector
  kafka/
    producer.ts           EventProducer factory (publishRaw/Normalized/Dlq)
    consumer.ts           kafkajs consumer factory
  couchbase/
    client.ts             couchbase.connect + scope/collection handles
    projection.ts         pure functions for doc keys + evolveState
  logging/
    index.ts              ILogger + Pino+ECS factory (sync destination)
  normalize.ts            pure normalization (severity, status, ids)
  types.ts                shared TS types (no Zod here)
test/
  preload.ts              sets LOG_LEVEL=silent for bun test
  unit/
    normalize.test.ts
    projection.test.ts
```

### Two-process model

| Process | Entry | Responsibility |
|---|---|---|
| **gateway** | `src/gateway/index.ts` | `POST /webhooks/elastic/autoops` — validate (Zod `.safeParse`), normalize, publish raw + normalized events, return 202. `/healthz` reports producer status. 503 on Kafka publish failure (never silently drop). |
| **writer** | `src/writer/index.ts` | kafkajs consumer on `ops.elastic.autoops.events.v1` (group `autoops-couchbase-writer-v1`). Upserts history doc + rolling state doc per message. Parse failures and missing required fields go to `ops.elastic.autoops.dlq.v1` — the consumer never throws on bad messages. |

The gateway and writer never call each other in-process — Kafka is the only coupling. Add new downstream behavior (Slack, PagerDuty, aggregates) as a **separate consumer group** on the events topic, not bolted into the writer.

### Kafka topics

- `ops.elastic.autoops.raw.v1` — verbatim webhook body for replay.
- `ops.elastic.autoops.events.v1` — normalized events. Headers (`source`, `eventType`, `severity`, `schemaVersion`, `idempotencyKey`) let consumers filter without parsing the body.
- `ops.elastic.autoops.dlq.v1` — malformed messages from the writer.

### Couchbase doc model

- `autoops_events` (append-only history) — key `autoops::event::<resourceId>::<occurredAt>::<idempotencyKey>`.
- `autoops_state` (rolling state per alert) — key `autoops::state::<resourceId>::<alertSignature>`.
- `evolveState` (in `src/couchbase/projection.ts`) is the **only** place that constructs an `AutoOpsStateDoc`. The writer calls it with the previous doc (or `null`) and the new event.

### Normalization contract (`src/normalize.ts`)

- `alertSignature = slugify(resourceId :: title)` — groups opened+closed of the same alert into one state doc.
- `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` — retries upsert the same history doc; opened+closed pair produces two distinct history docs because `status` differs.
- AutoOps docs spell status `open` / `close`; operator templates often emit `opened` / `closed`. `normalizeStatus` accepts both and emits `opened` | `closed` | `unknown`.

## Config shape (4-pillar)

```
config.app.{name, version, environment, tenant}
config.server.{port}
config.kafka.{brokers, clientIdGateway, clientIdWriter, groupId, topics:{raw, events, dlq}}
config.couchbase.{connStr, username, password, bucket, scope, historyCollection, stateCollection}
config.observability.{logLevel}
```

- `src/config/defaults.ts` — every key has a default; `version` comes from `package.json`.
- `src/config/envMapping.ts` — explicit env-var → field mapping; returns deep-partial overrides.
- `src/config/schemas.ts` — Zod v4 `strictObject` + `.superRefine()` prod-safety: no `localhost` in `couchbase.connStr` / `kafka.brokers` when `app.environment === "prod"`, `couchbases://` required, default password forbidden.
- `src/config/loader.ts` — merge defaults + env, validate via `.safeParse()`, lazy Proxy singleton with `resetConfigCache()` for tests.

`config` is a Proxy — validation runs the first time any field is read, not at module import.

## Commands

```bash
bun install
docker compose up -d                                # Redpanda + Couchbase, topics auto-created
bun run dev:gateway                                 # watch mode, port 3000
bun run dev:writer                                  # watch mode
bun run start:gateway                               # no watch
bun run start:writer                                # no watch
bun test                                            # all tests, silent (LOG_LEVEL=silent via preload)
bun test test/unit/normalize.test.ts                # single file
bun test -t "buildIdempotencyKey"                   # by test name
bun run typecheck                                   # tsc --noEmit
docker compose down                                 # stop services (add -v to drop Couchbase volume)
```

Before first run after `docker compose up -d`: create bucket `ops` and collections `autoops_events` and `autoops_state` under `_default` scope via the Couchbase UI at http://localhost:8091 (Administrator / password). Redpanda creates topics automatically via `redpanda-init`.

## Linear Project

- Team: **Siobytes** (key `SIO`) — commit format: `SIO-XX: message`
- Project: [Event Gate](https://linear.app/siobytes/project/event-gate-9bf5601b0c39/overview) (status: Backlog, lead: Simon Owusu)
- All issues are assigned to Simon Owusu (`me` via the Linear MCP `assignee` arg).
- **Every approved plan MUST have a Linear issue in this project before implementation begins.** When exiting plan mode or completing a planning session, create the issue with the full plan (goals, steps, acceptance criteria, verification) and add it to the Event Gate project.
- Never set issues to "Done" without explicit user approval.
- Always preserve existing content when updating Linear issues (append, don't replace).

## Critical Rules

### Workflow

- **NEVER commit** without explicit user authorization (slash commands count as authorization).
- **NEVER push to `main`** directly for code changes — all code goes through PR review.
- Token usage and budget are NOT your concern — execute instructions as given.
- Always check ports before starting servers: `lsof -i :3000` (gateway), `lsof -i :9092` (Kafka), `lsof -i :8091` (Couchbase UI).
- Kill background processes when finished.

### Code

- **No emojis** in code, logs, comments, commit messages, or any output. Use plain words.
- **TypeScript strict mode, never use `any`.** Forbidden: `: any`, `as any`, `Function`, `Record<string, any>`.
  - Tool/handler args → `z.infer<typeof schema>` or `unknown` with `typeof` guards.
  - Opaque payload fields (e.g. the raw webhook on `NormalizedEvent.raw`) → `unknown`, narrow at the use site.
  - Generic helpers → `<T>(x: T): T` to preserve caller types end-to-end.
- **Zod v4 for runtime validation** at every system boundary (HTTP body, env vars). Use `.safeParse()`, not `try { .parse() }`. Use `.strictObject()` for closed shapes. Use `.describe()` on schema fields so the schema doubles as docs.
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

### Idempotency

The writer **must** stay idempotent — Kafka retries and consumer rebalances will re-deliver messages. Don't add side effects that aren't safe to replay. Couchbase upserts keyed by `historyDocKey(event)` and `stateDocKey(event)` are the only mutations; both are safe.

### Testing

- Pure logic (normalize, projection) is the unit-tested layer (`test/unit/`). Kafka and Couchbase are exercised manually via the README smoke test.
- Tests use `bun:test` with `bunfig.toml` `[test]` `root = "./test"` + `preload = ["./test/preload.ts"]` setting `LOG_LEVEL=silent`.
- Run `bun run typecheck` and `bun test` after every change.
- If you touch validation or config shape, also run the prod-safety probe: `ENVIRONMENT=prod COUCHBASE_CONNSTR=couchbase://localhost bun run start:gateway` — expected to exit with a Zod refinement error citing `localhost-in-prod`.

### Servers / ports

| Port | Service |
|---|---|
| 3000 | gateway (Bun.serve) |
| 9092 | Redpanda (Kafka API) |
| 8082 | Redpanda (Pandaproxy) |
| 8091–8096 | Couchbase Server |
| 11210 | Couchbase memcached |

## Out of scope (do not add without discussion)

Webhook auth (AutoOps connectors don't support native HMAC; defer until v2 adds shared-token header validation), Flink rolling aggregates, the Couchbase Kafka Sink Connector, a Dockerfile, OpenTelemetry instrumentation (Phase 3 of the conformance plan — deferred), Couchbase Capella hardening (Phase 4 — TLS connection-string validation, timeout profiles, `AmbiguousTimeoutError` handling, circuit breaker), and Bun workspace catalogs (single-package repo).
