# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Project-specific documentation lives under `docs/` (index at `docs/README.md`), organised per `guides/documentation-guide.md`. Portable, project-agnostic patterns live under `guides/`. When adding a doc, place it in the matching `docs/` subdirectory and update `docs/README.md`.

## Project Overview

eventgate is a single-process Bun ingestion service for Elastic AutoOps webhook notifications. Flow: Elastic AutoOps → HTTP gateway (Bun.serve) → Kafka (Redpanda locally / AWS MSK / Confluent Cloud). Anything downstream of Kafka (alerting, projection, storage) lives in other services as separate consumers on the events topic.

## Current State

Single-package Bun project (no workspaces). Conforms to the team `guides/` for: Zod v4 schemas with `.describe()` and `.safeParse()` at boundaries, the 4-pillar configuration pattern (`src/config/{defaults,envMapping,schemas,loader,index}.ts`), Pino 10 + ECS NDJSON logging via `@elastic/ecs-pino-format` with a synchronous Bun-compatible destination, and `bunfig.toml` test wiring with a silent-log preload. Tests live under `test/unit/`. Kafka backend selection is abstracted behind a `KafkaProvider` factory (`src/kafka/providers/`) — local Redpanda, AWS MSK (IAM/TLS/none), or Confluent Cloud (SASL/PLAIN + TLS). Phases explicitly deferred: OpenTelemetry instrumentation, webhook auth.

## Architecture

```
src/
  config/                 4-pillar config (defaults, envMapping, schemas, loader, index)
  gateway/                Bun.serve HTTP receiver
    index.ts              entry point — wires KafkaProvider + EventProducer
    routes.ts             object-style routes (Bun 1.2+)
    schema.ts             Zod v4 webhook schema with .describe()
  kafka/
    producer.ts           EventProducer wrapping @platformatic/kafka Producer
    providers/
      types.ts            KafkaProvider, KafkaConnectionConfig, MskAuthMode
      errors.ts           KafkaProviderError + ProviderErrorCode
      local.ts            PLAINTEXT, no SASL/TLS
      msk.ts              SASL/OAUTHBEARER + TLS (IAM); broker discovery; 60s token cache
      confluent.ts        SASL/PLAIN + TLS
      index.ts            createKafkaProvider(config) factory
  logging/
    index.ts              ILogger + Pino+ECS factory (sync destination)
  normalize.ts            pure normalization (severity, status, ids)
  types.ts                shared TS types (no Zod here)
test/
  preload.ts              sets LOG_LEVEL=silent for bun test
  unit/
    normalize.test.ts
    config.kafka-provider.test.ts
    kafka.providers.factory.test.ts
    kafka.providers.msk.test.ts
```

### Single-process model

| Process | Entry | Responsibility |
|---|---|---|
| **gateway** | `src/gateway/index.ts` | `POST /webhooks/elastic/autoops` — validate (Zod `.safeParse`), normalize, publish raw + normalized events, return 202. `/healthz` reports producer status (503 when disconnected). Kafka publish failures are logged at `warn` but still return 202 (receive-fast, process-async). |

Add new downstream behavior (Slack, PagerDuty, aggregates, sinks into a database) as a **separate consumer group** on `ops.elastic.autoops.events.v1` in another service — never bolt it into the gateway.

### Kafka topics

- `ops.elastic.autoops.raw.v1` — verbatim webhook body for replay.
- `ops.elastic.autoops.events.v1` — normalized events. Headers (`source`, `eventType`, `severity`, `schemaVersion`, `idempotencyKey`) let consumers filter without parsing the body.
- `ops.elastic.autoops.dlq.v1` — DLQ slot reserved for downstream consumers.

### Kafka provider factory

Selected by `KAFKA_PROVIDER`. The factory (`src/kafka/providers/index.ts:createKafkaProvider`) dispatches on the value and returns a `KafkaProvider` that exposes `getConnectionConfig()` and `close()`. The producer (`src/kafka/producer.ts`) is provider-agnostic.

- **local**: `KAFKA_LOCAL_BOOTSTRAP_SERVERS` (default `localhost:9092`).
- **msk**: `MSK_REGION` + (`MSK_CLUSTER_ARN` or `MSK_BROKERS`); optional `MSK_AUTH_MODE` (iam/tls/none, default iam). AWS SDK + signer are lazy-imported so non-MSK runs don't load them.
- **confluent**: `CONFLUENT_BOOTSTRAP_SERVERS` + `CONFLUENT_API_KEY` + `CONFLUENT_API_SECRET`.

Portable pattern lives at `guides/kafka-provider-factory.md`. Project-specific application at `docs/architecture/kafka-provider-factory.md`.

### Normalization contract (`src/normalize.ts`)

- `alertSignature = slugify(resourceId :: title)` — stable across the open + close pair of the same alert; useful for downstream rollup.
- `idempotencyKey = sha256(resourceId :: title :: status :: startTime :: endTime)` — stable across retries of the same delivery; differs between the open and close of the same alert because `status` differs.
- AutoOps docs spell status `open` / `close`; operator templates often emit `opened` / `closed`. `normalizeStatus` accepts both and emits `opened` | `closed` | `unknown`.

## Config shape (4-pillar)

```
config.app.{name, version, environment, tenant}
config.server.{port}
config.kafka.{provider, clientId, topics:{raw, events, dlq}, local:{bootstrapServers}, msk:{region, clusterArn, brokers, authMode}, confluent:{bootstrapServers, apiKey, apiSecret}}
config.observability.{logLevel}
```

- `src/config/defaults.ts` — every key has a default; `version` comes from `package.json`. Default provider is `local`.
- `src/config/envMapping.ts` — explicit env-var → field mapping; returns deep-partial overrides.
- `src/config/schemas.ts` — Zod v4 `strictObject` + `.superRefine()` cross-field rules:
  - `provider=msk` requires `msk.region` AND (`msk.clusterArn` OR `msk.brokers`).
  - `provider=confluent` requires `confluent.bootstrapServers` + `apiKey` + `apiSecret`.
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
bun test test/unit/normalize.test.ts                # single file
bun test -t "buildIdempotencyKey"                   # by test name
bun run typecheck                                   # tsc --noEmit
docker compose down                                 # stop services
```

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
- Always check ports before starting servers: `lsof -i :3000` (gateway), `lsof -i :9092` (Kafka).
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

### Testing

- Pure logic (normalize) and config/factory dispatch are the unit-tested layers (`test/unit/`). Kafka I/O is exercised manually via the local smoke test in `docs/development/getting-started.md`.
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

Webhook auth (AutoOps connectors don't support native HMAC; defer until v2 adds shared-token header validation), Flink rolling aggregates, OpenTelemetry instrumentation, Bun workspace catalogs (single-package repo), any database integration in this repo (downstream consumers own their storage), runtime provider switching (KAFKA_PROVIDER is read once at startup), connection pooling at the provider layer.
