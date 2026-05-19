# Architecture Overview

> **Targets:** Bun 1.3.11+ | TypeScript 5.x
> **Last updated:** 2026-05-19
> **Conventions:** See [../../guides/documentation-guide.md](../../guides/documentation-guide.md)

eventgate is a single-process Bun service that ingests Elastic AutoOps webhook notifications and durably publishes them to Kafka. The gateway validates, normalizes, and produces; everything downstream — alerting, projection, fan-out — is implemented as separate consumers on the events topic, in other services.

## Data Flow

```
+------------------+      POST        +------------------+    produce     +------------------+
|                  | ---------------> |                  | -------------> |                  |
|  Elastic AutoOps |                  |  gateway         |                |  Kafka           |
|  (webhook)       |                  |  Bun.serve       |                |  raw.v1          |
|                  | <--- 202 ack --- |  port 3000       |                |  events.v1       |
+------------------+                  +--------+---------+                |  dlq.v1          |
                                               |                          +--------+---------+
                                               |                                   |
                                               v                                   v
                                       +------------------+                +------------------+
                                       | KafkaProvider    |                |  downstream      |
                                       | factory          |                |  consumers       |
                                       | local / msk /    |                |  (other services)|
                                       | confluent        |                |                  |
                                       +------------------+                +------------------+
```

## Process

| Process | Entry point | Responsibility |
|---------|------------|----------------|
| gateway | `src/gateway/index.ts` | Validate, normalize, publish raw + normalized events, return `202`. Reports producer health on `/healthz`. |

There is no writer or projector in this repo. Add new downstream behaviour (Slack, PagerDuty, rolling aggregates, sinks into a database) as a **separate consumer group** on `events.v1` — never bolt it into the gateway.

## Kafka Topics

| Topic | Producer | Purpose |
|-------|----------|---------|
| `ops.elastic.autoops.raw.v1` | gateway | Verbatim webhook body, keyed by `resourceId`, retained for replay |
| `ops.elastic.autoops.events.v1` | gateway | Normalized event, keyed by `resourceId`; headers carry `source`, `eventType`, `severity`, `schemaVersion`, `idempotencyKey` for filter-without-parse |
| `ops.elastic.autoops.dlq.v1` | gateway (reserved) | DLQ slot. The gateway currently rejects malformed bodies with `400` instead of publishing to the DLQ; the topic exists so future consumers can quarantine messages they cannot process. |

## Kafka Provider Factory

The gateway connects to Kafka through a provider abstraction selected by `KAFKA_PROVIDER`:

| Provider | Auth | When |
|----------|------|------|
| `local` | PLAINTEXT | Local development against Redpanda |
| `msk` | OAUTHBEARER (IAM), TLS, or PLAINTEXT | AWS MSK / MSK Serverless |
| `confluent` | SASL/PLAIN over TLS | Confluent Cloud |

See [kafka-provider-factory.md](kafka-provider-factory.md) for env vars, field requirements, and the portable pattern.

## Normalization Contract

The lenient inbound schema lives in `src/gateway/schema.ts`; the pure normalization function is `normalizeElasticAutoOps()` in `src/normalize.ts`.

| Field | Rule | Reason |
|-------|------|--------|
| `severity` | Lowercase, map `high|medium|low`; anything else → `"unknown"` | AutoOps docs spell these `High|Medium|Low`, operators sometimes lowercase |
| `status` | Lowercase, map both `open`/`opened` → `"opened"`, `close`/`closed`/`resolved` → `"closed"` | AutoOps docs say `open|close`; templates and the resolved-event flow use other spellings |
| `alertSignature` | `slugify(resourceId :: title)` | Stable across the open + close pair of the same alert — useful for downstream rollup |
| `idempotencyKey` | `sha256(resourceId :: title :: status :: startTime :: endTime)` | Stable across retries of the same delivery; differs between the open and close of the same alert because `status` differs |
| `affectedNodes` / `affectedIndices` | CSV string or `string[]` accepted; coerced to `string[]` | AutoOps templates emit either format |

### Hyphenated keys and synthetic test bodies

`src/gateway/schema.ts` maps AutoOps' hyphenated default keys (`deployment-id`, `start-time`, ...) to camelCase before validation, and detects AutoOps' "Validate" button test body (every value is an un-substituted `${VAR}` placeholder) so the gateway can acknowledge it with `202` instead of failing schema validation.

## Failure Handling

| Failure | Where | Action |
|---------|-------|--------|
| Invalid JSON body | gateway (`src/gateway/routes.ts`) | Return `400`, do not publish |
| Schema validation failure | gateway | Return `400` with Zod issues, do not publish |
| Kafka publish failure | gateway | Log a warning with `{ err, resourceId }`, still return `202` (receive-fast, process-async) |
| Producer disconnected | gateway `/healthz` | Return `503` |

## Configuration

Configuration follows the 4-pillar pattern — defaults, env mapping, schema, loader — and is documented in [../configuration/environment-variables.md](../configuration/environment-variables.md). Production safety rules are enforced by `.superRefine()` in `src/config/schemas.ts`:

- `provider=local` is rejected when `ENVIRONMENT=prod` (prod must use `msk` or `confluent`).
- `msk` requires `MSK_REGION` plus one of `MSK_CLUSTER_ARN` / `MSK_BROKERS`.
- `confluent` requires `CONFLUENT_BOOTSTRAP_SERVERS`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET`.

For the project-agnostic 4-pillar pattern see [../../guides/4-pillar-configuration-guide.md](../../guides/4-pillar-configuration-guide.md). For the project-agnostic provider factory pattern see [../../guides/kafka-provider-factory.md](../../guides/kafka-provider-factory.md).

## Out of Scope

These are documented separately in `CLAUDE.md` and the v1 plan; flagged here so readers know not to look for them in this overview:

- Webhook authentication (deferred to v2 once a shared-token header is agreed).
- Flink rolling aggregates.
- OpenTelemetry instrumentation.
- Any database integration in this repo — downstream consumers own their storage.

## See Also

- [kafka-provider-factory.md](kafka-provider-factory.md) — the provider abstraction the gateway uses to reach Kafka.
- [../api/webhooks.md](../api/webhooks.md) — request and response shapes for the gateway endpoints.
- [../deployment/aws-ecs.md](../deployment/aws-ecs.md) — how the gateway is deployed.
- [../operations/logging.md](../operations/logging.md) — log shape and filter patterns.
- [../plans/v1-implementation-plan.md](../plans/v1-implementation-plan.md) — original design rationale (historical; predates Couchbase removal).

## Changelog

| Date | Change |
|------|--------|
| 2026-05-19 | Initial architecture overview created |
| 2026-05-19 | Rewritten for gateway-only architecture: removed writer + Couchbase doc model, added Kafka provider factory layer (SIO-795) |
